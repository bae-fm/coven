//! Cache publication and eviction obey the same bytes-before-row rule as
//! staged files. Cache bytes are written and read with no writer held (§21.2).

use super::DatabaseAccess;
use crate::{sqlite::DatabaseConnection, DbError, FileRef};
use coven_crypto::FILE_CHUNK_TAG_LEN;
use coven_foundation::files::StoreDirAccess;

pub(super) use crate::internal_schema::{
    CACHE_COMPLETE_FILE as COMPLETE_FILE, CACHE_HEADER as HEADER,
};
use coven_foundation::files::{FileArea, FileName, ObservationError};

/// The cache's name for a stored file's bytes, which every file over them
/// shares. Only a stored file is cached.
pub(crate) fn id(file: &FileRef) -> Result<String, DbError> {
    let (location, _) = stored(file)?;
    Ok(file_id(location.uploader(), location.object()))
}
/// A stored file's location and content hash.
fn stored(file: &FileRef) -> Result<(&crate::FileLocation, coven_crypto::ContentHash), DbError> {
    match file.place() {
        crate::FilePlace::Cloud {
            location,
            content_hash,
            ..
        } => Ok((location, *content_hash)),
        crate::FilePlace::DeviceOnly { .. } => Err(DbError::Invariant {
            what: "only a stored file has cached bytes",
        }),
    }
}
/// The cache's name for an uploader's stored object, which coven's pin view
/// computes too (Appendix C.1).
pub(crate) fn file_id(
    uploader: coven_foundation::id_source::DeviceId,
    file: coven_foundation::id_source::FileId,
) -> String {
    format!("{}/{}", uploader.0, file)
}
fn tick(db: &DatabaseConnection) -> Result<i64, DbError> {
    db.query_row(
        "SELECT coalesce(max(last_read),0)+1 FROM _coven_cache",
        [],
        |r| r.get(0),
    )
}
/// One stored entry and where `index` lies in it: a verified range of
/// consecutive chunks, a header, or a checked whole file.
fn covering(
    db: &DatabaseConnection,
    file: &FileRef,
    index: i64,
) -> Result<Option<(String, i64, i64, i64)>, DbError> {
    let id = id(file)?;
    if let Some(whole) = db
        .query(
            "SELECT path,chunk,chunks,size FROM _coven_cache
             WHERE namespace=?1 AND file_id=?2 AND chunk=?3",
            (file.namespace(), &id, COMPLETE_FILE),
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )?
        .pop()
    {
        return Ok(Some(whole));
    }
    let lower = if index == HEADER { HEADER } else { 0 };
    Ok(db
        .query(
            "SELECT path,chunk,chunks,size FROM _coven_cache
             WHERE namespace=?1 AND file_id=?2 AND chunk>=?3 AND chunk<=?4
             ORDER BY chunk DESC LIMIT 1",
            (file.namespace(), &id, lower, index),
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )?
        .pop()
        .filter(|(_, first, chunks, _): &(String, i64, i64, i64)| index < first + chunks))
}
/// Encrypted byte offset and length of `index` in the stored entry
/// beginning at `first`.
#[expect(
    clippy::map_err_ignore,
    reason = "a stored value that won't convert or decode is DamagedDatabase"
)]
fn within(file: &FileRef, first: i64, index: i64) -> Result<(u64, usize), DbError> {
    if index == HEADER {
        return Ok((0, coven_format::file::FILE_HEADER_LEN));
    }
    let header = coven_format::file::FileHeader::new(file.size());
    let chunk = |index: i64| -> Result<_, DbError> {
        Ok(header.chunk(u64::try_from(index).map_err(|_| DbError::DamagedDatabase)?)?)
    };
    let target = chunk(index)?;
    let base = if first == COMPLETE_FILE {
        0
    } else {
        chunk(first)?.offset
    };
    Ok((
        target.offset - base,
        target.plaintext_length + FILE_CHUNK_TAG_LEN,
    ))
}
/// The cached bytes of chunk `index`, or the header (-1). The entry is
/// looked up, and its read recorded, under the writer; its bytes are read
/// with no writer held (§21.2). An entry evicted between the lookup and
/// opening its bytes is no longer cached.
#[expect(
    clippy::map_err_ignore,
    reason = "a stored value that won't convert or decode is DamagedDatabase"
)]
pub(super) fn read<L>(
    access: &DatabaseAccess<L>,
    file: &FileRef,
    index: i64,
) -> Result<Option<Vec<u8>>, DbError> {
    let entry = access.with_writer(|db| db.read_transaction(|| covering(db, file, index)))?;
    let Some((path, first, _, length)) = entry else {
        return Ok(None);
    };
    let name = FileName::new(&path).map_err(|_| DbError::DamagedDatabase)?;
    let reader = match access.directory.file(FileArea::Cache, &name).open_reader() {
        Ok(reader) => reader,
        Err(ObservationError::Missing(_)) => {
            let recorded = access.with_writer(|db| {
                db.query_row(
                    "SELECT EXISTS(SELECT 1 FROM _coven_cache WHERE path=?1)",
                    [&path],
                    |r| r.get::<_, bool>(0),
                )
            })?;
            return if recorded {
                Err(DbError::DamagedDatabase)
            } else {
                Ok(None)
            };
        }
        Err(error) => return Err(observation(error)),
    };
    if reader.size() != u64::try_from(length).map_err(|_| DbError::DamagedDatabase)? {
        return Err(DbError::DamagedDatabase);
    }
    let (offset, length) = within(file, first, index)?;
    let bytes = reader.read_at(offset, length).map_err(observation)?;
    access.with_writer(|db| {
        db.transaction(|db| {
            db.internal_execute(
                "UPDATE _coven_cache SET last_read=?2 WHERE path=?1",
                (&path, tick(db)?),
            )?;
            Ok(())
        })
    })?;
    Ok(Some(bytes))
}
/// Publish one verified range of `chunks` consecutive chunks from `first`,
/// or the header (`first` = -1), as one file and one row (Appendix C.3). Its name
/// is recorded as being written before any byte is written, the bytes are
/// written with no writer held (§21.2), and the row replaces that record in
/// the transaction that publishes it. The shared cache-write lock, held
/// throughout, keeps opening from taking the record as abandoned.
pub(super) fn put<L>(
    access: &DatabaseAccess<L>,
    file: &FileRef,
    first: i64,
    chunks: i64,
    name: &FileName,
    bytes: &[u8],
) -> Result<(), DbError> {
    if chunks < 1 || (first < 0 && chunks != 1) {
        return Err(DbError::Invariant {
            what: "a cached range covers whole chunks, or only the header",
        });
    }
    let _writing = access.directory.lock_cache_write()?;
    let recorded = access.with_writer(|db| {
        db.transaction(|db| {
            if db.query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM _coven_cache WHERE path=?1 UNION ALL
                     SELECT 1 FROM _coven_cache_writes WHERE path=?1 UNION ALL
                     SELECT 1 FROM _coven_file_removals WHERE path=?1 AND area='cache')",
                [name.as_str()],
                |r| r.get::<_, bool>(0),
            )? {
                return Err(DbError::FileNameReused { name: name.clone() });
            }
            // Stored entries never overlap: a range already covered in part
            // is not written at all.
            if overlaps(db, file, first, chunks)? {
                return Ok(false);
            }
            db.internal_execute(
                "INSERT INTO _coven_cache_writes(path) VALUES(?1)",
                [name.as_str()],
            )?;
            Ok(true)
        })
    })?;
    if !recorded {
        return Ok(());
    }
    let published = access
        .directory
        .file(FileArea::Cache, name)
        .write_recorded(bytes)
        .map_err(DbError::from)
        .and_then(|()| {
            access.with_writer(|db| {
                db.transaction(|db| publish_range(db, file, first, chunks, name, bytes))
            })
        });
    let result = match published {
        Ok(true) => return Ok(()),
        // Another handle published part of this range meanwhile; these
        // bytes are left unused.
        Ok(false) => Ok(()),
        Err(error) => Err(error),
    };
    crate::write_failure::after_cleanup(result, release(access, name))
}
/// Whether any chunk of the range already has a stored entry.
fn overlaps(
    db: &DatabaseConnection,
    file: &FileRef,
    first: i64,
    chunks: i64,
) -> Result<bool, DbError> {
    for index in first..first + chunks {
        if covering(db, file, index)?.is_some() {
            return Ok(true);
        }
    }
    Ok(false)
}
/// Publish a written range's row in place of its record. If another handle
/// published part of the range meanwhile, or no record here names the
/// file's stored bytes any more, its name becomes a recorded removal
/// instead and the result is false.
fn publish_range(
    db: &DatabaseConnection,
    file: &FileRef,
    first: i64,
    chunks: i64,
    name: &FileName,
    bytes: &[u8],
) -> Result<bool, DbError> {
    if db.internal_execute(
        "DELETE FROM _coven_cache_writes WHERE path=?1",
        [name.as_str()],
    )? != 1
    {
        return Err(DbError::Invariant {
            what: "a cache range being published is recorded as being written",
        });
    }
    if overlaps(db, file, first, chunks)? || !named(db, file)? {
        db.internal_execute(
            "INSERT INTO _coven_file_removals(area,path) VALUES('cache',?1)",
            [name.as_str()],
        )?;
        return Ok(false);
    }
    db.internal_execute(
        "INSERT INTO _coven_cache(namespace,file_id,chunk,chunks,path,size,last_read,pinned)
         VALUES(?1,?2,?3,?4,?5,?6,?7,0)",
        rusqlite::params![
            file.namespace(),
            id(file)?,
            first,
            chunks,
            name.as_str(),
            bytes.len() as i64,
            tick(db)?
        ],
    )?;
    Ok(true)
}
/// Remove an unpublished range's bytes: its record becomes a recorded
/// removal, unless publication already made it one, and the bytes go once
/// the writer is free. Returns the failures; a record they leave is
/// removed by the next open.
fn release<L>(access: &DatabaseAccess<L>, name: &FileName) -> Vec<DbError> {
    let recorded = access.with_writer(|db| {
        db.transaction(|db| {
            if db.internal_execute(
                "DELETE FROM _coven_cache_writes WHERE path=?1",
                [name.as_str()],
            )? == 1
            {
                db.internal_execute(
                    "INSERT INTO _coven_file_removals(area,path) VALUES('cache',?1)",
                    [name.as_str()],
                )?;
            }
            Ok(())
        })
    });
    match recorded {
        Ok(()) => access.remove_released(crate::file_removals::choose_cache(&[name
            .as_str()
            .to_owned()])),
        Err(error) => vec![error],
    }
}
/// Give each namespace its declared budget the first time it is declared;
/// `set_cache_budget` changes it after (E8). Two declarations of one
/// namespace are refused.
pub(crate) fn declare_budgets(
    db: &DatabaseConnection,
    namespaces: &[crate::Namespace],
) -> Result<(), DbError> {
    let mut declared = std::collections::BTreeSet::new();
    for namespace in namespaces {
        if !declared.insert(namespace.name.as_str()) {
            return Err(crate::SchemaError::DuplicateNamespace {
                namespace: namespace.name.clone(),
            }
            .into());
        }
    }
    db.transaction(|db| {
        for namespace in namespaces {
            db.internal_execute(
                "INSERT INTO _coven_cache_budgets(namespace,bytes) VALUES(?1,?2)
                 ON CONFLICT(namespace) DO NOTHING",
                (
                    &namespace.name,
                    namespace.cache_budget.to_be_bytes().as_slice(),
                ),
            )?;
        }
        Ok(())
    })
}
#[expect(
    clippy::map_err_ignore,
    reason = "a stored value that won't convert or decode is DamagedDatabase"
)]
pub(super) fn budget(db: &DatabaseConnection, namespace: &str) -> Result<u64, DbError> {
    let bytes = db
        .query(
            "SELECT bytes FROM _coven_cache_budgets WHERE namespace=?1",
            [namespace],
            |r| r.get::<_, Vec<u8>>(0),
        )?
        .pop()
        .ok_or_else(|| DbError::UndeclaredNamespace {
            namespace: namespace.to_owned(),
        })?;
    Ok(u64::from_be_bytes(
        bytes.try_into().map_err(|_| DbError::DamagedDatabase)?,
    ))
}
/// The namespace's cached bytes, maintained with its cache records.
pub(crate) fn total(db: &DatabaseConnection, namespace: &str) -> Result<i64, DbError> {
    Ok(db
        .query(
            "SELECT total FROM _coven_cache_budgets WHERE namespace=?1",
            [namespace],
            |r| r.get::<_, i64>(0),
        )?
        .pop()
        .unwrap_or(0))
}
/// Evict over-budget entries; returns their recorded names, whose bytes the
/// caller removes once the writer is free (Appendix C.3).
#[expect(
    clippy::map_err_ignore,
    reason = "a stored value that won't convert or decode is DamagedDatabase"
)]
pub(super) fn trim(db: &DatabaseConnection, namespace: &str) -> Result<Vec<String>, DbError> {
    db.transaction(|db| {
        let budget = budget(db, namespace)?;
        let mut total = total(db, namespace)?;
        let mut removed = Vec::new();
        while u64::try_from(total).map_err(|_| DbError::DamagedDatabase)? > budget {
            // Chunks need their header for offline opens. Once the last chunk
            // goes, that header competes on recency with every other entry.
            let next = db.query(
                "SELECT path,size FROM _coven_cache AS candidate
                 WHERE namespace=?1 AND pinned=0
                   AND (chunk!=?2 OR NOT EXISTS(
                       SELECT 1 FROM _coven_cache AS chunks
                       WHERE chunks.namespace=candidate.namespace
                         AND chunks.file_id=candidate.file_id AND chunks.chunk>=0))
                 ORDER BY last_read,path LIMIT 1",
                (namespace, HEADER),
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)),
            )?;
            let Some((path, size)) = next.into_iter().next() else {
                break;
            };
            db.internal_execute("DELETE FROM _coven_cache WHERE path=?1", [&path])?;
            db.internal_execute(
                "INSERT INTO _coven_file_removals(area,path) VALUES('cache',?1)",
                [&path],
            )?;
            total -= size;
            removed.push(path);
        }
        Ok(removed)
    })
}
/// Drop a file's cache entries; returns their recorded names, whose bytes
/// the caller removes once the writer is free (Appendix C.3).
pub(super) fn evict(db: &DatabaseConnection, file: &FileRef) -> Result<Vec<String>, DbError> {
    let id = id(file)?;
    db.transaction(|db| remove_entries(db, file.namespace(), &id))
}
/// Pin a verified whole cache file, if one is kept. Returns whether one was.
pub(super) fn pin_complete(db: &DatabaseConnection, file: &FileRef) -> Result<bool, DbError> {
    let (_, hash) = stored(file)?;
    Ok(db.internal_execute(
        "UPDATE _coven_cache SET pinned=1 WHERE namespace=?1 AND file_id=?2 AND chunk=?4 AND checked_hash=?3",
        (file.namespace(), id(file)?, hash.as_bytes().as_slice(), COMPLETE_FILE),
    )? == 1)
}
pub(super) fn unpin(db: &DatabaseConnection, file: &FileRef) -> Result<(), DbError> {
    db.internal_execute(
        "UPDATE _coven_cache SET pinned=0 WHERE namespace=?1 AND file_id=?2",
        (file.namespace(), id(file)?),
    )?;
    Ok(())
}
/// Whether a pinned, checked whole copy of this file is on the device.
pub(crate) fn kept(db: &DatabaseConnection, file: &FileRef) -> Result<bool, DbError> {
    let (_, hash) = stored(file)?;
    db.query_row(
        "SELECT EXISTS(SELECT 1 FROM _coven_cache WHERE namespace=?1 AND file_id=?2 AND chunk=?4 AND pinned=1 AND checked_hash=?3)",
        (file.namespace(), id(file)?, hash.as_bytes().as_slice(), COMPLETE_FILE),
        |r| r.get(0),
    )
}
/// Publish a written whole file as the file's only cache entry, pinned or
/// under the budget; older entries become recorded removals in the same
/// transaction. Its writer wrote the layout the file's size fixes, so that
/// layout's encrypted size is its size on disk. If no record here names the
/// file's stored bytes any more, nothing is published, the name's removal
/// record stays, and the result is false.
#[expect(
    clippy::map_err_ignore,
    reason = "a size past SQLite's integer is TooLarge, which names the field and size"
)]
pub(super) fn publish(
    db: &DatabaseConnection,
    file: &FileRef,
    name: &FileName,
    pinned: bool,
) -> Result<bool, DbError> {
    let encrypted = coven_format::file::FileHeader::new(file.size()).encrypted_size()?;
    let (_, hash) = stored(file)?;
    let size = i64::try_from(encrypted).map_err(|_| DbError::TooLarge {
        field: "cached file",
        actual: encrypted,
        maximum: i64::MAX as u64,
    })?;
    if !named(db, file)? {
        return Ok(false);
    }
    let id = id(file)?;
    remove_entries(db, file.namespace(), &id)?;
    // The reservation's cleanup record goes before the row names its bytes.
    db.internal_execute(
        "DELETE FROM _coven_file_removals WHERE area='cache' AND path=?1",
        [name.as_str()],
    )?;
    db.internal_execute("INSERT INTO _coven_cache(namespace,file_id,chunk,path,size,last_read,pinned,checked_hash) VALUES(?1,?2,?7,?3,?4,?5,?8,?6)",rusqlite::params![file.namespace(),id,name.as_str(),size,tick(db)?,hash.as_bytes().as_slice(),COMPLETE_FILE,pinned])?;
    Ok(true)
}
/// Whether a record here names the stored bytes `file` was taken over. The
/// cache keeps bytes only while one does (§17.7): the transaction taking
/// the last such record off them removes their entries, and one publishing
/// an entry after it would name bytes nothing could read or unpin.
fn named(db: &DatabaseConnection, file: &FileRef) -> Result<bool, DbError> {
    db.query_row(
        "SELECT EXISTS(SELECT 1 FROM _coven_stored_files WHERE namespace=?1 AND cache_id=?2)",
        (file.namespace(), id(file)?),
        |r| r.get(0),
    )
}
/// Delete a file's cache entries and record their names for removal, once
/// no row names them; returns the names.
pub(crate) fn remove_entries(
    db: &DatabaseConnection,
    namespace: &str,
    id: &str,
) -> Result<Vec<String>, DbError> {
    let paths = db.query(
        "DELETE FROM _coven_cache WHERE namespace=?1 AND file_id=?2 RETURNING path",
        (namespace, id),
        |r| r.get::<_, String>(0),
    )?;
    for path in &paths {
        db.internal_execute(
            "INSERT INTO _coven_file_removals(area,path) VALUES('cache',?1)",
            [path],
        )?;
    }
    Ok(paths)
}
fn observation(error: coven_foundation::files::ObservationError) -> DbError {
    match error {
        coven_foundation::files::ObservationError::File(error) => DbError::Disk(error),
        _ => DbError::DamagedDatabase,
    }
}

//! Cache publication and eviction obey the same bytes-before-row rule as attachments.

use crate::{sqlite::DatabaseConnection, DbError, FileRef};
use coven_crypto::FILE_CHUNK_TAG_LEN;

pub(super) use crate::internal_schema::{
    CACHE_COMPLETE_FILE as COMPLETE_FILE, CACHE_HEADER as HEADER,
};
use coven_foundation::files::{FileArea, FileName, StoreDir};

pub(crate) fn id(file: &FileRef) -> Result<String, DbError> {
    Ok(format!("{}/{}", file.uploader().0, file.id()))
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
fn within(file: &FileRef, first: i64, index: i64) -> Result<(u64, usize), DbError> {
    if index == HEADER {
        return Ok((0, coven_format::file::FILE_HEADER_LEN));
    }
    let header = coven_format::file::FileHeader::new(file.plaintext_size());
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
pub(super) fn read(
    db: &DatabaseConnection,
    directory: &StoreDir,
    file: &FileRef,
    index: i64,
) -> Result<Option<Vec<u8>>, DbError> {
    db.transaction(|db| {
        let Some((path, first, _, length)) = covering(db, file, index)? else {
            return Ok(None);
        };
        let name = FileName::new(&path).map_err(|_| DbError::DamagedDatabase)?;
        let reader = directory
            .file(FileArea::Cache, &name)
            .open_reader()
            .map_err(observation)?;
        if reader.size() != u64::try_from(length).map_err(|_| DbError::DamagedDatabase)? {
            return Err(DbError::DamagedDatabase);
        }
        let (offset, length) = within(file, first, index)?;
        let bytes = reader.read_at(offset, length).map_err(observation)?;
        db.internal_execute(
            "UPDATE _coven_cache SET last_read=?2 WHERE path=?1",
            (&path, tick(db)?),
        )?;
        Ok(Some(bytes))
    })
}
/// Publish one verified range of `chunks` consecutive chunks from `first`,
/// or the header (`first` = -1), as one file and one row: its name is
/// recorded for removal before any byte is written, and the row replaces
/// that record in the transaction that publishes it (§16.6).
pub(super) fn put(
    db: &DatabaseConnection,
    directory: &StoreDir,
    file: &FileRef,
    first: i64,
    chunks: i64,
    name: &FileName,
    bytes: &[u8],
) -> Result<(), DbError> {
    if chunks < 1 || (first < 0 && chunks != 1) {
        return Err(DbError::DamagedDatabase);
    }
    let id = id(file)?;
    db.transaction(|db| {
        if db.query_row(
            "SELECT EXISTS(SELECT 1 FROM _coven_cache WHERE path=?1)",
            [name.as_str()],
            |r| r.get::<_, bool>(0),
        )? {
            return Err(DbError::FileNameReused { name: name.clone() });
        }
        db.internal_execute(
            "INSERT INTO _coven_file_removals(area,path) VALUES('cache',?1)",
            [name.as_str()],
        )?;
        Ok(())
    })?;
    // The immediate transaction excludes another process's eviction and startup
    // cleanup. If cleanup won the interval, no bytes have been written yet.
    let result = db.transaction(|db| {
        if !db.query_row(
            "SELECT EXISTS(SELECT 1 FROM _coven_file_removals WHERE area='cache' AND path=?1)",
            [name.as_str()],
            |r| r.get::<_, bool>(0),
        )? {
            return Err(DbError::DamagedDatabase);
        }
        // Another reader may have published part of this range meanwhile;
        // stored entries never overlap, so these bytes are then left unused.
        let overlapping = (first..first + chunks)
            .map(|index| covering(db, file, index).map(|entry| entry.is_some()))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .any(|covered| covered);
        if !overlapping {
            directory.file(FileArea::Cache, name).replace(bytes)?;
            db.internal_execute(
                "INSERT INTO _coven_cache(namespace,file_id,chunk,chunks,path,size,last_read,pinned)
                 VALUES(?1,?2,?3,?4,?5,?6,?7,0)",
                rusqlite::params![
                    file.namespace(),
                    id,
                    first,
                    chunks,
                    name.as_str(),
                    bytes.len() as i64,
                    tick(db)?
                ],
            )?;
        }
        db.internal_execute(
            "DELETE FROM _coven_file_removals WHERE area='cache' AND path=?1",
            [name.as_str()],
        )?;
        Ok(())
    });
    if result.is_err() {
        let cleanup = remove_pending(db, directory, name);
        if let Err(cleanup) = cleanup {
            return Err(DbError::FileCleanup {
                write: result.map_err(Box::new),
                failures: vec![cleanup],
            });
        }
    }
    result
}
fn remove_pending(
    db: &DatabaseConnection,
    directory: &StoreDir,
    name: &FileName,
) -> Result<(), DbError> {
    db.transaction(|db| crate::file_removals::remove(db, directory, name, "cache"))
}
pub(super) fn budget(db: &DatabaseConnection, namespace: &str) -> Result<Option<u64>, DbError> {
    db.query(
        "SELECT bytes FROM _coven_cache_budgets WHERE namespace=?1 AND bytes IS NOT NULL",
        [namespace],
        |r| r.get::<_, Vec<u8>>(0),
    )?
    .into_iter()
    .next()
    .map(|bytes| {
        Ok(u64::from_be_bytes(
            bytes.try_into().map_err(|_| DbError::DamagedDatabase)?,
        ))
    })
    .transpose()
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
pub(super) fn trim(
    db: &DatabaseConnection,
    directory: &StoreDir,
    namespace: &str,
) -> Result<(), DbError> {
    let paths = db.transaction(|db| {
        let Some(budget) = budget(db, namespace)? else {
            return Ok(Vec::new());
        };
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
            db.internal_execute(
                "INSERT INTO _coven_file_removals(area,path) VALUES('cache',?1)",
                [&path],
            )?;
            db.internal_execute("DELETE FROM _coven_cache WHERE path=?1", [&path])?;
            total -= size;
            removed.push(path);
        }
        Ok(removed)
    })?;
    remove_paths(db, directory, paths)
}
pub(super) fn evict(
    db: &DatabaseConnection,
    directory: &StoreDir,
    file: &FileRef,
) -> Result<(), DbError> {
    let id = id(file)?;
    let paths = db.transaction(|db| {
        let paths = db.query(
            "SELECT path FROM _coven_cache WHERE namespace=?1 AND file_id=?2",
            (file.namespace(), &id),
            |r| r.get::<_, String>(0),
        )?;
        for path in &paths {
            db.internal_execute(
                "INSERT INTO _coven_file_removals(area,path) VALUES('cache',?1)",
                [path],
            )?;
        }
        db.internal_execute(
            "DELETE FROM _coven_cache WHERE namespace=?1 AND file_id=?2",
            (file.namespace(), &id),
        )?;
        Ok(paths)
    })?;
    remove_paths(db, directory, paths)
}
fn remove_paths(
    db: &DatabaseConnection,
    directory: &StoreDir,
    paths: Vec<String>,
) -> Result<(), DbError> {
    let mut failures = Vec::new();
    for path in paths {
        match FileName::new(path)
            .map_err(|_| DbError::DamagedDatabase)
            .and_then(|name| remove_pending(db, directory, &name))
        {
            Ok(()) => {}
            Err(error) => failures.push(error),
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(DbError::FileCleanup {
            write: Ok(()),
            failures,
        })
    }
}
pub(super) fn pin_complete(db: &DatabaseConnection, file: &FileRef) -> Result<bool, DbError> {
    let id = id(file)?;
    Ok(db.internal_execute("UPDATE _coven_cache SET pinned=1 WHERE namespace=?1 AND file_id=?2 AND chunk=?4 AND checked_hash=?3",(file.namespace(),id,file.content_hash().as_bytes().as_slice(),COMPLETE_FILE))?==1)
}
pub(super) fn unpin(db: &DatabaseConnection, file: &FileRef) -> Result<(), DbError> {
    db.internal_execute(
        "UPDATE _coven_cache SET pinned=0 WHERE namespace=?1 AND file_id=?2",
        (file.namespace(), id(file)?),
    )?;
    Ok(())
}
pub(crate) fn pinned(db: &DatabaseConnection, file: &FileRef) -> Result<bool, DbError> {
    let id = id(file)?;
    db.query_row("SELECT EXISTS(SELECT 1 FROM _coven_cache WHERE namespace=?1 AND file_id=?2 AND chunk=?4 AND pinned=1 AND checked_hash=?3)",(file.namespace(),id,file.content_hash().as_bytes().as_slice(),COMPLETE_FILE),|r|r.get(0))
}
pub(super) fn publish(
    db: &DatabaseConnection,
    directory: &StoreDir,
    file: &FileRef,
    name: &FileName,
) -> Result<(), DbError> {
    let id = id(file)?;
    let reader = directory
        .file(FileArea::Cache, name)
        .open_reader()
        .map_err(observation)?;
    let header = coven_format::file::FileHeader::decode(
        &reader
            .read_at(0, coven_format::file::FILE_HEADER_LEN)
            .map_err(observation)?,
    )?;
    if header.size() != file.plaintext_size() || header.encrypted_size()? != reader.size() {
        return Err(DbError::DamagedDatabase);
    }
    let size = i64::try_from(reader.size()).map_err(|_| DbError::TooLarge {
        field: "cached file",
        actual: reader.size(),
        maximum: i64::MAX as u64,
    })?;
    db.internal_execute("INSERT INTO _coven_file_removals(area,path) SELECT 'cache',path FROM _coven_cache WHERE namespace=?1 AND file_id=?2",(file.namespace(),&id))?;
    db.internal_execute(
        "DELETE FROM _coven_cache WHERE namespace=?1 AND file_id=?2",
        (file.namespace(), &id),
    )?;
    db.internal_execute("INSERT INTO _coven_cache(namespace,file_id,chunk,path,size,last_read,pinned,checked_hash) VALUES(?1,?2,?7,?3,?4,?5,1,?6)",rusqlite::params![file.namespace(),id,name.as_str(),size,tick(db)?,file.content_hash().as_bytes().as_slice(),COMPLETE_FILE])?;
    db.internal_execute(
        "DELETE FROM _coven_file_removals WHERE area='cache' AND path=?1",
        [name.as_str()],
    )?;
    Ok(())
}
fn observation(error: coven_foundation::files::ObservationError) -> DbError {
    match error {
        coven_foundation::files::ObservationError::File(error) => DbError::Disk(error),
        _ => DbError::DamagedDatabase,
    }
}

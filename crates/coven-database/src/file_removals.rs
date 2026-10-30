//! Pending deletion remains durable until both the bytes and their record are gone.
//!
//! The transaction that ends a file's last reference records it in
//! `_coven_file_removals`. Its bytes go only after that writer is released,
//! so a stalled disk holds back no app write; then its record goes, under the
//! writer (§21.2, Appendix C.3). A crash between them leaves the record, and the next
//! open removes the bytes the same way, from that list rather than a scan.
//! A cache range being written is recorded in `_coven_cache_writes` instead,
//! which no removal here reads until opening finds it abandoned.
//!
//! Copies coven writes into a folder the person picked follow the same
//! steps from `_coven_copies`: one whose switch lost goes after the write
//! that found it lost, and one a call left unswitched goes as that call
//! fails, or at the next open after a crash (§17.3). A copy goes only while
//! it is as coven left it, and a folder coven made only while it is empty.

use coven_foundation::files::StoreDirAccess;
use std::collections::BTreeSet;

use crate::{sqlite::DatabaseConnection, DbError};
use coven_foundation::files::{FileArea, FileName, StoreDir};

/// The removals to perform, or the failure choosing them.
pub(crate) type Chosen = Result<Vec<Released>, DbError>;

/// Choose, under the writer that just committed, every recorded removal
/// outside `active`, and every copy that isn't kept outside `copying`, the
/// copies of calls still running. Its read sees only committed records:
/// after a failed rollback the writer still holds a transaction and the read
/// is refused, so uncommitted records never name bytes to remove.
pub(crate) fn choose(
    writer: &DatabaseConnection,
    active: &BTreeSet<FileName>,
    copying: &BTreeSet<String>,
) -> Chosen {
    writer.read_transaction(|| {
        let mut released = pending(writer, active)?;
        released.extend(copies(writer, &Copies::Settled(copying))?);
        Ok(released)
    })
}

/// The copies at `paths` a call that is failing left unswitched, with the
/// folders it made for them, for that call to remove (§17.3).
pub(crate) fn choose_copies(writer: &DatabaseConnection, paths: &[String]) -> Chosen {
    writer.read_transaction(|| copies(writer, &Copies::Of(paths)))
}

/// Which copies a removal takes.
enum Copies<'a> {
    /// Each copy whose switch lost, and each one a call left unswitched,
    /// failing or dropped, except those of the calls still running.
    Settled(&'a BTreeSet<String>),
    /// These copies, which their failing call left unswitched.
    Of(&'a [String]),
}

/// The copies `which` names, then the folders coven made for them that no
/// other copy needs, innermost first, so each is emptied before its parent.
#[expect(
    clippy::map_err_ignore,
    reason = "a stored value that won't convert or decode is DamagedDatabase"
)]
fn copies(db: &DatabaseConnection, which: &Copies<'_>) -> Chosen {
    type Row = (String, String, String, Option<i64>, Option<Vec<u8>>);
    let read = |r: &rusqlite::Row<'_>| -> rusqlite::Result<Row> {
        Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))
    };
    let rows: Vec<Row> = match which {
        Copies::Settled(copying) => db
            .query(
                "SELECT path,temporary,state,size,modified_at FROM _coven_copies
                 WHERE state IN ('writing','placed','lost') ORDER BY path",
                [],
                read,
            )?
            .into_iter()
            .filter(|row| row.2 == "lost" || !copying.contains(&row.0))
            .collect(),
        Copies::Of(paths) => {
            let mut rows = Vec::new();
            for path in *paths {
                rows.extend(db.query(
                    "SELECT path,temporary,state,size,modified_at FROM _coven_copies
                     WHERE path=?1 AND state IN ('writing','placed')",
                    [path],
                    read,
                )?);
            }
            rows
        }
    };
    let chosen: BTreeSet<&str> = rows.iter().map(|row| row.0.as_str()).collect();
    let mut folders = BTreeSet::new();
    let mut released = Vec::new();
    for (path, temporary, state, size, modified_at) in &rows {
        let bytes = match (state.as_str(), size, modified_at) {
            ("writing", None, None) => CopyBytes::Temporary(temporary.into()),
            ("placed" | "lost", Some(size), Some(modified_at)) => CopyBytes::Placed {
                size: u64::try_from(*size).map_err(|_| DbError::DamagedDatabase)?,
                modified_at: crate::user_file::decode_time(modified_at)?,
            },
            _ => return Err(DbError::DamagedDatabase),
        };
        released.push(Released::Copy {
            path: path.clone(),
            state: state.clone(),
            bytes,
        });
        for folder in db.query(
            "SELECT folder FROM _coven_copy_folders WHERE copy=?1 AND made=1",
            [path],
            |r| r.get::<_, String>(0),
        )? {
            let others = db.query(
                "SELECT copy FROM _coven_copy_folders WHERE folder=?1",
                [&folder],
                |r| r.get::<_, String>(0),
            )?;
            if others.iter().all(|copy| chosen.contains(copy.as_str())) {
                folders.insert(folder);
            }
        }
    }
    let mut folders: Vec<String> = folders.into_iter().collect();
    folders.sort_by_key(|folder| std::cmp::Reverse(folder.len()));
    released.extend(folders.into_iter().map(Released::Folder));
    Ok(released)
}

/// The cache names among `paths` that a cache operation's committed
/// transaction just recorded for removal.
#[expect(
    clippy::map_err_ignore,
    reason = "a stored value that won't convert or decode is DamagedDatabase"
)]
pub(crate) fn choose_cache(paths: &[String]) -> Chosen {
    paths
        .iter()
        .map(|path| {
            Ok(Released::Owned {
                name: FileName::new(path.clone()).map_err(|_| DbError::DamagedDatabase)?,
                area: "cache".into(),
            })
        })
        .collect()
}

/// A committed removal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Released {
    /// Bytes in the store's directory: a file name and the area holding it.
    /// No row names it: the schema refuses to record a named path, or to
    /// name a recorded one.
    Owned { name: FileName, area: String },
    /// A copy coven wrote at `path` in a folder the person picked, in the
    /// state its record was chosen in.
    Copy {
        path: String,
        state: String,
        bytes: CopyBytes,
    },
    /// A folder coven made for copies that are all going.
    Folder(String),
}

/// A copy's bytes as its record says they are.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CopyBytes {
    /// Still being written, at its temporary name.
    Temporary(std::path::PathBuf),
    /// At its path, with the size and modification time it had there.
    Placed {
        size: u64,
        modified_at: std::time::SystemTime,
    },
}

/// Every recorded removal outside `active`, the names a staging write or a
/// cache assembly still holds; the caller holds the writer and the set
/// `active` came from, so a reservation still holds its name or its
/// publication has committed and removed the record.
#[expect(
    clippy::map_err_ignore,
    reason = "a stored value that won't convert or decode is DamagedDatabase"
)]
fn pending(db: &DatabaseConnection, active: &BTreeSet<FileName>) -> Chosen {
    let rows = db.query(
        "SELECT path,area FROM _coven_file_removals ORDER BY area,path",
        [],
        |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
    )?;
    let mut released = Vec::new();
    for (name, area) in rows {
        let name = FileName::new(name).map_err(|_| DbError::DamagedDatabase)?;
        if !active.contains(&name) {
            released.push(Released::Owned { name, area });
        }
    }
    Ok(released)
}

/// Remove released bytes while no database lock is held. Returns the files
/// whose bytes are gone, including ones already absent, and the failures,
/// whose records stay for the next attempt.
pub(crate) fn remove_bytes(
    directory: &StoreDir,
    released: Vec<Released>,
) -> (Vec<Released>, Vec<DbError>) {
    let mut removed = Vec::new();
    let mut failures = Vec::new();
    for file in released {
        let result = match &file {
            Released::Owned { name, area } => {
                let area = match area.as_str() {
                    "files" => FileArea::AppProvided,
                    "cache" => FileArea::Cache,
                    _ => {
                        failures.push(DbError::DamagedDatabase);
                        continue;
                    }
                };
                directory.file(area, name).remove()
            }
            Released::Copy {
                bytes: CopyBytes::Temporary(temporary),
                ..
            } => coven_foundation::files::remove_temporary(temporary),
            Released::Copy {
                path,
                bytes: CopyBytes::Placed { size, modified_at },
                ..
            } => coven_foundation::files::remove_copy(path.as_ref(), *size, *modified_at),
            Released::Folder(folder) => coven_foundation::files::remove_folder(folder.as_ref()),
        };
        match result {
            Ok(()) => removed.push(file),
            Err(error) => failures.push(error.into()),
        }
    }
    (removed, failures)
}

/// Delete the records of files whose bytes are gone, in the writer's
/// transaction. A record already gone needs nothing.
pub(crate) fn forget(db: &DatabaseConnection, removed: &[Released]) -> Result<(), DbError> {
    for file in removed {
        #[cfg(any(test, feature = "test-utils"))]
        db.checkpoint(crate::test_utils::WriteCheckpoint::FileRemoved);
        match file {
            Released::Owned { name, area } => {
                db.internal_execute(
                    "DELETE FROM _coven_file_removals WHERE path=?1 AND area=?2",
                    (name.as_str(), area.as_str()),
                )?;
            }
            // Its folder links go with it.
            Released::Copy { path, state, .. } => {
                db.internal_execute(
                    "DELETE FROM _coven_copies WHERE path=?1 AND state=?2",
                    (path, state),
                )?;
            }
            Released::Folder(_) => {}
        }
        #[cfg(any(test, feature = "test-utils"))]
        db.checkpoint(crate::test_utils::WriteCheckpoint::FileRemovalRecorded);
    }
    Ok(())
}

/// Opening finishes every removal a crash or failure left recorded, before
/// any app write can run, by the same steps. First, cache ranges still
/// recorded as being written whose writer is gone become removals.
pub(crate) fn finish_at_open(
    writer: &DatabaseConnection,
    directory: &StoreDir,
) -> Result<(), DbError> {
    abandon_cache_writes(writer, directory)?;
    let released = writer.read_transaction(|| {
        let mut released = pending(writer, &BTreeSet::new())?;
        released.extend(copies(writer, &Copies::Settled(&BTreeSet::new()))?);
        Ok::<_, DbError>(released)
    })?;
    let (removed, mut failures) = remove_bytes(directory, released);
    if !removed.is_empty() {
        if let Err(error) = writer.transaction(|db| forget(db, &removed)) {
            failures.push(error);
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

/// A read-only handle in another process may be writing a recorded cache
/// range while this writer opens. Every range write holds the cache-write
/// lock shared, so once it is held exclusively, the ranges still recorded
/// as being written have no live writer: they become removals. No other
/// database lock is held while waiting, so a waited-for range can commit.
fn abandon_cache_writes(writer: &DatabaseConnection, directory: &StoreDir) -> Result<(), DbError> {
    if !writer.query_row(
        "SELECT EXISTS(SELECT 1 FROM _coven_cache_writes)",
        [],
        |r| r.get::<_, bool>(0),
    )? {
        return Ok(());
    }
    let _excluded = directory.exclude_cache_writes()?;
    writer.transaction(|db| {
        db.internal_execute(
            "INSERT INTO _coven_file_removals(area,path) SELECT 'cache',path FROM _coven_cache_writes",
            [],
        )?;
        db.internal_execute("DELETE FROM _coven_cache_writes", [])?;
        Ok(())
    })
}

#[cfg(test)]
#[path = "file_removals_tests.rs"]
mod tests;

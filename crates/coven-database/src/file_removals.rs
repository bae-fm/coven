//! Pending deletion remains durable until both the bytes and their record are gone.
//!
//! The transaction that ends a file's last reference records it in
//! `_coven_file_removals`. Its bytes go only after that writer is released,
//! so a stalled disk holds back no app write; then its record goes, under the
//! writer (§21.2, Appendix C.3). A crash between them leaves the record, and the next
//! open removes the bytes the same way, from that list rather than a scan.
//! A cache range being written is recorded in `_coven_cache_writes` instead,
//! which no removal here reads until opening finds it abandoned.

use coven_foundation::files::StoreDirAccess;
use std::collections::BTreeSet;

use crate::{sqlite::DatabaseConnection, DbError};
use coven_foundation::files::{FileArea, FileName, StoreDir};

/// The removals to perform, or the failure choosing them.
pub(crate) type Chosen = Result<Vec<Released>, DbError>;

/// Choose, under the writer that just committed, every recorded removal
/// outside `active`. Its read sees only committed records: after a failed
/// rollback the writer still holds a transaction and the read is refused,
/// so uncommitted records never name bytes to remove.
pub(crate) fn choose(writer: &DatabaseConnection, active: &BTreeSet<FileName>) -> Chosen {
    writer.read_transaction(|| pending(writer, active))
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
            Ok(Released {
                name: FileName::new(path.clone()).map_err(|_| DbError::DamagedDatabase)?,
                area: "cache".into(),
            })
        })
        .collect()
}

/// A committed removal: a file name and the area holding its bytes. No row
/// names it: the schema refuses to record a named path, or to name a
/// recorded one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Released {
    name: FileName,
    area: String,
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
            released.push(Released { name, area });
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
        let area = match file.area.as_str() {
            "files" => FileArea::AppProvided,
            "cache" => FileArea::Cache,
            _ => {
                failures.push(DbError::DamagedDatabase);
                continue;
            }
        };
        match directory.file(area, &file.name).remove() {
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
        db.internal_execute(
            "DELETE FROM _coven_file_removals WHERE path=?1 AND area=?2",
            (file.name.as_str(), file.area.as_str()),
        )?;
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
    let released = choose(writer, &BTreeSet::new())?;
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

//! Pending deletion remains durable until both the bytes and their record are gone.
//!
//! The transaction that ends a file's last reference records it in
//! `_coven_file_removals`. Its bytes go only after that writer is released,
//! so a stalled disk holds back no app write; then its record goes, under the
//! writer (§3.1, §16.6). A crash between them leaves the record, and the next
//! open removes the bytes the same way, from that list rather than a scan.

use std::collections::BTreeSet;

use crate::{sqlite::DatabaseConnection, DbError};
use coven_foundation::files::{FileArea, FileName, StoreDir};

/// The removals to perform and the failures found choosing them.
pub(crate) type Chosen = Result<(Vec<Released>, Vec<DbError>), DbError>;

/// Choose, under the writer that just committed, every recorded removal
/// outside `active`. Its read sees only committed records: after a failed
/// rollback the writer still holds a transaction and the read is refused,
/// so uncommitted records never name bytes to remove.
pub(crate) fn choose(writer: &DatabaseConnection, active: &BTreeSet<FileName>) -> Chosen {
    writer.read_transaction(|| pending(writer, active))
}

/// Choose, under the writer, the cache names among `paths` that a cache
/// operation just recorded for removal.
pub(crate) fn choose_cache(writer: &DatabaseConnection, paths: &[String]) -> Chosen {
    writer.read_transaction(|| recorded_cache(writer, paths))
}

/// A committed removal: a file name and the area holding its bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Released {
    name: FileName,
    area: String,
}

/// Every recorded removal outside `active`, the names a staging write or a
/// cache assembly still holds; the caller holds the writer and the set
/// `active` came from, so a reservation still holds its name or its
/// publication has committed and removed the record. A recorded name that a
/// row still references is a failure, and its bytes stay.
fn pending(
    db: &DatabaseConnection,
    active: &BTreeSet<FileName>,
) -> Result<(Vec<Released>, Vec<DbError>), DbError> {
    let rows = db.query(
        "SELECT path,area FROM _coven_file_removals ORDER BY area,path",
        [],
        |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
    )?;
    let mut released = Vec::new();
    let mut failures = Vec::new();
    for (name, area) in rows {
        let name = FileName::new(name).map_err(|_| DbError::DamagedDatabase)?;
        if active.contains(&name) {
            continue;
        }
        match unreferenced(db, &name) {
            Ok(()) => released.push(Released { name, area }),
            Err(error) => failures.push(error),
        }
    }
    Ok((released, failures))
}

fn recorded_cache(
    db: &DatabaseConnection,
    paths: &[String],
) -> Result<(Vec<Released>, Vec<DbError>), DbError> {
    let mut released = Vec::new();
    let mut failures = Vec::new();
    for path in paths {
        let name = FileName::new(path.clone()).map_err(|_| DbError::DamagedDatabase)?;
        if !db.query_row(
            "SELECT EXISTS(SELECT 1 FROM _coven_file_removals WHERE path=?1 AND area='cache')",
            [name.as_str()],
            |r| r.get::<_, bool>(0),
        )? {
            continue;
        }
        match unreferenced(db, &name) {
            Ok(()) => released.push(Released {
                name,
                area: "cache".into(),
            }),
            Err(error) => failures.push(error),
        }
    }
    Ok((released, failures))
}

fn unreferenced(db: &DatabaseConnection, name: &FileName) -> Result<(), DbError> {
    if db.query_row(
        "SELECT EXISTS(SELECT 1 FROM _coven_device_files WHERE path=?1 UNION ALL SELECT 1 FROM _coven_cache WHERE path=?1)",
        [name.as_str()],
        |r| r.get::<_, bool>(0),
    )? {
        return Err(DbError::Invariant {
            what: "a file awaiting removal is no longer referenced",
        });
    }
    Ok(())
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
/// any app write can run, by the same steps.
pub(crate) fn finish_at_open(
    writer: &DatabaseConnection,
    directory: &StoreDir,
) -> Result<(), DbError> {
    let (released, mut failures) = choose(writer, &BTreeSet::new())?;
    let (removed, removal_failures) = remove_bytes(directory, released);
    failures.extend(removal_failures);
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

#[cfg(test)]
#[path = "file_removals_tests.rs"]
mod tests;

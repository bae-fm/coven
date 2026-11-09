//! The files a switch to the cloud asked this device to keep whole (§17.8):
//! a durable request, recorded in the switching write, that coven fulfils
//! itself, resuming after a restart. An app file's request keeps coven's copy
//! until the cache copy is made from it; a user's original is never read for
//! the cache, so its request downloads from storage.

use crate::{sqlite::DatabaseConnection, DbError};
use coven_foundation::{files::StagedName, id_source::FileId};
use rusqlite::params;

/// One file this device keeps whole as a switch asked.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PinRequest {
    /// The request's own id, never reused: its wait, and what coven keeps
    /// of it in memory, are never a later request's for the same file.
    pub id: i64,
    /// The file.
    pub file: FileId,
    /// Coven's copy of an app file's bytes, while the cache copy is still to
    /// be made from it; `None` for a file the pin downloads from storage.
    pub copy: Option<StagedName>,
    /// The persisted wait after a failed attempt, if one is armed (§6.7).
    pub retry: Option<crate::RetryWait>,
}

/// Ask this device to keep `file` whole, from coven's copy when it has one.
pub(crate) fn request(db: &DatabaseConnection, file: FileId) -> Result<(), DbError> {
    db.internal_execute(
        "INSERT INTO _coven_pin_requests(file,copy)
         SELECT ?1,(SELECT path FROM _coven_device_files WHERE file=?1)
         ON CONFLICT DO NOTHING",
        params![file],
    )?;
    Ok(())
}

/// End `file`'s request, if one waits: the app unpinned the file, or it is
/// gone from this device or left its stored bytes. Returns whether a request
/// waited.
pub(crate) fn end(db: &DatabaseConnection, file: FileId) -> Result<bool, DbError> {
    let ended =
        db.query("DELETE FROM _coven_pin_requests WHERE file=?1 RETURNING copy", [file], |r| {
            r.get::<_, Option<String>>(0)
        })?;
    release(db, ended)
}

/// End the request `id`, if it still waits: its pin is made, or its file
/// left the stored bytes. Returns whether it waited.
pub(crate) fn end_request(db: &DatabaseConnection, id: i64) -> Result<bool, DbError> {
    let ended =
        db.query("DELETE FROM _coven_pin_requests WHERE id=?1 RETURNING copy", [id], |r| {
            r.get::<_, Option<String>>(0)
        })?;
    release(db, ended)
}

/// An ended request's record and wait went with its row; the copy it kept
/// goes once nothing else names it. Returns whether a request ended.
fn release(db: &DatabaseConnection, mut ended: Vec<Option<String>>) -> Result<bool, DbError> {
    let Some(copy) = ended.pop() else {
        return Ok(false);
    };
    if let Some(copy) = copy {
        crate::file_queue::release_copy(db, &copy)?;
    }
    Ok(true)
}

/// The cache copy couldn't be made from coven's copy for request `id`: the
/// pin downloads from storage instead, and the copy goes once nothing else
/// names it.
pub(crate) fn drop_copy(db: &DatabaseConnection, id: i64) -> Result<(), DbError> {
    let copy = db
        .query("SELECT copy FROM _coven_pin_requests WHERE id=?1", [id], |r| {
            r.get::<_, Option<String>>(0)
        })?
        .pop()
        .flatten();
    let Some(copy) = copy else {
        return Ok(());
    };
    db.internal_execute("UPDATE _coven_pin_requests SET copy=NULL WHERE id=?1", [id])?;
    crate::file_queue::release_copy(db, &copy)
}

/// Every request, with its persisted wait.
pub(crate) fn read(db: &DatabaseConnection) -> Result<Vec<PinRequest>, DbError> {
    db.query("SELECT id,file,copy FROM _coven_pin_requests ORDER BY file", [], |r| {
        Ok((r.get::<_, i64>(0)?, r.get::<_, FileId>(1)?, r.get::<_, Option<String>>(2)?))
    })?
    .into_iter()
    .map(|(id, file, copy)| {
        Ok(PinRequest {
            id,
            file,
            copy: copy
                .map(|copy| StagedName::parse(&copy).ok_or(DbError::DamagedDatabase))
                .transpose()?,
            retry: crate::retry::find(db, crate::RetrySubject::Pin(id))?,
        })
    })
    .collect()
}

#[cfg(test)]
#[path = "file_pins_tests.rs"]
pub(crate) mod tests;

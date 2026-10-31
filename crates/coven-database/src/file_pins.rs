//! The files a switch to the cloud asked this device to keep whole (§17.8):
//! a durable request, recorded in the switching write, that coven fulfils
//! itself, resuming after a restart. An app file's request keeps coven's copy
//! until the cache copy is made from it; a user's original is never read for
//! the cache, so its request downloads from storage.

use crate::{sqlite::DatabaseConnection, DbError};
use coven_foundation::{files::FileName, id_source::FileId};
use rusqlite::params;

/// One file this device keeps whole as a switch asked.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PinRequest {
    /// The file.
    pub file: FileId,
    /// Coven's copy of an app file's bytes, while the cache copy is still to
    /// be made from it; `None` for a file the pin downloads from storage.
    pub copy: Option<FileName>,
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

/// End the request: its pin is made, or its file is gone from this device.
/// Its record and wait go with it, and the copy it kept goes once nothing
/// else names it.
pub(crate) fn end(db: &DatabaseConnection, file: FileId) -> Result<(), DbError> {
    let copy = db
        .query(
            "DELETE FROM _coven_pin_requests WHERE file=?1 RETURNING copy",
            [file],
            |r| r.get::<_, Option<String>>(0),
        )?
        .pop()
        .flatten();
    if let Some(copy) = copy {
        crate::file_queue::release_copy(db, &copy)?;
    }
    Ok(())
}

/// The cache copy couldn't be made from coven's copy: the pin downloads from
/// storage instead, and the copy goes once nothing else names it.
pub(crate) fn drop_copy(db: &DatabaseConnection, file: FileId) -> Result<(), DbError> {
    let copy = db
        .query(
            "SELECT copy FROM _coven_pin_requests WHERE file=?1",
            [file],
            |r| r.get::<_, Option<String>>(0),
        )?
        .pop()
        .flatten();
    let Some(copy) = copy else {
        return Ok(());
    };
    db.internal_execute(
        "UPDATE _coven_pin_requests SET copy=NULL WHERE file=?1",
        [file],
    )?;
    crate::file_queue::release_copy(db, &copy)
}

/// Every request, with its persisted wait.
#[expect(
    clippy::map_err_ignore,
    reason = "a stored value that won't convert or decode is DamagedDatabase"
)]
pub(crate) fn read(db: &DatabaseConnection) -> Result<Vec<PinRequest>, DbError> {
    db.query(
        "SELECT file,copy FROM _coven_pin_requests ORDER BY file",
        [],
        |r| Ok((r.get::<_, FileId>(0)?, r.get::<_, Option<String>>(1)?)),
    )?
    .into_iter()
    .map(|(file, copy)| {
        Ok(PinRequest {
            file,
            copy: copy
                .map(|copy| FileName::new(copy).map_err(|_| DbError::DamagedDatabase))
                .transpose()?,
            retry: crate::retry::find(db, crate::RetrySubject::Pin(file))?,
        })
    })
    .collect()
}

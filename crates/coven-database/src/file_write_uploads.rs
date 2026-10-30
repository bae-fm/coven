//! The uploads a write queues for its staged files and moves, and the write
//! that completes one (§17.3, §17.8).

use super::{FileWrite, StagedFile, StagedSource};
use crate::{
    file_location::{FileLocation, Place},
    file_table::Record,
    sqlite::DatabaseConnection,
    DbError,
};
use coven_crypto::ContentHash;
use coven_format::file::FileHeader;
use coven_foundation::id_source::FileId;
use rusqlite::params;

/// What the transaction completing an upload does once its write applied:
/// clear the upload's pending record (§17.8).
pub(crate) type AfterCompletion = Box<dyn FnOnce(&DatabaseConnection) -> Result<(), DbError>>;

/// What a write retires once it applied: the uploads whose files it
/// switched, and those no file waits for.
#[derive(Default)]
pub(super) struct Completion {
    switched: Vec<i64>,
    retired: Vec<i64>,
    after: Vec<AfterCompletion>,
}

/// Keep a staged file's bytes on this device by its id, as its record says
/// it is (§17.3): the user's original as observed, or coven's copy of app
/// bytes. Its staging cleanup record goes before anything names the copy.
pub(super) fn keep_source(db: &DatabaseConnection, file: &StagedFile) -> Result<(), DbError> {
    match &file.source {
        StagedSource::Copy(name) => {
            let hashes = file.hashes.as_ref().ok_or(DbError::Invariant {
                what: "coven's copy of app bytes has their hashes",
            })?;
            db.internal_execute(
                "DELETE FROM _coven_file_removals WHERE path=?1 AND area='files'",
                [name.as_str()],
            )?;
            db.internal_execute(
                "INSERT INTO _coven_device_files(file,path,hash) VALUES(?1,?2,?3)",
                params![file.id, name.as_str(), hashes.content.as_bytes().as_slice()],
            )?;
            for (index, hash) in hashes.chunks().iter().enumerate() {
                db.internal_execute(
                    "INSERT INTO _coven_copy_chunks(path,chunk,hash) VALUES(?1,?2,?3)",
                    params![name.as_str(), index as i64, hash.as_bytes().as_slice()],
                )?;
            }
        }
        StagedSource::Original(observed) => {
            let size = i64::try_from(observed.size()).map_err(|_overflow| DbError::TooLarge {
                field: "file size",
                actual: observed.size(),
                maximum: i64::MAX as u64,
            })?;
            db.internal_execute(
                "INSERT INTO _coven_user_files(file,path,size,modified_at) VALUES(?1,?2,?3,?4)",
                params![
                    file.id,
                    coven_foundation::files::path_bytes(observed.path()),
                    size,
                    crate::user_file::encode_time(observed.modified_at())
                ],
            )?;
        }
    }
    Ok(())
}

/// One upload to queue: the files it stores, which share its bytes, each
/// with whether this device keeps it whole once stored, their size, source
/// and first-read hashes, and the move it belongs to.
pub(super) struct Queued<'a> {
    pub(super) files: &'a [(FileId, bool)],
    pub(super) size: u64,
    pub(super) source: &'a StagedSource,
    pub(super) hashes: &'a crate::file_hashes::FileHashes,
    pub(super) of: crate::MoveId,
}

/// Queue the upload to `location` of files for the cloud, with their source
/// and one hash per plaintext chunk, in their move (§17.3, §17.8).
pub(super) fn enqueue(
    db: &DatabaseConnection,
    queued: Queued<'_>,
    location: &FileLocation,
    now: std::time::SystemTime,
) -> Result<(), DbError> {
    let Queued {
        files,
        size,
        source,
        hashes,
        of,
    } = queued;
    let header = FileHeader::new(size);
    if hashes.chunks().len() as u64 != header.chunk_count() {
        return Err(DbError::Invariant {
            what: "a staged file hashes every plaintext chunk",
        });
    }
    // So every queued file has a whole encrypted size to send.
    header
        .encrypted_size()
        .map_err(|_overflow| DbError::Invariant {
            what: "a queued file's encrypted layout fits in its offsets",
        })?;
    let plain = size;
    let size = i64::try_from(size).map_err(|_overflow| DbError::TooLarge {
        field: "file size",
        actual: plain,
        maximum: i64::MAX as u64,
    })?;
    let device = location.uploader().0.to_be_bytes();
    let object = location.object().0;
    let (original, modified_at, copy) = match source {
        StagedSource::Original(observed) => (
            Some(coven_foundation::files::path_bytes(observed.path())),
            Some(crate::user_file::encode_time(observed.modified_at())),
            None,
        ),
        StagedSource::Copy(name) => (None, None, Some(name.as_str())),
    };
    let upload = db.query_row(
        "INSERT INTO _coven_file_uploads(location,device,file,size,hash,original,modified_at,copy,move,queued_at)
         VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10) RETURNING id",
        params![
            location.text(),
            device,
            object.as_bytes(),
            size,
            hashes.content.as_bytes().as_slice(),
            original,
            modified_at,
            copy,
            of,
            crate::user_file::encode_time(now)
        ],
        |r| r.get::<_, i64>(0),
    )?;
    for (index, hash) in hashes.chunks().iter().enumerate() {
        db.internal_execute(
            "INSERT INTO _coven_file_chunks(device,file,chunk,hash) VALUES(?1,?2,?3,?4)",
            params![
                device,
                object.as_bytes(),
                index as i64,
                hash.as_bytes().as_slice()
            ],
        )?;
    }
    for (file, keep) in files {
        db.internal_execute(
            "INSERT INTO _coven_upload_files(file,upload,keep) VALUES(?1,?2,?3)",
            params![file, upload, keep],
        )?;
    }
    Ok(())
}

/// A new file over `from`'s bytes, on this device only as `from` is, shares
/// its bytes here, the user's original or coven's copy, and its upload if
/// one is queued: the upload's completion switches both to the cloud
/// (§17.2).
pub(crate) fn share(db: &DatabaseConnection, from: FileId, to: FileId) -> Result<(), DbError> {
    db.internal_execute(
        "INSERT INTO _coven_upload_files(file,upload,keep)
         SELECT ?2,upload,keep FROM _coven_upload_files WHERE file=?1",
        params![from, to],
    )?;
    db.internal_execute(
        "INSERT INTO _coven_user_files(file,path,size,modified_at)
         SELECT ?2,path,size,modified_at FROM _coven_user_files WHERE file=?1",
        params![from, to],
    )?;
    db.internal_execute(
        "INSERT INTO _coven_device_files(file,path,hash)
         SELECT ?2,path,hash FROM _coven_device_files WHERE file=?1",
        params![from, to],
    )?;
    Ok(())
}

impl FileWrite<'_> {
    /// The app phase of the write completing upload `upload` (§17.3, §17.8):
    /// its bytes are stored, and once every upload of its move is, this
    /// write switches each file of the move to its stored bytes. A file gone
    /// meanwhile switches nothing, and an upload no file waits for any more,
    /// of a move that failed or was cancelled, retires. `after` runs once the
    /// write applied.
    pub(crate) fn complete_upload(
        &self,
        upload: i64,
        after: AfterCompletion,
    ) -> Result<(), DbError> {
        let db = self.database;
        let linked: bool = db
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM _coven_upload_files WHERE upload=u.id)
                 FROM _coven_file_uploads AS u WHERE u.id=?1",
                [upload],
                |r| r.get(0),
            )
            .map_err(|error| match error {
                DbError::Sqlite(rusqlite::Error::QueryReturnedNoRows) => DbError::Invariant {
                    what: "a completed upload is queued",
                },
                error => error,
            })?;
        let mut completion = self.completion.borrow_mut();
        let completion = completion.get_or_insert_with(Completion::default);
        completion.after.push(after);
        if linked {
            crate::file_moves::stored(db, upload)?;
        } else {
            completion.retired.push(upload);
        }
        Ok(())
    }

    /// Retire the uploads no file waits for any more, except those whose
    /// objects are `in_flight`, once this write applied, each with its
    /// pending record, and switch every move now complete (§17.3, §17.8).
    /// Returns the retired queue ids.
    pub(crate) fn retire_unlinked<S: serde::Serialize + 'static>(
        &self,
        in_flight: &std::collections::BTreeSet<FileId>,
        subject: impl Fn(&FileLocation) -> S,
    ) -> Result<Vec<i64>, DbError> {
        let mut retired = Vec::new();
        let mut completion = self.completion.borrow_mut();
        let completion = completion.get_or_insert_with(Completion::default);
        for (id, location) in crate::file_queue::unlinked(self.database)? {
            if in_flight.contains(&location.object()) {
                continue;
            }
            let subject =
                serde_json::to_vec(&subject(&location)).map_err(DbError::PendingEncoding)?;
            completion.after.push(Box::new(move |db| {
                crate::pending::clear_encoded(db, subject)
            }));
            completion.retired.push(id);
            retired.push(id);
        }
        Ok(retired)
    }

    /// Switch each file of every move whose uploads are all stored to its
    /// upload's stored bytes, in this write (§17.3): the move's files
    /// switch together.
    fn switch_moves(&self) -> Result<(), DbError> {
        let db = self.database;
        let mut switched = Vec::new();
        for (id, uploads) in crate::file_moves::complete(db)? {
            for upload in uploads {
                let (location, hash): (String, Vec<u8>) = db.query_row(
                    "SELECT location,hash FROM _coven_file_uploads WHERE id=?1",
                    [upload],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )?;
                let location = FileLocation::decode(&location)?;
                let hash = ContentHash::from_bytes(
                    hash.try_into()
                        .map_err(|_wrong_size| DbError::DamagedDatabase)?,
                );
                for file in db.query(
                    "SELECT file FROM _coven_upload_files WHERE upload=?1 ORDER BY file",
                    [upload],
                    |r| r.get::<_, FileId>(0),
                )? {
                    let shown = Record::read(db, file)?;
                    if shown.is_some_and(|record| record.place == Place::DeviceOnly(self.device)) {
                        Record::store(db, file, &location, hash)?;
                    }
                }
                switched.push(upload);
            }
            crate::file_moves::finish(db, id)?;
        }
        if !switched.is_empty() {
            self.completion
                .borrow_mut()
                .get_or_insert_with(Completion::default)
                .switched
                .extend(switched);
        }
        Ok(())
    }

    /// The app phase of this device's write settling what other devices'
    /// writes left (§17.2): delete each record they left with no reference,
    /// and switch each new file their moves made over stored bytes to those
    /// bytes. A file of a reloading audience waits.
    pub(crate) fn settle(&self) -> Result<(), DbError> {
        let db = self.database;
        for file in db.query(
            "SELECT file FROM _coven_unreferenced_files ORDER BY file",
            [],
            |r| r.get::<_, FileId>(0),
        )? {
            let Some(record) = Record::read(db, file)? else {
                continue;
            };
            let referenced: bool = db.query_row(
                "SELECT EXISTS(SELECT 1 FROM _coven_file_references WHERE file=?1)",
                [file],
                |r| r.get(0),
            )?;
            if !referenced && !crate::reload_gate::reloading(db, &record.audience)? {
                Record::delete(db, file)?;
            }
        }
        for (file, location, hash) in db.query(
            "SELECT file,location,hash FROM _coven_moved_copies ORDER BY file",
            [],
            |r| {
                Ok((
                    r.get::<_, FileId>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, Vec<u8>>(2)?,
                ))
            },
        )? {
            let record = Record::read(db, file)?;
            if let Some(record) = &record {
                if crate::reload_gate::reloading(db, &record.audience)? {
                    continue;
                }
            }
            db.internal_execute("DELETE FROM _coven_moved_copies WHERE file=?1", [file])?;
            if record.is_some_and(|record| record.place == Place::DeviceOnly(self.device)) {
                let hash = ContentHash::from_bytes(
                    hash.try_into()
                        .map_err(|_wrong_size| DbError::DamagedDatabase)?,
                );
                Record::store(db, file, &FileLocation::decode(&location)?, hash)?;
            }
        }
        Ok(())
    }

    /// The app phase's end for a write completing or retiring uploads:
    /// switch every move now complete.
    pub(crate) fn finish_uploads(&self) -> Result<(), DbError> {
        if self.completion.borrow().is_some() {
            self.switch_moves()?;
        }
        Ok(())
    }

    /// Retire the uploads this write completed or found unneeded, after it
    /// applied: the queue rows go, a switched file's copy with them, and
    /// their stored paths become deletion candidates unless a record names
    /// them.
    pub(super) fn finish_completion(&self) -> Result<(), DbError> {
        let Some(completion) = self.completion.borrow_mut().take() else {
            return Ok(());
        };
        for upload in completion.switched {
            crate::file_queue::switched(self.database, upload)?;
        }
        for upload in completion.retired {
            crate::file_queue::retire(self.database, upload)?;
        }
        for after in completion.after {
            after(self.database)?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "file_write_uploads_tests.rs"]
mod tests;

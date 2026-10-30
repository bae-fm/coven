//! The uploads a write queues for its staged files, and the write that
//! completes one (§17.8).

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

pub(super) struct Completion {
    upload: i64,
    after: AfterCompletion,
}

/// Queue a staged file's upload to `location`, with its source and one hash
/// per plaintext chunk, and record the user's original (§17.1, §17.8).
pub(super) fn enqueue(
    db: &DatabaseConnection,
    file: &StagedFile,
    location: &FileLocation,
    now: std::time::SystemTime,
) -> Result<(), DbError> {
    let header = FileHeader::new(file.size);
    if file.hashes.chunks().len() as u64 != header.chunk_count() {
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
    let size = i64::try_from(file.size).map_err(|_overflow| DbError::TooLarge {
        field: "file size",
        actual: file.size,
        maximum: i64::MAX as u64,
    })?;
    let device = location.uploader().0.to_be_bytes();
    let object = location.object().0;
    let (original, modified_at) = match &file.source {
        StagedSource::Original(observed) => (
            Some(crate::user_file::encode_path(observed.path())),
            Some(crate::user_file::encode_time(observed.modified_at())),
        ),
        StagedSource::Copy(_) => (None, None),
    };
    let upload = db.query_row(
        "INSERT INTO _coven_file_uploads(location,device,file,size,hash,original,modified_at,queued_at)
         VALUES(?1,?2,?3,?4,?5,?6,?7,?8) RETURNING id",
        params![
            location.text(),
            device,
            object.as_bytes(),
            size,
            file.hashes.content.as_bytes().as_slice(),
            original,
            modified_at,
            crate::user_file::encode_time(now)
        ],
        |r| r.get::<_, i64>(0),
    )?;
    match &file.source {
        StagedSource::Copy(name) => {
            // Its staging cleanup record goes before the queue names its bytes.
            db.internal_execute(
                "DELETE FROM _coven_file_removals WHERE path=?1 AND area='files'",
                [name.as_str()],
            )?;
            db.internal_execute(
                "INSERT INTO _coven_device_files(device,file,path) VALUES(?1,?2,?3)",
                params![device, object.as_bytes(), name.as_str()],
            )?;
        }
        StagedSource::Original(observed) => {
            db.internal_execute(
                "INSERT INTO _coven_user_files(file,path,size,modified_at) VALUES(?1,?2,?3,?4)",
                params![
                    file.id,
                    crate::user_file::encode_path(observed.path()),
                    observed.size().to_be_bytes().as_slice(),
                    crate::user_file::encode_time(observed.modified_at())
                ],
            )?;
        }
    }
    for (index, hash) in file.hashes.chunks().iter().enumerate() {
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
    db.internal_execute(
        "INSERT INTO _coven_upload_files(file,upload) VALUES(?1,?2)",
        params![file.id, upload],
    )?;
    Ok(())
}

/// A new file over `from`'s bytes shares its upload and its original: the
/// upload's completion switches both to the cloud (§17.2).
pub(crate) fn share(db: &DatabaseConnection, from: FileId, to: FileId) -> Result<(), DbError> {
    db.internal_execute(
        "INSERT INTO _coven_upload_files(file,upload)
         SELECT ?2,upload FROM _coven_upload_files WHERE file=?1",
        params![from, to],
    )?;
    db.internal_execute(
        "INSERT INTO _coven_user_files(file,path,size,modified_at)
         SELECT ?2,path,size,modified_at FROM _coven_user_files WHERE file=?1",
        params![from, to],
    )?;
    Ok(())
}

impl FileWrite<'_> {
    /// The app phase of the write completing upload `upload` (§17.8): switch
    /// each file the upload stores to its stored bytes, as this write. A file
    /// gone meanwhile switches nothing. `after` runs once the write applied.
    pub(crate) fn complete_upload(
        &self,
        upload: i64,
        after: AfterCompletion,
    ) -> Result<(), DbError> {
        let db = self.database;
        let (location, hash): (String, Vec<u8>) = db
            .query_row(
                "SELECT location,hash FROM _coven_file_uploads WHERE id=?1",
                [upload],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .map_err(|error| match error {
                DbError::Sqlite(rusqlite::Error::QueryReturnedNoRows) => DbError::Invariant {
                    what: "a completed upload is queued",
                },
                error => error,
            })?;
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
        *self.completion.borrow_mut() = Some(Completion { upload, after });
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

    /// Retire a completing upload after its write applied: the queue row
    /// goes, its stored path a deletion candidate unless a record names it.
    pub(super) fn finish_completion(&self) -> Result<(), DbError> {
        let Some(completion) = self.completion.borrow_mut().take() else {
            return Ok(());
        };
        crate::file_queue::complete(self.database, completion.upload)?;
        (completion.after)(self.database)
    }
}

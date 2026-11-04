//! The uploads a write queues for its staged files and moves, the write
//! that completes one, and the app's pause of them (§17.3, §17.9).

use super::{FileWrite, StagedFile, StagedSource};
use crate::{
    file_location::{FileLocation, Place},
    file_table::Record,
    sqlite::DatabaseConnection,
    DbError,
};
use coven_foundation::id_source::FileId;

/// What the transaction completing an upload does once its write applied:
/// clear the upload's pending record (§17.9).
pub(crate) type AfterCompletion = Box<dyn FnOnce(&DatabaseConnection) -> Result<(), DbError>>;

/// What a write retires once it applied: the uploads whose files it
/// switched, and those no file waits for; and whether it completes a bound
/// upload, freeing the write that upload holds (§17.4).
#[derive(Default)]
pub(super) struct Completion {
    switched: Vec<i64>,
    retired: Vec<i64>,
    after: Vec<AfterCompletion>,
    bound: bool,
}

/// Keep a staged file's bytes on this device for `id`, the file whose
/// record says they are here (§17.3): the user's original as read, or
/// coven's copy of app bytes, each with the hashes its first read found,
/// which an upload checks if the file moves to the cloud. Its staging
/// cleanup record goes before anything names the copy.
pub(super) fn keep_source(
    db: &DatabaseConnection,
    id: FileId,
    file: &StagedFile,
) -> Result<(), DbError> {
    let hashes = &file.hashes;
    match &file.source {
        StagedSource::Copy(name) => {
            db.internal_execute(
                "DELETE FROM _coven_file_removals WHERE path=?1 AND area='files'",
                [name.as_str()],
            )?;
            crate::file_queue::keep_copy(db, id, name.as_str(), hashes)?;
        }
        StagedSource::Original(original) => crate::user_file::record(db, id, original, hashes)?,
    }
    Ok(())
}

/// A new file over `from`'s bytes, on this device only as `from` is, shares
/// its bytes here, the user's original or coven's copy, and its upload if
/// one is queued: the upload's completion switches both to the cloud
/// (§17.2).
pub(crate) fn share(db: &DatabaseConnection, from: FileId, to: FileId) -> Result<(), DbError> {
    crate::file_queue::share(db, from, to)?;
    crate::user_file::share(db, from, to)
}

impl FileWrite<'_> {
    /// Persist the app's pause setting for trailing uploads (§17.4).
    pub(crate) fn set_uploads_paused(&self, paused: bool) -> Result<(), DbError> {
        crate::file_queue::set_paused(self.database, paused)
    }

    /// Persist the app's setting to move files only on unmetered networks
    /// (§17.9).
    pub(crate) fn set_unmetered_only(&self, unmetered_only: bool) -> Result<(), DbError> {
        crate::file_queue::set_unmetered_only(self.database, unmetered_only)
    }

    /// The app phase of the write completing upload `upload` (§17.3, §17.9):
    /// its bytes are stored, and once every upload of its move is, this
    /// write switches each file of the move to its stored bytes. A file gone
    /// meanwhile switches nothing, and an upload no file waits for any more,
    /// of a move that failed or was cancelled, retires. `after` runs once the
    /// write applied.
    pub(crate) fn complete_upload(
        &self,
        upload: i64,
        after: AfterCompletion,
    ) -> Result<crate::UploadCompletion, DbError> {
        let db = self.database;
        let (linked, bound): (bool, bool) = db
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM _coven_upload_files WHERE upload=u.id),
                     u.holds IS NOT NULL
                 FROM _coven_file_uploads AS u WHERE u.id=?1",
                [upload],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .map_err(|error| match error {
                DbError::Sqlite(rusqlite::Error::QueryReturnedNoRows) => {
                    DbError::Invariant { what: "a completed upload is queued" }
                }
                error => error,
            })?;
        let mut completion = self.files.completion.borrow_mut();
        let completion = completion.get_or_insert_with(Completion::default);
        completion.after.push(after);
        completion.bound |= bound;
        if linked {
            crate::file_queue::stored(db, upload)?;
            Ok(crate::UploadCompletion::Stored)
        } else {
            completion.retired.push(upload);
            Ok(crate::UploadCompletion::Retired)
        }
    }

    /// Retire the uploads no file waits for any more, except those whose
    /// objects are `in_flight`, once this write applied, each with its
    /// pending record, and switch every move now complete (§17.3, §17.9).
    /// Returns the retired queue ids.
    pub(crate) fn retire_unlinked<S: serde::Serialize + 'static>(
        &self,
        in_flight: &std::collections::BTreeSet<FileId>,
        subject: impl Fn(&FileLocation) -> S,
    ) -> Result<Vec<i64>, DbError> {
        let mut retired = Vec::new();
        let mut completion = self.files.completion.borrow_mut();
        let completion = completion.get_or_insert_with(Completion::default);
        for (id, location) in crate::file_queue::retirable(self.database)? {
            if in_flight.contains(&location.object()) {
                continue;
            }
            let subject = crate::pending::SubjectKey::of(&subject(&location))?;
            completion.after.push(Box::new(move |db| crate::pending::clear_key(db, &subject)));
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
                let hash = crate::file_hashes::stored_hash(hash)?;
                for file in db.query(
                    "SELECT file FROM _coven_upload_files WHERE upload=?1 ORDER BY file",
                    [upload],
                    |r| r.get::<_, FileId>(0),
                )? {
                    let shown = Record::read(db, file)?;
                    if shown
                        .is_some_and(|record| record.place == Place::DeviceOnly(self.files.device))
                    {
                        crate::file_table::store(db, file, &location, hash)?;
                    }
                }
                switched.push(upload);
            }
            crate::file_moves::finish(db, id)?;
        }
        if !switched.is_empty() {
            self.files
                .completion
                .borrow_mut()
                .get_or_insert_with(Completion::default)
                .switched
                .extend(switched);
        }
        Ok(())
    }

    /// The app phase of this device's write settling what other devices'
    /// writes left (§17.2): delete each record they left with no reference.
    /// A file of a reloading audience waits.
    pub(crate) fn settle(&self) -> Result<(), DbError> {
        let db = self.database;
        for file in
            db.query("SELECT file FROM _coven_unreferenced_files ORDER BY file", [], |r| {
                r.get::<_, FileId>(0)
            })?
        {
            let Some(record) = Record::read(db, file)? else {
                continue;
            };
            if !crate::file_protection::referenced(db, file)?
                && !crate::reload_gate::reloading(db, &record.audience)?
            {
                crate::file_table::delete(db, file)?;
            }
        }
        Ok(())
    }

    /// Whether this write completes a bound upload. A self-replacement's
    /// pause lets it through: the write the upload holds is one the
    /// replacement must send first (§12.3, §17.4).
    pub(crate) fn completes_bound(&self) -> bool {
        self.files.completion.borrow().as_ref().is_some_and(|completion| completion.bound)
    }

    /// The app phase's end for a write completing or retiring uploads:
    /// switch every move now complete.
    pub(crate) fn finish_uploads(&self) -> Result<(), DbError> {
        if self.files.completion.borrow().is_some() {
            self.switch_moves()?;
        }
        Ok(())
    }

    /// Retire the uploads this write completed or found unneeded, after it
    /// applied: the queue rows go, a switched file's copy with them, and
    /// their stored paths become deletion candidates unless a record names
    /// them.
    pub(super) fn finish_completion(&self) -> Result<(), DbError> {
        let Some(completion) = self.files.completion.borrow_mut().take() else {
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
pub(crate) mod tests;

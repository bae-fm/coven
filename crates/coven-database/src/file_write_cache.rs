//! The cache changes an app asks for as steps of its write (§4, §17.8):
//! each runs in the write's transaction, and the bytes it evicts go once
//! the write has committed and the writer is free (Appendix C.3).

use super::FileWrite;
use crate::{file_table::Record, DbError, FilePlace, FileRef};
use coven_foundation::id_source::FileId;

impl FileWrite<'_> {
    /// Remove these files' copies from this device: every cache entry of
    /// their stored bytes, pinned whole copy and cached chunks alike, and
    /// the partial download a pin left, and end the pin requests coven keeps
    /// for them. No trim follows: removing bytes cannot put a namespace over
    /// its budget, and nothing else goes. A file on one device only, or one
    /// this device has no record of, has nothing to remove.
    pub(crate) fn unpin(&self, files: &[FileId]) -> Result<(), DbError> {
        let db = self.database;
        for &id in files {
            let Some(record) = Record::read(db, id)? else {
                continue;
            };
            let file = FileRef::of(record)?;
            if let FilePlace::Cloud { .. } = file.place() {
                let object = crate::database::file_database::object_path_of(&file)?;
                crate::database::file_database::remove_cached(db, file.namespace(), &object)?;
                crate::database::file_database::remove_partial_pin(db, file.namespace(), &object)?;
                // The app no longer wants the file kept here, so coven
                // doesn't make the pin a switch asked for either (§17.8).
                crate::file_pins::end(db, id)?;
            }
        }
        Ok(())
    }

    /// Pin a prepared pin's files: publish each whole copy pinned, its older
    /// entries going, and keep each copy already kept. Every name must be
    /// reserved in this store (`PreparedElsewhere`); a copy whose file no
    /// record here names the stored bytes of any more is `NoLongerStored`,
    /// and a kept copy evicted or unpinned since is `PinChanged`.
    pub(crate) fn pin(&self, prepared: crate::PreparedPin) -> Result<(), DbError> {
        let db = self.database;
        self.files.reservations.held_here(prepared.names())?;
        let (copies, kept) = prepared.take();
        // The names go free once the write has decided, whatever the rest
        // of it does.
        self.files.brought.borrow_mut().extend(
            copies.iter().map(|(_, name)| crate::file_removals::OwnedFile::Cached(name.clone())),
        );
        for (file, name) in copies {
            if !crate::database::file_database::publish_cached(db, &file, &name)? {
                return Err(DbError::NoLongerStored { file: file.id() });
            }
        }
        for file in kept {
            if !crate::database::file_database::cache_kept(db, &file)? {
                return Err(DbError::PinChanged { file: file.id() });
            }
        }
        Ok(())
    }

    /// End the pin request `id`, returning whether it still waited (§17.8).
    pub(crate) fn end_pin_request(&self, id: i64) -> Result<bool, DbError> {
        crate::file_pins::end_request(self.database, id)
    }

    /// Set a declared namespace's budget and evict its unpinned entries,
    /// least recently read first, down to it.
    pub(crate) fn set_cache_budget(&self, namespace: &str, bytes: u64) -> Result<(), DbError> {
        crate::database::file_database::set_cache_budget(self.database, namespace, bytes)?;
        crate::database::file_database::trim_cached(self.database, namespace)?;
        Ok(())
    }

    /// Clear a declared namespace's cache: every unpinned file and chunk
    /// goes, and the pinned whole copies and the partial downloads pins left
    /// stay. An undeclared namespace fails with `UndeclaredNamespace`. The
    /// bytes go once the write has committed.
    pub(crate) fn clear_cache(&self, namespace: &str) -> Result<(), DbError> {
        crate::database::file_database::clear_cached(self.database, namespace)
    }

    /// Remove a stored file's cached copies, pinned or not, with no storage
    /// operation: every entry of its bytes. A file this device has no record
    /// of, or one on one device only, has none.
    pub(crate) fn evict(&self, file: FileId) -> Result<(), DbError> {
        let db = self.database;
        let Some(record) = Record::read(db, file)? else {
            return Ok(());
        };
        let file = FileRef::of(record)?;
        if let FilePlace::Cloud { .. } = file.place() {
            let object = crate::database::file_database::object_path_of(&file)?;
            crate::database::file_database::remove_cached(db, file.namespace(), &object)?;
        }
        Ok(())
    }
}

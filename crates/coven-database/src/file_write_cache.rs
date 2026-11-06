//! The cache changes an app asks for as steps of its write (§4, §17.8):
//! each runs in the write's transaction, and the bytes it evicts go once
//! the write has committed and the writer is free (Appendix C.3).

use super::FileWrite;
use crate::{file_table::Record, DbError, FilePlace, FileRef};
use coven_foundation::id_source::FileId;
use std::collections::BTreeSet;

impl FileWrite<'_> {
    /// Release the pins on these files' stored bytes, and end the pin
    /// requests coven keeps for them, then evict each namespace they are in
    /// down to its budget. A file this device has no record of, or one on
    /// one device only, has no pin to release.
    pub(crate) fn unpin(&self, files: &[FileId]) -> Result<(), DbError> {
        let db = self.database;
        let mut namespaces = BTreeSet::new();
        for &id in files {
            let Some(record) = Record::read(db, id)? else {
                continue;
            };
            let file = FileRef::of(record)?;
            if let FilePlace::Cloud { .. } = file.place() {
                crate::database::file_database::unpin_cached(db, &file)?;
                // The app no longer wants the file kept here, so coven
                // doesn't make the pin a switch asked for either (§17.8).
                crate::file_pins::end(db, id)?;
                namespaces.insert(file.namespace().to_owned());
            }
        }
        for namespace in namespaces {
            crate::database::file_database::trim_cached(db, &namespace)?;
        }
        Ok(())
    }

    /// Pin a prepared pin's files: publish each whole copy pinned, its older
    /// entries going, and pin each copy already kept. Every name must be
    /// reserved in this store (`PreparedElsewhere`); a copy whose file no
    /// record here names the stored bytes of any more is `NoLongerStored`,
    /// and a kept copy evicted since is `PinChanged`.
    pub(crate) fn pin(&self, prepared: crate::PreparedPin) -> Result<(), DbError> {
        let db = self.database;
        self.files.reservations.held_here(prepared.names())?;
        let (copies, kept) = prepared.take();
        // The names go free once the write has decided, whatever the rest
        // of it does.
        self.files.brought.borrow_mut().extend(copies.iter().map(|(_, name)| name.clone()));
        for (file, name) in copies {
            if !crate::database::file_database::publish_cached(db, &file, &name, true)? {
                return Err(DbError::NoLongerStored { file: file.id() });
            }
        }
        for file in kept {
            if !crate::database::file_database::pin_cached(db, &file)? {
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
            let id = crate::database::file_database::cache_id(&file)?;
            crate::database::file_database::remove_cached(db, file.namespace(), &id)?;
        }
        Ok(())
    }
}

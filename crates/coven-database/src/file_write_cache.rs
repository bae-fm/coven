//! The cache changes an app asks for as steps of its write (§4, §17.8):
//! each runs in the write's transaction, and the bytes it evicts go once
//! the write has committed and the writer is free (Appendix C.3).

use super::FileWrite;
use crate::{file_table::Record, DbError, FilePlace, FileRef};
use coven_foundation::id_source::FileId;
use std::collections::BTreeSet;

impl FileWrite<'_> {
    /// Release the pins on these files' stored bytes, then evict each
    /// namespace they are in down to its budget. A file this device has no
    /// record of, or one on one device only, has no pin to release.
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
                namespaces.insert(file.namespace().to_owned());
            }
        }
        for namespace in namespaces {
            crate::database::file_database::trim_cached(db, &namespace)?;
        }
        Ok(())
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

//! A pin prepared before the write that commits it (§17.8): each file's
//! whole cache copy written, synced and checked under a name reserved in
//! this store, or a checked whole copy already kept. Preparing pins
//! nothing; the write's `pin` step does, for every file at once.

use super::{read_only, CacheReservation, DatabaseInner, FileDatabase, FileDatabaseAccess};
use crate::{DbError, FileRef};
use coven_foundation::files::FileName;
use std::sync::{Arc, RwLock};

/// Files ready to pin in one write (§17.8). It holds nothing close waits
/// for. Dropped uncommitted, or after the store closes, its copies' names go
/// free, and the next write or open removes their bytes.
pub struct PreparedPin {
    database: Arc<RwLock<Option<DatabaseInner>>>,
    /// Each file's whole copy, under the name reserved for it.
    copies: Vec<(FileRef, FileName)>,
    /// Files a checked whole copy of which was already kept when prepared.
    kept: Vec<FileRef>,
}

impl FileDatabase {
    /// A pin of `copies`, whole files written, synced and checked, and of
    /// `kept`, files already kept whole. The copies' names stay reserved
    /// until a write commits the pin or it is dropped.
    pub fn prepared_pin(
        &self,
        copies: Vec<CacheReservation>,
        kept: Vec<FileRef>,
    ) -> Result<PreparedPin, DbError> {
        let FileDatabaseAccess::Writer(database) = &self.access else {
            return Err(read_only());
        };
        let copies = copies
            .into_iter()
            .map(|mut reservation| {
                // The name stays reserved, now for the prepared pin.
                let _lease = reservation.lease.take();
                (reservation.file.clone(), reservation.name.clone())
            })
            .collect();
        Ok(PreparedPin { database: database.inner.clone(), copies, kept })
    }

    /// Commit a prepared pin in a write of its own, as an app's write does
    /// with its `pin` step, for tests.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn commit_pin(&self, prepared: PreparedPin) -> Result<(), DbError> {
        let FileDatabaseAccess::Writer(database) = &self.access else {
            return Err(read_only());
        };
        database.write(move |sql| sql.pin(prepared)).await
    }

    /// Whether a checked whole copy of this stored file is kept here, pinned
    /// or not.
    pub async fn whole_kept(&self, file: &FileRef) -> Result<bool, DbError> {
        let file = file.clone();
        self.read(move |db| super::cache::whole(db, &file)).await
    }
}

impl PreparedPin {
    /// The names reserved for its copies.
    pub(crate) fn names(&self) -> impl Iterator<Item = &FileName> {
        self.copies.iter().map(|(_, name)| name)
    }

    /// Its copies and kept files, for the write committing them; dropping
    /// what is left releases nothing.
    pub(crate) fn take(mut self) -> (Vec<(FileRef, FileName)>, Vec<FileRef>) {
        (std::mem::take(&mut self.copies), std::mem::take(&mut self.kept))
    }
}

impl std::fmt::Debug for PreparedPin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedPin")
            .field("copies", &self.copies.len())
            .field("kept", &self.kept.len())
            .finish_non_exhaustive()
    }
}

impl Drop for PreparedPin {
    fn drop(&mut self) {
        if self.copies.is_empty() {
            return;
        }
        let slot = self.database.read().expect("database lock poisoned");
        // Closing forgot every reservation with its store.
        if let Some(inner) = slot.as_ref() {
            inner.release_file_reservations(self.copies.iter().map(|(_, name)| name));
        }
    }
}

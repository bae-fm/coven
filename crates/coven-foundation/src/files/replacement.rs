//! A new installation of a published store, loaded beside the store's own
//! files before it takes their place (§19.3).
//!
//! `reset_device` and `restore_device` load the replacement in a hidden
//! directory inside the store's directory, while the store stays as it was
//! and its writer lock stays held. Only once the replacement is loaded does
//! the swap move the old local state into an archive and the new
//! installation into place; a restore then removes the archive. A failure
//! before the swap leaves the store untouched, and the call that swapped can
//! undo it. An interrupted swap only moves forward: the next call of the same
//! kind finishes it, and the other kind is refused.

use super::{
    atomic_file::{self, exists},
    bootstrap::{self, BootstrapLease},
    creation, device_reset, lock, settings, BootstrapDirectoryError, BootstrapStore, DeviceReset,
    DirectoryCreationError, FileError, ReplacedBy, ReplacementRecord, StoreDir, StoreDirAccess,
    StoreLock, StoreLockError,
};
use crate::id_source::{IdSource, StoreId};
use std::{
    fs, io,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

/// A replacement of the installation in a published store's directory. It
/// holds the store's bootstrap lease, and its writer lock until the new
/// installation is in place. Dropping it keeps its work for the next call.
pub struct ReplacementStore {
    // Drops before the lease so surviving directory clones are closing.
    _closing: lock::StoreClosingGuard,
    path: PathBuf,
    id: StoreId,
    reset: DeviceReset,
    by: ReplacedBy,
    lease: Arc<BootstrapLease>,
    /// Keeps every handle out while the new installation loads and swaps in.
    writer: Mutex<Option<StoreLock>>,
    /// The new installation's own bootstrap while it loads in the hidden
    /// directory; none once it has swapped in.
    loading: Mutex<Option<BootstrapStore>>,
}

impl ReplacementStore {
    /// The installation being replaced and its fresh successor.
    pub fn device_reset(&self) -> DeviceReset {
        self.reset
    }

    /// Whether the new installation has taken the store's place.
    pub fn is_swapped(&self) -> bool {
        self.loading
            .lock()
            .expect("replacement state lock poisoned")
            .is_none()
    }

    /// Where the new installation is: its hidden directory while it loads,
    /// then the store's own, unpublished until `publish`.
    pub fn directory(&self) -> StoreDir {
        match &*self
            .loading
            .lock()
            .expect("replacement state lock poisoned")
        {
            Some(loading) => loading.directory(),
            None => StoreDir::bootstrapping(self.path.clone(), self.id, self.lease.clone()),
        }
    }

    /// The replaced installation's database: in place while the new one
    /// loads, then in its archive. Only its operation journal is read.
    pub fn discarded_database_path(&self) -> PathBuf {
        match &*self
            .loading
            .lock()
            .expect("replacement state lock poisoned")
        {
            Some(_) => self.path.join(super::directory::DATABASE),
            None => device_reset::discarded_database(&self.path, self.reset.old_device),
        }
    }

    /// Record the swap, naming the call that began it and the store's `name`
    /// in the loaded database, then move the old local state into its archive
    /// and the loaded installation into the store's directory, keeping the
    /// store's custody files in place. Then release the writer lock: from here
    /// the marker refuses other opens. The loading database must be closed. A
    /// failure undoes whatever moved and leaves the store as it was.
    pub fn swap(&self, name: String) -> Result<(), BootstrapDirectoryError> {
        let mut loading = self
            .loading
            .lock()
            .expect("replacement state lock poisoned");
        if loading.is_none() {
            return Ok(());
        }
        // No reader may hold the moving files open.
        let _readers = lock::exclude_readers(&self.path, self.id)?;
        let record = ReplacementRecord {
            reset: self.reset,
            by: self.by,
            name,
        };
        let swapped = device_reset::record(&self.path, &record)
            .map_err(DirectoryCreationError::from)
            .and_then(|_| device_reset::forward(&self.path, self.id, self.reset));
        if let Err(error) = swapped {
            return Err(
                match device_reset::backward(&self.path, self.id, self.reset) {
                    Ok(()) => error.into(),
                    Err(cleanup) => DirectoryCreationError::Rollback {
                        operation: Box::new(error),
                        cleanup: io::Error::other(cleanup),
                    }
                    .into(),
                },
            );
        }
        drop(loading.take());
        self.release_writer();
        Ok(())
    }

    /// Put the old installation back after a step that followed this call's
    /// own swap failed with nothing committed. The new installation's
    /// database must be closed. A failure leaves the swap for the next call
    /// to finish.
    pub async fn unswap(self) -> Result<(), BootstrapDirectoryError> {
        let _writer = self.directory().lock_for_open().await?;
        let _readers = lock::exclude_readers(&self.path, self.id)?;
        Ok(device_reset::backward(&self.path, self.id, self.reset)?)
    }

    /// Remove the swapped-out installation's archive before publication: a
    /// restore keeps no old copy. A failure leaves the swap for the next call
    /// to finish.
    pub fn remove_replaced(&self) -> Result<(), BootstrapDirectoryError> {
        assert!(
            self.is_swapped(),
            "the old installation is removed after the swap"
        );
        Ok(device_reset::discard(&self.path, self.reset.old_device)?)
    }

    /// Publish the swapped installation: remove the hidden directory, then
    /// the marker, then the record. A failure before the marker leaves the
    /// swap for the next call to finish. After a `Create(Published { .. })`
    /// error the store is visible.
    pub fn publish(&self) -> Result<(), BootstrapDirectoryError> {
        assert!(self.is_swapped(), "publication follows the swap");
        remove_area(&self.path)?;
        let published = |source: io::Error| -> BootstrapDirectoryError {
            DirectoryCreationError::Published {
                id: self.id,
                source,
            }
            .into()
        };
        let result = bootstrap::remove_marker(&self.path, published);
        self.lease.release();
        result?;
        device_reset::finish(&self.path).map_err(|error| published(io::Error::other(error)))
    }

    /// Remove the unswapped replacement after its database closed, leaving the
    /// store as it was. A later call starts a new one with a fresh id.
    pub fn cancel(self) -> Result<(), BootstrapDirectoryError> {
        let loading = self
            .loading
            .lock()
            .expect("replacement state lock poisoned")
            .take()
            .expect("only an unswapped replacement is cancelled");
        loading.cancel()?;
        drop(loading);
        Ok(remove_area(&self.path)?)
    }

    fn release_writer(&self) {
        self.writer
            .lock()
            .expect("replacement writer lock poisoned")
            .take();
    }
}

fn remove_area(directory: &Path) -> Result<(), FileError> {
    let area = directory.join(device_reset::AREA);
    if exists("inspect replacement area", &area)? {
        fs::remove_dir_all(&area)
            .map_err(|source| FileError::at("remove replacement area", &area, source))?;
        atomic_file::sync_publication(&area)?;
    }
    Ok(())
}

/// Hold the store's bootstrap and writer locks, then continue a swap already
/// under way or begin loading a replacement, resuming one that was loading.
/// A swap that the other call began is refused before anything changes.
pub(super) async fn begin(
    root: &Path,
    id: StoreId,
    by: ReplacedBy,
    ids: &dyn IdSource,
) -> Result<ReplacementStore, BootstrapDirectoryError> {
    let root = fs::canonicalize(root)
        .map_err(|source| FileError::at("resolve store layout", root, source))?;
    let path = root.join(id.to_string());
    let lock = lock::acquire_bootstrap(&path, id).await?;
    let closing = lock.closing_guard();
    let lease = Arc::new(BootstrapLease::new(lock));
    // A live handle, in this process or another, refuses the replacement.
    let writer = lock::acquire_for_open(&path, id).await?;
    let current = settings::read(&path, id).map_err(DirectoryCreationError::Settings)?;
    let pending = bootstrap::is_pending(&path)?;
    let recorded = match device_reset::read(&path)? {
        // Publication removed the marker but not the record: that replacement
        // is finished, and this call begins a new one.
        Some(record) if !pending && current.device_id == record.reset.new_device => {
            device_reset::finish(&path)?;
            None
        }
        recorded => recorded,
    };
    let (reset, loading) = match recorded {
        // Finishing the other call's swap would load under different keys,
        // or keep or remove a different copy.
        Some(record) if record.by != by => {
            return Err(StoreLockError::ReplacementUnfinished {
                store: id,
                by: record.by,
            }
            .into())
        }
        // An interrupted swap or undo finishes the swap.
        Some(record) => {
            let _readers = lock::exclude_readers(&path, id)?;
            device_reset::forward(&path, id, record.reset)?;
            (record.reset, None)
        }
        None if pending => return Err(StoreLockError::BootstrapPending(id).into()),
        None => {
            let area = path.join(device_reset::AREA);
            // A cancelled replacement may have left its area's lock files.
            if !exists(
                "inspect loading replacement",
                &device_reset::loading(&path, id),
            )? {
                remove_area(&path)?;
            }
            creation::create_directory(&area)
                .map_err(|source| FileError::at("create replacement area", &area, source))?;
            let loading = bootstrap::reserve_bootstrap_directory(&area, id, ids).await?;
            let new = loading
                .directory()
                .settings()
                .map_err(DirectoryCreationError::Settings)?
                .device_id;
            (
                DeviceReset {
                    old_device: current.device_id,
                    new_device: new,
                },
                Some(loading),
            )
        }
    };
    // Once swapped in, the marker refuses other opens.
    let writer = loading.is_some().then_some(writer);
    Ok(ReplacementStore {
        _closing: closing,
        path,
        id,
        reset,
        by,
        lease,
        writer: Mutex::new(writer),
        loading: Mutex::new(loading),
    })
}

#[cfg(test)]
#[path = "replacement_tests.rs"]
mod tests;

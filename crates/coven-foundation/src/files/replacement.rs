//! A new installation of a published store, loaded beside the store's own
//! files before it takes their place (§19.3).
//!
//! `reset_device` and `restore_device` load the replacement in a hidden
//! directory inside the store's directory, while the store stays as it was
//! and its writer lock stays held. Before the fresh device registers, the
//! replacement records itself: from then on the store refuses opens and the
//! replacement only moves forward, unless the facts end it before the swap.
//! Once the registration applied, the swap moves the old local state into an
//! archive and the new installation into place; a restore then removes the
//! archive. A failure before the record leaves the store untouched. A
//! replacement recorded or swapping is finished by the next call of the same
//! kind, and the other kind is refused.

use super::{
    atomic_file::{self, exists},
    bootstrap::{self, BootstrapLease},
    creation, device_reset,
    file_lock::FileLock,
    lock,
    protection::Protection,
    settings, BootstrapDirectoryError, BootstrapStore, DeviceReset, DirectoryCreationError,
    FileError, ReplacedBy, ReplacementPhase, ReplacementRecord, StoreDir, StoreDirAccess,
    StoreLock, StoreLockError,
};
use crate::id_source::{IdSource, StoreId};
use crate::locks::lock_unpoisoned;
use std::{
    fs, io,
    path::{Path, PathBuf},
    sync::Mutex,
};

/// A replacement of the installation in a published store's directory. It
/// holds the store's bootstrap lease, and its writer lock until the new
/// installation is in place. Dropping it keeps its work for the next call.
pub struct ReplacementStore {
    // Drops before the lease: the store's locks are closing before the
    // bootstrap lock is released.
    _closing: lock::StoreClosingGuard,
    path: PathBuf,
    id: StoreId,
    reset: DeviceReset,
    by: ReplacedBy,
    lease: BootstrapLease,
    /// Keeps every handle out while the new installation loads and swaps in.
    writer: Mutex<Option<StoreLock>>,
    /// Keeps read-only handles out from the record until the swap, so a
    /// recorded replacement never waits on one (§19.3).
    readers: Mutex<Option<FileLock>>,
    /// The replacement's record, once written: past it, it only goes forward.
    record: Mutex<Option<ReplacementRecord>>,
    /// The new installation's own bootstrap while it loads in the hidden
    /// directory; none once it has swapped in.
    loading: Mutex<Option<BootstrapStore>>,
    protection: Protection,
    bugs: crate::task::BugSinkRef,
}

impl ReplacementStore {
    /// The installation being replaced and its fresh successor.
    pub fn device_reset(&self) -> DeviceReset {
        self.reset
    }

    /// Whether the new installation has taken the store's place.
    pub fn is_swapped(&self) -> bool {
        lock_unpoisoned(&self.loading).is_none()
    }

    /// Where the new installation is: its hidden directory while it loads,
    /// then the store's own, unpublished until `publish`.
    pub fn directory(&self) -> StoreDir {
        match &*lock_unpoisoned(&self.loading) {
            Some(loading) => loading.directory(),
            None => StoreDir::bootstrapping(
                self.path.clone(),
                self.id,
                self.lease.instance(),
                self.protection,
                self.bugs.clone(),
            ),
        }
    }

    /// The replaced installation's database: in place while the new one
    /// loads, then in its archive. Only its operation journal is read.
    pub fn discarded_database_path(&self) -> PathBuf {
        match &*lock_unpoisoned(&self.loading) {
            Some(_) => self.path.join(super::directory::DATABASE),
            None => device_reset::discarded_database(&self.path, self.reset.old_device),
        }
    }

    /// Whether the replacement is recorded: past its point of no return.
    pub fn is_recorded(&self) -> bool {
        lock_unpoisoned(&self.record).is_some()
    }

    /// Record the replacement before its fresh device registers, naming the
    /// call that began it and the store's `name` in the loaded database:
    /// from here the store refuses opens and lists as this call's to finish,
    /// and read-only handles are kept out until the swap. A reader holding
    /// the store refuses the record, changing nothing. Recording again keeps
    /// the first record.
    pub fn record(&self, name: String) -> Result<(), BootstrapDirectoryError> {
        let mut recorded = lock_unpoisoned(&self.record);
        if recorded.is_some() {
            return Ok(());
        }
        let readers = self.exclude_readers()?;
        let record = ReplacementRecord {
            reset: self.reset,
            by: self.by,
            name,
            phase: ReplacementPhase::Registering,
        };
        let kept =
            device_reset::record(&self.path, &record).map_err(DirectoryCreationError::from)?;
        *lock_unpoisoned(&self.readers) = Some(readers);
        *recorded = Some(kept);
        Ok(())
    }

    /// Mark the swap as begun, recording the replacement first when it has
    /// no registration to precede (an unconnected reset), then move the old
    /// local state into its archive and the loaded installation into the
    /// store's directory, keeping the store's custody files in place. Then
    /// release the writer and reader locks: from here the marker refuses
    /// other opens. The loading database must be closed. A failure leaves the
    /// swap for the next call to finish; nothing moves back.
    pub fn swap(&self, name: String) -> Result<(), BootstrapDirectoryError> {
        let mut loading = lock_unpoisoned(&self.loading);
        if loading.is_none() {
            return Ok(());
        }
        // No reader may hold the moving files open.
        let held = lock_unpoisoned(&self.readers).take();
        let _readers = match held {
            Some(readers) => readers,
            None => self.exclude_readers()?,
        };
        let recorded = lock_unpoisoned(&self.record).clone();
        let record = match recorded {
            Some(record) => record,
            None => device_reset::record(
                &self.path,
                &ReplacementRecord {
                    reset: self.reset,
                    by: self.by,
                    name,
                    phase: ReplacementPhase::Swapping,
                },
            )
            .map_err(DirectoryCreationError::from)?,
        };
        device_reset::swapping(&self.path, &record).map_err(DirectoryCreationError::from)?;
        device_reset::forward(&self.path, self.id, self.reset)?;
        drop(loading.take());
        self.release_writer();
        Ok(())
    }

    /// End a recorded replacement before its swap, once the facts say nothing
    /// in storage names its fresh device as the old one's active replacement:
    /// remove the record, so the old installation is the store again, then
    /// the new installation. The loading database must be closed. A failure
    /// part way leaves the new installation without its record, which the
    /// next call continues as an unrecorded load.
    pub fn abandon(self) -> Result<(), BootstrapDirectoryError> {
        assert!(!self.is_swapped(), "only an unswapped replacement is abandoned");
        device_reset::finish(&self.path)?;
        lock_unpoisoned(&self.record).take();
        lock_unpoisoned(&self.readers).take();
        self.cancel()
    }

    fn exclude_readers(&self) -> Result<FileLock, StoreLockError> {
        lock::exclude_readers(&self.path, self.id)
    }

    /// Remove the swapped-out installation's archive before publication: a
    /// restore keeps no old copy. The archive holds the old installation's
    /// files and cache, of any size, so the removal runs on the blocking pool
    /// (§21.2). A failure leaves the swap for the next call to finish.
    pub async fn remove_replaced(&self) -> Result<(), BootstrapDirectoryError> {
        assert!(self.is_swapped(), "the old installation is removed after the swap");
        let path = self.path.clone();
        let old = self.reset.old_device;
        crate::task::blocking_completing(
            "remove the replaced installation",
            self.bugs.clone(),
            move || device_reset::discard(&path, old),
        )
        .await
        .map_err(FileError::from)??;
        Ok(())
    }

    /// Publish the swapped installation: remove the hidden directory, then
    /// the marker, then the record. A failure before the marker leaves the
    /// swap for the next call to finish. After a `Create(Published { .. })`
    /// error the store is visible.
    pub fn publish(&self) -> Result<(), BootstrapDirectoryError> {
        assert!(self.is_swapped(), "publication follows the swap");
        remove_area(&self.path)?;
        let published = |source: io::Error| -> BootstrapDirectoryError {
            DirectoryCreationError::Published { id: self.id, source }.into()
        };
        let result = bootstrap::remove_marker(&self.path, published);
        self.lease.release();
        result?;
        device_reset::finish(&self.path).map_err(|error| published(io::Error::other(error)))
    }

    /// Remove the unrecorded replacement after its database closed, leaving
    /// the store as it was. A later call starts a new one with a fresh id.
    pub fn cancel(self) -> Result<(), BootstrapDirectoryError> {
        assert!(!self.is_recorded(), "a recorded replacement only moves forward");
        let loading = lock_unpoisoned(&self.loading)
            .take()
            .expect("only an unswapped replacement is cancelled");
        loading.cancel()?;
        drop(loading);
        Ok(remove_area(&self.path)?)
    }

    fn release_writer(&self) {
        lock_unpoisoned(&self.writer).take();
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
    protection: Protection,
    bugs: crate::task::BugSinkRef,
) -> Result<ReplacementStore, BootstrapDirectoryError> {
    let root = fs::canonicalize(root)
        .map_err(|source| FileError::at("resolve store layout", root, source))?;
    let path = root.join(id.to_string());
    let lock = lock::acquire_bootstrap(&path, id, &bugs).await?;
    let closing = lock.closing_guard();
    let lease = BootstrapLease::new(lock);
    // A live handle, in this process or another, refuses the replacement.
    let writer = lock::acquire_for_open(&path, id, &bugs).await?;
    let current = settings::read(&path, id).map_err(DirectoryCreationError::Settings)?;
    // The device is leaving the store: only finishing that deletion, or
    // discarding the store, ends it.
    if super::deletion::read(&path)?.is_some() {
        return Err(StoreLockError::DeletionUnfinished(id).into());
    }
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
    match &recorded {
        // Finishing the other call's swap would load under different keys,
        // or keep or remove a different copy.
        Some(record) if record.by != by => {
            return Err(StoreLockError::ReplacementUnfinished { store: id, by: record.by }.into())
        }
        None if pending => return Err(StoreLockError::BootstrapPending(id).into()),
        _ => {}
    }
    // The installation that stays until the swap, and the replacement's
    // area, made inside the store's directory, take the class (E1).
    protection.apply(&path)?;
    let (reset, loading, readers) = match &recorded {
        // An interrupted swap finishes the swap.
        Some(record) if record.phase == ReplacementPhase::Swapping => {
            let _readers = lock::exclude_readers(&path, id)?;
            device_reset::forward(&path, id, record.reset)?;
            (record.reset, None, None)
        }
        // A recorded replacement continues its load, registration included,
        // with the fresh id it recorded; read-only handles stay out.
        Some(record) => {
            let readers = lock::exclude_readers(&path, id)?;
            let area = path.join(device_reset::AREA);
            let loading_path = device_reset::loading(&path, id);
            if !exists("inspect recorded replacement", &loading_path)? {
                return Err(FileError::at(
                    "resume a recorded replacement",
                    &loading_path,
                    io::Error::new(
                        io::ErrorKind::NotFound,
                        "the recorded replacement's installation is missing",
                    ),
                )
                .into());
            }
            let loading =
                bootstrap::reserve_bootstrap_directory(&area, id, ids, protection, bugs.clone())
                    .await?;
            let new =
                loading.directory().settings().map_err(DirectoryCreationError::Settings)?.device_id;
            if new != record.reset.new_device {
                return Err(FileError::at(
                    "resume a recorded replacement",
                    &loading_path,
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "the loaded installation is not the recorded fresh device",
                    ),
                )
                .into());
            }
            (record.reset, Some(loading), Some(readers))
        }
        None => {
            let area = path.join(device_reset::AREA);
            // A cancelled replacement may have left its area's lock files.
            if !exists("inspect loading replacement", &device_reset::loading(&path, id))? {
                remove_area(&path)?;
            }
            creation::create_directory(&area)
                .map_err(|source| FileError::at("create replacement area", &area, source))?;
            let loading =
                bootstrap::reserve_bootstrap_directory(&area, id, ids, protection, bugs.clone())
                    .await?;
            let new =
                loading.directory().settings().map_err(DirectoryCreationError::Settings)?.device_id;
            (DeviceReset { old_device: current.device_id, new_device: new }, Some(loading), None)
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
        readers: Mutex::new(readers),
        record: Mutex::new(recorded),
        loading: Mutex::new(loading),
        protection,
        bugs,
    })
}

#[cfg(test)]
#[path = "replacement_tests.rs"]
mod tests;

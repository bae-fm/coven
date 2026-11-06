//! The stores under an app directory, and publication of a new device's store.

use std::fs;
use std::io;
use std::path::PathBuf;

#[cfg(any(test, feature = "test-utils"))]
use crate::files::DirectoryCreationError;

use crate::files::protection::Protection;
use crate::files::{
    settings, BootstrapDirectoryError, FileError, SettingsError, StoreDeletionLock, StoreDir,
    StoreDirAccess, StoreLockError,
};
use crate::id_source::{IdSource, StoreId};
use crate::task::BugSinkRef;

/// The stores under `app_dir`, one directory each (E1).
#[cfg_attr(
    not(feature = "test-utils"),
    doc = r#"
Directory-only creation is a test fixture.
```compile_fail
use coven_foundation::{files::StoreDirectories, id_source::{StoreId, UuidIds, IdSource}};
let layout = StoreDirectories::new("stores".into(), std::sync::Arc::new(coven_foundation::task::LoggedBugs));
let _ = layout.create_store_dir(StoreId(UuidIds.new_id()), &UuidIds);
```"#
)]
#[derive(Clone)]
pub struct StoreDirectories {
    app_dir: PathBuf,
    /// The class every store's files take, where the app chose one.
    protection: Protection,
    /// Where a bug in work nobody waits for goes, for every store here
    /// (E15): the app's own reporter.
    bugs: BugSinkRef,
}

impl std::fmt::Debug for StoreDirectories {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoreDirectories")
            .field("app_dir", &self.app_dir)
            .field("protection", &self.protection)
            .finish_non_exhaustive()
    }
}

/// Which of the store directories a listing reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Installations {
    /// Published stores, including those whose replacement is unfinished.
    Published,
    /// Stores whose first installation on this device hasn't been published.
    UnfinishedFirst,
}

impl StoreDirectories {
    /// The stores under `app_dir`, one directory each (E1). The app creates
    /// `app_dir`; coven creates directories only inside it. A bug in work
    /// nobody waits for, in any of the stores, goes to `bugs` (E15).
    pub fn new(app_dir: PathBuf, bugs: BugSinkRef) -> Self {
        Self { app_dir, protection: Protection::none(), bugs }
    }

    /// Where a bug in work nobody waits for goes, for every store here
    /// (E15).
    pub fn bugs(&self) -> &BugSinkRef {
        &self.bugs
    }

    /// Give the files of every store these directories create, restore,
    /// join, replace or open for writing Apple's protection class
    /// `protection` (E1). On a volume that keeps no classes each of those
    /// fails before any file changes. Without it, files keep the class they
    /// have, and new ones take their folder's.
    #[cfg(target_vendor = "apple")]
    pub fn file_protection(mut self, protection: crate::files::FileProtection) -> Self {
        self.protection = Protection::new(protection);
        self
    }

    /// The folder that holds one directory per store.
    fn stores(&self) -> PathBuf {
        self.app_dir.join("stores")
    }

    /// The stores on this device, ordered by id (E1), including those whose
    /// replacement is unfinished, but no unpublished first installation.
    /// Only directories with a canonical UUID name and matching valid settings
    /// are stores. Invalid entries are logged; filesystem failures are errors.
    pub async fn store_ids(&self) -> Result<Vec<StoreId>, FileError> {
        self.listed(Installations::Published).await
    }

    /// The stores whose first installation on this device is unfinished, a
    /// restore or a join that hasn't loaded, ordered by id (§19.1). A store
    /// whose replacement is unfinished is listed by `store_ids` instead.
    pub async fn unfinished_first_installations(&self) -> Result<Vec<StoreId>, FileError> {
        self.listed(Installations::UnfinishedFirst).await
    }

    /// Lock an unfinished first installation for deletion while no call holds
    /// it: no restore or join, in this process or another, holds its
    /// bootstrap lock, and one closing in this process has been waited for.
    /// `None` when a call holds it, or it is no longer an unfinished first
    /// installation: published, being replaced, or gone. Whether anything
    /// still refers to it, a join's saved code or a restore's record, is for
    /// the caller to read while it holds the lock, since no call can begin on
    /// the installation meanwhile (§13.4, §11.6).
    pub async fn lock_unfinished_first(
        &self,
        id: StoreId,
    ) -> Result<Option<StoreDeletionLock>, BootstrapDirectoryError> {
        self.store_dir(&id).closed().await?;
        let path = self.stores().join(id.to_string());
        crate::task::blocking("lock an unfinished installation", move || {
            let lock = match super::lock::for_deletion(&path, id, None) {
                Ok(Some(lock)) => lock,
                // A call holds it, or nothing of it is left.
                Ok(None) | Err(StoreLockError::AlreadyOpen(_)) => return Ok(None),
                Err(error) => return Err(BootstrapDirectoryError::from(error)),
            };
            let first =
                super::bootstrap::is_pending(&path)? && super::device_reset::read(&path)?.is_none();
            Ok(first.then_some(lock))
        })
        .await
        .map_err(FileError::from)?
    }

    /// The stores whose deletion from this device is recorded and
    /// unfinished, ordered by id: a directory under its deletion name, or the
    /// store's writer or reader lock file with no directory. A deletion's
    /// first step renames the store's directory, so either is what a cut
    /// deletion leaves (E1).
    pub async fn recorded_deletions(&self) -> Result<Vec<StoreId>, FileError> {
        let root = self.stores();
        crate::task::blocking("list recorded deletions", move || recorded_deletions(&root)).await?
    }

    /// Whether the deletion of the store with `id` is recorded and
    /// unfinished, as `recorded_deletions` reads it.
    pub async fn deletion_recorded(&self, id: StoreId) -> Result<bool, FileError> {
        let published = self.stores().join(id.to_string());
        crate::task::blocking("inspect a recorded deletion", move || {
            deletion_recorded(&published, id)
        })
        .await?
    }

    async fn listed(&self, installations: Installations) -> Result<Vec<StoreId>, FileError> {
        let layout = self.clone();
        crate::task::blocking("list stores", move || layout.list_stores(installations)).await?
    }

    fn list_stores(&self, installations: Installations) -> Result<Vec<StoreId>, FileError> {
        let root = self.stores();
        let entries = match fs::read_dir(&root) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                tracing::debug!(path = %root.display(), "stores directory is absent");
                return Ok(Vec::new());
            }
            Err(source) => return Err(FileError::at("list store directories", &root, source)),
        };
        let mut stores = Vec::new();
        for entry in entries {
            let entry = entry
                .map_err(|source| FileError::at("read store directory entry", &root, source))?;
            let path = entry.path();
            if !entry
                .file_type()
                .map_err(|source| FileError::at("inspect store directory", &path, source))?
                .is_dir()
            {
                tracing::debug!(path = %path.display(), "entry is not a store directory");
                continue;
            }
            let name = entry.file_name();
            let id = match name.to_str().map(crate::id_source::canonical_uuid) {
                Some(Ok(id)) => StoreId(id),
                _ => {
                    tracing::debug!(path = %path.display(), "directory name is not a store id");
                    continue;
                }
            };
            // A first installation stays hidden from the stores until
            // published; a store whose replacement is unfinished lists, so
            // the app can finish it.
            let first =
                super::bootstrap::is_pending(&path)? && super::device_reset::read(&path)?.is_none();
            if first != (installations == Installations::UnfinishedFirst) {
                tracing::debug!(path = %path.display(), first, "store is not in this listing");
                continue;
            }
            match settings::read(&path, id) {
                Ok(_) => stores.push(id),
                Err(SettingsError::File(error)) => return Err(error),
                Err(SettingsError::Bug(bug)) => return Err(FileError::Bug(bug)),
                Err(error) => {
                    tracing::warn!(path = %path.display(), error = %error, "directory has no valid store settings")
                }
            }
        }
        stores.sort();
        Ok(stores)
    }

    /// The directory of the store with `id` (E1). This does not create it,
    /// read settings, or acquire the writer's lock.
    pub fn store_dir(&self, id: &StoreId) -> StoreDir {
        StoreDir::new(self.stores().join(id.to_string()), *id, self.protection, self.bugs.clone())
    }

    /// Publish a directory fixture without initializing member custody.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn create_store_dir(
        &self,
        id: StoreId,
        ids: &dyn IdSource,
    ) -> Result<StoreDir, DirectoryCreationError> {
        crate::files::creation::create(
            &self.stores(),
            id,
            ids,
            false,
            self.protection,
            self.bugs.clone(),
        )
    }
}

/// Whether a deletion of the store whose published directory is `published`
/// is recorded: its directory under the deletion name, or, with no directory
/// at all, its writer or reader lock file.
fn deletion_recorded(published: &std::path::Path, id: StoreId) -> Result<bool, FileError> {
    use super::atomic_file::exists;
    // Creation refuses an id while its deletion's remains are there, so a
    // published directory has none beside it.
    if exists("inspect a store", published)? {
        return Ok(false);
    }
    if exists("inspect a store's deletion", &super::lock::deletion_path(published, id))? {
        return Ok(true);
    }
    for path in super::lock::lock_paths(published, id) {
        if exists("inspect a store's lock", &path)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn recorded_deletions(root: &std::path::Path) -> Result<Vec<StoreId>, FileError> {
    let entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(source) => return Err(FileError::at("list store directories", root, source)),
    };
    let mut named = std::collections::BTreeSet::new();
    for entry in entries {
        let entry =
            entry.map_err(|source| FileError::at("read store directory entry", root, source))?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            tracing::debug!(name = ?name, "entry name is not UTF-8, so no store's");
            continue;
        };
        // The names a deletion leaves: `.coven-delete-<id>`, `.<id>.lock` and
        // `.<id>.readers.lock`.
        let id = name
            .strip_prefix(".coven-delete-")
            .or_else(|| name.strip_prefix('.').and_then(|rest| rest.strip_suffix(".readers.lock")))
            .or_else(|| name.strip_prefix('.').and_then(|rest| rest.strip_suffix(".lock")));
        if let Some(Ok(id)) = id.map(crate::id_source::canonical_uuid) {
            named.insert(StoreId(id));
        }
    }
    let mut recorded = Vec::new();
    for id in named {
        if deletion_recorded(&root.join(id.to_string()), id)? {
            recorded.push(id);
        }
    }
    Ok(recorded)
}

impl StoreDirectories {
    /// Reserve a hidden, restartable new-device store. A second attempt for the
    /// same id waits for a dropped bootstrap's remaining work to release its
    /// locks; a live bootstrap still refuses it. Ordinary store listings exclude
    /// it until `BootstrapStore::publish` succeeds.
    pub async fn begin_bootstrap(
        &self,
        id: StoreId,
        ids: &dyn IdSource,
    ) -> Result<super::BootstrapStore, super::BootstrapDirectoryError> {
        super::bootstrap::reserve_bootstrap_directory(
            &self.stores(),
            id,
            ids,
            self.protection,
            self.bugs.clone(),
        )
        .await
    }

    /// Replace the installation of a published store, or continue a
    /// replacement under way: hold the store's bootstrap and writer locks, so
    /// no handle opens it meanwhile, and load the new installation in a
    /// hidden directory inside the store's directory (§19.3). A recorded
    /// replacement continues its load, and a swap under way finishes. One
    /// that the other call began is refused with
    /// `StoreLockError::ReplacementUnfinished`, changing nothing.
    pub async fn begin_replacement(
        &self,
        id: StoreId,
        by: super::ReplacedBy,
        ids: &dyn IdSource,
    ) -> Result<super::ReplacementStore, super::BootstrapDirectoryError> {
        super::replacement::begin(&self.stores(), id, by, ids, self.protection, self.bugs.clone())
            .await
    }
}

#[cfg(test)]
#[path = "layout_tests.rs"]
mod tests;

//! The stores under an app directory, and publication of a new device's store.

use std::fs;
use std::io;
use std::path::PathBuf;

use uuid::Uuid;

#[cfg(any(test, feature = "test-utils"))]
use crate::files::DirectoryCreationError;

use crate::files::{settings, FileError, SettingsError, StoreDir};
use crate::id_source::{IdSource, StoreId};

/// The stores under `app_dir`, one directory each (E1).
#[cfg_attr(
    not(feature = "test-utils"),
    doc = r#"
Directory-only creation is a test fixture.
```compile_fail
use coven_foundation::{files::StoreDirectories, id_source::{StoreId, UuidIds, IdSource}};
let layout = StoreDirectories::new("stores".into());
let _ = layout.create_store_dir(StoreId(UuidIds.new_id()), &UuidIds);
```"#
)]
#[derive(Clone, Debug)]
pub struct StoreDirectories {
    app_dir: PathBuf,
}

/// A failure reading the app's store directories.
#[derive(Debug, thiserror::Error)]
pub enum StoreDirectoriesError {
    /// The operating system refused a listing or a settings read.
    #[error("list stores: {0}")]
    File(#[from] FileError),
}

impl StoreDirectories {
    /// The stores under `app_dir`, one directory each (E1).
    pub fn new(app_dir: PathBuf) -> Self {
        Self { app_dir }
    }

    /// The stores on this device, ordered by id (E1).
    /// Only directories with a canonical UUID name and matching valid settings
    /// are stores. Invalid entries are logged; filesystem failures are errors.
    pub async fn store_ids(&self) -> Result<Vec<StoreId>, StoreDirectoriesError> {
        let layout = self.clone();
        match tokio::task::spawn_blocking(move || layout.list_stores()).await {
            Ok(result) => result,
            Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
            Err(error) => panic!("store listing task cancelled: {error}"),
        }
    }

    fn list_stores(&self) -> Result<Vec<StoreId>, StoreDirectoriesError> {
        let root = self.app_dir.join("stores");
        let entries = match fs::read_dir(&root) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                tracing::debug!(path = %root.display(), "stores directory is absent");
                return Ok(Vec::new());
            }
            Err(source) => {
                return Err(FileError::at("list store directories", &root, source).into())
            }
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
            let id = match name.to_str().map(Uuid::parse_str) {
                Some(Ok(id)) if name == std::ffi::OsStr::new(&id.to_string()) => StoreId(id),
                _ => {
                    tracing::debug!(path = %path.display(), "directory name is not a store id");
                    continue;
                }
            };
            if super::bootstrap::is_pending(&path)? {
                tracing::debug!(path = %path.display(), "store is awaiting bootstrap publication");
                continue;
            }
            match settings::read(&path, id) {
                Ok(_) => stores.push(id),
                Err(SettingsError::File(error)) => return Err(error.into()),
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
        StoreDir::new(self.app_dir.join("stores").join(id.to_string()), *id)
    }

    /// Publish a directory fixture without initializing member custody.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn create_store_dir(
        &self,
        id: StoreId,
        ids: &dyn IdSource,
    ) -> Result<StoreDir, DirectoryCreationError> {
        crate::files::creation::create(&self.app_dir.join("stores"), id, ids, false)
    }
}

/// Crate-to-crate operations for coven's owners; not re-exported by the app API.
pub trait StoreDirectoriesAccess: Sized {
    /// Reserve a hidden, restartable new-device store. A second attempt for the
    /// same id waits for a dropped bootstrap's remaining work to release its
    /// locks; a live bootstrap still refuses it. Ordinary store listings exclude
    /// it until `BootstrapStore::publish` succeeds.
    fn begin_bootstrap(
        &self,
        id: StoreId,
        ids: &dyn IdSource,
    ) -> impl std::future::Future<
        Output = Result<super::BootstrapStore, super::BootstrapDirectoryError>,
    > + Send;
    /// Replace the installation of a published store, or continue a
    /// replacement under way: hold the store's bootstrap and writer locks, so
    /// no handle opens it meanwhile, and load the new installation in a
    /// hidden directory inside the store's directory (§19.3).
    fn begin_replacement(
        &self,
        id: StoreId,
        ids: &dyn IdSource,
    ) -> impl std::future::Future<
        Output = Result<super::ReplacementStore, super::BootstrapDirectoryError>,
    > + Send;
}

impl StoreDirectoriesAccess for StoreDirectories {
    async fn begin_bootstrap(
        &self,
        id: StoreId,
        ids: &dyn IdSource,
    ) -> Result<super::BootstrapStore, super::BootstrapDirectoryError> {
        super::bootstrap::reserve_bootstrap_directory(&self.app_dir.join("stores"), id, ids).await
    }
    async fn begin_replacement(
        &self,
        id: StoreId,
        ids: &dyn IdSource,
    ) -> Result<super::ReplacementStore, super::BootstrapDirectoryError> {
        super::replacement::begin(&self.app_dir.join("stores"), id, ids).await
    }
}

#[cfg(test)]
#[path = "layout_tests.rs"]
mod tests;

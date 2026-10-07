//! The stores under an app directory, and publication of a new device's store.

use std::fs;
use std::io;
use std::path::PathBuf;

use uuid::Uuid;

use crate::files::{settings, FileError, SettingsError, StoreCreationError, StoreDir};
use crate::id_source::{IdSource, StoreId};

/// The stores under `app_dir`, one directory each (E1).
#[derive(Clone, Debug)]
pub struct StoreLayout {
    app_dir: PathBuf,
}

/// A store on this device, by id and name (E1).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoreInfo {
    /// The store's identity.
    pub id: StoreId,
    /// The store's name.
    pub name: String,
}

/// A failure reading the app's store directories.
#[derive(Debug, thiserror::Error)]
pub enum StoreLayoutError {
    /// The operating system refused a listing or a settings read.
    #[error("list stores: {0}")]
    File(#[from] FileError),
}

impl StoreLayout {
    /// The stores under `app_dir`, one directory each (E1).
    pub fn new(app_dir: PathBuf) -> Self {
        Self { app_dir }
    }

    /// The stores on this device, by id and name, ordered by id (E1).
    /// Only directories with a canonical UUID name and matching valid settings
    /// are stores. Invalid entries are logged; filesystem failures are errors.
    pub async fn stores(&self) -> Result<Vec<StoreInfo>, StoreLayoutError> {
        let layout = self.clone();
        match tokio::task::spawn_blocking(move || layout.list_stores()).await {
            Ok(result) => result,
            Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
            Err(error) => panic!("store listing task cancelled: {error}"),
        }
    }

    fn list_stores(&self) -> Result<Vec<StoreInfo>, StoreLayoutError> {
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
            match settings::read(&path, id) {
                Ok(settings) => stores.push(StoreInfo {
                    id,
                    name: settings.name,
                }),
                Err(SettingsError::File(error)) => return Err(error.into()),
                Err(error) => {
                    tracing::warn!(path = %path.display(), error = %error, "directory has no valid store settings")
                }
            }
        }
        stores.sort_by_key(|store| store.id);
        Ok(stores)
    }

    /// The directory of the store with `id` (E1). This does not create it,
    /// read settings, or acquire the writer's lock.
    pub fn store_dir(&self, id: &StoreId) -> StoreDir {
        StoreDir::new(self.app_dir.join("stores").join(id.to_string()), *id)
    }

    /// Reserve a hidden, restartable new-device store. A second attempt for the
    /// same id cannot run until the returned lease is dropped. Ordinary store
    /// listings exclude it until `BootstrapStore::publish` succeeds.
    pub fn begin_bootstrap(
        &self,
        id: StoreId,
        name: &str,
        ids: &dyn IdSource,
    ) -> Result<super::BootstrapStore, super::BootstrapDirectoryError> {
        super::bootstrap::reserve_bootstrap_directory(&self.app_dir.join("stores"), id, name, ids)
    }

    /// Make and publish one store directory and its settings, with a fresh
    /// device id from `ids`. The facade supplies a new store id for creation,
    /// or the existing store id for restore and join. The app never supplies a
    /// device id (§10, E1).
    ///
    /// Initialization takes place in an unpublished sibling. Its complete
    /// contents are renamed into place without replacing an existing store.
    pub fn create_store_dir(
        &self,
        id: StoreId,
        name: &str,
        ids: &dyn IdSource,
    ) -> Result<StoreDir, StoreCreationError> {
        self.create_store_dir_with(id, name, ids, |_| Ok(()))
    }
    /// Prepare outside-directory state before publishing the store. The
    /// initializer sees its final settings, and any failure removes the
    /// unpublished directory. Used for a non-restored device identity (§10).
    pub fn create_store_dir_with<E: std::error::Error + Send + Sync + 'static>(
        &self,
        id: StoreId,
        name: &str,
        ids: &dyn IdSource,
        initialize: impl FnOnce(&crate::files::StoreSettings) -> Result<(), E>,
    ) -> Result<StoreDir, StoreCreationError<E>> {
        crate::files::creation::create(&self.app_dir.join("stores"), id, name, ids, initialize)
    }
}

#[cfg(test)]
#[path = "layout_tests.rs"]
mod tests;

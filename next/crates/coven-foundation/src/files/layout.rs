//! The stores under an app directory, and publication of a new device's store.

use std::fs;
use std::io;
use std::path::PathBuf;

use uuid::Uuid;

use crate::files::{settings, FileError, SettingsError, StoreCreationError, StoreDir};
use crate::id_source::{IdSource, StoreId};

/// The stores under `app_dir`, one directory each (§20.1).
#[derive(Clone, Debug)]
pub struct StoreLayout {
    app_dir: PathBuf,
}

/// A store on this device, by id and name (§20.1).
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
    /// The stores under `app_dir`, one directory each (§20.1).
    pub fn new(app_dir: PathBuf) -> Self {
        Self { app_dir }
    }

    /// The stores on this device, by id and name, ordered by id (§20.1).
    /// Only directories with a canonical UUID name and matching valid settings
    /// are stores. Invalid entries are logged; filesystem failures are errors.
    pub fn stores(&self) -> Result<Vec<StoreInfo>, StoreLayoutError> {
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

    /// The directory of the store with `id` (§20.1). This does not create it,
    /// read settings, or acquire the writer's lock.
    pub fn store_dir(&self, id: &StoreId) -> StoreDir {
        StoreDir::new(self.app_dir.join("stores").join(id.to_string()), *id)
    }

    /// Make and publish one store directory and its settings, with a fresh
    /// device id from `ids`. The facade supplies a new store id for creation,
    /// or the existing store id for restore and join. The app never supplies a
    /// device id (§10, §20.1).
    ///
    /// Initialization takes place in an unpublished sibling. Its complete
    /// contents are renamed into place without replacing an existing store.
    pub fn create_store_dir(
        &self,
        id: StoreId,
        name: &str,
        ids: &dyn IdSource,
    ) -> Result<StoreDir, StoreCreationError> {
        crate::files::creation::create(&self.app_dir.join("stores"), id, name, ids)
    }
}

#[cfg(test)]
#[path = "layout_tests.rs"]
mod tests;

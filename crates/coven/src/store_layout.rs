//! Store listings read names from the same database as the open handle.

use crate::{CovenError, FileError, StoreDir, StoreId};
use coven_foundation::files::{
    BootstrapDirectoryError, BootstrapStore, ReplacedBy, ReplacementStore, StoreDirAccess,
    StoreDirectoriesAccess,
};
use coven_foundation::id_source::IdSource;
use std::path::PathBuf;

/// The stores under an app directory, one directory each (E1).
#[derive(Clone, Debug)]
pub struct StoreLayout {
    directories: coven_foundation::files::StoreDirectories,
}

/// A store on this device (E1).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreInfo {
    /// The store's identity.
    pub id: StoreId,
    /// The current name from this device's committed database state. While
    /// a replacement is unfinished, the name its new installation loaded.
    pub name: String,
    /// The call that must finish a replacement of this device's installation
    /// before the store opens (§19.3); `None` when no replacement is unfinished.
    pub unfinished_replacement: Option<ReplacedBy>,
}

/// A failure listing stores or reading a committed store name.
#[derive(Debug, thiserror::Error)]
pub enum StoreLayoutError {
    /// The directory listing or an identity settings read failed.
    #[error(transparent)]
    File(#[from] FileError),
    /// Reading a store's current name failed.
    #[error("read name of store {store}: {source}")]
    Store {
        /// The store whose name could not be read.
        store: StoreId,
        /// The database or lock failure.
        #[source]
        source: CovenError,
    },
}

impl From<coven_foundation::files::StoreDirectoriesError> for StoreLayoutError {
    fn from(error: coven_foundation::files::StoreDirectoriesError) -> Self {
        let coven_foundation::files::StoreDirectoriesError::File(error) = error;
        Self::File(error)
    }
}

impl StoreLayout {
    /// The app directory under which stores have their own directories (E1).
    /// The app creates `app_dir`; coven creates directories only inside it
    /// and never opens a directory above it, which Android refuses.
    pub fn new(app_dir: PathBuf) -> Self {
        Self { directories: coven_foundation::files::StoreDirectories::new(app_dir) }
    }

    /// Stores on this device, ordered by id, with their committed names.
    /// A damaged name is an error; a listing never substitutes an older name.
    /// A store whose replacement is unfinished lists with the call that
    /// finishes it, so the app can offer to; an unfinished first installation
    /// is not listed.
    pub async fn stores(&self) -> Result<Vec<StoreInfo>, StoreLayoutError> {
        let mut stores = Vec::new();
        for id in self.directories.store_ids().await? {
            let directory = self.store_dir(&id);
            let failed = |source: CovenError| StoreLayoutError::Store { store: id, source };
            let unfinished = {
                let directory = directory.clone();
                crate::coven::blocking(move || directory.unfinished_replacement())
                    .await
                    .map_err(|error| failed(error.into()))?
            };
            stores.push(match unfinished {
                Some(record) => {
                    StoreInfo { id, name: record.name, unfinished_replacement: Some(record.by) }
                }
                None => StoreInfo {
                    id,
                    name: coven_database::read_store_name(directory).await.map_err(failed)?,
                    unfinished_replacement: None,
                },
            });
        }
        Ok(stores)
    }

    /// Address a store without reading settings or acquiring the writer lock.
    pub fn store_dir(&self, id: &StoreId) -> StoreDir {
        self.directories.store_dir(id)
    }

    /// Publish a directory fixture without a database or member custody.
    #[cfg(feature = "test-utils")]
    pub fn create_store_dir(
        &self,
        id: StoreId,
        ids: &dyn IdSource,
    ) -> Result<StoreDir, crate::DirectoryCreationError> {
        self.directories.create_store_dir(id, ids)
    }

    /// The stores whose first installation on this device is unfinished,
    /// ordered by id: a restore or a join that hasn't loaded.
    pub(crate) async fn unfinished_first_installations(&self) -> Result<Vec<StoreId>, FileError> {
        self.directories
            .unfinished_first_installations()
            .await
            .map_err(|coven_foundation::files::StoreDirectoriesError::File(error)| error)
    }

    /// Give the files of every store this layout creates, restores, joins,
    /// replaces or opens for writing Apple's protection class `protection`.
    #[cfg(target_vendor = "apple")]
    pub(crate) fn file_protection(mut self, protection: crate::FileProtection) -> Self {
        self.directories = self.directories.file_protection(protection);
        self
    }

    pub(crate) async fn begin_bootstrap(
        &self,
        id: StoreId,
        ids: &dyn IdSource,
    ) -> Result<BootstrapStore, BootstrapDirectoryError> {
        self.directories.begin_bootstrap(id, ids).await
    }

    pub(crate) async fn begin_replacement(
        &self,
        id: StoreId,
        by: ReplacedBy,
        ids: &dyn IdSource,
    ) -> Result<ReplacementStore, BootstrapDirectoryError> {
        self.directories.begin_replacement(id, by, ids).await
    }
}

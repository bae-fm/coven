//! Store listings read names from the same database as the open handle.

use crate::{CovenError, FileError, StoreDir, StoreId, StoreInfo};
use coven_foundation::files::{BootstrapDirectoryError, BootstrapStore, StoreDirectoriesAccess};
use coven_foundation::id_source::IdSource;
use std::path::PathBuf;

/// The stores under an app directory, one directory each (E1).
#[derive(Clone, Debug)]
pub struct StoreLayout {
    directories: coven_foundation::files::StoreDirectories,
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
    pub fn new(app_dir: PathBuf) -> Self {
        Self {
            directories: coven_foundation::files::StoreDirectories::new(app_dir),
        }
    }

    /// Stores on this device, ordered by id, with their committed names.
    /// A damaged name is an error; a listing never substitutes an older name.
    pub async fn stores(&self) -> Result<Vec<StoreInfo>, StoreLayoutError> {
        let mut stores = Vec::new();
        for id in self.directories.store_ids().await? {
            let name = coven_database::read_store_name(self.store_dir(&id))
                .await
                .map_err(|source| StoreLayoutError::Store { store: id, source })?;
            stores.push(StoreInfo { id, name });
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

    pub(crate) async fn begin_bootstrap(
        &self,
        id: StoreId,
        ids: &dyn IdSource,
    ) -> Result<BootstrapStore, BootstrapDirectoryError> {
        self.directories.begin_bootstrap(id, ids).await
    }
}

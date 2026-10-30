//! Files on disk: the store layout, directory, settings and exclusive lock.

#[cfg(target_vendor = "apple")]
mod asset_files;
#[cfg(target_vendor = "apple")]
pub use asset_files::{AssetFile, AssetFiles};

mod atomic_file;
mod bootstrap;
mod creation;
mod device_reset;
mod directory;
mod file_lock;
mod file_reader;
mod folder_copy;
mod layout;
mod lock;
mod lock_registry;
mod observed_file;
#[cfg(any(test, feature = "test-utils"))]
mod open_files;
mod relative_path;
mod replacement;
mod settings;

pub use atomic_file::{replace as write_atomic, AtomicFile, FileError, FileWriter};
pub use bootstrap::{BootstrapDirectoryError, BootstrapStore};
pub use creation::DirectoryCreationError;
pub use device_reset::{DeviceReset, ReplacedBy, ReplacementRecord};
pub use directory::{
    is_folder, FileArea, FileName, FileNameError, StoreDir, StoreDirAccess, StoreFile,
};
pub use file_lock::{FileLock, LockMode};
pub use file_reader::FileReader;
pub use folder_copy::{
    remove_copy, remove_folder, remove_temporary, CopyFolder, CopyPathError, CopyPlan, CopyWriter,
};
pub use layout::{StoreDirectories, StoreDirectoriesAccess, StoreDirectoriesError};
pub use lock::{
    CacheWriteLock, StoreClosingGuard, StoreDeletionLock, StoreLock, StoreLockError, StoreReadLock,
};
pub use observed_file::{observe_file, observe_metadata, ObservationError, ObservedFile};
#[cfg(any(test, feature = "test-utils"))]
pub use open_files::{assert_no_open_files_under, open_files_under};
pub use relative_path::{RelativePath, RelativePathError};
pub use replacement::ReplacementStore;
pub use settings::{SettingsError, StoreSettings};

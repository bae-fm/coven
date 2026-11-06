//! Files on disk: the store layout, directory, settings and exclusive lock.

#[cfg(target_vendor = "apple")]
mod asset_files;
#[cfg(target_vendor = "apple")]
pub use asset_files::{AssetFile, AssetFiles};

mod atomic_file;
mod bootstrap;
mod creation;
mod deletion;
mod device_reset;
mod directory;
mod file_lock;
mod file_reader;
mod flush_batch;
mod folder_copy;
mod layout;
mod lock;
mod lock_registry;
mod observed_file;
#[cfg(any(test, feature = "test-utils"))]
mod open_files;
mod path_bytes;
#[cfg(target_vendor = "apple")]
mod protection;
#[cfg(not(target_vendor = "apple"))]
#[path = "protection_elsewhere.rs"]
mod protection;
#[cfg(any(test, feature = "test-utils"))]
mod reached;
mod relative_path;
mod replacement;
mod settings;
#[cfg(any(test, feature = "test-utils"))]
mod tree_copy;

#[cfg(any(test, feature = "test-utils"))]
pub use atomic_file::read_existing;
pub use atomic_file::{replace as write_atomic, AtomicFile, FileError, FileWriter};
pub use bootstrap::{BootstrapDirectoryError, BootstrapStore};
pub use creation::DirectoryCreationError;
pub use deletion::IfUnsent;
pub use device_reset::{DeviceReset, ReplacedBy, ReplacementPhase, ReplacementRecord};
pub use directory::{
    protect_files, verify_lock, FileArea, FileName, FileNameError, StoreDir, StoreDirAccess,
    StoreFile, TemporaryFolder, WriterError,
};
pub use file_lock::{FileLock, LockMode};
pub use file_reader::FileReader;
#[cfg(any(test, feature = "test-utils"))]
pub use flush_batch::FlushHook;
pub use flush_batch::{FlushBatch, Flushed};
pub use folder_copy::{CopyFolder, CopyPathError, CopyPlan, CopyWriter, MadeFolder, RecordedCopy};
pub use layout::StoreDirectories;
#[cfg(any(test, feature = "test-utils"))]
pub use lock::{bootstrap_lock, cache_writes_lock, writer_lock};
pub use lock::{
    CacheWriteLock, StoreClosingGuard, StoreDeletionLock, StoreLock, StoreLockError, StoreReadLock,
};
pub use observed_file::{ObservationError, ObservedFile, UserOriginal};
#[cfg(any(test, feature = "test-utils"))]
pub use open_files::{assert_no_open_files_under, open_files_under};
pub use path_bytes::{path_bytes, path_bytes_text, path_from_bytes, path_text, PathBytesError};
#[cfg(target_vendor = "apple")]
pub use protection::FileProtection;
#[cfg(all(target_vendor = "apple", any(test, feature = "test-utils")))]
pub use protection::ProtectionSupport;
#[cfg(any(test, feature = "test-utils"))]
pub use reached::when_reached;
pub use relative_path::{windows_device, RelativePath, RelativePathError};
pub use replacement::ReplacementStore;
pub use settings::{SettingsError, StoreSettings};
#[cfg(any(test, feature = "test-utils"))]
pub use tree_copy::{copy_tree, copy_tree_keeping};

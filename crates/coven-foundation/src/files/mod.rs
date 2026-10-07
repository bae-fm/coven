//! Files on disk: the store layout, directory, settings and exclusive lock.

mod atomic_file;
mod bootstrap;
mod creation;
mod directory;
mod file_reader;
mod layout;
mod lock;
mod observed_file;
mod recovery;
mod settings;

pub use atomic_file::{AtomicFile, FileError, FileWriter};
pub use bootstrap::{BootstrapDirectoryError, BootstrapStore};
pub use creation::StoreCreationError;
pub use directory::{FileArea, FileName, FileNameError, StoreDir, StoreFile};
pub use file_reader::FileReader;
pub use layout::{StoreInfo, StoreLayout, StoreLayoutError};
pub use lock::{StoreDeletionLock, StoreLock, StoreLockError, StoreReadLock};
pub use observed_file::{observe_file, ObservationError, ObservedFile};
pub use settings::{SettingsError, StoreSettings};

pub use recovery::DatabaseRecovery;

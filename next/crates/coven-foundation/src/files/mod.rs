//! Files on disk: the store layout, directory, settings and exclusive lock.

mod atomic_file;
mod creation;
mod directory;
mod layout;
mod lock;
mod observed_file;
mod settings;

pub use atomic_file::{AtomicFile, FileError};
pub use creation::StoreCreationError;
pub use directory::{FileArea, FileName, FileNameError, StoreDir, StoreFile};
pub use layout::{StoreInfo, StoreLayout, StoreLayoutError};
pub use lock::{StoreLock, StoreLockError};
pub use observed_file::{observe_file, ObservationError, ObservedFile};
pub use settings::{SettingsError, StoreSettings};

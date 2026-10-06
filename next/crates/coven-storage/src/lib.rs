//! Storage providers, encrypted objects at paths, and recorded upload sessions (§4).
//!
//! This crate handles bytes without encrypting, hashing or signing them. The
//! caller records upload sessions and commits credentials to key custody only
//! after setup succeeds. Providers never read the database.
mod config;
mod credentials;
mod error;
mod path;
pub mod providers;
mod secret_json;
mod session;
mod settings;
mod storage;
mod transfer;

pub use config::*;
pub use credentials::*;
pub use error::*;
pub use path::*;
pub use session::UploadSession;
pub use settings::StorageSettings;
pub use storage::*;

#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

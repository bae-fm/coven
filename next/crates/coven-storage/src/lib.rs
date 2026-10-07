//! Storage providers, encrypted objects at paths, and recorded upload sessions (§4).
//!
//! This crate handles bytes without encrypting, hashing or signing them. The
//! caller records upload sessions and commits credentials to key custody only
//! after setup succeeds. Providers never read the database.
mod config;
mod connection;
mod credentials;
mod error;
mod invitation;
mod path;
pub mod providers;
mod restore_storage;
mod secret_json;
mod session;
mod settings;
mod storage;
mod transfer;

pub use config::*;
pub use connection::StorageConnection;
pub use credentials::*;
pub use error::*;
pub use invitation::StorageInvitation;
pub use path::*;
pub use restore_storage::RestoreStorage;
pub use session::UploadSession;
pub use settings::StorageSettings;
pub use storage::*;

#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

mod invite_storage;
pub use invite_storage::InviteStorage;

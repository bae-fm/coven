//! Storage providers, encrypted objects at paths, and recorded upload sessions (§4).
//!
//! This crate handles bytes without encrypting, hashing or signing them. The
//! caller records upload sessions and commits credentials to key custody only
//! after setup succeeds. Providers never read the database.
mod config;
mod connection;
mod connection_credentials;
mod credentials;
mod error;
mod invitation;
mod object_listing;
mod path;
pub use object_listing::ObjectListing;
mod provider_check;
mod provider_ops;
pub use provider_check::check_provider;
pub use provider_ops::{ProviderOps, ProviderRevocation};
pub mod providers;
mod restore_storage;
mod secret_json;
mod session;
mod settings;
mod storage;
mod transfer;
mod web_url;

pub use config::*;
pub use connection::StorageConnection;
pub use connection_credentials::ConnectionCredentials;
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

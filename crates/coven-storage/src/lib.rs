//! Storage providers, encrypted objects at paths, and recorded upload sessions (§4).
//!
//! This crate handles encrypted objects without performing coven's object
//! cryptography. Provider adapters authenticate their native requests. The
//! caller records upload sessions and commits credentials to key custody only
//! after setup succeeds. Providers never read the database.
mod config;
mod provider_access;
pub use provider_access::*;
mod connection;
mod connection_credentials;
mod credentials;
mod error;
mod invitation;
mod object_listing;
mod object_metadata;
pub use object_metadata::*;
mod path;
pub use object_listing::ObjectListing;
mod provider_check;
mod provider_ops;
pub use provider_check::check_provider;
pub use provider_ops::{ProviderOps, ProviderRevocation};
pub mod providers;
mod restore_storage;
mod s3_detection;
pub use s3_detection::*;
mod s3_settings;
pub use s3_settings::*;
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
pub use session::UploadSession;
pub use settings::StorageSettings;
pub use storage::*;

#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

mod invite_storage;
pub use invite_storage::InviteStorage;

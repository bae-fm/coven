//! Storage providers, encrypted objects at paths, and recorded upload sessions (§20).
//!
//! This crate handles encrypted objects without performing coven's object
//! cryptography. Provider adapters authenticate their native requests. The
//! caller records upload sessions and commits credentials to key custody only
//! after setup succeeds. Providers never read the database.
mod access_admin;
pub use access_admin::{
    AccessKeyAdmin, AdminCredential, ListedKey, MintScope, S3MemberKey, StoreListing,
    UnkeyedPrincipal,
};
mod config;
mod provider_access;
pub use provider_access::*;
mod bucket_claim;
mod connection;
mod connection_credentials;
mod cooldown;
pub use cooldown::{
    retry_delay, Cooldown, CooldownNotRecorded, CoolingDown, NoCooldown, RetryAfter,
    LONGEST_RETRY_DELAY,
};
mod credentials;
mod error;
mod invitation;
mod object_listing;
mod object_metadata;
mod object_stream;
pub use object_metadata::*;
pub use object_stream::{ObjectBody, ObjectStream, BUFFERED_READ_LIMIT};
mod path;
pub use object_listing::ObjectListing;
mod provider_check;
mod provider_ops;
mod publication;
pub use provider_check::check_provider;
pub use provider_ops::ProviderOps;
pub use publication::{PublicationGate, PublicationRequest};
pub mod providers;
mod restore_storage;
mod s3_detection;
pub use s3_detection::*;
mod s3_prefix;
pub use s3_prefix::{S3Prefix, S3PrefixError};
mod s3_settings;
pub use s3_settings::*;
mod secret_json;
mod session;
mod settings;
mod storage;
mod token_custody;
pub(crate) use token_custody::HeldTokens;
pub use token_custody::{ProviderAuth, TokenCustody};
mod transfer_budget;
pub use transfer_budget::{TransferBudget, TransferLimits};
mod transfer;
pub use transfer::{buffered_request_limit, BUFFERED_REQUEST_LIMIT};
mod web_url;

pub use config::*;
pub use connection::StorageConnection;
pub use connection_credentials::ConnectionCredentials;
pub use credentials::*;
pub use error::*;
pub use invitation::StorageInvitation;
pub use path::*;
pub use session::{UploadSession, UploadTarget};
pub use settings::StorageSettings;
pub use storage::*;

#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

mod invite_storage;
pub use invite_storage::InviteStorage;

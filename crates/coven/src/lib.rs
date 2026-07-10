//! coven — the native host integration for end-to-end encrypted, multi-writer,
//! bring-your-own-storage SQLite sync. The engine lives in `coven-core`; this
//! crate wires it to the filesystem, the platform keyring, and native cloud
//! providers, and re-exports the curated host API.
//!
//! The public API is exactly the crate-root re-exports below. The engine's
//! implementation modules are `pub(crate)`, so a host reaches coven only through
//! these names — never through `coven::sync::…` or `coven::blob::…`. In
//! particular the sync driver is private: it starts only through
//! [`CovenHandle::connect_sync`], which holds the lifecycle lock, so a host
//! cannot drive the loop out from under the handle.
//!
//! ```compile_fail
//! // `sync` is a private module; the sync driver is unreachable from outside.
//! let _ = coven::sync::sync_manager::SyncManager::start_sync;
//! ```

pub(crate) mod keys;
pub(crate) mod oauth;

pub(crate) mod blob {
    pub use coven_core::blob::*;
    pub mod transition {
        pub use coven_core::blob::transition::*;
    }
}

pub(crate) mod clock {
    pub use coven_core::clock::*;
}

pub(crate) mod config {
    pub use coven_core::config::*;
}

pub(crate) mod database {
    pub use coven_core::database::*;
}

pub(crate) mod encryption {
    pub use coven_core::encryption::*;
}

pub(crate) mod id_provider {
    pub use coven_core::id_provider::*;
}

pub(crate) mod join_code {
    pub use coven_core::join_code::*;

    pub fn generate_join_request(email: Option<String>) -> Result<String, crate::keys::KeyError> {
        let global_ks = crate::keys::KeyService::new("global".to_string());
        let keypair = global_ks.get_or_create_user_keypair()?;
        Ok(coven_core::join_code::generate_join_request_for_keypair(
            &keypair, email,
        ))
    }
}

/// Fetch the email of the account `tokens` authenticated, for the given OAuth
/// provider. The joining device calls this right after authenticating so the
/// approver can share the OAuth folder to its provider-account email.
///
/// Only the OAuth providers are valid here; a non-OAuth provider (S3, CloudKit)
/// is a programming error and surfaces as an error rather than a silent default.
#[cfg(feature = "oauth-providers")]
pub async fn fetch_account_email(
    provider: crate::config::CloudProvider,
    tokens: &oauth::OAuthTokens,
) -> Result<String, oauth::OAuthError> {
    use crate::config::CloudProvider;
    use crate::storage::cloud::account_email;

    let result = match provider {
        CloudProvider::GoogleDrive => account_email::fetch_google(tokens).await,
        CloudProvider::Dropbox => account_email::fetch_dropbox(tokens).await,
        CloudProvider::OneDrive => account_email::fetch_onedrive(tokens).await,
        other => {
            return Err(oauth::OAuthError::AccountFetch(format!(
                "{other:?} does not use OAuth; account email is only fetched for OAuth providers"
            )))
        }
    };
    result.map_err(|e| oauth::OAuthError::AccountFetch(e.to_string()))
}

pub(crate) mod library_dir {
    pub use coven_core::library_dir::*;
}

mod local_blob_backend;

pub(crate) mod local_blob {
    pub use coven_core::local_blob::*;
}

pub(crate) mod migration {
    pub use coven_core::migration::*;
}

pub(crate) mod storage {
    pub mod cloud {
        pub use coven_core::storage::cloud::*;

        pub mod s3_common {
            pub use coven_core::storage::cloud::s3_common::*;
        }

        #[cfg(feature = "oauth-providers")]
        pub mod account_email;
        pub mod cloudkit;
        #[cfg(feature = "oauth-providers")]
        pub mod dropbox;
        #[cfg(feature = "oauth-providers")]
        pub mod google_drive;
        #[cfg(feature = "oauth-providers")]
        mod http;
        #[cfg(feature = "oauth-providers")]
        mod key_encoding;
        #[cfg(feature = "oauth-providers")]
        mod oauth_rest;
        #[cfg(feature = "oauth-providers")]
        pub mod oauth_session;
        #[cfg(feature = "oauth-providers")]
        pub mod onedrive;
        #[cfg(feature = "oauth-providers")]
        mod resumable;
        pub mod s3;
        pub mod setup;
        #[cfg(feature = "oauth-providers")]
        mod sharing;

        #[cfg(feature = "oauth-providers")]
        fn require_oauth_token(
            key_service: &crate::keys::KeyService,
            provider_name: &str,
        ) -> Result<String, CloudHomeError> {
            match key_service.get_cloud_home_credentials().map_err(|e| {
                CloudHomeError::Configuration(format!("{provider_name} credentials error: {e}"))
            })? {
                Some(crate::keys::CloudHomeCredentials::OAuth { token_json }) => Ok(token_json),
                _ => Err(CloudHomeError::Configuration(format!(
                    "{provider_name} OAuth token not in keyring"
                ))),
            }
        }

        #[cfg(feature = "oauth-providers")]
        fn parse_oauth_tokens(
            key_service: &crate::keys::KeyService,
            provider_name: &str,
        ) -> Result<crate::oauth::OAuthTokens, CloudHomeError> {
            let token_json = require_oauth_token(key_service, provider_name)?;
            serde_json::from_str(&token_json).map_err(|e| {
                CloudHomeError::Configuration(format!("invalid OAuth token JSON: {e}"))
            })
        }

        /// Build a [`CloudHome`] from `config`, surfacing a missing or malformed
        /// provider configuration as a non-retryable
        /// [`CloudHomeError::Configuration`] so a host can tell "fix your settings"
        /// apart from a transient failure it should keep retrying.
        pub async fn create_cloud_home(
            config: &crate::config::Config,
            key_service: &crate::keys::KeyService,
            clock: crate::clock::ClockRef,
        ) -> Result<Box<dyn CloudHome>, CloudHomeError> {
            create_cloud_home_with_cloudkit(config, key_service, clock, None).await
        }

        pub async fn create_cloud_home_with_cloudkit(
            config: &crate::config::Config,
            key_service: &crate::keys::KeyService,
            clock: crate::clock::ClockRef,
            cloudkit_ops: Option<std::sync::Arc<dyn cloudkit::CloudKitOps>>,
        ) -> Result<Box<dyn CloudHome>, CloudHomeError> {
            use crate::config::CloudProvider;

            #[cfg(not(feature = "oauth-providers"))]
            let _ = &clock;

            match config.cloud_home.provider {
                Some(CloudProvider::S3) | None => {
                    let bucket = config.cloud_home.s3_bucket.clone().ok_or_else(|| {
                        CloudHomeError::Configuration("S3 bucket not configured".to_string())
                    })?;
                    let region = config.cloud_home.s3_region.clone().ok_or_else(|| {
                        CloudHomeError::Configuration("S3 region not configured".to_string())
                    })?;
                    let endpoint = config.cloud_home.s3_endpoint.clone();

                    let (access_key, secret_key) =
                        match key_service.get_cloud_home_credentials().map_err(|e| {
                            CloudHomeError::Configuration(format!("S3 credentials error: {e}"))
                        })? {
                            Some(crate::keys::CloudHomeCredentials::S3 {
                                access_key,
                                secret_key,
                            }) => (access_key, secret_key),
                            _ => {
                                return Err(CloudHomeError::Configuration(
                                    "S3 credentials not in keyring".to_string(),
                                ))
                            }
                        };

                    let s3 = s3::S3CloudHome::new(
                        bucket,
                        region,
                        endpoint,
                        access_key,
                        secret_key,
                        config.cloud_home.s3_key_prefix.clone(),
                    )
                    .await?;
                    Ok(Box::new(s3))
                }
                #[cfg(feature = "oauth-providers")]
                Some(CloudProvider::GoogleDrive) => {
                    let folder_id = config
                        .cloud_home
                        .google_drive_folder_id
                        .clone()
                        .ok_or_else(|| {
                            CloudHomeError::Configuration(
                                "Google Drive folder ID not configured".to_string(),
                            )
                        })?;
                    let tokens = parse_oauth_tokens(key_service, "Google Drive")?;
                    Ok(Box::new(google_drive::GoogleDriveCloudHome::new(
                        folder_id,
                        tokens,
                        key_service.clone(),
                        clock,
                    )?))
                }
                #[cfg(feature = "oauth-providers")]
                Some(CloudProvider::Dropbox) => {
                    let folder_path =
                        config
                            .cloud_home
                            .dropbox_folder_path
                            .clone()
                            .ok_or_else(|| {
                                CloudHomeError::Configuration(
                                    "Dropbox folder path not configured".to_string(),
                                )
                            })?;
                    let tokens = parse_oauth_tokens(key_service, "Dropbox")?;
                    Ok(Box::new(dropbox::DropboxCloudHome::new(
                        folder_path,
                        tokens,
                        key_service.clone(),
                        clock,
                    )?))
                }
                #[cfg(feature = "oauth-providers")]
                Some(CloudProvider::OneDrive) => {
                    let drive_id =
                        config.cloud_home.onedrive_drive_id.clone().ok_or_else(|| {
                            CloudHomeError::Configuration(
                                "OneDrive drive ID not configured".to_string(),
                            )
                        })?;
                    let folder_id =
                        config
                            .cloud_home
                            .onedrive_folder_id
                            .clone()
                            .ok_or_else(|| {
                                CloudHomeError::Configuration(
                                    "OneDrive folder ID not configured".to_string(),
                                )
                            })?;
                    let tokens = parse_oauth_tokens(key_service, "OneDrive")?;
                    Ok(Box::new(onedrive::OneDriveCloudHome::new(
                        drive_id,
                        folder_id,
                        tokens,
                        key_service.clone(),
                        clock,
                    )?))
                }
                #[cfg(not(feature = "oauth-providers"))]
                Some(
                    CloudProvider::GoogleDrive | CloudProvider::Dropbox | CloudProvider::OneDrive,
                ) => Err(CloudHomeError::Configuration(
                    "OAuth cloud providers are not supported in this build".to_string(),
                )),
                Some(CloudProvider::CloudKit) => {
                    let ops = cloudkit_ops.ok_or_else(|| {
                        CloudHomeError::Configuration("CloudKit driver not provided".to_string())
                    })?;
                    match (
                        config.cloud_home.cloudkit_share_url.as_ref(),
                        config.cloud_home.cloudkit_owner_name.as_ref(),
                        config.cloud_home.cloudkit_zone_name.as_ref(),
                    ) {
                        (None, None, None) => {
                            Ok(Box::new(cloudkit::CloudKitCloudHome::new_private(ops)))
                        }
                        (Some(_), Some(owner_name), Some(zone_name)) => {
                            Ok(Box::new(cloudkit::CloudKitCloudHome::new_shared(
                                ops,
                                owner_name.clone(),
                                zone_name.clone(),
                            )))
                        }
                        _ => Err(CloudHomeError::Configuration(
                            "CloudKit share config requires share URL, owner name, and zone name"
                                .to_string(),
                        )),
                    }
                }
            }
        }
    }

    pub mod local;
}

pub(crate) mod sync {
    pub use coven_core::sync::*;

    pub mod join;
    #[cfg(test)]
    mod join_tests;
    pub mod restore;
    #[cfg(test)]
    mod restore_tests;
    pub mod sync_loop;
    pub mod sync_manager;
}

mod coven;
mod database_backend;
mod handle;
mod keyring_backend;
mod read_handle;

pub(crate) fn install_platform() {
    database_backend::install_platform_connection_opener();
    local_blob_backend::install_platform_backend();
}

// coven's public API is exactly the crate-root re-exports below. The
// implementation modules are `pub(crate)`; a host reaches coven only through
// these names, never through `coven::sync::…` or `coven::blob::…`.

pub use coven::{
    Coven, CovenBuilder, CovenConfig, CovenError, CovenResult, SqlContext, WriteBatch,
};
pub use handle::CovenHandle;
pub use read_handle::CovenReadHandle;

// --- coven-core's curated engine surface, re-exported so a host names it as
//     `coven::…` and never depends on `coven-core` directly. ---

/// The exact `rusqlite` coven owns the connection through; see [`CovenHandle::sql`].
pub use coven_core::rusqlite;

// Host schema declaration: the synced-table set and the synced-schema migration ladder.
pub use coven_core::{BlobDecl, Migration, MigrationStep, SyncedTable};

// Config.
pub use coven_core::{CloudHomeConfig, CloudProvider, Config, ConfigError, HomeStorage};

// Blob descriptors, cache error, the host-implemented transition observer.
pub use coven_core::{
    BlobCacheError, BlobRef, BlobScope, BlobTransitionObserver, CacheFill, Provenance,
};

// Applied-sync change notification.
pub use coven_core::{ChangeOp, RowChange};

// At-rest crypto the host configures, the library directory, the DB error.
pub use coven_core::{DbError, EncryptionError, EncryptionService, LibraryDir, CHUNK_SIZE};

// The register clock vocabulary carried on every synced row.
pub use coven_core::{Hlc, Timestamp, UpdatedAtStamper};

// Membership.
pub use coven_core::{MemberInfo, MemberRole};

// Clock / id abstractions the host injects, plus the deterministic test fakes.
pub use coven_core::{Clock, ClockRef, IdProvider, IdRef, SystemClock, UuidProvider};
#[cfg(any(test, feature = "test-utils"))]
pub use coven_core::{FixedClock, SequentialIdProvider, SteppingClock};

// Bootstrap decoders and the cloud at-rest cipher.
pub use coven_core::{
    decode_invite_code_info, decode_join_request, decode_restore_code_info, CloudCipher,
    JoinCodeError,
};

// Cloud provider trait surface a provider implementor needs, the thread-safety
// floor those traits carry, and the pull-result rejection reports.
pub use coven_core::{
    BlobBody, BoxPartSink, CloudAccessGrant, CloudAccessRevoke, CloudHome, CloudHomeError,
    CloudHomeJoinInfo, InvalidSignature, MaybeThreadSafe, PartSink, RejectedUnauthorized,
    UploadProgress,
};

// Sync-status surface a host renders from `CovenHandle::subscribe_sync_status`:
// the status enum, its completed-cycle success payload, the per-cycle alert
// bundle, the per-device activity, and the held-changeset detail the alerts carry.
pub use coven_core::{
    DeviceActivity, HeldChangeset, HeldChangesetReason, SyncLoopAlerts, SyncLoopSuccess,
};
pub use sync::sync_loop::SyncLoopStatus;

// In-memory cloud home and durable upload-queue rows for host integration tests.
#[cfg(any(test, feature = "test-utils"))]
pub use coven_core::{InMemoryCloudHome, OutboxEntry, OutboxOperation};

// --- Native additions and native-only re-exports. ---

pub use blob::transition::{MakeLocalError, MakeRemoteError};
pub use join_code::generate_join_request;
pub use keys::{
    keyring_service, read_keyring, set_keyring_service, CloudHomeCredentials, KeyError,
    KeyPersistence, KeyService, UserKeypair,
};
pub use oauth::{set_oauth_client_creds, OAuthClientCreds, OAuthClientCredsConflict, OAuthTokens};
pub use storage::cloud::setup::generate_restore_code;
pub use storage::cloud::{
    cloudkit::{CloudKitOps, CloudKitScope, CloudKitShare},
    create_cloud_home,
    s3::S3CloudHome,
};
pub use storage::local::BlobStore;
pub use sync::join::join_from_invite_code;
pub use sync::restore::{restore_from_cloud, restore_from_code, RestoreSource};
pub use sync::sync_manager::SyncError;

#[cfg(feature = "oauth-providers")]
pub use oauth::{
    authorize_provider, build_authorize_request_for_provider, exchange_code_for_provider,
    OAuthClientCredsError, OAuthError,
};

#[cfg(feature = "oauth-providers")]
pub use storage::cloud::setup::{sign_in_dropbox, sign_in_google_drive, sign_in_onedrive};

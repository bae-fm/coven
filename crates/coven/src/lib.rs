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
        let keypair = crate::keys::DeviceKeys::get_or_create_user_keypair()?;
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

pub(crate) mod store_dir {
    pub use coven_core::store_dir::*;
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
            key_service: &crate::keys::StoreKeys,
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
            key_service: &crate::keys::StoreKeys,
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
            key_service: &crate::keys::StoreKeys,
            clock: crate::clock::ClockRef,
        ) -> Result<Box<dyn CloudHome>, CloudHomeError> {
            create_cloud_home_with_cloudkit(config, key_service, clock, None).await
        }

        pub async fn create_cloud_home_with_cloudkit(
            config: &crate::config::Config,
            key_service: &crate::keys::StoreKeys,
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
                        config.cloud_home.cloudkit_owner_name.as_ref(),
                        config.cloud_home.cloudkit_zone_name.as_ref(),
                    ) {
                        (None, None) => {
                            Ok(Box::new(cloudkit::CloudKitCloudHome::new_private(ops)))
                        }
                        (Some(owner_name), Some(zone_name)) => {
                            Ok(Box::new(cloudkit::CloudKitCloudHome::new_shared(
                                ops,
                                owner_name.clone(),
                                zone_name.clone(),
                            )))
                        }
                        _ => Err(CloudHomeError::Configuration(
                            "CloudKit share config requires both cloudkit_owner_name and cloudkit_zone_name"
                                .to_string(),
                        )),
                    }
                }
            }
        }

        #[cfg(test)]
        mod tests {
            use super::*;
            use crate::clock::FixedClock;
            use crate::config::{CloudProvider, Config, HomeStorage};
            use crate::keys::StoreKeys;
            use crate::storage::cloud::cloudkit::{CloudKitOps, CloudKitScope, CloudKitShare};
            use crate::store_dir::StoreDir;
            use std::sync::Mutex;

            /// Records the scope each `list_records` call carried, so a test can
            /// tell whether `create_cloud_home_with_cloudkit` built a private or a
            /// shared home without reaching into its private fields. Every other
            /// method is unused by these tests and panics if called.
            struct ScopeRecordingOps {
                seen: Mutex<Vec<CloudKitScope>>,
            }

            impl ScopeRecordingOps {
                fn new() -> Self {
                    Self {
                        seen: Mutex::new(Vec::new()),
                    }
                }
            }

            impl CloudKitOps for ScopeRecordingOps {
                fn write_record(
                    &self,
                    _scope: &CloudKitScope,
                    _key: &str,
                    _data: Vec<u8>,
                ) -> Result<(), CloudHomeError> {
                    unimplemented!("not exercised by these tests")
                }
                fn read_record(
                    &self,
                    _scope: &CloudKitScope,
                    _key: &str,
                ) -> Result<Vec<u8>, CloudHomeError> {
                    unimplemented!("not exercised by these tests")
                }
                fn list_records(
                    &self,
                    scope: &CloudKitScope,
                    _prefix: &str,
                ) -> Result<Vec<String>, CloudHomeError> {
                    self.seen.lock().unwrap().push(scope.clone());
                    Ok(Vec::new())
                }
                fn delete_record(
                    &self,
                    _scope: &CloudKitScope,
                    _key: &str,
                ) -> Result<(), CloudHomeError> {
                    unimplemented!("not exercised by these tests")
                }
                fn record_exists(
                    &self,
                    _scope: &CloudKitScope,
                    _key: &str,
                ) -> Result<bool, CloudHomeError> {
                    unimplemented!("not exercised by these tests")
                }
                fn grant_share(
                    &self,
                    _member_pubkey: &str,
                ) -> Result<CloudKitShare, CloudHomeError> {
                    unimplemented!("not exercised by these tests")
                }
                fn revoke_share(&self, _member_pubkey: &str) -> Result<(), CloudHomeError> {
                    unimplemented!("not exercised by these tests")
                }
                fn accept_share(&self, _share_url: &str) -> Result<CloudKitShare, CloudHomeError> {
                    unimplemented!("not exercised by these tests")
                }
            }

            fn cloudkit_config(owner_zone: Option<(&str, &str)>) -> Config {
                let mut config = Config::with_defaults(
                    "store-1".to_string(),
                    "device-1".to_string(),
                    StoreDir::new("unused-store-dir"),
                    "CloudKit Store".to_string(),
                );
                config.cloud_home.provider = Some(CloudProvider::CloudKit);
                config.cloud_home.storage = HomeStorage::Opaque;
                if let Some((owner, zone)) = owner_zone {
                    config.cloud_home.cloudkit_owner_name = Some(owner.to_string());
                    config.cloud_home.cloudkit_zone_name = Some(zone.to_string());
                }
                config
            }

            /// Neither `cloudkit_owner_name` nor `cloudkit_zone_name` set builds a
            /// private home — the scope every subsequent op sees is `Private`.
            #[tokio::test]
            async fn neither_owner_nor_zone_builds_a_private_home() {
                let config = cloudkit_config(None);
                let key_service = StoreKeys::new(config.store_id.clone());
                let ops = std::sync::Arc::new(ScopeRecordingOps::new());
                let clock: crate::clock::ClockRef =
                    std::sync::Arc::new(FixedClock(chrono::Utc::now()));

                let home = create_cloud_home_with_cloudkit(
                    &config,
                    &key_service,
                    clock,
                    Some(ops.clone()),
                )
                .await
                .expect("private CloudKit config builds a home");
                home.list("").await.expect("list against the built home");

                assert_eq!(
                    ops.seen.lock().unwrap().as_slice(),
                    [CloudKitScope::Private]
                );
            }

            /// Both `cloudkit_owner_name` and `cloudkit_zone_name` set builds a
            /// shared home scoped to that owner/zone pair.
            #[tokio::test]
            async fn both_owner_and_zone_build_a_shared_home() {
                let config = cloudkit_config(Some(("owner-name", "zone-name")));
                let key_service = StoreKeys::new(config.store_id.clone());
                let ops = std::sync::Arc::new(ScopeRecordingOps::new());
                let clock: crate::clock::ClockRef =
                    std::sync::Arc::new(FixedClock(chrono::Utc::now()));

                let home = create_cloud_home_with_cloudkit(
                    &config,
                    &key_service,
                    clock,
                    Some(ops.clone()),
                )
                .await
                .expect("shared CloudKit config builds a home");
                home.list("").await.expect("list against the built home");

                assert_eq!(
                    ops.seen.lock().unwrap().as_slice(),
                    [CloudKitScope::Shared {
                        owner_name: "owner-name".to_string(),
                        zone_name: "zone-name".to_string(),
                    }]
                );
            }

            /// Only one of `cloudkit_owner_name` / `cloudkit_zone_name` set is the
            /// broken shape the config module can't rule out by construction (see
            /// the module doc: config stays flat, provider-prefixed fields rather
            /// than one enum for CloudKit alone) — surfaced as a `Configuration`
            /// error naming both fields, not a silent guess at which home to build.
            #[tokio::test]
            async fn mixed_owner_zone_is_a_configuration_error() {
                let mut config = cloudkit_config(None);
                config.cloud_home.cloudkit_owner_name = Some("owner-name".to_string());
                let key_service = StoreKeys::new(config.store_id.clone());
                let ops = std::sync::Arc::new(ScopeRecordingOps::new());
                let clock: crate::clock::ClockRef =
                    std::sync::Arc::new(FixedClock(chrono::Utc::now()));

                let result =
                    create_cloud_home_with_cloudkit(&config, &key_service, clock, Some(ops)).await;
                match result {
                    Ok(_) => panic!("mixed owner/zone must not build a home"),
                    Err(CloudHomeError::Configuration(message)) => {
                        assert!(message.contains("cloudkit_owner_name"), "{message}");
                        assert!(message.contains("cloudkit_zone_name"), "{message}");
                    }
                    Err(other) => panic!("expected Configuration error, got {other:?}"),
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

// At-rest crypto the host configures, the store directory and its
// host-configurable layout, the DB error.
pub use coven_core::{
    DbError, EncryptionError, EncryptionService, StoreDir, StoreLayout, CHUNK_SIZE,
};

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
    keyring_service, set_keyring_service, CloudHomeCredentials, DeviceKeys, KeyError,
    KeyPersistence, StoreKeys, UserKeypair,
};
pub use oauth::{set_oauth_client_creds, OAuthClientCreds, OAuthClientCredsConflict, OAuthTokens};
pub use storage::cloud::setup::generate_restore_code;
pub use storage::cloud::{
    cloudkit::{CloudKitOps, CloudKitScope, CloudKitShare},
    create_cloud_home,
    s3::S3CloudHome,
};
pub use storage::local::BlobStore;
pub use sync::join::{join_from_invite_code, BootstrapError};
pub use sync::restore::{restore_from_cloud, restore_from_code, RestoreSource};
pub use sync::sync_manager::SyncError;

#[cfg(feature = "oauth-providers")]
pub use oauth::{
    authorize_provider, build_authorize_request_for_provider, exchange_code_for_provider,
    OAuthClientCredsError, OAuthError,
};

#[cfg(feature = "oauth-providers")]
pub use storage::cloud::setup::{sign_in_dropbox, sign_in_google_drive, sign_in_onedrive};

//! coven — host integration for end-to-end encrypted, multi-writer,
//! bring-your-own-storage SQLite sync. The engine lives in `coven-core`; this
//! crate wires it to the filesystem, the platform keyring, and cloud
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

pub(crate) mod custody;
pub(crate) mod envelope;
pub(crate) mod identity_custody;
pub(crate) mod keys;
pub(crate) mod oauth;

pub(crate) mod blob {
    pub(crate) use coven_core::blob::*;
    pub(crate) mod transition {
        pub use coven_core::blob::transition::*;
    }
}

pub(crate) mod clock {
    pub(crate) use coven_core::clock::*;
}

pub(crate) mod config {
    pub(crate) use coven_core::config::*;
}

pub(crate) mod database {
    pub(crate) use coven_core::database::*;
}

pub(crate) mod encryption {
    pub(crate) use coven_core::encryption::*;
}

pub(crate) mod id_provider {
    pub(crate) use coven_core::id_provider::*;
}

pub(crate) mod join_code {
    pub(crate) use coven_core::join_code::*;

    /// Build a join request carrying a freshly minted pending identity (see
    /// `mint_pending_identity`) — the joiner sends its public
    /// key before it learns which store the invite names, so the keypair is
    /// generated now and held under a pending slot keyed by that public key.
    /// Completing the join with [`crate::DeviceJoinClient`] (constructed with
    /// this same code) promotes it into the joined store's own identity;
    /// [`crate::abandon_join_request`] discards it if the request is
    /// abandoned instead.
    pub fn generate_join_request(email: Option<String>) -> Result<String, crate::keys::KeyError> {
        let keypair = crate::keys::mint_pending_identity()?;
        Ok(coven_core::join_code::generate_join_request_for_keypair(
            &keypair, email,
        ))
    }

    /// Abandon a join request this device generated but never completed —
    /// removes the pending identity [`generate_join_request`] minted for it.
    /// `Ok` whether or not one was still pending.
    pub fn abandon_join_request(request_code: &str) -> Result<(), crate::keys::KeyError> {
        let request = decode_join_request(request_code)
            .map_err(|e| crate::keys::KeyError::Crypto(e.to_string()))?;
        crate::keys::discard_pending_identity(&request.public_key)
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
    pub(crate) use coven_core::store_dir::*;
}

pub(crate) mod local_blob {
    pub(crate) use coven_core::local_blob::*;
}

pub(crate) mod migration {
    pub(crate) use coven_core::migration::*;
}

pub(crate) mod storage {
    pub(crate) mod cloud {
        pub(crate) use coven_core::storage::cloud::*;

        pub(crate) mod s3_common {
            pub(crate) use coven_core::storage::cloud::s3_common::*;
        }

        #[cfg(feature = "oauth-providers")]
        pub(crate) mod account_email;
        pub(crate) mod cloudkit;
        #[cfg(feature = "oauth-providers")]
        pub(crate) mod dropbox;
        #[cfg(feature = "oauth-providers")]
        pub(crate) mod google_drive;
        #[cfg(feature = "oauth-providers")]
        mod http;
        #[cfg(feature = "oauth-providers")]
        mod key_encoding;
        #[cfg(feature = "oauth-providers")]
        mod oauth_rest;
        #[cfg(feature = "oauth-providers")]
        pub(crate) mod oauth_session;
        #[cfg(feature = "oauth-providers")]
        pub(crate) mod onedrive;
        #[cfg(feature = "oauth-providers")]
        mod resumable;
        pub(crate) mod s3;
        pub(crate) mod setup;
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

        pub(crate) async fn create_cloud_home_with_cloudkit(
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
                        config.cloud_home.s3_exact_slots,
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
            use crate::storage::cloud::cloudkit::{
                CloudKitAcceptedShareRecord, CloudKitAtomicCreateBatch, CloudKitOps,
                CloudKitProviderIdentity, CloudKitRecordCreate, CloudKitRecordVersion,
                CloudKitScope, CloudKitShare,
            };
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
                fn provider_identity(
                    &self,
                    scope: &CloudKitScope,
                ) -> Result<CloudKitProviderIdentity, CloudHomeError> {
                    let (owner_name, zone_name) = match scope {
                        CloudKitScope::Private => ("test-owner", "test-zone"),
                        CloudKitScope::Shared {
                            owner_name,
                            zone_name,
                        } => (owner_name.as_str(), zone_name.as_str()),
                    };
                    Ok(CloudKitProviderIdentity {
                        container_id: "iCloud.test.coven".to_string(),
                        environment: crate::CloudKitEnvironment::Development,
                        owner_name: owner_name.to_string(),
                        zone_name: zone_name.to_string(),
                        current_user_record_name: "test-user".to_string(),
                    })
                }

                fn accepted_read_write_share(
                    &self,
                    _scope: &CloudKitScope,
                ) -> Result<CloudKitAcceptedShareRecord, CloudHomeError> {
                    Err(CloudHomeError::NotFound(
                        "accepted CloudKit share".to_string(),
                    ))
                }

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
                fn read_versioned_record(
                    &self,
                    _scope: &CloudKitScope,
                    _key: &str,
                ) -> Result<crate::storage::cloud::CloudVersionedObject, CloudHomeError>
                {
                    unimplemented!("not exercised by these tests")
                }

                fn begin_atomic_create(
                    &self,
                    _scope: &CloudKitScope,
                ) -> Result<CloudKitAtomicCreateBatch, CloudHomeError> {
                    unimplemented!("not exercised by these tests")
                }
                fn stage_atomic_create_record(
                    &self,
                    _scope: &CloudKitScope,
                    _batch: &CloudKitAtomicCreateBatch,
                    _record: CloudKitRecordCreate,
                ) -> Result<(), CloudHomeError> {
                    unimplemented!("not exercised by these tests")
                }
                fn commit_atomic_create(
                    &self,
                    _scope: &CloudKitScope,
                    _batch: &CloudKitAtomicCreateBatch,
                ) -> Result<Vec<CloudKitRecordVersion>, CloudHomeError> {
                    unimplemented!("not exercised by these tests")
                }
                fn discard_atomic_create(
                    &self,
                    _scope: &CloudKitScope,
                    _batch: &CloudKitAtomicCreateBatch,
                ) -> Result<(), CloudHomeError> {
                    unimplemented!("not exercised by these tests")
                }
                fn delete_record_versions(
                    &self,
                    _scope: &CloudKitScope,
                    _records: &[CloudKitRecordVersion],
                ) -> Result<(), CloudHomeError> {
                    unimplemented!("not exercised by these tests")
                }
                fn grant_share(
                    &self,
                    _member_pubkey: &str,
                ) -> Result<CloudKitShare, CloudHomeError> {
                    unimplemented!("not exercised by these tests")
                }
                fn share_for_member(
                    &self,
                    _member_pubkey: &str,
                ) -> Result<Option<CloudKitShare>, CloudHomeError> {
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
}

pub(crate) mod sync {
    pub(crate) use coven_core::sync::*;

    #[cfg(test)]
    mod device_join_facade_tests;
    pub(crate) mod device_join_transport;
    #[cfg(test)]
    mod device_join_transport_tests;
    pub(crate) mod join;
    #[cfg(test)]
    mod join_tests;
    pub(crate) mod restore;
    #[cfg(test)]
    mod restore_tests;
    pub(crate) mod sync_loop;
    pub(crate) mod sync_manager;
}

#[cfg(test)]
mod blob_facade_tests;
mod circles;
mod coven;
mod handle;
mod keyring_backend;
mod read_handle;

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
pub use coven_core::{BlobDecl, Migration, MigrationStep, RowIdentity, SyncedTable};

// Config.
pub use coven_core::sync::storage::CloudKitEnvironment;
pub use coven_core::{
    CloudHomeConfig, CloudProvider, Config, ConfigError, CustomS3ExactSlots, HomeStorage,
};

// Blob descriptors, cache error, the host-implemented transition observer.
pub use coven_core::{
    BlobCacheError, BlobRef, BlobReplacement, BlobScope, BlobStream, BlobTransitionObserver,
    CacheFill, ExternalBlob, MakeRemoteProgress, Provenance, QueuedDelete, QueuedUpload,
    RowBlobAuthority, RowBlobRef,
};
// A host computes a blob's content hash at import and writes it into the row's
// declared hash column — including for a file it registers rather than hands
// over, where the row's hash is what a read validates the file against.
// `ContentHasher` is the same hash fed a chunk at a time, for a file too large
// to hold in memory.
pub use coven_core::blob::{content_hash, ContentHasher};

// Applied-sync change notification.
pub use coven_core::{ChangeOp, RowChange};

// At-rest crypto the host configures (the host sizes cloud stream reads from
// `CHUNK_SIZE`), and the store directory and its host-configurable layout,
// the DB error. `MasterKeyring` is the master-key custody value type — the
// payload `KeyCustody::InMemory` takes and `import_master_key`/
// `initialize_master_key` traffic in internally. `SealError` is what
// `CovenHandle::seal_app_data` / `open_app_data` return.
pub use coven_core::{
    DbError, EncryptionError, MasterKeyring, SealError, StoreDir, StoreLayout, CHUNK_SIZE,
};

// `EncryptionService` is the cipher coven builds internally from whatever
// custody supplies; a production host never constructs one. It stays
// reachable for host integration tests, which build a `CloudCipher` directly
// to drive `CovenHandle::connect_sync_with_test_home`.
#[cfg(any(test, feature = "test-utils"))]
pub use coven_core::EncryptionService;

// The register clock vocabulary carried on every synced row.
pub use coven_core::{Hlc, Timestamp, UpdatedAtStamper};

// Membership. `MembershipCoord` (an author's membership-head coordinate) is
// exposed only because `generate_restore_code` takes a caller-supplied
// membership floor made of them; a host driving that free function directly
// (bypassing `CovenHandle`) must be able to name the type.
pub use circles::{CircleError, Circles};
pub use coven_core::sync::membership::MembershipCoord;
pub use coven_core::sync::store::{
    DeviceJoinAbandonment, DeviceJoinAction, DeviceJoinActivation, DeviceJoinCancellation,
    DeviceJoinCleanupActivation, DeviceJoinCleanupProgress, DeviceJoinCleanupReceipt,
    DeviceJoinError, DeviceJoinJournalDatabase, DeviceJoinJournalRecord, DeviceJoinOffer,
    DeviceJoinProducer, DeviceJoinProducerWriteRevocation, DeviceJoinReadiness, DeviceJoinRole,
    DeviceJoinStatus, DeviceJoinWriteRevocationExecutor, DeviceProviderAccessAdministrator,
    DeviceProviderAccessRequest, DeviceProviderAdmission, DeviceProviderAdmissionApproval,
    DeviceProviderAdmissionCompletion, DeviceProviderReadiness, DeviceRegistrationRequest,
    JoinedStore, JoinerJoinClosure, JoinerJoinTerminal, ProviderAdminJoinClosure,
    ProviderAdminJoinTerminal, ProviderReadyDeviceBootstrap, ProviderWriteAuthorityRef,
    ProvisionalDeviceBootstrap,
};
// The storage-mediated device-join transport: the offer bundle a host encodes
// as its join code, the slot namespace it names, and the two role drivers.
// The three driver functions themselves stay unexported: each takes a `Store`,
// which a host never holds, so naming them here would publish a surface nobody
// outside this crate can call. `CovenHandle`'s `begin_device_invite`,
// `drive_device_join`, `cancel_device_invite`, and `abandon_device_invite` are
// the facade's path to them.
pub use coven_core::sync::store::{
    DeviceJoinApproval, DeviceJoinApprovalPolicy, DeviceJoinDriveOutcome, DeviceJoinOfferBundle,
    DeviceJoinTransportError, DeviceJoinTransportKind, DeviceJoinTransportParams,
    DeviceJoinTransportTiming,
};
pub use coven_core::sync::store_commit::{DeviceJoinAttemptId, DeviceJoinAttemptRef};
pub use coven_core::{
    Audience, Circle, CircleCloseParticipant, CircleCloseSettlement, CircleCloseStatus,
    CircleControlCoord, CircleEpochCloseId, CircleId, CircleMemberInfo, CircleOperationBlock,
    CircleOperationId, CircleOperationInfo, CircleOperationKind, CircleOperationState, CircleRole,
    CircleState, StoreDeviceId,
};
pub use coven_core::{MemberInfo, MemberRole, MembershipConflictChoice, MembershipConflictInfo};

// Clock / id abstractions the host injects, plus the deterministic test fakes.
pub use coven_core::{Clock, ClockRef, IdProvider, IdRef, SystemClock, UuidProvider};
#[cfg(any(test, feature = "test-utils"))]
pub use coven_core::{FixedClock, SequentialIdProvider, SteppingClock};

// Bootstrap decoders.
pub use coven_core::{
    decode_invite_code_info, decode_join_request, decode_restore_code_info, JoinCodeError,
};

// The cloud at-rest cipher. coven resolves it from custody internally; a
// production host never names it. Host integration tests build one directly
// to drive `CovenHandle::connect_sync_with_test_home`.
#[cfg(any(test, feature = "test-utils"))]
pub use coven_core::CloudCipher;

// Cloud provider trait surface a provider implementor needs and its
// thread-safety floor.
pub use coven_core::storage::cloud::{
    write_cloud_object_stream, CloudFileReadError, CloudObjectStream, ExactSlotStorage, ObjectSlot,
    PhysicalObjectLocator,
};
pub use coven_core::sync::provider::{
    CloudKitAcceptedShare, CrossPrincipalProbeReceipt, ExactSlotProbeReceipt,
    ProviderAccessLocator, ProviderAccessWithdrawal, ProviderAdminChange, ProviderAdminGrantId,
    ProviderAdminGrantRecord, ProviderAdminMembershipChange, ProviderAdminState,
    ProviderCapabilityProof, ProviderProbeId,
};
pub use coven_core::sync::storage::{
    AwsPrincipal, GoogleDriveCorpus, ProviderDeviceBinding, ProviderPrincipalId,
    ResolvedProviderBinding, S3EndpointBinding, StoreProviderBinding,
};
pub use coven_core::{
    BlobBody, BoxPartSink, CloudAccessOutcome, CloudAccessState, CloudHome, CloudHomeError,
    CloudHomeJoinInfo, CloudObjectVersion, CloudVersionedObject, PartSink, UploadProgress,
};

// Sync-status surface a host renders from `CovenHandle::subscribe_sync_status`:
// the status enum, its completed-cycle success payload, the per-cycle alert
// bundle, the per-device activity, and the held-changeset detail the alerts carry.
pub use coven_core::{
    AffectedRow, DeviceActivity, HeldStoreCoordinate, HeldStorePosition, HeldStorePositionReason,
    ObjectHash, PendingWrite, PublishedPosition, StoreBatchCommitRef, StoreCommitCoord,
    SyncLoopAlerts, SyncLoopSuccess, WriteBlock, WriteId, WriteReceipt, WriteResolution,
    WriteStatus,
};
pub use sync::sync_loop::SyncLoopStatus;

// In-memory cloud home and durable upload-queue rows for host integration tests.
#[cfg(any(test, feature = "test-utils"))]
pub use coven_core::InMemoryCloudHome;

pub use blob::transition::{MakeLocalError, MakeRemoteError};
pub use custody::{rewrap_passphrase_custody, KeyCustody, Passphrase};
pub use identity_custody::{rewrap_passphrase_identity_custody, IdentityCustody};
pub use join_code::{abandon_join_request, generate_join_request};
pub use keys::{
    keyring_service, set_keyring_service, CloudHomeCredentials, DeviceIdentityCustody,
    IdentityError, KeyError, MasterKeyCustody, MasterKeyError, StoreKeys, UserKeypair,
};

// The sole keyring entry-construction chokepoint (`keys::entry_for`), reached
// across the crate boundary so an integration test can install a specific
// keyring store and assert which construction path it took, without
// re-implementing that dispatch. A production host never calls this.
#[cfg(any(test, feature = "test-utils"))]
pub use keys::entry_for_test;

pub use oauth::{set_oauth_client_creds, OAuthClientCreds, OAuthClientCredsConflict, OAuthTokens};
pub use storage::cloud::setup::generate_restore_code;
pub use storage::cloud::{
    cloudkit::{
        CloudKitAcceptedShareRecord, CloudKitAtomicCreateBatch, CloudKitOps,
        CloudKitProviderIdentity, CloudKitRecordCreate, CloudKitRecordVersion, CloudKitScope,
        CloudKitShare, CloudKitShareAcceptance, CloudKitSharePermission,
    },
    create_cloud_home,
    s3::S3CloudHome,
};
pub use sync::device_join_transport::{
    close_scanned_invite_join, join_with_scanned_invite, DeviceJoinInvite,
    DeviceJoinTransportOutcome,
};
#[cfg(any(test, feature = "test-utils"))]
pub use sync::device_join_transport::{
    close_scanned_invite_join_over_test_home, join_with_scanned_invite_over_test_home,
};
pub use sync::join::{BootstrapError, DeviceJoinClient};
pub use sync::restore::{restore_from_cloud, restore_from_code, RestoreSource};
pub use sync::restore_code::{
    ActivatedContinuation, OwnerRecoveryAuthority, RestoreAuthority, RestoreCode,
};
pub use sync::sync_manager::SyncError;

#[cfg(feature = "oauth-providers")]
pub use oauth::{
    authorize_provider, build_authorize_request_for_provider, exchange_code_for_provider,
    OAuthClientCredsError,
};

// `OAuthError` is compiled wherever the token-refresh path is — which includes a
// test build with no `oauth-providers` feature, since refreshing is not the
// interactive sign-in the feature gates. Its re-export carries the same cfg, so
// the type is reachable from the crate root exactly where it exists; gating it on
// the feature alone left it `pub` but unreachable, which `unreachable_pub` denies.
#[cfg(any(test, feature = "oauth-providers"))]
pub use oauth::OAuthError;

#[cfg(feature = "oauth-providers")]
pub use storage::cloud::setup::{sign_in_dropbox, sign_in_google_drive, sign_in_onedrive};

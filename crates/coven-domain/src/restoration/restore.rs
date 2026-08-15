//! Restore an existing store from cloud storage.
//!
//! Unlike join (which unwraps the encryption key from an invite), restore takes
//! the encryption key directly from the user — present for an opaque home,
//! absent for a browsable one.

use std::sync::Arc;

use tokio::sync::watch;
use tracing::info;

use crate::joining::{build_config, derive_credentials, BootstrapCleanup, BootstrapError};
use coven_database::Migration;
use coven_foundation::config::{Config, HomeStorage};
use coven_foundation::store_dir::StoreLayout;
use coven_keys::custody::KeyCustody;
use coven_keys::encryption::{EncryptionService, MasterKeyring};
use coven_keys::identity_custody::IdentityCustody;
use coven_keys::keys::{StoreKeys, UserKeypair};
use coven_protocol::synced_schema::SyncedTable;
use coven_replication::sync::store::{
    PreparedSnapshotBootstrap, SnapshotBlobReconcile, SnapshotError,
};
use coven_storage::cloud::{CloudHomeJoinInfo, ExactCloudHome};
use coven_storage::oauth::OAuthTokens;
use coven_storage::{BlobPathScheme, CloudCipher, CloudSyncConnection};

/// Cloud provider source for restore: the join info a restore code carries
/// plus the extras it can't (`RestoreCode` omits OAuth tokens because they
/// expire — the user re-authenticates on restore — and holds no live CloudKit
/// driver).
pub struct RestoreSource {
    join_info: CloudHomeJoinInfo,
    exact_upload_verification: coven_foundation::config::ExactUploadVerification,
    oauth_clients: coven_storage::oauth::OAuthClients,
    oauth_tokens: Option<OAuthTokens>,
    cloudkit_ops: Option<Arc<dyn coven_storage::cloud::cloudkit::CloudKitOps>>,
}

impl RestoreSource {
    pub fn new(
        join_info: CloudHomeJoinInfo,
        exact_upload_verification: coven_foundation::config::ExactUploadVerification,
        oauth_clients: coven_storage::oauth::OAuthClients,
        oauth_tokens: Option<OAuthTokens>,
        cloudkit_ops: Option<Arc<dyn coven_storage::cloud::cloudkit::CloudKitOps>>,
    ) -> Self {
        Self {
            join_info,
            exact_upload_verification,
            oauth_clients,
            oauth_tokens,
            cloudkit_ops,
        }
    }

    async fn open_cloud_home(
        &self,
        store_keys: &StoreKeys,
        clock: coven_foundation::clock::ClockRef,
    ) -> Result<Arc<dyn ExactCloudHome>, BootstrapError> {
        use coven_storage::cloud::*;

        let Self {
            join_info,
            exact_upload_verification,
            oauth_clients,
            oauth_tokens,
            cloudkit_ops,
        } = self;

        // Consumed only by the oauth provider arms below.
        #[cfg(not(feature = "oauth-providers"))]
        let _ = (&store_keys, &clock, &oauth_clients, &oauth_tokens);

        #[cfg(feature = "oauth-providers")]
        let require_oauth = |provider_name: &str| {
            let tokens = oauth_tokens.clone().ok_or_else(|| {
                BootstrapError::Provider(format!("{provider_name} restore requires OAuth token"))
            })?;
            store_keys.set_cloud_home_oauth_tokens(&tokens)?;
            Ok::<_, BootstrapError>(tokens)
        };

        #[cfg(feature = "oauth-providers")]
        let credential_custody =
            coven_keys::keys::CloudHomeCredentialsOwner::new(store_keys.clone()).current();

        let home: Arc<dyn ExactCloudHome> = match join_info {
            CloudHomeJoinInfo::S3 {
                bucket,
                region,
                endpoint,
                access_key,
                secret_key,
                key_prefix,
            } => Arc::new(
                s3::open_cloud_home(
                    bucket.clone(),
                    region.clone(),
                    endpoint.clone(),
                    access_key.clone(),
                    secret_key.clone(),
                    key_prefix.clone(),
                    *exact_upload_verification,
                )
                .await?,
            ),

            CloudHomeJoinInfo::CloudKit => {
                let ops = cloudkit_ops.clone().ok_or_else(|| {
                    BootstrapError::Provider("CloudKit driver not provided".to_string())
                })?;
                Arc::new(cloudkit::CloudKitCloudHome::new_private(
                    ops,
                    *exact_upload_verification,
                ))
            }

            // Restore recovers your own zone, never one shared to you;
            // `decode_restore_code` already rejects this for the code path, but
            // `RestoreSource` is public API another caller could construct
            // directly, so this guard is independent of that decode-time check.
            CloudHomeJoinInfo::CloudKitShare { .. } => {
                return Err(BootstrapError::Provider(
                "restoring from a CloudKit share is not supported — restore recovers your own zone, not a shared one".to_string(),
            ));
            }

            #[cfg(feature = "oauth-providers")]
            CloudHomeJoinInfo::GoogleDrive { folder_id } => {
                let tokens = require_oauth("Google Drive")?;
                let oauth_config = oauth_clients
                    .config_for(coven_foundation::config::CloudProvider::GoogleDrive)?;
                let session = oauth_session::OAuthSession::new(
                    tokens,
                    credential_custody.clone(),
                    clock,
                    oauth_config,
                    "Google Drive",
                );
                Arc::new(google_drive::GoogleDriveCloudHome::new(
                    folder_id.clone(),
                    session,
                    *exact_upload_verification,
                ))
            }

            #[cfg(feature = "oauth-providers")]
            CloudHomeJoinInfo::Dropbox { folder_path } => {
                let tokens = require_oauth("Dropbox")?;
                let oauth_config =
                    oauth_clients.config_for(coven_foundation::config::CloudProvider::Dropbox)?;
                let session = oauth_session::OAuthSession::new(
                    tokens,
                    credential_custody.clone(),
                    clock,
                    oauth_config,
                    "Dropbox",
                );
                Arc::new(dropbox::DropboxCloudHome::new(
                    folder_path.clone(),
                    session,
                    *exact_upload_verification,
                ))
            }

            #[cfg(feature = "oauth-providers")]
            CloudHomeJoinInfo::OneDrive {
                drive_id,
                folder_id,
            } => {
                let tokens = require_oauth("OneDrive")?;
                let oauth_config =
                    oauth_clients.config_for(coven_foundation::config::CloudProvider::OneDrive)?;
                let session = oauth_session::OAuthSession::new(
                    tokens,
                    credential_custody,
                    clock,
                    oauth_config,
                    "OneDrive",
                );
                Arc::new(onedrive::OneDriveCloudHome::new(
                    drive_id.clone(),
                    folder_id.clone(),
                    session,
                    *exact_upload_verification,
                ))
            }

            #[cfg(not(feature = "oauth-providers"))]
            CloudHomeJoinInfo::GoogleDrive { .. }
            | CloudHomeJoinInfo::Dropbox { .. }
            | CloudHomeJoinInfo::OneDrive { .. } => {
                return Err(BootstrapError::Provider(
                    "OAuth cloud providers are not supported in this build".to_string(),
                ));
            }
        };

        Ok(home)
    }
}

/// Restore a store from cloud storage.
///
/// Validates inputs, constructs the cloud home from the source, runs the sync
/// protocol, and sets the store as active. `keypair` is the restored device's
/// signing identity (recovered from the restore code); the storage signs the
/// control objects it writes with it, and it is the same key the caller imports
/// once restore succeeds.
#[allow(clippy::too_many_arguments)]
pub async fn restore_from_cloud(
    store_id: &str,
    store_root: coven_protocol::store_commit::StoreRootRef,
    serialized_keyring: Option<&str>,
    store_name: &str,
    synced_tables: &[SyncedTable],
    migrations: &[Migration],
    key_custody: KeyCustody,
    identity_custody: IdentityCustody,
    source: RestoreSource,
    membership_floor: &coven_protocol::membership::MembershipFloor,
    keypair: &UserKeypair,
    authority: &coven_protocol::recovery::RestoreAuthority,
    continuation_device_signer: Option<&UserKeypair>,
    layout: &StoreLayout,
    clock: coven_foundation::clock::ClockRef,
    ids: coven_foundation::id_provider::IdRef,
    on_status: impl Fn(&str),
    cancel: &watch::Receiver<bool>,
) -> Result<Config, BootstrapError> {
    // Guard the destructive `stores/<id>` create/delete against any direct
    // caller, independent of the decode-time check on untrusted input.
    coven_foundation::store_dir::validate_path_token(store_id)?;
    coven_storage::cloud::setup::require_exact_slot_capabilities_join_info(
        &source.join_info,
        source.exact_upload_verification,
    )
    .map_err(|provider| BootstrapError::ExactSlotsUnavailable { provider })?;
    let exact_upload_verification = source.exact_upload_verification;

    let store_dir = layout.store_dir(store_id);

    // Hoisted here, before any durable write below, so a failure at any step —
    // including `RestoreSource::open_cloud_home`'s OAuth persist, which runs before the store
    // directory is created — funnels through the same rollback instead of a
    // bare `?` escaping it.
    let store_keys = StoreKeys::bind(store_id.to_string());
    let custody = key_custody.resolve(&store_keys, &store_dir);
    let identity_custody = identity_custody.resolve(&store_keys, &store_dir);

    // Refuse a *completed* store (config present) and clear a torn one before
    // any provider side effect. The decode guaranteed the id is a safe single
    // component, so the directory is a direct child of the layout's stores dir
    // and cannot escape it. Re-running a restore for a store you already have
    // adds nothing — the existing store is the data — and letting it proceed
    // would, on any bootstrap failure below, delete that store's database and
    // blobs during cleanup. Dispatching here makes the failure-cleanup only
    // ever remove a directory this invocation created.
    let cleanup = BootstrapCleanup::new(
        &store_dir,
        &store_keys,
        custody.as_ref(),
        identity_custody.as_ref(),
    );
    cleanup.refuse_completed_or_clear(store_id)?;

    let result = async {
        on_status("Preparing restore...");

        // The key's presence is the home's storage mode: a key present ⇒ an
        // opaque home (encrypted, obfuscated blob paths); a key absent ⇒ a
        // browsable home (plaintext, readable blob paths). The cipher and the
        // blob-path scheme both follow from it, so this device computes the
        // same blob keys the source wrote. Parsed once here so the cipher and
        // the persisted master key always agree on the same value.
        let storage = if serialized_keyring.is_some() {
            HomeStorage::Opaque
        } else {
            HomeStorage::Browsable
        };
        let master_key: Option<MasterKeyring> = match serialized_keyring {
            Some(serialized_keyring) => {
                on_status("Verifying encryption key...");
                Some(MasterKeyring::from_serialized(serialized_keyring)?)
            }
            None => None,
        };
        let cipher = match &master_key {
            Some(keyring) => CloudCipher::Encrypted(keyring.clone().into()),
            None => CloudCipher::Plaintext,
        };

        let blob_paths = BlobPathScheme::for_storage(storage);

        let cloud_home = source.open_cloud_home(&store_keys, clock.clone()).await?;
        let join_info = &source.join_info;

        let storage: Arc<dyn coven_storage::CloudSyncObjectStorage> =
            Arc::new(CloudSyncConnection::new(
                cloud_home,
                cipher.clone(),
                blob_paths,
                store_id.to_string(),
                keypair.clone(),
            ));

        // Create the store directory under `stores/` (its non-existence was
        // checked up front, so this create and the failure-cleanup below own
        // it entirely).
        let device_id = match authority {
            coven_protocol::recovery::RestoreAuthority::ActivatedContinuation(continuation) => {
                continuation.registration.device_id.to_string()
            }
            coven_protocol::recovery::RestoreAuthority::OwnerRecovery(_) => ids.new_id(),
        };
        store_dir.ensure_created()?;

        let continuation = match (authority, continuation_device_signer) {
            (
                coven_protocol::recovery::RestoreAuthority::ActivatedContinuation(continuation),
                Some(_),
            ) => Some(continuation),
            (coven_protocol::recovery::RestoreAuthority::ActivatedContinuation(_), None) => {
                return Err(crate::joining::SigningKeyError::MissingContinuationSigner.into());
            }
            (coven_protocol::recovery::RestoreAuthority::OwnerRecovery(_), None) => None,
            (coven_protocol::recovery::RestoreAuthority::OwnerRecovery(_), Some(_)) => {
                return Err(crate::joining::SigningKeyError::UnexpectedOwnerRecoverySigner.into());
            }
        };

        // Cancellation is checked between phases, never during a download or
        // durable write. Every cancellation still exits through this restore's
        // failure cleanup.
        if *cancel.borrow() {
            return Err(BootstrapError::Cancelled);
        }
        on_status("Downloading store snapshot...");
        let history_verifier =
            coven_replication::sync::store::HistoryConstructionAuthority::for_snapshot()
                .open_pinned(storage.as_ref(), &store_root)
                .await
                .map_err(SnapshotError::from)?;
        let bootstrap = PreparedSnapshotBootstrap::prepare(
            &storage,
            history_verifier,
            membership_floor,
            coven_database::supported_version(migrations),
            &store_dir.db_path(),
            keypair,
        )
        .await?;

        info!(
            "Bootstrapped from snapshot ({} device coverage entries)",
            bootstrap.coverage_count()
        );

        if *cancel.borrow() {
            return Err(BootstrapError::Cancelled);
        }
        on_status("Applying recent changes...");
        let routing_encryption = master_key
            .as_ref()
            .map(|keyring| EncryptionService::from(keyring.clone()));
        let mut store = bootstrap
            .install(
                &store_dir,
                synced_tables.to_vec(),
                coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
                coven_protocol::blob::TransferLimits::one_at_a_time(),
                device_id.clone(),
                clock.clone(),
                migrations,
                routing_encryption.as_ref(),
            )
            .await?;

        match store.reconcile_snapshot_blobs(cancel).await? {
            SnapshotBlobReconcile::Complete => {}
            SnapshotBlobReconcile::Cancelled => return Err(BootstrapError::Cancelled),
        }

        if *cancel.borrow() {
            return Err(BootstrapError::Cancelled);
        }
        let pull_result = store.pull(routing_encryption.as_ref()).await?;

        if let Some(continuation) = continuation {
            store
                .install_activated_device_continuation(continuation.clone())
                .await?;
        }
        if let coven_protocol::recovery::RestoreAuthority::OwnerRecovery(recovery) = authority {
            store.recover_owner_device(recovery).await?;
        }

        if pull_result.changesets_applied > 0 {
            info!(
                "Applied {} changesets since snapshot",
                pull_result.changesets_applied
            );
        }

        if let Some(keyring) = &master_key {
            custody.persist(keyring)?;
        }
        if let Some(credentials) = derive_credentials(join_info) {
            store_keys.set_cloud_home_credentials(&credentials)?;
        }
        identity_custody.establish(keypair)?;

        // The config is the completion marker, so report this phase after all
        // other durable local state is present and immediately before saving it.
        on_status("Saving configuration...");
        let mut config = build_config(store_id, &device_id, store_name, join_info, &cipher);
        config.cloud_home.exact_upload_verification = exact_upload_verification;
        config.save_to_config_yaml(&store_dir)?;
        Ok(config)
    }
    .await;

    match result {
        Ok(config) => {
            // The host records this as the active store after this returns.
            info!("Cloud restore complete: store at {}", store_dir.display());
            Ok(config)
        }
        Err(err) => Err(cleanup.after_failure(err)),
    }
}

/// Restore a store from a restore code string.
///
/// Decodes the restore code, fills a `RestoreSource` from its join info plus
/// the caller-supplied OAuth tokens and CloudKit driver, imports the signing
/// key, and delegates to `restore_from_cloud`.
#[allow(clippy::too_many_arguments)]
pub async fn restore_from_code(
    code: &str,
    synced_tables: &[SyncedTable],
    migrations: &[Migration],
    exact_upload_verification: coven_foundation::config::ExactUploadVerification,
    key_custody: KeyCustody,
    identity_custody: IdentityCustody,
    oauth_clients: coven_storage::oauth::OAuthClients,
    oauth_tokens: Option<coven_storage::oauth::OAuthTokens>,
    cloudkit_ops: Option<Arc<dyn coven_storage::cloud::cloudkit::CloudKitOps>>,
    layout: &StoreLayout,
    clock: coven_foundation::clock::ClockRef,
    ids: coven_foundation::id_provider::IdRef,
    on_status: impl Fn(&str),
    cancel: &watch::Receiver<bool>,
) -> Result<Config, BootstrapError> {
    let parsed = super::code::decode_restore_code(code)?;
    coven_storage::cloud::setup::require_exact_slot_capabilities_join_info(
        &parsed.provider,
        exact_upload_verification,
    )
    .map_err(|provider| BootstrapError::ExactSlotsUnavailable { provider })?;
    // `decode_restore_code` already validated the field; rebuild this store's
    // restored signing identity from it. The storage signs its control objects
    // with this keypair during restore, and `restore_from_cloud` imports it into
    // custody just before saving the config.
    let identity_secret = match &parsed.authority {
        coven_protocol::recovery::RestoreAuthority::ActivatedContinuation(continuation) => {
            &continuation.identity_signing_secret
        }
        coven_protocol::recovery::RestoreAuthority::OwnerRecovery(recovery) => {
            &recovery.owner_identity_secret
        }
    };
    let signing_key: [u8; coven_keys::keys::SIGN_SECRETKEYBYTES] =
        coven_foundation::code_envelope::decode_fixed_hex(
            "identity signing key",
            identity_secret,
            coven_keys::keys::SIGN_SECRETKEYBYTES,
        )
        .map_err(crate::joining::SigningKeyError::from)?
        .try_into()
        .expect("decode_fixed_hex returned the requested signing-key length");
    let keypair = UserKeypair::from_signing_key_bytes(&signing_key).map_err(BootstrapError::Key)?;
    let continuation_device_signer = match &parsed.authority {
        coven_protocol::recovery::RestoreAuthority::ActivatedContinuation(continuation) => {
            let bytes: [u8; coven_keys::keys::SIGN_SECRETKEYBYTES] =
                coven_foundation::code_envelope::decode_fixed_hex(
                    "device signing key",
                    &continuation.device_signing_secret,
                    coven_keys::keys::SIGN_SECRETKEYBYTES,
                )
                .map_err(crate::joining::SigningKeyError::from)?
                .try_into()
                .expect("decode_fixed_hex returned the requested signing-key length");
            Some(UserKeypair::from_signing_key_bytes(&bytes).map_err(BootstrapError::Key)?)
        }
        coven_protocol::recovery::RestoreAuthority::OwnerRecovery(_) => None,
    };

    // `parsed.provider` is already the shared `CloudHomeJoinInfo`; restore matches
    // on it and pulls in these extras, so there's
    // no per-provider conversion left to do here.
    let source = RestoreSource::new(
        parsed.provider.clone(),
        exact_upload_verification,
        oauth_clients,
        oauth_tokens,
        cloudkit_ops,
    );

    // `restore_from_cloud` imports this store's signing identity as the step
    // before it saves the config, so a saved config always has its identity in
    // custody. Nothing identity-related is left for this caller to do.
    Box::pin(restore_from_cloud(
        &parsed.sid,
        parsed.store_root,
        parsed.ek.as_deref(),
        &parsed.name,
        synced_tables,
        migrations,
        key_custody,
        identity_custody,
        source,
        &parsed.membership_floor,
        &keypair,
        &parsed.authority,
        continuation_device_signer.as_ref(),
        layout,
        clock,
        ids,
        on_status,
        cancel,
    ))
    .await
}

// The only test here exercises the OAuth-provider arms of `RestoreSource::open_cloud_home`,
// which only exist under this feature; the module (not just the test fn) is
// gated so its imports aren't unused in a build without the feature.
#[cfg(all(test, feature = "oauth-providers"))]
mod tests {
    use super::*;

    /// Restore's provider opening must save the caller-supplied
    /// OAuth tokens to the store-scoped keyring, the same way join's parallel
    /// arms already do. Launch-time home construction reads them back through
    /// `StoreKeys` and errors when they're
    /// absent, so a store restored over an OAuth provider must be able to build
    /// its cloud home again on the next launch. Dropbox is the smallest OAuth
    /// arm, so it stands in for Google Drive and OneDrive here.
    #[tokio::test]
    async fn restore_dropbox_open_cloud_home_persists_oauth_tokens() {
        coven_keys::keys::test_keyring::install();

        let store_id = "restore-dropbox-persist-test";
        let tokens = OAuthTokens {
            access_token: "access-token".to_string(),
            refresh_token: Some("refresh-token".to_string()),
            expires_at: None,
        };
        let source = RestoreSource::new(
            CloudHomeJoinInfo::Dropbox {
                folder_path: "/Apps/coven/my-store".to_string(),
            },
            coven_foundation::config::ExactUploadVerification::MetadataHash,
            coven_storage::oauth::OAuthClients::for_tests(),
            Some(tokens.clone()),
            None,
        );

        let store_keys = StoreKeys::bind(store_id.to_string());
        source
            .open_cloud_home(&store_keys, Arc::new(coven_foundation::clock::SystemClock))
            .await
            .expect("build restore cloud home for Dropbox");

        let stored = StoreKeys::bind(store_id.to_string())
            .get_cloud_home_oauth_tokens()
            .expect("read cloud home credentials")
            .expect("restore must persist OAuth tokens to the keyring");
        assert_eq!(stored.access_token, tokens.access_token);
        assert_eq!(stored.refresh_token, tokens.refresh_token);
    }
}

// These exercise provider-opening arms that don't need the OAuth
// providers (S3, CloudKit), so unlike the module above they run regardless of
// the `oauth-providers` feature.
#[cfg(test)]
mod open_cloud_home_tests {
    use super::*;

    /// Opening an S3 home must preserve the restore source's key prefix because
    /// that same join information becomes `Config.cloud_home.s3_key_prefix`.
    #[tokio::test]
    async fn open_cloud_home_s3_preserves_key_prefix() {
        let source = RestoreSource::new(
            CloudHomeJoinInfo::S3 {
                bucket: "b".to_string(),
                region: "us-east-1".to_string(),
                endpoint: None,
                access_key: "ak".to_string(),
                secret_key: "sk".to_string(),
                key_prefix: Some("prefix/".to_string()),
            },
            coven_foundation::config::ExactUploadVerification::MetadataHash,
            coven_storage::oauth::OAuthClients::empty(),
            None,
            None,
        );

        let store_keys = StoreKeys::bind("store-id".to_string());
        source
            .open_cloud_home(&store_keys, Arc::new(coven_foundation::clock::SystemClock))
            .await
            .expect("build S3 cloud home");

        match &source.join_info {
            CloudHomeJoinInfo::S3 { key_prefix, .. } => {
                assert_eq!(key_prefix, &Some("prefix/".to_string()));
            }
            other => panic!("expected S3 join info, got {other:?}"),
        }
    }

    /// `RestoreSource` is public API a caller can construct directly, bypassing
    /// `decode_restore_code`'s rejection of `CloudKitShare`. Provider opening
    /// must refuse it on its own: restore recovers your own zone, never one
    /// shared to you.
    #[tokio::test]
    async fn open_cloud_home_rejects_cloudkit_share() {
        let source = RestoreSource::new(
            CloudHomeJoinInfo::CloudKitShare {
                share_url: "https://share.example".to_string(),
                owner_name: "owner".to_string(),
                zone_name: "zone".to_string(),
            },
            coven_foundation::config::ExactUploadVerification::MetadataHash,
            coven_storage::oauth::OAuthClients::empty(),
            None,
            None,
        );

        let store_keys = StoreKeys::bind("store-id".to_string());
        let result = source
            .open_cloud_home(&store_keys, Arc::new(coven_foundation::clock::SystemClock))
            .await;

        match result {
            Err(BootstrapError::Provider(_)) => {}
            Ok(_) => panic!("expected a Provider error rejecting the CloudKit share, got Ok"),
            Err(other) => {
                panic!("expected a Provider error rejecting the CloudKit share, got {other:?}")
            }
        }
    }
}

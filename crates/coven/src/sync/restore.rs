//! Restore an existing store from cloud storage.
//!
//! Unlike join (which unwraps the encryption key from an invite), restore takes
//! the encryption key directly from the user — present for an opaque home,
//! absent for a browsable one.

use std::sync::Arc;

use tokio::sync::watch;
use tracing::info;

use crate::config::{Config, HomeStorage};
use crate::custody::KeyCustody;
use crate::encryption::MasterKeyring;
use crate::identity_custody::IdentityCustody;
use crate::keys::{DeviceIdentityCustody, MasterKeyCustody, StoreKeys, UserKeypair};
use crate::migration::Migration;
use crate::oauth::OAuthTokens;
use crate::storage::cloud::{CloudHome, CloudHomeJoinInfo};
use crate::store_dir::StoreLayout;
use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
use crate::sync::join::{
    bootstrap_and_save_store, cleanup_after_bootstrap_failure, BootstrapError,
};
use crate::sync::membership::MembershipCoord;
use crate::sync::session::SyncedTable;

/// Cloud provider source for restore: the join info a restore code carries
/// plus the extras it can't (`RestoreCode` omits OAuth tokens because they
/// expire — the user re-authenticates on restore — and holds no live CloudKit
/// driver).
pub struct RestoreSource {
    pub join_info: CloudHomeJoinInfo,
    pub oauth_tokens: Option<OAuthTokens>,
    pub cloudkit_ops: Option<Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>>,
}

/// Require OAuth tokens for a provider that needs them and persist them to the
/// store-scoped keyring, the same way join's parallel arms do, so the next
/// launch's home construction (`parse_oauth_tokens` in `storage::cloud`) can
/// read them back instead of erroring on their absence.
#[cfg(feature = "oauth-providers")]
fn require_and_persist_oauth(
    oauth_tokens: Option<OAuthTokens>,
    store_id: &str,
    provider_name: &str,
) -> Result<(OAuthTokens, StoreKeys), BootstrapError> {
    let tokens = oauth_tokens.ok_or_else(|| {
        BootstrapError::Provider(format!("{provider_name} restore requires OAuth token"))
    })?;
    let ks = StoreKeys::new(store_id.to_string());
    crate::sync::join::persist_oauth_tokens(&ks, &tokens)?;
    Ok((tokens, ks))
}

/// Build a `(CloudHomeJoinInfo, Box<dyn CloudHome>)` from a `RestoreSource`.
async fn build_cloud_home(
    source: RestoreSource,
    store_id: &str,
    clock: crate::clock::ClockRef,
) -> Result<(CloudHomeJoinInfo, Box<dyn CloudHome>), BootstrapError> {
    use crate::storage::cloud::*;

    let RestoreSource {
        join_info,
        oauth_tokens,
        cloudkit_ops,
    } = source;

    // Consumed only by the oauth provider arms below.
    #[cfg(not(feature = "oauth-providers"))]
    let _ = (store_id, &clock, &oauth_tokens);

    let home: Box<dyn CloudHome> = match &join_info {
        CloudHomeJoinInfo::S3 {
            bucket,
            region,
            endpoint,
            access_key,
            secret_key,
            key_prefix,
        } => Box::new(
            s3::S3CloudHome::new(
                bucket.clone(),
                region.clone(),
                endpoint.clone(),
                access_key.clone(),
                secret_key.clone(),
                key_prefix.clone(),
            )
            .await?,
        ),

        CloudHomeJoinInfo::CloudKit => {
            let ops = cloudkit_ops.ok_or_else(|| {
                BootstrapError::Provider("CloudKit driver not provided".to_string())
            })?;
            Box::new(cloudkit::CloudKitCloudHome::new_private(ops)) as Box<dyn CloudHome>
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
            let (tokens, ks) = require_and_persist_oauth(oauth_tokens, store_id, "Google Drive")?;
            Box::new(google_drive::GoogleDriveCloudHome::new(
                folder_id.clone(),
                tokens,
                ks,
                clock,
            )?) as Box<dyn CloudHome>
        }

        #[cfg(feature = "oauth-providers")]
        CloudHomeJoinInfo::Dropbox { folder_path } => {
            let (tokens, ks) = require_and_persist_oauth(oauth_tokens, store_id, "Dropbox")?;
            Box::new(dropbox::DropboxCloudHome::new(
                folder_path.clone(),
                tokens,
                ks,
                clock,
            )?) as Box<dyn CloudHome>
        }

        #[cfg(feature = "oauth-providers")]
        CloudHomeJoinInfo::OneDrive {
            drive_id,
            folder_id,
        } => {
            let (tokens, ks) = require_and_persist_oauth(oauth_tokens, store_id, "OneDrive")?;
            Box::new(onedrive::OneDriveCloudHome::new(
                drive_id.clone(),
                folder_id.clone(),
                tokens,
                ks,
                clock,
            )?) as Box<dyn CloudHome>
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

    Ok((join_info, home))
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
    genesis_hash: crate::sync::store_commit::ObjectHash,
    founder_pubkey: &str,
    encryption_key_hex: Option<&str>,
    store_name: &str,
    synced_tables: &[SyncedTable],
    migrations: &[Migration],
    custody: Arc<dyn MasterKeyCustody>,
    identity_custody: Arc<dyn DeviceIdentityCustody>,
    source: RestoreSource,
    membership_floor: &[MembershipCoord],
    keypair: &UserKeypair,
    layout: &StoreLayout,
    clock: crate::clock::ClockRef,
    ids: crate::id_provider::IdRef,
    on_status: impl Fn(&str),
    cancel: &watch::Receiver<bool>,
) -> Result<Config, BootstrapError> {
    crate::install_platform();

    // Guard the destructive `stores/<id>` create/delete against any direct
    // caller, independent of the decode-time check on untrusted input.
    crate::store_dir::validate_path_token(store_id)
        .map_err(|e| BootstrapError::InvalidCode(format!("invalid store id: {e}")))?;

    let store_dir = layout.store_dir(store_id);

    // Hoisted here, before any durable write below, so a failure at any step —
    // including `build_cloud_home`'s OAuth persist, which runs before the store
    // directory is created — funnels through the same rollback instead of a
    // bare `?` escaping it.
    let store_keys = StoreKeys::new(store_id.to_string());

    // Refuse a *completed* store (config present) and clear a torn one before
    // any provider side effect. The decode guaranteed the id is a safe single
    // component, so the directory is a direct child of the layout's stores dir
    // and cannot escape it. Re-running a restore for a store you already have
    // adds nothing — the existing store is the data — and letting it proceed
    // would, on any bootstrap failure below, delete that store's database and
    // blobs during cleanup. Dispatching here makes the failure-cleanup only
    // ever remove a directory this invocation created.
    crate::sync::join::refuse_completed_or_clear_torn_store(
        &store_dir,
        &store_keys,
        custody.as_ref(),
        identity_custody.as_ref(),
        store_id,
    )?;

    let result = async {
        on_status("Preparing restore...");

        // The key's presence is the home's storage mode: a key present ⇒ an
        // opaque home (encrypted, obfuscated blob paths); a key absent ⇒ a
        // browsable home (plaintext, readable blob paths). The cipher and the
        // blob-path scheme both follow from it, so this device computes the
        // same blob keys the source wrote. Parsed once here (not re-parsed
        // inside `bootstrap_and_save_store`) so the cipher and the persisted
        // master key always agree on the same value.
        let storage = if encryption_key_hex.is_some() {
            HomeStorage::Opaque
        } else {
            HomeStorage::Browsable
        };
        let master_key: Option<MasterKeyring> = match encryption_key_hex {
            Some(key_hex) => {
                on_status("Verifying encryption key...");
                Some(MasterKeyring::from_serialized(key_hex)?)
            }
            None => None,
        };
        let cipher = match &master_key {
            Some(keyring) => CloudCipher::Encrypted(keyring.clone().into()),
            None => CloudCipher::Plaintext,
        };

        let blob_paths = BlobPathScheme::for_storage(storage);

        let (join_info, cloud_home) = build_cloud_home(source, store_id, clock.clone()).await?;

        let storage = CloudSyncStorage::new(
            std::sync::Arc::from(cloud_home),
            cipher.clone(),
            blob_paths,
            store_id.to_string(),
            keypair.clone(),
        )
        .with_copy_ids(std::sync::Arc::new(
            crate::storage::cloud::RandomCopyIdGenerator,
        ));

        // Create the store directory under `stores/` (its non-existence was
        // checked up front, so this create and the failure-cleanup below own
        // it entirely).
        let device_id = ids.new_id();
        std::fs::create_dir_all(&*store_dir)?;

        bootstrap_and_save_store(
            &storage,
            &cipher,
            master_key.as_ref(),
            &store_dir,
            store_id,
            &device_id,
            genesis_hash,
            crate::sync::join::BootstrapContext::Restore {
                founder_pubkey,
                keypair,
            },
            membership_floor,
            synced_tables,
            migrations,
            &join_info,
            store_name,
            &store_keys,
            custody.as_ref(),
            identity_custody.as_ref(),
            &on_status,
            cancel,
        )
        .await
    }
    .await;

    match result {
        Ok(config) => {
            // The host records this as the active store after this returns.
            info!(
                "Cloud restore complete: store at {}",
                config.store_dir.display()
            );
            Ok(config)
        }
        Err(err) => Err(cleanup_after_bootstrap_failure(
            &store_dir,
            &store_keys,
            custody.as_ref(),
            identity_custody.as_ref(),
            err,
        )),
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
    key_custody: KeyCustody,
    identity_custody: IdentityCustody,
    oauth_tokens: Option<crate::oauth::OAuthTokens>,
    cloudkit_ops: Option<Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>>,
    layout: &StoreLayout,
    clock: crate::clock::ClockRef,
    ids: crate::id_provider::IdRef,
    on_status: impl Fn(&str),
    cancel: &watch::Receiver<bool>,
) -> Result<Config, BootstrapError> {
    use crate::sync::restore_code;

    crate::install_platform();

    let parsed = restore_code::decode_restore_code(code)
        .map_err(|e| BootstrapError::InvalidCode(e.to_string()))?;
    let custody = key_custody.resolve(&parsed.sid, &layout.store_dir(&parsed.sid));
    let identity_custody = identity_custody.resolve(&parsed.sid, &layout.store_dir(&parsed.sid));

    // `decode_restore_code` already validated the field; rebuild this store's
    // restored signing identity from it. The storage signs its control objects
    // with this keypair during restore, and `restore_from_cloud` imports it into
    // custody just before saving the config.
    let signing_key: [u8; crate::keys::SIGN_SECRETKEYBYTES] = hex::decode(&parsed.sk)
        .map_err(|e| BootstrapError::InvalidSigningKey(format!("invalid encoding: {e}")))?
        .try_into()
        .map_err(|_| {
            BootstrapError::InvalidSigningKey(format!(
                "Signing key must be {} bytes",
                crate::keys::SIGN_SECRETKEYBYTES
            ))
        })?;
    let keypair = UserKeypair::from_signing_key_bytes(&signing_key).map_err(BootstrapError::Key)?;

    // `parsed.provider` is already the shared `CloudHomeJoinInfo`; `build_cloud_home`
    // (via `restore_from_cloud`) matches on it and pulls in these extras, so there's
    // no per-provider conversion left to do here.
    let source = RestoreSource {
        join_info: parsed.provider,
        oauth_tokens,
        cloudkit_ops,
    };

    // `restore_from_cloud` imports this store's signing identity as the step
    // before it saves the config, so a saved config always has its identity in
    // custody. Nothing identity-related is left for this caller to do.
    restore_from_cloud(
        &parsed.sid,
        parsed.genesis_hash,
        &parsed.founder_pubkey,
        parsed.ek.as_deref(),
        &parsed.name,
        synced_tables,
        migrations,
        custody.clone(),
        identity_custody.clone(),
        source,
        &parsed.membership_floor,
        &keypair,
        layout,
        clock,
        ids,
        on_status,
        cancel,
    )
    .await
}

// The only test here exercises the OAuth-provider arms of `build_cloud_home`,
// which only exist under this feature; the module (not just the test fn) is
// gated so its imports aren't unused in a build without the feature.
#[cfg(all(test, feature = "oauth-providers"))]
mod tests {
    use super::*;
    use crate::keys::CloudHomeCredentials;

    /// Restore's per-provider `build_cloud_home` must save the caller-supplied
    /// OAuth tokens to the store-scoped keyring, the same way join's parallel
    /// arms already do. Launch-time home construction (`parse_oauth_tokens` in
    /// `storage::cloud`) reads them back from there and errors when they're
    /// absent, so a store restored over an OAuth provider must be able to build
    /// its cloud home again on the next launch. Dropbox is the smallest OAuth
    /// arm, so it stands in for Google Drive and OneDrive here.
    #[tokio::test]
    async fn restore_dropbox_build_cloud_home_persists_oauth_tokens() {
        crate::keys::test_keyring::install();
        crate::oauth::install_test_client_creds();

        let store_id = "restore-dropbox-persist-test";
        let tokens = OAuthTokens {
            access_token: "access-token".to_string(),
            refresh_token: Some("refresh-token".to_string()),
            expires_at: None,
        };
        let source = RestoreSource {
            join_info: CloudHomeJoinInfo::Dropbox {
                folder_path: "/Apps/coven/my-store".to_string(),
            },
            oauth_tokens: Some(tokens.clone()),
            cloudkit_ops: None,
        };

        build_cloud_home(source, store_id, Arc::new(crate::clock::SystemClock))
            .await
            .expect("build restore cloud home for Dropbox");

        let stored = StoreKeys::new(store_id.to_string())
            .get_cloud_home_credentials()
            .expect("read cloud home credentials")
            .expect("restore must persist OAuth tokens to the keyring");
        match stored {
            CloudHomeCredentials::OAuth { token_json } => {
                let stored_tokens: OAuthTokens =
                    serde_json::from_str(&token_json).expect("stored OAuth tokens deserialize");
                assert_eq!(stored_tokens.access_token, tokens.access_token);
                assert_eq!(stored_tokens.refresh_token, tokens.refresh_token);
            }
            other => panic!("expected OAuth credentials, got {other:?}"),
        }
    }
}

// These exercise arms of `build_cloud_home` that don't need the OAuth
// providers (S3, CloudKit), so unlike the module above they run regardless of
// the `oauth-providers` feature.
#[cfg(test)]
mod build_cloud_home_tests {
    use super::*;

    /// `RestoreSource.join_info` now carries `CloudHomeJoinInfo::S3` directly —
    /// there's no more `RestoreSource::S3` hop that dropped `key_prefix` on the
    /// floor (it built the `CloudHomeJoinInfo` returned to the caller with
    /// `key_prefix: None` unconditionally). `build_cloud_home` must carry the
    /// restore code's `key_prefix` through to the join info it returns, which
    /// becomes `Config.cloud_home.s3_key_prefix` — otherwise every restore of
    /// an S3 home configured with a key prefix loses it.
    #[tokio::test]
    async fn build_cloud_home_s3_preserves_key_prefix() {
        let source = RestoreSource {
            join_info: CloudHomeJoinInfo::S3 {
                bucket: "b".to_string(),
                region: "us-east-1".to_string(),
                endpoint: None,
                access_key: "ak".to_string(),
                secret_key: "sk".to_string(),
                key_prefix: Some("prefix/".to_string()),
            },
            oauth_tokens: None,
            cloudkit_ops: None,
        };

        let (returned_info, _home) =
            build_cloud_home(source, "store-id", Arc::new(crate::clock::SystemClock))
                .await
                .expect("build S3 cloud home");

        match returned_info {
            CloudHomeJoinInfo::S3 { key_prefix, .. } => {
                assert_eq!(key_prefix, Some("prefix/".to_string()));
            }
            other => panic!("expected S3 join info, got {other:?}"),
        }
    }

    /// `RestoreSource` is public API a caller can construct directly, bypassing
    /// `decode_restore_code`'s rejection of `CloudKitShare`. `build_cloud_home`
    /// must refuse it on its own: restore recovers your own zone, never one
    /// shared to you.
    #[tokio::test]
    async fn build_cloud_home_rejects_cloudkit_share() {
        let source = RestoreSource {
            join_info: CloudHomeJoinInfo::CloudKitShare {
                share_url: "https://share.example".to_string(),
                owner_name: "owner".to_string(),
                zone_name: "zone".to_string(),
            },
            oauth_tokens: None,
            cloudkit_ops: None,
        };

        let result =
            build_cloud_home(source, "store-id", Arc::new(crate::clock::SystemClock)).await;

        match result {
            Err(BootstrapError::Provider(_)) => {}
            Ok(_) => panic!("expected a Provider error rejecting the CloudKit share, got Ok"),
            Err(other) => {
                panic!("expected a Provider error rejecting the CloudKit share, got {other:?}")
            }
        }
    }
}

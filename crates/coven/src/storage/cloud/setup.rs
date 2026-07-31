//! Cloud provider setup and management.
//!
//! Contains the OAuth sign-in flows for Google Drive, Dropbox, and OneDrive,
//! as well as managed service signup/login, disconnect, and account display logic.

// `info` is used only by OAuth sign-in flows; `warn` by the always-present
// account-display path.
#[cfg(feature = "oauth-providers")]
use tracing::info;

use crate::config::{CloudProvider, Config};
#[cfg(any(feature = "oauth-providers", test))]
use crate::keys::StoreKeys;

#[cfg(feature = "oauth-providers")]
fn oauth_token_save_error(error: crate::keys::KeyError) -> SetupError {
    SetupError(format!("Failed to save OAuth token: {error}"))
}

/// Google Drive OAuth sign-in: authorize, find/create the store folder, save
/// tokens to the keyring. Returns the folder id for the host to persist in its
/// own config (coven never writes the host's config).
///
/// Drives coven's localhost-callback OAuth flow, which binds a TCP port and
/// opens the system browser. Gated on `oauth-providers`.
#[cfg(feature = "oauth-providers")]
pub(crate) async fn sign_in_google_drive(
    client: &reqwest::Client,
    oauth_config: crate::oauth::OAuthConfig,
    key_service: &StoreKeys,
    store_name: &str,
    oauth_cancel: tokio::sync::watch::Receiver<bool>,
    clock: &dyn crate::clock::Clock,
) -> Result<String, SetupError> {
    let tokens = crate::oauth::authorize(client, &oauth_config, oauth_cancel, clock)
        .await
        .map_err(|e| SetupError(format!("Google Drive authorization failed: {e}")))?;

    // Create or find the folder
    let folder_name = format!("your-app - {store_name}");

    let search_query = super::google_drive::folder_search_query(&folder_name);
    let search_resp = super::google_drive::supports_all_drives(
        client.get("https://www.googleapis.com/drive/v3/files"),
    )
    .bearer_auth(&tokens.access_token)
    .query(&[
        ("q", search_query.as_str()),
        ("fields", "files(id)"),
        ("includeItemsFromAllDrives", "true"),
    ])
    .send()
    .await
    .map_err(|e| {
        SetupError(format!(
            "Failed to search for existing Google Drive folder: {e}"
        ))
    })?;

    if !search_resp.status().is_success() {
        let body = super::http::body_text(search_resp).await;
        return Err(SetupError(format!(
            "Failed to search for existing Google Drive folder: {body}"
        )));
    }

    let search_json: serde_json::Value = search_resp
        .json()
        .await
        .map_err(|e| SetupError(format!("Failed to parse Google Drive search response: {e}")))?;

    let existing_folder_id = search_json["files"][0]["id"]
        .as_str()
        .map(|s| s.to_string());

    let folder_id = if let Some(id) = existing_folder_id {
        id
    } else {
        let create_body = serde_json::json!({
            "name": folder_name,
            "mimeType": "application/vnd.google-apps.folder",
        });
        let resp = super::google_drive::supports_all_drives(
            client.post("https://www.googleapis.com/drive/v3/files"),
        )
        .bearer_auth(&tokens.access_token)
        .json(&create_body)
        .send()
        .await
        .map_err(|e| SetupError(format!("Failed to create Google Drive folder: {e}")))?;

        if !resp.status().is_success() {
            let body = super::http::body_text(resp).await;
            return Err(SetupError(format!(
                "Failed to create Google Drive folder: {body}"
            )));
        }

        let folder_resp: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| SetupError(format!("Failed to parse folder response: {e}")))?;
        folder_resp["id"]
            .as_str()
            .ok_or_else(|| SetupError("Google Drive folder response missing 'id'".to_string()))?
            .to_string()
    };

    key_service
        .set_cloud_home_oauth_tokens(&tokens)
        .map_err(oauth_token_save_error)?;

    info!("Authorized Google Drive; folder ready");
    Ok(folder_id)
}

/// Dropbox OAuth sign-in: authorize, create the store folder, save tokens to
/// the keyring. Returns the folder path for the host to persist in its config.
///
/// Uses the localhost-callback flow; gated on `oauth-providers`.
#[cfg(feature = "oauth-providers")]
pub(crate) async fn sign_in_dropbox(
    client: &reqwest::Client,
    oauth_config: crate::oauth::OAuthConfig,
    key_service: &StoreKeys,
    store_name: &str,
    oauth_cancel: tokio::sync::watch::Receiver<bool>,
    clock: &dyn crate::clock::Clock,
) -> Result<String, SetupError> {
    let tokens = crate::oauth::authorize(client, &oauth_config, oauth_cancel, clock)
        .await
        .map_err(|e| SetupError(format!("Dropbox authorization failed: {e}")))?;

    let folder_path = format!("/Apps/your-app/{store_name}");

    // Create the folder (ignore error if it already exists)
    let create_body = serde_json::json!({
        "path": folder_path,
        "autorename": false,
    });
    let resp = client
        .post("https://api.dropboxapi.com/2/files/create_folder_v2")
        .bearer_auth(&tokens.access_token)
        .json(&create_body)
        .send()
        .await
        .map_err(|e| SetupError(format!("Failed to create Dropbox folder: {e}")))?;

    let status = resp.status();
    if !status.is_success() {
        let body = super::http::body_text(resp).await;
        // 409 with "path/conflict" means the folder already exists -- fine
        if !(status == reqwest::StatusCode::CONFLICT && body.contains("conflict")) {
            return Err(SetupError(format!(
                "Failed to create Dropbox folder (HTTP {status}): {body}"
            )));
        }
    }

    key_service
        .set_cloud_home_oauth_tokens(&tokens)
        .map_err(oauth_token_save_error)?;

    info!("Authorized Dropbox; folder ready");
    Ok(folder_path)
}

/// OneDrive OAuth sign-in: authorize, resolve the default drive, create the app
/// folder, save tokens to the keyring. Returns `(drive_id, folder_id)` for the
/// host to persist in its config.
///
/// Uses the localhost-callback flow; gated on `oauth-providers`.
#[cfg(feature = "oauth-providers")]
pub(crate) async fn sign_in_onedrive(
    client: &reqwest::Client,
    oauth_config: crate::oauth::OAuthConfig,
    key_service: &StoreKeys,
    oauth_cancel: tokio::sync::watch::Receiver<bool>,
    clock: &dyn crate::clock::Clock,
) -> Result<(String, String), SetupError> {
    let tokens = crate::oauth::authorize(client, &oauth_config, oauth_cancel, clock)
        .await
        .map_err(|e| SetupError(format!("OneDrive authorization failed: {e}")))?;

    // Get the user's default drive
    let drive_resp = client
        .get("https://graph.microsoft.com/v1.0/me/drive")
        .bearer_auth(&tokens.access_token)
        .send()
        .await
        .map_err(|e| SetupError(format!("Failed to get drive info: {e}")))?;

    if !drive_resp.status().is_success() {
        let body = super::http::body_text(drive_resp).await;
        return Err(SetupError(format!("Failed to get OneDrive info: {body}")));
    }

    let drive_json: serde_json::Value = drive_resp
        .json()
        .await
        .map_err(|e| SetupError(format!("Failed to parse drive response: {e}")))?;

    let drive_id = drive_json["id"]
        .as_str()
        .ok_or_else(|| SetupError("Drive response missing 'id' field".to_string()))?
        .to_string();

    // Create the app folder
    let create_resp = client
        .post(format!(
            "https://graph.microsoft.com/v1.0/drives/{}/root/children",
            drive_id
        ))
        .bearer_auth(&tokens.access_token)
        .json(&serde_json::json!({
            "name": "your-app",
            "folder": {},
            "@microsoft.graph.conflictBehavior": "useExisting",
        }))
        .send()
        .await
        .map_err(|e| SetupError(format!("Failed to create OneDrive folder: {e}")))?;

    if !create_resp.status().is_success() {
        let body = super::http::body_text(create_resp).await;
        return Err(SetupError(format!(
            "Failed to create OneDrive folder: {body}"
        )));
    }

    let folder_json: serde_json::Value = create_resp
        .json()
        .await
        .map_err(|e| SetupError(format!("Failed to parse folder response: {e}")))?;

    let folder_id = folder_json["id"]
        .as_str()
        .ok_or_else(|| SetupError("Folder response missing 'id' field".to_string()))?
        .to_string();

    key_service
        .set_cloud_home_oauth_tokens(&tokens)
        .map_err(oauth_token_save_error)?;

    info!("Authorized OneDrive; folder ready");
    Ok((drive_id, folder_id))
}

/// Why building the sync storage from config failed. Each arm preserves the
/// typed error the layer below produced — notably [`CloudHomeError`], so the
/// [`CloudHomeError::is_retryable`] verdict survives up to the caller rather than
/// being flattened into a string here.
#[derive(Debug, thiserror::Error)]
pub enum StorageSetupError {
    #[error("failed to build cloud home: {0}")]
    CloudHome(#[from] super::CloudHomeError),
    #[error("key error: {0}")]
    Key(#[from] crate::keys::KeyError),
    #[error("no encryption key found for an encrypted cloud home")]
    NoEncryptionKey,
    #[error("{provider:?} cannot provide exact protocol and blob slots with this configuration")]
    ExactSlotsUnavailable { provider: CloudProvider },
}

pub(crate) fn require_exact_slot_capabilities_config(
    config: &Config,
) -> Result<(), StorageSetupError> {
    let provider = config.cloud_home.provider.clone().ok_or_else(|| {
        super::CloudHomeError::Configuration(
            "sync requires a cloud provider with exact-slot storage".to_string(),
        )
    })?;
    if exact_slot_capabilities_supported(
        &provider,
        config.cloud_home.s3_endpoint.is_some(),
        config.cloud_home.s3_exact_slots,
    ) {
        Ok(())
    } else {
        Err(StorageSetupError::ExactSlotsUnavailable { provider })
    }
}

pub(crate) fn require_exact_slot_capabilities_join_info(
    join_info: &crate::storage::cloud::CloudHomeJoinInfo,
    custom_s3_exact_slots: Option<crate::config::CustomS3ExactSlots>,
) -> Result<(), CloudProvider> {
    let provider = join_info.cloud_provider();
    let custom_endpoint = matches!(
        join_info,
        crate::storage::cloud::CloudHomeJoinInfo::S3 {
            endpoint: Some(_),
            ..
        }
    );
    if exact_slot_capabilities_supported(&provider, custom_endpoint, custom_s3_exact_slots) {
        Ok(())
    } else {
        Err(provider)
    }
}

pub(crate) fn require_exact_slot_capabilities_home(
    home: std::sync::Arc<dyn super::CloudHome>,
    provider: Option<CloudProvider>,
) -> Result<(), StorageSetupError> {
    if home.exact_slot_storage().is_some() {
        Ok(())
    } else if let Some(provider) = provider {
        Err(StorageSetupError::ExactSlotsUnavailable { provider })
    } else {
        Err(super::CloudHomeError::Configuration(
            "sync requires a cloud provider with exact-slot storage".to_string(),
        )
        .into())
    }
}

fn exact_slot_capabilities_supported(
    provider: &CloudProvider,
    custom_s3_endpoint: bool,
    custom_s3_exact_slots: Option<crate::config::CustomS3ExactSlots>,
) -> bool {
    match provider {
        CloudProvider::S3 if !custom_s3_endpoint => true,
        CloudProvider::S3 => {
            custom_s3_exact_slots
                == Some(crate::config::CustomS3ExactSlots::StandardConditionalRequests)
        }
        CloudProvider::GoogleDrive
        | CloudProvider::Dropbox
        | CloudProvider::OneDrive
        | CloudProvider::CloudKit => true,
    }
}

/// Cloud provider setup error.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct SetupError(pub String);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::HomeStorage;
    use crate::restoration::decode_restore_code;
    use crate::storage::cloud::CloudHomeJoinInfo;
    use crate::store_dir::StoreDir;
    use std::sync::Arc;

    fn membership_floor(
        author_pubkey: String,
    ) -> Vec<crate::protocol::membership::MembershipHeadRef> {
        let coord = crate::protocol::membership::MembershipCoord {
            author_pubkey,
            author_owner_grant: crate::protocol::membership::MembershipGrantId(
                crate::protocol::store_commit::ObjectHash::digest(b"restore test owner grant"),
            ),
            stream_id: "0000000000000000000000000000000000000000000000000000000000000001"
                .parse()
                .expect("canonical test author stream id"),
            seq: 1,
            entry_hash: crate::protocol::store_commit::ObjectHash::digest(
                b"restore test founder entry",
            ),
        };
        let stored = b"restore setup membership head";
        vec![crate::protocol::membership::MembershipHeadRef {
            coord,
            head_hash: crate::protocol::store_commit::ObjectHash::digest(
                b"restore setup membership head semantic bytes",
            ),
            object: crate::storage::ExactObjectRef::new(
                crate::storage::cloud::ObjectSlot::logical(
                    "store-v1/membership/heads/restore-setup/1.json".to_string(),
                )
                .expect("valid test membership-head slot"),
                stored.len() as u64,
                crate::protocol::store_commit::ObjectHash::digest(stored),
            ),
        }]
    }

    fn store_root() -> crate::protocol::store_commit::StoreRootRef {
        let stored = b"restore setup Store root";
        crate::protocol::store_commit::StoreRootRef {
            store_root_id: crate::protocol::store_commit::ObjectHash::digest(
                b"restore setup Store root identity",
            ),
            store_root_hash: crate::protocol::store_commit::ObjectHash::digest(stored),
            object: crate::storage::ExactObjectRef::new(
                crate::storage::cloud::ObjectSlot::logical(
                    "store-v1/protocol/root/restore-setup.json".to_string(),
                )
                .expect("valid test Store-root slot"),
                stored.len() as u64,
                crate::protocol::store_commit::ObjectHash::digest(stored),
            ),
        }
    }

    fn restore_authority() -> crate::restoration::RestoreAuthority {
        let owner_grant = crate::protocol::membership::MembershipGrantId(
            crate::protocol::store_commit::ObjectHash::digest(b"restore test owner grant"),
        );
        let root = store_root();
        let owner_pubkey = hex::encode([7u8; 32]);
        let anchor = crate::protocol::store_commit::GrantStreamAnchor::OwnerRecovery {
            first_slot: crate::storage::cloud::ObjectSlot::logical(
                "store-v1/recovery/restore-setup/first.json".to_string(),
            )
            .expect("valid recovery slot"),
        };
        let activation = crate::protocol::store_commit::OwnerRecoveryActivationId::derive(
            &root,
            &owner_pubkey,
            &owner_grant,
            &anchor,
        )
        .expect("valid recovery activation");
        crate::restoration::RestoreAuthority::OwnerRecovery(
            crate::restoration::OwnerRecoveryAuthority {
                owner_identity_secret: hex::encode(
                    crate::keys::UserKeypair::generate().to_keypair_bytes(),
                ),
                owner_grant: owner_grant.clone(),
                recovery: crate::protocol::store_commit::OwnerRecoveryCursor {
                    owner_grant,
                    position: crate::protocol::store_commit::OwnerRecoveryPosition::BeforeFirst {
                        activation,
                    },
                },
                published_at: "2026-07-17T00:00:00Z".to_string(),
            },
        )
    }

    /// A CloudKit config with `storage: Browsable` so the test exercises only
    /// the CloudKit provider arm, never the opaque-home encryption-key read
    /// (that path is unrelated to the restore-code provider guard under test).
    fn cloudkit_config(owner_zone: Option<(&str, &str)>) -> Config {
        let mut config = Config::with_defaults(
            "store-1".to_string(),
            "device-1".to_string(),
            StoreDir::new("unused-store-dir"),
            "CloudKit Store".to_string(),
        );
        config.cloud_home.provider = Some(CloudProvider::CloudKit);
        config.cloud_home.storage = HomeStorage::Browsable;
        if let Some((owner, zone)) = owner_zone {
            config.cloud_home.cloudkit_owner_name = Some(owner.to_string());
            config.cloud_home.cloudkit_zone_name = Some(zone.to_string());
        }
        config
    }

    fn store_security(
        config: &Config,
        keys: StoreKeys,
        master_keys: Arc<dyn crate::keys::MasterKeyCustody>,
    ) -> crate::store_security::StoreSecurity {
        let identity = crate::identity_custody::IdentityCustody::InMemory(
            crate::keys::UserKeypair::generate(),
        )
        .resolve(&keys, &config.store_dir);
        crate::store_security::StoreSecurity::new(
            keys,
            master_keys,
            identity,
            crate::oauth::OAuthClients::empty(),
        )
    }

    #[test]
    fn exact_slot_admission_is_universal_and_uses_local_s3_assertions() {
        let mut config = Config::with_defaults(
            "store-1".to_string(),
            "device-1".to_string(),
            StoreDir::new("unused-store-dir"),
            "Provider matrix".to_string(),
        );

        config.cloud_home.provider = Some(CloudProvider::S3);
        config.cloud_home.s3_endpoint = None;
        assert!(require_exact_slot_capabilities_config(&config).is_ok());

        config.cloud_home.s3_endpoint = Some("https://objects.example".to_string());
        assert!(matches!(
            require_exact_slot_capabilities_config(&config),
            Err(StorageSetupError::ExactSlotsUnavailable {
                provider: CloudProvider::S3
            })
        ));
        config.cloud_home.s3_exact_slots =
            Some(crate::CustomS3ExactSlots::StandardConditionalRequests);
        assert!(require_exact_slot_capabilities_config(&config).is_ok());

        for provider in [
            CloudProvider::GoogleDrive,
            CloudProvider::Dropbox,
            CloudProvider::OneDrive,
            CloudProvider::CloudKit,
        ] {
            config.cloud_home.provider = Some(provider.clone());
            assert!(require_exact_slot_capabilities_config(&config).is_ok());
        }
    }

    #[test]
    fn custom_s3_join_requires_the_local_exact_slot_assertion() {
        let join_info = CloudHomeJoinInfo::S3 {
            bucket: "bucket".to_string(),
            region: "region".to_string(),
            endpoint: Some("https://objects.example".to_string()),
            access_key: "access".to_string(),
            secret_key: "secret".to_string(),
            key_prefix: None,
        };

        assert_eq!(
            require_exact_slot_capabilities_join_info(&join_info, None),
            Err(CloudProvider::S3),
        );
        assert!(require_exact_slot_capabilities_join_info(
            &join_info,
            Some(crate::CustomS3ExactSlots::StandardConditionalRequests),
        )
        .is_ok());
    }

    #[test]
    fn custom_s3_exact_slot_assertion_stays_out_of_restore_wire() {
        crate::keys::test_keyring::install();
        let dir = tempfile::tempdir().expect("store directory");
        let mut config = Config::with_defaults(
            "local-assertion-wire-test".to_string(),
            "device-1".to_string(),
            StoreDir::new(dir.path()),
            "Wire Test".to_string(),
        );
        config.cloud_home.provider = Some(CloudProvider::S3);
        config.cloud_home.storage = HomeStorage::Browsable;
        config.cloud_home.s3_bucket = Some("bucket".to_string());
        config.cloud_home.s3_region = Some("region".to_string());
        config.cloud_home.s3_endpoint = Some("https://objects.example".to_string());
        config.cloud_home.s3_exact_slots =
            Some(crate::CustomS3ExactSlots::StandardConditionalRequests);
        let key_service = StoreKeys::bind(config.store_id.clone());
        key_service
            .set_cloud_home_credentials(&crate::keys::CloudHomeCredentials::S3 {
                access_key: "access".to_string(),
                secret_key: "secret".to_string(),
            })
            .expect("store credentials");
        let custody =
            crate::custody::KeyCustody::InMemory(crate::encryption::MasterKeyring::generate())
                .resolve(&key_service, &config.store_dir);
        let security = store_security(&config, key_service, custody);
        let encoded = security
            .generate_restore_code(
                &config,
                store_root(),
                hex::encode([7u8; 32]),
                crate::joining::MembershipFloor(membership_floor(hex::encode([7u8; 32]))),
                restore_authority(),
            )
            .expect("generate restore code");
        let decoded = decode_restore_code(&encoded).expect("decode restore code");
        let provider_wire = serde_json::to_string(&decoded.provider).expect("serialize provider");

        assert!(!provider_wire.contains("s3_exact_slots"));
        assert!(!provider_wire.contains("strong_reads"));
        assert_eq!(
            config.cloud_home.s3_exact_slots,
            Some(crate::CustomS3ExactSlots::StandardConditionalRequests),
        );
    }

    /// A device that joined via a CloudKit share has `cloudkit_owner_name` /
    /// `cloudkit_zone_name` set (`build_config`'s `CloudKitShare` arm).
    /// Restoring a share is illegitimate — decode already rejects
    /// `CloudKitShare` (`RestoreCodeError::CloudKitShareNotRestorable`) — so
    /// generation must refuse a share-joined config too, rather than mapping
    /// it to `CloudHomeJoinInfo::CloudKit`, which would restore against the
    /// restoring device's own empty private zone.
    #[test]
    fn generate_restore_code_rejects_a_share_joined_cloudkit_config() {
        crate::keys::test_keyring::install();

        let config = cloudkit_config(Some(("owner-name", "zone-name")));
        let key_service = StoreKeys::bind(config.store_id.clone());
        let custody = crate::custody::KeyCustody::Keyring.resolve(&key_service, &config.store_dir);
        let security = store_security(&config, key_service, custody);
        let err = security
            .generate_restore_code(
                &config,
                store_root(),
                hex::encode([7u8; 32]),
                crate::joining::MembershipFloor(membership_floor(hex::encode([7u8; 32]))),
                restore_authority(),
            )
            .expect_err("a share-joined CloudKit config must not generate a restore code");
        let message = err.to_string();
        assert!(
            message.contains("share"),
            "error should explain the store was joined via a share: {message}"
        );
    }

    /// A truly private CloudKit config (no owner/zone set) still emits a `ck`
    /// restore code that decodes back to `CloudHomeJoinInfo::CloudKit`.
    #[test]
    fn generate_restore_code_private_cloudkit_round_trips() {
        crate::keys::test_keyring::install();

        let config = cloudkit_config(None);
        let key_service = StoreKeys::bind(config.store_id.clone());
        let custody = crate::custody::KeyCustody::Keyring.resolve(&key_service, &config.store_dir);
        let security = store_security(&config, key_service, custody);
        let code = security
            .generate_restore_code(
                &config,
                store_root(),
                hex::encode([7u8; 32]),
                crate::joining::MembershipFloor(membership_floor(hex::encode([7u8; 32]))),
                restore_authority(),
            )
            .expect("a private CloudKit config generates a restore code");
        let decoded = decode_restore_code(&code).expect("generated code decodes");
        assert!(
            matches!(decoded.provider, CloudHomeJoinInfo::CloudKit),
            "expected CloudKit provider, got {:?}",
            decoded.provider
        );
    }
}

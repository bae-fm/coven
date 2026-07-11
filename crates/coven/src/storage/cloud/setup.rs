//! Cloud provider setup and management.
//!
//! Contains the OAuth sign-in flows for Google Drive, Dropbox, and OneDrive,
//! as well as managed service signup/login, disconnect, and account display logic.

// `info` is used only by the native-only oauth sign-in flows (also gated on
// `oauth-providers`); `warn` by the always-present account-display path.
#[cfg(all(not(target_arch = "wasm32"), feature = "oauth-providers"))]
use tracing::info;

use crate::config::{CloudProvider, Config};
use crate::keys::{CloudHomeCredentials, DeviceKeys, StoreKeys};
#[cfg(all(not(target_arch = "wasm32"), feature = "oauth-providers"))]
use crate::oauth::OAuthTokens;
#[cfg(not(target_arch = "wasm32"))]
use crate::sync::cloud_storage::BlobPathScheme;
use crate::sync::cloud_storage::CloudCipher;
#[cfg(not(target_arch = "wasm32"))]
use std::sync::Arc;

#[cfg(all(not(target_arch = "wasm32"), feature = "oauth-providers"))]
fn save_oauth_tokens(key_service: &StoreKeys, tokens: &OAuthTokens) -> Result<(), SetupError> {
    key_service
        .set_cloud_home_oauth_tokens(tokens)
        .map_err(|e| SetupError(format!("Failed to save OAuth token: {e}")))
}

/// Google Drive OAuth sign-in: authorize, find/create the store folder, save
/// tokens to the keyring. Returns the folder id for the host to persist in its
/// own config (coven never writes the host's config).
///
/// Native-only: drives coven's localhost-callback OAuth flow ([`crate::oauth::authorize`]),
/// which binds a TCP port and opens a browser — neither exists on wasm. Also gated
/// on `oauth-providers`.
#[cfg(all(not(target_arch = "wasm32"), feature = "oauth-providers"))]
pub async fn sign_in_google_drive(
    key_service: &StoreKeys,
    store_name: &str,
    oauth_cancel: tokio::sync::watch::Receiver<bool>,
    clock: &dyn crate::clock::Clock,
) -> Result<String, SetupError> {
    let oauth_config = super::google_drive::GoogleDriveCloudHome::oauth_config()
        .map_err(|e| SetupError(e.to_string()))?;
    let tokens = crate::oauth::authorize(&oauth_config, oauth_cancel, clock)
        .await
        .map_err(|e| SetupError(format!("Google Drive authorization failed: {e}")))?;

    let client = reqwest::Client::new();

    // Create or find the folder
    let folder_name = format!("your-app - {store_name}");

    let search_query = super::google_drive::folder_search_query(&folder_name);
    let search_resp = client
        .get("https://www.googleapis.com/drive/v3/files")
        .bearer_auth(&tokens.access_token)
        .query(&[("q", &search_query), ("fields", &"files(id)".to_string())])
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
        let resp = client
            .post("https://www.googleapis.com/drive/v3/files")
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

    save_oauth_tokens(key_service, &tokens)?;

    info!("Authorized Google Drive; folder ready");
    Ok(folder_id)
}

/// Dropbox OAuth sign-in: authorize, create the store folder, save tokens to
/// the keyring. Returns the folder path for the host to persist in its config.
///
/// Native-only (see [`sign_in_google_drive`]); also gated on `oauth-providers`.
#[cfg(all(not(target_arch = "wasm32"), feature = "oauth-providers"))]
pub async fn sign_in_dropbox(
    key_service: &StoreKeys,
    store_name: &str,
    oauth_cancel: tokio::sync::watch::Receiver<bool>,
    clock: &dyn crate::clock::Clock,
) -> Result<String, SetupError> {
    let oauth_config =
        super::dropbox::DropboxCloudHome::oauth_config().map_err(|e| SetupError(e.to_string()))?;
    let tokens = crate::oauth::authorize(&oauth_config, oauth_cancel, clock)
        .await
        .map_err(|e| SetupError(format!("Dropbox authorization failed: {e}")))?;

    let client = reqwest::Client::new();

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

    save_oauth_tokens(key_service, &tokens)?;

    info!("Authorized Dropbox; folder ready");
    Ok(folder_path)
}

/// OneDrive OAuth sign-in: authorize, resolve the default drive, create the app
/// folder, save tokens to the keyring. Returns `(drive_id, folder_id)` for the
/// host to persist in its config.
///
/// Native-only (see [`sign_in_google_drive`]); also gated on `oauth-providers`.
#[cfg(all(not(target_arch = "wasm32"), feature = "oauth-providers"))]
pub async fn sign_in_onedrive(
    key_service: &StoreKeys,
    oauth_cancel: tokio::sync::watch::Receiver<bool>,
    clock: &dyn crate::clock::Clock,
) -> Result<(String, String), SetupError> {
    let oauth_config = super::onedrive::OneDriveCloudHome::oauth_config()
        .map_err(|e| SetupError(e.to_string()))?;
    let tokens = crate::oauth::authorize(&oauth_config, oauth_cancel, clock)
        .await
        .map_err(|e| SetupError(format!("OneDrive authorization failed: {e}")))?;

    let client = reqwest::Client::new();

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

    save_oauth_tokens(key_service, &tokens)?;

    info!("Authorized OneDrive; folder ready");
    Ok((drive_id, folder_id))
}

/// Build a RestoreCode from config and keyring, then encode it.
pub fn generate_restore_code(
    config: &Config,
    key_service: &StoreKeys,
) -> Result<String, SetupError> {
    use crate::storage::cloud::CloudHomeJoinInfo;
    use crate::sync::restore_code::{encode_restore_code, RestoreCode, RESTORE_CODE_VERSION};

    let cloud_provider = config.cloud_home.provider.as_ref().ok_or_else(|| {
        SetupError("No cloud provider configured. Set up sync first.".to_string())
    })?;

    // An opaque home carries its store key in the restore code so a second
    // device can read the bucket; a browsable home has no key (`ek` is omitted),
    // and the restorer rebuilds the browsable (plaintext, readable) home from its
    // absence.
    let ek = if config.cloud_home.storage.is_opaque() {
        Some(
            key_service
                .get_encryption_key()
                .map_err(|e| SetupError(format!("Failed to read encryption key: {e}")))?
                .ok_or_else(|| SetupError("No encryption key found".to_string()))?,
        )
    } else {
        None
    };

    let keypair = DeviceKeys::get_or_create_user_keypair()
        .map_err(|e| SetupError(format!("Failed to get signing key: {e}")))?;

    let provider = match cloud_provider {
        CloudProvider::S3 => {
            let creds = key_service
                .get_cloud_home_credentials()
                .map_err(|e| SetupError(format!("Failed to read cloud credentials: {e}")))?
                .ok_or_else(|| SetupError("No S3 credentials found in keyring".to_string()))?;
            let (access_key, secret_key) = match creds {
                CloudHomeCredentials::S3 {
                    access_key,
                    secret_key,
                } => (access_key, secret_key),
                _ => {
                    return Err(SetupError(
                        "Expected S3 credentials but found different type".to_string(),
                    ))
                }
            };
            let bucket = config
                .cloud_home
                .s3_bucket
                .clone()
                .ok_or_else(|| SetupError("S3 bucket not configured".to_string()))?;
            let region = config
                .cloud_home
                .s3_region
                .clone()
                .ok_or_else(|| SetupError("S3 region not configured".to_string()))?;
            CloudHomeJoinInfo::S3 {
                bucket,
                region,
                endpoint: config.cloud_home.s3_endpoint.clone(),
                key_prefix: config.cloud_home.s3_key_prefix.clone(),
                access_key,
                secret_key,
            }
        }
        CloudProvider::CloudKit => {
            // A device that joined via a CloudKit share has these set
            // (`build_config`'s `CloudKitShare` arm); restore recovers your
            // own zone, never one shared to you — the same line
            // `decode_restore_code` already draws by rejecting
            // `CloudHomeJoinInfo::CloudKitShare`. Only a truly private config
            // (neither set) may emit `CloudHomeJoinInfo::CloudKit`.
            if config.cloud_home.cloudkit_owner_name.is_some()
                || config.cloud_home.cloudkit_zone_name.is_some()
            {
                return Err(SetupError(
                    "This store was joined through a CloudKit share; only the store's owner can create a restore code.".to_string(),
                ));
            }
            CloudHomeJoinInfo::CloudKit
        }
        CloudProvider::GoogleDrive => CloudHomeJoinInfo::GoogleDrive {
            folder_id: config
                .cloud_home
                .google_drive_folder_id
                .clone()
                .ok_or_else(|| SetupError("Google Drive folder ID not configured".to_string()))?,
        },
        CloudProvider::Dropbox => CloudHomeJoinInfo::Dropbox {
            folder_path: config
                .cloud_home
                .dropbox_folder_path
                .clone()
                .ok_or_else(|| SetupError("Dropbox folder path not configured".to_string()))?,
        },
        CloudProvider::OneDrive => {
            let drive_id = config
                .cloud_home
                .onedrive_drive_id
                .clone()
                .ok_or_else(|| SetupError("OneDrive drive ID not configured".to_string()))?;
            let folder_id = config
                .cloud_home
                .onedrive_folder_id
                .clone()
                .ok_or_else(|| SetupError("OneDrive folder ID not configured".to_string()))?;
            CloudHomeJoinInfo::OneDrive {
                drive_id,
                folder_id,
            }
        }
    };

    let code = RestoreCode {
        v: RESTORE_CODE_VERSION,
        sid: config.store_id.clone(),
        ek,
        name: config.store_name.clone(),
        provider,
        sk: hex::encode(keypair.to_keypair_bytes()),
    };

    Ok(encode_restore_code(&code))
}

/// Build the [`CloudCipher`] a store's config selects: an opaque home seals
/// every object under the keyring's store key; a browsable home
/// (`cloud_home.storage == Browsable`) stores objects in the clear.
///
/// A browsable home has no store key, so it never reads the keyring — the
/// absence of a key there is expected, not an error.
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
    #[error("failed to build the encryption service: {0}")]
    Encryption(#[from] crate::encryption::EncryptionError),
}

pub fn build_cloud_cipher(
    config: &Config,
    key_service: &StoreKeys,
) -> Result<CloudCipher, StorageSetupError> {
    if config.cloud_home.storage.is_browsable() {
        return Ok(CloudCipher::Plaintext);
    }
    let key = key_service
        .get_encryption_key()?
        .ok_or(StorageSetupError::NoEncryptionKey)?;
    let enc = crate::encryption::EncryptionService::new(&key)?;
    Ok(CloudCipher::Encrypted(enc))
}

/// Create sync storage from config and credentials.
///
/// This is a lighter version of `sync::cycle::init_sync` that only creates the
/// storage client without starting a sync session or extracting raw DB handles.
/// Used by membership management which only needs storage access.
///
/// `cipher` lets the caller reuse an already-built cipher (so the sync loop and
/// storage share one instance for in-place key rotation); when `None` it is
/// built from config via [`build_cloud_cipher`].
#[cfg(not(target_arch = "wasm32"))]
pub async fn create_sync_storage_with_cloudkit(
    config: &Config,
    key_service: &StoreKeys,
    cipher: Option<CloudCipher>,
    clock: crate::clock::ClockRef,
    cloudkit_ops: Option<std::sync::Arc<dyn super::cloudkit::CloudKitOps>>,
) -> Result<crate::sync::cloud_storage::CloudSyncStorage, StorageSetupError> {
    let cloud_home =
        super::create_cloud_home_with_cloudkit(config, key_service, clock, cloudkit_ops).await?;
    create_sync_storage_with_home(config, key_service, Arc::from(cloud_home), cipher)
}

#[cfg(not(target_arch = "wasm32"))]
pub fn create_sync_storage_with_home(
    config: &Config,
    key_service: &StoreKeys,
    home: Arc<dyn super::CloudHome>,
    cipher: Option<CloudCipher>,
) -> Result<crate::sync::cloud_storage::CloudSyncStorage, StorageSetupError> {
    let cipher = match cipher {
        Some(c) => c,
        None => build_cloud_cipher(config, key_service)?,
    };

    // The device's global signing identity, used to sign the control objects the
    // storage writes (its head, the min_schema floor) so a reader can attribute
    // and verify them against the membership chain.
    let keypair = DeviceKeys::get_or_create_user_keypair()?;

    Ok(crate::sync::cloud_storage::CloudSyncStorage::new(
        home,
        cipher,
        BlobPathScheme::for_storage(config.cloud_home.storage),
        config.store_id.clone(),
        keypair,
    ))
}

/// Cloud provider setup error.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct SetupError(pub String);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::HomeStorage;
    use crate::storage::cloud::CloudHomeJoinInfo;
    use crate::store_dir::StoreDir;
    use crate::sync::restore_code::decode_restore_code;

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
        crate::keys::DeviceKeys::get_or_create_user_keypair().expect("seed device keypair");

        let config = cloudkit_config(Some(("owner-name", "zone-name")));
        let key_service = StoreKeys::new(config.store_id.clone());

        let err = generate_restore_code(&config, &key_service)
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
        crate::keys::DeviceKeys::get_or_create_user_keypair().expect("seed device keypair");

        let config = cloudkit_config(None);
        let key_service = StoreKeys::new(config.store_id.clone());

        let code = generate_restore_code(&config, &key_service)
            .expect("a private CloudKit config generates a restore code");
        let decoded = decode_restore_code(&code).expect("generated code decodes");
        assert!(
            matches!(decoded.provider, CloudHomeJoinInfo::CloudKit),
            "expected CloudKit provider, got {:?}",
            decoded.provider
        );
    }
}

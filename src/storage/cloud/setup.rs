//! Cloud provider setup and management.
//!
//! Contains the OAuth sign-in flows for Google Drive, Dropbox, and OneDrive,
//! as well as managed service signup/login, disconnect, and account display logic.

use tracing::{info, warn};

use crate::config::{CloudProvider, Config};
use crate::keys::{CloudHomeCredentials, KeyService};

/// Google Drive OAuth sign-in: authorize, find/create the library folder, save
/// tokens to the keyring. Returns the folder id for the host to persist in its
/// own config (coven never writes the host's config).
pub async fn sign_in_google_drive(
    key_service: &KeyService,
    library_name: &str,
    oauth_cancel: tokio::sync::watch::Receiver<bool>,
    clock: &dyn crate::clock::Clock,
) -> Result<String, SetupError> {
    let oauth_config = super::google_drive::GoogleDriveCloudHome::oauth_config();
    let tokens = crate::oauth::authorize(&oauth_config, oauth_cancel, clock)
        .await
        .map_err(|e| SetupError(format!("Google Drive authorization failed: {e}")))?;

    let client = reqwest::Client::new();

    // Create or find the folder
    let folder_name = format!("bae - {library_name}");

    let search_query = format!(
        "name = '{}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false",
        folder_name.replace('\'', "\\'")
    );
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
        let body = search_resp.text().await.unwrap_or_default();
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
            let body = resp
                .text()
                .await
                .unwrap_or_else(|e| format!("<body read failed: {e}>"));
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

    // Save tokens to keyring
    let token_json = serde_json::to_string(&tokens)
        .map_err(|e| SetupError(format!("Failed to serialize tokens: {e}")))?;
    key_service
        .set_cloud_home_credentials(&CloudHomeCredentials::OAuth { token_json })
        .map_err(|e| SetupError(format!("Failed to save OAuth token: {e}")))?;

    info!("Authorized Google Drive; folder ready");
    Ok(folder_id)
}

/// Dropbox OAuth sign-in: authorize, create the library folder, save tokens to
/// the keyring. Returns the folder path for the host to persist in its config.
pub async fn sign_in_dropbox(
    key_service: &KeyService,
    library_name: &str,
    oauth_cancel: tokio::sync::watch::Receiver<bool>,
    clock: &dyn crate::clock::Clock,
) -> Result<String, SetupError> {
    let oauth_config = super::dropbox::DropboxCloudHome::oauth_config();
    let tokens = crate::oauth::authorize(&oauth_config, oauth_cancel, clock)
        .await
        .map_err(|e| SetupError(format!("Dropbox authorization failed: {e}")))?;

    let client = reqwest::Client::new();

    let folder_path = format!("/Apps/bae/{library_name}");

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
        let body = resp
            .text()
            .await
            .unwrap_or_else(|e| format!("<body read failed: {e}>"));
        // 409 with "path/conflict" means the folder already exists -- fine
        if !(status == reqwest::StatusCode::CONFLICT && body.contains("conflict")) {
            return Err(SetupError(format!(
                "Failed to create Dropbox folder (HTTP {status}): {body}"
            )));
        }
    }

    // Save tokens to keyring
    let token_json = serde_json::to_string(&tokens)
        .map_err(|e| SetupError(format!("Failed to serialize tokens: {e}")))?;
    key_service
        .set_cloud_home_credentials(&CloudHomeCredentials::OAuth { token_json })
        .map_err(|e| SetupError(format!("Failed to save OAuth token: {e}")))?;

    info!("Authorized Dropbox; folder ready");
    Ok(folder_path)
}

/// OneDrive OAuth sign-in: authorize, resolve the default drive, create the app
/// folder, save tokens to the keyring. Returns `(drive_id, folder_id)` for the
/// host to persist in its config.
pub async fn sign_in_onedrive(
    key_service: &KeyService,
    oauth_cancel: tokio::sync::watch::Receiver<bool>,
    clock: &dyn crate::clock::Clock,
) -> Result<(String, String), SetupError> {
    let oauth_config = super::onedrive::OneDriveCloudHome::oauth_config();
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
        let body = drive_resp
            .text()
            .await
            .unwrap_or_else(|e| format!("<body read failed: {e}>"));
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
            "name": "bae",
            "folder": {},
            "@microsoft.graph.conflictBehavior": "useExisting",
        }))
        .send()
        .await
        .map_err(|e| SetupError(format!("Failed to create OneDrive folder: {e}")))?;

    if !create_resp.status().is_success() {
        let body = create_resp
            .text()
            .await
            .unwrap_or_else(|e| format!("<body read failed: {e}>"));
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

    // Save tokens to keyring
    let token_json = serde_json::to_string(&tokens)
        .map_err(|e| SetupError(format!("Failed to serialize tokens: {e}")))?;
    key_service
        .set_cloud_home_credentials(&CloudHomeCredentials::OAuth { token_json })
        .map_err(|e| SetupError(format!("Failed to save OAuth token: {e}")))?;

    info!("Authorized OneDrive; folder ready");
    Ok((drive_id, folder_id))
}

/// Get a display string for the current cloud account (bucket name, username, etc.)
pub fn cloud_account_display_for(config: &Config, key_service: &KeyService) -> Option<String> {
    match config.cloud_home.provider.as_ref()? {
        CloudProvider::HttpProxy => config.cloud_home.http_url.clone(),
        CloudProvider::S3 => config
            .cloud_home
            .s3_bucket
            .as_ref()
            .map(|b| format!("s3://{b}")),
        CloudProvider::CloudKit => Some("iCloud".to_string()),
        CloudProvider::GoogleDrive | CloudProvider::Dropbox | CloudProvider::OneDrive => {
            match key_service.get_cloud_home_credentials() {
                Ok(Some(CloudHomeCredentials::OAuth { .. })) => Some("Connected".to_string()),
                Ok(_) => None,
                Err(e) => {
                    warn!("reading cloud home credentials for account display: {e}");
                    None
                }
            }
        }
    }
}

/// Build a RestoreCode from config and keyring, then encode it.
pub fn generate_restore_code(
    config: &Config,
    key_service: &KeyService,
) -> Result<String, SetupError> {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine;

    use crate::sync::restore_code::{encode_restore_code, RestoreCode, RestoreProvider};

    let cloud_provider = config.cloud_home.provider.as_ref().ok_or_else(|| {
        SetupError("No cloud provider configured. Set up sync first.".to_string())
    })?;

    let encryption_key_hex = key_service
        .get_encryption_key()
        .map_err(|e| SetupError(format!("Failed to read encryption key: {e}")))?
        .ok_or_else(|| SetupError("No encryption key found".to_string()))?;

    let keypair = key_service
        .get_or_create_user_keypair()
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
            RestoreProvider::S3 {
                bucket,
                region,
                endpoint: config.cloud_home.s3_endpoint.clone(),
                key_prefix: config.cloud_home.s3_key_prefix.clone(),
                access_key,
                secret_key,
            }
        }
        CloudProvider::CloudKit => RestoreProvider::CloudKit,
        CloudProvider::GoogleDrive => RestoreProvider::GoogleDrive {
            folder_id: config
                .cloud_home
                .google_drive_folder_id
                .clone()
                .ok_or_else(|| SetupError("Google Drive folder ID not configured".to_string()))?,
        },
        CloudProvider::Dropbox => RestoreProvider::Dropbox {
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
            RestoreProvider::OneDrive {
                drive_id,
                folder_id,
            }
        }
        CloudProvider::HttpProxy => RestoreProvider::HttpProxy {
            url: config
                .cloud_home
                .http_url
                .clone()
                .ok_or_else(|| SetupError("HTTP proxy URL not configured".to_string()))?,
        },
    };

    let code = RestoreCode {
        v: 1,
        lid: config.library_id.clone(),
        ek: encryption_key_hex,
        name: config.library_name.clone(),
        provider,
        sk: URL_SAFE_NO_PAD.encode(keypair.signing_key),
    };

    Ok(encode_restore_code(&code))
}

/// Create sync storage from config and credentials.
///
/// This is a lighter version of `sync::cycle::init_sync` that only creates the
/// storage client without starting a sync session or extracting raw DB handles.
/// Used by membership management which only needs storage access.
pub async fn create_sync_storage(
    config: &Config,
    key_service: &KeyService,
    encryption_service: &Option<crate::encryption::EncryptionService>,
    clock: crate::clock::ClockRef,
) -> Result<crate::sync::encrypted_storage::EncryptedSyncStorage, String> {
    let cloud_home = super::create_cloud_home(config, key_service, clock)
        .await
        .map_err(|e| format!("{e}"))?;

    let encryption = match encryption_service {
        Some(enc) => enc.clone(),
        None => {
            let key = key_service
                .get_encryption_key()
                .map_err(|e| format!("Failed to read encryption key: {e}"))?
                .ok_or("No encryption key found")?;
            crate::encryption::EncryptionService::new(&key)
                .map_err(|e| format!("Failed to create encryption service: {e}"))?
        }
    };

    Ok(crate::sync::encrypted_storage::EncryptedSyncStorage::new(
        cloud_home, encryption,
    ))
}

/// Cloud provider setup error.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct SetupError(pub String);

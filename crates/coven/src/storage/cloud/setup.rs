//! Cloud provider setup and management.
//!
//! Contains the OAuth sign-in flows for Google Drive, Dropbox, and OneDrive,
//! as well as managed service signup/login, disconnect, and account display logic.

// `info` is used only by OAuth sign-in flows; `warn` by the always-present
// account-display path.
#[cfg(feature = "oauth-providers")]
use tracing::info;

use crate::config::{CloudProvider, Config};
use crate::keys::{CloudHomeCredentials, DeviceIdentityCustody, MasterKeyCustody, StoreKeys};
#[cfg(feature = "oauth-providers")]
use crate::oauth::OAuthTokens;
use crate::sync::cloud_storage::BlobPathScheme;
use crate::sync::cloud_storage::CloudCipher;
use std::sync::Arc;

#[cfg(feature = "oauth-providers")]
fn save_oauth_tokens(key_service: &StoreKeys, tokens: &OAuthTokens) -> Result<(), SetupError> {
    key_service
        .set_cloud_home_oauth_tokens(tokens)
        .map_err(|e| SetupError(format!("Failed to save OAuth token: {e}")))
}

/// Google Drive OAuth sign-in: authorize, find/create the store folder, save
/// tokens to the keyring. Returns the folder id for the host to persist in its
/// own config (coven never writes the host's config).
///
/// Drives coven's localhost-callback OAuth flow ([`crate::oauth::authorize`]),
/// which binds a TCP port and opens the system browser. Gated on
/// `oauth-providers`.
#[cfg(feature = "oauth-providers")]
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
/// Uses the localhost-callback flow; gated on `oauth-providers`.
#[cfg(feature = "oauth-providers")]
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
/// Uses the localhost-callback flow; gated on `oauth-providers`.
#[cfg(feature = "oauth-providers")]
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

/// Build a RestoreCode from config and custody, then encode it.
///
/// `membership_floor` is the exact policy-shaped membership state at mint
/// time. The caller fetches it because this function never touches the network.
pub fn generate_restore_code(
    config: &Config,
    key_service: &StoreKeys,
    custody: &dyn MasterKeyCustody,
    identity_custody: &dyn DeviceIdentityCustody,
    store_root_hash: crate::sync::store_commit::ObjectHash,
    founder_pubkey: String,
    membership_floor: crate::join_code::MembershipFloor,
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
            custody
                .unlock()
                .map_err(|e| SetupError(format!("Failed to read master key: {e}")))?
                .ok_or_else(|| SetupError("No encryption key found".to_string()))?
                .to_serialized(),
        )
    } else {
        None
    };

    // A restore code embeds this store's signing identity so a second device
    // can re-establish it; the store already has one — established when it
    // was created, joined, or restored — so this reads it rather than
    // minting: a connect-shaped precondition, not a query.
    let keypair = crate::keys::require_identity(identity_custody)
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
        store_root_hash,
        founder_pubkey,
        membership_floor,
    };

    Ok(encode_restore_code(&code))
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
    #[error("{provider:?} cannot coordinate a Serial Store with this configuration")]
    SerialCoordinationUnavailable { provider: CloudProvider },
    #[error(
        "{provider:?} cannot store immutable protocol and blob copies with this configuration"
    )]
    ImmutableCopiesUnavailable { provider: CloudProvider },
}

pub(crate) fn require_immutable_copy_capabilities_config(
    config: &Config,
) -> Result<(), StorageSetupError> {
    let provider = config.cloud_home.provider.clone().ok_or_else(|| {
        super::CloudHomeError::Configuration(
            "sync requires a cloud provider with immutable-copy storage".to_string(),
        )
    })?;
    if immutable_copy_capabilities_supported(
        &provider,
        config.cloud_home.s3_endpoint.is_some(),
        config.cloud_home.s3_immutable_copies,
    ) {
        Ok(())
    } else {
        Err(StorageSetupError::ImmutableCopiesUnavailable { provider })
    }
}

pub(crate) fn require_immutable_copy_capabilities_join_info(
    join_info: &crate::storage::cloud::CloudHomeJoinInfo,
    custom_s3_immutable_copies: Option<crate::config::CustomS3ImmutableCopies>,
) -> Result<(), CloudProvider> {
    let provider = join_info.cloud_provider();
    let custom_endpoint = matches!(
        join_info,
        crate::storage::cloud::CloudHomeJoinInfo::S3 {
            endpoint: Some(_),
            ..
        }
    );
    if immutable_copy_capabilities_supported(&provider, custom_endpoint, custom_s3_immutable_copies)
    {
        Ok(())
    } else {
        Err(provider)
    }
}

pub(crate) fn require_immutable_copy_capabilities_home(
    home: std::sync::Arc<dyn super::CloudHome>,
    provider: Option<CloudProvider>,
) -> Result<(), StorageSetupError> {
    if home.immutable_copy_storage().is_some() {
        Ok(())
    } else if let Some(provider) = provider {
        Err(StorageSetupError::ImmutableCopiesUnavailable { provider })
    } else {
        Err(super::CloudHomeError::Configuration(
            "sync requires a cloud provider with immutable-copy storage".to_string(),
        )
        .into())
    }
}

fn immutable_copy_capabilities_supported(
    provider: &CloudProvider,
    custom_s3_endpoint: bool,
    custom_s3_immutable_copies: Option<crate::config::CustomS3ImmutableCopies>,
) -> bool {
    match provider {
        CloudProvider::S3 if !custom_s3_endpoint => true,
        CloudProvider::S3 => custom_s3_immutable_copies
            == Some(
                crate::config::CustomS3ImmutableCopies::ConditionalCreateStrongReadsCompleteListing,
            ),
        CloudProvider::GoogleDrive
        | CloudProvider::Dropbox
        | CloudProvider::OneDrive
        | CloudProvider::CloudKit => true,
    }
}

/// Build the [`CloudCipher`] a store's config selects: an opaque home seals
/// every object under the keyring's store key; a browsable home
/// (`cloud_home.storage == Browsable`) stores objects in the clear.
///
/// A browsable home has no store key, so it never reads the keyring — the
/// absence of a key there is expected, not an error.
pub(crate) fn build_cloud_cipher(
    config: &Config,
    custody: &dyn MasterKeyCustody,
) -> Result<CloudCipher, StorageSetupError> {
    if config.cloud_home.storage.is_browsable() {
        return Ok(CloudCipher::Plaintext);
    }
    let keyring = custody
        .unlock()?
        .ok_or(StorageSetupError::NoEncryptionKey)?;
    Ok(CloudCipher::Encrypted(keyring.into()))
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
pub(crate) async fn create_sync_storage_with_cloudkit(
    config: &Config,
    key_service: &StoreKeys,
    custody: &dyn MasterKeyCustody,
    identity_custody: &dyn DeviceIdentityCustody,
    cipher: Option<CloudCipher>,
    clock: crate::clock::ClockRef,
    cloudkit_ops: Option<std::sync::Arc<dyn super::cloudkit::CloudKitOps>>,
) -> Result<crate::sync::cloud_storage::CloudSyncStorage, StorageSetupError> {
    require_immutable_copy_capabilities_config(config)?;
    let cloud_home =
        super::create_cloud_home_with_cloudkit(config, key_service, clock, cloudkit_ops.clone())
            .await?;
    let coordination = match config.cloud_home.provider.as_ref() {
        Some(CloudProvider::S3) if serial_coordination_eligible(config) => {
            s3_coordination(config, key_service).await?
        }
        Some(CloudProvider::CloudKit) if serial_coordination_eligible(config) => {
            cloudkit_coordination(config, cloudkit_ops.clone())?
        }
        _ => None,
    };
    let storage = create_sync_storage_with_home(
        config,
        custody,
        identity_custody,
        Arc::from(cloud_home),
        cipher,
    )?;
    Ok(match coordination {
        Some((primary, peer)) => storage.with_serial_coordination_clients(primary, peer),
        None => storage,
    })
}

pub(crate) type CoordinationClients = (
    Arc<dyn crate::storage::cloud::CloudHeadStorage>,
    Arc<dyn crate::storage::cloud::CloudHeadStorage>,
);

fn cloudkit_coordination(
    config: &Config,
    ops: Option<Arc<dyn super::cloudkit::CloudKitOps>>,
) -> Result<Option<CoordinationClients>, StorageSetupError> {
    if config.cloud_home.provider != Some(CloudProvider::CloudKit) {
        return Ok(None);
    }
    let ops = ops.ok_or_else(|| {
        super::CloudHomeError::Configuration("CloudKit driver not provided".to_string())
    })?;
    let (primary, peer) = match (
        config.cloud_home.cloudkit_owner_name.as_ref(),
        config.cloud_home.cloudkit_zone_name.as_ref(),
    ) {
        (None, None) => (
            super::cloudkit::CloudKitCloudHome::new_private(ops.clone()),
            super::cloudkit::CloudKitCloudHome::new_private(ops),
        ),
        (Some(owner), Some(zone)) => (
            super::cloudkit::CloudKitCloudHome::new_shared(
                ops.clone(),
                owner.clone(),
                zone.clone(),
            ),
            super::cloudkit::CloudKitCloudHome::new_shared(ops, owner.clone(), zone.clone()),
        ),
        _ => {
            return Err(StorageSetupError::CloudHome(
                super::CloudHomeError::Configuration(
                    "CloudKit share config requires both cloudkit_owner_name and cloudkit_zone_name"
                        .to_string(),
                ),
            ));
        }
    };
    Ok(Some((Arc::new(primary), Arc::new(peer))))
}

async fn s3_coordination(
    config: &Config,
    key_service: &StoreKeys,
) -> Result<Option<CoordinationClients>, StorageSetupError> {
    if config.cloud_home.provider != Some(CloudProvider::S3) {
        return Ok(None);
    }
    let bucket = config.cloud_home.s3_bucket.clone().ok_or_else(|| {
        super::CloudHomeError::Configuration("S3 bucket not configured".to_string())
    })?;
    let region = config.cloud_home.s3_region.clone().ok_or_else(|| {
        super::CloudHomeError::Configuration("S3 region not configured".to_string())
    })?;
    let credentials = key_service
        .get_cloud_home_credentials()
        .map_err(StorageSetupError::Key)?
        .ok_or_else(|| {
            super::CloudHomeError::Configuration("S3 credentials not in keyring".to_string())
        })?;
    let crate::keys::CloudHomeCredentials::S3 {
        access_key,
        secret_key,
    } = credentials
    else {
        return Err(StorageSetupError::CloudHome(
            super::CloudHomeError::Configuration(
                "configured cloud credentials are not S3 credentials".to_string(),
            ),
        ));
    };
    let (primary, peer) = super::s3::S3CloudHome::new_pair(
        bucket,
        region,
        config.cloud_home.s3_endpoint.clone(),
        access_key,
        secret_key,
        config.cloud_home.s3_key_prefix.clone(),
        config.cloud_home.s3_immutable_copies,
    )
    .await?;
    Ok(Some((Arc::new(primary), Arc::new(peer))))
}

pub(crate) fn require_serial_coordination_config(
    config: &Config,
    write_policy: crate::WritePolicy,
) -> Result<(), StorageSetupError> {
    if write_policy != crate::WritePolicy::Serial || serial_coordination_eligible(config) {
        return Ok(());
    }
    Err(StorageSetupError::SerialCoordinationUnavailable {
        provider: config.cloud_home.provider.clone().ok_or_else(|| {
            super::CloudHomeError::Configuration(
                "Serial coordination requires a configured provider".to_string(),
            )
        })?,
    })
}

pub(crate) fn require_serial_coordination_join_info(
    join_info: &crate::storage::cloud::CloudHomeJoinInfo,
    custom_s3_serial: Option<crate::config::CustomS3Serial>,
    write_policy: crate::WritePolicy,
) -> Result<(), CloudProvider> {
    if write_policy != crate::WritePolicy::Serial {
        return Ok(());
    }
    let supported = serial_coordination_supported(
        &join_info.cloud_provider(),
        matches!(
            join_info,
            crate::storage::cloud::CloudHomeJoinInfo::S3 {
                endpoint: Some(_),
                ..
            }
        ),
        custom_s3_serial,
    );
    if supported {
        Ok(())
    } else {
        Err(join_info.cloud_provider())
    }
}

fn serial_coordination_eligible(config: &Config) -> bool {
    config.cloud_home.provider.as_ref().is_some_and(|provider| {
        serial_coordination_supported(
            provider,
            config.cloud_home.s3_endpoint.is_some(),
            config.cloud_home.s3_serial,
        )
    })
}

fn serial_coordination_supported(
    provider: &CloudProvider,
    custom_s3_endpoint: bool,
    custom_s3_serial: Option<crate::config::CustomS3Serial>,
) -> bool {
    match provider {
        CloudProvider::CloudKit => true,
        CloudProvider::S3 if !custom_s3_endpoint => true,
        CloudProvider::S3 => {
            custom_s3_serial == Some(crate::config::CustomS3Serial::ConditionalPutAndStrongReads)
        }
        CloudProvider::GoogleDrive | CloudProvider::Dropbox | CloudProvider::OneDrive => false,
    }
}

/// Create sync storage over an already-built [`CloudHome`](super::CloudHome).
/// Unlike [`create_sync_storage_with_cloudkit`], this needs no store-scoped
/// cloud credentials (the home is already built) — only custody, for the
/// `cipher = None` fallback's master-key read.
pub(crate) fn create_sync_storage_with_home(
    config: &Config,
    custody: &dyn MasterKeyCustody,
    identity_custody: &dyn DeviceIdentityCustody,
    home: Arc<dyn super::CloudHome>,
    cipher: Option<CloudCipher>,
) -> Result<crate::sync::cloud_storage::CloudSyncStorage, StorageSetupError> {
    require_immutable_copy_capabilities_home(home.clone(), config.cloud_home.provider.clone())?;
    let cipher = match cipher {
        Some(c) => c,
        None => build_cloud_cipher(config, custody)?,
    };

    // This store's signing identity, used to sign the control objects the
    // storage writes (its head, the min_schema floor) so a reader can attribute
    // and verify them against the membership chain. A connect path, so this
    // never mints: an unestablished identity fails with
    // `KeyError::NoDeviceIdentity` rather than forging one.
    let keypair = crate::keys::require_identity(identity_custody)?;

    Ok(crate::sync::cloud_storage::CloudSyncStorage::new(
        home,
        cipher,
        BlobPathScheme::for_storage(config.cloud_home.storage),
        config.store_id.clone(),
        keypair,
        std::sync::Arc::new(crate::storage::cloud::RandomCopyIdGenerator),
    )?)
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

    fn membership_floor(author_pubkey: String) -> Vec<crate::sync::membership::MembershipCoord> {
        vec![crate::sync::membership::MembershipCoord {
            author_pubkey,
            author_owner_grant: crate::sync::membership::MembershipGrantId(
                crate::sync::store_commit::ObjectHash::digest(b"restore test owner grant"),
            ),
            stream_id: "00000000000000000000000000000001"
                .parse()
                .expect("canonical test author stream id"),
            seq: 1,
            entry_hash: crate::sync::store_commit::ObjectHash::digest(
                b"restore test founder entry",
            ),
        }]
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

    #[test]
    fn serial_coordination_provider_matrix_is_explicit() {
        let mut config = Config::with_defaults(
            "store-1".to_string(),
            "device-1".to_string(),
            StoreDir::new("unused-store-dir"),
            "Provider matrix".to_string(),
        );

        config.cloud_home.provider = Some(CloudProvider::CloudKit);
        assert!(serial_coordination_eligible(&config));

        config.cloud_home.provider = Some(CloudProvider::S3);
        config.cloud_home.s3_endpoint = None;
        assert!(serial_coordination_eligible(&config));

        config.cloud_home.s3_endpoint = Some("https://objects.example".to_string());
        config.cloud_home.s3_serial = None;
        assert!(!serial_coordination_eligible(&config));
        config.cloud_home.s3_serial =
            Some(crate::config::CustomS3Serial::ConditionalPutAndStrongReads);
        assert!(serial_coordination_eligible(&config));

        for provider in [
            CloudProvider::GoogleDrive,
            CloudProvider::Dropbox,
            CloudProvider::OneDrive,
        ] {
            config.cloud_home.provider = Some(provider);
            assert!(!serial_coordination_eligible(&config));
        }
    }

    #[test]
    fn immutable_copy_admission_is_universal_and_uses_local_s3_assertions() {
        let mut config = Config::with_defaults(
            "store-1".to_string(),
            "device-1".to_string(),
            StoreDir::new("unused-store-dir"),
            "Provider matrix".to_string(),
        );

        config.cloud_home.provider = Some(CloudProvider::S3);
        config.cloud_home.s3_endpoint = None;
        assert!(require_immutable_copy_capabilities_config(&config).is_ok());

        config.cloud_home.s3_endpoint = Some("https://objects.example".to_string());
        assert!(matches!(
            require_immutable_copy_capabilities_config(&config),
            Err(StorageSetupError::ImmutableCopiesUnavailable {
                provider: CloudProvider::S3
            })
        ));
        config.cloud_home.s3_immutable_copies =
            Some(crate::CustomS3ImmutableCopies::ConditionalCreateStrongReadsCompleteListing);
        assert!(require_immutable_copy_capabilities_config(&config).is_ok());

        for provider in [
            CloudProvider::GoogleDrive,
            CloudProvider::Dropbox,
            CloudProvider::OneDrive,
            CloudProvider::CloudKit,
        ] {
            config.cloud_home.provider = Some(provider.clone());
            assert!(require_immutable_copy_capabilities_config(&config).is_ok());
        }
    }

    #[test]
    fn custom_s3_join_requires_the_local_immutable_copy_assertion() {
        let join_info = CloudHomeJoinInfo::S3 {
            bucket: "bucket".to_string(),
            region: "region".to_string(),
            endpoint: Some("https://objects.example".to_string()),
            access_key: "access".to_string(),
            secret_key: "secret".to_string(),
            key_prefix: None,
        };

        assert_eq!(
            require_immutable_copy_capabilities_join_info(&join_info, None),
            Err(CloudProvider::S3),
        );
        assert!(require_immutable_copy_capabilities_join_info(
            &join_info,
            Some(crate::CustomS3ImmutableCopies::ConditionalCreateStrongReadsCompleteListing),
        )
        .is_ok());
    }

    #[test]
    fn custom_s3_immutable_assertion_stays_out_of_restore_wire() {
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
        config.cloud_home.s3_immutable_copies =
            Some(crate::CustomS3ImmutableCopies::ConditionalCreateStrongReadsCompleteListing);
        let key_service = StoreKeys::new(config.store_id.clone());
        key_service
            .set_cloud_home_credentials(&crate::keys::CloudHomeCredentials::S3 {
                access_key: "access".to_string(),
                secret_key: "secret".to_string(),
            })
            .expect("store credentials");
        let custody =
            crate::custody::KeyCustody::InMemory(crate::encryption::MasterKeyring::generate())
                .resolve(&config.store_id, &config.store_dir);
        let identity_custody = crate::identity_custody::IdentityCustody::InMemory(
            crate::keys::UserKeypair::generate(),
        )
        .resolve(&config.store_id, &config.store_dir);

        let encoded = generate_restore_code(
            &config,
            &key_service,
            custody.as_ref(),
            identity_custody.as_ref(),
            crate::sync::store_commit::ObjectHash::digest(b"wire test root"),
            hex::encode([7u8; 32]),
            crate::join_code::MembershipFloor::MergeConcurrent(membership_floor(hex::encode(
                [7u8; 32],
            ))),
        )
        .expect("generate restore code");
        let decoded = decode_restore_code(&encoded).expect("decode restore code");
        let provider_wire = serde_json::to_string(&decoded.provider).expect("serialize provider");

        assert!(!provider_wire.contains("s3_immutable_copies"));
        assert!(!provider_wire.contains("strong_reads"));
        assert_eq!(
            config.cloud_home.s3_immutable_copies,
            Some(crate::CustomS3ImmutableCopies::ConditionalCreateStrongReadsCompleteListing),
        );
    }

    #[test]
    fn serial_join_provider_matrix_requires_the_local_custom_s3_assertion() {
        let custom_s3 = CloudHomeJoinInfo::S3 {
            bucket: "bucket".to_string(),
            region: "region".to_string(),
            endpoint: Some("https://objects.example".to_string()),
            access_key: "access".to_string(),
            secret_key: "secret".to_string(),
            key_prefix: None,
        };
        assert_eq!(
            require_serial_coordination_join_info(&custom_s3, None, crate::WritePolicy::Serial,),
            Err(CloudProvider::S3),
        );
        assert!(require_serial_coordination_join_info(
            &custom_s3,
            Some(crate::CustomS3Serial::ConditionalPutAndStrongReads),
            crate::WritePolicy::Serial,
        )
        .is_ok());
        assert!(require_serial_coordination_join_info(
            &CloudHomeJoinInfo::GoogleDrive {
                folder_id: "folder".to_string(),
            },
            None,
            crate::WritePolicy::Serial,
        )
        .is_err());
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
        let key_service = StoreKeys::new(config.store_id.clone());
        let custody =
            crate::custody::KeyCustody::Keyring.resolve(&config.store_id, &config.store_dir);
        let identity_custody = crate::identity_custody::IdentityCustody::InMemory(
            crate::keys::UserKeypair::generate(),
        )
        .resolve(&config.store_id, &config.store_dir);

        let err = generate_restore_code(
            &config,
            &key_service,
            custody.as_ref(),
            identity_custody.as_ref(),
            crate::sync::store_commit::ObjectHash::digest(b"restore test store protocol root"),
            hex::encode([7u8; 32]),
            crate::join_code::MembershipFloor::MergeConcurrent(membership_floor(hex::encode(
                [7u8; 32],
            ))),
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
        let key_service = StoreKeys::new(config.store_id.clone());
        let custody =
            crate::custody::KeyCustody::Keyring.resolve(&config.store_id, &config.store_dir);
        let identity_custody = crate::identity_custody::IdentityCustody::InMemory(
            crate::keys::UserKeypair::generate(),
        )
        .resolve(&config.store_id, &config.store_dir);

        let code = generate_restore_code(
            &config,
            &key_service,
            custody.as_ref(),
            identity_custody.as_ref(),
            crate::sync::store_commit::ObjectHash::digest(b"restore test store protocol root"),
            hex::encode([7u8; 32]),
            crate::join_code::MembershipFloor::MergeConcurrent(membership_floor(hex::encode(
                [7u8; 32],
            ))),
        )
        .expect("a private CloudKit config generates a restore code");
        let decoded = decode_restore_code(&code).expect("generated code decodes");
        assert!(
            matches!(decoded.provider, CloudHomeJoinInfo::CloudKit),
            "expected CloudKit provider, got {:?}",
            decoded.provider
        );
    }

    #[test]
    fn generate_restore_code_round_trips_an_empty_serial_store_floor() {
        crate::keys::test_keyring::install();

        let config = cloudkit_config(None);
        let key_service = StoreKeys::new(config.store_id.clone());
        let custody =
            crate::custody::KeyCustody::Keyring.resolve(&config.store_id, &config.store_dir);
        let identity_custody = crate::identity_custody::IdentityCustody::InMemory(
            crate::keys::UserKeypair::generate(),
        )
        .resolve(&config.store_id, &config.store_dir);

        let code = generate_restore_code(
            &config,
            &key_service,
            custody.as_ref(),
            identity_custody.as_ref(),
            crate::sync::store_commit::ObjectHash::digest(b"empty Serial restore root"),
            hex::encode([7u8; 32]),
            crate::join_code::MembershipFloor::Serial(None),
        )
        .expect("mint empty Serial restore code");
        let decoded = decode_restore_code(&code).expect("decode empty Serial restore code");
        assert_eq!(
            decoded.membership_floor,
            crate::join_code::MembershipFloor::Serial(None)
        );
    }
}

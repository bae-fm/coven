use super::{cloudkit, CloudHome, CloudHomeError};

#[cfg(feature = "oauth-providers")]
fn require_oauth_token(
    key_service: &crate::keys::StoreKeys,
    provider_name: &str,
) -> Result<String, CloudHomeError> {
    match key_service.get_cloud_home_credentials().map_err(|error| {
        CloudHomeError::Configuration(format!("{provider_name} credentials error: {error}"))
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
    serde_json::from_str(&token_json).map_err(|error| {
        CloudHomeError::Configuration(format!("invalid OAuth token JSON: {error}"))
    })
}

pub async fn create_cloud_home(
    config: &crate::config::Config,
    key_service: &crate::keys::StoreKeys,
    oauth_clients: &crate::oauth::OAuthClients,
    clock: crate::clock::ClockRef,
) -> Result<Box<dyn CloudHome>, CloudHomeError> {
    create_cloud_home_with_cloudkit(config, key_service, oauth_clients, clock, None).await
}

pub(crate) async fn create_cloud_home_with_cloudkit(
    config: &crate::config::Config,
    key_service: &crate::keys::StoreKeys,
    oauth_clients: &crate::oauth::OAuthClients,
    clock: crate::clock::ClockRef,
    cloudkit_ops: Option<std::sync::Arc<dyn cloudkit::CloudKitOps>>,
) -> Result<Box<dyn CloudHome>, CloudHomeError> {
    use crate::config::CloudProvider;

    #[cfg(not(feature = "oauth-providers"))]
    let _ = (&clock, oauth_clients);

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
                match key_service.get_cloud_home_credentials().map_err(|error| {
                    CloudHomeError::Configuration(format!("S3 credentials error: {error}"))
                })? {
                    Some(crate::keys::CloudHomeCredentials::S3 {
                        access_key,
                        secret_key,
                    }) => (access_key, secret_key),
                    _ => {
                        return Err(CloudHomeError::Configuration(
                            "S3 credentials not in keyring".to_string(),
                        ));
                    }
                };

            let s3 = super::s3::S3CloudHome::new(
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
            let oauth_config = oauth_clients
                .config_for(CloudProvider::GoogleDrive)
                .map_err(|error| CloudHomeError::Configuration(error.to_string()))?;
            Ok(Box::new(super::google_drive::GoogleDriveCloudHome::new(
                folder_id,
                oauth_config,
                tokens,
                key_service.clone(),
                clock,
            )))
        }
        #[cfg(feature = "oauth-providers")]
        Some(CloudProvider::Dropbox) => {
            let folder_path = config
                .cloud_home
                .dropbox_folder_path
                .clone()
                .ok_or_else(|| {
                    CloudHomeError::Configuration("Dropbox folder path not configured".to_string())
                })?;
            let tokens = parse_oauth_tokens(key_service, "Dropbox")?;
            let oauth_config = oauth_clients
                .config_for(CloudProvider::Dropbox)
                .map_err(|error| CloudHomeError::Configuration(error.to_string()))?;
            Ok(Box::new(super::dropbox::DropboxCloudHome::new(
                folder_path,
                oauth_config,
                tokens,
                key_service.clone(),
                clock,
            )))
        }
        #[cfg(feature = "oauth-providers")]
        Some(CloudProvider::OneDrive) => {
            let drive_id = config.cloud_home.onedrive_drive_id.clone().ok_or_else(|| {
                CloudHomeError::Configuration("OneDrive drive ID not configured".to_string())
            })?;
            let folder_id = config
                .cloud_home
                .onedrive_folder_id
                .clone()
                .ok_or_else(|| {
                    CloudHomeError::Configuration("OneDrive folder ID not configured".to_string())
                })?;
            let tokens = parse_oauth_tokens(key_service, "OneDrive")?;
            let oauth_config = oauth_clients
                .config_for(CloudProvider::OneDrive)
                .map_err(|error| CloudHomeError::Configuration(error.to_string()))?;
            Ok(Box::new(super::onedrive::OneDriveCloudHome::new(
                drive_id,
                folder_id,
                oauth_config,
                tokens,
                key_service.clone(),
                clock,
            )))
        }
        #[cfg(not(feature = "oauth-providers"))]
        Some(CloudProvider::GoogleDrive | CloudProvider::Dropbox | CloudProvider::OneDrive) => {
            Err(CloudHomeError::Configuration(
                "OAuth cloud providers are not supported in this build".to_string(),
            ))
        }
        Some(CloudProvider::CloudKit) => {
            let ops = cloudkit_ops.ok_or_else(|| {
                CloudHomeError::Configuration("CloudKit driver not provided".to_string())
            })?;
            match (
                config.cloud_home.cloudkit_owner_name.as_ref(),
                config.cloud_home.cloudkit_zone_name.as_ref(),
            ) {
                (None, None) => Ok(Box::new(cloudkit::CloudKitCloudHome::new_private(ops))),
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
mod tests;

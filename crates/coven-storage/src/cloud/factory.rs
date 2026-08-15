use super::{cloudkit, CloudHomeError, ExactCloudHome};

use std::sync::Arc;

use coven_keys::keys::{CloudHomeCredentialCustody, CloudHomeCredentials};

#[derive(Clone)]
pub struct CloudHomeFactory {
    oauth_clients: crate::oauth::OAuthClients,
}

#[cfg(feature = "oauth-providers")]
pub struct PreparedOAuthCloudHome {
    pub cloud_home: coven_foundation::config::CloudHomeConfig,
    pub credentials: CloudHomeCredentials,
}

impl CloudHomeFactory {
    pub fn new(oauth_clients: crate::oauth::OAuthClients) -> Self {
        Self { oauth_clients }
    }

    #[cfg(feature = "oauth-providers")]
    pub async fn prepare_oauth_cloud_home(
        &self,
        mut cloud_home: coven_foundation::config::CloudHomeConfig,
        store_name: &str,
        cancel: tokio::sync::watch::Receiver<bool>,
        clock: &dyn coven_foundation::clock::Clock,
    ) -> Result<PreparedOAuthCloudHome, crate::cloud::SetupError> {
        use crate::oauth::OAuthCloudHomeLocation;
        use coven_foundation::config::CloudProvider;

        let prepared = match cloud_home.provider {
            Some(CloudProvider::GoogleDrive) => {
                self.oauth_clients
                    .prepare_google_drive(store_name, cancel, clock)
                    .await?
            }
            Some(CloudProvider::Dropbox) => {
                self.oauth_clients
                    .prepare_dropbox(store_name, cancel, clock)
                    .await?
            }
            Some(CloudProvider::OneDrive) => {
                self.oauth_clients.prepare_onedrive(cancel, clock).await?
            }
            Some(provider) => {
                return Err(crate::cloud::SetupError::Configuration(format!(
                    "provider {provider:?} does not use OAuth"
                )))
            }
            None => {
                return Err(crate::cloud::SetupError::Configuration(
                    "OAuth cloud-home setup requires a provider".to_string(),
                ))
            }
        };
        match prepared.location {
            OAuthCloudHomeLocation::GoogleDrive { folder_id } => {
                cloud_home.google_drive_folder_id = Some(folder_id);
            }
            OAuthCloudHomeLocation::Dropbox { folder_path } => {
                cloud_home.dropbox_folder_path = Some(folder_path);
            }
            OAuthCloudHomeLocation::OneDrive {
                drive_id,
                folder_id,
            } => {
                cloud_home.onedrive_drive_id = Some(drive_id);
                cloud_home.onedrive_folder_id = Some(folder_id);
            }
        }
        Ok(PreparedOAuthCloudHome {
            cloud_home,
            credentials: CloudHomeCredentials::OAuth {
                tokens: prepared.tokens,
            },
        })
    }

    pub async fn create(
        &self,
        config: &coven_foundation::config::Config,
        clock: coven_foundation::clock::ClockRef,
        cloudkit_ops: Option<std::sync::Arc<dyn cloudkit::CloudKitOps>>,
        credential_custody: Arc<dyn CloudHomeCredentialCustody>,
    ) -> Result<Box<dyn ExactCloudHome>, CloudHomeError> {
        use coven_foundation::config::CloudProvider;

        #[cfg(not(feature = "oauth-providers"))]
        let _ = (&clock, &self.oauth_clients);

        #[cfg(feature = "oauth-providers")]
        let oauth_tokens = |provider_name: &str| {
            credential_custody
                .unlock()
                .map_err(|error| {
                    CloudHomeError::configuration(
                        format!("read {provider_name} credentials"),
                        error,
                    )
                })?
                .and_then(|credentials| match credentials {
                    CloudHomeCredentials::OAuth { tokens } => Some(tokens),
                    CloudHomeCredentials::S3 { .. } => None,
                })
                .ok_or_else(|| {
                    CloudHomeError::Configuration(format!(
                        "{provider_name} OAuth token not in keyring"
                    ))
                })
        };

        match config.cloud_home.provider {
            Some(CloudProvider::S3) | None => {
                let bucket = config.cloud_home.s3_bucket.clone().ok_or_else(|| {
                    CloudHomeError::Configuration("S3 bucket not configured".to_string())
                })?;
                let region = config.cloud_home.s3_region.clone().ok_or_else(|| {
                    CloudHomeError::Configuration("S3 region not configured".to_string())
                })?;
                let endpoint = config.cloud_home.s3_endpoint.clone();

                let (access_key, secret_key) = match credential_custody
                    .unlock()
                    .map_err(|error| CloudHomeError::configuration("read S3 credentials", error))?
                {
                    Some(CloudHomeCredentials::S3 {
                        access_key,
                        secret_key,
                    }) => (access_key, secret_key),
                    _ => {
                        return Err(CloudHomeError::Configuration(
                            "S3 credentials not in keyring".to_string(),
                        ));
                    }
                };

                let s3 = super::s3::open_cloud_home(
                    bucket,
                    region,
                    endpoint,
                    access_key,
                    secret_key,
                    config.cloud_home.s3_key_prefix.clone(),
                    config.cloud_home.exact_upload_verification,
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
                let tokens = oauth_tokens("Google Drive")?;
                let oauth_config = self
                    .oauth_clients
                    .config_for(CloudProvider::GoogleDrive)
                    .map_err(|error| {
                        CloudHomeError::configuration(
                            "read Google Drive OAuth configuration",
                            error,
                        )
                    })?;
                let session = super::oauth_session::OAuthSession::new(
                    tokens,
                    credential_custody.clone(),
                    clock,
                    oauth_config,
                    "Google Drive",
                );
                Ok(Box::new(super::google_drive::GoogleDriveCloudHome::new(
                    folder_id,
                    session,
                    config.cloud_home.exact_upload_verification,
                )))
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
                let tokens = oauth_tokens("Dropbox")?;
                let oauth_config = self
                    .oauth_clients
                    .config_for(CloudProvider::Dropbox)
                    .map_err(|error| {
                        CloudHomeError::configuration("read Dropbox OAuth configuration", error)
                    })?;
                let session = super::oauth_session::OAuthSession::new(
                    tokens,
                    credential_custody.clone(),
                    clock,
                    oauth_config,
                    "Dropbox",
                );
                Ok(Box::new(super::dropbox::DropboxCloudHome::new(
                    folder_path,
                    session,
                    config.cloud_home.exact_upload_verification,
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
                        CloudHomeError::Configuration(
                            "OneDrive folder ID not configured".to_string(),
                        )
                    })?;
                let tokens = oauth_tokens("OneDrive")?;
                let oauth_config = self
                    .oauth_clients
                    .config_for(CloudProvider::OneDrive)
                    .map_err(|error| {
                        CloudHomeError::configuration("read OneDrive OAuth configuration", error)
                    })?;
                let session = super::oauth_session::OAuthSession::new(
                    tokens,
                    credential_custody.clone(),
                    clock,
                    oauth_config,
                    "OneDrive",
                );
                Ok(Box::new(super::onedrive::OneDriveCloudHome::new(
                    drive_id,
                    folder_id,
                    session,
                    config.cloud_home.exact_upload_verification,
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
                    (None, None) => Ok(Box::new(cloudkit::CloudKitCloudHome::new_private(
                        ops,
                        config.cloud_home.exact_upload_verification,
                    ))),
                    (Some(owner_name), Some(zone_name)) => {
                        Ok(Box::new(cloudkit::CloudKitCloudHome::new_shared(
                            ops,
                            owner_name.clone(),
                            zone_name.clone(),
                            config.cloud_home.exact_upload_verification,
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
}

#[cfg(test)]
mod tests;

use super::*;

/// Provider namespace/corpus facts signed once by the Store root.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "provider", rename_all = "snake_case", deny_unknown_fields)]
pub enum StoreProviderBinding {
    S3 {
        endpoint: S3EndpointBinding,
        region: String,
        bucket: String,
        key_prefix: Option<String>,
    },
    GoogleDrive {
        corpus: GoogleDriveCorpus,
    },
    Dropbox {
        namespace_id: String,
    },
    OneDrive {
        drive_id: String,
        folder_id: String,
    },
    CloudKit {
        container_id: String,
        environment: CloudKitEnvironment,
        owner_name: String,
        zone_name: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum S3EndpointBinding {
    Aws { partition: String },
    Custom { origin: String },
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "corpus", rename_all = "snake_case", deny_unknown_fields)]
pub enum GoogleDriveCorpus {
    MyDrive { folder_id: String },
    SharedDrive { drive_id: String, folder_id: String },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CloudKitEnvironment {
    Development,
    Production,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "provider", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProviderPrincipalId {
    Aws {
        account_id: String,
        principal: AwsPrincipal,
    },
    CustomS3Credential {
        access_key_id_hash: ObjectHash,
    },
    GoogleDrive {
        permission_id: String,
    },
    Dropbox {
        account_id: String,
    },
    OneDrive {
        user_id: String,
    },
    CloudKitPrivateZoneOwner {
        record_name: String,
    },
    CloudKitSharedZoneParticipant {
        record_name: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AwsPrincipal {
    Root,
    User { arn: String, user_id: String },
    Role { role_id: String },
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderDeviceBinding {
    pub principal: ProviderPrincipalId,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedProviderBinding {
    pub store: StoreProviderBinding,
    pub device: ProviderDeviceBinding,
}

impl StoreProviderBinding {
    pub fn validate(&self) -> Result<(), StorageError> {
        fn present(label: &str, value: &str) -> Result<(), StorageError> {
            if value.is_empty() {
                Err(StorageError::Configuration(format!("{label} is empty")))
            } else {
                Ok(())
            }
        }

        match self {
            Self::S3 {
                endpoint,
                region,
                bucket,
                key_prefix,
            } => {
                present("S3 region", region)?;
                present("S3 bucket", bucket)?;
                if key_prefix.as_deref().is_some_and(str::is_empty) {
                    return Err(StorageError::Configuration(
                        "S3 key prefix is empty instead of absent".to_string(),
                    ));
                }
                match endpoint {
                    S3EndpointBinding::Aws { partition } => present("AWS partition", partition),
                    S3EndpointBinding::Custom { origin } => {
                        let canonical = crate::provider::canonical_custom_s3_origin(origin)?;
                        if canonical != *origin {
                            return Err(StorageError::Configuration(
                                "custom S3 origin is not canonical".to_string(),
                            ));
                        }
                        Ok(())
                    }
                }
            }
            Self::GoogleDrive { corpus } => match corpus {
                GoogleDriveCorpus::MyDrive { folder_id } => {
                    present("Google Drive folder id", folder_id)
                }
                GoogleDriveCorpus::SharedDrive {
                    drive_id,
                    folder_id,
                } => {
                    present("Google Drive id", drive_id)?;
                    present("Google Drive folder id", folder_id)
                }
            },
            Self::Dropbox { namespace_id } => present("Dropbox namespace id", namespace_id),
            Self::OneDrive {
                drive_id,
                folder_id,
            } => {
                present("OneDrive drive id", drive_id)?;
                present("OneDrive folder id", folder_id)
            }
            Self::CloudKit {
                container_id,
                owner_name,
                zone_name,
                ..
            } => {
                present("CloudKit container id", container_id)?;
                present("CloudKit owner name", owner_name)?;
                present("CloudKit zone name", zone_name)
            }
        }
    }
}

impl ProviderDeviceBinding {
    pub fn validate_for(&self, store: &StoreProviderBinding) -> Result<(), StorageError> {
        fn present(label: &str, value: &str) -> Result<(), StorageError> {
            if value.is_empty() {
                Err(StorageError::Configuration(format!("{label} is empty")))
            } else {
                Ok(())
            }
        }

        let compatible = matches!(
            (store, &self.principal),
            (
                StoreProviderBinding::S3 {
                    endpoint: S3EndpointBinding::Aws { .. },
                    ..
                },
                ProviderPrincipalId::Aws { .. }
            ) | (
                StoreProviderBinding::S3 {
                    endpoint: S3EndpointBinding::Custom { .. },
                    ..
                },
                ProviderPrincipalId::CustomS3Credential { .. }
            ) | (
                StoreProviderBinding::GoogleDrive { .. },
                ProviderPrincipalId::GoogleDrive { .. }
            ) | (
                StoreProviderBinding::Dropbox { .. },
                ProviderPrincipalId::Dropbox { .. }
            ) | (
                StoreProviderBinding::OneDrive { .. },
                ProviderPrincipalId::OneDrive { .. }
            ) | (
                StoreProviderBinding::CloudKit { .. },
                ProviderPrincipalId::CloudKitPrivateZoneOwner { .. }
                    | ProviderPrincipalId::CloudKitSharedZoneParticipant { .. }
            )
        );
        if !compatible {
            return Err(StorageError::Configuration(
                "provider principal is incompatible with the Store provider binding".to_string(),
            ));
        }
        match &self.principal {
            ProviderPrincipalId::Aws {
                account_id,
                principal,
            } => {
                if account_id.len() != 12 || !account_id.bytes().all(|byte| byte.is_ascii_digit()) {
                    return Err(StorageError::Configuration(
                        "AWS account id must contain exactly 12 decimal digits".to_string(),
                    ));
                }
                match principal {
                    AwsPrincipal::Root => Ok(()),
                    AwsPrincipal::User { arn, user_id } => {
                        present("AWS user id", user_id)?;
                        let fields: Vec<_> = arn.splitn(6, ':').collect();
                        let StoreProviderBinding::S3 {
                            endpoint: S3EndpointBinding::Aws { partition },
                            ..
                        } = store
                        else {
                            return Err(StorageError::Configuration(
                                "AWS user principal is bound to non-AWS S3".to_string(),
                            ));
                        };
                        if fields.len() != 6
                            || fields[0] != "arn"
                            || fields[1] != partition
                            || fields[2] != "iam"
                            || !fields[3].is_empty()
                            || fields[4] != account_id
                            || !fields[5].starts_with("user/")
                            || fields[5].len() == "user/".len()
                        {
                            return Err(StorageError::Configuration(
                                "AWS IAM user ARN is malformed or differs from its Store binding"
                                    .to_string(),
                            ));
                        }
                        Ok(())
                    }
                    AwsPrincipal::Role { role_id } => {
                        present("AWS role id", role_id)?;
                        if role_id.contains(':') {
                            return Err(StorageError::Configuration(
                                "AWS role id must be the stable prefix before the session separator"
                                    .to_string(),
                            ));
                        }
                        Ok(())
                    }
                }
            }
            ProviderPrincipalId::CustomS3Credential { .. } => Ok(()),
            ProviderPrincipalId::GoogleDrive { permission_id } => {
                present("Google Drive permission id", permission_id)
            }
            ProviderPrincipalId::Dropbox { account_id } => {
                present("Dropbox account id", account_id)
            }
            ProviderPrincipalId::OneDrive { user_id } => present("OneDrive user id", user_id),
            ProviderPrincipalId::CloudKitPrivateZoneOwner { record_name } => {
                present("CloudKit private-zone owner record name", record_name)
            }
            ProviderPrincipalId::CloudKitSharedZoneParticipant { record_name } => {
                present("CloudKit shared-zone participant record name", record_name)
            }
        }
    }
}

impl ResolvedProviderBinding {
    pub fn validate(&self) -> Result<(), StorageError> {
        self.store.validate()?;
        self.device.validate_for(&self.store)
    }
}

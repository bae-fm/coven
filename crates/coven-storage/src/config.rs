use serde::{Deserialize, Serialize};

/// A provider supported by coven (§4).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CloudProvider {
    /// S3, including compatible providers.
    S3,
    /// Google Drive.
    GoogleDrive,
    /// Dropbox.
    Dropbox,
    /// OneDrive.
    OneDrive,
    /// iCloud through the app's CloudKit bridge.
    CloudKit,
}

impl CloudProvider {
    /// Instructions for cutting off a removed device's provider access (§13).
    /// This depends on the provider kind, not on a connected account or request.
    pub fn sign_out(self) -> crate::ProviderSignOut {
        match self {
            Self::S3 => crate::ProviderSignOut::ReplaceAccessKey,
            Self::CloudKit => crate::ProviderSignOut::RemoveFromAppleAccount,
            provider => crate::ProviderSignOut::RemoveAppAccess { provider },
        }
    }
}

/// The store's location; credentials are kept separately in key custody.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum StorageConfig {
    /// An existing S3 bucket and the prefix reserved for this store.
    S3 {
        /// Bucket name.
        bucket: String,
        /// Signing region.
        region: String,
        /// A compatible provider's endpoint, or AWS's regional endpoint.
        endpoint: Option<url::Url>,
        /// The store's prefix, without leading or trailing slashes.
        prefix: String,
    },
    /// The shared Google Drive folder.
    GoogleDrive {
        /// Folder id shared with members' Google accounts.
        folder_id: String,
    },
    /// A Dropbox shared folder namespace, independent of each member's mount path.
    Dropbox {
        /// The shared folder's namespace id.
        namespace_id: String,
    },
    /// The shared OneDrive folder.
    OneDrive {
        /// Drive containing the folder.
        drive_id: String,
        /// Folder shared with members' Microsoft accounts.
        folder_id: String,
    },
    /// The CloudKit zone reached by the app's bridge.
    CloudKit {
        /// App's CloudKit container.
        container: String,
        /// Owner's CloudKit record name.
        owner: String,
        /// Custom zone containing the store.
        zone: String,
    },
}

impl StorageConfig {
    /// The provider of this location.
    pub fn provider(&self) -> CloudProvider {
        match self {
            Self::S3 { .. } => CloudProvider::S3,
            Self::GoogleDrive { .. } => CloudProvider::GoogleDrive,
            Self::Dropbox { .. } => CloudProvider::Dropbox,
            Self::OneDrive { .. } => CloudProvider::OneDrive,
            Self::CloudKit { .. } => CloudProvider::CloudKit,
        }
    }

    /// Reject missing location information before making a request.
    pub fn validate(&self) -> Result<(), crate::StorageError> {
        let fields: Vec<&str> = match self {
            Self::S3 {
                bucket,
                region,
                endpoint,
                prefix,
            } => {
                if let Some(endpoint) = endpoint {
                    if !crate::web_url::is_web_url(endpoint)
                        || endpoint.query().is_some()
                        || endpoint.fragment().is_some()
                    {
                        return Err(
                            crate::StorageFailure::InvalidConfiguration.with_source("S3 endpoint")
                        );
                    }
                }
                crate::path::validate_root(prefix)?;
                vec![bucket, region]
            }
            Self::GoogleDrive { folder_id } => vec![folder_id],
            Self::Dropbox { namespace_id } => vec![namespace_id],
            Self::OneDrive {
                drive_id,
                folder_id,
            } => vec![drive_id, folder_id],
            Self::CloudKit {
                container,
                owner,
                zone,
            } => vec![container, owner, zone],
        };
        if fields
            .iter()
            .any(|value| value.trim().is_empty() || value.chars().any(char::is_control))
        {
            return Err(crate::StorageFailure::InvalidConfiguration
                .with_source("empty or invalid location"));
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "config_tests.rs"]
mod tests;

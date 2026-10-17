use serde::{Deserialize, Serialize};

/// A provider supported by coven (§20.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CloudProvider {
    /// S3, including compatible providers.
    S3,
    /// Dropbox.
    Dropbox,
    /// OneDrive.
    OneDrive,
    /// iCloud through coven-storage's native Apple-platform adapter.
    CloudKit,
}

impl CloudProvider {
    /// Instructions for cutting off a removed device's provider access (§14.2).
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
    /// An existing S3 bucket and a base prefix. Where the service's member
    /// keys can be limited to a prefix, the base prefix may hold several
    /// stores; where they reach a whole bucket (R2, and every custom or
    /// detected profile), the store has the bucket to itself (§20.7).
    S3 {
        /// Bucket name.
        bucket: String,
        /// Signing region.
        region: String,
        /// None is AWS's own endpoint. Catalogued hosts (Appendix E.3) require https.
        endpoint: Option<url::Url>,
        /// A catalogued service's name or custom settings; either must agree
        /// with a catalogued endpoint host. None selects by host:
        /// storage.googleapis.com is GCS XML, hosts ending in
        /// .r2.cloudflarestorage.com are R2, no endpoint or .amazonaws.com is
        /// AWS, and setup detects any other endpoint's settings, recording
        /// them as Custom. All run the setup checks.
        profile: Option<crate::S3Profile>,
        /// The base prefix, without leading or trailing slashes; paths add
        /// the store id. Empty where member keys reach a whole bucket;
        /// otherwise `OwnBucketRequired { reason: Prefix }`.
        prefix: String,
    },
    /// Dropbox's access and location, fixed when cloud storage is created.
    Dropbox {
        /// Required choice: not-shared App folder or a shared-capable namespace.
        location: crate::DropboxLocation,
    },
    /// A OneDrive folder; paths within it start with the store id.
    OneDrive {
        /// Drive containing the folder.
        drive_id: String,
        /// Folder shared with members' Microsoft accounts.
        folder_id: String,
    },
    /// The custom zone reached by the built-in CloudKit adapter.
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
    /// Access required to open this location.
    pub fn oauth_access(&self) -> Option<crate::OAuthAccess> {
        match self {
            Self::Dropbox { location } => Some(crate::OAuthAccess::Dropbox(location.access())),
            Self::OneDrive { .. } => Some(crate::OAuthAccess::OneDrive),
            Self::S3 { .. } | Self::CloudKit { .. } => None,
        }
    }
    /// The provider of this location.
    pub fn provider(&self) -> CloudProvider {
        match self {
            Self::S3 { .. } => CloudProvider::S3,
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
                profile,
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
                let scope = crate::providers::s3_key_scope(endpoint.as_ref(), profile.as_ref())?;
                crate::path::validate_root(prefix)?;
                // A key that reaches the whole bucket reaches every store in
                // it, so such a store occupies the bucket's root (§20.7).
                if scope == crate::providers::S3KeyScope::Bucket && !prefix.is_empty() {
                    return Err(crate::StorageFailure::OwnBucketRequired {
                        reason: crate::OwnBucketReason::Prefix,
                    }
                    .into());
                }
                vec![bucket, region]
            }
            Self::Dropbox {
                location: crate::DropboxLocation::AppFolder,
            } => vec![],
            Self::Dropbox {
                location: crate::DropboxLocation::SharedFolder { namespace_id },
            } => vec![namespace_id],
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

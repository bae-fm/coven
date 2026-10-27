use coven_format::code_fields::{FieldReader, FieldWriter};
use coven_format::Error as FieldError;
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
    /// None on S3: the member's other devices share the key, which stays theirs.
    pub fn sign_out(self) -> Option<crate::ProviderSignOut> {
        match self {
            Self::S3 => None,
            Self::CloudKit => Some(crate::ProviderSignOut::RemoveFromAppleAccount),
            provider => Some(crate::ProviderSignOut::RemoveAppAccess { provider }),
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
        /// The base prefix, parsed where the location is built; paths add
        /// the store id. Empty where member keys reach a whole bucket;
        /// otherwise `OwnBucketRequired { reason: Prefix }`.
        prefix: crate::S3Prefix,
    },
    /// Dropbox's access and location, fixed when cloud storage is created.
    Dropbox {
        /// Required choice: not-shared App folder or a shared-capable namespace.
        location: crate::DropboxLocation,
        /// Signed-in member's display label; absent before setup. Never routes or authorizes.
        account_label: Option<String>,
    },
    /// A OneDrive folder; paths within it start with the store id.
    OneDrive {
        /// Drive containing the folder.
        drive_id: String,
        /// Folder shared with members' Microsoft accounts.
        folder_id: String,
        /// Signed-in member's display label; absent before setup. Never routes or authorizes.
        account_label: Option<String>,
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
    /// Whether these settings reach the same location, ignoring display labels.
    pub fn same_location(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Dropbox { location: a, .. }, Self::Dropbox { location: b, .. }) => a == b,
            (
                Self::OneDrive {
                    drive_id: ad,
                    folder_id: af,
                    ..
                },
                Self::OneDrive {
                    drive_id: bd,
                    folder_id: bf,
                    ..
                },
            ) => ad == bd && af == bf,
            // These providers carry no account label.
            (Self::S3 { .. }, Self::S3 { .. }) | (Self::CloudKit { .. }, Self::CloudKit { .. }) => {
                self == other
            }
            _ => false,
        }
    }

    /// Access required to open this location.
    pub fn oauth_access(&self) -> Option<crate::OAuthAccess> {
        match self {
            Self::Dropbox { location, .. } => Some(crate::OAuthAccess::Dropbox(location.access())),
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
                ..
            } => vec![],
            Self::Dropbox {
                location: crate::DropboxLocation::SharedFolder { namespace_id },
                ..
            } => vec![namespace_id],
            Self::OneDrive {
                drive_id,
                folder_id,
                ..
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

// A location's fields in a code's storage part (format.md D13): the
// provider's tag, then that provider's own fields.
const S3: u8 = 0;
const DROPBOX: u8 = 1;
const ONEDRIVE: u8 = 2;
const CLOUDKIT: u8 = 3;

impl StorageConfig {
    /// Pack this location's fields into a code's storage part.
    pub(crate) fn pack(&self, out: &mut FieldWriter) -> Result<(), FieldError> {
        match self {
            Self::S3 {
                bucket,
                region,
                endpoint,
                profile,
                prefix,
            } => {
                out.u8(S3)?;
                out.text(bucket)?;
                out.text(region)?;
                out.option(endpoint.as_ref(), |out, url| out.text(url.as_str()))?;
                out.option(profile.as_ref(), |out, profile| profile.pack(out))?;
                out.text(prefix.as_str())
            }
            Self::Dropbox {
                location,
                account_label,
            } => {
                out.u8(DROPBOX)?;
                match location {
                    crate::DropboxLocation::AppFolder => out.u8(0)?,
                    crate::DropboxLocation::SharedFolder { namespace_id } => {
                        out.u8(1)?;
                        out.text(namespace_id)?;
                    }
                }
                out.option(account_label.as_deref(), FieldWriter::text)
            }
            Self::OneDrive {
                drive_id,
                folder_id,
                account_label,
            } => {
                out.u8(ONEDRIVE)?;
                out.text(drive_id)?;
                out.text(folder_id)?;
                out.option(account_label.as_deref(), FieldWriter::text)
            }
            Self::CloudKit {
                container,
                owner,
                zone,
            } => {
                out.u8(CLOUDKIT)?;
                out.text(container)?;
                out.text(owner)?;
                out.text(zone)
            }
        }
    }

    /// Read a location packed by [`Self::pack`], checked as `validate` checks
    /// one. A text written in a spelling `pack` never writes, such as an
    /// endpoint URL or base prefix that isn't canonical, is refused.
    pub(crate) fn unpack(input: &mut FieldReader<'_>) -> Result<Self, crate::StorageError> {
        let location = match input.u8()? {
            S3 => Self::S3 {
                bucket: input.text("bucket")?,
                region: input.text("region")?,
                endpoint: input
                    .option(|input| input.text("endpoint"))?
                    .map(|text| recorded_url(&text))
                    .transpose()?,
                profile: input.option(crate::S3Profile::unpack)?,
                prefix: crate::S3Prefix::recorded(&input.text("prefix")?)?,
            },
            DROPBOX => Self::Dropbox {
                location: match input.u8()? {
                    0 => crate::DropboxLocation::AppFolder,
                    1 => crate::DropboxLocation::SharedFolder {
                        namespace_id: input.text("namespace id")?,
                    },
                    tag => {
                        return Err(FieldError::UnknownTag {
                            field: "Dropbox location",
                            tag,
                        }
                        .into())
                    }
                },
                account_label: input.option(|input| input.text("account label"))?,
            },
            ONEDRIVE => Self::OneDrive {
                drive_id: input.text("drive id")?,
                folder_id: input.text("folder id")?,
                account_label: input.option(|input| input.text("account label"))?,
            },
            CLOUDKIT => Self::CloudKit {
                container: input.text("container")?,
                owner: input.text("owner")?,
                zone: input.text("zone")?,
            },
            tag => {
                return Err(FieldError::UnknownTag {
                    field: "provider",
                    tag,
                }
                .into())
            }
        };
        location.validate()?;
        Ok(location)
    }
}

/// An endpoint as `pack` writes it: the URL's own serialization, and no other.
fn recorded_url(text: &str) -> Result<url::Url, crate::StorageError> {
    let url = url::Url::parse(text)
        .map_err(|error| crate::StorageFailure::Encoding.with_source(error))?;
    if url.as_str() != text {
        return Err(crate::StorageFailure::Encoding
            .with_source("a recorded S3 endpoint is not in its canonical spelling"));
    }
    Ok(url)
}

#[cfg(test)]
#[path = "config_tests.rs"]
mod tests;

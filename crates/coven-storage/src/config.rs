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

    /// Whether a person signs in to it in a browser (OAuth): Dropbox and
    /// OneDrive.
    pub fn signs_in_with_oauth(self) -> bool {
        matches!(self, Self::Dropbox | Self::OneDrive)
    }
}

/// The store's location; credentials are kept separately in key custody.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum StorageConfig {
    /// An existing S3 bucket, which the store has to itself: coven can't
    /// tell how far a member's key reaches, so it takes every key to reach
    /// the whole bucket (§20.7). Paths start at the bucket's root.
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
    },
    /// Dropbox's access and location, fixed when cloud storage is created.
    Dropbox {
        /// Required choice: not-shared App folder or a shared-capable namespace.
        location: crate::DropboxLocation,
        /// Signed-in member's display label; absent before setup. Never routes or authorizes.
        account_label: Option<String>,
        /// The shared folder's name and web address as the latest connection
        /// read them; absent before setup and in the App folder (§20.2).
        folder_label: Option<FolderLabel>,
    },
    /// A OneDrive folder; paths within it start with the store id.
    OneDrive {
        /// Drive containing the folder.
        drive_id: String,
        /// Folder shared with members' Microsoft accounts.
        folder_id: String,
        /// Signed-in member's display label; absent before setup. Never routes or authorizes.
        account_label: Option<String>,
        /// The folder's name and web address as the latest connection read
        /// them; absent before setup (§20.2).
        folder_label: Option<FolderLabel>,
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

/// A Dropbox shared folder's or a OneDrive folder's name and web address,
/// as the member's account named them at this device's latest connection
/// (§20.2). Display only: coven never reaches storage through them.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FolderLabel {
    /// The provider's name for the folder.
    pub name: String,
    /// Where the folder opens in a browser: an https address.
    pub web_url: url::Url,
}

impl FolderLabel {
    /// The label a provider answered with; one this refuses is a protocol
    /// failure.
    pub(crate) fn answered(name: &str, web_url: &str) -> Result<Self, crate::StorageError> {
        let web_url = url::Url::parse(web_url)
            .map_err(|error| crate::StorageFailure::Protocol.with_source(error))?;
        let label = Self { name: name.into(), web_url };
        label.validate().map_err(|error| crate::StorageFailure::Protocol.with_source(error))?;
        Ok(label)
    }

    /// Refuse an empty name or an address a browser shouldn't be sent to.
    fn validate(&self) -> Result<(), crate::StorageError> {
        if self.name.trim().is_empty() || self.name.chars().any(char::is_control) {
            return Err(crate::StorageFailure::InvalidConfiguration
                .with_source("empty or invalid folder name"));
        }
        if self.web_url.scheme() != "https" || !crate::web_url::is_web_url(&self.web_url) {
            return Err(crate::StorageFailure::InvalidConfiguration
                .with_source("a folder's web address must be https"));
        }
        Ok(())
    }
}

impl StorageConfig {
    /// Whether these settings reach the same location, ignoring display labels.
    pub fn same_location(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Dropbox { location: a, .. }, Self::Dropbox { location: b, .. }) => a == b,
            (
                Self::OneDrive { drive_id: ad, folder_id: af, .. },
                Self::OneDrive { drive_id: bd, folder_id: bf, .. },
            ) => ad == bd && af == bf,
            // These providers carry no account label.
            (Self::S3 { .. }, Self::S3 { .. }) | (Self::CloudKit { .. }, Self::CloudKit { .. }) => {
                self == other
            }
            _ => false,
        }
    }

    /// Whether both are S3 locations naming the same bucket, at one
    /// endpoint and region, whatever profile reaches it.
    pub fn same_s3_bucket(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::S3 { bucket: ab, region: ar, endpoint: ae, .. },
                Self::S3 { bucket: bb, region: br, endpoint: be, .. },
            ) => ab == bb && ar == br && ae == be,
            _ => false,
        }
    }

    /// Whether every member reaches the whole location, so it holds one
    /// store, which the location marker at its root names (§20.3): a
    /// Dropbox shared folder, a OneDrive folder, or an S3 bucket. Nobody else
    /// reaches the Dropbox App folder, and a CloudKit store's zone is its own.
    pub fn holds_one_store(&self) -> bool {
        match self {
            Self::S3 { .. } | Self::OneDrive { .. } => true,
            Self::Dropbox { location, .. } => match location {
                crate::DropboxLocation::SharedFolder { .. } => true,
                crate::DropboxLocation::AppFolder => false,
            },
            Self::CloudKit { .. } => false,
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
            Self::S3 { bucket, region, endpoint, profile } => {
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
                crate::providers::check_s3_profile(endpoint.as_ref(), profile.as_ref())?;
                vec![bucket, region]
            }
            Self::Dropbox {
                location: crate::DropboxLocation::AppFolder,
                folder_label: Some(_),
                ..
            } => {
                return Err(crate::StorageFailure::InvalidConfiguration
                    .with_source("the Dropbox App folder has no folder label"));
            }
            Self::Dropbox { location: crate::DropboxLocation::AppFolder, .. } => vec![],
            Self::Dropbox {
                location: crate::DropboxLocation::SharedFolder { namespace_id },
                folder_label,
                ..
            } => {
                if let Some(label) = folder_label {
                    label.validate()?;
                }
                vec![namespace_id]
            }
            Self::OneDrive { drive_id, folder_id, folder_label, .. } => {
                if let Some(label) = folder_label {
                    label.validate()?;
                }
                vec![drive_id, folder_id]
            }
            Self::CloudKit { container, owner, zone } => vec![container, owner, zone],
        };
        if fields.iter().any(|value| value.trim().is_empty() || value.chars().any(char::is_control))
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
            Self::S3 { bucket, region, endpoint, profile } => {
                out.u8(S3)?;
                out.text(bucket)?;
                out.text(region)?;
                out.option(endpoint.as_ref(), |out, url| out.text(url.as_str()))?;
                out.option(profile.as_ref(), |out, profile| profile.pack(out))
            }
            // A code carries no folder label (format.md D13).
            Self::Dropbox { location, account_label, folder_label: _ } => {
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
            Self::OneDrive { drive_id, folder_id, account_label, folder_label: _ } => {
                out.u8(ONEDRIVE)?;
                out.text(drive_id)?;
                out.text(folder_id)?;
                out.option(account_label.as_deref(), FieldWriter::text)
            }
            Self::CloudKit { container, owner, zone } => {
                out.u8(CLOUDKIT)?;
                out.text(container)?;
                out.text(owner)?;
                out.text(zone)
            }
        }
    }

    /// Read a location packed by [`Self::pack`], checked as `validate` checks
    /// one. A text written in a spelling `pack` never writes, such as an
    /// endpoint URL that isn't canonical, is refused.
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
            },
            DROPBOX => Self::Dropbox {
                location: match input.u8()? {
                    0 => crate::DropboxLocation::AppFolder,
                    1 => crate::DropboxLocation::SharedFolder {
                        namespace_id: input.text("namespace id")?,
                    },
                    tag => {
                        return Err(FieldError::UnknownTag { field: "Dropbox location", tag }.into())
                    }
                },
                account_label: input.option(|input| input.text("account label"))?,
                folder_label: None,
            },
            ONEDRIVE => Self::OneDrive {
                drive_id: input.text("drive id")?,
                folder_id: input.text("folder id")?,
                account_label: input.option(|input| input.text("account label"))?,
                folder_label: None,
            },
            CLOUDKIT => Self::CloudKit {
                container: input.text("container")?,
                owner: input.text("owner")?,
                zone: input.text("zone")?,
            },
            tag => return Err(FieldError::UnknownTag { field: "provider", tag }.into()),
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

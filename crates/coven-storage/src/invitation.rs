use crate::{CloudProvider, StorageConfig, StorageError, StorageFailure};
use coven_crypto::{SecretBytes, SecretText};
use serde::{Deserialize, Serialize};

/// Provider information carried inside coven's encrypted invite code. Native
/// share tokens admit the recipient; they never identify grants for revocation.
#[derive(Clone, Serialize, Deserialize)]
#[serde(try_from = "RecordedInvitation")]
pub struct StorageInvitation {
    location: StorageConfig,
    pub(crate) acceptance: InvitationAcceptance,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) enum InvitationAcceptance {
    Granted,
    DropboxMount,
    OneDriveShare { token: SecretText },
    CloudKitShare { url: SecretText },
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordedInvitation {
    location: StorageConfig,
    acceptance: InvitationAcceptance,
}
impl TryFrom<RecordedInvitation> for StorageInvitation {
    type Error = StorageError;
    fn try_from(value: RecordedInvitation) -> Result<Self, Self::Error> {
        Self::new(value.location, value.acceptance)
    }
}
impl std::fmt::Debug for StorageInvitation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageInvitation")
            .field("provider", &self.location.provider())
            .finish_non_exhaustive()
    }
}
impl StorageInvitation {
    /// An already granted account location, or Dropbox's shared namespace to
    /// mount. An S3 admin uses this after making the member's key in the console.
    /// CloudKit instead needs the share URL returned by `grant_access`.
    pub fn for_account(location: StorageConfig) -> Result<Self, StorageError> {
        let acceptance = match location.provider() {
            CloudProvider::Dropbox => InvitationAcceptance::DropboxMount,
            CloudProvider::CloudKit => {
                return Err(StorageFailure::InvalidConfiguration
                    .with_source("CloudKit requires a share URL"))
            }
            _ => InvitationAcceptance::Granted,
        };
        Self::new(location, acceptance)
    }
    pub(crate) fn new(
        location: StorageConfig,
        acceptance: InvitationAcceptance,
    ) -> Result<Self, StorageError> {
        location.validate()?;
        match (location.provider(), &acceptance) {
            (
                CloudProvider::S3 | CloudProvider::GoogleDrive | CloudProvider::OneDrive,
                InvitationAcceptance::Granted,
            )
            | (CloudProvider::Dropbox, InvitationAcceptance::DropboxMount) => {}
            (CloudProvider::OneDrive, InvitationAcceptance::OneDriveShare { token })
                if !token.as_str().is_empty() => {}
            (CloudProvider::CloudKit, InvitationAcceptance::CloudKitShare { url }) => {
                let url = url::Url::parse(url.as_str())
                    .map_err(|error| StorageFailure::InvalidConfiguration.with_source(error))?;
                if url.scheme() != "https"
                    || url.host_str().is_none()
                    || !url.username().is_empty()
                    || url.password().is_some()
                {
                    return Err(StorageFailure::InvalidConfiguration
                        .with_source("invalid CloudKit share URL"));
                }
            }
            _ => {
                return Err(StorageFailure::InvalidConfiguration
                    .with_source("invitation does not match its provider"))
            }
        }
        Ok(Self {
            location,
            acceptance,
        })
    }
    /// Nonsecret location used to construct the joining account's adapter.
    pub fn location(&self) -> &StorageConfig {
        &self.location
    }
    /// Encode for the enclosing encrypted invite code, with bytes erased on drop.
    pub fn encode(&self) -> Result<SecretBytes, StorageError> {
        crate::secret_json::encode(self)
    }
    /// Decode and validate an invitation after the facade opens its enclosing code.
    pub fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        serde_json::from_slice(bytes).map_err(|error| StorageFailure::Encoding.with_source(error))
    }
    pub(crate) fn check(&self, location: &StorageConfig) -> Result<(), StorageError> {
        if &self.location != location {
            return Err(StorageFailure::InvitationMismatch.into());
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "invitation_tests.rs"]
mod tests;

use crate::{CloudProvider, StorageConfig, StorageError, StorageFailure};
use coven_crypto::{SecretBytes, SecretText};
use coven_format::code_fields::{FieldReader, FieldWriter};
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
        if matches!(
            location,
            StorageConfig::Dropbox { location: crate::DropboxLocation::AppFolder, .. }
        ) {
            return Err(StorageFailure::DropboxAppFolderNotShareable.into());
        }
        match (location.provider(), &acceptance) {
            (CloudProvider::S3 | CloudProvider::OneDrive, InvitationAcceptance::Granted)
            | (CloudProvider::Dropbox, InvitationAcceptance::DropboxMount) => {}
            (CloudProvider::OneDrive, InvitationAcceptance::OneDriveShare { token })
                if !token.as_str().is_empty() => {}
            (CloudProvider::CloudKit, InvitationAcceptance::CloudKitShare { url }) => {
                let url = url::Url::parse(url.as_str())
                    .map_err(|error| StorageFailure::InvalidConfiguration.with_source(error))?;
                if !crate::web_url::is_web_url(&url) || url.scheme() != "https" {
                    return Err(StorageFailure::InvalidConfiguration
                        .with_source("invalid CloudKit share URL"));
                }
            }
            _ => {
                return Err(StorageFailure::InvalidConfiguration
                    .with_source("invitation does not match its provider"))
            }
        }
        Ok(Self { location, acceptance })
    }
    /// Nonsecret location used to construct the joining account's adapter.
    pub fn location(&self) -> &StorageConfig {
        &self.location
    }
    /// Encode as the database records a granted invitation, with bytes erased
    /// on drop. An invite code packs it instead (format.md D13).
    pub fn encode(&self) -> Result<SecretBytes, StorageError> {
        crate::secret_json::encode(self)
    }
    /// Decode and validate an invitation the database recorded.
    pub fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        serde_json::from_slice(bytes).map_err(|error| StorageFailure::Encoding.with_source(error))
    }
    /// Pack the location, then how the recipient accepts it, into an invite
    /// code's storage part (format.md D13). The location's provider fixes
    /// which acceptance follows: nothing on S3 and Dropbox, an optional
    /// share token on OneDrive, the share URL on CloudKit.
    pub(crate) fn pack(&self, out: &mut FieldWriter) -> Result<(), coven_format::Error> {
        self.location.pack(out)?;
        match &self.acceptance {
            InvitationAcceptance::Granted
                if self.location.provider() == CloudProvider::OneDrive =>
            {
                out.option(None::<&str>, FieldWriter::text)
            }
            InvitationAcceptance::Granted | InvitationAcceptance::DropboxMount => Ok(()),
            InvitationAcceptance::OneDriveShare { token } => {
                out.option(Some(token.as_str()), FieldWriter::text)
            }
            InvitationAcceptance::CloudKitShare { url } => out.text(url.as_str()),
        }
    }
    /// Read an invitation packed by [`Self::pack`], checked as `new` checks one.
    pub(crate) fn unpack(input: &mut FieldReader<'_>) -> Result<Self, StorageError> {
        let location = StorageConfig::unpack(input)?;
        let acceptance = match location.provider() {
            CloudProvider::S3 => InvitationAcceptance::Granted,
            CloudProvider::Dropbox => InvitationAcceptance::DropboxMount,
            CloudProvider::OneDrive => {
                match input.option(|input| input.secret_text("share token"))? {
                    Some(token) => InvitationAcceptance::OneDriveShare { token },
                    None => InvitationAcceptance::Granted,
                }
            }
            CloudProvider::CloudKit => {
                InvitationAcceptance::CloudKitShare { url: input.secret_text("share URL")? }
            }
        };
        Self::new(location, acceptance)
    }
    pub(crate) fn check(&self, location: &StorageConfig) -> Result<(), StorageError> {
        if !self.location.same_location(location) {
            return Err(StorageFailure::InvitationMismatch.into());
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "invitation_tests.rs"]
mod tests;

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
    shared_with: Option<crate::SharedWith>,
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
    shared_with: Option<crate::SharedWith>,
}
impl TryFrom<RecordedInvitation> for StorageInvitation {
    type Error = StorageError;
    fn try_from(value: RecordedInvitation) -> Result<Self, Self::Error> {
        let invitation = Self::new(value.location, value.acceptance)?;
        Ok(Self { shared_with: value.shared_with, ..invitation })
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
        Ok(Self { location, acceptance, shared_with: None })
    }
    /// This invitation as shared with `account`, which a Dropbox or OneDrive
    /// invite code carries so the joining device can check the sign-in it
    /// holds is that account's (§13.2).
    pub fn shared_with(self, account: crate::SharedWith) -> Self {
        Self { shared_with: Some(account), ..self }
    }
    /// The account the folder was shared with, when the invitation records one.
    pub fn account(&self) -> Option<&crate::SharedWith> {
        self.shared_with.as_ref()
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
        crate::secret_json::decode(bytes)
    }
    /// Pack the location, then how the recipient accepts it, into an invite
    /// code's storage part (format.md D13). The location's provider fixes
    /// which acceptance follows: nothing on S3, an optional share token on
    /// OneDrive, the share URL on CloudKit; then, on Dropbox and OneDrive,
    /// the account the folder was shared with.
    pub(crate) fn pack(&self, out: &mut FieldWriter) -> Result<(), coven_format::Error> {
        self.location.pack(out)?;
        match &self.acceptance {
            InvitationAcceptance::Granted
                if self.location.provider() == CloudProvider::OneDrive =>
            {
                out.option(None::<&str>, FieldWriter::text)?
            }
            InvitationAcceptance::Granted | InvitationAcceptance::DropboxMount => (),
            InvitationAcceptance::OneDriveShare { token } => {
                out.option(Some(token.as_str()), FieldWriter::text)?
            }
            InvitationAcceptance::CloudKitShare { url } => out.text(url.as_str())?,
        }
        match &self.shared_with {
            Some(account) => {
                out.text(account.address())?;
                out.option(account.id(), FieldWriter::text)
            }
            None => Ok(()),
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
        let invitation = Self::new(location, acceptance)?;
        match invitation.location.provider() {
            CloudProvider::Dropbox | CloudProvider::OneDrive => {
                let address = input.text("shared-with account")?;
                let id = input.option(|input| input.text("shared-with account id"))?;
                let shared = crate::SharedWith::new(&address, id)?;
                // One spelling per account, as for a location.
                if shared.address() != address {
                    return Err(StorageFailure::Encoding
                        .with_source("shared-with account is not in normal form"));
                }
                Ok(invitation.shared_with(shared))
            }
            CloudProvider::S3 | CloudProvider::CloudKit => Ok(invitation),
        }
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

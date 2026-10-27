//! Provider admission and credentials carried together inside an invite code.

use crate::{CloudProvider, S3Credentials, StorageError, StorageFailure, StorageInvitation};
use coven_crypto::SecretBytes;
use coven_format::code_fields::{pack, FieldReader};

/// The storage part of format.md D13's invitation. Account credentials are
/// never shared; S3 credentials are the key created specifically for this invite.
#[derive(Clone)]
pub enum InviteStorage {
    /// The recipient signs in to their own provider account.
    Account(StorageInvitation),
    /// The recipient uses the key made for them in the provider console.
    S3 {
        /// The store location to open.
        invitation: StorageInvitation,
        /// The invited member's own key.
        credentials: S3Credentials,
    },
}

impl InviteStorage {
    /// Pack the invitation and, on S3, the member's key (format.md D13), in
    /// zeroizing storage. An S3 invitation without a key, or a key for
    /// another provider, is refused.
    pub fn encode(&self) -> Result<SecretBytes, StorageError> {
        let (invitation, credentials) = match self {
            Self::Account(invitation) => (invitation, None),
            Self::S3 {
                invitation,
                credentials,
            } => (invitation, Some(credentials)),
        };
        if (invitation.location().provider() == CloudProvider::S3) != credentials.is_some() {
            return Err(StorageFailure::InvalidConfiguration
                .with_source("an S3 invite carries the member's key; no other invite does"));
        }
        if let Some(credentials) = credentials {
            credentials.validate()?;
        }
        Ok(pack(|out| {
            invitation.pack(out)?;
            match credentials {
                Some(credentials) => credentials.pack(out),
                None => Ok(()),
            }
        })?)
    }
    /// Decode a packed payload after checking the enclosing invitation: the
    /// location's provider says whether a key follows.
    pub fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        let mut input = FieldReader::new(bytes)?;
        let invitation = StorageInvitation::unpack(&mut input)?;
        let storage = match invitation.location().provider() {
            CloudProvider::S3 => Self::S3 {
                invitation,
                credentials: S3Credentials::unpack(&mut input)?,
            },
            _ => Self::Account(invitation),
        };
        input.finish()?;
        Ok(storage)
    }
}

#[cfg(test)]
#[path = "invite_storage_tests.rs"]
mod tests;

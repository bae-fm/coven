//! Checked code metadata used by bootstrap (E10).

use coven_format::codes::{InviteCode, InviteDeliveryKind, RestoreCode};
use coven_foundation::id_source::{MemberId, StoreId};
use coven_storage::{
    CloudProvider, ConnectionCredentials, InviteStorage, OAuthAccess, StorageConfig,
};

/// The two codes that can open a store on a new device.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CodeKind {
    /// This person's member id and current private pair, store id and
    /// storage location only.
    Restore,
    /// A one-time request carrying the chosen member id, initial key,
    /// inviting writer and its published key-copy number; approval is required.
    Invite,
}

/// Code metadata safe for presentation before opening the store.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CodeInfo {
    /// A restore code reveals no store name before loading.
    Restore(CodeMetadata),
    /// An invite carries the store's name captured when the admin made it,
    /// the new person's name and how it is approved.
    Invite {
        /// The code's shared metadata.
        metadata: CodeMetadata,
        /// The store name at invitation creation.
        store_name: String,
        /// The new person's name, as the admin typed it; `None` for a
        /// replacement.
        invitee: Option<String>,
        /// In person or sent: whether the joining device shows a
        /// confirmation number (§13.2).
        delivery: InviteDeliveryKind,
    },
}

impl CodeInfo {
    /// The store id, member and provider facts carried by either kind of code.
    pub fn metadata(&self) -> &CodeMetadata {
        match self {
            Self::Restore(metadata) | Self::Invite { metadata, .. } => metadata,
        }
    }
}

/// Store, member and provider facts shared by both code kinds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CodeMetadata {
    /// The shared store's identity.
    pub store_id: StoreId,
    /// The stable member id either kind of code fixes: the restoring
    /// person's own, or the one the invite's admin chose (§13).
    pub member: MemberId,
    /// The storage provider.
    pub cloud_provider: CloudProvider,
    /// The code's provider location and account label, without credentials or share tokens.
    pub location: StorageConfig,
    /// The sign-in required by this code; S3 and CloudKit do not use OAuth.
    pub oauth_access: Option<OAuthAccess>,
}

/// A malformed code or one intended for the other operation.
#[derive(Clone, Debug, thiserror::Error)]
pub enum CodeError {
    /// Encoding, checksum or provider payload validation failed.
    #[error("invalid restore or invite code")]
    Invalid,
    /// A valid code names the other operation.
    #[error("expected {expected:?} code, received {actual:?}")]
    WrongKind {
        /// The requested kind.
        expected: CodeKind,
        /// The actual kind.
        actual: CodeKind,
    },
    /// A valid restore code names another store than the one being restored.
    #[error("restore code is for store {actual}, not {expected}")]
    WrongStore {
        /// The store being restored.
        expected: StoreId,
        /// The store the code names.
        actual: StoreId,
    },
}

/// Decode and validate the complete code before returning public metadata.
pub fn decode_code_info(code: &str) -> Result<CodeInfo, CodeError> {
    let (code, location) = decode(code)?;
    let metadata = |store_id, member| CodeMetadata {
        store_id,
        member,
        cloud_provider: location.provider(),
        oauth_access: location.oauth_access(),
        location,
    };
    Ok(match code {
        DecodedCode::Restore(code) => CodeInfo::Restore(metadata(code.store, code.member)),
        DecodedCode::Invite(code) => CodeInfo::Invite {
            metadata: metadata(code.store, code.member),
            store_name: code.name,
            invitee: code.invitee,
            delivery: code.delivery,
        },
    })
}

/// Decode a restore code for bootstrap, preserving a
/// valid wrong-kind distinction. Provider payloads have already been validated.
pub fn read_restore_code(code: &str) -> Result<RestoreCode, CodeError> {
    match decode(code)?.0 {
        DecodedCode::Restore(code) => Ok(code),
        DecodedCode::Invite(_) => Err(CodeError::WrongKind {
            expected: CodeKind::Restore,
            actual: CodeKind::Invite,
        }),
    }
}

/// Decode an invitation for the joining side after validating provider admission.
pub fn read_invite_code(code: &str) -> Result<InviteCode, CodeError> {
    match decode(code)?.0 {
        DecodedCode::Invite(code) => Ok(code),
        DecodedCode::Restore(_) => Err(CodeError::WrongKind {
            expected: CodeKind::Invite,
            actual: CodeKind::Restore,
        }),
    }
}

enum DecodedCode {
    Restore(RestoreCode),
    Invite(InviteCode),
}

#[expect(
    clippy::map_err_ignore,
    reason = "a code that does not decode is Invalid, whichever field failed"
)]
fn decode(text: &str) -> Result<(DecodedCode, StorageConfig), CodeError> {
    if text.starts_with("CVR1-") {
        let code = RestoreCode::from_text(text).map_err(|_| CodeError::Invalid)?;
        let storage =
            StorageConfig::decode(code.storage.as_bytes()).map_err(|_| CodeError::Invalid)?;
        Ok((DecodedCode::Restore(code), storage))
    } else if text.starts_with("CVI1-") {
        let code = InviteCode::from_text(text).map_err(|_| CodeError::Invalid)?;
        let storage =
            InviteStorage::decode(code.storage.as_bytes()).map_err(|_| CodeError::Invalid)?;
        let location = match storage {
            InviteStorage::Account(invitation)
                if invitation.location().provider() != CloudProvider::S3 =>
            {
                invitation.location().clone()
            }
            InviteStorage::S3 {
                invitation,
                credentials,
            } => {
                let storage = ConnectionCredentials {
                    location: invitation.location().clone(),
                    credentials: coven_storage::StorageCredentials::S3(credentials),
                };
                storage.validate().map_err(|_| CodeError::Invalid)?;
                storage.location
            }
            _ => return Err(CodeError::Invalid),
        };
        Ok((DecodedCode::Invite(code), location))
    } else {
        Err(CodeError::Invalid)
    }
}

#[cfg(test)]
#[path = "codes_tests.rs"]
mod tests;

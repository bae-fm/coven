//! Checked code metadata used by bootstrap (E10).

use coven_format::codes::{InviteCode, RestoreCode};
use coven_foundation::id_source::{MemberId, StoreId};
use coven_storage::{
    CloudProvider, ConnectionCredentials, GoogleDriveAccess, InviteStorage, OAuthAccess,
    StorageConfig,
};

/// The two codes that can open a store on a new device.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CodeKind {
    /// This person's member id and current private pair, store id and name,
    /// and storage location only.
    Restore,
    /// A one-time request carrying the chosen member id, initial key,
    /// inviting writer and its published key-copy number; approval is required.
    Invite,
}

/// Code metadata safe for presentation before opening the store.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CodeInfo {
    /// The operation the code permits.
    pub kind: CodeKind,
    /// The shared store's identity.
    pub store_id: StoreId,
    /// The shared store's name.
    pub store_name: String,
    /// The stable member id either kind of code fixes: the restoring
    /// person's own, or the one the invite's admin chose (§12).
    pub member: MemberId,
    /// The storage provider.
    pub cloud_provider: CloudProvider,
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
}

/// Decode and validate the complete code before returning public metadata.
pub fn decode_code_info(code: &str) -> Result<CodeInfo, CodeError> {
    let (code, location) = decode(code)?;
    let (kind, store_id, store_name, member) = match code {
        DecodedCode::Restore(code) => (CodeKind::Restore, code.store, code.name, code.member),
        DecodedCode::Invite(code) => (CodeKind::Invite, code.store, code.name, code.member),
    };
    Ok(CodeInfo {
        kind,
        store_id,
        store_name,
        member,
        cloud_provider: location.provider(),
        oauth_access: if kind == CodeKind::Invite
            && location.provider() == CloudProvider::GoogleDrive
        {
            Some(OAuthAccess::GoogleDrive(GoogleDriveAccess::Full))
        } else {
            location.oauth_access()
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

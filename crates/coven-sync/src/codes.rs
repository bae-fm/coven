//! Checked code metadata shared by bootstrap and credential replacement (E10).

use coven_format::codes::{InviteCode, RestoreCode};
use coven_foundation::id_source::StoreId;
use coven_storage::{CloudProvider, ConnectionCredentials, InviteStorage, RestoreStorage};

/// The two codes that can open a store on a new device.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CodeKind {
    /// This person's identity, storage location and S3 key where needed.
    Restore,
    /// A one-time request requiring another member's approval.
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
    /// The storage provider.
    pub cloud_provider: CloudProvider,
    /// Whether the new device needs its own OAuth sign-in.
    pub needs_oauth: bool,
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
    let (code, cloud_provider) = decode(code)?;
    let (kind, store_id, store_name) = match code {
        DecodedCode::Restore(code) => (CodeKind::Restore, code.store, code.name),
        DecodedCode::Invite(code) => (CodeKind::Invite, code.store, code.name),
    };
    Ok(CodeInfo {
        kind,
        store_id,
        store_name,
        cloud_provider,
        needs_oauth: matches!(
            cloud_provider,
            CloudProvider::GoogleDrive | CloudProvider::Dropbox | CloudProvider::OneDrive
        ),
    })
}

/// Decode a restore code for bootstrap or credential replacement, preserving a
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

fn decode(text: &str) -> Result<(DecodedCode, CloudProvider), CodeError> {
    if text.starts_with("CVR1-") {
        let code = RestoreCode::from_text(text).map_err(|_| CodeError::Invalid)?;
        let storage =
            RestoreStorage::decode(code.storage.as_bytes()).map_err(|_| CodeError::Invalid)?;
        Ok((DecodedCode::Restore(code), storage.location().provider()))
    } else if text.starts_with("CVI1-") {
        let code = InviteCode::from_text(text).map_err(|_| CodeError::Invalid)?;
        let storage =
            InviteStorage::decode(code.storage.as_bytes()).map_err(|_| CodeError::Invalid)?;
        let provider = match storage {
            InviteStorage::Account(invitation)
                if invitation.location().provider() != CloudProvider::S3 =>
            {
                invitation.location().provider()
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
                storage.location.provider()
            }
            _ => return Err(CodeError::Invalid),
        };
        Ok((DecodedCode::Invite(code), provider))
    } else {
        Err(CodeError::Invalid)
    }
}

#[cfg(test)]
#[path = "codes_tests.rs"]
mod tests;

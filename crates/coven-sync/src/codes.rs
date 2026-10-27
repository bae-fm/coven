//! Checked code metadata used by bootstrap (E10).

use coven_format::codes::{InviteCode, InviteDeliveryKind, RestoreCode};
use coven_foundation::id_source::{MemberId, StoreId};
use coven_storage::{CloudProvider, InviteStorage, OAuthAccess, RestoreStorage, StorageConfig};

/// The two codes that can open a store on a new device.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CodeKind {
    /// This person's member id and current private pair, store id and
    /// storage location, and on S3 perhaps their key.
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
    Invite(InviteInfo),
}

impl CodeInfo {
    /// The store id, member and provider facts carried by either kind of code.
    pub fn metadata(&self) -> &CodeMetadata {
        match self {
            Self::Restore(metadata) | Self::Invite(InviteInfo { metadata, .. }) => metadata,
        }
    }
}

/// What an invite code tells before connecting: as typed or scanned, and as a
/// join under way saved it (§13.2).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InviteInfo {
    /// The code's shared metadata, including the sign-in the join needs.
    pub metadata: CodeMetadata,
    /// The store name at invitation creation.
    pub store_name: String,
    /// The new person's name, as the admin typed it; `None` for a
    /// replacement.
    pub invitee: Option<String>,
    /// In person or sent: whether the joining device shows a confirmation
    /// number (§13.2).
    pub delivery: InviteDeliveryKind,
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
    /// Whether the code carries an S3 key, so the device asks for none: an S3
    /// invite always does, and a restore code when the person's own device
    /// put the key it holds in it (§13.1).
    pub carries_s3_key: bool,
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
    /// A valid code names another store than the one it is used for: a
    /// restore code for a store being restored in place, or an invite code
    /// saved for another store's join.
    #[error("code is for store {actual}, not {expected}")]
    WrongStore {
        /// The store the code is used for.
        expected: StoreId,
        /// The store the code names.
        actual: StoreId,
    },
}

/// Decode and validate the complete code before returning public metadata.
pub fn decode_code_info(code: &str) -> Result<CodeInfo, CodeError> {
    let (code, location, carries_s3_key) = decode(code)?;
    let metadata = |store_id, member| CodeMetadata {
        store_id,
        member,
        cloud_provider: location.provider(),
        oauth_access: location.oauth_access(),
        location,
        carries_s3_key,
    };
    Ok(match code {
        DecodedCode::Restore(code) => CodeInfo::Restore(metadata(code.store, code.member)),
        DecodedCode::Invite(code) => CodeInfo::Invite(InviteInfo {
            metadata: metadata(code.store, code.member),
            store_name: code.name,
            invitee: code.invitee,
            delivery: code.delivery,
        }),
    })
}

/// Decode a restore code for bootstrap, preserving a
/// valid wrong-kind distinction. Its storage part has already been validated.
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
fn decode(text: &str) -> Result<(DecodedCode, StorageConfig, bool), CodeError> {
    if text.starts_with("CVR1-") {
        let code = RestoreCode::from_text(text).map_err(|_| CodeError::Invalid)?;
        let storage =
            RestoreStorage::decode(code.storage.as_bytes()).map_err(|_| CodeError::Invalid)?;
        let carries_s3_key = storage.s3_key.is_some();
        Ok((DecodedCode::Restore(code), storage.location, carries_s3_key))
    } else if text.starts_with("CVI1-") {
        let code = InviteCode::from_text(text).map_err(|_| CodeError::Invalid)?;
        // Decoding checks that an S3 invite, and only one, carries a whole key.
        let (invitation, carries_s3_key) =
            match InviteStorage::decode(code.storage.as_bytes()).map_err(|_| CodeError::Invalid)? {
                InviteStorage::Account(invitation) => (invitation, false),
                InviteStorage::S3 { invitation, .. } => (invitation, true),
            };
        Ok((
            DecodedCode::Invite(code),
            invitation.location().clone(),
            carries_s3_key,
        ))
    } else {
        Err(CodeError::Invalid)
    }
}

#[cfg(test)]
#[path = "codes_tests.rs"]
mod tests;

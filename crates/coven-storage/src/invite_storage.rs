//! Provider admission and credentials carried together inside an invite code.

use crate::{S3Credentials, StorageError, StorageFailure, StorageInvitation};
use coven_crypto::SecretBytes;
use serde::{Deserialize, Serialize};

/// The storage payload of Appendix D13's invitation. Account credentials are
/// never shared; S3 credentials are the key created specifically for this invite.
#[derive(Clone, Serialize, Deserialize)]
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
    /// Encode the provider-specific payload in zeroizing storage.
    pub fn encode(&self) -> Result<SecretBytes, StorageError> {
        crate::secret_json::encode(self)
    }
    /// Decode the payload after checking the enclosing invitation.
    pub fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        serde_json::from_slice(bytes).map_err(|e| StorageFailure::Encoding.with_source(e))
    }
}

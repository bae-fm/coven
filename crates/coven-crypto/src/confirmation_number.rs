//! The six digits a sent invite's requester and admin compare (§13.2, D11).

use crate::{cipher, derivation::CONFIRMATION_NUMBER, MemberPublicKeys};
use coven_foundation::id_source::InviteId;
use sha2::{Digest, Sha256};
use std::fmt;

/// Six digits naming a join request's keys, which the requester's device and
/// the admin's both compute (§13.2). It names only the keys and the invite:
/// the invite secret adds nothing a stranger who saw the code lacks, and the
/// device name is the requester's free choice. Displays as `041 778`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ConfirmationNumber(pub u32);

impl ConfirmationNumber {
    /// SHA-256 of the context of the label, the invite id and the keys'
    /// `MemberKeys` encoding; its first 8 bytes, big-endian, modulo 1,000,000 (D11).
    pub fn of(invite: InviteId, keys: &MemberPublicKeys) -> Self {
        let mut encoded = [0; 64];
        encoded[..32].copy_from_slice(keys.signing.as_bytes());
        encoded[32..].copy_from_slice(keys.sealing.as_bytes());
        let digest =
            Sha256::digest(cipher::context(&[CONFIRMATION_NUMBER, invite.0.as_bytes(), &encoded]));
        let first = u64::from_be_bytes(digest[..8].try_into().expect("8 of 32 bytes"));
        Self(u32::try_from(first % 1_000_000).expect("below 1,000,000"))
    }
}

impl fmt::Display for ConfirmationNumber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:03} {:03}", self.0 / 1000, self.0 % 1000)
    }
}

#[cfg(test)]
#[path = "confirmation_number_tests.rs"]
mod tests;

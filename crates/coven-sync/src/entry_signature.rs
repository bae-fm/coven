//! The key that verifies a store-log entry: creation's supplied pair, or the
//! stable author's current signing key in the entry's recorded view (§9, D6).

use crate::Refusal;
use coven_crypto::{ObjectDigest, Signature};
use coven_database::StoreLogState;
use coven_format::store_log::{StoreChange, StoreLogEntry};
use coven_storage::ObjectPath;
use std::sync::Arc;

/// A decrypted entry whose signature waits for its signer: creation's supplied
/// initial pair, or else the author's current signing key in the replay of its
/// recorded `had_read`, implicit own entries included (§9, D6). Until
/// verified, the author and view are lookup hints only.
#[derive(Clone, Debug)]
pub(crate) struct OpenedEntry {
    pub(crate) entry: StoreLogEntry,
    path: ObjectPath,
    digest: ObjectDigest,
    signature: Signature,
}

impl OpenedEntry {
    pub(crate) fn new(
        entry: StoreLogEntry,
        path: ObjectPath,
        digest: ObjectDigest,
        signature: Signature,
    ) -> Self {
        Self {
            entry,
            path,
            digest,
            signature,
        }
    }

    /// Verify against `view`, the replay of exactly this entry's recorded past.
    /// An author with no keys in that view has no current signer, so the entry
    /// is refused like any other signature the view does not bind (§9,
    /// `Keyed.unknown_author_not_current`): it never enters replay, lateness
    /// or finality.
    pub(crate) fn verify(&self, view: &StoreLogState) -> Result<(), Refusal> {
        let signer = match &self.entry.change {
            StoreChange::CreateStore { admin, .. } => admin.signing,
            _ => match view.members.get(&self.entry.author) {
                Some(author) => author.keys.signing,
                None => return Err(Refusal::Signature { cause: None }),
            },
        };
        signer
            .verify_object(self.path.as_str(), &self.digest, &self.signature)
            .map_err(|error| Refusal::Signature {
                cause: Some(Arc::new(error)),
            })
    }
}

#[cfg(test)]
#[path = "entry_signature_tests.rs"]
pub(crate) mod tests;

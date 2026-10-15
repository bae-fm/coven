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
    /// An author no kept entry in that view added has no current key, so no
    /// signature can make it a member: only a self-registration reaches this
    /// check (see [`author_matches_view`](crate::store_log_object::author_matches_view)), and replay drops it as `NotAllowed`
    /// in that same view, as Appendix C's keyed replay does for a signer that
    /// is not current there.
    pub(crate) fn verify(&self, view: &StoreLogState) -> Result<(), Refusal> {
        let signer = match &self.entry.change {
            StoreChange::CreateStore { admin, .. } => admin.signing,
            _ => match view.members.get(&self.entry.author) {
                Some(author) => author.keys.signing,
                None => return Ok(()),
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

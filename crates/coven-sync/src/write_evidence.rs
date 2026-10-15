//! Another member's write as a snapshot carries it: its cleartext prefix,
//! sealed header, part roots and header signature, without its parts (D9).
//! The database checks what a snapshot claims about that member's rows
//! against these; sync opens the header and verifies its signature with the
//! author's key in the header's recorded store-log view, as for a received
//! write (§8.8, §15).

use crate::replay_cache::ReplayCache;
use crate::SyncError;
use coven_crypto::{AuthorshipHasher, StoreKeyring};
use coven_database::{DbError, SnapshotError, StoreLog};
use coven_format::authorship::WriteEvidence;
use coven_format::sealed_write::WriteObjectPrefix;
use coven_format::write_stream::WriteHeaderFrame;
use coven_foundation::id_source::{KeyId, StoreId};
use coven_merge::Audience;

pub(crate) struct EvidenceKeys {
    store: StoreId,
    ring: Option<StoreKeyring>,
    log: StoreLog,
}

impl EvidenceKeys {
    pub(crate) fn new(store: StoreId, ring: Option<StoreKeyring>, log: StoreLog) -> Self {
        Self { store, ring, log }
    }

    fn keys(&self, audience: &Audience, key: KeyId) -> Result<coven_crypto::DerivedKeys, DbError> {
        let waits = || SnapshotError::KeyUnavailable {
            audience: audience.clone(),
            key,
        };
        let ring = self.ring.as_ref().ok_or_else(waits)?;
        crate::write_seal::derive(ring, audience, key).map_err(|error| match error {
            SyncError::KeyUnavailable(_) => waits().into(),
            SyncError::Database(error) => error,
            error => {
                tracing::error!(%error, "deriving a held key failed");
                DbError::Invariant {
                    what: "a held key derives its keys",
                }
            }
        })
    }
}

fn invalid(reason: &'static str) -> DbError {
    SnapshotError::Inconsistent(reason).into()
}

impl coven_database::Authorship for EvidenceKeys {
    fn header(&self, evidence: &WriteEvidence) -> Result<WriteHeaderFrame, DbError> {
        let path = crate::write_seal::path(self.store, evidence.write);
        let prefix = WriteObjectPrefix::decode(&evidence.prefix).map_err(SnapshotError::Format)?;
        if evidence.roots.len() != prefix.part_keys.len()
            || WriteObjectPrefix::header_chunk_length(&evidence.header)
                .map_err(SnapshotError::Format)?
                != evidence.header.len()
        {
            return Err(invalid("write evidence does not match its prefix"));
        }
        let sealed =
            WriteObjectPrefix::header_chunk(&evidence.header).map_err(SnapshotError::Format)?;
        let plain = self
            .keys(&Audience::Store, prefix.store_key)?
            .open_object_chunk(path.as_str(), &evidence.prefix, 0, 0, sealed)
            .map_err(|_| invalid("write evidence header does not open"))?;
        let header = WriteHeaderFrame::decode(&plain).map_err(SnapshotError::Format)?;
        if header.header.position != evidence.write || header.parts.len() != evidence.roots.len() {
            return Err(invalid("write evidence names another write"));
        }
        // A snapshot's signed store-log coverage includes every covered
        // write's recorded view, and loading waits for that coverage first.
        let missing: Vec<_> = header
            .header
            .store_log_read
            .0
            .iter()
            .copied()
            .filter(|id| !self.log.replay.entries.contains_key(id))
            .collect();
        if !missing.is_empty() {
            return Err(SnapshotError::StoreLog { missing }.into());
        }
        let signer =
            crate::write_object::authority(&self.log, &mut ReplayCache::new(&self.log), &header)
                .map_err(|_| invalid("write evidence author was not authorized to write"))?;
        signer
            .verify_write_header(
                path.as_str(),
                &evidence.prefix,
                &evidence.header,
                &evidence.roots,
                &coven_crypto::Signature::from_bytes(evidence.signature),
            )
            .map_err(|_| invalid("write evidence header signature does not verify"))?;
        Ok(header)
    }

    fn hasher(&self, audience: &Audience, key: KeyId) -> Result<AuthorshipHasher, DbError> {
        Ok(self.keys(audience, key)?.authorship_hasher())
    }
}

#[cfg(test)]
#[path = "write_evidence_tests.rs"]
mod tests;

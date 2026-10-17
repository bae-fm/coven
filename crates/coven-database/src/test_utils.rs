//! Schema fixtures and crash checkpoints for tests of the production database.

use crate::{Migration, RowIdentity, SyncedTable};

#[path = "work_counts.rs"]
mod work_counts;
pub use work_counts::{WorkCounter, WorkCounts};

pub(crate) type WriteCheckpointObserver = Box<dyn Fn(WriteCheckpoint) + Send>;

/// Boundaries after a local write's durable commit, for process-crash tests.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteCheckpoint {
    /// SQLite has committed, before notifying queries and sync.
    Committed,
    /// Commit observers have been notified.
    Observed,
    /// The write has released its staged-file reservations.
    StagingReleased,
    /// An obsolete file's bytes have been removed, after the writer that
    /// released them; its pending-removal row is about to be deleted.
    FileRemoved,
    /// The obsolete file's pending-removal row has been deleted.
    FileRemovalRecorded,
    /// Post-commit file cleanup has committed the deletion of those rows.
    Finished,
}

/// Fixed member key used to authenticate fixture writes.
pub fn signer() -> coven_crypto::SigningKey {
    coven_format::test_utils::keys_from_seed(0xA5)
        .public_keys()
        .signing
}

/// Declarations for a UUID-keyed notes table in the store audience.
pub fn notes_tables() -> Vec<SyncedTable> {
    vec![SyncedTable::new("notes", RowIdentity::IndependentUuid)]
}

/// The corresponding app migration sequence.
pub fn notes_migrations() -> Vec<Migration> {
    vec![Migration::addition(1, "notes", "CREATE TABLE notes (id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL, body TEXT NOT NULL DEFAULT '')")]
}

/// Authorship of test writes, sealed and hashed with one fixed test key for
/// every audience. It opens headers it sealed itself and checks no signature:
/// the database checks roots and openings; sync checks signatures (D9).
#[derive(Clone, Copy, Debug, Default)]
pub struct TestAuthorship;

impl TestAuthorship {
    const STORE: &'static str = "10101010-1010-1010-1010-101010101010";

    /// The test key sealing headers and keying every part's leaves.
    pub fn key() -> coven_crypto::StoreKey {
        coven_crypto::StoreKey::from_bytes(
            coven_foundation::id_source::KeyId(uuid::Uuid::from_bytes([1; 16])),
            [17; 32],
        )
    }

    fn path(write: coven_merge::WriteId) -> String {
        format!("{}/seq/{}", Self::STORE, write.number)
    }

    /// The evidence and hashers a stream of this record carries.
    pub fn write(record: &coven_format::write::WriteRecord) -> crate::WriteAuthorship {
        let encoder = coven_format::write_stream::WriteEncoder::new(record).unwrap();
        Self::for_header(
            encoder.header(),
            record
                .parts
                .iter()
                .map(coven_format::authorship::part_leaves)
                .collect(),
        )
    }

    /// Seal an explicit stream header, including unopened parts.
    pub(crate) fn for_header(
        frame: &coven_format::write_stream::WriteHeaderFrame,
        leaves: Vec<Vec<coven_format::authorship::Leaf>>,
    ) -> crate::WriteAuthorship {
        let key = Self::key();
        let prefix = coven_format::sealed_write::WriteObjectPrefix {
            format: coven_format::FormatVersion::CURRENT,
            store_key: key.id(),
            part_keys: vec![key.id(); frame.parts.len()],
        }
        .encode()
        .unwrap();
        let sealed = key
            .derive()
            .seal_object_chunk(
                &Self::path(frame.header.position),
                &prefix,
                0,
                0,
                &frame.encode().unwrap(),
            )
            .unwrap();
        let mut header = ((sealed.len() - coven_crypto::SEALED_OBJECT_CHUNK_OVERHEAD) as u32)
            .to_be_bytes()
            .to_vec();
        header.extend(sealed);
        let hasher = key.derive().authorship_hasher();
        crate::WriteAuthorship {
            evidence: crate::HeaderEvidence {
                prefix,
                header,
                roots: leaves
                    .iter()
                    .map(|leaves| coven_format::authorship::root(&hasher, leaves).unwrap())
                    .collect(),
                signature: [0; 64],
            },
            hashers: frame.parts.iter().map(|_| Some(hasher.clone())).collect(),
        }
    }
}

impl crate::Authorship for TestAuthorship {
    fn header(
        &self,
        evidence: &coven_format::authorship::WriteEvidence,
    ) -> Result<coven_format::write_stream::WriteHeaderFrame, crate::DbError> {
        let sealed = coven_format::sealed_write::WriteObjectPrefix::header_chunk(&evidence.header)?;
        let plain = Self::key()
            .derive()
            .open_object_chunk(&Self::path(evidence.write), &evidence.prefix, 0, 0, sealed)
            .map_err(|_| crate::snapshot_error::invalid("test evidence header does not open"))?;
        Ok(coven_format::write_stream::WriteHeaderFrame::decode(
            &plain,
        )?)
    }

    fn hasher(
        &self,
        _audience: &coven_merge::Audience,
        _key: coven_foundation::id_source::KeyId,
    ) -> Result<coven_crypto::AuthorshipHasher, crate::DbError> {
        Ok(Self::key().derive().authorship_hasher())
    }
}

#[cfg(test)]
#[path = "test_utils_tests.rs"]
pub(crate) mod tests;

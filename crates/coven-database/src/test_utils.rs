//! Schema fixtures and crash checkpoints for tests of the production database.

use crate::{Migration, RowIdentity, SyncedTable};

#[path = "work_counts.rs"]
mod work_counts;
pub use work_counts::{WorkCounter, WorkCounts};

/// One measured number, as a line `scripts/perf.sh` reads:
/// `COST metric=<name> value=<n> unit=<ms|ratio|count>`. Lower is better in
/// every unit. A character in `metric` outside letters, digits and `_./-`
/// becomes `_`, so a label can be passed as it reads.
pub fn report_cost(metric: &str, value: f64, unit: &str) {
    let metric: String = metric
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || "_./-".contains(c) { c } else { '_' })
        .collect();
    eprintln!("COST metric={metric} value={value} unit={unit}");
}

/// [`report_cost`] for a time, in milliseconds.
pub fn report_time(metric: &str, time: std::time::Duration) {
    report_cost(metric, time.as_secs_f64() * 1000.0, "ms");
}

pub(crate) type WriteCheckpointObserver = Box<dyn Fn(WriteCheckpoint) + Send>;

/// Boundaries after a local write's durable commit, for process-crash tests.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteCheckpoint {
    /// Live queries have judged the write, before SQLite commits it.
    Judged,
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

/// A live query's results as a test follows them: each in the order the
/// query reports it, kept as the latest one seen.
pub struct Followed<T, E> {
    query: crate::LiveQuery<T, E>,
    latest: Option<T>,
}

impl<T, E> Followed<T, E>
where
    T: Clone + PartialEq + std::fmt::Debug + Send + 'static,
    E: From<crate::DbError> + coven_foundation::task::CarriesBug + std::fmt::Debug + Send + 'static,
{
    /// Follow `query` from its first result.
    pub fn new(query: crate::LiveQuery<T, E>) -> Self {
        Self { query, latest: None }
    }

    /// The next result the query reports. Panics on an error result or
    /// after the query ends, and when none ever comes.
    pub async fn next(&mut self) -> T {
        let next = coven_foundation::clock::within(self.query.next())
            .await
            .unwrap_or_else(|| panic!("no result after {:?}", self.latest))
            .expect("the live query ended")
            .expect("the live query failed");
        self.latest = Some(next.clone());
        next
    }

    /// The latest result seen, waiting for the first one.
    pub async fn latest(&mut self) -> T {
        match self.latest.clone() {
            Some(latest) => latest,
            None => self.next().await,
        }
    }

    /// The first result `matches` accepts, from the latest one seen on.
    pub async fn wait_for(&mut self, mut matches: impl FnMut(&T) -> bool) -> T {
        let mut result = self.latest().await;
        while !matches(&result) {
            result = self.next().await;
        }
        result
    }
}

/// Writers for a test not about forgetting kept records: one other device
/// that can still write but never lands a write, so every kept record stays
/// (§16.7).
pub fn idle_writers() -> crate::Writers {
    crate::Writers::new(std::collections::BTreeMap::from([(
        coven_foundation::id_source::DeviceId(u64::MAX),
        None,
    )]))
}

/// Fixed member key used to authenticate fixture writes.
pub fn signer() -> coven_crypto::SigningKey {
    coven_format::test_utils::keys_from_seed(0xA5).public_keys().signing
}

/// Stage a snapshot's whole plaintext from `input`, one chunk at a time, as
/// sync stages an opened body (§16.2).
pub async fn stage_plaintext(
    database: &crate::Database,
    id: coven_format::admin::SnapshotId,
    prefix: coven_format::sealed_snapshot::SnapshotObjectPrefix,
    mut input: impl std::io::Read,
) -> Result<crate::SnapshotStaging, crate::DbError> {
    let mut staging = database.stage_snapshot(id, prefix).await?;
    loop {
        let mut chunk = vec![0; coven_format::chunks::CHUNK_SIZE];
        let length = match input.read(&mut chunk) {
            Ok(0) => break,
            Ok(length) => length,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(crate::SnapshotError::Read(error).into()),
        };
        chunk.truncate(length);
        staging.stage(chunk).await?;
    }
    staging.finish().await?;
    Ok(staging)
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
    const STORE: coven_foundation::id_source::StoreId =
        coven_foundation::id_source::StoreId(uuid::Uuid::from_bytes([0x10; 16]));

    /// The test key sealing headers and keying every part's leaves.
    pub fn key() -> coven_crypto::StoreKey {
        coven_crypto::StoreKey::from_bytes(
            coven_foundation::id_source::KeyId(uuid::Uuid::from_bytes([1; 16])),
            [17; 32],
        )
    }

    fn path(write: coven_merge::WriteId) -> String {
        coven_format::path::ObjectPath::slot(
            Self::STORE,
            std::num::NonZeroU64::new(write.number).expect("a positive write number"),
        )
        .into()
    }

    /// The evidence and hashers a stream of this record carries.
    pub fn write(record: &coven_format::write::WriteRecord) -> crate::WriteAuthorship {
        let encoder = coven_format::write_stream::WriteEncoder::new(record).unwrap();
        Self::for_header(
            encoder.header(),
            record.parts.iter().map(coven_format::authorship::part_leaves).collect(),
        )
    }

    /// Seal an explicit stream header, including unopened parts.
    pub(crate) fn for_header(
        frame: &coven_format::write_stream::WriteHeaderFrame,
        leaves: Vec<Vec<coven_format::authorship::Leaf>>,
    ) -> crate::WriteAuthorship {
        let key = Self::key();
        let object = coven_format::sealed_write::WriteObjectPrefix {
            format: coven_format::FormatVersion::CURRENT,
            store_key: key.id(),
            part_keys: vec![key.id(); frame.parts.len()],
        };
        let prefix = object.encode().unwrap();
        let plain = frame.encode().unwrap();
        let sealed = key
            .derive()
            .seal_object_chunk(&Self::path(frame.header.position), &prefix, 0, 0, &plain)
            .unwrap();
        let header = coven_format::sealed_write::WriteObjectLayout::new(
            object,
            &plain,
            frame.parts.iter().map(|part| part.plaintext_length).collect(),
        )
        .unwrap()
        .encode_chunk(&sealed)
        .unwrap();
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
    #[expect(
        clippy::map_err_ignore,
        reason = "a test fixture whose header does not open is the fixture refusal this names"
    )]
    fn header(
        &self,
        evidence: &coven_format::authorship::WriteEvidence,
    ) -> Result<coven_format::write_stream::WriteHeaderFrame, crate::DbError> {
        let sealed = coven_format::sealed_write::WriteObjectPrefix::header_chunk(&evidence.header)?;
        let plain = Self::key()
            .derive()
            .open_object_chunk(&Self::path(evidence.write), &evidence.prefix, 0, 0, sealed)
            .map_err(|_| crate::snapshot_error::invalid("test evidence header does not open"))?;
        Ok(coven_format::write_stream::WriteHeaderFrame::decode(&plain)?)
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

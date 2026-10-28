//! A staged snapshot's write evidence authenticated before installation, and
//! its records read back in order without decoding the snapshot again.
use crate::{
    authorship::Authorship,
    input_preparation::PreparedInputs,
    snapshot_error::{invalid, SnapshotError},
    snapshot_metadata::SnapshotMetadata,
    sqlite::DatabaseConnection,
    DbError, StagedRecords,
};
use coven_format::snapshot::{SnapshotHeader, SnapshotRecord};
use coven_merge::WriteOracle;
use std::collections::VecDeque;

pub(crate) struct PreparedEvidence {
    pub(crate) original: coven_format::authorship::WriteEvidence,
    pub(crate) verified: crate::authorship::AuthenticatedEvidence,
}

/// A staged snapshot ready to install. Its evidence is authenticated, in
/// staged order; its other records stay on the input connection.
pub(crate) struct PreparedSnapshot<'a> {
    db: &'a DatabaseConnection,
    source: i64,
    pub(crate) header: SnapshotHeader,
    evidence: VecDeque<PreparedEvidence>,
}

/// Authenticate each staged write evidence once the snapshot's history is
/// known (D7.5). Staging checked every record but this one.
pub(crate) fn prepare<'a>(
    inputs: &PreparedInputs<'a>,
    records: &StagedRecords,
    authorship: &dyn Authorship,
) -> Result<PreparedSnapshot<'a>, DbError> {
    let source = records.id();
    let header = records.header().clone();
    inputs.run(|db| {
        let oracle = SnapshotMetadata::staged(db, source);
        let mut evidence = VecDeque::new();
        db.for_each(
            "SELECT record FROM temp._coven_staged_records WHERE staging=?1 AND evidence=1 ORDER BY ordinal",
            [source],
            |row| {
                let record = SnapshotRecord::decode_checked(&row.get::<_, Vec<u8>>(0)?, &oracle)
                    .map_err(SnapshotError::Format)?;
                let SnapshotRecord::Evidence(original) = record else {
                    return Err(invalid("staged evidence is another record"));
                };
                let verified =
                    crate::authorship::authenticate(authorship, &header.id.audience, &original)?;
                evidence.push_back(PreparedEvidence { original, verified });
                Ok::<_, DbError>(())
            },
        )?;
        Ok(PreparedSnapshot {
            db,
            source,
            header,
            evidence,
        })
    })
}

impl PreparedSnapshot<'_> {
    /// Read back each staged record in order, without its snapshot's checks:
    /// staging made them. A merge row rebuilds its state with `oracle`.
    pub(crate) fn each(
        self,
        oracle: &impl WriteOracle,
        mut consume: impl FnMut(SnapshotRecord<PreparedEvidence>) -> Result<(), DbError>,
    ) -> Result<(), DbError> {
        let Self {
            db,
            source,
            mut evidence,
            ..
        } = self;
        db.for_each(
            "SELECT evidence,record FROM temp._coven_staged_records WHERE staging=?1 ORDER BY ordinal",
            [source],
            |row| {
                if row.get::<_, bool>(0)? {
                    let prepared = evidence
                        .pop_front()
                        .ok_or_else(|| invalid("staged evidence was not prepared"))?;
                    return consume(SnapshotRecord::Evidence(prepared));
                }
                let record = SnapshotRecord::decode_checked(&row.get::<_, Vec<u8>>(1)?, oracle)
                    .map_err(SnapshotError::Format)?;
                consume(record.try_map_evidence(|_| Err(invalid("staged evidence is unmarked")))?)
            },
        )
    }
}

#[cfg(test)]
#[path = "snapshot_preparation_tests.rs"]
mod tests;

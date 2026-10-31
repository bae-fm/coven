//! A staged snapshot's write evidence authenticated before installation, and
//! its staged tables read back without decoding the snapshot again.
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
    pub(crate) staging: i64,
    pub(crate) header: SnapshotHeader,
    evidence: VecDeque<PreparedEvidence>,
    /// The first record staging found failing the loader's checks, and its
    /// section.
    refusal: Option<(u8, DbError)>,
}

/// Authenticate each staged write evidence once the snapshot's history is
/// known (D7.5). Staging checked every record but this one.
pub(crate) fn prepare<'a>(
    inputs: &PreparedInputs<'a>,
    records: &StagedRecords,
    authorship: &dyn Authorship,
) -> Result<PreparedSnapshot<'a>, DbError> {
    let staging = records.id();
    let header = records.header().clone();
    let refusal = records.refusal();
    inputs.run(|db| {
        let oracle = SnapshotMetadata::staged(db, staging);
        let mut evidence = VecDeque::new();
        db.for_each(
            &format!(
                "SELECT record FROM {} WHERE evidence=1 ORDER BY rowid",
                crate::database::input_database::staging::table(staging, "records")
            ),
            [],
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
        Ok(PreparedSnapshot { db, staging, header, evidence, refusal })
    })
}

impl PreparedSnapshot<'_> {
    /// Refuse the body for the first record staging found failing the
    /// loader's checks, once installing reaches that record's section, in
    /// the order the checks that need the writer would have met it.
    pub(crate) fn refuse(&mut self, section: u8) -> Result<(), DbError> {
        match self.refusal.take() {
            Some((at, refusal)) if at == section => Err(refusal),
            other => {
                self.refusal = other;
                Ok(())
            }
        }
    }

    /// Visit each row `sql` selects from the staged tables, on the input
    /// connection.
    pub(crate) fn each<P: rusqlite::Params>(
        &self,
        sql: &str,
        params: P,
        visit: impl FnMut(&rusqlite::Row<'_>) -> Result<(), DbError>,
    ) -> Result<(), DbError> {
        self.db.for_each(sql, params, visit)
    }

    /// One of the staging's tables.
    pub(crate) fn table(&self, name: &str) -> String {
        crate::database::input_database::staging::table(self.staging, name)
    }

    /// Read back each staged frame of `sections` in order, decoding it with
    /// `oracle`. Evidence comes authenticated.
    pub(crate) fn frames(
        &mut self,
        sections: std::ops::RangeInclusive<u8>,
        oracle: &impl WriteOracle,
        mut consume: impl FnMut(SnapshotRecord<PreparedEvidence>) -> Result<(), DbError>,
    ) -> Result<(), DbError> {
        let evidence = &mut self.evidence;
        self.db.for_each(
            &format!(
                "SELECT evidence,record FROM {} WHERE section BETWEEN ?1 AND ?2 ORDER BY rowid",
                crate::database::input_database::staging::table(self.staging, "records")
            ),
            [*sections.start(), *sections.end()],
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

//! Snapshot framing and cryptographic evidence prepared before installation.
use crate::{
    authorship::Authorship,
    input_preparation::PreparedInputs,
    snapshot_error::{invalid, SnapshotError},
    snapshot_metadata::SnapshotMetadata,
    write_schema::WriteSchema,
    DbError,
};
use coven_format::{
    admin::SnapshotId,
    sealed_snapshot::SnapshotObjectPrefix,
    snapshot::{SnapshotHeader, SnapshotRecord},
    snapshot_stream::SnapshotChunkDecoder,
};
use std::io::Read;

pub(crate) struct PreparedEvidence {
    pub(crate) original: coven_format::authorship::WriteEvidence,
    pub(crate) verified: crate::authorship::AuthenticatedEvidence,
}

pub(crate) struct PreparedSnapshot {
    pub(crate) header: SnapshotHeader,
    records: Vec<SnapshotRecord<PreparedEvidence>>,
}
impl PreparedSnapshot {
    /// Decoded records and authenticated evidence, consumed without reparsing.
    pub(crate) fn records(self) -> Vec<SnapshotRecord<PreparedEvidence>> {
        self.records
    }
}

pub(crate) fn prepare(
    inputs: &PreparedInputs<'_>,
    schema: &WriteSchema,
    expected: &SnapshotId,
    prefix: SnapshotObjectPrefix,
    input: impl Read,
    authorship: &dyn Authorship,
) -> Result<PreparedSnapshot, DbError> {
    inputs.run(|db| prepare_with(db, schema, expected, prefix, input, authorship))
}

fn prepare_with(
    db: &crate::sqlite::DatabaseConnection,
    schema: &WriteSchema,
    expected: &SnapshotId,
    prefix: SnapshotObjectPrefix,
    mut input: impl Read,
    authorship: &dyn Authorship,
) -> Result<PreparedSnapshot, DbError> {
    #[cfg(any(test, feature = "test-utils"))]
    db.record_work(|work| work.snapshot_reads += 1);
    SnapshotMetadata::create_table(db)?;
    let metadata = SnapshotMetadata::new(db);
    let mut decoder = SnapshotChunkDecoder::new(prefix);
    let mut records = Vec::new();
    let mut checked_header = false;
    let mut chunk = [0; coven_format::chunks::CHUNK_SIZE];
    loop {
        let length = match input.read(&mut chunk) {
            Ok(0) => break,
            Ok(length) => length,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(SnapshotError::Read(error).into()),
        };
        let mut bytes = &chunk[..length];
        loop {
            let record = decoder.next_record(&mut bytes, &metadata);
            metadata.check()?;
            let record = record.map_err(SnapshotError::Format)?;
            if !checked_header {
                if let Some(header) = decoder.header() {
                    check_header(schema, expected, header)?;
                    checked_header = true;
                }
            }
            let Some(record) = record else {
                break;
            };
            if let SnapshotRecord::Write(write) = &record {
                metadata.stage(write)?;
            }
            records.push(record.try_map_evidence(|original| {
                let verified =
                    crate::authorship::authenticate(authorship, &expected.audience, &original)?;
                Ok::<_, DbError>(PreparedEvidence { original, verified })
            })?);
        }
    }
    decoder.finish().map_err(SnapshotError::Format)?;
    let header = decoder
        .header()
        .ok_or_else(|| invalid("snapshot has no header"))?
        .clone();
    check_header(schema, expected, &header)?;
    metadata.validate(header.covers)?;
    db.batch("DROP TABLE temp._coven_snapshot_writes")?;
    Ok(PreparedSnapshot { header, records })
}

fn check_header(
    schema: &WriteSchema,
    expected: &SnapshotId,
    header: &SnapshotHeader,
) -> Result<(), DbError> {
    if header.id != *expected {
        return Err(invalid(
            "snapshot identity differs from the requested object",
        ));
    }
    if !schema.versions.contains(&header.schema_version) {
        return Err(SnapshotError::Schema {
            snapshot: header.schema_version,
            database: *schema.versions.end(),
        }
        .into());
    }
    Ok(())
}

#[cfg(test)]
#[path = "snapshot_preparation_tests.rs"]
mod tests;

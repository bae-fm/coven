//! Snapshot framing and cryptographic evidence prepared before installation.
use crate::{
    authorship::Authorship,
    input_preparation::{PreparedInputs, PreparedRecords},
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

pub(crate) struct PreparedSnapshot<'a> {
    pub(crate) header: SnapshotHeader,
    records: PreparedRecords<'a>,
}
impl<'a> PreparedSnapshot<'a> {
    /// The decoded records, in their original order, and their authorship.
    pub(crate) fn records(&self) -> &PreparedRecords<'a> {
        &self.records
    }
}

pub(crate) fn prepare<'a>(
    inputs: &PreparedInputs<'a>,
    schema: &WriteSchema,
    expected: &SnapshotId,
    prefix: SnapshotObjectPrefix,
    input: impl Read,
    authorship: &dyn Authorship,
) -> Result<PreparedSnapshot<'a>, DbError> {
    inputs.run(|db| prepare_with(db, inputs, schema, expected, prefix, input, authorship))
}

fn prepare_with<'a>(
    db: &crate::sqlite::DatabaseConnection,
    inputs: &PreparedInputs<'a>,
    schema: &WriteSchema,
    expected: &SnapshotId,
    prefix: SnapshotObjectPrefix,
    mut input: impl Read,
    authorship: &dyn Authorship,
) -> Result<PreparedSnapshot<'a>, DbError> {
    #[cfg(any(test, feature = "test-utils"))]
    db.record_work(|work| work.snapshot_reads += 1);
    SnapshotMetadata::create_table(db)?;
    let metadata = SnapshotMetadata::new(db);
    let mut decoder = SnapshotChunkDecoder::new(prefix);
    let mut records = inputs.records();
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
            match &record {
                SnapshotRecord::Write(write) => metadata.stage(write)?,
                SnapshotRecord::Evidence(evidence) => {
                    let verified =
                        crate::authorship::authenticate(authorship, &expected.audience, evidence)?;
                    records.keep_authorship(evidence.write, verified)?;
                }
                _ => (),
            }
            records.push(coven_format::merge_fields::encode_snapshot_record(&record)?)?;
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

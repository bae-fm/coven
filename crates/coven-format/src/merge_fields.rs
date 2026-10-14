//! Unframed fields of snapshot merge metadata, also used in coven's SQLite blobs.
//!
//! These are the same bytes as the corresponding snapshot fields, using the
//! shared bounded encoder and decoder. The containing database schema or frame
//! supplies the format version. Decoders reject trailing bytes and malformed
//! fields; relationships between fields and writes remain merge's responsibility.

use crate::wire::{Decoder, Encoder, Wire};
use crate::{
    merge_wire,
    value::{Value, WritePositions},
    Error,
};
use coven_merge::{ColumnValue, Parent, Rule, Timestamp, WriteId};
use std::collections::{BTreeMap, BTreeSet};

fn encode<T: Wire>(value: &T) -> Result<Vec<u8>, Error> {
    let mut out = Encoder::new();
    value.put(&mut out)?;
    Ok(out.bytes)
}

fn decode<T: Wire>(bytes: &[u8]) -> Result<T, Error> {
    decode_with(bytes, T::get)
}

fn decode_with<T>(
    bytes: &[u8],
    get: impl FnOnce(&mut Decoder<'_>) -> Result<T, Error>,
) -> Result<T, Error> {
    let mut input = Decoder::new(bytes)?;
    let value = get(&mut input)?;
    input.finish()?;
    Ok(value)
}

/// Encode a write timestamp using its snapshot field bytes.
pub fn encode_timestamp(value: &Timestamp) -> Result<Vec<u8>, Error> {
    encode(value)
}

/// Decode exactly one snapshot field containing a write timestamp.
pub fn decode_timestamp(bytes: &[u8]) -> Result<Timestamp, Error> {
    let value: Timestamp = decode(bytes)?;
    Ok(value)
}

/// Encode a setter or replacing write using its snapshot field bytes.
pub fn encode_write_id(value: &WriteId) -> Result<Vec<u8>, Error> {
    crate::value::positive(value.number)?;
    encode(value)
}

/// Decode exactly one snapshot field containing a setter or replacing write.
pub fn decode_write_id(bytes: &[u8]) -> Result<WriteId, Error> {
    let value: WriteId = decode(bytes)?;
    crate::value::positive(value.number)?;
    Ok(value)
}

/// Encode a had-read set using its snapshot field bytes.
pub fn encode_write_positions(value: &WritePositions) -> Result<Vec<u8>, Error> {
    value.validate()?;
    encode(value)
}

/// Decode exactly one snapshot field containing a had-read set.
pub fn decode_write_positions(bytes: &[u8]) -> Result<WritePositions, Error> {
    let value: WritePositions = decode(bytes)?;
    value.validate()?;
    Ok(value)
}

/// Encode store-log positions using their snapshot prefix bytes.
pub fn encode_entry_positions(value: &crate::value::EntryPositions) -> Result<Vec<u8>, Error> {
    value.validate()?;
    encode(value)
}

/// Decode exactly one field containing store-log positions.
pub fn decode_entry_positions(bytes: &[u8]) -> Result<crate::value::EntryPositions, Error> {
    let value: crate::value::EntryPositions = decode(bytes)?;
    value.validate()?;
    Ok(value)
}

/// Encode a foreign key's stable identity using its snapshot field bytes.
pub fn encode_foreign_key(value: &coven_merge::ForeignKey) -> Result<Vec<u8>, Error> {
    merge_wire::foreign_key(value)?;
    encode(value)
}

/// Decode exactly one foreign key identity.
pub fn decode_foreign_key(bytes: &[u8]) -> Result<coven_merge::ForeignKey, Error> {
    let value = decode(bytes)?;
    merge_wire::foreign_key(&value)?;
    Ok(value)
}

/// Encode a unique constraint's terms and partial predicate.
pub fn encode_unique_constraint(value: &coven_merge::UniqueConstraint) -> Result<Vec<u8>, Error> {
    merge_wire::unique_constraint(value)?;
    encode(value)
}

/// Decode exactly one unique constraint identity.
pub fn decode_unique_constraint(bytes: &[u8]) -> Result<coven_merge::UniqueConstraint, Error> {
    let value = decode(bytes)?;
    merge_wire::unique_constraint(&value)?;
    Ok(value)
}

/// Encode reference parent generations using its snapshot field bytes.
pub fn encode_parents(value: &BTreeMap<coven_merge::ForeignKey, Parent>) -> Result<Vec<u8>, Error> {
    merge_wire::parents(value)?;
    encode(value)
}

/// Decode exactly one snapshot field containing reference parent generations.
pub fn decode_parents(bytes: &[u8]) -> Result<BTreeMap<coven_merge::ForeignKey, Parent>, Error> {
    let value: BTreeMap<coven_merge::ForeignKey, Parent> = decode(bytes)?;
    merge_wire::parents(&value)?;
    Ok(value)
}

/// Encode a lost cell value and its parents using its snapshot field bytes.
pub fn encode_column_value(value: &ColumnValue<Value>) -> Result<Vec<u8>, Error> {
    merge_wire::column(value)?;
    encode(value)
}

/// Decode exactly one snapshot field containing a lost cell value and its parents.
pub fn decode_column_value(bytes: &[u8]) -> Result<ColumnValue<Value>, Error> {
    let value: ColumnValue<Value> = decode(bytes)?;
    merge_wire::column(&value)?;
    Ok(value)
}

/// Encode a removed row’s values and parents using its snapshot field bytes.
pub fn encode_columns(value: &BTreeMap<String, ColumnValue<Value>>) -> Result<Vec<u8>, Error> {
    merge_wire::columns(value)?;
    encode(value)
}

/// Decode exactly one snapshot field containing a removed row’s values and parents.
pub fn decode_columns(bytes: &[u8]) -> Result<BTreeMap<String, ColumnValue<Value>>, Error> {
    let value = decode_with(bytes, crate::wire::get_name_map)?;
    merge_wire::columns(&value)?;
    Ok(value)
}

/// Encode a removed row’s setters using its snapshot field bytes.
pub fn encode_setters(value: &BTreeMap<String, WriteId>) -> Result<Vec<u8>, Error> {
    merge_wire::setters(value)?;
    encode(value)
}

/// Decode exactly one snapshot field containing a removed row’s setters.
pub fn decode_setters(bytes: &[u8]) -> Result<BTreeMap<String, WriteId>, Error> {
    let value = decode_with(bytes, crate::wire::get_name_map)?;
    merge_wire::setters(&value)?;
    Ok(value)
}

/// Encode removal rules using its snapshot field bytes.
pub fn encode_rules(value: &BTreeSet<Rule>) -> Result<Vec<u8>, Error> {
    merge_wire::rules(value)?;
    encode(value)
}

/// Decode exactly one snapshot field containing removal rules.
pub fn decode_rules(bytes: &[u8]) -> Result<BTreeSet<Rule>, Error> {
    let value: BTreeSet<Rule> = decode(bytes)?;
    merge_wire::rules(&value)?;
    Ok(value)
}

/// Encode the excluded write and the breaking schema version that excluded it.
pub fn encode_exclusion(write: WriteId, version: u32) -> Result<Vec<u8>, Error> {
    crate::value::positive(write.number)?;
    crate::error::require(
        version > 0,
        "breaking schema version",
        crate::error::Rule::Required,
    )?;
    let mut out = Encoder::new();
    write.put(&mut out)?;
    version.put(&mut out)?;
    Ok(out.bytes)
}

/// Decode an excluded write's identity and breaking version from its loss record.
pub fn decode_exclusion(bytes: &[u8]) -> Result<(WriteId, u32), Error> {
    let (write, version): (WriteId, u32) =
        decode_with(bytes, |input| Ok((Wire::get(input)?, Wire::get(input)?)))?;
    crate::value::positive(write.number)?;
    crate::error::require(
        version > 0,
        "breaking schema version",
        crate::error::Rule::Required,
    )?;
    Ok((write, version))
}

/// Encode a dependent change's write and failed sources for its loss record.
pub fn encode_dependency(
    write: WriteId,
    sources: &[crate::write_sources::ChangeId],
) -> Result<Vec<u8>, Error> {
    let mut out = Encoder::new();
    write.put(&mut out)?;
    sources.to_vec().put(&mut out)?;
    Ok(out.bytes)
}

/// Decode a dependent change's write and failed sources.
pub fn decode_dependency(
    bytes: &[u8],
) -> Result<(WriteId, Vec<crate::write_sources::ChangeId>), Error> {
    let (write, sources): (WriteId, Vec<crate::write_sources::ChangeId>) =
        decode_with(bytes, |input| Ok((Wire::get(input)?, Wire::get(input)?)))?;
    crate::value::positive(write.number)?;
    Ok((write, sources))
}

/// Encode a change's verdict with the bytes its snapshot evidence uses.
pub fn encode_change_verdict(
    value: &crate::snapshot_rows::ChangeVerdict,
) -> Result<Vec<u8>, Error> {
    value.validate()?;
    encode(value)
}

/// Decode a change's verdict stored with its original change.
pub fn decode_change_verdict(bytes: &[u8]) -> Result<crate::snapshot_rows::ChangeVerdict, Error> {
    let value: crate::snapshot_rows::ChangeVerdict = decode(bytes)?;
    value.validate()?;
    Ok(value)
}

/// Encode one loss using the same bytes as the snapshot loss section.
pub fn encode_loss(value: &crate::loss::Loss) -> Result<Vec<u8>, Error> {
    value.validate()?;
    encode(value)
}

/// Decode one retained loss using the snapshot loss fields.
pub fn decode_loss(bytes: &[u8]) -> Result<crate::loss::Loss, Error> {
    let value: crate::loss::Loss = decode(bytes)?;
    value.validate()?;
    Ok(value)
}

/// Encode retained generations and winning cells using the snapshot merge fields.
pub fn encode_merge_row(value: &crate::snapshot_rows::MergeRow) -> Result<Vec<u8>, Error> {
    value.validate()?;
    let mut out = Encoder::new();
    value.put(&mut out)?;
    Ok(out.bytes)
}

/// Decode a retained merge row against its original checked write metadata.
pub fn decode_merge_row(
    bytes: &[u8],
    oracle: &impl coven_merge::WriteOracle,
) -> Result<crate::snapshot_rows::MergeRow, Error> {
    let value = decode_with(bytes, |input| {
        crate::snapshot_rows::MergeRow::get(input, oracle)
    })?;
    value.validate()?;
    Ok(value)
}

/// Encode a received write header for local retention, using its D5 field bytes.
/// The receiving transaction checks the write; a refused write commits nothing.
pub fn encode_write_header(value: &crate::write::WriteHeader) -> Result<Vec<u8>, Error> {
    encode(value)
}

/// Decode a retained write header. Its checks ran when the write was accepted.
pub fn decode_write_header(bytes: &[u8]) -> Result<crate::write::WriteHeader, Error> {
    decode(bytes)
}

/// Encode a received row change or dismissal as its D5 frame for local retention.
/// Decoding with `WriteFrame::decode` checks it again before it is reapplied.
pub fn encode_write_frame(value: &crate::dismissal::WriteFrame) -> Result<Vec<u8>, Error> {
    match value {
        crate::dismissal::WriteFrame::Change(row) => crate::encode_frame(2, row),
        crate::dismissal::WriteFrame::Dismissal(dismissal) => crate::encode_frame(3, dismissal),
    }
}

/// Encode a loss identity without its values or changing replacement details.
pub fn encode_loss_identity(value: &crate::loss::Loss) -> Result<Vec<u8>, Error> {
    use crate::loss::{LossCause, LossValues};
    let mut out = Encoder::new();
    value.row.put(&mut out)?;
    value.generation.put(&mut out)?;
    match &value.values {
        LossValues::Cell { column, cell } => {
            0u8.put(&mut out)?;
            column.put(&mut out)?;
            cell.write.put(&mut out)?;
        }
        LossValues::Row(cells) => {
            1u8.put(&mut out)?;
            cells
                .iter()
                .map(|(n, c)| (n.clone(), c.write))
                .collect::<BTreeMap<_, _>>()
                .put(&mut out)?;
        }
    }
    u8::from(value.retired).put(&mut out)?;
    match value.cause {
        LossCause::Write(_) => 0u8,
        LossCause::Rules(_) => 1,
        LossCause::Excluded { .. } => 2,
        LossCause::Dependency { .. } => 3,
    }
    .put(&mut out)?;
    if let LossCause::Excluded { write, .. } | LossCause::Dependency { write, .. } = value.cause {
        write.put(&mut out)?;
    }
    Ok(out.bytes)
}

#[cfg(test)]
#[path = "merge_fields_tests.rs"]
mod tests;

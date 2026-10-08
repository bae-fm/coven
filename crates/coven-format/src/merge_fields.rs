//! Unframed fields of snapshot merge metadata, also used in coven's SQLite blobs.
//!
//! These are the same bytes as the corresponding snapshot fields, using the
//! shared bounded encoder and decoder. The containing database schema or frame
//! supplies the format version. Decoders reject trailing bytes and malformed
//! fields; relationships between fields and writes remain merge's responsibility.

use crate::wire::{Decoder, Encoder, Wire};
use crate::{
    merge_wire,
    snapshot_rows::LostWriteCause,
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

/// Encode an excluded write’s replacement cause using its snapshot field bytes.
pub fn encode_lost_write_cause(value: &LostWriteCause) -> Result<Vec<u8>, Error> {
    value.validate()?;
    encode(value)
}

/// Decode exactly one snapshot field containing an excluded write’s replacement cause.
pub fn decode_lost_write_cause(bytes: &[u8]) -> Result<LostWriteCause, Error> {
    let value: LostWriteCause = decode(bytes)?;
    value.validate()?;
    Ok(value)
}

/// Encode the excluded write and the boundary that displaced it.
pub fn encode_exclusion(write: WriteId, cause: LostWriteCause) -> Result<Vec<u8>, Error> {
    let mut bytes = encode_write_id(&write)?;
    bytes.extend(encode_lost_write_cause(&cause)?);
    Ok(bytes)
}

/// Decode an excluded write's identity and boundary from its loss record.
pub fn decode_exclusion(bytes: &[u8]) -> Result<(WriteId, LostWriteCause), Error> {
    let (write, cause): (WriteId, LostWriteCause) =
        decode_with(bytes, |input| Ok((Wire::get(input)?, Wire::get(input)?)))?;
    crate::value::positive(write.number)?;
    cause.validate()?;
    Ok((write, cause))
}

/// Encode one loss using the same bytes as the snapshot loss section.
pub fn encode_loss(value: &crate::loss::Loss) -> Result<Vec<u8>, Error> {
    value.validate()?;
    encode(value)
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
    }
    .put(&mut out)?;
    if let LossCause::Excluded { write, .. } = value.cause {
        write.put(&mut out)?;
    }
    Ok(out.bytes)
}

#[cfg(test)]
#[path = "merge_fields_tests.rs"]
mod tests;

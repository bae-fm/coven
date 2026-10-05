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
    let mut input = Decoder::new(bytes)?;
    let value = T::get(&mut input)?;
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

/// Encode reference parent generations using its snapshot field bytes.
pub fn encode_parents(value: &BTreeMap<String, Parent>) -> Result<Vec<u8>, Error> {
    merge_wire::parents(value)?;
    encode(value)
}

/// Decode exactly one snapshot field containing reference parent generations.
pub fn decode_parents(bytes: &[u8]) -> Result<BTreeMap<String, Parent>, Error> {
    let value: BTreeMap<String, Parent> = decode(bytes)?;
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
    let value: BTreeMap<String, ColumnValue<Value>> = decode(bytes)?;
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
    let value: BTreeMap<String, WriteId> = decode(bytes)?;
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
    value.entry().validate()?;
    encode(value)
}

/// Decode exactly one snapshot field containing an excluded write’s replacement cause.
pub fn decode_lost_write_cause(bytes: &[u8]) -> Result<LostWriteCause, Error> {
    let value: LostWriteCause = decode(bytes)?;
    value.entry().validate()?;
    Ok(value)
}

#[cfg(test)]
#[path = "merge_fields_tests.rs"]
mod tests;

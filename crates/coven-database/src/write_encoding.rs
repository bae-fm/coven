//! Conversions at the SQL/format boundary and timestamp construction.

use std::time::{SystemTime, UNIX_EPOCH};

use coven_format::value::Value;
use coven_foundation::id_source::DeviceId;
use coven_merge::{Audience, Timestamp};
use rusqlite::types::{Type, ValueRef};
use rusqlite::Row;

use crate::DbError;

pub(crate) fn encoded<T>(result: Result<T, coven_format::Error>) -> Result<T, DbError> {
    result.map_err(|error| match error {
        coven_format::Error::Limit { field, actual, maximum } => {
            DbError::TooLarge { field, actual: actual as u64, maximum: maximum as u64 }
        }
        error => panic!("invalid locally constructed write: {error:?}"),
    })
}

/// A stored fact at `column` that doesn't decode: the database is damaged
/// (§19.3).
pub(crate) fn damaged(column: usize, kind: Type) -> rusqlite::Error {
    decode_failure(column, kind, DbError::DamagedDatabase)
}

/// Stored bytes as coven wrote them, decoded; bytes that fail to decode are
/// damage (§19.3).
#[expect(
    clippy::map_err_ignore,
    reason = "stored bytes that fail to decode are damage, which damaged() names"
)]
pub(crate) fn decoded<T>(result: Result<T, coven_format::Error>) -> rusqlite::Result<T> {
    result.map_err(|_| damaged(0, Type::Blob))
}

/// The value at `column`, which the table's constraints and coven's own
/// writes keep decodable as `T`; a value that isn't is `DamagedDatabase`
/// (§19.3).
pub(crate) fn stored<T: rusqlite::types::FromSql>(
    row: &Row<'_>,
    column: usize,
    kind: Type,
) -> rusqlite::Result<T> {
    row.get(column).map_err(|error| match error {
        rusqlite::Error::InvalidColumnType(..)
        | rusqlite::Error::FromSqlConversionFailure(..)
        | rusqlite::Error::IntegralValueOutOfRange(..) => damaged(column, kind),
        error => error,
    })
}

pub(crate) fn decode_failure(
    column: usize,
    kind: Type,
    error: impl std::error::Error + Send + Sync + 'static,
) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(column, kind, Box::new(error))
}

/// Applied and locally queued originals share one timestamp clock.
pub(crate) fn latest_timestamp(
    database: &crate::sqlite::DatabaseConnection,
) -> Result<Option<Timestamp>, DbError> {
    database.query_row("SELECT max(timestamp) FROM _coven_writes", [], |row| {
        row.get::<_, Option<Vec<u8>>>(0)?
            .map(|bytes| decoded(coven_format::merge_fields::decode_timestamp(&bytes)))
            .transpose()
    })
}

#[expect(clippy::map_err_ignore, reason = "a time past u64 milliseconds is ClockOutOfRange")]
pub(crate) fn timestamp(
    latest: Option<Timestamp>,
    now: SystemTime,
    device: DeviceId,
) -> Result<Timestamp, DbError> {
    let milliseconds = match now.duration_since(UNIX_EPOCH) {
        Ok(elapsed) => u64::try_from(elapsed.as_millis()).map_err(|_| DbError::ClockOutOfRange)?,
        // Before the epoch cannot exceed a stored timestamp. With no writes yet,
        // the timestamp sequence starts at the first representable instant.
        Err(_) => 0,
    };
    Timestamp::next(latest, milliseconds, device).map_err(|error| match error {
        coven_merge::MergeError::MillisecondsOutOfRange(_)
        | coven_merge::MergeError::TimestampExhausted => DbError::ClockOutOfRange,
        error => panic!("timestamp construction violated its invariant: {error:?}"),
    })
}

pub(crate) fn value(value: ValueRef<'_>) -> rusqlite::Result<Value> {
    Ok(match value {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(number) => Value::Integer(number),
        // SQLite identifies both signs of zero as the same value.
        ValueRef::Real(number) => Value::Real(if number == 0.0 { 0 } else { number.to_bits() }),
        ValueRef::Text(bytes) => Value::Text(std::str::from_utf8(bytes)?.to_owned()),
        ValueRef::Blob(bytes) => Value::Blob(bytes.to_vec()),
    })
}

pub(crate) fn sql_value(value: &Value) -> rusqlite::types::Value {
    match value {
        Value::Null => rusqlite::types::Value::Null,
        Value::Integer(number) => (*number).into(),
        Value::Real(bits) => f64::from_bits(*bits).into(),
        Value::Text(text) => text.clone().into(),
        Value::Blob(bytes) => bytes.clone().into(),
    }
}

/// The audience `text` names, spelled exactly as [`Audience`]'s `Display`
/// and SQL form write it: receivers compare a root's audience cell with that
/// text, so any other spelling of the same circle id names no audience (§15).
pub(crate) fn canonical_audience(text: &str) -> Option<Audience> {
    text.parse().ok()
}

/// The audience a column of coven's tables holds; a value no audience is
/// spelled as is damage (§19.3).
pub(crate) fn audience(row: &Row<'_>, column: usize) -> rusqlite::Result<Audience> {
    stored(row, column, Type::Text)
}

/// A stored counter, eight big-endian bytes; any other length is damage
/// (§19.3).
pub(crate) fn counter(bytes: Vec<u8>) -> rusqlite::Result<u64> {
    let bytes: [u8; 8] =
        bytes.try_into().map_err(|_not_eight_bytes: Vec<u8>| damaged(0, Type::Blob))?;
    Ok(u64::from_be_bytes(bytes))
}

#[cfg(test)]
#[path = "write_encoding_tests.rs"]
mod tests;

//! Conversions at the SQL/format boundary and timestamp construction.

use std::time::{SystemTime, UNIX_EPOCH};

use coven_format::value::Value;
use coven_foundation::id_source::DeviceId;
use coven_merge::{Audience, Timestamp};
use rusqlite::types::{Type, ValueRef};

use crate::DbError;

pub(crate) fn encoded<T>(result: Result<T, coven_format::Error>) -> Result<T, DbError> {
    result.map_err(|error| match error {
        coven_format::Error::Limit {
            field,
            actual,
            maximum,
        } => DbError::TooLarge {
            field,
            actual: actual as u64,
            maximum: maximum as u64,
        },
        error => panic!("invalid locally constructed write: {error:?}"),
    })
}

pub(crate) fn decoded<T>(result: Result<T, coven_format::Error>) -> rusqlite::Result<T> {
    result
        .map_err(|error| rusqlite::Error::FromSqlConversionFailure(0, Type::Blob, Box::new(error)))
}

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

pub(crate) fn audience_text(audience: &Audience) -> String {
    match audience {
        Audience::Store => "store".into(),
        Audience::Circle(circle) => circle.to_string(),
    }
}

pub(crate) fn audience(text: &str) -> rusqlite::Result<Audience> {
    if text == "store" {
        Ok(Audience::Store)
    } else {
        uuid::Uuid::parse_str(text)
            .map(|id| Audience::Circle(coven_foundation::id_source::CircleId(id)))
            .map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(0, Type::Text, Box::new(error))
            })
    }
}

pub(crate) fn counter(bytes: Vec<u8>) -> u64 {
    u64::from_be_bytes(
        bytes
            .try_into()
            .expect("stored counter is exactly eight bytes"),
    )
}

#[cfg(test)]
#[path = "write_encoding_tests.rs"]
mod tests;

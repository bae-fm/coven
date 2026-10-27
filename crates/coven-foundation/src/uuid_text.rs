//! Coven's ids at the SQL value boundary, without database access: UUIDs as
//! canonical text, and device ids as the eight big-endian bytes coven stores.

use crate::id_source::{CircleId, DeviceId, FileId, MemberId};
use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, ToSqlOutput, Value, ValueRef};
use rusqlite::ToSql;
use uuid::Uuid;

/// Bind or read a UUID as canonical lowercase hyphenated SQL text.
/// Rusqlite's `uuid` feature binds a bare UUID as a BLOB instead.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UuidText(
    /// The UUID to bind or read as text.
    pub Uuid,
);

impl From<UuidText> for Value {
    fn from(value: UuidText) -> Self {
        Self::Text(value.0.to_string())
    }
}

impl ToSql for UuidText {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Owned((*self).into()))
    }
}

#[derive(Debug, thiserror::Error)]
#[error("not a canonical lowercase hyphenated UUID")]
struct InvalidUuidText;

impl FromSql for UuidText {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let text = value.as_str()?;
        match Uuid::try_parse(text) {
            Ok(id) if id.to_string() == text => Ok(Self(id)),
            _ => Err(FromSqlError::Other(Box::new(InvalidUuidText))),
        }
    }
}

impl ToSql for CircleId {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Owned(UuidText(self.0).into()))
    }
}

impl FromSql for CircleId {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        UuidText::column_result(value).map(|id| Self(id.0))
    }
}

/// A member id as canonical UUID text, as `coven_members` holds it.
impl ToSql for MemberId {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Owned(UuidText(self.0).into()))
    }
}

impl FromSql for MemberId {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        UuidText::column_result(value).map(|id| Self(id.0))
    }
}

/// A file id as canonical UUID text, as `coven_abandoned` holds it.
impl ToSql for FileId {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Owned(UuidText(self.0).into()))
    }
}

impl FromSql for FileId {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        UuidText::column_result(value).map(|id| Self(id.0))
    }
}

/// A device id as its eight big-endian bytes, as `coven_devices` holds it:
/// the whole 64-bit id, which an SQL integer would hold only as a signed
/// number.
impl ToSql for DeviceId {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Owned(Value::Blob(
            self.0.to_be_bytes().to_vec(),
        )))
    }
}

impl FromSql for DeviceId {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let bytes = value.as_blob()?;
        let bytes =
            <[u8; 8]>::try_from(bytes).map_err(|_wrong_size| FromSqlError::InvalidBlobSize {
                expected_size: 8,
                blob_size: bytes.len(),
            })?;
        Ok(Self(u64::from_be_bytes(bytes)))
    }
}

#[cfg(test)]
#[path = "uuid_text_tests.rs"]
mod tests;

//! Coven's ids at the SQL value boundary, without database access: UUIDs as
//! canonical text; store, key and stored objects' ids as their sixteen bytes;
//! and device ids as the eight big-endian bytes coven stores.

use crate::id_source::{CircleId, DeviceId, FileId, KeyId, MemberId, ObjectId, StoreId};
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

impl FromSql for UuidText {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        crate::id_source::canonical_uuid(value.as_str()?)
            .map(Self)
            .map_err(|error| FromSqlError::Other(Box::new(error)))
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

/// A member id as canonical UUID text, as coven's member columns and the
/// `coven_members` view hold it.
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

/// A file id as canonical UUID text, as `coven_files` and the app's file
/// columns hold it.
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

/// A UUID's sixteen bytes, as a BLOB.
fn uuid_blob(id: &Uuid) -> ToSqlOutput<'_> {
    ToSqlOutput::Borrowed(ValueRef::Blob(id.as_bytes()))
}

/// A UUID from a BLOB of exactly sixteen bytes.
fn uuid_from_blob(value: ValueRef<'_>) -> FromSqlResult<Uuid> {
    let bytes = value.as_blob()?;
    let bytes = <[u8; 16]>::try_from(bytes).map_err(|_wrong_size| {
        FromSqlError::InvalidBlobSize { expected_size: 16, blob_size: bytes.len() }
    })?;
    Ok(Uuid::from_bytes(bytes))
}

/// A stored object's id as its sixteen bytes, as coven's object columns hold
/// it.
impl ToSql for ObjectId {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(uuid_blob(&self.0))
    }
}

impl FromSql for ObjectId {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        uuid_from_blob(value).map(Self)
    }
}

/// A store id as its sixteen bytes, as `_coven_store` holds it.
impl ToSql for StoreId {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(uuid_blob(&self.0))
    }
}

impl FromSql for StoreId {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        uuid_from_blob(value).map(Self)
    }
}

/// A store or circle key's id as its sixteen bytes, as coven's key columns
/// hold it.
impl ToSql for KeyId {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(uuid_blob(&self.0))
    }
}

impl FromSql for KeyId {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        uuid_from_blob(value).map(Self)
    }
}

/// A device id as its eight big-endian bytes, as `coven_devices` holds it:
/// the whole 64-bit id, which an SQL integer would hold only as a signed
/// number.
impl ToSql for DeviceId {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Owned(Value::Blob(self.0.to_be_bytes().to_vec())))
    }
}

impl FromSql for DeviceId {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let bytes = value.as_blob()?;
        let bytes = <[u8; 8]>::try_from(bytes).map_err(|_wrong_size| {
            FromSqlError::InvalidBlobSize { expected_size: 8, blob_size: bytes.len() }
        })?;
        Ok(Self(u64::from_be_bytes(bytes)))
    }
}

#[cfg(test)]
#[path = "uuid_text_tests.rs"]
mod tests;

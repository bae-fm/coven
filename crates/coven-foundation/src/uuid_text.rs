//! Canonical UUID text at the SQL value boundary, without database access.

use crate::id_source::CircleId;
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

#[cfg(test)]
#[path = "uuid_text_tests.rs"]
mod tests;

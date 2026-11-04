//! An audience's one text spelling, `store` or the circle's canonical id,
//! which app rows, coven's tables, object paths and saved state share.

use crate::Audience;
use coven_foundation::id_source::CircleId;
use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, ToSqlOutput, ValueRef};
use rusqlite::ToSql;

impl std::fmt::Display for Audience {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Store => f.write_str("store"),
            Self::Circle(circle) => std::fmt::Display::fmt(circle, f),
        }
    }
}

impl std::str::FromStr for Audience {
    type Err = InvalidAudience;
    /// Only the spelling [`Audience`]'s `Display` writes: any other spelling
    /// of the same circle id names no audience.
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        if text == "store" {
            return Ok(Self::Store);
        }
        coven_foundation::id_source::canonical_uuid(text)
            .map(|id| Self::Circle(CircleId(id)))
            .map_err(|_not_canonical| InvalidAudience)
    }
}

/// Text that is neither `store` nor a circle's canonical id.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("not an audience: `store` or a circle's canonical id")]
pub struct InvalidAudience;

impl ToSql for Audience {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Owned(self.to_string().into()))
    }
}

impl FromSql for Audience {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        value.as_str()?.parse().map_err(|error| FromSqlError::Other(Box::new(error)))
    }
}

#[cfg(test)]
#[path = "audience_sql_tests.rs"]
mod tests;

//! Audience text bindings share the representation used by app-row validation.

use crate::Audience;
use coven_foundation::id_source::CircleId;
use rusqlite::types::{FromSql, FromSqlResult, ToSqlOutput, ValueRef};
use rusqlite::ToSql;

impl std::fmt::Display for Audience {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Store => f.write_str("store"),
            Self::Circle(circle) => std::fmt::Display::fmt(circle, f),
        }
    }
}

impl ToSql for Audience {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Owned(self.to_string().into()))
    }
}

impl FromSql for Audience {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        if value.as_str()? == "store" {
            Ok(Self::Store)
        } else {
            CircleId::column_result(value).map(Self::Circle)
        }
    }
}

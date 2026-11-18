//! Coven stores some values in its tables and views as names, such as a
//! member's role in `coven_members.role`. Each value has one name. Coven
//! writes and reads that name, and the app reads it from the column or
//! parses it from a JSON array of the column's values (api.md E4).

/// The SQL value conversions [`column_names!`](crate::column_names!)
/// implements. The macro names them through this crate, so a crate that
/// calls it needs no rusqlite dependency of its own.
#[doc(hidden)]
pub mod sql {
    pub use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, ToSqlOutput, ValueRef};
    pub use rusqlite::{Result, ToSql};
}

/// Text that names none of a type's values.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("`{text}` names no {kind}")]
pub struct UnknownName {
    /// The kind of value coven tried to parse the text as, such as
    /// "member role".
    pub kind: &'static str,
    /// The text.
    pub text: String,
}

/// Give the enum `$type`, whose values are `Copy`, the name each value has
/// in coven's columns: `name()` gives it, `FromStr` reads it, and `ToSql`
/// and `FromSql` bind and read it as SQL text. `$kind` names the kind of
/// value, such as "member role", that `UnknownName` reports when `FromStr`
/// finds no value with the text's name.
#[macro_export]
macro_rules! column_names {
    ($type:ty, $kind:literal, { $($value:ident => $name:literal),+ $(,)? }) => {
        impl $type {
            /// The name coven's columns hold for this value.
            pub fn name(self) -> &'static str {
                match self {
                    $(Self::$value => $name,)+
                }
            }
        }

        impl ::std::str::FromStr for $type {
            type Err = $crate::column_names::UnknownName;
            fn from_str(text: &str) -> Result<Self, Self::Err> {
                match text {
                    $($name => Ok(Self::$value),)+
                    _ => Err($crate::column_names::UnknownName {
                        kind: $kind,
                        text: text.to_owned(),
                    }),
                }
            }
        }

        const _: () = {
            use $crate::column_names::sql::{
                FromSql, FromSqlError, FromSqlResult, Result, ToSql, ToSqlOutput, ValueRef,
            };

            impl ToSql for $type {
                fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
                    Ok(ToSqlOutput::Borrowed(ValueRef::Text(self.name().as_bytes())))
                }
            }

            impl FromSql for $type {
                fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
                    value.as_str()?.parse().map_err(|error| FromSqlError::Other(Box::new(error)))
                }
            }
        };
    };
}

#[cfg(test)]
#[path = "column_names_tests.rs"]
mod tests;

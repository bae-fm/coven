//! Coven stores some values in its tables and views as names, such as a
//! member's role in `coven_members.role`. Each value has one name. Coven
//! writes and reads that name, and the app reads it from the column or
//! parses it from a JSON array of the column's values (api.md E4).

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

        impl ::rusqlite::ToSql for $type {
            fn to_sql(&self) -> ::rusqlite::Result<::rusqlite::types::ToSqlOutput<'_>> {
                Ok(::rusqlite::types::ToSqlOutput::Borrowed(::rusqlite::types::ValueRef::Text(
                    self.name().as_bytes(),
                )))
            }
        }

        impl ::rusqlite::types::FromSql for $type {
            fn column_result(
                value: ::rusqlite::types::ValueRef<'_>,
            ) -> ::rusqlite::types::FromSqlResult<Self> {
                value
                    .as_str()?
                    .parse()
                    .map_err(|error| ::rusqlite::types::FromSqlError::Other(Box::new(error)))
            }
        }
    };
}

#[cfg(test)]
#[path = "column_names_tests.rs"]
mod tests;

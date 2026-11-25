//! What the compiler knows of each compiled call: the manifest records that
//! crates hand to the crates that spread their fragments, and the typed
//! fields the generated code reads.

use super::header::{Parameter, Relation, RustType};

/// How a field reads its column.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Reading {
    /// The column's value, as the field's type reads it.
    Value,
    /// The column's text, which the field's type decodes from JSON.
    Json,
    /// The columns of a fragment, the first of which is the field's column.
    Spread {
        /// The Rust path of the fragment's type, as the call's own code
        /// writes it.
        path: String,
        /// The position, from the field's column, of the fragment's first
        /// column that can't be NULL, which a fragment that holds this one
        /// reports as its own.
        presence: Option<usize>,
    },
    /// The columns of a fragment that can be missing: they are all NULL then.
    MissingSpread {
        /// The Rust path of the fragment's type, as the call's own code
        /// writes it.
        path: String,
        /// The position, from the field's column, of the fragment's first
        /// column that can't be NULL, which tells a row from the NULLs of a
        /// missing row.
        presence: usize,
    },
}

/// The name of a field: one that Rust reads as an identifier, raw when it is
/// a keyword.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FieldName(String);

impl FieldName {
    /// The name `name`, when it can be an identifier.
    pub(crate) fn new(name: &str) -> Option<Self> {
        let bare = name.strip_prefix("r#").unwrap_or(name);
        let mut characters = bare.chars();
        let starts = characters.next().is_some_and(|first| first.is_alphabetic() || first == '_');
        let rest = characters.all(|next| next.is_alphanumeric() || next == '_');
        let reserved = matches!(bare, "_" | "self" | "Self" | "super" | "crate");
        (starts && rest && !reserved).then(|| Self(name.to_owned()))
    }

    /// The name as text.
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

/// One field of a generated row or fragment type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Field {
    /// The field's name.
    pub(crate) name: FieldName,
    /// The field's Rust type.
    pub(crate) ty: RustType,
    /// How it reads its column.
    pub(crate) reading: Reading,
    /// The column's position among the call's result columns.
    pub(crate) column: usize,
}

/// A fragment as the crates that spread it need it (`SPEC.md`, section 10).
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct Record {
    /// The fragment type's Rust path.
    pub(crate) path: String,
    /// The parameters its SQL reads.
    pub(crate) parameters: Vec<Parameter>,
    /// The table of the parent row it reads.
    pub(crate) parent: Option<String>,
    /// The relations it reads.
    pub(crate) relations: Vec<Relation>,
    /// Its SQL with every spread expanded.
    pub(crate) sql: String,
    /// How many columns it returns.
    pub(crate) width: usize,
    /// The position of its first column that can't be NULL, which tells a
    /// row from the NULLs of a missing row.
    pub(crate) presence: Option<usize>,
    /// Whether it returns exactly one row for any parent row.
    pub(crate) one_row: bool,
}

/// The text of a crate's manifest: the records of its fragments.
#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
pub(crate) struct Manifest {
    pub(crate) fragments: Vec<Record>,
}

/// The Rust type that a table column of the STRICT type `declared` reads
/// as, when it has one.
pub(crate) fn strict_type(declared: &str) -> Option<&'static str> {
    match declared.to_ascii_uppercase().as_str() {
        "INTEGER" | "INT" => Some("i64"),
        "TEXT" => Some("String"),
        "BLOB" => Some("Vec<u8>"),
        "REAL" => Some("f64"),
        "ANY" => Some("::sqlive_compose::generated::sqlive::SqlValue"),
        _ => None,
    }
}

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
        /// The positions, from the field's column, of the fragment's columns
        /// that each of its rows holds a value in (`Record::required`),
        /// which a fragment that holds this one reports as its own.
        required: Vec<usize>,
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

impl Reading {
    /// The path of the fragment whose type the field names.
    pub(crate) fn fragment(&self) -> Option<&str> {
        match self {
            Reading::Spread { path, .. } | Reading::MissingSpread { path, .. } => Some(path),
            Reading::Value | Reading::Json => None,
        }
    }
}

/// The name of a field: an identifier, raw when the name is a keyword.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FieldName(syn::Ident);

impl FieldName {
    /// The name `name`, when Rust reads it, or reads it after `r#`, as an
    /// identifier.
    pub(crate) fn new(name: &str) -> Option<Self> {
        let bare = syn::parse_str::<syn::Ident>(name);
        let raw = || syn::parse_str::<syn::Ident>(&format!("r#{name}"));
        bare.or_else(|_| raw()).ok().map(Self)
    }

    /// The name as an identifier.
    pub(crate) fn ident(&self) -> syn::Ident {
        self.0.clone()
    }

    /// The name as written in Rust: with `r#` when it is raw.
    pub(crate) fn as_text(&self) -> String {
        self.0.to_string()
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
    /// The positions, in order, of the columns that each of its rows holds a
    /// value in: those its type reads into a field that holds no NULL.
    pub(crate) required: Vec<usize>,
    /// Whether it returns exactly one row for any parent row.
    pub(crate) one_row: bool,
}

impl Record {
    /// The position of its first column that can't be NULL, which tells a
    /// row from the NULLs of a missing row.
    pub(crate) fn presence(&self) -> Option<usize> {
        self.required.first().copied()
    }
}

/// The text of a crate's manifest: the records of its fragments.
#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
pub(crate) struct Manifest {
    pub(crate) fragments: Vec<Record>,
}

/// The Rust type that a table column of the STRICT type `declared` reads
/// as, when it has one.
pub(crate) fn strict_type(declared: &str, macros: &MacrosCrate) -> Option<String> {
    let name = match declared.to_ascii_uppercase().as_str() {
        "INTEGER" | "INT" => "i64",
        "TEXT" => "String",
        "BLOB" => "Vec<u8>",
        "REAL" => "f64",
        "ANY" => return Some(macros.generated("sqlive_reads::SqlValue")),
        _ => return None,
    };
    Some(name.to_owned())
}

/// The crate through which the compiled crate's code names the macros
/// (`Input::macros_crate`). The generated code names everything it uses in
/// that crate's `generated` module.
pub(crate) struct MacrosCrate(syn::Ident);

impl MacrosCrate {
    /// The crate named `name`, when `name` is one Rust identifier.
    pub(crate) fn new(name: &str) -> Option<Self> {
        syn::parse_str(name).ok().map(Self)
    }

    /// Whether `name` is the crate's name.
    pub(crate) fn is(&self, name: &str) -> bool {
        self.0 == name
    }

    /// The path of `tail` in the crate's `generated` module.
    pub(crate) fn generated(&self, tail: &str) -> String {
        format!("::{}::generated::{tail}", self.0)
    }
}

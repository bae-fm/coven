//! What the compiler reports: an error stops the build, and a warning
//! doesn't.

use std::fmt;

use super::header::HeaderError;

/// Where a macro call is: the file's path in its crate, and the call's line
/// and column, from 1.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Location {
    /// The file's path as the source reader gets it, below the crate's
    /// folder, which the crate's name stands for.
    pub file: String,
    /// The line of the macro's path.
    pub line: usize,
    /// The column of the macro's path.
    pub column: usize,
}

impl fmt::Display for Location {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}:{}:{}", self.file, self.line, self.column)
    }
}

/// Why a macro call fails the build.
#[derive(Debug, thiserror::Error)]
pub enum Cause {
    /// The source reader couldn't read a module's file.
    #[error("can't read {path}: {source}")]
    SourceRead {
        /// The file's path.
        path: String,
        /// The reader's error.
        source: std::io::Error,
    },
    /// A module's file isn't Rust.
    #[error("can't parse {path}: {message}")]
    SourceSyntax {
        /// The file's path.
        path: String,
        /// The parser's message.
        message: String,
    },
    /// The call's tokens aren't a header and SQL.
    #[error("{0}")]
    Header(#[from] HeaderError),
    /// Two calls of one crate have the same name, so they have one file.
    #[error("{name} is also the name of the call at {other}")]
    DuplicateName {
        /// The shared name.
        name: String,
        /// The first call that has it.
        other: Location,
    },
    /// SQLite refused the SQL.
    #[error("{}", sqlite_message(.0))]
    Sqlite(sqlive_reads::SqliteError),
    /// The SQL reads a parameter that the header doesn't declare.
    #[error("{concerns} reads :{parameter}, which its header does not declare")]
    UndeclaredParameter {
        /// The call's name.
        concerns: String,
        /// The parameter.
        parameter: String,
    },
    /// The header declares a parameter that the SQL doesn't read.
    #[error("{concerns} declares {parameter}, which its SQL does not read")]
    UnreadParameter {
        /// The call's name.
        concerns: String,
        /// The parameter.
        parameter: String,
    },
    /// A column the SQL computes has no declared type.
    #[error("{concerns} computes the column {column}, and its header gives {column} no type")]
    UntypedColumn {
        /// The call's name.
        concerns: String,
        /// The column.
        column: String,
    },
    /// A column the table declares with a type that has no Rust type of its
    /// own has no declaration.
    #[error("{concerns}: the column {column} has the type {declared}, which has no Rust type until the header gives {column} one")]
    UnmappedType {
        /// The call's name.
        concerns: String,
        /// The column.
        column: String,
        /// The type the table declares.
        declared: String,
    },
    /// A column that can be NULL has a declared type that can't hold NULL.
    #[error(
        "{column} can be NULL{}, and the header types it {declared}, which holds no NULL",
        in_select(.select)
    )]
    NullableColumn {
        /// The column.
        column: String,
        /// The first SELECT of a compound, counted from 1, that can give it
        /// NULL.
        select: Option<usize>,
        /// The declared type.
        declared: String,
    },
    /// A `write!` whose SQL is not one INSERT, REPLACE, UPDATE or DELETE.
    #[error(
        "{concerns} is a write, and its SQL is not one INSERT, REPLACE, UPDATE or DELETE statement"
    )]
    NotAWrite {
        /// The call's name.
        concerns: String,
    },
    /// A fragment or a read whose SQL changes the database.
    #[error("{concerns} changes the database, and only a write! may")]
    WriteInRead {
        /// The call's name.
        concerns: String,
    },
    /// A parameter that can be NULL is the whole value of a NOT NULL column.
    #[error("{concerns} binds :{parameter}, which can be NULL, to {column}, which is NOT NULL")]
    NullIntoNotNull {
        /// The call's name.
        concerns: String,
        /// The parameter.
        parameter: String,
        /// The column.
        column: String,
    },
    /// The header declares a column that the SQL doesn't return.
    #[error("{concerns} declares the column {column}, which its SQL does not return")]
    UnreturnedColumn {
        /// The call's name.
        concerns: String,
        /// The column.
        column: String,
    },
    /// sqlive can't read the statement's conditions.
    #[error("sqlive can't analyze {concerns}: {why}")]
    Unanalyzable {
        /// The call's name.
        concerns: String,
        /// sqlive's reason.
        why: String,
    },
    /// SQLite takes `@name` as a parameter of its own, and the compiler
    /// reserves `@` for its markers.
    #[error(
        "{concerns} writes {text}: `@` marks a spread or a parent column, and it is no parameter"
    )]
    StrayMarker {
        /// The call's name.
        concerns: String,
        /// The marker's text.
        text: String,
    },
    /// The SQL reads a parameter that isn't written `:name`.
    #[error("{concerns} reads the parameter {parameter}, and a parameter is written :name")]
    ForeignParameter {
        /// The call's name.
        concerns: String,
        /// The parameter, as SQLite names it.
        parameter: String,
    },
    /// Two result columns have one name.
    #[error("{concerns} returns the column {column} twice")]
    DuplicateColumn {
        /// The call's name.
        concerns: String,
        /// The column.
        column: String,
    },
    /// Two `*` result columns leave no way to tell which table gives each
    /// column.
    #[error("{concerns} selects more than one `*`, so name the columns")]
    Stars {
        /// The call's name.
        concerns: String,
    },
    /// A spread names a fragment that no crate in reach offers.
    #[error("{concerns} spreads {path}, which is no fragment of this crate or of a dependency")]
    UnknownFragment {
        /// The call's name.
        concerns: String,
        /// The path as written.
        path: String,
    },
    /// A spread's arguments or its markers aren't written as the header
    /// syntax has them.
    #[error("{concerns}: the spread of {fragment} is not written `@path(name: value, …)`")]
    BadSpread {
        /// The call's name.
        concerns: String,
        /// The fragment.
        fragment: String,
    },
    /// A spread binds a name that the fragment doesn't declare.
    #[error("the spread of {fragment} binds {argument}, which the fragment does not declare")]
    UnknownArgument {
        /// The fragment.
        fragment: String,
        /// The argument.
        argument: String,
    },
    /// A spread binds one name twice.
    #[error("the spread of {fragment} binds {argument} twice")]
    DuplicateArgument {
        /// The fragment.
        fragment: String,
        /// The argument.
        argument: String,
    },
    /// A spread binds a name to a value of the wrong form.
    #[error("the spread of {fragment} binds {argument} to a value that is not the form it takes")]
    BadArgument {
        /// The fragment.
        fragment: String,
        /// The argument.
        argument: String,
    },
    /// A spread binds no value to a parameter.
    #[error("the spread of {fragment} binds no value to its parameter {parameter}")]
    NoValue {
        /// The fragment.
        fragment: String,
        /// The parameter.
        parameter: String,
    },
    /// A spread of a fragment that reads a parent row names none.
    #[error("the spread of {fragment} binds no parent row, and the fragment reads one")]
    MissingParent {
        /// The fragment.
        fragment: String,
    },
    /// A spread names a parent row for a fragment that reads none.
    #[error("the spread of {fragment} binds a parent row, and the fragment reads none")]
    NoParentRow {
        /// The fragment.
        fragment: String,
    },
    /// A spread gives no rows to a relation.
    #[error("the spread of {fragment} binds no rows to its relation {relation}")]
    MissingRelation {
        /// The fragment.
        fragment: String,
        /// The relation.
        relation: String,
    },
    /// `@parent` outside a fragment that reads a parent row.
    #[error("{concerns} writes @parent, and only a fragment with a parent row can")]
    ParentOutsideFragment {
        /// The call's name.
        concerns: String,
    },
    /// A fragment that reads a parent row is spread as a table.
    #[error("{fragment} reads a parent row, so its spread is a value, not a table in FROM")]
    ParentSpreadInFrom {
        /// The fragment.
        fragment: String,
    },
    /// The FROM item a spread names as its parent row doesn't read the
    /// fragment's parent table.
    #[error("the spread of {fragment} takes its parent row from {alias}, which is no FROM item that reads {table}")]
    WrongParentTable {
        /// The fragment.
        fragment: String,
        /// The alias the spread names.
        alias: String,
        /// The table the fragment reads a row of.
        table: String,
    },
    /// A fragment that reads a parent row returns more or fewer than one
    /// column.
    #[error(
        "{concerns} reads a parent row and returns {width} columns, and a value is one column"
    )]
    ParentColumns {
        /// The call's name.
        concerns: String,
        /// How many columns.
        width: usize,
    },
    /// A spread that can be missing has no column that is NULL only when it
    /// is.
    #[error("{concerns} spreads {fragment} where its row can be missing, and it has no NOT NULL column to tell")]
    NoPresence {
        /// The call's name.
        concerns: String,
        /// The fragment.
        fragment: String,
    },
    /// A later SELECT of a compound can give NULL in a column of a spread
    /// that each row of its fragment holds a value in, in a row the reader
    /// takes for one of the fragment's.
    #[error("{concerns}: SELECT {select} of the compound can give NULL in {column}, where each row of {fragment} holds a value")]
    NullInSpread {
        /// The call's name.
        concerns: String,
        /// The SELECT, counted from 1.
        select: usize,
        /// The column, after the spread's alias.
        column: String,
        /// The fragment.
        fragment: String,
    },
    /// The result items of the statement don't add up to its columns.
    #[error("{concerns}: the compiler can't tell which result item gives which column")]
    Unaligned {
        /// The call's name.
        concerns: String,
    },
    /// The keys of a JSON document that the SQL builds differ from the fields
    /// of the shape that the header declares.
    #[error("{}", json_keys_message(column, shape, extra, missing))]
    JsonKeys {
        /// The column.
        column: String,
        /// The shape.
        shape: String,
        /// The keys the SQL builds that the shape lacks.
        extra: Vec<String>,
        /// The shape's fields that the SQL builds no key for.
        missing: Vec<String>,
    },
    /// The SQL builds a document of another kind than the header declares.
    #[error("{column}: the SQL builds {built}, and the header declares {declared}")]
    JsonKind {
        /// The column.
        column: String,
        /// What the header declares.
        declared: String,
        /// What the SQL builds.
        built: String,
    },
    /// A column or spread has a name that can't be a field's.
    #[error("{concerns} returns {name}, which can't name a Rust field: give the column another name with AS")]
    BadIdentifier {
        /// The call's name.
        concerns: String,
        /// The name.
        name: String,
    },
    /// A dependency's manifest names a fragment by a path that isn't Rust's.
    #[error("a manifest names a fragment at {path}, which is no Rust path")]
    BadPath {
        /// The path.
        path: String,
    },
    /// Fragments spread each other in a circle.
    #[error("{first} spreads itself: {}", chain.join(", "))]
    Cycle {
        /// The fragment the circle starts and ends at.
        first: String,
        /// The fragments in the circle, from the first back to it.
        chain: Vec<String>,
    },
    /// A dependency's manifest isn't a manifest.
    #[error("can't read a dependency's manifest: {0}")]
    ManifestRead(serde_json::Error),
    /// The schema's setup fails.
    #[error("the schema's setup fails: {0}")]
    Setup(sqlive_reads::SqliteError),
    /// The crate's manifest can't be written as text.
    #[error("can't write the crate's manifest: {0}")]
    Manifest(serde_json::Error),
}

fn keys(names: &[String]) -> String {
    let noun = if names.len() == 1 { "key" } else { "keys" };
    format!("{noun} {}", names.join(", "))
}

/// Where a compound's error names the SELECT that can give NULL.
fn in_select(select: &Option<usize>) -> String {
    select.map_or_else(String::new, |select| format!(" in SELECT {select} of the compound"))
}

fn json_keys_message(column: &str, shape: &str, extra: &[String], missing: &[String]) -> String {
    let mut parts = Vec::new();
    if !extra.is_empty() {
        parts.push(format!("has the {}, which {shape} lacks", keys(extra)));
    }
    if !missing.is_empty() {
        parts.push(format!("lacks {shape}'s {}", keys(missing)));
    }
    format!("{column}: the document {}", parts.join(", and "))
}

/// What SQLite says of the SQL it refused, without the SQL's text, which the
/// error's location stands for.
fn sqlite_message(error: &sqlive_reads::SqliteError) -> String {
    match error {
        sqlive_reads::SqliteError::SqlInputError { msg, .. } => msg.clone(),
        other => other.to_string(),
    }
}

/// A failure of the compile.
#[derive(Debug)]
pub enum CompileError {
    /// A failure of one macro call.
    Call {
        /// The call's place.
        location: Location,
        /// The call's name.
        concerns: String,
        /// Why.
        cause: Cause,
    },
    /// A failure that belongs to no call: the schema's setup, a source file or
    /// a dependency's manifest.
    Crate {
        /// Why.
        cause: Cause,
    },
}

impl CompileError {
    /// The call's place, when a call failed.
    pub fn location(&self) -> Option<&Location> {
        match self {
            Self::Call { location, .. } => Some(location),
            Self::Crate { .. } => None,
        }
    }

    /// Why it failed.
    pub fn cause(&self) -> &Cause {
        match self {
            Self::Call { cause, .. } | Self::Crate { cause } => cause,
        }
    }
}

impl fmt::Display for CompileError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Call { location, cause, .. } => write!(formatter, "{location}: {cause}"),
            Self::Crate { cause } => write!(formatter, "{cause}"),
        }
    }
}

impl std::error::Error for CompileError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.cause())
    }
}

/// A read that sqlive analyzes and reports whole in a table: the build goes
/// on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Warning {
    /// The read's place.
    pub location: Location,
    /// The read's name.
    pub concerns: String,
    /// The table or view the read depends on every row of.
    pub table: String,
    /// sqlive's reasons.
    pub reason: String,
}

impl fmt::Display for Warning {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}: {} reads every row of {}: {}",
            self.location, self.concerns, self.table, self.reason
        )
    }
}

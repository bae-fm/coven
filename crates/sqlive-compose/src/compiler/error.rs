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
    Sqlite(sqlive::SqliteError),
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
    #[error("{column} can be NULL, and the header types it {declared}, which holds no NULL")]
    NullableColumn {
        /// The column.
        column: String,
        /// The declared type.
        declared: String,
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
    /// A bare column name that more than one FROM item of different joins
    /// could give.
    #[error("{concerns} names the column {column} with no table, and more than one FROM item could give it")]
    UnplacedColumn {
        /// The call's name.
        concerns: String,
        /// The column.
        column: String,
    },
    /// The schema's setup fails.
    #[error("the schema's setup fails: {0}")]
    Setup(sqlive::SqliteError),
    /// The crate's manifest can't be written as text.
    #[error("can't write the crate's manifest: {0}")]
    Manifest(serde_json::Error),
}

/// What SQLite says of the SQL it refused, without the SQL's text, which the
/// error's location stands for.
fn sqlite_message(error: &sqlive::SqliteError) -> String {
    match error {
        sqlive::SqliteError::SqlInputError { msg, .. } => msg.clone(),
        other => other.to_string(),
    }
}

/// A failure of one macro call, or of the schema's setup.
#[derive(Debug)]
pub struct CompileError {
    /// The call's place, or `None` for the schema's setup, which no call
    /// holds.
    pub location: Option<Location>,
    /// The call's name, or empty for the schema's setup.
    pub concerns: String,
    /// Why.
    pub cause: Cause,
}

impl fmt::Display for CompileError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.location {
            Some(location) => write!(formatter, "{location}: {}", self.cause),
            None => write!(formatter, "{}", self.cause),
        }
    }
}

impl std::error::Error for CompileError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.cause)
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

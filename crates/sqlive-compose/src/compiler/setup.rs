//! The schema an app's reads are checked against, as data.

/// A SQL function that the schema's SQL names: its name, its number of
/// arguments and whether it is deterministic.
pub use sqlive_reads::StandInFunction as Function;

/// What the compiler runs to build the schema: the SQL texts in the order to
/// run them, and what the SQL names that SQLite doesn't have. SQLite fails to
/// prepare a statement that names an unknown function or collation, so the
/// compiler registers a stand-in for each. It also names the views and tables
/// the host's live queries read whole, which each read's scope plan depends
/// on.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Setup {
    /// The schema's SQL texts, in the order to run.
    pub sql: Vec<String>,
    /// The functions the schema uses.
    pub functions: Vec<Function>,
    /// The collations the schema uses, by name.
    pub collations: Vec<String>,
}

impl Setup {
    pub(crate) fn probe(&self) -> sqlive_reads::SqliteResult<sqlive_reads::SchemaProbe> {
        sqlive_reads::SchemaProbe::open(&self.sql, &self.functions, &self.collations)
    }
}

//! The schema an app's reads are checked against, as data.

/// A SQL function that the schema's SQL names.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Function {
    /// The function's name.
    pub name: String,
    /// How many arguments it takes, or -1 for any number.
    pub arguments: i32,
    /// Whether it gives the same result for the same arguments.
    pub deterministic: bool,
}

/// What the compiler runs to build the schema: the SQL texts in the order to
/// run them, and what the SQL names that SQLite doesn't have. SQLite fails to
/// prepare a statement that names an unknown function or collation, so the
/// compiler registers a stand-in for each.
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
    pub(crate) fn probe(&self) -> sqlive::SqliteResult<sqlive::SchemaProbe> {
        let functions: Vec<sqlive::StandInFunction> = self
            .functions
            .iter()
            .map(|function| sqlive::StandInFunction {
                name: function.name.clone(),
                arguments: function.arguments,
                deterministic: function.deterministic,
            })
            .collect();
        sqlive::SchemaProbe::open(&self.sql, &functions, &self.collations)
    }
}

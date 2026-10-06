//! Numbered app migrations and their transaction-scoped SQL access.

use crate::sqlite::DatabaseConnection;
use crate::{DbError, MigrationError};
use rusqlite::{Params, Row};

/// Whether a writable open may migrate coven's internal schema.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CovenMigrationPolicy {
    /// Apply each pending internal migration in its own transaction.
    ApplyPending,
    /// Refuse an open that needs to migrate coven's tables.
    RefusePending,
}

/// How one migration changed SQLite's schema (§17.1).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MigrationChange {
    /// No synced definition changed and no table disappeared. Local-only and
    /// view changes must not be mistaken for a synced schema addition (§17.1).
    NoChange,
    /// Only synced tables or their columns were added.
    Addition,
    /// A table disappeared or an existing synced definition changed beyond an addition.
    Breaking,
}

/// The classification of a migration committed by this open.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MigrationOutcome {
    /// Its position in the app's migration sequence.
    pub version: u32,
    /// The app's name for this migration.
    pub name: &'static str,
    /// Its own before-and-after schema comparison, before subsequent migrations.
    pub change: MigrationChange,
}

type MigrationBody = dyn Fn(&MigrationContext<'_>) -> Result<(), DbError> + Send + Sync;

/// One numbered change to the app's database.
pub struct Migration {
    pub(crate) version: u32,
    pub(crate) name: &'static str,
    body: Box<MigrationBody>,
}

impl Migration {
    /// A migration expressed as SQL.
    pub fn sql(version: u32, name: &'static str, sql: &'static str) -> Self {
        Self::run(version, name, move |context| {
            context.execute_batch(sql)?;
            Ok(())
        })
    }

    /// A migration expressed as code, including backfills and table rebuilds.
    pub fn run<F>(version: u32, name: &'static str, f: F) -> Self
    where
        F: Fn(&MigrationContext<'_>) -> Result<(), DbError> + Send + Sync + 'static,
    {
        Self {
            version,
            name,
            body: Box::new(f),
        }
    }

    pub(crate) fn apply(&self, context: &MigrationContext<'_>) -> Result<(), DbError> {
        (self.body)(context)
    }
}

pub(crate) fn validate_versions(migrations: &[Migration]) -> Result<u32, MigrationError> {
    let mut supported = 0u32;
    for (position, migration) in migrations.iter().enumerate() {
        let expected = supported
            .checked_add(1)
            .expect("migration list fits in u32");
        if migration.version != expected {
            return Err(MigrationError::NotContiguous {
                position,
                found: migration.version,
                expected,
            });
        }
        supported = expected;
    }
    Ok(supported)
}

/// App SQL inside the transaction owned by the migration run.
/// The context borrows its owner and cannot change the transaction or connection.
pub struct MigrationContext<'connection> {
    database: &'connection DatabaseConnection,
}

impl<'connection> MigrationContext<'connection> {
    pub(crate) fn new(database: &'connection DatabaseConnection) -> Self {
        Self { database }
    }

    /// Execute one app statement with parameters.
    pub fn execute<P: Params>(&self, sql: &str, params: P) -> rusqlite::Result<usize> {
        self.database.app_execute(sql, params)
    }

    /// Execute app statements without escaping the migration transaction.
    pub fn execute_batch(&self, sql: &str) -> rusqlite::Result<()> {
        self.database.app_batch(sql)
    }

    /// Map one row from an app query.
    pub fn query_row<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<T>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> rusqlite::Result<T>,
    {
        self.database.app_query_row(sql, params, map)
    }

    /// Map every result of an app query.
    pub fn query<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> rusqlite::Result<T>,
    {
        self.database.app_query(sql, params, map)
    }
}

#[cfg(test)]
#[path = "migration_tests.rs"]
mod tests;

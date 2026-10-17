//! Numbered app migrations and their transaction-scoped SQL access.

use crate::migration_names::MigrationNames;
use crate::sqlite::DatabaseConnection;
use crate::{DbError, MigrationError};
use rusqlite::{Params, Row};
use std::cell::RefCell;
use std::sync::Arc;

/// How one migration changed the schema and synced rows (§18.1).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MigrationChange {
    /// No synced definition or row changed and no table disappeared. Local-only and
    /// view changes must not be mistaken for a synced schema addition (§18.1).
    NoChange,
    /// Only synced tables or their columns were added.
    Addition,
    /// A table disappeared, a synced definition changed beyond an addition,
    /// or statements changed rows of synced tables.
    Breaking,
}

/// The classification of a migration committed by this open.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MigrationOutcome {
    /// Its position in the app's migration sequence.
    pub version: u32,
    /// The app's name for this migration.
    pub name: &'static str,
    /// Its schema comparison and row changes, before subsequent migrations.
    pub change: MigrationChange,
}

pub(crate) type MigrationOperation =
    dyn Fn(u32) -> Result<crate::NewOperation, DbError> + Send + Sync;

type WriteConversion = dyn Fn(&mut crate::RowChange) -> Result<(), DbError> + Send + Sync;

type MigrationBody = dyn Fn(&MigrationContext<'_>) -> Result<(), DbError> + Send + Sync;

/// What a breaking migration does with received late old-version writes.
#[derive(Clone)]
pub enum OldWrites {
    /// Convert eligible changes without rewriting their original upload records.
    Convert(Arc<WriteConversion>),
    /// Keep each original change as a frozen schema loss; never discard its values.
    Drop,
}

#[derive(Clone)]
pub(crate) enum MigrationKind {
    Addition,
    Breaking(OldWrites),
}

/// One numbered change to the app's database, with its declared kind.
/// Clones retain the same callbacks across bootstrap and database opening.
#[derive(Clone)]
pub struct Migration {
    pub(crate) version: u32,
    pub(crate) name: &'static str,
    body: Arc<MigrationBody>,
    pub(crate) kind: MigrationKind,
}

impl Migration {
    /// SQL that adds tables or columns, or changes only local objects.
    /// A runtime breaking classification refuses the whole migration batch.
    pub fn addition(version: u32, name: &'static str, sql: &'static str) -> Self {
        Self::addition_with(version, name, move |context| {
            context.execute_batch(sql)?;
            Ok(())
        })
    }

    /// Code that adds tables or columns, or changes only local objects.
    pub fn addition_with<F>(version: u32, name: &'static str, f: F) -> Self
    where
        F: Fn(&MigrationContext<'_>) -> Result<(), DbError> + Send + Sync + 'static,
    {
        Self {
            version,
            name,
            body: Arc::new(f),
            kind: MigrationKind::Addition,
        }
    }

    /// Breaking SQL with an explicit choice for late old-version writes.
    /// An addition or local-only classification refuses the whole batch.
    ///
    /// A breaking migration cannot omit the old-write choice:
    /// ```compile_fail
    /// use coven_database::Migration;
    /// Migration::breaking(2, "rename", "ALTER TABLE notes RENAME COLUMN title TO name");
    /// ```
    pub fn breaking(
        version: u32,
        name: &'static str,
        sql: &'static str,
        old_writes: OldWrites,
    ) -> Self {
        Self::breaking_with(
            version,
            name,
            move |context| {
                context.execute_batch(sql)?;
                Ok(())
            },
            old_writes,
        )
    }

    /// Breaking code, including backfills and rebuilds, with its old-write choice.
    ///
    /// ```compile_fail
    /// use coven_database::Migration;
    /// Migration::breaking_with(2, "backfill", |context| {
    ///     context.execute_batch("UPDATE notes SET title=upper(title)")?;
    ///     Ok(())
    /// });
    /// ```
    pub fn breaking_with<F>(version: u32, name: &'static str, f: F, old_writes: OldWrites) -> Self
    where
        F: Fn(&MigrationContext<'_>) -> Result<(), DbError> + Send + Sync + 'static,
    {
        Self {
            version,
            name,
            body: Arc::new(f),
            kind: MigrationKind::Breaking(old_writes),
        }
    }

    pub(crate) fn check_kind(&self, breaking: bool) -> Result<(), DbError> {
        if matches!(self.kind, MigrationKind::Breaking(_)) != breaking {
            return Err(DbError::MigrationKindMismatch {
                version: self.version,
            });
        }
        Ok(())
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
    names: RefCell<MigrationNames>,
    before: &'connection crate::schema::Schema,
    capture: &'connection RefCell<crate::migration_snapshot::MigrationCapture>,
}

impl<'connection> MigrationContext<'connection> {
    pub(crate) fn new(
        database: &'connection DatabaseConnection,
        before: &'connection crate::schema::Schema,
        capture: &'connection RefCell<crate::migration_snapshot::MigrationCapture>,
    ) -> Self {
        Self {
            database,
            names: RefCell::new(MigrationNames::new(before)),
            before,
            capture,
        }
    }

    pub(crate) fn finish(
        self,
        before: &crate::schema::Schema,
        after: &crate::schema::Schema,
    ) -> crate::migration_names::MigrationEffects {
        self.names.into_inner().finish(before, after)
    }

    fn statement<T>(
        &self,
        sql: &str,
        run: impl FnOnce() -> rusqlite::Result<T>,
    ) -> rusqlite::Result<T> {
        self.before_statement(sql)?;
        let before = self.database.schema_cookie()?;
        let result = run();
        if self.database.schema_cookie()? != before {
            self.record(sql)?;
        }
        result
    }

    pub(crate) fn before_statement(&self, sql: &str) -> rusqlite::Result<()> {
        self.capture
            .borrow_mut()
            .before(self.database, self.before, sql)
    }

    pub(crate) fn record(&self, sql: &str) -> rusqlite::Result<()> {
        self.names.borrow_mut().record(sql)
    }

    /// Execute one app statement with parameters.
    pub fn execute<P: Params>(&self, sql: &str, params: P) -> rusqlite::Result<usize> {
        self.statement(sql, || self.database.app_execute(sql, params))
    }

    /// Execute app statements without escaping the migration transaction.
    pub fn execute_batch(&self, sql: &str) -> rusqlite::Result<()> {
        self.database.app_batch_tracked(sql, Some(self))
    }

    /// Map one row from an app query.
    pub fn query_row<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<T>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> rusqlite::Result<T>,
    {
        self.statement(sql, || self.database.app_query_row(sql, params, map))
    }

    /// Map every result of an app query.
    pub fn query<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> rusqlite::Result<T>,
    {
        self.statement(sql, || self.database.app_query(sql, params, map))
    }
}

#[cfg(test)]
#[path = "migration_tests.rs"]
pub(crate) mod tests;

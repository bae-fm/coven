//! The leaf SQLite capability. Its connection is never returned or borrowed out.

use std::path::Path;

use rusqlite::{
    fallible_iterator::FallibleIterator, functions::FunctionFlags, Batch, Connection, OpenFlags,
    Params, Row,
};

use crate::authorization::SqlAuthorization;
use crate::internal_schema;
use crate::migration::{validate_versions, MigrationContext};
use crate::schema::Schema;
use crate::{
    CovenMigrationError, CovenMigrationPolicy, CovenResult, DbError, Migration, MigrationError,
    MigrationOutcome, SyncedTable,
};

pub(crate) struct DatabaseConnection {
    connection: Connection,
    authorization: SqlAuthorization,
}

impl DatabaseConnection {
    pub(crate) fn open(
        path: &Path,
        read_only: bool,
        authorization: SqlAuthorization,
    ) -> Result<Self, DbError> {
        let flags = if read_only {
            OpenFlags::SQLITE_OPEN_READ_ONLY
        } else {
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE
        };
        // URI interpretation is deliberately absent: this is a StoreDir file,
        // never a caller-selected SQLite URI or in-memory database.
        let connection = Connection::open_with_flags(path, flags | OpenFlags::SQLITE_OPEN_NO_MUTEX)
            .map_err(opening_error)?;
        connection.set_db_config(rusqlite::config::DbConfig::SQLITE_DBCONFIG_DQS_DML, false)?;
        connection.set_db_config(rusqlite::config::DbConfig::SQLITE_DBCONFIG_DQS_DDL, false)?;
        connection.create_scalar_function(
            "coven_applying",
            0,
            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_INNOCUOUS,
            |_| Ok(false),
        )?;
        connection.authorizer(Some(authorization.callback()))?;
        let db = Self {
            connection,
            authorization,
        };
        db.batch("PRAGMA foreign_keys = ON; PRAGMA recursive_triggers = ON; PRAGMA trusted_schema = OFF;")?;
        Ok(db)
    }

    pub(crate) fn check_integrity(&self) -> Result<(), DbError> {
        let check: Vec<String> = self
            .query("PRAGMA integrity_check", [], |row| row.get(0))
            .map_err(|error| match error {
                DbError::Sqlite(error) => opening_error(error),
                error => error,
            })?;
        if check != ["ok"] {
            return Err(DbError::DamagedDatabase);
        }
        Ok(())
    }

    pub(crate) fn enable_wal(&self) -> Result<(), DbError> {
        let mode: String = self.query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))?;
        if !mode.eq_ignore_ascii_case("wal") {
            return Err(DbError::WalUnavailable { mode });
        }
        self.batch("PRAGMA synchronous = FULL;")
    }

    pub(crate) fn prepare_schema(
        &self,
        tables: &[SyncedTable],
        migrations: &[Migration],
        policy: CovenMigrationPolicy,
        read_only: bool,
    ) -> CovenResult<Vec<MigrationOutcome>> {
        let supported = validate_versions(migrations)?;
        let internal: i32 = self.query_row("PRAGMA application_id", [], |r| r.get(0))?;
        let internal = internal as u32;
        if internal != internal_schema::VERSION {
            if read_only
                || policy == CovenMigrationPolicy::RefusePending
                || internal > internal_schema::VERSION
            {
                return Err(CovenMigrationError::Pending.into());
            }
            // Only the current greenfield schema is supported. An uninitialized
            // database is version zero; an unknown newer schema is never rewritten.
            self.transaction(|db| {
                db.batch(&internal_schema::initial_schema())?;
                db.batch(&format!(
                    "PRAGMA application_id = {}",
                    internal_schema::VERSION
                ))
            })
            .map_err(|source| CovenMigrationError::Failed {
                source: Box::new(source),
            })?;
        }
        let current = self.schema_version()?;
        if current > supported {
            return Err(MigrationError::SchemaTooNew { current, supported }.into());
        }
        if read_only || current == supported {
            Schema::read(self)?.validate(self, tables)?;
            return Ok(Vec::new());
        }
        let mut at = migrations
            .iter()
            .find(|m| m.version > current)
            .expect("pending migration");
        let result = self.transaction(|db| {
            let mut outcomes = Vec::new();
            let mut before = Schema::read(db)?;
            for migration in migrations.iter().filter(|m| m.version > current) {
                at = migration;
                migration.apply(&MigrationContext::new(db))?;
                db.require_transaction()?;
                let after = Schema::read(db)?;
                outcomes.push(MigrationOutcome {
                    version: migration.version,
                    name: migration.name,
                    change: before.change_to(&after),
                });
                before = after;
            }
            before.validate(db, tables)?;
            db.batch(&format!("PRAGMA user_version = {}", supported as i32))?;
            Ok(outcomes)
        });
        result.map_err(|source| {
            MigrationError::Failed {
                version: at.version,
                name: at.name,
                source: Box::new(source),
            }
            .into()
        })
    }

    pub(crate) fn schema_version(&self) -> Result<u32, DbError> {
        self.query_row("PRAGMA user_version", [], |r| {
            r.get::<_, i32>(0).map(|v| v as u32)
        })
    }

    pub(crate) fn batch(&self, sql: &str) -> Result<(), DbError> {
        let _scope = self.authorization.internal();
        self.connection.execute_batch(sql).map_err(Into::into)
    }

    pub(crate) fn query_row<T, P: Params>(
        &self,
        sql: &str,
        params: P,
        map: impl FnOnce(&Row<'_>) -> rusqlite::Result<T>,
    ) -> Result<T, DbError> {
        let _scope = self.authorization.internal();
        self.connection
            .query_row(sql, params, map)
            .map_err(Into::into)
    }

    pub(crate) fn query<T, P: Params>(
        &self,
        sql: &str,
        params: P,
        map: impl FnMut(&Row<'_>) -> rusqlite::Result<T>,
    ) -> Result<Vec<T>, DbError> {
        let _scope = self.authorization.internal();
        let mut statement = self.connection.prepare(sql)?;
        let rows = statement
            .query_map(params, map)?
            .collect::<rusqlite::Result<_>>()?;
        Ok(rows)
    }

    pub(crate) fn scan<P: Params>(
        &self,
        sql: &str,
        params: P,
        mut visit: impl FnMut(&Row<'_>) -> rusqlite::Result<()>,
    ) -> Result<(), DbError> {
        let _scope = self.authorization.internal();
        let mut statement = self.connection.prepare(sql)?;
        let mut rows = statement.query(params)?;
        while let Some(row) = rows.next()? {
            visit(row)?;
        }
        Ok(())
    }

    pub(crate) fn app_execute<P: Params>(&self, sql: &str, params: P) -> rusqlite::Result<usize> {
        self.authorization.check_sql(sql)?;
        self.begin_app_statement()?;
        self.authorization
            .app_result(self.connection.execute(sql, params))
    }

    pub(crate) fn app_batch(&self, sql: &str) -> rusqlite::Result<()> {
        self.authorization.check_sql(sql)?;
        let mut batch = Batch::new(&self.connection, sql);
        loop {
            self.begin_app_statement()?;
            let Some(mut statement) = self.authorization.app_result(batch.next())? else {
                return Ok(());
            };
            // Step every row so later execution errors cannot be discarded.
            let result = (|| {
                let mut rows = statement.raw_query();
                while rows.next()?.is_some() {}
                Ok(())
            })();
            self.authorization.app_result(result)?;
        }
    }

    pub(crate) fn app_query_row<T, P: Params>(
        &self,
        sql: &str,
        params: P,
        map: impl FnOnce(&Row<'_>) -> rusqlite::Result<T>,
    ) -> rusqlite::Result<T> {
        self.authorization.check_sql(sql)?;
        self.begin_app_statement()?;
        self.authorization
            .app_result(self.connection.query_row(sql, params, map))
    }

    pub(crate) fn app_query<T, P: Params>(
        &self,
        sql: &str,
        params: P,
        map: impl FnMut(&Row<'_>) -> rusqlite::Result<T>,
    ) -> rusqlite::Result<Vec<T>> {
        self.authorization.check_sql(sql)?;
        self.begin_app_statement()?;
        let result = (|| {
            let mut statement = self.connection.prepare(sql)?;
            let rows = statement.query_map(params, map)?.collect();
            rows
        })();
        self.authorization.app_result(result)
    }

    fn require_transaction(&self) -> Result<(), DbError> {
        if self.connection.is_autocommit() {
            Err(DbError::TransactionEnded)
        } else {
            Ok(())
        }
    }

    fn begin_app_statement(&self) -> rusqlite::Result<()> {
        self.require_transaction()
            .map_err(|error| rusqlite::Error::UserFunctionError(Box::new(error)))?;
        self.authorization.begin_app_call();
        Ok(())
    }

    pub(crate) fn transaction<T>(
        &self,
        run: impl FnOnce(&Self) -> Result<T, DbError>,
    ) -> Result<T, DbError> {
        self.batch("BEGIN IMMEDIATE")?;
        let mut guard = SqlTransaction {
            database: self,
            active: true,
        };
        let result = run(self).and_then(|result| {
            self.require_transaction()?;
            self.batch("COMMIT")?;
            Ok(result)
        });
        guard.active = false;
        match result {
            Ok(result) => Ok(result),
            Err(operation) if self.connection.is_autocommit() => Err(operation),
            Err(operation) => match self.batch("ROLLBACK") {
                Ok(()) => Err(operation),
                Err(DbError::Sqlite(rollback)) => Err(DbError::Rollback {
                    operation: Box::new(operation),
                    rollback,
                }),
                Err(error) => panic!("internal rollback returned a non-SQLite error: {error}"),
            },
        }
    }

    pub(crate) fn close(self) -> Result<(), DbError> {
        self.connection
            .close()
            .map_err(|(_connection, error)| error.into())
    }
}

struct SqlTransaction<'a> {
    database: &'a DatabaseConnection,
    active: bool,
}

impl Drop for SqlTransaction<'_> {
    fn drop(&mut self) {
        if self.active && !self.database.connection.is_autocommit() {
            self.database
                .batch("ROLLBACK")
                .expect("rollback panicking migration");
        }
    }
}

fn opening_error(error: rusqlite::Error) -> DbError {
    match &error {
        rusqlite::Error::SqliteFailure(code, _)
            if matches!(
                code.code,
                rusqlite::ErrorCode::DatabaseCorrupt | rusqlite::ErrorCode::NotADatabase
            ) =>
        {
            DbError::DamagedDatabase
        }
        _ => error.into(),
    }
}

#[cfg(test)]
#[path = "sqlite_tests.rs"]
mod tests;

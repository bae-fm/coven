//! Opening is the composition root; a shared handle owns all connections and
//! the writer lock. Blocking calls keep that owner alive until they finish.

use std::sync::{Arc, Condvar, Mutex, RwLock};

use coven_foundation::files::{StoreDir, StoreLock};

use crate::authorization::SqlAuthorization;
use crate::sqlite::DatabaseConnection;
use crate::{
    CovenError, CovenMigrationPolicy, CovenResult, DbError, Migration, MigrationOutcome,
    SyncedTable,
};

/// Choices needed to open the database part of a store.
pub struct DatabaseBuilder {
    directory: StoreDir,
    tables: Option<Vec<SyncedTable>>,
    migrations: Option<Vec<Migration>>,
    policy: Option<CovenMigrationPolicy>,
}

impl DatabaseBuilder {
    /// Begin an open using a directory supplied by the store layout.
    pub fn new(directory: StoreDir) -> Self {
        Self {
            directory,
            tables: None,
            migrations: None,
            policy: None,
        }
    }

    /// The tables that sync, described at the newest app schema version.
    pub fn synced_tables(mut self, tables: Vec<SyncedTable>) -> Self {
        self.tables = Some(tables);
        self
    }

    /// The complete app migration sequence, numbered from one.
    pub fn migrations(mut self, migrations: Vec<Migration>) -> Self {
        self.migrations = Some(migrations);
        self
    }

    /// Required for a writable open; read-only opens always refuse migration.
    pub fn coven_migration_policy(mut self, policy: CovenMigrationPolicy) -> Self {
        self.policy = Some(policy);
        self
    }

    /// Open one writer under the store lock and four read-only connections.
    pub async fn open(self) -> CovenResult<Database> {
        finish_blocking(tokio::task::spawn_blocking(move || self.open_graph(false)).await)
    }

    /// Open read-only connections without locking or migrating, including from
    /// a second process while the store's writer is open.
    pub async fn open_read_only(self) -> CovenResult<Database> {
        finish_blocking(tokio::task::spawn_blocking(move || self.open_graph(true)).await)
    }

    fn open_graph(self, read_only: bool) -> CovenResult<Database> {
        let tables = self.tables.ok_or(CovenError::MissingConfiguration {
            field: "synced_tables",
        })?;
        let migrations = self.migrations.ok_or(CovenError::MissingConfiguration {
            field: "migrations",
        })?;
        let policy = if read_only {
            CovenMigrationPolicy::RefusePending
        } else {
            self.policy.ok_or(CovenError::MissingConfiguration {
                field: "coven_migration_policy",
            })?
        };
        // Refuse a directory that is not a store before creating its database.
        self.directory.settings()?;
        let lock = if read_only {
            None
        } else {
            Some(self.directory.lock_exclusive()?)
        };
        let path = self.directory.database_path();
        let first = DatabaseConnection::open(&path, read_only, SqlAuthorization::new(&tables))?;
        first.check_integrity()?;
        if !read_only {
            first.enable_wal()?;
        }
        let migrations = first.prepare_schema(&tables, &migrations, policy, read_only)?;
        let mut readers = Vec::new();
        let writer = if read_only {
            readers.push(Mutex::new(first));
            None
        } else {
            Some(Mutex::new(first))
        };
        while readers.len() < 4 {
            readers.push(Mutex::new(DatabaseConnection::open(
                &path,
                true,
                SqlAuthorization::new(&tables),
            )?));
        }
        Ok(Database {
            inner: Arc::new(RwLock::new(Some(DatabaseInner {
                writer,
                idle_readers: Mutex::new((0..readers.len()).collect()),
                reader_ready: Condvar::new(),
                readers,
                migrations,
                lock,
            }))),
        })
    }
}

/// Shared ownership of the database. Closing any clone closes them all.
#[derive(Clone)]
pub struct Database {
    inner: Arc<RwLock<Option<DatabaseInner>>>,
}

struct DatabaseInner {
    // Drop every SQLite connection before releasing the writer lock.
    writer: Option<Mutex<DatabaseConnection>>,
    readers: Vec<Mutex<DatabaseConnection>>,
    idle_readers: Mutex<Vec<usize>>,
    reader_ready: Condvar,
    migrations: Vec<MigrationOutcome>,
    lock: Option<StoreLock>,
}

impl Database {
    /// The committed app schema version, read on a read-only connection.
    pub async fn schema_version(&self) -> Result<u32, DbError> {
        let database = self.clone();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                let inner = database.inner.read().expect("database lock poisoned");
                let inner = inner.as_ref().ok_or(DbError::StoreClosed)?;
                let reader = inner.acquire_reader();
                reader.schema_version()
            })
            .await,
        )
    }

    /// The migrations this open committed, classified individually.
    pub fn applied_migrations(&self) -> Result<Vec<MigrationOutcome>, DbError> {
        let inner = self.inner.read().expect("database lock poisoned");
        Ok(inner
            .as_ref()
            .ok_or(DbError::StoreClosed)?
            .migrations
            .clone())
    }

    /// Wait for database calls, close all connections, then release the writer lock.
    /// Every subsequent database call, including close, reports `StoreClosed`.
    pub async fn close(&self) -> Result<(), DbError> {
        let database = self.clone();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                let mut slot = database.inner.write().expect("database lock poisoned");
                let Some(inner) = slot.take() else {
                    return Err(DbError::StoreClosed);
                };
                let DatabaseInner {
                    writer,
                    readers,
                    lock,
                    ..
                } = inner;
                let mut failures = Vec::new();
                for reader in readers {
                    if let Err(error) = reader
                        .into_inner()
                        .expect("read connection lock poisoned")
                        .close()
                    {
                        failures.push(error);
                    }
                }
                if let Some(writer) = writer {
                    if let Err(error) = writer
                        .into_inner()
                        .expect("writer connection lock poisoned")
                        .close()
                    {
                        failures.push(error);
                    }
                }
                drop(lock);
                if failures.is_empty() {
                    Ok(())
                } else {
                    Err(DbError::Closing { failures })
                }
            })
            .await,
        )
    }
}

impl DatabaseInner {
    fn acquire_reader(&self) -> ReaderLease<'_> {
        let mut idle = self.idle_readers.lock().expect("reader pool poisoned");
        loop {
            if let Some(index) = idle.pop() {
                return ReaderLease {
                    database: self,
                    index,
                };
            }
            idle = self.reader_ready.wait(idle).expect("reader pool poisoned");
        }
    }
}

// A call reserves one available reader while borrowing the database owner.
// Dropping the reservation wakes a caller even when the call panics.
struct ReaderLease<'a> {
    database: &'a DatabaseInner,
    index: usize,
}

impl ReaderLease<'_> {
    fn schema_version(&self) -> Result<u32, DbError> {
        self.database.readers[self.index]
            .lock()
            .expect("read connection lock poisoned")
            .schema_version()
    }
}

impl Drop for ReaderLease<'_> {
    fn drop(&mut self) {
        self.database
            .idle_readers
            .lock()
            .expect("reader pool poisoned")
            .push(self.index);
        self.database.reader_ready.notify_one();
    }
}

fn finish_blocking<T>(result: Result<T, tokio::task::JoinError>) -> T {
    match result {
        Ok(result) => result,
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        Err(error) => panic!("blocking database task was cancelled: {error}"),
    }
}

#[cfg(test)]
#[path = "database_tests.rs"]
mod tests;

//! The host's synced-schema ladder, tracked in `PRAGMA user_version`.
//!
//! `user_version` is a SQLite header field, so it travels inside a snapshot's
//! byte-for-byte DB image (`VACUUM INTO`/`VACUUM` preserve it): a device that
//! bootstraps from a snapshot inherits the writer's applied version directly, and
//! that same number is the wire `schema_version` every changeset is stamped with.
//! Bumping the schema is therefore adding a migration — a device cannot stamp a
//! version it has not migrated to.
//!
//! This is the host's *synced* schema only. On a fresh database, the complete
//! host ladder, sync-routing validation, and Coven bookkeeping initialization
//! commit in one transaction. On an initialized database, pending host steps
//! commit only when their final routing contract exactly matches the pinned
//! contract. Coven's bookkeeping tables are not part of this ladder.

use rusqlite::Connection;

use crate::database::DbError;

/// One ordered step in the host's synced-schema ladder.
pub struct Migration {
    /// 1-based, contiguous across the registered set. A gap, duplicate, or a set
    /// that does not start at 1 is a startup error, not a silent skip.
    pub version: u32,
    /// Recorded in logs, e.g. "initial", "add_album_disc_count".
    pub name: &'static str,
    pub up: MigrationStep,
}

/// The boxed closure a [`MigrationStep::Run`] holds. `Fn` (not `FnOnce`) because
/// the engine runs migrations through a `&[Migration]`, and a step runs at most
/// once anyway. `Send + Sync` keeps `Migration` `Sync`, so `&[Migration]` is
/// `Send`: the bootstrap paths (`join`/`restore`) hold the slice across an
/// `.await`, and a host that spawns those futures on a multi-threaded runtime
/// needs them to stay `Send`.
type MigrationFn = Box<dyn Fn(&Connection) -> Result<(), DbError> + Send + Sync>;

/// How a migration applies its change to the synced schema.
pub enum MigrationStep {
    /// A DDL batch (`CREATE` / `ALTER` / `CREATE INDEX`), e.g. `include_str!` of a
    /// `.sql` file.
    Sql(&'static str),
    /// Table rebuilds and backfills that DDL alone cannot express. Invoked at most
    /// once (only when its version is above the on-disk one). All pending steps
    /// share the open transaction, so a failed step or routing validation rolls
    /// the full ladder back with `user_version`.
    Run(MigrationFn),
}

impl Migration {
    /// A migration whose `up` is a static DDL batch.
    pub fn sql(version: u32, name: &'static str, sql: &'static str) -> Self {
        Migration {
            version,
            name,
            up: MigrationStep::Sql(sql),
        }
    }

    /// A migration whose `up` runs a closure for rebuilds/backfills DDL cannot
    /// express.
    pub fn run<F>(version: u32, name: &'static str, f: F) -> Self
    where
        F: Fn(&Connection) -> Result<(), DbError> + Send + Sync + 'static,
    {
        Migration {
            version,
            name,
            up: MigrationStep::Run(Box::new(f)),
        }
    }
}

/// The top synced-schema version this binary supports: the count of registered
/// migrations. [`run_migrations`] validates the set is `1..=N` contiguous, so the
/// count is the highest version (and `0` for an empty ladder — no synced schema).
/// The snapshot bootstrap gate compares an incoming snapshot's version against
/// this before adopting the image, so it lives beside the ladder rather than being
/// re-derived at each bootstrap call site.
pub(crate) fn supported_version(migrations: &[Migration]) -> u32 {
    migrations.len() as u32
}

/// Why running the synced-schema ladder failed. Carried as its own arm of
/// [`crate::database::OpenError`] at the `Database::open` boundary — not
/// flattened into a [`DbError`] string — so the variants stay typed for the
/// engine's own tests, the snapshot bootstrap gate, and hosts matching
/// [`MigrationError::SchemaTooNew`] to prompt an app update.
#[derive(Debug, thiserror::Error)]
pub enum MigrationError {
    /// The registered set is not exactly `1..=N` ascending with no gaps or
    /// duplicates — a host wiring bug, surfaced at startup before any DDL runs.
    #[error(
        "migration at position {position} has version {found}, expected {expected}: \
         the registered set must be contiguous 1..=N, strictly ascending"
    )]
    NotContiguous {
        position: usize,
        found: u32,
        expected: u32,
    },
    /// The on-disk schema version is newer than this binary's top migration, so
    /// this binary cannot apply a changeset (or open a snapshot image) at that
    /// version. Covers opening a snapshot written by a newer device and an older
    /// binary reopening a db a newer binary already migrated.
    #[error(
        "on-disk schema version {current} is newer than this binary supports \
         ({supported}); update the app"
    )]
    SchemaTooNew { current: u32, supported: u32 },
    /// A migration's DDL or backfill failed. Its transaction rolled back, so
    /// `user_version` did not advance over the half-applied step.
    #[error("migration {version} ({name}) failed: {source}")]
    Failed {
        version: u32,
        name: &'static str,
        source: DbError,
    },
    /// Reading or writing `user_version`, or the `BEGIN`/`COMMIT` around a step,
    /// failed.
    #[error("migration ledger access failed: {0}")]
    Ledger(DbError),
}

/// Apply every pending migration on a transaction the caller already owns.
///
/// The database initializer uses this form so the whole pending ladder, its
/// `user_version` advances, final routing-contract validation, and fresh Coven
/// metadata either commit together or roll back together. This function never
/// begins or commits a transaction; returning an error requires its caller to
/// roll back the enclosing transaction.
pub(crate) fn run_migrations_in_transaction(
    conn: &Connection,
    migrations: &[Migration],
) -> Result<u32, MigrationError> {
    let current = validate_registered_migrations(conn, migrations)?;
    for migration in migrations
        .iter()
        .filter(|migration| migration.version > current)
    {
        if let Err(source) = apply_step(conn, &migration.up) {
            return Err(MigrationError::Failed {
                version: migration.version,
                name: migration.name,
                source,
            });
        }
        conn.pragma_update(None, "user_version", migration.version)
            .map_err(|error| MigrationError::Ledger(DbError::from(error)))?;
    }
    read_user_version(conn)
}

fn validate_registered_migrations(
    conn: &Connection,
    migrations: &[Migration],
) -> Result<u32, MigrationError> {
    // Validate the registered set before touching the db: versions must be exactly
    // 1, 2, …, N in order. This one check rejects a gap, a duplicate, a non-ascending
    // pair, and a set that does not start at 1, all at once.
    for (position, migration) in migrations.iter().enumerate() {
        let expected = position as u32 + 1;
        if migration.version != expected {
            return Err(MigrationError::NotContiguous {
                position,
                found: migration.version,
                expected,
            });
        }
    }
    let current = read_user_version(conn)?;
    let top = supported_version(migrations);
    if current > top {
        return Err(MigrationError::SchemaTooNew {
            current,
            supported: top,
        });
    }
    Ok(current)
}

/// Validate the ladder and check the on-disk schema is one this binary supports,
/// **without applying anything** — the read-only counterpart of
/// [`run_migrations_in_transaction`]
/// for [`Database::open_read_only`](crate::database::Database::open_read_only).
///
/// Runs the same contiguity validation and the same `SchemaTooNew` refusal as
/// [`run_migrations_in_transaction`], then returns the current
/// `PRAGMA user_version` unchanged. A reader cannot migrate (its connection is
/// read-only), so an on-disk version below this binary's top is left as-is: the
/// reader reads the schema the writer left, and a writer that opens the same db
/// migrates it forward.
pub(crate) fn ensure_schema_supported(
    conn: &Connection,
    migrations: &[Migration],
) -> Result<u32, MigrationError> {
    for (i, m) in migrations.iter().enumerate() {
        let expected = i as u32 + 1;
        if m.version != expected {
            return Err(MigrationError::NotContiguous {
                position: i,
                found: m.version,
                expected,
            });
        }
    }
    let top = supported_version(migrations);
    let current = read_user_version(conn)?;
    if current > top {
        return Err(MigrationError::SchemaTooNew {
            current,
            supported: top,
        });
    }
    Ok(current)
}

/// Read the db's applied synced-schema version from `PRAGMA user_version`.
fn read_user_version(conn: &Connection) -> Result<u32, MigrationError> {
    conn.pragma_query_value(None, "user_version", |r| r.get::<_, i64>(0))
        .map(|v| v as u32)
        .map_err(|e| MigrationError::Ledger(DbError::from(e)))
}

fn apply_step(conn: &Connection, step: &MigrationStep) -> Result<(), DbError> {
    match step {
        MigrationStep::Sql(sql) => conn.execute_batch(sql.as_ref()).map_err(DbError::from),
        MigrationStep::Run(f) => f(conn),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `Migration` must stay `Send + Sync` so `&[Migration]` is `Send`. The
    /// bootstrap paths (`join`/`restore`) hold the slice across an `.await`, and a
    /// host that spawns those futures on a multi-threaded runtime requires them to
    /// be `Send` — dropping `Sync` from the `Run` closure regresses that silently
    /// (coven builds fine; the host fails to compile). This fails to compile if it
    /// regresses.
    #[test]
    fn migration_types_are_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Migration>();
        assert_send_sync::<MigrationStep>();
    }

    fn user_version(conn: &Connection) -> u32 {
        read_user_version(conn).expect("read user_version")
    }

    fn table_exists(conn: &Connection, name: &str) -> bool {
        conn.query_row(
            "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
            [name],
            |r| r.get::<_, i64>(0),
        )
        .expect("query sqlite_master")
            > 0
    }

    fn run_migrations(conn: &Connection, migrations: &[Migration]) -> Result<u32, MigrationError> {
        let transaction = conn
            .unchecked_transaction()
            .expect("begin migration transaction");
        let version = run_migrations_in_transaction(&transaction, migrations)?;
        transaction
            .commit()
            .map_err(|error| MigrationError::Ledger(DbError::from(error)))?;
        Ok(version)
    }

    #[test]
    fn fresh_db_applies_every_migration_and_lands_at_top() {
        let conn = Connection::open_in_memory().expect("open");
        let migrations = vec![
            Migration::sql(1, "a", "CREATE TABLE a (id TEXT PRIMARY KEY)"),
            Migration::sql(2, "b", "CREATE TABLE b (id TEXT PRIMARY KEY)"),
            Migration::sql(3, "c", "CREATE TABLE c (id TEXT PRIMARY KEY)"),
        ];
        let version = run_migrations(&conn, &migrations).expect("run migrations");
        assert_eq!(version, 3);
        assert_eq!(user_version(&conn), 3);
        for t in ["a", "b", "c"] {
            assert!(table_exists(&conn, t), "table {t} should exist");
        }
    }

    #[test]
    fn reopen_with_same_list_is_a_noop() {
        let conn = Connection::open_in_memory().expect("open");
        let migrations = || {
            vec![
                Migration::sql(1, "a", "CREATE TABLE a (id TEXT PRIMARY KEY)"),
                Migration::sql(2, "b", "CREATE TABLE b (id TEXT PRIMARY KEY)"),
                Migration::sql(3, "c", "CREATE TABLE c (id TEXT PRIMARY KEY)"),
            ]
        };
        assert_eq!(run_migrations(&conn, &migrations()).expect("first"), 3);
        // The second run finds current == N: every DDL above is a `CREATE TABLE`
        // that would fail if re-run, so a no-op proves nothing re-executed.
        assert_eq!(run_migrations(&conn, &migrations()).expect("second"), 3);
        assert_eq!(user_version(&conn), 3);
    }

    #[test]
    fn run_backfill_mutates_rows_and_bumps_version_together() {
        let conn = Connection::open_in_memory().expect("open");
        let migrations = vec![
            Migration::sql(
                1,
                "create",
                "CREATE TABLE t (id TEXT PRIMARY KEY, n INTEGER NOT NULL)",
            ),
            Migration::run(2, "backfill", |conn| {
                conn.execute("INSERT INTO t (id, n) VALUES ('row', 1)", [])
                    .map_err(DbError::from)?;
                conn.execute("UPDATE t SET n = 42 WHERE id = 'row'", [])
                    .map_err(DbError::from)?;
                Ok(())
            }),
        ];
        let version = run_migrations(&conn, &migrations).expect("run migrations");
        assert_eq!(version, 2);
        let n: i64 = conn
            .query_row("SELECT n FROM t WHERE id = 'row'", [], |r| r.get(0))
            .expect("read backfilled row");
        assert_eq!(n, 42);
    }

    #[test]
    fn failing_run_rolls_back_the_pending_ladder() {
        let conn = Connection::open_in_memory().expect("open");
        // Migration 2 fails after creating a table. The caller-owned transaction
        // rolls back both pending steps, their DDL, and both version advances.
        let migrations = vec![
            Migration::sql(1, "create", "CREATE TABLE t (id TEXT PRIMARY KEY)"),
            Migration::run(2, "boom", |conn| {
                conn.execute("CREATE TABLE late (id TEXT PRIMARY KEY)", [])
                    .map_err(DbError::from)?;
                Err(DbError::Message("backfill failed".to_string()))
            }),
        ];
        let err = run_migrations(&conn, &migrations).expect_err("must fail");
        assert!(matches!(
            err,
            MigrationError::Failed {
                version: 2,
                name,
                ..
            } if name == "boom"
        ));
        assert_eq!(
            user_version(&conn),
            0,
            "version must not advance over a failed pending ladder"
        );
        assert!(
            !table_exists(&conn, "t"),
            "the earlier pending step must roll back with the ladder",
        );
        assert!(
            !table_exists(&conn, "late"),
            "the failed step's DDL must have rolled back",
        );
    }

    #[test]
    fn malformed_sets_are_rejected_before_any_ddl() {
        // Gap: [1, 3].
        let conn = Connection::open_in_memory().expect("open");
        let gap = vec![
            Migration::sql(1, "a", "CREATE TABLE a (id TEXT PRIMARY KEY)"),
            Migration::sql(3, "c", "CREATE TABLE c (id TEXT PRIMARY KEY)"),
        ];
        assert!(matches!(
            run_migrations(&conn, &gap),
            Err(MigrationError::NotContiguous {
                position: 1,
                found: 3,
                expected: 2
            })
        ));
        assert!(!table_exists(&conn, "a"), "no DDL runs on a malformed set");

        // Duplicate: [1, 1].
        let dup = vec![
            Migration::sql(1, "a", "CREATE TABLE a (id TEXT PRIMARY KEY)"),
            Migration::sql(1, "a2", "CREATE TABLE a2 (id TEXT PRIMARY KEY)"),
        ];
        assert!(matches!(
            run_migrations(&conn, &dup),
            Err(MigrationError::NotContiguous {
                position: 1,
                found: 1,
                expected: 2
            })
        ));

        // Not from 1: [2].
        let not_from_one = vec![Migration::sql(
            2,
            "b",
            "CREATE TABLE b (id TEXT PRIMARY KEY)",
        )];
        assert!(matches!(
            run_migrations(&conn, &not_from_one),
            Err(MigrationError::NotContiguous {
                position: 0,
                found: 2,
                expected: 1
            })
        ));
    }

    #[test]
    fn ensure_schema_supported_checks_without_migrating() {
        let migrations = vec![
            Migration::sql(1, "a", "CREATE TABLE a (id TEXT PRIMARY KEY)"),
            Migration::sql(2, "b", "CREATE TABLE b (id TEXT PRIMARY KEY)"),
        ];

        // A db at version 1 (an older schema than this 2-step binary) is supported and
        // read as-is: no migration runs, no table is created, the version is unchanged.
        let conn = Connection::open_in_memory().expect("open");
        conn.pragma_update(None, "user_version", 1u32)
            .expect("set user_version");
        assert_eq!(
            ensure_schema_supported(&conn, &migrations).expect("v1 is supported"),
            1
        );
        assert!(
            !table_exists(&conn, "b"),
            "ensure_schema_supported must not apply any migration",
        );
        assert_eq!(user_version(&conn), 1, "the version is left untouched");

        // A db a newer binary migrated past this one is refused with the matchable
        // variant — same policy as run_migrations, so a reader can prompt "update the app".
        let ahead = Connection::open_in_memory().expect("open");
        ahead
            .pragma_update(None, "user_version", 5u32)
            .expect("set user_version");
        assert!(matches!(
            ensure_schema_supported(&ahead, &migrations),
            Err(MigrationError::SchemaTooNew {
                current: 5,
                supported: 2
            })
        ));
    }

    #[test]
    fn schema_newer_than_binary_is_refused() {
        let conn = Connection::open_in_memory().expect("open");
        // A db migrated to version 5 by a newer binary, reopened with a 3-migration
        // list: the engine refuses rather than apply a schema it does not know.
        conn.pragma_update(None, "user_version", 5u32)
            .expect("set user_version");
        let migrations = vec![
            Migration::sql(1, "a", "CREATE TABLE a (id TEXT PRIMARY KEY)"),
            Migration::sql(2, "b", "CREATE TABLE b (id TEXT PRIMARY KEY)"),
            Migration::sql(3, "c", "CREATE TABLE c (id TEXT PRIMARY KEY)"),
        ];
        assert!(matches!(
            run_migrations(&conn, &migrations),
            Err(MigrationError::SchemaTooNew {
                current: 5,
                supported: 3
            })
        ));
    }
}

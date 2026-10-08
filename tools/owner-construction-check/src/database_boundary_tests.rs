use super::*;
use crate::policy::Capability;

const POLICY: Policy = Policy {
    capabilities: crate::policy::Capabilities {
        sqlite: Capability {
            name: "SQLite",
            homes: &["crates/coven-database/src/"],
            gates: &[],
        },
        ..Policy::EMPTY.capabilities
    },
    database_schema: Some((
        "crates/coven-database/src/schema.rs",
        &["coven_tables", "coven_audience_tables"],
    )),
    ..Policy::EMPTY
};

fn kinds(path: &str, source: &str) -> BTreeSet<String> {
    find_database_boundary_violations(&[RustFile::fixture(path, source)], &POLICY)
        .into_iter()
        .map(|violation| {
            violation
                .message
                .trim_end_matches(" is confined to coven-database")
                .to_string()
        })
        .collect()
}

#[test]
fn sqlite_connection_is_rejected_outside_the_database_crate() {
    assert_eq!(
        kinds(
            "crates/coven-sync/src/leak.rs",
            r#"
            use rusqlite::Connection;
            fn leak(conn: &Connection) { conn.execute("DELETE FROM notes", []).unwrap(); }
            "#,
        ),
        BTreeSet::from(["raw SQLite connection".to_string()]),
    );
}

/// The API re-exports the database crate's `rusqlite` so a host can name the
/// types its own SQL uses. That path names the database crate, not SQLite.
#[test]
fn the_apis_reexport_of_the_database_crates_rusqlite_is_allowed() {
    assert!(kinds(
        "crates/coven/src/lib.rs",
        "pub use coven_database::rusqlite;"
    )
    .is_empty());
}

#[test]
fn every_raw_sqlite_ownership_path_is_rejected() {
    assert_eq!(
        kinds(
            "crates/coven-sync/src/leak.rs",
            r#"
            use rusqlite::{Connection as SqlConnection, Transaction};
            use rusqlite::session::Session;
            use rusqlite::*;
            use rusqlite as sqlite;
            fn qualified() -> rusqlite::Result<()> {
                let _ = rusqlite::Connection::open_in_memory()?;
                Ok(())
            }
            "#,
        ),
        BTreeSet::from(
            [
                "raw SQLite connection",
                "raw SQLite crate import",
                "raw SQLite session",
                "raw SQLite transaction",
                "raw SQLite wildcard import",
            ]
            .map(str::to_string)
        ),
    );
}

#[test]
fn the_database_crate_owns_raw_sqlite() {
    assert!(kinds(
        "crates/coven-database/src/transaction.rs",
        r#"
        use rusqlite::{Connection, Transaction};
        fn transact(connection: &Connection, transaction: &Transaction<'_>) {}
        "#,
    )
    .is_empty());
}

#[test]
fn host_sql_and_query_values_are_allowed_outside_the_database_crate() {
    assert!(kinds(
        "crates/coven/src/rows.rs",
        r#"
        fn read(context: SqlRead<'_>) -> rusqlite::Result<String> {
            context.query_row(
                "SELECT body FROM notes WHERE id = ?1",
                rusqlite::params!["note-id"],
                |row: &rusqlite::Row<'_>| row.get(0),
            )
        }
        "#,
    )
    .is_empty());
}

#[test]
fn coven_owned_sql_is_rejected_outside_the_database_crate() {
    let files = vec![
        RustFile::fixture(
            "crates/coven-database/src/schema.rs",
            r#"
            macro_rules! coven_tables {
                ($visit:ident) => {
                    $visit!(_coven_writes, "key TEXT PRIMARY KEY");
                };
            }
            macro_rules! coven_audience_tables {
                ($visit:ident) => {
                    $visit!(_coven_row_audiences, "table_name TEXT NOT NULL");
                };
            }
            "#,
        ),
        RustFile::fixture(
            "crates/coven-sync/src/leak.rs",
            r#"
            fn leak(database: DatabaseTestSql<'_>) {
                database.execute("DELETE FROM _coven_writes", []).unwrap();
                database.query("SELECT * FROM _coven_row_audiences", [], |_| Ok(())).unwrap();
            }
            "#,
        ),
    ];

    let kinds = find_database_boundary_violations(&files, &POLICY)
        .into_iter()
        .map(|violation| {
            violation
                .message
                .trim_end_matches(" is confined to coven-database")
                .to_string()
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        kinds,
        BTreeSet::from([
            "coven-owned SQL for table _coven_row_audiences".to_string(),
            "coven-owned SQL for table _coven_writes".to_string(),
        ]),
    );
}

#[test]
fn raw_handles_and_coven_sql_inside_macro_calls_are_rejected() {
    let files = vec![
        RustFile::fixture(
            "crates/coven-database/src/schema.rs",
            r#"
            macro_rules! coven_tables {
                ($visit:ident) => {
                    $visit!(_coven_writes, "key TEXT PRIMARY KEY");
                };
            }
            "#,
        ),
        RustFile::fixture(
            "crates/coven-sync/src/leak.rs",
            r#"
            fn leak(sql: SqlWrite<'_>, id: &str) {
                sql.execute(&format!("DELETE FROM _coven_writes WHERE id = '{id}'"), []).unwrap();
                let connections = vec![rusqlite::Connection::open_in_memory()];
            }
            macro_rules! wipe {
                ($sql:expr) => { $sql.execute("DELETE FROM _coven_writes", []) };
            }
            "#,
        ),
    ];

    let violations = find_database_boundary_violations(&files, &POLICY)
        .into_iter()
        .map(|violation| {
            (
                violation.line,
                violation
                    .message
                    .trim_end_matches(" is confined to coven-database")
                    .to_string(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        violations,
        vec![
            (3, "coven-owned SQL for table _coven_writes".to_string()),
            (4, "raw SQLite connection".to_string()),
            (7, "coven-owned SQL for table _coven_writes".to_string()),
        ],
    );
}

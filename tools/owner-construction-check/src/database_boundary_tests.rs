use super::*;
use crate::crate_code::checked;

fn policy() -> Policy {
    Policy::fixture(
        r#"
        [database]
        capability = "SQLite"
        schema = "crates/app-database/src/schema.rs"
        table_macros = ["app_tables", "app_audience_tables"]
        rusqlite_reexport = "app-database"

        [[capabilities]]
        name = "SQLite"
        homes = ["crates/app-database/src/"]
        "#,
    )
}

fn kinds(path: &str, source: &str) -> BTreeSet<String> {
    {
        let files = [RustFile::fixture(path, source)];
        find_database_boundary_violations(&checked(&files), &files, &policy())
    }
    .into_iter()
    .map(|violation| {
        violation.message.trim_end_matches(" is confined to crates/app-database/src/").to_string()
    })
    .collect()
}

#[test]
fn sqlite_connection_is_rejected_outside_the_database_crate() {
    assert_eq!(
        kinds(
            "crates/app-sync/src/leak.rs",
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
    assert!(kinds("crates/app/src/lib.rs", "pub use app_database::rusqlite;").is_empty());
}

#[test]
fn every_raw_sqlite_ownership_path_is_rejected() {
    assert_eq!(
        kinds(
            "crates/app-sync/src/leak.rs",
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
        "crates/app-database/src/transaction.rs",
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
        "crates/app/src/rows.rs",
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
fn sql_on_the_databases_own_tables_is_rejected_outside_its_homes() {
    let files = vec![
        RustFile::fixture(
            "crates/app-database/src/schema.rs",
            r#"
            macro_rules! app_tables {
                ($visit:ident) => {
                    $visit!(_app_writes, "key TEXT PRIMARY KEY");
                };
            }
            macro_rules! app_audience_tables {
                ($visit:ident) => {
                    $visit!(_app_row_audiences, "table_name TEXT NOT NULL");
                };
            }
            "#,
        ),
        RustFile::fixture(
            "crates/app-sync/src/leak.rs",
            r#"
            fn leak(database: DatabaseTestSql<'_>) {
                database.execute("DELETE FROM _app_writes", []).unwrap();
                database.query("SELECT * FROM _app_row_audiences", [], |_| Ok(())).unwrap();
            }
            "#,
        ),
    ];

    let kinds = find_database_boundary_violations(&checked(&files), &files, &policy())
        .into_iter()
        .map(|violation| {
            violation
                .message
                .trim_end_matches(" is confined to crates/app-database/src/")
                .to_string()
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        kinds,
        BTreeSet::from([
            "SQL on the database's own table _app_row_audiences".to_string(),
            "SQL on the database's own table _app_writes".to_string(),
        ]),
    );
}

/// The schema module includes its `#[path]` submodules; a table macro of the
/// same name elsewhere in the crate declares no table of the database.
#[test]
fn table_macros_are_read_from_every_file_of_the_schema_module() {
    let files = vec![
        RustFile::fixture(
            "crates/app-database/src/schema.rs",
            r#"
            macro_rules! app_tables {
                ($visit:ident) => {
                    $visit!(_app_writes, "key TEXT PRIMARY KEY");
                };
            }
            #[path = "schema_audience.rs"]
            mod audience;
            "#,
        ),
        RustFile::fixture(
            "crates/app-database/src/schema_audience.rs",
            r#"
            macro_rules! app_audience_tables {
                ($visit:ident) => {
                    $visit!(_app_row_audiences, "table_name TEXT NOT NULL");
                };
            }
            "#,
        ),
        RustFile::fixture(
            "crates/app-database/src/elsewhere.rs",
            r#"
            macro_rules! app_audience_tables {
                ($visit:ident) => {
                    $visit!(notes, "body TEXT");
                };
            }
            "#,
        ),
        RustFile::fixture(
            "crates/app-sync/src/leak.rs",
            r#"
            fn leak(database: DatabaseTestSql<'_>) {
                database.query("SELECT * FROM _app_row_audiences", [], |_| Ok(())).unwrap();
                database.execute("DELETE FROM notes", []).unwrap();
            }
            "#,
        ),
    ];

    let violations = find_database_boundary_violations(&checked(&files), &files, &policy())
        .into_iter()
        .map(|violation| (violation.path, violation.line, violation.message))
        .collect::<Vec<_>>();
    assert_eq!(
        violations,
        vec![(
            "crates/app-sync/src/leak.rs".to_string(),
            3,
            "SQL on the database's own table _app_row_audiences is confined to crates/app-database/src/"
                .to_string(),
        )],
    );
}

#[test]
fn raw_handles_and_own_table_sql_inside_macro_calls_are_rejected() {
    let files = vec![
        RustFile::fixture(
            "crates/app-database/src/schema.rs",
            r#"
            macro_rules! app_tables {
                ($visit:ident) => {
                    $visit!(_app_writes, "key TEXT PRIMARY KEY");
                };
            }
            "#,
        ),
        RustFile::fixture(
            "crates/app-sync/src/leak.rs",
            r#"
            fn leak(sql: SqlWrite<'_>, id: &str) {
                sql.execute(&format!("DELETE FROM _app_writes WHERE id = '{id}'"), []).unwrap();
                let connections = vec![rusqlite::Connection::open_in_memory()];
            }
            macro_rules! wipe {
                ($sql:expr) => { $sql.execute("DELETE FROM _app_writes", []) };
            }
            fn clear(sql: SqlWrite<'_>) { wipe!(sql); }
            "#,
        ),
    ];

    let violations = find_database_boundary_violations(&checked(&files), &files, &policy())
        .into_iter()
        .map(|violation| {
            (
                violation.line,
                violation
                    .message
                    .trim_end_matches(" is confined to crates/app-database/src/")
                    .to_string(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        violations,
        vec![
            (3, "SQL on the database's own table _app_writes".to_string()),
            (4, "raw SQLite connection".to_string()),
            (9, "SQL on the database's own table _app_writes".to_string()),
        ],
    );
}

fn domain_policy() -> Policy {
    Policy::fixture(
        r#"
        [database]
        capability = "SQLite"

        [[database.domains]]
        name = "listing"
        homes = ["crates/app-core/src/listing/", "crates/app-core/src/listing.rs"]
        tables = ["Note_Listing", "note_search"]

        [[database.domains]]
        name = "records"
        homes = ["crates/app-database/src/"]
        tables = ["notes"]

        [[capabilities]]
        name = "SQLite"
        homes = ["crates/app-database/src/"]
        "#,
    )
}

/// A domain's tables are written only in its homes, the database's
/// included; any file may read them.
#[test]
fn a_table_is_written_only_in_its_domain_s_homes() {
    let files = vec![
        RustFile::fixture(
            "crates/app-core/src/listing/refresh.rs",
            r#"fn refresh(sql: Sql) { sql.run("INSERT INTO note_listing SELECT id FROM notes"); }"#,
        ),
        RustFile::fixture(
            "crates/app-database/src/triggers.rs",
            r#"
            const LISTED: &str = "CREATE TRIGGER listed AFTER INSERT ON notes
                BEGIN INSERT INTO note_listing (id) VALUES (new.id); END";
            fn read(sql: Sql) { sql.run("SELECT * FROM note_search"); }
            "#,
        ),
        RustFile::fixture(
            "crates/app-sync/src/apply.rs",
            r#"
            fn apply(sql: Sql) {
                sql.run("UPDATE OR IGNORE notes SET body = ?1");
                // Built while the program runs: no literal names the table.
                sql.run(&format!("DELETE FROM {}", "note_search"));
                sql.run("SELECT body FROM notes JOIN note_listing USING (id)");
            }
            "#,
        ),
    ];
    let violations = find_database_boundary_violations(&checked(&files), &files, &domain_policy())
        .into_iter()
        .map(|violation| (violation.path, violation.line, violation.message))
        .collect::<Vec<_>>();
    assert_eq!(
        violations,
        [
            (
                "crates/app-database/src/triggers.rs".to_string(),
                2,
                "SQL writes note_listing, which only the listing domain writes, in crates/app-core/src/listing/, crates/app-core/src/listing.rs".to_string()
            ),
            (
                "crates/app-sync/src/apply.rs".to_string(),
                3,
                "SQL writes notes, which only the records domain writes, in crates/app-database/src/".to_string()
            ),
        ]
    );
}

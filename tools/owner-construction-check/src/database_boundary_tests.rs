use super::*;
use crate::crate_code::CrateCode;
use crate::sources::SqlFile;

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
    find_database_boundary_violations(
        &CrateCode::of(&[RustFile::fixture(path, source)], &[]),
        &policy(),
    )
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

    let kinds = find_database_boundary_violations(&CrateCode::of(&files, &[]), &policy())
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

    let violations = find_database_boundary_violations(&CrateCode::of(&files, &[]), &policy())
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

    let violations = find_database_boundary_violations(&CrateCode::of(&files, &[]), &policy())
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
        owner = "Listing"

        [[database.domains]]
        name = "records"
        homes = ["crates/app-database/src/"]
        tables = ["notes"]
        owner = "Database"

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
    let violations =
        find_database_boundary_violations(&CrateCode::of(&files, &[]), &domain_policy())
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

fn sql(path: &str, text: &str) -> SqlFile {
    let package = path.split('/').nth(1).expect("fixture path names a member");
    SqlFile {
        relative_path: path.into(),
        package_root: format!("crates/{package}/"),
        package: package.into(),
        text: text.into(),
    }
}

fn domain_violations(files: &[RustFile], sql: &[SqlFile]) -> Vec<(String, usize, String)> {
    find_database_boundary_violations(&CrateCode::of(files, sql), &domain_policy())
        .into_iter()
        .map(|violation| (violation.path, violation.line, violation.message))
        .collect()
}

/// A SQL file's writes answer to the domains as a string literal's do, each
/// at its own line.
#[test]
fn sql_files_write_only_their_domains_tables() {
    let files = [
        sql(
            "crates/app-core/src/listing/refresh.sql",
            "INSERT INTO note_listing SELECT id FROM notes;\nDELETE FROM note_search;",
        ),
        sql(
            "crates/app-sync/src/apply.sql",
            "SELECT 1;\n\nUPDATE notes\n   SET body = NULL;\nDELETE FROM note_search WHERE id IN (SELECT id FROM notes);",
        ),
    ];
    let violations = domain_violations(&[], &files);
    let message = |table: &str, domain: &str, homes: &str| {
        format!("SQL writes {table}, which only the {domain} domain writes, in {homes}")
    };
    assert_eq!(
        violations,
        [
            (
                "crates/app-sync/src/apply.sql".to_string(),
                3,
                message("notes", "records", "crates/app-database/src/")
            ),
            (
                "crates/app-sync/src/apply.sql".to_string(),
                5,
                message(
                    "note_search",
                    "listing",
                    "crates/app-core/src/listing/, crates/app-core/src/listing.rs"
                )
            ),
        ]
    );
}

/// A table's schema is read where its text is written: a SQL file, or a
/// string literal, a macro's definition included, whatever invokes it.
#[test]
fn a_tables_schema_lives_in_its_domains_homes() {
    let files = [
        RustFile::fixture(
            "crates/app-core/src/listing.rs",
            r#"
            macro_rules! listing_tables {
                () => { "CREATE TABLE note_listing (id TEXT); CREATE INDEX listed ON note_listing(id);" };
            }
            "#,
        ),
        RustFile::fixture(
            "crates/app-database/src/schema.rs",
            r#"
            fn schema() -> String {
                [listing_tables!(), "CREATE TABLE notes (id TEXT)"].concat()
            }
            const SEARCH: &str = "CREATE TABLE IF NOT EXISTS note_search (id TEXT)";
            const STAGED: &str = "CREATE TEMP TABLE note_listing (id TEXT)";
            "#,
        ),
    ];
    let schema = [sql(
        "crates/app-core/src/listing/schema.sql",
        "CREATE TABLE note_listing (id TEXT);
CREATE INDEX notes_body ON notes(body);
CREATE TRIGGER notes_checked BEFORE INSERT ON notes BEGIN SELECT RAISE(ABORT, 'no'); END;
CREATE TRIGGER notes_listed AFTER INSERT ON notes BEGIN INSERT INTO note_listing VALUES (new.id); END;",
    )];
    let homes = "crates/app-database/src/";
    assert_eq!(
        domain_violations(&files, &schema),
        [
            (
                "crates/app-core/src/listing/schema.sql".to_string(),
                2,
                format!("SQL indexes notes, whose schema the records domain keeps in {homes}")
            ),
            (
                "crates/app-core/src/listing/schema.sql".to_string(),
                3,
                format!("SQL creates a trigger, writing no domain's table, on notes, whose schema the records domain keeps in {homes}")
            ),
            (
                "crates/app-database/src/schema.rs".to_string(),
                5,
                "SQL creates table note_search, whose schema the listing domain keeps in crates/app-core/src/listing/, crates/app-core/src/listing.rs".to_string()
            ),
        ]
    );
}

/// A write is read where its text is written, as schema is: a macro's
/// definition writing a domain's table answers for it in its own file, and
/// the code that invokes the macro writes nothing itself. A table's seed row
/// in its domain's schema macro is the domain's own write wherever the macro
/// is expanded.
#[test]
fn a_write_in_a_macro_is_read_where_the_macro_is_written() {
    let files = [
        RustFile::fixture(
            "crates/app-core/src/listing.rs",
            r#"
            macro_rules! listing_tables {
                () => { "CREATE TABLE note_listing (id TEXT); INSERT INTO note_listing VALUES (0);" };
            }
            "#,
        ),
        RustFile::fixture(
            "crates/app-sync/src/apply.rs",
            r#"
            macro_rules! clear_search {
                () => { "DELETE FROM note_search" };
            }
            "#,
        ),
        RustFile::fixture(
            "crates/app-database/src/schema.rs",
            r#"
            fn schema() -> String {
                [listing_tables!(), clear_search!(), "CREATE TABLE notes (id TEXT)"].concat()
            }
            "#,
        ),
    ];
    assert_eq!(
        domain_violations(&files, &[]),
        [(
            "crates/app-sync/src/apply.rs".to_string(),
            3,
            "SQL writes note_search, which only the listing domain writes, in crates/app-core/src/listing/, crates/app-core/src/listing.rs".to_string()
        )]
    );
}

/// A loop over a literal list of table names writes each table its SQL names
/// through the loop's variable, as the statement it runs for each one.
#[test]
fn a_loop_over_table_names_writes_each_table() {
    let files = [RustFile::fixture(
        "crates/app-sync/src/apply.rs",
        r#"
        fn clear(sql: Sql) {
            for table in ["note_listing", "notes"] {
                sql.run(&format!("DELETE FROM {table} WHERE id=?1"));
            }
            for other in ["notes"] {
                sql.run(&format!("SELECT * FROM {other}"));
            }
        }
        "#,
    )];
    assert_eq!(
        domain_violations(&files, &[]),
        [
            (
                "crates/app-sync/src/apply.rs".to_string(),
                4,
                "SQL writes note_listing, which only the listing domain writes, in crates/app-core/src/listing/, crates/app-core/src/listing.rs".to_string()
            ),
            (
                "crates/app-sync/src/apply.rs".to_string(),
                4,
                "SQL writes notes, which only the records domain writes, in crates/app-database/src/".to_string()
            ),
        ]
    );
}

/// A domain's tests live in its homes: a home file's `<name>_tests.rs`, and
/// its directory's test files, may write its tables; test code elsewhere,
/// a test-only item of another file or a SQL file under `tests/`, may not.
#[test]
fn test_code_writing_a_domains_tables_lives_in_its_homes() {
    let files = [
        RustFile::fixture(
            "crates/app-core/src/listing_tests.rs",
            r#"fn fill(sql: Sql) { sql.run("INSERT INTO note_listing VALUES (1)"); }"#,
        ),
        RustFile::fixture(
            "crates/app-core/src/listing/tests/fill.rs",
            r#"fn fill(sql: Sql) { sql.run("INSERT INTO note_search VALUES (1)"); }"#,
        ),
        RustFile::fixture(
            "crates/app-sync/src/apply_tests.rs",
            r#"fn fill(sql: Sql) { sql.run("INSERT INTO note_listing VALUES (1)"); }"#,
        ),
        RustFile::fixture(
            "crates/app-sync/src/apply.rs",
            r#"
            fn apply(sql: Sql) { sql.run("SELECT * FROM note_listing"); }
            #[cfg(test)]
            fn fill(sql: Sql) { sql.run("CREATE TABLE note_search (id TEXT)"); }
            "#,
        ),
    ];
    let fixture = [sql("crates/app-core/tests/listing.sql", "DELETE FROM note_listing;")];
    let homes = "crates/app-core/src/listing/, crates/app-core/src/listing.rs";
    assert_eq!(
        domain_violations(&files, &fixture),
        [
            (
                "crates/app-core/tests/listing.sql".to_string(),
                1,
                format!("SQL writes note_listing, which only the listing domain writes, in {homes}")
            ),
            (
                "crates/app-sync/src/apply.rs".to_string(),
                4,
                format!("SQL creates table note_search, whose schema the listing domain keeps in {homes}")
            ),
            (
                "crates/app-sync/src/apply_tests.rs".to_string(),
                1,
                format!("SQL writes note_listing, which only the listing domain writes, in {homes}")
            ),
        ]
    );
}

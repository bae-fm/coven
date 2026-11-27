use std::sync::Mutex;

use rusqlite::trace::{TraceEvent, TraceEventCodes};
use rusqlite::Connection;
use sqlite3_parser::ast::Name;

use super::{MainSchema, SchemaLookups};
use crate::read_capture::WholeViews;
use crate::scope_plan::{ScopePlan, ScopePlanSize};

/// The statements the traced connection ran.
static RAN: Mutex<Vec<String>> = Mutex::new(Vec::new());

/// A schema of 60 tables, 3 contentful FTS5 tables and 20 views, each view
/// joining 4 tables, and a statement that joins every view.
fn schema_and_statement() -> (String, String) {
    let mut schema = String::new();
    for t in 0..60 {
        schema += &format!(
            "CREATE TABLE t{t}(id INTEGER PRIMARY KEY, parent INTEGER NOT NULL, name TEXT, \
             size INTEGER, twice INTEGER AS (size * 2)); CREATE UNIQUE INDEX u{t} ON t{t}(name);"
        );
    }
    for f in 0..3 {
        schema += &format!("CREATE VIRTUAL TABLE f{f} USING fts5(title, body);");
    }
    for v in 0..20 {
        let [a, b, c, d] = [0, 1, 2, 3].map(|n| (v * 3 + n) % 60);
        schema += &format!(
            "CREATE VIEW v{v} AS SELECT a.id AS id, b.name AS name, count(*) AS n FROM t{a} AS a \
             JOIN t{b} AS b ON b.parent = a.id JOIN t{c} AS c USING (id) \
             LEFT JOIN t{d} AS d ON d.parent = c.id GROUP BY a.id, b.name;"
        );
    }
    let mut sql =
        "SELECT v0.name, (SELECT title FROM f0 WHERE f0.rowid = v0.id) FROM v0".to_owned();
    for v in 1..20 {
        sql += &format!(" JOIN v{v} ON v{v}.id = v0.id");
    }
    sql += " WHERE v0.id = ?1 ORDER BY v0.name LIMIT 5";
    (schema, sql)
}

/// The statements the connection ran since the last call that read its
/// schema, each with its values.
fn schema_reads() -> Vec<String> {
    std::mem::take(&mut *RAN.lock().unwrap())
        .into_iter()
        .filter(|sql| {
            (sql.contains("pragma_") || sql.contains("sqlite_schema"))
                && !sql.contains("pragma_function_list")
        })
        .collect()
}

/// Reading the schema asks SQLite each question of it once, and building a
/// plan from the read asks none.
#[test]
fn building_a_plan_asks_sqlite_nothing_about_the_schema_it_read() {
    let (made, sql) = schema_and_statement();
    let mut connection = Connection::open_in_memory().unwrap();
    connection.execute_batch(&made).unwrap();
    connection
        .trace_v2(
            TraceEventCodes::SQLITE_TRACE_STMT,
            Some(|event: TraceEvent<'_>| {
                if let TraceEvent::Stmt(statement, _) = event {
                    let sql = statement.expanded_sql().expect("SQLite expands each statement");
                    RAN.lock().unwrap().push(sql);
                }
            }),
        )
        .unwrap();
    let statement = connection.prepare(&sql).unwrap();
    schema_reads();

    let schema = MainSchema::read(&connection).unwrap();
    let mut read = schema_reads();
    let ran = read.len();
    read.sort();
    read.dedup();
    assert_eq!(read.len(), ran, "a schema statement ran twice: {read:#?}");

    let plan =
        ScopePlan::build(&connection, &schema, &sql, &statement, &WholeViews::new([])).unwrap();
    assert_eq!(plan.size, ScopePlanSize { sources: 101, cores: 22, edges: 138 });
    assert_eq!(schema_reads(), Vec::<String>::new());
}

#[test]
fn a_name_is_found_however_a_statement_writes_it() {
    let connection = Connection::open_in_memory().unwrap();
    connection
        .execute_batch(
            "CREATE TABLE Plain(id INTEGER PRIMARY KEY);
             CREATE VIEW Seen AS SELECT id FROM Plain;
             CREATE VIRTUAL TABLE Found USING fts5(title);
             CREATE VIRTUAL TABLE Bare USING fts5(title, content='');",
        )
        .unwrap();
    let schema = MainSchema::read(&connection).unwrap();
    let schema = SchemaLookups::new(&schema);
    let object = |name| schema.object(&Name(name));
    assert_eq!(object("plain"), Some(("Plain", "table")));
    assert_eq!(object("\"SEEN\""), Some(("Seen", "view")));
    assert_eq!(object("[found]"), Some(("Found", "virtual")));
    assert_eq!(object("`Bare`"), None);
    assert_eq!(object("found_content"), None);
    assert_eq!(object("absent"), None);
}

#[test]
fn a_view_over_a_dropped_table_fails_only_a_lookup_of_its_columns() {
    let connection = Connection::open_in_memory().unwrap();
    connection
        .execute_batch(
            "CREATE TABLE gone(id INTEGER PRIMARY KEY);
             CREATE TABLE kept(id INTEGER PRIMARY KEY);
             CREATE VIEW dangling AS SELECT id FROM gone;
             DROP TABLE gone;",
        )
        .unwrap();
    let schema = MainSchema::read(&connection).unwrap();
    let schema = SchemaLookups::new(&schema);
    assert_eq!(schema.columns("kept").unwrap().len(), 1);
    let Err(error) = schema.columns("dangling") else { panic!("SQLite can't list its columns") };
    assert!(error.to_string().contains("no such table: main.gone"), "{error}");
}

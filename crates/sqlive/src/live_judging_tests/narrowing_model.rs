//! sqlive's narrowing against its Lean model (`crates/sqlive/proofs`, run
//! through its `narrowingRunner`) and SQLite. Each rule's generator writes
//! statements of its rule's shape, as SQL and as the runner's JSON tree. For
//! each statement the test opens a live query and makes random writes; after
//! each it fails when the model finds the statement not well formed, when
//! sqlive and the model disagree on whether the write reruns the query, given
//! whether one of sqlive's questions about the write stopped at its step
//! limit, when a write either lets pass changed the statement's rows, or when
//! the model's rows differ from SQLite's.
//! `SQLIVE_NARROWING_LEAN` names the runner; `SQLIVE_NARROWING_SEEDS` sets how
//! many statements each test runs (default 60). A failure names the seed.

use crate::random_scopes::Rng;
use crate::tests::{QueryError, TestDatabase, TestRead, HANG_GUARD};
use crate::{LiveQuery, SqlValue};
use serde_json::{json, Value as Json};
use std::io::Write as _;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[path = "narrowing_model/aggregate.rs"]
mod aggregate;
#[path = "narrowing_model/carry.rs"]
mod carry;
#[path = "narrowing_model/carry_derived.rs"]
mod carry_derived;
#[path = "narrowing_model/cte_names.rs"]
mod cte_names;
#[path = "narrowing_model/distinct.rs"]
mod distinct;
#[path = "narrowing_model/edge.rs"]
mod edge;
#[path = "narrowing_model/group_key.rs"]
mod group_key;
#[path = "narrowing_model/is_edge.rs"]
mod is_edge;
#[path = "narrowing_model/limit.rs"]
mod limit;
#[path = "narrowing_model/mixed.rs"]
mod mixed;
#[path = "narrowing_model/nested_derived.rs"]
mod nested_derived;
#[path = "narrowing_model/net.rs"]
mod net;
#[path = "narrowing_model/nonstrict_edge.rs"]
mod nonstrict_edge;
#[path = "narrowing_model/one_of.rs"]
mod one_of;
#[path = "narrowing_model/or.rs"]
mod or;
#[path = "narrowing_model/or_derived.rs"]
mod or_derived;
#[path = "narrowing_model/parenthesized.rs"]
mod parenthesized;
#[path = "narrowing_model/push_down.rs"]
mod push_down;
#[path = "narrowing_model/references.rs"]
mod references;
#[path = "narrowing_model/row_edge.rs"]
mod row_edge;
#[path = "narrowing_model/row_member.rs"]
mod row_member;
#[path = "narrowing_model/row_subquery_edge.rs"]
mod row_subquery_edge;
#[path = "narrowing_model/subquery_edge.rs"]
mod subquery_edge;
#[path = "narrowing_model/subquery_window.rs"]
mod subquery_window;
#[path = "narrowing_model/syntax.rs"]
mod syntax;
#[path = "narrowing_model/term.rs"]
mod term;
#[path = "narrowing_model/unique_order.rs"]
mod unique_order;
#[path = "narrowing_model/unread_column.rs"]
mod unread_column;
#[path = "narrowing_model/using.rs"]
mod using;
#[path = "narrowing_model/view.rs"]
mod view;
#[path = "narrowing_model/whole.rs"]
mod whole;
#[path = "narrowing_model/window.rs"]
mod window;

use syntax::*;

/// One of `names`, at random.
pub(super) fn pick(rng: &mut Rng, names: &[&'static str]) -> &'static str {
    names[rng.below(names.len())]
}

/// The tables the unique order rule's statements read beside the others': one
/// with the unique keys (a, c) and (b, a), of which `a` alone holds no NULL.
pub(super) const KEYED: &str = "k";
const KEYED_SQL: &str = "(id INTEGER PRIMARY KEY, a INTEGER NOT NULL, b, c INTEGER, \
     n INTEGER COLLATE NOCASE, s TEXT COLLATE fold, UNIQUE (a, c), UNIQUE (b, a))";
/// Every table: `TABLES`, then `KEYED`.
const ALL_TABLES: [&str; 5] = {
    let [t, u, w, search] = TABLES;
    [t, u, w, search, KEYED]
};
/// The tables every statement reads: an INTEGER key, numeric columns, an
/// untyped one (BLOB affinity) and one whose collation sqlive doesn't narrow
/// by; and `SEARCH`, a contentful FTS5 table of the same columns, each
/// untyped and BINARY, whose `id` the writes keep equal to its rowid. Every
/// value is an integer or NULL, which SQLite compares as the model does.
pub(super) const TABLES: [&str; 4] = ["t", "u", "w", SEARCH];
pub(super) const COLUMNS: [&str; 5] = ["id", "a", "b", "c", "n"];
/// The plain tables' columns: `COLUMNS`, then `s`.
const PLAIN_COLUMNS: [&str; 6] = {
    let [id, a, b, c, n] = COLUMNS;
    [id, a, b, c, n, "s"]
};
const TABLE_SQL: &str =
    "(id INTEGER PRIMARY KEY, a INTEGER, b, c INTEGER, n INTEGER COLLATE NOCASE, \
     s TEXT COLLATE fold)";

/// The columns of `table`: the plain tables also have `s`, a text column of
/// the digits `0` to `3`, whose `fold` collation ties `0` with `3`, as
/// NOCASE ties `'a'` with `'A'`. `SEARCH`'s columns have no collation.
pub(super) fn columns(table: &str) -> &'static [&'static str] {
    if table == SEARCH {
        &COLUMNS
    } else {
        &PLAIN_COLUMNS
    }
}

/// The order `fold` gives two strings of digits.
pub(super) fn fold(left: &str, right: &str) -> std::cmp::Ordering {
    let key = |text: &str| text.parse::<i64>().map(|number| number.rem_euclid(3));
    key(left).unwrap().cmp(&key(right).unwrap())
}

fn options() -> crate::tests::TestOptions {
    crate::tests::TestOptions { collations: vec![("fold", fold)], ..Default::default() }
}
/// The contentful FTS5 table, whose content table holds its rows.
pub(super) const SEARCH: &str = "s";

/// The app's views, which the view rule's statements read: each with the
/// columns its statements can name.
fn views() -> Vec<(&'static str, Query, Vec<&'static str>)> {
    let plain = |alias: &str| {
        vec![
            out(col(alias, "id")),
            out(col(alias, "a")),
            out(col(alias, "b")),
            out(col(alias, "c")),
        ]
    };
    let core = |from: Vec<Item>, conditions: Vec<Cond>, select: Vec<Out>| Core {
        from,
        conditions,
        select,
        group: Vec::new(),
        distinct: false,
    };
    let four = vec!["id", "a", "b", "c"];
    vec![
        ("tv", select(core(vec![table("x", "t")], Vec::new(), plain("x"))), four.clone()),
        (
            "tuv",
            select(core(
                vec![
                    table("x", "t"),
                    item(
                        "y",
                        Source::Table("u"),
                        Join::Inner,
                        vec![eq(col("y", "a"), col("x", "id"))],
                    ),
                ],
                Vec::new(),
                vec![
                    out(col("x", "id")),
                    out(col("x", "a")),
                    out(col("y", "b")),
                    out(col("y", "c")),
                ],
            )),
            four.clone(),
        ),
        (
            "uwv",
            Query {
                arms: vec![
                    core(vec![table("x", "u")], Vec::new(), plain("x")),
                    core(vec![table("y", "w")], Vec::new(), plain("y")),
                ],
                ops: vec!["UNION ALL"],
                ..Query::default()
            },
            four.clone(),
        ),
        (
            "gv",
            select(Core {
                from: vec![table("x", "t")],
                conditions: Vec::new(),
                select: vec![
                    Out::Expr(col("x", "a"), Some("a".to_owned())),
                    Out::Count(Some("n".to_owned())),
                ],
                group: vec![col("x", "a")],
                distinct: false,
            }),
            vec!["a", "n"],
        ),
        (
            "lv",
            select(core(
                vec![
                    table("x", "u"),
                    item(
                        "y",
                        Source::Table("w"),
                        Join::Left,
                        vec![eq(col("y", "id"), col("x", "c"))],
                    ),
                ],
                Vec::new(),
                vec![
                    out(col("x", "id")),
                    out(col("x", "a")),
                    out(col("y", "b")),
                    out(col("y", "c")),
                ],
            )),
            four.clone(),
        ),
        (
            "fv",
            select(core(vec![table("x", "w")], vec![cmp("<", col("x", "c"), lit(3))], plain("x"))),
            four,
        ),
    ]
}

fn schema_sql() -> String {
    let mut sql: Vec<String> = ALL_TABLES
        .iter()
        .map(|name| match *name {
            KEYED => format!("CREATE TABLE {name}{KEYED_SQL}"),
            SEARCH => format!("CREATE VIRTUAL TABLE {name} USING fts5(id UNINDEXED, a, b, c, n)"),
            _ => format!("CREATE TABLE {name}{TABLE_SQL}"),
        })
        .collect();
    sql.push("CREATE INDEX u_a ON u(a)".to_owned());
    sql.push("CREATE INDEX w_c ON w(c)".to_owned());
    for (name, query, _) in views() {
        sql.push(format!("CREATE VIEW {name} AS {}", query.sql()));
    }
    sql.join(";\n")
}

fn schema_json() -> Json {
    let columns = |table: &str| -> Vec<Json> {
        self::columns(table)
            .iter()
            .map(|name| {
                let affinity = match (table, *name) {
                    (SEARCH, _) | (_, "b") => json!("blob"),
                    (_, "n" | "s") => Json::Null,
                    _ => json!("numeric"),
                };
                let mut column = json!({"name": name, "affinity": affinity});
                if table == KEYED && *name == "a" {
                    column["not_null"] = json!(true);
                }
                if *name == "s" {
                    column["collation"] = json!("fold");
                }
                column
            })
            .collect()
    };
    json!({
        // sqlive doesn't know `SEARCH`'s first column to hold its rowid.
        "tables": ALL_TABLES.iter().map(|name| {
            // Each of `KEYED`'s unique keys, by its columns' places.
            let keys = if *name == KEYED { json!([[1, 3], [2, 1]]) } else { json!([]) };
            json!({"name": name, "columns": columns(name), "rowid": *name != SEARCH, "keys": keys})
        }).collect::<Vec<_>>(),
        "views": views().into_iter().map(|(name, query, _)| json!({"name": name, "query": query.json()})).collect::<Vec<_>>(),
    })
}

/// A value of the columns a to n.
pub(super) fn value(rng: &mut Rng) -> Option<i64> {
    if rng.chance(15) {
        None
    } else {
        Some(rng.below(4) as i64)
    }
}

/// A value of the text column `s`: the digit as a string.
pub(super) fn text_value(value: Option<i64>) -> String {
    value.map_or_else(|| "NULL".to_owned(), |value| format!("'{value}'"))
}

pub(super) fn sql_value(value: Option<i64>) -> String {
    value.map_or_else(|| "NULL".to_owned(), |value| value.to_string())
}

/// An insert of a row of `id` into `table`, as `verb`: into `SEARCH` with its
/// rowid `id` too.
pub(super) fn insert(verb: &str, table: &str, id: usize, rng: &mut Rng) -> String {
    let (rowid, key) =
        if table == SEARCH { ("rowid, ", format!("{id}, ")) } else { ("", String::new()) };
    let (text_column, text) = if table == SEARCH {
        (String::new(), String::new())
    } else {
        (", s".to_owned(), format!(", {}", text_value(value(rng))))
    };
    format!(
        "{verb} INTO {table}({rowid}id, a, b, c, n{text_column}) VALUES ({key}{id}, {}, {}, {}, {}{text})",
        sql_value(value(rng)),
        sql_value(value(rng)),
        rng.below(6),
        sql_value(value(rng))
    )
}

/// A write of one to three statements, each to one of `TABLES`.
pub(super) fn write(rng: &mut Rng) -> String {
    write_to(rng, &TABLES)
}

/// A write of one to three statements, half of them to `KEYED`.
fn keyed_write(rng: &mut Rng) -> String {
    write_to(rng, &[KEYED, KEYED, KEYED, "t", "u", "w"])
}

/// A write of one to three statements, each to one of `tables`. A statement
/// leaves a row that would break a constraint as it is.
fn write_to(rng: &mut Rng, tables: &[&'static str]) -> String {
    let mut statements = Vec::new();
    for _ in 0..1 + rng.below(3) {
        let table = pick(rng, tables);
        let id = rng.below(8);
        statements.push(match rng.below(9) {
            0 | 1 => insert("INSERT OR IGNORE", table, id, rng),
            2 | 3 => {
                let column = pick(rng, &["a", "b", "n", "s"]);
                if column == "s" && table != SEARCH {
                    let text = text_value(value(rng));
                    format!("UPDATE OR IGNORE {table} SET s = {text} WHERE id = {id}")
                } else {
                    let column = if column == "s" { "a" } else { column };
                    format!(
                        "UPDATE OR IGNORE {table} SET {column} = {} WHERE id = {id}",
                        sql_value(value(rng))
                    )
                }
            }
            4 => format!("UPDATE OR IGNORE {table} SET c = {} WHERE id = {id}", rng.below(6)),
            5 => {
                let (to, a) = (rng.below(6), rng.below(4));
                format!("UPDATE OR IGNORE {table} SET c = {to} WHERE a = {a}")
            }
            6 => {
                let to = rng.below(8);
                let rowid = if table == SEARCH { format!("rowid = {to}, ") } else { String::new() };
                format!("UPDATE OR IGNORE {table} SET {rowid}id = {to} WHERE id = {id}")
            }
            _ => format!("DELETE FROM {table} WHERE id = {id}"),
        });
    }
    statements.join("; ")
}

type Rows = Vec<Vec<Option<i64>>>;

fn integers(row: &rusqlite::Row<'_>) -> rusqlite::Result<Vec<Option<i64>>> {
    (0..row.as_ref().column_count())
        .map(|index| {
            Ok(match row.get::<_, SqlValue>(index)? {
                SqlValue::Null => None,
                SqlValue::Integer(value) => Some(value),
                // The text column `s` holds the digits the model holds as
                // integers.
                SqlValue::Text(text) => Some(
                    String::from_utf8(text)
                        .ok()
                        .and_then(|text| text.parse().ok())
                        .expect("a value the model holds"),
                ),
                other => panic!("a value the model doesn't hold: {other:?}"),
            })
        })
        .collect()
}

fn read(sql: &TestRead<'_>, text: &str) -> rusqlite::Result<Rows> {
    sql.query(text, [], integers)
}

fn one_shot(db: &TestDatabase, sql: &str) -> Rows {
    let sql = sql.to_owned();
    db.read_app(move |context| Ok::<_, QueryError>(read(&context, &sql)?)).unwrap()
}

/// Each table's rows, by id.
fn snapshot(db: &TestDatabase) -> Json {
    let mut tables = serde_json::Map::new();
    for name in ALL_TABLES {
        tables.insert(
            name.to_owned(),
            json!(one_shot(
                db,
                &format!("SELECT {} FROM {name} ORDER BY id", columns(name).join(", "))
            )),
        );
    }
    Json::Object(tables)
}

/// A copy of the database on a plain connection, which applies each write
/// one statement at a time to find the rows each statement changes, which
/// the model folds into each row's net change as sqlive does. `SEARCH`'s
/// changes are those SQLite makes to its content table, which a hook on the
/// copy records as sqlive's pre-update hook records the test's writes: FTS5
/// deletes and inserts again each row an UPDATE names, changed or not.
struct Mirror {
    connection: rusqlite::Connection,
    /// The operations on `SEARCH`'s content table since the last statement,
    /// as changes of `SEARCH`'s rows.
    search: Arc<std::sync::Mutex<Vec<Json>>>,
}

impl Mirror {
    fn open() -> Self {
        use rusqlite::hooks::{Action, PreUpdateCase};
        use rusqlite::types::ValueRef;
        let mut connection = rusqlite::Connection::open_in_memory().unwrap();
        connection.create_collation("fold", fold).unwrap();
        connection.execute_batch(&schema_sql()).unwrap();
        let search = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen = Arc::clone(&search);
        let content = format!("{SEARCH}_content");
        connection
            .preupdate_hook(Some(move |_: Action, _: &str, table: &str, case: &PreUpdateCase| {
                if table != content {
                    return;
                }
                // The content table's `id` is the rowid; `c0` to `c4` hold
                // the table's columns.
                let cell = |value: rusqlite::Result<ValueRef<'_>>| match value.unwrap() {
                    ValueRef::Null => None,
                    ValueRef::Integer(value) => Some(value),
                    other => panic!("a value the model doesn't hold: {other:?}"),
                };
                let row = |value: &dyn Fn(i32) -> Option<i64>| -> Json {
                    json!((1..=COLUMNS.len() as i32).map(value).collect::<Vec<_>>())
                };
                let (old, new) = match case {
                    PreUpdateCase::Insert(new) => {
                        (Json::Null, row(&|index| cell(new.get_new_column_value(index))))
                    }
                    PreUpdateCase::Delete(old) => {
                        (row(&|index| cell(old.get_old_column_value(index))), Json::Null)
                    }
                    PreUpdateCase::Update { old_value_accessor: old, new_value_accessor: new } => (
                        row(&|index| cell(old.get_old_column_value(index))),
                        row(&|index| cell(new.get_new_column_value(index))),
                    ),
                    PreUpdateCase::Unknown => return,
                };
                seen.lock().unwrap().push(json!({"table": SEARCH, "old": old, "new": new}));
            }))
            .unwrap();
        Self { connection, search }
    }

    fn rows(&self, table: &str) -> std::collections::BTreeMap<i64, Vec<Option<i64>>> {
        let mut statement = self
            .connection
            .prepare(&format!("SELECT {} FROM {table}", columns(table).join(", ")))
            .unwrap();
        let rows = statement.query_map([], integers).unwrap();
        rows.map(|row| {
            let row = row.unwrap();
            (row[0].expect("a rowid"), row)
        })
        .collect()
    }

    /// Apply `write`, and the row changes of each of its statements in turn.
    fn apply(&self, write: &str) -> Vec<Json> {
        let mut changes = Vec::new();
        let tables = ALL_TABLES.iter().filter(|table| **table != SEARCH);
        for statement in write.split("; ") {
            let before: Vec<_> = tables.clone().map(|table| self.rows(table)).collect();
            self.connection.execute_batch(statement).unwrap();
            for (table, old) in tables.clone().zip(before) {
                let new = self.rows(table);
                let ids: std::collections::BTreeSet<i64> =
                    old.keys().chain(new.keys()).copied().collect();
                for id in ids {
                    let (old, new) = (old.get(&id), new.get(&id));
                    if old != new {
                        changes.push(json!({"table": table, "old": old, "new": new}));
                    }
                }
            }
            changes.append(&mut self.search.lock().unwrap());
        }
        changes
    }
}

/// The model's answer for one write.
struct Answer {
    reruns: bool,
    /// Whether the statement is well formed (`Statement.wellFormed`), as the
    /// composed proof assumes of every statement the JSON reader builds.
    well_formed: bool,
    before: Rows,
    after: Rows,
    scopes: Json,
}

fn ask(runner: &std::ffi::OsStr, input: &Json) -> Answer {
    let mut child = Command::new(runner)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("the runner starts");
    child.stdin.take().unwrap().write_all(input.to_string().as_bytes()).unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "the runner refused {input}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let answer: Json = serde_json::from_slice(&output.stdout).unwrap();
    let rows = |key: &str| -> Rows { serde_json::from_value(answer[key].clone()).unwrap() };
    Answer {
        reruns: answer["reruns"].as_bool().unwrap(),
        well_formed: answer["wellFormed"].as_bool().unwrap(),
        before: rows("before"),
        after: rows("after"),
        scopes: answer["scopes"].clone(),
    }
}

fn sorted(rows: &Rows) -> Rows {
    let mut rows = rows.clone();
    rows.sort();
    rows
}

async fn next_rows(query: &mut LiveQuery<(usize, Rows), QueryError>) -> Rows {
    tokio::time::timeout(HANG_GUARD, query.next_inline())
        .await
        .expect("the live query answers")
        .expect("the live query is open")
        .unwrap()
        .1
}

/// The app's function `nz(e)`, `e` or 0 where `e` is NULL, on every
/// connection, registered deterministic (`Expr::App`).
fn register_nz(db: &TestDatabase) {
    use rusqlite::functions::FunctionFlags;
    db.with_database(|database| {
        database.each_connection(|connection| {
            connection
                .connection
                .create_scalar_function(
                    "nz",
                    1,
                    FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
                    |context| Ok(context.get::<Option<i64>>(0)?.unwrap_or(0)),
                )
                .unwrap()
        })
    });
}

/// Each write a statement's live query sees.
const STEPS: usize = 12;

/// Run one generated statement through `writes`, comparing sqlive's
/// decisions with the model's and both with SQLite's rows.
async fn run_case(
    runner: &std::ffi::OsStr,
    seed: u64,
    rule: &str,
    statement: &Query,
    writes: fn(&mut Rng) -> String,
    rng: &mut Rng,
) -> usize {
    let sql = statement.sql();
    let db = TestDatabase::open_with(&schema_sql(), options());
    register_nz(&db);
    let mut rows = Vec::new();
    for table in ALL_TABLES {
        // `KEYED` leaves out a row that would break a constraint.
        let verb = if table == KEYED { "INSERT OR IGNORE" } else { "INSERT" };
        for id in 0..6 {
            rows.push(insert(verb, table, id, rng));
        }
    }
    let mirror = Mirror::open();
    mirror.apply(&rows.join("; "));
    db.write(&rows.join("; ")).unwrap();
    let work = crate::ConnectionWorkCounter::new();
    db.track_work(&work);
    let runs = Arc::new(AtomicUsize::new(0));
    let text = sql.clone();
    let mut live = db.subscribe(move |context| {
        let run = runs.fetch_add(1, Ordering::SeqCst);
        Ok((run, read(&context, &text)?))
    });
    let mut last = next_rows(&mut live).await;
    let ordered = !statement.order.is_empty();
    let differ = |a: &Rows, b: &Rows| if ordered { a != b } else { sorted(a) != sorted(b) };
    let mut spared = 0;
    for step in 0..STEPS {
        let before = snapshot(&db);
        let change = writes(rng);
        let changes = mirror.apply(&change);
        work.take();
        db.write(&change).unwrap();
        let stopped = work.take().stopped_questions > 0;
        let after = snapshot(&db);
        let fresh = one_shot(&db, &sql);
        let marked = live.is_marked_for_rerun();
        let input = json!({
            "schema": schema_json(),
            "statement": statement.json(),
            "before": before,
            "changes": changes,
            "after": after,
            "stopped": stopped,
        });
        let model = ask(runner, &input);
        let case = || {
            format!(
                "{rule}, seed {seed}, step {step}: `{sql}` after `{change}`\nsqlive reruns: {marked}, the model reruns: {}, a question stopped: {stopped}\nscopes: {}\nrows before: {last:?}\nrows after: {fresh:?}\nbefore: {before}\nafter: {after}\ninput: {input}",
                model.reruns, model.scopes
            )
        };
        assert!(model.well_formed, "the model's statement is not well formed: {}", case());
        assert_eq!(marked, model.reruns, "sqlive and the model disagree: {}", case());
        if differ(&fresh, &last) {
            assert!(marked, "a write sqlive and the model let pass changed the rows: {}", case());
        }
        if !statement.ambiguous() {
            assert_eq!(
                sorted(&model.before),
                sorted(&last),
                "the model's rows before differ from SQLite's: {}",
                case()
            );
            assert_eq!(
                sorted(&model.after),
                sorted(&fresh),
                "the model's rows after differ from SQLite's: {}",
                case()
            );
        }
        if marked {
            let rerun = next_rows(&mut live).await;
            assert_eq!(rerun, fresh, "a rerun differs from a read: {}", case());
            last = rerun;
        } else {
            spared += 1;
        }
    }
    drop(live);
    db.close();
    spared
}

/// Run `generate`'s statements, one a seed, through random writes.
async fn differential(rule: &str, generate: fn(&mut Rng) -> Query) {
    differential_with(rule, generate, write).await;
}

/// Run `generate`'s statements, one a seed, through the writes `writes`
/// makes.
async fn differential_with(
    rule: &str,
    generate: fn(&mut Rng) -> Query,
    writes: fn(&mut Rng) -> String,
) {
    let runner = std::env::var_os("SQLIVE_NARROWING_LEAN")
        .expect("SQLIVE_NARROWING_LEAN names narrowingRunner");
    let seeds: u64 = std::env::var("SQLIVE_NARROWING_SEEDS")
        .ok()
        .map_or(60, |seeds| seeds.parse().expect("SQLIVE_NARROWING_SEEDS is a count"));
    let mut spared = 0;
    for seed in 0..seeds {
        let mut rng = Rng::seeded(seed);
        let statement = generate(&mut rng);
        spared += run_case(&runner, seed, rule, &statement, writes, &mut rng).await;
    }
    eprintln!("{rule}: {seeds} statements, {spared} writes neither reran");
    assert!(spared > 0, "{rule}: every write reran its statement, so nothing narrowed");
}

#[tokio::test]
#[ignore = "requires SQLIVE_NARROWING_LEAN; scripts/check.sh builds narrowingRunner"]
async fn term_matches_lean() {
    differential("term", term::statement).await;
}

#[tokio::test]
#[ignore = "requires SQLIVE_NARROWING_LEAN; scripts/check.sh builds narrowingRunner"]
async fn edge_matches_lean() {
    differential("edge", edge::statement).await;
}

#[tokio::test]
#[ignore = "requires SQLIVE_NARROWING_LEAN; scripts/check.sh builds narrowingRunner"]
async fn row_edge_matches_lean() {
    differential("row edge", row_edge::statement).await;
}

#[tokio::test]
#[ignore = "requires SQLIVE_NARROWING_LEAN; scripts/check.sh builds narrowingRunner"]
async fn row_member_matches_lean() {
    differential("row member", row_member::statement).await;
}

#[tokio::test]
#[ignore = "requires SQLIVE_NARROWING_LEAN; scripts/check.sh builds narrowingRunner"]
async fn nonstrict_edge_matches_lean() {
    differential("nonstrict edge", nonstrict_edge::statement).await;
}

#[tokio::test]
#[ignore = "requires SQLIVE_NARROWING_LEAN; scripts/check.sh builds narrowingRunner"]
async fn is_edge_matches_lean() {
    differential("is edge", is_edge::statement).await;
}

#[tokio::test]
#[ignore = "requires SQLIVE_NARROWING_LEAN; scripts/check.sh builds narrowingRunner"]
async fn or_matches_lean() {
    differential("or", or::statement).await;
}

#[tokio::test]
#[ignore = "requires SQLIVE_NARROWING_LEAN; scripts/check.sh builds narrowingRunner"]
async fn or_derived_matches_lean() {
    differential("or through subqueries", or_derived::statement).await;
}

#[tokio::test]
#[ignore = "requires SQLIVE_NARROWING_LEAN; scripts/check.sh builds narrowingRunner"]
async fn one_of_matches_lean() {
    differential("one of", one_of::statement).await;
}

#[tokio::test]
#[ignore = "requires SQLIVE_NARROWING_LEAN; scripts/check.sh builds narrowingRunner"]
async fn subquery_edge_matches_lean() {
    differential("subquery edge", subquery_edge::statement).await;
}

#[tokio::test]
#[ignore = "requires SQLIVE_NARROWING_LEAN; scripts/check.sh builds narrowingRunner"]
async fn subquery_window_matches_lean() {
    differential("subquery window", subquery_window::statement).await;
}

#[tokio::test]
#[ignore = "requires SQLIVE_NARROWING_LEAN; scripts/check.sh builds narrowingRunner"]
async fn row_subquery_edge_matches_lean() {
    differential("row subquery edge", row_subquery_edge::statement).await;
}

#[tokio::test]
#[ignore = "requires SQLIVE_NARROWING_LEAN; scripts/check.sh builds narrowingRunner"]
async fn push_down_matches_lean() {
    differential("push down", push_down::statement).await;
}

#[tokio::test]
#[ignore = "requires SQLIVE_NARROWING_LEAN; scripts/check.sh builds narrowingRunner"]
async fn group_key_matches_lean() {
    differential("group key", group_key::statement).await;
}

#[tokio::test]
#[ignore = "requires SQLIVE_NARROWING_LEAN; scripts/check.sh builds narrowingRunner"]
async fn unread_column_matches_lean() {
    differential("unread column", unread_column::statement).await;
}

#[tokio::test]
#[ignore = "requires SQLIVE_NARROWING_LEAN; scripts/check.sh builds narrowingRunner"]
async fn view_matches_lean() {
    differential("view", view::statement).await;
}

#[tokio::test]
#[ignore = "requires SQLIVE_NARROWING_LEAN; scripts/check.sh builds narrowingRunner"]
async fn nested_derived_matches_lean() {
    differential("nested derived", nested_derived::statement).await;
}

#[tokio::test]
#[ignore = "requires SQLIVE_NARROWING_LEAN; scripts/check.sh builds narrowingRunner"]
async fn cte_names_match_lean() {
    differential("cte names", cte_names::statement).await;
}

#[tokio::test]
#[ignore = "requires SQLIVE_NARROWING_LEAN; scripts/check.sh builds narrowingRunner"]
async fn parenthesized_matches_lean() {
    differential("parenthesized", parenthesized::statement).await;
}

#[tokio::test]
#[ignore = "requires SQLIVE_NARROWING_LEAN; scripts/check.sh builds narrowingRunner"]
async fn limit_matches_lean() {
    differential("limit", limit::statement).await;
}

#[tokio::test]
#[ignore = "requires SQLIVE_NARROWING_LEAN; scripts/check.sh builds narrowingRunner"]
async fn using_matches_lean() {
    differential("using", using::statement).await;
}

#[tokio::test]
#[ignore = "requires SQLIVE_NARROWING_LEAN; scripts/check.sh builds narrowingRunner"]
async fn whole_matches_lean() {
    differential("whole", whole::statement).await;
}

#[tokio::test]
#[ignore = "requires SQLIVE_NARROWING_LEAN; scripts/check.sh builds narrowingRunner"]
async fn aggregate_matches_lean() {
    differential("aggregate", aggregate::statement).await;
}

#[tokio::test]
#[ignore = "requires SQLIVE_NARROWING_LEAN; scripts/check.sh builds narrowingRunner"]
async fn distinct_matches_lean() {
    differential("distinct", distinct::statement).await;
}

#[tokio::test]
#[ignore = "requires SQLIVE_NARROWING_LEAN; scripts/check.sh builds narrowingRunner"]
async fn window_matches_lean() {
    differential("window", window::statement).await;
}

#[tokio::test]
#[ignore = "requires SQLIVE_NARROWING_LEAN; scripts/check.sh builds narrowingRunner"]
async fn carry_matches_lean() {
    differential("carry", carry::statement).await;
}

#[tokio::test]
#[ignore = "requires SQLIVE_NARROWING_LEAN; scripts/check.sh builds narrowingRunner"]
async fn references_match_lean() {
    differential("one table in several places", references::statement).await;
}

#[tokio::test]
#[ignore = "requires SQLIVE_NARROWING_LEAN; scripts/check.sh builds narrowingRunner"]
async fn carry_derived_matches_lean() {
    differential("carry through subqueries", carry_derived::statement).await;
}

#[tokio::test]
#[ignore = "requires SQLIVE_NARROWING_LEAN; scripts/check.sh builds narrowingRunner"]
async fn mixed_shapes_match_lean() {
    differential("mixed", mixed::statement).await;
}

#[tokio::test]
#[ignore = "requires SQLIVE_NARROWING_LEAN; scripts/check.sh builds narrowingRunner"]
async fn net_change_matches_lean() {
    differential_with("net change", net::statement, net::write).await;
}

#[tokio::test]
#[ignore = "requires SQLIVE_NARROWING_LEAN; scripts/check.sh builds narrowingRunner"]
async fn unique_order_matches_lean() {
    differential_with("unique order", unique_order::statement, keyed_write).await;
}

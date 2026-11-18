//! sqlive's narrowing against its Lean model (`crates/sqlive/proofs`, run
//! through its `narrowingRunner`) and SQLite. Each rule's generator writes
//! statements of its rule's shape, as SQL and as the runner's JSON tree. For
//! each statement the test opens a live query and makes random writes; after
//! each it fails when sqlive and the model disagree on whether the write
//! reruns the query, when a write either lets pass changed the statement's
//! rows, or when the model's rows differ from SQLite's.
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

#[path = "narrowing_model/edge.rs"]
mod edge;
#[path = "narrowing_model/limit.rs"]
mod limit;
#[path = "narrowing_model/mixed.rs"]
mod mixed;
#[path = "narrowing_model/nested_derived.rs"]
mod nested_derived;
#[path = "narrowing_model/push_down.rs"]
mod push_down;
#[path = "narrowing_model/subquery_edge.rs"]
mod subquery_edge;
#[path = "narrowing_model/term.rs"]
mod term;
#[path = "narrowing_model/using.rs"]
mod using;
#[path = "narrowing_model/view.rs"]
mod view;
#[path = "narrowing_model/whole.rs"]
mod whole;

/// One of `names`, at random.
pub(super) fn pick(rng: &mut Rng, names: &[&'static str]) -> &'static str {
    names[rng.below(names.len())]
}

/// The tables every statement reads: an INTEGER key, numeric columns, an
/// untyped one (BLOB affinity) and one whose collation sqlive doesn't narrow
/// by. Every value is an integer or NULL, which SQLite compares as the model
/// does.
pub(super) const TABLES: [&str; 3] = ["t", "u", "w"];
pub(super) const COLUMNS: [&str; 5] = ["id", "a", "b", "c", "n"];
const TABLE_SQL: &str =
    "(id INTEGER PRIMARY KEY, a INTEGER, b, c INTEGER, n INTEGER COLLATE NOCASE)";

/// One SELECT statement, or a query inside one.
#[derive(Clone, Default)]
pub(super) struct Query {
    pub(super) with: Vec<(String, Query)>,
    pub(super) arms: Vec<Core>,
    /// The operator before each arm after the first: `UNION ALL`, `UNION`,
    /// `EXCEPT` or `INTERSECT`.
    pub(super) ops: Vec<&'static str>,
    /// Result column positions, each descending when `true`.
    pub(super) order: Vec<(usize, bool)>,
    /// Whether the ORDER BY names each term's column instead of its position.
    pub(super) order_named: bool,
    pub(super) limit: Option<u32>,
}

#[derive(Clone, Default)]
pub(super) struct Core {
    pub(super) from: Vec<Item>,
    pub(super) conditions: Vec<Cond>,
    pub(super) select: Vec<Out>,
    pub(super) group: Vec<Expr>,
}

#[derive(Clone)]
pub(super) struct Item {
    pub(super) alias: String,
    pub(super) source: Source,
    pub(super) join: Join,
    pub(super) on: Vec<Cond>,
}

#[derive(Clone, Copy, PartialEq)]
pub(super) enum Join {
    /// The first item, or one after a comma.
    Comma,
    Inner,
    Left,
    /// `JOIN ... USING (columns)`, or `LEFT JOIN` when `left`: the model reads
    /// it as the equality of each column with the leftmost item before it
    /// that has the column.
    Using {
        left: bool,
        columns: &'static [&'static str],
    },
    Natural,
}

#[derive(Clone)]
pub(super) enum Source {
    Table(&'static str),
    View(&'static str),
    Cte(String),
    Query(Box<Query>),
}

#[derive(Clone)]
pub(super) enum Expr {
    Col(String, &'static str),
    Lit(Option<i64>),
    Add(Box<Expr>, Box<Expr>),
    Sub(Box<Query>),
}

#[derive(Clone)]
pub(super) enum Cond {
    Cmp(&'static str, Expr, Expr),
    Null(Expr, bool),
    InList(Expr, Vec<Expr>),
    InQuery(Expr, Box<Query>, bool),
    Exists(Box<Query>, bool),
    And(Box<Cond>, Box<Cond>),
    Or(Box<Cond>, Box<Cond>),
    Not(Box<Cond>),
}

#[derive(Clone)]
pub(super) enum Out {
    Expr(Expr, Option<String>),
    Count(Option<String>),
    Max(Expr, Option<String>),
}

pub(super) fn col(alias: &str, column: &'static str) -> Expr {
    Expr::Col(alias.to_owned(), column)
}

pub(super) fn lit(value: i64) -> Expr {
    Expr::Lit(Some(value))
}

pub(super) fn cmp(op: &'static str, left: Expr, right: Expr) -> Cond {
    Cond::Cmp(op, left, right)
}

pub(super) fn eq(left: Expr, right: Expr) -> Cond {
    Cond::Cmp("=", left, right)
}

pub(super) fn out(e: Expr) -> Out {
    Out::Expr(e, None)
}

pub(super) fn item(alias: &str, source: Source, join: Join, on: Vec<Cond>) -> Item {
    Item { alias: alias.to_owned(), source, join, on }
}

pub(super) fn table(alias: &str, name: &'static str) -> Item {
    item(alias, Source::Table(name), Join::Comma, Vec::new())
}

/// A query of one core.
pub(super) fn select(core: Core) -> Query {
    Query { arms: vec![core], ..Query::default() }
}

impl Query {
    pub(super) fn sql(&self) -> String {
        let mut sql = String::new();
        if !self.with.is_empty() {
            let ctes: Vec<String> = self
                .with
                .iter()
                .map(|(name, query)| format!("{name} AS ({})", query.sql()))
                .collect();
            sql.push_str(&format!("WITH {} ", ctes.join(", ")));
        }
        for (position, arm) in self.arms.iter().enumerate() {
            if position > 0 {
                sql.push_str(&format!(" {} ", self.ops[position - 1]));
            }
            sql.push_str(&arm.sql());
        }
        if !self.order.is_empty() {
            let terms: Vec<String> = self
                .order
                .iter()
                .map(|(position, descending)| {
                    let term = match (&self.arms[0].select[*position], self.order_named) {
                        (Out::Expr(e, _), true) => e.sql(),
                        _ => (position + 1).to_string(),
                    };
                    if *descending {
                        format!("{term} DESC")
                    } else {
                        term
                    }
                })
                .collect();
            sql.push_str(&format!(" ORDER BY {}", terms.join(", ")));
        }
        if let Some(limit) = self.limit {
            sql.push_str(&format!(" LIMIT {limit}"));
        }
        sql
    }

    pub(super) fn json(&self) -> Json {
        let with: Vec<Json> = self
            .with
            .iter()
            .map(|(name, query)| json!({"name": name, "query": query.json()}))
            .collect();
        let order: Vec<Json> = self
            .order
            .iter()
            .map(|(position, descending)| json!({"position": position, "desc": descending}))
            .collect();
        json!({
            "with": with,
            "arms": self.arms.iter().map(Core::json).collect::<Vec<_>>(),
            "ops": self.ops.iter().map(|op| op.to_ascii_lowercase()).collect::<Vec<_>>(),
            "order": order,
            "order_named": self.order_named,
            "limit": self.limit,
        })
    }

    /// Whether SQLite may pick other rows than the model does: a LIMIT
    /// anywhere in it breaks ties by an order neither states, and a scalar
    /// subquery returns whichever row SQLite reads first.
    fn ambiguous(&self) -> bool {
        self.limit.is_some()
            || self.with.iter().any(|(_, query)| query.ambiguous())
            || self.arms.iter().any(Core::ambiguous)
    }
}

impl Core {
    fn sql(&self) -> String {
        let columns: Vec<String> = self.select.iter().map(Out::sql).collect();
        let mut sql = format!("SELECT {}", columns.join(", "));
        for (position, item) in self.from.iter().enumerate() {
            let source = match &item.source {
                Source::Table(name) | Source::View(name) => (*name).to_owned(),
                Source::Cte(name) => name.clone(),
                Source::Query(query) => format!("({})", query.sql()),
            };
            let on = || {
                let conditions: Vec<String> = item.on.iter().map(Cond::sql).collect();
                if conditions.is_empty() {
                    String::new()
                } else {
                    format!(" ON {}", conditions.join(" AND "))
                }
            };
            match (position, item.join) {
                (0, _) => sql.push_str(&format!(" FROM {source} AS {}", item.alias)),
                (_, Join::Comma) => sql.push_str(&format!(", {source} AS {}", item.alias)),
                (_, Join::Inner) => {
                    sql.push_str(&format!(" JOIN {source} AS {}{}", item.alias, on()))
                }
                (_, Join::Left) => {
                    sql.push_str(&format!(" LEFT JOIN {source} AS {}{}", item.alias, on()));
                }
                (_, Join::Using { left, columns }) => {
                    sql.push_str(&format!(
                        " {}JOIN {source} AS {} USING ({})",
                        if left { "LEFT " } else { "" },
                        item.alias,
                        columns.join(", ")
                    ));
                }
                (_, Join::Natural) => {
                    sql.push_str(&format!(" NATURAL JOIN {source} AS {}", item.alias));
                }
            }
        }
        if !self.conditions.is_empty() {
            let conditions: Vec<String> = self.conditions.iter().map(Cond::sql).collect();
            sql.push_str(&format!(" WHERE {}", conditions.join(" AND ")));
        }
        if !self.group.is_empty() {
            let terms: Vec<String> = self.group.iter().map(Expr::sql).collect();
            sql.push_str(&format!(" GROUP BY {}", terms.join(", ")));
        }
        sql
    }

    fn json(&self) -> Json {
        let from: Vec<Json> = self
            .from
            .iter()
            .map(|item| {
                let (left, using) = match item.join {
                    Join::Left => (true, &[][..]),
                    Join::Using { left, columns } => (left, columns),
                    _ => (false, &[][..]),
                };
                let mut entry = json!({
                    "alias": item.alias,
                    "left": left,
                    "using": using,
                    "on": item.on.iter().map(Cond::json).collect::<Vec<_>>(),
                    "whole": if item.join == Join::Natural { vec!["natural"] } else { Vec::new() },
                });
                let (key, value) = match &item.source {
                    Source::Table(name) => ("table", json!(name)),
                    Source::View(name) => ("view", json!(name)),
                    Source::Cte(name) => ("cte", json!(name)),
                    Source::Query(query) => ("query", query.json()),
                };
                entry[key] = value;
                entry
            })
            .collect();
        json!({
            "from": from,
            "where": self.conditions.iter().map(Cond::json).collect::<Vec<_>>(),
            "select": self.select.iter().map(Out::json).collect::<Vec<_>>(),
            "group": self.group.iter().map(Expr::json).collect::<Vec<_>>(),
        })
    }

    fn ambiguous(&self) -> bool {
        self.from.iter().any(|item| match &item.source {
            Source::Query(query) => query.ambiguous(),
            _ => false,
        }) || self.conditions.iter().any(Cond::ambiguous)
            || self.select.iter().any(|column| match column {
                Out::Expr(e, _) | Out::Max(e, _) => e.ambiguous(),
                Out::Count(_) => false,
            })
            || self.from.iter().any(|item| item.on.iter().any(Cond::ambiguous))
    }
}

impl Expr {
    pub(super) fn sql(&self) -> String {
        match self {
            Self::Col(alias, column) => format!("{alias}.{column}"),
            Self::Lit(Some(value)) => value.to_string(),
            Self::Lit(None) => "NULL".to_owned(),
            Self::Add(left, right) => format!("({} + {})", left.sql(), right.sql()),
            Self::Sub(query) => format!("({})", query.sql()),
        }
    }

    fn json(&self) -> Json {
        match self {
            Self::Col(alias, column) => json!({"col": [alias, column]}),
            Self::Lit(value) => json!({"lit": value}),
            Self::Add(left, right) => json!({"add": [left.json(), right.json()]}),
            Self::Sub(query) => json!({"query": query.json()}),
        }
    }

    fn ambiguous(&self) -> bool {
        match self {
            Self::Col(..) | Self::Lit(_) => false,
            Self::Add(left, right) => left.ambiguous() || right.ambiguous(),
            Self::Sub(_) => true,
        }
    }
}

impl Cond {
    pub(super) fn sql(&self) -> String {
        match self {
            Self::Cmp(op, left, right) => format!("{} {op} {}", left.sql(), right.sql()),
            Self::Null(e, false) => format!("{} IS NULL", e.sql()),
            Self::Null(e, true) => format!("{} IS NOT NULL", e.sql()),
            Self::InList(e, values) => {
                let values: Vec<String> = values.iter().map(Expr::sql).collect();
                format!("{} IN ({})", e.sql(), values.join(", "))
            }
            Self::InQuery(e, query, not) => {
                format!("{} {}IN ({})", e.sql(), if *not { "NOT " } else { "" }, query.sql())
            }
            Self::Exists(query, not) => {
                format!("{}EXISTS ({})", if *not { "NOT " } else { "" }, query.sql())
            }
            Self::And(left, right) => format!("({} AND {})", left.sql(), right.sql()),
            Self::Or(left, right) => format!("({} OR {})", left.sql(), right.sql()),
            Self::Not(c) => format!("NOT ({})", c.sql()),
        }
    }

    fn json(&self) -> Json {
        match self {
            Self::Cmp(op, left, right) => {
                json!({"cmp": [op.to_ascii_lowercase(), left.json(), right.json()]})
            }
            Self::Null(e, not) => json!({"null": e.json(), "not": not}),
            Self::InList(e, values) => {
                json!({"in": e.json(), "list": values.iter().map(Expr::json).collect::<Vec<_>>()})
            }
            Self::InQuery(e, query, not) => {
                json!({"in": e.json(), "query": query.json(), "not": not})
            }
            Self::Exists(query, not) => json!({"exists": query.json(), "not": not}),
            Self::And(left, right) => json!({"and": [left.json(), right.json()]}),
            Self::Or(left, right) => json!({"or": [left.json(), right.json()]}),
            Self::Not(c) => json!({"negate": c.json()}),
        }
    }

    fn ambiguous(&self) -> bool {
        match self {
            Self::Cmp(_, left, right) => left.ambiguous() || right.ambiguous(),
            Self::Null(e, _) => e.ambiguous(),
            Self::InList(e, values) => e.ambiguous() || values.iter().any(Expr::ambiguous),
            Self::InQuery(e, query, _) => e.ambiguous() || query.ambiguous(),
            Self::Exists(query, _) => query.ambiguous(),
            Self::And(left, right) | Self::Or(left, right) => left.ambiguous() || right.ambiguous(),
            Self::Not(c) => c.ambiguous(),
        }
    }
}

impl Out {
    fn sql(&self) -> String {
        let (sql, alias) = match self {
            Self::Expr(e, alias) => (e.sql(), alias),
            Self::Count(alias) => ("count(*)".to_owned(), alias),
            Self::Max(e, alias) => (format!("max({})", e.sql()), alias),
        };
        match alias {
            Some(alias) => format!("{sql} AS {alias}"),
            None => sql,
        }
    }

    fn json(&self) -> Json {
        match self {
            Self::Expr(e, alias) => json!({"expr": e.json(), "as": alias}),
            Self::Count(alias) => json!({"count": true, "as": alias}),
            Self::Max(e, alias) => json!({"max": e.json(), "as": alias}),
        }
    }
}

/// The app's views, which the view rule's statements read: each with the
/// columns its statements can name.
pub(super) fn views() -> Vec<(&'static str, Query, Vec<&'static str>)> {
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
    let mut sql: Vec<String> =
        TABLES.iter().map(|name| format!("CREATE TABLE {name}{TABLE_SQL}")).collect();
    sql.push("CREATE INDEX u_a ON u(a)".to_owned());
    sql.push("CREATE INDEX w_c ON w(c)".to_owned());
    for (name, query, _) in views() {
        sql.push(format!("CREATE VIEW {name} AS {}", query.sql()));
    }
    sql.join(";\n")
}

fn schema_json() -> Json {
    let columns: Vec<Json> = COLUMNS
        .iter()
        .map(|name| {
            let affinity = match *name {
                "b" => json!("blob"),
                "n" => Json::Null,
                _ => json!("numeric"),
            };
            json!({"name": name, "affinity": affinity})
        })
        .collect();
    json!({
        "tables": TABLES.iter().map(|name| json!({"name": name, "columns": columns})).collect::<Vec<_>>(),
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

fn sql_value(value: Option<i64>) -> String {
    value.map_or_else(|| "NULL".to_owned(), |value| value.to_string())
}

/// A write of one to three statements, each to one table.
fn write(rng: &mut Rng) -> String {
    let mut statements = Vec::new();
    for _ in 0..1 + rng.below(3) {
        let table = pick(rng, &TABLES);
        let id = rng.below(8);
        statements.push(match rng.below(9) {
            0 | 1 => format!(
                "INSERT OR IGNORE INTO {table}(id, a, b, c, n) VALUES ({id}, {}, {}, {}, {})",
                sql_value(value(rng)),
                sql_value(value(rng)),
                rng.below(6),
                sql_value(value(rng))
            ),
            2 | 3 => {
                let column = pick(rng, &["a", "b", "n"]);
                format!("UPDATE {table} SET {column} = {} WHERE id = {id}", sql_value(value(rng)))
            }
            4 => format!("UPDATE {table} SET c = {} WHERE id = {id}", rng.below(6)),
            5 => format!("UPDATE {table} SET c = {} WHERE a = {}", rng.below(6), rng.below(4)),
            6 => format!("UPDATE OR IGNORE {table} SET id = {} WHERE id = {id}", rng.below(8)),
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
    for name in TABLES {
        tables.insert(
            name.to_owned(),
            json!(one_shot(db, &format!("SELECT id, a, b, c, n FROM {name} ORDER BY id"))),
        );
    }
    Json::Object(tables)
}

/// A copy of the database on a plain connection, which applies each write
/// one statement at a time to find the rows each statement changes, as
/// sqlive's pre-update hook reports them.
struct Mirror(rusqlite::Connection);

impl Mirror {
    fn open() -> Self {
        let connection = rusqlite::Connection::open_in_memory().unwrap();
        connection.execute_batch(&schema_sql()).unwrap();
        Self(connection)
    }

    fn rows(&self, table: &str) -> std::collections::BTreeMap<i64, Vec<Option<i64>>> {
        let mut statement = self.0.prepare(&format!("SELECT id, a, b, c, n FROM {table}")).unwrap();
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
        for statement in write.split("; ") {
            let before: Vec<_> = TABLES.iter().map(|table| self.rows(table)).collect();
            self.0.execute_batch(statement).unwrap();
            for (table, old) in TABLES.iter().zip(before) {
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
        }
        changes
    }
}

/// The model's answer for one write.
struct Answer {
    reruns: bool,
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

/// Each write a statement's live query sees.
const STEPS: usize = 12;

/// Run one generated statement through random writes, comparing sqlive's
/// decisions with the model's and both with SQLite's rows.
async fn run_case(
    runner: &std::ffi::OsStr,
    seed: u64,
    rule: &str,
    statement: &Query,
    rng: &mut Rng,
) -> usize {
    let sql = statement.sql();
    let db = TestDatabase::open(&schema_sql());
    let mut rows = Vec::new();
    for table in TABLES {
        for id in 0..6 {
            rows.push(format!(
                "INSERT INTO {table}(id, a, b, c, n) VALUES ({id}, {}, {}, {}, {})",
                sql_value(value(rng)),
                sql_value(value(rng)),
                rng.below(6),
                sql_value(value(rng))
            ));
        }
    }
    let mirror = Mirror::open();
    mirror.apply(&rows.join("; "));
    db.write(&rows.join("; ")).unwrap();
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
        let change = write(rng);
        let changes = mirror.apply(&change);
        db.write(&change).unwrap();
        let after = snapshot(&db);
        let fresh = one_shot(&db, &sql);
        let marked = live.is_marked_for_rerun();
        let input = json!({
            "schema": schema_json(),
            "statement": statement.json(),
            "before": before,
            "changes": changes,
            "after": after,
        });
        let model = ask(runner, &input);
        let case = || {
            format!(
                "{rule}, seed {seed}, step {step}: `{sql}` after `{change}`\nsqlive reruns: {marked}, the model reruns: {}\nscopes: {}\nrows before: {last:?}\nrows after: {fresh:?}\nbefore: {before}\nafter: {after}\ninput: {input}",
                model.reruns, model.scopes
            )
        };
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

/// Run `generate`'s statements, one a seed.
async fn differential(rule: &str, generate: fn(&mut Rng) -> Query) {
    let runner = std::env::var_os("SQLIVE_NARROWING_LEAN")
        .expect("SQLIVE_NARROWING_LEAN names narrowingRunner");
    let seeds: u64 = std::env::var("SQLIVE_NARROWING_SEEDS")
        .ok()
        .map_or(60, |seeds| seeds.parse().expect("SQLIVE_NARROWING_SEEDS is a count"));
    let mut spared = 0;
    for seed in 0..seeds {
        let mut rng = Rng::seeded(seed);
        let statement = generate(&mut rng);
        spared += run_case(&runner, seed, rule, &statement, &mut rng).await;
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
async fn subquery_edge_matches_lean() {
    differential("subquery edge", subquery_edge::statement).await;
}

#[tokio::test]
#[ignore = "requires SQLIVE_NARROWING_LEAN; scripts/check.sh builds narrowingRunner"]
async fn push_down_matches_lean() {
    differential("push down", push_down::statement).await;
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
async fn mixed_shapes_match_lean() {
    differential("mixed", mixed::statement).await;
}

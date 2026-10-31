//! Each way a write reaches live queries records the values their scopes
//! test, so a write outside a scope wakes nothing and one inside it does.
//! And over generated queries, schemas and writes, a live query is marked to
//! run again whenever a commit changes what it returns, and its rerun returns
//! what a one-shot read of the same SQL does (§4); a failure names the seed.
//! Writes change several tables at once, joins chain through joined tables,
//! and some lookups an index serves, so each write's lookups judge joins in
//! its own transaction; a marked query sometimes waits through more writes.
//! `COVEN_SCOPE_SEEDS` sets how many seeds run (default 40).

use crate::sql_value::SqlValue;
use crate::tests::TestStore;
use crate::{CovenResult, Database, LiveQuery, RowIdentity, SyncedTable};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

async fn next<T: Clone + PartialEq + Send + 'static>(query: &mut LiveQuery<T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), query.next())
        .await
        .expect("query must answer")
        .expect("the live query answers")
        .unwrap()
}

/// One write, run when awaited.
type Pending = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>;

const TRACKS: &str = "CREATE TABLE tracks(id TEXT NOT NULL PRIMARY KEY, release TEXT, title TEXT)";

fn titles(db: &Database) -> LiveQuery<Vec<String>> {
    db.subscribe(|sql| {
        Ok(sql.query("SELECT title FROM tracks WHERE release='a' ORDER BY id", [], |r| {
            r.get::<_, String>(0)
        })?)
    })
}

/// Through `write`, as the app's SQL in a local write, and through
/// `commit`, as coven's own transaction: other releases' tracks wake
/// nothing, this release's do, and so does a track moving in or out.
async fn narrows(db: &Database, write: impl Fn(&'static str) -> Pending) {
    write("INSERT INTO tracks VALUES ('1','a','one'),('2','b','two')").await;
    let mut query = titles(db);
    assert_eq!(next(&mut query).await, ["one"]);
    write("UPDATE tracks SET title='deux' WHERE id='2'").await;
    write("INSERT INTO tracks VALUES ('3','c','three')").await;
    write("DELETE FROM tracks WHERE id='3'").await;
    assert!(!query.is_marked_for_rerun());
    write("UPDATE tracks SET title='uno' WHERE id='1'").await;
    assert_eq!(next(&mut query).await, ["uno"]);
    write("UPDATE tracks SET release='a' WHERE id='2'").await;
    assert_eq!(next(&mut query).await, ["uno", "deux"]);
    write("UPDATE tracks SET release='b' WHERE id='1'").await;
    assert_eq!(next(&mut query).await, ["deux"]);
    write("UPDATE tracks SET id='9' WHERE id='1'").await;
    assert!(!query.is_marked_for_rerun());
}

fn local_write(db: &Database) -> impl Fn(&'static str) -> Pending {
    let db = db.clone();
    move |sql| {
        let db = db.clone();
        Box::pin(async move { crate::write::tests::sql(&db, sql).await.unwrap() })
    }
}

#[tokio::test]
async fn a_synced_tables_local_writes_record_scoped_values() {
    let store = TestStore::new();
    let db = store
        .schema(vec![SyncedTable::new("tracks", RowIdentity::SharedKey)], TRACKS)
        .await
        .unwrap();
    narrows(&db, local_write(&db)).await;
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_device_local_tables_local_writes_record_scoped_values() {
    let store = TestStore::new();
    let db = store.schema(vec![], TRACKS).await.unwrap();
    narrows(&db, local_write(&db)).await;
    db.close().await.unwrap();
}

/// A session leaves out the columns an UPDATE didn't change; coven reads
/// their values from the row before the transaction commits.
#[tokio::test]
async fn coven_transactions_read_unchanged_scoped_values() {
    let store = TestStore::new();
    let db = store.schema(vec![], TRACKS).await.unwrap();
    let commit = {
        let db = db.clone();
        move |sql: &'static str| -> Pending {
            db.commit_writer(|writer| writer.batch(sql).unwrap());
            Box::pin(async {})
        }
    };
    narrows(&db, commit).await;
    db.close().await.unwrap();
}

/// Applying another device's write records the values a waiting scope
/// tests, as a local write does: another release's tracks wake nothing.
#[tokio::test]
async fn another_devices_writes_record_scoped_values() {
    use coven_format::value::Value;
    let store = TestStore::new();
    let db = store
        .schema(vec![SyncedTable::new("tracks", RowIdentity::SharedKey)], TRACKS)
        .await
        .unwrap();
    crate::write::tests::sql(&db, "INSERT INTO tracks VALUES ('1','a','one'),('2','b','two')")
        .await
        .unwrap();
    let mut query = titles(&db);
    assert_eq!(next(&mut query).await, ["one"]);
    crate::tests::remote_update(&db, "tracks", "2", &[("title", Value::Text("deux".into()))]).await;
    assert!(!query.is_marked_for_rerun());
    crate::tests::remote_update(&db, "tracks", "1", &[("title", Value::Text("uno".into()))]).await;
    assert_eq!(next(&mut query).await, ["uno"]);
    crate::tests::remote_update(&db, "tracks", "2", &[("release", Value::Text("a".into()))]).await;
    assert_eq!(next(&mut query).await, ["uno", "deux"]);
    db.close().await.unwrap();
}

/// A write made while a query is reading records no values for the scope
/// that run will have: the changes it couldn't judge still wake it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_write_during_the_read_that_reveals_the_scope_wakes_it() {
    let store = TestStore::new();
    let db = store.schema(vec![], TRACKS).await.unwrap();
    let (entered, entry) = tokio::sync::oneshot::channel();
    let entered = Mutex::new(Some(entered));
    let (release, released) = std::sync::mpsc::channel();
    let released = Mutex::new(released);
    let mut query = db.subscribe(move |sql| -> CovenResult<_> {
        if let Some(entered) = entered.lock().unwrap().take() {
            entered.send(()).unwrap();
            released.lock().unwrap().recv_timeout(Duration::from_secs(5)).unwrap();
        }
        Ok(sql.query("SELECT title FROM tracks WHERE release='a' ORDER BY id", [], |r| {
            r.get::<_, String>(0)
        })?)
    });
    let task = tokio::spawn(async move {
        let value = next(&mut query).await;
        (query, value)
    });
    entry.await.unwrap();
    crate::write::tests::sql(&db, "INSERT INTO tracks VALUES ('1','a','one')").await.unwrap();
    release.send(()).unwrap();
    let (mut query, first) = task.await.unwrap();
    assert!(first.is_empty());
    assert!(query.is_marked_for_rerun());
    assert_eq!(next(&mut query).await, ["one"]);
    db.close().await.unwrap();
}

/// A small deterministic generator, so a seed reproduces a case.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, bound: usize) -> usize {
        (self.next() % bound as u64) as usize
    }

    fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len())]
    }

    fn chance(&mut self, percent: usize) -> bool {
        self.below(100) < percent
    }
}

/// Device-local tables keyed by integer, and a synced one keyed by text,
/// with columns whose small domains make values collide, and text values
/// that name the synced table's keys, so joins find rows.
const SCHEMA: &str = "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c INTEGER);
CREATE TABLE u(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c INTEGER, n TEXT COLLATE NOCASE);
CREATE TABLE s(id TEXT NOT NULL PRIMARY KEY, a INTEGER, b TEXT, c INTEGER);
CREATE VIEW tv AS SELECT id, a, b, c FROM t;
CREATE VIEW tu AS SELECT t.id, t.a, u.b, u.c FROM t JOIN u ON u.a = t.id;
CREATE VIEW tuv AS SELECT id, a, b, c FROM t UNION ALL SELECT * FROM (SELECT id, a, b, c FROM u);
CREATE VIEW grouped AS SELECT a AS id, count(*) AS a, b, min(c) AS c FROM t GROUP BY a;
CREATE VIEW lefty AS SELECT u.id, u.a, t.b, t.c FROM u LEFT JOIN t ON t.id = u.c;
CREATE VIEW sv(id, a, b, c) AS SELECT s.id, s.a, s.b, s.c FROM s WHERE s.c < 8;
CREATE INDEX u_a ON u(a);
CREATE INDEX s_b ON s(b);";

/// Views of the generated tables, each with the columns id, a, b and c, and
/// the table whose ids theirs are.
const VIEWS: [(&str, &str); 6] =
    [("tv", "t"), ("tu", "t"), ("tuv", "t"), ("grouped", "t"), ("lefty", "u"), ("sv", "s")];

const TABLES: [&str; 3] = ["t", "u", "s"];

fn key(table: &str, id: usize) -> String {
    if table == "s" {
        format!("'k{id}'")
    } else {
        id.to_string()
    }
}

fn a_value(rng: &mut Rng) -> String {
    (*rng.pick(&["NULL", "0", "1", "2", "3", "4"])).to_owned()
}

fn b_value(rng: &mut Rng) -> String {
    (*rng.pick(&["NULL", "'x'", "'k1'", "'k2'", "'k3'", "'1'"])).to_owned()
}

/// One generated live query.
struct Generated {
    sql: String,
    parameters: Vec<SqlValue>,
    /// Whether its WHERE holds a condition coven narrows by.
    narrows: bool,
}

/// A condition on one row of `table`, named `name` in the statement, and
/// whether coven can narrow by it.
fn condition(
    rng: &mut Rng,
    table: &str,
    name: &str,
    parameters: &mut Vec<SqlValue>,
    depth: usize,
) -> (String, bool) {
    if depth < 2 && rng.chance(30) {
        let (left, left_narrows) = condition(rng, table, name, parameters, depth + 1);
        let (right, right_narrows) = condition(rng, table, name, parameters, depth + 1);
        return if rng.chance(60) {
            (format!("({left} AND {right})"), left_narrows || right_narrows)
        } else {
            (format!("({left} OR {right})"), left_narrows && right_narrows)
        };
    }
    let id = rng.below(10);
    match rng.below(11) {
        0 => (format!("{name}.id = {}", key(table, id)), true),
        1 => (format!("{name}.a = {}", rng.below(5)), true),
        2 => (format!("{name}.b = {}", b_value(rng)), true),
        3 => (format!("{name}.c >= {}", rng.below(10)), true),
        4 => (format!("{name}.a IN ({}, {})", rng.below(5), rng.below(5)), true),
        5 => {
            // A parameter, sometimes of a type the column would convert.
            parameters.push(if rng.chance(70) {
                SqlValue::Integer(rng.below(5) as i64)
            } else {
                SqlValue::Text(rng.below(5).to_string().into_bytes())
            });
            let narrows = matches!(parameters.last(), Some(SqlValue::Integer(_)));
            (format!("{name}.a = ?{}", parameters.len()), narrows)
        }
        6 => (format!("{name}.b <> {}", b_value(rng)), false),
        7 => (format!("{name}.a IS NULL"), false),
        8 => (format!("abs({name}.c) = {}", rng.below(10)), false),
        9 => (format!("{name}.b LIKE {}", b_value(rng)), false),
        _ => (format!("{name}.c < {}", rng.below(10)), true),
    }
}

/// The joins of a generated statement: its FROM clause, the qualifiers it
/// names, and conditions the joins themselves add to the WHERE.
fn joins(
    rng: &mut Rng,
    parameters: &mut Vec<SqlValue>,
) -> (String, Vec<(&'static str, &'static str)>) {
    match rng.below(20) {
        14..=16 => (
            "t JOIN u ON u.a = t.id JOIN s ON s.id = u.b".into(),
            vec![("t", "t"), ("u", "u"), ("s", "s")],
        ),
        17..=19 => (
            "s JOIN t ON t.b = s.id JOIN u ON u.c = t.id".into(),
            vec![("s", "s"), ("t", "t"), ("u", "u")],
        ),
        12 => ("tv JOIN s ON s.id = tv.b".into(), vec![("t", "tv"), ("s", "s")]),
        13 => (
            "s JOIN tuv ON tuv.b = s.id LEFT JOIN lefty ON lefty.c = tuv.id".into(),
            vec![("s", "s"), ("t", "tuv"), ("u", "lefty")],
        ),
        10 => ("t JOIN u ON u.n = t.b".into(), vec![("t", "t"), ("u", "u")]),
        11 => ("u JOIN t ON t.b = u.n".into(), vec![("u", "u"), ("t", "t")]),
        0 => ("t JOIN u ON u.a = t.id".into(), vec![("t", "t"), ("u", "u")]),
        1 => ("u JOIN t ON t.id = u.c".into(), vec![("u", "u"), ("t", "t")]),
        2 => ("t, s WHERE s.id = t.b".into(), vec![("t", "t"), ("s", "s")]),
        3 => ("u LEFT JOIN t ON t.id = u.c".into(), vec![("u", "u"), ("t", "t")]),
        4 => (
            "s JOIN t ON t.b = s.id LEFT JOIN u ON u.a = t.id AND u.b = 'x'".into(),
            vec![("s", "s"), ("t", "t"), ("u", "u")],
        ),
        5 => {
            let ids: Vec<String> =
                (0..1 + rng.below(3)).map(|_| rng.below(10).to_string()).collect();
            parameters.push(SqlValue::Text(format!("[{}]", ids.join(",")).into_bytes()));
            (
                format!("json_each(?{}) AS w JOIN t ON t.id = w.value", parameters.len()),
                vec![("t", "t")],
            )
        }
        6 => ("t JOIN u ON u.c = t.id + 1".into(), vec![("t", "t"), ("u", "u")]),
        7 => ("t JOIN u ON t.b = u.a".into(), vec![("t", "t"), ("u", "u")]),
        8 => (
            "t, json_each(json_array(t.b, 'k1')) AS w JOIN s ON s.id = w.value".into(),
            vec![("t", "t"), ("s", "s")],
        ),
        _ => ("t a JOIN t b ON b.c = a.c".into(), vec![("t", "a"), ("t", "b")]),
    }
}

/// A subquery over a generated table named `name`, with a condition of its
/// own and, sometimes, equalities with columns of `outer`, the sources
/// around it, and conditions on those alone, which narrow nothing; `select`
/// gives what it selects. Also whether its own conditions narrow.
fn subquery(
    rng: &mut Rng,
    name: &str,
    outer: &[(&str, &str)],
    select: &str,
    parameters: &mut Vec<SqlValue>,
) -> (String, bool) {
    let table = *rng.pick(&TABLES);
    let mut conditions = Vec::new();
    let mut narrows = false;
    if rng.chance(70) {
        let (condition, narrowing) = condition(rng, table, name, parameters, 1);
        narrows = narrowing;
        conditions.push(condition);
    }
    if !outer.is_empty() && rng.chance(80) {
        let (outer_table, outer_name) = *rng.pick(outer);
        let (correlation, narrowing) = match rng.below(5) {
            0 => (format!("{name}.a = {outer_name}.a"), true),
            1 => (format!("{name}.b = {outer_name}.b"), true),
            2 if table == "s" => (format!("{name}.id = {outer_name}.b"), true),
            3 if outer_table == "s" => (format!("{name}.b = {outer_name}.id"), true),
            3 => (format!("{name}.c = {outer_name}.id + 1"), true),
            // Columns of different affinities narrow nothing.
            _ => (format!("{name}.a = {outer_name}.b"), false),
        };
        narrows |= narrowing;
        conditions.push(correlation);
    }
    if !outer.is_empty() && rng.chance(25) {
        let (outer_table, outer_name) = *rng.pick(outer);
        let (_, other_name) = *rng.pick(outer);
        conditions.push(if other_name != outer_name && rng.chance(50) {
            format!("{outer_name}.a = {other_name}.a")
        } else {
            condition(rng, outer_table, outer_name, parameters, 1).0
        });
    }
    if rng.chance(20) {
        // A subquery inside this one, which sees this one's rows too.
        let inner = format!("{name}{name}");
        let mut around = outer.to_vec();
        around.push((table, name));
        let (nested, _) = subquery(rng, &inner, &around, "1", parameters);
        conditions.push(format!("EXISTS ({nested})"));
    }
    let filter = if conditions.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", conditions.join(" AND "))
    };
    (format!("SELECT {} FROM {table} {name}{filter}", select.replace("{}", name)), narrows)
}

/// A condition holding a subquery, correlated with `sources` or not.
fn subquery_condition(
    rng: &mut Rng,
    sources: &[(&str, &str)],
    parameters: &mut Vec<SqlValue>,
) -> (String, bool) {
    let outer = if rng.chance(75) { sources } else { &[] };
    let (_, name) = *rng.pick(sources);
    match rng.below(4) {
        0 => {
            let (sql, narrows) = subquery(rng, "q", outer, "1", parameters);
            (format!("EXISTS ({sql})"), narrows)
        }
        1 => {
            let (sql, narrows) = subquery(rng, "q", outer, "1", parameters);
            (format!("NOT EXISTS ({sql})"), narrows)
        }
        2 => {
            let (sql, narrows) = subquery(rng, "q", outer, "{}.a", parameters);
            (format!("{name}.a IN ({sql})"), narrows)
        }
        _ => {
            let (sql, narrows) = subquery(rng, "q", outer, "count(*)", parameters);
            (format!("{name}.c >= ({sql})"), narrows)
        }
    }
}

fn query(rng: &mut Rng) -> Generated {
    let mut parameters = Vec::new();
    match rng.below(10) {
        0 => {
            // A compound: each arm narrows by its own condition.
            let mut arms = Vec::new();
            let mut narrows = true;
            for _ in 0..2 {
                let table = *rng.pick(&TABLES);
                let (condition, narrowing) = condition(rng, table, "x", &mut parameters, 0);
                narrows &= narrowing;
                arms.push(format!("SELECT x.id, x.a FROM {table} x WHERE {condition}"));
            }
            let operator = *rng.pick(&["UNION", "UNION ALL", "EXCEPT", "INTERSECT"]);
            return Generated {
                sql: format!("{} {operator} {} ORDER BY 1, 2", arms[0], arms[1]),
                parameters,
                narrows,
            };
        }
        1 => {
            // No FROM: only what its subqueries read.
            let (exists, narrows) = subquery(rng, "q", &[], "1", &mut parameters);
            let (count, _) = subquery(rng, "p", &[], "count(*)", &mut parameters);
            return Generated {
                sql: format!("SELECT EXISTS ({exists}), ({count})"),
                parameters,
                narrows,
            };
        }
        _ => {}
    }
    let mut with = String::new();
    let (from, sources) = match rng.below(10) {
        0..=3 => {
            let table = *rng.pick(&TABLES);
            (table.to_owned(), vec![(table, table)])
        }
        4 | 5 => {
            let (view, table) = *rng.pick(&VIEWS);
            (view.to_owned(), vec![(table, view)])
        }
        6 => {
            // A subquery in FROM, with a condition of its own.
            let table = *rng.pick(&TABLES);
            let (condition, _) = condition(rng, table, "x", &mut parameters, 1);
            (
                format!("(SELECT x.id, x.a, x.b, x.c FROM {table} x WHERE {condition}) AS d"),
                vec![(table, "d")],
            )
        }
        7 => {
            // A CTE, sometimes kept apart as SQLite reads it.
            let table = *rng.pick(&TABLES);
            let materialized = *rng.pick(&["", "MATERIALIZED "]);
            with = format!("WITH d AS {materialized}(SELECT * FROM {table}) ");
            ("d".to_owned(), vec![(table, "d")])
        }
        _ => joins(rng, &mut parameters),
    };
    let mut narrows = false;
    let mut conditions = Vec::new();
    if rng.chance(85) {
        let (table, name) = *rng.pick(&sources);
        let (condition, narrowing) = condition(rng, table, name, &mut parameters, 0);
        narrows = narrowing;
        conditions.push(condition);
    }
    if rng.chance(35) {
        let (condition, narrowing) = subquery_condition(rng, &sources, &mut parameters);
        narrows |= narrowing;
        conditions.push(condition);
    }
    // A join of the form `t, s WHERE …` brings its own WHERE.
    let joined = !from.starts_with('(') && from.contains(" WHERE ");
    let filter = match (joined, conditions.is_empty()) {
        (_, true) => String::new(),
        (true, false) => format!(" AND {}", conditions.join(" AND ")),
        (false, false) => format!(" WHERE {}", conditions.join(" AND ")),
    };
    let (_, last) = sources[sources.len() - 1];
    let (_, first) = sources[0];
    let sql = match rng.below(7) {
        0 => format!("SELECT count(*) FROM {from}{filter}"),
        1 => format!("SELECT sum({last}.c), count({first}.b) FROM {from}{filter}"),
        2 => format!(
            "SELECT {first}.a, count(*) FROM {from}{filter} GROUP BY {first}.a ORDER BY 1"
        ),
        3 => format!(
            "SELECT {first}.id, {last}.b FROM {from}{filter} ORDER BY {first}.c, 1, 2 LIMIT {}",
            1 + rng.below(3)
        ),
        4 => {
            let (sql, _) = subquery(rng, "q", &sources, "max({}.c)", &mut parameters);
            format!("SELECT {first}.id, ({sql}) FROM {from}{filter} ORDER BY 1, 2")
        }
        _ => format!(
            "SELECT {first}.id, {first}.a, {last}.id, {last}.b, {last}.c FROM {from}{filter} ORDER BY 1, 2, 3, 4, 5"
        ),
    };
    Generated { sql: format!("{with}{sql}"), parameters, narrows }
}

/// One write's SQL, and whether it goes through coven's own transaction
/// instead of the app's local write.
fn write(rng: &mut Rng) -> (String, bool) {
    let mut statements = Vec::new();
    let mut synced = false;
    for _ in 0..1 + rng.below(3) {
        // Each statement its own table, so one write can change a joined
        // table and its partners together.
        let table = *rng.pick(&TABLES);
        synced |= table == "s";
        let id = key(table, rng.below(10));
        statements.push(match rng.below(8) {
            // Text that a NOCASE comparison folds together.
            7 if table == "u" => format!(
                "UPDATE u SET n = {} WHERE id = {id}",
                rng.pick(&["NULL", "'x'", "'X'", "'k1'", "'K1'", "'k2'"])
            ),
            0 | 1 => format!(
                "INSERT OR IGNORE INTO {table}(id, a, b, c) VALUES ({id}, {}, {}, {})",
                a_value(rng),
                b_value(rng),
                rng.below(10)
            ),
            2 => format!("UPDATE {table} SET a = {} WHERE id = {id}", a_value(rng)),
            3 => format!("UPDATE {table} SET b = {}, c = c + 1 WHERE id = {id}", b_value(rng)),
            4 => format!("UPDATE {table} SET c = {} WHERE a = {}", rng.below(10), rng.below(5)),
            5 if table != "s" => {
                format!("UPDATE OR IGNORE {table} SET id = {} WHERE id = {id}", rng.below(10))
            }
            _ => format!("DELETE FROM {table} WHERE id = {id}"),
        });
    }
    // Coven's own transactions write device-local tables only here: synced
    // rows change through writes, so their merge state stays whole.
    (statements.join("; "), !synced && rng.chance(40))
}

/// Apply another device's update of one of `rows`, the synced table's
/// keys, and describe it. Whether the merge keeps its values or not, live
/// queries must follow what the table then holds.
async fn remote(db: &Database, rng: &mut Rng, rows: &[String]) -> String {
    use coven_format::value::Value;
    let id = rng.pick(rows).clone();
    let (column, value) = match rng.below(3) {
        0 => ("a", Value::Integer(rng.below(5) as i64)),
        1 => ("b", Value::Text((*rng.pick(&["x", "k1", "k2", "k3", "1"])).into())),
        _ => ("c", Value::Integer(rng.below(10) as i64)),
    };
    let described = format!("another device sets s.{column} = {value:?} where id = {id}");
    crate::tests::try_remote_update(db, "s", &id, &[(column, value)]).await.unwrap();
    described
}

type Rows = Vec<Vec<SqlValue>>;

fn read(
    sql: &crate::SqlReadContext<'_>,
    text: &str,
    parameters: &[SqlValue],
) -> rusqlite::Result<Rows> {
    sql.query(text, rusqlite::params_from_iter(parameters), |row| {
        (0..row.as_ref().column_count()).map(|index| row.get::<_, SqlValue>(index)).collect()
    })
}

async fn one_shot(db: &Database, query: &Generated) -> Rows {
    let sql = query.sql.clone();
    let parameters = query.parameters.clone();
    db.read(move |context| Ok(read(&context, &sql, &parameters)?)).await.unwrap()
}

async fn next_rows(query: &mut LiveQuery<(usize, Rows)>) -> (usize, Rows) {
    tokio::time::timeout(Duration::from_secs(5), query.next())
        .await
        .expect("query must answer")
        .expect("the live query answers")
        .unwrap()
}

/// What one seed found: commits that left a narrowing query's result alone
/// without marking it.
async fn run_seed(seed: u64) -> usize {
    let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
    let store = TestStore::new();
    let db =
        store.schema(vec![SyncedTable::new("s", RowIdentity::SharedKey)], SCHEMA).await.unwrap();
    // Rows in every table from the start, so joins, chained ones too, find
    // partners for the writes to change.
    let mut rows = Vec::new();
    for table in TABLES {
        for id in 0..6 {
            rows.push(format!(
                "INSERT INTO {table}(id, a, b, c) VALUES ({}, {}, {}, {})",
                key(table, id),
                a_value(&mut rng),
                b_value(&mut rng),
                rng.below(10)
            ));
        }
    }
    let seeded: &'static str = Box::leak(rows.join("; ").into_boxed_str());
    crate::write::tests::sql(&db, seeded).await.unwrap();
    let queries: Vec<Generated> = (0..16).map(|_| query(&mut rng)).collect();
    let mut live = Vec::new();
    for query in &queries {
        let runs = Arc::new(AtomicUsize::new(0));
        let sql = query.sql.clone();
        let parameters = query.parameters.clone();
        let mut subscription = db.subscribe(move |context| {
            let run = runs.fetch_add(1, Ordering::SeqCst);
            Ok((run, read(&context, &sql, &parameters)?))
        });
        let (_, rows) = next_rows(&mut subscription).await;
        live.push((subscription, rows));
    }
    let mut spared = 0;
    for step in 0..24 {
        let (mut sql, coven) = write(&mut rng);
        let synced: Vec<String> = db
            .read(|context| Ok(context.query("SELECT id FROM s", [], |r| r.get(0))?))
            .await
            .unwrap();
        if !synced.is_empty() && rng.chance(20) {
            // Another device's write to one of the synced rows, applied here.
            sql = remote(&db, &mut rng, &synced).await;
        } else if coven {
            let statement = sql.clone();
            db.commit_writer(move |writer| writer.batch(&statement).unwrap());
        } else {
            let leaked: &'static str = Box::leak(sql.clone().into_boxed_str());
            crate::write::tests::sql(&db, leaked).await.unwrap();
        }
        for (query, (subscription, last)) in queries.iter().zip(live.iter_mut()) {
            let fresh = one_shot(&db, query).await;
            let marked = subscription.is_marked_for_rerun();
            let case = || {
                format!(
                    "seed {seed}, step {step}: {} {:?} after `{sql}`",
                    query.sql, query.parameters
                )
            };
            if fresh != *last {
                assert!(marked, "a changed result wasn't marked: {}", case());
            }
            // A marked query sometimes stays marked through the next write,
            // which its judging then skips.
            if marked && rng.chance(75) {
                let (_, rows) = next_rows(subscription).await;
                assert_eq!(rows, fresh, "a rerun differs from a read: {}", case());
                *last = rows;
            } else if !marked && query.narrows {
                spared += 1;
            }
        }
    }
    db.close().await.unwrap();
    spared
}

#[tokio::test]
async fn live_queries_rerun_whenever_their_results_change() {
    let seeds: u64 = std::env::var("COVEN_SCOPE_SEEDS")
        .ok()
        .map_or(40, |seeds| seeds.parse().expect("COVEN_SCOPE_SEEDS is a count"));
    let mut spared = 0;
    for seed in 0..seeds {
        spared += run_seed(seed).await;
    }
    // The test would pass with every table whole; narrowing must also spare
    // the queries it can.
    assert!(spared > 0, "no narrowing query was spared a rerun");
}

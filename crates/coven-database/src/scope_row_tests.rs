use crate::scope_lookup::tests::follows;
use crate::sql_value::SqlValue;
use crate::tests::next;
use crate::{tests::TestStore, Database, DbError, Determinism, Migration, SqlFunctions};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

fn write(db: &Database, sql: &str) {
    db.commit_writer(|db| db.batch(sql).unwrap());
}

const ITEMS: &str =
    "CREATE TABLE items(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c INTEGER, value INTEGER)";

const ROWS: &str = "DELETE FROM items; INSERT INTO items VALUES
    (1,1,'x',1,0),(2,2,'k1',2,0),(3,3,'y',3,0),(4,4,'z1',-3,0),(5,5,'X',5,0)";

/// Every case of [`any_condition_on_one_rows_columns_narrows`]: the
/// condition, its bound values, a row inside it, a row outside it, an
/// update that moves the outside row in, and one that moves the inside row
/// out.
type Case = (&'static str, Vec<SqlValue>, i64, i64, &'static str, &'static str);

fn text(value: &str) -> SqlValue {
    SqlValue::Text(value.as_bytes().to_vec())
}

fn cases() -> Vec<Case> {
    vec![
        ("lower(b) = 'x'", vec![], 5, 3, "b='X'", "b='q'"),
        ("abs(c) = 3", vec![], 4, 1, "c=-3", "c=0"),
        ("b LIKE 'k%'", vec![], 2, 3, "b='K7'", "b='n'"),
        ("b GLOB '*1*'", vec![], 2, 3, "b='1'", "b='k'"),
        ("b <> 'x'", vec![], 2, 1, "b='q'", "b='x'"),
        ("NOT (a = 1)", vec![], 2, 1, "a=9", "a=1"),
        ("(a, c) > (1, 2)", vec![], 3, 1, "a=2", "a=0"),
        ("b COLLATE NOCASE = 'X'", vec![], 5, 3, "b='x'", "b='q'"),
        (
            "CASE WHEN a = ?1 THEN 0 WHEN b = ?2 THEN 1 ELSE 2 END = ?3",
            vec![SqlValue::Integer(1), text("y"), SqlValue::Integer(1)],
            3,
            2,
            "b='y'",
            "b='n'",
        ),
        (
            "CASE WHEN a = ?1 THEN 0 WHEN b = ?2 THEN 1 ELSE 2 END = ?3",
            vec![SqlValue::Integer(2), text("zz"), SqlValue::Integer(0)],
            2,
            3,
            "a=2",
            "a=0",
        ),
        (
            "CASE WHEN a = ?1 THEN 0 WHEN b = ?2 THEN 1 ELSE 2 END = ?3",
            vec![SqlValue::Integer(1), text("q"), SqlValue::Integer(2)],
            2,
            1,
            "a=7",
            "a=1",
        ),
        (
            "a + 1 > ?1 AND lower(b) <> ?2",
            vec![SqlValue::Integer(3), text("x")],
            4,
            1,
            "a=9, b='w'",
            "a=0",
        ),
        ("id <> 3 AND b <> ?1", vec![text("k1")], 4, 2, "b='n'", "b='k1'"),
    ]
}

/// A condition coven's comparison handling can't read narrows the table by
/// the rows SQLite says pass it: a write to a row outside is spared, one to
/// a row inside reruns the query, and so does one that moves a row in or
/// out. Each rerun reads what a one-shot read does.
#[tokio::test]
async fn any_condition_on_one_rows_columns_narrows() {
    let store = TestStore::new();
    let db = store.schema(vec![], ITEMS).await.unwrap();
    for (condition, parameters, inside, outside, moves_in, moves_out) in cases() {
        write(&db, ROWS);
        // The data does what the case says.
        let check = format!("SELECT id FROM items WHERE {condition} ORDER BY id");
        let bound = parameters.clone();
        let found: Vec<i64> = db
            .read(move |db| {
                Ok::<_, DbError>(
                    db.query(&check, rusqlite::params_from_iter(&bound), |r| r.get(0))?,
                )
            })
            .await
            .unwrap();
        assert!(found.contains(&inside) && !found.contains(&outside), "{condition}");
        let sql: &'static str =
            Box::leak(format!("SELECT value FROM items WHERE {condition} ORDER BY id").into());
        let writes = [
            (format!("UPDATE items SET value=value+1 WHERE id={outside}"), false),
            (format!("UPDATE items SET value=value+1 WHERE id={inside}"), true),
            (format!("UPDATE items SET {moves_in} WHERE id={outside}"), true),
            (format!("UPDATE items SET {moves_out} WHERE id={inside}"), true),
        ];
        let writes: Vec<(&str, bool)> =
            writes.iter().map(|(sql, to)| (sql.as_str(), *to)).collect();
        follows(&db, sql, parameters.clone(), &writes).await;
    }
    db.close().await.unwrap();
}

/// A row is judged as it was and as it is: a row inserted or deleted passes
/// or fails on its one image.
#[tokio::test]
async fn an_inserted_or_deleted_row_is_judged_by_its_one_image() {
    let store = TestStore::new();
    let db = store.schema(vec![], ITEMS).await.unwrap();
    write(&db, ROWS);
    follows(
        &db,
        "SELECT value FROM items WHERE lower(b) = 'x' ORDER BY id",
        vec![],
        &[
            ("INSERT INTO items VALUES (6,6,'q',6,0)", false),
            ("INSERT INTO items VALUES (7,7,'X',7,0)", true),
            ("DELETE FROM items WHERE id = 6", false),
            ("DELETE FROM items WHERE id = 7", true),
        ],
    )
    .await;
    db.close().await.unwrap();
}

/// What stays whole: a clock, a nondeterministic function, a subquery, a
/// column of another collation, a generated column, the rowid, and a
/// condition over two sources. Each reruns for a write to any row.
#[tokio::test]
async fn conditions_coven_cannot_ask_sqlite_about_keep_whole_tables() {
    let store = TestStore::new();
    let db = store
        .schema(
            vec![],
            "CREATE TABLE items(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c INTEGER, value INTEGER);
             CREATE TABLE u(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, n TEXT COLLATE NOCASE, g TEXT AS (upper(b)));
             INSERT INTO items VALUES (1,1,'x',1,0),(9,9,'q',90,0);
             INSERT INTO u VALUES (1,1,'x','x'),(9,9,'q','q')",
        )
        .await
        .unwrap();
    for sql in [
        "SELECT value FROM items WHERE random() < 0.5 AND id = 1 OR id <> 1",
        "SELECT value FROM items WHERE c > unixepoch('now')",
        "SELECT value FROM items WHERE date('now', c || ' days') > '2000-01-01'",
        "SELECT value FROM items WHERE CURRENT_TIMESTAMP > c || ''",
        "SELECT value FROM items WHERE b = (SELECT b FROM u LIMIT 1)",
        "SELECT value FROM items WHERE lower(b) IN (SELECT b FROM u)",
        "SELECT items.value FROM items JOIN u ON u.id = items.id WHERE u.n = 'x'",
        "SELECT items.value FROM items JOIN u ON u.id = items.id WHERE lower(u.g) = 'x'",
        "SELECT value FROM items WHERE rowid > 3",
        "SELECT value FROM items WHERE abs(rowid) > 3",
        "SELECT items.value FROM items JOIN u ON u.id = items.id WHERE items.a + u.a = 3",
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let mut query: crate::LiveQuery<_, DbError> = db.subscribe(move |db| {
            let run = count.fetch_add(1, Ordering::SeqCst) + 1;
            Ok((run, db.query(sql, [], |r| r.get::<_, i64>(0))?))
        });
        next(&mut query).await;
        write(&db, "UPDATE items SET value=value+1 WHERE id=9");
        assert_eq!(next(&mut query).await.0, 2, "{sql}");
    }
    db.close().await.unwrap();
}

/// The judgment agrees with SQLite's own `SELECT ... WHERE` on the table:
/// each value takes its column's type, so a TEXT column holding `'5'` is
/// equal to 5, a numeric one holding 5 to `'5'`, and a REAL one holding 3.0
/// is still real. A value no cast keeps, a blob in a TEXT column, counts as
/// a match.
#[tokio::test]
async fn the_judgment_compares_each_value_as_its_column_does() {
    let store = TestStore::new();
    let db = store
        .schema(
            vec![],
            "CREATE TABLE typed(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c REAL, value INTEGER);
             INSERT INTO typed VALUES (1,5,'5',3.0,0),(2,6,'6',4.5,0),(3,NULL,x'ff',NULL,0),(4,6,'zz',4.5,0)",
        )
        .await
        .unwrap();
    // The condition, the id of a row SQLite says passes, and the id of one it
    // says fails; and whether the failing row's image is one coven can't cast.
    for (condition, passes, fails, uncastable) in [
        ("b = 5", 1, 2, false),
        ("a = '5'", 1, 2, false),
        ("typeof(c) = 'real'", 1, 3, false),
        ("typeof(a) = 'integer'", 1, 3, false),
        ("lower(b) = 'zz'", 4, 3, true),
        ("b = 5 OR b = 6 OR b = 'zz'", 1, 3, true),
    ] {
        let sql: &'static str =
            Box::leak(format!("SELECT value FROM typed WHERE {condition} ORDER BY id").into());
        let found: Vec<i64> = db
            .read(move |db| {
                Ok::<_, DbError>(db.query(
                    &format!("SELECT id FROM typed WHERE {condition} ORDER BY id"),
                    [],
                    |r| r.get(0),
                )?)
            })
            .await
            .unwrap();
        assert!(found.contains(&passes), "{condition}: SQLite says row {passes} passes");
        assert!(uncastable || !found.contains(&fails), "{condition}: row {fails}");
        let update = |id: i64| format!("UPDATE typed SET value=value+1 WHERE id={id}");
        let (pass_write, fail_write) = (update(passes), update(fails));
        follows(
            &db,
            sql,
            vec![],
            &[
                (&pass_write, true),
                // A blob in a TEXT column can't be cast: it counts as a match.
                (&fail_write, uncastable),
            ],
        )
        .await;
    }
    db.close().await.unwrap();
}

/// A LEFT JOIN's right side keeps a condition SQLite judges from narrowing
/// it and from making the join inner: it can be true of the NULLs a missing
/// right row gives.
#[tokio::test]
async fn a_judged_condition_on_a_left_joins_right_side_keeps_it_whole() {
    let store = TestStore::new();
    let db = store
        .schema(
            vec![],
            "CREATE TABLE lt(id INTEGER PRIMARY KEY, a INTEGER, b TEXT);
             CREATE TABLE lu(id INTEGER PRIMARY KEY, c INTEGER);
             INSERT INTO lu VALUES (1,10),(2,11),(3,12),(4,13);
             INSERT INTO lt VALUES (10,1,'x'),(11,2,'y')",
        )
        .await
        .unwrap();
    follows(
        &db,
        "SELECT lu.id FROM lu LEFT JOIN lt ON lt.id = lu.c WHERE lower(lt.b) = 'x'",
        vec![],
        &[
            // The join still narrows `lt` to the rows `lu` names.
            ("INSERT INTO lt VALUES (99,7,'q')", false),
            // The condition fails for this row, and doesn't narrow.
            ("INSERT INTO lt VALUES (12,2,'q')", true),
            ("UPDATE lt SET b = 'q' WHERE id = 10", true),
            ("UPDATE lt SET b = 'x' WHERE id = 10", true),
            // The join isn't inner: a left row no `lt` row joins is read.
            ("INSERT INTO lu VALUES (9,777)", true),
        ],
    )
    .await;
    // A condition that makes the join inner narrows it too, in either order,
    // and the judged condition narrows with it.
    for sql in [
        "SELECT lu.id FROM lu LEFT JOIN lt ON lt.id = lu.c WHERE lower(lt.b) = 'x' AND lt.a = 1",
        "SELECT lu.id FROM lu LEFT JOIN lt ON lt.id = lu.c WHERE lt.a = 1 AND lower(lt.b) = 'x'",
    ] {
        write(&db, "DELETE FROM lt; INSERT INTO lt VALUES (10,1,'x'),(11,2,'y')");
        follows(
            &db,
            sql,
            vec![],
            &[
                ("INSERT INTO lt VALUES (12,2,'x')", false),
                ("INSERT INTO lt VALUES (13,1,'q')", false),
                ("UPDATE lt SET b = 'x' WHERE id = 13", true),
                ("UPDATE lt SET b = 'q' WHERE id = 13", true),
            ],
        )
        .await;
    }
    // In the join's own ON it narrows the right side.
    write(&db, "DELETE FROM lt; INSERT INTO lt VALUES (10,1,'x'),(11,2,'y')");
    follows(
        &db,
        "SELECT lu.id FROM lu LEFT JOIN lt ON lt.id = lu.c AND lower(lt.b) = 'x'",
        vec![],
        &[
            ("INSERT INTO lt VALUES (12,2,'q')", false),
            ("UPDATE lt SET a = 3 WHERE id = 11", false),
            ("UPDATE lt SET b = 'X' WHERE id = 11", true),
        ],
    )
    .await;
    db.close().await.unwrap();
}

/// A join whose partner a judged condition narrows looks the partner up
/// with the condition in its SQL, bound to the run's values: a credit on a
/// track the title picks reruns the page, one on another track doesn't, and
/// a credit moved onto or off the track does.
#[tokio::test]
async fn a_partner_narrowed_by_a_judged_condition_is_looked_up_with_it() {
    let store = TestStore::new();
    let db = store
        .schema(
            vec![],
            "CREATE TABLE tracks(id INTEGER PRIMARY KEY, title TEXT);
             CREATE TABLE credits(id INTEGER PRIMARY KEY, track INTEGER, artist TEXT);
             INSERT INTO tracks VALUES (1,'One'),(2,'Two'),(3,'one');
             INSERT INTO credits VALUES (1,1,'ann'),(2,2,'bob')",
        )
        .await
        .unwrap();
    follows(
        &db,
        "SELECT c.artist FROM tracks t JOIN credits c ON c.track = t.id WHERE lower(t.title) = ?1 ORDER BY c.id",
        vec![text("one")],
        &[
            ("INSERT INTO credits VALUES (3,2,'cy')", false),
            ("UPDATE credits SET artist = 'bobby' WHERE id = 2", false),
            ("INSERT INTO credits VALUES (4,1,'dee')", true),
            ("UPDATE credits SET track = 2 WHERE id = 4", true),
            ("UPDATE credits SET track = 3 WHERE id = 3", true),
            ("UPDATE credits SET track = 2 WHERE id = 3", true),
            // Neither title picks the track.
            ("UPDATE tracks SET title = 'two' WHERE id = 2", false),
            ("UPDATE tracks SET title = 'ONE' WHERE id = 2", true),
            ("UPDATE tracks SET title = 'Two' WHERE id = 2", true),
        ],
    )
    .await;
    db.close().await.unwrap();
}

/// An app function registered deterministic, over a column, is asked about
/// as a built-in is; one registered nondeterministic keeps the table whole.
#[tokio::test]
async fn an_app_function_narrows_only_when_deterministic() {
    for (determinism, narrows) in
        [(Determinism::Deterministic { version: 1 }, true), (Determinism::Nondeterministic, false)]
    {
        let store = TestStore::new();
        let db = store
            .builder(
                vec![],
                vec![Migration::addition(
                    1,
                    "schema",
                    "CREATE TABLE notes(id INTEGER PRIMARY KEY, title TEXT, value INTEGER);
                     INSERT INTO notes VALUES (1,'École',0),(2,'Other',0),(3,'école',0)",
                )],
            )
            .sql_functions(SqlFunctions::default().scalar("fold", 1, determinism, |arguments| {
                Ok(arguments.get::<String>(0)?.to_lowercase())
            }))
            .open()
            .await
            .unwrap();
        follows(
            &db,
            "SELECT value FROM notes WHERE fold(title) = 'école' ORDER BY id",
            vec![],
            &[
                ("UPDATE notes SET value = 1 WHERE id = 2", !narrows),
                ("UPDATE notes SET value = 1 WHERE id = 1", true),
                ("UPDATE notes SET title = 'ÉCOLE' WHERE id = 2", true),
                ("UPDATE notes SET title = 'Autre' WHERE id = 2", true),
                ("UPDATE notes SET value = 2 WHERE id = 2", !narrows),
            ],
        )
        .await;
        db.close().await.unwrap();
    }
}

/// The median time and judging steps a row of `writes` writes of `rows`
/// new tracks each, the first key being `from`.
async fn spend(
    db: &Database,
    counter: &crate::test_utils::WorkCounter,
    from: usize,
    writes: usize,
    rows: usize,
) -> (std::time::Duration, f64) {
    let mut times = Vec::new();
    let mut steps = Vec::new();
    for run in 0..writes {
        let first = from + run * rows;
        let sql = format!(
            "WITH RECURSIVE n(i) AS (VALUES({first}) UNION ALL SELECT i+1 FROM n WHERE i<{}) \
             INSERT INTO tracks SELECT i, 'Title ' || i, i % 12, '' FROM n",
            first + rows - 1
        );
        counter.take();
        let started = std::time::Instant::now();
        write(db, &sql);
        times.push(started.elapsed());
        steps.push(counter.take().lookup_steps);
    }
    times.sort();
    steps.sort();
    (times[times.len() / 2] / rows as u32, steps[steps.len() / 2] as f64 / rows as f64)
}

/// What a write pays: the steps and time per row of a write of 270 rows
/// with four queries open, each holding a judged condition on the written
/// table, against the same write with none open. Run explicitly, in the
/// release profile.
#[tokio::test]
#[ignore = "one timing measurement, run explicitly"]
async fn writes_pay_for_the_judged_conditions_they_meet() {
    const ROWS: usize = 270;
    const WRITES: usize = 200;
    let store = TestStore::new();
    let db = store
        .schema(
            vec![],
            "CREATE TABLE tracks(id INTEGER PRIMARY KEY, title TEXT, position INTEGER, note TEXT)",
        )
        .await
        .unwrap();
    let counter = crate::test_utils::WorkCounter::new();
    db.test_track_work(counter.clone()).await.unwrap();
    let (alone, _) = spend(&db, &counter, 1, WRITES, ROWS).await;
    let mut queries = Vec::new();
    for sql in [
        "SELECT note FROM tracks WHERE lower(title) = 'title 5' ORDER BY id",
        "SELECT note FROM tracks WHERE title LIKE 'x%' ORDER BY id",
        "SELECT note FROM tracks WHERE position <> 3 AND id > 1000000 ORDER BY id",
        "SELECT note FROM tracks WHERE CASE WHEN position = 2 THEN 0 ELSE 1 END = 0 AND title GLOB 'zz*' ORDER BY id",
    ] {
        let mut query: crate::LiveQuery<_, DbError> =
            db.subscribe(move |db| Ok(db.query(sql, [], |r| r.get::<_, String>(0))?));
        next(&mut query).await;
        queries.push(query);
    }
    let (beside, steps) = spend(&db, &counter, 1 + WRITES * ROWS, WRITES, ROWS).await;
    for (metric, time) in
        [("judged_write/no_live_query", alone), ("judged_write/four_queries_open", beside)]
    {
        crate::test_utils::report_time(metric, time);
    }
    eprintln!(
        "a write of {ROWS} rows: {alone:?} a row with no live query, {beside:?} a row with four \
         open, {steps:.2} judging steps a row"
    );
    drop(queries);
    db.close().await.unwrap();
}

/// An error SQLite returns for the question about a changed row counts as a
/// match: a write that stores malformed JSON, or takes `abs` of the least
/// integer, still commits while a live query holds a condition on it, and
/// the query is marked to run again.
#[tokio::test]
async fn an_error_in_the_question_counts_as_a_match() {
    for (condition, bad) in [
        ("json_extract(data, '$.k') = 'x'", "UPDATE docs SET data = '{broken' WHERE id = 2"),
        ("abs(n) = 3", "UPDATE docs SET n = -9223372036854775808 WHERE id = 2"),
    ] {
        let store = TestStore::new();
        let db = store
            .schema(
                vec![],
                "CREATE TABLE docs(id INTEGER PRIMARY KEY, data TEXT, n INTEGER, value INTEGER);
                 INSERT INTO docs VALUES (1,'{\"k\":\"x\"}',3,0),(2,'{\"k\":\"y\"}',4,0)",
            )
            .await
            .unwrap();
        let sql = format!("SELECT value FROM docs WHERE {condition}");
        let mut query: crate::LiveQuery<_, DbError> =
            db.subscribe(move |db| Ok(db.query(&sql, [], |r| r.get::<_, i64>(0))?));
        next(&mut query).await;
        write(&db, bad);
        assert!(query.is_marked_for_rerun(), "{condition}");
        db.close().await.unwrap();
    }
}

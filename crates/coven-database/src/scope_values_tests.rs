//! A joined table's scope: the values its join columns matched in each run.

use crate::tests::TestStore;
use crate::{Database, LiveQuery};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

async fn next<T: Clone + PartialEq + Send + 'static>(query: &mut LiveQuery<T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), query.next())
        .await
        .expect("query must answer")
        .expect("the live query answers")
        .unwrap()
}

fn write(db: &Database, sql: &str) {
    db.commit_writer(|db| db.batch(sql).unwrap());
}

const MUSIC: &str = "CREATE TABLE releases(id TEXT NOT NULL PRIMARY KEY, album TEXT, year INTEGER);
CREATE TABLE tracks(id INTEGER PRIMARY KEY, release TEXT, position INTEGER, title TEXT);
CREATE VIEW dated AS SELECT id, album FROM releases WHERE year IS NOT NULL;
INSERT INTO releases VALUES ('r1','x',2001),('r2','x',2002),('r3','y',2003);
INSERT INTO tracks VALUES (1,'r1',1,'one'),(2,'r1',2,'two'),(3,'r2',1,'three'),(4,'r3',1,'four')";

/// Run `sql` live, then each write: `false` must leave it unmarked, `true`
/// must rerun it, and the rerun must read what a one-shot read does.
async fn follows(
    db: &Database,
    sql: &'static str,
    parameters: Vec<crate::sql_value::SqlValue>,
    writes: &[(&str, bool)],
) {
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let bound = parameters.clone();
    let mut query = db.subscribe(move |db| {
        let run = count.fetch_add(1, Ordering::SeqCst);
        let rows = db.query(sql, rusqlite::params_from_iter(&bound), |r| {
            r.get::<_, crate::sql_value::SqlValue>(0)
        })?;
        Ok((run, rows))
    });
    let mut runs = next(&mut query).await.0;
    for (write_sql, reruns) in writes {
        write(db, write_sql);
        if *reruns {
            let (run, rows) = next(&mut query).await;
            assert!(run > runs, "{sql} after {write_sql}");
            runs = run;
            let parameters = parameters.clone();
            let fresh = db
                .read(move |db| {
                    Ok(db.query(sql, rusqlite::params_from_iter(&parameters), |r| {
                        r.get::<_, crate::sql_value::SqlValue>(0)
                    })?)
                })
                .await
                .unwrap();
            assert_eq!(rows, fresh, "{sql} after {write_sql}");
        } else {
            assert!(!query.is_marked_for_rerun(), "{sql} after {write_sql}");
        }
    }
}

/// A joined table depends on the rows whose join column holds a value its
/// partner's rows in their own scope hold, found anew each run: tracks
/// joining another album's releases wake nothing; a new release of this
/// album, with no tracks yet, does, and then so do its tracks.
#[tokio::test]
async fn an_inner_join_narrows_by_the_values_its_partner_matched() {
    let store = TestStore::new();
    let db = store.schema(vec![], MUSIC).await.unwrap();
    for sql in [
        "SELECT t.title FROM tracks t JOIN releases r ON t.release = r.id WHERE r.album = 'x' ORDER BY t.id",
        "SELECT t.title FROM releases r, tracks t WHERE r.id = t.release AND r.album = 'x' ORDER BY t.id",
        "SELECT t.title FROM tracks t CROSS JOIN releases r ON r.id = t.release WHERE r.album = 'x' ORDER BY t.id",
        "SELECT group_concat(t.title) FROM tracks t JOIN releases r ON t.release = r.id WHERE r.album = 'x'",
    ] {
        follows(&db, sql, vec![], &[
            ("UPDATE tracks SET title = title || '.' WHERE id = 4", false),
            ("UPDATE releases SET year = 1999 WHERE id = 'r3'", false),
            ("INSERT INTO tracks VALUES (5,'r3',2,'five')", false),
            ("UPDATE tracks SET title = title || '.' WHERE id = 3", true),
            ("INSERT INTO releases VALUES ('r4','x',2004)", true),
            ("INSERT INTO tracks VALUES (6,'r4',1,'six')", true),
            ("UPDATE tracks SET release = 'r3' WHERE id = 6", true),
            ("UPDATE releases SET album = 'x' WHERE id = 'r3'", true),
            ("UPDATE releases SET album = 'z' WHERE id = 'r3'; UPDATE releases SET album = 'z' WHERE id = 'r4'", true),
            ("DELETE FROM tracks WHERE id IN (5, 6)", false),
        ]).await;
        write(&db, "DELETE FROM releases WHERE id = 'r4'; UPDATE releases SET album = 'y' WHERE id = 'r3'; UPDATE tracks SET title = 'three' WHERE id = 3; UPDATE tracks SET title = 'four' WHERE id = 4");
    }
    db.close().await.unwrap();
}

/// A condition on the child side narrows the parent through the same
/// equality, the other way round.
#[tokio::test]
async fn a_join_narrows_toward_either_side() {
    let store = TestStore::new();
    let db = store.schema(vec![], MUSIC).await.unwrap();
    follows(
        &db,
        "SELECT r.year FROM releases r JOIN tracks t ON t.release = r.id WHERE t.id = 3",
        vec![],
        &[
            ("UPDATE releases SET year = 1 WHERE id = 'r1'", false),
            ("UPDATE releases SET year = 2 WHERE id = 'r2'", true),
            ("UPDATE tracks SET release = 'r1' WHERE id = 3", true),
            ("UPDATE releases SET year = 3 WHERE id = 'r2'", false),
            ("UPDATE releases SET year = 4 WHERE id = 'r1'", true),
        ],
    )
    .await;
    db.close().await.unwrap();
}

/// A LEFT JOIN's right side is narrowed by its ON alone, by the values its
/// left side matched: a release with no tracks gaining one reruns the query.
#[tokio::test]
async fn a_left_join_narrows_its_right_side_by_its_on() {
    let store = TestStore::new();
    let db = store.schema(vec![], MUSIC).await.unwrap();
    write(&db, "INSERT INTO releases VALUES ('r5','x',2005)");
    follows(&db, "SELECT r.id || ':' || coalesce(t.title, '-') FROM releases r LEFT JOIN tracks t ON t.release = r.id AND t.position = 1 WHERE r.album = 'x' ORDER BY r.id", vec![], &[
        ("UPDATE tracks SET title = 'quatre' WHERE id = 4", false),
        ("UPDATE tracks SET title = 'deux' WHERE id = 2", false),
        ("INSERT INTO tracks VALUES (7,'r5',1,'seven')", true),
        ("UPDATE tracks SET position = 2 WHERE id = 7", true),
        ("INSERT INTO tracks VALUES (8,'r3',1,'eight')", false),
    ]).await;
    db.close().await.unwrap();
}

/// Two sources over one table: the table depends on rows either one does.
#[tokio::test]
async fn a_self_join_depends_on_both_sides_rows() {
    let store = TestStore::new();
    let db = store.schema(vec![], MUSIC).await.unwrap();
    follows(&db, "SELECT b.title FROM tracks a JOIN tracks b ON b.release = a.release WHERE a.id = 1 ORDER BY b.id", vec![], &[
        ("UPDATE tracks SET title = '.' WHERE id = 4", false),
        ("UPDATE tracks SET title = 'zwei' WHERE id = 2", true),
        ("UPDATE tracks SET release = 'r2' WHERE id = 1", true),
        ("UPDATE tracks SET title = 'drei' WHERE id = 3", true),
        ("UPDATE tracks SET title = 'zwei.' WHERE id = 2", false),
    ]).await;
    db.close().await.unwrap();
}

/// Rows a table function lists, a view's rows, or a bound parameter's JSON
/// give a joined table its values too.
#[tokio::test]
async fn table_functions_and_views_give_joined_tables_values() {
    let store = TestStore::new();
    let db = store.schema(vec![], MUSIC).await.unwrap();
    follows(
        &db,
        "SELECT t.title FROM json_each(?1) AS w JOIN tracks t ON t.id = w.value ORDER BY t.id",
        vec![crate::sql_value::SqlValue::Text(b"[1,3]".to_vec())],
        &[
            ("UPDATE tracks SET title = '.' WHERE id = 2", false),
            ("UPDATE tracks SET title = 'tres' WHERE id = 3", true),
        ],
    )
    .await;
    follows(
        &db,
        "SELECT t.title FROM tracks t JOIN dated d ON t.release = d.id WHERE d.album = 'y'",
        vec![],
        &[
            ("INSERT INTO tracks VALUES (9,'zz',1,'nine')", false),
            ("UPDATE tracks SET title = 'cuatro' WHERE id = 4", true),
            ("UPDATE releases SET year = 2010 WHERE id = 'r1'", false),
            ("UPDATE releases SET year = NULL WHERE id = 'r3'", true),
        ],
    )
    .await;
    db.close().await.unwrap();
}

/// Join columns of different affinities, or a partner holding more values
/// than the bound, keep the joined table whole.
#[tokio::test]
async fn mismatched_affinities_and_many_values_keep_tables_whole() {
    let store = TestStore::new();
    let db = store.schema(vec![], "CREATE TABLE parents(id INTEGER PRIMARY KEY, label TEXT, kind TEXT); CREATE TABLE children(id INTEGER PRIMARY KEY, parent INTEGER, label TEXT, note TEXT)").await.unwrap();
    let rows = crate::scope_values::MATCHED_VALUES + 1;
    write(&db, &format!("WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<{rows}) INSERT INTO parents SELECT i, 'p' || i, 'many' FROM n; INSERT INTO parents VALUES (0, '0', 'one'); INSERT INTO children VALUES (1, 1, '1', ''), (2, 0, '0', '')"));
    follows(
        &db,
        "SELECT c.note FROM parents p JOIN children c ON c.parent = p.id WHERE p.kind = 'one'",
        vec![],
        &[("UPDATE children SET note = '.' WHERE id = 1", false)],
    )
    .await;
    follows(
        &db,
        "SELECT c.note FROM parents p JOIN children c ON c.parent = p.id WHERE p.kind = 'many'",
        vec![],
        &[("UPDATE children SET note = '..' WHERE id = 2", true)],
    )
    .await;
    follows(
        &db,
        "SELECT c.note FROM parents p JOIN children c ON c.label = p.id WHERE p.kind = 'one'",
        vec![],
        &[("UPDATE children SET note = '...' WHERE id = 1", true)],
    )
    .await;
    db.close().await.unwrap();
}

/// What finding a joined query's values adds to each of its runs, against
/// the same SQL read once, which finds none: an album page's tracks with
/// their credited artists, over 10,000 releases of 12 tracks. Run
/// explicitly, in the release profile.
#[tokio::test]
#[ignore = "one timing measurement, run explicitly"]
async fn joined_runs_against_one_shot_reads() {
    let store = TestStore::new();
    let db = store.schema(vec![], "CREATE TABLE releases(id INTEGER PRIMARY KEY, album INTEGER);
        CREATE TABLE tracks(id INTEGER PRIMARY KEY, release INTEGER, position INTEGER, title TEXT);
        CREATE INDEX tracks_release ON tracks(release);
        CREATE TABLE track_artists(track INTEGER, position INTEGER, artist INTEGER, PRIMARY KEY(track, position));
        CREATE TABLE artists(id INTEGER PRIMARY KEY, name TEXT);").await.unwrap();
    write(&db, "WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<10000) INSERT INTO releases SELECT i, i FROM n;
        WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<120000) INSERT INTO tracks SELECT i, (i-1)/12+1, (i-1)%12, 'track ' || i FROM n;
        INSERT INTO track_artists SELECT id, 0, id % 500 FROM tracks;
        WITH RECURSIVE n(i) AS (VALUES(0) UNION ALL SELECT i+1 FROM n WHERE i<499) INSERT INTO artists SELECT i, 'artist ' || i FROM n;");
    const SQL: &str = "SELECT t.title, a.name FROM tracks t LEFT JOIN track_artists c ON c.track = t.id LEFT JOIN artists a ON a.id = c.artist WHERE t.release = ?1 ORDER BY t.position, c.position";
    const RUNS: u32 = 200;
    let mut one_shot = Duration::ZERO;
    for _ in 0..RUNS {
        one_shot += db
            .read(|sql| {
                let started = std::time::Instant::now();
                sql.query(SQL, [5000], |r| r.get::<_, String>(0))?;
                Ok(started.elapsed())
            })
            .await
            .unwrap();
    }
    let one_shot = one_shot / RUNS;
    let spent = Arc::new(std::sync::Mutex::new(Vec::new()));
    let times = spent.clone();
    let mut live = db.subscribe(move |sql| {
        let started = std::time::Instant::now();
        let rows = sql.query(SQL, [5000], |r| r.get::<_, String>(0))?;
        times.lock().unwrap().push(started.elapsed());
        Ok(rows)
    });
    next(&mut live).await;
    for run in 0..RUNS {
        write(
            &db,
            &format!("UPDATE tracks SET title = 'run {run}' WHERE id = 59990"),
        );
        next(&mut live).await;
        assert!(!live.is_marked_for_rerun());
    }
    write(&db, "UPDATE tracks SET title = 'outside' WHERE id = 1");
    assert!(!live.is_marked_for_rerun());
    let spent = spent.lock().unwrap();
    let live_run = spent.iter().skip(1).sum::<Duration>() / RUNS;
    eprintln!(
        "joined query: {one_shot:?} a one-shot read, {live_run:?} a live run with its values"
    );
}

/// A table function whose arguments name another source's columns lists
/// rows only beside that source's rows: it gives no values of its own.
#[tokio::test]
async fn a_table_function_of_another_sources_columns_gives_no_values() {
    let store = TestStore::new();
    let db = store.schema(vec![], "CREATE TABLE lists(id INTEGER PRIMARY KEY, items TEXT); CREATE TABLE things(id INTEGER PRIMARY KEY, name TEXT); INSERT INTO lists VALUES (1,'[1,2]'),(2,'[3]'); INSERT INTO things VALUES (1,'one'),(2,'two'),(3,'three')").await.unwrap();
    follows(
        &db,
        "SELECT t.name FROM lists l, json_each(l.items) AS j JOIN things t ON t.id = j.value WHERE l.id = 1 ORDER BY t.id",
        vec![],
        &[
            ("UPDATE lists SET items = '[1]' WHERE id = 2", false),
            ("UPDATE things SET name = 'trois' WHERE id = 3", true),
            ("UPDATE lists SET items = '[3]' WHERE id = 1", true),
        ],
    )
    .await;
    db.close().await.unwrap();
}

/// A correlated subquery's tables are narrowed by its own conditions and
/// by the values the core around it matched, whatever the subquery's
/// result decides: EXISTS and NOT EXISTS, IN and a scalar alike.
#[tokio::test]
async fn a_correlated_subquery_narrows_by_the_values_around_it() {
    let store = TestStore::new();
    let db = store.schema(vec![], MUSIC).await.unwrap();
    for sql in [
        "SELECT r.id FROM releases r WHERE r.album = 'x' AND EXISTS (SELECT 1 FROM tracks t WHERE t.release = r.id AND t.position = 2) ORDER BY r.id",
        "SELECT r.id FROM releases r WHERE r.album = 'x' AND NOT EXISTS (SELECT 1 FROM tracks t WHERE t.release = r.id AND t.position = 2) ORDER BY r.id",
        "SELECT r.id FROM releases r WHERE r.album = 'x' AND 2 IN (SELECT t.position FROM tracks t WHERE t.release = r.id) ORDER BY r.id",
        "SELECT r.id || ':' || (SELECT count(*) FROM tracks t WHERE t.release = r.id AND t.position = 2) FROM releases r WHERE r.album = 'x' ORDER BY r.id",
        "SELECT r.id FROM releases r WHERE r.album = 'x' ORDER BY (SELECT t.title FROM tracks t WHERE t.release = r.id AND t.position = 2), r.id",
    ] {
        follows(&db, sql, vec![], &[
            ("UPDATE tracks SET position = 2 WHERE id = 4", false),
            ("INSERT INTO tracks VALUES (5,'r3',2,'five')", false),
            ("UPDATE tracks SET position = 1 WHERE id = 2", true),
            ("INSERT INTO tracks VALUES (6,'r2',2,'six')", true),
            ("UPDATE releases SET album = 'x' WHERE id = 'r3'", true),
            ("UPDATE tracks SET position = 3 WHERE id = 5", true),
            ("INSERT INTO releases VALUES ('r7','z',2007)", false),
        ]).await;
        write(&db, "DELETE FROM tracks WHERE id > 4; UPDATE tracks SET position = 2 WHERE id = 2; UPDATE tracks SET position = 1 WHERE id = 4; UPDATE releases SET album = 'y' WHERE id = 'r3'; DELETE FROM releases WHERE id = 'r7'");
    }
    db.close().await.unwrap();
}

/// A subquery with no names from around it is narrowed by its own
/// conditions alone, and narrows nothing around it: the releases whose id
/// a track's subquery gives depend on every release.
#[tokio::test]
async fn an_uncorrelated_subquery_narrows_itself_alone() {
    let store = TestStore::new();
    let db = store.schema(vec![], MUSIC).await.unwrap();
    for sql in [
        "SELECT r.year FROM releases r WHERE r.id IN (SELECT t.release FROM tracks t WHERE t.id = 3)",
        "SELECT r.year FROM releases r WHERE r.id = (SELECT t.release FROM tracks t WHERE t.id = 3)",
    ] {
        follows(&db, sql, vec![], &[
            ("UPDATE tracks SET release = 'r9' WHERE id = 4", false),
            ("UPDATE releases SET year = year + 1 WHERE id = 'r1'", true),
            ("UPDATE tracks SET release = 'r1' WHERE id = 3", true),
        ]).await;
        write(&db, "UPDATE tracks SET release = 'r2' WHERE id = 3; UPDATE tracks SET release = 'r3' WHERE id = 4");
    }
    db.close().await.unwrap();
}

/// A SELECT with no FROM depends on what its subqueries read, and each arm
/// of a compound on its own rows.
#[tokio::test]
async fn from_less_selects_and_compound_arms_narrow_each_core() {
    let store = TestStore::new();
    let db = store.schema(vec![], MUSIC).await.unwrap();
    follows(
        &db,
        "SELECT EXISTS (SELECT 1 FROM tracks WHERE release = ?1 AND position = 2)",
        vec![crate::sql_value::SqlValue::Text(b"r2".to_vec())],
        &[
            ("UPDATE tracks SET position = 2 WHERE id = 4", false),
            ("INSERT INTO tracks VALUES (5,'r2',2,'five')", true),
        ],
    )
    .await;
    follows(
        &db,
        "SELECT title FROM tracks WHERE release = 'r1' UNION SELECT title FROM tracks WHERE id = 4 ORDER BY 1",
        vec![],
        &[
            ("UPDATE tracks SET title = 'drei' WHERE id = 3", false),
            ("UPDATE tracks SET title = 'vier' WHERE id = 4", true),
            ("UPDATE tracks SET title = 'zwei' WHERE id = 2", true),
        ],
    )
    .await;
    db.close().await.unwrap();
}

/// A name a subquery doesn't hold resolves as SQLite resolves it: to the
/// nearest core around it with that column, or to a result column's name
/// there, which narrows nothing. Here the innermost `album` is the middle
/// core's `t.title`, not the outer release's album.
#[tokio::test]
async fn a_subquerys_names_resolve_to_the_nearest_core() {
    let store = TestStore::new();
    let db = store.schema(vec![], MUSIC).await.unwrap();
    follows(
        &db,
        "SELECT r.id FROM releases r WHERE r.id = 'r1' AND EXISTS (SELECT t.title AS album FROM tracks t WHERE t.release = r.id AND EXISTS (SELECT 1 FROM tracks o WHERE o.title = album AND o.id <> t.id))",
        vec![],
        &[
            ("UPDATE tracks SET title = 'one' WHERE id = 4", true),
            ("UPDATE releases SET id = 'r3b' WHERE id = 'r3'", false),
        ],
    )
    .await;
    db.close().await.unwrap();
}

/// Values compare as SQLite compares them: a join whose left side is a
/// column of another collation compares by that one, and the values
/// coven reads stay apart by their bytes.
#[tokio::test]
async fn joins_on_columns_of_other_collations_compare_as_sqlite_does() {
    let store = TestStore::new();
    let db = store.schema(vec![], "CREATE TABLE folded(id INTEGER PRIMARY KEY, name TEXT COLLATE NOCASE); CREATE TABLE named(name TEXT NOT NULL PRIMARY KEY, note TEXT); INSERT INTO folded VALUES (1, 'A'), (2, 'a'); INSERT INTO named VALUES ('a', '')").await.unwrap();
    for sql in [
        "SELECT n.note FROM folded f JOIN named n ON f.name = n.name WHERE f.id = 1",
        "SELECT n.note FROM folded f JOIN named n ON n.name = f.name ORDER BY f.id",
    ] {
        follows(
            &db,
            sql,
            vec![],
            &[("UPDATE named SET note = note || '.' WHERE name = 'a'", true)],
        )
        .await;
    }
    db.close().await.unwrap();
}

/// What SQLite counts stepping `sql` once, bound to `parameters`, on a
/// connection of its own.
fn steps(plain: &rusqlite::Connection, sql: &str, parameters: &[&str]) -> usize {
    let mut statement = plain.prepare(sql).unwrap();
    let mut rows = statement
        .query(rusqlite::params_from_iter(parameters))
        .unwrap();
    while rows.next().unwrap().is_some() {}
    drop(rows);
    statement.get_status(rusqlite::StatementStatus::VmStep) as usize
}

const ALBUM_X: &str =
    "SELECT t.title FROM tracks t JOIN releases r ON t.release = r.id WHERE r.album = 'x' ORDER BY t.id";

/// A live query's run counts the steps SQLite runs: its own statement's,
/// and those of coven's read of the release ids album x's releases hold,
/// which narrow `tracks` to theirs.
#[tokio::test]
async fn a_live_querys_run_counts_its_statements_and_its_narrowing_steps() {
    let store = TestStore::new();
    let db = store.schema(vec![], MUSIC).await.unwrap();
    let counter = crate::test_utils::WorkCounter::default();
    db.test_track_work(counter.clone()).await.unwrap();
    let mut query = db.subscribe(|sql| Ok(sql.query(ALBUM_X, [], |r| r.get::<_, String>(0))?));
    assert_eq!(next(&mut query).await, ["one", "two", "three"]);
    let work = counter.take();
    let plain = rusqlite::Connection::open_with_flags(
        store.database_path(),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    assert_eq!(work.app_read_steps, steps(&plain, ALBUM_X, &[]));
    let narrowing = steps(
        &plain,
        r#"SELECT DISTINCT (r."id") COLLATE BINARY FROM main."releases" AS r WHERE r."album" = ?1 LIMIT 1001"#,
        &["x"],
    );
    assert!(narrowing > 0);
    assert_eq!(work.narrowing_steps, narrowing);
    assert_eq!(work.read_steps(), work.app_read_steps + narrowing);
    drop(query);
    db.close().await.unwrap();
}

/// A one-shot read records nothing of what it read, since nothing runs it
/// again, so it reads none of the values its joins would narrow by: its
/// steps are its statement's alone. The same statement run live narrows.
#[tokio::test]
async fn a_one_shot_read_runs_no_narrowing_reads() {
    let store = TestStore::new();
    let db = store.schema(vec![], MUSIC).await.unwrap();
    let counter = crate::test_utils::WorkCounter::default();
    db.test_track_work(counter.clone()).await.unwrap();
    let titles = db
        .read(|sql| Ok(sql.query(ALBUM_X, [], |r| r.get::<_, String>(0))?))
        .await
        .unwrap();
    assert_eq!(titles, ["one", "two", "three"]);
    let once = counter.take();
    assert!(once.app_read_steps > 0);
    assert_eq!(once.narrowing_steps, 0);
    let mut query = db.subscribe(|sql| Ok(sql.query(ALBUM_X, [], |r| r.get::<_, String>(0))?));
    assert_eq!(next(&mut query).await, titles);
    let live = counter.take();
    assert_eq!(live.app_read_steps, once.app_read_steps);
    assert!(live.narrowing_steps > 0);
    drop(query);
    db.close().await.unwrap();
}

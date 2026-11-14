use crate::scope_lookup::tests::follows;
use crate::tests::next;
use crate::{tests::TestStore, Database, DbError};
use coven_foundation::clock::HANG_GUARD;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

fn write(db: &Database, sql: &str) {
    db.commit_writer(|db| db.batch(sql).unwrap());
}

#[tokio::test]
async fn each_predicate_ignores_outside_keys_and_observes_inside_keys() {
    let store = TestStore::new();
    let db = store.schema(vec![], "CREATE TABLE items(id INTEGER PRIMARY KEY, value INTEGER, extra TEXT); INSERT INTO items VALUES (1,0,''),(2,0,''),(3,0,''),(4,0,''),(5,0,''),(6,0,''),(7,0,'')").await.unwrap();
    for (predicate, expected, outside, inside) in [
        ("id = 3", vec![3], 1, 3),
        ("3 = id", vec![3], 1, 3),
        ("id < 3", vec![1, 2], 3, 2),
        ("3 > id", vec![1, 2], 3, 2),
        ("id <= 3", vec![1, 2, 3], 4, 3),
        ("3 >= id", vec![1, 2, 3], 4, 3),
        ("id > 3", vec![4, 5, 6, 7], 3, 4),
        ("3 < id", vec![4, 5, 6, 7], 3, 4),
        ("id >= 3", vec![3, 4, 5, 6, 7], 2, 3),
        ("3 <= id", vec![3, 4, 5, 6, 7], 2, 3),
        ("id IN (2,4,6)", vec![2, 4, 6], 3, 4),
        ("id > 2 AND id < 5", vec![3, 4], 5, 3),
        ("id = 1 OR id >= 6", vec![1, 6, 7], 4, 6),
        ("((id=1 OR id=3) AND (id>=2))", vec![3], 1, 3),
        ("\"i\".\"id\" = 3", vec![3], 1, 3),
        ("main.i.id = 3", vec![3], 1, 3),
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let sql = format!("SELECT id,value FROM main.items AS i WHERE {predicate} ORDER BY id");
        let mut query: crate::LiveQuery<_, DbError> = db.subscribe(move |db| {
            count.fetch_add(1, Ordering::SeqCst);
            Ok(db.query(&sql, [], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)))?)
        });
        assert_eq!(
            next(&mut query).await.iter().map(|r| r.0).collect::<Vec<_>>(),
            expected,
            "{predicate}"
        );
        write(&db, &format!("UPDATE items SET value=value+1 WHERE id={outside}"));
        assert!(!query.is_marked_for_rerun(), "outside: {predicate}");
        assert_eq!(calls.load(Ordering::SeqCst), 1, "outside: {predicate}");
        write(&db, &format!("UPDATE items SET extra='ignored' WHERE id={inside}"));
        assert!(!query.is_marked_for_rerun(), "column: {predicate}");
        assert_eq!(calls.load(Ordering::SeqCst), 1, "column: {predicate}");
        write(&db, &format!("UPDATE items SET value=value+1 WHERE id={inside}"));
        assert_eq!(next(&mut query).await.iter().map(|r| r.0).collect::<Vec<_>>(), expected);
        assert_eq!(calls.load(Ordering::SeqCst), 2, "inside: {predicate}");
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn inserts_and_old_and_new_keys_can_enter_or_leave_an_empty_range() {
    let store = TestStore::new();
    let db = store
        .schema(vec![], "CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)")
        .await
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let mut query: crate::LiveQuery<_, DbError> = db.subscribe(move |db| {
        count.fetch_add(1, Ordering::SeqCst);
        Ok(db.query("SELECT id,value FROM items WHERE id>=?1 AND id<?2", [2, 5], |r| {
            Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?))
        })?)
    });
    assert!(next(&mut query).await.is_empty());
    write(&db, "INSERT INTO items VALUES (9,'x')");
    assert!(!query.is_marked_for_rerun());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    write(&db, "INSERT INTO items VALUES (3,'three')");
    assert_eq!(next(&mut query).await, [(3, "three".into())]);
    write(&db, "UPDATE items SET id=8 WHERE id=3");
    assert!(next(&mut query).await.is_empty());
    write(&db, "UPDATE items SET id=2 WHERE id=9");
    assert_eq!(next(&mut query).await, [(2, "x".into())]);
    write(&db, "UPDATE items SET id=3 WHERE id=2");
    assert_eq!(next(&mut query).await, [(3, "x".into())]);
    assert_eq!(calls.load(Ordering::SeqCst), 5);
    write(&db, "DELETE FROM items WHERE id=8");
    assert!(!query.is_marked_for_rerun());
    assert_eq!(calls.load(Ordering::SeqCst), 5);
    write(&db, "DELETE FROM items WHERE id=3");
    assert!(next(&mut query).await.is_empty());
    db.close().await.unwrap();
}

#[tokio::test]
async fn text_blob_and_composite_keys_use_bound_values_and_binary_order() {
    let store = TestStore::new();
    let db = store.schema(vec![], "CREATE TABLE items(tenant TEXT, id BLOB, value INTEGER, PRIMARY KEY(tenant,id)) WITHOUT ROWID; INSERT INTO items VALUES ('a',x'0100',0),('b',x'0100',0),('a',x'0200',0),('a',x'0300',0)").await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let mut query: crate::LiveQuery<_, DbError> = db.subscribe(move |db| {
        count.fetch_add(1, Ordering::SeqCst);
        Ok(db.query("SELECT value FROM items WHERE tenant=:tenant AND id IN (:first,:second) ORDER BY id", crate::named_params!{":tenant": "a", ":first": [1_u8,0].as_slice(), ":second": [2_u8,0].as_slice()}, |r| r.get::<_, i64>(0))?)
    });
    assert_eq!(next(&mut query).await, [0, 0]);
    write(&db, "UPDATE items SET value=1 WHERE tenant='b' OR id=x'0300'");
    assert!(!query.is_marked_for_rerun());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    write(&db, "UPDATE items SET value=2 WHERE tenant='a' AND id=x'0200'");
    assert_eq!(next(&mut query).await, [0, 2]);
    write(&db, "UPDATE items SET tenant='c' WHERE tenant='a' AND id=x'0100'");
    assert_eq!(next(&mut query).await, [2]);
    write(&db, "UPDATE items SET id=x'0001' WHERE tenant='a' AND id=x'0200'");
    assert!(next(&mut query).await.is_empty());
    db.close().await.unwrap();
}

#[tokio::test]
async fn separate_statements_keep_their_own_columns_and_ranges() {
    let store = TestStore::new();
    let db = store.schema(vec![], "CREATE TABLE items(id TEXT NOT NULL PRIMARY KEY, title TEXT, body TEXT); INSERT INTO items VALUES ('a','a','a'),('b','b','b')").await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let mut query: crate::LiveQuery<_, DbError> = db.subscribe(move |db| {
        count.fetch_add(1, Ordering::SeqCst);
        Ok((
            db.query_row("SELECT title FROM items WHERE id='a'", [], |r| r.get::<_, String>(0))?,
            db.query_row("SELECT body FROM items WHERE id='b'", [], |r| r.get::<_, String>(0))?,
        ))
    });
    assert_eq!(next(&mut query).await, ("a".into(), "b".into()));
    write(&db, "UPDATE items SET body='ignored' WHERE id='a'; UPDATE items SET title='ignored' WHERE id='b'");
    assert!(!query.is_marked_for_rerun());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    write(&db, "UPDATE items SET body='changed' WHERE id='b'");
    assert_eq!(next(&mut query).await, ("a".into(), "changed".into()));
    db.close().await.unwrap();
}

#[tokio::test]
async fn unsupported_statement_shapes_keep_whole_table_dependencies() {
    let store = TestStore::new();
    let db = store.schema(vec![], "CREATE TABLE items(id INTEGER PRIMARY KEY, value INTEGER); INSERT INTO items VALUES (1,10),(9,90); CREATE TABLE numbers(value INTEGER)").await.unwrap();
    for sql in [
        "SELECT value FROM items WHERE id=abs(1)",
        "SELECT value FROM items WHERE id=1 AND EXISTS(SELECT 1 FROM items)",
        "SELECT value FROM items WHERE id IN (SELECT id FROM items WHERE id=1)",
        "SELECT value FROM items WHERE id=1 AND value NOT IN numbers",
        "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<3) SELECT value FROM items, n WHERE id=1",
        "SELECT value FROM (SELECT * FROM items LIMIT 5) WHERE id=1",
        "SELECT value FROM (SELECT id, max(value) AS value FROM items GROUP BY id) WHERE id=1",
        "SELECT value FROM (SELECT id, value, sum(value) OVER () AS total FROM items) WHERE id=1",
        "SELECT a.value FROM items a JOIN items b ON a.id<b.id WHERE b.value=90",
        "SELECT a.value FROM items a NATURAL JOIN items b WHERE a.id=1",
        "SELECT a.value FROM items a JOIN items b USING (id) WHERE a.id=1",
        "SELECT value FROM items WHERE id BETWEEN 1 AND 1",
        "SELECT value FROM items WHERE id IS NOT 1",
        "SELECT value FROM items WHERE id!=9",
        "SELECT value FROM items WHERE id NOT IN (9)",
        "SELECT value FROM items WHERE id=1 OR value>20",
        "SELECT value FROM items WHERE id COLLATE NOCASE=1",
        "SELECT value FROM items WHERE id='1'",
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let mut query: crate::LiveQuery<_, DbError> = db.subscribe(move |db| {
            let run = count.fetch_add(1, Ordering::SeqCst) + 1;
            Ok((run, db.query(sql, [], |r| r.get::<_, i64>(0))?))
        });
        next(&mut query).await;
        write(&db, "UPDATE items SET value=value+1 WHERE id=9");
        assert_eq!(next(&mut query).await.0, 2);
        assert_eq!(calls.load(Ordering::SeqCst), 2, "{sql}");
    }
    db.close().await.unwrap();
}

/// Functions, aggregates, grouping, ordering and limits act on the rows the
/// WHERE already chose, so they keep its range.
#[tokio::test]
async fn functions_and_aggregates_keep_the_where_range() {
    let store = TestStore::new();
    let db = store.schema(vec![], "CREATE TABLE items(id INTEGER PRIMARY KEY, value INTEGER); INSERT INTO items VALUES (1,10),(9,90)").await.unwrap();
    for sql in [
        "SELECT abs(value) FROM items WHERE id=1",
        "SELECT value FROM items WHERE id=1 ORDER BY abs(value)",
        "SELECT value FROM items WHERE id=1 LIMIT abs(1)",
        "SELECT value FROM items WHERE id=1 LIMIT 1 OFFSET abs(0)",
        "SELECT sum(value) FROM items WHERE id=1",
        "SELECT count(value) FROM items WHERE id=1",
        "SELECT value FROM items WHERE id=1 GROUP BY abs(value)",
        "SELECT value FROM items WHERE id=1 GROUP BY value HAVING abs(value)>0",
        "SELECT sum(value) OVER (ORDER BY id) FROM items WHERE id=1",
        "SELECT value FROM items WHERE id=1 OR value<20",
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let mut query: crate::LiveQuery<_, DbError> = db.subscribe(move |db| {
            let run = count.fetch_add(1, Ordering::SeqCst) + 1;
            Ok((run, db.query(sql, [], |r| r.get::<_, i64>(0))?))
        });
        next(&mut query).await;
        write(&db, "UPDATE items SET value=value+1 WHERE id=9");
        assert!(!query.is_marked_for_rerun(), "{sql}");
        write(&db, "UPDATE items SET value=value+1 WHERE id=1");
        assert_eq!(next(&mut query).await.0, 2, "{sql}");
    }
    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_before_the_statement_reveals_its_range_is_not_missed() {
    let store = TestStore::new();
    let db = store
        .schema(vec![], "CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)")
        .await
        .unwrap();
    let (entered, entry) = tokio::sync::oneshot::channel();
    let entered = Mutex::new(Some(entered));
    let (release, released) = std::sync::mpsc::channel();
    let released = Mutex::new(released);
    let mut query = db.subscribe(move |db| -> Result<_, DbError> {
        if let Some(entered) = entered.lock().unwrap().take() {
            entered.send(()).unwrap();
            released.lock().unwrap().recv_timeout(HANG_GUARD).unwrap();
        }
        Ok(db.query("SELECT value FROM items WHERE id=3", [], |r| r.get::<_, String>(0))?)
    });
    let task = tokio::spawn(async move {
        let value = next(&mut query).await;
        (query, value)
    });
    entry.await.unwrap(); // WAL snapshot pinned, no statement dependency yet
    write(&db, "INSERT INTO items VALUES (3,'new'),(9,'unrelated')");
    release.send(()).unwrap();
    let (mut query, first) = task.await.unwrap();
    assert!(first.is_empty());
    assert!(query.is_marked_for_rerun());
    assert_eq!(next(&mut query).await, ["new"]);
    db.close().await.unwrap();
}

#[tokio::test]
async fn parameters_preserve_nul_text_and_real_precision_and_slot_identity() {
    let store = TestStore::new();
    let db = store.schema(vec![], "CREATE TABLE texts(id TEXT NOT NULL PRIMARY KEY, value INTEGER); CREATE TABLE numbers(id INTEGER PRIMARY KEY, value INTEGER); INSERT INTO numbers VALUES (1,0),(2,0),(3,0)").await.unwrap();
    db.commit_writer(|db| {
        db.internal_execute("INSERT INTO texts VALUES (?1,0),(?2,0)", ["a\0b", "a"]).unwrap();
    });
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let mut text: crate::LiveQuery<_, DbError> = db.subscribe(move |db| {
        count.fetch_add(1, Ordering::SeqCst);
        Ok(db.query_row("SELECT value FROM texts WHERE id=?", ["a\0b"], |r| r.get::<_, i64>(0))?)
    });
    assert_eq!(next(&mut text).await, 0);
    write(&db, "UPDATE texts SET value=1 WHERE id='a'");
    assert!(!text.is_marked_for_rerun());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    db.commit_writer(|db| {
        db.internal_execute("UPDATE texts SET value=2 WHERE id=?", ["a\0b"]).unwrap()
    });
    assert_eq!(next(&mut text).await, 2);

    // Expanded SQL prints this f64 as 2.0, which would exclude key 2 from `<`.
    let bound = f64::from_bits(2.0_f64.to_bits() + 1);
    let mut numeric: crate::LiveQuery<_, DbError> = db.subscribe(move |db| {
        Ok(db.query("SELECT value FROM numbers WHERE id<?1 ORDER BY id", [bound], |r| {
            r.get::<_, i64>(0)
        })?)
    });
    assert_eq!(next(&mut numeric).await, [0, 0]);
    write(&db, "UPDATE numbers SET value=1 WHERE id=2");
    assert_eq!(next(&mut numeric).await, [0, 1]);

    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let mut slots: crate::LiveQuery<_, DbError> = db.subscribe(move |db| {
        count.fetch_add(1, Ordering::SeqCst);
        // The first two slots occur before WHERE; repeated ?3 and anonymous ?
        // must follow SQLite's indexing, including the higher numbered slot.
        Ok(db.query(
            "SELECT ? || ?,value FROM numbers WHERE id=?3 OR id=? ORDER BY id",
            crate::params!["x", "y", 2, 3],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)),
        )?)
    });
    assert_eq!(next(&mut slots).await, [("xy".into(), 1), ("xy".into(), 0)]);
    write(&db, "UPDATE numbers SET value=4 WHERE id=1");
    assert!(!slots.is_marked_for_rerun());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    write(&db, "UPDATE numbers SET value=5 WHERE id=3");
    assert_eq!(next(&mut slots).await, [("xy".into(), 1), ("xy".into(), 5)]);
    let mut holes: crate::LiveQuery<_, DbError> = db.subscribe(|db| {
        Ok(db.query_row(
            "SELECT value FROM numbers WHERE id=?3",
            crate::named_params! {"?3": 2},
            |r| r.get::<_, i64>(0),
        )?)
    });
    assert_eq!(next(&mut holes).await, 1);
    write(&db, "UPDATE numbers SET value=6 WHERE id=2");
    assert_eq!(next(&mut holes).await, 6);
    db.close().await.unwrap();
}

#[tokio::test]
async fn unsupported_key_schemas_use_whole_tables() {
    let store = TestStore::new();
    let db = store.schema(vec![], "CREATE TABLE folded(id TEXT NOT NULL COLLATE NOCASE PRIMARY KEY, value INTEGER); CREATE TABLE unkeyed(id INTEGER, value INTEGER); INSERT INTO folded VALUES ('A',0),('z',0); INSERT INTO unkeyed VALUES(1,0),(9,0)").await.unwrap();
    for (select, update) in [
        ("SELECT value FROM folded WHERE id='a'", "UPDATE folded SET value=value+1 WHERE id='z'"),
        ("SELECT value FROM unkeyed WHERE id=1", "UPDATE unkeyed SET value=value+1 WHERE id=9"),
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let mut query: crate::LiveQuery<_, DbError> = db.subscribe(move |db| {
            let run = count.fetch_add(1, Ordering::SeqCst) + 1;
            Ok((run, db.query(select, [], |r| r.get::<_, i64>(0))?))
        });
        next(&mut query).await;
        write(&db, update);
        assert_eq!(next(&mut query).await.0, 2);
        assert_eq!(calls.load(Ordering::SeqCst), 2, "{select}");
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn empty_in_lists_have_no_matching_keys() {
    let store = TestStore::new();
    let db = store
        .schema(vec![], "CREATE TABLE items(a INTEGER PRIMARY KEY,value INTEGER)")
        .await
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let mut empty: crate::LiveQuery<_, DbError> = db.subscribe(move |db| {
        count.fetch_add(1, Ordering::SeqCst);
        Ok(db.query("SELECT value FROM items WHERE a IN ()", [], |r| r.get::<_, i64>(0))?)
    });
    assert!(next(&mut empty).await.is_empty());
    write(&db, "INSERT INTO items VALUES(3,3)");
    assert!(!empty.is_marked_for_rerun());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    db.close().await.unwrap();
}

#[tokio::test]
async fn multiple_statement_errors_remain_live_without_inventing_a_key_range() {
    let store = TestStore::new();
    let db = store.schema(vec![], "CREATE TABLE items(id INTEGER PRIMARY KEY, value INTEGER); INSERT INTO items VALUES(1,0),(9,0)").await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let mut query = db.subscribe(move |db| {
        count.fetch_add(1, Ordering::SeqCst);
        Ok(db.query("SELECT value FROM items WHERE id=1; SELECT 42", [], |r| r.get::<_, i64>(0))?)
    });
    for update in [false, true] {
        if update {
            write(&db, "UPDATE items SET value=1 WHERE id=9");
        }
        assert!(matches!(
            tokio::time::timeout(HANG_GUARD, query.next())
                .await
                .unwrap()
                .expect("the live query answers"),
            Err(crate::DbError::Sqlite(rusqlite::Error::MultipleStatement))
        ));
    }
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    db.close().await.unwrap();
}

#[tokio::test]
async fn request_replacement_installs_its_new_range_and_preserves_causes() {
    use crate::{LiveQueryCause, LiveQueryRevision};
    let store = TestStore::new();
    let db = store.schema(vec![], "CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT); INSERT INTO items VALUES (1,'one'),(2,'two'),(3,'three')").await.unwrap();
    let mut query: crate::ReconfigurableLiveQuery<_, _, DbError> =
        db.subscribe_reconfigurable(1, |id, db| {
            Ok(db
                .query_row("SELECT value FROM items WHERE id=?", [id], |r| r.get::<_, String>(0))?)
        });
    let requests = query.requests();
    let first = query.next().await.expect("the live query answers");
    assert_eq!(
        (first.revision, first.cause, first.result.unwrap()),
        (LiveQueryRevision(0), LiveQueryCause::Request, "one".into())
    );
    write(&db, "UPDATE items SET value='outside' WHERE id=3");
    assert!(!query.is_marked_for_rerun());
    assert_eq!(requests.set(2).unwrap(), LiveQueryRevision(1));
    let second = query.next().await.expect("the live query answers");
    assert_eq!(
        (second.revision, second.cause, second.result.unwrap()),
        (LiveQueryRevision(1), LiveQueryCause::Request, "two".into())
    );
    write(&db, "UPDATE items SET value='old range' WHERE id=1");
    assert!(!query.is_marked_for_rerun());
    write(&db, "UPDATE items SET value='new range' WHERE id=2");
    assert!(query.is_marked_for_rerun());
    assert_eq!(requests.set(1).unwrap(), LiveQueryRevision(2));
    let third = query.next().await.expect("the live query answers");
    assert_eq!(
        (third.revision, third.cause, third.result.unwrap()),
        (LiveQueryRevision(2), LiveQueryCause::RequestAndWrite, "old range".into())
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn raw_text_keys_and_quoted_names_are_not_lossily_converted() {
    let store = TestStore::new();
    let db = store.schema(vec![], "CREATE TABLE \"a\"\"b\"(\"i\"\"d\" TEXT NOT NULL PRIMARY KEY, value INTEGER); INSERT INTO \"a\"\"b\" VALUES ('other',0)").await.unwrap();
    db.commit_writer(|db| {
        db.internal_execute(
            "INSERT INTO \"a\"\"b\" VALUES (?,0)",
            [crate::sql_value::SqlValue::Text(vec![255, 0, 1])],
        )
        .unwrap()
    });
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let mut query: crate::LiveQuery<_, DbError> = db.subscribe(move |db| {
        count.fetch_add(1, Ordering::SeqCst);
        Ok(db.query_row(
            "SELECT value FROM \"a\"\"b\" AS \"x\"\"y\" WHERE \"x\"\"y\".\"i\"\"d\"=?",
            [crate::sql_value::SqlValue::Text(vec![255, 0, 1])],
            |r| r.get::<_, i64>(0),
        )?)
    });
    assert_eq!(next(&mut query).await, 0);
    write(&db, "UPDATE \"a\"\"b\" SET value=1 WHERE \"i\"\"d\"='other'");
    assert!(!query.is_marked_for_rerun());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    db.commit_writer(|db| {
        db.internal_execute(
            "UPDATE \"a\"\"b\" SET value=2 WHERE \"i\"\"d\"=?",
            [crate::sql_value::SqlValue::Text(vec![255, 0, 1])],
        )
        .unwrap()
    });
    assert_eq!(next(&mut query).await, 2);
    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commits_while_dependencies_are_unknown_still_filter_outside_keys() {
    let store = TestStore::new();
    let db = store
        .schema(vec![], "CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)")
        .await
        .unwrap();
    let (entered, entry) = tokio::sync::oneshot::channel();
    let entered = Mutex::new(Some(entered));
    let (release, released) = std::sync::mpsc::channel();
    let released = Mutex::new(released);
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let mut query = db.subscribe(move |db| -> Result<_, DbError> {
        count.fetch_add(1, Ordering::SeqCst);
        if let Some(entered) = entered.lock().unwrap().take() {
            entered.send(()).unwrap();
            released.lock().unwrap().recv_timeout(HANG_GUARD).unwrap();
        }
        Ok(db.query("SELECT value FROM items WHERE id=3", [], |r| r.get::<_, String>(0))?)
    });
    let task = tokio::spawn(async move {
        let value = next(&mut query).await;
        (query, value)
    });
    entry.await.unwrap();
    write(&db, "INSERT INTO items VALUES (9,'outside')");
    db.inspect_writer(|writer| {
        assert!(writer
            .transaction::<()>(|writer| {
                writer.batch("INSERT INTO items VALUES (3,'rolled back')")?;
                Err(crate::DbError::TransactionEnded)
            })
            .is_err());
    });
    release.send(()).unwrap();
    let (mut query, first) = task.await.unwrap();
    assert!(first.is_empty());
    // The run has installed its dependencies and filtered the buffered commits.
    assert!(!query.is_marked_for_rerun());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    write(&db, "INSERT INTO items VALUES (3,'committed')");
    assert_eq!(next(&mut query).await, ["committed"]);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    db.close().await.unwrap();
}

#[tokio::test]
async fn and_keeps_key_bounds_without_renumbering_skipped_parameters() {
    let store = TestStore::new();
    let db = store.schema(vec![], "CREATE TABLE items(id INTEGER PRIMARY KEY, deleted INTEGER, value INTEGER); INSERT INTO items VALUES(1,0,10),(2,0,20),(3,0,30)").await.unwrap();
    for (sql, params) in [
        ("SELECT value+? FROM items WHERE deleted=? AND id=? ORDER BY ? LIMIT ?,?",vec![0,0,2,1,0,1]),
        ("SELECT value FROM items WHERE deleted=? /* ? is a comment */ AND id=? AND '?'='?'",vec![0,2]),
        ("SELECT value FROM items WHERE id=? AND deleted=0", vec![2]),
        ("SELECT value FROM items WHERE deleted=? AND id=?", vec![0,2]),
        ("SELECT value FROM items WHERE (deleted=? AND id=?) OR (value>? AND id=?) ORDER BY id", vec![0,2,0,3]),
        ("SELECT value+? FROM items WHERE deleted=? AND ?=id AND deleted=?", vec![0,0,2,0]),
        ("SELECT value FROM items WHERE deleted=?3 AND id=? AND deleted=:deleted AND id IN (?4,?)", vec![0,0,0,2,0,2]),
        ("SELECT value FROM items WHERE (id=? AND value>?) AND (deleted=? OR value=?)", vec![2,0,0,-1]),
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let mut query: crate::LiveQuery<_, DbError> = db.subscribe(move |db| {
            count.fetch_add(1, Ordering::SeqCst);
            Ok(db.query(sql, rusqlite::params_from_iter(&params), |r| r.get::<_,i64>(0))?)
        });
        let first = next(&mut query).await;
        assert!(!first.is_empty(), "{sql}");
        write(&db, "UPDATE items SET value=value+1 WHERE id=1");
        assert!(!query.is_marked_for_rerun(), "outside key: {sql}");
        assert_eq!(calls.load(Ordering::SeqCst),1,"outside key: {sql}");
        write(&db, "UPDATE items SET value=value+1 WHERE id=2");
        assert_ne!(next(&mut query).await,first,"inside key: {sql}");
    }
    db.close().await.unwrap();
}

/// A statement a row mapper runs records its own reads; the statement
/// around it keeps its key range, before and after the inner one.
#[tokio::test]
async fn a_row_mappers_statement_leaves_the_outer_range() {
    let store = TestStore::new();
    let db = store.schema(vec![], "CREATE TABLE items(id INTEGER PRIMARY KEY, value INTEGER); CREATE TABLE notes(id INTEGER PRIMARY KEY, text TEXT); INSERT INTO items VALUES (1,0),(2,0),(9,0); INSERT INTO notes VALUES (1,'one'),(9,'nine')").await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let mut query: crate::LiveQuery<_, DbError> = db.subscribe(move |db| {
        count.fetch_add(1, Ordering::SeqCst);
        Ok(db.query("SELECT id,value FROM items WHERE id IN (1,2) ORDER BY id", [], |r| {
            let id = r.get::<_, i64>(0)?;
            let note =
                db.query("SELECT text FROM notes WHERE id=?1", [id], |n| n.get::<_, String>(0))?;
            Ok((id, r.get::<_, i64>(1)?, note))
        })?)
    });
    assert_eq!(next(&mut query).await.len(), 2);
    write(
        &db,
        "UPDATE items SET value=1 WHERE id=9; UPDATE notes SET text='nine again' WHERE id=9",
    );
    assert!(!query.is_marked_for_rerun());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    write(&db, "UPDATE items SET value=1 WHERE id=2");
    assert_eq!(next(&mut query).await[1], (2, 1, vec![]));
    write(&db, "UPDATE notes SET text='uno' WHERE id=1");
    assert_eq!(next(&mut query).await[0], (1, 0, vec!["uno".to_string()]));
    db.close().await.unwrap();
}

/// Conditions on columns outside the key narrow as key conditions do: each
/// equality, IN list and range, in the row's old and new image alike.
#[tokio::test]
async fn conditions_on_any_column_narrow() {
    let store = TestStore::new();
    let db = store.schema(vec![], "CREATE TABLE tracks(id INTEGER PRIMARY KEY, release TEXT, position INTEGER, title TEXT); INSERT INTO tracks VALUES (1,'a',1,'one'),(2,'a',2,'two'),(3,'b',1,'three'),(4,'c',5,'four')").await.unwrap();
    for (sql, outside, inside) in [
        ("SELECT title FROM tracks WHERE release='a' ORDER BY position", 3, 2),
        ("SELECT title FROM tracks WHERE release IN ('a','c') ORDER BY id", 3, 4),
        ("SELECT title FROM tracks WHERE position>=2 ORDER BY id", 3, 4),
        ("SELECT group_concat(title) FROM tracks WHERE release='b' AND position<3", 1, 3),
        ("SELECT title FROM tracks t WHERE t.release=?1 ORDER BY id", 3, 1),
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let mut query: crate::LiveQuery<_, DbError> = db.subscribe(move |db| {
            let run = count.fetch_add(1, Ordering::SeqCst) + 1;
            let parameters: &[&str] = if sql.contains('?') { &["a"] } else { &[] };
            Ok((
                run,
                db.query(sql, rusqlite::params_from_iter(parameters), |r| r.get::<_, String>(0))?,
            ))
        });
        next(&mut query).await;
        write(&db, &format!("UPDATE tracks SET title=title||'.' WHERE id={outside}"));
        assert!(!query.is_marked_for_rerun(), "outside: {sql}");
        write(&db, &format!("UPDATE tracks SET title=title||'.' WHERE id={inside}"));
        assert_eq!(next(&mut query).await.0, 2, "inside: {sql}");
    }
    // A row whose condition column moves into or out of the range counts.
    let mut query: crate::LiveQuery<_, DbError> = db.subscribe(|db| {
        Ok(db.query("SELECT id FROM tracks WHERE release='a' ORDER BY id", [], |r| {
            r.get::<_, i64>(0)
        })?)
    });
    assert_eq!(next(&mut query).await, [1, 2]);
    write(&db, "UPDATE tracks SET release='a' WHERE id=3");
    assert_eq!(next(&mut query).await, [1, 2, 3]);
    write(&db, "UPDATE tracks SET release='z' WHERE id=1");
    assert_eq!(next(&mut query).await, [2, 3]);
    write(&db, "INSERT INTO tracks VALUES (5,'z',1,'five'); DELETE FROM tracks WHERE id=4");
    assert!(!query.is_marked_for_rerun());
    write(&db, "INSERT INTO tracks VALUES (6,'a',9,'six')");
    assert_eq!(next(&mut query).await, [2, 3, 6]);
    db.close().await.unwrap();
}

/// A WHERE condition on a LEFT JOIN's right side narrows that side, as it
/// would an inner join's: the NULLs a missing right row gives fail it, so
/// such a row can't reach the result either way. A view's LEFT JOIN takes
/// the condition put on its rows the same way.
#[tokio::test]
async fn a_where_condition_narrows_a_left_joins_right_side() {
    let store = TestStore::new();
    let db = store.schema(vec![], "CREATE TABLE tracks(id INTEGER PRIMARY KEY, release_id TEXT);
CREATE TABLE credits(id INTEGER PRIMARY KEY, track_id INTEGER, role TEXT, name TEXT);
CREATE VIEW credited AS SELECT t.id, t.release_id, c.id AS credit, c.role, c.name FROM tracks t LEFT JOIN credits c ON c.track_id = t.id;
INSERT INTO tracks VALUES (1,'r1'),(2,'r2');").await.unwrap();
    for sql in [
        "SELECT t.id, c.name FROM tracks t LEFT JOIN credits c ON c.track_id = t.id WHERE t.release_id = ?1 AND c.role = 'x' ORDER BY c.id",
        "SELECT id, name FROM credited WHERE release_id = ?1 AND role = 'x' ORDER BY credit",
    ] {
        write(&db, "DELETE FROM credits; INSERT INTO credits VALUES (1,1,'x','Ana'),(2,1,'y','Ben'),(3,2,'x','Carol')");
        let mut query: crate::LiveQuery<_, DbError> = db.subscribe(move |db| {
            Ok(db.query(sql, ["r1"], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))?)
        });
        assert_eq!(next(&mut query).await, [(1, "Ana".into())]);
        write(&db, "UPDATE credits SET name = 'Benny' WHERE id = 2");
        assert!(!query.is_marked_for_rerun(), "another role: {sql}");
        write(&db, "UPDATE credits SET name = 'Caroline' WHERE id = 3");
        assert!(!query.is_marked_for_rerun(), "another release: {sql}");
        write(&db, "UPDATE credits SET role = 'x' WHERE id = 2");
        assert_eq!(next(&mut query).await, [(1, "Ana".into()), (1, "Benny".into())], "{sql}");
        write(&db, "DELETE FROM credits WHERE id = 1");
        assert_eq!(next(&mut query).await, [(1, "Benny".into())], "{sql}");
        write(&db, "INSERT INTO credits VALUES (4,1,'y','Dan')");
        assert!(!query.is_marked_for_rerun(), "a new credit of another role: {sql}");
    }
    db.close().await.unwrap();
}

/// A LEFT JOIN whose right side a WHERE condition narrows is an inner join:
/// a left row with NULLs in its place fails the condition. So it narrows as
/// one, its ON both ways, and its right side by the rows its left side
/// reaches from the album: an 'x' credit on another album's track doesn't
/// rerun the query. In a view, the condition put on its rows does the same.
#[tokio::test]
async fn a_left_join_a_where_condition_narrows_is_an_inner_join() {
    let store = TestStore::new();
    let db = store.schema(vec![], "CREATE TABLE releases(id TEXT NOT NULL PRIMARY KEY, album TEXT);
CREATE TABLE tracks(id INTEGER PRIMARY KEY, release TEXT);
CREATE TABLE credits(id INTEGER PRIMARY KEY, track INTEGER, role TEXT, name TEXT);
CREATE VIEW album_credits AS SELECT r.album, t.id, c.id AS credit, c.role, c.name FROM releases r JOIN tracks t ON t.release = r.id LEFT JOIN credits c ON c.track = t.id;
INSERT INTO releases VALUES ('r1','a1'),('r2','a2');
INSERT INTO tracks VALUES (1,'r1'),(2,'r2');").await.unwrap();
    for sql in [
        "SELECT t.id, c.name FROM releases r JOIN tracks t ON t.release = r.id LEFT JOIN credits c ON c.track = t.id WHERE r.album = ?1 AND c.role = 'x' ORDER BY c.id",
        "SELECT id, name FROM album_credits WHERE album = ?1 AND role = 'x' ORDER BY credit",
    ] {
        write(&db, "DELETE FROM credits; INSERT INTO credits VALUES (1,1,'x','Ana'),(2,1,'y','Ben'),(3,2,'x','Carol')");
        let mut query: crate::LiveQuery<_, DbError> = db.subscribe(move |db| {
            Ok(db.query(sql, ["a1"], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))?)
        });
        assert_eq!(next(&mut query).await, [(1, "Ana".into())]);
        write(&db, "UPDATE credits SET name = 'Caroline' WHERE id = 3");
        assert!(!query.is_marked_for_rerun(), "another album: {sql}");
        write(&db, "INSERT INTO credits VALUES (4,2,'x','Dan')");
        assert!(!query.is_marked_for_rerun(), "a new credit on another album: {sql}");
        write(&db, "UPDATE credits SET name = 'Benny' WHERE id = 2");
        assert!(!query.is_marked_for_rerun(), "another role: {sql}");
        write(&db, "UPDATE credits SET role = 'x' WHERE id = 2");
        assert_eq!(next(&mut query).await, [(1, "Ana".into()), (1, "Benny".into())], "{sql}");
        write(&db, "INSERT INTO credits VALUES (5,1,'x','Eve')");
        assert_eq!(
            next(&mut query).await,
            [(1, "Ana".into()), (1, "Benny".into()), (1, "Eve".into())],
            "{sql}"
        );
    }
    // Its ON narrows the left side by the right's rows, as an inner join's
    // does: a track with no 'x' credit doesn't rerun the query.
    let mut query: crate::LiveQuery<_, DbError> = db.subscribe(|db| {
        Ok(db.query(
            "SELECT t.release, c.name FROM tracks t LEFT JOIN credits c ON c.track = t.id WHERE c.role = 'x' ORDER BY c.id",
            [],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
        )?)
    });
    next(&mut query).await;
    write(&db, "INSERT INTO tracks VALUES (3,'r1')");
    assert!(!query.is_marked_for_rerun(), "a track with no 'x' credit");
    write(&db, "UPDATE tracks SET release = 'r2' WHERE id = 1");
    assert_eq!(next(&mut query).await[0], ("r2".into(), "Ana".into()));
    db.close().await.unwrap();
}

/// A comparison SQLite would make after converting a value, or under a
/// collation other than BINARY, or on a generated column, keeps the table
/// whole: coven compares values as they are.
#[tokio::test]
async fn conversions_collations_and_generated_columns_keep_whole_tables() {
    let store = TestStore::new();
    let db = store.schema(vec![], "CREATE TABLE items(id INTEGER PRIMARY KEY, number INTEGER, name TEXT COLLATE NOCASE, label TEXT, shout TEXT AS (upper(label))); INSERT INTO items(id,number,name,label) VALUES (1,1,'a','x'),(9,9,'z','y')").await.unwrap();
    for (sql, parameter) in [
        (
            "SELECT label FROM items WHERE number=?1",
            crate::sql_value::SqlValue::Text(b"1".to_vec()),
        ),
        ("SELECT label FROM items WHERE label=?1", crate::sql_value::SqlValue::Integer(1)),
        ("SELECT label FROM items WHERE name=?1", crate::sql_value::SqlValue::Text(b"a".to_vec())),
        ("SELECT label FROM items WHERE shout=?1", crate::sql_value::SqlValue::Text(b"X".to_vec())),
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let mut query: crate::LiveQuery<_, DbError> = db.subscribe(move |db| {
            let run = count.fetch_add(1, Ordering::SeqCst) + 1;
            Ok((run, db.query(sql, [&parameter], |r| r.get::<_, String>(0))?))
        });
        next(&mut query).await;
        write(&db, "UPDATE items SET label=label||'.' WHERE id=9");
        assert_eq!(next(&mut query).await.0, 2, "{sql}");
    }
    db.close().await.unwrap();
}

/// A NULL test narrows as a comparison does: a write to a row outside it
/// wakes nothing, one inside it does, and so does a row moving in or out
/// by changing the tested column between NULL and a value.
#[tokio::test]
async fn null_tests_and_is_narrow() {
    use crate::sql_value::SqlValue;
    let store = TestStore::new();
    let db = store
        .schema(
            vec![],
            "CREATE TABLE items(id INTEGER PRIMARY KEY, x INTEGER, value INTEGER, extra TEXT)",
        )
        .await
        .unwrap();
    // The predicate, its parameters, the ids it keeps, an id outside it and
    // one inside, and the values of `x` that move a row in and out.
    type Case = (&'static str, Vec<SqlValue>, Vec<i64>, i64, i64, &'static str, &'static str);
    let cases: Vec<Case> = vec![
        ("x IS NULL", vec![], vec![1, 2], 3, 1, "NULL", "7"),
        ("x ISNULL", vec![], vec![1, 2], 3, 1, "NULL", "7"),
        ("x COLLATE NOCASE IS NULL", vec![], vec![1, 2], 3, 1, "NULL", "7"),
        ("(x COLLATE NOCASE) ISNULL", vec![], vec![1, 2], 3, 1, "NULL", "7"),
        ("x IS NOT NULL", vec![], vec![3, 4, 5], 1, 3, "1", "NULL"),
        ("x NOTNULL", vec![], vec![3, 4, 5], 1, 3, "1", "NULL"),
        ("x NOT NULL", vec![], vec![3, 4, 5], 1, 3, "1", "NULL"),
        ("x COLLATE NOCASE IS NOT NULL", vec![], vec![3, 4, 5], 1, 3, "1", "NULL"),
        ("x IS ?1", vec![SqlValue::Null], vec![1, 2], 3, 1, "NULL", "7"),
        ("x IS ?1", vec![SqlValue::Integer(3)], vec![3, 4], 5, 3, "3", "9"),
        ("x IS 3", vec![], vec![3, 4], 5, 3, "3", "9"),
        ("x IS 3", vec![], vec![3, 4], 1, 3, "3", "NULL"),
        ("3 IS x", vec![], vec![3, 4], 5, 3, "3", "9"),
        ("NULL IS x", vec![], vec![1, 2], 3, 1, "NULL", "7"),
    ];
    for (predicate, parameters, expected, outside, inside, moves_in, moves_out) in cases {
        write(&db, "DELETE FROM items; INSERT INTO items VALUES (1,NULL,0,''),(2,NULL,0,''),(3,3,0,''),(4,3,0,''),(5,9,0,'')");
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let sql = format!("SELECT id, value FROM items WHERE {predicate} ORDER BY id");
        let bound = parameters.clone();
        let mut query: crate::LiveQuery<_, DbError> = db.subscribe(move |db| {
            count.fetch_add(1, Ordering::SeqCst);
            Ok(db.query(&sql, rusqlite::params_from_iter(&bound), |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?))
            })?)
        });
        assert_eq!(
            next(&mut query).await.iter().map(|row| row.0).collect::<Vec<_>>(),
            expected,
            "{predicate}"
        );
        write(&db, &format!("UPDATE items SET value=value+1 WHERE id={outside}"));
        assert!(!query.is_marked_for_rerun(), "outside: {predicate} {parameters:?}");
        write(&db, &format!("UPDATE items SET extra='ignored' WHERE id={inside}"));
        assert!(!query.is_marked_for_rerun(), "column: {predicate} {parameters:?}");
        write(&db, &format!("UPDATE items SET value=value+1 WHERE id={inside}"));
        next(&mut query).await;
        assert_eq!(calls.load(Ordering::SeqCst), 2, "inside: {predicate} {parameters:?}");
        write(&db, &format!("UPDATE items SET x={moves_in} WHERE id={outside}"));
        next(&mut query).await;
        write(&db, &format!("UPDATE items SET x={moves_out} WHERE id={inside}"));
        next(&mut query).await;
        assert_eq!(calls.load(Ordering::SeqCst), 4, "moving: {predicate} {parameters:?}");
    }
    db.close().await.unwrap();
}

/// A NULL test on a LEFT JOIN's right side is true of the NULLs a missing
/// right row gives, so it doesn't make the join an inner join and doesn't
/// narrow the right side: a credit that joins a track takes it out of an
/// anti-join's result. Through a view, the same. Another condition that
/// makes the join inner lets the NULL test narrow too, in either order, and
/// in the join's own ON it narrows the right side.
#[tokio::test]
async fn a_null_test_on_a_left_joins_right_side_keeps_it_whole() {
    let store = TestStore::new();
    let db = store.schema(vec![], "CREATE TABLE tracks(id INTEGER PRIMARY KEY, release TEXT);
CREATE TABLE credits(id INTEGER PRIMARY KEY, track INTEGER, role TEXT, name TEXT);
CREATE VIEW credited AS SELECT t.id, t.release, c.id AS credit, c.track, c.role, c.name FROM tracks t LEFT JOIN credits c ON c.track = t.id;").await.unwrap();
    let reset = "DELETE FROM tracks; DELETE FROM credits; INSERT INTO tracks VALUES (1,'r1'),(2,'r1'),(3,'r2'); INSERT INTO credits VALUES (1,2,'x','Ana'),(2,3,'x','Ben')";
    for sql in [
        "SELECT t.id FROM tracks t LEFT JOIN credits c ON c.track = t.id WHERE t.release = 'r1' AND c.track IS NULL ORDER BY t.id",
        "SELECT id FROM credited WHERE release = 'r1' AND track IS NULL ORDER BY id",
    ] {
        write(&db, reset);
        follows(&db, sql, vec![], &[
            ("INSERT INTO credits VALUES (3,1,'x','Cy')", true),
            ("DELETE FROM credits WHERE id = 3", true),
            ("INSERT INTO credits VALUES (4,3,'x','Dee')", false),
            ("UPDATE credits SET name = 'Benn' WHERE id = 2", false),
            ("UPDATE credits SET track = 1 WHERE id = 1", true),
        ]).await;
    }
    for sql in [
        "SELECT t.id FROM tracks t LEFT JOIN credits c ON c.track = t.id WHERE t.release = 'r1' AND c.name IS NULL AND c.role = 'x' ORDER BY t.id",
        "SELECT t.id FROM tracks t LEFT JOIN credits c ON c.track = t.id WHERE t.release = 'r1' AND c.role = 'x' AND c.name IS NULL ORDER BY t.id",
    ] {
        write(&db, reset);
        write(&db, "UPDATE credits SET name = NULL WHERE id = 1");
        follows(&db, sql, vec![], &[
            ("UPDATE credits SET role = 'x' WHERE id = 2", false),
            ("UPDATE credits SET name = 'Anna' WHERE id = 1", true),
            ("UPDATE credits SET name = NULL WHERE id = 1", true),
            ("INSERT INTO credits VALUES (3,1,'y',NULL)", false),
            ("INSERT INTO credits VALUES (4,1,'x','Cy')", false),
            ("INSERT INTO credits VALUES (5,3,'x',NULL)", false),
            ("INSERT INTO credits VALUES (6,1,'x',NULL)", true),
        ]).await;
    }
    write(&db, reset);
    write(&db, "UPDATE credits SET name = NULL WHERE id = 1");
    follows(&db, "SELECT c.role FROM tracks t LEFT JOIN credits c ON c.track = t.id AND c.name IS NULL WHERE t.release = 'r1' ORDER BY t.id", vec![], &[
        ("INSERT INTO credits VALUES (3,1,'y','Cy')", false),
        ("UPDATE credits SET role = 'z' WHERE id = 3", false),
        ("INSERT INTO credits VALUES (4,3,'x',NULL)", false),
        ("UPDATE credits SET role = 'w' WHERE id = 1", true),
        ("UPDATE credits SET name = 'Anna' WHERE id = 1", true),
    ]).await;
    db.close().await.unwrap();
}

/// A comparison under a COLLATE, or against a value under one, keeps the
/// table whole: coven compares values by BINARY only. So do `IS NOT` a
/// value, and `IS` under a collation, which compare as `=` does.
#[tokio::test]
async fn collated_comparisons_keep_whole_tables() {
    let store = TestStore::new();
    let db = store.schema(vec![], "CREATE TABLE items(id INTEGER PRIMARY KEY, x TEXT, value INTEGER); INSERT INTO items VALUES (1,'a',0),(2,'z',0)").await.unwrap();
    for sql in [
        "SELECT value FROM items WHERE x COLLATE NOCASE = 'a'",
        "SELECT value FROM items WHERE x = 'a' COLLATE NOCASE",
        "SELECT value FROM items WHERE x COLLATE NOCASE IN ('a')",
        "SELECT value FROM items WHERE x COLLATE NOCASE IS 'a'",
        "SELECT value FROM items WHERE x COLLATE NOCASE IS ?1",
        "SELECT value FROM items WHERE x IS NOT 'z'",
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let mut query: crate::LiveQuery<_, DbError> = db.subscribe(move |db| {
            let run = count.fetch_add(1, Ordering::SeqCst) + 1;
            let parameters: &[&str] = if sql.contains('?') { &["a"] } else { &[] };
            Ok((
                run,
                db.query(sql, rusqlite::params_from_iter(parameters), |r| r.get::<_, i64>(0))?,
            ))
        });
        next(&mut query).await;
        write(&db, "UPDATE items SET value=value+1 WHERE id=2");
        assert_eq!(next(&mut query).await.0, 2, "{sql}");
    }
    db.close().await.unwrap();
}

use crate::{tests::TestStore, CovenResult, Database, LiveQuery};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

async fn next<T: Clone + PartialEq + Send + 'static>(query: &mut LiveQuery<T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), query.next())
        .await
        .expect("query must answer")
        .unwrap()
}

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
        let mut query = db.subscribe(move |db| {
            count.fetch_add(1, Ordering::SeqCst);
            Ok(db.query(&sql, [], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)))?)
        });
        assert_eq!(
            next(&mut query)
                .await
                .iter()
                .map(|r| r.0)
                .collect::<Vec<_>>(),
            expected,
            "{predicate}"
        );
        write(
            &db,
            &format!("UPDATE items SET value=value+1 WHERE id={outside}"),
        );
        assert!(!query.is_marked_for_rerun(), "outside: {predicate}");
        assert_eq!(calls.load(Ordering::SeqCst), 1, "outside: {predicate}");
        write(
            &db,
            &format!("UPDATE items SET extra='ignored' WHERE id={inside}"),
        );
        assert!(!query.is_marked_for_rerun(), "column: {predicate}");
        assert_eq!(calls.load(Ordering::SeqCst), 1, "column: {predicate}");
        write(
            &db,
            &format!("UPDATE items SET value=value+1 WHERE id={inside}"),
        );
        assert_eq!(
            next(&mut query)
                .await
                .iter()
                .map(|r| r.0)
                .collect::<Vec<_>>(),
            expected
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2, "inside: {predicate}");
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn inserts_and_old_and_new_keys_can_enter_or_leave_an_empty_range() {
    let store = TestStore::new();
    let db = store
        .schema(
            vec![],
            "CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)",
        )
        .await
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let mut query = db.subscribe(move |db| {
        count.fetch_add(1, Ordering::SeqCst);
        Ok(db.query(
            "SELECT id,value FROM items WHERE id>=?1 AND id<?2",
            [2, 5],
            |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)),
        )?)
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
    let mut query = db.subscribe(move |db| {
        count.fetch_add(1, Ordering::SeqCst);
        Ok(db.query("SELECT value FROM items WHERE tenant=:tenant AND id IN (:first,:second) ORDER BY id", crate::named_params!{":tenant": "a", ":first": [1_u8,0].as_slice(), ":second": [2_u8,0].as_slice()}, |r| r.get::<_, i64>(0))?)
    });
    assert_eq!(next(&mut query).await, [0, 0]);
    write(
        &db,
        "UPDATE items SET value=1 WHERE tenant='b' OR id=x'0300'",
    );
    assert!(!query.is_marked_for_rerun());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    write(
        &db,
        "UPDATE items SET value=2 WHERE tenant='a' AND id=x'0200'",
    );
    assert_eq!(next(&mut query).await, [0, 2]);
    write(
        &db,
        "UPDATE items SET tenant='c' WHERE tenant='a' AND id=x'0100'",
    );
    assert_eq!(next(&mut query).await, [2]);
    write(
        &db,
        "UPDATE items SET id=x'0001' WHERE tenant='a' AND id=x'0200'",
    );
    assert!(next(&mut query).await.is_empty());
    db.close().await.unwrap();
}

#[tokio::test]
async fn separate_statements_keep_their_own_columns_and_ranges() {
    let store = TestStore::new();
    let db = store.schema(vec![], "CREATE TABLE items(id TEXT NOT NULL PRIMARY KEY, title TEXT, body TEXT); INSERT INTO items VALUES ('a','a','a'),('b','b','b')").await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let mut query = db.subscribe(move |db| {
        count.fetch_add(1, Ordering::SeqCst);
        Ok((
            db.query_row("SELECT title FROM items WHERE id='a'", [], |r| {
                r.get::<_, String>(0)
            })?,
            db.query_row("SELECT body FROM items WHERE id='b'", [], |r| {
                r.get::<_, String>(0)
            })?,
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
    let db = store.schema(vec![], "CREATE TABLE items(id INTEGER PRIMARY KEY, value INTEGER); INSERT INTO items VALUES (1,10),(9,90); CREATE VIEW item_view AS SELECT * FROM items").await.unwrap();
    for sql in [
        "SELECT abs(value) FROM items WHERE id=1",
        "SELECT value FROM items WHERE id=abs(1)",
        "SELECT value FROM items WHERE id=1 ORDER BY abs(value)",
        "SELECT value FROM items WHERE id=1 LIMIT abs(1)",
        "SELECT value FROM items WHERE id=1 LIMIT 1 OFFSET abs(0)",
        "SELECT sum(value) FROM items WHERE id=1",
        "SELECT value FROM items WHERE id=1 GROUP BY abs(value)",
        "SELECT value FROM items WHERE id=1 GROUP BY value HAVING abs(value)>0",
        "SELECT (SELECT value FROM items WHERE id=1) FROM items WHERE id=1",
        "SELECT value FROM items WHERE id=1 AND EXISTS(SELECT 1 FROM items)",
        "SELECT value FROM items WHERE id IN (SELECT id FROM items WHERE id=1)",
        "WITH selected AS (SELECT * FROM items) SELECT value FROM selected WHERE id=1",
        "SELECT value FROM items WHERE id=1 UNION SELECT value FROM items WHERE id=1",
        "SELECT a.value FROM items a JOIN items b ON a.id=b.id WHERE a.id=1",
        "SELECT value FROM item_view WHERE id=1",
        "SELECT value FROM items WHERE id BETWEEN 1 AND 1",
        "SELECT value FROM items WHERE id IS 1",
        "SELECT value FROM items WHERE id!=9",
        "SELECT value FROM items WHERE id NOT IN (9)",
        "SELECT value FROM items WHERE id=1 OR value<20",
        "SELECT value FROM items WHERE id COLLATE NOCASE=1",
        "SELECT value FROM items WHERE id='1'",
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let mut query = db.subscribe(move |db| {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_before_the_statement_reveals_its_range_is_not_missed() {
    let store = TestStore::new();
    let db = store
        .schema(
            vec![],
            "CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)",
        )
        .await
        .unwrap();
    let (entered, entry) = tokio::sync::oneshot::channel();
    let entered = Mutex::new(Some(entered));
    let (release, released) = std::sync::mpsc::channel();
    let released = Mutex::new(released);
    let mut query = db.subscribe(move |db| -> CovenResult<_> {
        if let Some(entered) = entered.lock().unwrap().take() {
            entered.send(()).unwrap();
            released
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(5))
                .unwrap();
        }
        Ok(db.query("SELECT value FROM items WHERE id=3", [], |r| {
            r.get::<_, String>(0)
        })?)
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
        db.internal_execute("INSERT INTO texts VALUES (?1,0),(?2,0)", ["a\0b", "a"])
            .unwrap();
    });
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let mut text = db.subscribe(move |db| {
        count.fetch_add(1, Ordering::SeqCst);
        Ok(
            db.query_row("SELECT value FROM texts WHERE id=?", ["a\0b"], |r| {
                r.get::<_, i64>(0)
            })?,
        )
    });
    assert_eq!(next(&mut text).await, 0);
    write(&db, "UPDATE texts SET value=1 WHERE id='a'");
    assert!(!text.is_marked_for_rerun());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    db.commit_writer(|db| {
        db.internal_execute("UPDATE texts SET value=2 WHERE id=?", ["a\0b"])
            .unwrap()
    });
    assert_eq!(next(&mut text).await, 2);

    // Expanded SQL prints this f64 as 2.0, which would exclude key 2 from `<`.
    let bound = f64::from_bits(2.0_f64.to_bits() + 1);
    let mut numeric = db.subscribe(move |db| {
        Ok(db.query(
            "SELECT value FROM numbers WHERE id<?1 ORDER BY id",
            [bound],
            |r| r.get::<_, i64>(0),
        )?)
    });
    assert_eq!(next(&mut numeric).await, [0, 0]);
    write(&db, "UPDATE numbers SET value=1 WHERE id=2");
    assert_eq!(next(&mut numeric).await, [0, 1]);

    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let mut slots = db.subscribe(move |db| {
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
    let mut holes = db.subscribe(|db| {
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
    let db = store.schema(vec![], "CREATE TABLE folded(id TEXT COLLATE NOCASE PRIMARY KEY, value INTEGER); CREATE TABLE unkeyed(id INTEGER, value INTEGER); CREATE TABLE decimal(id NUMERIC PRIMARY KEY, value INTEGER); INSERT INTO folded VALUES ('A',0),('z',0); INSERT INTO unkeyed VALUES(1,0),(9,0); INSERT INTO decimal VALUES(1,0),(9,0)").await.unwrap();
    for (select, update) in [
        (
            "SELECT value FROM folded WHERE id='a'",
            "UPDATE folded SET value=value+1 WHERE id='z'",
        ),
        (
            "SELECT value FROM unkeyed WHERE id=1",
            "UPDATE unkeyed SET value=value+1 WHERE id=9",
        ),
        (
            "SELECT value FROM decimal WHERE id=1",
            "UPDATE decimal SET value=value+1 WHERE id=9",
        ),
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let mut query = db.subscribe(move |db| {
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
async fn nullable_composite_keys_observe_changes_to_null_rows() {
    let store = TestStore::new();
    let db = store.schema(vec![], "CREATE TABLE items(a INTEGER, b TEXT, value INTEGER, PRIMARY KEY(a,b)); INSERT INTO items VALUES (NULL,'yes',0),(1,NULL,0),(2,'no',0)").await.unwrap();
    let mut query = db.subscribe(|db| {
        Ok(db.query(
            "SELECT value FROM items WHERE a=1 OR b='yes' ORDER BY rowid",
            [],
            |r| r.get::<_, i64>(0),
        )?)
    });
    assert_eq!(next(&mut query).await, [0, 0]);
    write(&db, "UPDATE items SET value=1 WHERE a IS NULL OR b IS NULL");
    assert_eq!(next(&mut query).await, [1, 1]);
    write(&db, "DELETE FROM items");
    assert!(next(&mut query).await.is_empty());
    db.close().await.unwrap();
}

#[tokio::test]
async fn empty_in_lists_have_no_matching_keys() {
    let store = TestStore::new();
    let db = store
        .schema(
            vec![],
            "CREATE TABLE items(a INTEGER PRIMARY KEY,value INTEGER)",
        )
        .await
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let mut empty = db.subscribe(move |db| {
        count.fetch_add(1, Ordering::SeqCst);
        Ok(db.query("SELECT value FROM items WHERE a IN ()", [], |r| {
            r.get::<_, i64>(0)
        })?)
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
        Ok(
            db.query("SELECT value FROM items WHERE id=1; SELECT 42", [], |r| {
                r.get::<_, i64>(0)
            })?,
        )
    });
    for update in [false, true] {
        if update {
            write(&db, "UPDATE items SET value=1 WHERE id=9");
        }
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(5), query.next())
                .await
                .unwrap(),
            Err(crate::CovenError::Database(crate::DbError::Sqlite(
                rusqlite::Error::MultipleStatement
            )))
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
    let mut query = db.subscribe_reconfigurable(1, |id, db| {
        Ok(
            db.query_row("SELECT value FROM items WHERE id=?", [id], |r| {
                r.get::<_, String>(0)
            })?,
        )
    });
    let requests = query.requests();
    let first = query.next().await;
    assert_eq!(
        (first.revision, first.cause, first.result.unwrap()),
        (LiveQueryRevision(0), LiveQueryCause::Request, "one".into())
    );
    write(&db, "UPDATE items SET value='outside' WHERE id=3");
    assert!(!query.is_marked_for_rerun());
    assert_eq!(requests.set(2).unwrap(), LiveQueryRevision(1));
    let second = query.next().await;
    assert_eq!(
        (second.revision, second.cause, second.result.unwrap()),
        (LiveQueryRevision(1), LiveQueryCause::Request, "two".into())
    );
    write(&db, "UPDATE items SET value='old range' WHERE id=1");
    assert!(!query.is_marked_for_rerun());
    write(&db, "UPDATE items SET value='new range' WHERE id=2");
    assert!(query.is_marked_for_rerun());
    assert_eq!(requests.set(1).unwrap(), LiveQueryRevision(2));
    let third = query.next().await;
    assert_eq!(
        (third.revision, third.cause, third.result.unwrap()),
        (
            LiveQueryRevision(2),
            LiveQueryCause::RequestAndWrite,
            "old range".into()
        )
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
    let mut query = db.subscribe(move |db| {
        count.fetch_add(1, Ordering::SeqCst);
        Ok(db.query_row(
            "SELECT value FROM \"a\"\"b\" AS \"x\"\"y\" WHERE \"x\"\"y\".\"i\"\"d\"=?",
            [crate::sql_value::SqlValue::Text(vec![255, 0, 1])],
            |r| r.get::<_, i64>(0),
        )?)
    });
    assert_eq!(next(&mut query).await, 0);
    write(
        &db,
        "UPDATE \"a\"\"b\" SET value=1 WHERE \"i\"\"d\"='other'",
    );
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
        .schema(
            vec![],
            "CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)",
        )
        .await
        .unwrap();
    let (entered, entry) = tokio::sync::oneshot::channel();
    let entered = Mutex::new(Some(entered));
    let (release, released) = std::sync::mpsc::channel();
    let released = Mutex::new(released);
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let mut query = db.subscribe(move |db| -> CovenResult<_> {
        count.fetch_add(1, Ordering::SeqCst);
        if let Some(entered) = entered.lock().unwrap().take() {
            entered.send(()).unwrap();
            released
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(5))
                .unwrap();
        }
        Ok(db.query("SELECT value FROM items WHERE id=3", [], |r| {
            r.get::<_, String>(0)
        })?)
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
        let mut query = db.subscribe(move |db| {
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

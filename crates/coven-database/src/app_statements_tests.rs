use crate::tests::TestStore;
use crate::write::tests::sql;
use crate::{DbError, Migration, RowIdentity, SyncedTable};

async fn notes(store: &TestStore) -> crate::Database {
    store
        .builder(
            vec![SyncedTable::new("notes", RowIdentity::SharedKey)],
            vec![Migration::addition(
                1,
                "notes",
                "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL)",
            )],
        )
        .open()
        .await
        .unwrap()
}

const INSERT: &str = "INSERT INTO notes VALUES (?1, 'title')";

/// An app write's statement is prepared once, then run from the cache.
#[tokio::test]
async fn a_repeated_app_statement_is_prepared_once() {
    let store = TestStore::new();
    let db = notes(&store).await;
    let before = db.inspect_writer(|writer| writer.app_statements.fresh.get());
    for id in ["1", "2", "3"] {
        db.write(move |sql| {
            sql.execute(INSERT, [id])?;
            Ok(())
        })
        .await
        .unwrap();
    }
    let prepared = db.inspect_writer(|writer| writer.app_statements.fresh.get()) - before;
    assert_eq!(prepared, 1);
    db.close().await.unwrap();
}

/// A live query's statement from the cache records what it reads as its
/// first preparation did, so later writes still rerun the query.
#[tokio::test]
async fn a_cached_query_still_records_its_reads() {
    let store = TestStore::new();
    let db = notes(&store).await;
    let mut titles = db.subscribe(|sql| {
        Ok(sql.query("SELECT title FROM notes ORDER BY id", [], |r| {
            r.get::<_, String>(0)
        })?)
    });
    assert!(titles.next().await.unwrap().is_empty());
    for (id, title) in [("1", "a"), ("2", "b"), ("3", "c")] {
        db.write(move |sql| {
            sql.execute("INSERT INTO notes VALUES (?1, ?2)", [id, title])?;
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(titles.next().await.unwrap().last().unwrap(), title);
    }
    db.close().await.unwrap();
}

/// The same SQL is judged in each context it runs in: kept prepared from a
/// write, it is still refused in a read.
#[tokio::test]
async fn a_statement_cached_in_a_write_is_still_refused_in_a_read() {
    let store = TestStore::new();
    let db = notes(&store).await;
    db.write(|sql| {
        sql.execute(INSERT, ["1"])?;
        Ok(())
    })
    .await
    .unwrap();
    let refused = db.inspect_writer(|writer| {
        writer
            .transaction(|writer| {
                let _reading = writer.authorization.reading();
                Ok(writer.app_query(INSERT, ["2"], |_| Ok(())))
            })
            .unwrap()
    });
    match refused {
        Err(rusqlite::Error::UserFunctionError(error)) => assert!(
            matches!(
                error.downcast_ref::<DbError>(),
                Some(DbError::StatementForbidden {
                    operation: "writing through a read context"
                })
            ),
            "{error:?}"
        ),
        result => panic!("expected the read's refusal, received {result:?}"),
    }
    // And a refused read leaves the write's statement working.
    sql(&db, "INSERT INTO notes VALUES ('3','title')")
        .await
        .unwrap();
    db.close().await.unwrap();
}

/// A schema change drops what the cache kept: the statement is prepared and
/// judged again against the new schema.
#[tokio::test]
async fn a_schema_change_prepares_again() {
    let store = TestStore::new();
    let db = notes(&store).await;
    let write = |id: &'static str| {
        db.write(move |sql| {
            sql.execute(INSERT, [id])?;
            Ok(())
        })
    };
    write("1").await.unwrap();
    let before = db.inspect_writer(|writer| writer.app_statements.fresh.get());
    db.inspect_writer(|writer| writer.batch("CREATE TABLE elsewhere(x)").unwrap());
    write("2").await.unwrap();
    write("3").await.unwrap();
    let prepared = db.inspect_writer(|writer| writer.app_statements.fresh.get()) - before;
    assert_eq!(prepared, 1);
    let count: i64 = db
        .read(|sql| Ok(sql.query_row("SELECT count(*) FROM notes", [], |r| r.get(0))?))
        .await
        .unwrap();
    assert_eq!(count, 3);
    db.close().await.unwrap();
}

/// A synced table whose triggers keep a device-local listing from a view
/// over several tables, as an import list's are.
const TRIGGERED: &str = "CREATE TABLE scans(id TEXT NOT NULL PRIMARY KEY, path TEXT NOT NULL, title TEXT NOT NULL);
CREATE TABLE edits(id TEXT NOT NULL PRIMARY KEY, title TEXT);
CREATE TABLE verdicts(id TEXT NOT NULL PRIMARY KEY, kind TEXT, position INTEGER);
CREATE TABLE matches(id TEXT NOT NULL, position INTEGER NOT NULL, pressing TEXT, failure TEXT, PRIMARY KEY(id, position));
CREATE TABLE origins(id TEXT NOT NULL PRIMARY KEY, created_at INTEGER);
CREATE TABLE skipped(path TEXT NOT NULL PRIMARY KEY);
CREATE TABLE listing(id TEXT NOT NULL PRIMARY KEY, tab TEXT NOT NULL, state TEXT, title TEXT NOT NULL, unit_path TEXT NOT NULL);
CREATE VIEW placed AS
  SELECT s.id, CASE WHEN o.id IS NOT NULL THEN 'done' WHEN k.path IS NOT NULL THEN 'skipped' ELSE 'pending' END AS tab,
         CASE WHEN v.kind IS NULL THEN 'not_looked_up'
              WHEN (SELECT count(DISTINCT pressing) FROM matches m WHERE m.id=s.id) <> 1 THEN 'several'
              WHEN EXISTS(SELECT 1 FROM matches m WHERE m.id=s.id AND m.failure IS NOT NULL) THEN 'unread'
              WHEN v.kind = 'found' THEN 'identified' ELSE 'needs_you' END AS state,
         coalesce(e.title, s.title) AS title,
         CASE WHEN instr(s.path,'/')>0 THEN substr(s.path,1,instr(s.path,'/')-1) ELSE s.path END AS unit_path
  FROM scans s LEFT JOIN edits e ON e.id=s.id LEFT JOIN verdicts v ON v.id=s.id
  LEFT JOIN origins o ON o.id=s.id LEFT JOIN skipped k ON k.path=s.path;
CREATE TRIGGER scans_listed AFTER INSERT ON scans BEGIN
  INSERT OR REPLACE INTO listing SELECT * FROM placed WHERE id=NEW.id; END;
CREATE TRIGGER scans_relisted AFTER UPDATE ON scans BEGIN
  INSERT OR REPLACE INTO listing SELECT * FROM placed WHERE id=NEW.id; END;
CREATE TRIGGER scans_unlisted AFTER DELETE ON scans BEGIN DELETE FROM listing WHERE id=OLD.id; END;";

/// What one app statement whose triggers reach a view costs through coven,
/// against plain SQLite preparing it each time and from its cache. Run
/// explicitly.
#[tokio::test]
#[ignore = "one timing measurement, run explicitly"]
async fn a_triggered_statements_cost() {
    const STATEMENTS: usize = 500;
    const INSERT: &str = "INSERT INTO scans VALUES (?1, ?2, ?3)";
    let directory = tempfile::tempdir().unwrap();
    let plain = rusqlite::Connection::open(directory.path().join("plain.db")).unwrap();
    plain
        .execute_batch(&format!(
            "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; {TRIGGERED}"
        ))
        .unwrap();
    let row = |i: usize| {
        (
            format!("id-{i}"),
            format!("Artist/Album {i}"),
            format!("title {i}"),
        )
    };
    let plain_run = |cached: bool, start: usize| {
        plain.execute_batch("BEGIN").unwrap();
        let started = std::time::Instant::now();
        for i in start..start + STATEMENTS {
            if cached {
                plain
                    .prepare_cached(INSERT)
                    .unwrap()
                    .execute(row(i))
                    .unwrap();
            } else {
                plain.prepare(INSERT).unwrap().execute(row(i)).unwrap();
            }
        }
        let elapsed = started.elapsed();
        plain.execute_batch("COMMIT").unwrap();
        elapsed / STATEMENTS as u32
    };
    let fresh = plain_run(false, 0);
    let cached = plain_run(true, STATEMENTS);
    // The same triggers compiled into a statement that changes no row, so
    // its cost is preparing it.
    const MISS: &str = "UPDATE scans SET title=?2 WHERE id=?1";
    let plain_miss = |cached: bool| {
        let started = std::time::Instant::now();
        for i in 0..STATEMENTS {
            let row = (format!("absent-{i}"), "t");
            if cached {
                plain.prepare_cached(MISS).unwrap().execute(row).unwrap();
            } else {
                plain.prepare(MISS).unwrap().execute(row).unwrap();
            }
        }
        started.elapsed() / STATEMENTS as u32
    };
    let (fresh_miss, cached_miss) = (plain_miss(false), plain_miss(true));

    let store = TestStore::new();
    let db = store
        .builder(
            vec![SyncedTable::new("scans", RowIdentity::SharedKey)],
            vec![crate::Migration::addition(1, "scans", TRIGGERED)],
        )
        .open()
        .await
        .unwrap();
    let started = std::time::Instant::now();
    db.write(move |sql| {
        for i in 0..STATEMENTS {
            sql.execute(INSERT, row(i))?;
        }
        Ok(())
    })
    .await
    .unwrap();
    let coven = started.elapsed() / STATEMENTS as u32;
    let started = std::time::Instant::now();
    db.write(move |sql| {
        for i in 0..STATEMENTS {
            sql.execute(MISS, (format!("absent-{i}"), "t"))?;
        }
        Ok(())
    })
    .await
    .unwrap();
    let coven_miss = started.elapsed() / STATEMENTS as u32;
    db.close().await.unwrap();
    eprintln!(
        "insert per statement: coven {coven:?}; plain SQLite preparing each {fresh:?}, from its cache {cached:?}"
    );
    eprintln!(
        "statement changing nothing: coven {coven_miss:?}; plain SQLite preparing each {fresh_miss:?}, from its cache {cached_miss:?}"
    );
}

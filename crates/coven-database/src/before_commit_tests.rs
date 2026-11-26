//! The app's before-commit function runs last in each transaction that
//! changes the app's rows: a local write, a bulk write, an applied slot, a
//! snapshot load and a migration as the store opens (§9.4).

use crate::joining_write::tests::{dan, registered, registration};
use crate::tests::{next, remote_update, remote_update_beside_queue, try_remote_update, TestStore};
use crate::write::tests::{count, records};
use crate::{CommitSql, DbError, Migration, StoreDatabase, SyncedTable};
use coven_format::value::Value;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

const SCHEMA: &str = "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL, body TEXT NOT NULL DEFAULT '');
CREATE TABLE shown(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL);
CREATE TABLE runs(run INTEGER NOT NULL PRIMARY KEY);
CREATE TABLE local_rows(id TEXT NOT NULL PRIMARY KEY);
CREATE TABLE tally(rows INTEGER NOT NULL PRIMARY KEY);";

fn tables() -> Vec<SyncedTable> {
    vec![SyncedTable::new("notes", crate::RowIdentity::SharedKey)]
}

/// A function that counts its runs, shows each note's title upper-cased, a
/// rule SQL doesn't state, and tallies `local_rows`; then fails while
/// `failing` holds.
fn computing(
    failing: &Arc<AtomicBool>,
) -> impl Fn(&CommitSql<'_>) -> Result<(), DbError> + Send + Sync + 'static {
    let failing = failing.clone();
    move |sql| {
        sql.execute("INSERT INTO runs SELECT count(*) FROM runs", [])?;
        let notes = sql.query("SELECT id, title FROM notes", [], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
        })?;
        sql.execute("DELETE FROM shown", [])?;
        for (id, title) in notes {
            sql.execute("INSERT INTO shown VALUES(?1,?2)", [id, title.to_uppercase()])?;
        }
        sql.execute_batch("DELETE FROM tally; INSERT INTO tally SELECT count(*) FROM local_rows")?;
        if failing.load(Ordering::SeqCst) {
            return Err(DbError::Invariant { what: "the app's before-commit function failed" });
        }
        Ok(())
    }
}

async fn open(
    store: &TestStore,
    function: impl Fn(&CommitSql<'_>) -> Result<(), DbError> + Send + Sync + 'static,
) -> StoreDatabase {
    store
        .builder(tables(), vec![Migration::addition(1, "notes", SCHEMA)])
        .before_commit(function)
        .open()
        .await
        .unwrap()
}

fn shown(db: &StoreDatabase) -> Vec<(String, String)> {
    db.inspect_writer(|db, _| {
        db.query("SELECT id, title FROM shown ORDER BY id", [], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
    })
}

fn tally(db: &StoreDatabase) -> i64 {
    db.inspect_writer(|db, _| db.query_row("SELECT rows FROM tally", [], |r| r.get(0)).unwrap())
}

fn shown_as(id: &str, title: &str) -> Vec<(String, String)> {
    vec![(id.into(), title.into())]
}

async fn write_note(db: &StoreDatabase, id: &str, title: &str) -> Result<(), DbError> {
    let (id, title) = (id.to_owned(), title.to_owned());
    db.write(move |sql| {
        sql.execute("INSERT INTO notes(id,title) VALUES(?1,?2)", [id, title])?;
        Ok(())
    })
    .await
}

#[tokio::test]
async fn a_migration_as_the_store_opens_runs_the_function() {
    let store = TestStore::new();
    let db = open(&store, computing(&Arc::new(AtomicBool::new(false)))).await;
    assert_eq!(count(&db, "runs"), 1);
    assert_eq!(tally(&db), 0);
    db.close().await.unwrap();
    // An open with no migration to run changes no app row and runs nothing.
    let db = open(&store, computing(&Arc::new(AtomicBool::new(false)))).await;
    assert_eq!(count(&db, "runs"), 1);
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_failing_function_fails_the_migration() {
    let store = TestStore::new();
    let failing = Arc::new(AtomicBool::new(true));
    let refused = store
        .builder(tables(), vec![Migration::addition(1, "notes", SCHEMA)])
        .before_commit(computing(&failing))
        .open()
        .await;
    assert!(
        matches!(&refused, Err(crate::OpenError::Migration(crate::MigrationError::Failed { .. }))),
        "{:?}",
        refused.err()
    );
    // Nothing of the migration committed.
    let db = open(&store, computing(&Arc::new(AtomicBool::new(false)))).await;
    assert_eq!(count(&db, "runs"), 1);
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_local_write_stores_what_the_function_computes_in_its_own_commit() {
    let store = TestStore::new();
    let db = open(&store, computing(&Arc::new(AtomicBool::new(false)))).await;
    write_note(&db, "n1", "milk").await.unwrap();
    assert_eq!(shown(&db), shown_as("n1", "MILK"));
    // It runs once in the write, after the app's SQL.
    assert_eq!(count(&db, "runs"), 2);
    // Its rows are this device's own: the write's record holds only the note.
    let written = records(&db);
    assert_eq!(written.len(), 1);
    let rows: Vec<_> = written[0].parts.iter().flat_map(|part| &part.rows).collect();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row.table, "notes");
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_failing_function_rolls_the_local_write_back() {
    let store = TestStore::new();
    let failing = Arc::new(AtomicBool::new(false));
    let db = open(&store, computing(&failing)).await;
    failing.store(true, Ordering::SeqCst);
    let before = crate::tests::contents(&db);
    let refused = write_note(&db, "n1", "milk").await;
    assert!(matches!(refused, Err(DbError::Invariant { .. })), "{refused:?}");
    assert_eq!(crate::tests::contents(&db), before);
    failing.store(false, Ordering::SeqCst);
    write_note(&db, "n1", "milk").await.unwrap();
    assert_eq!(shown(&db), shown_as("n1", "MILK"));
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_bulk_write_runs_the_function_after_its_sql() {
    let store = TestStore::new();
    let db = open(&store, computing(&Arc::new(AtomicBool::new(false)))).await;
    let mut query = db.subscribe(|sql| {
        Ok::<_, DbError>(sql.query_row("SELECT rows FROM tally", [], |r| r.get::<_, i64>(0))?)
    });
    assert_eq!(next(&mut query).await, 0);
    db.write_bulk(|sql| {
        sql.execute_batch("INSERT INTO local_rows VALUES('a'),('b')")?;
        Ok::<_, DbError>(())
    })
    .await
    .unwrap();
    assert_eq!(tally(&db), 2);
    // The live query reading what the function wrote runs again.
    assert_eq!(next(&mut query).await, 2);
    db.close().await.unwrap();
}

#[tokio::test]
async fn an_applied_slot_runs_the_function_before_it_commits() {
    let store = TestStore::new();
    let db = open(&store, computing(&Arc::new(AtomicBool::new(false)))).await;
    write_note(&db, "n1", "milk").await.unwrap();
    let mut query = db.subscribe(|sql| {
        Ok::<_, DbError>(
            sql.query("SELECT title FROM shown ORDER BY id", [], |r| r.get::<_, String>(0))?,
        )
    });
    assert_eq!(next(&mut query).await, vec!["MILK".to_owned()]);
    remote_update(&db, "notes", "n1", &[("title", Value::Text("bread".into()))]).await;
    assert_eq!(shown(&db), shown_as("n1", "BREAD"));
    assert_eq!(next(&mut query).await, vec!["BREAD".to_owned()]);
    // This device's own write has landed, so the next update is delivered
    // alone: one slot, applied in one transaction that runs the function
    // once.
    let (runs, applied) = (count(&db, "runs"), db.applied_slot().await.unwrap());
    remote_update_beside_queue(&db, "notes", "n1", &[("title", Value::Text("eggs".into()))]).await;
    assert_eq!(db.applied_slot().await.unwrap(), applied + 1);
    assert_eq!(count(&db, "runs"), runs + 1);
    assert_eq!(shown(&db), shown_as("n1", "EGGS"));
    assert_eq!(next(&mut query).await, vec!["EGGS".to_owned()]);
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_failing_function_leaves_the_slot_unapplied_until_it_succeeds() {
    let store = TestStore::new();
    let failing = Arc::new(AtomicBool::new(false));
    let db = open(&store, computing(&failing)).await;
    write_note(&db, "n1", "milk").await.unwrap();
    let applied = db.applied_slot().await.unwrap();
    failing.store(true, Ordering::SeqCst);
    let title = [("title", Value::Text("bread".into()))];
    let refused = try_remote_update(&db, "notes", "n1", &title).await;
    assert!(matches!(refused, Err(DbError::Invariant { .. })), "{refused:?}");
    assert_eq!(db.applied_slot().await.unwrap(), applied);
    assert_eq!(shown(&db), shown_as("n1", "MILK"));
    failing.store(false, Ordering::SeqCst);
    remote_update(&db, "notes", "n1", &title).await;
    assert!(db.applied_slot().await.unwrap() > applied);
    assert_eq!(shown(&db), shown_as("n1", "BREAD"));
    db.close().await.unwrap();
}

/// A store whose joining write adds a note as its device registers.
async fn open_joining(store: &TestStore, failing: &Arc<AtomicBool>) -> StoreDatabase {
    store
        .builder(tables(), vec![Migration::addition(1, "notes", SCHEMA)])
        .joining_write(|sql, _| {
            sql.execute("INSERT INTO notes(id,title) VALUES('profile','dan')", [])?;
            Ok(())
        })
        .before_commit(computing(failing))
        .open()
        .await
        .unwrap()
}

#[tokio::test]
async fn the_slot_that_registers_this_device_runs_the_function_after_the_joining_write() {
    let store = TestStore::new();
    let db = open_joining(&store, &Arc::new(AtomicBool::new(false))).await;
    let runs = count(&db, "runs");
    db.admin_fixture(registration(), registered(&store, dan()), None).await.unwrap();
    assert_eq!(shown(&db), shown_as("profile", "DAN"));
    assert_eq!(count(&db, "runs"), runs + 1);
    db.close().await.unwrap();
}

async fn load(db: &StoreDatabase, state: Option<crate::StoreState>) -> Result<(), DbError> {
    let mut reload = crate::tests::reload::<std::io::Empty, std::io::Empty>(
        db,
        Vec::new(),
        Vec::new(),
        Arc::new(crate::test_utils::TestAuthorship),
    )
    .await;
    if let Some(state) = state {
        reload.state = state;
    }
    reload.snapshots = vec![crate::SnapshotSource::Empty(coven_merge::Audience::Store, 0)];
    db.load_snapshots::<crate::pending::tests::Record, std::io::Empty>(reload, None).await?;
    Ok(())
}

#[tokio::test]
async fn a_load_runs_the_function_before_it_commits() {
    let store = TestStore::new();
    let failing = Arc::new(AtomicBool::new(false));
    let db = open_joining(&store, &failing).await;
    let runs = count(&db, "runs");
    failing.store(true, Ordering::SeqCst);
    let refused = load(&db, None).await;
    assert!(matches!(refused, Err(DbError::Invariant { .. })), "{refused:?}");
    assert_eq!(count(&db, "runs"), runs);
    failing.store(false, Ordering::SeqCst);
    load(&db, None).await.unwrap();
    assert_eq!(count(&db, "runs"), runs + 1);
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_load_that_registers_this_device_runs_the_function_after_the_joining_write() {
    let store = TestStore::new();
    let db = open_joining(&store, &Arc::new(AtomicBool::new(false))).await;
    let runs = count(&db, "runs");
    load(&db, Some(registered(&store, dan()))).await.unwrap();
    assert_eq!(shown(&db), shown_as("profile", "DAN"));
    assert_eq!(count(&db, "runs"), runs + 1);
    db.close().await.unwrap();
}

#[tokio::test]
async fn the_function_writes_device_local_tables_only() {
    const INSERT: &str = "INSERT INTO notes(id,title) VALUES('n9','x') ON CONFLICT(id) DO NOTHING";
    let store = TestStore::new();
    let synced = Arc::new(AtomicBool::new(false));
    let db = open(&store, {
        let synced = synced.clone();
        move |sql| {
            if synced.load(Ordering::SeqCst) {
                sql.execute(INSERT, [])?;
            }
            Ok(())
        }
    })
    .await;
    synced.store(true, Ordering::SeqCst);
    let before = crate::tests::contents(&db);
    // The write runs the same statement first, which a write may: the
    // function's run of it is judged again, not taken from the write's.
    let refused = db
        .write(|sql| {
            sql.execute(INSERT, [])?;
            Ok(())
        })
        .await;
    assert!(
        matches!(&refused, Err(DbError::SyncedTableBeforeCommit { table }) if table == "notes"),
        "{refused:?}"
    );
    assert_eq!(crate::tests::contents(&db), before);
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_statement_the_function_lets_fail_still_fails_the_transaction() {
    let store = TestStore::new();
    let ignoring = Arc::new(AtomicBool::new(false));
    let db = open(&store, {
        let ignoring = ignoring.clone();
        move |sql| {
            if ignoring.load(Ordering::SeqCst) {
                let _ignored = sql.execute("INSERT INTO shown VALUES('n1', NULL)", []);
            }
            Ok(())
        }
    })
    .await;
    ignoring.store(true, Ordering::SeqCst);
    let refused = write_note(&db, "n1", "milk").await;
    assert!(matches!(refused, Err(DbError::StatementFailed)), "{refused:?}");
    assert_eq!(count(&db, "notes"), 0);
    db.close().await.unwrap();
}

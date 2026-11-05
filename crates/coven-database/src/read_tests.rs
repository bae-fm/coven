use crate::{
    test_utils::{notes_migrations, notes_tables},
    tests::TestStore,
    DbError,
};
use coven_foundation::clock::HANG_GUARD;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    mpsc, Arc,
};

#[tokio::test]
async fn read_is_lazy_and_readonly_opens_use_the_same_api() {
    let store = TestStore::new();
    let db = store.builder(notes_tables(), notes_migrations()).open().await.unwrap();
    db.commit_writer(|sql| {
        sql.batch("INSERT INTO notes VALUES ('f47ac10b-58cc-4372-a567-0e02b2c3d479','Plan','body')")
            .unwrap()
    });
    let calls = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&calls);
    let read = db.read(move |sql| {
        count.fetch_add(1, Ordering::SeqCst);
        Ok::<_, DbError>(sql.query_row(
            "SELECT title FROM notes WHERE id=?1",
            ["f47ac10b-58cc-4372-a567-0e02b2c3d479"],
            |r| r.get::<_, String>(0),
        )?)
    });
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(read.await.unwrap(), "Plan");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let ro = store.builder(notes_tables(), notes_migrations()).open_read_only().await.unwrap();
    assert_eq!(
        ro.read(|sql| Ok::<_, DbError>(
            sql.query_row("SELECT body FROM notes", [], |r| r.get::<_, String>(0))?
        ))
        .await
        .unwrap(),
        "body"
    );
    ro.close().await.unwrap();
    assert!(matches!(ro.read(|_| Ok(())).await, Err(DbError::StoreClosed)));
    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_is_fixed_before_the_closure_and_across_its_statements() {
    let store = TestStore::new();
    let db = store.builder(notes_tables(), notes_migrations()).open().await.unwrap();
    db.commit_writer(|sql| sql.batch("INSERT INTO notes VALUES ('a','old','')").unwrap());
    let (entered, entry) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let clone = db.clone();
    let task = tokio::spawn(async move {
        clone
            .read(move |sql| {
                entered.send(()).unwrap();
                released.recv_timeout(HANG_GUARD).unwrap();
                let first =
                    sql.query_row("SELECT title FROM notes", [], |r| r.get::<_, String>(0))?;
                let second =
                    sql.query_row("SELECT title FROM notes", [], |r| r.get::<_, String>(0))?;
                Ok::<_, DbError>((first, second))
            })
            .await
    });
    entry.recv_timeout(HANG_GUARD).unwrap();
    db.commit_writer(|sql| sql.batch("UPDATE notes SET title='new'").unwrap());
    release.send(()).unwrap();
    assert_eq!(task.await.unwrap().unwrap(), ("old".into(), "old".into()));
    assert_eq!(
        db.read(|sql| Ok::<_, DbError>(
            sql.query_row("SELECT title FROM notes", [], |r| r.get::<_, String>(0))?
        ))
        .await
        .unwrap(),
        "new"
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn read_context_refuses_writes_and_internal_tables_with_typed_errors() {
    let store = TestStore::new();
    let db = store.builder(notes_tables(), notes_migrations()).open().await.unwrap();
    for statement in [
        "INSERT INTO notes VALUES ('a','b','c') RETURNING title",
        "UPDATE notes SET title='changed' RETURNING title",
        "DELETE FROM notes RETURNING title",
        "CREATE TEMP TABLE temp_write (x)",
        "BEGIN",
        "COMMIT",
        "ROLLBACK",
        "PRAGMA user_version=9",
        "PRAGMA data_version",
        "ATTACH ':memory:' AS other",
    ] {
        let error =
            db.read(move |sql| Ok(sql.query(statement, [], |_| Ok(()))?)).await.unwrap_err();
        assert!(matches!(error, DbError::StatementForbidden { .. }), "{statement}: {error:?}");
    }
    for statement in
        ["SELECT * FROM _coven_lost", "SELECT * FROM _CoVeN_rows", "SELECT * FROM _coven_columns"]
    {
        let error =
            db.read(move |sql| Ok(sql.query(statement, [], |_| Ok(()))?)).await.unwrap_err();
        assert!(matches!(error, DbError::InternalTable { .. }), "{error:?}");
    }
    assert_eq!(
        db.read(|sql| Ok::<_, DbError>(
            sql.query_row("SELECT count(*) FROM notes", [], |r| r.get::<_, i64>(0))?
        ))
        .await
        .unwrap(),
        0
    );
    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_releases_all_reader_leases_and_store_borrows() {
    let store = TestStore::new();
    let db = store.builder(notes_tables(), notes_migrations()).open().await.unwrap();
    let (entered, entry) = mpsc::channel();
    let mut releases = Vec::new();
    let mut tasks = Vec::new();
    for _ in 0..4 {
        let clone = db.clone();
        let entered = entered.clone();
        let (release, released) = mpsc::channel();
        releases.push(release);
        tasks.push(tokio::spawn(async move {
            clone
                .read(|sql| {
                    Ok::<_, DbError>(
                        sql.query_row("SELECT count(*) FROM notes", [], |r| r.get::<_, i64>(0))?,
                    )
                })
                .process(move |value| {
                    entered.send(()).unwrap();
                    released.recv_timeout(HANG_GUARD).unwrap();
                    Ok(value + 1)
                })
                .await
        }));
    }
    for _ in 0..4 {
        entry.recv_timeout(HANG_GUARD).unwrap();
    }
    assert_eq!(
        tokio::time::timeout(HANG_GUARD, db.read(|_| Ok::<_, DbError>(42))).await.unwrap().unwrap(),
        42
    );
    tokio::time::timeout(HANG_GUARD, db.close()).await.unwrap().unwrap();
    for release in releases {
        release.send(()).unwrap();
    }
    for task in tasks {
        assert_eq!(task.await.unwrap().unwrap(), 1);
    }
}

#[tokio::test]
async fn a_panicking_read_releases_its_snapshot_and_keeps_the_reader_usable() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let error = db.read(|_| -> Result<(), DbError> { panic!("app read panic") }).await.unwrap_err();
    assert!(read_bug(&error, "app read panic"), "{error:?}");
    assert_eq!(
        db.read(|sql| Ok::<_, DbError>(sql.query_row("SELECT 42", [], |r| r.get::<_, i64>(0))?))
            .await
            .unwrap(),
        42
    );
    db.close().await.unwrap();

    let ro = store.builder(vec![], vec![]).open_read_only().await.unwrap();
    let error =
        ro.read(|_| -> Result<(), DbError> { panic!("read-only panic") }).await.unwrap_err();
    assert!(read_bug(&error, "read-only panic"), "{error:?}");
    assert_eq!(
        ro.read(|sql| Ok::<_, DbError>(sql.query_row("SELECT 42", [], |r| r.get::<_, i64>(0))?))
            .await
            .unwrap(),
        42
    );
    ro.close().await.unwrap();
}

/// Whether `error` is the bug of a read whose closure panicked with `message`.
fn read_bug(error: &DbError, message: &str) -> bool {
    matches!(error, DbError::Bug(bug) if bug.operation == "read" && bug.message == message)
}

fn rollback_failed<T>(result: Result<T, DbError>) -> bool {
    matches!(result, Err(DbError::RollbackFailed))
}

#[tokio::test]
async fn a_failed_snapshot_rollback_fails_the_handle_until_reopen() {
    // A read that succeeds reports the failed ROLLBACK; one that fails keeps its cause.
    for read_fails in [false, true] {
        let store = TestStore::new();
        let open = || store.builder(notes_tables(), notes_migrations()).open();
        let db = open().await.unwrap();
        let mut notes = db.subscribe(|sql| {
            Ok(sql.query("SELECT title FROM notes", [], |r| r.get::<_, String>(0))?)
        });
        assert!(notes.next().await.expect("the live query answers").unwrap().is_empty());
        db.inspect_readers(|reader| reader.refuse_transaction_end(true));
        let error = db
            .read(move |sql| {
                sql.query_row("SELECT 1", [], |r| r.get::<_, i64>(0))?;
                if read_fails {
                    Err(DbError::DamagedDatabase)
                } else {
                    Ok(())
                }
            })
            .await
            .unwrap_err();
        if read_fails {
            assert!(matches!(error, DbError::DamagedDatabase), "{error:?}");
        } else {
            assert!(matches!(error, DbError::Sqlite(_)), "{error:?}");
        }
        assert!(rollback_failed(db.read(|_| Ok(())).await));
        assert!(matches!(db.schema_version().await, Err(DbError::RollbackFailed)));
        assert!(matches!(db.write(|_| Ok(())).await, Err(DbError::RollbackFailed)));
        assert!(rollback_failed(notes.next().await.expect("the live query answers")));
        db.close().await.unwrap();
        let db = open().await.unwrap();
        assert!(db.read(|_| Ok::<_, DbError>(())).await.is_ok());
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn a_panicking_read_whose_rollback_fails_keeps_its_bug_and_fails_the_handle() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    db.inspect_readers(|reader| reader.refuse_transaction_end(true));
    let error = db.read(|_| -> Result<(), DbError> { panic!("app read panic") }).await.unwrap_err();
    assert!(read_bug(&error, "app read panic"), "{error:?}");
    assert!(rollback_failed(db.read(|_| Ok(())).await));
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_read_only_handle_fails_after_any_of_its_connections_fails_to_roll_back() {
    let store = TestStore::new();
    let open = || store.builder(notes_tables(), notes_migrations());
    open().open().await.unwrap().close().await.unwrap();
    // A reader ending its snapshot.
    let ro = open().open_read_only().await.unwrap();
    ro.inspect_readers(|reader| reader.refuse_transaction_end(true));
    assert!(matches!(ro.read(|_| Ok(())).await, Err(DbError::Sqlite(_))));
    assert!(rollback_failed(ro.read(|_| Ok(())).await));
    assert!(matches!(ro.schema_version().await, Err(DbError::RollbackFailed)));
    ro.close().await.unwrap();
    // The cache connection rolling back a transaction whose COMMIT failed.
    let ro = open().open_read_only().await.unwrap();
    ro.inspect_writer(|writer| writer.refuse_transaction_end(true));
    let files = crate::FileDatabase::new(ro.clone());
    let error = files.trim_cache("files").await.unwrap_err();
    assert!(matches!(error, DbError::Rollback { .. }), "{error:?}");
    assert!(matches!(files.trim_cache("files").await, Err(DbError::RollbackFailed)));
    assert!(rollback_failed(ro.read(|_| Ok(())).await));
    ro.close().await.unwrap();
    let ro = open().open_read_only().await.unwrap();
    assert!(ro.read(|_| Ok::<_, DbError>(())).await.is_ok());
    ro.close().await.unwrap();
}

/// An app's own error type, as an app declares one for its reads and
/// writes: coven's failures convert into it, its own refusals sit beside
/// them, and a write's failed rollback or byte removal stays beside the
/// refusal it followed.
#[derive(Debug, thiserror::Error)]
pub(crate) enum AppError {
    #[error(transparent)]
    Database(DbError),
    #[error("refused: {0}")]
    Refused(&'static str),
    #[error("{operation}; rollback failed: {rollback}")]
    Rollback {
        operation: Box<AppError>,
        #[source]
        rollback: rusqlite::Error,
    },
    #[error("file cleanup failed: {failures:?}; write failed: {write}")]
    FileCleanup { write: Box<AppError>, failures: Vec<DbError> },
}

impl From<DbError> for AppError {
    fn from(error: DbError) -> Self {
        Self::Database(error)
    }
}

impl From<rusqlite::Error> for AppError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Database(error.into())
    }
}

impl coven_foundation::task::CarriesBug for AppError {
    fn bug(&self) -> Option<&coven_foundation::task::Bug> {
        match self {
            Self::Database(error) => error.bug(),
            Self::Refused(_) => None,
            Self::Rollback { operation, .. } => operation.bug(),
            Self::FileCleanup { write, failures } => {
                write.bug().or_else(|| failures.iter().find_map(DbError::bug))
            }
        }
    }
}

impl crate::WriteFailure for AppError {
    fn with_rollback(self, rollback: rusqlite::Error) -> Self {
        match self {
            Self::Database(error) => Self::Database(error.with_rollback(rollback)),
            error => Self::Rollback { operation: Box::new(error), rollback },
        }
    }
    fn with_cleanup(write: Self, failures: Vec<DbError>) -> Self {
        match write {
            Self::Database(error) => Self::Database(DbError::with_cleanup(error, failures)),
            error => Self::FileCleanup { write: Box::new(error), failures },
        }
    }
}

/// A read whose closure refuses with the app's own error returns that
/// error as it was, and `process` fails with the same type.
#[tokio::test]
async fn a_read_fails_with_its_closures_own_error() {
    let store = TestStore::new();
    let db = store.builder(notes_tables(), notes_migrations()).open().await.unwrap();
    let refused = db.read(|_| Err::<(), _>(AppError::Refused("no"))).await;
    assert!(matches!(refused, Err(AppError::Refused("no"))), "{refused:?}");
    let processed = db
        .read(|sql| {
            Ok::<_, AppError>(
                sql.query_row("SELECT count(*) FROM notes", [], |r| r.get::<_, i64>(0))?,
            )
        })
        .process(|count| if count == 0 { Err(AppError::Refused("empty")) } else { Ok(count) })
        .await;
    assert!(matches!(processed, Err(AppError::Refused("empty"))), "{processed:?}");
    db.close().await.unwrap();
    let closed = db.read(|_| Ok::<_, AppError>(())).await;
    assert!(matches!(closed, Err(AppError::Database(DbError::StoreClosed))), "{closed:?}");
}

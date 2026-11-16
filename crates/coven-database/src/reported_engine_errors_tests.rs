use crate::store_failure::StoreFailure;
use crate::DbError;
use coven_foundation::task::Bug;

/// A handle that failed on a bug, a checkpoint's panic among them, fails
/// every later call with `StoreFailed` carrying that bug (§4).
#[test]
fn a_handle_failed_on_a_bug_fails_the_store_with_it() {
    let bug = Bug::new("sqlive WAL checkpoint", "the checkpoint broke");
    let failed = DbError::from(sqlive::Error::<StoreFailure, DbError>::HandleFailed(bug.clone()));
    assert!(matches!(&failed, DbError::StoreFailed(found) if *found == bug), "{failed:?}");
}

/// When SQLite fails a checkpoint, the store fails every later call with
/// `CheckpointFailed`, which carries SQLite's cause (§4).
#[test]
fn a_failed_checkpoint_fails_the_store_with_its_cause() {
    let cause = sqlive::Error::Sqlite(sqlive::rusqlite::Error::SqliteFailure(
        sqlive::rusqlite::ffi::Error::new(sqlive::rusqlite::ffi::SQLITE_IOERR),
        None,
    ));
    let failure = StoreFailure::from(sqlive::EngineFailure::Checkpoint(cause));
    let failed = DbError::from(sqlive::Error::<StoreFailure, DbError>::Host(failure));
    let DbError::CheckpointFailed(cause) = &failed else {
        panic!("{failed:?}");
    };
    assert!(
        matches!(
            cause.as_ref(),
            DbError::Sqlite(sqlive::rusqlite::Error::SqliteFailure(failure, _))
                if failure.code == sqlive::rusqlite::ErrorCode::SystemIoFailure
        ),
        "{cause:?}"
    );
}

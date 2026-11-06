use super::*;

fn bug() -> Bug {
    Bug { operation: "read the bytes", message: "the read panicked".into() }
}

fn refused() -> SqliteError {
    SqliteError::ExecuteReturnedResults
}

/// A panic in custody's, the lock's or a sync step's work is the call's
/// own bug (api.md E15).
#[test]
fn a_bug_in_custody_lock_or_sync_work_is_the_calls_bug() {
    for error in [
        CovenError::from(KeyError::Bug(bug())),
        CovenError::from(StoreLockError::Bug(bug())),
        CovenError::from(OpenError::Bug(bug())),
        CovenError::from(SyncError::Bug(bug())),
    ] {
        assert!(matches!(&error, CovenError::Bug(found) if *found == bug()), "{error:?}");
        assert_eq!(error.bug(), Some(&bug()));
    }
}

/// A database call's bug stays the call's own inside `CovenError`, as its
/// other failures do (api.md E15).
#[test]
fn a_database_calls_bug_stays_under_database() {
    for error in [
        CovenError::from(DbError::Bug(bug())),
        CovenError::from(OpenError::Database(DbError::Bug(bug()))),
        CovenError::from(SyncError::Database(DbError::Bug(bug()))),
    ] {
        assert!(
            matches!(&error, CovenError::Database(DbError::Bug(found)) if *found == bug()),
            "{error:?}"
        );
        assert_eq!(error.bug(), Some(&bug()));
    }
}

/// Opening's failures keep the names the app matches (E1).
#[test]
fn opening_keeps_each_cause_under_its_own_name() {
    let store = StoreId(uuid::Uuid::from_u128(1));
    assert!(matches!(
        CovenError::from(OpenError::from(StoreLockError::AlreadyOpen(store))),
        CovenError::Lock(StoreLockError::AlreadyOpen(found)) if found == store
    ));
    assert!(matches!(
        CovenError::from(OpenError::MissingConfiguration { field: "migrations" }),
        CovenError::MissingConfiguration { field: "migrations" }
    ));
    assert!(matches!(
        CovenError::from(OpenError::Database(DbError::DamagedDatabase)),
        CovenError::Database(DbError::DamagedDatabase)
    ));
}

/// A sync step's refusal stays sync's own error, and a failed rollback
/// after it stays inside that error, so `CovenError::Sync` still holds the
/// step's refusal (§4, Appendix C.3).
#[test]
fn a_sync_steps_refusal_keeps_a_failed_rollback_in_its_own_error() {
    let error = CovenError::from(SyncError::NoStorage);
    assert!(matches!(error, CovenError::Sync(SyncError::NoStorage)), "{error:?}");
    let error = error.with_rollback(refused());
    let CovenError::Sync(SyncError::Cleanup { operation, .. }) = &error else {
        panic!("the rollback left sync's error: {error:?}")
    };
    assert!(matches!(**operation, SyncError::NoStorage), "{operation:?}");
}

/// A bug in reading the store's settings is the call's own bug, not a
/// settings failure, and one a settings error holds is still found
/// (api.md E15).
#[test]
fn a_bug_reading_settings_is_the_calls_bug() {
    let settings = || crate::SettingsError::from(crate::FileError::Bug(bug()));
    for error in [CovenError::from(settings()), CovenError::from(OpenError::from(settings()))] {
        assert!(matches!(&error, CovenError::Bug(found) if *found == bug()), "{error:?}");
        assert_eq!(error.bug(), Some(&bug()));
    }
    let held = CovenError::Settings(crate::SettingsError::File(crate::FileError::Bug(bug())));
    assert_eq!(held.bug(), Some(&bug()));
}

use super::*;
use crate::LocalFileError;
use coven_crypto::custody::KeyError;
use coven_foundation::files::FileError;

fn bug() -> Bug {
    Bug { operation: "read the bytes", message: "the read panicked".into() }
}

/// A panic in file work a database call ran is the call's bug, not a disk
/// failure, and custody's or a file read's the same.
#[test]
fn a_bug_in_file_or_custody_work_is_each_errors_bug() {
    assert!(matches!(DbError::from(FileError::Bug(bug())), DbError::Bug(found) if found == bug()));
    let writer = coven_foundation::files::WriterError::File(FileError::Bug(bug()));
    assert!(matches!(DbError::from(writer), DbError::Bug(found) if found == bug()));
    assert!(
        matches!(CovenError::from(KeyError::Bug(bug())), CovenError::Bug(found) if found == bug())
    );
    for local in [
        LocalFileError::from(FileError::Bug(bug())),
        LocalFileError::from(DbError::Bug(bug())),
        LocalFileError::from(StoreLockError::Bug(bug())),
    ] {
        assert!(matches!(&local, LocalFileError::Bug(found) if *found == bug()), "{local:?}");
        assert_eq!(local.bug(), Some(&bug()));
    }
}

/// A database call's bug stays the call's own inside `CovenError`, as its
/// other failures do (api.md E15).
#[test]
fn a_database_calls_bug_stays_under_database() {
    let error = CovenError::from(DbError::Bug(bug()));
    assert!(matches!(&error, CovenError::Database(DbError::Bug(found)) if *found == bug()));
    assert_eq!(error.bug(), Some(&bug()));
}

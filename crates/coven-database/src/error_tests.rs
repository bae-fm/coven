use super::*;
use crate::LocalFileError;
use coven_foundation::files::FileError;

fn bug() -> Bug {
    Bug { operation: "read the bytes", message: "the read panicked".into() }
}

/// A panic in file work a database call ran is the call's bug, not a disk
/// failure, and taking the lock to open or a file read's the same.
#[test]
fn a_bug_in_file_or_custody_work_is_each_errors_bug() {
    assert!(matches!(DbError::from(FileError::Bug(bug())), DbError::Bug(found) if found == bug()));
    let writer = coven_foundation::files::WriterError::File(FileError::Bug(bug()));
    assert!(matches!(DbError::from(writer), DbError::Bug(found) if found == bug()));
    let open = OpenError::from(StoreLockError::Bug(bug()));
    assert!(matches!(&open, OpenError::Bug(found) if *found == bug()), "{open:?}");
    assert_eq!(open.bug(), Some(&bug()));
    for local in [
        LocalFileError::from(FileError::Bug(bug())),
        LocalFileError::from(DbError::Bug(bug())),
        LocalFileError::from(StoreLockError::Bug(bug())),
    ] {
        assert!(matches!(&local, LocalFileError::Bug(found) if *found == bug()), "{local:?}");
        assert_eq!(local.bug(), Some(&bug()));
    }
}

/// A bug in reading the store's settings while opening is opening's own
/// bug, not a settings failure (api.md E15).
#[test]
fn a_bug_reading_settings_is_openings_bug() {
    let settings = SettingsError::from(FileError::Bug(bug()));
    assert!(matches!(&settings, SettingsError::Bug(found) if *found == bug()), "{settings:?}");
    let open = OpenError::from(settings);
    assert!(matches!(&open, OpenError::Bug(found) if *found == bug()), "{open:?}");
    assert_eq!(open.bug(), Some(&bug()));
    let file = OpenError::Settings(SettingsError::File(FileError::Bug(bug())));
    assert_eq!(file.bug(), Some(&bug()));
}

use super::*;

#[test]
fn a_crash_after_writing_the_temporary_sibling_leaves_the_old_target() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("settings");
    let file = AtomicFile::new(path.clone());
    file.replace(b"old").unwrap();
    let stage = prepare(&path, b"new").unwrap();
    assert_eq!(fs::read(stage.path()).unwrap(), b"new");
    // Reopen without renaming or deleting the synced temporary sibling.
    assert_eq!(
        AtomicFile::new(path).read_optional().unwrap().unwrap(),
        b"old"
    );
    stage.close().unwrap();
    file.replace(b"new").unwrap();
    assert_eq!(file.read_optional().unwrap().unwrap(), b"new");
    assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 1);
}

#[test]
fn missing_and_unreadable_are_distinct() {
    let directory = tempfile::tempdir().unwrap();
    assert_eq!(
        AtomicFile::new(directory.path().join("absent"))
            .read_optional()
            .unwrap(),
        None
    );
    assert!(matches!(
        AtomicFile::new(directory.path().to_owned()).read_optional(),
        Err(FileError::Io { .. })
    ));
}

#[test]
fn failed_rename_preserves_the_target_and_removes_the_stage() {
    let directory = tempfile::tempdir().unwrap();
    let target = directory.path().join("occupied");
    fs::create_dir(&target).unwrap();
    let error = AtomicFile::new(target.clone()).replace(b"new").unwrap_err();
    assert!(!error.installed_new_bytes());
    assert!(target.is_dir());
    assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 1);
}

#[test]
fn a_directory_sync_error_reports_that_the_bytes_are_installed() {
    let error = FileError::AfterReplace {
        path: PathBuf::from("settings"),
        source: io::Error::other("directory sync failed"),
    };
    assert!(error.installed_new_bytes());
    assert!(std::error::Error::source(&error).is_some());
}

#[test]
fn concurrent_writers_and_readers_observe_whole_versions() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let directory = tempfile::tempdir().unwrap();
    let file = AtomicFile::new(directory.path().join("shared"));
    file.replace(&vec![0; 32768]).unwrap();
    let completed = AtomicUsize::new(0);
    std::thread::scope(|scope| {
        let mut writers = Vec::new();
        for value in 1..=4 {
            let file = &file;
            let completed = &completed;
            writers.push(scope.spawn(move || {
                let result = (0..12).try_for_each(|_| file.replace(&vec![value; 32768]));
                completed.fetch_add(1, Ordering::Release);
                result
            }));
        }
        loop {
            let bytes = file.read_optional().unwrap().unwrap();
            assert_eq!(bytes.len(), 32768);
            assert!(bytes[0] <= 4);
            assert!(bytes.iter().all(|byte| *byte == bytes[0]));
            if completed.load(Ordering::Acquire) == 4 {
                break;
            }
        }
        for writer in writers {
            writer.join().unwrap().unwrap();
        }
    });
    assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 1);
}

#[test]
fn replacement_supports_paths_past_the_windows_path_limit() {
    let directory = tempfile::tempdir().unwrap();
    let mut parent = directory.path().to_owned();
    while parent.as_os_str().len() < 300 {
        parent.push("long-directory-component");
    }
    fs::create_dir_all(&parent).unwrap();
    let file = AtomicFile::new(parent.join("settings"));
    file.replace(b"first").unwrap();
    file.replace(b"second").unwrap();
    assert_eq!(file.read_optional().unwrap().unwrap(), b"second");
}

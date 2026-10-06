use super::*;

#[test]
fn removal_is_idempotent_and_does_not_hide_operating_system_errors() {
    let directory = tempfile::tempdir().unwrap();
    let file = AtomicFile::new(directory.path().join("secret"));
    file.remove().unwrap();
    file.replace(b"secret").unwrap();
    file.remove().unwrap();
    file.remove().unwrap();
    assert!(file.read_optional().unwrap().is_none());
    let error = AtomicFile::new(directory.path().to_owned())
        .remove()
        .unwrap_err();
    assert!(matches!(error, FileError::Io { .. }));
    AtomicFile::new(directory.path().join("absent-parent/file"))
        .remove()
        .unwrap();
}

#[test]
fn a_crash_after_writing_the_temporary_sibling_leaves_the_old_target() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("settings");
    let file = AtomicFile::new(path.clone());
    file.replace(b"old").unwrap();
    let stage = prepare(&path, b"new").unwrap();
    assert_eq!(read_optional(stage.path()).unwrap().unwrap(), b"new");
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
fn replacement_keeps_open_versions_readable_and_the_name_available() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("shared");
    let file = AtomicFile::new(path.clone());
    let mut readers = Vec::new();
    for value in 0..4 {
        let bytes = vec![value; 32768];
        file.replace(&bytes).unwrap();
        assert_eq!(file.read_optional().unwrap().unwrap(), bytes);
        readers.push(open_reader(&path).unwrap());
        // Earlier versions still have open handles, but only the current
        // version has a name in this directory.
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 1);
    }
    for (value, mut reader) in readers.into_iter().enumerate() {
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, vec![value as u8; 32768]);
    }
}

#[test]
fn a_nul_in_the_destination_cannot_replace_a_different_file() {
    let directory = tempfile::tempdir().unwrap();
    let original = AtomicFile::new(directory.path().join("settings"));
    original.replace(b"old").unwrap();
    let invalid = AtomicFile::new(directory.path().join("settings\0suffix"));
    let error = invalid.replace(b"new").unwrap_err();
    assert!(matches!(error, FileError::Io { .. }));
    assert!(!error.installed_new_bytes());
    assert_eq!(original.read_optional().unwrap().unwrap(), b"old");
    assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 1);
}

#[cfg(windows)]
#[test]
fn a_reader_refusing_delete_sharing_preserves_the_target_on_failure() {
    use std::os::windows::fs::OpenOptionsExt;
    use windows_sys::Win32::Storage::FileSystem::{FILE_SHARE_READ, FILE_SHARE_WRITE};

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("shared");
    let file = AtomicFile::new(path.clone());
    file.replace(b"old").unwrap();
    let reader = fs::OpenOptions::new()
        .read(true)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
        .open(&path)
        .unwrap();
    let error = file.replace(b"new").unwrap_err();
    assert!(matches!(error, FileError::Io { .. }));
    assert!(!error.installed_new_bytes());
    assert_eq!(file.read_optional().unwrap().unwrap(), b"old");
    assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 1);
    drop(reader);
    file.replace(b"new").unwrap();
    assert_eq!(file.read_optional().unwrap().unwrap(), b"new");
}

#[test]
fn replacement_supports_paths_past_the_windows_path_limit() {
    let directory = tempfile::tempdir().unwrap();
    let mut parent = directory.path().to_owned();
    while parent.as_os_str().len() < 300 {
        parent.push("long-directory-component");
    }
    fs::create_dir_all(&parent).unwrap();
    let file = AtomicFile::new(parent.join("settings-音楽-🎵"));
    file.replace(b"first").unwrap();
    file.replace(b"second").unwrap();
    assert_eq!(file.read_optional().unwrap().unwrap(), b"second");
}

#[cfg(windows)]
#[test]
fn a_published_file_is_not_marked_temporary() {
    use std::os::windows::fs::MetadataExt;
    use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_TEMPORARY;

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("settings");
    AtomicFile::new(path.clone()).replace(b"durable").unwrap();
    assert_eq!(
        fs::metadata(path).unwrap().file_attributes() & FILE_ATTRIBUTE_TEMPORARY,
        0
    );
}

#[test]
fn streamed_creation_syncs_bytes_and_refuses_to_overwrite_a_kept_file() {
    let directory = tempfile::tempdir().unwrap();
    let file = AtomicFile::new(directory.path().join("kept"));
    let count = file
        .create(|out| {
            for _ in 0..10 {
                out.write_all(&[1; 4096])?;
            }
            Ok(40960)
        })
        .unwrap();
    assert_eq!(file.read_optional().unwrap().unwrap(), vec![1; count]);
    let error = file.create(|out| out.write_all(b"different")).unwrap_err();
    assert!(
        matches!(error, FileError::Io {source, ..} if source.kind() == io::ErrorKind::AlreadyExists)
    );
    assert_eq!(file.read_optional().unwrap().unwrap(), vec![1; count]);
}

#[test]
fn failed_or_panicking_streams_leave_the_named_bytes_for_the_callers_recorded_cleanup() {
    let directory = tempfile::tempdir().unwrap();
    let file = AtomicFile::new(directory.path().join("partial"));
    let error = file
        .create(|out| {
            out.write_all(b"partial")?;
            Err::<(), _>(io::Error::other("source failed"))
        })
        .unwrap_err();
    assert!(matches!(error, FileError::Io { .. }));
    assert_eq!(file.read_optional().unwrap().unwrap(), b"partial");
    file.remove().unwrap();
    let failure = std::panic::catch_unwind(|| {
        file.create(|out| -> io::Result<()> {
            out.write_all(b"partial")?;
            panic!("source panicked");
        })
    });
    assert!(failure.is_err());
    assert_eq!(file.read_optional().unwrap().unwrap(), b"partial");
    file.remove().unwrap();
    assert!(file.read_optional().unwrap().is_none());
}

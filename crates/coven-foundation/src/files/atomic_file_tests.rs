use super::*;
use crate::files::StoreDirAccess;

#[test]
fn existence_checks_directory_entries_and_preserves_inspection_errors() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("file");
    let file = AtomicFile::new(path.clone());
    assert!(!exists("inspect", &path).unwrap());
    file.replace(b"present").unwrap();
    assert!(exists("inspect", &path).unwrap());
    assert!(exists("inspect", directory.path()).unwrap());
    let invalid = directory.path().join("file\0suffix");
    assert!(matches!(
        exists("inspect store destination", &invalid),
        Err(FileError::Io { operation: "inspect store destination", path, source })
            if path == invalid && source.downcast_ref::<std::io::Error>().unwrap().kind() == io::ErrorKind::InvalidInput
    ));
    #[cfg(unix)]
    {
        let path = directory.path().join("dangling");
        std::os::unix::fs::symlink(directory.path().join("absent"), &path).unwrap();
        assert!(exists("inspect", &path).unwrap());
    }
}

#[test]
fn removal_is_idempotent_and_does_not_hide_operating_system_errors() {
    let directory = tempfile::tempdir().unwrap();
    let file = AtomicFile::new(directory.path().join("secret"));
    file.remove().unwrap();
    file.replace(b"secret").unwrap();
    file.remove().unwrap();
    file.remove().unwrap();
    assert!(file.read_optional().unwrap().is_none());
    let error = AtomicFile::new(directory.path().to_owned()).remove().unwrap_err();
    assert!(matches!(error, FileError::Io { .. }));
    AtomicFile::new(directory.path().join("absent-parent/file")).remove().unwrap();
}

#[test]
fn a_crash_after_writing_the_temporary_sibling_leaves_the_old_target() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("settings");
    let file = AtomicFile::new(path.clone());
    file.replace(b"old").unwrap();
    let stage = prepare(&path, b"new", TEMPORARY).unwrap();
    assert_eq!(read_optional(stage.path()).unwrap().unwrap(), b"new");
    // Reopen without renaming or deleting the synced temporary sibling.
    assert_eq!(AtomicFile::new(path).read_optional().unwrap().unwrap(), b"old");
    stage.close().unwrap();
    file.replace(b"new").unwrap();
    assert_eq!(file.read_optional().unwrap().unwrap(), b"new");
    assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 1);
}

/// A crash left two whole versions of `store-keys.sealed` beside it, and
/// another file's beside that. Removing the file with its temporaries takes
/// its own and leaves the other's, and an absent file with none is no error.
#[test]
fn removing_with_temporaries_takes_the_files_own_and_no_others() {
    let directory = tempfile::tempdir().unwrap();
    let file = AtomicFile::new(directory.path().join("store-keys.sealed"));
    let other = AtomicFile::new(directory.path().join("store-keys.sealed-2"));
    file.replace(b"keys").unwrap();
    other.replace(b"others").unwrap();
    let first = file.leave_temporary(b"older keys");
    assert_ne!(first, file.leave_temporary(b"oldest keys"), "each writer has its own name");
    let theirs = other.leave_temporary(b"other keys");
    file.remove_with_temporaries().unwrap();
    let mut left: Vec<_> =
        fs::read_dir(directory.path()).unwrap().map(|entry| entry.unwrap().file_name()).collect();
    left.sort();
    assert_eq!(left, [theirs.file_name().unwrap(), "store-keys.sealed-2".as_ref()]);
    file.remove_with_temporaries().unwrap();
}

/// A crash left a temporary sibling, and a failed rename whose removal failed
/// too left another. Both go; the live file and a name that only looks close
/// stay. One that can't be removed fails loudly.
#[cfg(unix)]
#[test]
fn removing_temporaries_takes_what_writes_left_and_nothing_else() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("settings");
    let file = AtomicFile::new(path.clone());
    file.replace(b"live").unwrap();
    fs::write(directory.path().join(".coven-writer"), b"another's").unwrap();
    file.leave_temporary(b"crash");
    let temp = prepare(&path, b"failed", TEMPORARY).unwrap();
    set_mode(directory.path(), 0o500);
    assert!(matches!(install(temp, &path), Err(FileError::Cleanup { .. })));
    set_mode(directory.path(), 0o700);
    assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 4);
    remove_temporaries(directory.path()).unwrap();
    assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 2);
    assert_eq!(file.read_optional().unwrap().unwrap(), b"live");
    fs::create_dir(directory.path().join(".coven-write-stuck")).unwrap();
    assert!(matches!(remove_temporaries(directory.path()), Err(FileError::Io { .. })));
}

#[test]
fn missing_and_unreadable_are_distinct() {
    let directory = tempfile::tempdir().unwrap();
    assert_eq!(AtomicFile::new(directory.path().join("absent")).read_optional().unwrap(), None);
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

/// Set `directory`'s permission bits.
#[cfg(unix)]
fn set_mode(directory: &Path, mode: u32) {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(directory, fs::Permissions::from_mode(mode)).unwrap();
}

/// A folder that can be written but not opened: renames and removals in it
/// work, and syncing it fails, since syncing opens it first.
#[cfg(unix)]
const UNOPENABLE: u32 = 0o300;

#[cfg(unix)]
#[test]
fn a_failed_directory_sync_after_replacing_leaves_the_new_bytes_visible() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("settings");
    let file = AtomicFile::new(path.clone());
    file.replace(b"old").unwrap();
    set_mode(directory.path(), UNOPENABLE);
    let error = file.replace(b"new").unwrap_err();
    set_mode(directory.path(), 0o700);
    assert!(error.installed_new_bytes());
    assert!(std::error::Error::source(&error).is_some());
    assert!(matches!(&error, FileError::AfterReplace { path: named, .. } if *named == path));
    assert_eq!(file.read_optional().unwrap().unwrap(), b"new");
    assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 1, "no temporary sibling is left");
}

#[cfg(unix)]
#[test]
fn a_failed_directory_sync_after_removing_leaves_the_file_gone_and_can_be_retried() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("settings");
    let file = AtomicFile::new(path.clone());
    file.replace(b"old").unwrap();
    set_mode(directory.path(), UNOPENABLE);
    let error = file.remove().unwrap_err();
    set_mode(directory.path(), 0o700);
    assert!(!error.installed_new_bytes());
    assert!(std::error::Error::source(&error).is_some());
    assert!(matches!(&error, FileError::AfterRemove { path: named, .. } if *named == path));
    assert_eq!(file.read_optional().unwrap(), None);
    file.remove().unwrap();
}

#[cfg(unix)]
#[test]
fn a_failed_removal_of_the_temporary_file_keeps_both_causes_and_the_old_target() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("settings");
    let file = AtomicFile::new(path.clone());
    file.replace(b"old").unwrap();
    let temp = prepare(&path, b"new", TEMPORARY).unwrap();
    // Neither the rename nor removing the temporary file can change the folder.
    set_mode(directory.path(), 0o500);
    let error = install(temp, &path).unwrap_err();
    set_mode(directory.path(), 0o700);
    assert!(!error.installed_new_bytes());
    let FileError::Cleanup { operation, cleanup } = error else {
        panic!("both failures, not {error:?}");
    };
    assert!(matches!(*operation, FileError::Io { operation: "rename temporary file", .. }));
    let cleanup = cleanup.downcast_ref::<io::Error>().expect("the operating system's cause");
    assert_eq!(cleanup.kind(), io::ErrorKind::PermissionDenied);
    assert_eq!(file.read_optional().unwrap().unwrap(), b"old");
    file.replace(b"new").unwrap();
    assert_eq!(file.read_optional().unwrap().unwrap(), b"new");
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
    assert_eq!(fs::metadata(path).unwrap().file_attributes() & FILE_ATTRIBUTE_TEMPORARY, 0);
}

#[tokio::test]
async fn streamed_creation_syncs_bytes_and_refuses_to_overwrite_a_kept_file() {
    let directory = tempfile::tempdir().unwrap();
    let store = crate::files::StoreDirectories::new(
        directory.path().to_owned(),
        crate::task::HeardBugs::shared(),
    )
    .create_store_dir(
        crate::id_source::StoreId(uuid::Uuid::from_u128(123)),
        &crate::id_source::UuidIds,
    )
    .unwrap();
    let name = crate::files::FileName::new("kept").unwrap();
    let file = store.file(crate::files::FileArea::Cache, &name);
    let bytes = vec![1; 40960];
    let mut count = 0;
    let mut batch = crate::files::FlushBatch::new(crate::task::HeardBugs::shared());
    store
        .create_writer(crate::files::FileArea::Cache, &name)
        .unwrap()
        .write_from(&mut bytes.as_slice(), |chunk| count += chunk.len(), &mut batch)
        .await
        .unwrap();
    let _flushed = batch.flush().await.unwrap();
    assert_eq!(count, bytes.len());
    assert_eq!(file.read_optional().unwrap().unwrap(), bytes);
    let error = store.create_writer(crate::files::FileArea::Cache, &name).err().unwrap();
    assert!(
        matches!(error, crate::files::WriterError::File(FileError::Io {source, ..}) if source.downcast_ref::<std::io::Error>().unwrap().kind() == io::ErrorKind::AlreadyExists)
    );
    assert_eq!(file.read_optional().unwrap().unwrap(), bytes);
}

#[tokio::test]
async fn a_dropped_writer_leaves_its_name_for_recorded_cleanup() {
    let directory = tempfile::tempdir().unwrap();
    let store = crate::files::StoreDirectories::new(
        directory.path().to_owned(),
        crate::task::HeardBugs::shared(),
    )
    .create_store_dir(
        crate::id_source::StoreId(uuid::Uuid::from_u128(123)),
        &crate::id_source::UuidIds,
    )
    .unwrap();
    let name = crate::files::FileName::new("partial").unwrap();
    let file = store.file(crate::files::FileArea::Cache, &name);
    drop(store.create_writer(crate::files::FileArea::Cache, &name).unwrap());
    assert_eq!(file.read_optional().unwrap().unwrap(), b"");
    file.remove().unwrap();
    assert!(file.read_optional().unwrap().is_none());
}

#[test]
fn overwriting_changes_only_its_range_of_an_existing_file() {
    let directory = tempfile::tempdir().unwrap();
    let file = AtomicFile::new(directory.path().join("cached"));
    assert!(matches!(file.overwrite_at(0, b"x"), Err(crate::files::ObservationError::Missing(_))));
    file.write_recorded(b"abcdef").unwrap();
    file.overwrite_at(2, b"XY").unwrap();
    assert_eq!(file.read_optional().unwrap().unwrap(), b"abXYef");
    // A range past the end writes nothing: the file keeps its size.
    assert!(matches!(file.overwrite_at(5, b"XY"), Err(crate::files::ObservationError::File(_))));
    assert_eq!(file.read_optional().unwrap().unwrap(), b"abXYef");
}

#[test]
fn resizing_cuts_a_file_short_or_adds_zeros() {
    let directory = tempfile::tempdir().unwrap();
    let file = AtomicFile::new(directory.path().join("cached"));
    assert!(matches!(file.set_len(3), Err(crate::files::ObservationError::Missing(_))));
    file.write_recorded(b"abcdef").unwrap();
    file.set_len(3).unwrap();
    assert_eq!(file.read_optional().unwrap().unwrap(), b"abc");
    file.set_len(5).unwrap();
    assert_eq!(file.read_optional().unwrap().unwrap(), b"abc\0\0");
}

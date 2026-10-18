//! The application surface cannot bypass store owners or their journals.
//! Each example names one operation, so a failure cannot hide another leak.
//!
//! `StoreDir::database_path` is a crate-to-crate operation.
//! ```compile_fail
//! let _ = coven::StoreDir::database_path;
//! ```
//!
//! `StoreDir::input_database_path` is a crate-to-crate operation.
//! ```compile_fail
//! let _ = coven::StoreDir::input_database_path;
//! ```
//!
//! `StoreDir::record_device_reset` is a crate-to-crate operation.
//! ```compile_fail
//! let _ = coven::StoreDir::record_device_reset;
//! ```
//!
//! `StoreDir::unfinished_device_reset` is a crate-to-crate operation.
//! ```compile_fail
//! let _ = coven::StoreDir::unfinished_device_reset;
//! ```
//!
//! `StoreDir::settings` is a crate-to-crate operation.
//! ```compile_fail
//! let _ = coven::StoreDir::settings;
//! ```
//!
//! `StoreDir::lock_exclusive` is a crate-to-crate operation.
//! ```compile_fail
//! let _ = coven::StoreDir::lock_exclusive;
//! ```
//!
//! `StoreDir::lock_for_open` is a crate-to-crate operation.
//! ```compile_fail
//! let _ = coven::StoreDir::lock_for_open;
//! ```
//!
//! `StoreDir::lock_read_only` is a crate-to-crate operation.
//! ```compile_fail
//! let _ = coven::StoreDir::lock_read_only;
//! ```
//!
//! `StoreDir::lock_cache_write` is a crate-to-crate operation.
//! ```compile_fail
//! let _ = coven::StoreDir::lock_cache_write;
//! ```
//!
//! `StoreDir::exclude_cache_writes` is a crate-to-crate operation.
//! ```compile_fail
//! let _ = coven::StoreDir::exclude_cache_writes;
//! ```
//!
//! `StoreDir::lock_for_deletion` is a crate-to-crate operation.
//! ```compile_fail
//! let _ = coven::StoreDir::lock_for_deletion;
//! ```
//!
//! `StoreDir::verify_lock` is a crate-to-crate operation.
//! ```compile_fail
//! let _ = coven::StoreDir::verify_lock;
//! ```
//!
//! `StoreDir::owned_file` is a crate-to-crate operation.
//! ```compile_fail
//! let _ = coven::StoreDir::owned_file;
//! ```
//!
//! `StoreDir::file` is a crate-to-crate operation.
//! ```compile_fail
//! let _ = coven::StoreDir::file;
//! ```
//!
//! `StoreLayout::begin_bootstrap` is a crate-to-crate operation.
//! ```compile_fail
//! let _ = coven::StoreLayout::begin_bootstrap;
//! ```
//!
//! `OAuthClients::refresh` is a crate-to-crate operation.
//! ```compile_fail
//! let _ = coven::OAuthClients::refresh;
//! ```
//!
//! `FileRef::reference` is a crate-to-crate operation.
//! ```compile_fail
//! let _ = coven::FileRef::reference;
//! ```
//!
//! `FileRef::encode` is a crate-to-crate operation.
//! ```compile_fail
//! let _ = coven::FileRef::encode;
//! ```
//!
//! `FileRef::decode` is a crate-to-crate operation.
//! ```compile_fail
//! let _ = coven::FileRef::decode;
//! ```
//!
//! `UploadQueue::from_state` is a crate-to-crate operation.
//! ```compile_fail
//! let _ = coven::UploadQueue::from_state;
//! ```
//!
//! `MemberAccess` belongs to the internal protocol.
//! ```compile_fail
//! use coven::MemberAccess;
//! ```
//!
//! `MigrationChange` belongs to the internal protocol.
//! ```compile_fail
//! use coven::MigrationChange;
//! ```
//!
//! `MigrationOutcome` belongs to the internal protocol.
//! ```compile_fail
//! use coven::MigrationOutcome;
//! ```

#[test]
fn app_file_replacement_preserves_open_readers_and_uses_the_format_chunk_size() {
    use crate::{write_atomic, CHUNK_SIZE};
    use std::io::Read;

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("settings");
    write_atomic(&path, b"old").unwrap();
    let mut reader = std::fs::File::open(&path).unwrap();
    let bytes = vec![7; CHUNK_SIZE + 3];
    write_atomic(&path, &bytes).unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
    assert_eq!(
        bytes
            .chunks(CHUNK_SIZE)
            .map(<[u8]>::len)
            .collect::<Vec<_>>(),
        [65536, 3]
    );
    let mut old = Vec::new();
    reader.read_to_end(&mut old).unwrap();
    assert_eq!(old, b"old");
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);

    let occupied = directory.path().join("occupied");
    std::fs::create_dir(&occupied).unwrap();
    let error: crate::FileError =
        write_atomic(&occupied, b"cannot replace a directory").unwrap_err();
    assert!(!error.installed_new_bytes());
    assert!(occupied.is_dir());
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 2);
}

#[cfg(feature = "test-utils")]
#[test]
fn app_tests_can_find_open_files_and_assert_they_are_released() {
    use crate::{assert_no_open_files_under, open_files_under, write_atomic};

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("settings");
    write_atomic(&path, b"settings").unwrap();
    let path = path.canonicalize().unwrap();
    assert_no_open_files_under(directory.path());
    let file = std::fs::File::open(&path).unwrap();
    assert_eq!(open_files_under(directory.path()), vec![path]);
    assert!(std::panic::catch_unwind(|| assert_no_open_files_under(directory.path())).is_err());
    drop(file);
    assert_no_open_files_under(directory.path());
}

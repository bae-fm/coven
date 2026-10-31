//! These run on macOS, whose APFS volumes keep a file's protection class.
//! macOS takes `None` but keeps such a file at
//! `CompleteUntilFirstUserAuthentication`, which is also the class a new file
//! gets there by default, so the tests use the two classes that differ from
//! both. No test changes a file's class twice: for a few milliseconds after
//! a change Apple refuses another with `EBUSY`.

use super::*;
use crate::files::{StoreDirAccess, StoreDirectories, StoreDirectoriesAccess};
use crate::id_source::{StoreId, UuidIds};
use std::os::unix::fs::PermissionsExt;
use uuid::Uuid;

const COMPLETE: Option<FileProtection> = Some(FileProtection::Complete);
const UNLESS_OPEN: Option<FileProtection> = Some(FileProtection::CompleteUnlessOpen);

/// Every folder and file under `folder`, the folder included, with its class.
fn classes(folder: &Path) -> Vec<(std::path::PathBuf, Option<FileProtection>)> {
    let mut found = vec![(folder.to_owned(), FileProtection::of(folder).unwrap())];
    for entry in fs::read_dir(folder).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            found.extend(classes(&path));
        } else {
            found.push((path.clone(), FileProtection::of(&path).unwrap()));
        }
    }
    found
}

fn assert_all(folder: &Path, class: Option<FileProtection>) {
    for (path, found) in classes(folder) {
        assert_eq!(found, class, "{}", path.display());
    }
}

/// The files a writable open finds: the database, its WAL and shared
/// memory, and a file in each of coven's folders.
fn add_files(store: &Path) {
    for name in ["store.db", "store.db-wal", "store.db-shm", "files/copy", "cache/entry"] {
        fs::write(store.join(name), b"bytes").unwrap();
    }
}

/// A store with files, created by `layout`.
fn store_with_files(layout: &StoreDirectories, app_dir: &Path) -> (StoreId, std::path::PathBuf) {
    let id = StoreId(Uuid::from_u128(1));
    layout.create_store_dir(id, &UuidIds).unwrap();
    let store = app_dir.join("stores").join(id.to_string());
    add_files(&store);
    (id, store)
}

fn open(layout: &StoreDirectories, id: StoreId) -> Result<(), FileError> {
    let directory = layout.store_dir(&id);
    let lock = directory.lock_exclusive().unwrap();
    directory.protect_files(&lock)
}

#[test]
fn a_new_store_and_everything_made_in_it_take_the_class() {
    let app_dir = tempfile::tempdir().unwrap();
    let layout = StoreDirectories::new(app_dir.path().to_owned())
        .file_protection(FileProtection::CompleteUnlessOpen);
    // The folders, the settings and the record are made in the stage after
    // it takes the class, and the files after them, as SQLite makes its WAL,
    // take their folder's.
    let (_, store) = store_with_files(&layout, app_dir.path());
    assert_all(&store, UNLESS_OPEN);
    assert_eq!(recorded(&store).unwrap(), UNLESS_OPEN);
}

#[test]
fn opening_a_store_made_without_a_class_gives_it_to_every_file() {
    let app_dir = tempfile::tempdir().unwrap();
    let plain = StoreDirectories::new(app_dir.path().to_owned());
    let (id, store) = store_with_files(&plain, app_dir.path());
    assert_ne!(FileProtection::of(&store.join("store.db")).unwrap(), COMPLETE);
    open(&plain.file_protection(FileProtection::Complete), id).unwrap();
    assert_all(&store, COMPLETE);
    assert_eq!(recorded(&store).unwrap(), COMPLETE);
}

#[test]
fn opening_with_another_class_gives_it_to_every_file() {
    let app_dir = tempfile::tempdir().unwrap();
    let directories = || StoreDirectories::new(app_dir.path().to_owned());
    let (id, store) = store_with_files(
        &directories().file_protection(FileProtection::CompleteUnlessOpen),
        app_dir.path(),
    );
    open(&directories().file_protection(FileProtection::Complete), id).unwrap();
    assert_all(&store, COMPLETE);
    assert_eq!(recorded(&store).unwrap(), COMPLETE);
}

#[test]
fn an_open_with_the_recorded_class_walks_nothing() {
    let app_dir = tempfile::tempdir().unwrap();
    let layout =
        StoreDirectories::new(app_dir.path().to_owned()).file_protection(FileProtection::Complete);
    let (id, store) = store_with_files(&layout, app_dir.path());
    // A walk would fail at a file it can't open for writing.
    let stuck = store.join("files").join("copy");
    fs::set_permissions(&stuck, fs::Permissions::from_mode(0o400)).unwrap();
    open(&layout, id).unwrap();
}

#[test]
fn a_walk_that_stops_partway_is_not_recorded_and_the_next_open_finishes_it() {
    let app_dir = tempfile::tempdir().unwrap();
    let plain = StoreDirectories::new(app_dir.path().to_owned());
    let (id, store) = store_with_files(&plain, app_dir.path());
    let layout = plain.file_protection(FileProtection::Complete);
    // A file the walk can't open for writing stops it.
    let stuck = store.join("files").join("copy");
    fs::set_permissions(&stuck, fs::Permissions::from_mode(0o400)).unwrap();
    let stopped = open(&layout, id);
    assert!(
        matches!(&stopped, Err(FileError::Io { path, .. }) if path.ends_with("files/copy")),
        "{stopped:?}"
    );
    assert_eq!(recorded(&store).unwrap(), None);
    assert_ne!(FileProtection::of(&stuck).unwrap(), COMPLETE);
    // The next open sets only what the first didn't reach, so nothing it
    // changed is changed again.
    fs::set_permissions(&stuck, fs::Permissions::from_mode(0o600)).unwrap();
    open(&layout, id).unwrap();
    assert_all(&store, COMPLETE);
    assert_eq!(recorded(&store).unwrap(), COMPLETE);
}

#[test]
fn an_installation_inside_the_store_gets_its_record_rewritten() {
    let app_dir = tempfile::tempdir().unwrap();
    let plain = StoreDirectories::new(app_dir.path().to_owned());
    let (id, store) = store_with_files(&plain, app_dir.path());
    // A replacement loading inside the store's directory, made with another
    // class, as one a swap would move into the store's place.
    let loading = store.join(".coven-replacement").join(id.to_string());
    for folder in ["files", "cache"] {
        fs::create_dir_all(loading.join(folder)).unwrap();
    }
    add_files(&loading);
    record(&loading, FileProtection::CompleteUnlessOpen).unwrap();
    open(&plain.file_protection(FileProtection::Complete), id).unwrap();
    assert_all(&store, COMPLETE);
    assert_eq!(recorded(&loading).unwrap(), COMPLETE);
}

#[test]
fn without_a_class_files_keep_theirs() {
    let app_dir = tempfile::tempdir().unwrap();
    let directories = || StoreDirectories::new(app_dir.path().to_owned());
    let (id, store) = store_with_files(
        &directories().file_protection(FileProtection::CompleteUnlessOpen),
        app_dir.path(),
    );
    // A file moved in keeps the class it was made with elsewhere.
    let elsewhere = app_dir.path().join("elsewhere");
    fs::write(&elsewhere, b"bytes").unwrap();
    let class = FileProtection::of(&elsewhere).unwrap();
    assert_ne!(class, UNLESS_OPEN);
    let moved = store.join("cache").join("moved");
    fs::rename(&elsewhere, &moved).unwrap();
    open(&directories(), id).unwrap();
    assert_eq!(FileProtection::of(&moved).unwrap(), class);
    assert_eq!(recorded(&store).unwrap(), UNLESS_OPEN);
}

#[tokio::test]
async fn a_resumed_installation_takes_the_class() {
    let app_dir = tempfile::tempdir().unwrap();
    let directories = || StoreDirectories::new(app_dir.path().to_owned());
    let id = StoreId(Uuid::from_u128(1));
    // A restore or join that loaded files, then stopped.
    let store = app_dir.path().join("stores").join(id.to_string());
    drop(directories().begin_bootstrap(id, &UuidIds).await.unwrap());
    add_files(&store);
    let resumed = directories()
        .file_protection(FileProtection::Complete)
        .begin_bootstrap(id, &UuidIds)
        .await
        .unwrap();
    assert_all(&store, COMPLETE);
    assert_eq!(recorded(&store).unwrap(), COMPLETE);
    drop(resumed);
}

#[tokio::test]
async fn a_replacement_gives_the_class_to_both_installations() {
    let app_dir = tempfile::tempdir().unwrap();
    let plain = StoreDirectories::new(app_dir.path().to_owned());
    let (id, store) = store_with_files(&plain, app_dir.path());
    let replacement = plain
        .file_protection(FileProtection::Complete)
        .begin_replacement(id, crate::files::ReplacedBy::Reset, &UuidIds)
        .await
        .unwrap();
    // The new installation loads beside the old one, in a folder made after
    // the store's directory took the class.
    add_files(&store.join(".coven-replacement").join(id.to_string()));
    assert_all(&store, COMPLETE);
    assert_eq!(recorded(&store).unwrap(), COMPLETE);
    drop(replacement);
}

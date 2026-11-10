//! These run on macOS. Each test first learns what the volume holding its
//! temporary directory does with protection classes now, by giving a scratch
//! file one, and asserts what coven does there: on a volume that keeps them,
//! the class every file gets; on one that keeps none, the refusal before any
//! file changes; and while the device is locked, the class every file it
//! could make gets, and `DeviceLocked` wherever the lock refuses a file. A
//! file's class reads without opening it, so a locked device answers too.
//!
//! macOS takes `None` but keeps such a file at
//! `CompleteUntilFirstUserAuthentication`, which is also the class a new file
//! gets there by default, so the tests use the two classes that differ from
//! both. No test changes a file's class twice: for a few milliseconds after
//! a change Apple refuses another with `EBUSY`.

use super::*;
use crate::files::{DirectoryCreationError, StoreDirAccess, StoreDirectories, StoreLockProbe};
use crate::id_source::{StoreId, UuidIds};
use std::os::unix::fs::PermissionsExt;
use uuid::Uuid;

const COMPLETE: Option<FileProtection> = Some(FileProtection::Complete);
const UNLESS_OPEN: Option<FileProtection> = Some(FileProtection::CompleteUnlessOpen);
const ID: StoreId = StoreId(Uuid::from_u128(1));

/// What the volume holding `folder` does with protection classes now.
fn support(folder: &Path) -> ProtectionSupport {
    FileProtection::settable_in(folder).unwrap()
}

/// Whether the volume holding `folder` keeps protection classes, the device
/// locked or not.
fn keeps_classes(folder: &Path) -> bool {
    support(folder) != ProtectionSupport::KeepsNone
}

/// Whether the device is locked, so the lock refuses `Complete` files and
/// closed `CompleteUnlessOpen` ones.
fn locked(folder: &Path) -> bool {
    support(folder) == ProtectionSupport::Locked
}

fn assert_locked<T: std::fmt::Debug>(result: Result<T, FileError>) {
    assert!(matches!(result, Err(FileError::DeviceLocked { .. })), "{result:?}");
}

/// The store's record names `class`; while the device is locked, a record of
/// a class the lock closes can't be read, and says so.
fn assert_recorded(store: &Path, class: Option<FileProtection>, app_dir: &Path) {
    if locked(app_dir) && class.is_some_and(|class| class != FileProtection::None) {
        assert_locked(recorded(store));
    } else {
        assert_eq!(recorded(store).unwrap(), class);
    }
}

/// Whether `error` is the refusal of a class on a volume that keeps none.
fn refused(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut cause = Some(error);
    while let Some(error) = cause {
        if error.downcast_ref::<io::Error>().is_some_and(|e| e.kind() == io::ErrorKind::Unsupported)
        {
            return true;
        }
        cause = error.source();
    }
    false
}

fn assert_refused<T: std::fmt::Debug, E: std::error::Error + 'static>(result: Result<T, E>) {
    match result {
        Err(error) => assert!(refused(&error), "{error:?}"),
        Ok(value) => panic!("a volume that keeps no classes took one: {value:?}"),
    }
}

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
fn store_with_files(
    layout: &StoreDirectories,
    app_dir: &Path,
) -> Result<std::path::PathBuf, DirectoryCreationError> {
    layout.create_store_dir(ID, &UuidIds)?;
    let store = app_dir.join("stores").join(ID.to_string());
    add_files(&store);
    Ok(store)
}

/// A store with files, created by `layout`, which gives it a class. On a
/// volume that keeps none the creation is refused and leaves nothing: no
/// store, and no stage it was being made in.
fn classed_store_with_files(
    layout: &StoreDirectories,
    app_dir: &Path,
) -> Option<std::path::PathBuf> {
    if keeps_classes(app_dir) {
        return Some(store_with_files(layout, app_dir).unwrap());
    }
    assert_refused(store_with_files(layout, app_dir));
    let left: Vec<_> = fs::read_dir(app_dir.join("stores"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .filter(|name| name != ".coven-stores.lock")
        .collect();
    assert!(left.is_empty(), "{left:?}");
    None
}

fn open(layout: &StoreDirectories, id: StoreId) -> Result<(), FileError> {
    let directory = layout.store_dir(&id);
    let lock = directory.lock_exclusive().unwrap();
    crate::files::settle_files(&directory, &lock)
}

#[test]
fn a_new_store_and_everything_made_in_it_take_the_class() {
    let app_dir = tempfile::tempdir().unwrap();
    let layout = StoreDirectories::new(app_dir.path().to_owned(), crate::task::HeardBugs::shared())
        .file_protection(FileProtection::CompleteUnlessOpen);
    // The folders, the settings and the record are made in the stage after
    // it takes the class, and the files after them, as SQLite makes its WAL,
    // take their folder's.
    let Some(store) = classed_store_with_files(&layout, app_dir.path()) else {
        return;
    };
    assert_all(&store, UNLESS_OPEN);
    assert_recorded(&store, UNLESS_OPEN, app_dir.path());
}

#[test]
fn opening_a_store_made_without_a_class_gives_it_to_every_file() {
    let app_dir = tempfile::tempdir().unwrap();
    let plain = StoreDirectories::new(app_dir.path().to_owned(), crate::task::HeardBugs::shared());
    let store = store_with_files(&plain, app_dir.path()).unwrap();
    let opened = open(&plain.file_protection(FileProtection::Complete), ID);
    if !keeps_classes(app_dir.path()) {
        assert_refused(opened);
        assert_eq!(recorded(&store).unwrap(), None);
        return;
    }
    if locked(app_dir.path()) {
        // The lock refuses `Complete` to the first file the walk reaches.
        assert_locked(opened);
        assert_eq!(recorded(&store).unwrap(), None);
        return;
    }
    opened.unwrap();
    assert_all(&store, COMPLETE);
    assert_eq!(recorded(&store).unwrap(), COMPLETE);
}

#[test]
fn opening_with_another_class_gives_it_to_every_file() {
    let app_dir = tempfile::tempdir().unwrap();
    let directories =
        || StoreDirectories::new(app_dir.path().to_owned(), crate::task::HeardBugs::shared());
    let Some(store) = classed_store_with_files(
        &directories().file_protection(FileProtection::CompleteUnlessOpen),
        app_dir.path(),
    ) else {
        return;
    };
    let opened = open(&directories().file_protection(FileProtection::Complete), ID);
    if locked(app_dir.path()) {
        // The record, made `CompleteUnlessOpen`, can't be opened.
        assert_locked(opened);
        return;
    }
    opened.unwrap();
    assert_all(&store, COMPLETE);
    assert_eq!(recorded(&store).unwrap(), COMPLETE);
}

/// The record lets an open skip the walk, not the volume's check: a store
/// moved onto a volume that keeps no classes is refused.
#[test]
fn an_open_with_the_recorded_class_walks_nothing() {
    let app_dir = tempfile::tempdir().unwrap();
    let plain = StoreDirectories::new(app_dir.path().to_owned(), crate::task::HeardBugs::shared());
    let store = store_with_files(&plain, app_dir.path()).unwrap();
    record(&store, FileProtection::Complete).unwrap();
    // A walk would fail at a file it can't open for writing.
    let stuck = store.join("files").join("copy");
    fs::set_permissions(&stuck, fs::Permissions::from_mode(0o400)).unwrap();
    let opened = open(&plain.file_protection(FileProtection::Complete), ID);
    if keeps_classes(app_dir.path()) {
        opened.unwrap();
    } else {
        assert_refused(opened);
    }
}

#[test]
fn a_walk_that_stops_partway_is_not_recorded_and_the_next_open_finishes_it() {
    let app_dir = tempfile::tempdir().unwrap();
    let plain = StoreDirectories::new(app_dir.path().to_owned(), crate::task::HeardBugs::shared());
    let store = store_with_files(&plain, app_dir.path()).unwrap();
    let layout = plain.file_protection(FileProtection::Complete);
    // A file the walk can't open for writing stops it.
    let stuck = store.join("files").join("copy");
    fs::set_permissions(&stuck, fs::Permissions::from_mode(0o400)).unwrap();
    let stopped = open(&layout, ID);
    assert_eq!(recorded(&store).unwrap(), None);
    if !keeps_classes(app_dir.path()) {
        // Refused before the walk reaches any file.
        assert_refused(stopped);
        return;
    }
    if locked(app_dir.path()) {
        assert_locked(stopped);
        return;
    }
    assert!(
        matches!(&stopped, Err(FileError::Io { path, .. }) if path.ends_with("files/copy")),
        "{stopped:?}"
    );
    assert_ne!(FileProtection::of(&stuck).unwrap(), COMPLETE);
    // The next open sets only what the first didn't reach, so nothing it
    // changed is changed again.
    fs::set_permissions(&stuck, fs::Permissions::from_mode(0o600)).unwrap();
    open(&layout, ID).unwrap();
    assert_all(&store, COMPLETE);
    assert_eq!(recorded(&store).unwrap(), COMPLETE);
}

#[test]
fn an_installation_inside_the_store_gets_its_record_rewritten() {
    let app_dir = tempfile::tempdir().unwrap();
    let plain = StoreDirectories::new(app_dir.path().to_owned(), crate::task::HeardBugs::shared());
    let store = store_with_files(&plain, app_dir.path()).unwrap();
    // A replacement loading inside the store's directory, made with another
    // class, as one a swap would move into the store's place.
    let loading = store.join(".coven-replacement").join(ID.to_string());
    for folder in ["files", "cache"] {
        fs::create_dir_all(loading.join(folder)).unwrap();
    }
    add_files(&loading);
    record(&loading, FileProtection::CompleteUnlessOpen).unwrap();
    let opened = open(&plain.file_protection(FileProtection::Complete), ID);
    if !keeps_classes(app_dir.path()) {
        assert_refused(opened);
        assert_eq!(recorded(&loading).unwrap(), UNLESS_OPEN);
        return;
    }
    if locked(app_dir.path()) {
        assert_locked(opened);
        assert_eq!(recorded(&loading).unwrap(), UNLESS_OPEN);
        return;
    }
    opened.unwrap();
    assert_all(&store, COMPLETE);
    assert_eq!(recorded(&loading).unwrap(), COMPLETE);
}

#[test]
fn without_a_class_files_keep_theirs() {
    let app_dir = tempfile::tempdir().unwrap();
    let directories =
        || StoreDirectories::new(app_dir.path().to_owned(), crate::task::HeardBugs::shared());
    let Some(store) = classed_store_with_files(
        &directories().file_protection(FileProtection::CompleteUnlessOpen),
        app_dir.path(),
    ) else {
        return;
    };
    // A file moved in keeps the class it was made with elsewhere.
    let elsewhere = app_dir.path().join("elsewhere");
    fs::write(&elsewhere, b"bytes").unwrap();
    let class = FileProtection::of(&elsewhere).unwrap();
    assert_ne!(class, UNLESS_OPEN);
    let moved = store.join("cache").join("moved");
    fs::rename(&elsewhere, &moved).unwrap();
    open(&directories(), ID).unwrap();
    assert_eq!(FileProtection::of(&moved).unwrap(), class);
    assert_recorded(&store, UNLESS_OPEN, app_dir.path());
}

/// Without a class, a store opens on any volume, one that keeps no classes
/// included, and no record is made.
#[test]
fn without_a_class_any_volume_opens() {
    let app_dir = tempfile::tempdir().unwrap();
    let plain = StoreDirectories::new(app_dir.path().to_owned(), crate::task::HeardBugs::shared());
    let store = store_with_files(&plain, app_dir.path()).unwrap();
    open(&plain, ID).unwrap();
    assert_eq!(recorded(&store).unwrap(), None);
}

#[tokio::test]
async fn a_resumed_installation_takes_the_class() {
    let app_dir = tempfile::tempdir().unwrap();
    let directories =
        || StoreDirectories::new(app_dir.path().to_owned(), crate::task::HeardBugs::shared());
    // A restore or join that loaded files, then stopped.
    let store = app_dir.path().join("stores").join(ID.to_string());
    drop(directories().begin_bootstrap(ID, &UuidIds).await.unwrap());
    add_files(&store);
    let resumed =
        directories().file_protection(FileProtection::Complete).begin_bootstrap(ID, &UuidIds).await;
    if !keeps_classes(app_dir.path()) {
        assert_refused(resumed.map(drop));
        assert_eq!(recorded(&store).unwrap(), None);
        return;
    }
    if locked(app_dir.path()) {
        assert!(
            matches!(&resumed, Err(error) if error.device_locked().is_some()),
            "{:?}",
            resumed.map(drop)
        );
        assert_eq!(recorded(&store).unwrap(), None);
        return;
    }
    let resumed = resumed.unwrap();
    assert_all(&store, COMPLETE);
    assert_eq!(recorded(&store).unwrap(), COMPLETE);
    drop(resumed);
}

#[tokio::test]
async fn a_replacement_gives_the_class_to_both_installations() {
    let app_dir = tempfile::tempdir().unwrap();
    let plain = StoreDirectories::new(app_dir.path().to_owned(), crate::task::HeardBugs::shared());
    let store = store_with_files(&plain, app_dir.path()).unwrap();
    let replacement = plain
        .file_protection(FileProtection::Complete)
        .begin_replacement(ID, crate::files::ReplacedBy::Reset, &UuidIds)
        .await;
    let area = store.join(".coven-replacement");
    if !keeps_classes(app_dir.path()) {
        // Refused before the replacement's area is made.
        assert_refused(replacement.map(drop));
        assert!(!area.exists());
        assert_eq!(recorded(&store).unwrap(), None);
        return;
    }
    if locked(app_dir.path()) {
        assert!(
            matches!(&replacement, Err(error) if error.device_locked().is_some()),
            "{:?}",
            replacement.map(drop)
        );
        assert_eq!(recorded(&store).unwrap(), None);
        return;
    }
    let replacement = replacement.unwrap();
    // The new installation loads beside the old one, in a folder made after
    // the store's directory took the class.
    add_files(&area.join(ID.to_string()));
    assert_all(&store, COMPLETE);
    assert_eq!(recorded(&store).unwrap(), COMPLETE);
    drop(replacement);
}

/// An `EPERM` is the lock's refusal only where the file, or the folder a new
/// one goes in, has a class the lock closes; anywhere else it stays the
/// operating system's error. Folders take a class locked or not, so this
/// holds whether the device is locked.
#[test]
fn only_a_class_the_lock_closes_makes_a_refusal_device_locked() {
    let app_dir = tempfile::tempdir().unwrap();
    if !keeps_classes(app_dir.path()) {
        return;
    }
    let refusal = |path: &Path| {
        FileError::at("open store file", path, io::Error::from_raw_os_error(libc::EPERM))
    };
    let closed = app_dir.path().join("closed");
    fs::create_dir(&closed).unwrap();
    set(&closed, FileProtection::CompleteUnlessOpen, Entry::Folder).unwrap();
    let file = closed.join("record");
    assert!(matches!(
        locked_or(&file, &closed, refusal(&file)),
        FileError::DeviceLocked { path } if path == file
    ));
    let open = app_dir.path().join("open");
    fs::create_dir(&open).unwrap();
    let file = open.join("record");
    assert!(matches!(locked_or(&file, &open, refusal(&file)), FileError::Io { .. }));
}

use super::*;
use std::fs::OpenOptions;
use std::path::Path;

fn open(path: &Path) -> File {
    OpenOptions::new().read(true).write(true).create(true).truncate(false).open(path).unwrap()
}

fn refused(path: &Path, mode: LockMode) -> bool {
    matches!(FileLock::try_lock(open(path), mode), Err(TryLockError::WouldBlock))
}

#[test]
fn an_exclusive_lock_refuses_every_other_open_until_dropped() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("held.lock");
    let held = FileLock::try_lock(open(&path), LockMode::Exclusive).unwrap();
    assert!(refused(&path, LockMode::Exclusive));
    assert!(refused(&path, LockMode::Shared));
    drop(held);
    FileLock::try_lock(open(&path), LockMode::Exclusive).unwrap();
}

#[test]
fn shared_locks_coexist_and_refuse_an_exclusive_one() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("held.lock");
    let first = FileLock::lock(open(&path), LockMode::Shared).unwrap();
    let second = FileLock::try_lock(open(&path), LockMode::Shared).unwrap();
    assert!(refused(&path, LockMode::Exclusive));
    drop((first, second));
    FileLock::try_lock(open(&path), LockMode::Exclusive).unwrap();
}

#[test]
fn a_waiting_lock_takes_the_file_once_the_holder_drops() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("held.lock");
    let held = FileLock::lock(open(&path), LockMode::Exclusive).unwrap();
    let waiting = {
        let file = open(&path);
        std::thread::spawn(move || FileLock::lock(file, LockMode::Exclusive).unwrap())
    };
    std::thread::sleep(std::time::Duration::from_millis(50));
    assert!(!waiting.is_finished(), "the waiter must not take a held lock");
    drop(held);
    let taken = waiting.join().unwrap();
    assert!(refused(&path, LockMode::Shared));
    drop(taken);
}

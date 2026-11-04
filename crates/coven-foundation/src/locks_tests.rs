use super::*;

/// A mutex a panic poisoned while it held `value`.
fn poisoned<T: Send>(value: T) -> Mutex<T> {
    let mutex = Mutex::new(value);
    std::thread::scope(|scope| {
        let poisoning = scope.spawn(|| {
            let _held = mutex.lock().unwrap();
            panic!("the holder panicked");
        });
        assert!(poisoning.join().is_err());
    });
    assert!(mutex.is_poisoned());
    mutex
}

#[test]
fn an_unpoisoned_lock_hands_out_the_whole_value_a_panic_left() {
    let counts = poisoned(vec![1, 2]);
    lock_unpoisoned(&counts).push(3);
    assert_eq!(*lock_unpoisoned(&counts), [1, 2, 3]);
}

#[test]
fn an_unpoisoned_rwlock_reads_and_writes_after_a_writers_panic() {
    let value = RwLock::new(1);
    std::thread::scope(|scope| {
        let poisoning = scope.spawn(|| {
            let _held = value.write().unwrap();
            panic!("the writer panicked");
        });
        assert!(poisoning.join().is_err());
    });
    *write_unpoisoned(&value) += 1;
    assert_eq!(*read_unpoisoned(&value), 2);
}

/// Set in the child process `a_poisoned_process_wide_lock_ends_the_process`
/// runs, which takes the poisoned lock.
const ABORTING: &str = "COVEN_LOCKS_TEST_ABORTS";

#[test]
fn a_poisoned_process_wide_lock_ends_the_process() {
    if std::env::var_os(ABORTING).is_some() {
        let registry = poisoned(0);
        let _never = lock_or_abort(&registry, "the test registry");
        unreachable!("taking a poisoned process-wide lock returned");
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "--nocapture",
            "locks::tests::a_poisoned_process_wide_lock_ends_the_process",
        ])
        .env(ABORTING, "1")
        .output()
        .unwrap();
    assert!(!output.status.success());
    #[cfg(unix)]
    assert_eq!(
        std::os::unix::process::ExitStatusExt::signal(&output.status),
        Some(6),
        "the process aborts"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("a panic left the test registry half-changed"), "{stderr}");
}

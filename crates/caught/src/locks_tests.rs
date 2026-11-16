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

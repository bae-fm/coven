//! Taking a lock whose holder may have panicked.
//!
//! A panic in a thread that holds a `Mutex`, or holds an `RwLock` for
//! writing, poisons the lock: std's way of saying the value may be
//! half-changed. These helpers are for a lock whose every change is whole
//! before anything in it can panic, so the caller uses the poisoned lock's
//! value as it is. A
//! lock whose change can leave half-made state needs a different answer, which
//! its owner chooses.

use std::sync::{Condvar, Mutex, MutexGuard, PoisonError, RwLock};
use std::sync::{RwLockReadGuard, RwLockWriteGuard};

/// Lock `mutex`, whose value no panic can leave half-changed.
pub fn lock_unpoisoned<T: ?Sized>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Read `lock`, whose value no panic can leave half-changed.
pub fn read_unpoisoned<T: ?Sized>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(PoisonError::into_inner)
}

/// Write `lock`, whose value no panic can leave half-changed.
pub fn write_unpoisoned<T: ?Sized>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    lock.write().unwrap_or_else(PoisonError::into_inner)
}

/// Return the value of `mutex`, which no panic can leave half-changed, and
/// consume the mutex.
pub fn into_unpoisoned<T>(mutex: Mutex<T>) -> T {
    mutex.into_inner().unwrap_or_else(PoisonError::into_inner)
}

/// Wait on `condvar` with `guard`, the guard of a lock whose value no panic
/// can leave half-changed.
pub fn wait_unpoisoned<'a, T>(condvar: &Condvar, guard: MutexGuard<'a, T>) -> MutexGuard<'a, T> {
    condvar.wait(guard).unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
#[path = "locks_tests.rs"]
mod tests;

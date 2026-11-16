//! Reader reservations keep committed reads independent of the writer.

use crate::authorization::Policy;
use crate::authorization::PolicyError;
use crate::sqlite::DatabaseConnection;
use coven_foundation::locks::lock_unpoisoned;
use std::sync::{Condvar, Mutex};

pub(super) struct ReadPool<P: Policy> {
    readers: Vec<Mutex<DatabaseConnection<P>>>,
    idle_readers: Mutex<Vec<usize>>,
    reader_ready: Condvar,
    #[cfg(test)]
    waiting: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

impl<P: Policy> ReadPool<P> {
    /// Run `visit` on each reader in turn, under its own lock.
    #[cfg(any(test, feature = "test-utils"))]
    pub(super) fn each(&self, mut visit: impl FnMut(&mut DatabaseConnection<P>)) {
        for reader in &self.readers {
            crate::database::with_locked(reader, |reader| visit(reader));
        }
    }

    pub(super) fn new(readers: Vec<Mutex<DatabaseConnection<P>>>) -> Self {
        Self {
            idle_readers: Mutex::new((0..readers.len()).collect()),
            readers,
            reader_ready: Condvar::new(),
            #[cfg(test)]
            waiting: Mutex::new(None),
        }
    }

    pub(super) fn close(self) -> Vec<PolicyError<P>> {
        let mut failures = Vec::new();
        for reader in self.readers {
            if let Err(error) = coven_foundation::locks::into_unpoisoned(reader).close() {
                failures.push(error);
            }
        }
        failures
    }

    pub(super) fn acquire_reader(&self) -> Reader<'_, P> {
        let mut idle = lock_unpoisoned(&self.idle_readers);
        loop {
            if let Some(index) = idle.pop() {
                return Reader { pool: self, index };
            }
            #[cfg(test)]
            if let Some(waiting) = self.waiting.lock().unwrap().take() {
                waiting.send(()).unwrap();
            }
            idle = coven_foundation::locks::wait_unpoisoned(&self.reader_ready, idle);
        }
    }
}

/// One of a database's readers, reserved for its caller until it drops.
/// Dropping it wakes a caller waiting for a reader, even when a call on it
/// panics.
pub(crate) struct Reader<'a, P: Policy> {
    pool: &'a ReadPool<P>,
    index: usize,
}

impl<P: Policy> Reader<'_, P> {
    /// Run `read` on the reader.
    pub(crate) fn host<R>(&self, read: impl FnOnce(&DatabaseConnection<P>) -> R) -> R {
        crate::database::with_locked(&self.pool.readers[self.index], |reader| read(reader))
    }
}

impl<P: Policy> Drop for Reader<'_, P> {
    fn drop(&mut self) {
        lock_unpoisoned(&self.pool.idle_readers).push(self.index);
        self.pool.reader_ready.notify_one();
    }
}

#[cfg(test)]
#[path = "read_pool_tests.rs"]
mod tests;

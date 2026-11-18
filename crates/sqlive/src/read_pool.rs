//! Reader reservations keep committed reads independent of the writer.

use crate::authorization::Policy;
use crate::authorization::PolicyError;
use crate::connection::HostSql;
use panic_as_error::locks::lock_unpoisoned;
use std::sync::{Condvar, Mutex};

pub(super) struct ReadPool<P: Policy> {
    readers: Vec<Mutex<HostSql<P>>>,
    idle_readers: Mutex<Vec<usize>>,
    reader_ready: Condvar,
    #[cfg(any(test, feature = "test-utils"))]
    waiting: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

impl<P: Policy> ReadPool<P> {
    /// Run `visit` on each reader in turn, under its own lock.
    #[cfg(any(test, feature = "test-utils"))]
    pub(super) fn each(&self, mut visit: impl FnMut(&mut HostSql<P>)) {
        for reader in &self.readers {
            super::with_locked(reader, |reader| visit(reader));
        }
    }

    pub(super) fn new(readers: Vec<Mutex<HostSql<P>>>) -> Self {
        Self {
            idle_readers: Mutex::new((0..readers.len()).collect()),
            readers,
            reader_ready: Condvar::new(),
            #[cfg(any(test, feature = "test-utils"))]
            waiting: Mutex::new(None),
        }
    }

    pub(super) fn close(self) -> Vec<PolicyError<P>> {
        let mut failures = Vec::new();
        for reader in self.readers {
            if let Err(error) = panic_as_error::locks::into_unpoisoned(reader).close() {
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
            #[cfg(any(test, feature = "test-utils"))]
            if let Some(waiting) = lock_unpoisoned(&self.waiting).take() {
                waiting.send(()).unwrap();
            }
            idle = panic_as_error::locks::wait_unpoisoned(&self.reader_ready, idle);
        }
    }
}

/// One of a database's readers, reserved for its caller until it drops.
/// Dropping it wakes a caller waiting for a reader, even when a call on it
/// panics.
pub struct Reader<'a, P: Policy> {
    pool: &'a ReadPool<P>,
    index: usize,
}

impl<P: Policy> Reader<'_, P> {
    /// Run `read` on the reader.
    pub fn host<R>(&self, read: impl FnOnce(&HostSql<P>) -> R) -> R {
        super::with_locked(&self.pool.readers[self.index], |reader| read(reader))
    }
}

impl<P: Policy> Drop for Reader<'_, P> {
    fn drop(&mut self) {
        lock_unpoisoned(&self.pool.idle_readers).push(self.index);
        self.pool.reader_ready.notify_one();
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl<P: Policy> ReadPool<P> {
    /// Return a receiver that gets a message when the next caller waits for
    /// a reader.
    pub(super) fn observe_next_wait(&self) -> tokio::sync::oneshot::Receiver<()> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        assert!(lock_unpoisoned(&self.waiting).replace(sender).is_none());
        receiver
    }
}

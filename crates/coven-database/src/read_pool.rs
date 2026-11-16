//! Reader reservations keep committed reads independent of the writer.

use crate::engine_error::EngineError;
use crate::sqlite::DatabaseConnection;
use crate::store_failure::StoreFailure;
use crate::DbError;
use coven_foundation::locks::lock_unpoisoned;
use std::sync::{Condvar, Mutex};

pub(super) struct ReadPool {
    readers: Vec<Mutex<DatabaseConnection>>,
    idle_readers: Mutex<Vec<usize>>,
    reader_ready: Condvar,
    #[cfg(test)]
    waiting: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

impl ReadPool {
    #[cfg(any(test, feature = "test-utils"))]
    pub(super) fn test_track_work(&self, counter: &crate::test_utils::WorkCounter) {
        for reader in &self.readers {
            lock_unpoisoned(reader).test_track_work(counter.clone());
        }
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(super) fn test_record_statements(&self, log: &crate::test_utils::StatementLog) {
        for reader in &self.readers {
            lock_unpoisoned(reader).test_record_statements(log.clone());
        }
    }

    pub(super) fn new(readers: Vec<Mutex<DatabaseConnection>>) -> Self {
        Self {
            idle_readers: Mutex::new((0..readers.len()).collect()),
            readers,
            reader_ready: Condvar::new(),
            #[cfg(test)]
            waiting: Mutex::new(None),
        }
    }

    pub(super) fn close(self) -> Vec<EngineError<StoreFailure, DbError>> {
        let mut failures = Vec::new();
        for reader in self.readers {
            if let Err(error) = coven_foundation::locks::into_unpoisoned(reader).close() {
                failures.push(error);
            }
        }
        failures
    }

    pub(super) fn acquire_reader(&self) -> ReaderLease<'_> {
        let mut idle = lock_unpoisoned(&self.idle_readers);
        loop {
            if let Some(index) = idle.pop() {
                return ReaderLease { database: self, index };
            }
            #[cfg(test)]
            if let Some(waiting) = self.waiting.lock().unwrap().take() {
                waiting.send(()).unwrap();
            }
            idle = coven_foundation::locks::wait_unpoisoned(&self.reader_ready, idle);
        }
    }
}

// A call reserves one available reader while borrowing the database owner.
// Dropping the reservation wakes a caller even when the call panics.
pub(super) struct ReaderLease<'a> {
    database: &'a ReadPool,
    index: usize,
}

impl ReaderLease<'_> {
    pub(super) fn with_reader<R>(&self, read: impl FnOnce(&DatabaseConnection) -> R) -> R {
        super::with_locked(&self.database.readers[self.index], |reader| read(reader))
    }
}

impl Drop for ReaderLease<'_> {
    fn drop(&mut self) {
        lock_unpoisoned(&self.database.idle_readers).push(self.index);
        self.database.reader_ready.notify_one();
    }
}

#[cfg(test)]
#[path = "read_pool_tests.rs"]
mod tests;

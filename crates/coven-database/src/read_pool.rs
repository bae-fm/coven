//! Reader reservations keep committed reads independent of the writer.

use crate::{observation::ReadSet, sqlite::DatabaseConnection, DbError, SqlReadContext};
use std::sync::{Condvar, Mutex};

pub(super) struct ReadPool {
    readers: Vec<Mutex<DatabaseConnection>>,
    idle_readers: Mutex<Vec<usize>>,
    reader_ready: Condvar,
    #[cfg(test)]
    waiting: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

impl ReadPool {
    pub(super) fn new(readers: Vec<Mutex<DatabaseConnection>>) -> Self {
        Self {
            idle_readers: Mutex::new((0..readers.len()).collect()),
            readers,
            reader_ready: Condvar::new(),
            #[cfg(test)]
            waiting: Mutex::new(None),
        }
    }

    pub(super) fn close(self) -> Vec<DbError> {
        let mut failures = Vec::new();
        for reader in self.readers {
            if let Err(error) = reader
                .into_inner()
                .expect("reader connection lock poisoned")
                .close()
            {
                failures.push(error);
            }
        }
        failures
    }

    pub(super) fn acquire_reader(&self) -> ReaderLease<'_> {
        let mut idle = self.idle_readers.lock().expect("reader pool poisoned");
        loop {
            if let Some(index) = idle.pop() {
                return ReaderLease {
                    database: self,
                    index,
                };
            }
            #[cfg(test)]
            if let Some(waiting) = self.waiting.lock().unwrap().take() {
                waiting.send(()).unwrap();
            }
            idle = self.reader_ready.wait(idle).expect("reader pool poisoned");
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
    pub(super) fn local_file(
        &self,
        schema: &crate::write_schema::WriteSchema,
        directory: &coven_foundation::files::StoreDir,
        device: coven_foundation::id_source::DeviceId,
        reference: &crate::FileRef,
    ) -> Result<crate::LocalFileStream, crate::LocalFileError> {
        self.read_snapshot(|sql| sql.open_local_file(schema, directory, device, reference))
            .0
    }

    pub(super) fn read_snapshot<F, R, E>(&self, read: F) -> (Result<R, E>, ReadSet)
    where
        F: FnOnce(SqlReadContext<'_>) -> Result<R, E>,
        E: From<DbError>,
    {
        self.with_reader(|reader| reader.read_snapshot(read))
    }

    pub(super) fn with_reader<R>(&self, read: impl FnOnce(&DatabaseConnection) -> R) -> R {
        super::with_connection(&self.database.readers[self.index], read)
    }

    pub(super) fn schema_version(&self) -> Result<u32, DbError> {
        self.with_reader(DatabaseConnection::schema_version)
    }
}

impl Drop for ReaderLease<'_> {
    fn drop(&mut self) {
        self.database
            .idle_readers
            .lock()
            .expect("reader pool poisoned")
            .push(self.index);
        self.database.reader_ready.notify_one();
    }
}

#[cfg(test)]
#[path = "read_pool_tests.rs"]
mod tests;

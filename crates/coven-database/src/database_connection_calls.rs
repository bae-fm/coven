//! How a call on a store's connections gets one: a reader or the writer,
//! with the state coven keeps for that call beside it.

use super::DatabaseAccess;
use crate::sqlite::DatabaseConnection;
use crate::store_call::StoreCall;
use crate::DbError;
use coven_foundation::task::CarriesBug;

impl<L> DatabaseAccess<L> {
    /// Read one committed state on a reader: read-only decisions never take
    /// the writer (§21.2).
    pub(super) fn with_committed<R, E: From<DbError> + CarriesBug>(
        &self,
        run: impl FnOnce(&DatabaseConnection, &StoreCall<'_>) -> Result<R, E>,
    ) -> Result<R, E> {
        self.call(|| {
            self.with_reader(|reader, call| {
                crate::transaction_constants::read_with_constants(reader, call, || {
                    run(reader, call)
                })
            })
        })
    }

    /// Run `run` on an idle reader, with constants of its own.
    pub(super) fn with_reader<R>(
        &self,
        run: impl FnOnce(&DatabaseConnection, &StoreCall<'_>) -> R,
    ) -> R {
        self.with_leased(&self.database.reader(), run)
    }

    /// Run `run` on `reader`, which the caller reserved, with constants of its own.
    pub(super) fn with_leased<R>(
        &self,
        reader: &crate::sqlite_database::Reader<'_, crate::app_policy::CovenPolicy>,
        run: impl FnOnce(&DatabaseConnection, &StoreCall<'_>) -> R,
    ) -> R {
        reader.host(|reader| run(reader, &StoreCall::new(None, &self.tests)))
    }

    pub(super) fn with_writer<R, E: From<DbError>>(
        &self,
        run: impl FnOnce(&mut DatabaseConnection, &StoreCall<'_>) -> Result<R, E>,
    ) -> Result<R, E> {
        self.database.host(|writer| {
            #[cfg(any(test, feature = "test-utils"))]
            self.tests.record_work(|work| work.writer_holds += 1);
            if let Some(error) = self.observer.error() {
                return Err(DbError::from(error).into());
            }
            let device = writer.observes_commits().then_some(self.device);
            let call = StoreCall::new(device, &self.tests);
            #[cfg(any(test, feature = "test-utils"))]
            let run = |writer, call| coven_foundation::test_work::writing(|| run(writer, call));
            run(writer, &call)
        })
    }
}

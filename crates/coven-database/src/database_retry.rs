//! Persisted retry waits through the owning database (§16.5).

use super::Database;
use crate::{
    retry::{self, RetrySubject, RetryWait},
    DbError,
};

impl Database {
    /// Every persisted wait, for arming on reopen (§16.5).
    pub async fn retry_waits(&self) -> Result<Vec<(RetrySubject, RetryWait)>, DbError> {
        self.call(move |inner| {
            inner
                .access
                .readers
                .acquire_reader()
                .with_reader(retry::read_all)
        })
        .await
    }

    /// The subject's persisted wait, if one is armed.
    pub async fn retry_wait(&self, subject: RetrySubject) -> Result<Option<RetryWait>, DbError> {
        self.call(move |inner| {
            inner
                .access
                .readers
                .acquire_reader()
                .with_reader(|db| retry::find(db, subject))
        })
        .await
    }

    /// Persist `T` and `W` before the subject waits. A failure here fails the
    /// initiating work; it is never permission to retry immediately.
    pub async fn record_retry(
        &self,
        subject: RetrySubject,
        wait: RetryWait,
    ) -> Result<(), DbError> {
        self.call(move |inner| {
            inner
                .access
                .with_writer(|writer| writer.transaction(|db| retry::save(db, subject, &wait)))
        })
        .await
    }

    /// Forget a wait once its work succeeds or no longer retries automatically.
    pub async fn clear_retry(&self, subject: RetrySubject) -> Result<(), DbError> {
        self.call(move |inner| {
            inner
                .access
                .with_writer(|writer| writer.transaction(|db| retry::clear(db, subject)))
        })
        .await
    }
}

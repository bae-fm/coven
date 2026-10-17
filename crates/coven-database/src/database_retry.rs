//! Persisted retry waits through the owning database (§6.7).

use super::Database;
use crate::{
    retry::{self, RetrySubject, RetryWait},
    DbError,
};

impl Database {
    /// Persist a failed pass's connection condition and retry deadline together.
    /// A failure already owned by a slot or operation supplies no connection record.
    pub async fn record_sync_retry<P: crate::PendingValue>(
        &self,
        connection: P::Subject,
        record: Option<P>,
        wait: Option<RetryWait>,
    ) -> Result<(), DbError>
    where
        P::Subject: Send + 'static,
    {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.pending_transaction(|db| {
                    if let Some(record) = record {
                        crate::pending::save(db, &record)?;
                    } else {
                        crate::pending::clear(db, &connection)?;
                    }
                    match wait {
                        Some(wait) => retry::save(db, RetrySubject::Sync, &wait),
                        None => retry::clear(db, RetrySubject::Sync),
                    }
                })
            })
        })
        .await
    }

    /// A successful pass settles its connection condition and wait atomically.
    pub async fn clear_sync_retry<S: serde::Serialize + Send + 'static>(
        &self,
        connection: S,
    ) -> Result<(), DbError> {
        self.call(move |inner| {
            let changed = inner.access.readers.acquire_reader().with_reader(|db| {
                Ok::<_, DbError>(
                    retry::find(db, RetrySubject::Sync)?.is_some()
                        || crate::pending::recorded(db, &connection)?,
                )
            })?;
            if !changed {
                return Ok(());
            }
            inner.access.with_writer(|writer| {
                writer.pending_transaction(|db| {
                    retry::clear(db, RetrySubject::Sync)?;
                    crate::pending::clear(db, &connection)
                })
            })
        })
        .await
    }

    /// Persist an operation's failure and its automatic retry deadline together.
    pub async fn record_operation_retry<P: crate::PendingValue>(
        &self,
        record: P,
        wait: Option<RetryWait>,
    ) -> Result<(), DbError> {
        self.call(move |inner| {
            let id = record.operation().ok_or(DbError::Invariant {
                what: "an operation retry names its journal row",
            })?;
            inner.access.with_writer(|writer| {
                writer.pending_transaction(|db| {
                    crate::pending::save(db, &record)?;
                    match wait {
                        Some(wait) => retry::save(db, RetrySubject::Operation(id), &wait),
                        None => retry::clear(db, RetrySubject::Operation(id)),
                    }
                })
            })
        })
        .await
    }

    /// Every persisted wait, for arming on reopen (§6.7).
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

    /// Every eager download's persisted wait (§6.7).
    pub async fn download_retry_waits(&self) -> Result<Vec<(RetrySubject, RetryWait)>, DbError> {
        self.call(move |inner| {
            inner
                .access
                .readers
                .acquire_reader()
                .with_reader(retry::read_downloads)
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

    /// Record a failed download's pending reason and, in the same
    /// transaction, its next wait, or delete its wait when it no longer
    /// retries automatically. The wait belongs to that record: clearing the
    /// record, for any reason, deletes it (§6.7).
    pub async fn record_pending_retry<P: crate::PendingValue>(
        &self,
        record: P,
        subject: RetrySubject,
        wait: Option<RetryWait>,
    ) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                if record.operation().is_some() {
                    return Err(DbError::Invariant {
                        what: "a download retry has no operation",
                    });
                }
                writer.pending_transaction(|db| {
                    crate::pending::save(db, &record)?;
                    match &wait {
                        Some(wait) => {
                            let pending = serde_json::to_vec(record.subject())
                                .map_err(DbError::PendingEncoding)?;
                            retry::save_download(db, subject, &pending, wait)
                        }
                        None => retry::clear(db, subject),
                    }
                })
            })
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

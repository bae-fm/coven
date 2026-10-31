//! Connection attempts and passes obey the same failure classes and durable wait.

use crate::{
    PassFailure, PendingRecord, PendingSubject, Retry, RetryWaits, SyncError, SyncFailure,
};
use coven_database::{Database, RetrySubject};

/// What permits this connection to attempt another pass. Time is held by
/// `RetryWaits`. A failure that waits for something other than time holds
/// every pass, and keeps what the status showed for it.
pub enum ConnectionRetry {
    /// The persisted backoff and provider cooldown decide when to try.
    Automatic,
    /// Only new credentials or a new connection can clear this failure.
    AppAction {
        /// The connection the failure happened on.
        version: u64,
        /// What the status showed for it.
        failure: PassFailure,
    },
    /// Only reopening with an applicable update can retry the input.
    AfterUpdate(PassFailure),
    /// This installation must be reset before it can retry.
    Never(PassFailure),
}

impl ConnectionRetry {
    /// Reopening is a new connection, but cannot clear a permanent failure or
    /// recheck immutable input with the same software.
    pub async fn open(database: &Database) -> Result<Self, SyncError> {
        let record = database
            .pending::<PendingRecord>()
            .await?
            .into_iter()
            .find(|record| record.subject == PendingSubject::Connection);
        let Some(record) = record else {
            return Ok(Self::Automatic);
        };
        let never = record.retry() == Retry::Never;
        let after_update = !never
            && record.retry() == Retry::AfterUpdate
            && !database.pending_can_retry::<PendingRecord>(PendingSubject::Connection).await?;
        if !never && !after_update {
            return Ok(Self::Automatic);
        }
        let failure = PassFailure::Failed {
            error: match record.reason {
                crate::PendingReason::Storage { failure, .. } => {
                    SyncFailure::Storage(std::sync::Arc::new(failure.into()))
                }
                crate::PendingReason::Removed => SyncFailure::Removed,
                crate::PendingReason::UpdateRequired(update) => SyncFailure::UpdateRequired(update),
                _ => SyncError::Pending(Box::new(record)).into(),
            },
        };
        Ok(if never { Self::Never(failure) } else { Self::AfterUpdate(failure) })
    }

    /// The failure holding every pass, if evidence does not permit an
    /// attempt; the caller also enforces its time wait.
    pub fn held(&self, waits: &RetryWaits) -> Option<&PassFailure> {
        match self {
            Self::Automatic => None,
            Self::AppAction { version, failure } => {
                (*version == waits.connection_version()).then_some(failure)
            }
            Self::AfterUpdate(failure) | Self::Never(failure) => Some(failure),
        }
    }

    /// Save the failure and next deadline in one transaction before waiting,
    /// and return what the status shows for it: `shown` turns the error into
    /// that failure. If saving fails, the returned error is that failure
    /// together with the save's, and only new credentials or a new connection
    /// retry it.
    pub async fn failed(
        &mut self,
        database: &Database,
        waits: &RetryWaits,
        reason: crate::PendingReason,
        connection_version: u64,
        error: SyncError,
        shown: impl FnOnce(SyncError) -> PassFailure,
    ) -> Result<PassFailure, PassFailure> {
        if matches!(error, SyncError::DeviceIdInUse(_)) {
            let failure = shown(error);
            *self = Self::Never(failure.clone());
            return Ok(failure);
        }
        let record = PendingRecord::new(PendingSubject::Connection, reason);
        let retry = match &error {
            SyncError::Pending(record) => record.retry(),
            _ => record.retry(),
        };
        // These failures already belong to a required input; a second
        // connection record would duplicate that input's pending condition.
        let connection = !matches!(
            error,
            SyncError::Pending(_)
                | SyncError::Damaged(_)
                | SyncError::KeyUnavailable(_)
                | SyncError::UpdateRequired(_)
        );
        // Whether the save itself failed: its wait is then held for the app.
        let saved = async {
            let previous = database
                .retry_wait(RetrySubject::Sync)
                .await
                .map_err(|error| (error.into(), false))?;
            let wait = match retry {
                Retry::Automatic => {
                    Some(waits.next(previous.as_ref()).map_err(|error| (error, false))?)
                }
                _ => None,
            };
            database
                .record_sync_retry(PendingSubject::Connection, connection.then_some(record), wait)
                .await
                .map_err(|error| (error.into(), true))?;
            Ok::<_, (SyncError, bool)>(wait)
        }
        .await;
        let wait = match saved {
            Ok(wait) => wait,
            Err((cleanup, held)) => {
                let failure = PassFailure::Failed {
                    error: SyncError::Cleanup {
                        operation: Box::new(error),
                        cleanup: Box::new(cleanup),
                    }
                    .into(),
                };
                if held {
                    waits.hold(RetrySubject::Sync);
                    *self =
                        Self::AppAction { version: connection_version, failure: failure.clone() };
                }
                return Err(failure);
            }
        };
        match wait {
            Some(wait) => waits.arm(RetrySubject::Sync, &wait),
            None => waits.forget(RetrySubject::Sync),
        }
        let failure = shown(error);
        *self = match retry {
            Retry::Automatic => Self::Automatic,
            Retry::AppAction => {
                Self::AppAction { version: connection_version, failure: failure.clone() }
            }
            Retry::AfterUpdate => Self::AfterUpdate(failure.clone()),
            Retry::Never => Self::Never(failure.clone()),
        };
        Ok(failure)
    }

    /// A completed pass clears its condition and exponent together.
    pub async fn succeeded(
        &mut self,
        database: &Database,
        waits: &RetryWaits,
    ) -> Result<(), SyncError> {
        database.clear_sync_retry(PendingSubject::Connection).await?;
        waits.forget(RetrySubject::Sync);
        *self = Self::Automatic;
        Ok(())
    }
}

/// The last attempt is offline only when its network failure received no reply.
pub fn connection_failure(error: SyncError) -> PassFailure {
    match SyncFailure::from(error) {
        SyncFailure::Storage(error)
            if crate::pending::offline(error.failure(), error.answered()) =>
        {
            PassFailure::Offline { error }
        }
        error => PassFailure::Failed { error },
    }
}

#[cfg(test)]
#[path = "connection_retry_tests.rs"]
mod tests;

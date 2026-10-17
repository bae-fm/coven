//! Connection attempts and passes obey the same failure classes and durable wait.

use crate::{PendingRecord, PendingSubject, Retry, RetryWaits, SyncError, SyncFailure, SyncStatus};
use coven_database::{Database, RetrySubject};
use coven_storage::StorageFailure;

/// What permits this connection to attempt another pass. Time is held by `RetryWaits`.
pub enum ConnectionRetry {
    /// The persisted backoff and provider cooldown decide when to try.
    Automatic,
    /// Only new credentials or a new connection can clear this failure.
    AppAction(u64),
    /// Only reopening with an applicable update can retry the input.
    AfterUpdate,
    /// This installation must be reset before it can retry.
    Never,
}

impl ConnectionRetry {
    /// Reopening is a new connection, but cannot clear a permanent failure or
    /// recheck immutable input with the same software. Return the retained
    /// failure so a blocked connection can publish it.
    pub async fn open(database: &Database) -> Result<(Self, Option<SyncFailure>), SyncError> {
        let record = database
            .pending::<PendingRecord>()
            .await?
            .into_iter()
            .find(|record| record.subject == PendingSubject::Connection);
        let retry = match &record {
            Some(record) if record.retry() == Retry::Never => Self::Never,
            Some(record)
                if record.retry() == Retry::AfterUpdate
                    && !database
                        .pending_can_retry::<PendingRecord>(PendingSubject::Connection)
                        .await? =>
            {
                Self::AfterUpdate
            }
            _ => Self::Automatic,
        };
        let failure = record.map(|record| match record.reason {
            crate::PendingReason::Storage { failure, .. } => {
                SyncFailure::Storage(std::sync::Arc::new(failure.into()))
            }
            crate::PendingReason::Removed => SyncFailure::Removed,
            crate::PendingReason::UpdateRequired(update) => SyncFailure::UpdateRequired(update),
            _ => SyncError::Pending(Box::new(record)).into(),
        });
        Ok((retry, failure))
    }

    /// Whether evidence permits an attempt; the caller also enforces its time wait.
    pub fn ready(&self, waits: &RetryWaits) -> bool {
        match self {
            Self::Automatic => true,
            Self::AppAction(version) => *version != waits.connection_version(),
            Self::AfterUpdate | Self::Never => false,
        }
    }

    /// Save the failure and next deadline in one transaction before waiting.
    pub async fn failed(
        &mut self,
        database: &Database,
        waits: &RetryWaits,
        reason: crate::PendingReason,
        connection_version: u64,
        error: &SyncError,
    ) -> Result<(), SyncError> {
        if matches!(error, SyncError::DeviceIdInUse(_)) {
            *self = Self::Never;
            return Ok(());
        }
        let record = PendingRecord {
            subject: PendingSubject::Connection,
            reason,
        };
        let retry = match error {
            SyncError::Pending(record) => record.retry(),
            _ => record.retry(),
        };
        let previous = database.retry_wait(RetrySubject::Sync).await?;
        let wait = match retry {
            Retry::Automatic => Some(waits.next(previous.as_ref())?),
            _ => None,
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
        if let Err(error) = database
            .record_sync_retry(
                PendingSubject::Connection,
                connection.then_some(record),
                wait,
            )
            .await
        {
            waits.hold(RetrySubject::Sync);
            *self = Self::AppAction(connection_version);
            return Err(error.into());
        }
        match wait {
            Some(wait) => waits.arm(RetrySubject::Sync, &wait),
            None => waits.forget(RetrySubject::Sync),
        }
        *self = match retry {
            Retry::Automatic => Self::Automatic,
            Retry::AppAction => Self::AppAction(connection_version),
            Retry::AfterUpdate => Self::AfterUpdate,
            Retry::Never => Self::Never,
        };
        Ok(())
    }

    /// A completed pass clears its condition and exponent together.
    pub async fn succeeded(
        &mut self,
        database: &Database,
        waits: &RetryWaits,
    ) -> Result<(), SyncError> {
        database
            .clear_sync_retry(PendingSubject::Connection)
            .await?;
        waits.forget(RetrySubject::Sync);
        *self = Self::Automatic;
        Ok(())
    }
}

/// The last attempt is offline only when its network failure received no reply.
pub fn connection_failure(error: SyncError) -> SyncStatus {
    match SyncFailure::from(error) {
        SyncFailure::Storage(error)
            if error.failure() == StorageFailure::Network && !error.answered() =>
        {
            SyncStatus::Offline { error }
        }
        error => SyncStatus::Failed { error },
    }
}

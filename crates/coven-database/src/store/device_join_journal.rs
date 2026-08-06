//! The adjacency rules the device-join journal's compare-and-swap update
//! enforces between recorded steps, and the failures a journal write reports.

use coven_protocol::store_commit::device_join_exchange::DeviceJoinAbandonment;
use coven_protocol::store_commit::device_join_journal::{
    DeviceJoinJournalRecord, DeviceJoinRoleProgress, JoinerJoinProgress, OwnerJoinProgress,
    ProviderAdminJoinProgress,
};

/// A journal transition that contradicts the durable record. Workflow errors
/// wrap it at the operation boundary.
#[derive(Debug, thiserror::Error)]
pub enum DeviceJoinJournalError {
    #[error("device join journal transition is not the declared adjacent transition")]
    NonAdjacentJournalTransition,
    #[error("device join journal has a different durable value for this role and attempt")]
    JournalConflict,
    #[error("device join journal: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("device join journal: {0}")]
    Database(#[from] crate::DbError),
}

/// The progress values a role's first record may hold.
pub fn validate_initial_progress(
    progress: &DeviceJoinRoleProgress,
) -> Result<(), DeviceJoinJournalError> {
    if matches!(
        progress,
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Offered(_))
            | DeviceJoinRoleProgress::ProviderAdministrator(
                ProviderAdminJoinProgress::AccessRequested(_)
            )
            | DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::OfferReceived(_))
    ) {
        Ok(())
    } else {
        Err(DeviceJoinJournalError::NonAdjacentJournalTransition)
    }
}

pub fn require_initial(record: &DeviceJoinJournalRecord) -> Result<(), DeviceJoinJournalError> {
    validate_initial_progress(&record.progress)
}

/// A replacement of an attempt this device never ran opens the journal at a
/// terminal, so only the write-revoked terminals may be installed first.
pub fn require_replacement_terminal(
    record: &DeviceJoinJournalRecord,
) -> Result<(), DeviceJoinJournalError> {
    if matches!(
        &*record.progress,
        DeviceJoinRoleProgress::ProviderAdministrator(ProviderAdminJoinProgress::WriteRevoked(_))
            | DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::WriteRevoked(_))
    ) {
        Ok(())
    } else {
        Err(DeviceJoinJournalError::JournalConflict)
    }
}

pub fn validate_successor(
    previous: &DeviceJoinJournalRecord,
    next: &DeviceJoinJournalRecord,
) -> Result<(), DeviceJoinJournalError> {
    if previous.attempt_id != next.attempt_id {
        return Err(DeviceJoinJournalError::JournalConflict);
    }
    validate_transition(&previous.progress, &next.progress)
}

/// The joiner record an observed abandonment advances to, or `None` when the
/// journal already holds that exact abandonment.
pub fn joiner_abandonment_transition(
    record: &DeviceJoinJournalRecord,
    abandonment: &DeviceJoinAbandonment,
) -> Result<Option<DeviceJoinJournalRecord>, DeviceJoinJournalError> {
    if record.attempt_id != abandonment.abandonment.attempt_id {
        return Err(DeviceJoinJournalError::JournalConflict);
    }
    match &*record.progress {
        DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::Abandoned(existing)) => {
            if existing == abandonment {
                Ok(None)
            } else {
                Err(DeviceJoinJournalError::JournalConflict)
            }
        }
        DeviceJoinRoleProgress::Joiner(
            JoinerJoinProgress::AccessRequested(_) | JoinerJoinProgress::ApprovalReceived(_),
        ) => Ok(Some(DeviceJoinJournalRecord {
            attempt_id: record.attempt_id,
            progress: Box::new(DeviceJoinRoleProgress::Joiner(
                JoinerJoinProgress::Abandoned(abandonment.clone()),
            )),
        })),
        _ => Err(DeviceJoinJournalError::JournalConflict),
    }
}

fn validate_transition(
    previous: &DeviceJoinRoleProgress,
    next: &DeviceJoinRoleProgress,
) -> Result<(), DeviceJoinJournalError> {
    let adjacent = match (previous, next) {
        (DeviceJoinRoleProgress::Owner(previous), DeviceJoinRoleProgress::Owner(next)) => {
            owner_adjacent(previous, next)
        }
        (
            DeviceJoinRoleProgress::ProviderAdministrator(previous),
            DeviceJoinRoleProgress::ProviderAdministrator(next),
        ) => provider_admin_adjacent(previous, next),
        (DeviceJoinRoleProgress::Joiner(previous), DeviceJoinRoleProgress::Joiner(next)) => {
            joiner_adjacent(previous, next)
        }
        _ => false,
    };
    if adjacent {
        Ok(())
    } else {
        Err(DeviceJoinJournalError::NonAdjacentJournalTransition)
    }
}

fn owner_adjacent(previous: &OwnerJoinProgress, next: &OwnerJoinProgress) -> bool {
    matches!(
        (previous, next),
        (
            OwnerJoinProgress::Offered(_),
            OwnerJoinProgress::RegistrationRequested(_)
        ) | (
            OwnerJoinProgress::Offered(_),
            OwnerJoinProgress::AbandonmentCreateIntent { .. }
        ) | (
            OwnerJoinProgress::RegistrationRequested(_),
            OwnerJoinProgress::AttemptActivated(_)
        ) | (
            OwnerJoinProgress::RegistrationRequested(_),
            OwnerJoinProgress::AbandonmentCreateIntent { .. }
        ) | (
            OwnerJoinProgress::AbandonmentCreateIntent { .. },
            OwnerJoinProgress::Abandoned(_)
        ) | (
            OwnerJoinProgress::AttemptActivated(_),
            OwnerJoinProgress::ActivationCreateIntent { .. }
        ) | (
            OwnerJoinProgress::AttemptActivated(_),
            OwnerJoinProgress::CancellationCreateIntent { .. }
        ) | (
            OwnerJoinProgress::ActivationCreateIntent { .. },
            OwnerJoinProgress::ActivationPrepared { .. }
        ) | (
            OwnerJoinProgress::CancellationCreateIntent { .. },
            OwnerJoinProgress::Cancelled(_)
        ) | (
            OwnerJoinProgress::Cancelled(_),
            OwnerJoinProgress::CleanupReceiptCreateIntent { .. }
        ) | (
            OwnerJoinProgress::CleanupReceiptCreateIntent { .. },
            OwnerJoinProgress::CleanupReceipt(_)
        ) | (
            OwnerJoinProgress::CleanupReceipt(_),
            OwnerJoinProgress::CleanupActivated(_)
        ) | (
            OwnerJoinProgress::CleanupActivated(_),
            OwnerJoinProgress::CancelledComplete(_)
        )
    )
}

fn provider_admin_adjacent(
    previous: &ProviderAdminJoinProgress,
    next: &ProviderAdminJoinProgress,
) -> bool {
    matches!(
        (previous, next),
        (
            ProviderAdminJoinProgress::AccessRequested(_),
            ProviderAdminJoinProgress::AccessGrantPrepared { .. }
        ) | (
            ProviderAdminJoinProgress::AccessGrantPrepared { .. },
            ProviderAdminJoinProgress::ApprovalPrepared(_)
        ) | (
            ProviderAdminJoinProgress::ApprovalPrepared(_),
            ProviderAdminJoinProgress::AttemptObserved(_)
        ) | (
            ProviderAdminJoinProgress::ApprovalPrepared(_),
            ProviderAdminJoinProgress::CleanupIntent { .. }
        ) | (
            ProviderAdminJoinProgress::AttemptObserved(_),
            ProviderAdminJoinProgress::ChallengeCreateIntent(_)
        ) | (
            ProviderAdminJoinProgress::AttemptObserved(_),
            ProviderAdminJoinProgress::CleanupIntent { .. }
        ) | (
            ProviderAdminJoinProgress::ChallengeCreateIntent(_),
            ProviderAdminJoinProgress::ProviderReady(_)
        ) | (
            ProviderAdminJoinProgress::ChallengeCreateIntent(_),
            ProviderAdminJoinProgress::CleanupIntent { .. }
        ) | (
            ProviderAdminJoinProgress::ProviderReady(_),
            ProviderAdminJoinProgress::ResponseObserved(_)
        ) | (
            ProviderAdminJoinProgress::ProviderReady(_),
            ProviderAdminJoinProgress::CleanupIntent { .. }
        ) | (
            ProviderAdminJoinProgress::ResponseObserved(_),
            ProviderAdminJoinProgress::Completed(_)
        ) | (
            ProviderAdminJoinProgress::ResponseObserved(_),
            ProviderAdminJoinProgress::CleanupIntent { .. }
        ) | (
            ProviderAdminJoinProgress::ApprovalPrepared(_),
            ProviderAdminJoinProgress::WriteRevoked(_)
        ) | (
            ProviderAdminJoinProgress::AttemptObserved(_),
            ProviderAdminJoinProgress::WriteRevoked(_)
        ) | (
            ProviderAdminJoinProgress::ChallengeCreateIntent(_),
            ProviderAdminJoinProgress::WriteRevoked(_)
        ) | (
            ProviderAdminJoinProgress::ProviderReady(_),
            ProviderAdminJoinProgress::WriteRevoked(_)
        ) | (
            ProviderAdminJoinProgress::ResponseObserved(_),
            ProviderAdminJoinProgress::WriteRevoked(_)
        ) | (
            ProviderAdminJoinProgress::CleanupIntent { .. },
            ProviderAdminJoinProgress::Cancelled(_)
        ) | (
            ProviderAdminJoinProgress::CleanupIntent { .. },
            ProviderAdminJoinProgress::WriteRevoked(_)
        )
    )
}

fn joiner_adjacent(previous: &JoinerJoinProgress, next: &JoinerJoinProgress) -> bool {
    matches!(
        (previous, next),
        (
            JoinerJoinProgress::OfferReceived(_),
            JoinerJoinProgress::AccessRequested(_)
        ) | (
            JoinerJoinProgress::AccessRequested(_),
            JoinerJoinProgress::ApprovalReceived(_)
        ) | (
            JoinerJoinProgress::AccessRequested(_),
            JoinerJoinProgress::Abandoned(_)
        ) | (
            JoinerJoinProgress::ApprovalReceived(_),
            JoinerJoinProgress::RegistrationPrepared(_)
        ) | (
            JoinerJoinProgress::ApprovalReceived(_),
            JoinerJoinProgress::Abandoned(_)
        ) | (
            JoinerJoinProgress::RegistrationPrepared(_),
            JoinerJoinProgress::Ready(_)
        ) | (
            JoinerJoinProgress::RegistrationPrepared(_),
            JoinerJoinProgress::CleanupIntent { .. }
        ) | (
            JoinerJoinProgress::Ready(_),
            JoinerJoinProgress::ActivationObserved { .. }
        ) | (
            JoinerJoinProgress::Ready(_),
            JoinerJoinProgress::CleanupIntent { .. }
        ) | (
            JoinerJoinProgress::RegistrationPrepared(_),
            JoinerJoinProgress::WriteRevoked(_)
        ) | (
            JoinerJoinProgress::CleanupIntent { .. },
            JoinerJoinProgress::Cancelled(_)
        ) | (
            JoinerJoinProgress::ActivationObserved { .. },
            JoinerJoinProgress::Activated(_)
        ) | (
            JoinerJoinProgress::Cancelled(_),
            JoinerJoinProgress::CleanupActivated(_)
        ) | (
            JoinerJoinProgress::WriteRevoked(_),
            JoinerJoinProgress::CleanupActivated(_)
        ) | (
            JoinerJoinProgress::CleanupActivated(_),
            JoinerJoinProgress::CancelledComplete(_)
        )
    )
}

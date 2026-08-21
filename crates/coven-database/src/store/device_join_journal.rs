//! The adjacency rules the device-join journal's compare-and-swap update
//! enforces between recorded steps, and the failures a journal write reports.

use coven_protocol::store_commit::device_join_exchange::DeviceJoinAbandonment;
use coven_protocol::store_commit::device_join_journal::{
    DeviceJoinJournalRecord, DeviceJoinRoleProgress, JoinerJoinProgress, OwnerJoinProgress,
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

/// The admitting device's steps, in one chain. One device answers the access
/// request, prepares the storage grant, signs the approval, registers the
/// joining device and activates it, so every step below follows the previous
/// one on the same journal row.
///
/// The chain ends where it ends. Up to the attempt commit the admitting device
/// can still give up, and abandonment says so; past it there is nothing to take
/// back, because approving the join is what granted the joining device storage
/// access and undoing that is member removal with a key rotation.
fn owner_adjacent(previous: &OwnerJoinProgress, next: &OwnerJoinProgress) -> bool {
    if let (
        OwnerJoinProgress::AccessRequested(request),
        OwnerJoinProgress::ApprovalPrepared(approval),
    ) = (previous, next)
    {
        return approval.request.as_ref() == request
            && matches!(
                approval.admission,
                coven_protocol::store_commit::device_join_exchange::DeviceProviderAdmission::SamePrincipal
            );
    }
    if let (
        OwnerJoinProgress::ProviderReady(ready),
        OwnerJoinProgress::Completed(
            coven_protocol::store_commit::device_join_exchange::DeviceProviderAdmissionCompletion::SamePrincipal {
                bootstrap,
            },
        ),
    ) = (previous, next)
    {
        return ready == bootstrap.as_ref();
    }
    matches!(
        (previous, next),
        (
            OwnerJoinProgress::Offered(_),
            OwnerJoinProgress::AccessRequested(_)
        ) | (
            OwnerJoinProgress::AccessRequested(_),
            OwnerJoinProgress::AccessGrantPrepared { .. }
        ) | (
            OwnerJoinProgress::AccessGrantPrepared { .. },
            OwnerJoinProgress::ApprovalPrepared(_)
        ) | (
            OwnerJoinProgress::ApprovalPrepared(_),
            OwnerJoinProgress::RegistrationRequested(_)
        ) | (
            OwnerJoinProgress::RegistrationRequested(_),
            OwnerJoinProgress::AttemptActivated(_)
        ) | (
            OwnerJoinProgress::RegistrationRequested(_),
            OwnerJoinProgress::SamePrincipalActivationCreateIntent { .. }
        ) | (
            OwnerJoinProgress::SamePrincipalActivationCreateIntent { .. },
            OwnerJoinProgress::SamePrincipalCompleted { .. }
        ) | (
            OwnerJoinProgress::Offered(_),
            OwnerJoinProgress::AbandonmentCreateIntent { .. }
        ) | (
            OwnerJoinProgress::AccessRequested(_),
            OwnerJoinProgress::AbandonmentCreateIntent { .. }
        ) | (
            OwnerJoinProgress::AccessGrantPrepared { .. },
            OwnerJoinProgress::AbandonmentCreateIntent { .. }
        ) | (
            OwnerJoinProgress::ApprovalPrepared(_),
            OwnerJoinProgress::AbandonmentCreateIntent { .. }
        ) | (
            OwnerJoinProgress::RegistrationRequested(_),
            OwnerJoinProgress::AbandonmentCreateIntent { .. }
        ) | (
            OwnerJoinProgress::AbandonmentCreateIntent { .. },
            OwnerJoinProgress::Abandoned(_)
        ) | (
            OwnerJoinProgress::AttemptActivated(_),
            OwnerJoinProgress::ChallengeCreateIntent(_)
        ) | (
            OwnerJoinProgress::ChallengeCreateIntent(_),
            OwnerJoinProgress::ProviderReady(_)
        ) | (
            OwnerJoinProgress::ProviderReady(_),
            OwnerJoinProgress::ResponseObserved(_)
        ) | (
            OwnerJoinProgress::ResponseObserved(_),
            OwnerJoinProgress::Completed(_)
        ) | (
            OwnerJoinProgress::Completed(_),
            OwnerJoinProgress::ActivationCreateIntent { .. }
        ) | (
            OwnerJoinProgress::ActivationCreateIntent { .. },
            OwnerJoinProgress::ActivationPrepared { .. }
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
            JoinerJoinProgress::Ready(_),
            JoinerJoinProgress::ActivationObserved { .. }
        )
    )
}

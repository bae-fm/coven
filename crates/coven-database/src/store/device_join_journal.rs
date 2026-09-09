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
/// Whether `record` is a joiner row an abandonment may retire.
///
/// The joining device keeps no abandoned state: accepting an abandonment
/// deletes the row, so afterwards its absence is the whole answer and there is
/// no state left for a second acceptance to compare against. Only the two
/// waiting states can be abandoned — past them the device has been approved and
/// holds storage access, which an abandonment does not take back.
pub fn joiner_abandonment_retires(
    record: &DeviceJoinJournalRecord,
    abandonment: &DeviceJoinAbandonment,
) -> Result<(), DeviceJoinJournalError> {
    if record.attempt_id != abandonment.abandonment.attempt_id {
        return Err(DeviceJoinJournalError::JournalConflict);
    }
    match &*record.progress {
        DeviceJoinRoleProgress::Joiner(
            JoinerJoinProgress::AccessRequested(_) | JoinerJoinProgress::ApprovalReceived(_),
        ) => Ok(()),
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
    if let OwnerJoinProgress::StorePublicationPrepared(prepared) = previous {
        return prepared
            .validates_accepted_progress(prepared_operation_attempt_id(&prepared.operation), next);
    }
    if let OwnerJoinProgress::StorePublicationPrepared(prepared) = next {
        return owner_publication_follows(previous, &prepared.operation);
    }
    matches!(
        (previous, next),
        (
            OwnerJoinProgress::Offered(_),
            OwnerJoinProgress::AccessRequested(_)
        ) | (
            OwnerJoinProgress::AccessGrantActivated { .. },
            OwnerJoinProgress::ApprovalPrepared(_)
        ) | (
            OwnerJoinProgress::ApprovalPrepared(_),
            OwnerJoinProgress::RegistrationRequested(_)
        ) | (
            OwnerJoinProgress::SamePrincipalActivated { .. },
            OwnerJoinProgress::SamePrincipalCompleted { .. }
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
        )
    )
}

fn owner_publication_follows(
    previous: &OwnerJoinProgress,
    operation: &coven_protocol::store_commit::device_join_journal::OwnerJoinPublication,
) -> bool {
    use coven_protocol::store_commit::device_join_journal::OwnerJoinPublication;

    match (previous, operation) {
        (
            OwnerJoinProgress::AccessRequested(previous),
            OwnerJoinPublication::ProviderAccessGrant { request, .. },
        ) => previous == request,
        (
            OwnerJoinProgress::RegistrationRequested(previous),
            OwnerJoinPublication::Attempt { request }
            | OwnerJoinPublication::SamePrincipalActivation { request },
        ) => previous == request,
        (
            OwnerJoinProgress::Completed(previous),
            OwnerJoinPublication::JoinActivation { completion },
        ) => previous == completion,
        (previous, OwnerJoinPublication::Abandonment { offer, .. }) => {
            let durable_offer = match previous {
                OwnerJoinProgress::Offered(durable) => Some(durable),
                OwnerJoinProgress::AccessRequested(request) => Some(request.offer.as_ref()),
                OwnerJoinProgress::AccessGrantActivated { request, .. } => {
                    Some(request.offer.as_ref())
                }
                OwnerJoinProgress::ApprovalPrepared(approval) => {
                    Some(approval.request.offer.as_ref())
                }
                OwnerJoinProgress::RegistrationRequested(request) => {
                    Some(request.approval().request.offer.as_ref())
                }
                _ => None,
            };
            durable_offer == Some(offer)
        }
        _ => false,
    }
}

fn prepared_operation_attempt_id(
    operation: &coven_protocol::store_commit::device_join_journal::OwnerJoinPublication,
) -> coven_protocol::store_commit::DeviceJoinAttemptId {
    use coven_protocol::store_commit::device_join_journal::OwnerJoinPublication;
    match operation {
        OwnerJoinPublication::ProviderAccessGrant { request, .. } => request.offer.attempt_id,
        OwnerJoinPublication::Attempt { request }
        | OwnerJoinPublication::SamePrincipalActivation { request } => {
            request.approval().request.offer.attempt_id
        }
        OwnerJoinPublication::Abandonment { offer, .. } => offer.attempt_id,
        OwnerJoinPublication::JoinActivation { completion } => completion.attempt_id(),
    }
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
            JoinerJoinProgress::ApprovalReceived(_),
            JoinerJoinProgress::RegistrationPrepared(_)
        ) | (
            JoinerJoinProgress::RegistrationPrepared(_),
            JoinerJoinProgress::Ready(_)
        ) | (
            JoinerJoinProgress::Ready(_),
            JoinerJoinProgress::ActivationObserved { .. }
        )
    )
}

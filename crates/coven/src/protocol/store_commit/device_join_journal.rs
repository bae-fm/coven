//! The durable device-join journal model: role progress, status and action
//! derivation, and the adjacency rules the journal database's
//! compare-and-swap update enforces between recorded steps.

use serde::{Deserialize, Serialize};

use crate::protocol::objects::ExactObjectRef;
use crate::protocol::provider::StoreMemberProviderAccessGrant;
use crate::protocol::store_commit::device_join::{DeviceJoinAttemptRef, DeviceJoinOutcomeRef};
use crate::protocol::store_commit::device_join_exchange::{
    DeviceJoinAbandonment, DeviceJoinActivation, DeviceJoinCancellation,
    DeviceJoinCleanupActivation, DeviceJoinCleanupReceipt, DeviceJoinOffer, DeviceJoinProducer,
    DeviceJoinProducerWriteRevocation, DeviceJoinReadiness, DeviceProviderAccessRequest,
    DeviceProviderAdmissionApproval, DeviceProviderAdmissionCompletion, DeviceRegistrationRequest,
    JoinedStore, JoinerJoinClosure, JoinerJoinTerminal, JoinerResponseDisposition,
    ProviderAdminJoinClosure, ProviderAdminJoinTerminal, ProviderChallengeDisposition,
    ProviderReadyDeviceBootstrap, ProvisionalDeviceBootstrap, SlotDisposition,
};

/// A join journal transition that contradicts the durable record. Workflow
/// errors wrap it at the operation boundary.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DeviceJoinJournalError {
    #[error("device join journal transition is not the declared adjacent transition")]
    NonAdjacentJournalTransition,
    #[error("device join journal has a different durable value for this role and attempt")]
    JournalConflict,
    #[error("device join journal: {0}")]
    Journal(String),
}

impl From<serde_json::Error> for DeviceJoinJournalError {
    fn from(error: serde_json::Error) -> Self {
        DeviceJoinJournalError::Journal(error.to_string())
    }
}

use super::*;
use crate::protocol::store_commit::{DeviceJoinAbandonmentRef, DeviceJoinCleanupReceiptRef};

/// Derived from a journal record on demand and never stored, so it carries no
/// wire form of its own.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeviceJoinStatus {
    AwaitingAccessRequest {
        offer: DeviceJoinOffer,
    },
    AwaitingProviderAdmission {
        request: DeviceProviderAccessRequest,
    },
    AwaitingRegistrationRequest {
        approval: DeviceProviderAdmissionApproval,
    },
    AwaitingBootstrap {
        request: DeviceRegistrationRequest,
    },
    AwaitingChallengePublication {
        bootstrap: ProvisionalDeviceBootstrap,
    },
    AwaitingReadiness {
        bootstrap: ProviderReadyDeviceBootstrap,
    },
    AwaitingProviderCompletion {
        readiness: DeviceJoinReadiness,
    },
    AwaitingActivation {
        completion: DeviceProviderAdmissionCompletion,
    },
    AwaitingCompletion {
        activation: DeviceJoinActivation,
    },
    Activated {
        store: JoinedStore,
    },
    Abandoned {
        abandonment: DeviceJoinAbandonment,
    },
    ProviderAccessGrantCreatePending {
        request: DeviceProviderAccessRequest,
        grant: StoreMemberProviderAccessGrant,
    },
    AbandonmentCreatePending {
        abandonment: DeviceJoinAbandonmentRef,
    },
    CancellationCreatePending {
        cancellation: DeviceJoinOutcomeRef,
    },
    ProviderClosurePending {
        cancellation: DeviceJoinCancellation,
        producer: DeviceJoinProducer,
    },
    ProviderClosed {
        terminal: ProviderAdminJoinTerminal,
    },
    JoinerClosed {
        terminal: JoinerJoinTerminal,
    },
    CleanupReceiptCreatePending {
        cancellation: DeviceJoinCancellation,
        receipt: DeviceJoinCleanupReceiptRef,
    },
    Cancelled {
        cancellation: DeviceJoinCancellation,
    },
    CleanupPending {
        cancellation: DeviceJoinCancellation,
    },
    AwaitingCleanupActivation {
        receipt: DeviceJoinCleanupReceipt,
    },
    CleanupActivated {
        activation: DeviceJoinCleanupActivation,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum DeviceJoinAction {
    TransferOffer(DeviceJoinOffer),
    TransferProviderAccessRequest(DeviceProviderAccessRequest),
    TransferProviderAdmissionApproval(DeviceProviderAdmissionApproval),
    TransferRegistrationRequest(DeviceRegistrationRequest),
    TransferProvisionalBootstrap(ProvisionalDeviceBootstrap),
    TransferProviderReadyBootstrap(ProviderReadyDeviceBootstrap),
    TransferReadiness(DeviceJoinReadiness),
    TransferProviderAdmissionCompletion(DeviceProviderAdmissionCompletion),
    TransferActivation(DeviceJoinActivation),
    TransferAbandonment(DeviceJoinAbandonment),
    TransferCancellation(DeviceJoinCancellation),
    TransferProviderAdminTerminal(ProviderAdminJoinTerminal),
    TransferJoinerTerminal(JoinerJoinTerminal),
    TransferCleanupReceipt(DeviceJoinCleanupReceipt),
    TransferCleanupActivation(DeviceJoinCleanupActivation),
    CompleteJoin(DeviceJoinActivation),
    CompleteCleanup(DeviceJoinCleanupActivation),
    ResumeOperation {
        attempt_id: DeviceJoinAttemptId,
        role: DeviceJoinRole,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum OwnerJoinProgress {
    Offered(DeviceJoinOffer),
    RegistrationRequested(DeviceRegistrationRequest),
    AbandonmentCreateIntent {
        offer: DeviceJoinOffer,
        abandonment: DeviceJoinAbandonmentRef,
        prepared: PreparedDeviceJoinObject,
    },
    AttemptActivated(ProvisionalDeviceBootstrap),
    ActivationCreateIntent {
        bootstrap: ProvisionalDeviceBootstrap,
        completion: DeviceProviderAdmissionCompletion,
        outcome: DeviceJoinOutcomeRef,
        prepared: PreparedDeviceJoinObject,
    },
    CancellationCreateIntent {
        attempt: DeviceJoinAttemptRef,
        cancellation: DeviceJoinOutcomeRef,
        prepared: PreparedDeviceJoinObject,
    },
    ActivationPrepared {
        completion: DeviceProviderAdmissionCompletion,
        activation: DeviceJoinActivation,
    },
    Abandoned(DeviceJoinAbandonment),
    Cancelled(DeviceJoinCancellation),
    CleanupReceiptCreateIntent {
        cancellation: DeviceJoinCancellation,
        receipt: DeviceJoinCleanupReceiptRef,
        receipt_bytes: Vec<u8>,
        prepared: PreparedDeviceJoinObject,
    },
    CleanupReceipt(DeviceJoinCleanupReceipt),
    CleanupActivated(DeviceJoinCleanupActivation),
    CancelledComplete(DeviceJoinCleanupActivation),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ProviderAdminJoinProgress {
    AccessRequested(DeviceProviderAccessRequest),
    AccessGrantPrepared {
        request: DeviceProviderAccessRequest,
        grant: StoreMemberProviderAccessGrant,
        prepared: PreparedDeviceJoinObject,
    },
    ApprovalPrepared(DeviceProviderAdmissionApproval),
    AttemptObserved(ProvisionalDeviceBootstrap),
    ChallengeCreateIntent(ProvisionalDeviceBootstrap),
    ProviderReady(ProviderReadyDeviceBootstrap),
    ResponseObserved(DeviceJoinReadiness),
    CleanupIntent {
        cancellation: DeviceJoinCancellation,
        challenge: ProviderChallengeDisposition,
        prior_state_hash: ObjectHash,
    },
    Completed(DeviceProviderAdmissionCompletion),
    Cancelled(ProviderAdminJoinClosure),
    WriteRevoked(DeviceJoinProducerWriteRevocation),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PreparedDeviceJoinObject {
    pub object: ExactObjectRef,
    pub stored_bytes: Vec<u8>,
}

impl PreparedDeviceJoinObject {
    pub(crate) fn from_prepared(prepared: &crate::protocol::objects::PreparedExactObject) -> Self {
        Self {
            object: prepared.reference().clone(),
            stored_bytes: prepared.stored_bytes().to_vec(),
        }
    }

    /// The prepared object this journal entry recorded, ready to be created
    /// again. A resumed write creates these exact bytes rather than preparing a
    /// second object, so the retry writes what the journal already committed to.
    pub(crate) fn restore(
        &self,
    ) -> Result<crate::protocol::objects::PreparedExactObject, crate::protocol::objects::StorageError>
    {
        crate::protocol::objects::PreparedExactObject::new(
            self.object.clone(),
            self.stored_bytes.clone(),
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum JoinerJoinProgress {
    OfferReceived(DeviceJoinOffer),
    AccessRequested(DeviceProviderAccessRequest),
    ApprovalReceived(DeviceProviderAdmissionApproval),
    RegistrationPrepared(DeviceRegistrationRequest),
    Ready(DeviceJoinReadiness),
    ActivationObserved {
        readiness: DeviceJoinReadiness,
        activation: DeviceJoinActivation,
    },
    Activated(JoinedStore),
    Abandoned(DeviceJoinAbandonment),
    CleanupIntent {
        cancellation: DeviceJoinCancellation,
        registration: SlotDisposition,
        initial_ack: SlotDisposition,
        response: JoinerResponseDisposition,
        prior_state_hash: ObjectHash,
    },
    Cancelled(JoinerJoinClosure),
    WriteRevoked(DeviceJoinProducerWriteRevocation),
    CleanupActivated(DeviceJoinCleanupActivation),
    CancelledComplete(DeviceJoinCleanupActivation),
}

impl JoinerJoinProgress {
    /// Whether the joiner holds staged join work a cancellation has to retract:
    /// every state that can enter a cleanup intent, and the cleanup intent
    /// itself. Before these the joiner published nothing to retract; after them
    /// the attempt has reached a terminal the cleanup cannot revisit.
    pub(crate) fn holds_staged_work(&self) -> bool {
        matches!(
            self,
            Self::RegistrationPrepared(_) | Self::Ready(_) | Self::CleanupIntent { .. }
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum DeviceJoinRoleProgress {
    Owner(OwnerJoinProgress),
    ProviderAdministrator(ProviderAdminJoinProgress),
    Joiner(JoinerJoinProgress),
}

/// A role's own progress type. Each one names the role whose journal rows hold
/// it, so a journal bound to a role accepts only that role's progress.
pub(crate) trait DeviceJoinRoleProgressKind: Into<DeviceJoinRoleProgress> {
    const ROLE: DeviceJoinRole;
}

impl From<OwnerJoinProgress> for DeviceJoinRoleProgress {
    fn from(progress: OwnerJoinProgress) -> Self {
        Self::Owner(progress)
    }
}

impl DeviceJoinRoleProgressKind for OwnerJoinProgress {
    const ROLE: DeviceJoinRole = DeviceJoinRole::Owner;
}

impl From<ProviderAdminJoinProgress> for DeviceJoinRoleProgress {
    fn from(progress: ProviderAdminJoinProgress) -> Self {
        Self::ProviderAdministrator(progress)
    }
}

impl DeviceJoinRoleProgressKind for ProviderAdminJoinProgress {
    const ROLE: DeviceJoinRole = DeviceJoinRole::ProviderAdministrator;
}

impl From<JoinerJoinProgress> for DeviceJoinRoleProgress {
    fn from(progress: JoinerJoinProgress) -> Self {
        Self::Joiner(progress)
    }
}

impl DeviceJoinRoleProgressKind for JoinerJoinProgress {
    const ROLE: DeviceJoinRole = DeviceJoinRole::Joiner;
}

impl DeviceJoinRoleProgress {
    pub(crate) fn role(&self) -> DeviceJoinRole {
        match self {
            Self::Owner(_) => DeviceJoinRole::Owner,
            Self::ProviderAdministrator(_) => DeviceJoinRole::ProviderAdministrator,
            Self::Joiner(_) => DeviceJoinRole::Joiner,
        }
    }

    pub(crate) fn role_name(&self) -> &'static str {
        self.role().as_str()
    }

    fn validate_transition(&self, next: &Self) -> Result<(), DeviceJoinJournalError> {
        let adjacent = match (self, next) {
            (Self::Owner(previous), Self::Owner(next)) => owner_adjacent(previous, next),
            (Self::ProviderAdministrator(previous), Self::ProviderAdministrator(next)) => {
                provider_admin_adjacent(previous, next)
            }
            (Self::Joiner(previous), Self::Joiner(next)) => joiner_adjacent(previous, next),
            _ => false,
        };
        if adjacent {
            Ok(())
        } else {
            Err(DeviceJoinJournalError::NonAdjacentJournalTransition)
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceJoinJournalRecord {
    pub attempt_id: DeviceJoinAttemptId,
    pub(crate) progress: Box<DeviceJoinRoleProgress>,
}

impl DeviceJoinJournalRecord {
    pub(crate) fn owner_offered(offer: DeviceJoinOffer) -> Self {
        Self {
            attempt_id: offer.attempt_id,
            progress: Box::new(DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Offered(
                offer,
            ))),
        }
    }

    pub(crate) fn require_initial(&self) -> Result<(), DeviceJoinJournalError> {
        validate_initial_progress(&self.progress)
    }

    pub(crate) fn require_replacement_terminal(&self) -> Result<(), DeviceJoinJournalError> {
        if matches!(
            &*self.progress,
            DeviceJoinRoleProgress::ProviderAdministrator(ProviderAdminJoinProgress::WriteRevoked(
                _
            )) | DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::WriteRevoked(_))
        ) {
            Ok(())
        } else {
            Err(DeviceJoinJournalError::JournalConflict)
        }
    }

    pub(crate) fn store_key(&self) -> String {
        store_journal_key(self.attempt_id, self.progress.role_name())
    }

    pub(crate) fn store_key_for(attempt_id: DeviceJoinAttemptId, role: DeviceJoinRole) -> String {
        store_journal_key(attempt_id, role.as_str())
    }

    pub(crate) fn validate_successor(&self, next: &Self) -> Result<(), DeviceJoinJournalError> {
        if self.attempt_id != next.attempt_id {
            return Err(DeviceJoinJournalError::JournalConflict);
        }
        self.progress.validate_transition(&next.progress)
    }

    pub(crate) fn status(&self) -> DeviceJoinStatus {
        device_join_status(self)
    }

    pub(crate) fn action(&self) -> Option<DeviceJoinAction> {
        device_join_action(self)
    }

    pub(crate) fn sort_key(&self) -> (DeviceJoinAttemptId, DeviceJoinRole) {
        (self.attempt_id, self.progress.role())
    }

    pub(crate) fn attempt_key(&self) -> String {
        attempt_key(self.attempt_id)
    }

    pub(crate) fn joiner_abandonment_transition(
        &self,
        abandonment: &DeviceJoinAbandonment,
    ) -> Result<Option<Self>, DeviceJoinJournalError> {
        if self.attempt_id != abandonment.abandonment.attempt_id {
            return Err(DeviceJoinJournalError::JournalConflict);
        }
        match &*self.progress {
            DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::Abandoned(existing)) => {
                if existing == abandonment {
                    Ok(None)
                } else {
                    Err(DeviceJoinJournalError::JournalConflict)
                }
            }
            DeviceJoinRoleProgress::Joiner(
                JoinerJoinProgress::AccessRequested(_) | JoinerJoinProgress::ApprovalReceived(_),
            ) => Ok(Some(Self {
                attempt_id: self.attempt_id,
                progress: Box::new(DeviceJoinRoleProgress::Joiner(
                    JoinerJoinProgress::Abandoned(abandonment.clone()),
                )),
            })),
            _ => Err(DeviceJoinJournalError::JournalConflict),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum DeviceJoinRole {
    Owner,
    ProviderAdministrator,
    Joiner,
}

impl DeviceJoinRole {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Owner => "owner",
            Self::ProviderAdministrator => "provider_administrator",
            Self::Joiner => "joiner",
        }
    }
}

pub(crate) fn device_join_status(record: &DeviceJoinJournalRecord) -> DeviceJoinStatus {
    match &*record.progress {
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Offered(offer))
        | DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::OfferReceived(offer)) => {
            DeviceJoinStatus::AwaitingAccessRequest {
                offer: offer.clone(),
            }
        }
        DeviceJoinRoleProgress::ProviderAdministrator(
            ProviderAdminJoinProgress::AccessRequested(request),
        )
        | DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::AccessRequested(request)) => {
            DeviceJoinStatus::AwaitingProviderAdmission {
                request: request.clone(),
            }
        }
        DeviceJoinRoleProgress::ProviderAdministrator(
            ProviderAdminJoinProgress::ApprovalPrepared(approval),
        )
        | DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::ApprovalReceived(approval)) => {
            DeviceJoinStatus::AwaitingRegistrationRequest {
                approval: approval.clone(),
            }
        }
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::RegistrationRequested(request))
        | DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::RegistrationPrepared(request)) => {
            DeviceJoinStatus::AwaitingBootstrap {
                request: request.clone(),
            }
        }
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::AttemptActivated(bootstrap))
        | DeviceJoinRoleProgress::ProviderAdministrator(
            ProviderAdminJoinProgress::AttemptObserved(bootstrap),
        )
        | DeviceJoinRoleProgress::ProviderAdministrator(
            ProviderAdminJoinProgress::ChallengeCreateIntent(bootstrap),
        ) => DeviceJoinStatus::AwaitingChallengePublication {
            bootstrap: bootstrap.clone(),
        },
        DeviceJoinRoleProgress::ProviderAdministrator(
            ProviderAdminJoinProgress::ProviderReady(bootstrap),
        ) => DeviceJoinStatus::AwaitingReadiness {
            bootstrap: bootstrap.clone(),
        },
        DeviceJoinRoleProgress::ProviderAdministrator(
            ProviderAdminJoinProgress::ResponseObserved(readiness),
        )
        | DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::Ready(readiness)) => {
            DeviceJoinStatus::AwaitingProviderCompletion {
                readiness: readiness.clone(),
            }
        }
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::ActivationCreateIntent {
            completion,
            ..
        })
        | DeviceJoinRoleProgress::ProviderAdministrator(ProviderAdminJoinProgress::Completed(
            completion,
        )) => DeviceJoinStatus::AwaitingActivation {
            completion: completion.clone(),
        },
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::ActivationPrepared {
            activation, ..
        })
        | DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::ActivationObserved {
            activation,
            ..
        }) => DeviceJoinStatus::AwaitingCompletion {
            activation: activation.clone(),
        },
        DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::Activated(store)) => {
            DeviceJoinStatus::Activated {
                store: store.clone(),
            }
        }
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Abandoned(abandonment))
        | DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::Abandoned(abandonment)) => {
            DeviceJoinStatus::Abandoned {
                abandonment: abandonment.clone(),
            }
        }
        DeviceJoinRoleProgress::ProviderAdministrator(
            ProviderAdminJoinProgress::AccessGrantPrepared { request, grant, .. },
        ) => DeviceJoinStatus::ProviderAccessGrantCreatePending {
            request: request.clone(),
            grant: grant.clone(),
        },
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::AbandonmentCreateIntent {
            abandonment,
            ..
        }) => DeviceJoinStatus::AbandonmentCreatePending {
            abandonment: abandonment.clone(),
        },
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::CancellationCreateIntent {
            cancellation,
            ..
        }) => DeviceJoinStatus::CancellationCreatePending {
            cancellation: cancellation.clone(),
        },
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Cancelled(cancellation)) => {
            DeviceJoinStatus::CleanupPending {
                cancellation: cancellation.clone(),
            }
        }
        DeviceJoinRoleProgress::ProviderAdministrator(
            ProviderAdminJoinProgress::CleanupIntent { cancellation, .. },
        ) => DeviceJoinStatus::ProviderClosurePending {
            cancellation: cancellation.clone(),
            producer: DeviceJoinProducer::ProviderAdministrator,
        },
        DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::CleanupIntent {
            cancellation, ..
        }) => DeviceJoinStatus::ProviderClosurePending {
            cancellation: cancellation.clone(),
            producer: DeviceJoinProducer::Joiner,
        },
        DeviceJoinRoleProgress::ProviderAdministrator(ProviderAdminJoinProgress::Cancelled(
            closure,
        )) => DeviceJoinStatus::ProviderClosed {
            terminal: ProviderAdminJoinTerminal::Cancelled(closure.clone()),
        },
        DeviceJoinRoleProgress::ProviderAdministrator(ProviderAdminJoinProgress::WriteRevoked(
            revocation,
        )) => DeviceJoinStatus::ProviderClosed {
            terminal: ProviderAdminJoinTerminal::WriteRevoked(revocation.clone()),
        },
        DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::Cancelled(closure)) => {
            DeviceJoinStatus::JoinerClosed {
                terminal: JoinerJoinTerminal::Cancelled(closure.clone()),
            }
        }
        DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::WriteRevoked(revocation)) => {
            DeviceJoinStatus::JoinerClosed {
                terminal: JoinerJoinTerminal::WriteRevoked(revocation.clone()),
            }
        }
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::CleanupReceiptCreateIntent {
            cancellation,
            receipt,
            ..
        }) => DeviceJoinStatus::CleanupReceiptCreatePending {
            cancellation: cancellation.clone(),
            receipt: receipt.clone(),
        },
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::CleanupReceipt(receipt)) => {
            DeviceJoinStatus::AwaitingCleanupActivation {
                receipt: receipt.clone(),
            }
        }
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::CleanupActivated(activation))
        | DeviceJoinRoleProgress::Owner(OwnerJoinProgress::CancelledComplete(activation))
        | DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::CleanupActivated(activation))
        | DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::CancelledComplete(activation)) => {
            DeviceJoinStatus::CleanupActivated {
                activation: activation.clone(),
            }
        }
    }
}

pub(crate) fn device_join_action(record: &DeviceJoinJournalRecord) -> Option<DeviceJoinAction> {
    let resume = || DeviceJoinAction::ResumeOperation {
        attempt_id: record.attempt_id,
        role: record.progress.role(),
    };
    match &*record.progress {
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Offered(offer)) => {
            Some(DeviceJoinAction::TransferOffer(offer.clone()))
        }
        DeviceJoinRoleProgress::Owner(
            OwnerJoinProgress::RegistrationRequested(_)
            | OwnerJoinProgress::AbandonmentCreateIntent { .. }
            | OwnerJoinProgress::ActivationCreateIntent { .. }
            | OwnerJoinProgress::CancellationCreateIntent { .. }
            | OwnerJoinProgress::CleanupReceiptCreateIntent { .. },
        ) => Some(resume()),
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::AttemptActivated(bootstrap)) => Some(
            DeviceJoinAction::TransferProvisionalBootstrap(bootstrap.clone()),
        ),
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::ActivationPrepared {
            activation, ..
        }) => Some(DeviceJoinAction::TransferActivation(activation.clone())),
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Abandoned(abandonment)) => {
            Some(DeviceJoinAction::TransferAbandonment(abandonment.clone()))
        }
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Cancelled(cancellation)) => {
            Some(DeviceJoinAction::TransferCancellation(cancellation.clone()))
        }
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::CleanupReceipt(receipt)) => {
            Some(DeviceJoinAction::TransferCleanupReceipt(receipt.clone()))
        }
        DeviceJoinRoleProgress::Owner(
            OwnerJoinProgress::CleanupActivated(activation)
            | OwnerJoinProgress::CancelledComplete(activation),
        ) => Some(DeviceJoinAction::TransferCleanupActivation(
            activation.clone(),
        )),

        DeviceJoinRoleProgress::ProviderAdministrator(
            ProviderAdminJoinProgress::AccessRequested(_)
            | ProviderAdminJoinProgress::AccessGrantPrepared { .. }
            | ProviderAdminJoinProgress::AttemptObserved(_)
            | ProviderAdminJoinProgress::ChallengeCreateIntent(_)
            | ProviderAdminJoinProgress::ResponseObserved(_)
            | ProviderAdminJoinProgress::CleanupIntent { .. },
        ) => Some(resume()),
        DeviceJoinRoleProgress::ProviderAdministrator(
            ProviderAdminJoinProgress::ApprovalPrepared(approval),
        ) => Some(DeviceJoinAction::TransferProviderAdmissionApproval(
            approval.clone(),
        )),
        DeviceJoinRoleProgress::ProviderAdministrator(
            ProviderAdminJoinProgress::ProviderReady(bootstrap),
        ) => Some(DeviceJoinAction::TransferProviderReadyBootstrap(
            bootstrap.clone(),
        )),
        DeviceJoinRoleProgress::ProviderAdministrator(ProviderAdminJoinProgress::Completed(
            completion,
        )) => Some(DeviceJoinAction::TransferProviderAdmissionCompletion(
            completion.clone(),
        )),
        DeviceJoinRoleProgress::ProviderAdministrator(ProviderAdminJoinProgress::Cancelled(
            closure,
        )) => Some(DeviceJoinAction::TransferProviderAdminTerminal(
            ProviderAdminJoinTerminal::Cancelled(closure.clone()),
        )),
        DeviceJoinRoleProgress::ProviderAdministrator(ProviderAdminJoinProgress::WriteRevoked(
            revocation,
        )) => Some(DeviceJoinAction::TransferProviderAdminTerminal(
            ProviderAdminJoinTerminal::WriteRevoked(revocation.clone()),
        )),

        DeviceJoinRoleProgress::Joiner(
            JoinerJoinProgress::OfferReceived(_)
            | JoinerJoinProgress::ApprovalReceived(_)
            | JoinerJoinProgress::CleanupIntent { .. },
        ) => Some(resume()),
        DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::AccessRequested(request)) => Some(
            DeviceJoinAction::TransferProviderAccessRequest(request.clone()),
        ),
        DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::RegistrationPrepared(request)) => Some(
            DeviceJoinAction::TransferRegistrationRequest(request.clone()),
        ),
        DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::Ready(readiness)) => {
            Some(DeviceJoinAction::TransferReadiness(readiness.clone()))
        }
        DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::ActivationObserved {
            activation,
            ..
        }) => Some(DeviceJoinAction::CompleteJoin(activation.clone())),
        DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::Cancelled(closure)) => {
            Some(DeviceJoinAction::TransferJoinerTerminal(
                JoinerJoinTerminal::Cancelled(closure.clone()),
            ))
        }
        DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::WriteRevoked(revocation)) => {
            Some(DeviceJoinAction::TransferJoinerTerminal(
                JoinerJoinTerminal::WriteRevoked(revocation.clone()),
            ))
        }
        DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::CleanupActivated(activation)) => {
            Some(DeviceJoinAction::CompleteCleanup(activation.clone()))
        }
        DeviceJoinRoleProgress::Joiner(
            JoinerJoinProgress::Activated(_)
            | JoinerJoinProgress::Abandoned(_)
            | JoinerJoinProgress::CancelledComplete(_),
        ) => None,
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

pub(crate) fn validate_initial_progress(
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

pub(crate) fn store_journal_key(attempt_id: DeviceJoinAttemptId, role: &str) -> String {
    format!("device_join/{}/{role}", attempt_key(attempt_id))
}

pub(crate) fn attempt_key(attempt_id: DeviceJoinAttemptId) -> String {
    serde_json::to_value(attempt_id)
        .expect("device join attempt id serialization cannot fail")
        .as_str()
        .expect("device join attempt id serializes as a string")
        .to_string()
}

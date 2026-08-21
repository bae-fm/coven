//! The durable device-join journal model: role progress, and the status and
//! action each recorded step derives.

use serde::{Deserialize, Serialize};

use crate::objects::ExactObjectRef;
use crate::provider::StoreMemberProviderAccessGrant;
use crate::store_commit::device_join_exchange::{
    DeviceJoinAbandonment, DeviceJoinActivation, DeviceJoinOffer, DeviceJoinReadiness,
    DeviceProviderAccessRequest, DeviceProviderAdmissionApproval,
    DeviceProviderAdmissionCompletion, DeviceRegistrationRequest, JoinedStore,
    ProviderReadyDeviceBootstrap, ProvisionalDeviceBootstrap, SamePrincipalDeviceJoin,
};

use super::*;
use crate::store_commit::DeviceJoinAbandonmentRef;

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
    SamePrincipalActivationCreatePending {
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
    SamePrincipalCompleted {
        join: SamePrincipalDeviceJoin,
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
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum DeviceJoinAction {
    TransferOffer(DeviceJoinOffer),
    TransferProviderAccessRequest(DeviceProviderAccessRequest),
    TransferProviderAdmissionApproval(DeviceProviderAdmissionApproval),
    TransferRegistrationRequest(DeviceRegistrationRequest),
    TransferProviderReadyBootstrap(ProviderReadyDeviceBootstrap),
    TransferReadiness(DeviceJoinReadiness),
    TransferSamePrincipalJoin(SamePrincipalDeviceJoin),
    TransferActivation(DeviceJoinActivation),
    TransferAbandonment(DeviceJoinAbandonment),
    CompleteJoin(DeviceJoinActivation),
    ResumeOperation {
        attempt_id: DeviceJoinAttemptId,
        role: DeviceJoinRole,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum OwnerJoinProgress {
    Offered(DeviceJoinOffer),
    /// The joining device asked for provider access. The admitting device holds
    /// the store's provider-administrator grant, so answering this request, the
    /// grant it prepares, and the approval it signs are all its own steps.
    AccessRequested(DeviceProviderAccessRequest),
    AccessGrantPrepared {
        request: DeviceProviderAccessRequest,
        grant: StoreMemberProviderAccessGrant,
        prepared: PreparedDeviceJoinObject,
    },
    ApprovalPrepared(DeviceProviderAdmissionApproval),
    RegistrationRequested(DeviceRegistrationRequest),
    AbandonmentCreateIntent {
        offer: DeviceJoinOffer,
        abandonment: DeviceJoinAbandonmentRef,
        prepared: PreparedDeviceJoinObject,
    },
    AttemptActivated(ProvisionalDeviceBootstrap),
    ChallengeCreateIntent(ProvisionalDeviceBootstrap),
    ProviderReady(ProviderReadyDeviceBootstrap),
    ResponseObserved(DeviceJoinReadiness),
    Completed(DeviceProviderAdmissionCompletion),
    SamePrincipalActivationCreateIntent {
        request: DeviceRegistrationRequest,
        bootstrap_cut: StoreHistoryCut,
        membership: StoreMembershipStateRef,
        registration: StoreDeviceRegistrationRef,
        registration_prepared: PreparedDeviceJoinObject,
    },
    ActivationCreateIntent {
        completion: DeviceProviderAdmissionCompletion,
    },
    /// The owner published the activation commit and has nothing left to do
    /// but hand the artifact over.
    ///
    /// `registration` is the joined device's, carried so the owner can tell
    /// when that device has actually arrived: its announcement stream id is a
    /// pure function of this reference, and a stream that appears in the
    /// materialized frontier is the device's own first commit — the one thing
    /// it publishes that the owner did not write for it.
    ActivationPrepared {
        completion: DeviceProviderAdmissionCompletion,
        activation: DeviceJoinActivation,
        registration: StoreDeviceRegistrationRef,
    },
    /// The same-principal join completed, carried closure and all.
    ///
    /// `registration` is the joined device's, for the same reason
    /// [`ActivationPrepared`](Self::ActivationPrepared) carries one: this row is
    /// the largest a join writes — a snapshot's metadata and the bootstrap
    /// closure live inside `join` — and the owner needs to be able to tell when
    /// the device it activated has arrived so the row can go.
    SamePrincipalCompleted {
        join: SamePrincipalDeviceJoin,
        registration: StoreDeviceRegistrationRef,
    },
    Abandoned(DeviceJoinAbandonment),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedDeviceJoinObject {
    pub object: ExactObjectRef,
    pub stored_bytes: Vec<u8>,
}

impl PreparedDeviceJoinObject {
    pub fn from_prepared(prepared: &crate::objects::PreparedExactObject) -> Self {
        Self {
            object: prepared.reference().clone(),
            stored_bytes: prepared.stored_bytes().to_vec(),
        }
    }

    /// The prepared object this journal entry recorded, ready to be created
    /// again. A resumed write creates these exact bytes rather than preparing a
    /// second object, so the retry writes what the journal already committed to.
    pub fn restore(
        &self,
    ) -> Result<crate::objects::PreparedExactObject, crate::objects::StorageError> {
        crate::objects::PreparedExactObject::new(self.object.clone(), self.stored_bytes.clone())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum JoinerJoinProgress {
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
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum DeviceJoinRoleProgress {
    Owner(OwnerJoinProgress),
    Joiner(JoinerJoinProgress),
}

/// A role's own progress type. Each one names the role whose journal rows hold
/// it, so a journal bound to a role accepts only that role's progress.
pub trait DeviceJoinRoleProgressKind: Into<DeviceJoinRoleProgress> {
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

impl From<JoinerJoinProgress> for DeviceJoinRoleProgress {
    fn from(progress: JoinerJoinProgress) -> Self {
        Self::Joiner(progress)
    }
}

impl DeviceJoinRoleProgressKind for JoinerJoinProgress {
    const ROLE: DeviceJoinRole = DeviceJoinRole::Joiner;
}

impl DeviceJoinRoleProgress {
    pub fn role(&self) -> DeviceJoinRole {
        match self {
            Self::Owner(_) => DeviceJoinRole::Owner,
            Self::Joiner(_) => DeviceJoinRole::Joiner,
        }
    }

    pub fn role_name(&self) -> &'static str {
        self.role().as_str()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceJoinJournalRecord {
    pub attempt_id: DeviceJoinAttemptId,
    pub progress: Box<DeviceJoinRoleProgress>,
}

impl DeviceJoinJournalRecord {
    pub fn owner_offered(offer: DeviceJoinOffer) -> Self {
        Self {
            attempt_id: offer.attempt_id,
            progress: Box::new(DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Offered(
                offer,
            ))),
        }
    }

    pub fn store_key(&self) -> String {
        store_journal_key(self.attempt_id, self.progress.role_name())
    }

    pub fn store_key_for(attempt_id: DeviceJoinAttemptId, role: DeviceJoinRole) -> String {
        store_journal_key(attempt_id, role.as_str())
    }

    pub fn status(&self) -> DeviceJoinStatus {
        device_join_status(self)
    }

    pub fn action(&self) -> Option<DeviceJoinAction> {
        device_join_action(self)
    }

    pub fn sort_key(&self) -> (DeviceJoinAttemptId, DeviceJoinRole) {
        (self.attempt_id, self.progress.role())
    }

    pub fn attempt_key(&self) -> String {
        attempt_key(self.attempt_id)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
/// The two sides of a join. One device admits — it answers the access
/// request, prepares the storage grant, signs the approval, registers the
/// device and activates it — and the other is the device being admitted.
pub enum DeviceJoinRole {
    Owner,
    Joiner,
}

impl DeviceJoinRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Owner => "owner",
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
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::AccessRequested(request))
        | DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::AccessRequested(request)) => {
            DeviceJoinStatus::AwaitingProviderAdmission {
                request: request.clone(),
            }
        }
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::AccessGrantPrepared {
            request,
            grant,
            ..
        }) => DeviceJoinStatus::ProviderAccessGrantCreatePending {
            request: request.clone(),
            grant: grant.clone(),
        },
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::ApprovalPrepared(approval))
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
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::SamePrincipalActivationCreateIntent {
            request,
            ..
        }) => DeviceJoinStatus::SamePrincipalActivationCreatePending {
            request: request.clone(),
        },
        DeviceJoinRoleProgress::Owner(
            OwnerJoinProgress::AttemptActivated(bootstrap)
            | OwnerJoinProgress::ChallengeCreateIntent(bootstrap),
        ) => DeviceJoinStatus::AwaitingChallengePublication {
            bootstrap: bootstrap.clone(),
        },
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::ProviderReady(bootstrap)) => {
            DeviceJoinStatus::AwaitingReadiness {
                bootstrap: bootstrap.clone(),
            }
        }
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::ResponseObserved(readiness))
        | DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::Ready(readiness)) => {
            DeviceJoinStatus::AwaitingProviderCompletion {
                readiness: readiness.clone(),
            }
        }
        DeviceJoinRoleProgress::Owner(
            OwnerJoinProgress::ActivationCreateIntent { completion, .. }
            | OwnerJoinProgress::Completed(completion),
        ) => DeviceJoinStatus::AwaitingActivation {
            completion: completion.clone(),
        },
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::SamePrincipalCompleted {
            join, ..
        }) => DeviceJoinStatus::SamePrincipalCompleted { join: join.clone() },
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
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::AbandonmentCreateIntent {
            abandonment,
            ..
        }) => DeviceJoinStatus::AbandonmentCreatePending {
            abandonment: abandonment.clone(),
        },
    }
}

pub fn device_join_action(record: &DeviceJoinJournalRecord) -> Option<DeviceJoinAction> {
    let resume = || DeviceJoinAction::ResumeOperation {
        attempt_id: record.attempt_id,
        role: record.progress.role(),
    };
    match &*record.progress {
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Offered(offer)) => {
            Some(DeviceJoinAction::TransferOffer(offer.clone()))
        }
        DeviceJoinRoleProgress::Owner(
            OwnerJoinProgress::AccessRequested(_)
            | OwnerJoinProgress::AccessGrantPrepared { .. }
            | OwnerJoinProgress::RegistrationRequested(_)
            | OwnerJoinProgress::AttemptActivated(_)
            | OwnerJoinProgress::ChallengeCreateIntent(_)
            | OwnerJoinProgress::ResponseObserved(_)
            | OwnerJoinProgress::Completed(_)
            | OwnerJoinProgress::SamePrincipalActivationCreateIntent { .. }
            | OwnerJoinProgress::AbandonmentCreateIntent { .. }
            | OwnerJoinProgress::ActivationCreateIntent { .. },
        ) => Some(resume()),
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::ApprovalPrepared(approval)) => Some(
            DeviceJoinAction::TransferProviderAdmissionApproval(approval.clone()),
        ),
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::ProviderReady(bootstrap)) => Some(
            DeviceJoinAction::TransferProviderReadyBootstrap(bootstrap.clone()),
        ),
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::SamePrincipalCompleted {
            join, ..
        }) => Some(DeviceJoinAction::TransferSamePrincipalJoin(join.clone())),
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::ActivationPrepared {
            activation, ..
        }) => Some(DeviceJoinAction::TransferActivation(activation.clone())),
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Abandoned(abandonment)) => {
            Some(DeviceJoinAction::TransferAbandonment(abandonment.clone()))
        }

        DeviceJoinRoleProgress::Joiner(
            JoinerJoinProgress::OfferReceived(_) | JoinerJoinProgress::ApprovalReceived(_),
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
        DeviceJoinRoleProgress::Joiner(
            JoinerJoinProgress::Activated(_) | JoinerJoinProgress::Abandoned(_),
        ) => None,
    }
}

pub(crate) fn store_journal_key(attempt_id: DeviceJoinAttemptId, role: &str) -> String {
    format!("device_join/{}/{role}", attempt_key(attempt_id))
}

pub fn attempt_key(attempt_id: DeviceJoinAttemptId) -> String {
    serde_json::to_value(attempt_id)
        .expect("device join attempt id serialization cannot fail")
        .as_str()
        .expect("device join attempt id serializes as a string")
        .to_string()
}

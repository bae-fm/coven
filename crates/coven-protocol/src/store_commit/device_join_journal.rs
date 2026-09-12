//! The durable device-join journal model: role progress, and the status and
//! action each recorded step derives.

use serde::{Deserialize, Serialize};

use crate::provider::DeviceJoinChallengePublicationAuthorization;
use crate::provider::StoreMemberProviderAccessGrant;
use crate::store_commit::device_join_exchange::{
    DeviceJoinAbandonment, DeviceJoinAbandonmentObject, DeviceJoinActivation, DeviceJoinOffer,
    DeviceJoinReadiness, DeviceProviderAccessRequest, DeviceProviderAdmissionApproval,
    DeviceProviderAdmissionCompletion, DeviceRegistrationRequest, ProviderReadyDeviceBootstrap,
    ProvisionalDeviceBootstrap, SamePrincipalDeviceJoin,
};

use super::*;

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
    SamePrincipalActivationPublished {
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
    Abandoned {
        abandonment: DeviceJoinAbandonment,
    },
    ProviderAccessGrantPublished {
        request: DeviceProviderAccessRequest,
        grant: StoreMemberProviderAccessGrant,
    },
    StorePublicationPending {
        operation: OwnerJoinPublication,
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
    StorePublicationPrepared(PreparedOwnerJoinPublication),
    AccessGrantActivated {
        request: DeviceProviderAccessRequest,
        grant: StoreMemberProviderAccessGrant,
        grant_ref: crate::provider::StoreMemberProviderAccessGrantRef,
        activation: StoreBatchCommitRef,
    },
    ApprovalPrepared(DeviceProviderAdmissionApproval),
    RegistrationRequested(DeviceRegistrationRequest),
    AttemptActivated(ProvisionalDeviceBootstrap),
    ChallengeCreateIntent(ProvisionalDeviceBootstrap),
    ProviderReady(ProviderReadyDeviceBootstrap),
    ResponseObserved(DeviceJoinReadiness),
    Completed(DeviceProviderAdmissionCompletion),
    SamePrincipalActivated {
        request: DeviceRegistrationRequest,
        registration: StoreDeviceRegistrationRef,
        activation: StoreBatchCommitRef,
        accepted_current: StoreCurrentPublicationRecord,
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
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum OwnerJoinPublication {
    ProviderAccessGrant {
        request: DeviceProviderAccessRequest,
        grant: StoreMemberProviderAccessGrant,
    },
    Attempt {
        request: DeviceRegistrationRequest,
    },
    Abandonment {
        offer: DeviceJoinOffer,
        abandonment: DeviceJoinAbandonmentObject,
    },
    SamePrincipalActivation {
        request: DeviceRegistrationRequest,
    },
    JoinActivation {
        completion: DeviceProviderAdmissionCompletion,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedOwnerJoinPublication {
    pub operation: OwnerJoinPublication,
    pub candidate: Box<crate::prepared_commit::PreparedStoreOperationCommit>,
}

impl PreparedOwnerJoinPublication {
    pub fn validate_for(
        &self,
        attempt_id: DeviceJoinAttemptId,
    ) -> Result<(), crate::prepared_commit::PreparedCommitError> {
        use crate::prepared_commit::PreparedCommitError;

        self.candidate.validate_closed_shape()?;
        let operations = self.candidate.commit.operations().ok_or_else(|| {
            PreparedCommitError::Invariant(
                "device join publication candidate contains another Store body".to_string(),
            )
        })?;
        let has_unrelated_operation = operations.acknowledgement.is_some()
            || !operations.circle_acknowledgements.is_empty()
            || !operations.device_exclusion_outcomes.is_empty()
            || !operations.stream_activations.is_empty()
            || !operations.circle_controls.is_empty()
            || operations.store_package.is_some()
            || !operations.circle_packages.is_empty();
        if has_unrelated_operation {
            return Err(PreparedCommitError::Invariant(
                "device join publication candidate contains an unrelated Store operation"
                    .to_string(),
            ));
        }
        match &self.operation {
            OwnerJoinPublication::SamePrincipalActivation { .. }
            | OwnerJoinPublication::JoinActivation { .. } => {
                let publication = self.candidate.prepared_membership_publication()?;
                if !matches!(&publication.entry.change,
                    crate::membership::StoreAuthorityChange::DeviceRegistrationActivation { registration }
                        if operations.device_registrations.as_slice() == std::slice::from_ref(registration))
                {
                    return Err(PreparedCommitError::Invariant(
                        "join authority differs from its exact registration activation".into(),
                    ));
                }
            }
            _ if operations.control.is_some() => {
                return Err(PreparedCommitError::Invariant(
                    "device join operation has unrelated membership authority".into(),
                ))
            }
            _ => {}
        }
        let registration = self.candidate.registration_activation.as_ref();
        let exact = match &self.operation {
            OwnerJoinPublication::ProviderAccessGrant { request, grant } => {
                request.offer.attempt_id == attempt_id
                    && operations.device_join_attempt_decisions.is_empty()
                    && operations.device_registrations.is_empty()
                    && registration.is_none()
                    && matches!(operations.provider_access_grants.as_slice(), [reference]
                        if reference.verify(grant).is_ok()
                            && reference.object.verify(&grant.to_bytes()).is_ok())
            }
            OwnerJoinPublication::Attempt { request } => {
                request.approval().request.offer.attempt_id == attempt_id
                    && operations.provider_access_grants.is_empty()
                    && operations.device_registrations.is_empty()
                    && registration.is_none()
                    && operations.device_join_attempt_decisions
                        == [DeviceJoinAttemptDecisionRef::Attempt(attempt_id)]
            }
            OwnerJoinPublication::Abandonment { offer, abandonment } => {
                offer.attempt_id == attempt_id
                    && abandonment.attempt_id == attempt_id
                    && abandonment.owner_registration == offer.owner_registration
                    && operations.provider_access_grants.is_empty()
                    && operations.device_registrations.is_empty()
                    && registration.is_none()
                    && matches!(operations.device_join_attempt_decisions.as_slice(),
                        [DeviceJoinAttemptDecisionRef::Abandoned(reference)]
                            if reference.attempt_id == attempt_id
                                && reference.abandonment_hash == abandonment.abandonment_hash()
                                && reference.object.verify(&abandonment.to_bytes()).is_ok())
            }
            OwnerJoinPublication::SamePrincipalActivation { request } => {
                let expected = request.expected_registration();
                request.approval().request.offer.attempt_id == attempt_id
                    && operations.provider_access_grants.is_empty()
                    && operations.device_join_attempt_decisions
                        == [DeviceJoinAttemptDecisionRef::Attempt(attempt_id)]
                    && matches!((operations.device_registrations.as_slice(), registration),
                        ([reference], Some(activated))
                            if activated.verify_reference(reference).is_ok()
                                && activated.value() == expected
                                && activated.reference().object.verify(&expected.to_bytes()).is_ok()
                                && matches!(activated.activation(), StoreDeviceRegistrationActivation::Join { attempt_id: activated_attempt } if *activated_attempt == attempt_id))
            }
            OwnerJoinPublication::JoinActivation { completion } => {
                let request = &completion.bootstrap().bootstrap.request;
                let expected = request.expected_registration();
                completion.attempt_id() == attempt_id
                    && operations.provider_access_grants.is_empty()
                    && operations.device_join_attempt_decisions.is_empty()
                    && matches!((operations.device_registrations.as_slice(), registration),
                        ([reference], Some(activated))
                            if activated.verify_reference(reference).is_ok()
                                && activated.value() == expected
                                && activated.reference().object.verify(&expected.to_bytes()).is_ok()
                                && matches!(activated.activation(), StoreDeviceRegistrationActivation::Join { attempt_id: activated_attempt } if *activated_attempt == attempt_id))
            }
        };
        if !exact {
            return Err(PreparedCommitError::Invariant(
                "device join publication differs from its exact journal operation".to_string(),
            ));
        }
        Ok(())
    }

    pub fn remote_objects(
        &self,
        attempt_id: DeviceJoinAttemptId,
    ) -> Result<
        Vec<crate::remote_object::ClosedRemoteObject>,
        crate::prepared_commit::PreparedCommitError,
    > {
        self.validate_for(attempt_id)?;
        let authority = self.authority_remote_object(attempt_id)?;
        match authority {
            Some(authority) => match &self.operation {
                OwnerJoinPublication::SamePrincipalActivation { .. }
                | OwnerJoinPublication::JoinActivation { .. } => self
                    .candidate
                    .retained_control_remote_objects(vec![authority]),
                _ => self
                    .candidate
                    .retained_authority_remote_objects(vec![authority]),
            },
            None => Ok(vec![self.candidate.candidate_remote_object()?]),
        }
    }

    pub fn authority_remote_object(
        &self,
        attempt_id: DeviceJoinAttemptId,
    ) -> Result<
        Option<crate::remote_object::ClosedRemoteObject>,
        crate::prepared_commit::PreparedCommitError,
    > {
        self.validate_for(attempt_id)?;
        let candidate = &self.candidate;
        let activation = candidate.reference.clone();
        Ok(match &self.operation {
            OwnerJoinPublication::ProviderAccessGrant { grant, .. } => {
                let reference = candidate.commit.provider_access_grants()[0].clone();
                Some(crate::remote_object::RemoteObjectRecord::candidate_activated_provider_access_grant(
                    reference,
                    &grant.to_bytes(),
                    &grant.to_bytes(),
                    activation,
                )?)
            }
            OwnerJoinPublication::Abandonment { abandonment, .. } => {
                let DeviceJoinAttemptDecisionRef::Abandoned(reference) =
                    &candidate.commit.device_join_attempt_decisions()[0]
                else {
                    unreachable!("validated abandonment publication has an abandonment reference")
                };
                Some(crate::remote_object::RemoteObjectRecord::candidate_activated_device_join_abandonment(
                    reference.clone(),
                    &abandonment.to_bytes(),
                    &abandonment.to_bytes(),
                    activation,
                )?)
            }
            OwnerJoinPublication::SamePrincipalActivation { request } => {
                let activated = candidate
                    .registration_activation
                    .as_ref()
                    .expect("validated device registration publication has an activation");
                Some(crate::remote_object::RemoteObjectRecord::candidate_activated_device_registration(
                    activated.reference().clone(),
                    &request.expected_registration().to_bytes(),
                    &request.expected_registration().to_bytes(),
                    activation,
                )?)
            }
            OwnerJoinPublication::JoinActivation { completion } => {
                let activated = candidate
                    .registration_activation
                    .as_ref()
                    .expect("validated device registration publication has an activation");
                let registration = completion
                    .bootstrap()
                    .bootstrap
                    .request
                    .expected_registration();
                Some(crate::remote_object::RemoteObjectRecord::candidate_activated_device_registration(
                    activated.reference().clone(),
                    &registration.to_bytes(),
                    &registration.to_bytes(),
                    activation,
                )?)
            }
            OwnerJoinPublication::Attempt { .. } => None,
        })
    }

    pub fn accepted_progress(
        &self,
        attempt_id: DeviceJoinAttemptId,
        accepted_current: StoreCurrentPublicationRecord,
    ) -> Result<OwnerJoinProgress, crate::prepared_commit::PreparedCommitError> {
        self.validate_for(attempt_id)?;
        let activation = self.candidate.reference.clone();
        Ok(match &self.operation {
            OwnerJoinPublication::ProviderAccessGrant { request, grant } => {
                OwnerJoinProgress::AccessGrantActivated {
                    request: request.clone(),
                    grant: grant.clone(),
                    grant_ref: self.candidate.commit.provider_access_grants()[0].clone(),
                    activation,
                }
            }
            OwnerJoinPublication::Attempt { request } => {
                OwnerJoinProgress::AttemptActivated(ProvisionalDeviceBootstrap {
                    request: Box::new(request.clone()),
                    publication_authorization: DeviceJoinChallengePublicationAuthorization {
                        attempt_id,
                        attempt_activation: activation,
                    },
                })
            }
            OwnerJoinPublication::Abandonment { .. } => {
                let DeviceJoinAttemptDecisionRef::Abandoned(reference) =
                    &self.candidate.commit.device_join_attempt_decisions()[0]
                else {
                    unreachable!("validated abandonment publication has an abandonment reference")
                };
                OwnerJoinProgress::Abandoned(DeviceJoinAbandonment {
                    abandonment: reference.clone(),
                    abandonment_activation: activation,
                })
            }
            OwnerJoinPublication::SamePrincipalActivation { request } => {
                OwnerJoinProgress::SamePrincipalActivated {
                    request: request.clone(),
                    registration: self
                        .candidate
                        .registration_activation
                        .as_ref()
                        .expect("validated registration publication has an activation")
                        .reference()
                        .clone(),
                    activation,
                    accepted_current,
                }
            }
            OwnerJoinPublication::JoinActivation { completion } => {
                OwnerJoinProgress::ActivationPrepared {
                    completion: completion.clone(),
                    activation: DeviceJoinActivation {
                        attempt_id,
                        outcome_activation: activation,
                    },
                    registration: self
                        .candidate
                        .registration_activation
                        .as_ref()
                        .expect("validated registration publication has an activation")
                        .reference()
                        .clone(),
                }
            }
        })
    }

    pub fn validates_accepted_progress(
        &self,
        attempt_id: DeviceJoinAttemptId,
        progress: &OwnerJoinProgress,
    ) -> bool {
        if self.validate_for(attempt_id).is_err() {
            return false;
        }
        let candidate = &self.candidate;
        let activated_registration = candidate
            .registration_activation
            .as_ref()
            .map(ActivatedStoreDeviceRegistration::reference);
        match (&self.operation, progress) {
            (
                OwnerJoinPublication::ProviderAccessGrant { request, grant },
                OwnerJoinProgress::AccessGrantActivated {
                    request: accepted_request,
                    grant: accepted_grant,
                    grant_ref,
                    activation,
                },
            ) => {
                request == accepted_request
                    && grant == accepted_grant
                    && candidate.commit.provider_access_grants() == std::slice::from_ref(grant_ref)
                    && activation == &candidate.reference
            }
            (
                OwnerJoinPublication::Attempt { request },
                OwnerJoinProgress::AttemptActivated(bootstrap),
            ) => {
                bootstrap.request.as_ref() == request
                    && bootstrap.publication_authorization.attempt_id == attempt_id
                    && bootstrap.publication_authorization.attempt_activation == candidate.reference
            }
            (
                OwnerJoinPublication::Abandonment { .. },
                OwnerJoinProgress::Abandoned(abandonment),
            ) => {
                candidate.commit.device_join_attempt_decisions()
                    == [DeviceJoinAttemptDecisionRef::Abandoned(
                        abandonment.abandonment.clone(),
                    )]
                    && abandonment.abandonment_activation == candidate.reference
            }
            (
                OwnerJoinPublication::SamePrincipalActivation { request },
                OwnerJoinProgress::SamePrincipalActivated {
                    request: accepted_request,
                    registration,
                    activation,
                    accepted_current,
                },
            ) => {
                request == accepted_request
                    && Some(registration) == activated_registration
                    && activation == &candidate.reference
                    && accepted_current.store_root_hash == candidate.commit.store_root_hash
                    && accepted_current.accepted().is_some()
            }
            (
                OwnerJoinPublication::JoinActivation { completion },
                OwnerJoinProgress::ActivationPrepared {
                    completion: accepted_completion,
                    activation,
                    registration,
                },
            ) => {
                completion == accepted_completion
                    && Some(registration) == activated_registration
                    && activation.attempt_id == attempt_id
                    && activation.outcome_activation == candidate.reference
            }
            _ => false,
        }
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
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::StorePublicationPrepared(prepared)) => {
            DeviceJoinStatus::StorePublicationPending {
                operation: prepared.operation.clone(),
            }
        }
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::AccessGrantActivated {
            request,
            grant,
            ..
        }) => DeviceJoinStatus::ProviderAccessGrantPublished {
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
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::SamePrincipalActivated {
            request,
            ..
        }) => DeviceJoinStatus::SamePrincipalActivationPublished {
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
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Completed(completion)) => {
            DeviceJoinStatus::AwaitingActivation {
                completion: completion.clone(),
            }
        }
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
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Abandoned(abandonment)) => {
            DeviceJoinStatus::Abandoned {
                abandonment: abandonment.clone(),
            }
        }
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
            | OwnerJoinProgress::StorePublicationPrepared(_)
            | OwnerJoinProgress::AccessGrantActivated { .. }
            | OwnerJoinProgress::RegistrationRequested(_)
            | OwnerJoinProgress::AttemptActivated(_)
            | OwnerJoinProgress::ChallengeCreateIntent(_)
            | OwnerJoinProgress::ResponseObserved(_)
            | OwnerJoinProgress::Completed(_)
            | OwnerJoinProgress::SamePrincipalActivated { .. },
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

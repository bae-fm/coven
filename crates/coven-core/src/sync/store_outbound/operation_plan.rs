use super::*;
use crate::sync::store_engine::serial::publication::{
    current_serial_authorization_snapshot, SerialAuthorizationSnapshot,
};

pub(crate) enum StoreOperationBatch {
    Control(StoreControl),
    Acknowledgement {
        reference: super::store_commit::StoreAckRef,
        value: super::store_commit::StoreAck,
    },
    ProviderAccessGrant(super::provider::StoreMemberProviderAccessGrantRef),
    Attempt(DeviceJoinAttemptRef),
    Abandonment(super::device_join::DeviceJoinAbandonmentRef),
    Outcome {
        outcome: DeviceJoinOutcomeRef,
        registration: Option<Box<DeviceJoinRegistrationActivation>>,
    },
    CleanupReceipt(super::device_join::DeviceJoinCleanupReceiptRef),
    DeviceExclusionProposal(super::store_commit::RetainedStoreDeviceExclusionProposal),
    DeviceExclusionOutcome(super::store_commit::RetainedStoreDeviceExclusionOutcome),
    ReclaimAuthorization(Box<super::store_reclaim::ReclaimAuthorizationRef>),
    ReclaimReceipt(Box<super::store_reclaim::ReclaimReceiptRef>),
    OwnerPromotionRequest(super::store_commit::OwnerPromotionRequest),
    MergeMembershipActivation {
        transition: super::membership::MergeMembershipHeadTransition,
        stream_activations: Vec<super::store_commit::StreamActivation>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DeviceJoinRegistrationActivation {
    pub reference: ActivatedStoreDeviceRegistrationRef,
    pub registration: StoreDeviceRegistration,
    pub authority: super::store_commit::StoreDeviceRegistrationActivation,
}

pub(crate) enum StoreOperationCommitPlan {
    MergeConcurrent(MergeStoreOperationCommitPlan),
    Serial(SerialStoreOperationCommitPlan),
}

pub(crate) struct StoreOperationPlanCommon {
    pub(super) root: StoreRootRef,
    pub(super) registration_ref: StoreDeviceRegistrationRef,
    pub(super) registration: Box<StoreDeviceRegistration>,
    pub(super) device_signer: UserKeypair,
    pub(super) coord: StoreCommitCoord,
    pub(super) order: StoreCommitOrder,
    pub(super) membership_state: super::circle_control::StoreMembershipStateRef,
    pub(super) device_state: super::store_commit::StoreDeviceStateRef,
    pub(super) membership_authority: StoreOperationMembershipAuthority,
    pub(super) owner_grant: Option<super::membership::MembershipGrantId>,
}

pub(crate) struct MergeStoreOperationCommitPlan {
    pub(super) common: StoreOperationPlanCommon,
    pub(super) membership: MembershipChain,
    pub(super) predecessor_state: super::store_commit::ResolvedStoreDeviceState,
}

pub(crate) struct SerialStoreOperationCommitPlan {
    pub(super) common: StoreOperationPlanCommon,
    pub(super) base_head: VersionedObject,
    pub(super) authorization: SerialAuthorizationState,
}

#[derive(Clone, Copy)]
pub(crate) enum StoreOperationPreparation<'a> {
    MergeConcurrent {
        membership: &'a MembershipChain,
    },
    Serial {
        coordination: &'a dyn CoordinationStorage,
    },
}

impl<'a> StoreOperationPreparation<'a> {
    pub(crate) fn from_dependencies(
        policy: crate::WritePolicy,
        coordination: Option<&'a dyn CoordinationStorage>,
        membership: Option<&'a MembershipChain>,
    ) -> Result<Self, StoreOutboundError> {
        match (policy, coordination, membership) {
            (crate::WritePolicy::MergeConcurrent, None, Some(membership)) => {
                Ok(StoreOperationPreparation::MergeConcurrent { membership })
            }
            (crate::WritePolicy::Serial, Some(coordination), None) => {
                Ok(StoreOperationPreparation::Serial { coordination })
            }
            (crate::WritePolicy::MergeConcurrent, _, None) => {
                Err(StoreOutboundError::InvalidOutbound(
                    "Merge Store operation has no exact membership state".to_string(),
                ))
            }
            (crate::WritePolicy::MergeConcurrent, Some(_), Some(_)) => {
                Err(StoreOutboundError::InvalidOutbound(
                    "Merge Store operation received Serial coordination".to_string(),
                ))
            }
            (crate::WritePolicy::Serial, None, _) => {
                Err(StoreOutboundError::MissingSerialCoordination)
            }
            (crate::WritePolicy::Serial, Some(_), Some(_)) => {
                Err(StoreOutboundError::InvalidOutbound(
                    "Serial Store operation received Merge membership".to_string(),
                ))
            }
        }
    }

    fn policy(self) -> crate::WritePolicy {
        match self {
            Self::MergeConcurrent { .. } => crate::WritePolicy::MergeConcurrent,
            Self::Serial { .. } => crate::WritePolicy::Serial,
        }
    }
}

impl std::ops::Deref for MergeStoreOperationCommitPlan {
    type Target = StoreOperationPlanCommon;

    fn deref(&self) -> &Self::Target {
        &self.common
    }
}

impl std::ops::Deref for SerialStoreOperationCommitPlan {
    type Target = StoreOperationPlanCommon;

    fn deref(&self) -> &Self::Target {
        &self.common
    }
}

impl std::ops::Deref for StoreOperationCommitPlan {
    type Target = StoreOperationPlanCommon;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::MergeConcurrent(plan) => &plan.common,
            Self::Serial(plan) => &plan.common,
        }
    }
}

impl StoreOperationPlanCommon {
    pub(crate) fn root(&self) -> &StoreRootRef {
        &self.root
    }

    pub(crate) fn registration_ref(&self) -> &StoreDeviceRegistrationRef {
        &self.registration_ref
    }

    pub(crate) fn registration(&self) -> &StoreDeviceRegistration {
        &self.registration
    }

    pub(crate) fn device_signer(&self) -> &UserKeypair {
        &self.device_signer
    }
}

impl MergeStoreOperationCommitPlan {
    pub(crate) fn common(&self) -> &StoreOperationPlanCommon {
        &self.common
    }

    pub(crate) fn membership(&self) -> &MembershipChain {
        &self.membership
    }

    pub(crate) fn predecessor_state(&self) -> &super::store_commit::ResolvedStoreDeviceState {
        &self.predecessor_state
    }
}

impl SerialStoreOperationCommitPlan {
    pub(crate) fn common(&self) -> &StoreOperationPlanCommon {
        &self.common
    }

    pub(crate) fn base_head(&self) -> &VersionedObject {
        &self.base_head
    }

    pub(crate) fn authorization(&self) -> &SerialAuthorizationState {
        &self.authorization
    }
}

pub(crate) struct MergeConflictResolutionCommitPlan {
    root: StoreRootRef,
    registration_ref: StoreDeviceRegistrationRef,
    registration: Box<StoreDeviceRegistration>,
    device_signer: UserKeypair,
    coord: StoreCommitCoord,
    order: StoreCommitOrder,
    membership: MembershipChain,
    device_state: super::store_commit::StoreDeviceStateRef,
    device_state_value: super::store_commit::ResolvedStoreDeviceState,
}

impl MergeConflictResolutionCommitPlan {
    pub(crate) fn root(&self) -> &StoreRootRef {
        &self.root
    }

    pub(crate) fn registration_ref(&self) -> &StoreDeviceRegistrationRef {
        &self.registration_ref
    }

    pub(crate) fn registration(&self) -> &StoreDeviceRegistration {
        &self.registration
    }

    pub(crate) fn device_state(&self) -> &super::store_commit::StoreDeviceStateRef {
        &self.device_state
    }

    pub(crate) fn membership(&self) -> &MembershipChain {
        &self.membership
    }

    pub(crate) fn finish(
        self,
        membership: &MembershipChain,
        resolution: &super::membership::StoreMembershipConflictResolutionRef,
    ) -> Result<StoreOperationCommitPlan, StoreOutboundError> {
        let super::membership::MembershipStatus::Resolved(resolved) = membership.status() else {
            return Err(StoreOutboundError::InvalidOutbound(
                "conflict-resolution candidate membership remains conflicted".to_string(),
            ));
        };
        if membership
            .resolution_refs()
            .binary_search(resolution)
            .is_err()
        {
            return Err(StoreOutboundError::InvalidOutbound(
                "conflict-resolution candidate membership omits its exact resolution".to_string(),
            ));
        }
        let replacement_grant = super::membership::derive_store_resolution_grant(
            &resolution.conflict_hash,
            &resolution.resolver_pubkey,
        );
        let authority = super::membership::MembershipGrantCreationAuthority::ConflictResolution(
            resolution.clone(),
        );
        if membership
            .active_grant(&replacement_grant)
            .is_none_or(|record| {
                record.member_pubkey != self.registration.author_pubkey
                    || record.creation_authority != authority
            })
        {
            return Err(StoreOutboundError::InvalidOutbound(
                "conflict-resolution candidate is not authorized by its replacement Owner grant"
                    .to_string(),
            ));
        }
        let membership_state = super::circle_control::StoreMembershipStateRef::merge_concurrent(
            membership.head_refs().to_vec(),
            membership.resolution_refs().to_vec(),
            self.device_state.recovery().to_vec(),
            resolved.state_hash,
        )
        .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
        Ok(StoreOperationCommitPlan::MergeConcurrent(
            MergeStoreOperationCommitPlan {
                common: StoreOperationPlanCommon {
                    root: self.root,
                    registration_ref: self.registration_ref,
                    registration: self.registration,
                    device_signer: self.device_signer,
                    coord: self.coord,
                    order: self.order,
                    membership_state,
                    device_state: self.device_state,
                    membership_authority: StoreOperationMembershipAuthority::MergeConcurrent {
                        predecessor: authority,
                    },
                    owner_grant: Some(replacement_grant),
                },
                membership: membership.clone(),
                predecessor_state: self.device_state_value,
            },
        ))
    }
}

impl StoreOperationCommitPlan {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn sign_owner_promotion_request(
        &self,
        promotion_id: super::store_commit::OwnerPromotionId,
        member_registration: super::store_commit::StoreDeviceRegistrationRef,
        member_pubkey: String,
        member_grant: super::membership::MembershipGrantId,
        finalization: super::store_commit::OwnerPromotionFinalization,
        identity_signer: &UserKeypair,
    ) -> Result<super::store_commit::OwnerPromotionRequest, StoreOutboundError> {
        let promoter_owner_grant = self.owner_grant.clone().ok_or_else(|| {
            StoreOutboundError::InvalidOutbound(
                "Owner-promotion request author has no active Owner grant".to_string(),
            )
        })?;
        super::store_commit::OwnerPromotionRequest::signed(
            promotion_id,
            &self.root,
            self.registration_ref.clone(),
            &self.registration,
            promoter_owner_grant,
            member_pubkey,
            member_grant,
            member_registration,
            self.membership_state.clone(),
            self.device_state.clone(),
            finalization,
            identity_signer,
        )
        .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))
    }
}

impl StoreOperationCommitPlan {
    pub(crate) fn predecessor_cut(&self) -> Result<StoreHistoryCut, StoreOutboundError> {
        self.order
            .predecessor_cut()
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))
    }

    pub(crate) fn membership_state(&self) -> &super::circle_control::StoreMembershipStateRef {
        &self.membership_state
    }

    pub(crate) fn device_state(&self) -> &super::store_commit::StoreDeviceStateRef {
        &self.device_state
    }

    pub(crate) fn root(&self) -> &StoreRootRef {
        &self.root
    }

    pub(crate) fn registration_ref(&self) -> &StoreDeviceRegistrationRef {
        &self.registration_ref
    }

    pub(crate) fn registration(&self) -> &StoreDeviceRegistration {
        &self.registration
    }

    pub(crate) fn device_signer(&self) -> &UserKeypair {
        &self.device_signer
    }

    pub(crate) fn owner_grant(&self) -> Option<&super::membership::MembershipGrantId> {
        self.owner_grant.as_ref()
    }

    pub(crate) fn serial_member_grant(
        &self,
        member_pubkey: &str,
    ) -> Option<super::membership::MembershipGrantId> {
        let Self::Serial(serial) = self else {
            return None;
        };
        let grants = serial
            .authorization
            .membership
            .active_grant_ids(member_pubkey);
        let grant = grants.iter().next()?;
        (grants.len() == 1
            && serial
                .authorization
                .membership
                .is_member_grant(member_pubkey, grant))
        .then(|| grant.clone())
    }

    pub(crate) fn serial_authorization(&self) -> Option<&SerialAuthorizationState> {
        let Self::Serial(serial) = self else {
            return None;
        };
        Some(&serial.authorization)
    }

    pub(crate) fn validate_acknowledgement(
        &self,
        acknowledgement: &super::store_commit::StoreAck,
    ) -> Result<(), StoreOutboundError> {
        if acknowledgement.registration != self.registration_ref
            || acknowledgement.store_cut != self.predecessor_cut()?
            || acknowledgement.device_state != self.device_state
        {
            return Err(StoreOutboundError::InvalidOutbound(
                "Store acknowledgement differs from its operation commit predecessor".to_string(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) async fn serial_successor_plan_for_test(
    db: &Database,
    device_id: &str,
    signer: &UserKeypair,
    predecessor: &PreparedStoreOperationCommit,
    base_head: VersionedObject,
) -> Result<StoreOperationCommitPlan, StoreOutboundError> {
    let PreparedStoreOperationCommit::Serial(predecessor) = predecessor else {
        return Err(StoreOutboundError::InvalidOutbound(
            "test Serial successor predecessor uses Merge publication".to_string(),
        ));
    };
    if base_head.bytes != predecessor.head.to_bytes() {
        return Err(StoreOutboundError::InvalidOutbound(
            "test Serial successor receipt differs from its predecessor head".to_string(),
        ));
    }
    let (root, registration_ref, registration, device_signer) =
        load_local_store_authority(db, device_id, signer).await?;
    let sequence = predecessor
        .reference
        .coord
        .sequence()
        .checked_add(1)
        .ok_or_else(|| {
            StoreOutboundError::InvalidOutbound(
                "test Serial successor sequence overflow".to_string(),
            )
        })?;
    let position = StoreSerialPredecessor::Commit(predecessor.reference.clone());
    let membership_state = super::circle_control::StoreMembershipStateRef::serial(
        position.clone(),
        predecessor.commit.membership_state.recovery().to_vec(),
        &predecessor.authorization_after,
    )
    .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    let device_state = super::store_commit::StoreDeviceStateRef::Serial {
        position: position.clone(),
        recovery: predecessor.commit.device_state.recovery().to_vec(),
        state_hash: predecessor.commit.device_state.state_hash(),
    };
    let owner_grant = predecessor
        .authorization_after
        .membership
        .active_owner_grant(&registration.author_pubkey);
    Ok(StoreOperationCommitPlan::Serial(
        SerialStoreOperationCommitPlan {
            common: StoreOperationPlanCommon {
                root,
                registration_ref,
                registration: Box::new(registration),
                device_signer,
                coord: StoreCommitCoord::Serial { sequence },
                order: StoreCommitOrder::Serial {
                    seq: sequence,
                    predecessor: position,
                },
                membership_state,
                device_state,
                membership_authority: StoreOperationMembershipAuthority::Serial,
                owner_grant,
            },
            base_head,
            authorization: predecessor.authorization_after.clone(),
        },
    ))
}

pub(crate) async fn prepare_merge_conflict_resolution_commit(
    db: &Database,
    storage: &dyn SyncStorage,
    device_id: &str,
    keypair: &UserKeypair,
    candidate_membership_heads: &[super::membership::MembershipHeadRef],
) -> Result<MergeConflictResolutionCommitPlan, StoreOutboundError> {
    if db.write_policy() != crate::WritePolicy::MergeConcurrent {
        return Err(StoreOutboundError::InvalidOutbound(
            "membership conflict resolution requires MergeConcurrent policy".to_string(),
        ));
    }
    let (root, registration_ref, registration, device_signer) =
        load_local_store_authority(db, device_id, keypair).await?;
    let previous = db.latest_local_store_position().await?;
    let dependencies = super::store_commit::CommitFrontier::from_refs(
        crate::WritePolicy::MergeConcurrent,
        db.materialized_frontier().await?,
    )
    .and_then(|frontier| frontier.merge_commits().cloned())
    .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    let seq = next_store_sequence(previous.as_ref())?;
    let coord = StoreCommitCoord::MergeConcurrent {
        stream_id: super::store_commit::StreamActivation::device_authorized_stream_id(
            root.store_root_hash,
            &registration_ref,
            super::store_commit::StreamAnchorDomain::StoreAnnouncements,
        ),
        sequence: seq,
    };
    let order = StoreCommitOrder::MergeConcurrent {
        seq,
        predecessor: previous,
        dependencies,
    };
    let authorization = super::store_pull::load_merge_conflict_resolution_authorization(
        db,
        storage,
        &root,
        &order,
        candidate_membership_heads,
        &registration_ref,
        &registration.author_pubkey,
    )
    .await
    .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    Ok(MergeConflictResolutionCommitPlan {
        root,
        registration_ref,
        registration: Box::new(registration),
        device_signer,
        coord,
        order,
        membership: authorization.membership,
        device_state: authorization.device_state_ref,
        device_state_value: authorization.device_state,
    })
}

async fn prepare_serial_store_operation_plan(
    db: &Database,
    root: StoreRootRef,
    registration_ref: StoreDeviceRegistrationRef,
    registration: StoreDeviceRegistration,
    device_signer: UserKeypair,
    snapshot: SerialAuthorizationSnapshot,
) -> Result<StoreOperationCommitPlan, StoreOutboundError> {
    let seq = next_store_sequence(snapshot.base.as_ref())?;
    let predecessor = snapshot.base.clone().map_or_else(
        || StoreSerialPredecessor::Genesis {
            root: root.clone(),
            founder_registration: registration_ref.clone(),
        },
        StoreSerialPredecessor::Commit,
    );
    let coord = StoreCommitCoord::Serial { sequence: seq };
    let order = StoreCommitOrder::Serial {
        seq,
        predecessor: predecessor.clone(),
    };
    let (device_state, resolved_devices) = db.store_device_state_for_order(&order).await?;
    let membership_state = super::circle_control::StoreMembershipStateRef::serial(
        predecessor,
        resolved_devices.recovery,
        &snapshot.authorization,
    )
    .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    let owner_grant = snapshot
        .authorization
        .membership
        .active_owner_grant(&registration.author_pubkey);
    Ok(StoreOperationCommitPlan::Serial(
        SerialStoreOperationCommitPlan {
            common: StoreOperationPlanCommon {
                root,
                registration_ref,
                registration: Box::new(registration),
                device_signer,
                coord,
                order,
                membership_state,
                device_state,
                membership_authority: StoreOperationMembershipAuthority::Serial,
                owner_grant,
            },
            base_head: snapshot.base_head,
            authorization: snapshot.authorization,
        },
    ))
}

pub(crate) async fn prepare_store_operation_commit_from_serial_snapshot(
    db: &Database,
    device_id: &str,
    keypair: &UserKeypair,
    snapshot: SerialAuthorizationSnapshot,
) -> Result<StoreOperationCommitPlan, StoreOutboundError> {
    if db.write_policy() != crate::WritePolicy::Serial {
        return Err(StoreOutboundError::InvalidOutbound(
            "Serial Store operation snapshot used with MergeConcurrent policy".to_string(),
        ));
    }
    let (root, registration_ref, registration, device_signer) =
        load_local_store_authority(db, device_id, keypair).await?;
    prepare_serial_store_operation_plan(
        db,
        root,
        registration_ref,
        registration,
        device_signer,
        snapshot,
    )
    .await
}

pub(crate) async fn prepare_store_operation_commit(
    db: &Database,
    storage: &dyn SyncStorage,
    preparation: StoreOperationPreparation<'_>,
    device_id: &str,
    keypair: &UserKeypair,
) -> Result<StoreOperationCommitPlan, StoreOutboundError> {
    if db.write_policy() != preparation.policy() {
        return Err(StoreOutboundError::InvalidOutbound(format!(
            "Store operation preparation policy {:?} differs from database policy {:?}",
            preparation.policy(),
            db.write_policy()
        )));
    }
    let (root, registration_ref, registration, device_signer) =
        load_local_store_authority(db, device_id, keypair).await?;
    let (coord, order, membership_state, device_state, membership_authority, owner_grant, policy) =
        match preparation {
            StoreOperationPreparation::MergeConcurrent {
                membership: candidate_membership,
            } => {
                let previous = db.latest_local_store_position().await?;
                let dependencies = super::store_commit::CommitFrontier::from_refs(
                    crate::WritePolicy::MergeConcurrent,
                    db.materialized_frontier().await?,
                )
                .and_then(|frontier| frontier.merge_commits().cloned())
                .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
                let seq = next_store_sequence(previous.as_ref())?;
                let coord = StoreCommitCoord::MergeConcurrent {
                    stream_id: super::store_commit::StreamActivation::device_authorized_stream_id(
                        root.store_root_hash,
                        &registration_ref,
                        super::store_commit::StreamAnchorDomain::StoreAnnouncements,
                    ),
                    sequence: seq,
                };
                let order = StoreCommitOrder::MergeConcurrent {
                    seq,
                    predecessor: previous,
                    dependencies,
                };
                let authorization = super::store_pull::load_retained_merge_outbound_authorization(
                    db,
                    storage,
                    &root,
                    &order,
                    candidate_membership.head_refs(),
                    &registration_ref,
                )
                .await
                .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
                let owner_grant = authorization
                    .membership
                    .active_owner_grant(&registration.author_pubkey);
                let predecessor = authorization
                    .membership
                    .write_grant_authority(&registration.author_pubkey)
                    .ok_or_else(|| {
                        StoreOutboundError::InvalidOutbound(format!(
                            "Merge Store operation author {} has no active write grant",
                            registration.author_pubkey
                        ))
                    })?;
                (
                    coord,
                    order,
                    authorization.membership_state,
                    authorization.device_state_ref,
                    StoreOperationMembershipAuthority::MergeConcurrent { predecessor },
                    owner_grant,
                    (authorization.membership, authorization.device_state),
                )
            }
            StoreOperationPreparation::Serial { coordination } => {
                let snapshot = Box::pin(current_serial_authorization_snapshot(
                    db,
                    storage,
                    coordination,
                ))
                .await?;
                return prepare_serial_store_operation_plan(
                    db,
                    root,
                    registration_ref,
                    registration,
                    device_signer,
                    snapshot,
                )
                .await;
            }
        };
    let (membership, predecessor_state) = policy;
    Ok(StoreOperationCommitPlan::MergeConcurrent(
        MergeStoreOperationCommitPlan {
            common: StoreOperationPlanCommon {
                root,
                registration_ref,
                registration: Box::new(registration),
                device_signer,
                coord,
                order,
                membership_state,
                device_state,
                membership_authority,
                owner_grant,
            },
            membership,
            predecessor_state,
        },
    ))
}

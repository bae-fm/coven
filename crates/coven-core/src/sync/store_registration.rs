//! Durable append-only Store device registration and retirement.

use std::collections::BTreeMap;

use crate::database::Database;
use crate::keys::UserKeypair;

use super::membership::MembershipChain;
use super::storage::{CoordinationStorage, PreparedExactObject, ProtocolObjectDomain, SyncStorage};
use super::store_commit::{
    ack_slot_prefix, commit_semantic_prefix, device_self_retirement_semantic_prefix,
    head_slot_prefix, owner_recovery_semantic_prefix, registration_semantic_prefix,
    snapshot_slot_prefix, ActivatedStoreDeviceRegistrationRef, CandidateFamilyId, CommitFrontier,
    DeviceJoinAttempt, DeviceJoinAttemptDecisionRef, DeviceJoinAttemptRef, DeviceReadinessProof,
    DeviceRecoveryId, DeviceRecoveryReadiness, DeviceStreamAnchor, ObjectHash, OwnerRecoveryNode,
    OwnerRecoveryNodeRef, OwnerRecoveryPosition, SerialRecoveryActivation, StoreAck,
    StoreAckExclusionState, StoreAckRef, StoreBatchCommit, StoreBatchCommitRef, StoreCommitAnchor,
    StoreCommitCoord, StoreCommitOrder, StoreDeviceHead, StoreDeviceRegistration,
    StoreDeviceRegistrationActivation, StoreDeviceRegistrationActivationRef,
    StoreDeviceRegistrationOrigin, StoreDeviceRegistrationRef, StoreDeviceSelfRetirement,
    StoreDeviceSelfRetirementRef, StoreDeviceStateRef, StoreHistoryCut,
    StoreOperationMembershipAuthority, StoreSerialHead, StoreSerialHeadState,
    StoreSerialPredecessor, SuccessorLink, SERIAL_STREAM_ID,
};
use super::store_objects::StoreObjectError;

#[derive(Debug, thiserror::Error)]
pub enum StoreRegistrationError {
    #[error("Store device registration database state: {0}")]
    Database(String),
    #[error("{0}")]
    Object(#[from] StoreObjectError),
    #[error("exact Store root authority is absent")]
    ExactRootAuthorityMissing,
    #[error("Store device registration bytes are invalid: {0}")]
    Invalid(String),
    #[error("this Store installation requires an activated Join or Recovery registration")]
    ActivationRequired,
    #[error("retired Store device {device_id:?} cannot become active again")]
    RetiredDevice { device_id: String },
    #[error("Store device registration activation: {0}")]
    Outbound(#[from] super::store_outbound::StoreOutboundError),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct DurableRetirement {
    retirement_bytes: Vec<u8>,
    retirement_prepared: PreparedExactObject,
    commit_bytes: Vec<u8>,
    commit_prepared: PreparedExactObject,
    commit_ref: StoreBatchCommitRef,
    publication: RetirementPublication,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum RetirementPublication {
    MergeConcurrent {
        head_bytes: Vec<u8>,
        head_prepared: PreparedExactObject,
    },
    Serial {
        base_head: super::storage::VersionedObject,
        head_bytes: Vec<u8>,
        authorization_after: super::membership::SerialAuthorizationState,
    },
}

impl DurableRetirement {
    fn closed_remote_objects(
        &self,
    ) -> Result<Vec<super::remote_object::RemoteObjectRecord>, StoreRegistrationError> {
        let commit: StoreBatchCommit = serde_json::from_slice(&self.commit_bytes)
            .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
        self.commit_ref
            .verify_commit(&commit)
            .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
        let mut remotes = super::remote_object::CandidateObjectGraph::from_commit(&commit)
            .and_then(|graph| {
                graph.close(
                    &commit,
                    &self.commit_ref,
                    vec![super::remote_object::CandidateObjectMaterial {
                        object: self.retirement_prepared.reference().clone(),
                        canonical_semantic_bytes: self.retirement_bytes.clone(),
                        stored_bytes: self.retirement_prepared.stored_bytes().to_vec(),
                    }],
                )
            })
            .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
        remotes.push(
            super::remote_object::RemoteObjectRecord::candidate_commit(
                self.commit_ref.clone(),
                self.commit_bytes.clone(),
                self.commit_prepared.stored_bytes().to_vec(),
            )
            .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?,
        );
        if let RetirementPublication::MergeConcurrent {
            head_bytes,
            head_prepared,
        } = &self.publication
        {
            let head: StoreDeviceHead = serde_json::from_slice(head_bytes)
                .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
            remotes.push(
                super::remote_object::RemoteObjectRecord::candidate_activated_store_head(
                    super::store_commit::StoreDeviceHeadRef {
                        head_hash: head.head_hash(),
                        object: head_prepared.reference().clone(),
                    },
                    head_bytes.clone(),
                    head_prepared.stored_bytes().to_vec(),
                    self.commit_ref.clone(),
                )
                .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?,
            );
        }
        Ok(remotes)
    }
}

#[cfg(any(test, feature = "test-utils"))]
pub async fn ensure_active_registration(
    db: &Database,
    storage: &dyn SyncStorage,
    signer: &UserKeypair,
) -> Result<(), StoreRegistrationError> {
    ensure_active_registration_with_coordination(
        db,
        storage,
        None,
        signer,
        None,
        "1970-01-01T00:00:00Z",
    )
    .await
}

pub async fn ensure_active_registration_with_coordination(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn CoordinationStorage>,
    signer: &UserKeypair,
    membership: Option<&MembershipChain>,
    published_at: &str,
) -> Result<(), StoreRegistrationError> {
    drain_registration_outbox(db, storage, coordination, signer, membership, published_at).await?;
    match db
        .latest_local_store_device_registration()
        .await
        .map_err(database_error)?
    {
        Some(registration) if registration.is_activated() => {
            require_activated_registration(db, storage, &registration).await?;
            return Ok(());
        }
        Some(registration) if registration.is_retired() => {
            return Err(StoreRegistrationError::RetiredDevice {
                device_id: registration.device_id.to_string(),
            });
        }
        Some(_) => return Err(StoreRegistrationError::ActivationRequired),
        None => {}
    }

    db.local_store_root_ref()
        .await
        .map_err(database_error)?
        .ok_or(StoreRegistrationError::ExactRootAuthorityMissing)?;
    Err(StoreRegistrationError::ActivationRequired)
}

pub(crate) async fn install_existing_founder_device(
    db: &Database,
    storage: &dyn SyncStorage,
    root: &super::store_commit::StoreRootRef,
    signer: &UserKeypair,
) -> Result<(), StoreRegistrationError> {
    let founder = super::store_objects::load_founder_registration(storage, root).await?;
    if founder.value.author_pubkey != crate::keys::public_key_hex(signer) {
        return Err(StoreRegistrationError::Invalid(
            "Store founder registration belongs to another identity".to_string(),
        ));
    }
    if founder.value.provider
        != storage
            .provider_binding()
            .await
            .map_err(StoreObjectError::from)?
            .device
    {
        return Err(StoreRegistrationError::Invalid(
            "Store founder registration belongs to another provider principal".to_string(),
        ));
    }
    founder
        .value
        .device_signer(signer)
        .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;

    let registration_context = super::storage::ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreDeviceRegistration,
    );
    let registration_prefix =
        super::store_commit::founder_registration_semantic_prefix(match founder.value.origin {
            StoreDeviceRegistrationOrigin::Founder { creation_id } => creation_id,
            _ => {
                return Err(StoreRegistrationError::Invalid(
                    "Store founder registration has a non-founder origin".to_string(),
                ))
            }
        });
    let (registration_bytes, registration_prepared) = storage
        .read_prepared_protocol_slot(
            &registration_context,
            founder.object.slot(),
            &registration_prefix,
        )
        .await
        .map_err(StoreObjectError::from)?;
    if registration_bytes != founder.bytes || registration_prepared.reference() != &founder.object {
        return Err(StoreRegistrationError::Invalid(
            "prepared founder registration differs from its verified exact object".to_string(),
        ));
    }
    let registration_ref =
        StoreDeviceRegistrationRef::from_registration(&founder.value, founder.object.clone());
    let DeviceStreamAnchor::StoreAcknowledgements { first_slot } = &founder.value.acknowledgements
    else {
        return Err(StoreRegistrationError::Invalid(
            "Store founder registration has no acknowledgement anchor".to_string(),
        ));
    };
    let ack_context = super::storage::ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreAck,
    );
    let ack_prefix = ack_slot_prefix(&founder.value.device_id.to_string(), 1);
    let (ack_bytes, ack_prepared) = storage
        .read_prepared_protocol_slot(&ack_context, first_slot, &ack_prefix)
        .await
        .map_err(StoreObjectError::from)?;
    let unverified_ack: StoreAck = serde_json::from_slice(&ack_bytes)
        .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
    let ack_ref = StoreAckRef {
        registration: registration_ref.clone(),
        sequence: unverified_ack.sequence,
        ack_hash: unverified_ack.ack_hash(),
        object: ack_prepared.reference().clone(),
    };
    let ack = StoreAck::parse_at(&ack_bytes, root, &ack_ref, &founder.value)
        .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
    if ack.registration != registration_ref {
        return Err(StoreRegistrationError::Invalid(
            "Store founder acknowledgement names another registration".to_string(),
        ));
    }
    db.install_existing_local_founder_device(
        crate::database::ExactProtocolObject {
            value: founder.value,
            bytes: registration_bytes,
            object: registration_prepared.reference().clone(),
            prepared: registration_prepared,
        },
        ack_ref,
        crate::database::ExactProtocolObject {
            value: ack,
            bytes: ack_bytes,
            object: ack_prepared.reference().clone(),
            prepared: ack_prepared,
        },
    )
    .await
    .map_err(database_error)
}

pub async fn retire_registration_with_coordination(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn CoordinationStorage>,
    signer: &UserKeypair,
    membership: Option<&MembershipChain>,
    _published_at: &str,
) -> Result<bool, StoreRegistrationError> {
    drain_registration_outbox(db, storage, coordination, signer, membership, _published_at).await?;
    let Some(registration) = db
        .latest_local_store_device_registration()
        .await
        .map_err(database_error)?
    else {
        return Ok(false);
    };
    if !registration.is_activated() && !registration.is_retired() {
        return Err(StoreRegistrationError::ActivationRequired);
    }
    if db
        .local_store_device_retirement()
        .await
        .map_err(database_error)?
        .is_none()
    {
        if registration.is_retired() {
            return Err(StoreRegistrationError::Database(
                "retired local registration has no exact retirement journal".into(),
            ));
        }
        let durable =
            prepare_self_retirement(db, storage, coordination, signer, membership).await?;
        let payload = serde_json::to_vec(&durable)
            .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
        let remotes = durable.closed_remote_objects()?;
        db.stage_local_store_device_retirement(payload, remotes)
            .await
            .map_err(database_error)?;
    }
    publish_self_retirement(db, storage, coordination).await?;
    Ok(true)
}

async fn prepare_self_retirement(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn CoordinationStorage>,
    signer: &UserKeypair,
    membership: Option<&MembershipChain>,
) -> Result<DurableRetirement, StoreRegistrationError> {
    let device_id = db
        .latest_local_store_device_registration()
        .await
        .map_err(database_error)?
        .ok_or(StoreRegistrationError::ActivationRequired)?
        .device_id
        .to_string();
    let (root, registration_ref, registration, device_signer) =
        super::store_outbound::load_local_store_authority(db, &device_id, signer).await?;
    let write_id = db.new_write_id();
    let (coord, order, membership_state, device_state, publication_seed) = match db.write_policy() {
        crate::WritePolicy::MergeConcurrent => {
            let previous = db
                .latest_local_store_position()
                .await
                .map_err(database_error)?;
            let dependencies = CommitFrontier::from_refs(
                crate::WritePolicy::MergeConcurrent,
                db.materialized_frontier().await.map_err(database_error)?,
            )
            .and_then(|frontier| frontier.merge_commits().cloned())
            .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
            let seq = previous
                .as_ref()
                .map_or(1, |reference| reference.coord.sequence().saturating_add(1));
            let stream_id = super::store_commit::StreamActivation::device_authorized_stream_id(
                root.store_root_hash,
                &registration_ref,
                super::store_commit::StreamAnchorDomain::StoreAnnouncements,
            );
            let coord = StoreCommitCoord::MergeConcurrent {
                stream_id,
                sequence: seq,
            };
            let order = StoreCommitOrder::MergeConcurrent {
                seq,
                predecessor: previous.clone(),
                dependencies,
            };
            let (device_state, resolved_devices) = db
                .store_device_state_for_order(&order)
                .await
                .map_err(database_error)?;
            if !resolved_devices
                .devices
                .get(&registration_ref.device_id)
                .is_some_and(|record| {
                    record.registration == registration_ref
                        && matches!(
                            record.status,
                            super::store_commit::StoreDeviceStatus::Active
                        )
                })
            {
                return Err(StoreRegistrationError::Invalid(
                    "local registration is not active at the exact retirement cut".into(),
                ));
            }
            let chain = membership.ok_or_else(|| {
                StoreRegistrationError::Invalid(
                    "Merge self-retirement has no exact membership state".into(),
                )
            })?;
            let super::membership::MembershipStatus::Resolved(resolved) = chain.status() else {
                return Err(StoreRegistrationError::Invalid(
                    "Merge self-retirement requires resolved membership".into(),
                ));
            };
            let membership_state =
                super::circle_control::StoreMembershipStateRef::merge_concurrent(
                    chain.head_refs().to_vec(),
                    chain.resolution_refs().to_vec(),
                    resolved_devices.recovery.clone(),
                    resolved.state_hash,
                )
                .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
            (coord, order, membership_state, device_state, None)
        }
        crate::WritePolicy::Serial => {
            let coordination = coordination.ok_or_else(|| {
                StoreRegistrationError::Invalid(
                    "Serial self-retirement requires coordination".into(),
                )
            })?;
            let snapshot = super::store_outbound::current_serial_authorization_snapshot(
                db,
                storage,
                coordination,
            )
            .await?;
            let seq = snapshot
                .base
                .as_ref()
                .map_or(1, |reference| reference.coord.sequence().saturating_add(1));
            let predecessor = snapshot.base.clone().map_or_else(
                || StoreSerialPredecessor::Genesis {
                    root: root.clone(),
                    founder_registration: registration_ref.clone(),
                },
                StoreSerialPredecessor::Commit,
            );
            let order = StoreCommitOrder::Serial {
                seq,
                predecessor: predecessor.clone(),
            };
            let (device_state, resolved_devices) = db
                .store_device_state_for_order(&order)
                .await
                .map_err(database_error)?;
            if !resolved_devices
                .devices
                .get(&registration_ref.device_id)
                .is_some_and(|record| {
                    record.registration == registration_ref
                        && matches!(
                            record.status,
                            super::store_commit::StoreDeviceStatus::Active
                        )
                })
            {
                return Err(StoreRegistrationError::Invalid(
                    "local registration is not active at the exact retirement cut".into(),
                ));
            }
            let membership_state = super::circle_control::StoreMembershipStateRef::serial(
                predecessor,
                resolved_devices.recovery,
                &snapshot.authorization,
            )
            .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
            (
                StoreCommitCoord::Serial { sequence: seq },
                order,
                membership_state,
                device_state,
                Some(snapshot),
            )
        }
    };
    let candidate_family =
        CandidateFamilyId::derive(root.store_root_hash, &registration_ref, &write_id, &order);
    let retiring_cut = match &order {
        StoreCommitOrder::MergeConcurrent {
            predecessor,
            dependencies,
            ..
        } => {
            let mut cut = dependencies.clone();
            if let Some(predecessor) = predecessor {
                let StoreCommitCoord::MergeConcurrent { stream_id, .. } = predecessor.coord else {
                    return Err(StoreRegistrationError::Invalid(
                        "Merge predecessor is Serial".into(),
                    ));
                };
                cut.insert(stream_id, predecessor.clone());
            }
            StoreHistoryCut::MergeConcurrent(cut)
        }
        StoreCommitOrder::Serial { predecessor, .. } => {
            StoreHistoryCut::Serial(predecessor.clone())
        }
    };
    let retirement = StoreDeviceSelfRetirement::signed(
        root.store_root_hash,
        candidate_family,
        registration_ref.clone(),
        retiring_cut,
        &device_signer,
    )
    .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
    let retirement_prefix = device_self_retirement_semantic_prefix(
        candidate_family,
        &registration_ref.device_id,
        retirement.retirement_hash(),
    );
    let retirement_context = super::storage::ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreDeviceSelfRetirement,
    );
    let retirement_slot = storage
        .allocate_protocol_slot(&retirement_context, &retirement_prefix, ".json")
        .await
        .map_err(StoreObjectError::from)?;
    let retirement_prepared = storage
        .prepare_protocol_object(
            &retirement_context,
            retirement_slot,
            &retirement_prefix,
            retirement.to_bytes(),
        )
        .map_err(StoreObjectError::from)?;
    let retirement_ref = StoreDeviceSelfRetirementRef::from_retirement(
        &retirement,
        retirement_prepared.reference().clone(),
    );
    let commit_context = super::storage::ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreCommit,
    );
    let stream = match coord {
        StoreCommitCoord::MergeConcurrent { stream_id, .. } => stream_id.to_string(),
        StoreCommitCoord::Serial { .. } => SERIAL_STREAM_ID.to_string(),
    };
    let commit = StoreBatchCommit::signed_with_self_retirement(
        root.store_root_hash,
        write_id,
        coord.clone(),
        registration_ref.clone(),
        &registration,
        order,
        membership_state,
        device_state,
        None,
        retirement_ref,
        &device_signer,
    )
    .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
    let commit_prefix = commit_semantic_prefix(
        commit.candidate_family(),
        &stream,
        commit.seq(),
        commit.commit_hash(),
    );
    let commit_slot = storage
        .allocate_protocol_slot(&commit_context, &commit_prefix, ".json")
        .await
        .map_err(StoreObjectError::from)?;
    let commit_prepared = storage
        .prepare_protocol_object(
            &commit_context,
            commit_slot,
            &commit_prefix,
            commit.to_bytes(),
        )
        .map_err(StoreObjectError::from)?;
    let commit_ref =
        StoreBatchCommitRef::from_commit(&commit, coord, commit_prepared.reference().clone())
            .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
    let publication = match publication_seed {
        None => {
            let previous = commit.order.predecessor().cloned();
            let (head_slot, predecessor_head) =
                super::store_outbound::exact_next_announcement_slot(
                    storage,
                    &root,
                    &registration_ref,
                    &registration,
                    previous.as_ref(),
                )
                .await?;
            let head_context = super::storage::ProtocolObjectContext::signed_plaintext(
                root.store_root_hash,
                ProtocolObjectDomain::StoreHead,
            );
            let next_prefix = head_slot_prefix(
                &registration_ref.device_id.to_string(),
                commit.seq().saturating_add(1),
            );
            let next_slot = storage
                .allocate_protocol_slot(&head_context, &next_prefix, ".json")
                .await
                .map_err(StoreObjectError::from)?;
            let head = StoreDeviceHead::signed(
                root.store_root_hash,
                registration_ref.clone(),
                commit_ref.clone(),
                SuccessorLink {
                    activation: registration
                        .store_announcement_activation(&registration_ref)
                        .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?
                        .activation_id(),
                    predecessor: predecessor_head.map(|head| head.object),
                    next_slot,
                },
                &device_signer,
            )
            .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
            let head_prefix =
                head_slot_prefix(&registration_ref.device_id.to_string(), commit.seq());
            let head_prepared = storage
                .prepare_protocol_object(&head_context, head_slot, &head_prefix, head.to_bytes())
                .map_err(StoreObjectError::from)?;
            RetirementPublication::MergeConcurrent {
                head_bytes: head.to_bytes(),
                head_prepared,
            }
        }
        Some(snapshot) => {
            let authorization_after = snapshot
                .authorization
                .authorize_and_apply(&commit_ref, &commit, &registration)
                .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
            let head = StoreSerialHead::signed(
                root.store_root_hash,
                StoreSerialHeadState::Commit {
                    author_registration: registration_ref,
                    commit: commit_ref.clone(),
                },
                &device_signer,
            )
            .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
            RetirementPublication::Serial {
                base_head: snapshot.base_head,
                head_bytes: head.to_bytes(),
                authorization_after,
            }
        }
    };
    Ok(DurableRetirement {
        retirement_bytes: retirement.to_bytes(),
        retirement_prepared,
        commit_bytes: commit.to_bytes(),
        commit_prepared,
        commit_ref,
        publication,
    })
}

async fn publish_self_retirement(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn CoordinationStorage>,
) -> Result<(), StoreRegistrationError> {
    let Some((payload, published)) = db
        .local_store_device_retirement()
        .await
        .map_err(database_error)?
    else {
        return Ok(());
    };
    if published {
        return Ok(());
    }
    let durable: DurableRetirement = serde_json::from_slice(&payload)
        .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
    let closed_remotes = durable.closed_remote_objects()?;
    let root = db
        .local_store_root_ref()
        .await
        .map_err(database_error)?
        .ok_or(StoreRegistrationError::ExactRootAuthorityMissing)?;
    let unverified: StoreBatchCommit = serde_json::from_slice(&durable.commit_bytes)
        .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
    let registration = db
        .activated_store_device_registration(unverified.author_registration.clone())
        .await
        .map_err(database_error)?;
    let commit = StoreBatchCommit::parse_at(
        &durable.commit_bytes,
        root.store_root_hash,
        &durable.commit_ref.coord,
        &registration,
    )
    .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
    durable
        .commit_ref
        .verify_commit(&commit)
        .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
    let [retirement_ref] = commit.device_retirements() else {
        return Err(StoreRegistrationError::Invalid(
            "retirement commit does not carry exactly one terminal".into(),
        ));
    };
    let retirement = StoreDeviceSelfRetirement::parse_at(
        &durable.retirement_bytes,
        retirement_ref,
        &registration,
    )
    .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
    if durable.retirement_prepared.reference() != &retirement_ref.object
        || durable.commit_prepared.reference() != &durable.commit_ref.object
    {
        return Err(StoreRegistrationError::Invalid(
            "durable retirement refs differ from their prepared objects".into(),
        ));
    }
    storage
        .create_protocol_object(&durable.retirement_prepared)
        .await
        .map_err(StoreObjectError::from)?;
    let retirement_context = super::storage::ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreDeviceSelfRetirement,
    );
    let opened = storage
        .read_protocol_object(
            &retirement_context,
            &retirement_ref.object,
            &device_self_retirement_semantic_prefix(
                retirement_ref.candidate_family,
                &retirement_ref.target.device_id,
                retirement_ref.retirement_hash,
            ),
        )
        .await
        .map_err(StoreObjectError::from)?;
    if opened != durable.retirement_bytes || retirement.to_bytes() != durable.retirement_bytes {
        return Err(StoreRegistrationError::Invalid(
            "retirement exact readback differs from its signed bytes".into(),
        ));
    }
    let retirement_remote = closed_remotes
        .iter()
        .find(|remote| remote.object() == &retirement_ref.object)
        .cloned()
        .ok_or_else(|| {
            StoreRegistrationError::Invalid(
                "retirement object is absent from its closed candidate graph".to_string(),
            )
        })?;
    db.mark_remote_object_uploaded(retirement_remote)
        .await
        .map_err(database_error)?;
    let serial_authorization = match &durable.publication {
        RetirementPublication::MergeConcurrent {
            head_bytes,
            head_prepared,
        } => {
            storage
                .create_protocol_object(&durable.commit_prepared)
                .await
                .map_err(StoreObjectError::from)?;
            storage
                .create_protocol_object(head_prepared)
                .await
                .map_err(StoreObjectError::from)?;
            let StoreCommitCoord::MergeConcurrent { stream_id, .. } = durable.commit_ref.coord
            else {
                return Err(StoreRegistrationError::Invalid(
                    "Merge retirement journal carries a Serial commit".into(),
                ));
            };
            let opened_commit = storage
                .read_protocol_object(
                    &super::storage::ProtocolObjectContext::signed_plaintext(
                        root.store_root_hash,
                        ProtocolObjectDomain::StoreCommit,
                    ),
                    &durable.commit_ref.object,
                    &commit_semantic_prefix(
                        commit.candidate_family(),
                        &stream_id.to_string(),
                        commit.seq(),
                        commit.commit_hash(),
                    ),
                )
                .await
                .map_err(StoreObjectError::from)?;
            let opened_head = storage
                .read_protocol_object(
                    &super::storage::ProtocolObjectContext::signed_plaintext(
                        root.store_root_hash,
                        ProtocolObjectDomain::StoreHead,
                    ),
                    head_prepared.reference(),
                    &head_slot_prefix(
                        &commit.author_registration.device_id.to_string(),
                        commit.seq(),
                    ),
                )
                .await
                .map_err(StoreObjectError::from)?;
            if opened_commit != durable.commit_bytes || opened_head != *head_bytes {
                return Err(StoreRegistrationError::Invalid(
                    "retirement commit or head exact readback differs".into(),
                ));
            }
            for object in [
                durable.commit_prepared.reference(),
                head_prepared.reference(),
            ] {
                let remote = closed_remotes
                    .iter()
                    .find(|remote| remote.object() == object)
                    .cloned()
                    .ok_or_else(|| {
                        StoreRegistrationError::Invalid(
                            "retirement publication object is absent from its closed graph"
                                .to_string(),
                        )
                    })?;
                db.mark_remote_object_uploaded(remote)
                    .await
                    .map_err(database_error)?;
            }
            None
        }
        RetirementPublication::Serial {
            base_head,
            head_bytes,
            authorization_after,
        } => {
            let coordination = coordination.ok_or_else(|| {
                StoreRegistrationError::Invalid(
                    "Serial self-retirement requires coordination".into(),
                )
            })?;
            let head = StoreSerialHead::parse(head_bytes, root.store_root_hash, &registration)
                .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
            super::store_outbound::activate_serial_commit_head(
                db,
                storage,
                coordination,
                base_head,
                &commit,
                &durable.commit_prepared,
                &durable.commit_ref,
                &head,
            )
            .await?;
            let commit_remote = closed_remotes
                .iter()
                .find(|remote| remote.object() == durable.commit_prepared.reference())
                .cloned()
                .ok_or_else(|| {
                    StoreRegistrationError::Invalid(
                        "retirement commit is absent from its closed candidate graph".to_string(),
                    )
                })?;
            db.mark_remote_object_uploaded(commit_remote)
                .await
                .map_err(database_error)?;
            Some(authorization_after.clone())
        }
    };
    db.complete_local_store_device_retirement(
        payload,
        retirement_ref.clone(),
        commit,
        durable.commit_ref,
        serial_authorization,
        closed_remotes
            .iter()
            .map(|remote| remote.object_id())
            .collect(),
    )
    .await
    .map_err(database_error)
}

pub(crate) async fn prepare_registration_for_origin(
    storage: &dyn SyncStorage,
    identity_signer: &UserKeypair,
    write_policy: crate::WritePolicy,
    store_root: super::store_commit::StoreRootRef,
    origin: StoreDeviceRegistrationOrigin,
    reserved_slot: crate::storage::cloud::ObjectSlot,
    expected_provider: super::storage::ProviderDeviceBinding,
    store_commits: StoreCommitAnchor,
    acknowledgements: DeviceStreamAnchor,
    snapshots: DeviceStreamAnchor,
) -> Result<(StoreDeviceRegistration, PreparedExactObject), StoreRegistrationError> {
    let provider = storage
        .provider_binding()
        .await
        .map_err(StoreObjectError::from)?
        .device;
    if provider != expected_provider {
        return Err(StoreRegistrationError::Invalid(
            "live provider principal differs from the reserved founder authority".to_string(),
        ));
    }
    if write_policy
        != match &store_commits {
            StoreCommitAnchor::MergeConcurrent { .. } => crate::WritePolicy::MergeConcurrent,
            StoreCommitAnchor::Serial => crate::WritePolicy::Serial,
        }
    {
        return Err(StoreRegistrationError::Invalid(
            "reserved registration commit anchor differs from the Store policy".to_string(),
        ));
    }
    let registration = StoreDeviceRegistration::signed(
        store_root,
        origin,
        provider,
        store_commits,
        acknowledgements,
        snapshots,
        identity_signer,
    )
    .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
    let prepared = prepare_registration_object(storage, &registration, reserved_slot)?;
    Ok((registration, prepared))
}

fn prepare_registration_object(
    storage: &dyn SyncStorage,
    registration: &StoreDeviceRegistration,
    slot: crate::storage::cloud::ObjectSlot,
) -> Result<PreparedExactObject, StoreRegistrationError> {
    let semantic_prefix = slot
        .logical_key()
        .strip_suffix(".json")
        .ok_or_else(|| {
            StoreRegistrationError::Invalid(
                "reserved registration slot has no .json suffix".to_string(),
            )
        })?
        .to_string();
    let context = super::storage::ProtocolObjectContext::signed_plaintext(
        registration.store_root.store_root_hash,
        ProtocolObjectDomain::StoreDeviceRegistration,
    );
    storage
        .prepare_protocol_object(&context, slot, &semantic_prefix, registration.to_bytes())
        .map_err(StoreObjectError::from)
        .map_err(StoreRegistrationError::from)
}

async fn prepare_or_load_recovery_registration(
    storage: &dyn SyncStorage,
    root: &super::store_commit::StoreRootRef,
    expected: StoreDeviceRegistration,
    slot: crate::storage::cloud::ObjectSlot,
    semantic_prefix: &str,
) -> Result<
    (
        StoreDeviceRegistration,
        StoreDeviceRegistrationRef,
        PreparedExactObject,
        bool,
    ),
    StoreRegistrationError,
> {
    let context = super::storage::ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreDeviceRegistration,
    );
    match storage
        .read_prepared_protocol_slot(&context, &slot, semantic_prefix)
        .await
    {
        Ok((bytes, prepared)) => {
            let registration = StoreDeviceRegistration::parse_at(&bytes, root, expected.device_id)
                .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
            if registration != expected {
                return Err(StoreRegistrationError::Invalid(
                    "existing Owner recovery registration differs from its exact authority".into(),
                ));
            }
            let reference = StoreDeviceRegistrationRef::from_registration(
                &registration,
                prepared.reference().clone(),
            );
            Ok((registration, reference, prepared, true))
        }
        Err(super::storage::StorageError::NotFound(_)) => {
            let prepared = storage
                .prepare_protocol_object(&context, slot, semantic_prefix, expected.to_bytes())
                .map_err(StoreObjectError::from)?;
            let reference = StoreDeviceRegistrationRef::from_registration(
                &expected,
                prepared.reference().clone(),
            );
            Ok((expected, reference, prepared, false))
        }
        Err(error) => Err(StoreObjectError::from(error).into()),
    }
}

async fn prepared_protocol_object_exists(
    storage: &dyn SyncStorage,
    context: &super::storage::ProtocolObjectContext,
    prepared: &PreparedExactObject,
    semantic_prefix: &str,
    expected_bytes: &[u8],
) -> Result<bool, StoreRegistrationError> {
    match storage
        .read_prepared_protocol_slot(context, prepared.reference().slot(), semantic_prefix)
        .await
    {
        Ok((bytes, opened))
            if bytes == expected_bytes && opened.reference() == prepared.reference() =>
        {
            Ok(true)
        }
        Ok(_) => Err(StoreRegistrationError::Invalid(format!(
            "exact object {semantic_prefix:?} differs from its staged Owner recovery bytes"
        ))),
        Err(super::storage::StorageError::NotFound(_)) => Ok(false),
        Err(error) => Err(StoreObjectError::from(error).into()),
    }
}

async fn prepare_or_load_initial_recovery_ack(
    storage: &dyn SyncStorage,
    root: &super::store_commit::StoreRootRef,
    registration: &StoreDeviceRegistration,
    registration_ref: &StoreDeviceRegistrationRef,
    first_slot: crate::storage::cloud::ObjectSlot,
    store_cut: StoreHistoryCut,
    device_state: StoreDeviceStateRef,
    published_at: &str,
    device_signer: &UserKeypair,
) -> Result<(StoreAck, Vec<u8>, StoreAckRef, PreparedExactObject, bool), StoreRegistrationError> {
    let context = super::storage::ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreAck,
    );
    let prefix = ack_slot_prefix(&registration.device_id.to_string(), 1);
    match storage
        .read_prepared_protocol_slot(&context, &first_slot, &prefix)
        .await
    {
        Ok((bytes, prepared)) => {
            let object = prepared.reference();
            let decoded: StoreAck = serde_json::from_slice(&bytes)
                .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
            let reference = StoreAckRef {
                registration: registration_ref.clone(),
                sequence: decoded.sequence,
                ack_hash: decoded.ack_hash(),
                object: object.clone(),
            };
            let ack = StoreAck::parse_at(&bytes, root, &reference, registration)
                .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
            let expected_activation = registration
                .store_acknowledgement_activation(registration_ref)
                .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?
                .activation_id();
            if ack.sequence != 1
                || ack.successor.predecessor.is_some()
                || ack.registration != *registration_ref
                || ack.store_cut != store_cut
                || ack.device_state != device_state
                || ack.last_sync != published_at
                || ack.successor.activation != expected_activation
                || ack.successor.next_slot == first_slot
            {
                return Err(StoreRegistrationError::Invalid(
                    "existing Owner recovery acknowledgement differs from its exact authority"
                        .into(),
                ));
            }
            Ok((ack, bytes, reference, prepared, true))
        }
        Err(super::storage::StorageError::NotFound(_)) => {
            let next_slot = storage
                .allocate_protocol_slot(
                    &context,
                    &ack_slot_prefix(&registration.device_id.to_string(), 2),
                    ".json",
                )
                .await
                .map_err(StoreObjectError::from)?;
            let ack = StoreAck::signed(
                root.store_root_hash,
                registration_ref.clone(),
                1,
                store_cut,
                device_state,
                None,
                match &registration.store_commits {
                    StoreCommitAnchor::MergeConcurrent { .. } => {
                        StoreAckExclusionState::MergeConcurrent {
                            proposal_freezes: Vec::new(),
                        }
                    }
                    StoreCommitAnchor::Serial => StoreAckExclusionState::Serial,
                },
                published_at.to_string(),
                SuccessorLink {
                    activation: registration
                        .store_acknowledgement_activation(registration_ref)
                        .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?
                        .activation_id(),
                    predecessor: None,
                    next_slot,
                },
                device_signer,
            )
            .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
            let bytes = ack.to_bytes();
            let prepared = storage
                .prepare_protocol_object(&context, first_slot, &prefix, bytes.clone())
                .map_err(StoreObjectError::from)?;
            let reference = StoreAckRef {
                registration: registration_ref.clone(),
                sequence: 1,
                ack_hash: ack.ack_hash(),
                object: prepared.reference().clone(),
            };
            Ok((ack, bytes, reference, prepared, false))
        }
        Err(error) => Err(StoreObjectError::from(error).into()),
    }
}

#[allow(clippy::too_many_arguments)]
async fn prepare_or_load_owner_recovery_node(
    storage: &dyn SyncStorage,
    root: &super::store_commit::StoreRootRef,
    recovery_slot: crate::storage::cloud::ObjectSlot,
    owner_pubkey: &str,
    owner_grant: &super::membership::MembershipGrantId,
    sequence: u64,
    recovery_id: DeviceRecoveryId,
    membership: &super::circle_control::StoreMembershipStateRef,
    predecessor: &Option<OwnerRecoveryNodeRef>,
    readiness: &DeviceRecoveryReadiness,
    identity_signer: &UserKeypair,
) -> Result<
    (
        OwnerRecoveryNode,
        OwnerRecoveryNodeRef,
        PreparedExactObject,
        bool,
    ),
    StoreRegistrationError,
> {
    let context = super::storage::ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::OwnerRecoveryNode,
    );
    let prefix = owner_recovery_semantic_prefix(owner_pubkey, owner_grant.clone(), sequence);
    match storage
        .read_prepared_protocol_slot(&context, &recovery_slot, &prefix)
        .await
    {
        Ok((bytes, prepared)) => {
            let object = prepared.reference();
            let decoded: OwnerRecoveryNode = serde_json::from_slice(&bytes)
                .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
            let reference = OwnerRecoveryNodeRef {
                owner_pubkey: decoded.owner_pubkey.clone(),
                owner_grant: decoded.owner_grant.clone(),
                sequence: decoded.sequence,
                node_hash: decoded.node_hash(),
                object: object.clone(),
            };
            let node = OwnerRecoveryNode::parse_at(&bytes, root, &reference)
                .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
            if node.recovery_id != recovery_id
                || node.owner_pubkey != owner_pubkey
                || node.owner_grant != *owner_grant
                || node.sequence != sequence
                || node.membership != *membership
                || node.predecessor != *predecessor
                || node.readiness != *readiness
                || node.next_slot == recovery_slot
            {
                return Err(StoreRegistrationError::Invalid(
                    "existing Owner recovery node differs from its exact authority".into(),
                ));
            }
            Ok((node, reference, prepared, true))
        }
        Err(super::storage::StorageError::NotFound(_)) => {
            let next_sequence = sequence.checked_add(1).ok_or_else(|| {
                StoreRegistrationError::Invalid("Owner recovery sequence overflow".into())
            })?;
            let next_slot = storage
                .allocate_protocol_slot(
                    &context,
                    &owner_recovery_semantic_prefix(
                        owner_pubkey,
                        owner_grant.clone(),
                        next_sequence,
                    ),
                    ".json",
                )
                .await
                .map_err(StoreObjectError::from)?;
            let node = OwnerRecoveryNode::signed(
                root.store_root_hash,
                recovery_id,
                owner_grant.clone(),
                sequence,
                membership.clone(),
                predecessor.clone(),
                readiness.clone(),
                next_slot,
                identity_signer,
            )
            .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
            let prepared = storage
                .prepare_protocol_object(&context, recovery_slot, &prefix, node.to_bytes())
                .map_err(StoreObjectError::from)?;
            let reference = OwnerRecoveryNodeRef {
                owner_pubkey: owner_pubkey.to_string(),
                owner_grant: owner_grant.clone(),
                sequence,
                node_hash: node.node_hash(),
                object: prepared.reference().clone(),
            };
            Ok((node, reference, prepared, false))
        }
        Err(error) => Err(StoreObjectError::from(error).into()),
    }
}

#[allow(clippy::too_many_arguments)]
async fn install_activated_owner_recovery(
    db: &Database,
    storage: &dyn SyncStorage,
    root: &super::store_commit::StoreRootRef,
    origin: &StoreDeviceRegistrationOrigin,
    device_id: super::store_commit::StoreDeviceId,
    recovery_id: DeviceRecoveryId,
    recovery_slot: &crate::storage::cloud::ObjectSlot,
    owner_pubkey: &str,
    owner_grant: &super::membership::MembershipGrantId,
    sequence: u64,
    predecessor: &Option<OwnerRecoveryNodeRef>,
) -> Result<Option<StoreDeviceRegistrationRef>, StoreRegistrationError> {
    let Some((registration_ref, registration, activation)) = db
        .activated_store_device_registration_for_device(device_id)
        .await
        .map_err(database_error)?
    else {
        return Ok(None);
    };
    let StoreDeviceRegistrationActivation::Recovery {
        recovery_id: activated_recovery_id,
        node: node_ref,
    } = activation.clone()
    else {
        return Err(StoreRegistrationError::Invalid(
            "derived Owner recovery device has a non-recovery activation".into(),
        ));
    };
    if registration.origin != *origin
        || registration.author_pubkey != owner_pubkey
        || activated_recovery_id != recovery_id
        || node_ref.owner_pubkey != owner_pubkey
        || node_ref.owner_grant != *owner_grant
        || node_ref.sequence != sequence
        || node_ref.object.slot() != recovery_slot
    {
        return Err(StoreRegistrationError::Invalid(
            "activated Owner recovery registration differs from the requested authority".into(),
        ));
    }
    let provider = storage
        .provider_binding()
        .await
        .map_err(StoreObjectError::from)?
        .device;
    if registration.provider != provider {
        return Err(StoreRegistrationError::Invalid(
            "activated Owner recovery registration belongs to another provider principal".into(),
        ));
    }
    let node = super::store_objects::load_owner_recovery_node_ref(storage, root, &node_ref).await?;
    if node.value.recovery_id != recovery_id
        || node.value.predecessor != *predecessor
        || node.value.readiness.registration != registration_ref
    {
        return Err(StoreRegistrationError::Invalid(
            "activated Owner recovery node differs from the requested authority".into(),
        ));
    }
    let initial_ack_ref = node.value.readiness.initial_ack;
    let initial_ack =
        super::store_objects::load_store_ack_ref(storage, root, &initial_ack_ref, &registration)
            .await?;
    if initial_ack.value.store_cut != node.value.readiness.bootstrap_cut {
        return Err(StoreRegistrationError::Invalid(
            "activated Owner recovery acknowledgement differs from its recovery node".into(),
        ));
    }

    let registration_context = super::storage::ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreDeviceRegistration,
    );
    let (registration_bytes, registration_prepared) = storage
        .read_prepared_protocol_slot(
            &registration_context,
            registration_ref.object.slot(),
            &registration_semantic_prefix(&registration_ref.device_id.to_string()),
        )
        .await
        .map_err(StoreObjectError::from)?;
    if registration_bytes != registration.to_bytes()
        || registration_prepared.reference() != &registration_ref.object
    {
        return Err(StoreRegistrationError::Invalid(
            "activated Owner recovery registration differs from its prepared exact object".into(),
        ));
    }
    let ack_context = super::storage::ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreAck,
    );
    let (initial_ack_bytes, initial_ack_prepared) = storage
        .read_prepared_protocol_slot(
            &ack_context,
            initial_ack_ref.object.slot(),
            &ack_slot_prefix(
                &registration_ref.device_id.to_string(),
                initial_ack_ref.sequence,
            ),
        )
        .await
        .map_err(StoreObjectError::from)?;
    if initial_ack_bytes != initial_ack.bytes
        || initial_ack_prepared.reference() != &initial_ack_ref.object
    {
        return Err(StoreRegistrationError::Invalid(
            "activated Owner recovery acknowledgement differs from its prepared exact object"
                .into(),
        ));
    }
    let already_activated = db
        .stage_owner_recovery_registration(
            crate::database::ExactProtocolObject {
                value: registration,
                bytes: registration_bytes,
                object: registration_prepared.reference().clone(),
                prepared: registration_prepared,
            },
            initial_ack_ref,
            crate::database::ExactProtocolObject {
                value: initial_ack.value,
                bytes: initial_ack_bytes,
                object: initial_ack_prepared.reference().clone(),
                prepared: initial_ack_prepared,
            },
            activation,
        )
        .await
        .map_err(database_error)?;
    if !already_activated {
        return Err(StoreRegistrationError::Invalid(
            "activated Owner recovery disappeared while installing its local journal".into(),
        ));
    }
    Ok(Some(registration_ref))
}

pub async fn recover_owner_device_merge(
    db: &Database,
    storage: &dyn SyncStorage,
    identity_signer: &UserKeypair,
    authority: &super::restore_code::OwnerRecoveryAuthority,
    membership: &MembershipChain,
) -> Result<StoreDeviceRegistrationRef, StoreRegistrationError> {
    let root = db
        .local_store_root_ref()
        .await
        .map_err(database_error)?
        .ok_or(StoreRegistrationError::ExactRootAuthorityMissing)?;
    let protocol = super::store_objects::load_store_protocol_root(storage, &root)
        .await?
        .value;
    let owner_pubkey = crate::keys::public_key_hex(identity_signer);
    if owner_pubkey != protocol.descriptor.founder_pubkey
        || authority.owner_grant != protocol.descriptor.founder_grant
        || membership.active_owner_grant(&owner_pubkey).as_ref() != Some(&authority.owner_grant)
        || authority.recovery.owner_grant != authority.owner_grant
    {
        return Err(StoreRegistrationError::Invalid(
            "Owner recovery authority differs from the active root founder grant".into(),
        ));
    }
    let super::store_commit::GrantStreamAnchor::OwnerRecovery { first_slot } =
        &protocol.descriptor.founder_recovery
    else {
        return Err(StoreRegistrationError::Invalid(
            "Store root has no founder recovery stream".into(),
        ));
    };
    let (recovery_slot, predecessor, sequence) = match &authority.recovery.position {
        OwnerRecoveryPosition::BeforeFirst { activation } => {
            let expected = super::store_commit::OwnerRecoveryActivationId::derive(
                &root,
                &owner_pubkey,
                &authority.owner_grant,
                &protocol.descriptor.founder_recovery,
            )
            .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
            if activation != &expected {
                return Err(StoreRegistrationError::Invalid(
                    "Owner recovery activation differs from the root anchor".into(),
                ));
            }
            (first_slot.clone(), None, 1)
        }
        OwnerRecoveryPosition::At { node } => {
            let loaded =
                super::store_objects::load_owner_recovery_node_ref(storage, &root, node).await?;
            if loaded.value.owner_pubkey != owner_pubkey
                || loaded.value.owner_grant != authority.owner_grant
            {
                return Err(StoreRegistrationError::Invalid(
                    "Owner recovery cursor belongs to another authority".into(),
                ));
            }
            (
                loaded.value.next_slot,
                Some(node.clone()),
                node.sequence.saturating_add(1),
            )
        }
    };
    let recovery_hash = ObjectHash::digest(
        &serde_json::to_vec(&(
            &root,
            &owner_pubkey,
            &authority.owner_grant,
            &authority.recovery,
        ))
        .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?,
    );
    let recovery_id = DeviceRecoveryId::from_hash(recovery_hash);
    let origin = StoreDeviceRegistrationOrigin::Recovery {
        recovery_id,
        recovery_slot: recovery_slot.clone(),
        owner_grant: authority.owner_grant.clone(),
    };
    let device_id = super::store_commit::StoreDeviceId::derive(&root, &origin);
    if let Some(registration) = install_activated_owner_recovery(
        db,
        storage,
        &root,
        &origin,
        device_id,
        recovery_id,
        &recovery_slot,
        &owner_pubkey,
        &authority.owner_grant,
        sequence,
        &predecessor,
    )
    .await?
    {
        return Ok(registration);
    }
    let context = |domain| {
        super::storage::ProtocolObjectContext::signed_plaintext(root.store_root_hash, domain)
    };
    let commit_context = context(ProtocolObjectDomain::StoreCommit);
    let staged = db
        .latest_local_store_device_registration()
        .await
        .map_err(database_error)?
        .filter(|durable| durable.device_id == device_id);
    let (
        registration,
        registration_ref,
        registration_prepared,
        registration_exists,
        initial_ack,
        initial_ack_bytes,
        initial_ack_ref,
        initial_ack_prepared,
        initial_ack_exists,
    ) = if let Some(durable) = staged {
        let registration = StoreDeviceRegistration::parse_at(
            &durable.registration_bytes,
            &root,
            durable.device_id,
        )
        .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
        if registration.origin != origin
            || registration.to_bytes() != durable.registration_bytes
            || registration.registration_hash() != durable.registration_hash
        {
            return Err(StoreRegistrationError::Invalid(
                "staged Owner recovery registration differs from its exact authority".into(),
            ));
        }
        let registration_ref = StoreDeviceRegistrationRef::from_registration(
            &registration,
            durable.prepared.reference().clone(),
        );
        let initial_ack = StoreAck::parse_at(
            &durable.initial_ack.bytes,
            &root,
            &durable.initial_ack_ref,
            &registration,
        )
        .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
        if initial_ack != durable.initial_ack.value
            || durable.initial_ack.object != *durable.initial_ack.prepared.reference()
        {
            return Err(StoreRegistrationError::Invalid(
                "staged Owner recovery acknowledgement differs from its exact authority".into(),
            ));
        }
        let registration_exists = prepared_protocol_object_exists(
            storage,
            &context(super::storage::ProtocolObjectDomain::StoreDeviceRegistration),
            &durable.prepared,
            &registration_semantic_prefix(&device_id.to_string()),
            &durable.registration_bytes,
        )
        .await?;
        let initial_ack_exists = prepared_protocol_object_exists(
            storage,
            &context(super::storage::ProtocolObjectDomain::StoreAck),
            &durable.initial_ack.prepared,
            &ack_slot_prefix(&device_id.to_string(), 1),
            &durable.initial_ack.bytes,
        )
        .await?;
        (
            registration,
            registration_ref,
            durable.prepared,
            registration_exists,
            initial_ack,
            durable.initial_ack.bytes,
            durable.initial_ack_ref,
            durable.initial_ack.prepared,
            initial_ack_exists,
        )
    } else {
        let head_context = context(super::storage::ProtocolObjectDomain::StoreHead);
        let ack_context = context(super::storage::ProtocolObjectDomain::StoreAck);
        let snapshot_context = context(super::storage::ProtocolObjectDomain::StoreSnapshotMeta);
        let registration_context =
            context(super::storage::ProtocolObjectDomain::StoreDeviceRegistration);
        let first_head = storage
            .allocate_protocol_slot(
                &head_context,
                &head_slot_prefix(&device_id.to_string(), 1),
                ".json",
            )
            .await
            .map_err(StoreObjectError::from)?;
        let first_ack = storage
            .allocate_protocol_slot(
                &ack_context,
                &ack_slot_prefix(&device_id.to_string(), 1),
                ".json",
            )
            .await
            .map_err(StoreObjectError::from)?;
        let first_snapshot = storage
            .allocate_protocol_slot(
                &snapshot_context,
                &snapshot_slot_prefix(&device_id.to_string(), 1),
                ".json",
            )
            .await
            .map_err(StoreObjectError::from)?;
        let registration_prefix = registration_semantic_prefix(&device_id.to_string());
        let registration_slot = storage
            .allocate_protocol_slot(&registration_context, &registration_prefix, ".json")
            .await
            .map_err(StoreObjectError::from)?;
        let provider = storage
            .provider_binding()
            .await
            .map_err(StoreObjectError::from)?
            .device;
        let expected_registration = StoreDeviceRegistration::signed(
            root.clone(),
            origin.clone(),
            provider,
            StoreCommitAnchor::MergeConcurrent {
                announcements: DeviceStreamAnchor::StoreAnnouncements {
                    first_slot: first_head,
                },
            },
            DeviceStreamAnchor::StoreAcknowledgements {
                first_slot: first_ack.clone(),
            },
            DeviceStreamAnchor::StoreSnapshots {
                first_slot: first_snapshot,
            },
            identity_signer,
        )
        .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
        let (registration, registration_ref, registration_prepared, registration_exists) =
            prepare_or_load_recovery_registration(
                storage,
                &root,
                expected_registration,
                registration_slot,
                &registration_prefix,
            )
            .await?;
        let dependencies = db
            .materialized_frontier()
            .await
            .map_err(database_error)?
            .into_iter()
            .map(|(stream, reference)| {
                stream
                    .parse()
                    .map(|stream| (stream, reference))
                    .map_err(|error| {
                        StoreRegistrationError::Invalid(format!(
                            "Owner recovery frontier stream {stream}: {error}"
                        ))
                    })
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        let device_signer = registration
            .device_signer(identity_signer)
            .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
        let bootstrap_cut = StoreHistoryCut::MergeConcurrent(dependencies);
        let (device_state, _) = db
            .store_device_state_for_history_cut(&bootstrap_cut)
            .await
            .map_err(database_error)?;
        let (
            initial_ack,
            initial_ack_bytes,
            initial_ack_ref,
            initial_ack_prepared,
            initial_ack_exists,
        ) = prepare_or_load_initial_recovery_ack(
            storage,
            &root,
            &registration,
            &registration_ref,
            first_ack,
            bootstrap_cut,
            device_state,
            &authority.published_at,
            &device_signer,
        )
        .await?;
        (
            registration,
            registration_ref,
            registration_prepared,
            registration_exists,
            initial_ack,
            initial_ack_bytes,
            initial_ack_ref,
            initial_ack_prepared,
            initial_ack_exists,
        )
    };
    let dependencies = match initial_ack.store_cut.clone() {
        StoreHistoryCut::MergeConcurrent(dependencies) => dependencies,
        StoreHistoryCut::Serial(_) => {
            return Err(StoreRegistrationError::Invalid(
                "Merge Owner recovery acknowledgement carries a Serial cut".into(),
            ));
        }
    };
    let device_signer = registration
        .device_signer(identity_signer)
        .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
    let bootstrap_cut = initial_ack.store_cut.clone();
    let super::membership::MembershipStatus::Resolved(resolved) = membership.status() else {
        return Err(StoreRegistrationError::Invalid(
            "Owner recovery requires resolved membership".into(),
        ));
    };
    let membership_state = super::circle_control::StoreMembershipStateRef::merge_concurrent(
        membership.head_refs().to_vec(),
        membership.resolution_refs().to_vec(),
        vec![authority.recovery.clone()],
        resolved.state_hash,
    )
    .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
    let readiness = DeviceRecoveryReadiness {
        registration: registration_ref.clone(),
        initial_ack: initial_ack_ref.clone(),
        bootstrap_cut: bootstrap_cut.clone(),
    };
    let (_node, node_ref, node_prepared, node_exists) = prepare_or_load_owner_recovery_node(
        storage,
        &root,
        recovery_slot,
        &owner_pubkey,
        &authority.owner_grant,
        sequence,
        recovery_id,
        &membership_state,
        &predecessor,
        &readiness,
        identity_signer,
    )
    .await?;
    let registration_activation = StoreDeviceRegistrationActivation::Recovery {
        recovery_id,
        node: node_ref.clone(),
    };
    let already_activated = db
        .stage_owner_recovery_registration(
            crate::database::ExactProtocolObject {
                value: registration.clone(),
                bytes: registration.to_bytes(),
                object: registration_prepared.reference().clone(),
                prepared: registration_prepared.clone(),
            },
            initial_ack_ref.clone(),
            crate::database::ExactProtocolObject {
                value: initial_ack.clone(),
                bytes: initial_ack.to_bytes(),
                object: initial_ack_prepared.reference().clone(),
                prepared: initial_ack_prepared.clone(),
            },
            registration_activation.clone(),
        )
        .await
        .map_err(database_error)?;
    if already_activated {
        return Ok(registration_ref);
    }
    if !registration_exists {
        storage
            .create_protocol_object(&registration_prepared)
            .await
            .map_err(StoreObjectError::from)?;
    }
    if !initial_ack_exists {
        storage
            .create_protocol_object(&initial_ack_prepared)
            .await
            .map_err(StoreObjectError::from)?;
    }
    if !node_exists {
        storage
            .create_protocol_object(&node_prepared)
            .await
            .map_err(StoreObjectError::from)?;
    }
    db.mark_local_store_device_registration_created(
        crate::database::ExactProtocolObject {
            value: registration.clone(),
            bytes: registration.to_bytes(),
            object: registration_prepared.reference().clone(),
            prepared: registration_prepared,
        },
        initial_ack_ref,
        crate::database::ExactProtocolObject {
            value: initial_ack,
            bytes: initial_ack_bytes,
            object: initial_ack_prepared.reference().clone(),
            prepared: initial_ack_prepared,
        },
    )
    .await
    .map_err(database_error)?;

    let stream_id = super::store_commit::StreamActivation::device_authorized_stream_id(
        root.store_root_hash,
        &registration_ref,
        super::store_commit::StreamAnchorDomain::StoreAnnouncements,
    );
    let order = StoreCommitOrder::MergeConcurrent {
        seq: 1,
        predecessor: None,
        dependencies,
    };
    let (device_state, _) = db
        .store_device_state_for_order(&order)
        .await
        .map_err(database_error)?;
    let coord = StoreCommitCoord::MergeConcurrent {
        stream_id,
        sequence: 1,
    };
    let activation_ref = ActivatedStoreDeviceRegistrationRef {
        registration: registration_ref.clone(),
        authority: StoreDeviceRegistrationActivationRef::Recovery {
            recovery_id,
            node: node_ref.clone(),
        },
    };
    let commit = StoreBatchCommit::signed_with_registrations(
        root.store_root_hash,
        crate::WriteId::from_generated(format!("owner-recovery-{recovery_hash}")),
        coord.clone(),
        registration_ref.clone(),
        &registration,
        order,
        membership_state,
        device_state,
        StoreOperationMembershipAuthority::MergeConcurrent {
            predecessor: membership
                .active_grant(&authority.owner_grant)
                .ok_or_else(|| {
                    StoreRegistrationError::Invalid(
                        "Owner recovery grant is absent from active membership".to_string(),
                    )
                })?
                .creation_authority
                .clone(),
        },
        vec![activation_ref],
        &device_signer,
    )
    .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
    let commit_prefix = commit_semantic_prefix(
        commit.candidate_family(),
        &stream_id.to_string(),
        1,
        commit.commit_hash(),
    );
    let commit_slot = storage
        .allocate_protocol_slot(&commit_context, &commit_prefix, ".json")
        .await
        .map_err(StoreObjectError::from)?;
    let commit_prepared = storage
        .prepare_protocol_object(
            &commit_context,
            commit_slot,
            &commit_prefix,
            commit.to_bytes(),
        )
        .map_err(StoreObjectError::from)?;
    let commit_ref =
        StoreBatchCommitRef::from_commit(&commit, coord, commit_prepared.reference().clone())
            .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
    let head_context = context(super::storage::ProtocolObjectDomain::StoreHead);
    let StoreCommitAnchor::MergeConcurrent {
        announcements: DeviceStreamAnchor::StoreAnnouncements { first_slot },
    } = &registration.store_commits
    else {
        return Err(StoreRegistrationError::Invalid(
            "Merge Owner recovery registration has no announcement stream anchor".into(),
        ));
    };
    let next_head = storage
        .allocate_protocol_slot(
            &head_context,
            &head_slot_prefix(&device_id.to_string(), 2),
            ".json",
        )
        .await
        .map_err(StoreObjectError::from)?;
    let head = StoreDeviceHead::signed(
        root.store_root_hash,
        registration_ref.clone(),
        commit_ref.clone(),
        SuccessorLink {
            activation: registration
                .store_announcement_activation(&registration_ref)
                .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?
                .activation_id(),
            predecessor: None,
            next_slot: next_head,
        },
        &device_signer,
    )
    .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
    let head_prepared = storage
        .prepare_protocol_object(
            &head_context,
            first_slot.clone(),
            &head_slot_prefix(&device_id.to_string(), 1),
            head.to_bytes(),
        )
        .map_err(StoreObjectError::from)?;
    storage
        .create_protocol_object(&commit_prepared)
        .await
        .map_err(StoreObjectError::from)?;
    storage
        .create_protocol_object(&head_prepared)
        .await
        .map_err(StoreObjectError::from)?;
    db.complete_merge_owner_recovery(commit, commit_ref, registration, registration_activation)
        .await
        .map_err(database_error)?;
    Ok(registration_ref)
}

pub async fn recover_owner_device_serial(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: &dyn CoordinationStorage,
    identity_signer: &UserKeypair,
    authority: &super::restore_code::OwnerRecoveryAuthority,
) -> Result<StoreDeviceRegistrationRef, StoreRegistrationError> {
    let root = db
        .local_store_root_ref()
        .await
        .map_err(database_error)?
        .ok_or(StoreRegistrationError::ExactRootAuthorityMissing)?;
    let protocol = super::store_objects::load_store_protocol_root(storage, &root)
        .await?
        .value;
    if protocol.descriptor.write_policy != crate::WritePolicy::Serial {
        return Err(StoreRegistrationError::Invalid(
            "Serial Owner recovery requires a Serial Store root".into(),
        ));
    }
    let owner_pubkey = crate::keys::public_key_hex(identity_signer);
    if owner_pubkey != protocol.descriptor.founder_pubkey
        || authority.owner_grant != protocol.descriptor.founder_grant
        || authority.recovery.owner_grant != authority.owner_grant
    {
        return Err(StoreRegistrationError::Invalid(
            "Owner recovery authority differs from the root founder grant".into(),
        ));
    }
    let super::store_commit::GrantStreamAnchor::OwnerRecovery { first_slot } =
        &protocol.descriptor.founder_recovery
    else {
        return Err(StoreRegistrationError::Invalid(
            "Store root has no founder recovery stream".into(),
        ));
    };
    let (recovery_slot, predecessor_node, recovery_sequence) = match &authority.recovery.position {
        OwnerRecoveryPosition::BeforeFirst { activation } => {
            let expected = super::store_commit::OwnerRecoveryActivationId::derive(
                &root,
                &owner_pubkey,
                &authority.owner_grant,
                &protocol.descriptor.founder_recovery,
            )
            .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
            if activation != &expected {
                return Err(StoreRegistrationError::Invalid(
                    "Owner recovery activation differs from the root anchor".into(),
                ));
            }
            (first_slot.clone(), None, 1)
        }
        OwnerRecoveryPosition::At { node } => {
            let loaded =
                super::store_objects::load_owner_recovery_node_ref(storage, &root, node).await?;
            if loaded.value.owner_pubkey != owner_pubkey
                || loaded.value.owner_grant != authority.owner_grant
            {
                return Err(StoreRegistrationError::Invalid(
                    "Owner recovery cursor belongs to another authority".into(),
                ));
            }
            let recovery_sequence = node.sequence.checked_add(1).ok_or_else(|| {
                StoreRegistrationError::Invalid("Owner recovery sequence overflow".into())
            })?;
            (
                loaded.value.next_slot,
                Some(node.clone()),
                recovery_sequence,
            )
        }
    };
    let recovery_hash = ObjectHash::digest(
        &serde_json::to_vec(&(
            &root,
            &owner_pubkey,
            &authority.owner_grant,
            &authority.recovery,
        ))
        .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?,
    );
    let recovery_id = DeviceRecoveryId::from_hash(recovery_hash);
    let origin = StoreDeviceRegistrationOrigin::Recovery {
        recovery_id,
        recovery_slot: recovery_slot.clone(),
        owner_grant: authority.owner_grant.clone(),
    };
    let device_id = super::store_commit::StoreDeviceId::derive(&root, &origin);
    if let Some(registration) = install_activated_owner_recovery(
        db,
        storage,
        &root,
        &origin,
        device_id,
        recovery_id,
        &recovery_slot,
        &owner_pubkey,
        &authority.owner_grant,
        recovery_sequence,
        &predecessor_node,
    )
    .await?
    {
        return Ok(registration);
    }
    let context = |domain| {
        super::storage::ProtocolObjectContext::signed_plaintext(root.store_root_hash, domain)
    };
    let ack_context = context(ProtocolObjectDomain::StoreAck);
    let snapshot_context = context(super::storage::ProtocolObjectDomain::StoreSnapshotMeta);
    let registration_context =
        context(super::storage::ProtocolObjectDomain::StoreDeviceRegistration);
    let commit_context = context(super::storage::ProtocolObjectDomain::StoreCommit);
    let first_ack = storage
        .allocate_protocol_slot(
            &ack_context,
            &ack_slot_prefix(&device_id.to_string(), 1),
            ".json",
        )
        .await
        .map_err(StoreObjectError::from)?;
    let first_snapshot = storage
        .allocate_protocol_slot(
            &snapshot_context,
            &snapshot_slot_prefix(&device_id.to_string(), 1),
            ".json",
        )
        .await
        .map_err(StoreObjectError::from)?;
    let registration_prefix = registration_semantic_prefix(&device_id.to_string());
    let registration_slot = storage
        .allocate_protocol_slot(&registration_context, &registration_prefix, ".json")
        .await
        .map_err(StoreObjectError::from)?;
    let provider = storage
        .provider_binding()
        .await
        .map_err(StoreObjectError::from)?
        .device;
    let expected_registration = StoreDeviceRegistration::signed(
        root.clone(),
        origin,
        provider,
        StoreCommitAnchor::Serial,
        DeviceStreamAnchor::StoreAcknowledgements {
            first_slot: first_ack.clone(),
        },
        DeviceStreamAnchor::StoreSnapshots {
            first_slot: first_snapshot,
        },
        identity_signer,
    )
    .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
    let (registration, registration_ref, registration_prepared, registration_exists) =
        prepare_or_load_recovery_registration(
            storage,
            &root,
            expected_registration,
            registration_slot,
            &registration_prefix,
        )
        .await?;
    let snapshot =
        super::store_outbound::current_serial_authorization_snapshot(db, storage, coordination)
            .await?;
    if !snapshot
        .authorization
        .membership
        .authorizes_owner_grant_id(&owner_pubkey, &authority.owner_grant)
    {
        return Err(StoreRegistrationError::Invalid(
            "Owner recovery grant is not active at the Serial head".into(),
        ));
    }
    let founder = super::store_objects::load_founder_registration(storage, &root).await?;
    let founder_ref =
        StoreDeviceRegistrationRef::from_registration(&founder.value, founder.object.clone());
    let serial_predecessor = snapshot.base.clone().map_or_else(
        || StoreSerialPredecessor::Genesis {
            root: root.clone(),
            founder_registration: founder_ref,
        },
        StoreSerialPredecessor::Commit,
    );
    let serial_sequence = snapshot.base.as_ref().map_or(Ok(1), |reference| {
        reference
            .coord
            .sequence()
            .checked_add(1)
            .ok_or_else(|| StoreRegistrationError::Invalid("Serial sequence overflow".into()))
    })?;
    let order = StoreCommitOrder::Serial {
        seq: serial_sequence,
        predecessor: serial_predecessor.clone(),
    };
    let (device_state, resolved_devices) = db
        .store_device_state_for_order(&order)
        .await
        .map_err(database_error)?;
    if !resolved_devices.recovery.contains(&authority.recovery) {
        return Err(StoreRegistrationError::Invalid(
            "Owner recovery cursor differs from the exact Serial device state".into(),
        ));
    }
    let membership_state = super::circle_control::StoreMembershipStateRef::serial(
        serial_predecessor.clone(),
        resolved_devices.recovery,
        &snapshot.authorization,
    )
    .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
    let bootstrap_cut = StoreHistoryCut::Serial(serial_predecessor);
    let device_signer = registration
        .device_signer(identity_signer)
        .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
    let (initial_ack, initial_ack_bytes, initial_ack_ref, initial_ack_prepared, initial_ack_exists) =
        prepare_or_load_initial_recovery_ack(
            storage,
            &root,
            &registration,
            &registration_ref,
            first_ack,
            bootstrap_cut.clone(),
            device_state.clone(),
            &authority.published_at,
            &device_signer,
        )
        .await?;
    let bootstrap_cut = initial_ack.store_cut.clone();
    let readiness = DeviceRecoveryReadiness {
        registration: registration_ref.clone(),
        initial_ack: initial_ack_ref.clone(),
        bootstrap_cut,
    };
    let (_node, node_ref, node_prepared, node_exists) = prepare_or_load_owner_recovery_node(
        storage,
        &root,
        recovery_slot,
        &owner_pubkey,
        &authority.owner_grant,
        recovery_sequence,
        recovery_id,
        &membership_state,
        &predecessor_node,
        &readiness,
        identity_signer,
    )
    .await?;
    let registration_activation = StoreDeviceRegistrationActivation::Recovery {
        recovery_id,
        node: node_ref.clone(),
    };
    let already_activated = db
        .stage_owner_recovery_registration(
            crate::database::ExactProtocolObject {
                value: registration.clone(),
                bytes: registration.to_bytes(),
                object: registration_prepared.reference().clone(),
                prepared: registration_prepared.clone(),
            },
            initial_ack_ref.clone(),
            crate::database::ExactProtocolObject {
                value: initial_ack.clone(),
                bytes: initial_ack_bytes.clone(),
                object: initial_ack_prepared.reference().clone(),
                prepared: initial_ack_prepared.clone(),
            },
            registration_activation.clone(),
        )
        .await
        .map_err(database_error)?;
    if already_activated {
        return Ok(registration_ref);
    }
    for (exists, prepared) in [
        (registration_exists, &registration_prepared),
        (initial_ack_exists, &initial_ack_prepared),
        (node_exists, &node_prepared),
    ] {
        if !exists {
            storage
                .create_protocol_object(prepared)
                .await
                .map_err(StoreObjectError::from)?;
        }
    }
    db.mark_local_store_device_registration_created(
        crate::database::ExactProtocolObject {
            value: registration.clone(),
            bytes: registration.to_bytes(),
            object: registration_prepared.reference().clone(),
            prepared: registration_prepared,
        },
        initial_ack_ref,
        crate::database::ExactProtocolObject {
            value: initial_ack,
            bytes: initial_ack_bytes,
            object: initial_ack_prepared.reference().clone(),
            prepared: initial_ack_prepared,
        },
    )
    .await
    .map_err(database_error)?;
    let activation = ActivatedStoreDeviceRegistrationRef {
        registration: registration_ref.clone(),
        authority: StoreDeviceRegistrationActivationRef::Recovery {
            recovery_id,
            node: node_ref.clone(),
        },
    };
    let coord = StoreCommitCoord::Serial {
        sequence: serial_sequence,
    };
    let commit = StoreBatchCommit::signed_with_serial_recovery(
        root.store_root_hash,
        crate::WriteId::from_generated(format!("owner-recovery-{recovery_hash}")),
        coord.clone(),
        registration_ref.clone(),
        &registration,
        order,
        membership_state,
        device_state,
        SerialRecoveryActivation {
            registration: activation,
        },
        &device_signer,
    )
    .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
    let commit_prefix = commit_semantic_prefix(
        commit.candidate_family(),
        SERIAL_STREAM_ID,
        serial_sequence,
        commit.commit_hash(),
    );
    let commit_slot = storage
        .allocate_protocol_slot(&commit_context, &commit_prefix, ".json")
        .await
        .map_err(StoreObjectError::from)?;
    let commit_prepared = storage
        .prepare_protocol_object(
            &commit_context,
            commit_slot,
            &commit_prefix,
            commit.to_bytes(),
        )
        .map_err(StoreObjectError::from)?;
    let commit_ref =
        StoreBatchCommitRef::from_commit(&commit, coord, commit_prepared.reference().clone())
            .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
    let authorization_after = snapshot
        .authorization
        .authorize_and_apply(&commit_ref, &commit, &registration)
        .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
    let head = StoreSerialHead::signed(
        root.store_root_hash,
        StoreSerialHeadState::Commit {
            author_registration: registration_ref.clone(),
            commit: commit_ref.clone(),
        },
        &device_signer,
    )
    .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
    super::store_outbound::activate_serial_commit_head(
        db,
        storage,
        coordination,
        &snapshot.base_head,
        &commit,
        &commit_prepared,
        &commit_ref,
        &head,
    )
    .await?;
    db.complete_serial_owner_recovery(
        commit,
        commit_ref,
        registration,
        registration_activation,
        authorization_after,
    )
    .await
    .map_err(database_error)?;
    Ok(registration_ref)
}

pub(crate) async fn bootstrap_pending_device(
    db: &Database,
    storage: &dyn SyncStorage,
    identity_signer: &UserKeypair,
    attempt_ref: DeviceJoinAttemptRef,
    verified_attempt: super::store_objects::VerifiedObject<DeviceJoinAttempt>,
    bootstrap_plan: super::store_pull::DeviceJoinBootstrapPlan,
    attempt_activation: StoreBatchCommitRef,
    owner: &StoreDeviceRegistration,
    published_at: &str,
) -> Result<DeviceReadinessProof, StoreRegistrationError> {
    if verified_attempt.semantic_hash != attempt_ref.attempt_hash
        || verified_attempt.object != attempt_ref.object
    {
        return Err(StoreRegistrationError::Invalid(
            "verified device join attempt differs from its exact reference".to_string(),
        ));
    }
    let attempt = verified_attempt.value;
    let activation_stream = match attempt_activation.coord {
        StoreCommitCoord::MergeConcurrent { stream_id, .. } => stream_id.to_string(),
        StoreCommitCoord::Serial { .. } => SERIAL_STREAM_ID.to_string(),
    };
    Box::pin(db.install_device_join_bootstrap(attempt.store_root.clone(), bootstrap_plan))
        .await
        .map_err(database_error)?;
    if Box::pin(db.exact_materialized_ref(&activation_stream, attempt_activation.coord.sequence()))
        .await
        .map_err(database_error)?
        .as_ref()
        != Some(&attempt_activation)
    {
        return Err(StoreRegistrationError::ActivationRequired);
    }
    let (activation_commit, activation_author) =
        Box::pin(super::store_pull::load_commit_with_author(
            storage,
            &attempt.store_root,
            &attempt_activation,
        ))
        .await?;
    if activation_author != *owner
        || activation_commit.author_registration != attempt.owner_registration
        || !activation_commit
            .device_join_attempt_decisions()
            .iter()
            .any(|decision| {
                matches!(
                    decision,
                    DeviceJoinAttemptDecisionRef::Attempt(reference)
                        if reference == &attempt_ref
                )
            })
        || activation_commit
            .order
            .predecessor_cut()
            .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?
            != attempt.bootstrap_cut
        || activation_commit.membership_state != attempt.membership
    {
        return Err(StoreRegistrationError::Invalid(
            "device join attempt is not activated by the named exact Store commit".to_string(),
        ));
    }
    let provider = Box::pin(storage.provider_binding())
        .await
        .map_err(StoreObjectError::from)?;
    if provider.device != attempt.expected_registration.provider {
        return Err(StoreRegistrationError::Invalid(
            "joiner provider principal differs from the signed device join attempt".to_string(),
        ));
    }
    let expected_registration = attempt.expected_registration.clone();
    if expected_registration.author_pubkey != crate::keys::public_key_hex(identity_signer) {
        return Err(StoreRegistrationError::Invalid(
            "joiner identity differs from the signed device registration request".to_string(),
        ));
    }
    let existing = Box::pin(db.latest_local_store_device_registration())
        .await
        .map_err(database_error)?;
    if let Some(existing) = existing.as_ref() {
        if existing.registration_bytes != expected_registration.to_bytes()
            || existing.prepared.reference().slot() != &attempt.registration_slot
            || existing.initial_ack.value.store_cut != attempt.bootstrap_cut
        {
            return Err(StoreRegistrationError::Invalid(
                "local join journal owns different exact registration bytes".to_string(),
            ));
        }
    } else {
        let registration_prepared = prepare_registration_object(
            storage,
            &expected_registration,
            attempt.registration_slot.clone(),
        )?;
        let registration_ref = super::store_commit::StoreDeviceRegistrationRef::from_registration(
            &expected_registration,
            registration_prepared.reference().clone(),
        );
        let DeviceStreamAnchor::StoreAcknowledgements { first_slot } =
            &expected_registration.acknowledgements
        else {
            return Err(StoreRegistrationError::Invalid(
                "join registration has no acknowledgement anchor".to_string(),
            ));
        };
        let ack_context = super::storage::ProtocolObjectContext::signed_plaintext(
            attempt.store_root.store_root_hash,
            ProtocolObjectDomain::StoreAck,
        );
        let next_slot = Box::pin(storage.allocate_protocol_slot(
            &ack_context,
            &ack_slot_prefix(&expected_registration.device_id.to_string(), 2),
            ".json",
        ))
        .await
        .map_err(StoreObjectError::from)?;
        let device_signer = expected_registration
            .device_signer(identity_signer)
            .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
        let (device_state, _) =
            Box::pin(db.store_device_state_for_history_cut(&attempt.bootstrap_cut))
                .await
                .map_err(database_error)?;
        let initial_ack = StoreAck::signed(
            attempt.store_root.store_root_hash,
            registration_ref.clone(),
            1,
            attempt.bootstrap_cut.clone(),
            device_state,
            None,
            match &expected_registration.store_commits {
                StoreCommitAnchor::MergeConcurrent { .. } => {
                    StoreAckExclusionState::MergeConcurrent {
                        proposal_freezes: Vec::new(),
                    }
                }
                StoreCommitAnchor::Serial => StoreAckExclusionState::Serial,
            },
            published_at.to_string(),
            SuccessorLink {
                activation: expected_registration
                    .store_acknowledgement_activation(&registration_ref)
                    .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?
                    .activation_id(),
                predecessor: None,
                next_slot,
            },
            &device_signer,
        )
        .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
        let ack_prepared = storage
            .prepare_protocol_object(
                &ack_context,
                first_slot.clone(),
                &ack_slot_prefix(&expected_registration.device_id.to_string(), 1),
                initial_ack.to_bytes(),
            )
            .map_err(StoreObjectError::from)?;
        let initial_ack_ref = StoreAckRef {
            registration: registration_ref,
            sequence: 1,
            ack_hash: initial_ack.ack_hash(),
            object: ack_prepared.reference().clone(),
        };
        Box::pin(db.stage_local_store_device_registration(
            crate::database::ExactProtocolObject {
                value: expected_registration.clone(),
                bytes: expected_registration.to_bytes(),
                object: registration_prepared.reference().clone(),
                prepared: registration_prepared,
            },
            initial_ack_ref,
            crate::database::ExactProtocolObject {
                value: initial_ack.clone(),
                bytes: initial_ack.to_bytes(),
                object: ack_prepared.reference().clone(),
                prepared: ack_prepared,
            },
        ))
        .await
        .map_err(database_error)?;
    }
    Box::pin(drain_registration_outbox(
        db,
        storage,
        None,
        identity_signer,
        None,
        published_at,
    ))
    .await?;
    let durable = Box::pin(db.latest_local_store_device_registration())
        .await
        .map_err(database_error)?
        .ok_or(StoreRegistrationError::ActivationRequired)?;
    if !matches!(
        durable.state,
        crate::database::LocalDeviceRegistrationState::Created
            | crate::database::LocalDeviceRegistrationState::Activated { .. }
    ) {
        return Err(StoreRegistrationError::ActivationRequired);
    }
    let registration = StoreDeviceRegistration::parse_at(
        &durable.registration_bytes,
        &attempt.store_root,
        durable.device_id,
    )
    .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
    let registration_ref = super::store_commit::StoreDeviceRegistrationRef::from_registration(
        &registration,
        durable.prepared.reference().clone(),
    );
    let device_signer = registration
        .device_signer(identity_signer)
        .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
    DeviceReadinessProof::signed(
        attempt_ref,
        registration_ref,
        durable.initial_ack_ref,
        attempt.bootstrap_cut,
        &registration,
        &device_signer,
    )
    .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))
}

pub async fn drain_registration_outbox(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn CoordinationStorage>,
    signer: &UserKeypair,
    membership: Option<&MembershipChain>,
    published_at: &str,
) -> Result<u64, StoreRegistrationError> {
    let _ = (coordination, signer, membership, published_at);
    let store_root = db
        .local_store_root_ref()
        .await
        .map_err(database_error)?
        .ok_or(StoreRegistrationError::ExactRootAuthorityMissing)?;
    let mut published = 0_u64;
    while let Some(outbound) = db
        .oldest_unpublished_store_device_registration()
        .await
        .map_err(database_error)?
    {
        let registration = StoreDeviceRegistration::parse_at(
            &outbound.registration_bytes,
            &store_root,
            outbound.device_id,
        )
        .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
        if registration.registration_hash() != outbound.registration_hash {
            return Err(StoreRegistrationError::Invalid(
                "durable registration columns differ from its exact signed bytes".to_string(),
            ));
        }
        let context = super::storage::ProtocolObjectContext::signed_plaintext(
            store_root.store_root_hash,
            ProtocolObjectDomain::StoreDeviceRegistration,
        );
        let semantic_prefix = registration_semantic_prefix(&outbound.device_id.to_string());
        storage
            .create_protocol_object(&outbound.prepared)
            .await
            .map_err(StoreObjectError::from)?;
        let opened = storage
            .read_protocol_object(&context, outbound.prepared.reference(), &semantic_prefix)
            .await
            .map_err(StoreObjectError::from)?;
        if opened != outbound.registration_bytes {
            return Err(StoreRegistrationError::Invalid(
                "Store registration exact readback differs from its durable bytes".to_string(),
            ));
        }
        let ack_context = super::storage::ProtocolObjectContext::signed_plaintext(
            store_root.store_root_hash,
            ProtocolObjectDomain::StoreAck,
        );
        storage
            .create_protocol_object(&outbound.initial_ack.prepared)
            .await
            .map_err(StoreObjectError::from)?;
        let opened_ack = storage
            .read_protocol_object(
                &ack_context,
                &outbound.initial_ack_ref.object,
                &ack_slot_prefix(&outbound.device_id.to_string(), 1),
            )
            .await
            .map_err(StoreObjectError::from)?;
        if opened_ack != outbound.initial_ack.bytes {
            return Err(StoreRegistrationError::Invalid(
                "Store initial acknowledgement exact readback differs from its durable bytes"
                    .to_string(),
            ));
        }
        db.mark_local_store_device_registration_created(
            crate::database::ExactProtocolObject {
                value: registration,
                bytes: outbound.registration_bytes,
                object: outbound.prepared.reference().clone(),
                prepared: outbound.prepared,
            },
            outbound.initial_ack_ref,
            outbound.initial_ack,
        )
        .await
        .map_err(database_error)?;
        published = published.checked_add(1).ok_or_else(|| {
            StoreRegistrationError::Database("registration publish count exceeded u64".to_string())
        })?;
    }
    Ok(published)
}

async fn require_activated_registration(
    db: &Database,
    storage: &dyn SyncStorage,
    durable: &crate::database::DurableDeviceRegistration,
) -> Result<(), StoreRegistrationError> {
    let root_ref = db
        .local_store_root_ref()
        .await
        .map_err(database_error)?
        .ok_or(StoreRegistrationError::ExactRootAuthorityMissing)?;
    let registration = StoreDeviceRegistration::parse_at(
        &durable.registration_bytes,
        &root_ref,
        durable.device_id,
    )
    .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
    if registration.registration_hash() != durable.registration_hash {
        return Err(StoreRegistrationError::Invalid(
            "local registration differs from its durable hash".to_string(),
        ));
    }
    let exact_ref = super::store_commit::StoreDeviceRegistrationRef::from_registration(
        &registration,
        durable.prepared.reference().clone(),
    );
    let crate::database::LocalDeviceRegistrationState::Activated { authority } = &durable.state
    else {
        return Err(StoreRegistrationError::ActivationRequired);
    };
    let activated = db
        .activated_store_device_registration_with_authority(exact_ref)
        .await
        .map_err(database_error)?;
    if activated != (registration.clone(), authority.clone()) {
        return Err(StoreRegistrationError::Invalid(
            "local registration differs from its exact activation authority".to_string(),
        ));
    }
    let live_provider = storage
        .provider_binding()
        .await
        .map_err(StoreObjectError::from)?;
    if live_provider.device != registration.provider {
        return Err(StoreRegistrationError::Invalid(
            "live provider principal differs from the founder registration".to_string(),
        ));
    }
    Ok(())
}

fn database_error(error: crate::database::DbError) -> StoreRegistrationError {
    StoreRegistrationError::Database(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::test_helpers::{open_serial_test_db, open_test_db, TestStore};

    fn founder_recovery_authority(
        store: &TestStore,
    ) -> super::super::restore_code::OwnerRecoveryAuthority {
        let owner_grant = store.protocol_root.descriptor.founder_grant.clone();
        let activation = super::super::store_commit::OwnerRecoveryActivationId::derive(
            &store.root,
            &crate::keys::public_key_hex(&store.signer),
            &owner_grant,
            &store.protocol_root.descriptor.founder_recovery,
        )
        .expect("derive founder recovery activation");
        super::super::restore_code::OwnerRecoveryAuthority {
            owner_identity_secret: hex::encode(store.signer.to_keypair_bytes()),
            owner_grant: owner_grant.clone(),
            recovery: super::super::store_commit::OwnerRecoveryCursor {
                owner_grant,
                position: OwnerRecoveryPosition::BeforeFirst { activation },
            },
            published_at: "2026-07-17T00:00:00Z".to_string(),
        }
    }

    async fn initialized() -> (TestStore, Database) {
        let signer = UserKeypair::generate();
        let db = open_test_db();
        let store = TestStore::create(&db, "registration-store-test", signer)
            .await
            .expect("create exact registration test Store");
        (store, db)
    }

    #[tokio::test]
    async fn store_root_state_failures_keep_registration_error_variants() {
        let db = open_test_db();
        let signer = UserKeypair::generate();
        let store = TestStore::for_store("registration-missing-root-storage").await;

        assert!(matches!(
            drain_registration_outbox(
                &db,
                &store.storage,
                None,
                &signer,
                None,
                "2026-01-01T00:00:00Z",
            )
            .await,
            Err(StoreRegistrationError::ExactRootAuthorityMissing)
        ));
    }

    #[tokio::test]
    async fn exact_founder_registration_is_already_activated() {
        let (store, db) = initialized().await;
        ensure_active_registration(&db, &store.storage, &store.signer)
            .await
            .expect("founder registration remains active");
        let activated = db.activated_store_device_registrations().await.unwrap();
        assert_eq!(activated.len(), 1);
        assert_eq!(activated[0].store_root, store.root);
    }

    #[tokio::test]
    async fn merge_owner_recovery_publishes_and_activates_replacement_device() {
        let (store, db) = initialized().await;
        let membership = super::super::pull::load_cycle_membership(&store.storage, &db)
            .await
            .expect("load exact membership")
            .chain
            .expect("resolved founder membership");
        let authority = founder_recovery_authority(&store);
        let registration =
            recover_owner_device_merge(&db, &store.storage, &store.signer, &authority, &membership)
                .await
                .expect("recover MergeConcurrent Owner device");

        let durable = db
            .latest_local_store_device_registration()
            .await
            .expect("load replacement registration")
            .expect("replacement registration exists");
        assert_eq!(durable.device_id, registration.device_id);
        assert!(durable.is_activated());
        ensure_active_registration(&db, &store.storage, &store.signer)
            .await
            .expect("replacement registration is usable");
    }

    #[tokio::test]
    async fn recovery_materialization_surfaces_a_corrupt_activated_registration() {
        let (store, db) = initialized().await;
        let membership = super::super::pull::load_cycle_membership(&store.storage, &db)
            .await
            .expect("load exact membership")
            .chain
            .expect("resolved founder membership");
        let authority = founder_recovery_authority(&store);
        let registration =
            recover_owner_device_merge(&db, &store.storage, &store.signer, &authority, &membership)
                .await
                .expect("recover MergeConcurrent Owner device");
        let mut recovered_commit = None;
        for reference in db
            .materialized_frontier()
            .await
            .expect("load materialized Store frontier")
            .into_values()
        {
            let (commit, _) = super::super::store_pull::load_commit_with_author(
                &store.storage,
                &store.root,
                &reference,
            )
            .await
            .expect("load materialized recovery commit");
            if commit.author_registration == registration {
                recovered_commit = Some((commit, reference));
                break;
            }
        }
        let (commit, reference) = recovered_commit.expect("recovery commit is materialized");
        let device_id = registration.device_id.to_string();
        let registration_hash = registration.registration_hash.to_string();
        db.call(move |conn| {
            conn.execute(
                "UPDATE store_device_registration_activations
                 SET registration_bytes = X'00'
                 WHERE device_id = ?1 AND registration_hash = ?2",
                (&device_id, &registration_hash),
            )
            .map_err(crate::database::DbError::from)?;
            Ok(())
        })
        .await
        .expect("corrupt activated recovery registration fixture");

        let error = db
            .call(move |conn| {
                crate::database::Database::record_materialized_commit_on(conn, &commit, &reference)
            })
            .await
            .expect_err("recovery materialization must surface corrupt registration bytes");
        assert!(
            error.to_string().contains("activated Store registration"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn merge_owner_recovery_retry_reuses_each_published_readiness_prefix() {
        for failed_call in [2, 3, 4] {
            let signer = UserKeypair::generate();
            let db = open_test_db();
            let store =
                TestStore::create(&db, &format!("merge-recovery-prefix-{failed_call}"), signer)
                    .await
                    .expect("create recovery prefix Store");
            let membership = super::super::pull::load_cycle_membership(&store.storage, &db)
                .await
                .expect("load exact membership")
                .chain
                .expect("resolved founder membership");
            let authority = founder_recovery_authority(&store);
            store.home.fail_exact_create_before_call(failed_call);
            assert!(
                recover_owner_device_merge(
                    &db,
                    &store.storage,
                    &store.signer,
                    &authority,
                    &membership,
                )
                .await
                .is_err(),
                "failure before exact create {failed_call} interrupts recovery",
            );

            let interrupted = db
                .latest_local_store_device_registration()
                .await
                .expect("read interrupted recovery journal")
                .expect("interrupted recovery journal exists");
            let interrupted_node = if failed_call == 4 {
                let registration = StoreDeviceRegistration::parse_at(
                    &interrupted.registration_bytes,
                    &store.root,
                    interrupted.device_id,
                )
                .expect("parse interrupted recovery registration");
                let StoreDeviceRegistrationOrigin::Recovery { recovery_slot, .. } =
                    registration.origin
                else {
                    panic!("interrupted registration is not a Recovery registration");
                };
                let context = super::super::storage::ProtocolObjectContext::signed_plaintext(
                    store.root.store_root_hash,
                    ProtocolObjectDomain::OwnerRecoveryNode,
                );
                Some(
                    store
                        .storage
                        .read_prepared_protocol_slot(
                            &context,
                            &recovery_slot,
                            &owner_recovery_semantic_prefix(
                                &crate::keys::public_key_hex(&store.signer),
                                authority.owner_grant.clone(),
                                1,
                            ),
                        )
                        .await
                        .expect("read published recovery node")
                        .1,
                )
            } else {
                None
            };

            recover_owner_device_merge(&db, &store.storage, &store.signer, &authority, &membership)
                .await
                .expect("retry completes absent recovery suffix");
            assert_eq!(
                store.home.exact_create_count(),
                6,
                "retry after boundary {failed_call} creates only the absent suffix",
            );
            let completed = db
                .latest_local_store_device_registration()
                .await
                .expect("read completed recovery journal")
                .expect("completed recovery journal exists");
            assert_eq!(
                completed.prepared.reference(),
                interrupted.prepared.reference(),
            );
            assert_eq!(
                completed.prepared.stored_bytes(),
                interrupted.prepared.stored_bytes(),
            );
            if failed_call >= 3 {
                assert_eq!(
                    completed.initial_ack.prepared.reference(),
                    interrupted.initial_ack.prepared.reference(),
                );
                assert_eq!(
                    completed.initial_ack.prepared.stored_bytes(),
                    interrupted.initial_ack.prepared.stored_bytes(),
                );
            }
            if let Some(interrupted_node) = interrupted_node {
                let registration = StoreDeviceRegistration::parse_at(
                    &completed.registration_bytes,
                    &store.root,
                    completed.device_id,
                )
                .expect("parse completed recovery registration");
                let StoreDeviceRegistrationOrigin::Recovery { recovery_slot, .. } =
                    registration.origin
                else {
                    panic!("completed registration is not a Recovery registration");
                };
                let context = super::super::storage::ProtocolObjectContext::signed_plaintext(
                    store.root.store_root_hash,
                    ProtocolObjectDomain::OwnerRecoveryNode,
                );
                let completed_node = store
                    .storage
                    .read_prepared_protocol_slot(
                        &context,
                        &recovery_slot,
                        &owner_recovery_semantic_prefix(
                            &crate::keys::public_key_hex(&store.signer),
                            authority.owner_grant.clone(),
                            1,
                        ),
                    )
                    .await
                    .expect("read completed recovery node")
                    .1;
                assert_eq!(completed_node.reference(), interrupted_node.reference());
                assert_eq!(
                    completed_node.stored_bytes(),
                    interrupted_node.stored_bytes(),
                );
            }
        }
    }

    #[tokio::test]
    async fn serial_owner_recovery_head_is_accepted_from_its_closed_activation() {
        let signer = UserKeypair::generate();
        let db = open_serial_test_db();
        let store = TestStore::create(&db, "serial-recovery-store", signer)
            .await
            .expect("create Serial recovery Store");
        let authority = founder_recovery_authority(&store);
        let coordination = store
            .storage
            .serial_coordination()
            .expect("Serial test coordination");
        let registration = recover_owner_device_serial(
            &db,
            &store.storage,
            coordination,
            &store.signer,
            &authority,
        )
        .await
        .expect("recover Serial Owner device");

        let observed = super::super::store_pull::load_serial_cycle_authorization(
            &store.storage,
            coordination,
            &store.root,
        )
        .await
        .expect("verify Serial recovery head");
        assert_eq!(
            observed.head.as_ref().map(|head| head.coord.sequence()),
            Some(1)
        );
        let durable = db
            .latest_local_store_device_registration()
            .await
            .expect("load replacement registration")
            .expect("replacement registration exists");
        assert_eq!(durable.device_id, registration.device_id);
        assert!(durable.is_activated());
    }

    #[tokio::test]
    async fn failed_retirement_create_retries_the_owned_exact_graph() {
        let (store, db) = initialized().await;
        let membership = super::super::pull::load_cycle_membership(&store.storage, &db)
            .await
            .expect("load exact membership");
        store.home.fail_exact_create_before_call(1);
        assert!(retire_registration_with_coordination(
            &db,
            &store.storage,
            None,
            &store.signer,
            membership.chain.as_ref(),
            "2026-07-16T00:00:00Z",
        )
        .await
        .is_err());
        let pending = db
            .local_store_device_retirement()
            .await
            .unwrap()
            .expect("retirement graph remains durably owned");
        assert!(!pending.1);
        let owned = pending.0;

        retire_registration_with_coordination(
            &db,
            &store.storage,
            None,
            &store.signer,
            membership.chain.as_ref(),
            "2026-07-16T00:00:00Z",
        )
        .await
        .expect("retry publishes the owned retirement graph");
        let published = db.local_store_device_retirement().await.unwrap().unwrap();
        assert!(published.1);
        assert_eq!(published.0, owned);
        assert!(db
            .latest_local_store_device_registration()
            .await
            .unwrap()
            .unwrap()
            .is_retired());
    }

    #[tokio::test]
    async fn retirement_is_idempotent_and_prevents_reactivation() {
        let (store, db) = initialized().await;
        let membership = super::super::pull::load_cycle_membership(&store.storage, &db)
            .await
            .expect("load exact membership");
        for _ in 0..2 {
            assert!(retire_registration_with_coordination(
                &db,
                &store.storage,
                None,
                &store.signer,
                membership.chain.as_ref(),
                "2026-07-16T00:00:00Z",
            )
            .await
            .expect("retirement is idempotent"));
        }
        assert!(matches!(
            ensure_active_registration(&db, &store.storage, &store.signer).await,
            Err(StoreRegistrationError::RetiredDevice { .. })
        ));
        assert!(db.local_store_device_retirement().await.unwrap().unwrap().1);
    }
}

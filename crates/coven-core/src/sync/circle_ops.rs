//! Durable creation and activation of circles through the Store commit stream.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::circle::{
    circle_control_head_prefix, circle_metadata_head_prefix, circle_roster_head_prefix,
    circle_semantic_prefix, CircleCreation, CircleCreationPolicyObjects, CircleId,
    CircleMetadataHeadRef, CircleOperationState, CircleRosterHeadRef, CircleSemanticSlot,
    StoreMembershipStateRef,
};
use super::membership::SerialAuthorizationState;
use super::storage::{
    ExactObjectRef, PreparedExactObject, ProtocolObjectContext, ProtocolObjectDomain, SyncStorage,
    VersionedObject,
};
use super::store_commit::{
    circle_access_envelope_semantic_prefix, circle_access_leaf_semantic_prefix,
    commit_semantic_prefix, head_slot_prefix, CandidateFamilyId, CircleAccessEnvelopeObjectRef,
    CircleAccessLeafObjectRef, CircleAccessObjectRef, CircleActivationObjects, CircleHeadObjectRef,
    CircleMetadataObjectRef, ObjectHash, StoreBatchCommit, StoreBatchCommitRef, StoreCommitCoord,
    StoreCommitOperationsInput, StoreCommitOrder, StoreDeviceHead, StoreDeviceRegistration,
    StoreDeviceRegistrationRef, StoreRootRef, StoreSerialHead, StoreSerialHeadState,
    StoreSerialPredecessor, StreamActivationId, SuccessorLink, SERIAL_STREAM_ID,
};
use crate::database::Database;
use crate::encryption::{EncryptionService, MasterKeyring};
use crate::keys::{self, UserKeypair};

pub(crate) use super::circle_activation::{
    load_circle_activations, load_exact_slot_bytes, verify_control_context,
    verify_local_circle_activation, VerifiedCircleReference,
};
#[cfg(test)]
pub(crate) use super::circle_activation::{VerifiedCircleAccess, VerifiedCircleActive};

#[cfg(test)]
use super::circle::{CircleAccessDisposition, CircleRole};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum CircleOperationPolicy {
    MergeConcurrent {
        head: StoreDeviceHead,
    },
    Serial {
        head: StoreSerialHead,
        base: Option<StoreBatchCommitRef>,
        base_head: VersionedObject,
        authorization: SerialAuthorizationState,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CircleOperationJournal {
    pub operation_id: String,
    pub status: CircleOperationState,
    pub creation: CircleCreation,
    pub commit_bytes: Vec<u8>,
    pub commit_ref: StoreBatchCommitRef,
    pub prepared_objects: BTreeMap<String, PreparedExactObject>,
    pub policy: CircleOperationPolicy,
    pub uploaded: BTreeSet<String>,
}

impl CircleOperationJournal {
    pub(crate) fn circle_id(&self) -> CircleId {
        self.creation.circle_id
    }

    pub(crate) fn commit(&self) -> Result<StoreBatchCommit, CircleOperationError> {
        serde_json::from_slice(&self.commit_bytes)
            .map_err(|error| CircleOperationError::Journal(format!("parse Store commit: {error}")))
    }
}

fn verify_prepared_objects_are_signed(
    journal: &CircleOperationJournal,
    reference: &super::store_commit::CircleControlRef,
) -> Result<(), CircleOperationError> {
    let objects = reference.objects();
    let mut signed = BTreeSet::<ExactObjectRef>::from([
        journal.commit_ref.object.clone(),
        objects.control.clone(),
    ]);
    signed.extend(objects.control_head.iter().map(|head| head.object.clone()));
    signed.extend(objects.roster_entries.values().cloned());
    signed.extend(
        objects
            .roster_heads
            .values()
            .map(|head| head.object.clone()),
    );
    signed.extend(objects.roster_resolutions.values().cloned());
    signed.extend(
        objects
            .metadata_entries
            .values()
            .map(|metadata| metadata.object.clone()),
    );
    signed.extend(
        objects
            .metadata_heads
            .values()
            .map(|head| head.object.clone()),
    );
    for access in &objects.access {
        signed.insert(access.leaf.object.clone());
        signed.insert(access.envelope.object.clone());
    }
    for (step, prepared) in &journal.prepared_objects {
        let is_merge_head = step == "store-head"
            && matches!(
                journal.policy,
                CircleOperationPolicy::MergeConcurrent { .. }
            );
        if !is_merge_head && !signed.contains(prepared.reference()) {
            return Err(CircleOperationError::Journal(format!(
                "Circle upload step {step:?} names an object outside its signed Store commit graph"
            )));
        }
    }
    Ok(())
}

fn signed_circle_commit(
    store_root_hash: ObjectHash,
    operation_id: crate::WriteId,
    coord: StoreCommitCoord,
    author_registration: StoreDeviceRegistrationRef,
    author: &StoreDeviceRegistration,
    order: StoreCommitOrder,
    membership_state: StoreMembershipStateRef,
    device_state: super::store_commit::StoreDeviceStateRef,
    membership_authority: Option<super::membership::MembershipGrantCreationAuthority>,
    creation: &CircleCreation,
    objects: CircleActivationObjects,
    device_signer: &UserKeypair,
) -> Result<StoreBatchCommit, CircleOperationError> {
    StoreBatchCommit::signed_operations(
        store_root_hash,
        operation_id,
        coord,
        author_registration,
        author,
        order,
        membership_state,
        device_state,
        membership_authority,
        StoreCommitOperationsInput {
            acknowledgement: None,
            control: None,
            device_join_attempts: Vec::new(),
            device_join_outcomes: Vec::new(),
            device_join_abandonments: Vec::new(),
            device_join_cleanup_receipts: Vec::new(),
            provider_access_grants: Vec::new(),
            provider_access_withdrawals: Vec::new(),
            device_registrations: Vec::new(),
            device_exclusion_proposals: Vec::new(),
            device_exclusion_outcomes: Vec::new(),
            circle_controls: vec![creation.control_ref(objects)],
            store_package: None,
            circle_packages: &[],
        },
        device_signer,
    )
    .map_err(|error| CircleOperationError::InvalidState(error.to_string()))
}

async fn prepare_circle_object(
    storage: &dyn SyncStorage,
    context: &ProtocolObjectContext,
    semantic_prefix: &str,
    extension: &str,
    bytes: Vec<u8>,
) -> Result<PreparedExactObject, CircleOperationError> {
    let slot = storage
        .allocate_protocol_slot(context, semantic_prefix, extension)
        .await
        .map_err(super::store_objects::StoreObjectError::from)?;
    storage
        .prepare_protocol_object(context, slot, semantic_prefix, bytes)
        .map_err(super::store_objects::StoreObjectError::from)
        .map_err(CircleOperationError::from)
}

async fn prepare_circle_head(
    storage: &dyn SyncStorage,
    context: &ProtocolObjectContext,
    semantic_prefix: &str,
    next_semantic_prefix: &str,
    bytes: Vec<u8>,
    activation: StreamActivationId,
) -> Result<(PreparedExactObject, CircleHeadObjectRef), CircleOperationError> {
    let prepared = prepare_circle_object(storage, context, semantic_prefix, ".json", bytes).await?;
    let next_slot = storage
        .allocate_protocol_slot(context, next_semantic_prefix, ".json")
        .await
        .map_err(super::store_objects::StoreObjectError::from)?;
    let reference = CircleHeadObjectRef {
        object: prepared.reference().clone(),
        successor: SuccessorLink {
            activation,
            predecessor: None,
            next_slot,
        },
    };
    Ok((prepared, reference))
}

async fn prepare_circle_activation_objects(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    creation: &CircleCreation,
    candidate_family: CandidateFamilyId,
) -> Result<
    (
        CircleActivationObjects,
        BTreeMap<String, PreparedExactObject>,
    ),
    CircleOperationError,
> {
    let store_root_hash = root.store_root_hash;
    let encryption = EncryptionService::from(
        MasterKeyring::from_serialized(&creation.keyring)
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?,
    );
    let metadata_encryption = encryption
        .service_for_fingerprint(creation.metadata.key_fingerprint.as_bytes())
        .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
    let metadata_context = ProtocolObjectContext::circle(
        store_root_hash,
        ProtocolObjectDomain::CircleMetadata,
        metadata_encryption,
    );
    let metadata_coord = creation.metadata.coord();
    let metadata_prefix = circle_semantic_prefix(CircleSemanticSlot::MetadataEntry {
        circle_id: creation.circle_id,
        coord: &metadata_coord,
    });
    let metadata = prepare_circle_object(
        storage,
        &metadata_context,
        &metadata_prefix,
        ".json",
        serde_json::to_vec(&creation.metadata).expect("Circle metadata serialization cannot fail"),
    )
    .await?;
    let control_context = ProtocolObjectContext::store_encrypted(
        store_root_hash,
        ProtocolObjectDomain::CircleControl,
    );
    let control_prefix = circle_semantic_prefix(CircleSemanticSlot::Control {
        circle_id: creation.circle_id,
        control: &creation.control.coord,
    });
    let control = prepare_circle_object(
        storage,
        &control_context,
        &control_prefix,
        ".json",
        creation.control.bytes.clone(),
    )
    .await?;

    let mut prepared = BTreeMap::from([
        ("metadata".to_string(), metadata.clone()),
        ("control".to_string(), control.clone()),
    ]);
    let mut roster_entries = BTreeMap::new();
    let mut roster_heads = BTreeMap::new();
    let mut metadata_heads = BTreeMap::new();
    let mut control_head = None;
    if let CircleCreationPolicyObjects::MergeConcurrent {
        roster_entry,
        roster_head,
        metadata_head,
        control_head: value,
    } = &creation.policy_objects
    {
        let metadata_head_ref = CircleMetadataHeadRef::from_head(metadata_head);
        let metadata_head_prefix = circle_semantic_prefix(CircleSemanticSlot::MetadataHead {
            circle_id: creation.circle_id,
            head: &metadata_head_ref,
        });
        let metadata_stream = metadata_head_ref.coord.stream_key();
        let (metadata_head_prepared, metadata_head_object) = prepare_circle_head(
            storage,
            &metadata_context,
            &metadata_head_prefix,
            &circle_metadata_head_prefix(
                creation.circle_id,
                &metadata_stream,
                metadata_head_ref.coord.seq + 1,
            ),
            serde_json::to_vec(metadata_head)
                .expect("Circle metadata head serialization cannot fail"),
            StreamActivationId::circle_stream(
                root,
                creation.circle_id,
                "metadata-head",
                &metadata_stream,
            ),
        )
        .await?;
        prepared.insert("metadata-head".to_string(), metadata_head_prepared);
        metadata_heads.insert(metadata_head_ref, metadata_head_object);

        let roster_context = ProtocolObjectContext::circle(
            store_root_hash,
            ProtocolObjectDomain::CircleRoster,
            encryption,
        );
        let roster_coord = roster_entry.coord();
        let roster_prefix = circle_semantic_prefix(CircleSemanticSlot::RosterEntry {
            circle_id: creation.circle_id,
            coord: &roster_coord,
        });
        let roster_entry_prepared = prepare_circle_object(
            storage,
            &roster_context,
            &roster_prefix,
            ".json",
            serde_json::to_vec(roster_entry)
                .expect("Circle roster entry serialization cannot fail"),
        )
        .await?;
        prepared.insert("roster-entry".to_string(), roster_entry_prepared.clone());
        roster_entries.insert(roster_coord, roster_entry_prepared.reference().clone());

        let roster_head_ref = CircleRosterHeadRef::from_head(roster_head);
        let roster_head_prefix = circle_semantic_prefix(CircleSemanticSlot::RosterHead {
            circle_id: creation.circle_id,
            head: &roster_head_ref,
        });
        let roster_stream = roster_head_ref.coord.stream_key();
        let (roster_head_prepared, roster_head_object) = prepare_circle_head(
            storage,
            &roster_context,
            &roster_head_prefix,
            &circle_roster_head_prefix(
                creation.circle_id,
                &roster_stream,
                roster_head_ref.coord.seq + 1,
            ),
            serde_json::to_vec(roster_head).expect("Circle roster head serialization cannot fail"),
            StreamActivationId::circle_stream(
                root,
                creation.circle_id,
                "roster-head",
                &roster_stream,
            ),
        )
        .await?;
        prepared.insert("roster-head".to_string(), roster_head_prepared);
        roster_heads.insert(roster_head_ref, roster_head_object);

        let control_head_ref = super::circle::CircleControlCoord::stream_key(&value.control)
            .ok_or_else(|| {
                CircleOperationError::InvalidState(
                    "Merge Circle control head has a Serial coordinate".to_string(),
                )
            })?;
        let control_head_prefix = circle_semantic_prefix(CircleSemanticSlot::ControlHead {
            circle_id: creation.circle_id,
            control: &value.control,
            head_hash: value.head_hash(),
        });
        let control_seq = match &value.control {
            super::circle::CircleControlCoord::MergeConcurrent { seq, .. } => *seq,
            super::circle::CircleControlCoord::Serial { .. } => unreachable!(),
        };
        let (control_head_prepared, control_head_object) = prepare_circle_head(
            storage,
            &control_context,
            &control_head_prefix,
            &circle_control_head_prefix(creation.circle_id, &control_head_ref, control_seq + 1),
            serde_json::to_vec(value).expect("Circle control head serialization cannot fail"),
            StreamActivationId::circle_stream(
                root,
                creation.circle_id,
                "control-head",
                &control_head_ref,
            ),
        )
        .await?;
        prepared.insert("control-head".to_string(), control_head_prepared);
        control_head = Some(control_head_object);
    }

    let mut access_objects = Vec::with_capacity(creation.access.len());
    for (index, access) in creation.access.iter().enumerate() {
        let leaf_prefix = circle_access_leaf_semantic_prefix(
            access.leaf.value.circle_id,
            candidate_family,
            &access.leaf.value.owner_pubkey,
            access.leaf.value.epoch_id,
            &access.leaf.value.recipient_slot,
            access.leaf.value.leaf_id,
        );
        let leaf = prepare_circle_object(
            storage,
            &ProtocolObjectContext::recipient_sealed(
                store_root_hash,
                ProtocolObjectDomain::CircleAccessLeaf,
            ),
            &leaf_prefix,
            "",
            access.leaf.bytes.clone(),
        )
        .await?;
        prepared.insert(format!("access-leaf-{index}"), leaf.clone());
        let leaf_reference = CircleAccessLeafObjectRef {
            owner_pubkey: access.leaf.value.owner_pubkey.clone(),
            epoch_id: access.leaf.value.epoch_id,
            recipient_slot: access.leaf.value.recipient_slot.clone(),
            leaf_id: access.leaf.value.leaf_id,
            leaf_hash: access.leaf.leaf_hash,
            object: leaf.reference().clone(),
        };

        let envelope_prefix = circle_access_envelope_semantic_prefix(
            access.envelope.circle_id,
            candidate_family,
            &access.envelope.owner_pubkey,
            &access.envelope.recipient_slot,
            access.envelope.control_hash,
        );
        let envelope = prepare_circle_object(
            storage,
            &ProtocolObjectContext::store_encrypted(
                store_root_hash,
                ProtocolObjectDomain::CircleAccessEnvelope,
            ),
            &envelope_prefix,
            ".json",
            serde_json::to_vec(&access.envelope)
                .expect("Circle access envelope serialization cannot fail"),
        )
        .await?;
        prepared.insert(format!("access-envelope-{index}"), envelope.clone());
        let envelope_reference = CircleAccessEnvelopeObjectRef {
            owner_pubkey: access.envelope.owner_pubkey.clone(),
            recipient_slot: access.envelope.recipient_slot.clone(),
            control_hash: access.envelope.control_hash,
            leaf_id: access.envelope.leaf_id,
            leaf_hash: access.envelope.leaf_hash,
            object: envelope.reference().clone(),
        };
        access_objects.push(CircleAccessObjectRef {
            leaf: leaf_reference,
            envelope: envelope_reference,
        });
    }

    Ok((
        CircleActivationObjects {
            control: control.reference().clone(),
            control_head,
            roster_entries,
            roster_heads,
            roster_resolutions: BTreeMap::new(),
            metadata_entries: BTreeMap::from([(
                metadata_coord,
                CircleMetadataObjectRef {
                    key_fingerprint: creation.metadata.key_fingerprint,
                    object: metadata.reference().clone(),
                },
            )]),
            metadata_heads,
            access: access_objects,
        },
        prepared,
    ))
}

#[derive(Debug, thiserror::Error)]
pub enum CircleOperationError {
    #[error("database: {0}")]
    Database(String),
    #[error("circle protocol state is absent: {0}")]
    MissingState(&'static str),
    #[error("circle protocol state is invalid: {0}")]
    InvalidState(String),
    #[error("circle construction: {0}")]
    Construction(#[from] super::circle::CircleCreateError),
    #[error("circle object: {0}")]
    Object(#[from] super::store_objects::StoreObjectError),
    #[error("Store publication: {0}")]
    StoreOutbound(#[from] super::store_outbound::StoreOutboundError),
    #[error("Store device registration: {0}")]
    StoreRegistration(#[from] super::store_registration::StoreRegistrationError),
    #[error("circles require opaque cloud storage")]
    BrowsableStorage,
    #[error("circle operation journal: {0}")]
    Journal(String),
    #[error("circle operation {circle_id} is blocked: {reason}")]
    Blocked { circle_id: CircleId, reason: String },
    #[error("circle command channel is closed")]
    CommandChannelClosed,
    #[error("circle command ended without a reply")]
    ReplyChannelClosed,
}

impl From<crate::database::DbError> for CircleOperationError {
    fn from(error: crate::database::DbError) -> Self {
        Self::Database(error.into_message())
    }
}

pub(crate) async fn create_circle(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn super::storage::CoordinationStorage>,
    device_id: &str,
    metadata_stamp: &str,
    name: &str,
    signer: &UserKeypair,
) -> Result<CircleId, CircleOperationError> {
    super::store_registration::ensure_active_registration_with_coordination(
        db,
        storage,
        coordination,
        signer,
        None,
        metadata_stamp,
    )
    .await?;
    let journal = prepare_circle_operation(
        db,
        storage,
        coordination,
        device_id,
        metadata_stamp,
        name,
        signer,
    )
    .await?;
    let circle_id = journal.circle_id();
    db.insert_circle_operation(journal).await?;
    publish_circle_operation(db, storage, coordination, circle_id, signer).await?;
    Ok(circle_id)
}

pub(crate) async fn resume_circle_operations(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn super::storage::CoordinationStorage>,
    identity: &UserKeypair,
) -> Result<(), CircleOperationError> {
    while let Some(journal) = db.oldest_pending_circle_operation().await? {
        if !matches!(journal.status, CircleOperationState::Pending) {
            return Err(CircleOperationError::Journal(format!(
                "pending circle operation {} contains a blocked payload",
                journal.circle_id()
            )));
        }
        match publish_circle_operation(db, storage, coordination, journal.circle_id(), identity)
            .await
        {
            Ok(()) | Err(CircleOperationError::Blocked { .. }) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

async fn prepare_circle_operation(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn super::storage::CoordinationStorage>,
    device_id: &str,
    metadata_stamp: &str,
    name: &str,
    signer: &UserKeypair,
) -> Result<CircleOperationJournal, CircleOperationError> {
    let (root, author_registration, author, device_signer) =
        super::store_outbound::load_local_store_authority(db, device_id, signer).await?;
    let store_root_hash = root.store_root_hash;
    let circle_device_id = author.device_id.to_string();
    let founder = db
        .get_protocol_state(super::membership_ops::OWNER_PUBKEY_STATE_KEY)
        .await?
        .ok_or(CircleOperationError::MissingState("Store founder"))?;
    let author_pubkey = keys::public_key_hex(signer);
    let operation_id = db.new_write_id();
    let (creation, commit, commit_ref, policy, prepared_objects) = match db.write_policy() {
        crate::WritePolicy::MergeConcurrent => {
            let current =
                super::membership_ops::load_and_persist_owner_anchor(storage, &root, &founder, db)
                    .await
                    .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
            let heads = current.head_refs().to_vec();
            let resolutions = current.resolution_refs().to_vec();
            let exact = super::membership_ops::load_anchored_chain_at_exact_heads(
                storage,
                &root,
                &founder,
                &heads,
                &resolutions,
            )
            .await
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
            let members = exact.current_members();
            let state_hash = match exact.status() {
                super::membership::MembershipStatus::Resolved(resolved) => resolved.state_hash,
                super::membership::MembershipStatus::Conflict(_) => {
                    return Err(CircleOperationError::InvalidState(
                        "circle creation requires resolved Store membership".to_string(),
                    ));
                }
            };
            let membership_authority =
                exact.write_grant_authority(&author_pubkey).ok_or_else(|| {
                    CircleOperationError::InvalidState(
                        "circle creator is not a current Store writer".to_string(),
                    )
                })?;
            let base = db.latest_local_store_position().await?;
            let seq = base
                .as_ref()
                .map_or(1, |reference| reference.coord.sequence() + 1);
            let stream_id = super::causal_grants::AuthorStreamId::store_announcements(
                &root,
                &author_registration,
            );
            let dependencies = super::store_commit::CommitFrontier::from_refs(
                crate::WritePolicy::MergeConcurrent,
                db.materialized_frontier().await?,
            )
            .and_then(|frontier| frontier.merge_commits().cloned())
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
            let coord = StoreCommitCoord::MergeConcurrent {
                stream_id,
                sequence: seq,
            };
            let order = StoreCommitOrder::MergeConcurrent {
                seq,
                predecessor: base.clone(),
                dependencies,
            };
            let (device_state, resolved_devices) = db.store_device_state_for_order(&order).await?;
            let membership_state = StoreMembershipStateRef::merge_concurrent(
                heads,
                resolutions,
                resolved_devices.recovery,
                state_hash,
            )
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
            let candidate_family = CandidateFamilyId::derive(
                store_root_hash,
                &author_registration,
                &operation_id,
                &order,
            );
            let creation = CircleCreation::founder(
                store_root_hash,
                candidate_family,
                &circle_device_id,
                name,
                metadata_stamp,
                membership_state.clone(),
                Some(membership_authority.clone()),
                members,
                db.id_provider(),
                signer,
            )?;
            let (objects, mut prepared_objects) =
                prepare_circle_activation_objects(storage, &root, &creation, candidate_family)
                    .await?;
            let commit = signed_circle_commit(
                store_root_hash,
                operation_id.clone(),
                coord.clone(),
                author_registration.clone(),
                &author,
                order,
                membership_state,
                device_state,
                Some(membership_authority),
                &creation,
                objects,
                &device_signer,
            )?;
            let commit_context = ProtocolObjectContext::signed_plaintext(
                store_root_hash,
                ProtocolObjectDomain::StoreCommit,
            );
            let commit_prefix = commit_semantic_prefix(
                commit.candidate_family(),
                &stream_id.to_string(),
                seq,
                commit.commit_hash(),
            );
            let commit_prepared = prepare_circle_object(
                storage,
                &commit_context,
                &commit_prefix,
                ".json",
                commit.to_bytes(),
            )
            .await?;
            let commit_ref = StoreBatchCommitRef::from_commit(
                &commit,
                coord,
                commit_prepared.reference().clone(),
            )
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
            prepared_objects.insert("store-commit".to_string(), commit_prepared);
            let head_context = ProtocolObjectContext::signed_plaintext(
                store_root_hash,
                ProtocolObjectDomain::StoreHead,
            );
            let device_id = author_registration.device_id.to_string();
            let head_prefix = head_slot_prefix(&device_id, seq);
            let (head_slot, predecessor_head) =
                super::store_outbound::exact_next_announcement_slot(
                    storage,
                    &root,
                    &author_registration,
                    &author,
                    base.as_ref(),
                )
                .await?;
            let next_head_slot = storage
                .allocate_protocol_slot(
                    &head_context,
                    &head_slot_prefix(&device_id, seq + 1),
                    ".json",
                )
                .await
                .map_err(super::store_objects::StoreObjectError::from)?;
            let head = StoreDeviceHead::signed(
                store_root_hash,
                author_registration.clone(),
                commit_ref.clone(),
                SuccessorLink {
                    activation: StreamActivationId::store_announcements(
                        &root,
                        &author_registration,
                    ),
                    predecessor: predecessor_head.map(|reference| reference.object),
                    next_slot: next_head_slot,
                },
                &device_signer,
            )
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
            let head_prepared = storage
                .prepare_protocol_object(&head_context, head_slot, &head_prefix, head.to_bytes())
                .map_err(super::store_objects::StoreObjectError::from)?;
            prepared_objects.insert("store-head".to_string(), head_prepared);
            (
                creation,
                commit,
                commit_ref,
                CircleOperationPolicy::MergeConcurrent { head },
                prepared_objects,
            )
        }
        crate::WritePolicy::Serial => {
            let coordination = coordination.ok_or_else(|| {
                CircleOperationError::InvalidState(
                    "Serial circle creation requires coordination storage".to_string(),
                )
            })?;
            let snapshot = super::store_outbound::current_serial_authorization_snapshot(
                db,
                storage,
                coordination,
            )
            .await?;
            if !snapshot.authorization.membership.can_write(&author_pubkey) {
                return Err(CircleOperationError::InvalidState(
                    "circle creator is not a current Store writer".to_string(),
                ));
            }
            let base = snapshot.base;
            let members = snapshot.authorization.membership.current_members();
            let seq = base
                .as_ref()
                .map_or(1, |reference| reference.coord.sequence() + 1);
            let coord = StoreCommitCoord::Serial { sequence: seq };
            let predecessor = match base.clone() {
                Some(reference) => StoreSerialPredecessor::Commit(reference),
                None => StoreSerialPredecessor::Genesis {
                    root: root.clone(),
                    founder_registration: author_registration.clone(),
                },
            };
            let order = StoreCommitOrder::Serial {
                seq,
                predecessor: predecessor.clone(),
            };
            let (device_state, resolved_devices) = db.store_device_state_for_order(&order).await?;
            let membership_state = StoreMembershipStateRef::serial(
                predecessor,
                resolved_devices.recovery,
                &snapshot.authorization,
            )
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
            let candidate_family = CandidateFamilyId::derive(
                store_root_hash,
                &author_registration,
                &operation_id,
                &order,
            );
            let creation = CircleCreation::founder(
                store_root_hash,
                candidate_family,
                &circle_device_id,
                name,
                metadata_stamp,
                membership_state.clone(),
                None,
                members,
                db.id_provider(),
                signer,
            )?;
            let (objects, mut prepared_objects) =
                prepare_circle_activation_objects(storage, &root, &creation, candidate_family)
                    .await?;
            let commit = signed_circle_commit(
                store_root_hash,
                operation_id.clone(),
                coord.clone(),
                author_registration.clone(),
                &author,
                order,
                membership_state,
                device_state,
                None,
                &creation,
                objects,
                &device_signer,
            )?;
            let commit_context = ProtocolObjectContext::signed_plaintext(
                store_root_hash,
                ProtocolObjectDomain::StoreCommit,
            );
            let commit_prefix = commit_semantic_prefix(
                commit.candidate_family(),
                SERIAL_STREAM_ID,
                seq,
                commit.commit_hash(),
            );
            let commit_prepared = prepare_circle_object(
                storage,
                &commit_context,
                &commit_prefix,
                ".json",
                commit.to_bytes(),
            )
            .await?;
            let commit_ref = StoreBatchCommitRef::from_commit(
                &commit,
                coord,
                commit_prepared.reference().clone(),
            )
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
            prepared_objects.insert("store-commit".to_string(), commit_prepared);
            let head = StoreSerialHead::signed(
                store_root_hash,
                StoreSerialHeadState::Commit {
                    author_registration: author_registration.clone(),
                    commit: commit_ref.clone(),
                },
                &device_signer,
            )
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
            (
                creation,
                commit,
                commit_ref,
                CircleOperationPolicy::Serial {
                    head,
                    base,
                    base_head: snapshot.base_head,
                    authorization: snapshot.authorization,
                },
                prepared_objects,
            )
        }
    };
    Ok(CircleOperationJournal {
        operation_id: operation_id.as_str().to_string(),
        status: CircleOperationState::Pending,
        creation,
        commit_bytes: commit.to_bytes(),
        commit_ref,
        prepared_objects,
        policy,
        uploaded: BTreeSet::new(),
    })
}

async fn publish_circle_operation(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn super::storage::CoordinationStorage>,
    circle_id: CircleId,
    identity: &UserKeypair,
) -> Result<(), CircleOperationError> {
    let mut journal = db
        .circle_operation(circle_id)
        .await?
        .ok_or_else(|| CircleOperationError::Journal(format!("circle {circle_id} is absent")))?;
    if let CircleOperationState::Blocked { reason } = &journal.status {
        return Err(CircleOperationError::Blocked {
            circle_id,
            reason: reason.clone(),
        });
    }
    let creation = journal.creation.clone();
    let store_root_hash = creation.control.value.store_root_hash;
    let circle_encryption = EncryptionService::from(
        MasterKeyring::from_serialized(&creation.keyring)
            .map_err(|error| CircleOperationError::Journal(format!("circle keyring: {error}")))?,
    );
    let commit = journal.commit()?;
    let author = db
        .activated_store_device_registration(commit.author_registration.clone())
        .await?;
    let reference = commit.circle_controls();
    let [reference] = reference else {
        return Err(CircleOperationError::InvalidState(
            "Circle operation commit must activate one control".to_string(),
        ));
    };
    verify_control_context(
        reference,
        &creation.control,
        &journal.commit_ref,
        &commit,
        &author,
    )?;
    verify_prepared_objects_are_signed(&journal, reference)?;
    if creation.access.iter().any(|access| {
        !access.leaf.verify_envelope(
            &creation.control,
            &access.envelope,
            commit.candidate_family(),
        )
    }) {
        return Err(CircleOperationError::InvalidState(
            "prepared Circle access bytes, plaintext hash, ciphertext hash, or envelope differ"
                .to_string(),
        ));
    }
    if commit.policy() == crate::WritePolicy::MergeConcurrent
        && !has_current_merge_authority(db, storage, &commit, &author).await?
    {
        let reason = "circle operation author is not a current Store writer under its exact grant"
            .to_string();
        db.block_circle_operation(circle_id, reason.clone()).await?;
        return Err(CircleOperationError::Blocked { circle_id, reason });
    }
    if let CircleOperationPolicy::MergeConcurrent { head } = &journal.policy {
        let root = db
            .local_store_root_ref()
            .await?
            .ok_or(CircleOperationError::MissingState("Store root reference"))?;
        let (expected_slot, predecessor_head) =
            super::store_outbound::exact_next_announcement_slot(
                storage,
                &root,
                &commit.author_registration,
                &author,
                commit.order.predecessor(),
            )
            .await?;
        let prepared_head = journal.prepared_objects.get("store-head").ok_or_else(|| {
            CircleOperationError::Journal(
                "Merge Circle operation lacks its prepared Store head".to_string(),
            )
        })?;
        StoreDeviceHead::parse_at(
            &head.to_bytes(),
            store_root_hash,
            &author,
            &journal.commit_ref,
        )
        .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        if prepared_head.reference().slot() != &expected_slot
            || head.successor.activation
                != StreamActivationId::store_announcements(&root, &commit.author_registration)
            || head.successor.predecessor != predecessor_head.map(|reference| reference.object)
        {
            return Err(CircleOperationError::Journal(
                "Merge Circle Store head differs from its reserved successor slot".to_string(),
            ));
        }
    }

    let metadata_encryption = circle_encryption
        .service_for_fingerprint(creation.metadata.key_fingerprint.as_bytes())
        .map_err(|error| {
            CircleOperationError::InvalidState(format!(
                "Circle metadata key fingerprint is absent from its keyring: {error}"
            ))
        })?;
    append_step(
        db,
        storage,
        &mut journal,
        "metadata",
        &ProtocolObjectContext::circle(
            store_root_hash,
            ProtocolObjectDomain::CircleMetadata,
            metadata_encryption,
        ),
        &circle_semantic_prefix(CircleSemanticSlot::MetadataEntry {
            circle_id: creation.circle_id,
            coord: &creation.metadata.coord(),
        }),
        &serde_json::to_vec(&creation.metadata).expect("circle metadata serialization cannot fail"),
    )
    .await?;
    if let CircleCreationPolicyObjects::MergeConcurrent {
        roster_entry,
        roster_head,
        metadata_head,
        ..
    } = &creation.policy_objects
    {
        let roster_context = ProtocolObjectContext::circle(
            store_root_hash,
            ProtocolObjectDomain::CircleRoster,
            circle_encryption.clone(),
        );
        append_step(
            db,
            storage,
            &mut journal,
            "metadata-head",
            &ProtocolObjectContext::circle(
                store_root_hash,
                ProtocolObjectDomain::CircleMetadata,
                circle_encryption.clone(),
            ),
            &circle_semantic_prefix(CircleSemanticSlot::MetadataHead {
                circle_id: creation.circle_id,
                head: &CircleMetadataHeadRef::from_head(metadata_head),
            }),
            &serde_json::to_vec(metadata_head)
                .expect("circle metadata head serialization cannot fail"),
        )
        .await?;
        append_step(
            db,
            storage,
            &mut journal,
            "roster-entry",
            &roster_context,
            &circle_semantic_prefix(CircleSemanticSlot::RosterEntry {
                circle_id: creation.circle_id,
                coord: &roster_entry.coord(),
            }),
            &serde_json::to_vec(roster_entry)
                .expect("circle roster entry serialization cannot fail"),
        )
        .await?;
        append_step(
            db,
            storage,
            &mut journal,
            "roster-head",
            &roster_context,
            &circle_semantic_prefix(CircleSemanticSlot::RosterHead {
                circle_id: creation.circle_id,
                head: &CircleRosterHeadRef::from_head(roster_head),
            }),
            &serde_json::to_vec(roster_head).expect("circle roster head serialization cannot fail"),
        )
        .await?;
    }
    for (index, access) in creation.access.iter().enumerate() {
        append_step(
            db,
            storage,
            &mut journal,
            &format!("access-leaf-{index}"),
            &ProtocolObjectContext::recipient_sealed(
                store_root_hash,
                ProtocolObjectDomain::CircleAccessLeaf,
            ),
            &circle_access_leaf_semantic_prefix(
                access.leaf.value.circle_id,
                commit.candidate_family(),
                &access.leaf.value.owner_pubkey,
                access.leaf.value.epoch_id,
                &access.leaf.value.recipient_slot,
                access.leaf.value.leaf_id,
            ),
            &access.leaf.bytes,
        )
        .await?;
    }
    append_step(
        db,
        storage,
        &mut journal,
        "control",
        &ProtocolObjectContext::store_encrypted(
            store_root_hash,
            ProtocolObjectDomain::CircleControl,
        ),
        &circle_semantic_prefix(CircleSemanticSlot::Control {
            circle_id: creation.circle_id,
            control: &creation.control.coord,
        }),
        &creation.control.bytes,
    )
    .await?;
    if let CircleCreationPolicyObjects::MergeConcurrent { control_head, .. } =
        &creation.policy_objects
    {
        append_step(
            db,
            storage,
            &mut journal,
            "control-head",
            &ProtocolObjectContext::store_encrypted(
                store_root_hash,
                ProtocolObjectDomain::CircleControl,
            ),
            &circle_semantic_prefix(CircleSemanticSlot::ControlHead {
                circle_id: creation.circle_id,
                control: &control_head.control,
                head_hash: control_head.head_hash(),
            }),
            &serde_json::to_vec(control_head)
                .expect("circle control head serialization cannot fail"),
        )
        .await?;
    }
    for (index, access) in creation.access.iter().enumerate() {
        append_step(
            db,
            storage,
            &mut journal,
            &format!("access-envelope-{index}"),
            &ProtocolObjectContext::store_encrypted(
                store_root_hash,
                ProtocolObjectDomain::CircleAccessEnvelope,
            ),
            &circle_access_envelope_semantic_prefix(
                access.envelope.circle_id,
                commit.candidate_family(),
                &access.envelope.owner_pubkey,
                &access.envelope.recipient_slot,
                access.envelope.control_hash,
            ),
            &serde_json::to_vec(&access.envelope)
                .expect("access envelope serialization cannot fail"),
        )
        .await?;
    }
    let policy = journal.policy.clone();
    match policy {
        CircleOperationPolicy::MergeConcurrent { head } => {
            let commit_bytes = journal.commit_bytes.clone();
            let commit_hash = journal.commit_ref.commit_hash;
            let StoreCommitCoord::MergeConcurrent { stream_id, .. } = journal.commit_ref.coord
            else {
                return Err(CircleOperationError::InvalidState(
                    "Merge Circle policy carries a Serial commit ref".to_string(),
                ));
            };
            append_step(
                db,
                storage,
                &mut journal,
                "store-commit",
                &ProtocolObjectContext::signed_plaintext(
                    store_root_hash,
                    ProtocolObjectDomain::StoreCommit,
                ),
                &commit_semantic_prefix(
                    commit.candidate_family(),
                    &stream_id.to_string(),
                    commit.seq(),
                    commit_hash,
                ),
                &commit_bytes,
            )
            .await?;
            append_step(
                db,
                storage,
                &mut journal,
                "store-head",
                &ProtocolObjectContext::signed_plaintext(
                    store_root_hash,
                    ProtocolObjectDomain::StoreHead,
                ),
                &head_slot_prefix(
                    &head.author_registration.device_id.to_string(),
                    commit.seq(),
                ),
                &head.to_bytes(),
            )
            .await?;
        }
        CircleOperationPolicy::Serial {
            head,
            base: _,
            base_head,
            ..
        } => {
            let coordination = coordination.ok_or_else(|| {
                CircleOperationError::InvalidState(
                    "Serial circle activation requires coordination storage".to_string(),
                )
            })?;
            if let Err(error) = super::store_outbound::activate_serial_commit_head(
                db,
                storage,
                coordination,
                &base_head,
                &commit,
                journal
                    .prepared_objects
                    .get("store-commit")
                    .ok_or_else(|| {
                        CircleOperationError::Journal(
                            "Circle operation lacks its prepared Store commit".to_string(),
                        )
                    })?,
                &journal.commit_ref,
                &head,
            )
            .await
            {
                if matches!(
                    error,
                    super::store_outbound::StoreOutboundError::SerialControlConflict { .. }
                ) {
                    let reason = error.to_string();
                    db.block_circle_operation(circle_id, reason.clone()).await?;
                    return Err(CircleOperationError::Blocked { circle_id, reason });
                }
                return Err(error.into());
            }
        }
    }
    db.activate_circle_operation(journal, identity).await?;
    Ok(())
}

async fn has_current_merge_authority(
    db: &Database,
    storage: &dyn SyncStorage,
    commit: &StoreBatchCommit,
    author: &StoreDeviceRegistration,
) -> Result<bool, CircleOperationError> {
    let founder = db
        .get_protocol_state(super::membership_ops::OWNER_PUBKEY_STATE_KEY)
        .await?
        .ok_or(CircleOperationError::MissingState("Store founder"))?;
    let root = db
        .local_store_root_ref()
        .await?
        .ok_or(CircleOperationError::MissingState("Store root reference"))?;
    if root.store_root_hash != commit.store_root_hash {
        return Err(CircleOperationError::InvalidState(
            "Circle commit names a different Store root".to_string(),
        ));
    }
    let current =
        super::membership_ops::load_and_persist_owner_anchor(storage, &root, &founder, db)
            .await
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
    Ok(commit
        .membership_authority
        .as_ref()
        .is_some_and(|authority| {
            current.authorizes_write_authority(authority, &author.author_pubkey)
        }))
}

async fn append_step(
    db: &Database,
    storage: &dyn SyncStorage,
    journal: &mut CircleOperationJournal,
    step: &str,
    context: &ProtocolObjectContext,
    semantic_prefix: &str,
    bytes: &[u8],
) -> Result<(), CircleOperationError> {
    let prepared = journal.prepared_objects.get(step).cloned().ok_or_else(|| {
        CircleOperationError::Journal(format!(
            "Circle upload step {step:?} lacks its prepared exact object"
        ))
    })?;
    if journal.uploaded.contains(step) {
        let persisted =
            load_exact_slot_bytes(storage, context, prepared.reference(), semantic_prefix).await?;
        if persisted != bytes {
            return Err(CircleOperationError::InvalidState(format!(
                "circle upload step {step:?} differs from its durable journal bytes"
            )));
        }
        return Ok(());
    }
    storage
        .create_protocol_object(&prepared)
        .await
        .map_err(super::store_objects::StoreObjectError::from)?;
    let persisted =
        load_exact_slot_bytes(storage, context, prepared.reference(), semantic_prefix).await?;
    if persisted != bytes {
        return Err(CircleOperationError::InvalidState(format!(
            "circle upload step {step:?} differs from its prepared journal bytes"
        )));
    }
    journal.uploaded.insert(step.to_string());
    db.update_circle_operation(journal.clone()).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use super::*;
    use crate::database::DbError;
    use crate::storage::cloud::test_utils::InMemoryCloudHome;
    use crate::sync::cloud_storage::{
        BlobPathScheme, CloudCipher, CloudCipherAccess, CloudSyncStorage,
    };
    use crate::sync::membership::MemberRole;
    use crate::sync::storage::{
        CoordinationError, CoordinationStorage, CreateHeadError, ProtocolObjectContext,
        ProtocolObjectDomain, ReplaceHeadError, VersionToken, VersionedObject,
    };
    use crate::sync::store_commit::{serial_head_key, StoreControl};
    use crate::sync::test_helpers::{
        create_exact_test_store, host_exec, install_active_device_fixture, open_serial_test_db,
        open_test_db, temp_store_dir, test_migrations, test_synced_tables, TestCustody, TestStore,
    };

    fn merge_storage(
        home: &InMemoryCloudHome,
        signer: &UserKeypair,
        name: &str,
    ) -> CloudSyncStorage {
        CloudSyncStorage::new(
            Arc::new(home.clone()),
            CloudCipher::Plaintext,
            BlobPathScheme::Plain,
            name,
            signer.clone(),
        )
        .expect("test cloud storage supports immutable copies")
    }

    fn serial_storage(
        home: &InMemoryCloudHome,
        signer: &UserKeypair,
        name: &str,
    ) -> CloudSyncStorage {
        merge_storage(home, signer, name).with_test_serial_coordination(Arc::new(home.clone()))
    }

    async fn local_device_id(db: &Database) -> String {
        db.get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
            .await
            .expect("read local Store device id")
            .expect("local Store device is active")
    }

    async fn create_test_store_in_its_own_task(
        db: &Database,
        name: &str,
        signer: &UserKeypair,
    ) -> TestStore {
        let db = db.clone();
        let name = name.to_string();
        let signer = signer.clone();
        tokio::spawn(async move { TestStore::create(&db, &name, signer).await })
            .await
            .expect("join Circle test Store creation")
            .expect("create exact Circle test Store")
    }

    async fn create_exact_serial_store_in_its_own_task(
        db: &Database,
        storage: CloudSyncStorage,
        name: &str,
        signer: &UserKeypair,
    ) -> (CloudSyncStorage, crate::sync::store_commit::StoreRootRef) {
        let db = db.clone();
        let name = name.to_string();
        let signer = signer.clone();
        tokio::spawn(async move {
            let root = create_exact_test_store(&db, &storage, &name, &signer).await?;
            Ok::<_, String>((storage, root))
        })
        .await
        .expect("join Serial Circle test Store creation")
        .expect("create exact Serial Circle test Store")
    }

    struct HeadChangesAfterAuthorization<'a> {
        inner: &'a dyn CoordinationStorage,
        authorization_head: Vec<u8>,
        reads: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl CoordinationStorage for HeadChangesAfterAuthorization<'_> {
        async fn provider_binding(
            &self,
        ) -> Result<crate::sync::storage::ResolvedProviderBinding, CoordinationError> {
            self.inner.provider_binding().await
        }

        async fn read_head(&self, key: &str) -> Result<VersionedObject, CoordinationError> {
            let mut current = self.inner.read_head(key).await?;
            if self.reads.fetch_add(1, Ordering::SeqCst) == 0 {
                current.bytes.clone_from(&self.authorization_head);
            }
            Ok(current)
        }

        async fn create_head(
            &self,
            key: &str,
            bytes: &[u8],
        ) -> Result<VersionedObject, CreateHeadError> {
            self.inner.create_head(key, bytes).await
        }

        async fn replace_head(
            &self,
            key: &str,
            expected: &VersionToken,
            bytes: &[u8],
        ) -> Result<VersionedObject, ReplaceHeadError> {
            self.inner.replace_head(key, expected, bytes).await
        }

        async fn delete_head(&self, key: &str) -> Result<(), CoordinationError> {
            self.inner.delete_head(key).await
        }
    }

    async fn persist_merge_operation(
        db: &Database,
        name: &str,
    ) -> (TestStore, UserKeypair, CircleOperationJournal) {
        let signer = UserKeypair::generate();
        let store = create_test_store_in_its_own_task(db, name, &signer).await;
        let device_id = db
            .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
            .await
            .expect("read Circle creator device id")
            .expect("Circle creator has an active exact device");
        let journal = prepare_circle_operation(
            db,
            &store.storage,
            None,
            &device_id,
            "0000000001000-0000-creator",
            "Household",
            &signer,
        )
        .await
        .expect("prepare circle operation");
        db.insert_circle_operation(journal.clone())
            .await
            .expect("persist circle operation");
        (store, signer, journal)
    }

    fn promote_store_member_access_without_adding_to_circle_roster(
        creation: &mut CircleCreation,
        owner: &UserKeypair,
        recipient: &UserKeypair,
    ) {
        let recipient_pubkey = keys::public_key_hex(recipient);
        let access = creation
            .access
            .iter_mut()
            .find(|access| access.leaf.value.recipient_pubkey == recipient_pubkey)
            .expect("Store member has a prepared inactive access leaf");
        access.leaf.value.disposition = CircleAccessDisposition::Active {
            keyring: creation.keyring.clone(),
            key_fingerprint: creation.control.value.key_fingerprint(),
            roster: creation.control.value.roster_state_ref(),
        };
        access.leaf.value.signature = keys::sign_hex(owner, &access.leaf.value.canonical_bytes()).1;
        let recipient_key = keys::ed25519_to_x25519_public_key(&recipient.public_key())
            .expect("convert recipient key");
        access.leaf.bytes = keys::seal_box_encrypt(
            &serde_json::to_vec(&access.leaf.value).expect("serialize promoted access leaf"),
            &recipient_key,
        );
        access.leaf.leaf_hash = ObjectHash::digest(&access.leaf.bytes);

        let leaf_hashes = creation
            .access
            .iter()
            .map(|access| access.leaf.leaf_hash)
            .collect::<Vec<_>>();
        let (access_root, proofs) =
            super::super::circle_control::merkle_root_and_proofs(&leaf_hashes);
        match &mut creation.control.value.value {
            super::super::circle::CircleControlValue::MergeConcurrent { active_epoch, .. } => {
                active_epoch.common.access_root = access_root;
            }
            super::super::circle::CircleControlValue::Serial { active_epoch, .. } => {
                active_epoch.common.access_root = access_root;
            }
        }
        creation.control.value.signature =
            keys::sign_hex(owner, &creation.control.value.canonical_bytes()).1;
        creation.control.coord = creation.control.value.coord();
        creation.control.bytes =
            serde_json::to_vec(&creation.control.value).expect("serialize promoted control");
        for (access, proof) in creation.access.iter_mut().zip(proofs) {
            access.envelope.control_hash = creation.control.coord.control_hash();
            access.envelope.leaf_hash = access.leaf.leaf_hash;
            access.envelope.value_hash = ObjectHash::digest(
                &serde_json::to_vec(&access.leaf.value).expect("serialize access leaf value"),
            );
            access.envelope.proof = proof;
            access.envelope.signature = keys::sign_hex(owner, &access.envelope.canonical_bytes()).1;
        }
        let CircleCreationPolicyObjects::MergeConcurrent { control_head, .. } =
            &mut creation.policy_objects
        else {
            panic!("promoted access fixture requires MergeConcurrent policy")
        };
        *control_head =
            super::super::circle::CircleControlHead::signed(&creation.control.value, owner);
    }

    async fn activation_count(db: &Database, circle_id: CircleId) -> i64 {
        let circle_id = circle_id.to_string();
        db.call(move |conn| {
            conn.query_row(
                "SELECT COUNT(*) FROM circle_control_activations WHERE circle_id = ?1",
                [circle_id],
                |row| row.get(0),
            )
            .map_err(DbError::from)
        })
        .await
        .expect("count circle activations")
    }

    fn assert_exact_operation(expected: &CircleOperationJournal, actual: &CircleOperationJournal) {
        assert_eq!(actual.operation_id, expected.operation_id);
        assert_eq!(actual.creation, expected.creation);
        assert_eq!(actual.commit_bytes, expected.commit_bytes);
        assert_eq!(actual.policy, expected.policy);
    }

    #[tokio::test]
    async fn merge_publication_handles_every_exact_create_failure_boundary() {
        tokio::spawn(async {
            for after_visible_write in [false, true] {
                let mut call = 1;
                loop {
                    let db = open_test_db();
                    let name = format!(
                        "circle-replay-{}-{call}",
                        if after_visible_write {
                            "after"
                        } else {
                            "before"
                        }
                    );
                    let (store, signer, expected) = persist_merge_operation(&db, &name).await;
                    if call > expected.prepared_objects.len() {
                        break;
                    }
                    assert_eq!(activation_count(&db, expected.circle_id()).await, 0);
                    assert!(db
                        .get_circles(&keys::public_key_hex(&signer))
                        .await
                        .expect("read active circles")
                        .is_empty());
                    assert_eq!(
                        db.get_circle_operations()
                            .await
                            .expect("read pending circle operations"),
                        vec![crate::sync::circle::CircleOperationInfo {
                            circle_id: expected.circle_id(),
                            name: "Household".to_string(),
                            state: crate::sync::circle::CircleOperationState::Pending,
                        }]
                    );
                    if after_visible_write {
                        store.home.fail_exact_create_after_call(call);
                    } else {
                        store.home.fail_exact_create_before_call(call);
                    }

                    let first = resume_circle_operations(&db, &store.storage, None, &signer).await;
                    if after_visible_write {
                        first.expect("lost exact-create response is settled by exact readback");
                    } else {
                        let error =
                            first.expect_err("failure before exact create interrupts activation");
                        assert!(matches!(error, CircleOperationError::Object(_)), "{error}");
                        let persisted = db
                            .circle_operation(expected.circle_id())
                            .await
                            .expect("read interrupted operation")
                            .expect("interrupted operation remains durable");
                        assert_exact_operation(&expected, &persisted);
                        assert_eq!(persisted.status, CircleOperationState::Pending);
                        assert_eq!(activation_count(&db, expected.circle_id()).await, 0);

                        resume_circle_operations(&db, &store.storage, None, &signer)
                            .await
                            .expect("resume exact circle operation");
                    }
                    assert!(db
                        .circle_operation(expected.circle_id())
                        .await
                        .expect("read completed operation")
                        .is_none());
                    assert_eq!(activation_count(&db, expected.circle_id()).await, 1);
                    assert_eq!(
                        db.get_circles(&keys::public_key_hex(&signer))
                            .await
                            .expect("read activated circle"),
                        vec![crate::sync::circle::CircleInfo {
                            id: expected.circle_id(),
                            name: "Household".to_string(),
                            role: crate::sync::circle::CircleRole::Owner,
                        }]
                    );
                    assert!(db
                        .get_circle_operations()
                        .await
                        .expect("read completed circle operations")
                        .is_empty());
                    call += 1;
                }
            }
        })
        .await
        .expect("Circle publication task completes");
    }

    #[tokio::test]
    async fn pending_circle_operation_reopens_with_identical_signed_state() {
        let temp = tempfile::tempdir().expect("create database directory");
        let path = temp.path().join("circle-restart.sqlite3");
        let (db, _stamper) = Database::open(
            &path,
            test_synced_tables(),
            crate::blob::BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::serial(),
            crate::WritePolicy::MergeConcurrent,
            "creator".to_string(),
            &test_migrations(),
        )
        .expect("open circle database");
        let (store, signer, expected) = persist_merge_operation(&db, "circle-restart").await;
        assert_eq!(activation_count(&db, expected.circle_id()).await, 0);
        std::thread::spawn(move || drop(db))
            .join()
            .expect("close circle database");

        let (reopened, _stamper) = Database::open(
            &path,
            test_synced_tables(),
            crate::blob::BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::serial(),
            crate::WritePolicy::MergeConcurrent,
            "creator".to_string(),
            &test_migrations(),
        )
        .expect("reopen circle database");
        let persisted = reopened
            .circle_operation(expected.circle_id())
            .await
            .expect("read reopened circle operation")
            .expect("circle operation survives restart");
        assert_exact_operation(&expected, &persisted);
        assert_eq!(persisted.status, CircleOperationState::Pending);

        resume_circle_operations(&reopened, &store.storage, None, &signer)
            .await
            .expect("resume reopened circle operation");
        assert_eq!(activation_count(&reopened, expected.circle_id()).await, 1);
    }

    #[tokio::test]
    async fn pending_serial_circle_operation_reopens_with_exact_policy_state() {
        let temp = tempfile::tempdir().expect("create database directory");
        let path = temp.path().join("serial-circle-restart.sqlite3");
        let founder = UserKeypair::generate();
        let home = InMemoryCloudHome::new();
        let storage = serial_storage(&home, &founder, "serial-circle-restart");
        let (db, _stamper) = Database::open(
            &path,
            test_synced_tables(),
            crate::blob::BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::serial(),
            crate::WritePolicy::Serial,
            "founder-device".to_string(),
            &test_migrations(),
        )
        .expect("open Serial Circle database");
        let (storage, _root) = create_exact_serial_store_in_its_own_task(
            &db,
            storage,
            "serial-circle-restart",
            &founder,
        )
        .await;
        let device_id = local_device_id(&db).await;
        let coordination = storage.serial_coordination().expect("Serial coordination");
        let expected = prepare_circle_operation(
            &db,
            &storage,
            Some(coordination),
            &device_id,
            "0000000001000-0000-founder",
            "Household",
            &founder,
        )
        .await
        .expect("prepare Serial Circle operation");
        assert!(matches!(
            expected.policy,
            CircleOperationPolicy::Serial { .. }
        ));
        db.insert_circle_operation(expected.clone())
            .await
            .expect("persist Serial Circle operation");
        std::thread::spawn(move || drop(db))
            .join()
            .expect("close Serial Circle database");

        let (reopened, _stamper) = Database::open(
            &path,
            test_synced_tables(),
            crate::blob::BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::serial(),
            crate::WritePolicy::Serial,
            "founder-device".to_string(),
            &test_migrations(),
        )
        .expect("reopen Serial Circle database");
        let persisted = reopened
            .circle_operation(expected.circle_id())
            .await
            .expect("read reopened Serial Circle operation")
            .expect("Serial Circle operation survives restart");
        assert_exact_operation(&expected, &persisted);

        resume_circle_operations(&reopened, &storage, Some(coordination), &founder)
            .await
            .expect("resume reopened Serial Circle operation");
        assert_eq!(activation_count(&reopened, expected.circle_id()).await, 1);
    }

    #[tokio::test]
    async fn persisted_merge_circle_operation_rejects_serial_policy_state() {
        let db = open_test_db();
        let (_store, _signer, journal) =
            persist_merge_operation(&db, "circle-merge-serial-state").await;
        let mut payload = serde_json::to_value(&journal).expect("serialize Merge journal");
        let policy = payload
            .get_mut("policy")
            .and_then(|policy| policy.get_mut("merge_concurrent"))
            .and_then(serde_json::Value::as_object_mut)
            .expect("Merge policy object");
        policy.insert("base".to_string(), serde_json::Value::Null);
        policy.insert(
            "base_head".to_string(),
            serde_json::json!({ "bytes": [115, 101, 114, 105, 97, 108], "version": "bad" }),
        );
        let payload = serde_json::to_vec(&payload).expect("serialize invalid journal");
        let circle_id = journal.circle_id();
        let stored_circle_id = circle_id.to_string();
        db.call(move |conn| {
            conn.execute(
                "UPDATE circle_operations SET payload = ?2 WHERE circle_id = ?1",
                rusqlite::params![stored_circle_id, payload],
            )
            .map(|_| ())
            .map_err(DbError::from)
        })
        .await
        .expect("install invalid durable payload");

        db.circle_operation(circle_id)
            .await
            .expect_err("Merge operation must reject Serial-only policy state");
    }

    #[tokio::test]
    async fn uploaded_circle_steps_are_read_back_after_restart_before_activation() {
        for corrupt in [false, true] {
            let temp = tempfile::tempdir().expect("create database directory");
            let path = temp.path().join(if corrupt {
                "circle-corrupt-upload.sqlite3"
            } else {
                "circle-missing-upload.sqlite3"
            });
            let (db, _stamper) = Database::open(
                &path,
                test_synced_tables(),
                crate::blob::BLOB_TOMBSTONE_GRACE,
                crate::blob::TransferLimits::serial(),
                crate::WritePolicy::MergeConcurrent,
                "creator".to_string(),
                &test_migrations(),
            )
            .expect("open circle database");
            let (store, signer, expected) =
                persist_merge_operation(&db, if corrupt { "corrupt" } else { "missing" }).await;
            store.home.fail_exact_create_before_call(2);
            resume_circle_operations(&db, &store.storage, None, &signer)
                .await
                .expect_err("second exact create failure interrupts publication");
            let persisted = db
                .circle_operation(expected.circle_id())
                .await
                .expect("read interrupted circle operation")
                .expect("interrupted circle operation remains durable");
            assert!(persisted.uploaded.contains("metadata"));

            let metadata = expected
                .prepared_objects
                .get("metadata")
                .expect("operation carries exact metadata object");
            if corrupt {
                store.home.replace_exact_object(
                    metadata.reference().slot(),
                    b"corrupt metadata bytes".to_vec(),
                );
            } else {
                store.home.remove_exact_object(metadata.reference().slot());
            }
            std::thread::spawn(move || drop(db))
                .join()
                .expect("close circle database");

            let (reopened, _stamper) = Database::open(
                &path,
                test_synced_tables(),
                crate::blob::BLOB_TOMBSTONE_GRACE,
                crate::blob::TransferLimits::serial(),
                crate::WritePolicy::MergeConcurrent,
                "creator".to_string(),
                &test_migrations(),
            )
            .expect("reopen circle database");
            resume_circle_operations(&reopened, &store.storage, None, &signer)
                .await
                .expect_err("durable upload marker must not bypass readback");
            assert_eq!(activation_count(&reopened, expected.circle_id()).await, 0);
            assert!(reopened
                .circle_operation(expected.circle_id())
                .await
                .expect("read rejected circle operation")
                .is_some());
        }
    }

    #[tokio::test]
    async fn local_activation_rejects_a_tampered_leaf_disposition() {
        let db = open_test_db();
        let (store, signer, mut journal) =
            persist_merge_operation(&db, "circle-tampered-local-access").await;
        let author = keys::public_key_hex(&signer);
        let own_access = journal
            .creation
            .access
            .iter_mut()
            .find(|access| access.leaf.value.recipient_pubkey == author)
            .expect("founder access");
        assert!(matches!(
            own_access.leaf.value.disposition,
            CircleAccessDisposition::Active { .. }
        ));
        own_access.leaf.value.disposition = CircleAccessDisposition::Inactive;
        db.update_circle_operation(journal.clone())
            .await
            .expect("persist tampered journal");

        resume_circle_operations(&db, &store.storage, None, &signer)
            .await
            .expect_err("local activation must verify journal access context");

        assert_eq!(activation_count(&db, journal.circle_id()).await, 0);
        assert!(db
            .circle_operation(journal.circle_id())
            .await
            .expect("read rejected operation")
            .is_some());
    }

    #[tokio::test]
    async fn local_activation_rejects_sealed_leaf_plaintext_substitution() {
        let db = open_test_db();
        let (_store, signer, mut journal) =
            persist_merge_operation(&db, "circle-mismatched-local-keyring").await;
        let author = keys::public_key_hex(&signer);
        let own_access = journal
            .creation
            .access
            .iter_mut()
            .find(|access| access.leaf.value.recipient_pubkey == author)
            .expect("founder access");
        let CircleAccessDisposition::Active { keyring, .. } =
            &mut own_access.leaf.value.disposition
        else {
            panic!("founder access must be active")
        };
        *keyring = MasterKeyring::generate().to_serialized();
        own_access.leaf.value.signature =
            keys::sign_hex(&signer, &own_access.leaf.value.canonical_bytes()).1;
        own_access.envelope.value_hash = ObjectHash::digest(
            &serde_json::to_vec(&own_access.leaf.value).expect("serialize mismatched access leaf"),
        );
        own_access.envelope.signature =
            keys::sign_hex(&signer, &own_access.envelope.canonical_bytes()).1;
        let commit = journal.commit().expect("parse prepared Store commit");
        let author = db
            .activated_store_device_registration(commit.author_registration.clone())
            .await
            .expect("load exact Circle commit author");
        let error = verify_local_circle_activation(
            &journal,
            &journal.commit_ref,
            &commit,
            &author,
            &signer,
        )
        .expect_err("local activation must reject substituted journal plaintext");
        assert!(error
            .to_string()
            .contains("sealed leaf differs from its journaled value"));
        assert_eq!(activation_count(&db, journal.circle_id()).await, 0);
    }

    #[tokio::test]
    async fn local_publication_rejects_a_prepared_object_outside_the_signed_graph() {
        let db = open_test_db();
        let (store, signer, mut journal) =
            persist_merge_operation(&db, "circle-substituted-local-object-ref").await;
        let original = journal
            .prepared_objects
            .get("metadata")
            .expect("operation carries exact metadata object");
        let substituted_slot = crate::storage::cloud::ObjectSlot::opaque(
            original.reference().slot().logical_key().to_string(),
            "substituted-metadata-object".to_string(),
        )
        .expect("construct alternate provider object slot");
        let substituted = PreparedExactObject::new(
            super::super::storage::ExactObjectRef::new(
                substituted_slot,
                original.reference().stored_size(),
                original.reference().stored_hash(),
            ),
            original.stored_bytes().to_vec(),
        )
        .expect("construct substituted prepared metadata object");
        journal
            .prepared_objects
            .insert("metadata".to_string(), substituted);
        db.update_circle_operation(journal.clone())
            .await
            .expect("persist substituted journal object");

        resume_circle_operations(&db, &store.storage, None, &signer)
            .await
            .expect_err("local publication must reject objects outside the signed graph");

        assert_eq!(activation_count(&db, journal.circle_id()).await, 0);
    }

    #[tokio::test]
    async fn local_publication_rejects_a_store_head_outside_its_reserved_slot() {
        let db = open_test_db();
        let (store, signer, mut journal) =
            persist_merge_operation(&db, "circle-substituted-local-head-slot").await;
        let original = journal
            .prepared_objects
            .get("store-head")
            .expect("Merge operation carries an exact Store head");
        let substituted_slot = crate::storage::cloud::ObjectSlot::opaque(
            original.reference().slot().logical_key().to_string(),
            "substituted-store-head".to_string(),
        )
        .expect("construct alternate Store head slot");
        let substituted = PreparedExactObject::new(
            super::super::storage::ExactObjectRef::new(
                substituted_slot,
                original.reference().stored_size(),
                original.reference().stored_hash(),
            ),
            original.stored_bytes().to_vec(),
        )
        .expect("construct substituted prepared Store head");
        journal
            .prepared_objects
            .insert("store-head".to_string(), substituted);
        db.update_circle_operation(journal.clone())
            .await
            .expect("persist substituted Store head slot");

        resume_circle_operations(&db, &store.storage, None, &signer)
            .await
            .expect_err("local publication must reject an unreserved Store head slot");

        assert_eq!(activation_count(&db, journal.circle_id()).await, 0);
    }

    #[tokio::test]
    async fn remote_activation_rejects_invented_access_refs_in_a_resigned_commit() {
        let db = open_test_db();
        let (store, signer, journal) =
            persist_merge_operation(&db, "circle-invented-access-refs").await;
        let old_commit = journal.commit().expect("parse prepared Store commit");
        for object in journal.prepared_objects.values() {
            super::super::store_objects::create_exact_object(&store.storage, object)
                .await
                .expect("publish original exact Circle activation object");
        }
        let mut objects = old_commit
            .operations()
            .expect("Circle commit carries operations")
            .circle_controls[0]
            .objects()
            .clone();
        let original_ref = objects.access[0].clone();
        let original_access = &journal.creation.access[0];
        let invented_recipient_slot = format!("{}-invented", original_ref.leaf.recipient_slot);
        let candidate_family = old_commit.candidate_family();
        let leaf_prefix = circle_access_leaf_semantic_prefix(
            journal.creation.circle_id,
            candidate_family,
            &original_ref.leaf.owner_pubkey,
            original_ref.leaf.epoch_id,
            &invented_recipient_slot,
            original_ref.leaf.leaf_id,
        );
        let leaf = prepare_circle_object(
            &store.storage,
            &ProtocolObjectContext::recipient_sealed(
                old_commit.store_root_hash,
                ProtocolObjectDomain::CircleAccessLeaf,
            ),
            &leaf_prefix,
            "",
            original_access.leaf.bytes.clone(),
        )
        .await
        .expect("prepare invented access leaf path");
        let envelope_prefix = circle_access_envelope_semantic_prefix(
            journal.creation.circle_id,
            candidate_family,
            &original_ref.envelope.owner_pubkey,
            &invented_recipient_slot,
            original_ref.envelope.control_hash,
        );
        let envelope = prepare_circle_object(
            &store.storage,
            &ProtocolObjectContext::store_encrypted(
                old_commit.store_root_hash,
                ProtocolObjectDomain::CircleAccessEnvelope,
            ),
            &envelope_prefix,
            ".json",
            serde_json::to_vec(&original_access.envelope)
                .expect("serialize original access envelope"),
        )
        .await
        .expect("prepare invented access envelope path");
        super::super::store_objects::create_exact_object(&store.storage, &leaf)
            .await
            .expect("publish invented access leaf path");
        super::super::store_objects::create_exact_object(&store.storage, &envelope)
            .await
            .expect("publish invented access envelope path");
        objects.access.push(CircleAccessObjectRef {
            leaf: CircleAccessLeafObjectRef {
                recipient_slot: invented_recipient_slot.clone(),
                object: leaf.reference().clone(),
                ..original_ref.leaf
            },
            envelope: CircleAccessEnvelopeObjectRef {
                recipient_slot: invented_recipient_slot,
                object: envelope.reference().clone(),
                ..original_ref.envelope
            },
        });
        let author = db
            .activated_store_device_registration(old_commit.author_registration.clone())
            .await
            .expect("load exact Circle commit author");
        let device_signer = author
            .device_signer(&signer)
            .expect("derive Circle commit device signer");
        let commit_coord = journal.commit_ref.coord.clone();
        let commit = signed_circle_commit(
            old_commit.store_root_hash,
            old_commit.write_id,
            commit_coord.clone(),
            old_commit.author_registration,
            &author,
            old_commit.order,
            old_commit.membership_state,
            old_commit.device_state,
            old_commit.membership_authority,
            &journal.creation,
            objects,
            &device_signer,
        )
        .expect("sign commit naming invented access refs");
        let StoreCommitCoord::MergeConcurrent { stream_id, .. } = commit_coord.clone() else {
            panic!("invented access test requires a MergeConcurrent commit")
        };
        let commit_prepared = prepare_circle_object(
            &store.storage,
            &ProtocolObjectContext::signed_plaintext(
                commit.store_root_hash,
                ProtocolObjectDomain::StoreCommit,
            ),
            &commit_semantic_prefix(
                commit.candidate_family(),
                &stream_id.to_string(),
                commit.seq(),
                commit.commit_hash(),
            ),
            ".json",
            commit.to_bytes(),
        )
        .await
        .expect("prepare re-signed Store commit");
        super::super::store_objects::create_exact_object(&store.storage, &commit_prepared)
            .await
            .expect("publish re-signed Store commit");
        let commit_ref = StoreBatchCommitRef::from_commit(
            &commit,
            commit_coord,
            commit_prepared.reference().clone(),
        )
        .expect("bind re-signed Store commit reference");

        let error = load_circle_activations(
            &store.storage,
            &store.root,
            &commit_ref,
            &commit,
            &author,
            &signer,
            &keys::public_key_hex(&signer),
        )
        .await
        .expect_err("invented access references must fail activation");
        assert!(
            error
                .to_string()
                .contains("circle access envelope failed verification"),
            "{error}"
        );

        let CircleOperationPolicy::MergeConcurrent {
            head: original_head,
        } = &journal.policy
        else {
            panic!("invented access test requires a MergeConcurrent head")
        };
        let forged_head = StoreDeviceHead::signed(
            commit.store_root_hash,
            commit.author_registration.clone(),
            commit_ref.clone(),
            original_head.successor.clone(),
            &device_signer,
        )
        .expect("sign Store head naming the re-signed commit");
        let original_head_object = journal
            .prepared_objects
            .get("store-head")
            .expect("Circle operation carries its Store head");
        let head_context = ProtocolObjectContext::signed_plaintext(
            commit.store_root_hash,
            ProtocolObjectDomain::StoreHead,
        );
        let forged_head_object = store
            .storage
            .prepare_protocol_object(
                &head_context,
                original_head_object.reference().slot().clone(),
                &head_slot_prefix(
                    &commit.author_registration.device_id.to_string(),
                    commit.seq(),
                ),
                forged_head.to_bytes(),
            )
            .expect("prepare Store head naming the re-signed commit");
        store.home.replace_exact_object(
            original_head_object.reference().slot(),
            forged_head_object.stored_bytes().to_vec(),
        );

        let (_store_temp, store_dir) = temp_store_dir();
        let pull = super::super::store_pull::pull_store_commits_with_identity(
            &db,
            &test_synced_tables(),
            &store.storage,
            None,
            store.root.store_root_hash,
            &store_dir,
            None,
            Some(&signer),
        )
        .await
        .expect("pull reports the invented access commit as held");
        assert!(pull.held_positions.iter().any(|held| {
            matches!(
                &held.reason,
                super::super::store_pull::HeldStorePositionReason::InvalidObject(reason)
                    if reason.contains("circle access envelope failed verification")
            )
        }));
        assert_eq!(activation_count(&db, journal.circle_id()).await, 0);
        assert!(db
            .exact_materialized_ref(&stream_id.to_string(), commit.seq())
            .await
            .expect("read invented access commit position")
            .is_none());
    }

    #[tokio::test]
    async fn remote_activation_rejects_active_access_for_a_nonmember() {
        let db = open_test_db();
        let founder = UserKeypair::generate();
        let store = TestStore::create(&db, "circle-active-access-nonmember", founder.clone())
            .await
            .expect("create exact Circle test Store");
        let peer = UserKeypair::generate();
        let peer_pubkey = keys::public_key_hex(&peer);
        super::super::membership_ops::invite_member(
            &store.storage,
            store.home.as_ref(),
            &founder,
            &super::super::hlc::Hlc::new("founder-device".to_string()),
            &peer_pubkey,
            None,
            MemberRole::Member,
            &EncryptionService::from_key([42; 32]),
            "circle-active-access-nonmember",
            "Active access test Store",
            &db,
        )
        .await
        .expect("invite Store member outside the Circle roster");
        let device_id = local_device_id(&db).await;
        let mut journal = prepare_circle_operation(
            &db,
            &store.storage,
            None,
            &device_id,
            "0000000001000-0000-founder",
            "Household",
            &founder,
        )
        .await
        .expect("prepare Circle with inactive Store-member access");
        promote_store_member_access_without_adding_to_circle_roster(
            &mut journal.creation,
            &founder,
            &peer,
        );
        let old_commit = journal.commit().expect("parse prepared Store commit");
        let candidate_family = old_commit.candidate_family();
        let (objects, prepared) = prepare_circle_activation_objects(
            &store.storage,
            &store.root,
            &journal.creation,
            candidate_family,
        )
        .await
        .expect("prepare exact promoted access objects");
        for object in prepared.values() {
            super::super::store_objects::create_exact_object(&store.storage, object)
                .await
                .expect("publish exact promoted access object");
        }
        let author = db
            .activated_store_device_registration(old_commit.author_registration.clone())
            .await
            .expect("load exact Circle commit author");
        let device_signer = author
            .device_signer(&founder)
            .expect("derive Circle commit device signer");
        let commit_coord = journal.commit_ref.coord.clone();
        let commit = signed_circle_commit(
            old_commit.store_root_hash,
            old_commit.write_id,
            commit_coord.clone(),
            old_commit.author_registration,
            &author,
            old_commit.order,
            old_commit.membership_state,
            old_commit.device_state,
            old_commit.membership_authority,
            &journal.creation,
            objects,
            &device_signer,
        )
        .expect("sign promoted access commit");
        let StoreCommitCoord::MergeConcurrent { stream_id, .. } = commit_coord.clone() else {
            panic!("promoted access test requires a MergeConcurrent commit")
        };
        let commit_prepared = prepare_circle_object(
            &store.storage,
            &ProtocolObjectContext::signed_plaintext(
                commit.store_root_hash,
                ProtocolObjectDomain::StoreCommit,
            ),
            &commit_semantic_prefix(
                commit.candidate_family(),
                &stream_id.to_string(),
                commit.seq(),
                commit.commit_hash(),
            ),
            ".json",
            commit.to_bytes(),
        )
        .await
        .expect("prepare promoted access Store commit");
        super::super::store_objects::create_exact_object(&store.storage, &commit_prepared)
            .await
            .expect("publish promoted access Store commit");
        let commit_ref = StoreBatchCommitRef::from_commit(
            &commit,
            commit_coord,
            commit_prepared.reference().clone(),
        )
        .expect("bind promoted access Store commit");

        let error = load_circle_activations(
            &store.storage,
            &store.root,
            &commit_ref,
            &commit,
            &author,
            &peer,
            &keys::public_key_hex(&founder),
        )
        .await
        .expect_err("Active access must name a resolved Circle member");
        assert!(
            error
                .to_string()
                .contains("Active access recipient is absent"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn remote_activation_rejects_metadata_with_a_different_historical_roster() {
        let baseline_db = open_test_db();
        let (baseline_store, baseline_signer, baseline) =
            persist_merge_operation(&baseline_db, "circle-remote-metadata-baseline").await;
        let baseline_commit = baseline.commit().expect("parse baseline Store commit");
        for object in baseline.prepared_objects.values() {
            super::super::store_objects::create_exact_object(&baseline_store.storage, object)
                .await
                .expect("publish baseline exact Circle activation object");
        }
        let baseline_author = baseline_db
            .activated_store_device_registration(baseline_commit.author_registration.clone())
            .await
            .expect("load baseline exact Circle commit author");
        load_circle_activations(
            &baseline_store.storage,
            &baseline_store.root,
            &baseline.commit_ref,
            &baseline_commit,
            &baseline_author,
            &baseline_signer,
            &keys::public_key_hex(&baseline_signer),
        )
        .await
        .expect("baseline exact Circle activation verifies remotely");

        let db = open_test_db();
        let (store, signer, mut journal) =
            persist_merge_operation(&db, "circle-remote-metadata-roster").await;
        let old_commit = journal.commit().expect("parse prepared Store commit");
        let commit_coord = journal.commit_ref.coord.clone();
        let creation = &mut journal.creation;
        let store_root_hash = creation.control.value.store_root_hash;
        let super::super::circle::CircleRosterStateRef::MergeConcurrent(roster_state) =
            &mut creation.metadata.author_roster
        else {
            panic!("Merge creation metadata must name a Merge roster")
        };
        roster_state.state_hash = ObjectHash::digest(b"different historical roster state");
        creation.metadata.signature =
            keys::sign_hex(&signer, &creation.metadata.canonical_bytes()).1;
        let metadata_head =
            super::super::circle::CircleMetadataHead::signed(&creation.metadata, &signer);
        let super::super::circle::CircleControlValue::MergeConcurrent { active_epoch, .. } =
            &mut creation.control.value.value
        else {
            panic!("Merge creation must carry Merge control")
        };
        active_epoch.metadata = super::super::circle::MergeCircleMetadataStateRef {
            heads: vec![super::super::circle::CircleMetadataHeadRef::from_head(
                &metadata_head,
            )],
            selected: creation.metadata.coord(),
            state_hash: creation.metadata.metadata_hash(),
        };
        creation.control.value.signature =
            keys::sign_hex(&signer, &creation.control.value.canonical_bytes()).1;
        creation.control.coord = creation.control.value.coord();
        creation.control.bytes =
            serde_json::to_vec(&creation.control.value).expect("serialize forged Circle control");
        for access in &mut creation.access {
            access.envelope.control_hash = creation.control.coord.control_hash();
            access.envelope.signature =
                keys::sign_hex(&signer, &access.envelope.canonical_bytes()).1;
        }
        {
            let CircleCreationPolicyObjects::MergeConcurrent {
                metadata_head: stored_metadata_head,
                control_head,
                ..
            } = &mut creation.policy_objects
            else {
                panic!("Merge creation must carry Merge policy objects")
            };
            *stored_metadata_head = metadata_head;
            *control_head =
                super::super::circle::CircleControlHead::signed(&creation.control.value, &signer);
        }
        let candidate_family = old_commit.candidate_family();
        let (objects, prepared) = prepare_circle_activation_objects(
            &store.storage,
            &store.root,
            creation,
            candidate_family,
        )
        .await
        .expect("prepare forged exact Circle activation objects");
        for object in prepared.values() {
            super::super::store_objects::create_exact_object(&store.storage, object)
                .await
                .expect("publish forged exact Circle activation object");
        }
        let author = db
            .activated_store_device_registration(old_commit.author_registration.clone())
            .await
            .expect("load exact Circle commit author");
        let device_signer = author
            .device_signer(&signer)
            .expect("derive Circle commit device signer");
        let commit = signed_circle_commit(
            store_root_hash,
            old_commit.write_id,
            commit_coord.clone(),
            old_commit.author_registration,
            &author,
            old_commit.order,
            old_commit.membership_state,
            old_commit.device_state,
            old_commit.membership_authority,
            creation,
            objects,
            &device_signer,
        )
        .expect("sign forged metadata activation commit");
        let StoreCommitCoord::MergeConcurrent { stream_id, .. } = commit_coord.clone() else {
            panic!("forged metadata test requires a MergeConcurrent commit")
        };
        let commit_prepared = prepare_circle_object(
            &store.storage,
            &ProtocolObjectContext::signed_plaintext(
                store_root_hash,
                ProtocolObjectDomain::StoreCommit,
            ),
            &commit_semantic_prefix(
                commit.candidate_family(),
                &stream_id.to_string(),
                commit.seq(),
                commit.commit_hash(),
            ),
            ".json",
            commit.to_bytes(),
        )
        .await
        .expect("prepare forged exact Store commit");
        super::super::store_objects::create_exact_object(&store.storage, &commit_prepared)
            .await
            .expect("publish forged exact Store commit");
        let commit_ref = StoreBatchCommitRef::from_commit(
            &commit,
            commit_coord,
            commit_prepared.reference().clone(),
        )
        .expect("bind forged exact Store commit reference");

        let error = load_circle_activations(
            &store.storage,
            &store.root,
            &commit_ref,
            &commit,
            &author,
            &signer,
            &keys::public_key_hex(&signer),
        )
        .await
        .expect_err("metadata cannot borrow authority from a different roster state");
        assert!(
            error.to_string().contains("roster state hash differs"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn merge_resume_blocks_revoked_journals_without_stopping_later_operations() {
        let db = open_test_db();
        let founder = UserKeypair::generate();
        let store =
            create_test_store_in_its_own_task(&db, "circle-merge-revoked-grant", &founder).await;
        let successor = UserKeypair::generate();
        let successor_pubkey = keys::public_key_hex(&successor);
        let encryption = EncryptionService::from_key([42; 32]);
        super::super::membership_ops::invite_member(
            &store.storage,
            store.home.as_ref(),
            &founder,
            &super::super::hlc::Hlc::new("founder-device".to_string()),
            &successor_pubkey,
            None,
            MemberRole::Member,
            &encryption,
            "circle-merge-revoked-grant",
            "Revocation test Store",
            &db,
        )
        .await
        .expect("invite successor member through the production membership path");

        let successor_db = open_test_db();
        install_active_device_fixture(
            &store,
            &db,
            &successor_db,
            &successor,
            "0000000001003-0000-successor",
        )
        .await
        .expect("activate successor exact device fixture");
        let successor_device_id = local_device_id(&successor_db).await;
        let journal = prepare_circle_operation(
            &successor_db,
            &store.storage,
            None,
            &successor_device_id,
            "0000000001003-0000-successor",
            "Revoked Circle",
            &successor,
        )
        .await
        .expect("prepare operation while successor is authorized");
        successor_db
            .insert_circle_operation(journal.clone())
            .await
            .expect("persist operation that will lose authorization");
        let custody = TestCustody::default();
        custody.set_initial_key([42; 32]);
        let cipher = store.storage.cipher_state().clone();
        let pending_rotation = store.storage.shared_pending_rotation();
        super::super::membership_ops::remove_member(
            &store.storage,
            store.home.as_ref(),
            &founder,
            &super::super::hlc::Hlc::new("founder-device".to_string()),
            &successor_pubkey,
            "circle-merge-revoked-grant",
            &encryption,
            &custody,
            cipher.as_ref(),
            pending_rotation.as_ref(),
            &db,
        )
        .await
        .expect("remove successor through the production membership path");
        let rotated_encryption = match cipher.snapshot() {
            CloudCipher::Encrypted(encryption) => encryption,
            CloudCipher::Plaintext => panic!("member removal requires encrypted storage"),
        };
        super::super::membership_ops::invite_member(
            &store.storage,
            store.home.as_ref(),
            &founder,
            &super::super::hlc::Hlc::new("founder-device".to_string()),
            &successor_pubkey,
            None,
            MemberRole::Member,
            &rotated_encryption,
            "circle-merge-revoked-grant",
            "Revocation test Store",
            &db,
        )
        .await
        .expect("re-add successor under a new exact membership grant");
        store
            .open_into(&successor_db)
            .await
            .expect("load successor's replacement membership grant");
        let later = prepare_circle_operation(
            &successor_db,
            &store.storage,
            None,
            &successor_device_id,
            "0000000001004-0000-successor",
            "Later Circle",
            &successor,
        )
        .await
        .expect("prepare still-authorized operation");
        successor_db
            .insert_circle_operation(later.clone())
            .await
            .expect("persist still-authorized operation");

        resume_circle_operations(&successor_db, &store.storage, None, &successor)
            .await
            .expect("revoked journal is blocked without interrupting the resume loop");

        let blocked = successor_db
            .circle_operation(journal.circle_id())
            .await
            .expect("read revoked journal")
            .expect("revoked journal remains durable");
        assert!(matches!(
            blocked.status,
            CircleOperationState::Blocked { .. }
        ));
        assert!(successor_db
            .circle_operation(later.circle_id())
            .await
            .expect("read later journal")
            .is_none());
        assert_eq!(
            successor_db
                .get_circles(&successor_pubkey)
                .await
                .expect("read successor circles"),
            vec![crate::sync::circle::CircleInfo {
                id: later.circle_id(),
                name: "Later Circle".to_string(),
                role: CircleRole::Owner,
            }]
        );
        assert_eq!(
            activation_count(&successor_db, journal.circle_id()).await,
            0
        );
    }

    #[tokio::test]
    async fn serial_circle_cannot_activate_from_authorization_before_a_removal_head() {
        let home = InMemoryCloudHome::new();
        let founder = UserKeypair::generate();
        let successor = UserKeypair::generate();
        let storage = serial_storage(&home, &founder, "circle-serial-authority-race");
        let db = open_serial_test_db();
        let (storage, _root) = create_exact_serial_store_in_its_own_task(
            &db,
            storage,
            "circle-serial-authority-race",
            &founder,
        )
        .await;
        let device_id = local_device_id(&db).await;
        let coordination = storage.serial_coordination().expect("Serial coordination");

        let authorization =
            super::super::store_outbound::current_serial_authorization(&db, &storage, coordination)
                .await
                .expect("founder authorization");
        let add_successor = authorization
            .membership
            .signed_set_member(
                &founder,
                keys::public_key_hex(&successor),
                None,
                MemberRole::Owner,
                "0000000000001-0000-founder".to_string(),
            )
            .expect("add successor owner");
        let prepared = super::super::store_outbound::prepare_serial_control(
            &db,
            &storage,
            coordination,
            &device_id,
            StoreControl::SerialMembership {
                entry: add_successor,
            },
            &founder,
        )
        .await
        .expect("prepare successor addition");
        super::super::store_outbound::activate_serial_control(
            &db,
            &storage,
            coordination,
            &prepared,
        )
        .await
        .expect("activate successor addition");
        let authorization_head = coordination
            .read_head(serial_head_key())
            .await
            .expect("read authorization head")
            .bytes;

        let authorization =
            super::super::store_outbound::current_serial_authorization(&db, &storage, coordination)
                .await
                .expect("authorization before removal");
        let remove_founder = authorization
            .membership
            .signed_remove_member(
                &founder,
                keys::public_key_hex(&founder),
                "0000000000002-0000-founder".to_string(),
            )
            .expect("remove founder");
        let prepared = super::super::store_outbound::prepare_serial_control(
            &db,
            &storage,
            coordination,
            &device_id,
            StoreControl::SerialMembershipAndKeyRotation {
                entry: remove_founder,
                generation: 2,
                wrapped_keys: vec![super::super::membership::test_wrapped_key_ref(
                    &keys::public_key_hex(&founder),
                    &keys::public_key_hex(&successor),
                    2,
                    b"circle Serial founder removal wrap",
                )],
            },
            &founder,
        )
        .await
        .expect("prepare founder removal");
        super::super::store_outbound::activate_serial_control(
            &db,
            &storage,
            coordination,
            &prepared,
        )
        .await
        .expect("activate founder removal");

        let changed = HeadChangesAfterAuthorization {
            inner: coordination,
            authorization_head,
            reads: AtomicUsize::new(0),
        };
        let journal = prepare_circle_operation(
            &db,
            &storage,
            Some(&changed),
            &device_id,
            "0000000001000-0000-founder",
            "Removed founder circle",
            &founder,
        )
        .await
        .expect("reproduce mismatched authorization and base snapshot");
        db.insert_circle_operation(journal.clone())
            .await
            .expect("persist raced operation");

        let error = publish_circle_operation(
            &db,
            &storage,
            Some(coordination),
            journal.circle_id(),
            &founder,
        )
        .await
        .expect_err("a removed writer must not activate a Circle commit");

        assert!(matches!(error, CircleOperationError::Blocked { .. }));
        assert!(db
            .get_circles(&keys::public_key_hex(&founder))
            .await
            .expect("read founder circles")
            .is_empty());
    }

    #[tokio::test]
    async fn serial_circle_rejects_different_head_bytes_at_the_same_commit_position_before_upload()
    {
        let home = InMemoryCloudHome::new();
        let founder = UserKeypair::generate();
        let successor = UserKeypair::generate();
        let storage = serial_storage(&home, &founder, "circle-serial-head-bytes");
        let db = open_serial_test_db();
        let (storage, root) = create_exact_serial_store_in_its_own_task(
            &db,
            storage,
            "circle-serial-head-bytes",
            &founder,
        )
        .await;
        let device_id = local_device_id(&db).await;
        let coordination = storage.serial_coordination().expect("Serial coordination");
        let authorization =
            super::super::store_outbound::current_serial_authorization(&db, &storage, coordination)
                .await
                .expect("founder authorization");
        let entry = authorization
            .membership
            .signed_set_member(
                &founder,
                keys::public_key_hex(&successor),
                None,
                MemberRole::Member,
                "0000000000001-0000-founder".to_string(),
            )
            .expect("sign membership entry");
        let prepared = super::super::store_outbound::prepare_serial_control(
            &db,
            &storage,
            coordination,
            &device_id,
            StoreControl::SerialMembership { entry },
            &founder,
        )
        .await
        .expect("prepare serial control");
        super::super::store_outbound::activate_serial_control(
            &db,
            &storage,
            coordination,
            &prepared,
        )
        .await
        .expect("activate serial control");

        let journal = prepare_circle_operation(
            &db,
            &storage,
            Some(coordination),
            &device_id,
            "0000000001000-0000-founder",
            "Same-position conflict",
            &founder,
        )
        .await
        .expect("prepare Circle operation at the first exact head");
        let CircleOperationPolicy::Serial {
            base: Some(base), ..
        } = &journal.policy
        else {
            panic!("expected Serial Circle operation with a base")
        };
        let (_, author_registration, _author, device_signer) =
            super::super::store_outbound::load_local_store_authority(&db, &device_id, &founder)
                .await
                .expect("load exact Serial founder authority");
        let mut competing_commit = base.clone();
        competing_commit.object = super::super::storage::ExactObjectRef::new(
            base.object.slot().clone(),
            base.object.stored_size(),
            ObjectHash::digest(b"different exact commit object"),
        );
        let competing = StoreSerialHead::signed(
            root.store_root_hash,
            StoreSerialHeadState::Commit {
                author_registration,
                commit: competing_commit,
            },
            &device_signer,
        )
        .expect("sign different exact commit reference at the same coordinate");
        let current = coordination
            .read_head(serial_head_key())
            .await
            .expect("read first head");
        coordination
            .replace_head(serial_head_key(), &current.version, &competing.to_bytes())
            .await
            .expect("install different same-position head");
        db.insert_circle_operation(journal.clone())
            .await
            .expect("persist Circle operation");

        let error = publish_circle_operation(
            &db,
            &storage,
            Some(coordination),
            journal.circle_id(),
            &founder,
        )
        .await
        .expect_err("same-position head substitution must block activation");

        assert!(matches!(error, CircleOperationError::Blocked { .. }));
        assert!(home
            .get(journal.commit_ref.object.slot().logical_key())
            .is_none());
    }

    #[tokio::test]
    async fn serial_circle_matching_head_without_its_commit_does_not_activate_or_reappend() {
        let home = InMemoryCloudHome::new();
        let founder = UserKeypair::generate();
        let storage = serial_storage(&home, &founder, "circle-serial-missing-commit");
        let db = open_serial_test_db();
        let (storage, _root) = create_exact_serial_store_in_its_own_task(
            &db,
            storage,
            "circle-serial-missing-commit",
            &founder,
        )
        .await;
        let device_id = local_device_id(&db).await;
        let coordination = storage.serial_coordination().expect("Serial coordination");
        let journal = prepare_circle_operation(
            &db,
            &storage,
            Some(coordination),
            &device_id,
            "0000000001000-0000-founder",
            "Missing commit",
            &founder,
        )
        .await
        .expect("prepare Circle operation");
        let CircleOperationPolicy::Serial { head, .. } = &journal.policy else {
            panic!("expected Serial Circle head")
        };
        let current_head = coordination
            .read_head(serial_head_key())
            .await
            .expect("read current Serial head");
        coordination
            .replace_head(serial_head_key(), &current_head.version, &head.to_bytes())
            .await
            .expect("publish head without its exact commit");
        db.insert_circle_operation(journal.clone())
            .await
            .expect("persist Circle operation");

        publish_circle_operation(
            &db,
            &storage,
            Some(coordination),
            journal.circle_id(),
            &founder,
        )
        .await
        .expect_err("an activated head cannot repair or trust an absent commit");

        assert_eq!(activation_count(&db, journal.circle_id()).await, 0);
        assert!(db
            .circle_operation(journal.circle_id())
            .await
            .expect("read Circle journal")
            .is_some());
        assert!(home
            .get(journal.commit_ref.object.slot().logical_key())
            .is_none());
    }

    #[tokio::test]
    async fn stale_serial_head_blocks_the_exact_operation_without_activation() {
        let home = InMemoryCloudHome::new();
        let signer = UserKeypair::generate();
        let storage = serial_storage(&home, &signer, "circle-stale-serial");
        let db = open_serial_test_db();
        let (storage, _root) =
            create_exact_serial_store_in_its_own_task(&db, storage, "circle-stale-serial", &signer)
                .await;
        let device_id = local_device_id(&db).await;
        let coordination = storage.serial_coordination().expect("Serial coordination");
        let expected = prepare_circle_operation(
            &db,
            &storage,
            Some(coordination),
            &device_id,
            "0000000001000-0000-creator",
            "Household",
            &signer,
        )
        .await
        .expect("prepare Serial circle operation");
        db.insert_circle_operation(expected.clone())
            .await
            .expect("persist Serial circle operation");

        host_exec(
            &db,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('serial-competitor', 'Competing write', NULL, 1, \
                     '0000000001001-0000-creator', '2026-01-01')",
        )
        .await;
        let (_store_temp, store_dir) = temp_store_dir();
        assert!(
            super::super::store_outbound::prepare_pending_store_write_with_coordination(
                &db,
                &storage,
                Some(coordination),
                &device_id,
                "0000000001001-0000-creator",
                &signer,
                &store_dir,
                None,
            )
            .await
            .expect("prepare competing Serial Store write")
        );
        super::super::store_outbound::drain_store_writes_with_coordination(
            &db,
            &storage,
            Some(coordination),
        )
        .await
        .expect("activate competing Serial Store write");
        let competing = db
            .latest_local_store_position()
            .await
            .expect("read competing Serial position")
            .expect("competing Serial write is materialized");

        let error = publish_circle_operation(
            &db,
            &storage,
            Some(coordination),
            expected.circle_id(),
            &signer,
        )
        .await
        .expect_err("stale Serial base must lose head activation");
        assert!(
            matches!(error, CircleOperationError::Blocked { .. }),
            "{error}"
        );
        let blocked = db
            .circle_operation(expected.circle_id())
            .await
            .expect("read blocked Serial operation")
            .expect("blocked Serial operation remains durable");
        assert_exact_operation(&expected, &blocked);
        assert!(matches!(
            blocked.status,
            CircleOperationState::Blocked { .. }
        ));
        let operations = db
            .get_circle_operations()
            .await
            .expect("read blocked circle operations");
        assert_eq!(operations.len(), 1);
        assert!(matches!(
            operations[0].state,
            crate::sync::circle::CircleOperationState::Blocked { .. }
        ));
        assert!(db
            .get_circles(&keys::public_key_hex(&signer))
            .await
            .expect("read inactive circles")
            .is_empty());
        assert_eq!(activation_count(&db, expected.circle_id()).await, 0);
        assert_eq!(
            db.materialized_frontier().await.expect("read frontier"),
            BTreeMap::from([(SERIAL_STREAM_ID.to_string(), competing)])
        );

        resume_circle_operations(&db, &storage, Some(coordination), &signer)
            .await
            .expect("blocked circle operation remains inert during resume");
        db.discard_blocked_circle_operation(expected.circle_id())
            .await
            .expect("discard blocked circle operation");
        db.discard_blocked_circle_operation(expected.circle_id())
            .await
            .expect("repeated discard remains idempotent");
        assert!(db
            .circle_operation(expected.circle_id())
            .await
            .expect("read discarded circle operation")
            .is_none());
        assert!(db
            .get_circle_operations()
            .await
            .expect("read circle operations after discard")
            .is_empty());
    }
}

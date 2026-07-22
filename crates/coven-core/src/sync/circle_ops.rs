//! Durable creation and activation of circles through the Store commit stream.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::circle::{
    circle_control_head_prefix, circle_metadata_head_prefix, circle_roster_head_prefix,
    circle_semantic_prefix, CircleAccessDisposition, CircleId, CircleMetadataHeadRef,
    CircleOperationId, CircleOperationKind, CircleOperationState, CircleRosterHeadRef,
    CircleSemanticSlot, CircleTransitionDraft, CircleTransitionPolicyObjects,
    PreparedCircleTransition, StoreMembershipStateRef,
};
use super::storage::{
    ExactObjectRef, PreparedExactObject, ProtocolObjectContext, ProtocolObjectDomain, SyncStorage,
};
use super::store_commit::{
    circle_access_envelope_semantic_prefix, circle_access_leaf_semantic_prefix,
    commit_semantic_prefix, head_slot_prefix, CandidateFamilyId, CircleAccessEnvelopeObjectRef,
    CircleAccessLeafObjectRef, CircleAccessObjectRef, CircleActivationObjects,
    CircleMetadataObjectRef, GrantStreamAnchor, ObjectHash, StoreBatchCommit, StoreBatchCommitRef,
    StoreCommitCoord, StoreCommitOperationsInput, StoreCommitOrder, StoreDeviceHead,
    StoreDeviceRegistration, StoreDeviceRegistrationRef, StoreOperationMembershipAuthority,
    StoreRootRef, StreamActivation, StreamAnchorDomain, SuccessorLink,
};
use crate::database::Database;
use crate::encryption::{EncryptionService, MasterKeyring};
use crate::keys::{self, UserKeypair};

pub(crate) use super::circle_activation::{
    load_circle_activations, load_exact_slot_bytes, verify_control_context, CircleAuthoringState,
    VerifiedCircleAccess, VerifiedCircleActivations, VerifiedCircleActive, VerifiedCircleReference,
};

#[cfg(test)]
use super::circle::CircleRole;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CircleOperationPolicy {
    pub head: StoreDeviceHead,
    pub history_summary: super::store_commit::RetainedVerifiedMergeHistorySummary,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PreparedCircleOperation {
    pub creation: PreparedCircleTransition,
    pub history: CircleTransitionHistory,
    pub commit_bytes: Vec<u8>,
    pub commit_ref: StoreBatchCommitRef,
    pub prepared_objects: BTreeMap<String, PreparedExactObject>,
    pub policy: CircleOperationPolicy,
    pub uploaded: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum CircleTransitionHistory {
    Founder,
    Successor(Box<super::store_commit::CircleControlRef>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum CircleOperationIntent {
    Create { name: String },
    Rename { name: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum CircleOperationProgress {
    Ready(Box<PreparedCircleOperation>),
    Blocked {
        reason: String,
        operation: Box<PreparedCircleOperation>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CircleOperationJournal {
    pub operation_id: CircleOperationId,
    pub circle_id: CircleId,
    pub intent: CircleOperationIntent,
    pub progress: CircleOperationProgress,
}

impl CircleOperationJournal {
    pub(crate) fn circle_id(&self) -> CircleId {
        self.circle_id
    }

    pub(crate) fn operation(&self) -> &PreparedCircleOperation {
        match &self.progress {
            CircleOperationProgress::Ready(operation)
            | CircleOperationProgress::Blocked { operation, .. } => operation,
        }
    }

    pub(crate) fn operation_mut(&mut self) -> &mut PreparedCircleOperation {
        match &mut self.progress {
            CircleOperationProgress::Ready(operation)
            | CircleOperationProgress::Blocked { operation, .. } => operation,
        }
    }

    pub(crate) fn closed_remote_objects(
        &self,
    ) -> Result<Vec<super::remote_object::RemoteObjectRecord>, CircleOperationError> {
        let operation = self.operation();
        let commit: StoreBatchCommit = serde_json::from_slice(&operation.commit_bytes)
            .map_err(|error| CircleOperationError::Journal(format!("Circle commit: {error}")))?;
        operation
            .commit_ref
            .verify_commit(&commit)
            .map_err(|error| CircleOperationError::Journal(error.to_string()))?;
        let access_refs = commit
            .circle_controls()
            .iter()
            .flat_map(|control| control.objects().access.iter())
            .collect::<Vec<_>>();
        if access_refs.len() != operation.creation.access.len() {
            return Err(CircleOperationError::Journal(
                "Circle access material does not cover the signed candidate graph".to_string(),
            ));
        }
        let prepared_for = |object: &ExactObjectRef| {
            operation
                .prepared_objects
                .values()
                .find(|prepared| prepared.reference() == object)
                .ok_or_else(|| {
                    CircleOperationError::Journal(format!(
                        "Circle candidate object {} has no prepared bytes",
                        super::remote_object::remote_object_id(object)
                    ))
                })
        };
        let mut materials = Vec::with_capacity(access_refs.len() * 2);
        for (access, reference) in operation.creation.access.iter().zip(access_refs) {
            let leaf = prepared_for(&reference.leaf.object)?;
            materials.push(super::remote_object::CandidateObjectMaterial {
                object: reference.leaf.object.clone(),
                canonical_semantic_bytes: serde_json::to_vec(&access.leaf.value).map_err(
                    |error| CircleOperationError::Journal(format!("Circle access leaf: {error}")),
                )?,
                stored_bytes: leaf.stored_bytes().to_vec(),
            });
            let envelope = prepared_for(&reference.envelope.object)?;
            materials.push(super::remote_object::CandidateObjectMaterial {
                object: reference.envelope.object.clone(),
                canonical_semantic_bytes: serde_json::to_vec(&access.envelope).map_err(
                    |error| {
                        CircleOperationError::Journal(format!("Circle access envelope: {error}"))
                    },
                )?,
                stored_bytes: envelope.stored_bytes().to_vec(),
            });
        }
        let mut remotes = super::remote_object::CandidateObjectGraph::from_commit(&commit)
            .and_then(|graph| graph.close(&commit, &operation.commit_ref, materials))
            .map_err(|error| CircleOperationError::Journal(error.to_string()))?;
        let commit_prepared = operation
            .prepared_objects
            .get("store-commit")
            .ok_or_else(|| {
                CircleOperationError::Journal(
                    "Circle operation lacks its prepared Store commit".to_string(),
                )
            })?;
        remotes.push(
            super::remote_object::RemoteObjectRecord::candidate_commit(
                operation.commit_ref.clone(),
                operation.commit_bytes.clone(),
                commit_prepared.stored_bytes().to_vec(),
            )
            .map_err(|error| CircleOperationError::Journal(error.to_string()))?,
        );
        let prepared = operation
            .prepared_objects
            .get("store-head")
            .ok_or_else(|| {
                CircleOperationError::Journal(
                    "Circle operation lacks its prepared Store head".to_string(),
                )
            })?;
        remotes.push(
            super::remote_object::RemoteObjectRecord::candidate_activated_store_head(
                super::store_commit::StoreDeviceHeadRef {
                    head_hash: operation.policy.head.head_hash(),
                    object: prepared.reference().clone(),
                },
                operation.policy.head.to_bytes(),
                prepared.stored_bytes().to_vec(),
                operation.commit_ref.clone(),
            )
            .map_err(|error| CircleOperationError::Journal(error.to_string()))?,
        );
        Ok(remotes)
    }

    pub(crate) fn state(&self) -> CircleOperationState {
        match &self.progress {
            CircleOperationProgress::Ready(_) => CircleOperationState::Pending,
            CircleOperationProgress::Blocked { reason, .. } => CircleOperationState::Blocked {
                reason: reason.clone(),
            },
        }
    }

    pub(crate) fn block(&mut self, reason: String) -> Result<(), CircleOperationError> {
        let CircleOperationProgress::Ready(operation) = &mut self.progress else {
            return Err(CircleOperationError::Journal(format!(
                "Circle operation {} is already blocked",
                self.operation_id
            )));
        };
        let operation = operation.clone();
        self.progress = CircleOperationProgress::Blocked { reason, operation };
        Ok(())
    }

    pub(crate) fn commit(&self) -> Result<StoreBatchCommit, CircleOperationError> {
        serde_json::from_slice(&self.operation().commit_bytes)
            .map_err(|error| CircleOperationError::Journal(format!("parse Store commit: {error}")))
    }

    pub(crate) fn validate_identity(&self) -> Result<(), CircleOperationError> {
        if self.operation().creation.circle_id != self.circle_id {
            return Err(CircleOperationError::Journal(format!(
                "circle operation {} payload names circle {} but its operation names circle {}",
                self.operation_id,
                self.circle_id,
                self.operation().creation.circle_id
            )));
        }
        let commit = self.commit()?;
        if commit.write_id.as_str() != self.operation_id.as_str() {
            return Err(CircleOperationError::Journal(format!(
                "circle operation id {} differs from payload commit operation id {}",
                self.operation_id, commit.write_id
            )));
        }
        Ok(())
    }

    pub(crate) fn kind(&self) -> CircleOperationKind {
        match self.intent {
            CircleOperationIntent::Create { .. } => CircleOperationKind::Create,
            CircleOperationIntent::Rename { .. } => CircleOperationKind::Rename,
        }
    }
}

fn verify_prepared_objects_are_signed(
    journal: &CircleOperationJournal,
    reference: &super::store_commit::CircleControlRef,
) -> Result<(), CircleOperationError> {
    let operation = journal.operation();
    let objects = reference.objects();
    let mut signed = BTreeSet::<ExactObjectRef>::from([
        operation.commit_ref.object.clone(),
        objects.control.clone(),
    ]);
    signed.insert(reference.head_object().clone());
    signed.extend(objects.roster_entries.values().cloned());
    signed.extend(objects.roster_heads.iter().map(|head| head.object.clone()));
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
            .iter()
            .map(|head| head.object.clone()),
    );
    for access in &objects.access {
        signed.insert(access.leaf.object.clone());
        signed.insert(access.envelope.object.clone());
    }
    for (step, prepared) in &operation.prepared_objects {
        if step != "store-head" && !signed.contains(prepared.reference()) {
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
    membership_authority: StoreOperationMembershipAuthority,
    circle_reference: super::store_commit::CircleControlRef,
    stream_activations: Vec<StreamActivation>,
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
            device_join_attempt_decisions: Vec::new(),
            device_join_outcomes: Vec::new(),
            device_join_cleanup_receipts: Vec::new(),
            provider_access_grants: Vec::new(),
            provider_access_withdrawals: Vec::new(),
            device_registrations: Vec::new(),
            device_exclusion_proposals: Vec::new(),
            device_exclusion_outcomes: Vec::new(),
            stream_activations,
            circle_controls: vec![circle_reference],
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

fn prepare_circle_object_at(
    storage: &dyn SyncStorage,
    context: &ProtocolObjectContext,
    slot: crate::storage::cloud::ObjectSlot,
    semantic_prefix: &str,
    bytes: Vec<u8>,
) -> Result<PreparedExactObject, CircleOperationError> {
    storage
        .prepare_protocol_object(context, slot, semantic_prefix, bytes)
        .map_err(super::store_objects::StoreObjectError::from)
        .map_err(CircleOperationError::from)
}

async fn prepare_circle_activation_objects(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    mut draft: CircleTransitionDraft,
    history: &CircleTransitionHistory,
    candidate_family: CandidateFamilyId,
    author_registration: &StoreDeviceRegistrationRef,
    author: &StoreDeviceRegistration,
    identity_signer: &UserKeypair,
    device_signer: &UserKeypair,
) -> Result<
    (
        PreparedCircleTransition,
        CircleActivationObjects,
        BTreeMap<String, PreparedExactObject>,
        Option<ExactObjectRef>,
        Vec<StreamActivation>,
    ),
    CircleOperationError,
> {
    let store_root_hash = root.store_root_hash;
    let encryption = EncryptionService::from(
        MasterKeyring::from_serialized(&draft.keyring)
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?,
    );
    let metadata_encryption = encryption
        .service_for_fingerprint(draft.metadata.key_fingerprint.as_bytes())
        .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
    let metadata_context = ProtocolObjectContext::circle(
        store_root_hash,
        ProtocolObjectDomain::CircleMetadata,
        metadata_encryption,
    );
    let roster_context = ProtocolObjectContext::circle(
        store_root_hash,
        ProtocolObjectDomain::CircleRoster,
        encryption.clone(),
    );
    let control_context = ProtocolObjectContext::store_encrypted(
        store_root_hash,
        ProtocolObjectDomain::CircleControl,
    );
    let previous_control = match history {
        CircleTransitionHistory::Founder => None,
        CircleTransitionHistory::Successor(reference) => Some(reference.as_ref()),
    };
    let previous_objects = previous_control.map(super::store_commit::CircleControlRef::objects);
    let mut roster_entries =
        previous_objects.map_or_else(BTreeMap::new, |objects| objects.roster_entries.clone());
    let mut roster_heads =
        previous_objects.map_or_else(Vec::new, |objects| objects.roster_heads.clone());
    let roster_resolutions =
        previous_objects.map_or_else(BTreeMap::new, |objects| objects.roster_resolutions.clone());
    let mut metadata_entries =
        previous_objects.map_or_else(BTreeMap::new, |objects| objects.metadata_entries.clone());
    let mut metadata_heads =
        previous_objects.map_or_else(Vec::new, |objects| objects.metadata_heads.clone());
    let mut prepared = BTreeMap::new();
    let mut stream_activations = Vec::new();

    let roster_entry = &mut draft.policy.roster_entry;
    let policy_objects = {
        let owner_grant = draft.metadata.author_owner_grant.clone();
        let selects_authored_metadata =
            draft.control.value.value.active_epoch.metadata.selected == draft.metadata.coord();
        let roster_stream = StreamActivation::grant_authorized_stream_id(
            store_root_hash,
            author_registration,
            &owner_grant,
            StreamAnchorDomain::CircleRoster {
                circle_id: draft.circle_id,
            },
        );
        let metadata_stream = StreamActivation::grant_authorized_stream_id(
            store_root_hash,
            author_registration,
            &owner_grant,
            StreamAnchorDomain::CircleMetadata {
                circle_id: draft.circle_id,
            },
        );
        let control_stream = StreamActivation::grant_authorized_stream_id(
            store_root_hash,
            author_registration,
            &owner_grant,
            StreamAnchorDomain::CircleControl {
                circle_id: draft.circle_id,
            },
        );
        if roster_stream == metadata_stream
            || roster_stream == control_stream
            || metadata_stream == control_stream
        {
            return Err(CircleOperationError::InvalidState(
                "Circle control, roster, and metadata domains derived the same stream".to_string(),
            ));
        }

        let prepared_roster = if let Some(entry) = roster_entry {
            entry.stream_id = roster_stream;
            entry.signature = keys::sign_hex(identity_signer, &entry.canonical_bytes()).1;
            let entry_prefix = circle_semantic_prefix(CircleSemanticSlot::RosterEntry {
                circle_id: draft.circle_id,
                coord: &entry.coord(),
            });
            let entry_prepared = prepare_circle_object(
                storage,
                &roster_context,
                &entry_prefix,
                ".json",
                serde_json::to_vec(entry).expect("Circle roster entry serialization cannot fail"),
            )
            .await?;
            prepared.insert("roster-entry".to_string(), entry_prepared.clone());
            roster_entries.insert(entry.coord(), entry_prepared.reference().clone());

            let stream_key = entry.coord().stream_key();
            if roster_heads
                .iter()
                .any(|head| head.coord.stream_key() == stream_key)
            {
                return Err(CircleOperationError::InvalidState(
                    "Circle founder roster stream is already active".to_string(),
                ));
            }
            let current_prefix = circle_roster_head_prefix(draft.circle_id, &stream_key, 1);
            let current_slot = storage
                .allocate_protocol_slot(&roster_context, &current_prefix, ".json")
                .await
                .map_err(super::store_objects::StoreObjectError::from)?;
            let activation = StreamActivation::grant_authorized(
                store_root_hash,
                author_registration.clone(),
                owner_grant.clone(),
                GrantStreamAnchor::CircleRoster {
                    circle_id: draft.circle_id,
                    first_slot: current_slot.clone(),
                },
            );
            let next_slot = storage
                .allocate_protocol_slot(
                    &roster_context,
                    &circle_roster_head_prefix(draft.circle_id, &stream_key, 2),
                    ".json",
                )
                .await
                .map_err(super::store_objects::StoreObjectError::from)?;
            let head = super::circle::CircleRosterHead::signed(
                entry,
                entry_prepared.reference().clone(),
                SuccessorLink {
                    activation: activation.activation_id(),
                    predecessor: None,
                    next_slot,
                },
                device_signer,
            );
            let head_prepared = prepare_circle_object_at(
                storage,
                &roster_context,
                current_slot,
                &current_prefix,
                serde_json::to_vec(&head).expect("Circle roster head serialization cannot fail"),
            )?;
            let head_ref =
                CircleRosterHeadRef::from_stored_head(&head, head_prepared.reference().clone());
            prepared.insert("roster-head".to_string(), head_prepared);
            roster_heads.push(head_ref.clone());
            roster_heads.sort_by_key(|head| head.coord.stream_key());
            stream_activations.push(activation);
            Some((entry.clone(), head, head_ref))
        } else {
            None
        };

        if let Some((entry, head, reference)) = &prepared_roster {
            let exact_head =
                super::circle::ExactCircleRosterHead::bind(head.clone(), reference.clone())
                    .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
            draft.roster = super::circle::CircleRosterChain::from_entries_with_heads(
                vec![entry.clone()],
                vec![exact_head],
            )
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?
            .resolved();
        }

        let roster_state = super::circle::MergeCircleRosterStateRef {
            heads: roster_heads.clone(),
            resolutions: roster_resolutions.keys().cloned().collect(),
            state_hash: draft.roster.state_hash,
        };
        draft.metadata.author_roster = roster_state.clone();

        let prior_metadata = metadata_heads
            .iter()
            .find(|head| head.coord.stream_id == metadata_stream)
            .cloned();
        let (metadata_slot, metadata_seq, metadata_previous, metadata_activation) =
            if let Some(reference) = &prior_metadata {
                let prefix = circle_semantic_prefix(CircleSemanticSlot::MetadataHead {
                    circle_id: draft.circle_id,
                    head: reference,
                });
                let bytes =
                    load_exact_slot_bytes(storage, &metadata_context, &reference.object, &prefix)
                        .await?;
                let head: super::circle::CircleMetadataHead = serde_json::from_slice(&bytes)
                    .map_err(|error| {
                        CircleOperationError::InvalidState(format!(
                            "parse predecessor Circle metadata head: {error}"
                        ))
                    })?;
                if !head.verify_for_registration(author) || head.coord() != reference.coord {
                    return Err(CircleOperationError::InvalidState(
                        "Circle metadata predecessor head failed verification".to_string(),
                    ));
                }
                (
                    head.successor.next_slot.clone(),
                    head.seq.checked_add(1).ok_or_else(|| {
                        CircleOperationError::InvalidState(
                            "Circle metadata sequence overflow".to_string(),
                        )
                    })?,
                    Some(head.tip_hash),
                    None,
                )
            } else {
                let stream_key = super::circle::CircleAuthorStreamKey {
                    author_pubkey: draft.metadata.author_pubkey.clone(),
                    device_id: draft.metadata.device_id.clone(),
                    stream_id: metadata_stream,
                    author_owner_grant: owner_grant.clone(),
                };
                let prefix = circle_metadata_head_prefix(draft.circle_id, &stream_key, 1);
                let slot = storage
                    .allocate_protocol_slot(&metadata_context, &prefix, ".json")
                    .await
                    .map_err(super::store_objects::StoreObjectError::from)?;
                let activation = StreamActivation::grant_authorized(
                    store_root_hash,
                    author_registration.clone(),
                    owner_grant.clone(),
                    GrantStreamAnchor::CircleMetadata {
                        circle_id: draft.circle_id,
                        first_slot: slot.clone(),
                    },
                );
                (slot, 1, None, Some(activation))
            };
        draft.metadata.stream_id = metadata_stream;
        draft.metadata.seq = metadata_seq;
        draft.metadata.previous_hash = metadata_previous;
        draft.metadata.dependencies = metadata_heads
            .iter()
            .map(|head| head.coord.clone())
            .collect();
        draft.metadata.signature =
            keys::sign_hex(identity_signer, &draft.metadata.canonical_bytes()).1;
        let metadata_prefix = circle_semantic_prefix(CircleSemanticSlot::MetadataEntry {
            circle_id: draft.circle_id,
            coord: &draft.metadata.coord(),
        });
        let metadata_prepared = prepare_circle_object(
            storage,
            &metadata_context,
            &metadata_prefix,
            ".json",
            serde_json::to_vec(&draft.metadata).expect("Circle metadata serialization cannot fail"),
        )
        .await?;
        prepared.insert("metadata".to_string(), metadata_prepared.clone());
        metadata_entries.insert(
            draft.metadata.coord(),
            CircleMetadataObjectRef {
                key_fingerprint: draft.metadata.key_fingerprint,
                object: metadata_prepared.reference().clone(),
            },
        );
        let metadata_activation_id = match &metadata_activation {
            Some(activation) => activation.activation_id(),
            None => {
                let reference = prior_metadata.as_ref().expect("prior metadata head");
                let prefix = circle_semantic_prefix(CircleSemanticSlot::MetadataHead {
                    circle_id: draft.circle_id,
                    head: reference,
                });
                let bytes =
                    load_exact_slot_bytes(storage, &metadata_context, &reference.object, &prefix)
                        .await?;
                let head: super::circle::CircleMetadataHead = serde_json::from_slice(&bytes)
                    .map_err(|error| {
                        CircleOperationError::InvalidState(format!(
                            "parse predecessor Circle metadata head: {error}"
                        ))
                    })?;
                head.successor.activation
            }
        };
        let metadata_stream_key = draft.metadata.coord().stream_key();
        let metadata_next_slot = storage
            .allocate_protocol_slot(
                &metadata_context,
                &circle_metadata_head_prefix(
                    draft.circle_id,
                    &metadata_stream_key,
                    metadata_seq.checked_add(1).ok_or_else(|| {
                        CircleOperationError::InvalidState(
                            "Circle metadata sequence overflow".to_string(),
                        )
                    })?,
                ),
                ".json",
            )
            .await
            .map_err(super::store_objects::StoreObjectError::from)?;
        let metadata_head = super::circle::CircleMetadataHead::signed(
            &draft.metadata,
            metadata_prepared.reference().clone(),
            SuccessorLink {
                activation: metadata_activation_id,
                predecessor: prior_metadata.as_ref().map(|head| head.object.clone()),
                next_slot: metadata_next_slot,
            },
            device_signer,
        );
        let metadata_head_prefix =
            circle_metadata_head_prefix(draft.circle_id, &metadata_stream_key, metadata_seq);
        let metadata_head_prepared = prepare_circle_object_at(
            storage,
            &metadata_context,
            metadata_slot,
            &metadata_head_prefix,
            serde_json::to_vec(&metadata_head)
                .expect("Circle metadata head serialization cannot fail"),
        )?;
        let metadata_head_ref = CircleMetadataHeadRef::from_stored_head(
            &metadata_head,
            metadata_head_prepared.reference().clone(),
        );
        prepared.insert("metadata-head".to_string(), metadata_head_prepared);
        metadata_heads.retain(|head| head.coord.stream_id != metadata_stream);
        metadata_heads.push(metadata_head_ref);
        metadata_heads.sort_by_key(|head| head.coord.stream_key());
        if let Some(activation) = metadata_activation {
            stream_activations.push(activation);
        }

        let selected = if selects_authored_metadata
            || draft
                .control
                .value
                .value
                .active_epoch
                .metadata
                .heads
                .is_empty()
        {
            draft.metadata.coord()
        } else {
            draft
                .control
                .value
                .value
                .active_epoch
                .metadata
                .selected
                .clone()
        };
        let metadata_state = super::circle::MergeCircleMetadataStateRef {
            heads: metadata_heads.clone(),
            selected: selected.clone(),
            state_hash: if selected == draft.metadata.coord() {
                draft.metadata.metadata_hash()
            } else {
                draft.control.value.value.active_epoch.metadata.state_hash
            },
        };

        for access in &mut draft.access {
            if let CircleAccessDisposition::Active { roster, .. } =
                &mut access.leaf.value.disposition
            {
                *roster = roster_state.clone();
            }
            access.leaf.value.signature =
                keys::sign_hex(identity_signer, &access.leaf.value.canonical_bytes()).1;
            let recipient_ed25519: [u8; keys::SIGN_PUBLICKEYBYTES] =
                hex::decode(&access.leaf.value.recipient_pubkey)
                    .map_err(|_| {
                        CircleOperationError::InvalidState(
                            "Circle access recipient key is malformed".to_string(),
                        )
                    })?
                    .try_into()
                    .map_err(|_| {
                        CircleOperationError::InvalidState(
                            "Circle access recipient key has the wrong length".to_string(),
                        )
                    })?;
            let recipient_x25519 =
                keys::ed25519_to_x25519_public_key(&recipient_ed25519).map_err(|error| {
                    CircleOperationError::InvalidState(format!(
                        "convert Circle access recipient key: {error}"
                    ))
                })?;
            let plaintext = serde_json::to_vec(&access.leaf.value)
                .expect("Circle access serialization cannot fail");
            access.leaf.bytes = keys::seal_box_encrypt(&plaintext, &recipient_x25519);
            access.leaf.leaf_hash = ObjectHash::digest(&access.leaf.bytes);
        }
        let leaf_hashes = draft
            .access
            .iter()
            .map(|access| access.leaf.leaf_hash)
            .collect::<Vec<_>>();
        let (access_root, proofs) = super::circle::merkle_root_and_proofs(&leaf_hashes);

        let mut control_frontier = draft
            .control
            .value
            .value
            .active_epoch
            .covered_control_heads
            .clone();
        if let Some(previous) = previous_control {
            let head_hash = previous.head_hash();
            let head_object = previous.head_object();
            control_frontier
                .retain(|head| head.coord.stream_key() != previous.control().stream_key());
            control_frontier.push(super::circle::MergeCircleControlHeadRef {
                coord: previous.control().clone(),
                head_hash,
                object: head_object.clone(),
            });
        }
        control_frontier.sort_by_key(|head| head.coord.stream_key());
        let prior_control = control_frontier
            .iter()
            .find(|head| head.coord.stream_key().stream_id == control_stream)
            .cloned();
        let (control_slot, control_seq, control_previous, control_activation) =
            if let Some(reference) = &prior_control {
                let prefix = circle_semantic_prefix(CircleSemanticSlot::ControlHead {
                    circle_id: draft.circle_id,
                    control: &reference.coord,
                    head_hash: reference.head_hash,
                });
                let bytes =
                    load_exact_slot_bytes(storage, &control_context, &reference.object, &prefix)
                        .await?;
                let head: super::circle::CircleControlHead = serde_json::from_slice(&bytes)
                    .map_err(|error| {
                        CircleOperationError::InvalidState(format!(
                            "parse predecessor Circle control head: {error}"
                        ))
                    })?;
                (
                    head.successor.next_slot.clone(),
                    head.control.seq.checked_add(1).ok_or_else(|| {
                        CircleOperationError::InvalidState(
                            "Circle control sequence overflow".to_string(),
                        )
                    })?,
                    Some(head.control.control_hash()),
                    None,
                )
            } else {
                let stream_key = super::circle::CircleAuthorStreamKey {
                    author_pubkey: draft.control.value.author_pubkey.clone(),
                    device_id: author_registration.device_id.to_string(),
                    stream_id: control_stream,
                    author_owner_grant: owner_grant.clone(),
                };
                let prefix = circle_control_head_prefix(draft.circle_id, &stream_key, 1);
                let slot = storage
                    .allocate_protocol_slot(&control_context, &prefix, ".json")
                    .await
                    .map_err(super::store_objects::StoreObjectError::from)?;
                let activation = StreamActivation::grant_authorized(
                    store_root_hash,
                    author_registration.clone(),
                    owner_grant.clone(),
                    GrantStreamAnchor::CircleControl {
                        circle_id: draft.circle_id,
                        first_slot: slot.clone(),
                    },
                );
                (slot, 1, None, Some(activation))
            };

        let super::circle::CircleControlValue {
            order,
            active_epoch,
            author_authority,
            membership_authority: _,
        } = &mut draft.control.value.value;
        order.device_id = author_registration.device_id.to_string();
        order.stream_id = control_stream;
        order.author_owner_grant = owner_grant.clone();
        order.seq = control_seq;
        order.previous_control_hash = control_previous;
        order.dependencies = control_frontier
            .iter()
            .filter(|head| head.coord.stream_key().stream_id != control_stream)
            .map(|head| head.coord.clone())
            .collect();
        active_epoch.roster = roster_state.clone();
        active_epoch.metadata = metadata_state;
        active_epoch.common.access_root = access_root;
        active_epoch.covered_control_heads = control_frontier;
        if let (
            Some((entry, _, _)),
            super::circle::MergeCircleOwnerAuthorityRef::Roster {
                roster, created_at, ..
            },
        ) = (&prepared_roster, author_authority)
        {
            *roster = roster_state;
            *created_at = entry.coord();
        }
        draft.control.value.signature =
            keys::sign_hex(identity_signer, &draft.control.value.canonical_bytes()).1;
        draft.control.coord = draft.control.value.coord();
        draft.control.bytes = serde_json::to_vec(&draft.control.value)
            .expect("Circle control serialization cannot fail");

        for (access, proof) in draft.access.iter_mut().zip(proofs) {
            access.envelope.control_hash = draft.control.coord.control_hash();
            access.envelope.leaf_hash = access.leaf.leaf_hash;
            access.envelope.value_hash = ObjectHash::digest(
                &serde_json::to_vec(&access.leaf.value)
                    .expect("Circle access leaf serialization cannot fail"),
            );
            access.envelope.proof = proof;
            access.envelope.signature =
                keys::sign_hex(identity_signer, &access.envelope.canonical_bytes()).1;
        }

        let control_prefix = circle_semantic_prefix(CircleSemanticSlot::Control {
            circle_id: draft.circle_id,
            control: &draft.control.coord,
        });
        let control_prepared = prepare_circle_object(
            storage,
            &control_context,
            &control_prefix,
            ".json",
            draft.control.bytes.clone(),
        )
        .await?;
        prepared.insert("control".to_string(), control_prepared.clone());
        let control_activation_id = match &control_activation {
            Some(activation) => activation.activation_id(),
            None => {
                let reference = prior_control.as_ref().expect("prior control head");
                let prefix = circle_semantic_prefix(CircleSemanticSlot::ControlHead {
                    circle_id: draft.circle_id,
                    control: &reference.coord,
                    head_hash: reference.head_hash,
                });
                let bytes =
                    load_exact_slot_bytes(storage, &control_context, &reference.object, &prefix)
                        .await?;
                let head: super::circle::CircleControlHead = serde_json::from_slice(&bytes)
                    .map_err(|error| {
                        CircleOperationError::InvalidState(format!(
                            "parse predecessor Circle control head: {error}"
                        ))
                    })?;
                head.successor.activation
            }
        };
        let control_stream_key = draft.control.coord.stream_key();
        let control_next_slot = storage
            .allocate_protocol_slot(
                &control_context,
                &circle_control_head_prefix(
                    draft.circle_id,
                    &control_stream_key,
                    control_seq.checked_add(1).ok_or_else(|| {
                        CircleOperationError::InvalidState(
                            "Circle control sequence overflow".to_string(),
                        )
                    })?,
                ),
                ".json",
            )
            .await
            .map_err(super::store_objects::StoreObjectError::from)?;
        let control_head = super::circle::CircleControlHead::signed(
            &draft.control.value,
            control_prepared.reference().clone(),
            SuccessorLink {
                activation: control_activation_id,
                predecessor: prior_control.as_ref().map(|head| head.object.clone()),
                next_slot: control_next_slot,
            },
            device_signer,
        );
        let control_head_prefix =
            circle_control_head_prefix(draft.circle_id, &control_stream_key, control_seq);
        let control_head_prepared = prepare_circle_object_at(
            storage,
            &control_context,
            control_slot,
            &control_head_prefix,
            serde_json::to_vec(&control_head)
                .expect("Circle control head serialization cannot fail"),
        )?;
        prepared.insert("control-head".to_string(), control_head_prepared.clone());
        if let Some(activation) = control_activation {
            stream_activations.push(activation);
        }

        CircleTransitionPolicyObjects {
            roster_entry: prepared_roster.as_ref().map(|(entry, _, _)| entry.clone()),
            roster_head: prepared_roster.map(|(_, head, _)| head),
            metadata_head,
            control_head,
        }
    };

    let mut access_objects = Vec::with_capacity(draft.access.len());
    for (index, access) in draft.access.iter().enumerate() {
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
        access_objects.push(CircleAccessObjectRef {
            leaf: CircleAccessLeafObjectRef {
                owner_pubkey: access.leaf.value.owner_pubkey.clone(),
                epoch_id: access.leaf.value.epoch_id,
                recipient_slot: access.leaf.value.recipient_slot.clone(),
                leaf_id: access.leaf.value.leaf_id,
                leaf_hash: access.leaf.leaf_hash,
                object: leaf.reference().clone(),
            },
            envelope: CircleAccessEnvelopeObjectRef {
                owner_pubkey: access.envelope.owner_pubkey.clone(),
                recipient_slot: access.envelope.recipient_slot.clone(),
                control_hash: access.envelope.control_hash,
                leaf_id: access.envelope.leaf_id,
                leaf_hash: access.envelope.leaf_hash,
                object: envelope.reference().clone(),
            },
        });
    }

    let control = prepared
        .get("control")
        .ok_or_else(|| {
            CircleOperationError::InvalidState(
                "prepared Circle graph lacks its control object".to_string(),
            )
        })?
        .reference()
        .clone();
    let control_head_object = prepared
        .get("control-head")
        .map(|object| object.reference().clone());
    stream_activations.sort();
    let transition = PreparedCircleTransition {
        circle_id: draft.circle_id,
        epoch_id: draft.epoch_id,
        keyring: draft.keyring,
        roster: draft.roster,
        policy_objects,
        metadata: draft.metadata,
        access: draft.access,
        control: draft.control,
    };
    Ok((
        transition,
        CircleActivationObjects {
            control,
            roster_entries,
            roster_heads,
            roster_resolutions,
            metadata_entries,
            metadata_heads,
            access: access_objects,
        },
        prepared,
        control_head_object,
        stream_activations,
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
    Construction(#[from] super::circle::CircleTransitionError),
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
    device_id: &str,
    metadata_stamp: &str,
    name: &str,
    signer: &UserKeypair,
) -> Result<CircleId, CircleOperationError> {
    super::store_registration::ensure_active_registration(db, storage).await?;
    let journal = Box::pin(prepare_circle_operation(
        db,
        storage,
        device_id,
        metadata_stamp,
        name,
        signer,
    ))
    .await?;
    let circle_id = journal.circle_id();
    let operation_id = journal.operation_id.clone();
    db.insert_circle_operation(journal).await?;
    Box::pin(publish_circle_operation(db, storage, &operation_id, signer)).await?;
    Ok(circle_id)
}

pub(crate) async fn rename_circle(
    db: &Database,
    storage: &dyn SyncStorage,
    device_id: &str,
    metadata_stamp: &str,
    circle_id: CircleId,
    name: &str,
    signer: &UserKeypair,
) -> Result<(), CircleOperationError> {
    super::store_registration::ensure_active_registration(db, storage).await?;
    let identity_pubkey = keys::public_key_hex(signer);
    let (current, activation_commit_ref) = db
        .circle_authoring_context(circle_id, &identity_pubkey)
        .await?;
    let root = db
        .local_store_root_ref()
        .await?
        .ok_or(CircleOperationError::MissingState("Store root reference"))?;
    let (activation_commit, _) =
        super::store_pull::load_commit_with_author(storage, &root, &activation_commit_ref).await?;
    if activation_commit.candidate_family() != current.candidate_family {
        return Err(CircleOperationError::InvalidState(format!(
            "Circle {circle_id} current state differs from its activating Store commit"
        )));
    }
    let reference = activation_commit
        .circle_controls()
        .iter()
        .find(|reference| {
            reference.circle_id() == circle_id && reference.control() == &current.control.coord
        })
        .ok_or_else(|| {
            CircleOperationError::InvalidState(format!(
                "Circle {circle_id} current control is absent from its activating Store commit"
            ))
        })?;
    let journal = Box::pin(prepare_circle_operation_request(
        db,
        storage,
        device_id,
        metadata_stamp,
        CircleOperationRequest::Rename(Box::new(CircleRenameRequest {
            circle_id,
            name: name.to_string(),
            current,
            previous_control: reference.clone(),
        })),
        signer,
    ))
    .await?;
    if journal.circle_id() != circle_id {
        return Err(CircleOperationError::InvalidState(
            "prepared Circle rename changed Circle identity".to_string(),
        ));
    }
    let operation_id = journal.operation_id.clone();
    db.insert_circle_operation(journal).await?;
    Box::pin(publish_circle_operation(db, storage, &operation_id, signer)).await
}

struct CircleRenameRequest {
    circle_id: CircleId,
    name: String,
    current: CircleAuthoringState,
    previous_control: super::store_commit::CircleControlRef,
}

enum CircleOperationRequest {
    Create { name: String },
    Rename(Box<CircleRenameRequest>),
}

impl CircleOperationRequest {
    fn name(&self) -> &str {
        match self {
            Self::Create { name } => name,
            Self::Rename(request) => &request.name,
        }
    }

    fn intent(&self) -> CircleOperationIntent {
        match self {
            Self::Create { name } => CircleOperationIntent::Create { name: name.clone() },
            Self::Rename(request) => CircleOperationIntent::Rename {
                name: request.name.clone(),
            },
        }
    }

    fn history(&self) -> CircleTransitionHistory {
        match self {
            Self::Create { .. } => CircleTransitionHistory::Founder,
            Self::Rename(request) => {
                CircleTransitionHistory::Successor(Box::new(request.previous_control.clone()))
            }
        }
    }
}

pub(crate) async fn resume_circle_operations(
    db: &Database,
    storage: &dyn SyncStorage,
    identity: &UserKeypair,
) -> Result<(), CircleOperationError> {
    while let Some(journal) = db.oldest_pending_circle_operation().await? {
        if !matches!(journal.state(), CircleOperationState::Pending) {
            return Err(CircleOperationError::Journal(format!(
                "pending circle operation {} contains a blocked payload",
                journal.circle_id()
            )));
        }
        match Box::pin(publish_circle_operation(
            db,
            storage,
            &journal.operation_id,
            identity,
        ))
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
    device_id: &str,
    metadata_stamp: &str,
    name: &str,
    signer: &UserKeypair,
) -> Result<CircleOperationJournal, CircleOperationError> {
    Box::pin(prepare_circle_operation_request(
        db,
        storage,
        device_id,
        metadata_stamp,
        CircleOperationRequest::Create {
            name: name.to_string(),
        },
        signer,
    ))
    .await
}

async fn prepare_circle_operation_request(
    db: &Database,
    storage: &dyn SyncStorage,
    device_id: &str,
    metadata_stamp: &str,
    request: CircleOperationRequest,
    signer: &UserKeypair,
) -> Result<CircleOperationJournal, CircleOperationError> {
    let (root, author_registration, author, device_signer) =
        crate::sync::store_engine::engine::operations::load_local_store_authority(
            db, device_id, signer,
        )
        .await?;
    let store_root_hash = root.store_root_hash;
    let circle_device_id = author.device_id.to_string();
    let founder = db
        .get_protocol_state(super::membership_ops::OWNER_PUBKEY_STATE_KEY)
        .await?
        .ok_or(CircleOperationError::MissingState("Store founder"))?;
    let author_pubkey = keys::public_key_hex(signer);
    let write_id = db.new_write_id();
    let operation_id = CircleOperationId::from_write_id(write_id.clone());
    let history = request.history();
    let intent = request.intent();
    let name = request.name();
    let (creation, commit, commit_ref, policy, prepared_objects) = {
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
        let stream_id = super::store_commit::StreamActivation::device_authorized_stream_id(
            root.store_root_hash,
            &author_registration,
            super::store_commit::StreamAnchorDomain::StoreAnnouncements,
        );
        let dependencies =
            super::store_commit::CommitFrontier::from_refs(db.materialized_frontier().await?)
                .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        let coord = StoreCommitCoord {
            stream_id,
            sequence: seq,
        };
        let order = StoreCommitOrder {
            seq,
            predecessor: base.clone(),
            dependencies: dependencies.0,
        };
        let (device_state, resolved_devices) = db.store_device_state_for_order(&order).await?;
        let membership_state = StoreMembershipStateRef::from_parts(
            heads,
            resolutions,
            resolved_devices.recovery.clone(),
            state_hash,
        )
        .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        let candidate_family =
            CandidateFamilyId::derive(store_root_hash, &author_registration, &write_id, &order);
        let creation = match &request {
            CircleOperationRequest::Create { .. } => CircleTransitionDraft::founder(
                store_root_hash,
                candidate_family,
                &circle_device_id,
                name,
                metadata_stamp,
                membership_state.clone(),
                membership_authority.clone(),
                members,
                db.id_provider(),
                signer,
            )?,
            CircleOperationRequest::Rename(request) => {
                if request.circle_id != request.current.control.value.circle_id {
                    return Err(CircleOperationError::InvalidState(
                        "Circle rename request differs from its current control".to_string(),
                    ));
                }
                let keyring = match &request.current.access.disposition {
                    CircleAccessDisposition::Active { keyring, .. } => keyring,
                    CircleAccessDisposition::Inactive => {
                        return Err(CircleOperationError::InvalidState(
                            "Circle rename requires active local access".to_string(),
                        ));
                    }
                };
                CircleTransitionDraft::rename(
                    candidate_family,
                    &circle_device_id,
                    name,
                    metadata_stamp,
                    membership_state.clone(),
                    membership_authority.clone(),
                    members,
                    &request.current.control,
                    &request.current.roster,
                    &request.current.metadata,
                    keyring,
                    db.id_provider(),
                    signer,
                )?
            }
        };
        let (creation, objects, mut prepared_objects, control_head_object, stream_activations) =
            Box::pin(prepare_circle_activation_objects(
                storage,
                &root,
                creation,
                &history,
                candidate_family,
                &author_registration,
                &author,
                signer,
                &device_signer,
            ))
            .await?;
        let circle_reference = creation.control_ref(objects, control_head_object);
        let commit = signed_circle_commit(
            store_root_hash,
            write_id.clone(),
            coord.clone(),
            author_registration.clone(),
            &author,
            order,
            membership_state,
            device_state,
            StoreOperationMembershipAuthority {
                predecessor: membership_authority,
            },
            circle_reference,
            stream_activations,
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
        let commit_ref =
            StoreBatchCommitRef::from_commit(&commit, coord, commit_prepared.reference().clone())
                .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        let history_summary = super::store_engine::engine::pull::prepare_merge_history_successor(
            db,
            &root,
            &commit,
            &commit_ref,
            &exact,
            &author,
            None,
            resolved_devices,
            super::store_engine::engine::pull::MergeHistorySuccessorEvidence::none(),
        )
        .await
        .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        prepared_objects.insert("store-commit".to_string(), commit_prepared);
        let head_context = ProtocolObjectContext::signed_plaintext(
            store_root_hash,
            ProtocolObjectDomain::StoreHead,
        );
        let device_id = author_registration.device_id.to_string();
        let head_prefix = head_slot_prefix(&device_id, seq);
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
            history_summary.summary.digest(),
            SuccessorLink {
                activation: author
                    .store_announcement_activation(&author_registration)
                    .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?
                    .activation_id(),
                predecessor: history_summary
                    .predecessor_head
                    .map(|reference| reference.object),
                next_slot: next_head_slot,
            },
            &device_signer,
        )
        .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        let head_prepared = storage
            .prepare_protocol_object(
                &head_context,
                history_summary.head_slot,
                &head_prefix,
                head.to_bytes(),
            )
            .map_err(super::store_objects::StoreObjectError::from)?;
        prepared_objects.insert("store-head".to_string(), head_prepared);
        (
            creation,
            commit,
            commit_ref,
            CircleOperationPolicy {
                head,
                history_summary: history_summary.summary,
            },
            prepared_objects,
        )
    };
    Ok(CircleOperationJournal {
        operation_id,
        circle_id: creation.circle_id,
        intent,
        progress: CircleOperationProgress::Ready(Box::new(PreparedCircleOperation {
            creation,
            history,
            commit_bytes: commit.to_bytes(),
            commit_ref,
            prepared_objects,
            policy,
            uploaded: BTreeSet::new(),
        })),
    })
}

async fn publish_circle_operation(
    db: &Database,
    storage: &dyn SyncStorage,
    operation_id: &CircleOperationId,
    identity: &UserKeypair,
) -> Result<(), CircleOperationError> {
    let mut journal = db.circle_operation(operation_id).await?.ok_or_else(|| {
        CircleOperationError::Journal(format!("circle operation {operation_id} is absent"))
    })?;
    let circle_id = journal.circle_id();
    if let CircleOperationState::Blocked { reason } = journal.state() {
        return Err(CircleOperationError::Blocked { circle_id, reason });
    }
    let creation = journal.operation().creation.clone();
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
        &journal.operation().commit_ref,
        &commit,
        &author,
    )?;
    if !commit
        .operations()
        .is_some_and(super::store_commit::StoreCommitOperations::is_circle_control_activation_only)
    {
        return Err(CircleOperationError::InvalidState(
            "Circle Store commit is not an exact control-only batch".to_string(),
        ));
    }
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
    if !has_current_merge_authority(db, storage, &commit, &author).await? {
        let reason = "circle operation author is not a current Store writer under its exact grant"
            .to_string();
        db.block_circle_operation(operation_id, reason.clone())
            .await?;
        return Err(CircleOperationError::Blocked { circle_id, reason });
    }
    {
        let CircleOperationPolicy {
            head,
            history_summary,
        } = &journal.operation().policy;
        let root = db
            .local_store_root_ref()
            .await?
            .ok_or(CircleOperationError::MissingState("Store root reference"))?;
        if root.store_root_hash != store_root_hash {
            return Err(CircleOperationError::InvalidState(
                "Circle commit names a different Store root".to_string(),
            ));
        }
        let prepared_head = journal
            .operation()
            .prepared_objects
            .get("store-head")
            .ok_or_else(|| {
                CircleOperationError::Journal(
                    "Merge Circle operation lacks its prepared Store head".to_string(),
                )
            })?;
        let (_, state_after) =
            super::store_engine::engine::pull::retained_merge_device_state_for_order(
                db,
                storage,
                &root,
                &commit.order,
            )
            .await
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        let head_ref = super::store_commit::StoreDeviceHeadRef {
            head_hash: head.head_hash(),
            object: prepared_head.reference().clone(),
        };
        history_summary
            .open(
                &commit,
                &journal.operation().commit_ref,
                head,
                &head_ref,
                &state_after,
            )
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
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
    let CircleTransitionPolicyObjects {
        roster_entry,
        roster_head,
        metadata_head,
        ..
    } = &creation.policy_objects;
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
            head: reference
                .objects()
                .metadata_heads
                .iter()
                .find(|head| head.coord == metadata_head.coord())
                .ok_or_else(|| {
                    CircleOperationError::Journal(
                        "prepared metadata head is absent from its signed object graph".to_string(),
                    )
                })?,
        }),
        &serde_json::to_vec(metadata_head).expect("circle metadata head serialization cannot fail"),
    )
    .await?;
    match (roster_entry, roster_head) {
        (Some(roster_entry), Some(roster_head)) => {
            let roster_context = ProtocolObjectContext::circle(
                store_root_hash,
                ProtocolObjectDomain::CircleRoster,
                circle_encryption.clone(),
            );
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
                    head: reference
                        .objects()
                        .roster_heads
                        .iter()
                        .find(|head| head.coord == roster_head.entry_coord())
                        .ok_or_else(|| {
                            CircleOperationError::Journal(
                                "prepared roster head is absent from its signed object graph"
                                    .to_string(),
                            )
                        })?,
                }),
                &serde_json::to_vec(roster_head)
                    .expect("circle roster head serialization cannot fail"),
            )
            .await?;
        }
        (None, None) => {}
        _ => {
            return Err(CircleOperationError::InvalidState(
                "Circle transition must carry both an authored roster entry and head".to_string(),
            ));
        }
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
    let control_head = &creation.policy_objects.control_head;
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
        &serde_json::to_vec(control_head).expect("circle control head serialization cannot fail"),
    )
    .await?;
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
    let root = db
        .local_store_root_ref()
        .await?
        .ok_or(CircleOperationError::MissingState("Store root reference"))?;
    let founder = db
        .get_protocol_state(super::membership_ops::OWNER_PUBKEY_STATE_KEY)
        .await?
        .ok_or(CircleOperationError::MissingState("Store founder"))?;
    let verified = load_circle_activations(
        db,
        storage,
        &root,
        &journal.operation().commit_ref,
        &commit,
        &author,
        identity,
        &founder,
    )
    .await?;
    let expected = expected_local_circle_activation(&creation, reference, &author.author_pubkey)?;
    if verified.circles() != std::slice::from_ref(&expected) {
        return Err(CircleOperationError::InvalidState(
            "stored verified Circle activation differs from its durable journal".to_string(),
        ));
    }
    {
        let head = journal.operation().policy.head.clone();
        let commit_bytes = journal.operation().commit_bytes.clone();
        let commit_hash = journal.operation().commit_ref.commit_hash;
        let stream_id = journal.operation().commit_ref.coord.stream_id;
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
    db.activate_circle_operation(journal, verified).await?;
    Ok(())
}

fn expected_local_circle_activation(
    creation: &PreparedCircleTransition,
    reference: &super::store_commit::CircleControlRef,
    author_pubkey: &str,
) -> Result<VerifiedCircleReference, CircleOperationError> {
    let access = creation
        .access
        .iter()
        .find(|access| access.leaf.value.recipient_pubkey == author_pubkey)
        .ok_or_else(|| {
            CircleOperationError::InvalidState(
                "Circle author has no journaled access disposition".to_string(),
            )
        })?;
    let active = match &access.leaf.value.disposition {
        CircleAccessDisposition::Active { .. } => Some(VerifiedCircleActive {
            roster: creation.roster.clone(),
            metadata: creation.metadata.clone(),
        }),
        CircleAccessDisposition::Inactive => None,
    };
    Ok(VerifiedCircleReference {
        reference: reference.clone(),
        circle_id: creation.circle_id,
        control: creation.control.clone(),
        local_access: Some(VerifiedCircleAccess {
            envelope: access.envelope.clone(),
            leaf: access.leaf.clone(),
            active,
        }),
    })
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
    let prepared = journal
        .operation()
        .prepared_objects
        .get(step)
        .cloned()
        .ok_or_else(|| {
            CircleOperationError::Journal(format!(
                "Circle upload step {step:?} lacks its prepared exact object"
            ))
        })?;
    if journal.operation().uploaded.contains(step) {
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
    journal.operation_mut().uploaded.insert(step.to_string());
    db.update_circle_operation(journal.clone()).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::database::DbError;
    use crate::storage::cloud::{test_utils::InMemoryCloudHome, CloudHome};
    use crate::sync::circle::CircleTransitionDraftPolicy;
    use crate::sync::cloud_storage::{CloudCipher, CloudCipherAccess};
    use crate::sync::membership::MemberRole;
    use crate::sync::storage::{ProtocolObjectContext, ProtocolObjectDomain};
    use crate::sync::test_helpers::{
        install_active_device_fixture, open_test_db, temp_store_dir, test_migrations,
        test_synced_tables, TestCustody, TestStore,
    };

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

    async fn assert_exact_object_absent(home: &InMemoryCloudHome, reference: &ExactObjectRef) {
        let storage = Arc::new(home.clone())
            .exact_slot_storage()
            .expect("in-memory storage supports exact slots");
        let error = storage
            .read_at(reference.slot())
            .await
            .expect_err("rejected Store object must be absent");
        assert!(
            matches!(error, crate::storage::cloud::CloudHomeError::NotFound(_)),
            "{error}"
        );
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

    async fn resign_merge_journal_with_objects(
        db: &Database,
        storage: &dyn SyncStorage,
        signer: &UserKeypair,
        journal: &mut CircleOperationJournal,
        mutate: impl FnOnce(&mut CircleActivationObjects),
    ) {
        let old_commit = journal.commit().expect("parse prepared Circle commit");
        let [old_reference] = old_commit.circle_controls() else {
            panic!("Circle operation must carry one control")
        };
        let mut objects = old_reference.objects().clone();
        mutate(&mut objects);
        let reference = journal
            .operation()
            .creation
            .control_ref(objects, Some(old_reference.head_object().clone()));
        resign_merge_journal_with_reference(db, storage, signer, journal, reference, |_| {}).await;
    }

    async fn resign_merge_journal_with_reference(
        db: &Database,
        storage: &dyn SyncStorage,
        signer: &UserKeypair,
        journal: &mut CircleOperationJournal,
        reference: super::super::store_commit::CircleControlRef,
        mutate_commit: impl FnOnce(&mut StoreBatchCommit),
    ) {
        let old_commit = journal.commit().expect("parse prepared Circle commit");
        let author = db
            .activated_store_device_registration(old_commit.author_registration.clone())
            .await
            .expect("load Circle commit author");
        let device_signer = author
            .device_signer(signer)
            .expect("derive Circle device signer");
        let coord = journal.operation().commit_ref.coord.clone();
        let stream_activations = old_commit.stream_activations().to_vec();
        let mut commit = signed_circle_commit(
            old_commit.store_root_hash,
            old_commit.write_id.clone(),
            coord.clone(),
            old_commit.author_registration.clone(),
            &author,
            old_commit.order.clone(),
            old_commit.membership_state.clone(),
            old_commit.device_state.clone(),
            old_commit
                .operations_membership_authority()
                .expect("prepared Circle commit carries validated operations"),
            reference,
            stream_activations,
            &device_signer,
        )
        .expect("re-sign Circle commit with substituted exact graph");
        mutate_commit(&mut commit);
        commit.signature = keys::sign_hex(&device_signer, &commit.canonical_signed_bytes()).1;
        let StoreCommitCoord { stream_id, .. } = coord.clone();
        let commit_prepared = prepare_circle_object(
            storage,
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
        .expect("prepare substituted Circle commit");
        let commit_ref =
            StoreBatchCommitRef::from_commit(&commit, coord, commit_prepared.reference().clone())
                .expect("bind substituted Circle commit");
        let policy = &journal.operation().policy;
        let old_head = &policy.head;
        let history_summary = &policy.history_summary;
        let history_summary = history_summary.clone();
        let head = StoreDeviceHead::signed(
            commit.store_root_hash,
            commit.author_registration.clone(),
            commit_ref.clone(),
            history_summary.digest(),
            old_head.successor.clone(),
            &device_signer,
        )
        .expect("sign substituted Circle Store head");
        let head_slot = journal
            .operation()
            .prepared_objects
            .get("store-head")
            .expect("Circle operation carries a Store head")
            .reference()
            .slot()
            .clone();
        let head_prepared = prepare_circle_object_at(
            storage,
            &ProtocolObjectContext::signed_plaintext(
                commit.store_root_hash,
                ProtocolObjectDomain::StoreHead,
            ),
            head_slot,
            &head_slot_prefix(
                &commit.author_registration.device_id.to_string(),
                commit.seq(),
            ),
            head.to_bytes(),
        )
        .expect("prepare substituted Circle Store head");
        let operation = journal.operation_mut();
        operation.commit_bytes = commit.to_bytes();
        operation.commit_ref = commit_ref;
        operation
            .prepared_objects
            .insert("store-commit".to_string(), commit_prepared);
        operation
            .prepared_objects
            .insert("store-head".to_string(), head_prepared);
        operation.policy = CircleOperationPolicy {
            head,
            history_summary,
        };
        operation.uploaded.clear();
    }

    #[tokio::test]
    async fn circle_operation_lookup_rejects_a_payload_with_another_operation_id() {
        let db = open_test_db();
        let (_store, _signer, journal) =
            persist_merge_operation(&db, "circle-operation-id-mismatch").await;
        let expected_operation_id = journal.operation_id.clone();
        let replacement_write_id =
            crate::WriteId::from_generated("another-circle-operation".to_string());
        let mut replacement = journal.clone();
        replacement.operation_id = CircleOperationId::from_write_id(replacement_write_id.clone());
        let mut replacement_commit = replacement.commit().expect("parse replacement commit");
        replacement_commit.write_id = replacement_write_id;
        replacement.operation_mut().commit_bytes =
            serde_json::to_vec(&replacement_commit).expect("serialize replacement commit");
        let payload =
            serde_json::to_vec(&replacement).expect("serialize mismatched Circle operation");
        db.call(move |conn| {
            conn.execute(
                "UPDATE circle_operations SET payload = ?2 WHERE operation_id = ?1",
                rusqlite::params![expected_operation_id.as_str(), payload],
            )
            .map(|_| ())
            .map_err(DbError::from)
        })
        .await
        .expect("install mismatched Circle operation payload");

        let error = db
            .circle_operation(&journal.operation_id)
            .await
            .expect_err("lookup authority must match the payload operation id");
        assert!(error.to_string().contains("operation id"), "{error}");
    }

    #[tokio::test]
    async fn circle_operation_lookup_rejects_a_payload_with_another_circle_id() {
        let db = open_test_db();
        let (_store, _signer, journal) = persist_merge_operation(&db, "circle-id-mismatch").await;
        let expected_operation_id = journal.operation_id.clone();
        let replacement_circle_id = CircleId::from_bytes([7; 16]);
        let mut replacement = journal.clone();
        replacement.circle_id = replacement_circle_id;
        replacement.operation_mut().creation.circle_id = replacement_circle_id;
        let payload =
            serde_json::to_vec(&replacement).expect("serialize mismatched Circle operation");
        db.call(move |conn| {
            conn.execute(
                "UPDATE circle_operations SET payload = ?2 WHERE operation_id = ?1",
                rusqlite::params![expected_operation_id.as_str(), payload],
            )
            .map(|_| ())
            .map_err(DbError::from)
        })
        .await
        .expect("install mismatched Circle operation payload");

        let error = db
            .circle_operation(&journal.operation_id)
            .await
            .expect_err("lookup authority must match the payload Circle id");
        assert!(error.to_string().contains("payload circle id"), "{error}");
    }

    #[tokio::test]
    async fn blocking_a_circle_operation_targets_its_exact_operation_id() {
        let db = open_test_db();
        let (store, signer, first) = persist_merge_operation(&db, "circle-block-first").await;
        let device_id = local_device_id(&db).await;
        let second = prepare_circle_operation(
            &db,
            &store.storage,
            &device_id,
            "0000000002000-0000-creator",
            "Second household",
            &signer,
        )
        .await
        .expect("prepare second Circle operation");
        db.insert_circle_operation(second.clone())
            .await
            .expect("persist second Circle operation");

        db.block_circle_operation(&first.operation_id, "authority changed".to_string())
            .await
            .expect("block first Circle operation");

        let first = db
            .circle_operation(&first.operation_id)
            .await
            .expect("read first Circle operation")
            .expect("first Circle operation remains durable");
        let second = db
            .circle_operation(&second.operation_id)
            .await
            .expect("read second Circle operation")
            .expect("second Circle operation remains durable");
        assert!(matches!(
            first.state(),
            CircleOperationState::Blocked { .. }
        ));
        assert_eq!(second.state(), CircleOperationState::Pending);
    }

    #[tokio::test]
    async fn publishing_a_circle_operation_targets_its_exact_operation_id() {
        let db = open_test_db();
        let (store, signer, journal) = persist_merge_operation(&db, "circle-publish-id").await;
        let absent_operation_id = CircleOperationId::from_write_id(crate::WriteId::from_generated(
            "absent-circle-operation".to_string(),
        ));

        let error = publish_circle_operation(&db, &store.storage, &absent_operation_id, &signer)
            .await
            .expect_err("publication requires the exact durable operation id");

        assert!(matches!(error, CircleOperationError::Journal(_)), "{error}");
        assert_eq!(
            db.circle_operation(&journal.operation_id)
                .await
                .expect("read exact Circle operation")
                .expect("exact Circle operation remains durable")
                .state(),
            CircleOperationState::Pending
        );
    }

    fn promote_store_member_access_without_adding_to_circle_roster(
        creation: &mut CircleTransitionDraft,
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
        creation.control.value.value.active_epoch.common.access_root = access_root;
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
    }

    fn draft_from_transition(creation: &PreparedCircleTransition) -> CircleTransitionDraft {
        let policy = CircleTransitionDraftPolicy {
            roster_entry: creation.policy_objects.roster_entry.clone(),
        };
        CircleTransitionDraft {
            circle_id: creation.circle_id,
            epoch_id: creation.epoch_id,
            keyring: creation.keyring.clone(),
            roster: creation.roster.clone(),
            policy,
            metadata: creation.metadata.clone(),
            access: creation.access.clone(),
            control: creation.control.clone(),
        }
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
        assert_eq!(actual.circle_id, expected.circle_id);
        assert_eq!(actual.intent, expected.intent);
        assert_eq!(actual.operation().creation, expected.operation().creation);
        assert_eq!(
            actual.operation().commit_bytes,
            expected.operation().commit_bytes
        );
        assert_eq!(actual.operation().policy, expected.operation().policy);
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
                    if call > expected.operation().prepared_objects.len() {
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
                            operation_id: expected.operation_id.clone(),
                            circle_id: expected.circle_id(),
                            kind: crate::sync::circle::CircleOperationKind::Create,
                            state: crate::sync::circle::CircleOperationState::Pending,
                        }]
                    );
                    if after_visible_write {
                        store.home.fail_exact_create_after_call(call);
                    } else {
                        store.home.fail_exact_create_before_call(call);
                    }

                    let first = resume_circle_operations(&db, &store.storage, &signer).await;
                    if after_visible_write {
                        first.expect("lost exact-create response is settled by exact readback");
                    } else {
                        let error =
                            first.expect_err("failure before exact create interrupts activation");
                        assert!(matches!(error, CircleOperationError::Object(_)), "{error}");
                        let persisted = db
                            .circle_operation(&expected.operation_id)
                            .await
                            .expect("read interrupted operation")
                            .expect("interrupted operation remains durable");
                        assert_exact_operation(&expected, &persisted);
                        assert_eq!(persisted.state(), CircleOperationState::Pending);
                        assert_eq!(activation_count(&db, expected.circle_id()).await, 0);

                        resume_circle_operations(&db, &store.storage, &signer)
                            .await
                            .expect("resume exact circle operation");
                    }
                    assert!(db
                        .circle_operation(&expected.operation_id)
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
            crate::blob::TransferLimits::one_at_a_time(),
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
            crate::blob::TransferLimits::one_at_a_time(),
            "creator".to_string(),
            &test_migrations(),
        )
        .expect("reopen circle database");
        assert_eq!(
            reopened
                .get_circle_operations()
                .await
                .expect("list reopened Circle operations")
                .into_iter()
                .map(|operation| operation.operation_id)
                .collect::<Vec<_>>(),
            vec![expected.operation_id.clone()]
        );
        let persisted = reopened
            .circle_operation(&expected.operation_id)
            .await
            .expect("read reopened circle operation")
            .expect("circle operation survives restart");
        assert_exact_operation(&expected, &persisted);
        assert_eq!(persisted.state(), CircleOperationState::Pending);

        resume_circle_operations(&reopened, &store.storage, &signer)
            .await
            .expect("resume reopened circle operation");
        assert_eq!(activation_count(&reopened, expected.circle_id()).await, 1);
    }

    #[tokio::test]
    async fn interrupted_rename_reopens_and_resumes_the_same_signed_transition() {
        let temp = tempfile::tempdir().expect("create database directory");
        let path = temp.path().join("circle-rename-restart.sqlite3");
        let (db, _stamper) = Database::open(
            &path,
            test_synced_tables(),
            crate::blob::BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::one_at_a_time(),
            "creator".to_string(),
            &test_migrations(),
        )
        .expect("open circle database");
        let (store, signer, founder) = persist_merge_operation(&db, "circle-rename-restart").await;
        let circle_id = founder.circle_id();
        resume_circle_operations(&db, &store.storage, &signer)
            .await
            .expect("activate founder transition");
        let device_id = local_device_id(&db).await;

        store.home.fail_exact_create_before_call(1);
        let error = rename_circle(
            &db,
            &store.storage,
            &device_id,
            "0000000002000-0000-creator",
            circle_id,
            "Household money",
            &signer,
        )
        .await
        .expect_err("failed exact create interrupts rename publication");
        assert!(matches!(error, CircleOperationError::Object(_)), "{error}");
        let operation_id = db
            .get_circle_operations()
            .await
            .expect("list interrupted rename")
            .into_iter()
            .find(|operation| operation.circle_id == circle_id)
            .expect("interrupted rename is listed")
            .operation_id;
        let expected = db
            .circle_operation(&operation_id)
            .await
            .expect("read interrupted rename")
            .expect("interrupted rename remains durable");
        assert_eq!(expected.kind(), CircleOperationKind::Rename);
        assert_eq!(expected.state(), CircleOperationState::Pending);
        assert_eq!(activation_count(&db, circle_id).await, 1);
        assert_eq!(
            expected.operation().creation.epoch_id,
            founder.operation().creation.epoch_id
        );
        assert_eq!(
            expected.operation().creation.keyring,
            founder.operation().creation.keyring
        );
        std::thread::spawn(move || drop(db))
            .join()
            .expect("close circle database");

        let (reopened, _stamper) = Database::open(
            &path,
            test_synced_tables(),
            crate::blob::BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::one_at_a_time(),
            "creator".to_string(),
            &test_migrations(),
        )
        .expect("reopen circle database");
        let persisted = reopened
            .circle_operation(&operation_id)
            .await
            .expect("read reopened rename")
            .expect("rename survives restart");
        assert_exact_operation(&expected, &persisted);

        resume_circle_operations(&reopened, &store.storage, &signer)
            .await
            .expect("resume reopened rename");
        assert_eq!(activation_count(&reopened, circle_id).await, 2);
        assert_eq!(
            reopened
                .get_circles(&keys::public_key_hex(&signer))
                .await
                .expect("read renamed circle"),
            vec![crate::sync::circle::CircleInfo {
                id: circle_id,
                name: "Household money".to_string(),
                role: crate::sync::circle::CircleRole::Owner,
            }]
        );
        assert!(reopened
            .get_circle_operations()
            .await
            .expect("read completed rename operations")
            .is_empty());
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
                crate::blob::TransferLimits::one_at_a_time(),
                "creator".to_string(),
                &test_migrations(),
            )
            .expect("open circle database");
            let (store, signer, expected) =
                persist_merge_operation(&db, if corrupt { "corrupt" } else { "missing" }).await;
            store.home.fail_exact_create_before_call(2);
            resume_circle_operations(&db, &store.storage, &signer)
                .await
                .expect_err("second exact create failure interrupts publication");
            let persisted = db
                .circle_operation(&expected.operation_id)
                .await
                .expect("read interrupted circle operation")
                .expect("interrupted circle operation remains durable");
            assert!(persisted.operation().uploaded.contains("metadata"));

            let metadata = expected
                .operation()
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
                crate::blob::TransferLimits::one_at_a_time(),
                "creator".to_string(),
                &test_migrations(),
            )
            .expect("reopen circle database");
            resume_circle_operations(&reopened, &store.storage, &signer)
                .await
                .expect_err("durable upload marker must not bypass readback");
            assert_eq!(activation_count(&reopened, expected.circle_id()).await, 0);
            assert!(reopened
                .circle_operation(&expected.operation_id)
                .await
                .expect("read rejected circle operation")
                .is_some());
        }
    }

    #[tokio::test]
    async fn uploaded_circle_candidate_fails_when_its_ownership_record_is_missing() {
        let db = open_test_db();
        let (_store, _signer, mut journal) =
            persist_merge_operation(&db, "circle-missing-candidate-ownership").await;
        let step = "access-leaf-0";
        let object_id = super::super::remote_object::remote_object_id(
            journal
                .operation()
                .prepared_objects
                .get(step)
                .expect("operation carries its access leaf")
                .reference(),
        );
        db.call(move |conn| {
            let deleted = conn
                .execute(
                    "DELETE FROM remote_objects WHERE object_id = ?1",
                    [object_id.to_string()],
                )
                .map_err(DbError::from)?;
            if deleted != 1 {
                return Err(DbError::Message(
                    "expected candidate ownership record was absent before deletion".to_string(),
                ));
            }
            Ok(())
        })
        .await
        .expect("remove candidate ownership record");
        journal.operation_mut().uploaded.insert(step.to_string());

        let error = db
            .update_circle_operation(journal.clone())
            .await
            .expect_err("an uploaded candidate must retain its ownership record");
        assert!(error.to_string().contains("remote object"), "{error}");
        let persisted = db
            .circle_operation(&journal.operation_id)
            .await
            .expect("read operation after rejected update")
            .expect("operation remains durable after rejected update");
        assert!(!persisted.operation().uploaded.contains(step));
    }

    #[tokio::test]
    async fn journal_update_rejects_a_tampered_leaf_disposition() {
        let db = open_test_db();
        let (_store, signer, mut journal) =
            persist_merge_operation(&db, "circle-tampered-local-access").await;
        let author = keys::public_key_hex(&signer);
        let own_access = journal
            .operation_mut()
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
        let error = db
            .update_circle_operation(journal.clone())
            .await
            .expect_err("journal update must verify its closed candidate graph");
        assert!(
            error.to_string().contains("stored reference differs"),
            "{error}"
        );

        assert_eq!(activation_count(&db, journal.circle_id()).await, 0);
        assert!(db
            .circle_operation(&journal.operation_id)
            .await
            .expect("read rejected operation")
            .is_some());
    }

    #[tokio::test]
    async fn local_activation_rejects_sealed_leaf_plaintext_substitution() {
        let db = open_test_db();
        let (store, signer, mut journal) =
            persist_merge_operation(&db, "circle-mismatched-local-keyring").await;
        let author = keys::public_key_hex(&signer);
        let own_access = journal
            .operation_mut()
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
        db.update_circle_operation(journal.clone())
            .await
            .expect("persist substituted journal plaintext");
        resume_circle_operations(&db, &store.storage, &signer)
            .await
            .expect_err("local activation must reject substituted journal plaintext");
        assert_eq!(activation_count(&db, journal.circle_id()).await, 0);
    }

    #[tokio::test]
    async fn local_publication_rejects_a_prepared_object_outside_the_signed_graph() {
        let db = open_test_db();
        let (store, signer, mut journal) =
            persist_merge_operation(&db, "circle-substituted-local-object-ref").await;
        let original = journal
            .operation()
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
            .operation_mut()
            .prepared_objects
            .insert("metadata".to_string(), substituted);
        db.update_circle_operation(journal.clone())
            .await
            .expect("persist substituted journal object");

        resume_circle_operations(&db, &store.storage, &signer)
            .await
            .expect_err("local publication must reject objects outside the signed graph");

        assert_eq!(activation_count(&db, journal.circle_id()).await, 0);
    }

    #[tokio::test]
    async fn local_activation_rejects_substituted_exact_circle_edges() {
        let db = open_test_db();
        let (store, signer, mut journal) =
            persist_merge_operation(&db, "circle-substituted-signed-edges").await;
        resign_merge_journal_with_objects(&db, &store.storage, &signer, &mut journal, |objects| {
            let roster_coord = objects
                .roster_entries
                .keys()
                .next()
                .cloned()
                .expect("founder graph carries a roster entry");
            let metadata_coord = objects
                .metadata_entries
                .keys()
                .next()
                .cloned()
                .expect("founder graph carries metadata");
            let roster = objects
                .roster_entries
                .remove(&roster_coord)
                .expect("remove exact roster edge");
            let metadata = objects
                .metadata_entries
                .get_mut(&metadata_coord)
                .expect("load exact metadata edge");
            let metadata_object = std::mem::replace(&mut metadata.object, roster);
            objects.roster_entries.insert(roster_coord, metadata_object);
        })
        .await;
        let store_commit = journal.operation().commit_ref.object.clone();
        let store_head = journal
            .operation()
            .prepared_objects
            .get("store-head")
            .expect("Merge operation carries a Store head")
            .reference()
            .clone();
        db.update_circle_operation(journal.clone())
            .await
            .expect("persist substituted signed Circle graph");

        resume_circle_operations(&db, &store.storage, &signer)
            .await
            .expect_err("local activation must verify every signed exact Circle edge");

        assert_eq!(activation_count(&db, journal.circle_id()).await, 0);
        assert_exact_object_absent(&store.home, &store_commit).await;
        assert_exact_object_absent(&store.home, &store_head).await;
    }

    #[tokio::test]
    async fn local_circle_activation_rejects_another_circle_or_grant_anchor() {
        for wrong_grant in [false, true] {
            let db = open_test_db();
            let label = if wrong_grant {
                "circle-wrong-stream-grant"
            } else {
                "circle-wrong-stream-circle"
            };
            let (store, signer, mut journal) = persist_merge_operation(&db, label).await;
            let commit = journal.commit().expect("parse Circle commit");
            let [reference] = commit.circle_controls() else {
                panic!("Circle commit carries one control")
            };
            resign_merge_journal_with_reference(
                &db,
                &store.storage,
                &signer,
                &mut journal,
                reference.clone(),
                move |commit| {
                    let activations = match &mut commit.body {
                        super::super::store_commit::StoreCommitBody::Operations(operations) => {
                            &mut operations.stream_activations
                        }
                        _ => panic!("Circle commit body carries operations"),
                    };
                    let activation = activations
                        .iter_mut()
                        .find(|activation| {
                            matches!(
                                activation,
                                StreamActivation::GrantAuthorized {
                                    anchor: GrantStreamAnchor::CircleRoster { .. },
                                    ..
                                }
                            )
                        })
                        .expect("founder Circle commit activates its roster stream");
                    let StreamActivation::GrantAuthorized {
                        grant_id, anchor, ..
                    } = activation
                    else {
                        unreachable!()
                    };
                    if wrong_grant {
                        *grant_id = super::super::membership::MembershipGrantId(
                            ObjectHash::digest(b"another Circle grant"),
                        );
                    } else {
                        let GrantStreamAnchor::CircleRoster { circle_id, .. } = anchor else {
                            unreachable!()
                        };
                        *circle_id = CircleId::from_bytes([99; 16]);
                    }
                    activations.sort();
                },
            )
            .await;
            let store_commit = journal.operation().commit_ref.object.clone();
            let store_head = journal
                .operation()
                .prepared_objects
                .get("store-head")
                .expect("Merge operation carries a Store head")
                .reference()
                .clone();
            let StoreCommitCoord {
                stream_id,
                sequence,
            } = journal.operation().commit_ref.coord;
            db.update_circle_operation(journal.clone())
                .await
                .expect("persist Circle journal with substituted stream authority");

            resume_circle_operations(&db, &store.storage, &signer)
                .await
                .expect_err("Circle stream activation must name its signed Circle and grant");
            assert_eq!(activation_count(&db, journal.circle_id()).await, 0);
            assert!(db
                .exact_materialized_ref(&stream_id.to_string(), sequence)
                .await
                .expect("read rejected Circle Store position")
                .is_none());
            assert_exact_object_absent(&store.home, &store_commit).await;
            assert_exact_object_absent(&store.home, &store_head).await;
        }
    }

    #[tokio::test]
    async fn local_circle_activation_rejects_an_unexpected_acknowledgement() {
        let db = open_test_db();
        let (store, signer, mut journal) =
            persist_merge_operation(&db, "circle-unexpected-acknowledgement").await;
        super::super::store_engine::stage_merge_acknowledgement_for_test(
            &db,
            &store.storage,
            super::super::store_commit::CommitFrontier::from_refs(
                db.materialized_frontier()
                    .await
                    .expect("read current Store frontier"),
            )
            .expect("materialized Merge frontier is typed"),
            "2026-07-19T00:00:00Z".to_string(),
            &signer,
        )
        .await
        .expect("stage a valid non-initial Store acknowledgement");
        let acknowledgement = db
            .oldest_outbound_store_ack()
            .await
            .expect("read staged Store acknowledgement")
            .expect("staged Store acknowledgement remains queued")
            .reference;
        let commit = journal.commit().expect("parse Circle commit");
        let [reference] = commit.circle_controls() else {
            panic!("Circle commit carries one control")
        };
        resign_merge_journal_with_reference(
            &db,
            &store.storage,
            &signer,
            &mut journal,
            reference.clone(),
            move |commit| {
                let super::super::store_commit::StoreCommitBody::Operations(operations) =
                    &mut commit.body
                else {
                    panic!("Circle commit body carries operations")
                };
                operations.acknowledgement = Some(acknowledgement);
            },
        )
        .await;
        let store_commit = journal.operation().commit_ref.object.clone();
        let store_head = journal
            .operation()
            .prepared_objects
            .get("store-head")
            .expect("Merge operation carries a Store head")
            .reference()
            .clone();
        let StoreCommitCoord {
            stream_id,
            sequence,
        } = journal.operation().commit_ref.coord;
        db.update_circle_operation(journal.clone())
            .await
            .expect("persist Circle journal with unexpected acknowledgement");

        let error = resume_circle_operations(&db, &store.storage, &signer)
            .await
            .expect_err("Circle journal must contain no operation besides its control");
        assert!(error.to_string().contains("control-only batch"), "{error}");
        assert_eq!(activation_count(&db, journal.circle_id()).await, 0);
        assert!(db
            .exact_materialized_ref(&stream_id.to_string(), sequence)
            .await
            .expect("read rejected Circle Store position")
            .is_none());
        assert_exact_object_absent(&store.home, &store_commit).await;
        assert_exact_object_absent(&store.home, &store_head).await;
    }

    #[tokio::test]
    async fn local_successor_rejects_an_unreserved_circle_predecessor() {
        let db = open_test_db();
        let (store, signer, founder) =
            persist_merge_operation(&db, "circle-unreserved-predecessor").await;
        let circle_id = founder.circle_id();
        resume_circle_operations(&db, &store.storage, &signer)
            .await
            .expect("publish founder Circle");
        let device_id = local_device_id(&db).await;
        store.home.fail_exact_create_before_call(1);
        rename_circle(
            &db,
            &store.storage,
            &device_id,
            "0000000002000-0000-creator",
            circle_id,
            "Renamed household",
            &signer,
        )
        .await
        .expect_err("interrupt rename before its first exact upload");
        let operation_id = db
            .get_circle_operations()
            .await
            .expect("list interrupted rename")
            .into_iter()
            .find(|operation| operation.circle_id == circle_id)
            .expect("interrupted rename remains pending")
            .operation_id;
        let mut journal = db
            .circle_operation(&operation_id)
            .await
            .expect("read interrupted rename")
            .expect("interrupted rename journal remains durable");
        let commit = journal.commit().expect("parse rename commit");
        let author = db
            .activated_store_device_registration(commit.author_registration.clone())
            .await
            .expect("load rename author");
        let device_signer = author
            .device_signer(&signer)
            .expect("derive rename device signer");
        let original_slot = journal
            .operation()
            .prepared_objects
            .get("control-head")
            .expect("rename carries a control head")
            .reference()
            .slot()
            .clone();
        let creation = &mut journal.operation_mut().creation;
        let CircleTransitionPolicyObjects { control_head, .. } = &mut creation.policy_objects;
        control_head.successor.predecessor = Some(super::super::storage::ExactObjectRef::new(
            crate::storage::cloud::ObjectSlot::logical(
                "store-v1/test-circle-controls/unreserved-predecessor.json".to_string(),
            )
            .expect("construct arbitrary predecessor slot"),
            1,
            ObjectHash::digest(b"unreserved Circle predecessor"),
        ));
        control_head.signature = keys::sign_hex(&device_signer, &control_head.canonical_bytes()).1;
        let head_prefix = circle_semantic_prefix(CircleSemanticSlot::ControlHead {
            circle_id,
            control: &control_head.control,
            head_hash: control_head.head_hash(),
        });
        let prepared_head = prepare_circle_object_at(
            &store.storage,
            &ProtocolObjectContext::store_encrypted(
                commit.store_root_hash,
                ProtocolObjectDomain::CircleControl,
            ),
            original_slot,
            &head_prefix,
            serde_json::to_vec(control_head).expect("serialize forged control head"),
        )
        .expect("prepare forged control head");
        journal
            .operation_mut()
            .prepared_objects
            .insert("control-head".to_string(), prepared_head.clone());
        let [old_reference] = commit.circle_controls() else {
            panic!("rename commit carries one Circle reference")
        };
        let reference = journal.operation().creation.control_ref(
            old_reference.objects().clone(),
            Some(prepared_head.reference().clone()),
        );
        resign_merge_journal_with_reference(
            &db,
            &store.storage,
            &signer,
            &mut journal,
            reference,
            |_| {},
        )
        .await;
        let store_commit = journal.operation().commit_ref.object.clone();
        let store_head = journal
            .operation()
            .prepared_objects
            .get("store-head")
            .expect("Merge operation carries a Store head")
            .reference()
            .clone();
        db.update_circle_operation(journal)
            .await
            .expect("persist forged successor journal");

        resume_circle_operations(&db, &store.storage, &signer)
            .await
            .expect_err("common verifier must reject an unreserved Circle predecessor");
        assert_eq!(activation_count(&db, circle_id).await, 1);
        assert_exact_object_absent(&store.home, &store_commit).await;
        assert_exact_object_absent(&store.home, &store_head).await;
    }

    #[tokio::test]
    async fn local_publication_rejects_a_store_head_outside_its_reserved_slot() {
        let db = open_test_db();
        let (store, signer, mut journal) =
            persist_merge_operation(&db, "circle-substituted-local-head-slot").await;
        let original = journal
            .operation()
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
            .operation_mut()
            .prepared_objects
            .insert("store-head".to_string(), substituted);
        db.update_circle_operation(journal.clone())
            .await
            .expect("persist substituted Store head slot");

        resume_circle_operations(&db, &store.storage, &signer)
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
        for object in journal.operation().prepared_objects.values() {
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
        let original_access = &journal.operation().creation.access[0];
        let invented_recipient_slot = format!("{}-invented", original_ref.leaf.recipient_slot);
        let candidate_family = old_commit.candidate_family();
        let leaf_prefix = circle_access_leaf_semantic_prefix(
            journal.operation().creation.circle_id,
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
            journal.operation().creation.circle_id,
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
        let commit_coord = journal.operation().commit_ref.coord.clone();
        let original_control = &old_commit.circle_controls()[0];
        let circle_reference = journal
            .operation()
            .creation
            .control_ref(objects, Some(original_control.head_object().clone()));
        let stream_activations = old_commit.stream_activations().to_vec();
        let commit = signed_circle_commit(
            old_commit.store_root_hash,
            old_commit.write_id.clone(),
            commit_coord.clone(),
            old_commit.author_registration.clone(),
            &author,
            old_commit.order.clone(),
            old_commit.membership_state.clone(),
            old_commit.device_state.clone(),
            old_commit
                .operations_membership_authority()
                .expect("prepared Circle commit carries validated operations"),
            circle_reference,
            stream_activations,
            &device_signer,
        )
        .expect("sign commit naming invented access refs");
        let StoreCommitCoord { stream_id, .. } = commit_coord.clone();
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
            &db,
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

        let original_head = &journal.operation().policy.head;
        let forged_head = StoreDeviceHead::signed(
            commit.store_root_hash,
            commit.author_registration.clone(),
            commit_ref.clone(),
            original_head.history_summary,
            original_head.successor.clone(),
            &device_signer,
        )
        .expect("sign Store head naming the re-signed commit");
        let original_head_object = journal
            .operation()
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
        let engine =
            super::super::store_engine::StoreEngine::authorize_borrowed(&store.storage, &db)
                .await
                .expect("authorize Merge engine for pull");
        let pull = engine
            .pull(&store_dir, &signer)
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
        let journal = prepare_circle_operation(
            &db,
            &store.storage,
            &device_id,
            "0000000001000-0000-founder",
            "Household",
            &founder,
        )
        .await
        .expect("prepare Circle with inactive Store-member access");
        let old_commit = journal.commit().expect("parse prepared Store commit");
        let candidate_family = old_commit.candidate_family();
        let author = db
            .activated_store_device_registration(old_commit.author_registration.clone())
            .await
            .expect("load exact Circle commit author");
        let device_signer = author
            .device_signer(&founder)
            .expect("derive Circle commit device signer");
        let mut draft = draft_from_transition(&journal.operation().creation);
        promote_store_member_access_without_adding_to_circle_roster(&mut draft, &founder, &peer);
        let (creation, objects, prepared, control_head_object, stream_activations) =
            prepare_circle_activation_objects(
                &store.storage,
                &store.root,
                draft,
                &journal.operation().history,
                candidate_family,
                &old_commit.author_registration,
                &author,
                &founder,
                &device_signer,
            )
            .await
            .expect("prepare exact promoted access objects");
        for object in prepared.values() {
            super::super::store_objects::create_exact_object(&store.storage, object)
                .await
                .expect("publish exact promoted access object");
        }
        let commit_coord = journal.operation().commit_ref.coord.clone();
        let circle_reference = creation.control_ref(objects, control_head_object);
        let commit = signed_circle_commit(
            old_commit.store_root_hash,
            old_commit.write_id.clone(),
            commit_coord.clone(),
            old_commit.author_registration.clone(),
            &author,
            old_commit.order.clone(),
            old_commit.membership_state.clone(),
            old_commit.device_state.clone(),
            old_commit
                .operations_membership_authority()
                .expect("prepared Circle commit carries validated operations"),
            circle_reference,
            stream_activations,
            &device_signer,
        )
        .expect("sign promoted access commit");
        let StoreCommitCoord { stream_id, .. } = commit_coord.clone();
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
            &db,
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
    async fn candidate_graph_rejects_partial_circle_access_ownership() {
        let db = open_test_db();
        let founder = UserKeypair::generate();
        let store = TestStore::create(&db, "circle-partial-access-graph", founder.clone())
            .await
            .expect("create exact Circle test Store");
        let device_id = local_device_id(&db).await;
        let mut journal = prepare_circle_operation(
            &db,
            &store.storage,
            &device_id,
            "0000000001000-0000-founder",
            "Partial access graph",
            &founder,
        )
        .await
        .expect("prepare Circle operation");
        let commit = journal.commit().expect("parse Circle commit");
        let envelope_object = commit.circle_controls()[0].objects().access[0]
            .envelope
            .object
            .clone();
        let removed = journal
            .operation_mut()
            .prepared_objects
            .iter()
            .find(|(_, prepared)| prepared.reference() == &envelope_object)
            .map(|(step, _)| step.clone())
            .expect("find prepared access envelope");
        journal.operation_mut().prepared_objects.remove(&removed);

        let error = journal
            .closed_remote_objects()
            .expect_err("a leaf without its envelope must not acquire candidate ownership");
        assert!(
            error.to_string().contains("has no prepared bytes"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn inactive_circle_member_verifies_public_first_head_activations() {
        let db = open_test_db();
        let founder = UserKeypair::generate();
        let store = TestStore::create(&db, "circle-inactive-member", founder.clone())
            .await
            .expect("create Circle Store");
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
            &EncryptionService::from_key([43; 32]),
            "circle-inactive-member",
            "Inactive Circle member Store",
            &db,
        )
        .await
        .expect("invite Store member outside the Circle");
        let device_id = local_device_id(&db).await;
        let journal = prepare_circle_operation(
            &db,
            &store.storage,
            &device_id,
            "0000000001000-0000-founder",
            "Household",
            &founder,
        )
        .await
        .expect("prepare founder Circle");
        let commit = journal.commit().expect("parse founder Circle commit");
        let commit_ref = journal.operation().commit_ref.clone();
        db.insert_circle_operation(journal.clone())
            .await
            .expect("persist founder Circle operation");
        resume_circle_operations(&db, &store.storage, &founder)
            .await
            .expect("publish founder Circle");
        let author = db
            .activated_store_device_registration(commit.author_registration.clone())
            .await
            .expect("load founder device registration");
        let verified = load_circle_activations(
            &db,
            &store.storage,
            &store.root,
            &commit_ref,
            &commit,
            &author,
            &peer,
            &keys::public_key_hex(&founder),
        )
        .await
        .expect("inactive Circle member verifies the public activation graph");
        let [circle] = verified.circles() else {
            panic!("founder commit must activate one Circle")
        };
        assert!(matches!(
            circle
                .local_access
                .as_ref()
                .expect("Store member receives an inactive leaf")
                .leaf
                .value
                .disposition,
            CircleAccessDisposition::Inactive
        ));
    }

    #[tokio::test]
    async fn remote_activation_rejects_metadata_with_a_different_historical_roster() {
        let baseline_db = open_test_db();
        let (baseline_store, baseline_signer, baseline) =
            persist_merge_operation(&baseline_db, "circle-remote-metadata-baseline").await;
        let baseline_commit = baseline.commit().expect("parse baseline Store commit");
        for object in baseline.operation().prepared_objects.values() {
            super::super::store_objects::create_exact_object(&baseline_store.storage, object)
                .await
                .expect("publish baseline exact Circle activation object");
        }
        let baseline_author = baseline_db
            .activated_store_device_registration(baseline_commit.author_registration.clone())
            .await
            .expect("load baseline exact Circle commit author");
        load_circle_activations(
            &baseline_db,
            &baseline_store.storage,
            &baseline_store.root,
            &baseline.operation().commit_ref,
            &baseline_commit,
            &baseline_author,
            &baseline_signer,
            &keys::public_key_hex(&baseline_signer),
        )
        .await
        .expect("baseline exact Circle activation verifies remotely");

        let db = open_test_db();
        let (store, signer, founder_journal) =
            persist_merge_operation(&db, "circle-remote-metadata-roster").await;
        let circle_id = founder_journal.circle_id();
        resume_circle_operations(&db, &store.storage, &signer)
            .await
            .expect("publish exact founder Circle");
        store.home.fail_exact_create_before_call(1);
        rename_circle(
            &db,
            &store.storage,
            &local_device_id(&db).await,
            "0000000002000-0000-creator",
            circle_id,
            "Renamed household",
            &signer,
        )
        .await
        .expect_err("interrupt rename before its first exact upload");
        let operation_id = db
            .get_circle_operations()
            .await
            .expect("list interrupted Circle rename")
            .into_iter()
            .find(|operation| operation.circle_id == circle_id)
            .expect("interrupted Circle rename remains pending")
            .operation_id;
        let journal = db
            .circle_operation(&operation_id)
            .await
            .expect("read interrupted Circle rename")
            .expect("interrupted Circle rename journal remains durable");
        let old_commit = journal.commit().expect("parse prepared Store commit");
        let commit_coord = journal.operation().commit_ref.coord.clone();
        let mut draft = draft_from_transition(&journal.operation().creation);
        let store_root_hash = draft.control.value.store_root_hash;
        let roster = &mut draft.roster;
        roster.state_hash = ObjectHash::digest(b"different historical roster state");
        let candidate_family = old_commit.candidate_family();
        let author = db
            .activated_store_device_registration(old_commit.author_registration.clone())
            .await
            .expect("load exact Circle commit author");
        let device_signer = author
            .device_signer(&signer)
            .expect("derive Circle commit device signer");
        let (creation, objects, prepared, control_head_object, stream_activations) =
            prepare_circle_activation_objects(
                &store.storage,
                &store.root,
                draft,
                &journal.operation().history,
                candidate_family,
                &old_commit.author_registration,
                &author,
                &signer,
                &device_signer,
            )
            .await
            .expect("prepare forged exact Circle activation objects");
        for object in prepared.values() {
            super::super::store_objects::create_exact_object(&store.storage, object)
                .await
                .expect("publish forged exact Circle activation object");
        }
        let circle_reference = creation.control_ref(objects, control_head_object);
        let commit = signed_circle_commit(
            store_root_hash,
            old_commit.write_id.clone(),
            commit_coord.clone(),
            old_commit.author_registration.clone(),
            &author,
            old_commit.order.clone(),
            old_commit.membership_state.clone(),
            old_commit.device_state.clone(),
            old_commit
                .operations_membership_authority()
                .expect("prepared Circle commit carries validated operations"),
            circle_reference,
            stream_activations,
            &device_signer,
        )
        .expect("sign forged metadata activation commit");
        let StoreCommitCoord { stream_id, .. } = commit_coord.clone();
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
            &db,
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

        resume_circle_operations(&successor_db, &store.storage, &successor)
            .await
            .expect("revoked journal is blocked without interrupting the resume loop");

        let blocked = successor_db
            .circle_operation(&journal.operation_id)
            .await
            .expect("read revoked journal")
            .expect("revoked journal remains durable");
        assert!(matches!(
            blocked.state(),
            CircleOperationState::Blocked { .. }
        ));
        assert!(successor_db
            .circle_operation(&later.operation_id)
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
    async fn retained_circle_activation_reverifies_every_retained_boundary() {
        fn replace_once(bytes: &[u8], original: &[u8], replacement: &[u8]) -> Vec<u8> {
            let positions = bytes
                .windows(original.len())
                .enumerate()
                .filter_map(|(index, candidate)| (candidate == original).then_some(index))
                .collect::<Vec<_>>();
            let [position] = positions.as_slice() else {
                panic!("retained fixture must contain exactly one replacement target")
            };
            let mut replaced = Vec::with_capacity(bytes.len() - original.len() + replacement.len());
            replaced.extend_from_slice(&bytes[..*position]);
            replaced.extend_from_slice(replacement);
            replaced.extend_from_slice(&bytes[*position + original.len()..]);
            replaced
        }

        let db = open_test_db();
        let founder = UserKeypair::generate();
        let store = TestStore::create(&db, "retained-circle-activation", founder.clone())
            .await
            .expect("create retained Circle Store");
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
            &EncryptionService::from_key([73; 32]),
            "retained-circle-activation",
            "Retained Circle activation Store",
            &db,
        )
        .await
        .expect("invite retained Circle peer");
        let journal = prepare_circle_operation(
            &db,
            &store.storage,
            &local_device_id(&db).await,
            "0000000001000-0000-founder",
            "Household",
            &founder,
        )
        .await
        .expect("prepare retained Circle activation");
        for object in journal.operation().prepared_objects.values() {
            super::super::store_objects::create_exact_object(&store.storage, object)
                .await
                .expect("publish retained Circle activation object");
        }
        let commit = journal.commit().expect("parse retained Circle commit");
        let commit_ref = &journal.operation().commit_ref;
        let author = db
            .activated_store_device_registration(commit.author_registration.clone())
            .await
            .expect("load retained Circle commit author");
        let verified = load_circle_activations(
            &db,
            &store.storage,
            &store.root,
            commit_ref,
            &commit,
            &author,
            &founder,
            &keys::public_key_hex(&founder),
        )
        .await
        .expect("verify retained Circle activation fixture");
        let retained = verified
            .to_retained()
            .expect("serialize retained Circle activation");
        let founder_pubkey = keys::public_key_hex(&founder);
        assert_eq!(
            VerifiedCircleActivations::parse_retained(
                &retained,
                &commit,
                commit_ref,
                &author,
                Some(&founder_pubkey),
            )
            .expect("parse retained Circle activation"),
            verified
        );
        assert_eq!(
            VerifiedCircleActivations::parse_retained(
                &retained, &commit, commit_ref, &author, None,
            )
            .expect("parse retained Circle activation before local registration"),
            verified
        );

        let local_access = verified.circles()[0]
            .local_access
            .as_ref()
            .expect("founder has retained Circle access");
        let envelope_bytes =
            serde_json::to_vec(&local_access.envelope).expect("serialize retained Circle envelope");
        let mut envelope_field = b",\"envelope\":".to_vec();
        envelope_field.extend_from_slice(&envelope_bytes);
        let omitted = replace_once(&retained, &envelope_field, &[]);
        let omitted_error = VerifiedCircleActivations::parse_retained(
            &omitted,
            &commit,
            commit_ref,
            &author,
            Some(&founder_pubkey),
        )
        .expect_err("retained Circle access cannot omit its envelope");
        assert!(omitted_error
            .to_string()
            .contains("missing field `envelope`"));

        let peer_envelope = journal
            .operation()
            .creation
            .access
            .iter()
            .find(|access| access.leaf.value.recipient_pubkey == peer_pubkey)
            .expect("peer has an exact retained Circle envelope");
        let local_pair = serde_json::to_vec(&super::super::circle::PreparedCircleAccess {
            leaf: local_access.leaf.clone(),
            envelope: local_access.envelope.clone(),
        })
        .expect("serialize local retained Circle access pair");
        let substituted_pair =
            serde_json::to_vec(peer_envelope).expect("serialize substituted Circle access pair");
        let substituted = replace_once(&retained, &local_pair, &substituted_pair);
        let substituted_error = VerifiedCircleActivations::parse_retained(
            &substituted,
            &commit,
            commit_ref,
            &author,
            Some(&founder_pubkey),
        )
        .expect_err("retained Circle access cannot substitute another signed access pair");
        assert!(
            substituted_error
                .to_string()
                .contains("access names another local recipient"),
            "{substituted_error}"
        );

        let mut tampered_envelope = local_access.envelope.clone();
        tampered_envelope.signature.push('0');
        let tampered_envelope = serde_json::to_vec(&tampered_envelope)
            .expect("serialize tampered retained Circle envelope");
        let tampered = replace_once(&retained, &envelope_bytes, &tampered_envelope);
        let tampered_error = VerifiedCircleActivations::parse_retained(
            &tampered,
            &commit,
            commit_ref,
            &author,
            Some(&founder_pubkey),
        )
        .expect_err("retained Circle access cannot alter a signed envelope");
        assert!(
            tampered_error
                .to_string()
                .contains("access leaf and envelope failed verification"),
            "{tampered_error}"
        );

        let mut noncanonical = retained;
        noncanonical.push(b'\n');
        let canonical_error = VerifiedCircleActivations::parse_retained(
            &noncanonical,
            &commit,
            commit_ref,
            &author,
            Some(&founder_pubkey),
        )
        .expect_err("retained Circle activation bytes must be canonical");
        assert!(canonical_error.to_string().contains("not canonical"));
    }
}

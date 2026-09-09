use std::collections::BTreeMap;

use super::commands::CircleOperationRequest;
use super::{
    read_exact_circle_object, CircleOperationError, CircleOperationJournal,
    CircleTransitionHistory, PreparedCircleOperation,
};
use crate::sync::store::circles::bootstrap_blobs::CircleBootstrapBlobVerification;
use coven_database::StoreDatabase;
use coven_keys::encryption::{EncryptionService, MasterKeyring};
use coven_keys::keys;
use coven_protocol::circle::{
    circle_control_head_prefix, circle_metadata_head_prefix, circle_roster_head_prefix,
    circle_semantic_prefix, CircleAccessDisposition, CircleMetadataHeadRef, CircleOperationId,
    CirclePublicationBlocked, CircleRosterHeadRef, CircleSemanticSlot, CircleTransitionDraft,
    CircleTransitionPolicyObjects, PreparedCircleTransition,
};
use coven_protocol::objects::{
    ExactObjectRef, PreparedExactObject, ProtocolObjectContext, ProtocolObjectDomain,
};
use coven_protocol::store_commit::{
    circle_access_envelope_semantic_prefix, circle_access_leaf_semantic_prefix, CandidateFamilyId,
    CircleAccessEnvelopeObjectRef, CircleAccessLeafObjectRef, CircleAccessObjectRef,
    CircleActivationObjects, CircleMetadataObjectRef, GrantStreamAnchor, ObjectHash,
    StreamActivation, StreamAnchorDomain, SuccessorLink,
};
use coven_storage::CloudSyncObjectStorage;

pub(super) struct CircleCandidatePreparer<'operation, 'storage> {
    database: StoreDatabase,
    root: coven_protocol::store_commit::StoreRootRef,
    storage: std::sync::Arc<dyn CloudSyncObjectStorage>,
    local_writer: std::sync::Arc<crate::sync::store::commit_publication::LocalStoreWriter>,
    writer: &'operation mut crate::sync::store::AuthorizedWriterOperation<'storage>,
}

impl CircleBootstrapBlobVerification for CircleCandidatePreparer<'_, '_> {
    async fn verify_stored_blob(
        &self,
        stored: &coven_protocol::blob::locator::StoredBlobRef,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        self.storage.verify_blob_object(stored).await
    }
}

mod objects;
#[cfg(any(test, feature = "test-utils"))]
mod writer_test_support;

impl<'operation, 'storage> CircleCandidatePreparer<'operation, 'storage> {
    pub(super) fn new(
        database: StoreDatabase,
        root: coven_protocol::store_commit::StoreRootRef,
        storage: std::sync::Arc<dyn CloudSyncObjectStorage>,
        local_writer: std::sync::Arc<crate::sync::store::commit_publication::LocalStoreWriter>,
        writer: &'operation mut crate::sync::store::AuthorizedWriterOperation<'storage>,
    ) -> Self {
        Self {
            database,
            root,
            storage,
            local_writer,
            writer,
        }
    }

    #[cfg(test)]
    pub(super) async fn prepare_create(
        &mut self,
        metadata_stamp: &str,
        name: &str,
    ) -> Result<PreparedCircleJournal, CircleOperationError> {
        Box::pin(self.prepare_request(CircleOperationRequest::Create {
            name: name.to_string(),
            metadata_stamp: metadata_stamp.to_string(),
        }))
        .await
    }

    #[cfg(test)]
    pub(super) async fn prepare_request(
        &mut self,
        request: CircleOperationRequest,
    ) -> Result<PreparedCircleJournal, CircleOperationError> {
        let plan = self.writer.prepare_plan().await?;
        self.prepare_from_plan(&plan, request).await
    }

    pub(super) async fn prepare_from_plan(
        &mut self,
        plan: &crate::sync::store::commit_publication::operation::commit_plan::StoreOperationCommitPlan,
        request: CircleOperationRequest,
    ) -> Result<PreparedCircleJournal, CircleOperationError> {
        let database = self.database.clone();
        let current = plan.membership();
        let root = self.root.clone();
        let storage = self.storage.clone();
        let local_writer = std::sync::Arc::clone(&self.local_writer);
        let signer = local_writer.as_ref();
        let database = &database;
        let root = &root;
        let storage = storage.as_ref();
        let db = database;
        let store_root_hash = root.store_root_hash;
        let circle_device_id = local_writer.circle_device_id();
        let author_pubkey = local_writer.author_pubkey();
        let (operation_id, write_id) = match request.settlement() {
            Some((operation_id, write_id)) => (operation_id, write_id),
            None => {
                let write_id = db.new_store_write_id();
                (CircleOperationId::from_write_id(write_id.clone()), write_id)
            }
        };
        let history = request.history();
        let intent = request.intent();
        let (creation, store_commit, prepared_objects) = {
            let members = current.current_members();
            let rotation_checked_circle = match &request {
                CircleOperationRequest::Rename(request) => Some(request.circle_id),
                CircleOperationRequest::AddMember(request) => Some(request.circle_id),
                CircleOperationRequest::Create { .. }
                | CircleOperationRequest::RemoveMember(_)
                | CircleOperationRequest::ResolveControl(_)
                | CircleOperationRequest::Delete(_)
                | CircleOperationRequest::FinalizeEpochClose(_)
                | CircleOperationRequest::CancelEpochClose(_) => None,
            };
            if let Some(circle_id) = rotation_checked_circle {
                let active_store_members = current
                    .current_members()
                    .into_iter()
                    .map(|(pubkey, _)| pubkey)
                    .collect();
                if let Some(CirclePublicationBlocked::RotationRequired {
                    circle_id,
                    removed_members,
                }) = database
                    .circle_publication_rotation_block(circle_id, active_store_members)
                    .await?
                {
                    return Err(CircleOperationError::RotationRequired {
                        circle_id,
                        removed_members,
                    });
                }
            }
            let membership_authority = &plan.membership_authority().predecessor;
            let membership_state = plan.membership_state();
            let resolved_devices = plan.predecessor_state();
            let candidate_family = plan.candidate_family(&write_id);
            // A control-conflict resolution covers the losing branches' frontiers by
            // carrying their already-published activation objects (metadata and roster
            // heads and entries) into its own commit, so activation can verify the
            // merged frontier. Empty for every other operation.
            let mut merged_branch_objects: Vec<CircleActivationObjects> = Vec::new();
            let (creation, additional_prepared) = match &request {
                CircleOperationRequest::Create {
                    name,
                    metadata_stamp,
                } => (
                    CircleTransitionDraft::founder(
                        store_root_hash,
                        candidate_family,
                        &circle_device_id,
                        name,
                        metadata_stamp,
                        membership_state.clone(),
                        membership_authority.clone(),
                        members,
                        db,
                        signer,
                    )?,
                    Vec::new(),
                ),
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
                    (
                        CircleTransitionDraft::rename(
                            candidate_family,
                            &circle_device_id,
                            &request.name,
                            &request.metadata_stamp,
                            membership_state.clone(),
                            membership_authority.clone(),
                            members,
                            &request.current.control,
                            &request.current.roster,
                            &request.current.metadata,
                            keyring,
                            db,
                            signer,
                        )?,
                        Vec::new(),
                    )
                }
                CircleOperationRequest::AddMember(request) => {
                    if request.circle_id != request.current.control.value.circle_id {
                        return Err(CircleOperationError::InvalidState(
                            "Circle member-addition request differs from its current control"
                                .to_string(),
                        ));
                    }
                    let keyring = match &request.current.access.disposition {
                        CircleAccessDisposition::Active { keyring, .. } => keyring,
                        CircleAccessDisposition::Inactive => {
                            return Err(CircleOperationError::InvalidState(
                                "Circle member addition requires active local access".to_string(),
                            ));
                        }
                    };
                    let owner_grant = request
                        .current
                        .roster
                        .active_grants()
                        .find(|(_, record)| {
                            record.member_pubkey == author_pubkey
                                && record.role == coven_protocol::circle::CircleRole::Owner
                        })
                        .map(|(grant, _)| grant)
                        .ok_or_else(|| {
                            CircleOperationError::InvalidState(
                                "Circle member-addition author is not an active Owner".to_string(),
                            )
                        })?;
                    let roster_stream = local_writer.circle_grant_authorized_stream_id(
                        store_root_hash,
                        owner_grant,
                        StreamAnchorDomain::CircleRoster {
                            circle_id: request.circle_id,
                        },
                    );
                    let keyring_value =
                        coven_keys::encryption::MasterKeyring::from_serialized(keyring)?;
                    let circle_encryption =
                        coven_keys::encryption::EncryptionService::from(keyring_value);
                    let recipient_slot = coven_protocol::circle::recipient_slot(
                        signer,
                        &request.member_pubkey,
                        request.circle_id,
                    )?;
                    let bootstrap_blobs = self
                        .verify_snapshot_blobs(request.circle_id, request.bootstrap_blobs())
                        .await?;
                    let image_bytes = request
                        .read_bootstrap_image()
                        .await
                        .map_err(CircleOperationError::from)?;
                    let image_hash = ObjectHash::digest(&image_bytes);
                    let image_prefix =
                        coven_protocol::store_commit::circle_bootstrap_image_semantic_prefix(
                            request.circle_id,
                            candidate_family,
                            &author_pubkey,
                            request.current.control.value.epoch_id(),
                            &recipient_slot,
                            image_hash,
                        );
                    let image_context = ProtocolObjectContext::circle(
                        store_root_hash,
                        ProtocolObjectDomain::CircleBootstrapImage,
                        circle_encryption,
                    );
                    let bootstrap_prepared = self
                        .prepare_circle_object(&image_context, &image_prefix, ".db", image_bytes)
                        .await?;
                    let bootstrap = coven_protocol::circle::CircleBootstrapRef {
                        coverage: request.bootstrap_coverage().clone(),
                        schema_version: db.schema_version(),
                        sync_routing_hash: db.sync_routing_hash(),
                        image: coven_protocol::store_commit::SnapshotImageRef {
                            image_hash,
                            object: bootstrap_prepared.reference().clone(),
                        },
                        blobs: bootstrap_blobs,
                    };
                    (
                        CircleTransitionDraft::add_member(
                            candidate_family,
                            &circle_device_id,
                            membership_state.clone(),
                            membership_authority.clone(),
                            members,
                            &request.current.control,
                            &request.current.roster,
                            request.roster_chain.clone(),
                            &request.current.metadata,
                            keyring,
                            roster_stream,
                            request.member_pubkey.clone(),
                            request.role,
                            bootstrap,
                            db,
                            signer,
                        )?,
                        vec![("bootstrap-image".to_string(), bootstrap_prepared)],
                    )
                }
                CircleOperationRequest::RemoveMember(request) => {
                    if request.circle_id != request.current.control.value.circle_id
                        || request
                            .roster_chain
                            .try_resolved()
                            .map_err(CircleOperationError::from)?
                            != request.current.roster
                    {
                        return Err(CircleOperationError::InvalidState(
                            "Circle member-removal request differs from its current state"
                                .to_string(),
                        ));
                    }
                    let keyring = match &request.current.access.disposition {
                        CircleAccessDisposition::Active { keyring, .. } => keyring,
                        CircleAccessDisposition::Inactive => {
                            return Err(CircleOperationError::InvalidState(
                                "Circle member removal requires active local access".to_string(),
                            ));
                        }
                    };
                    let owner_grant = request
                        .current
                        .roster
                        .active_grants()
                        .find(|(_, record)| {
                            record.member_pubkey == author_pubkey
                                && record.role == coven_protocol::circle::CircleRole::Owner
                        })
                        .map(|(grant, _)| grant)
                        .ok_or_else(|| {
                            CircleOperationError::InvalidState(
                                "Circle member-removal author is not an active Owner".to_string(),
                            )
                        })?;
                    let roster_stream = local_writer.circle_grant_authorized_stream_id(
                        store_root_hash,
                        owner_grant,
                        StreamAnchorDomain::CircleRoster {
                            circle_id: request.circle_id,
                        },
                    );
                    let removal = request
                        .roster_chain
                        .signed_remove_member(
                            &circle_device_id,
                            roster_stream,
                            request.member_pubkey.clone(),
                            signer,
                        )
                        .map_err(CircleOperationError::from)?;
                    let remaining_roster = request
                        .roster_chain
                        .resolved_with_successor(removal.clone())
                        .map_err(CircleOperationError::from)?;
                    let remaining_members = remaining_roster.members();
                    let close_id = coven_protocol::circle::CircleEpochCloseId::from_operation_id(
                        &operation_id,
                    );
                    let intent = coven_protocol::circle::CircleEpochCloseIntent::signed(
                        store_root_hash,
                        request.circle_id,
                        close_id,
                        request.current.control.value.epoch_id(),
                        request.current.control.value.roster_state_ref(),
                        removal,
                        remaining_roster.state_hash(),
                        signer,
                    )?;
                    let intent_hash = intent.intent_hash();
                    let intent_prefix =
                        coven_protocol::circle::circle_epoch_close_intent_semantic_prefix(
                            request.circle_id,
                            close_id,
                            intent_hash,
                        );
                    let intent_encryption =
                        EncryptionService::from(MasterKeyring::from_serialized(keyring)?);
                    let intent_context = ProtocolObjectContext::circle(
                        store_root_hash,
                        ProtocolObjectDomain::CircleEpochCloseIntent,
                        intent_encryption,
                    );
                    let intent_prepared = self
                        .prepare_circle_object(
                            &intent_context,
                            &intent_prefix,
                            ".json",
                            serde_json::to_vec(&intent)
                                .expect("Circle epoch-close intent serialization cannot fail"),
                        )
                        .await?;
                    let intent_ref =
                        coven_protocol::circle::CircleEpochCloseIntentRef::from_intent(
                            &intent,
                            intent_prepared.reference().clone(),
                        )?;
                    let outcome_prefix =
                        coven_protocol::circle::circle_epoch_close_outcome_semantic_prefix(
                            request.circle_id,
                            close_id,
                        );
                    let close_outcome_context = ProtocolObjectContext::store_encrypted(
                        store_root_hash,
                        ProtocolObjectDomain::CircleEpochCloseOutcome,
                    );
                    let outcome_slot = storage
                        .allocate_protocol_slot(&close_outcome_context, &outcome_prefix, ".json")
                        .await
                        .map_err(coven_protocol::objects::StoreObjectError::from)?;
                    let mut participants = Vec::new();
                    for record in resolved_devices.devices.values() {
                        if !matches!(
                            record.status,
                            coven_protocol::store_commit::StoreDeviceStatus::Active
                        ) {
                            continue;
                        }
                        let registration = database
                            .activated_store_device_registration(record.registration.clone())
                            .await?;
                        if !remaining_members.contains_key(&registration.value().author_pubkey) {
                            continue;
                        }
                        let response_prefix =
                            coven_protocol::circle::circle_epoch_close_response_semantic_prefix(
                                request.circle_id,
                                close_id,
                                record.registration.device_id,
                            );
                        let response_context = ProtocolObjectContext::store_encrypted(
                            store_root_hash,
                            ProtocolObjectDomain::CircleEpochCloseResponse,
                        );
                        let response_slot = storage
                            .allocate_protocol_slot(&response_context, &response_prefix, ".json")
                            .await
                            .map_err(coven_protocol::objects::StoreObjectError::from)?;
                        participants.push(coven_protocol::circle::CircleEpochCloseParticipant {
                            registration: record.registration.clone(),
                            response_slot,
                        });
                    }
                    participants.sort_by_key(|participant| participant.registration.device_id);
                    if participants.is_empty() {
                        return Err(CircleOperationError::InvalidState(
                            "Circle epoch close has no remaining active device".to_string(),
                        ));
                    }
                    let provisional_frontier = plan
                        .predecessor_cut()
                        .map_err(CircleOperationError::from)?
                        .frontier();
                    (
                        CircleTransitionDraft::close_epoch(
                            candidate_family,
                            &circle_device_id,
                            membership_state.clone(),
                            membership_authority.clone(),
                            members,
                            &request.current.control,
                            &request.current.roster,
                            &request.current.metadata,
                            keyring,
                            close_id,
                            intent,
                            intent_ref,
                            plan.device_state().clone(),
                            participants,
                            provisional_frontier,
                            outcome_slot,
                            db,
                            signer,
                        )?,
                        vec![("epoch-close-intent".to_string(), intent_prepared)],
                    )
                }
                CircleOperationRequest::ResolveControl(request) => {
                    if request.circle_id != request.chosen.control.value.circle_id {
                        return Err(CircleOperationError::InvalidState(
                            "Circle control-resolution request differs from its chosen branch"
                                .to_string(),
                        ));
                    }
                    // The conflicting set the command captured must still equal the
                    // currently retained branches inside this journal transaction. A
                    // branch discovered since the command fails the resolution loud
                    // so it is never silently dropped; the Owner resolves the
                    // complete new set.
                    let retained = database
                        .circle_control_conflict_branches(request.circle_id)
                        .await?
                        .ok_or(CircleOperationError::NotConflicted {
                            circle_id: request.circle_id,
                        })?;
                    if retained != request.conflicting_branches {
                        return Err(CircleOperationError::InvalidState(
                            "Circle control conflict changed since the resolution was requested"
                                .to_string(),
                        ));
                    }
                    let keyring = match &request.chosen.access.disposition {
                        CircleAccessDisposition::Active { keyring, .. } => keyring,
                        CircleAccessDisposition::Inactive => {
                            return Err(CircleOperationError::InvalidState(
                            "Circle control resolution requires active local access to the chosen \
                             branch"
                                .to_string(),
                        ));
                        }
                    };
                    let mut losing_branches = Vec::with_capacity(request.losing_branches.len());
                    for branch in &request.losing_branches {
                        losing_branches.push(coven_protocol::circle::ResolvedConflictBranch {
                            control_head: coven_protocol::circle::MergeCircleControlHeadRef {
                                coord: branch.reference.control().clone(),
                                head_hash: branch.reference.head_hash(),
                                object: branch.reference.head_object().clone(),
                            },
                            metadata_heads: branch.reference.objects().metadata_heads.clone(),
                            roster_heads: branch.reference.objects().roster_heads.clone(),
                            selected_metadata: branch.selected_metadata.clone(),
                        });
                        merged_branch_objects.push(branch.reference.objects().clone());
                    }
                    (
                        CircleTransitionDraft::resolve(
                            candidate_family,
                            &circle_device_id,
                            membership_state.clone(),
                            membership_authority.clone(),
                            members,
                            &request.chosen.control,
                            &request.chosen.roster,
                            &request.chosen.metadata,
                            keyring,
                            losing_branches,
                            db,
                            signer,
                        )?,
                        Vec::new(),
                    )
                }
                CircleOperationRequest::Delete(request) => {
                    if request.circle_id != request.current.control.value.circle_id {
                        return Err(CircleOperationError::InvalidState(
                            "Circle deletion request differs from its current control".to_string(),
                        ));
                    }
                    let keyring = match &request.current.access.disposition {
                        CircleAccessDisposition::Active { keyring, .. } => keyring,
                        CircleAccessDisposition::Inactive => {
                            return Err(CircleOperationError::InvalidState(
                                "Circle deletion requires active local access".to_string(),
                            ));
                        }
                    };
                    (
                        CircleTransitionDraft::delete(
                            &circle_device_id,
                            membership_state.clone(),
                            membership_authority.clone(),
                            members,
                            &request.current.control,
                            &request.current.roster,
                            &request.current.metadata,
                            keyring,
                            db,
                            signer,
                        )?,
                        Vec::new(),
                    )
                }
                CircleOperationRequest::FinalizeEpochClose(request) => {
                    if request.circle_id != request.current.control.value.circle_id {
                        return Err(CircleOperationError::InvalidState(
                            "Circle close-finalization request differs from its current control"
                                .to_string(),
                        ));
                    }
                    let keyring = match &request.current.access.disposition {
                        CircleAccessDisposition::Active { keyring, .. } => keyring,
                        CircleAccessDisposition::Inactive => {
                            return Err(CircleOperationError::InvalidState(
                                "Circle close finalization requires retained active access"
                                    .to_string(),
                            ));
                        }
                    };
                    let mut draft = local_writer.finalize_circle_epoch_close(
                        candidate_family,
                        &request.metadata_stamp,
                        membership_state.clone(),
                        membership_authority.clone(),
                        members,
                        &request.current.control,
                        &request.current.roster,
                        request.roster_chain.clone(),
                        &request.current.metadata,
                        keyring,
                        request.intent.clone(),
                        request.responses.clone(),
                        db,
                    )?;
                    let bootstrap_blobs = self
                        .verify_snapshot_blobs(request.circle_id, request.bootstrap_blobs())
                        .await?;
                    let image_bytes = request
                        .read_bootstrap_image()
                        .await
                        .map_err(CircleOperationError::from)?;
                    let image_hash = ObjectHash::digest(&image_bytes);
                    let successor_encryption =
                        EncryptionService::from(MasterKeyring::from_serialized(&draft.keyring)?);
                    let mut bootstrap_objects = Vec::new();
                    for (index, access) in draft.access.iter_mut().enumerate() {
                        let image_prefix =
                            coven_protocol::store_commit::circle_bootstrap_image_semantic_prefix(
                                request.circle_id,
                                candidate_family,
                                &access.leaf.value.owner_pubkey,
                                draft.epoch_id,
                                &access.leaf.value.recipient_slot,
                                image_hash,
                            );
                        if let CircleAccessDisposition::Active {
                            bootstrap: active_bootstrap,
                            ..
                        } = &mut access.leaf.value.body_mut().disposition
                        {
                            let bootstrap_prepared = self
                                .prepare_circle_object(
                                    &ProtocolObjectContext::circle(
                                        store_root_hash,
                                        ProtocolObjectDomain::CircleBootstrapImage,
                                        successor_encryption.clone(),
                                    ),
                                    &image_prefix,
                                    ".db",
                                    image_bytes.clone(),
                                )
                                .await?;
                            let bootstrap = coven_protocol::circle::CircleBootstrapRef {
                                coverage: request.bootstrap_coverage().clone(),
                                schema_version: db.schema_version(),
                                sync_routing_hash: db.sync_routing_hash(),
                                image: coven_protocol::store_commit::SnapshotImageRef {
                                    image_hash,
                                    object: bootstrap_prepared.reference().clone(),
                                },
                                blobs: bootstrap_blobs.clone(),
                            };
                            *active_bootstrap = Some(bootstrap.clone());
                            bootstrap_objects
                                .push((format!("bootstrap-image-{index}"), bootstrap_prepared));
                        }
                    }
                    if bootstrap_objects.is_empty() {
                        return Err(CircleOperationError::InvalidState(
                            "Circle close finalization has no bootstrap recipient".to_string(),
                        ));
                    }
                    (draft, bootstrap_objects)
                }
                CircleOperationRequest::CancelEpochClose(request) => {
                    if request.circle_id != request.current.control.value.circle_id {
                        return Err(CircleOperationError::InvalidState(
                            "Circle close-cancellation request differs from its current control"
                                .to_string(),
                        ));
                    }
                    let keyring = match &request.current.access.disposition {
                        CircleAccessDisposition::Active { keyring, .. } => keyring,
                        CircleAccessDisposition::Inactive => {
                            return Err(CircleOperationError::InvalidState(
                                "Circle close cancellation requires retained active access"
                                    .to_string(),
                            ));
                        }
                    };
                    (
                        CircleTransitionDraft::reopen_epoch(
                            candidate_family,
                            &circle_device_id,
                            membership_state.clone(),
                            membership_authority.clone(),
                            members,
                            &request.current.control,
                            &request.current.roster,
                            &request.current.metadata,
                            keyring,
                            db,
                            signer,
                        )?,
                        Vec::new(),
                    )
                }
            };
            let (creation, objects, mut prepared_objects, control_head_object, stream_activations) =
                Box::pin(self.prepare_circle_activation_objects(
                    creation,
                    &history,
                    &merged_branch_objects,
                    candidate_family,
                ))
                .await?;
            for (step, object) in additional_prepared {
                if prepared_objects.insert(step.clone(), object).is_some() {
                    return Err(CircleOperationError::InvalidState(format!(
                        "Circle preparation repeats upload step {step}"
                    )));
                }
            }
            let circle_reference = creation.control_ref(objects, control_head_object);
            let store_commit = self.writer.prepare_candidate_for_write(
                plan,
                crate::sync::store::commit_publication::operation::commit_plan::StoreOperationBatch::Circle {
                    reference: circle_reference,
                    stream_activations,
                },
                write_id,
            ).await?;
            prepared_objects.insert(
                "store-commit".to_string(),
                store_commit
                    .prepared_commit()
                    .map_err(crate::sync::store::StoreError::from)?,
            );
            (creation, store_commit, prepared_objects)
        };
        let circle_id = creation.circle_id;
        Ok(PreparedCircleJournal {
            journal: CircleOperationJournal::ready(
                operation_id,
                circle_id,
                intent,
                PreparedCircleOperation {
                    creation,
                    history,
                    store_commit,
                    prepared_objects: prepared_objects
                        .iter()
                        .map(|(step, object)| (step.clone(), object.reference().clone()))
                        .collect(),
                },
            ),
            prepared_objects,
        })
    }
}

/// A freshly prepared Circle operation: the journal that names its objects, and
/// those objects' bytes.
///
/// The bytes travel beside the journal rather than inside it. The database
/// installs them when it commits the operation that owns them.
#[derive(Debug)]
pub(crate) struct PreparedCircleJournal {
    pub(crate) journal: CircleOperationJournal,
    pub(crate) prepared_objects: coven_database::PreparedCircleObjects,
}

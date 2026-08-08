use super::error::CircleOperationError;
use coven_database::StoreDatabase;
use coven_keys::encryption::{EncryptionService, MasterKeyring};
use coven_protocol::circle::{
    circle_semantic_prefix, CircleAccessDisposition, CircleOperationId, CircleOperationState,
    CircleSemanticSlot, CircleTransitionPolicyObjects, PreparedCircleTransition,
};
use coven_protocol::circle_activation::{
    verify_control_context_for_verified_commit, VerifiedCircleAccess, VerifiedCircleActive,
    VerifiedCircleReference,
};
use coven_protocol::circle_journal::{CircleOperationJournal, CircleOperationPolicy};
use coven_protocol::objects::StoreObjectError;
use coven_protocol::objects::{ProtocolObjectContext, ProtocolObjectDomain, StorageError};
use coven_protocol::store_commit::{
    circle_access_envelope_semantic_prefix, circle_access_leaf_semantic_prefix,
    commit_semantic_prefix, head_slot_prefix, StoreBatchCommit, StoreDeviceRegistration,
};
use coven_storage::CloudSyncObjectStorage;
use std::collections::BTreeSet;

pub(super) struct CircleCandidatePublisher<'operation, 'storage> {
    database: StoreDatabase,
    storage: std::sync::Arc<dyn CloudSyncObjectStorage>,
    store_dir: &'storage coven_foundation::store_dir::StoreDir,
    membership: coven_protocol::membership::MembershipChain,
    local_writer: std::sync::Arc<crate::sync::store::commit_publication::LocalStoreWriter>,
    history: super::VerifiedCircleHistory<'operation, 'storage>,
}

impl<'operation, 'storage> CircleCandidatePublisher<'operation, 'storage> {
    pub(super) fn new(
        database: StoreDatabase,
        storage: std::sync::Arc<dyn CloudSyncObjectStorage>,
        store_dir: &'storage coven_foundation::store_dir::StoreDir,
        membership: coven_protocol::membership::MembershipChain,
        local_writer: std::sync::Arc<crate::sync::store::commit_publication::LocalStoreWriter>,
        history: super::VerifiedCircleHistory<'operation, 'storage>,
    ) -> Self {
        Self {
            database,
            storage,
            store_dir,
            membership,
            local_writer,
            history,
        }
    }

    pub(super) async fn publish(
        &mut self,
        operation_id: &CircleOperationId,
        routing_key: Option<&coven_protocol::circle::RowRoutingKey>,
    ) -> Result<(), CircleOperationError> {
        let mut journal = self
            .database
            .circle_operation(operation_id)
            .await?
            .ok_or_else(|| {
                CircleOperationError::Journal(format!("circle operation {operation_id} is absent"))
            })?;
        let circle_id = journal.circle_id();
        if let CircleOperationState::Blocked { block } = journal.state() {
            return Err(CircleOperationError::Blocked { circle_id, block });
        }
        let creation = journal.operation().creation.clone();
        let store_root_hash = creation.control.value.store_root_hash;
        if self.history.root().store_root_hash != store_root_hash {
            return Err(CircleOperationError::InvalidState(
                "Circle commit names a different Store root".to_string(),
            ));
        }
        let circle_encryption =
            EncryptionService::from(MasterKeyring::from_serialized(&creation.keyring).map_err(
                |error| CircleOperationError::Journal(format!("circle keyring: {error}")),
            )?);
        let verified_commit = self
            .history
            .authenticate_commit_bytes(
                &journal.operation().commit_ref,
                &journal.operation().commit_bytes,
            )
            .await?;
        let author = verified_commit.author().clone();
        let commit = verified_commit.value();
        let reference = commit.circle_controls();
        let [reference] = reference else {
            return Err(CircleOperationError::InvalidState(
                "Circle operation commit must activate one control".to_string(),
            ));
        };
        verify_control_context_for_verified_commit(reference, &creation.control, &verified_commit)?;
        if !commit.operations().is_some_and(
            coven_protocol::store_commit::StoreCommitOperations::is_circle_control_activation_only,
        ) {
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
        let (_, state_after) = self
            .history
            .retained_device_state_for_order(&commit.order)
            .await
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        if let CurrentMergeAuthority::Revoked { grant_id } =
            self.current_merge_authority(commit, &author)?
        {
            let block = coven_protocol::circle::CircleOperationBlock::AuthorityLost { grant_id };
            self.database
                .block_circle_operation(operation_id, block.clone())
                .await?;
            return Err(CircleOperationError::Blocked { circle_id, block });
        }
        {
            let CircleOperationPolicy {
                head,
                history_summary,
            } = &journal.operation().policy;
            let prepared_head = journal
                .operation()
                .prepared_objects
                .get("store-head")
                .ok_or_else(|| {
                    CircleOperationError::Journal(
                        "Merge Circle operation lacks its prepared Store head".to_string(),
                    )
                })?;
            let head_ref = coven_protocol::store_commit::StoreDeviceHeadRef {
                head_hash: head.head_hash(),
                object: prepared_head.clone(),
            };
            history_summary
                .open(
                    commit,
                    &journal.operation().commit_ref,
                    head,
                    &head_ref,
                    &state_after,
                )
                .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        }

        let CircleTransitionPolicyObjects {
            roster,
            metadata_head,
            ..
        } = &creation.policy_objects;
        if let Some(metadata_head) = metadata_head {
            let metadata_encryption = circle_encryption
                .service_for_fingerprint(creation.metadata.key_fingerprint.as_bytes())
                .map_err(|error| {
                    CircleOperationError::InvalidState(format!(
                        "Circle metadata key fingerprint is absent from its keyring: {error}"
                    ))
                })?;
            self.append_step(
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
                &serde_json::to_vec(&creation.metadata)
                    .expect("circle metadata serialization cannot fail"),
            )
            .await?;
            self.append_step(
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
                                "prepared metadata head is absent from its signed object graph"
                                    .to_string(),
                            )
                        })?,
                }),
                &serde_json::to_vec(&metadata_head)
                    .expect("circle metadata head serialization cannot fail"),
            )
            .await?;
        }
        if let Some(roster) = roster {
            let roster_context = ProtocolObjectContext::circle(
                store_root_hash,
                ProtocolObjectDomain::CircleRoster,
                circle_encryption.clone(),
            );
            self.append_step(
                &mut journal,
                "roster-entry",
                &roster_context,
                &circle_semantic_prefix(CircleSemanticSlot::RosterEntry {
                    circle_id: creation.circle_id,
                    coord: &roster.entry.coord(),
                }),
                &serde_json::to_vec(&roster.entry)
                    .expect("circle roster entry serialization cannot fail"),
            )
            .await?;
            self.append_step(
                &mut journal,
                "roster-head",
                &roster_context,
                &circle_semantic_prefix(CircleSemanticSlot::RosterHead {
                    circle_id: creation.circle_id,
                    head: reference
                        .objects()
                        .roster_heads
                        .iter()
                        .find(|head| head.coord == roster.head.entry_coord())
                        .ok_or_else(|| {
                            CircleOperationError::Journal(
                                "prepared roster head is absent from its signed object graph"
                                    .to_string(),
                            )
                        })?,
                }),
                &serde_json::to_vec(&roster.head)
                    .expect("circle roster head serialization cannot fail"),
            )
            .await?;
        }
        let bootstrap_access = creation
            .access
            .iter()
            .zip(&reference.objects().access)
            .filter_map(|(access, object)| {
                let CircleAccessDisposition::Active {
                    bootstrap: Some(bootstrap),
                    ..
                } = &access.leaf.value.disposition
                else {
                    return None;
                };
                Some((access, object, bootstrap))
            })
            .collect::<Vec<_>>();
        for (access, object, bootstrap) in bootstrap_access {
            if object.bootstrap.as_ref() != Some(&bootstrap.image) {
                return Err(CircleOperationError::Journal(
                    "Circle bootstrap access differs from its signed object graph".to_string(),
                ));
            }
            for blob in &bootstrap.blobs {
                let stored = blob.stored().ok_or_else(|| {
                    CircleOperationError::InvalidState(
                        "Circle bootstrap row blob has no exact stored locator".to_string(),
                    )
                })?;
                self.storage
                    .as_ref()
                    .verify_blob_object(stored)
                    .await
                    .map_err(|error| {
                        CircleOperationError::InvalidState(format!(
                            "verify Circle bootstrap blob {}: {error}",
                            coven_protocol::remote_object::remote_object_id(stored.object())
                        ))
                    })?;
            }
            let mut matching_steps = journal
                .operation()
                .prepared_objects
                .iter()
                .filter(|(_, object)| *object == &bootstrap.image.object);
            let step = matching_steps
                .next()
                .map(|(step, _)| step.clone())
                .ok_or_else(|| {
                    CircleOperationError::Journal(
                        "Circle bootstrap image lacks its prepared exact object".to_string(),
                    )
                })?;
            if matching_steps.next().is_some() {
                return Err(CircleOperationError::Journal(
                    "Circle bootstrap image has more than one upload step".to_string(),
                ));
            }
            let prefix = coven_protocol::store_commit::circle_bootstrap_image_semantic_prefix(
                access.leaf.value.circle_id,
                commit.candidate_family(),
                &access.leaf.value.owner_pubkey,
                access.leaf.value.epoch_id,
                &access.leaf.value.recipient_slot,
                bootstrap.image.image_hash,
            );
            self.append_hashed_step(
                &mut journal,
                &step,
                &ProtocolObjectContext::circle(
                    store_root_hash,
                    ProtocolObjectDomain::CircleBootstrapImage,
                    circle_encryption.clone(),
                ),
                &prefix,
                bootstrap.image.image_hash,
            )
            .await?;
        }
        match (&creation.close_intent, &reference.objects().close_intent) {
            (Some(intent), Some(intent_ref))
                if intent.close_id == intent_ref.close_id
                    && intent.intent_hash() == intent_ref.intent_hash =>
            {
                self.append_step(
                    &mut journal,
                    "epoch-close-intent",
                    &ProtocolObjectContext::circle(
                        store_root_hash,
                        ProtocolObjectDomain::CircleEpochCloseIntent,
                        circle_encryption.clone(),
                    ),
                    &coven_protocol::circle::circle_epoch_close_intent_semantic_prefix(
                        creation.circle_id,
                        intent.close_id,
                        intent.intent_hash(),
                    ),
                    &serde_json::to_vec(intent)
                        .expect("Circle epoch-close intent serialization cannot fail"),
                )
                .await?;
            }
            (None, None) => {}
            _ => {
                return Err(CircleOperationError::Journal(
                    "Circle epoch-close intent differs from its signed object graph".to_string(),
                ));
            }
        }
        match (&creation.close_outcome, &reference.objects().close_outcome) {
            (Some(outcome), Some(outcome_ref))
                if outcome.close_id == outcome_ref.close_id
                    && outcome.outcome_hash() == outcome_ref.outcome_hash =>
            {
                self.append_step(
                    &mut journal,
                    "epoch-close-outcome",
                    &ProtocolObjectContext::store_encrypted(
                        store_root_hash,
                        ProtocolObjectDomain::CircleEpochCloseOutcome,
                    ),
                    &coven_protocol::circle::circle_epoch_close_outcome_semantic_prefix(
                        creation.circle_id,
                        outcome.close_id,
                    ),
                    &coven_protocol::circle::CircleEpochCloseSlotValue::Outcome(outcome.clone())
                        .to_bytes(),
                )
                .await?;
            }
            (None, None) => {}
            _ => {
                return Err(CircleOperationError::Journal(
                    "Circle epoch-close outcome differs from its signed object graph".to_string(),
                ));
            }
        }
        match (
            &creation.close_cancellation,
            &reference.objects().close_cancellation,
        ) {
            (Some(cancellation), Some(cancellation_ref))
                if cancellation.close_id == cancellation_ref.close_id
                    && cancellation.cancellation_hash() == cancellation_ref.cancellation_hash =>
            {
                self.append_step(
                    &mut journal,
                    "epoch-close-cancellation",
                    &ProtocolObjectContext::store_encrypted(
                        store_root_hash,
                        ProtocolObjectDomain::CircleEpochCloseOutcome,
                    ),
                    &coven_protocol::circle::circle_epoch_close_outcome_semantic_prefix(
                        creation.circle_id,
                        cancellation.close_id,
                    ),
                    &coven_protocol::circle::CircleEpochCloseSlotValue::Cancellation(
                        cancellation.clone(),
                    )
                    .to_bytes(),
                )
                .await?;
            }
            (None, None) => {}
            _ => {
                return Err(CircleOperationError::Journal(
                    "Circle epoch-close cancellation differs from its signed object graph"
                        .to_string(),
                ));
            }
        }
        for (index, access) in creation.access.iter().enumerate() {
            self.append_step(
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
        self.append_step(
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
        self.append_step(
            &mut journal,
            "control-head",
            &ProtocolObjectContext::store_encrypted(
                store_root_hash,
                ProtocolObjectDomain::CircleControl,
            ),
            &circle_semantic_prefix(CircleSemanticSlot::ControlHead {
                circle_id: creation.circle_id,
                control: &control_head.control,
            }),
            &serde_json::to_vec(control_head)
                .expect("circle control head serialization cannot fail"),
        )
        .await?;
        for (index, access) in creation.access.iter().enumerate() {
            self.append_step(
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
        let verified = self
            .local_writer
            .load_circle_activations(&mut self.history, &verified_commit, routing_key)
            .await?;
        let expected =
            expected_local_circle_activation(&creation, reference, &author.author_pubkey)?;
        if verified.circles() != std::slice::from_ref(&expected) {
            return Err(CircleOperationError::InvalidState(
                "stored verified Circle activation differs from its durable journal".to_string(),
            ));
        }
        {
            // Creating the head takes a position on this device's own stream, and
            // the activation below is what records the position as taken. A writer
            // that read the position between the two would compose against one this
            // operation has already claimed, so both run on one turn.
            let _authorship = self.database.author_own_stream().await;
            let head = journal.operation().policy.head.clone();
            let commit_bytes = journal.operation().commit_bytes.clone();
            let commit_hash = journal.operation().commit_ref.commit_hash;
            let stream_id = journal.operation().commit_ref.coord.stream_id;
            self.append_step(
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
            // A different writer may already hold this device's create-once head
            // slot. Observe and verify the occupant before declaring the position
            // lost; only a verified winner blocks this operation. Any invalid
            // occupant remains a loud storage or verification failure.
            let published_head = self
                .append_step(
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
                .await;
            if matches!(
                &published_head,
                Err(CircleOperationError::Object(StoreObjectError::Storage(
                    StorageError::SlotCollision(_)
                )))
            ) {
                let prepared_head = journal
                    .operation()
                    .prepared_objects
                    .get("store-head")
                    .ok_or_else(|| {
                        CircleOperationError::Journal(
                            "Merge Circle operation lacks its prepared Store head".to_string(),
                        )
                    })?;
                if let crate::sync::store::merge_conflict::ExcludedCandidateHeadObservation::MergeWinner(
                    winner,
                ) = self
                    .history
                    .observe_excluded_candidate_head(&head, &verified_commit, prepared_head)
                    .await?
                {
                    let block = coven_protocol::circle::CircleOperationBlock::PositionLost {
                        winner_commit: winner.winner().commit.commit_hash,
                    };
                    self.database
                        .block_circle_operation(operation_id, block.clone())
                        .await?;
                    return Err(CircleOperationError::Blocked { circle_id, block });
                }
            }
            published_head?;
            self.database
                .activate_circle_operation(journal, verified)
                .await?;
        }
        Ok(())
    }

    fn current_merge_authority(
        &self,
        commit: &StoreBatchCommit,
        author: &StoreDeviceRegistration,
    ) -> Result<CurrentMergeAuthority, CircleOperationError> {
        if let Some(conflict) = self.membership.conflict() {
            return Err(CircleOperationError::InvalidState(
                crate::sync::store::membership::MembershipOpsError::SemanticConflict(Box::new(
                    conflict.clone(),
                ))
                .to_string(),
            ));
        }
        if self.history.root().store_root_hash != commit.store_root_hash {
            return Err(CircleOperationError::InvalidState(
                "Circle commit names a different Store root".to_string(),
            ));
        }
        let authority = commit.membership_authority.as_ref().ok_or_else(|| {
            CircleOperationError::InvalidState(
                "Circle commit has no Store membership authority".to_string(),
            )
        })?;
        let coven_protocol::membership::MembershipStatus::Resolved(resolved) =
            self.membership.status()
        else {
            return Err(CircleOperationError::InvalidState(
                "current Store membership is conflicted".to_string(),
            ));
        };
        let mut matching = resolved.grants.iter().filter(|(_, state)| {
            let record = state.record();
            record.member_pubkey == author.author_pubkey
                && matches!(
                    record.role,
                    coven_protocol::membership::StoreMembershipRoleGrant::Owner { .. }
                        | coven_protocol::membership::StoreMembershipRoleGrant::Member
                )
                && &record.creation_authority == authority
        });
        let Some((grant_id, state)) = matching.next() else {
            return Err(CircleOperationError::InvalidState(
                "Circle commit Store membership authority identifies no exact grant".to_string(),
            ));
        };
        if matching.next().is_some() {
            return Err(CircleOperationError::InvalidState(
                "Circle commit Store membership authority identifies multiple grants".to_string(),
            ));
        }
        Ok(match state {
            coven_protocol::causal_grants::GrantState::Active { .. } => {
                CurrentMergeAuthority::Active
            }
            coven_protocol::causal_grants::GrantState::Tombstoned { .. } => {
                CurrentMergeAuthority::Revoked {
                    grant_id: grant_id.clone(),
                }
            }
        })
    }

    async fn append_step(
        &self,
        journal: &mut CircleOperationJournal,
        step: &str,
        context: &ProtocolObjectContext,
        semantic_prefix: &str,
        bytes: &[u8],
    ) -> Result<(), CircleOperationError> {
        let persisted = self
            .create_or_open_step(journal, step, context, semantic_prefix)
            .await?;
        if persisted != bytes {
            return Err(CircleOperationError::InvalidState(format!(
                "circle upload step {step:?} differs from its prepared journal bytes"
            )));
        }
        self.record_completed_step(journal, step).await
    }

    async fn append_hashed_step(
        &self,
        journal: &mut CircleOperationJournal,
        step: &str,
        context: &ProtocolObjectContext,
        semantic_prefix: &str,
        expected_hash: coven_protocol::store_commit::ObjectHash,
    ) -> Result<(), CircleOperationError> {
        let persisted = self
            .create_or_open_step(journal, step, context, semantic_prefix)
            .await?;
        if coven_protocol::store_commit::ObjectHash::digest(&persisted) != expected_hash {
            return Err(CircleOperationError::InvalidState(format!(
                "circle upload step {step:?} differs from its signed image hash"
            )));
        }
        self.record_completed_step(journal, step).await
    }

    /// Record the step as done, durably and then in the journal this
    /// publication is walking, so a resumed run skips exactly the steps whose
    /// rows committed.
    async fn record_completed_step(
        &self,
        journal: &mut CircleOperationJournal,
        step: &str,
    ) -> Result<(), CircleOperationError> {
        self.database
            .complete_circle_operation_upload_step(&journal.operation_id, step)
            .await?;
        journal.uploaded.insert(step.to_string());
        Ok(())
    }

    async fn create_or_open_step(
        &self,
        journal: &CircleOperationJournal,
        step: &str,
        context: &ProtocolObjectContext,
        semantic_prefix: &str,
    ) -> Result<Vec<u8>, CircleOperationError> {
        let object = journal
            .operation()
            .prepared_objects
            .get(step)
            .cloned()
            .ok_or_else(|| {
                CircleOperationError::Journal(format!(
                    "Circle upload step {step:?} lacks its prepared exact object"
                ))
            })?;
        // The bytes remain in the spool through operation completion. Opening
        // them locally validates the journal's semantic value without a cloud
        // body GET; the provider adapter proves the stored representation.
        let spool = coven_database::payload_spool::PayloadSpool::new(self.store_dir);
        let stored_bytes = spool.read(object.stored_hash()).await.map_err(|error| {
            CircleOperationError::Journal(format!("Circle upload step {step:?}: {error}"))
        })?;
        let prepared = coven_protocol::objects::PreparedExactObject::new(object, stored_bytes)
            .map_err(coven_protocol::objects::StoreObjectError::from)?;
        let opened = self
            .storage
            .open_prepared_protocol_object(context, &prepared, semantic_prefix)
            .await
            .map_err(coven_protocol::objects::StoreObjectError::from)?;
        if !journal.uploaded.contains(step) {
            self.storage
                .create_protocol_object(&prepared)
                .await
                .map_err(coven_protocol::objects::StoreObjectError::from)?;
        }
        Ok(opened)
    }
}

fn verify_prepared_objects_are_signed(
    journal: &CircleOperationJournal,
    reference: &coven_protocol::store_commit::CircleControlRef,
) -> Result<(), CircleOperationError> {
    let operation = journal.operation();
    let objects = reference.objects();
    let mut signed = BTreeSet::<coven_protocol::objects::ExactObjectRef>::from([
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
    if let Some(intent) = &objects.close_intent {
        signed.insert(intent.object.clone());
    }
    if let Some(outcome) = &objects.close_outcome {
        signed.insert(outcome.object.clone());
    }
    if let Some(cancellation) = &objects.close_cancellation {
        signed.insert(cancellation.object.clone());
    }
    for access in &objects.access {
        signed.insert(access.leaf.object.clone());
        signed.insert(access.envelope.object.clone());
        if let Some(bootstrap) = &access.bootstrap {
            signed.insert(bootstrap.object.clone());
        }
    }
    for (step, object) in &operation.prepared_objects {
        if step != "store-head" && !signed.contains(object) {
            return Err(CircleOperationError::Journal(format!(
                "Circle upload step {step:?} names an object outside its signed Store commit graph"
            )));
        }
    }
    Ok(())
}

fn expected_local_circle_activation(
    creation: &PreparedCircleTransition,
    reference: &coven_protocol::store_commit::CircleControlRef,
    author_pubkey: &str,
) -> Result<VerifiedCircleReference, CircleOperationError> {
    if creation.control.value.state().is_deleted() {
        // A deletion journals no access material; it activates locally to the
        // terminal Deleted state with no local access.
        return Ok(VerifiedCircleReference {
            reference: reference.clone(),
            circle_id: creation.circle_id,
            control: creation.control.clone(),
            local_access: None,
        });
    }
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

enum CurrentMergeAuthority {
    Active,
    Revoked {
        grant_id: coven_protocol::membership::MembershipGrantId,
    },
}

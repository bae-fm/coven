use super::{
    load_circle_activations, read_exact_circle_object, verify_control_context_for_verified_commit,
    verify_prepared_objects_are_signed, CircleOperationError, CircleOperationJournal,
    CircleOperationPolicy, VerifiedCircleAccess, VerifiedCircleActive, VerifiedCircleReference,
};
use crate::encryption::{EncryptionService, MasterKeyring};
use crate::keys::UserKeypair;
use crate::sync::circle::{
    circle_semantic_prefix, CircleAccessDisposition, CircleOperationId, CircleOperationState,
    CircleSemanticSlot, CircleTransitionPolicyObjects, PreparedCircleTransition,
};
use crate::sync::storage::{
    ProtocolObjectContext, ProtocolObjectDomain, StorageError, SyncStorage,
};
use crate::sync::store::database::StoreDatabase;
use crate::sync::store_commit::{
    circle_access_envelope_semantic_prefix, circle_access_leaf_semantic_prefix,
    commit_semantic_prefix, head_slot_prefix, StoreBatchCommit, StoreDeviceRegistration,
    VerifiedStoreBatchCommit,
};
use crate::sync::store_objects::StoreObjectError;

pub(super) async fn publish_circle_operation(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    operation_id: &CircleOperationId,
    identity: &UserKeypair,
    routing_key: Option<&crate::sync::circle::RowRoutingKey>,
) -> Result<(), CircleOperationError> {
    let root = database
        .local_store_root_ref()
        .await?
        .ok_or(CircleOperationError::MissingState("Store root reference"))?;
    let mut history_verifier = crate::sync::store::pull::MergeHistoryVerifier::new(storage, &root)
        .await
        .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
    publish_circle_operation_with_history(
        database,
        storage,
        &root,
        &mut history_verifier,
        operation_id,
        identity,
        routing_key,
    )
    .await
}

pub(super) async fn publish_circle_operation_with_history(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    root: &crate::sync::store_commit::StoreRootRef,
    history_verifier: &mut crate::sync::store::pull::MergeHistoryVerifier<'_>,
    operation_id: &CircleOperationId,
    identity: &UserKeypair,
    routing_key: Option<&crate::sync::circle::RowRoutingKey>,
) -> Result<(), CircleOperationError> {
    let mut journal = database
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
    if root.store_root_hash != store_root_hash {
        return Err(CircleOperationError::InvalidState(
            "Circle commit names a different Store root".to_string(),
        ));
    }
    if history_verifier.commit_verifier().root() != root {
        return Err(CircleOperationError::InvalidState(
            "Circle publication verifier belongs to another Store root".to_string(),
        ));
    }
    let circle_encryption = EncryptionService::from(
        MasterKeyring::from_serialized(&creation.keyring)
            .map_err(|error| CircleOperationError::Journal(format!("circle keyring: {error}")))?,
    );
    let verified_commit = history_verifier
        .commit_verifier()
        .authenticate_bytes(
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
        crate::sync::store_commit::StoreCommitOperations::is_circle_control_activation_only,
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
    if let CurrentMergeAuthority::Revoked { grant_id } =
        current_merge_authority(database, history_verifier, commit, &author).await?
    {
        let block = crate::sync::circle::CircleOperationBlock::AuthorityLost { grant_id };
        database
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
        let (_, state_after) = crate::sync::store::pull::retained_merge_device_state_for_order(
            database,
            storage,
            root,
            &commit.order,
        )
        .await
        .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        let head_ref = crate::sync::store_commit::StoreDeviceHeadRef {
            head_hash: head.head_hash(),
            object: prepared_head.reference().clone(),
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
        append_step(
            database,
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
            &serde_json::to_vec(&creation.metadata)
                .expect("circle metadata serialization cannot fail"),
        )
        .await?;
        append_step(
            database,
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
                            "prepared metadata head is absent from its signed object graph"
                                .to_string(),
                        )
                    })?,
            }),
            &serde_json::to_vec(metadata_head)
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
        append_step(
            database,
            storage,
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
        append_step(
            database,
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
            storage.verify_blob_object(stored).await.map_err(|error| {
                CircleOperationError::InvalidState(format!(
                    "verify Circle bootstrap blob {}: {error}",
                    crate::sync::remote_object::remote_object_id(stored.object())
                ))
            })?;
        }
        let mut matching_steps = journal
            .operation()
            .prepared_objects
            .iter()
            .filter(|(_, prepared)| prepared.reference() == &bootstrap.image.object);
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
        let prefix = crate::sync::store_commit::circle_bootstrap_image_semantic_prefix(
            access.leaf.value.circle_id,
            commit.candidate_family(),
            &access.leaf.value.owner_pubkey,
            access.leaf.value.epoch_id,
            &access.leaf.value.recipient_slot,
            bootstrap.image.image_hash,
        );
        append_hashed_step(
            database,
            storage,
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
            append_step(
                database,
                storage,
                &mut journal,
                "epoch-close-intent",
                &ProtocolObjectContext::circle(
                    store_root_hash,
                    ProtocolObjectDomain::CircleEpochCloseIntent,
                    circle_encryption.clone(),
                ),
                &crate::sync::circle::circle_epoch_close_intent_semantic_prefix(
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
            append_step(
                database,
                storage,
                &mut journal,
                "epoch-close-outcome",
                &ProtocolObjectContext::store_encrypted(
                    store_root_hash,
                    ProtocolObjectDomain::CircleEpochCloseOutcome,
                ),
                &crate::sync::circle::circle_epoch_close_outcome_semantic_prefix(
                    creation.circle_id,
                    outcome.close_id,
                ),
                &crate::sync::circle::CircleEpochCloseSlotValue::Outcome(outcome.clone())
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
            append_step(
                database,
                storage,
                &mut journal,
                "epoch-close-cancellation",
                &ProtocolObjectContext::store_encrypted(
                    store_root_hash,
                    ProtocolObjectDomain::CircleEpochCloseOutcome,
                ),
                &crate::sync::circle::circle_epoch_close_outcome_semantic_prefix(
                    creation.circle_id,
                    cancellation.close_id,
                ),
                &crate::sync::circle::CircleEpochCloseSlotValue::Cancellation(cancellation.clone())
                    .to_bytes(),
            )
            .await?;
        }
        (None, None) => {}
        _ => {
            return Err(CircleOperationError::Journal(
                "Circle epoch-close cancellation differs from its signed object graph".to_string(),
            ));
        }
    }
    for (index, access) in creation.access.iter().enumerate() {
        append_step(
            database,
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
        database,
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
        database,
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
            database,
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
    let verified = load_circle_activations(
        database,
        storage,
        root,
        history_verifier,
        &verified_commit,
        identity,
        routing_key,
    )
    .await?;
    let expected = expected_local_circle_activation(&creation, reference, &author.author_pubkey)?;
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
        let _authorship = database.author_own_stream().await;
        let head = journal.operation().policy.head.clone();
        let commit_bytes = journal.operation().commit_bytes.clone();
        let commit_hash = journal.operation().commit_ref.commit_hash;
        let stream_id = journal.operation().commit_ref.coord.stream_id;
        append_step(
            database,
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
        let published_head = append_step(
            database,
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
        .await;
        if matches!(
            &published_head,
            Err(CircleOperationError::Object(StoreObjectError::Storage(
                StorageError::SlotCollision(_)
            )))
        ) {
            block_lost_position(
                database,
                storage,
                history_verifier.commit_verifier(),
                &journal,
                &head,
                &verified_commit,
                operation_id,
                circle_id,
            )
            .await?;
        }
        published_head?;
        database
            .activate_circle_operation(journal, verified)
            .await?;
    }
    Ok(())
}

fn expected_local_circle_activation(
    creation: &PreparedCircleTransition,
    reference: &crate::sync::store_commit::CircleControlRef,
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
        grant_id: crate::sync::membership::MembershipGrantId,
    },
}

async fn current_merge_authority(
    database: &StoreDatabase,
    history_verifier: &mut crate::sync::store::pull::MergeHistoryVerifier<'_>,
    commit: &StoreBatchCommit,
    author: &StoreDeviceRegistration,
) -> Result<CurrentMergeAuthority, CircleOperationError> {
    let db = database.sqlite();
    let founder = db
        .get_protocol_state(crate::sync::store::membership::OWNER_PUBKEY_STATE_KEY)
        .await?
        .ok_or(CircleOperationError::MissingState("Store founder"))?;
    if history_verifier.root().store_root_hash != commit.store_root_hash {
        return Err(CircleOperationError::InvalidState(
            "Circle commit names a different Store root".to_string(),
        ));
    }
    let current = crate::sync::store::membership::load_and_persist_owner_anchor_with_history(
        history_verifier,
        &founder,
        database,
    )
    .await
    .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
    let authority = commit.membership_authority.as_ref().ok_or_else(|| {
        CircleOperationError::InvalidState(
            "Circle commit has no Store membership authority".to_string(),
        )
    })?;
    let crate::sync::membership::MembershipStatus::Resolved(resolved) = current.status() else {
        return Err(CircleOperationError::InvalidState(
            "current Store membership is conflicted".to_string(),
        ));
    };
    let mut matching = resolved.grants.iter().filter(|(_, state)| {
        let record = state.record();
        record.member_pubkey == author.author_pubkey
            && matches!(
                record.role,
                crate::sync::membership::StoreMembershipRoleGrant::Owner { .. }
                    | crate::sync::membership::StoreMembershipRoleGrant::Member
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
        crate::sync::causal_grants::GrantState::Active { .. } => CurrentMergeAuthority::Active,
        crate::sync::causal_grants::GrantState::Tombstoned { .. } => {
            CurrentMergeAuthority::Revoked {
                grant_id: grant_id.clone(),
            }
        }
    })
}

/// A different writer holds this device's head slot at the position the operation
/// composed against. Mirroring `resolve_head_collision`, the occupant is observed
/// and verified before anything is concluded from it: only a real winner ends the
/// operation. The candidate commit is bound to that create-once slot, so no
/// re-publish can ever take it — the operation is blocked as a fact reported to
/// its initiator, who discards it and re-issues. An occupant that does not verify
/// is not a lost race, and its loud failure is left to stand.
#[allow(clippy::too_many_arguments)]
async fn block_lost_position(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    commit_verifier: &mut crate::sync::store::pull::StoreCommitVerifier<'_>,
    journal: &CircleOperationJournal,
    head: &crate::sync::store_commit::StoreDeviceHead,
    commit: &VerifiedStoreBatchCommit,
    operation_id: &CircleOperationId,
    circle_id: crate::sync::circle::CircleId,
) -> Result<(), CircleOperationError> {
    let prepared_head = journal
        .operation()
        .prepared_objects
        .get("store-head")
        .ok_or_else(|| {
            CircleOperationError::Journal(
                "Merge Circle operation lacks its prepared Store head".to_string(),
            )
        })?;
    let crate::sync::store::abandonment::ExcludedCandidateHeadObservation::MergeWinner(winner) =
        crate::sync::store::abandonment::observe_excluded_candidate_head(
            database,
            storage,
            commit_verifier,
            head,
            commit,
            prepared_head.reference(),
        )
        .await?
    else {
        return Ok(());
    };
    let block = crate::sync::circle::CircleOperationBlock::PositionLost {
        winner_commit: winner.winner().commit.commit_hash,
    };
    database
        .block_circle_operation(operation_id, block.clone())
        .await?;
    Err(CircleOperationError::Blocked { circle_id, block })
}

async fn append_step(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    journal: &mut CircleOperationJournal,
    step: &str,
    context: &ProtocolObjectContext,
    semantic_prefix: &str,
    bytes: &[u8],
) -> Result<(), CircleOperationError> {
    let persisted = create_or_read_step(storage, journal, step, context, semantic_prefix).await?;
    if persisted != bytes {
        return Err(CircleOperationError::InvalidState(format!(
            "circle upload step {step:?} differs from its prepared journal bytes"
        )));
    }
    complete_step(database, journal, step).await
}

async fn append_hashed_step(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    journal: &mut CircleOperationJournal,
    step: &str,
    context: &ProtocolObjectContext,
    semantic_prefix: &str,
    expected_hash: crate::sync::store_commit::ObjectHash,
) -> Result<(), CircleOperationError> {
    let persisted = create_or_read_step(storage, journal, step, context, semantic_prefix).await?;
    if crate::sync::store_commit::ObjectHash::digest(&persisted) != expected_hash {
        return Err(CircleOperationError::InvalidState(format!(
            "circle upload step {step:?} differs from its signed image hash"
        )));
    }
    complete_step(database, journal, step).await
}

async fn create_or_read_step(
    storage: &dyn SyncStorage,
    journal: &CircleOperationJournal,
    step: &str,
    context: &ProtocolObjectContext,
    semantic_prefix: &str,
) -> Result<Vec<u8>, CircleOperationError> {
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
        return read_exact_circle_object(storage, context, prepared.reference(), semantic_prefix)
            .await;
    }
    storage
        .create_protocol_object(&prepared)
        .await
        .map_err(crate::sync::store_objects::StoreObjectError::from)?;
    read_exact_circle_object(storage, context, prepared.reference(), semantic_prefix).await
}

async fn complete_step(
    database: &StoreDatabase,
    journal: &mut CircleOperationJournal,
    step: &str,
) -> Result<(), CircleOperationError> {
    if journal.operation().uploaded.contains(step) {
        return Ok(());
    }
    journal.operation_mut().uploaded.insert(step.to_string());
    database.update_circle_operation(journal.clone()).await?;
    Ok(())
}

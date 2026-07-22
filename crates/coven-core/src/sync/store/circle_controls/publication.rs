use super::{
    load_circle_activations, load_exact_slot_bytes, verify_control_context,
    verify_prepared_objects_are_signed, CircleOperationError, CircleOperationJournal,
    CircleOperationPolicy, VerifiedCircleAccess, VerifiedCircleActive, VerifiedCircleReference,
};
use crate::database::Database;
use crate::encryption::{EncryptionService, MasterKeyring};
use crate::keys::UserKeypair;
use crate::sync::circle::{
    circle_semantic_prefix, CircleAccessDisposition, CircleOperationId, CircleOperationState,
    CircleSemanticSlot, CircleTransitionPolicyObjects, PreparedCircleTransition,
};
use crate::sync::storage::{ProtocolObjectContext, ProtocolObjectDomain, SyncStorage};
use crate::sync::store::database::StoreDatabase;
use crate::sync::store_commit::{
    circle_access_envelope_semantic_prefix, circle_access_leaf_semantic_prefix,
    commit_semantic_prefix, head_slot_prefix, StoreBatchCommit, StoreDeviceRegistration,
};

pub(super) async fn publish_circle_operation(
    db: &Database,
    storage: &dyn SyncStorage,
    operation_id: &CircleOperationId,
    identity: &UserKeypair,
) -> Result<(), CircleOperationError> {
    let mut journal = StoreDatabase::new(db)
        .circle_operation(operation_id)
        .await?
        .ok_or_else(|| {
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
    if !has_current_merge_authority(db, storage, &commit, &author).await? {
        let reason = "circle operation author is not a current Store writer under its exact grant"
            .to_string();
        StoreDatabase::new(db)
            .block_circle_operation(operation_id, reason.clone())
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
        let (_, state_after) = crate::sync::store::pull::retained_merge_device_state_for_order(
            db,
            storage,
            &root,
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
        .get_protocol_state(crate::sync::store::membership::OWNER_PUBKEY_STATE_KEY)
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
    StoreDatabase::new(db)
        .activate_circle_operation(journal, verified)
        .await?;
    Ok(())
}

fn expected_local_circle_activation(
    creation: &PreparedCircleTransition,
    reference: &crate::sync::store_commit::CircleControlRef,
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
        .get_protocol_state(crate::sync::store::membership::OWNER_PUBKEY_STATE_KEY)
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
        crate::sync::store::membership::load_and_persist_owner_anchor(storage, &root, &founder, db)
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
        .map_err(crate::sync::store_objects::StoreObjectError::from)?;
    let persisted =
        load_exact_slot_bytes(storage, context, prepared.reference(), semantic_prefix).await?;
    if persisted != bytes {
        return Err(CircleOperationError::InvalidState(format!(
            "circle upload step {step:?} differs from its prepared journal bytes"
        )));
    }
    journal.operation_mut().uploaded.insert(step.to_string());
    StoreDatabase::new(db)
        .update_circle_operation(journal.clone())
        .await?;
    Ok(())
}

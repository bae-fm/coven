use std::collections::{BTreeMap, BTreeSet};

use super::commands::CircleOperationRequest;
use super::{
    read_exact_circle_object, CircleOperationError, CircleOperationJournal, CircleOperationPolicy,
    CircleOperationProgress, CircleTransitionHistory, PreparedCircleOperation,
};
use crate::encryption::{EncryptionService, MasterKeyring};
use crate::keys::{self, UserKeypair};
use crate::sync::circle::{
    circle_control_head_prefix, circle_metadata_head_prefix, circle_roster_head_prefix,
    circle_semantic_prefix, CircleAccessDisposition, CircleMetadataHeadRef, CircleOperationId,
    CircleRosterHeadRef, CircleSemanticSlot, CircleTransitionDraft, CircleTransitionPolicyObjects,
    PreparedCircleTransition, StoreMembershipStateRef,
};
use crate::sync::storage::{
    ExactObjectRef, PreparedExactObject, ProtocolObjectContext, ProtocolObjectDomain, SyncStorage,
};
use crate::sync::store::owner::AuthorizedStoreAuthority;
use crate::sync::store_commit::{
    circle_access_envelope_semantic_prefix, circle_access_leaf_semantic_prefix,
    commit_semantic_prefix, head_slot_prefix, CandidateFamilyId, CircleAccessEnvelopeObjectRef,
    CircleAccessLeafObjectRef, CircleAccessObjectRef, CircleActivationObjects,
    CircleMetadataObjectRef, GrantStreamAnchor, ObjectHash, StoreBatchCommit, StoreCommitCoord,
    StoreCommitOperationsInput, StoreCommitOrder, StoreDeviceHead, StoreDeviceRegistration,
    StoreDeviceRegistrationRef, StoreOperationMembershipAuthority, StoreRootRef, StreamActivation,
    StreamAnchorDomain, SuccessorLink,
};

pub(super) fn verify_prepared_objects_are_signed(
    journal: &CircleOperationJournal,
    reference: &crate::sync::store_commit::CircleControlRef,
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
    for (step, prepared) in &operation.prepared_objects {
        if step != "store-head" && !signed.contains(prepared.reference()) {
            return Err(CircleOperationError::Journal(format!(
                "Circle upload step {step:?} names an object outside its signed Store commit graph"
            )));
        }
    }
    Ok(())
}

pub(super) fn signed_circle_commit(
    store_root_hash: ObjectHash,
    operation_id: crate::WriteId,
    coord: StoreCommitCoord,
    author_registration: StoreDeviceRegistrationRef,
    author: &StoreDeviceRegistration,
    order: StoreCommitOrder,
    membership_state: StoreMembershipStateRef,
    device_state: crate::sync::store_commit::StoreDeviceStateRef,
    membership_authority: StoreOperationMembershipAuthority,
    circle_reference: crate::sync::store_commit::CircleControlRef,
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
            circle_acknowledgements: Vec::new(),
            control: None,
            device_join_attempt_decisions: Vec::new(),
            device_join_outcomes: Vec::new(),
            device_join_cleanup_receipts: Vec::new(),
            provider_access_grants: Vec::new(),
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

pub(super) async fn prepare_circle_object(
    storage: &dyn SyncStorage,
    context: &ProtocolObjectContext,
    semantic_prefix: &str,
    extension: &str,
    bytes: Vec<u8>,
) -> Result<PreparedExactObject, CircleOperationError> {
    let slot = storage
        .allocate_protocol_slot(context, semantic_prefix, extension)
        .await
        .map_err(crate::sync::store_objects::StoreObjectError::from)?;
    storage
        .prepare_protocol_object(context, slot, semantic_prefix, bytes)
        .map_err(crate::sync::store_objects::StoreObjectError::from)
        .map_err(CircleOperationError::from)
}

pub(super) fn prepare_circle_object_at(
    storage: &dyn SyncStorage,
    context: &ProtocolObjectContext,
    slot: crate::storage::cloud::ObjectSlot,
    semantic_prefix: &str,
    bytes: Vec<u8>,
) -> Result<PreparedExactObject, CircleOperationError> {
    storage
        .prepare_protocol_object(context, slot, semantic_prefix, bytes)
        .map_err(crate::sync::store_objects::StoreObjectError::from)
        .map_err(CircleOperationError::from)
}

pub(crate) async fn verified_circle_bootstrap_blobs(
    storage: &dyn SyncStorage,
    circle_id: crate::sync::circle::CircleId,
    snapshot: &crate::sync::store::snapshot::CreatedSnapshot,
) -> Result<Vec<crate::blob::RowBlobRef>, CircleOperationError> {
    let mut blobs = Vec::with_capacity(snapshot.blobs.len());
    for captured in &snapshot.blobs {
        let crate::sync::store::snapshot::SnapshotBlobAudience::Circle {
            circle_id: captured_circle,
            ..
        } = captured.audience
        else {
            return Err(CircleOperationError::InvalidState(
                "Circle bootstrap contains a Store-audience blob".to_string(),
            ));
        };
        let previous = captured.fact.previous.as_ref().ok_or_else(|| {
            CircleOperationError::InvalidState(format!(
                "Circle bootstrap blob {}/{} has no activated exact remote binding",
                captured.fact.blob.namespace, captured.fact.blob.id
            ))
        })?;
        if captured_circle != circle_id
            || previous.authority.remote_audience()
                != crate::blob::locator::RemoteAudience::Circle(circle_id)
            || previous.stored.locator().audience()
                != crate::blob::locator::RemoteAudience::Circle(circle_id)
        {
            return Err(CircleOperationError::InvalidState(
                "Circle bootstrap blob belongs to another audience".to_string(),
            ));
        }
        storage
            .verify_blob_object(&previous.stored)
            .await
            .map_err(|error| {
                CircleOperationError::InvalidState(format!(
                    "verify Circle bootstrap blob {}/{}: {error}",
                    captured.fact.blob.namespace, captured.fact.blob.id
                ))
            })?;
        blobs.push(
            crate::blob::RowBlobRef::new(
                captured.fact.table.clone(),
                captured.fact.row_id.clone(),
                captured.fact.row_stamp.clone(),
                captured.fact.column.clone(),
                captured.fact.blob.clone(),
                captured.fact.plaintext_size,
                captured.fact.plaintext_hash,
                crate::blob::RowBlobAuthority::Remote(previous.authority.clone()),
                Some(previous.stored.clone()),
            )
            .map_err(CircleOperationError::InvalidState)?,
        );
    }
    blobs.sort_by_cached_key(|blob| {
        serde_json::to_vec(blob).expect("row blob reference serialization cannot fail")
    });
    if blobs.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(CircleOperationError::InvalidState(
            "Circle bootstrap repeats an exact row blob binding".to_string(),
        ));
    }
    Ok(blobs)
}

pub(super) async fn prepare_circle_activation_objects(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    mut draft: CircleTransitionDraft,
    history: &CircleTransitionHistory,
    merged_branch_objects: &[CircleActivationObjects],
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
    if encryption.seal_key_fingerprint() != draft.metadata.key_fingerprint {
        return Err(CircleOperationError::InvalidState(
            "Circle transition metadata does not use the keyring seal key".to_string(),
        ));
    }
    let metadata_context = ProtocolObjectContext::circle(
        store_root_hash,
        ProtocolObjectDomain::CircleMetadata,
        encryption.clone(),
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
    let previous_objects =
        previous_control.map(crate::sync::store_commit::CircleControlRef::objects);
    let mut roster_entries =
        previous_objects.map_or_else(BTreeMap::new, |objects| objects.roster_entries.clone());
    let mut roster_heads =
        previous_objects.map_or_else(Vec::new, |objects| objects.roster_heads.clone());
    let mut roster_frontier = if matches!(
        &draft.policy.roster,
        crate::sync::circle::CircleRosterDraftPolicy::Founder { .. }
    ) {
        Vec::new()
    } else {
        draft.control.value.access_epoch().roster.heads.clone()
    };
    let mut roster_resolutions =
        previous_objects.map_or_else(BTreeMap::new, |objects| objects.roster_resolutions.clone());
    let mut metadata_entries =
        previous_objects.map_or_else(BTreeMap::new, |objects| objects.metadata_entries.clone());
    let mut metadata_heads =
        previous_objects.map_or_else(Vec::new, |objects| objects.metadata_heads.clone());
    // A control-conflict resolution covers the losing branches too: union their
    // already-published objects into the seed so the resolution can verify both
    // its merged current frontier and historical authority references. Roster
    // heads are an object inventory: collapsing them by author stream would
    // discard the older head that created an Owner grant. Metadata heads carry
    // the current frontier because their signed predecessor links provide their
    // history. The draft control separately carries the signed current
    // frontiers that the resolution shaped.
    for branch in merged_branch_objects {
        roster_entries.extend(branch.roster_entries.clone());
        roster_resolutions.extend(branch.roster_resolutions.clone());
        metadata_entries.extend(branch.metadata_entries.clone());
        roster_heads.extend(branch.roster_heads.iter().cloned());
        for head in &branch.metadata_heads {
            crate::sync::circle::merge_frontier_head(
                &mut metadata_heads,
                head.clone(),
                |head| head.coord.stream_key(),
                |head| head.coord.seq,
            );
        }
    }
    roster_heads.sort();
    roster_heads.dedup();
    metadata_heads.sort_by_key(|head| head.coord.stream_key());
    let mut prepared = BTreeMap::new();
    let mut stream_activations = Vec::new();
    let mut close_outcome = None;
    let mut close_cancellation = None;

    let policy_objects = {
        let owner_grant = draft.metadata.author_owner_grant.clone();
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

        let roster_policy = std::mem::replace(
            &mut draft.policy.roster,
            crate::sync::circle::CircleRosterDraftPolicy::Inherited,
        );
        let roster_successor = match roster_policy {
            crate::sync::circle::CircleRosterDraftPolicy::Inherited => None,
            crate::sync::circle::CircleRosterDraftPolicy::Founder { entry } => {
                Some((true, None, entry))
            }
            crate::sync::circle::CircleRosterDraftPolicy::Successor { predecessor, entry } => {
                Some((false, Some(predecessor), entry))
            }
        };
        let prepared_roster = if let Some((founder, predecessor_chain, mut entry)) =
            roster_successor
        {
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
                serde_json::to_vec(&entry).expect("Circle roster entry serialization cannot fail"),
            )
            .await?;
            prepared.insert("roster-entry".to_string(), entry_prepared.clone());
            roster_entries.insert(entry.coord(), entry_prepared.reference().clone());

            let stream_key = entry.coord().stream_key();
            let prior_roster = roster_frontier
                .iter()
                .find(|head| head.coord.stream_key() == stream_key)
                .cloned();
            let (current_slot, seq, predecessor, activation_id, activation) =
                if let Some(reference) = &prior_roster {
                    let prefix = circle_semantic_prefix(CircleSemanticSlot::RosterHead {
                        circle_id: draft.circle_id,
                        head: reference,
                    });
                    let bytes = read_exact_circle_object(
                        storage,
                        &roster_context,
                        &reference.object,
                        &prefix,
                    )
                    .await?;
                    let head: crate::sync::circle::CircleRosterHead =
                        serde_json::from_slice(&bytes).map_err(|error| {
                            CircleOperationError::InvalidState(format!(
                                "parse predecessor Circle roster head: {error}"
                            ))
                        })?;
                    if !head.verify_for_registration(author)
                        || head.entry_coord() != reference.coord
                        || head.head_hash() != reference.head_hash
                    {
                        return Err(CircleOperationError::InvalidState(
                            "Circle roster predecessor head failed verification".to_string(),
                        ));
                    }
                    (
                        head.successor.next_slot.clone(),
                        head.seq.checked_add(1).ok_or_else(|| {
                            CircleOperationError::InvalidState(
                                "Circle roster sequence overflow".to_string(),
                            )
                        })?,
                        Some(reference.object.clone()),
                        head.successor.activation,
                        None,
                    )
                } else {
                    let current_prefix = circle_roster_head_prefix(draft.circle_id, &stream_key, 1);
                    let current_slot = storage
                        .allocate_protocol_slot(&roster_context, &current_prefix, ".json")
                        .await
                        .map_err(crate::sync::store_objects::StoreObjectError::from)?;
                    let activation = StreamActivation::grant_authorized(
                        store_root_hash,
                        author_registration.clone(),
                        owner_grant.clone(),
                        GrantStreamAnchor::CircleRoster {
                            circle_id: draft.circle_id,
                            first_slot: current_slot.clone(),
                        },
                    );
                    (
                        current_slot,
                        1,
                        None,
                        activation.activation_id(),
                        Some(activation),
                    )
                };
            if entry.seq != seq
                || entry.previous_hash
                    != prior_roster
                        .as_ref()
                        .map(|reference| reference.coord.entry_hash)
            {
                return Err(CircleOperationError::InvalidState(
                    "Circle roster successor differs from its exact author-stream predecessor"
                        .to_string(),
                ));
            }
            let current_prefix = circle_roster_head_prefix(draft.circle_id, &stream_key, seq);
            let next_slot = storage
                .allocate_protocol_slot(
                    &roster_context,
                    &circle_roster_head_prefix(
                        draft.circle_id,
                        &stream_key,
                        seq.checked_add(1).ok_or_else(|| {
                            CircleOperationError::InvalidState(
                                "Circle roster sequence overflow".to_string(),
                            )
                        })?,
                    ),
                    ".json",
                )
                .await
                .map_err(crate::sync::store_objects::StoreObjectError::from)?;
            let head = crate::sync::circle::CircleRosterHead::signed(
                &entry,
                entry_prepared.reference().clone(),
                SuccessorLink {
                    activation: activation_id,
                    predecessor,
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
            roster_frontier.retain(|reference| reference.coord.stream_key() != stream_key);
            roster_frontier.push(head_ref.clone());
            roster_frontier.sort_by_key(|head| head.coord.stream_key());
            roster_heads.push(head_ref.clone());
            roster_heads.sort();
            if let Some(activation) = activation {
                stream_activations.push(activation);
            }
            Some((founder, predecessor_chain, entry, head, head_ref))
        } else {
            None
        };

        if let Some((_, predecessor_chain, entry, head, reference)) = &prepared_roster {
            let exact_head =
                crate::sync::circle::ExactCircleRosterHead::bind(head.clone(), reference.clone())
                    .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
            let chain = match predecessor_chain {
                Some(predecessor) => predecessor.with_exact_successor(entry.clone(), exact_head),
                None => crate::sync::circle::CircleRosterChain::from_entries_with_heads(
                    vec![entry.clone()],
                    vec![exact_head],
                ),
            }
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
            draft.roster = chain
                .try_resolved()
                .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        }

        let roster_state = crate::sync::circle::MergeCircleRosterStateRef {
            heads: roster_frontier,
            resolutions: roster_resolutions.keys().cloned().collect(),
            state_hash: draft.roster.state_hash,
        };
        let (metadata_state, metadata_head) = if draft.policy.metadata_successor {
            let selects_authored_metadata =
                draft.control.value.access_epoch().metadata.selected == draft.metadata.coord();
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
                    let bytes = read_exact_circle_object(
                        storage,
                        &metadata_context,
                        &reference.object,
                        &prefix,
                    )
                    .await?;
                    let head: crate::sync::circle::CircleMetadataHead =
                        serde_json::from_slice(&bytes).map_err(|error| {
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
                    let stream_key = crate::sync::circle::CircleAuthorStreamKey {
                        author_pubkey: draft.metadata.author_pubkey.clone(),
                        device_id: draft.metadata.device_id.clone(),
                        stream_id: metadata_stream,
                        author_owner_grant: owner_grant.clone(),
                    };
                    let prefix = circle_metadata_head_prefix(draft.circle_id, &stream_key, 1);
                    let slot = storage
                        .allocate_protocol_slot(&metadata_context, &prefix, ".json")
                        .await
                        .map_err(crate::sync::store_objects::StoreObjectError::from)?;
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
                serde_json::to_vec(&draft.metadata)
                    .expect("Circle metadata serialization cannot fail"),
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
                    let bytes = read_exact_circle_object(
                        storage,
                        &metadata_context,
                        &reference.object,
                        &prefix,
                    )
                    .await?;
                    let head: crate::sync::circle::CircleMetadataHead =
                        serde_json::from_slice(&bytes).map_err(|error| {
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
                .map_err(crate::sync::store_objects::StoreObjectError::from)?;
            let metadata_head = crate::sync::circle::CircleMetadataHead::signed(
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
                    .state
                    .access_epoch()
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
                    .state
                    .access_epoch()
                    .metadata
                    .selected
                    .clone()
            };
            (
                crate::sync::circle::MergeCircleMetadataStateRef {
                    heads: metadata_heads.clone(),
                    selected: selected.clone(),
                    state_hash: if selected == draft.metadata.coord() {
                        draft.metadata.metadata_hash()
                    } else {
                        draft.control.value.access_epoch().metadata.state_hash
                    },
                },
                Some(metadata_head),
            )
        } else {
            let metadata_state = draft.control.value.access_epoch().metadata.clone();
            if !draft.metadata.verify()
                || metadata_state.selected != draft.metadata.coord()
                || !metadata_entries.contains_key(&metadata_state.selected)
            {
                return Err(CircleOperationError::InvalidState(
                    "Circle transition inherited invalid selected metadata".to_string(),
                ));
            }
            (metadata_state, None)
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
        // A deletion carries no access material: its control inherits the
        // predecessor's access root and publishes no leaves.
        let (access_root, proofs) = if draft.access.is_empty() {
            (None, Vec::new())
        } else {
            let leaf_hashes = draft
                .access
                .iter()
                .map(|access| access.leaf.leaf_hash)
                .collect::<Vec<_>>();
            let (root, proofs) = crate::sync::circle::merkle_root_and_proofs(&leaf_hashes);
            (Some(root), proofs)
        };

        let mut control_frontier = draft
            .control
            .value
            .value
            .state
            .access_epoch()
            .covered_control_heads
            .clone();
        if let Some(previous) = previous_control {
            let head_hash = previous.head_hash();
            let head_object = previous.head_object();
            control_frontier
                .retain(|head| head.coord.stream_key() != previous.control().stream_key());
            control_frontier.push(crate::sync::circle::MergeCircleControlHeadRef {
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
                    read_exact_circle_object(storage, &control_context, &reference.object, &prefix)
                        .await?;
                let head: crate::sync::circle::CircleControlHead = serde_json::from_slice(&bytes)
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
                let stream_key = crate::sync::circle::CircleAuthorStreamKey {
                    author_pubkey: draft.control.value.author_pubkey.clone(),
                    device_id: author_registration.device_id.to_string(),
                    stream_id: control_stream,
                    author_owner_grant: owner_grant.clone(),
                };
                let prefix = circle_control_head_prefix(draft.circle_id, &stream_key, 1);
                let slot = storage
                    .allocate_protocol_slot(&control_context, &prefix, ".json")
                    .await
                    .map_err(crate::sync::store_objects::StoreObjectError::from)?;
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

        let crate::sync::circle::CircleControlValue {
            order,
            state,
            author_authority,
            membership_authority: _,
        } = &mut draft.control.value.value;
        let access_epoch = state.access_epoch_mut();
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
        access_epoch.roster = roster_state.clone();
        access_epoch.metadata = metadata_state;
        if let Some(access_root) = access_root {
            access_epoch.common.access_root = access_root;
        }
        access_epoch.covered_control_heads = control_frontier;
        if let (
            Some((true, _, entry, _, _)),
            crate::sync::circle::MergeCircleOwnerAuthorityRef::Roster {
                roster, created_at, ..
            },
        ) = (&prepared_roster, author_authority)
        {
            *roster = roster_state;
            *created_at = entry.coord();
        }
        if let Some(finalization) = draft.close_finalization.take() {
            let active_epoch = state.active_epoch_mut().ok_or_else(|| {
                CircleOperationError::InvalidState(
                    "Circle close finalization did not construct an active epoch".to_string(),
                )
            })?;
            let successor = crate::sync::circle::CircleEpochSuccessor {
                epoch_id: active_epoch.common.epoch_id,
                key_fingerprint: active_epoch.common.key_fingerprint,
                owners: active_epoch.common.owners.clone(),
                access_root: active_epoch.common.access_root,
                metadata: active_epoch.metadata.clone(),
                roster: active_epoch.roster.clone(),
                store_membership: active_epoch.store_membership.clone(),
            };
            let outcome = crate::sync::circle::CircleEpochCloseOutcome::signed(
                &finalization.close_control,
                &finalization.intent,
                finalization.responses,
                successor,
                identity_signer,
            )?;
            let outcome_hash = outcome.outcome_hash();
            let close_id = outcome.close_id;
            active_epoch.common.origin = crate::sync::circle::CircleEpochOrigin::Closed {
                closed_epoch_id: finalization.close_control.value.epoch_id(),
                close_control: finalization.close_control.coord.clone(),
                close_id,
                outcome_hash,
                cutoff: outcome.cutoff.clone(),
            };
            let outcome_prefix = crate::sync::circle::circle_epoch_close_outcome_semantic_prefix(
                draft.circle_id,
                close_id,
            );
            let outcome_prepared = prepare_circle_object_at(
                storage,
                &ProtocolObjectContext::store_encrypted(
                    store_root_hash,
                    ProtocolObjectDomain::CircleEpochCloseOutcome,
                ),
                finalization.outcome_slot,
                &outcome_prefix,
                crate::sync::circle::CircleEpochCloseSlotValue::Outcome(outcome.clone()).to_bytes(),
            )?;
            let outcome_ref = crate::sync::circle::CircleEpochCloseOutcomeRef::from_outcome(
                &outcome,
                outcome_prepared.reference().clone(),
            )?;
            prepared.insert("epoch-close-outcome".to_string(), outcome_prepared);
            close_outcome = Some((outcome, outcome_ref));
        }
        if let Some(cancellation_draft) = draft.close_cancellation.take() {
            let cancellation = crate::sync::circle::CircleEpochCloseCancellation::signed(
                &cancellation_draft.close_control,
                identity_signer,
            )?;
            let cancellation_prefix =
                crate::sync::circle::circle_epoch_close_outcome_semantic_prefix(
                    draft.circle_id,
                    cancellation.close_id,
                );
            let cancellation_prepared = prepare_circle_object_at(
                storage,
                &ProtocolObjectContext::store_encrypted(
                    store_root_hash,
                    ProtocolObjectDomain::CircleEpochCloseOutcome,
                ),
                cancellation_draft.outcome_slot,
                &cancellation_prefix,
                crate::sync::circle::CircleEpochCloseSlotValue::Cancellation(cancellation.clone())
                    .to_bytes(),
            )?;
            let cancellation_ref =
                crate::sync::circle::CircleEpochCloseCancellationRef::from_cancellation(
                    &cancellation,
                    cancellation_prepared.reference().clone(),
                )?;
            prepared.insert(
                "epoch-close-cancellation".to_string(),
                cancellation_prepared,
            );
            close_cancellation = Some((cancellation, cancellation_ref));
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
                    read_exact_circle_object(storage, &control_context, &reference.object, &prefix)
                        .await?;
                let head: crate::sync::circle::CircleControlHead = serde_json::from_slice(&bytes)
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
            .map_err(crate::sync::store_objects::StoreObjectError::from)?;
        let control_head = crate::sync::circle::CircleControlHead::signed(
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
            roster: prepared_roster.map(|(_, _, entry, head, _)| {
                crate::sync::circle::CircleRosterPolicyObjects { entry, head }
            }),
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
            bootstrap: match &access.leaf.value.disposition {
                CircleAccessDisposition::Active { bootstrap, .. } => {
                    bootstrap.as_ref().map(|bootstrap| bootstrap.image.clone())
                }
                CircleAccessDisposition::Inactive => None,
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
    let close_intent = match draft.control.value.state() {
        crate::sync::circle::CircleControlState::ActiveEpoch(_)
        | crate::sync::circle::CircleControlState::Deleted(_) => None,
        crate::sync::circle::CircleControlState::EpochClose(close) => Some(close.intent.clone()),
    };
    let transition = PreparedCircleTransition {
        circle_id: draft.circle_id,
        epoch_id: draft.epoch_id,
        keyring: draft.keyring,
        roster: draft.roster,
        policy_objects,
        metadata: draft.metadata,
        close_intent: draft.close_intent,
        close_outcome: close_outcome.as_ref().map(|(outcome, _)| outcome.clone()),
        close_cancellation: close_cancellation
            .as_ref()
            .map(|(cancellation, _)| cancellation.clone()),
        access: draft.access,
        control: draft.control,
    };
    Ok((
        transition,
        CircleActivationObjects {
            control,
            close_intent,
            close_outcome: close_outcome.map(|(_, reference)| reference),
            close_cancellation: close_cancellation.map(|(_, reference)| reference),
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
pub(super) async fn prepare_circle_operation(
    authority: &mut AuthorizedStoreAuthority<'_, '_>,
    device_id: &str,
    metadata_stamp: &str,
    name: &str,
    signer: &UserKeypair,
) -> Result<CircleOperationJournal, CircleOperationError> {
    Box::pin(prepare_circle_operation_request(
        authority,
        device_id,
        CircleOperationRequest::Create {
            name: name.to_string(),
            metadata_stamp: metadata_stamp.to_string(),
        },
        signer,
    ))
    .await
}

pub(super) async fn prepare_circle_operation_request(
    authority: &mut AuthorizedStoreAuthority<'_, '_>,
    device_id: &str,
    request: CircleOperationRequest,
    signer: &UserKeypair,
) -> Result<CircleOperationJournal, CircleOperationError> {
    let database = authority.database;
    let history_verifier = &mut *authority.history_verifier;
    let current = &*authority.membership;
    let storage = history_verifier.storage();
    let db = database.sqlite();
    let (root, author_registration, author, device_signer) =
        crate::sync::store::operations::load_local_store_authority(database, device_id, signer)
            .await?;
    if history_verifier.commit_verifier().root() != &root {
        return Err(CircleOperationError::InvalidState(
            "Circle preparation verifier belongs to another Store root".to_string(),
        ));
    }
    let store_root_hash = root.store_root_hash;
    let circle_device_id = author.device_id.to_string();
    let author_pubkey = keys::public_key_hex(signer);
    let (operation_id, write_id) = match request.settlement() {
        Some((operation_id, write_id)) => (operation_id, write_id),
        None => {
            let write_id = db.new_write_id();
            (CircleOperationId::from_write_id(write_id.clone()), write_id)
        }
    };
    let history = request.history();
    let intent = request.intent();
    let (creation, commit, commit_ref, policy, prepared_objects) = {
        let heads = current.head_refs().to_vec();
        let resolutions = current.resolution_refs().to_vec();
        let members = current.current_members();
        let state_hash = match current.status() {
            crate::sync::membership::MembershipStatus::Resolved(resolved) => resolved.state_hash,
            crate::sync::membership::MembershipStatus::Conflict(_) => {
                return Err(CircleOperationError::InvalidState(
                    "circle creation requires resolved Store membership".to_string(),
                ));
            }
        };
        let membership_authority =
            current
                .write_grant_authority(&author_pubkey)
                .ok_or_else(|| {
                    CircleOperationError::InvalidState(
                        "circle creator is not a current Store writer".to_string(),
                    )
                })?;
        let commit_base = database.local_commit_base().await?;
        // Held until this request's candidate is durably staged, so the position
        // its order extends is still this device's next one when it lands.
        let _authorship = commit_base.authorship;
        let base = commit_base.predecessor;
        let seq = base
            .as_ref()
            .map_or(1, |reference| reference.coord.sequence() + 1);
        let stream_id = crate::sync::store_commit::StreamActivation::device_authorized_stream_id(
            root.store_root_hash,
            &author_registration,
            crate::sync::store_commit::StreamAnchorDomain::StoreAnnouncements,
        );
        let dependencies =
            crate::sync::store_commit::CommitFrontier::from_refs(commit_base.frontier)
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
        let (device_state, resolved_devices) =
            database.store_device_state_for_order(&order).await?;
        let membership_state = StoreMembershipStateRef::from_parts(
            heads,
            resolutions,
            resolved_devices.recovery.clone(),
            state_hash,
        )
        .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        let candidate_family =
            CandidateFamilyId::derive(store_root_hash, &author_registration, &write_id, &order);
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
                    db.id_provider(),
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
                        db.id_provider(),
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
                            && record.role == crate::sync::circle::CircleRole::Owner
                    })
                    .map(|(grant, _)| grant)
                    .ok_or_else(|| {
                        CircleOperationError::InvalidState(
                            "Circle member-addition author is not an active Owner".to_string(),
                        )
                    })?;
                let roster_stream = StreamActivation::grant_authorized_stream_id(
                    store_root_hash,
                    &author_registration,
                    owner_grant,
                    StreamAnchorDomain::CircleRoster {
                        circle_id: request.circle_id,
                    },
                );
                let keyring_value = crate::encryption::MasterKeyring::from_serialized(keyring)
                    .map_err(|error| {
                        CircleOperationError::InvalidState(format!(
                            "parse Circle bootstrap keyring: {error}"
                        ))
                    })?;
                let circle_encryption = crate::encryption::EncryptionService::from(keyring_value);
                let recipient_slot = crate::sync::circle::recipient_slot(
                    signer,
                    &request.member_pubkey,
                    request.circle_id,
                )?;
                let bootstrap_blobs = verified_circle_bootstrap_blobs(
                    storage,
                    request.circle_id,
                    &request.bootstrap.snapshot,
                )
                .await?;
                let image_hash = ObjectHash::digest(&request.bootstrap.snapshot.db_image);
                let image_prefix =
                    crate::sync::store_commit::circle_bootstrap_image_semantic_prefix(
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
                let bootstrap_prepared = prepare_circle_object(
                    storage,
                    &image_context,
                    &image_prefix,
                    ".db",
                    request.bootstrap.snapshot.db_image.clone(),
                )
                .await?;
                let bootstrap = crate::sync::circle::CircleBootstrapRef {
                    coverage: request.bootstrap.coverage.clone(),
                    schema_version: db.schema_version(),
                    sync_routing_hash: db.sync_routing_hash(),
                    image: crate::sync::store_commit::SnapshotImageRef {
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
                        db.id_provider(),
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
                        .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?
                        != request.current.roster
                {
                    return Err(CircleOperationError::InvalidState(
                        "Circle member-removal request differs from its current state".to_string(),
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
                            && record.role == crate::sync::circle::CircleRole::Owner
                    })
                    .map(|(grant, _)| grant)
                    .ok_or_else(|| {
                        CircleOperationError::InvalidState(
                            "Circle member-removal author is not an active Owner".to_string(),
                        )
                    })?;
                let roster_stream = StreamActivation::grant_authorized_stream_id(
                    store_root_hash,
                    &author_registration,
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
                    .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
                let remaining_roster = request
                    .roster_chain
                    .resolved_with_successor(removal.clone())
                    .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
                let remaining_members = remaining_roster.members();
                let close_id =
                    crate::sync::circle::CircleEpochCloseId::from_operation_id(&operation_id);
                let intent = crate::sync::circle::CircleEpochCloseIntent::signed(
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
                let intent_prefix = crate::sync::circle::circle_epoch_close_intent_semantic_prefix(
                    request.circle_id,
                    close_id,
                    intent_hash,
                );
                let intent_encryption = EncryptionService::from(
                    MasterKeyring::from_serialized(keyring).map_err(|error| {
                        CircleOperationError::InvalidState(format!(
                            "parse Circle epoch-close keyring: {error}"
                        ))
                    })?,
                );
                let intent_context = ProtocolObjectContext::circle(
                    store_root_hash,
                    ProtocolObjectDomain::CircleEpochCloseIntent,
                    intent_encryption,
                );
                let intent_prepared = prepare_circle_object(
                    storage,
                    &intent_context,
                    &intent_prefix,
                    ".json",
                    serde_json::to_vec(&intent)
                        .expect("Circle epoch-close intent serialization cannot fail"),
                )
                .await?;
                let intent_ref = crate::sync::circle::CircleEpochCloseIntentRef::from_intent(
                    &intent,
                    intent_prepared.reference().clone(),
                )?;
                let outcome_prefix =
                    crate::sync::circle::circle_epoch_close_outcome_semantic_prefix(
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
                    .map_err(crate::sync::store_objects::StoreObjectError::from)?;
                let mut participants = Vec::new();
                for record in resolved_devices.devices.values() {
                    if !matches!(
                        record.status,
                        crate::sync::store_commit::StoreDeviceStatus::Active
                    ) {
                        continue;
                    }
                    let registration = database
                        .activated_store_device_registration(record.registration.clone())
                        .await?;
                    if !remaining_members.contains_key(&registration.author_pubkey) {
                        continue;
                    }
                    let response_prefix =
                        crate::sync::circle::circle_epoch_close_response_semantic_prefix(
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
                        .map_err(crate::sync::store_objects::StoreObjectError::from)?;
                    participants.push(crate::sync::circle::CircleEpochCloseParticipant {
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
                let provisional_frontier = order
                    .predecessor_cut()
                    .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?
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
                        device_state.clone(),
                        participants,
                        provisional_frontier,
                        outcome_slot,
                        db.id_provider(),
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
                    losing_branches.push(crate::sync::circle::ResolvedConflictBranch {
                        control_head: crate::sync::circle::MergeCircleControlHeadRef {
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
                        db.id_provider(),
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
                        db.id_provider(),
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
                            "Circle close finalization requires retained active access".to_string(),
                        ));
                    }
                };
                let mut draft = CircleTransitionDraft::finalize_epoch_close(
                    candidate_family,
                    &circle_device_id,
                    &author_registration,
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
                    db.id_provider(),
                    signer,
                )?;
                let bootstrap_blobs = verified_circle_bootstrap_blobs(
                    storage,
                    request.circle_id,
                    &request.bootstrap.snapshot,
                )
                .await?;
                let image_hash = ObjectHash::digest(&request.bootstrap.snapshot.db_image);
                let successor_encryption = EncryptionService::from(
                    MasterKeyring::from_serialized(&draft.keyring).map_err(|error| {
                        CircleOperationError::InvalidState(format!(
                            "parse Circle successor keyring: {error}"
                        ))
                    })?,
                );
                let mut bootstrap_objects = Vec::new();
                for (index, access) in draft.access.iter_mut().enumerate() {
                    if let CircleAccessDisposition::Active {
                        bootstrap: active_bootstrap,
                        ..
                    } = &mut access.leaf.value.disposition
                    {
                        let image_prefix =
                            crate::sync::store_commit::circle_bootstrap_image_semantic_prefix(
                                request.circle_id,
                                candidate_family,
                                &access.leaf.value.owner_pubkey,
                                draft.epoch_id,
                                &access.leaf.value.recipient_slot,
                                image_hash,
                            );
                        let bootstrap_prepared = prepare_circle_object(
                            storage,
                            &ProtocolObjectContext::circle(
                                store_root_hash,
                                ProtocolObjectDomain::CircleBootstrapImage,
                                successor_encryption.clone(),
                            ),
                            &image_prefix,
                            ".db",
                            request.bootstrap.snapshot.db_image.clone(),
                        )
                        .await?;
                        let bootstrap = crate::sync::circle::CircleBootstrapRef {
                            coverage: request.bootstrap.coverage.clone(),
                            schema_version: db.schema_version(),
                            sync_routing_hash: db.sync_routing_hash(),
                            image: crate::sync::store_commit::SnapshotImageRef {
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
                            "Circle close cancellation requires retained active access".to_string(),
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
                        db.id_provider(),
                        signer,
                    )?,
                    Vec::new(),
                )
            }
        };
        let (creation, objects, mut prepared_objects, control_head_object, stream_activations) =
            Box::pin(prepare_circle_activation_objects(
                storage,
                &root,
                creation,
                &history,
                &merged_branch_objects,
                candidate_family,
                &author_registration,
                &author,
                signer,
                &device_signer,
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
        let verified_commit = crate::sync::store_commit::VerifiedStoreBatchCommit::parse_prepared(
            &commit.to_bytes(),
            store_root_hash,
            coord,
            commit_prepared.reference().clone(),
            &author,
        )
        .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        let commit_ref = verified_commit.reference().clone();
        let history_summary = crate::sync::store::pull::prepare_merge_history_successor(
            database,
            &root,
            &verified_commit,
            current,
            None,
            resolved_devices,
            crate::sync::store::pull::MergeHistorySuccessorEvidence::none(),
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
            .map_err(crate::sync::store_objects::StoreObjectError::from)?;
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
            .map_err(crate::sync::store_objects::StoreObjectError::from)?;
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

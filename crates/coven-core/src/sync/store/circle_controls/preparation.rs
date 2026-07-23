use std::collections::{BTreeMap, BTreeSet};

use super::commands::CircleOperationRequest;
use super::{
    load_exact_slot_bytes, CircleOperationError, CircleOperationJournal, CircleOperationPolicy,
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
use crate::sync::store::database::StoreDatabase;
use crate::sync::store_commit::{
    circle_access_envelope_semantic_prefix, circle_access_leaf_semantic_prefix,
    commit_semantic_prefix, head_slot_prefix, CandidateFamilyId, CircleAccessEnvelopeObjectRef,
    CircleAccessLeafObjectRef, CircleAccessObjectRef, CircleActivationObjects,
    CircleMetadataObjectRef, GrantStreamAnchor, ObjectHash, StoreBatchCommit, StoreBatchCommitRef,
    StoreCommitCoord, StoreCommitOperationsInput, StoreCommitOrder, StoreDeviceHead,
    StoreDeviceRegistration, StoreDeviceRegistrationRef, StoreOperationMembershipAuthority,
    StoreRootRef, StreamActivation, StreamAnchorDomain, SuccessorLink,
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

pub(super) async fn prepare_circle_activation_objects(
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
    let previous_objects =
        previous_control.map(crate::sync::store_commit::CircleControlRef::objects);
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
            let next_slot = storage
                .allocate_protocol_slot(
                    &roster_context,
                    &circle_roster_head_prefix(draft.circle_id, &stream_key, 2),
                    ".json",
                )
                .await
                .map_err(crate::sync::store_objects::StoreObjectError::from)?;
            let head = crate::sync::circle::CircleRosterHead::signed(
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
                crate::sync::circle::ExactCircleRosterHead::bind(head.clone(), reference.clone())
                    .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
            draft.roster = crate::sync::circle::CircleRosterChain::from_entries_with_heads(
                vec![entry.clone()],
                vec![exact_head],
            )
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?
            .resolved();
        }

        let roster_state = crate::sync::circle::MergeCircleRosterStateRef {
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
                let head: crate::sync::circle::CircleMetadataHead = serde_json::from_slice(&bytes)
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
                let head: crate::sync::circle::CircleMetadataHead = serde_json::from_slice(&bytes)
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
        let metadata_state = crate::sync::circle::MergeCircleMetadataStateRef {
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
        let (access_root, proofs) = crate::sync::circle::merkle_root_and_proofs(&leaf_hashes);

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
                    load_exact_slot_bytes(storage, &control_context, &reference.object, &prefix)
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
            crate::sync::circle::MergeCircleOwnerAuthorityRef::Roster {
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
pub(super) async fn prepare_circle_operation(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    device_id: &str,
    metadata_stamp: &str,
    name: &str,
    signer: &UserKeypair,
) -> Result<CircleOperationJournal, CircleOperationError> {
    Box::pin(prepare_circle_operation_request(
        database,
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

pub(super) async fn prepare_circle_operation_request(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    device_id: &str,
    metadata_stamp: &str,
    request: CircleOperationRequest,
    signer: &UserKeypair,
) -> Result<CircleOperationJournal, CircleOperationError> {
    let db = database.sqlite();
    let (root, author_registration, author, device_signer) =
        crate::sync::store::operations::load_local_store_authority(database, device_id, signer)
            .await?;
    let store_root_hash = root.store_root_hash;
    let circle_device_id = author.device_id.to_string();
    let founder = db
        .get_protocol_state(crate::sync::store::membership::OWNER_PUBKEY_STATE_KEY)
        .await?
        .ok_or(CircleOperationError::MissingState("Store founder"))?;
    let author_pubkey = keys::public_key_hex(signer);
    let write_id = db.new_write_id();
    let operation_id = CircleOperationId::from_write_id(write_id.clone());
    let history = request.history();
    let intent = request.intent();
    let name = request.name();
    let (creation, commit, commit_ref, policy, prepared_objects) = {
        let current = crate::sync::store::membership::load_and_persist_owner_anchor(
            storage, &root, &founder, db,
        )
        .await
        .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        let heads = current.head_refs().to_vec();
        let resolutions = current.resolution_refs().to_vec();
        let exact = crate::sync::store::membership::load_anchored_chain_at_exact_heads(
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
            crate::sync::membership::MembershipStatus::Resolved(resolved) => resolved.state_hash,
            crate::sync::membership::MembershipStatus::Conflict(_) => {
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
        let base = database.latest_local_store_position().await?;
        let seq = base
            .as_ref()
            .map_or(1, |reference| reference.coord.sequence() + 1);
        let stream_id = crate::sync::store_commit::StreamActivation::device_authorized_stream_id(
            root.store_root_hash,
            &author_registration,
            crate::sync::store_commit::StreamAnchorDomain::StoreAnnouncements,
        );
        let dependencies = crate::sync::store_commit::CommitFrontier::from_refs(
            database.materialized_frontier().await?,
        )
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
        let history_summary = crate::sync::store::pull::prepare_merge_history_successor(
            database,
            &root,
            &commit,
            &commit_ref,
            &exact,
            &author,
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

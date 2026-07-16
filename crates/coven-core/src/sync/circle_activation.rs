//! Verification and materialization of Store-activated Circle state.

use std::collections::{BTreeMap, BTreeSet};

use super::circle::{
    circle_semantic_prefix, recipient_slot_with_peer, verify_circle_semantic_prefix,
    AccessEnvelope, CircleAccessDisposition, CircleAccessLeaf, CircleControl, CircleControlCoord,
    CircleControlOrder, CircleId, CircleMetadata, CircleMetadataHeadRef, CircleRole,
    CircleRosterHeadRef, CircleSemanticSlot, PreparedAccessLeaf, PreparedCircleControl,
    ResolvedCircleRoster, StoreMembershipStateRef,
};
use super::circle_ops::{CircleOperationError, CircleOperationJournal};
use super::circle_roster::CircleMaterializedRoster;
use super::storage::{ProtocolObjectContext, ProtocolObjectDomain, SyncStorage};
use super::store_commit::{
    CircleControlRef, CommitPosition, ObjectHash, StoreBatchCommit, StoreDeviceRegistrationState,
    StoreProtocolError,
};
use super::store_objects::{load_commit_slot, load_registration_ref};
use crate::encryption::{EncryptionService, MasterKeyring};
use crate::keys::{self, UserKeypair};

#[derive(Debug, Clone)]
pub(crate) struct VerifiedCircleReference {
    pub reference: CircleControlRef,
    pub circle_id: CircleId,
    pub control: PreparedCircleControl,
    pub local_access: Option<VerifiedCircleAccess>,
}

#[derive(Debug, Clone)]
pub(crate) struct VerifiedCircleAccess {
    pub leaf: PreparedAccessLeaf,
    pub active: Option<VerifiedCircleActive>,
}

#[derive(Debug, Clone)]
pub(crate) struct VerifiedCircleActive {
    pub roster: CircleMaterializedRoster,
    pub metadata: CircleMetadata,
}

fn verify_circle_owner_authority(
    author_pubkey: &str,
    authority: &super::circle::CircleOwnerAuthorityRef,
    roster: &CircleMaterializedRoster,
) -> bool {
    match (authority, roster) {
        (
            super::circle::CircleOwnerAuthorityRef::MergeConcurrent {
                grant_id,
                created_at,
                ..
            },
            CircleMaterializedRoster::MergeConcurrent(roster),
        ) => roster.authorizes_owner_grant(author_pubkey, grant_id, created_at),
        (
            super::circle::CircleOwnerAuthorityRef::Serial {
                roster_state_hash,
                grant_id,
                created_at_generation,
            },
            CircleMaterializedRoster::Serial(roster),
        ) => {
            roster.state_hash == *roster_state_hash
                && roster.authorizes_owner_grant(author_pubkey, grant_id, *created_at_generation)
        }
        _ => false,
    }
}

pub(crate) async fn load_circle_activations(
    storage: &dyn SyncStorage,
    commit: &StoreBatchCommit,
    identity: &UserKeypair,
    founder_pubkey: &str,
) -> Result<Vec<VerifiedCircleReference>, CircleOperationError> {
    let mut activations = Vec::with_capacity(commit.circle_controls.len());
    for reference in &commit.circle_controls {
        let control_prefix = circle_semantic_prefix(CircleSemanticSlot::Control {
            circle_id: reference.circle_id(),
            control: reference.control(),
        });
        let loaded = super::store_objects::load_semantic_copies(
            storage,
            &ProtocolObjectContext::store(
                commit.store_root_hash,
                ProtocolObjectDomain::CircleControl,
            ),
            &control_prefix,
            reference.control().control_hash(),
            |bytes| {
                if ObjectHash::digest(bytes) != reference.control().control_hash() {
                    return Err(StoreProtocolError::ObjectHashMismatch {
                        expected: reference.control().control_hash(),
                        actual: ObjectHash::digest(bytes),
                    });
                }
                let value: CircleControl = serde_json::from_slice(bytes)
                    .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
                let declared_coord = value.coord();
                if !value.verify()
                    || verify_circle_semantic_prefix(
                        &control_prefix,
                        CircleSemanticSlot::Control {
                            circle_id: value.circle_id,
                            control: &declared_coord,
                        },
                    )
                    .is_err()
                {
                    return Err(StoreProtocolError::InvalidSignature);
                }
                Ok(value)
            },
        )
        .await?
        .ok_or_else(|| {
            CircleOperationError::InvalidState(format!(
                "circle control {} is absent",
                reference.control().control_hash()
            ))
        })?;
        let control = PreparedCircleControl {
            coord: reference.control().clone(),
            bytes: loaded.bytes,
            value: loaded.value,
        };
        if let CircleControlRef::MergeConcurrent {
            circle_id,
            control: control_coord,
            head_hash,
        } = reference
        {
            let CircleControlCoord::MergeConcurrent { .. } = control_coord else {
                return Err(CircleOperationError::InvalidState(
                    "Merge Circle ref contains a Serial coordinate".to_string(),
                ));
            };
            let prefix = circle_semantic_prefix(CircleSemanticSlot::ControlHead {
                circle_id: *circle_id,
                control: control_coord,
                head_hash: *head_hash,
            });
            super::store_objects::load_semantic_copies(
                storage,
                &ProtocolObjectContext::store(
                    commit.store_root_hash,
                    ProtocolObjectDomain::CircleControl,
                ),
                &prefix,
                *head_hash,
                |bytes| {
                    let head: super::circle::CircleControlHead = serde_json::from_slice(bytes)
                        .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
                    if !head.verify()
                        || head.head_hash() != *head_hash
                        || verify_circle_semantic_prefix(
                            &prefix,
                            CircleSemanticSlot::ControlHead {
                                circle_id: head.circle_id,
                                control: &head.control,
                                head_hash: head.head_hash(),
                            },
                        )
                        .is_err()
                        || head.store_root_hash != commit.store_root_hash
                        || head.circle_id != *circle_id
                    {
                        return Err(StoreProtocolError::InvalidSignature);
                    }
                    Ok(head)
                },
            )
            .await?
            .ok_or_else(|| {
                CircleOperationError::InvalidState(format!(
                    "Circle control head {head_hash} is absent"
                ))
            })?;
        }
        verify_control_context(reference, &control, commit)?;
        verify_preceding_merge_registration(storage, commit).await?;
        let checkpoint_members =
            verify_control_membership(storage, &control, founder_pubkey).await?;
        let own_pubkey = keys::public_key_hex(identity);
        if !checkpoint_members
            .iter()
            .any(|(pubkey, _)| pubkey == &own_pubkey)
        {
            activations.push(VerifiedCircleReference {
                reference: reference.clone(),
                circle_id: reference.circle_id(),
                control,
                local_access: None,
            });
            continue;
        }
        let owner_pubkey = &control.value.author_pubkey;
        let owner = (
            owner_pubkey.clone(),
            recipient_slot_with_peer(identity, owner_pubkey, reference.circle_id()).map_err(
                |error| {
                    CircleOperationError::InvalidState(format!(
                        "derive circle Owner recipient slot: {error}"
                    ))
                },
            )?,
        );
        let envelope_prefix = circle_semantic_prefix(CircleSemanticSlot::AccessEnvelope {
            circle_id: reference.circle_id(),
            owner_pubkey: &owner.0,
            recipient_slot: &owner.1,
            control_hash: reference.control().control_hash(),
        });
        let envelope_bytes = load_exact_slot_bytes(
            storage,
            &ProtocolObjectContext::store(
                commit.store_root_hash,
                ProtocolObjectDomain::CircleAccessEnvelope,
            ),
            &envelope_prefix,
        )
        .await?;
        let envelope: AccessEnvelope =
            serde_json::from_slice(&envelope_bytes).map_err(|error| {
                CircleOperationError::InvalidState(format!("parse circle access envelope: {error}"))
            })?;
        if verify_circle_semantic_prefix(
            &envelope_prefix,
            CircleSemanticSlot::AccessEnvelope {
                circle_id: envelope.circle_id,
                owner_pubkey: &envelope.owner_pubkey,
                recipient_slot: &envelope.recipient_slot,
                control_hash: envelope.control_hash,
            },
        )
        .is_err()
            || envelope.owner_pubkey != owner.0
            || envelope.recipient_slot != owner.1
            || !envelope.verify(&control)
        {
            return Err(CircleOperationError::InvalidState(
                "circle access envelope failed verification".to_string(),
            ));
        }
        let leaf_prefix = circle_semantic_prefix(CircleSemanticSlot::AccessLeaf {
            circle_id: reference.circle_id(),
            owner_pubkey: &owner.0,
            epoch_id: control.value.epoch_id,
            recipient_slot: &owner.1,
            leaf_id: envelope.leaf_id,
        });
        let loaded_leaf = super::store_objects::load_semantic_copies(
            storage,
            &ProtocolObjectContext::recipient_sealed(commit.store_root_hash),
            &leaf_prefix,
            envelope.leaf_hash,
            |bytes| {
                if ObjectHash::digest(bytes) != envelope.leaf_hash {
                    return Err(StoreProtocolError::ObjectHashMismatch {
                        expected: envelope.leaf_hash,
                        actual: ObjectHash::digest(bytes),
                    });
                }
                Ok(bytes.to_vec())
            },
        )
        .await?
        .ok_or_else(|| {
            CircleOperationError::InvalidState(format!(
                "circle access leaf {} is absent",
                envelope.leaf_hash
            ))
        })?;
        let plaintext =
            keys::seal_box_decrypt(&loaded_leaf.bytes, &identity.to_x25519_secret_key()).map_err(
                |error| {
                    CircleOperationError::InvalidState(format!("open circle access leaf: {error}"))
                },
            )?;
        let leaf: CircleAccessLeaf = serde_json::from_slice(&plaintext).map_err(|error| {
            CircleOperationError::InvalidState(format!("parse circle access leaf: {error}"))
        })?;
        let prepared_leaf = PreparedAccessLeaf {
            bytes: loaded_leaf.bytes,
            value: leaf,
            leaf_hash: envelope.leaf_hash,
        };
        let leaf = &prepared_leaf.value;
        if verify_circle_semantic_prefix(
            &leaf_prefix,
            CircleSemanticSlot::AccessLeaf {
                circle_id: leaf.circle_id,
                owner_pubkey: &leaf.owner_pubkey,
                epoch_id: leaf.epoch_id,
                recipient_slot: &leaf.recipient_slot,
                leaf_id: leaf.leaf_id,
            },
        )
        .is_err()
            || leaf.owner_pubkey != owner.0
            || leaf.recipient_pubkey != own_pubkey
            || leaf.recipient_slot != owner.1
            || leaf.store_membership != control.value.store_membership
            || !prepared_leaf.verify_envelope(&control, &envelope)
        {
            return Err(CircleOperationError::InvalidState(
                "circle access leaf failed context verification".to_string(),
            ));
        }
        let active = match &leaf.disposition {
            CircleAccessDisposition::Active {
                keyring,
                key_fingerprint,
                roster,
            } => {
                if *key_fingerprint != control.value.key_fingerprint
                    || roster != &control.value.roster
                {
                    return Err(CircleOperationError::InvalidState(
                        "circle Active access names a different key or roster state".to_string(),
                    ));
                }
                let encryption = EncryptionService::from(
                    MasterKeyring::from_serialized(keyring).map_err(|error| {
                        CircleOperationError::InvalidState(format!(
                            "parse circle access keyring: {error}"
                        ))
                    })?,
                );
                if encryption.seal_key_fingerprint() != *key_fingerprint {
                    return Err(CircleOperationError::InvalidState(
                        "circle access keyring fingerprint differs from control".to_string(),
                    ));
                }
                let authority_roster = load_circle_authority_roster(
                    storage,
                    commit,
                    reference.circle_id(),
                    &control,
                    encryption.clone(),
                )
                .await?;
                if !verify_circle_owner_authority(
                    &control.value.author_pubkey,
                    &control.value.author_authority,
                    &authority_roster,
                ) {
                    return Err(CircleOperationError::InvalidState(
                        "circle control author lacks its exact historical Owner grant".to_string(),
                    ));
                }
                let resolved = match (&control.value.order, &control.value.roster) {
                    (
                        CircleControlOrder::Serial {
                            roster: embedded, ..
                        },
                        super::circle::CircleRosterStateRef::Serial { state_hash },
                    ) => {
                        if !embedded.verify() || embedded.state_hash != *state_hash {
                            return Err(CircleOperationError::InvalidState(
                                "embedded Serial Circle roster state is invalid".to_string(),
                            ));
                        }
                        CircleMaterializedRoster::Serial(embedded.clone())
                    }
                    (
                        CircleControlOrder::MergeConcurrent { .. },
                        state @ super::circle::CircleRosterStateRef::MergeConcurrent { .. },
                    ) => CircleMaterializedRoster::MergeConcurrent(
                        load_circle_roster_state(
                            storage,
                            commit.store_root_hash,
                            reference.circle_id(),
                            state,
                            encryption.clone(),
                        )
                        .await?,
                    ),
                    _ => {
                        return Err(CircleOperationError::InvalidState(
                            "Circle roster state does not match Store policy".to_string(),
                        ))
                    }
                };
                let resolved_members = resolved.members();
                let roster_owners = resolved_members
                    .iter()
                    .filter_map(|(pubkey, role)| {
                        (*role == super::circle::CircleRole::Owner).then_some(pubkey.clone())
                    })
                    .collect::<Vec<_>>();
                if roster_owners != control.value.owners {
                    return Err(CircleOperationError::InvalidState(
                        "circle control Owners differ from its roster".to_string(),
                    ));
                }
                let metadata = load_circle_metadata_state(
                    storage,
                    commit,
                    reference.circle_id(),
                    &control,
                    &control.value.metadata,
                    encryption,
                )
                .await?;
                Some(VerifiedCircleActive {
                    roster: resolved,
                    metadata,
                })
            }
            CircleAccessDisposition::Inactive => None,
        };
        activations.push(VerifiedCircleReference {
            reference: reference.clone(),
            circle_id: reference.circle_id(),
            control,
            local_access: Some(VerifiedCircleAccess {
                leaf: prepared_leaf,
                active,
            }),
        });
    }
    Ok(activations)
}

async fn load_circle_roster_state(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    circle_id: CircleId,
    state: &super::circle::CircleRosterStateRef,
    encryption: EncryptionService,
) -> Result<ResolvedCircleRoster, CircleOperationError> {
    let super::circle::CircleRosterStateRef::MergeConcurrent { heads, state_hash } = state else {
        return Err(CircleOperationError::InvalidState(
            "Merge roster loader received Serial state".to_string(),
        ));
    };
    if heads.is_empty()
        || !heads
            .windows(2)
            .all(|pair| pair[0].coord.stream_key() < pair[1].coord.stream_key())
    {
        return Err(CircleOperationError::InvalidState(
            "Circle roster heads are not one canonical head per stream".to_string(),
        ));
    }
    let context = ProtocolObjectContext::circle(
        store_root_hash,
        ProtocolObjectDomain::CircleRoster,
        encryption,
    );
    let mut pending = BTreeSet::new();
    for reference in heads {
        let prefix = circle_semantic_prefix(CircleSemanticSlot::RosterHead {
            circle_id,
            head: reference,
        });
        let loaded = super::store_objects::load_semantic_copies(
            storage,
            &context,
            &prefix,
            reference.head_hash,
            |bytes| {
                let head: super::circle::CircleRosterHead = serde_json::from_slice(bytes)
                    .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
                let declared_ref = CircleRosterHeadRef::from_head(&head);
                if !head.verify()
                    || head.head_hash() != reference.head_hash
                    || verify_circle_semantic_prefix(
                        &prefix,
                        CircleSemanticSlot::RosterHead {
                            circle_id: head.circle_id,
                            head: &declared_ref,
                        },
                    )
                    .is_err()
                    || head.store_root_hash != store_root_hash
                    || head.circle_id != circle_id
                {
                    return Err(StoreProtocolError::InvalidSignature);
                }
                Ok(head)
            },
        )
        .await?
        .ok_or_else(|| {
            CircleOperationError::InvalidState(format!(
                "Circle roster head {} is absent",
                reference.head_hash
            ))
        })?;
        pending.insert(loaded.value.entry_coord());
    }

    let mut entries = BTreeMap::new();
    while let Some(coord) = pending.pop_first() {
        if entries.contains_key(&coord) {
            continue;
        }
        let prefix = circle_semantic_prefix(CircleSemanticSlot::RosterEntry {
            circle_id,
            coord: &coord,
        });
        let loaded = super::store_objects::load_semantic_copies(
            storage,
            &context,
            &prefix,
            coord.entry_hash,
            |bytes| {
                let entry: super::circle::CircleRosterEntry = serde_json::from_slice(bytes)
                    .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
                let declared_coord = entry.coord();
                if !entry.verify()
                    || verify_circle_semantic_prefix(
                        &prefix,
                        CircleSemanticSlot::RosterEntry {
                            circle_id: entry.circle_id,
                            coord: &declared_coord,
                        },
                    )
                    .is_err()
                    || entry.store_root_hash != store_root_hash
                    || entry.circle_id != circle_id
                {
                    return Err(StoreProtocolError::InvalidSignature);
                }
                Ok(entry)
            },
        )
        .await?
        .ok_or_else(|| {
            CircleOperationError::InvalidState(format!(
                "Circle roster entry {} is absent",
                coord.entry_hash
            ))
        })?;
        pending.extend(loaded.value.dependencies.iter().cloned());
        entries.insert(coord, loaded.value);
    }
    let chain = super::circle::CircleRosterChain::from_entries(entries.into_values().collect())
        .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
    let expected_heads = heads
        .iter()
        .map(|reference| reference.coord.clone())
        .collect::<Vec<_>>();
    if chain.author_heads() != expected_heads {
        return Err(CircleOperationError::InvalidState(
            "Circle roster signed heads do not name its effective frontier".to_string(),
        ));
    }
    let resolved = chain.resolved();
    if resolved.state_hash != *state_hash {
        return Err(CircleOperationError::InvalidState(
            "Circle roster state hash differs from its effective assignments".to_string(),
        ));
    }
    Ok(resolved)
}

async fn load_circle_authority_roster(
    storage: &dyn SyncStorage,
    commit: &StoreBatchCommit,
    circle_id: CircleId,
    control: &PreparedCircleControl,
    encryption: EncryptionService,
) -> Result<CircleMaterializedRoster, CircleOperationError> {
    match &control.value.author_authority {
        super::circle::CircleOwnerAuthorityRef::MergeConcurrent { roster, .. } => {
            Ok(CircleMaterializedRoster::MergeConcurrent(
                load_circle_roster_state(
                    storage,
                    commit.store_root_hash,
                    circle_id,
                    roster,
                    encryption,
                )
                .await?,
            ))
        }
        super::circle::CircleOwnerAuthorityRef::Serial {
            roster_state_hash, ..
        } => {
            let CircleControlOrder::Serial {
                roster,
                previous_control_hash,
                ..
            } = &control.value.order
            else {
                return Err(CircleOperationError::InvalidState(
                    "Serial Circle authority accompanies Merge control order".to_string(),
                ));
            };
            if previous_control_hash.is_none() {
                if roster.state_hash != *roster_state_hash {
                    return Err(CircleOperationError::InvalidState(
                        "founder Circle authority does not name its founder roster".to_string(),
                    ));
                }
                return Ok(CircleMaterializedRoster::Serial(roster.clone()));
            }
            let target_control_hash = previous_control_hash.expect("checked as present");
            let mut predecessor = commit
                .previous_commit_hash()
                .map(|commit_hash| CommitPosition {
                    seq: commit.seq() - 1,
                    commit_hash,
                });
            while let Some(position) = predecessor {
                let preceding_commit = super::store_objects::load_serial_commit_at_position(
                    storage,
                    commit.store_root_hash,
                    &position,
                )
                .await?
                .ok_or_else(|| {
                    CircleOperationError::InvalidState(format!(
                        "Serial Circle authority Store commit {} is absent",
                        position.commit_hash
                    ))
                })?
                .value;
                if let Some(reference) = preceding_commit.circle_controls.iter().find(|reference| {
                    reference.circle_id() == circle_id
                        && reference.control().control_hash() == target_control_hash
                }) {
                    let preceding_control = load_circle_control_at_reference(
                        storage,
                        commit.store_root_hash,
                        reference,
                    )
                    .await?;
                    let CircleControlOrder::Serial { roster, .. } = preceding_control.order else {
                        return Err(CircleOperationError::InvalidState(
                            "Serial Circle predecessor contains Merge control order".to_string(),
                        ));
                    };
                    if roster.state_hash != *roster_state_hash {
                        return Err(CircleOperationError::InvalidState(
                            "Serial Circle authority roster hash differs from its predecessor"
                                .to_string(),
                        ));
                    }
                    return Ok(CircleMaterializedRoster::Serial(roster));
                }
                predecessor =
                    preceding_commit
                        .previous_commit_hash()
                        .map(|commit_hash| CommitPosition {
                            seq: preceding_commit.seq() - 1,
                            commit_hash,
                        });
            }
            Err(CircleOperationError::InvalidState(format!(
                "Serial Circle predecessor control {target_control_hash} is absent from the Store chain"
            )))
        }
    }
}

async fn load_circle_control_at_reference(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    reference: &CircleControlRef,
) -> Result<CircleControl, CircleOperationError> {
    let semantic_prefix = circle_semantic_prefix(CircleSemanticSlot::Control {
        circle_id: reference.circle_id(),
        control: reference.control(),
    });
    let expected_hash = reference.control().control_hash();
    super::store_objects::load_semantic_copies(
        storage,
        &ProtocolObjectContext::store(store_root_hash, ProtocolObjectDomain::CircleControl),
        &semantic_prefix,
        expected_hash,
        |bytes| {
            let control: CircleControl = serde_json::from_slice(bytes)
                .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
            let declared_coord = control.coord();
            if ObjectHash::digest(bytes) != expected_hash
                || !control.verify()
                || verify_circle_semantic_prefix(
                    &semantic_prefix,
                    CircleSemanticSlot::Control {
                        circle_id: control.circle_id,
                        control: &declared_coord,
                    },
                )
                .is_err()
                || control.store_root_hash != store_root_hash
                || control.circle_id != reference.circle_id()
            {
                return Err(StoreProtocolError::InvalidSignature);
            }
            Ok(control)
        },
    )
    .await?
    .map(|loaded| loaded.value)
    .ok_or_else(|| {
        CircleOperationError::InvalidState(format!("Circle control {expected_hash} is absent"))
    })
}

async fn load_metadata_author_roster(
    storage: &dyn SyncStorage,
    commit: &StoreBatchCommit,
    circle_id: CircleId,
    control: &PreparedCircleControl,
    roster_ref: &super::circle::CircleRosterStateRef,
    encryption: EncryptionService,
) -> Result<CircleMaterializedRoster, CircleOperationError> {
    match roster_ref {
        super::circle::CircleRosterStateRef::MergeConcurrent { .. } => {
            Ok(CircleMaterializedRoster::MergeConcurrent(
                load_circle_roster_state(
                    storage,
                    commit.store_root_hash,
                    circle_id,
                    roster_ref,
                    encryption,
                )
                .await?,
            ))
        }
        super::circle::CircleRosterStateRef::Serial { state_hash } => {
            load_serial_circle_roster_by_state_hash(
                storage,
                commit,
                circle_id,
                control,
                *state_hash,
            )
            .await
            .map(CircleMaterializedRoster::Serial)
        }
    }
}

async fn load_serial_circle_roster_by_state_hash(
    storage: &dyn SyncStorage,
    commit: &StoreBatchCommit,
    circle_id: CircleId,
    current_control: &PreparedCircleControl,
    state_hash: ObjectHash,
) -> Result<super::circle::SerialCircleRoster, CircleOperationError> {
    let CircleControlOrder::Serial { roster, .. } = &current_control.value.order else {
        return Err(CircleOperationError::InvalidState(
            "Serial Circle roster reference accompanies Merge control order".to_string(),
        ));
    };
    if roster.state_hash == state_hash {
        return Ok(roster.clone());
    }
    let mut predecessor = commit
        .previous_commit_hash()
        .map(|commit_hash| CommitPosition {
            seq: commit.seq() - 1,
            commit_hash,
        });
    while let Some(position) = predecessor {
        let preceding_commit = super::store_objects::load_serial_commit_at_position(
            storage,
            commit.store_root_hash,
            &position,
        )
        .await?
        .ok_or_else(|| {
            CircleOperationError::InvalidState(format!(
                "Serial Circle history Store commit {} is absent",
                position.commit_hash
            ))
        })?
        .value;
        for reference in preceding_commit
            .circle_controls
            .iter()
            .filter(|reference| reference.circle_id() == circle_id)
        {
            let preceding_control =
                load_circle_control_at_reference(storage, commit.store_root_hash, reference)
                    .await?;
            let CircleControlOrder::Serial { roster, .. } = preceding_control.order else {
                return Err(CircleOperationError::InvalidState(
                    "Serial Circle history contains Merge control order".to_string(),
                ));
            };
            if roster.state_hash == state_hash {
                return Ok(roster);
            }
        }
        predecessor = preceding_commit
            .previous_commit_hash()
            .map(|commit_hash| CommitPosition {
                seq: preceding_commit.seq() - 1,
                commit_hash,
            });
    }
    Err(CircleOperationError::InvalidState(format!(
        "Serial Circle roster state {state_hash} is absent from the Store chain"
    )))
}

async fn load_circle_metadata_state(
    storage: &dyn SyncStorage,
    commit: &StoreBatchCommit,
    circle_id: CircleId,
    control: &PreparedCircleControl,
    state: &super::circle::CircleMetadataStateRef,
    encryption: EncryptionService,
) -> Result<CircleMetadata, CircleOperationError> {
    let store_root_hash = commit.store_root_hash;
    let context = ProtocolObjectContext::circle(
        store_root_hash,
        ProtocolObjectDomain::CircleMetadata,
        encryption.clone(),
    );
    let (mut pending, selected, expected_heads, expected_state_hash) = match state {
        super::circle::CircleMetadataStateRef::MergeConcurrent {
            heads,
            selected,
            state_hash,
        } => {
            if heads.is_empty()
                || !heads
                    .windows(2)
                    .all(|pair| pair[0].coord.stream_key() < pair[1].coord.stream_key())
            {
                return Err(CircleOperationError::InvalidState(
                    "Circle metadata heads are not one canonical head per stream".to_string(),
                ));
            }
            let mut pending = BTreeSet::new();
            for reference in heads {
                let prefix = circle_semantic_prefix(CircleSemanticSlot::MetadataHead {
                    circle_id,
                    head: reference,
                });
                let loaded = super::store_objects::load_semantic_copies(
                    storage,
                    &context,
                    &prefix,
                    reference.head_hash,
                    |bytes| {
                        let head: super::circle::CircleMetadataHead = serde_json::from_slice(bytes)
                            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
                        let declared_ref = CircleMetadataHeadRef::from_head(&head);
                        if !head.verify()
                            || head.head_hash() != reference.head_hash
                            || verify_circle_semantic_prefix(
                                &prefix,
                                CircleSemanticSlot::MetadataHead {
                                    circle_id: head.circle_id,
                                    head: &declared_ref,
                                },
                            )
                            .is_err()
                            || head.store_root_hash != store_root_hash
                            || head.circle_id != circle_id
                        {
                            return Err(StoreProtocolError::InvalidSignature);
                        }
                        Ok(head)
                    },
                )
                .await?
                .ok_or_else(|| {
                    CircleOperationError::InvalidState(format!(
                        "Circle metadata head {} is absent",
                        reference.head_hash
                    ))
                })?;
                pending.insert(loaded.value.coord());
            }
            (
                pending,
                selected.clone(),
                Some(
                    heads
                        .iter()
                        .map(|reference| reference.coord.clone())
                        .collect::<Vec<_>>(),
                ),
                Some(*state_hash),
            )
        }
        super::circle::CircleMetadataStateRef::Serial { current } => (
            BTreeSet::from([current.clone()]),
            current.clone(),
            None,
            None,
        ),
    };

    let mut entries = BTreeMap::new();
    while let Some(coord) = pending.pop_first() {
        if entries.contains_key(&coord) {
            continue;
        }
        let prefix = circle_semantic_prefix(CircleSemanticSlot::MetadataEntry {
            circle_id,
            coord: &coord,
        });
        let loaded = super::store_objects::load_semantic_copies(
            storage,
            &context,
            &prefix,
            coord.metadata_hash,
            |bytes| {
                let entry: CircleMetadata = serde_json::from_slice(bytes)
                    .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
                let declared_coord = entry.coord();
                if !entry.verify()
                    || verify_circle_semantic_prefix(
                        &prefix,
                        CircleSemanticSlot::MetadataEntry {
                            circle_id: entry.circle_id,
                            coord: &declared_coord,
                        },
                    )
                    .is_err()
                    || entry.store_root_hash != store_root_hash
                    || entry.circle_id != circle_id
                {
                    return Err(StoreProtocolError::InvalidSignature);
                }
                Ok(entry)
            },
        )
        .await?
        .ok_or_else(|| {
            CircleOperationError::InvalidState(format!(
                "Circle metadata entry {} is absent",
                coord.metadata_hash
            ))
        })?;
        let entry = loaded.value;
        let exact_encryption = encryption
            .service_for_fingerprint(entry.key_fingerprint.as_bytes())
            .map_err(|error| {
                CircleOperationError::InvalidState(format!(
                    "Circle metadata names an unavailable key fingerprint: {error}"
                ))
            })?;
        let exact_context = ProtocolObjectContext::circle(
            store_root_hash,
            ProtocolObjectDomain::CircleMetadata,
            exact_encryption,
        );
        let sealed_under_declared_key = super::store_objects::load_semantic_copies(
            storage,
            &exact_context,
            &prefix,
            coord.metadata_hash,
            |bytes| {
                let exact: CircleMetadata = serde_json::from_slice(bytes)
                    .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
                if exact != entry {
                    return Err(StoreProtocolError::InvalidSignature);
                }
                Ok(exact)
            },
        )
        .await?
        .is_some();
        if !sealed_under_declared_key {
            return Err(CircleOperationError::InvalidState(
                "Circle metadata is not sealed under its declared key fingerprint".to_string(),
            ));
        }
        let author_roster = load_metadata_author_roster(
            storage,
            commit,
            circle_id,
            control,
            &entry.author_roster,
            encryption.clone(),
        )
        .await?;
        let author_is_owner =
            match &author_roster {
                CircleMaterializedRoster::MergeConcurrent(roster) => roster
                    .authorizes_owner_grant_id(&entry.author_pubkey, &entry.author_owner_grant),
                CircleMaterializedRoster::Serial(roster) => roster
                    .authorizes_owner_grant_id(&entry.author_pubkey, &entry.author_owner_grant),
            };
        if !author_is_owner {
            return Err(CircleOperationError::InvalidState(
                "Circle metadata author lacks its exact grant in the named historical roster"
                    .to_string(),
            ));
        }
        pending.extend(entry.dependencies.iter().cloned());
        entries.insert(coord, entry);
    }
    verify_metadata_history(&entries, expected_heads.as_deref())?;
    let selected_entry = entries.get(&selected).ok_or_else(|| {
        CircleOperationError::InvalidState(
            "selected Circle metadata coordinate is not in its covered history".to_string(),
        )
    })?;
    if expected_heads.is_some() {
        let canonical_selected = entries
            .values()
            .max_by_key(|entry| {
                (
                    entry.metadata_stamp.as_str(),
                    entry.author_pubkey.as_str(),
                    entry.device_id.as_str(),
                    entry.metadata_hash(),
                )
            })
            .expect("metadata history has a selected entry");
        if canonical_selected.coord() != selected
            || expected_state_hash != Some(selected_entry.metadata_hash())
        {
            return Err(CircleOperationError::InvalidState(
                "Circle metadata selection or state hash is not canonical".to_string(),
            ));
        }
    }
    Ok(selected_entry.clone())
}

fn verify_metadata_history(
    entries: &BTreeMap<super::circle::CircleMetadataCoord, CircleMetadata>,
    expected_heads: Option<&[super::circle::CircleMetadataCoord]>,
) -> Result<(), CircleOperationError> {
    let mut streams =
        BTreeMap::<super::circle::CircleAuthorStreamKey, BTreeMap<u64, &CircleMetadata>>::new();
    for (coord, entry) in entries {
        if entry
            .dependencies
            .iter()
            .any(|dependency| !entries.contains_key(dependency))
        {
            return Err(CircleOperationError::InvalidState(
                "Circle metadata depends on an absent coordinate".to_string(),
            ));
        }
        if streams
            .entry(coord.stream_key())
            .or_default()
            .insert(coord.seq, entry)
            .is_some()
        {
            return Err(CircleOperationError::InvalidState(
                "Circle metadata stream has a conflicting sequence".to_string(),
            ));
        }
    }
    let mut actual_heads = Vec::new();
    for (stream, positions) in streams {
        let max = *positions
            .keys()
            .next_back()
            .expect("metadata stream is non-empty");
        let mut previous = None;
        for seq in 1..=max {
            let entry = positions.get(&seq).ok_or_else(|| {
                CircleOperationError::InvalidState(
                    "Circle metadata stream has a missing sequence".to_string(),
                )
            })?;
            if entry.previous_hash != previous {
                return Err(CircleOperationError::InvalidState(
                    "Circle metadata stream predecessor is invalid".to_string(),
                ));
            }
            if seq > 1 {
                let predecessor = positions[&(seq - 1)].coord();
                if !entry.dependencies.contains(&predecessor) {
                    return Err(CircleOperationError::InvalidState(
                        "Circle metadata entry lacks its exact own predecessor".to_string(),
                    ));
                }
            }
            previous = Some(entry.metadata_hash());
        }
        actual_heads.push(positions[&max].coord());
        debug_assert_eq!(
            actual_heads.last().map(|coord| coord.stream_key()),
            Some(stream)
        );
    }
    actual_heads.sort_by_key(|coord| coord.stream_key());
    if expected_heads.is_some_and(|expected| expected != actual_heads) {
        return Err(CircleOperationError::InvalidState(
            "Circle metadata heads do not name its exact frontier".to_string(),
        ));
    }
    Ok(())
}

async fn verify_control_membership(
    storage: &dyn SyncStorage,
    control: &PreparedCircleControl,
    founder_pubkey: &str,
) -> Result<Vec<(String, super::membership::MemberRole)>, CircleOperationError> {
    let members = match &control.value.store_membership {
        StoreMembershipStateRef::MergeConcurrent { heads, .. } => {
            let chain = super::membership_ops::load_anchored_chain_at_exact_heads(
                storage,
                control.value.store_root_hash,
                founder_pubkey,
                heads,
            )
            .await
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
            let grant = control.value.membership_grant.as_ref().ok_or_else(|| {
                CircleOperationError::InvalidState(
                    "Merge circle control lacks Store membership grant".to_string(),
                )
            })?;
            if !chain.authorizes_write_at(grant, &control.value.author_pubkey) {
                return Err(CircleOperationError::InvalidState(
                    "Store membership does not authorize circle control author".to_string(),
                ));
            }
            chain.current_members()
        }
        StoreMembershipStateRef::Serial { position, .. } => {
            let authorization = super::store_pull::load_serial_authorization_at_position(
                storage,
                control.value.store_root_hash,
                position.clone(),
            )
            .await
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
            if !authorization
                .membership
                .can_write(&control.value.author_pubkey)
            {
                return Err(CircleOperationError::InvalidState(
                    "Serial Store membership does not authorize circle control author".to_string(),
                ));
            }
            authorization.membership.current_members()
        }
    };
    if super::circle::store_membership_state_hash(&members)
        != control.value.store_membership.state_hash()
    {
        return Err(CircleOperationError::InvalidState(
            "circle control Store membership state hash is invalid".to_string(),
        ));
    }
    Ok(members)
}

pub(crate) fn verify_control_context(
    reference: &CircleControlRef,
    control: &PreparedCircleControl,
    commit: &StoreBatchCommit,
) -> Result<(), CircleOperationError> {
    let policy_matches = control.value.store_membership.write_policy() == commit.policy();
    let device_matches = match &control.value.order {
        CircleControlOrder::MergeConcurrent { device_id, .. } => {
            commit.policy() == crate::WritePolicy::MergeConcurrent && device_id == &commit.device_id
        }
        CircleControlOrder::Serial { .. } => commit.policy() == crate::WritePolicy::Serial,
    };
    if !control.verify()
        || reference.circle_id() != control.value.circle_id
        || reference.control() != &control.coord
        || control.value.store_root_hash != commit.store_root_hash
        || control.value.author_pubkey != commit.author_pubkey
        || !policy_matches
        || !device_matches
    {
        return Err(CircleOperationError::InvalidState(
            "circle control context differs from its Store reference and commit".to_string(),
        ));
    }
    Ok(())
}

pub(crate) async fn verify_preceding_merge_registration(
    storage: &dyn SyncStorage,
    circle_commit: &StoreBatchCommit,
) -> Result<(), CircleOperationError> {
    if circle_commit.policy() == crate::WritePolicy::Serial {
        return Ok(());
    }
    let mut expected = circle_commit
        .previous_commit_hash()
        .map(|commit_hash| CommitPosition {
            seq: circle_commit.seq() - 1,
            commit_hash,
        });
    while let Some(position) = expected {
        let predecessor = load_commit_slot(
            storage,
            circle_commit.store_root_hash,
            &circle_commit.device_id,
            position.seq,
        )
        .await?
        .ok_or_else(|| {
            CircleOperationError::InvalidState(format!(
                "Circle publisher registration predecessor {} is absent",
                position.seq
            ))
        })?
        .value;
        if predecessor.commit_hash() != position.commit_hash {
            return Err(CircleOperationError::InvalidState(format!(
                "Circle publisher predecessor {} has a different hash",
                position.seq
            )));
        }
        if let Some(reference) = predecessor
            .device_registrations
            .iter()
            .find(|reference| reference.device_id == circle_commit.device_id)
        {
            let registration =
                load_registration_ref(storage, circle_commit.store_root_hash, reference)
                    .await?
                    .ok_or_else(|| {
                        CircleOperationError::InvalidState(format!(
                            "Circle publisher registration {:?}/{} is absent",
                            reference.device_id, reference.revision
                        ))
                    })?
                    .value;
            if registration.author_pubkey != circle_commit.author_pubkey
                || registration.state != StoreDeviceRegistrationState::Active
            {
                return Err(CircleOperationError::InvalidState(
                    "Circle publisher has no preceding Active registration bound to its author"
                        .to_string(),
                ));
            }
            return Ok(());
        }
        expected = predecessor
            .previous_commit_hash()
            .map(|commit_hash| CommitPosition {
                seq: predecessor.seq() - 1,
                commit_hash,
            });
    }
    Err(CircleOperationError::InvalidState(
        "Circle publisher has no preceding Active Store device registration".to_string(),
    ))
}

pub(crate) fn verify_local_circle_activation(
    journal: &CircleOperationJournal,
    commit: &StoreBatchCommit,
) -> Result<VerifiedCircleReference, CircleOperationError> {
    let creation = &journal.creation;
    let control = &creation.control;
    verify_control_context(&creation.control_ref(), control, commit)?;
    let own_access = creation
        .access
        .iter()
        .find(|access| access.leaf.value.recipient_pubkey == commit.author_pubkey)
        .ok_or_else(|| {
            CircleOperationError::InvalidState(
                "circle creator has no access disposition".to_string(),
            )
        })?;
    let leaf = &own_access.leaf.value;
    let envelope = &own_access.envelope;
    if leaf.recipient_pubkey != commit.author_pubkey
        || leaf.owner_pubkey != control.value.author_pubkey
        || leaf.store_membership != control.value.store_membership
        || envelope.owner_pubkey != control.value.author_pubkey
        || !own_access.leaf.verify_envelope(control, envelope)
    {
        return Err(CircleOperationError::InvalidState(
            "local circle access failed leaf and envelope verification".to_string(),
        ));
    }
    let CircleAccessDisposition::Active {
        key_fingerprint,
        roster,
        ..
    } = &leaf.disposition
    else {
        return Err(CircleOperationError::InvalidState(
            "circle creator access is inactive".to_string(),
        ));
    };
    let resolved_roster = creation.resolved_roster();
    let resolved_members = resolved_roster.members();
    let roster_owners = resolved_members
        .iter()
        .filter_map(|(pubkey, role)| (*role == CircleRole::Owner).then_some(pubkey.clone()))
        .collect::<Vec<_>>();
    if *key_fingerprint != control.value.key_fingerprint
        || roster != &control.value.roster
        || !resolved_roster.verify()
        || !verify_circle_owner_authority(
            &commit.author_pubkey,
            &control.value.author_authority,
            &resolved_roster,
        )
        || roster_owners != control.value.owners
        || creation.metadata.store_root_hash != commit.store_root_hash
        || creation.metadata.circle_id != creation.circle_id
        || creation.metadata.epoch_id != control.value.epoch_id
        || creation.metadata.author_pubkey != commit.author_pubkey
        || creation.metadata.author_owner_grant != *control.value.author_authority.grant_id()
        || creation.metadata.author_roster != control.value.roster
        || creation.metadata.key_fingerprint != control.value.key_fingerprint
        || match &control.value.metadata {
            super::circle::CircleMetadataStateRef::MergeConcurrent {
                selected,
                state_hash,
                ..
            } => {
                selected != &creation.metadata.coord()
                    || *state_hash != creation.metadata.metadata_hash()
            }
            super::circle::CircleMetadataStateRef::Serial { current } => {
                current != &creation.metadata.coord()
            }
        }
        || !creation.metadata.verify()
    {
        return Err(CircleOperationError::InvalidState(
            "local circle roster or metadata failed context verification".to_string(),
        ));
    }
    Ok(VerifiedCircleReference {
        reference: creation.control_ref(),
        circle_id: creation.circle_id,
        control: control.clone(),
        local_access: Some(VerifiedCircleAccess {
            leaf: own_access.leaf.clone(),
            active: Some(VerifiedCircleActive {
                roster: resolved_roster,
                metadata: creation.metadata.clone(),
            }),
        }),
    })
}

pub(crate) async fn load_exact_slot_bytes(
    storage: &dyn SyncStorage,
    context: &ProtocolObjectContext,
    semantic_prefix: &str,
) -> Result<Vec<u8>, CircleOperationError> {
    let listing = storage
        .list_protocol_objects(&format!("{semantic_prefix}/copies/"))
        .await
        .map_err(super::store_objects::StoreObjectError::from)?;
    let mut canonical = None;
    for object in listing.objects {
        let bytes = storage
            .read_protocol_object(context, &object, semantic_prefix)
            .await
            .map_err(super::store_objects::StoreObjectError::from)?;
        if canonical.as_ref().is_some_and(|value| value != &bytes) {
            return Err(CircleOperationError::InvalidState(format!(
                "circle semantic slot {semantic_prefix:?} contains a fork"
            )));
        }
        canonical = Some(bytes);
    }
    canonical.ok_or_else(|| {
        CircleOperationError::InvalidState(format!(
            "circle semantic slot {semantic_prefix:?} is absent"
        ))
    })
}

#[cfg(test)]
mod authority_tests {
    use super::super::causal_grants::AuthorStreamId;
    use super::super::circle::{CircleOwnerAuthorityRef, CircleRosterChain, CircleRosterEntry};
    use super::super::membership::MembershipGrantId;
    use super::*;

    #[test]
    fn control_authority_uses_the_pre_transition_roster_for_self_demotion() {
        let author = UserKeypair::generate();
        let second_owner = UserKeypair::generate();
        let author_pubkey = keys::public_key_hex(&author);
        let author_grant = MembershipGrantId(ObjectHash::digest(b"self-demotion grant"));
        let store_root_hash = ObjectHash::digest(b"self-demotion Store");
        let circle_id = CircleId::founder(store_root_hash, &author_pubkey, &author_grant);
        let stream_id = AuthorStreamId::from_bytes([21; 16]);
        let founder = CircleRosterEntry::founder(
            store_root_hash,
            circle_id,
            "author-device",
            stream_id,
            author_grant.clone(),
            &author,
        );
        let author_created_at = founder.coord();
        let mut entries = vec![founder];
        let add_second_owner = CircleRosterChain::from_entries(entries.clone())
            .expect("load founder roster")
            .signed_set_member(
                "author-device",
                stream_id,
                keys::public_key_hex(&second_owner),
                CircleRole::Owner,
                &author,
            )
            .expect("add second Owner");
        entries.push(add_second_owner);
        let before = CircleRosterChain::from_entries(entries.clone())
            .expect("load pre-demotion roster")
            .resolved();
        let demotion = CircleRosterChain::from_entries(entries.clone())
            .expect("load pre-demotion roster")
            .signed_set_member(
                "author-device",
                stream_id,
                author_pubkey.clone(),
                CircleRole::Member,
                &author,
            )
            .expect("self-demote while another Owner remains");
        entries.push(demotion);
        let after = CircleRosterChain::from_entries(entries)
            .expect("load post-demotion roster")
            .resolved();
        let authority = CircleOwnerAuthorityRef::MergeConcurrent {
            roster: super::super::circle::CircleRosterStateRef::MergeConcurrent {
                heads: Vec::new(),
                state_hash: before.state_hash,
            },
            grant_id: author_grant,
            created_at: author_created_at,
        };

        assert!(verify_circle_owner_authority(
            &author_pubkey,
            &authority,
            &CircleMaterializedRoster::MergeConcurrent(before),
        ));
        assert!(!verify_circle_owner_authority(
            &author_pubkey,
            &authority,
            &CircleMaterializedRoster::MergeConcurrent(after),
        ));
    }
}

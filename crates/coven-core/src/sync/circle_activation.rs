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
        (
            super::circle::CircleOwnerAuthorityRef::ConflictResolution {
                conflict_hash,
                resolution_hash,
            },
            CircleMaterializedRoster::MergeConcurrent(roster),
        ) => {
            let grant_id =
                super::circle_roster::derive_circle_resolution_grant(conflict_hash, author_pubkey);
            roster.authorizes_resolution_grant(
                author_pubkey,
                &grant_id,
                &super::circle_roster::CircleRosterConflictResolutionRef {
                    conflict_hash: *conflict_hash,
                    resolver_pubkey: author_pubkey.to_string(),
                    resolution_hash: *resolution_hash,
                },
            )
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
    let super::circle::CircleRosterStateRef::MergeConcurrent {
        heads,
        resolutions,
        state_hash,
    } = state
    else {
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
        encryption.clone(),
    );
    if !resolutions.windows(2).all(|pair| pair[0] < pair[1]) {
        return Err(CircleOperationError::InvalidState(
            "Circle roster resolutions are not canonical".to_string(),
        ));
    }
    let loaded_heads =
        load_exact_circle_roster_heads(storage, store_root_hash, circle_id, &context, heads)
            .await?;
    let activated_resolutions = loaded_heads
        .iter()
        .flat_map(|head| head.resolutions.iter().cloned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if activated_resolutions != *resolutions {
        return Err(CircleOperationError::InvalidState(
            "Circle roster state resolution refs differ from its signed heads".to_string(),
        ));
    }
    let entries = load_circle_roster_entries_from_heads(
        storage,
        store_root_hash,
        circle_id,
        &context,
        &loaded_heads,
    )
    .await?;
    let loaded_resolutions = load_circle_roster_resolutions(
        storage,
        store_root_hash,
        circle_id,
        resolutions,
        &encryption,
    )
    .await?;
    let chain = if loaded_resolutions.is_empty() {
        super::circle::CircleRosterChain::from_entries_with_heads(
            entries.clone(),
            loaded_heads.clone(),
        )
        .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?
    } else {
        replay_circle_roster_resolutions(
            storage,
            store_root_hash,
            circle_id,
            &context,
            &entries,
            &loaded_heads,
            &loaded_resolutions,
        )
        .await?
    };
    let expected_heads = heads
        .iter()
        .map(|reference| reference.coord.clone())
        .collect::<Vec<_>>();
    if chain.author_heads() != expected_heads {
        return Err(CircleOperationError::InvalidState(
            "Circle roster signed heads do not name its raw frontier".to_string(),
        ));
    }
    let resolved = chain
        .try_resolved()
        .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
    if resolved.state_hash != *state_hash {
        return Err(CircleOperationError::InvalidState(
            "Circle roster state hash differs from its effective assignments".to_string(),
        ));
    }
    Ok(resolved)
}

async fn load_circle_roster_entries_from_heads(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    circle_id: CircleId,
    context: &ProtocolObjectContext,
    heads: &[super::circle::CircleRosterHead],
) -> Result<Vec<super::circle::CircleRosterEntry>, CircleOperationError> {
    let mut pending = heads
        .iter()
        .map(super::circle::CircleRosterHead::entry_coord)
        .collect::<BTreeSet<_>>();
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
            context,
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
    Ok(entries.into_values().collect())
}

async fn load_circle_roster_resolutions(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    circle_id: CircleId,
    resolutions: &[super::circle::CircleRosterConflictResolutionRef],
    encryption: &EncryptionService,
) -> Result<Vec<super::circle::CircleRosterConflictResolution>, CircleOperationError> {
    let mut loaded_resolutions = Vec::with_capacity(resolutions.len());
    for reference in resolutions {
        let loaded = super::store_objects::load_circle_roster_resolution_object(
            storage,
            store_root_hash,
            circle_id,
            encryption.clone(),
            reference,
        )
        .await?
        .ok_or_else(|| {
            CircleOperationError::InvalidState(format!(
                "Circle roster resolution {} is absent",
                reference.resolution_hash
            ))
        })?;
        loaded_resolutions.push(loaded.value);
    }
    Ok(loaded_resolutions)
}

async fn load_exact_circle_roster_heads(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    circle_id: CircleId,
    context: &ProtocolObjectContext,
    references: &[CircleRosterHeadRef],
) -> Result<Vec<super::circle::CircleRosterHead>, CircleOperationError> {
    let mut loaded_heads = Vec::with_capacity(references.len());
    for reference in references {
        let prefix = circle_semantic_prefix(CircleSemanticSlot::RosterHead {
            circle_id,
            head: reference,
        });
        let loaded = super::store_objects::load_semantic_copies(
            storage,
            context,
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
        loaded_heads.push(loaded.value);
    }
    Ok(loaded_heads)
}

async fn replay_circle_roster_resolutions(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    circle_id: CircleId,
    context: &ProtocolObjectContext,
    entries: &[super::circle::CircleRosterEntry],
    current_heads: &[super::circle::CircleRosterHead],
    resolutions: &[super::circle::CircleRosterConflictResolution],
) -> Result<super::circle::CircleRosterChain, CircleOperationError> {
    let known_resolution_refs = resolutions
        .iter()
        .map(|resolution| resolution.resolution_ref())
        .collect::<BTreeSet<_>>();
    let activated_resolution_refs = current_heads
        .iter()
        .flat_map(|head| head.resolutions.iter().cloned())
        .collect::<BTreeSet<_>>();
    if let Some(reference) = activated_resolution_refs
        .difference(&known_resolution_refs)
        .next()
    {
        return Err(CircleOperationError::InvalidState(format!(
            "Circle head references absent resolution {}",
            reference.resolution_hash
        )));
    }
    if known_resolution_refs != activated_resolution_refs {
        return Err(CircleOperationError::InvalidState(
            "Circle resolution objects differ from the exact signed head cut".to_string(),
        ));
    }
    let mut prepared = BTreeMap::new();
    for resolution in resolutions {
        let reference = resolution.resolution_ref();
        let conflict_heads = &resolution.conflicting_heads;
        if conflict_heads.is_empty()
            || !conflict_heads
                .windows(2)
                .all(|pair| pair[0].coord.stream_key() < pair[1].coord.stream_key())
        {
            return Err(CircleOperationError::InvalidState(
                "Circle roster resolution conflict heads are not canonical".to_string(),
            ));
        }
        let heads = load_exact_circle_roster_heads(
            storage,
            store_root_hash,
            circle_id,
            context,
            conflict_heads,
        )
        .await?;
        let conflict_entries = load_circle_roster_entries_from_heads(
            storage,
            store_root_hash,
            circle_id,
            context,
            &heads,
        )
        .await?;
        let dependencies = heads
            .iter()
            .flat_map(|head| head.resolutions.iter())
            .map(|reference| {
                known_resolution_refs
                    .contains(reference)
                    .then_some(reference.clone())
                    .ok_or_else(|| {
                        CircleOperationError::InvalidState(format!(
                            "Circle conflict head references absent resolution {}",
                            reference.resolution_hash
                        ))
                    })
            })
            .collect::<Result<BTreeSet<_>, _>>()?;
        if dependencies.contains(&reference) {
            return Err(CircleOperationError::InvalidState(
                "Circle roster resolution depends on itself".to_string(),
            ));
        }
        prepared.insert(
            reference,
            (resolution.clone(), heads, conflict_entries, dependencies),
        );
    }
    let mut resolved_by_ref = BTreeMap::<
        super::circle::CircleRosterConflictResolutionRef,
        super::circle::CircleRosterChain,
    >::new();
    let mut applied = BTreeSet::new();
    while !prepared.is_empty() {
        let next = super::causal_grants::canonical_ready_checkpoint(
            prepared
                .iter()
                .map(|(reference, (_, _, _, dependencies))| (reference, dependencies)),
            &applied,
        )
        .ok_or_else(|| {
            CircleOperationError::InvalidState(
                "Circle roster resolution checkpoints contain a causal cycle".to_string(),
            )
        })?;
        let (resolution, heads, conflict_entries, dependencies) =
            prepared.remove(&next).ok_or_else(|| {
                CircleOperationError::InvalidState(
                    "ready Circle roster resolution checkpoint is absent".to_string(),
                )
            })?;
        let dependency_chains = dependencies
            .iter()
            .map(|dependency| {
                resolved_by_ref.get(dependency).ok_or_else(|| {
                    CircleOperationError::InvalidState(
                        "ready Circle resolution dependency is absent".to_string(),
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut prefix = BTreeMap::new();
        for chain in &dependency_chains {
            prefix.extend(
                chain
                    .entries()
                    .iter()
                    .cloned()
                    .map(|entry| (entry.coord(), entry)),
            );
        }
        prefix.extend(
            conflict_entries
                .into_iter()
                .map(|entry| (entry.coord(), entry)),
        );
        let mut conflict_chain = if dependency_chains.is_empty() {
            super::circle::CircleRosterChain::from_entries_with_heads(
                prefix.into_values().collect(),
                heads,
            )
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?
        } else {
            super::circle::CircleRosterChain::replay_merged_resolved_histories_to_heads(
                &dependency_chains,
                prefix.into_values().collect(),
                heads,
            )
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?
        };
        conflict_chain
            .apply_resolutions(std::slice::from_ref(&resolution))
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        resolved_by_ref.insert(next.clone(), conflict_chain);
        applied.insert(next);
    }

    let current_by_coord = entries
        .iter()
        .cloned()
        .map(|entry| (entry.coord(), entry))
        .collect::<BTreeMap<_, _>>();
    let mut heads_by_cut = BTreeMap::<Vec<_>, Vec<_>>::new();
    for head in current_heads {
        heads_by_cut
            .entry(head.resolutions.clone())
            .or_default()
            .push(head.clone());
    }
    let mut branch_chains = Vec::new();
    for (cut, heads) in heads_by_cut {
        let cut_set = cut.iter().cloned().collect::<BTreeSet<_>>();
        let branch_heads = heads
            .into_iter()
            .filter(|head| {
                let coord = head.entry_coord();
                !resolved_by_ref.values().any(|checkpoint| {
                    let checkpoint_cut = checkpoint
                        .resolution_refs()
                        .iter()
                        .cloned()
                        .collect::<BTreeSet<_>>();
                    cut_set.is_subset(&checkpoint_cut)
                        && cut_set != checkpoint_cut
                        && checkpoint.resolution_checkpoint_covers(&coord)
                })
            })
            .collect::<Vec<_>>();
        if branch_heads.is_empty() {
            continue;
        }
        let dependencies = cut
            .iter()
            .map(|reference| {
                resolved_by_ref.get(reference).ok_or_else(|| {
                    CircleOperationError::InvalidState(format!(
                        "Circle head references absent resolution {}",
                        reference.resolution_hash
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut branch_history = BTreeMap::new();
        for chain in &dependencies {
            branch_history.extend(
                chain
                    .entries()
                    .iter()
                    .cloned()
                    .map(|entry| (entry.coord(), entry)),
            );
        }
        let mut pending = branch_heads
            .iter()
            .map(super::circle::CircleRosterHead::entry_coord)
            .collect::<BTreeSet<_>>();
        while let Some(coord) = pending.pop_first() {
            if branch_history.contains_key(&coord) {
                continue;
            }
            let entry = current_by_coord.get(&coord).ok_or_else(|| {
                CircleOperationError::InvalidState(format!(
                    "Circle suffix entry {} is absent",
                    coord.entry_hash
                ))
            })?;
            pending.extend(entry.dependencies.iter().cloned());
            branch_history.insert(coord, entry.clone());
        }
        let mut branch = if dependencies.is_empty() {
            super::circle::CircleRosterChain::from_entries_with_heads(
                branch_history.into_values().collect(),
                branch_heads,
            )
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?
        } else {
            super::circle::CircleRosterChain::replay_merged_resolved_histories_to_heads(
                &dependencies,
                branch_history.into_values().collect(),
                branch_heads,
            )
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?
        };
        branch
            .checkpoint_current_resolved_state()
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        branch_chains.push(branch);
    }
    let branch_refs = resolved_by_ref
        .values()
        .chain(branch_chains.iter())
        .collect::<Vec<_>>();
    let mut history = current_by_coord;
    for chain in &branch_refs {
        history.extend(
            chain
                .entries()
                .iter()
                .cloned()
                .map(|entry| (entry.coord(), entry)),
        );
    }
    super::circle::CircleRosterChain::replay_merged_resolved_histories_to_heads(
        &branch_refs,
        history.into_values().collect(),
        current_heads.to_vec(),
    )
    .map_err(|error| CircleOperationError::InvalidState(error.to_string()))
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
        super::circle::CircleOwnerAuthorityRef::ConflictResolution { .. } => {
            let roster = &control.value.roster;
            let super::circle::CircleRosterStateRef::MergeConcurrent { .. } = roster else {
                return Err(CircleOperationError::InvalidState(
                    "Circle conflict-resolution authority names a Serial roster".to_string(),
                ));
            };
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
    let (members, merge_state_hash) = match &control.value.store_membership {
        StoreMembershipStateRef::MergeConcurrent {
            heads, resolutions, ..
        } => {
            let chain = super::membership_ops::load_anchored_chain_at_exact_heads(
                storage,
                control.value.store_root_hash,
                founder_pubkey,
                heads,
                resolutions,
            )
            .await
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
            let authority = control.value.membership_authority.as_ref().ok_or_else(|| {
                CircleOperationError::InvalidState(
                    "Merge circle control lacks Store membership authority".to_string(),
                )
            })?;
            if !chain.authorizes_write_authority(authority, &control.value.author_pubkey) {
                return Err(CircleOperationError::InvalidState(
                    "Store membership does not authorize circle control author".to_string(),
                ));
            }
            let state_hash = match chain.status() {
                super::membership::MembershipStatus::Resolved(resolved) => resolved.state_hash,
                super::membership::MembershipStatus::Conflict(_) => {
                    return Err(CircleOperationError::InvalidState(
                        "Store membership state has an unresolved conflict".to_string(),
                    ));
                }
            };
            (chain.current_members(), Some(state_hash))
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
            let state_hash = authorization.membership.state_hash();
            (authorization.membership.current_members(), Some(state_hash))
        }
    };
    let expected_state_hash = control.value.store_membership.state_hash();
    let actual_state_hash = merge_state_hash.expect("both policies produce a membership hash");
    if actual_state_hash != expected_state_hash {
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
        || creation.metadata.author_owner_grant
            != control
                .value
                .author_authority
                .grant_id(&control.value.author_pubkey)
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
    use super::super::circle::{
        CircleOwnerAuthorityRef, CircleRole, CircleRosterChain, CircleRosterConflict,
        CircleRosterEntry, CircleRosterHead, CircleRosterHeadRef, CircleRosterStatus,
    };
    use super::super::membership::MembershipGrantId;
    use super::super::test_helpers::{user_keypair_from_seed, MockSyncStorage};
    use super::*;

    #[tokio::test]
    async fn exact_circle_loader_materializes_resolution_activation_without_assignment_suffix() {
        let first_owner = UserKeypair::generate();
        let second_owner = UserKeypair::generate();
        let first_pubkey = keys::public_key_hex(&first_owner);
        let second_pubkey = keys::public_key_hex(&second_owner);
        let store_root_hash = ObjectHash::digest(b"Circle replay Store root");
        let founder_grant = MembershipGrantId(ObjectHash::digest(b"Circle replay founder grant"));
        let circle_id = CircleId::founder(store_root_hash, &first_pubkey, &founder_grant);
        let first_stream = AuthorStreamId::from_bytes([71; 16]);
        let second_stream = AuthorStreamId::from_bytes([72; 16]);
        let founder = CircleRosterEntry::founder(
            store_root_hash,
            circle_id,
            "first-device",
            first_stream,
            founder_grant,
            &first_owner,
        );
        let mut base = vec![founder];
        let add_second = CircleRosterChain::from_entries(base.clone())
            .expect("founder roster")
            .signed_set_member(
                "first-device",
                first_stream,
                second_pubkey,
                CircleRole::Owner,
                &first_owner,
            )
            .expect("add second owner");
        base.push(add_second);
        let remove_second = CircleRosterChain::from_entries(base.clone())
            .expect("two-owner roster")
            .signed_remove_member(
                "first-device",
                first_stream,
                keys::public_key_hex(&second_owner),
                &first_owner,
            )
            .expect("first branch");
        let remove_first = CircleRosterChain::from_entries(base.clone())
            .expect("two-owner roster")
            .signed_remove_member(
                "second-device",
                second_stream,
                first_pubkey.clone(),
                &second_owner,
            )
            .expect("second branch");
        let mut conflict_entries = base;
        conflict_entries.extend([remove_second.clone(), remove_first.clone()]);
        let conflict_heads = vec![
            CircleRosterHead::signed(&remove_second, &first_owner),
            CircleRosterHead::signed(&remove_first, &second_owner),
        ];
        let conflicted = CircleRosterChain::from_entries_with_heads(
            conflict_entries.clone(),
            conflict_heads.clone(),
        )
        .expect("cross-revocation conflict");
        let resolver_branch = match conflicted.status() {
            CircleRosterStatus::Conflict(CircleRosterConflict::RevocationCycle {
                maximal_valid_branches,
                ..
            }) => maximal_valid_branches
                .iter()
                .find(|branch| {
                    branch
                        .active_grants
                        .values()
                        .any(|grant| grant.member_pubkey == first_pubkey)
                })
                .expect("first owner's branch")
                .heads
                .clone(),
            _ => panic!("expected revocation cycle"),
        };
        let resolution = conflicted
            .signed_cycle_resolution(resolver_branch, &first_owner)
            .expect("resolution");
        let mut resumed = conflicted.clone();
        resumed
            .apply_resolutions(std::slice::from_ref(&resolution))
            .expect("apply resolution");
        let resolution_only = resumed.clone();
        let mut activated = resumed.clone();
        let activation = activated
            .signed_resolution_activation(
                "first-device",
                AuthorStreamId::from_bytes([74; 16]),
                &resolution,
                &first_owner,
            )
            .expect("Circle resolution activation");
        assert_eq!(
            activation,
            activated
                .signed_resolution_activation(
                    "first-device",
                    AuthorStreamId::from_bytes([74; 16]),
                    &resolution,
                    &first_owner,
                )
                .expect("Circle resolution activation retry")
        );
        let frontier_before_activation = activated.effective_frontier();
        assert_eq!(activation.dependencies, frontier_before_activation);
        let rejects_activation = |entry: CircleRosterEntry| {
            let mut entries = resumed.entries().to_vec();
            entries.push(entry);
            matches!(
                resumed.replay_resolved_history_to_heads(entries, Vec::new()),
                Err(super::super::circle::CircleRosterError::InvalidEntry(_))
            )
        };
        let mut missing_ref = activation.clone();
        missing_ref.resolution_dependencies.clear();
        missing_ref.signature = keys::sign_hex(&first_owner, &missing_ref.canonical_bytes()).1;
        assert!(rejects_activation(missing_ref));

        let mut wrong_frontier = activation.clone();
        wrong_frontier.dependencies.clear();
        wrong_frontier.signature =
            keys::sign_hex(&first_owner, &wrong_frontier.canonical_bytes()).1;
        assert!(rejects_activation(wrong_frontier));

        let mut wrong_cut = activation.clone();
        let mut unapplied = resolution.resolution_ref();
        unapplied.resolution_hash = ObjectHash::digest(b"unapplied Circle resolution");
        wrong_cut.resolution_dependencies.push(unapplied);
        wrong_cut.resolution_dependencies.sort();
        wrong_cut.signature = keys::sign_hex(&first_owner, &wrong_cut.canonical_bytes()).1;
        assert!(rejects_activation(wrong_cut));

        let mut old_grant = activation.clone();
        old_grant.author_owner_grant = remove_second.author_owner_grant.clone();
        old_grant.signature = keys::sign_hex(&first_owner, &old_grant.canonical_bytes()).1;
        assert!(rejects_activation(old_grant));

        let mut wrong_ref = activation.clone();
        let super::super::circle::CircleRosterChange::ResolutionActivation {
            resolution: wrong_resolution_ref,
        } = &mut wrong_ref.change
        else {
            unreachable!()
        };
        wrong_resolution_ref.resolver_pubkey = keys::public_key_hex(&second_owner);
        wrong_ref.signature = keys::sign_hex(&first_owner, &wrong_ref.canonical_bytes()).1;
        assert!(rejects_activation(wrong_ref));
        let state_before_activation = activated.resolved();
        let mut activated_entries = activated.entries().to_vec();
        activated_entries.push(activation.clone());
        activated = activated
            .replay_resolved_history_to_heads(activated_entries, Vec::new())
            .expect("apply Circle resolution activation");
        assert_eq!(activated.resolved(), state_before_activation);
        assert_ne!(activated.effective_frontier(), frontier_before_activation);
        assert!(activated.effective_frontier().contains(&activation.coord()));
        let activation_head = activated
            .signed_head_for_stream(&first_owner, "first-device", activation.stream_id)
            .expect("chain-derived Circle activation head");
        assert_eq!(
            activation_head.resolutions,
            activation.resolution_dependencies
        );
        let suffix = resumed
            .signed_set_member(
                "first-device",
                AuthorStreamId::from_bytes([73; 16]),
                keys::public_key_hex(&UserKeypair::generate()),
                CircleRole::Member,
                &first_owner,
            )
            .expect("post-resolution suffix");
        let mut current_heads = conflict_heads.clone();
        current_heads.push(CircleRosterHead::signed(&suffix, &first_owner));
        current_heads.sort_by_key(CircleRosterHead::entry_coord);
        let mut resumed_entries = resumed.entries().to_vec();
        resumed_entries.push(suffix.clone());
        resumed = resumed
            .replay_resolved_history_to_heads(resumed_entries, current_heads.clone())
            .expect("apply suffix");

        let storage = MockSyncStorage::new();
        let encryption = EncryptionService::from(MasterKeyring::generate());
        let context = ProtocolObjectContext::circle(
            store_root_hash,
            ProtocolObjectDomain::CircleRoster,
            encryption.clone(),
        );
        for entry in &conflict_entries {
            super::super::store_objects::append_and_verify(
                &storage,
                &context,
                &circle_semantic_prefix(CircleSemanticSlot::RosterEntry {
                    circle_id,
                    coord: &entry.coord(),
                }),
                ".json",
                &serde_json::to_vec(entry).expect("serialize roster entry"),
            )
            .await
            .expect("upload conflict closure");
        }
        for head in &conflict_heads {
            super::super::store_objects::append_and_verify(
                &storage,
                &context,
                &circle_semantic_prefix(CircleSemanticSlot::RosterHead {
                    circle_id,
                    head: &CircleRosterHeadRef::from_head(head),
                }),
                ".json",
                &serde_json::to_vec(head).expect("serialize roster head"),
            )
            .await
            .expect("upload conflict head");
        }
        super::super::store_objects::append_and_verify(
            &storage,
            &context,
            &circle_semantic_prefix(CircleSemanticSlot::RosterEntry {
                circle_id,
                coord: &activation.coord(),
            }),
            ".json",
            &serde_json::to_vec(&activation).expect("serialize resolution activation"),
        )
        .await
        .expect("upload Circle resolution activation");
        super::super::store_objects::append_and_verify(
            &storage,
            &context,
            &circle_semantic_prefix(CircleSemanticSlot::RosterHead {
                circle_id,
                head: &CircleRosterHeadRef::from_head(&activation_head),
            }),
            ".json",
            &serde_json::to_vec(&activation_head).expect("serialize resolution activation head"),
        )
        .await
        .expect("upload Circle resolution activation head");
        super::super::store_objects::append_circle_roster_resolution_object(
            &storage,
            encryption.clone(),
            &resolution,
        )
        .await
        .expect("upload Circle resolution");
        let mut exact_heads = conflict_heads
            .iter()
            .map(CircleRosterHeadRef::from_head)
            .collect::<Vec<_>>();
        exact_heads.push(CircleRosterHeadRef::from_head(&activation_head));
        exact_heads.sort();
        let state = super::super::circle::CircleRosterStateRef::MergeConcurrent {
            heads: exact_heads,
            resolutions: vec![resolution.resolution_ref()],
            state_hash: activated.resolved().state_hash,
        };
        let resolved_without_assignment_suffix = load_circle_roster_state(
            &storage,
            store_root_hash,
            circle_id,
            &state,
            encryption.clone(),
        )
        .await
        .expect("resolution activation is an exact signed Circle cut");
        assert_eq!(resolved_without_assignment_suffix, activated.resolved());

        let adversarial_stream = AuthorStreamId::from_bytes([75; 16]);
        let first_in_stream = resolution_only
            .signed_set_member(
                "first-device",
                adversarial_stream,
                keys::public_key_hex(&UserKeypair::generate()),
                CircleRole::Member,
                &first_owner,
            )
            .expect("valid Circle sequence-one entry");
        let mut adversarial_entries = resolution_only.entries().to_vec();
        adversarial_entries.push(first_in_stream.clone());
        let adversarial_chain = resolution_only
            .replay_resolved_history_to_heads(adversarial_entries, Vec::new())
            .expect("apply Circle sequence-one entry");
        let mut forged_activation = adversarial_chain
            .signed_set_member(
                "first-device",
                adversarial_stream,
                keys::public_key_hex(&UserKeypair::generate()),
                CircleRole::Member,
                &first_owner,
            )
            .expect("valid Circle sequence-two entry");
        forged_activation.change = super::super::circle::CircleRosterChange::ResolutionActivation {
            resolution: resolution.resolution_ref(),
        };
        forged_activation.signature =
            keys::sign_hex(&first_owner, &forged_activation.canonical_bytes()).1;
        assert_eq!(forged_activation.seq, 2);
        assert!(forged_activation.previous_hash.is_some());
        for entry in [&first_in_stream, &forged_activation] {
            super::super::store_objects::append_and_verify(
                &storage,
                &context,
                &circle_semantic_prefix(CircleSemanticSlot::RosterEntry {
                    circle_id,
                    coord: &entry.coord(),
                }),
                ".json",
                &serde_json::to_vec(entry).expect("serialize adversarial Circle entry"),
            )
            .await
            .expect("upload adversarial Circle entry");
        }
        let forged_head = CircleRosterHead::signed(&forged_activation, &first_owner);
        super::super::store_objects::append_and_verify(
            &storage,
            &context,
            &circle_semantic_prefix(CircleSemanticSlot::RosterHead {
                circle_id,
                head: &CircleRosterHeadRef::from_head(&forged_head),
            }),
            ".json",
            &serde_json::to_vec(&forged_head).expect("serialize forged Circle head"),
        )
        .await
        .expect("upload forged Circle head");
        let mut forged_heads = conflict_heads
            .iter()
            .map(CircleRosterHeadRef::from_head)
            .collect::<Vec<_>>();
        forged_heads.push(CircleRosterHeadRef::from_head(&forged_head));
        forged_heads.sort();
        let forged_state = super::super::circle::CircleRosterStateRef::MergeConcurrent {
            heads: forged_heads,
            resolutions: vec![resolution.resolution_ref()],
            state_hash: adversarial_chain.resolved().state_hash,
        };
        load_circle_roster_state(
            &storage,
            store_root_hash,
            circle_id,
            &forged_state,
            encryption,
        )
        .await
        .expect_err("a Circle resolution activation cannot extend an existing stream");

        let loaded = replay_circle_roster_resolutions(
            &storage,
            store_root_hash,
            circle_id,
            &context,
            std::slice::from_ref(&suffix),
            &current_heads,
            &[resolution],
        )
        .await
        .expect("fresh reader loads exact Circle conflict closure");
        assert_eq!(loaded.author_heads(), resumed.author_heads());
        assert_eq!(loaded.resolved(), resumed.resolved());
    }

    #[tokio::test]
    async fn concurrent_same_circle_conflict_resolutions_merge_before_suffix_replay() {
        let first = UserKeypair::generate();
        let second = UserKeypair::generate();
        let first_pubkey = keys::public_key_hex(&first);
        let second_pubkey = keys::public_key_hex(&second);
        let root = ObjectHash::digest(b"same-conflict Circle Store");
        let founder_grant = MembershipGrantId(ObjectHash::digest(b"same-conflict founder"));
        let circle_id = CircleId::founder(root, &first_pubkey, &founder_grant);
        let first_stream = AuthorStreamId::from_bytes([211; 16]);
        let founder = CircleRosterEntry::founder(
            root,
            circle_id,
            "first-device",
            first_stream,
            founder_grant,
            &first,
        );
        let mut base = vec![founder];
        let add_second = CircleRosterChain::from_entries(base.clone())
            .unwrap()
            .signed_set_member(
                "first-device",
                first_stream,
                second_pubkey.clone(),
                CircleRole::Owner,
                &first,
            )
            .unwrap();
        base.push(add_second);
        let remove_second = CircleRosterChain::from_entries(base.clone())
            .unwrap()
            .signed_remove_member("first-device", first_stream, second_pubkey.clone(), &first)
            .unwrap();
        let remove_first = CircleRosterChain::from_entries(base.clone())
            .unwrap()
            .signed_remove_member(
                "second-device",
                AuthorStreamId::from_bytes([212; 16]),
                first_pubkey.clone(),
                &second,
            )
            .unwrap();
        let mut conflict_entries = base;
        conflict_entries.extend([remove_second.clone(), remove_first.clone()]);
        let conflict_heads = vec![
            CircleRosterHead::signed(&remove_second, &first),
            CircleRosterHead::signed(&remove_first, &second),
        ];
        let conflicted = CircleRosterChain::from_entries_with_heads(
            conflict_entries.clone(),
            conflict_heads.clone(),
        )
        .unwrap();
        let branches = match conflicted.status() {
            CircleRosterStatus::Conflict(CircleRosterConflict::RevocationCycle {
                maximal_valid_branches,
                ..
            }) => maximal_valid_branches,
            _ => panic!("expected Circle revocation cycle"),
        };
        let first_branch = branches
            .iter()
            .find(|branch| {
                branch.active_grants.values().any(|grant| {
                    grant.member_pubkey == first_pubkey && grant.role == CircleRole::Owner
                })
            })
            .unwrap()
            .heads
            .clone();
        let second_branch = branches
            .iter()
            .find(|branch| {
                branch.active_grants.values().any(|grant| {
                    grant.member_pubkey == second_pubkey && grant.role == CircleRole::Owner
                })
            })
            .unwrap()
            .heads
            .clone();
        let resolution_a = conflicted
            .signed_cycle_resolution(first_branch, &first)
            .unwrap();
        let resolution_b = conflicted
            .signed_cycle_resolution(second_branch, &second)
            .unwrap();
        let mut after_a = conflicted.clone();
        after_a
            .apply_resolutions(std::slice::from_ref(&resolution_a))
            .unwrap();
        let suffix_a = after_a
            .signed_set_member(
                "first-device",
                AuthorStreamId::from_bytes([213; 16]),
                second_pubkey.clone(),
                CircleRole::Owner,
                &first,
            )
            .unwrap();
        assert!(matches!(
            &suffix_a.change,
            super::super::circle::CircleRosterChange::SetMember { replaces, .. }
                if replaces.is_empty()
        ));
        let mut after_b = conflicted.clone();
        after_b
            .apply_resolutions(std::slice::from_ref(&resolution_b))
            .unwrap();
        let suffix_b = after_b
            .signed_set_member(
                "second-device",
                AuthorStreamId::from_bytes([214; 16]),
                keys::public_key_hex(&UserKeypair::generate()),
                CircleRole::Member,
                &second,
            )
            .unwrap();
        let mut current_heads = conflict_heads.clone();
        current_heads.extend([
            CircleRosterHead::signed(&suffix_a, &first),
            CircleRosterHead::signed(&suffix_b, &second),
        ]);
        current_heads.sort_by_key(CircleRosterHead::entry_coord);
        let storage = MockSyncStorage::new();
        let context = ProtocolObjectContext::circle(
            root,
            ProtocolObjectDomain::CircleRoster,
            EncryptionService::from(MasterKeyring::generate()),
        );
        for entry in &conflict_entries {
            super::super::store_objects::append_and_verify(
                &storage,
                &context,
                &circle_semantic_prefix(CircleSemanticSlot::RosterEntry {
                    circle_id,
                    coord: &entry.coord(),
                }),
                ".json",
                &serde_json::to_vec(entry).unwrap(),
            )
            .await
            .unwrap();
        }
        for head in &conflict_heads {
            super::super::store_objects::append_and_verify(
                &storage,
                &context,
                &circle_semantic_prefix(CircleSemanticSlot::RosterHead {
                    circle_id,
                    head: &CircleRosterHeadRef::from_head(head),
                }),
                ".json",
                &serde_json::to_vec(head).unwrap(),
            )
            .await
            .unwrap();
        }

        let unactivated = replay_circle_roster_resolutions(
            &storage,
            root,
            circle_id,
            &context,
            &[],
            &conflict_heads,
            &[resolution_b.clone(), resolution_a.clone()],
        )
        .await
        .expect_err("raw Circle resolution objects do not activate themselves");
        assert!(unactivated
            .to_string()
            .contains("differ from the exact signed head cut"));

        let loaded = replay_circle_roster_resolutions(
            &storage,
            root,
            circle_id,
            &context,
            &[suffix_b, suffix_a],
            &current_heads,
            &[resolution_b, resolution_a],
        )
        .await
        .expect("concurrent Circle resolutions replay as one causal cut");
        assert!(matches!(
            loaded.status(),
            CircleRosterStatus::Conflict(CircleRosterConflict::ConcurrentMemberAssignments {
                member_pubkey,
                ..
            }) if member_pubkey == &second_pubkey
        ));
    }

    #[tokio::test]
    async fn resolution_replay_orders_circle_checkpoints_by_signed_head_references() {
        let first = user_keypair_from_seed([11; 32]);
        let second = user_keypair_from_seed([12; 32]);
        let third = user_keypair_from_seed([13; 32]);
        let fourth = user_keypair_from_seed([14; 32]);
        let pubkeys = [&first, &second, &third, &fourth]
            .into_iter()
            .map(keys::public_key_hex)
            .collect::<Vec<_>>();
        let store_root_hash = ObjectHash::digest(b"ordered Circle replay Store");
        let founder_grant = MembershipGrantId(ObjectHash::digest(b"ordered Circle founder"));
        let circle_id = CircleId::founder(store_root_hash, &pubkeys[0], &founder_grant);
        let founder_stream = AuthorStreamId::from_bytes([151; 16]);
        let founder = CircleRosterEntry::founder(
            store_root_hash,
            circle_id,
            "first-device",
            founder_stream,
            founder_grant,
            &first,
        );
        let mut history = vec![founder];
        for pubkey in pubkeys.iter().skip(1) {
            let add = CircleRosterChain::from_entries(history.clone())
                .expect("load roster")
                .signed_set_member(
                    "first-device",
                    founder_stream,
                    pubkey.clone(),
                    CircleRole::Owner,
                    &first,
                )
                .expect("add Owner");
            history.push(add);
        }
        let base = CircleRosterChain::from_entries(history.clone()).expect("four-Owner roster");
        let remove_second = base
            .signed_remove_member("first-device", founder_stream, pubkeys[1].clone(), &first)
            .expect("first conflict branch");
        let remove_first = base
            .signed_remove_member(
                "second-device",
                AuthorStreamId::from_bytes([152; 16]),
                pubkeys[0].clone(),
                &second,
            )
            .expect("second conflict branch");
        history.extend([remove_second.clone(), remove_first.clone()]);
        let first_heads = vec![
            CircleRosterHead::signed(&remove_second, &first),
            CircleRosterHead::signed(&remove_first, &second),
        ];
        let first_conflict =
            CircleRosterChain::from_entries_with_heads(history.clone(), first_heads.clone())
                .expect("first conflict");
        let first_branch = match first_conflict.status() {
            CircleRosterStatus::Conflict(CircleRosterConflict::RevocationCycle {
                maximal_valid_branches,
                ..
            }) => maximal_valid_branches
                .iter()
                .find(|branch| {
                    branch.active_grants.values().any(|record| {
                        record.member_pubkey == pubkeys[0] && record.role == CircleRole::Owner
                    })
                })
                .expect("first Owner branch")
                .heads
                .clone(),
            _ => panic!("expected first conflict"),
        };
        let first_resolution = first_conflict
            .signed_cycle_resolution(first_branch, &first)
            .expect("first resolution");
        let mut resumed = first_conflict;
        resumed
            .apply_resolutions(std::slice::from_ref(&first_resolution))
            .expect("apply first resolution");

        let remove_fourth = resumed
            .signed_remove_member(
                "third-device",
                AuthorStreamId::from_bytes([2; 16]),
                pubkeys[3].clone(),
                &third,
            )
            .expect("third Owner removes fourth");
        let remove_third = resumed
            .signed_remove_member(
                "fourth-device",
                AuthorStreamId::from_bytes([102; 16]),
                pubkeys[2].clone(),
                &fourth,
            )
            .expect("fourth Owner removes third");
        let refs = vec![first_resolution.resolution_ref()];
        let second_heads = vec![
            CircleRosterHead::signed_with_resolutions(&remove_fourth, refs.clone(), &third),
            CircleRosterHead::signed_with_resolutions(&remove_third, refs, &fourth),
        ];
        let mut entries = resumed.entries().to_vec();
        entries.extend([remove_fourth.clone(), remove_third.clone()]);
        let mut heads = first_heads.clone();
        heads.extend(second_heads.clone());
        let mut second_conflict = resumed
            .replay_resolved_history_to_heads(entries, heads)
            .expect("second conflict");
        let second_branch = match second_conflict.status() {
            CircleRosterStatus::Conflict(CircleRosterConflict::RevocationCycle {
                maximal_valid_branches,
                ..
            }) => maximal_valid_branches
                .iter()
                .find(|branch| {
                    branch.active_grants.values().any(|record| {
                        record.member_pubkey == pubkeys[2] && record.role == CircleRole::Owner
                    })
                })
                .expect("third Owner branch")
                .heads
                .clone(),
            _ => panic!("expected second revocation cycle"),
        };
        let second_resolution = second_conflict
            .signed_cycle_resolution(second_branch, &third)
            .expect("second resolution");
        assert!(
            second_resolution.conflict_hash < first_resolution.conflict_hash,
            "fixture must put the causally later Circle resolution first by canonical key"
        );
        let second_entries = vec![remove_fourth, remove_third];
        second_conflict
            .apply_resolutions(std::slice::from_ref(&second_resolution))
            .expect("apply second resolution");
        let suffix = second_conflict
            .signed_set_member(
                "third-device",
                AuthorStreamId::from_bytes([250; 16]),
                keys::public_key_hex(&user_keypair_from_seed([15; 32])),
                CircleRole::Member,
                &third,
            )
            .expect("suffix");
        let mut final_entries = second_conflict.entries().to_vec();
        final_entries.push(suffix.clone());
        let mut current_heads = first_heads.clone();
        current_heads.extend(second_heads.clone());
        current_heads.push(CircleRosterHead::signed_with_resolutions(
            &suffix,
            second_conflict.resolution_refs().to_vec(),
            &third,
        ));
        current_heads.sort_by_key(CircleRosterHead::entry_coord);
        second_conflict = second_conflict
            .replay_resolved_history_to_heads(final_entries, current_heads.clone())
            .expect("apply suffix");
        history.extend(second_entries);

        let storage = MockSyncStorage::new();
        let context = ProtocolObjectContext::circle(
            store_root_hash,
            ProtocolObjectDomain::CircleRoster,
            EncryptionService::from(MasterKeyring::generate()),
        );
        for entry in &history {
            super::super::store_objects::append_and_verify(
                &storage,
                &context,
                &circle_semantic_prefix(CircleSemanticSlot::RosterEntry {
                    circle_id,
                    coord: &entry.coord(),
                }),
                ".json",
                &serde_json::to_vec(entry).expect("serialize entry"),
            )
            .await
            .expect("upload history");
        }
        for head in first_heads.iter().chain(&second_heads) {
            super::super::store_objects::append_and_verify(
                &storage,
                &context,
                &circle_semantic_prefix(CircleSemanticSlot::RosterHead {
                    circle_id,
                    head: &CircleRosterHeadRef::from_head(head),
                }),
                ".json",
                &serde_json::to_vec(head).expect("serialize head"),
            )
            .await
            .expect("upload historical head");
        }
        let absent = replay_circle_roster_resolutions(
            &storage,
            store_root_hash,
            circle_id,
            &context,
            std::slice::from_ref(&suffix),
            &current_heads,
            std::slice::from_ref(&second_resolution),
        )
        .await
        .expect_err("historical Circle head refs require their exact resolution object");
        assert!(absent.to_string().contains("references absent resolution"));
        let loaded = replay_circle_roster_resolutions(
            &storage,
            store_root_hash,
            circle_id,
            &context,
            std::slice::from_ref(&suffix),
            &current_heads,
            &[first_resolution.clone(), second_resolution],
        )
        .await
        .expect("signed Circle head refs impose causal checkpoint order");

        assert_eq!(loaded.resolved(), second_conflict.resolved());
        assert_eq!(loaded.resolution_refs(), second_conflict.resolution_refs());
    }

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
                resolutions: Vec::new(),
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

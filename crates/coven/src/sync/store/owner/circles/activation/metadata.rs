use std::collections::{BTreeMap, BTreeSet};

use super::{
    CircleActivationVerifier, CircleHeadKind, CircleHeadValue, VerifiedStreamActivationPrefix,
};
use crate::encryption::EncryptionService;
use crate::protocol::circle::{
    circle_semantic_prefix, verify_circle_semantic_prefix, CircleId, CircleMetadata,
    CircleMetadataHeadRef, CircleSemanticSlot,
};
use crate::protocol::store_commit::{
    CircleActivationObjects, GrantStreamAnchor, ObjectHash, StoreBatchCommit, StoreBatchCommitRef,
    StreamActivationId,
};
use crate::storage::{ProtocolObjectContext, ProtocolObjectDomain};
use crate::sync::store::circle_controls::CircleOperationError;

impl<'operation, 'storage> CircleActivationVerifier<'operation, 'storage> {
    pub(super) async fn load_circle_metadata_state(
        &mut self,
        verified_prefix: &VerifiedStreamActivationPrefix,
        commit: &StoreBatchCommit,
        circle_id: CircleId,
        state: &crate::protocol::circle::CircleMetadataStateRef,
        encryption: EncryptionService,
        objects: &CircleActivationObjects,
        commit_ref: &StoreBatchCommitRef,
        consumed_stream_activations: &mut BTreeSet<StreamActivationId>,
    ) -> Result<CircleMetadata, CircleOperationError> {
        let root = self.root.clone();
        commit_ref
            .verify_commit(commit)
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        if root.store_root_hash != commit.store_root_hash {
            return Err(CircleOperationError::InvalidState(
                "Circle metadata authority differs from its Store root".to_string(),
            ));
        }
        let store_root_hash = commit.store_root_hash;
        if state.heads.is_empty()
            || !state
                .heads
                .windows(2)
                .all(|pair| pair[0].coord.stream_key() < pair[1].coord.stream_key())
        {
            return Err(CircleOperationError::InvalidState(
                "Circle metadata heads are not one canonical head per stream".to_string(),
            ));
        }
        let mut pending = BTreeSet::new();
        for reference in &state.heads {
            let prefix = circle_semantic_prefix(CircleSemanticSlot::MetadataHead {
                circle_id,
                head: reference,
            });
            let object = objects
                .metadata_heads
                .iter()
                .find(|stored| *stored == reference)
                .ok_or_else(|| {
                    CircleOperationError::InvalidState(format!(
                        "Circle activation omits exact metadata head {}",
                        reference.head_hash
                    ))
                })?;
            let tip_object = objects
                .metadata_entries
                .get(&reference.coord)
                .ok_or_else(|| {
                    CircleOperationError::InvalidState(format!(
                        "Circle activation omits metadata head tip {}",
                        reference.coord.metadata_hash
                    ))
                })?;
            let head_encryption = encryption
                .service_for_fingerprint(tip_object.key_fingerprint.as_bytes())
                .map_err(|error| {
                    CircleOperationError::InvalidState(format!(
                        "Circle metadata head names an unavailable key fingerprint: {error}"
                    ))
                })?;
            let context = ProtocolObjectContext::circle(
                store_root_hash,
                ProtocolObjectDomain::CircleMetadata,
                head_encryption,
            );
            let bytes = self
                .storage
                .read_protocol_object(&context, &object.object, &prefix)
                .await
                .map_err(crate::storage::StoreObjectError::from)?;
            let head: crate::protocol::circle::CircleMetadataHead = serde_json::from_slice(&bytes)
                .map_err(|error| {
                    CircleOperationError::InvalidState(format!(
                        "parse exact Circle metadata head: {error}"
                    ))
                })?;
            let declared_ref =
                CircleMetadataHeadRef::from_stored_head(&head, object.object.clone());
            let authority = self
                .resolve_circle_stream_authority(
                    verified_prefix,
                    commit_ref,
                    commit,
                    head.successor.activation,
                    head.stream_id,
                    circle_id,
                    &head.author_owner_grant,
                    |circle_id, first_slot| GrantStreamAnchor::CircleMetadata {
                        circle_id,
                        first_slot,
                    },
                )
                .await?;
            self.verify_circle_head_chain(
                &ProtocolObjectContext::circle(
                    store_root_hash,
                    ProtocolObjectDomain::CircleMetadata,
                    encryption.clone(),
                ),
                CircleHeadKind::Metadata,
                CircleHeadValue::Metadata(head.clone()),
                object.object.clone(),
                &authority,
            )
            .await?;
            if !head.verify_for_registration(&authority.registration)
                || authority.registration.author_pubkey != head.author_pubkey
                || (authority.activated_here && head.seq != 1)
                || head.successor.activation != authority.activation_id
                || (head.seq == 1
                    && (head.successor.predecessor.is_some()
                        || object.object.slot() != &authority.first_slot))
                || head.head_hash() != reference.head_hash
                || head.tip != tip_object.object
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
                || &declared_ref != reference
            {
                return Err(CircleOperationError::InvalidState(
                    "Circle metadata head failed exact verification".to_string(),
                ));
            }
            if authority.activated_here {
                consumed_stream_activations.insert(authority.activation_id);
            }
            pending.insert(head.coord());
        }
        let selected = state.selected.clone();
        let expected_heads = state
            .heads
            .iter()
            .map(|reference| reference.coord.clone())
            .collect::<Vec<_>>();

        let mut entries = BTreeMap::new();
        while let Some(coord) = pending.pop_first() {
            if entries.contains_key(&coord) {
                continue;
            }
            let prefix = circle_semantic_prefix(CircleSemanticSlot::MetadataEntry {
                circle_id,
                coord: &coord,
            });
            let object = objects.metadata_entries.get(&coord).ok_or_else(|| {
                CircleOperationError::InvalidState(format!(
                    "Circle activation omits exact metadata entry {}",
                    coord.metadata_hash
                ))
            })?;
            let exact_encryption = encryption
                .service_for_fingerprint(object.key_fingerprint.as_bytes())
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
            let bytes = self
                .storage
                .read_protocol_object(&exact_context, &object.object, &prefix)
                .await
                .map_err(crate::storage::StoreObjectError::from)?;
            if ObjectHash::digest(&bytes) != coord.metadata_hash {
                return Err(CircleOperationError::InvalidState(
                    "Circle metadata bytes differ from the signed coordinate".to_string(),
                ));
            }
            let entry: CircleMetadata = serde_json::from_slice(&bytes).map_err(|error| {
                CircleOperationError::InvalidState(format!(
                    "parse exact Circle metadata entry: {error}"
                ))
            })?;
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
                || declared_coord != coord
                || entry.key_fingerprint != object.key_fingerprint
            {
                return Err(CircleOperationError::InvalidState(
                    "Circle metadata entry failed exact verification".to_string(),
                ));
            }
            let author_roster = self
                .load_circle_roster_state(
                    verified_prefix,
                    commit_ref,
                    commit,
                    circle_id,
                    &entry.author_roster,
                    encryption.clone(),
                    objects,
                    consumed_stream_activations,
                )
                .await?;
            let author_is_owner = author_roster
                .authorizes_owner_grant_id(&entry.author_pubkey, &entry.author_owner_grant);
            if !author_is_owner {
                return Err(CircleOperationError::InvalidState(
                    "Circle metadata author lacks its exact grant in the named historical roster"
                        .to_string(),
                ));
            }
            pending.extend(entry.dependencies.iter().cloned());
            entries.insert(coord, entry);
        }
        verify_metadata_history(&entries, Some(&expected_heads))?;
        let selected_entry = entries.get(&selected).ok_or_else(|| {
            CircleOperationError::InvalidState(
                "selected Circle metadata coordinate is not in its covered history".to_string(),
            )
        })?;
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
            || state.state_hash != selected_entry.metadata_hash()
        {
            return Err(CircleOperationError::InvalidState(
                "Circle metadata selection or state hash is not canonical".to_string(),
            ));
        }
        Ok(selected_entry.clone())
    }
}

fn verify_metadata_history(
    entries: &BTreeMap<crate::protocol::circle::CircleMetadataCoord, CircleMetadata>,
    expected_heads: Option<&[crate::protocol::circle::CircleMetadataCoord]>,
) -> Result<(), CircleOperationError> {
    let mut streams = BTreeMap::<
        crate::protocol::circle::CircleAuthorStreamKey,
        BTreeMap<u64, &CircleMetadata>,
    >::new();
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

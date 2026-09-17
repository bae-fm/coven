use std::collections::{BTreeMap, BTreeSet};

use super::provenance::CircleEntryProvenance;
use super::{CircleActivationVerifier, VerifiedCircleActivationPrefix};
use crate::sync::store::circles::CircleOperationError;
use coven_keys::encryption::EncryptionService;
use coven_protocol::circle::{
    circle_semantic_prefix, verify_circle_semantic_prefix, CircleId, CircleMetadata,
    CircleSemanticSlot,
};
use coven_protocol::objects::{ProtocolObjectContext, ProtocolObjectDomain};
use coven_protocol::store_commit::{
    CircleActivationObjects, StoreBatchCommit, StoreBatchCommitRef, StoreDeviceRegistration,
};

impl<'operation, 'storage> CircleActivationVerifier<'operation, 'storage> {
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn load_circle_metadata_state(
        &mut self,
        prefix: &VerifiedCircleActivationPrefix,
        commit: &StoreBatchCommit,
        author: &StoreDeviceRegistration,
        circle_id: CircleId,
        state: &coven_protocol::circle::CircleMetadataStateRef,
        encryption: EncryptionService,
        objects: &CircleActivationObjects,
        commit_ref: &StoreBatchCommitRef,
    ) -> Result<CircleMetadata, CircleOperationError> {
        let root = self.root().clone();
        commit_ref
            .verify_commit(commit)
            .map_err(CircleOperationError::from)?;
        if root.store_root_hash != commit.store_root_hash {
            return Err(CircleOperationError::InvalidState(
                "Circle metadata authority differs from its Store root".to_string(),
            ));
        }
        let store_root_hash = commit.store_root_hash;
        if state.frontier.is_empty()
            || !state
                .frontier
                .windows(2)
                .all(|pair| pair[0].stream_key() < pair[1].stream_key())
        {
            return Err(CircleOperationError::InvalidState(
                "Circle metadata frontier is not one canonical position per stream".to_string(),
            ));
        }
        let mut pending = state.frontier.iter().cloned().collect::<BTreeSet<_>>();
        let selected = state.selected.clone();
        let expected_heads = state.frontier.clone();

        let mut entries = BTreeMap::new();
        while let Some(coord) = pending.pop_first() {
            if entries.contains_key(&coord) {
                continue;
            }
            let prefix_path = circle_semantic_prefix(CircleSemanticSlot::MetadataEntry {
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
                .map_err(CircleOperationError::Encryption)?;
            let exact_context = ProtocolObjectContext::circle(
                store_root_hash,
                ProtocolObjectDomain::CircleMetadata,
                exact_encryption,
            );
            let bytes = self
                .storage
                .read_protocol_object(&exact_context, &object.object, &prefix_path)
                .await
                .map_err(coven_protocol::objects::StoreObjectError::from)?;
            let entry: CircleMetadata = serde_json::from_slice(&bytes)?;
            let declared_coord = entry.coord();
            if declared_coord.metadata_hash != coord.metadata_hash {
                return Err(CircleOperationError::InvalidState(
                    "Circle metadata identifies itself as another entry".to_string(),
                ));
            }
            if !entry.verify()
                || verify_circle_semantic_prefix(
                    &prefix_path,
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
            self.verify_circle_entry_origin(
                prefix,
                commit,
                author,
                circle_id,
                CircleEntryProvenance::Metadata {
                    coord: &coord,
                    entry: &entry,
                    reference: object,
                },
            )
            .await?;
            let author_roster = self
                .load_circle_roster_state(
                    prefix,
                    commit_ref,
                    commit,
                    author,
                    circle_id,
                    &entry.author_roster,
                    encryption.clone(),
                    objects,
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
        // The inventory a control signs is exactly the history its frontier
        // reaches: an entry the closure never visits would be an author-stream
        // position carried outside the reduction that settles it.
        if entries.keys().cloned().collect::<BTreeSet<_>>()
            != objects.metadata_entries.keys().cloned().collect()
        {
            return Err(CircleOperationError::InvalidState(
                "Circle metadata inventory differs from the history its frontier reaches"
                    .to_string(),
            ));
        }
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
    entries: &BTreeMap<coven_protocol::circle::CircleMetadataCoord, CircleMetadata>,
    expected_heads: Option<&[coven_protocol::circle::CircleMetadataCoord]>,
) -> Result<(), CircleOperationError> {
    let mut streams = BTreeMap::<
        coven_protocol::circle::CircleAuthorStreamKey,
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
            "Circle metadata entries do not reach its signed frontier".to_string(),
        ));
    }
    Ok(())
}

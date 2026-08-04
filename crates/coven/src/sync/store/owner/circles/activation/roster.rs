use std::collections::{BTreeMap, BTreeSet};

use super::{
    CircleActivationVerifier, CircleHeadKind, CircleHeadValue, VerifiedStreamActivationPrefix,
};
use crate::encryption::EncryptionService;
use crate::protocol::circle::{
    circle_semantic_prefix, verify_circle_semantic_prefix, CircleId, CircleRosterHeadRef,
    CircleSemanticSlot, MergeCircleOwnerAuthorityRef, PreparedCircleControl, ResolvedCircleRoster,
};
use crate::protocol::circle_roster::CircleMaterializedRoster;
use crate::protocol::objects::{ProtocolObjectContext, ProtocolObjectDomain};
use crate::protocol::store_commit::{
    CircleActivationObjects, GrantStreamAnchor, ObjectHash, StoreBatchCommit, StoreBatchCommitRef,
    StreamActivationId,
};
use crate::sync::store::circle_controls::CircleOperationError;

impl<'operation, 'storage> CircleActivationVerifier<'operation, 'storage> {
    pub(super) async fn load_circle_roster_state(
        &mut self,
        verified_prefix: &VerifiedStreamActivationPrefix,
        commit_ref: &StoreBatchCommitRef,
        commit: &StoreBatchCommit,
        circle_id: CircleId,
        state: &crate::protocol::circle::MergeCircleRosterStateRef,
        encryption: EncryptionService,
        objects: &CircleActivationObjects,
        consumed_stream_activations: &mut BTreeSet<StreamActivationId>,
    ) -> Result<ResolvedCircleRoster, CircleOperationError> {
        self.load_circle_roster_chain(
            verified_prefix,
            commit_ref,
            commit,
            circle_id,
            state,
            encryption,
            objects,
            consumed_stream_activations,
        )
        .await?
        .try_resolved()
        .map_err(|error| CircleOperationError::InvalidState(error.to_string()))
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn load_circle_roster_chain(
        &mut self,
        verified_prefix: &VerifiedStreamActivationPrefix,
        commit_ref: &StoreBatchCommitRef,
        commit: &StoreBatchCommit,
        circle_id: CircleId,
        state: &crate::protocol::circle::MergeCircleRosterStateRef,
        encryption: EncryptionService,
        objects: &CircleActivationObjects,
        consumed_stream_activations: &mut BTreeSet<StreamActivationId>,
    ) -> Result<crate::protocol::circle::CircleRosterChain, CircleOperationError> {
        let store_root_hash = commit.store_root_hash;
        if state.heads.is_empty()
            || !state
                .heads
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
        if !state.resolutions.windows(2).all(|pair| pair[0] < pair[1]) {
            return Err(CircleOperationError::InvalidState(
                "Circle roster resolutions are not canonical".to_string(),
            ));
        }
        let loaded_heads = self
            .load_exact_circle_roster_heads(
                verified_prefix,
                commit_ref,
                commit,
                circle_id,
                &context,
                &state.heads,
                objects,
                consumed_stream_activations,
            )
            .await?;
        let activated_resolutions = loaded_heads
            .iter()
            .flat_map(|head| head.head().resolutions.iter().cloned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        if activated_resolutions != state.resolutions {
            return Err(CircleOperationError::InvalidState(
                "Circle roster state resolution refs differ from its signed heads".to_string(),
            ));
        }
        let entries = self
            .load_circle_roster_entries_from_heads(
                store_root_hash,
                circle_id,
                &context,
                &loaded_heads,
                objects,
            )
            .await?;
        let loaded_resolutions = self
            .load_circle_roster_resolutions(
                store_root_hash,
                circle_id,
                &state.resolutions,
                &encryption,
                objects,
            )
            .await?;
        let chain = if loaded_resolutions.is_empty() {
            crate::protocol::circle::CircleRosterChain::from_entries_with_heads(
                entries.clone(),
                loaded_heads,
            )
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?
        } else {
            self.replay_circle_roster_resolutions(
                verified_prefix,
                commit_ref,
                commit,
                circle_id,
                &context,
                &entries,
                &loaded_heads,
                &loaded_resolutions,
                objects,
                consumed_stream_activations,
            )
            .await?
        };
        let expected_heads = state
            .heads
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
        if resolved.state_hash != state.state_hash {
            return Err(CircleOperationError::InvalidState(
                "Circle roster state hash differs from its effective assignments".to_string(),
            ));
        }
        Ok(chain)
    }

    async fn load_circle_roster_entries_from_heads(
        &self,
        store_root_hash: ObjectHash,
        circle_id: CircleId,
        context: &ProtocolObjectContext,
        heads: &[crate::protocol::circle::ExactCircleRosterHead],
        objects: &CircleActivationObjects,
    ) -> Result<Vec<crate::protocol::circle::CircleRosterEntry>, CircleOperationError> {
        let mut pending = heads
            .iter()
            .map(|head| head.head().entry_coord())
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
            let object = objects.roster_entries.get(&coord).ok_or_else(|| {
                CircleOperationError::InvalidState(format!(
                    "Circle activation omits exact roster entry {}",
                    coord.entry_hash
                ))
            })?;
            let bytes = self
                .storage
                .read_protocol_object(context, object, &prefix)
                .await
                .map_err(crate::protocol::objects::StoreObjectError::from)?;
            if ObjectHash::digest(&bytes) != coord.entry_hash {
                return Err(CircleOperationError::InvalidState(
                    "Circle roster entry bytes differ from the signed coordinate".to_string(),
                ));
            }
            let entry: crate::protocol::circle::CircleRosterEntry = serde_json::from_slice(&bytes)
                .map_err(|error| {
                    CircleOperationError::InvalidState(format!(
                        "parse Circle roster entry: {error}"
                    ))
                })?;
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
                || declared_coord != coord
            {
                return Err(CircleOperationError::InvalidState(
                    "Circle roster entry failed exact verification".to_string(),
                ));
            }
            pending.extend(entry.dependencies.iter().cloned());
            entries.insert(coord, entry);
        }
        Ok(entries.into_values().collect())
    }

    async fn load_circle_roster_resolutions(
        &self,
        store_root_hash: ObjectHash,
        circle_id: CircleId,
        resolutions: &[crate::protocol::circle::CircleRosterConflictResolutionRef],
        encryption: &EncryptionService,
        objects: &CircleActivationObjects,
    ) -> Result<Vec<crate::protocol::circle::CircleRosterConflictResolution>, CircleOperationError>
    {
        let mut loaded_resolutions = Vec::with_capacity(resolutions.len());
        for reference in resolutions {
            let object = objects.roster_resolutions.get(reference).ok_or_else(|| {
                CircleOperationError::InvalidState(format!(
                    "Circle activation omits exact roster resolution {}",
                    reference.resolution_hash
                ))
            })?;
            // A resolution's own domain, not the entry-and-head domain the rest of this
            // module reads under: the sealing context is
            // `store root hash || domain label || semantic prefix`, and the roster
            // domain's path rule accepts entry and head paths only, so a resolution
            // sealed or opened under it is refused before any bytes are read.
            let context = ProtocolObjectContext::circle(
                store_root_hash,
                ProtocolObjectDomain::CircleRosterResolution,
                encryption.clone(),
            );
            let prefix = circle_semantic_prefix(CircleSemanticSlot::RosterResolution {
                circle_id,
                resolution: reference,
            });
            let bytes = self
                .storage
                .read_protocol_object(&context, object, &prefix)
                .await
                .map_err(crate::protocol::objects::StoreObjectError::from)?;
            if ObjectHash::digest(&bytes) != reference.resolution_hash {
                return Err(CircleOperationError::InvalidState(
                    "Circle roster resolution bytes differ from the signed reference".to_string(),
                ));
            }
            let resolution = serde_json::from_slice(&bytes).map_err(|error| {
                CircleOperationError::InvalidState(format!(
                    "parse Circle roster resolution: {error}"
                ))
            })?;
            loaded_resolutions.push(resolution);
        }
        Ok(loaded_resolutions)
    }

    async fn load_exact_circle_roster_heads(
        &mut self,
        verified_prefix: &VerifiedStreamActivationPrefix,
        commit_ref: &StoreBatchCommitRef,
        commit: &StoreBatchCommit,
        circle_id: CircleId,
        context: &ProtocolObjectContext,
        references: &[CircleRosterHeadRef],
        objects: &CircleActivationObjects,
        consumed_stream_activations: &mut BTreeSet<StreamActivationId>,
    ) -> Result<Vec<crate::protocol::circle::ExactCircleRosterHead>, CircleOperationError> {
        let store_root_hash = commit.store_root_hash;
        let mut loaded_heads = Vec::with_capacity(references.len());
        for reference in references {
            let prefix = circle_semantic_prefix(CircleSemanticSlot::RosterHead {
                circle_id,
                head: reference,
            });
            let object = objects
                .roster_heads
                .iter()
                .find(|stored| *stored == reference)
                .ok_or_else(|| {
                    CircleOperationError::InvalidState(format!(
                        "Circle activation omits exact roster head {}",
                        reference.head_hash
                    ))
                })?;
            let bytes = self
                .storage
                .read_protocol_object(context, &object.object, &prefix)
                .await
                .map_err(crate::protocol::objects::StoreObjectError::from)?;
            let head: crate::protocol::circle::CircleRosterHead = serde_json::from_slice(&bytes)
                .map_err(|error| {
                    CircleOperationError::InvalidState(format!("parse Circle roster head: {error}"))
                })?;
            let declared_ref = CircleRosterHeadRef::from_stored_head(&head, object.object.clone());
            let authority = self
                .resolve_circle_stream_authority(
                    verified_prefix,
                    commit_ref,
                    commit,
                    head.successor.activation,
                    head.stream_id,
                    circle_id,
                    &head.author_owner_grant,
                    |circle_id, first_slot| GrantStreamAnchor::CircleRoster {
                        circle_id,
                        first_slot,
                    },
                )
                .await?;
            self.verify_circle_head_chain(
                context,
                CircleHeadKind::Roster,
                CircleHeadValue::Roster(head.clone()),
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
                || head.tip
                    != *objects
                        .roster_entries
                        .get(&reference.coord)
                        .ok_or_else(|| {
                            CircleOperationError::InvalidState(format!(
                                "Circle activation omits roster head tip {}",
                                reference.coord.entry_hash
                            ))
                        })?
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
                || &declared_ref != reference
            {
                return Err(CircleOperationError::InvalidState(
                    "Circle roster head failed exact verification".to_string(),
                ));
            }
            if authority.activated_here {
                consumed_stream_activations.insert(authority.activation_id);
            }
            loaded_heads.push(
                crate::protocol::circle::ExactCircleRosterHead::bind(head, reference.clone())
                    .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?,
            );
        }
        Ok(loaded_heads)
    }

    async fn replay_circle_roster_resolutions(
        &mut self,
        verified_prefix: &VerifiedStreamActivationPrefix,
        commit_ref: &StoreBatchCommitRef,
        commit: &StoreBatchCommit,
        circle_id: CircleId,
        context: &ProtocolObjectContext,
        entries: &[crate::protocol::circle::CircleRosterEntry],
        current_heads: &[crate::protocol::circle::ExactCircleRosterHead],
        resolutions: &[crate::protocol::circle::CircleRosterConflictResolution],
        objects: &CircleActivationObjects,
        consumed_stream_activations: &mut BTreeSet<StreamActivationId>,
    ) -> Result<crate::protocol::circle::CircleRosterChain, CircleOperationError> {
        let store_root_hash = commit.store_root_hash;
        let known_resolution_refs = resolutions
            .iter()
            .map(|resolution| resolution.resolution_ref())
            .collect::<BTreeSet<_>>();
        let current_head_refs = crate::protocol::circle::CircleRosterChain::validate_exact_heads(
            entries,
            current_heads,
        )
        .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        let current_by_coord = entries
            .iter()
            .cloned()
            .map(|entry| (entry.coord(), entry))
            .collect::<BTreeMap<_, _>>();
        let activated_resolution_refs = current_heads
            .iter()
            .flat_map(|head| head.head().resolutions.iter().cloned())
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
            let heads = self
                .load_exact_circle_roster_heads(
                    verified_prefix,
                    commit_ref,
                    commit,
                    circle_id,
                    context,
                    conflict_heads,
                    objects,
                    consumed_stream_activations,
                )
                .await?;
            let conflict_entries = self
                .load_circle_roster_entries_from_heads(
                    store_root_hash,
                    circle_id,
                    context,
                    &heads,
                    objects,
                )
                .await?;
            crate::protocol::circle::CircleRosterChain::validate_exact_heads(
                &conflict_entries,
                &heads,
            )
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
            let dependencies = heads
                .iter()
                .flat_map(|head| head.head().resolutions.iter())
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
            crate::protocol::circle::CircleRosterConflictResolutionRef,
            crate::protocol::circle::CircleRosterChain,
        >::new();
        let mut applied = BTreeSet::new();
        while !prepared.is_empty() {
            let next = crate::protocol::causal_grants::canonical_ready_checkpoint(
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
            let (resolution, conflict_heads, conflict_entries, dependencies) =
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
                crate::protocol::circle::CircleRosterChain::from_entries_with_heads(
                    prefix.into_values().collect(),
                    conflict_heads,
                )
                .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?
            } else {
                let conflict_head_refs = conflict_heads
                    .iter()
                    .map(|head| head.reference().clone())
                    .collect();
                crate::protocol::circle::CircleRosterChain::replay_merged_resolved_histories_to_heads(
                &dependency_chains,
                prefix.into_values().collect(),
                conflict_head_refs,
            )
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?
            };
            conflict_chain
                .apply_resolutions(std::slice::from_ref(&resolution))
                .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
            resolved_by_ref.insert(next.clone(), conflict_chain);
            applied.insert(next);
        }

        let mut heads_by_cut = BTreeMap::<Vec<_>, Vec<_>>::new();
        for head in &current_head_refs {
            let entry = current_by_coord.get(&head.coord).ok_or_else(|| {
                CircleOperationError::InvalidState(format!(
                    "Circle roster head tip {} is absent",
                    head.coord.entry_hash
                ))
            })?;
            heads_by_cut
                .entry(entry.resolution_dependencies.clone())
                .or_default()
                .push(head.clone());
        }
        let mut branch_chains = Vec::new();
        for (cut, heads) in heads_by_cut {
            let cut_set = cut.iter().cloned().collect::<BTreeSet<_>>();
            let branch_heads = heads
                .into_iter()
                .filter(|head| {
                    let coord = head.coord.clone();
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
                .map(|head| head.coord.clone())
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
            let branch_exact_heads = current_heads
                .iter()
                .filter(|head| branch_heads.contains(head.reference()))
                .cloned()
                .collect::<Vec<_>>();
            let mut branch = if dependencies.is_empty() {
                crate::protocol::circle::CircleRosterChain::from_entries_with_heads(
                    branch_history.into_values().collect(),
                    branch_exact_heads,
                )
                .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?
            } else {
                crate::protocol::circle::CircleRosterChain::replay_merged_resolved_histories_to_heads(
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
        crate::protocol::circle::CircleRosterChain::replay_merged_resolved_histories_to_heads(
            &branch_refs,
            history.into_values().collect(),
            current_head_refs,
        )
        .map_err(|error| CircleOperationError::InvalidState(error.to_string()))
    }

    pub(super) async fn load_circle_authority_roster(
        &mut self,
        verified_prefix: &VerifiedStreamActivationPrefix,
        commit: &StoreBatchCommit,
        circle_id: CircleId,
        control: &PreparedCircleControl,
        encryption: EncryptionService,
        objects: &CircleActivationObjects,
        commit_ref: &StoreBatchCommitRef,
        consumed_stream_activations: &mut BTreeSet<StreamActivationId>,
    ) -> Result<CircleMaterializedRoster, CircleOperationError> {
        commit_ref
            .verify_commit(commit)
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        let roster = match &control.value.value.author_authority {
            MergeCircleOwnerAuthorityRef::Roster { roster, .. } => roster,
            MergeCircleOwnerAuthorityRef::ConflictResolution { .. } => {
                &control.value.access_epoch().roster
            }
        };
        self.load_circle_roster_state(
            verified_prefix,
            commit_ref,
            commit,
            circle_id,
            roster,
            encryption,
            objects,
            consumed_stream_activations,
        )
        .await
    }
}

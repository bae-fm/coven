use std::collections::{BTreeMap, BTreeSet};

use super::{
    CircleActivationVerifier, CircleHeadKind, CircleHeadValue, VerifiedStreamActivationPrefix,
};
use crate::sync::store::circles::CircleOperationError;
use coven_keys::encryption::EncryptionService;
use coven_protocol::circle::{
    circle_semantic_prefix, verify_circle_semantic_prefix, CircleId, CircleRosterHeadRef,
    CircleSemanticSlot, PreparedCircleControl, ResolvedCircleRoster,
};
use coven_protocol::circle_roster::CircleMaterializedRoster;
use coven_protocol::objects::{ProtocolObjectContext, ProtocolObjectDomain};
use coven_protocol::store_commit::{
    CircleActivationObjects, GrantStreamAnchor, ObjectHash, StoreBatchCommit, StoreBatchCommitRef,
    StreamActivationId,
};

impl<'operation, 'storage> CircleActivationVerifier<'operation, 'storage> {
    pub(super) async fn load_circle_roster_state(
        &mut self,
        verified_prefix: &VerifiedStreamActivationPrefix,
        commit_ref: &StoreBatchCommitRef,
        commit: &StoreBatchCommit,
        circle_id: CircleId,
        state: &coven_protocol::circle::MergeCircleRosterStateRef,
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
        .map_err(CircleOperationError::from)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn load_circle_roster_chain(
        &mut self,
        verified_prefix: &VerifiedStreamActivationPrefix,
        commit_ref: &StoreBatchCommitRef,
        commit: &StoreBatchCommit,
        circle_id: CircleId,
        state: &coven_protocol::circle::MergeCircleRosterStateRef,
        encryption: EncryptionService,
        objects: &CircleActivationObjects,
        consumed_stream_activations: &mut BTreeSet<StreamActivationId>,
    ) -> Result<coven_protocol::circle::CircleRosterChain, CircleOperationError> {
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
            encryption,
        );
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
        let entries = self
            .load_circle_roster_entries_from_heads(
                store_root_hash,
                circle_id,
                &context,
                &loaded_heads,
                objects,
            )
            .await?;
        let chain = coven_protocol::circle::CircleRosterChain::from_entries_with_heads(
            entries,
            loaded_heads,
        )
        .map_err(CircleOperationError::from)?;
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
        let resolved = chain.try_resolved().map_err(CircleOperationError::from)?;
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
        heads: &[coven_protocol::circle::ExactCircleRosterHead],
        objects: &CircleActivationObjects,
    ) -> Result<Vec<coven_protocol::circle::CircleRosterEntry>, CircleOperationError> {
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
                .map_err(coven_protocol::objects::StoreObjectError::from)?;
            let entry: coven_protocol::circle::CircleRosterEntry = serde_json::from_slice(&bytes)?;
            let declared_coord = entry.coord();
            if declared_coord.entry_hash != coord.entry_hash {
                return Err(CircleOperationError::InvalidState(
                    "Circle roster entry identifies itself as another entry".to_string(),
                ));
            }
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
    ) -> Result<Vec<coven_protocol::circle::ExactCircleRosterHead>, CircleOperationError> {
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
                .map_err(coven_protocol::objects::StoreObjectError::from)?;
            let head: coven_protocol::circle::CircleRosterHead = serde_json::from_slice(&bytes)?;
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
                coven_protocol::circle::ExactCircleRosterHead::bind(head, reference.clone())
                    .map_err(CircleOperationError::from)?,
            );
        }
        Ok(loaded_heads)
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
            .map_err(CircleOperationError::from)?;
        self.load_circle_roster_state(
            verified_prefix,
            commit_ref,
            commit,
            circle_id,
            &control.value.value.author_authority.roster,
            encryption,
            objects,
            consumed_stream_activations,
        )
        .await
    }
}

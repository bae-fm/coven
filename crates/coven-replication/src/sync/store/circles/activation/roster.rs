use std::collections::{BTreeMap, BTreeSet};

use super::provenance::CircleEntryProvenance;
use super::{CircleActivationVerifier, VerifiedCircleActivationPrefix};
use crate::sync::store::circles::CircleOperationError;
use coven_keys::encryption::EncryptionService;
use coven_protocol::circle::{
    circle_semantic_prefix, verify_circle_semantic_prefix, CircleId, CircleSemanticSlot,
    PreparedCircleControl, ResolvedCircleRoster,
};
use coven_protocol::circle_roster::CircleMaterializedRoster;
use coven_protocol::objects::{ProtocolObjectContext, ProtocolObjectDomain};
use coven_protocol::store_commit::{
    CircleActivationObjects, ObjectHash, StoreBatchCommit, StoreBatchCommitRef,
    StoreDeviceRegistration,
};

impl<'operation, 'storage> CircleActivationVerifier<'operation, 'storage> {
    pub(super) async fn load_circle_roster_state(
        &mut self,
        prefix: &VerifiedCircleActivationPrefix,
        commit_ref: &StoreBatchCommitRef,
        commit: &StoreBatchCommit,
        author: &StoreDeviceRegistration,
        circle_id: CircleId,
        state: &coven_protocol::circle::MergeCircleRosterStateRef,
        encryption: EncryptionService,
        objects: &CircleActivationObjects,
    ) -> Result<ResolvedCircleRoster, CircleOperationError> {
        self.load_circle_roster_chain(
            prefix, commit_ref, commit, author, circle_id, state, encryption, objects,
        )
        .await?
        .try_resolved()
        .map_err(CircleOperationError::from)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn load_circle_roster_chain(
        &mut self,
        prefix: &VerifiedCircleActivationPrefix,
        commit_ref: &StoreBatchCommitRef,
        commit: &StoreBatchCommit,
        author: &StoreDeviceRegistration,
        circle_id: CircleId,
        state: &coven_protocol::circle::MergeCircleRosterStateRef,
        encryption: EncryptionService,
        objects: &CircleActivationObjects,
    ) -> Result<coven_protocol::circle::CircleRosterChain, CircleOperationError> {
        commit_ref
            .verify_commit(commit)
            .map_err(CircleOperationError::from)?;
        let store_root_hash = commit.store_root_hash;
        if state.frontier.is_empty()
            || !state
                .frontier
                .windows(2)
                .all(|pair| pair[0].stream_key() < pair[1].stream_key())
        {
            return Err(CircleOperationError::InvalidState(
                "Circle roster frontier is not one canonical position per stream".to_string(),
            ));
        }
        let context = ProtocolObjectContext::circle(
            store_root_hash,
            ProtocolObjectDomain::CircleRoster,
            encryption,
        );
        let entries = self
            .load_circle_roster_entries(
                prefix,
                commit,
                author,
                store_root_hash,
                circle_id,
                &context,
                &state.frontier,
                objects,
            )
            .await?;
        let chain = coven_protocol::circle::CircleRosterChain::from_entries(entries)
            .map_err(CircleOperationError::from)?;
        if chain.author_heads() != state.frontier {
            return Err(CircleOperationError::InvalidState(
                "Circle roster entries do not reach its signed frontier".to_string(),
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

    #[allow(clippy::too_many_arguments)]
    async fn load_circle_roster_entries(
        &mut self,
        prefix: &VerifiedCircleActivationPrefix,
        commit: &StoreBatchCommit,
        author: &StoreDeviceRegistration,
        store_root_hash: ObjectHash,
        circle_id: CircleId,
        context: &ProtocolObjectContext,
        frontier: &[coven_protocol::circle::CircleRosterCoord],
        objects: &CircleActivationObjects,
    ) -> Result<Vec<coven_protocol::circle::CircleRosterEntry>, CircleOperationError> {
        let mut pending = frontier.iter().cloned().collect::<BTreeSet<_>>();
        let mut entries = BTreeMap::new();
        while let Some(coord) = pending.pop_first() {
            if entries.contains_key(&coord) {
                continue;
            }
            let prefix_path = circle_semantic_prefix(CircleSemanticSlot::RosterEntry {
                circle_id,
                coord: &coord,
            });
            let reference = objects.roster_entries.get(&coord).ok_or_else(|| {
                CircleOperationError::InvalidState(format!(
                    "Circle activation omits exact roster entry {}",
                    coord.entry_hash
                ))
            })?;
            let bytes = self
                .storage
                .read_protocol_object(context, &reference.object, &prefix_path)
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
                    &prefix_path,
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
            self.verify_circle_entry_origin(
                prefix,
                commit,
                author,
                circle_id,
                CircleEntryProvenance::Roster {
                    coord: &coord,
                    entry: &entry,
                    reference,
                },
            )
            .await?;
            pending.extend(entry.dependencies.iter().cloned());
            entries.insert(coord, entry);
        }
        Ok(entries.into_values().collect())
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn load_circle_authority_roster(
        &mut self,
        prefix: &VerifiedCircleActivationPrefix,
        commit: &StoreBatchCommit,
        author: &StoreDeviceRegistration,
        circle_id: CircleId,
        control: &PreparedCircleControl,
        encryption: EncryptionService,
        objects: &CircleActivationObjects,
        commit_ref: &StoreBatchCommitRef,
    ) -> Result<CircleMaterializedRoster, CircleOperationError> {
        self.load_circle_roster_state(
            prefix,
            commit_ref,
            commit,
            author,
            circle_id,
            &control.value.value.author_authority.roster,
            encryption,
            objects,
        )
        .await
    }
}

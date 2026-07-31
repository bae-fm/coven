//! Causal discovery and atomic materialization for immutable Store commits.

use std::collections::BTreeMap;
use std::sync::Arc;

use super::*;
use crate::protocol::membership::MembershipChain;
use crate::protocol::store_commit::{CommitFrontier, StoreDeviceStatus, StoreHistoryCut};
use crate::store_dir::StoreDir;
use crate::sync::conflict::TableSchema;

pub(super) struct AuthorizedPull<'operation, 'storage> {
    history: &'operation mut super::AuthorizedStoreHistory<'storage>,
    store_dir: &'operation StoreDir,
    membership: &'operation MembershipChain,
    identity: Option<&'operation UserKeypair>,
    routing_encryption: Option<&'operation crate::encryption::EncryptionService>,
}

impl<'operation, 'storage> AuthorizedPull<'operation, 'storage> {
    pub(super) fn new(
        history: &'operation mut super::AuthorizedStoreHistory<'storage>,
        store_dir: &'operation StoreDir,
        membership: &'operation MembershipChain,
        identity: Option<&'operation UserKeypair>,
        routing_encryption: Option<&'operation crate::encryption::EncryptionService>,
    ) -> Self {
        Self {
            history,
            store_dir,
            membership,
            identity,
            routing_encryption,
        }
    }

    pub(super) async fn execute(&mut self) -> Result<StorePullExecution, StorePullError> {
        let retained = self.history.prepare_pull_retained_history().await?;
        let store_dir = self.store_dir;
        let membership = self.membership;
        let identity = self.identity;
        let routing_encryption = self.routing_encryption;
        let store_root_hash = self.history.root().store_root_hash;
        membership
            .ensure_resolved()
            .map_err(StorePullMembershipError::State)
            .map_err(StorePullError::Membership)?;
        let routing_key = if self.history.pull_has_scoped_graph() {
            let encryption = routing_encryption.ok_or_else(|| {
                StorePullError::Database(
                    "scoped Store pull requires row-routing encryption".to_string(),
                )
            })?;
            Some(
                crate::protocol::circle::derive_row_routing_key(encryption, store_root_hash)
                    .map_err(|error| {
                        StorePullError::Database(format!("derive row routing key: {error}"))
                    })?,
            )
        } else {
            None
        };
        let local_frontier = self
            .history
            .pull_materialized_frontier()
            .await
            .map_err(|error| {
                StorePullError::Database(format!("load discovery device-state frontier: {error}"))
            })?;
        let local_frontier = local_frontier
            .into_values()
            .map(|reference| (reference.coord.stream_id, reference))
            .collect::<BTreeMap<_, _>>();
        let (_, discovery_device_state) = self
            .history
            .pull_device_state_for_cut(&StoreHistoryCut(local_frontier))
            .await?;

        let mut active = self
            .history
            .load_active_pull_registrations()
            .await
            .map_err(|error| {
                StorePullError::Database(format!("load active Merge registrations: {error}"))
            })?;
        for recovered in self
            .history
            .discover_pull_owner_recoveries(membership)
            .await?
        {
            if active
                .iter()
                .all(|(reference, _)| reference != &recovered.0)
            {
                active.push(recovered);
            }
        }
        let mut candidates = BTreeMap::new();
        let mut visible_heads = Vec::new();
        let mut held = Vec::new();
        for (registration_ref, registration) in active {
            let inactive_cut = match discovery_device_state
                .devices
                .get(&registration_ref.device_id)
            {
                Some(record) if record.registration != registration_ref => {
                    return Err(StorePullError::Database(format!(
                        "discovery device state names another registration for {}",
                        registration_ref.device_id
                    )));
                }
                Some(record) => match &record.status {
                    StoreDeviceStatus::Active => None,
                    StoreDeviceStatus::Inactive { accepted_cut, .. } => Some(accepted_cut),
                },
                None => None,
            };
            let discovered = self
                .history
                .discover_pull_stream(&registration_ref, &registration, inactive_cut)
                .await
                .map_err(|error| {
                    StorePullError::Database(format!(
                        "discover Merge stream for {}: {error}",
                        registration.device_id
                    ))
                })?;
            if let Some(head) = discovered.latest_head {
                visible_heads.push(VerifiedStoreDeviceHead {
                    head,
                    author: registration.clone(),
                });
            }
            if let Some(block) = discovered.block {
                held.push(block.into_position());
            }
            for (activation_head_ref, activation_head, commit_ref, commit) in discovered.commits {
                if commit_ref.coord.sequence() != commit.seq() {
                    held.push(HeldStorePosition::commit(
                        &commit_ref,
                        HeldStorePositionReason::InvalidObject(
                            "exact commit coordinate differs from signed sequence".to_string(),
                        ),
                    ));
                    continue;
                }
                let stream_id = commit_stream_id(&commit_ref.coord);
                if let Some(materialized) = self
                    .history
                    .pull_exact_materialized_ref(&stream_id, commit_ref.coord.sequence())
                    .await?
                {
                    if materialized == commit_ref {
                        continue;
                    }
                    held.push(HeldStorePosition::commit(
                        &commit_ref,
                        HeldStorePositionReason::HashMismatch {
                            referenced_device_id: stream_id,
                            referenced_commit: commit_ref.clone(),
                            materialized_hash: materialized.commit_hash,
                        },
                    ));
                    continue;
                }
                if let Some(package) = commit.store_package() {
                    if package.schema_version > self.history.pull_schema_version() {
                        held.push(HeldStorePosition::commit(
                            &commit_ref,
                            HeldStorePositionReason::NewerSchema {
                                local: self.history.pull_schema_version(),
                                required: package.schema_version,
                            },
                        ));
                        continue;
                    }
                }
                if let Err(error) = self.history.verify_pull_refs([commit_ref.clone()]).await {
                    let reason = match error {
                        StorePullError::Object(error) => held_object_error(error),
                        error => HeldStorePositionReason::InvalidObject(error.to_string()),
                    };
                    held.push(HeldStorePosition::commit(&commit_ref, reason));
                    continue;
                }
                let verified = self
                    .history
                    .verified_pull_commit(&commit_ref)
                    .ok_or_else(|| {
                        StorePullError::Database(
                            "Merge candidate is absent from its operation-verified history"
                                .to_string(),
                        )
                    })?;
                if verified.verified.value() != &commit {
                    held.push(HeldStorePosition::commit(
                        &commit_ref,
                        HeldStorePositionReason::InvalidObject(
                            "Merge candidate differs from its operation-verified history"
                                .to_string(),
                        ),
                    ));
                    continue;
                }
                let predecessor_membership = verified.predecessor_membership.clone();
                let registrations = verified.registrations.clone();
                let device_operations = verified.operations.clone();
                let membership_control = verified.membership_control.clone();
                let membership_prefix = self
                    .history
                    .verified_pull_membership_prefix(commit_predecessor_references(&commit))?;
                let verified_commit = verified.verified.clone();
                let package = match self
                    .history
                    .load_pull_store_package(verified_commit.reference())
                    .await
                {
                    Ok(package) => package.map(|package| package.value),
                    Err(error) => {
                        held.push(HeldStorePosition::package(
                            &commit_ref,
                            &commit,
                            held_object_error(error),
                        ));
                        continue;
                    }
                };
                let key = (
                    commit_stream_id(&commit_ref.coord),
                    commit_ref.coord.sequence(),
                );
                candidates.insert(
                    key,
                    MergeCandidate {
                        activation_head,
                        activation_head_object: activation_head_ref.object,
                        candidate: Candidate {
                            verified: verified_commit,
                            package,
                            registrations,
                        },
                        predecessor_membership,
                        device_operations,
                        membership_control,
                        membership_prefix,
                    },
                );
            }
        }
        let mut loaded_predecessor_memberships = BTreeMap::new();
        for materialization in retained {
            if materialization.commit().membership_authority.is_none() {
                continue;
            }
            let membership = self
                .history
                .load_pull_predecessor_membership(&materialization.commit().membership_state)
                .await
                .map_err(|error| match error {
                    RegistrationLoadError::Object(error) => StorePullError::Object(error),
                    RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
                })?;
            loaded_predecessor_memberships.insert(materialization.commit_ref().clone(), membership);
        }
        for candidate in candidates.values() {
            loaded_predecessor_memberships.insert(
                candidate.candidate.commit_ref().clone(),
                candidate.predecessor_membership.clone(),
            );
        }
        let loaded_predecessor_memberships = LoadedMergePredecessorMemberships {
            by_commit: loaded_predecessor_memberships,
        };
        let schema: Arc<TableSchema> =
            Arc::new(self.history.pull_table_schema().await.map_err(|error| {
                StorePullError::Database(format!("load synced table schema: {error}"))
            })?);
        let coverage = self
            .history
            .pull_snapshot_coverage()
            .await
            .map_err(|error| {
                StorePullError::Database(format!("load snapshot coverage frontier: {error}"))
            })?;
        let mut frontier = self
            .history
            .pull_materialized_frontier()
            .await
            .map_err(|error| {
                StorePullError::Database(format!("load materialized frontier: {error}"))
            })?;
        let mut row_changes = Vec::new();
        let mut changesets_applied = 0_u64;
        let mut asset_downloads_failed = false;
        let mut blocked = BTreeMap::new();
        let mut latest_membership = membership.clone();

        loop {
            let mut progressed = false;
            let mut keys = candidates.keys().cloned().collect::<Vec<_>>();
            keys.sort_by_key(|key| {
                (
                    candidates.get(key).is_none_or(|candidate| {
                        candidate.candidate.commit().circle_controls().is_empty()
                    }),
                    key.clone(),
                )
            });
            for key in keys {
                let candidate = candidates.get(&key).ok_or_else(|| {
                    StorePullError::Database(
                        "Merge candidate disappeared while evaluating readiness".to_string(),
                    )
                })?;
                let exclusion_freezes = self.history.pull_exclusion_freezes().await?;
                let current_frontier = CommitFrontier::from_refs(frontier.clone())
                    .map_err(|error| StorePullError::Database(error.to_string()))?;
                let (_, current_device_state) = self
                    .history
                    .pull_device_state_for_cut(&StoreHistoryCut(current_frontier.0))
                    .await?;
                match self
                    .history
                    .pull_readiness(
                        &coverage,
                        &frontier,
                        &current_device_state,
                        &exclusion_freezes,
                        candidate.candidate.commit_ref(),
                        candidate.candidate.commit(),
                    )
                    .await
                    .map_err(|error| {
                        StorePullError::Database(format!(
                            "evaluate Store commit readiness for {}/{}: {error}",
                            key.0, key.1
                        ))
                    })? {
                    Readiness::AlreadyMaterialized => {
                        candidates.remove(&key);
                        blocked.remove(&key);
                        progressed = true;
                    }
                    Readiness::Held(held_position) => {
                        blocked.insert(key, held_position);
                    }
                    Readiness::Ready => {
                        let candidate = candidates.remove(&key).ok_or_else(|| {
                            StorePullError::Database(
                                "ready Merge candidate disappeared before apply".to_string(),
                            )
                        })?;
                        match Box::pin(apply_candidate(
                            self.history,
                            store_dir,
                            schema.clone(),
                            &candidate,
                            &loaded_predecessor_memberships,
                            identity,
                            &mut latest_membership,
                            routing_key.as_ref(),
                        ))
                        .await?
                        {
                            ApplyOutcome::Applied(changes) => {
                                let stream_id =
                                    commit_stream_id(&candidate.candidate.commit_ref().coord);
                                frontier.insert(
                                    stream_id.clone(),
                                    candidate.candidate.commit_ref().clone(),
                                );
                                row_changes.extend(changes);
                                changesets_applied =
                                    changesets_applied.checked_add(1).ok_or_else(|| {
                                        StorePullError::Database(
                                            "Store apply count exceeded u64".to_string(),
                                        )
                                    })?;
                                blocked.remove(&key);
                                progressed = true;
                            }
                            ApplyOutcome::Held(reason) => {
                                if matches!(reason, HeldStorePositionReason::BlobDownloadFailed) {
                                    asset_downloads_failed = true;
                                }
                                let held_position = HeldStorePosition::commit(
                                    candidate.candidate.commit_ref(),
                                    reason,
                                );
                                candidates.insert(key.clone(), candidate);
                                blocked.insert(key, held_position);
                            }
                        }
                    }
                }
            }
            if !progressed {
                break;
            }
        }

        held.extend(blocked.into_values());
        held.sort_by(|left, right| {
            (left.coordinate.device_id(), left.coordinate.seq())
                .cmp(&(right.coordinate.device_id(), right.coordinate.seq()))
        });
        let local_blob_cleanup_pending = self
            .history
            .finish_pull_blob_cleanup(store_dir)
            .await
            .map_err(|error| {
                StorePullError::Database(format!("drain local blob cleanup intents: {error}"))
            })?;
        Ok(StorePullExecution {
            result: StorePullResult {
                changesets_applied,
                held_positions: held,
                visible_heads,
                row_changes,
                asset_downloads_failed,
                local_blob_cleanup_pending,
                #[cfg(test)]
                frontier,
            },
            membership: latest_membership,
        })
    }
}

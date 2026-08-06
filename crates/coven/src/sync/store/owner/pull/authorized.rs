//! Causal discovery and atomic materialization for immutable Store commits.

use super::*;
use crate::database::{PreparedMergeMaterialization, PreparedMergeMaterializationPackage};
use crate::protocol::membership::ApplyOutcome;
use crate::protocol::membership::MembershipChain;
use crate::protocol::store_commit::{CommitFrontier, StoreDeviceStatus, StoreHistoryCut};
use std::collections::BTreeMap;

pub(crate) struct AuthorizedPull<'operation, 'storage> {
    history: &'operation mut super::AuthorizedStoreHistory<'storage>,
    package_schema: std::sync::Arc<crate::database::TableSchema>,
    membership: &'operation MembershipChain,
    identity: Option<&'operation UserKeypair>,
    routing_encryption: Option<&'operation crate::encryption::EncryptionService>,
}

impl<'operation, 'storage> AuthorizedPull<'operation, 'storage> {
    pub(crate) async fn load(
        history: &'operation mut super::AuthorizedStoreHistory<'storage>,
        membership: &'operation MembershipChain,
        identity: Option<&'operation UserKeypair>,
        routing_encryption: Option<&'operation crate::encryption::EncryptionService>,
    ) -> Result<Self, StorePullError> {
        let package_schema = history.pull_package_schema().await.map_err(|error| {
            StorePullError::Database(crate::database::DbError::context(
                "load pull package schema",
                error,
            ))
        })?;
        Ok(Self {
            history,
            package_schema,
            membership,
            identity,
            routing_encryption,
        })
    }

    pub(crate) async fn execute(&mut self) -> Result<StorePullExecution, StorePullError> {
        let retained = self.history.prepare_pull_retained_history().await?;
        let membership = self.membership;
        let routing_encryption = self.routing_encryption;
        let store_root_hash = self.history.root().store_root_hash;
        membership
            .ensure_resolved()
            .map_err(StorePullMembershipError::State)
            .map_err(StorePullError::Membership)?;
        let routing_key = if self.history.pull_has_scoped_graph() {
            let encryption = routing_encryption.ok_or_else(|| {
                StorePullError::InvalidState(
                    "scoped Store pull requires row-routing encryption".to_string(),
                )
            })?;
            Some(
                crate::protocol::circle::derive_row_routing_key(encryption, store_root_hash)
                    .map_err(|error| StorePullError::context("derive row routing key", error))?,
            )
        } else {
            None
        };
        let local_frontier = self
            .history
            .pull_materialized_frontier()
            .await
            .map_err(|error| {
                StorePullError::Database(crate::database::DbError::context(
                    "load discovery device-state frontier",
                    error,
                ))
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
            .map_err(|error| StorePullError::context("load active Merge registrations", error))?;
        for recovered in self
            .history
            .discover_pull_owner_recoveries(membership)
            .await?
        {
            if active
                .iter()
                .all(|registration| registration.reference() != recovered.reference())
            {
                active.push(recovered);
            }
        }
        let mut candidates = BTreeMap::new();
        let mut visible_heads = Vec::new();
        let mut held = Vec::new();
        for registration in active {
            let registration_ref = registration.reference();
            let inactive_cut = match discovery_device_state
                .devices
                .get(&registration_ref.device_id)
            {
                Some(record) if record.registration != *registration_ref => {
                    return Err(StorePullError::InvalidState(format!(
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
                .discover_pull_stream(registration_ref, registration.value(), inactive_cut)
                .await
                .map_err(|error| {
                    StorePullError::context(
                        format!(
                            "discover Merge stream for {}",
                            registration.value().device_id
                        ),
                        error,
                    )
                })?;
            if let Some(head) = discovered.latest_head {
                visible_heads.push(VerifiedStoreDeviceHead {
                    head,
                    author: registration.value().clone(),
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
                        StorePullError::InvalidState(
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
                    RegistrationLoadError::Invalid(error) => StorePullError::InvalidState(error),
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
        let coverage = self
            .history
            .pull_snapshot_coverage()
            .await
            .map_err(|error| {
                StorePullError::Database(crate::database::DbError::context(
                    "load snapshot coverage frontier",
                    error,
                ))
            })?;
        let mut frontier = self
            .history
            .pull_materialized_frontier()
            .await
            .map_err(|error| {
                StorePullError::Database(crate::database::DbError::context(
                    "load materialized frontier",
                    error,
                ))
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
                    StorePullError::InvalidState(
                        "Merge candidate disappeared while evaluating readiness".to_string(),
                    )
                })?;
                let exclusion_freezes = self.history.pull_exclusion_freezes().await?;
                let current_frontier = CommitFrontier::from_refs(frontier.clone())
                    .map_err(StorePullError::Protocol)?;
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
                        StorePullError::context(
                            format!("evaluate Store commit readiness for {}/{}", key.0, key.1),
                            error,
                        )
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
                            StorePullError::InvalidState(
                                "ready Merge candidate disappeared before apply".to_string(),
                            )
                        })?;
                        match Box::pin(self.apply_candidate(
                            &candidate,
                            &loaded_predecessor_memberships,
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
                                        StorePullError::InvalidState(
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
        let local_blob_cleanup_pending =
            self.history
                .drain_local_blob_cleanup()
                .await
                .map_err(|error| {
                    StorePullError::Database(crate::database::DbError::context(
                        "drain local blob cleanup intents",
                        error,
                    ))
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

    async fn apply_candidate(
        &mut self,
        merge_candidate: &MergeCandidate,
        loaded_predecessor_memberships: &LoadedMergePredecessorMemberships,
        latest_membership: &mut MembershipChain,
        routing_key: Option<&super::circle::RowRoutingKey>,
    ) -> Result<ApplyOutcome, StorePullError> {
        let root = self.history.root().clone();
        let candidate = &merge_candidate.candidate;
        let commit = candidate.commit();
        let commit_ref = candidate.commit_ref();
        let author = candidate.author();
        let device_operations = merge_candidate.device_operations.clone();
        if !commit.device_exclusion_proposals().is_empty()
            || !commit.device_exclusion_outcomes().is_empty()
        {
            let (state_ref, _) = self
                .history
                .pull_device_state_for_order(&commit.order)
                .await?;
            if state_ref != commit.device_state {
                return Err(StorePullError::InvalidState(
                    "Merge exclusion commit differs from its materialized predecessor device state"
                        .to_string(),
                ));
            }
        }
        let membership_objects = self
            .history
            .verified_pull_membership_objects(commit_ref, commit)
            .await?;
        let (local_store_membership, membership_after_candidate) = self
            .local_store_membership_after_candidate(
                latest_membership,
                &root,
                &merge_candidate.predecessor_membership,
                membership_objects.as_ref(),
            )?;
        let verified_prefix = VerifiedStreamActivationPrefix::empty();
        let circle_activations = if commit.control().is_some() {
            merge_candidate.membership_control.clone().ok_or_else(|| {
                super::CirclePackageReadError::Invalid(
                    "Merge membership control is absent from its operation-verified history"
                        .to_string(),
                )
            })
        } else {
            self.history
                .circles()
                .activations()
                .load_payload(
                    &candidate.verified,
                    self.identity
                        .filter(|_| local_store_membership.allows_circle_access()),
                    routing_key,
                    &verified_prefix,
                    &merge_candidate.membership_prefix,
                )
                .await
                .map_err(|error| super::CirclePackageReadError::Invalid(error.to_string()))
        };
        let verified_circle_activations = match circle_activations {
            Ok(activations) => activations,
            Err(super::CirclePackageReadError::Database(error)) => return Err(error.into()),
            Err(super::CirclePackageReadError::Invalid(error)) => {
                return Ok(ApplyOutcome::Held(HeldStorePositionReason::InvalidObject(
                    error,
                )))
            }
        };
        // An excluded device that cannot yet read its successor bootstrap records the
        // exclusion now — detection is derived from the verified outcome, not the
        // bootstrap — and holds the successor. Its position advances only once a later
        // pull reads the bootstrap and reseeds; publication stays gated meanwhile.
        if !verified_circle_activations
            .bootstrap_pending_exclusions()
            .is_empty()
        {
            let pending = verified_circle_activations
                .bootstrap_pending_exclusions()
                .to_vec();
            self.history
                .record_pull_circle_close_exclusions(pending)
                .await?;
            return Ok(ApplyOutcome::Held(HeldStorePositionReason::InvalidObject(
                "excluded device awaiting its successor bootstrap to reset".to_string(),
            )));
        }
        let circle_packages = match self
            .history
            .circles()
            .packages()
            .load_applicable(
                &candidate.verified,
                verified_circle_activations.circles(),
                author,
                local_store_membership,
            )
            .await
        {
            Ok(packages) => packages,
            Err(super::CirclePackageReadError::Database(error)) => return Err(error.into()),
            Err(super::CirclePackageReadError::Invalid(error)) => {
                return Ok(ApplyOutcome::Held(HeldStorePositionReason::InvalidObject(
                    error,
                )))
            }
        };
        let mut packages =
            Vec::with_capacity(usize::from(candidate.package.is_some()) + circle_packages.len());
        if let Some(bytes) = candidate.package.as_ref() {
            let package = match candidate.parse_store_package(bytes) {
                Ok(package) => package,
                Err(error) => {
                    return Ok(ApplyOutcome::Held(
                        HeldStorePositionReason::InvalidChangeset(error),
                    ))
                }
            };
            let protection = self.history.pull_store_blob_protection()?;
            match self
                .history
                .prepare_pull_package(package, protection, self.package_schema.clone())
                .await?
            {
                Ok(package) => packages.push(package),
                Err(reason) => return Ok(ApplyOutcome::Held(reason)),
            }
        }
        for loaded in &circle_packages {
            let package = match candidate.parse_circle_package(loaded) {
                Ok(package) => package,
                Err(error) => {
                    return Ok(ApplyOutcome::Held(
                        HeldStorePositionReason::InvalidChangeset(error),
                    ))
                }
            };
            match self
                .history
                .prepare_pull_package(
                    package,
                    loaded.blob_protection.clone(),
                    self.package_schema.clone(),
                )
                .await?
            {
                Ok(package) => packages.push(package),
                Err(reason) => return Ok(ApplyOutcome::Held(reason)),
            }
        }
        let outcome = Box::pin(self.commit_candidate(
            merge_candidate,
            packages,
            device_operations,
            verified_circle_activations,
            membership_objects,
            loaded_predecessor_memberships,
            local_store_membership,
            routing_key,
        ))
        .await?;
        if matches!(outcome, ApplyOutcome::Applied(_)) {
            *latest_membership = membership_after_candidate;
        }
        #[cfg(test)]
        if matches!(outcome, ApplyOutcome::Applied(_)) {
            self.history
                .reach_pull_after_remote_commit_test_point(
                    commit_stream_id(&commit_ref.coord),
                    commit.seq(),
                )
                .await;
        }
        Ok(outcome)
    }

    fn local_store_membership_after_candidate(
        &self,
        latest: &MembershipChain,
        root: &StoreRootRef,
        predecessor: &MembershipChain,
        membership_objects: Option<&VerifiedMergeMembershipClosure>,
    ) -> Result<(LocalStoreMembership, MembershipChain), StorePullError> {
        let candidate = if let Some(membership_objects) = membership_objects {
            let proof = &membership_objects.proof;
            let mut successor = predecessor.clone();
            match (&proof.resolution, &proof.resolution_value) {
                (Some(reference), Some(value)) => successor
                    .apply_resolutions(root.store_root_hash, &[(reference.clone(), value.clone())])
                    .map_err(|error| {
                        StorePullError::Membership(StorePullMembershipError::State(error))
                    })?,
                (None, None) => {}
                _ => {
                    return Err(StorePullError::InvalidState(
                        "verified Merge membership proof has incomplete resolution evidence"
                            .to_string(),
                    ))
                }
            }
            successor
                .add_entry(proof.entry_value.clone())
                .and_then(|()| successor.activate_head_ref(proof.head.clone()))
                .map_err(|error| {
                    StorePullError::Membership(StorePullMembershipError::State(error))
                })?;
            successor
        } else {
            predecessor.clone()
        };
        let candidate_state = LocalStoreMembership::from_membership(&candidate, self.identity)
            .map_err(StorePullMembershipError::State)
            .map_err(StorePullError::Membership)?;
        if candidate.causally_includes(latest) {
            return Ok((candidate_state, candidate));
        }
        if latest.causally_includes(&candidate) {
            let latest_state = LocalStoreMembership::from_membership(latest, self.identity)
                .map_err(StorePullMembershipError::State)
                .map_err(StorePullError::Membership);
            return Ok((
                historical_local_store_membership(latest_state?, candidate_state),
                latest.clone(),
            ));
        }
        Err(StorePullError::Membership(
            StorePullMembershipError::Message(
                "latest Store membership and exact candidate membership are causally incomparable"
                    .to_string(),
            ),
        ))
    }

    async fn commit_candidate(
        &mut self,
        merge_candidate: &MergeCandidate,
        packages: Vec<PreparedMergeMaterializationPackage>,
        device_operations: VerifiedStoreDeviceOperations,
        verified_circle_activations: VerifiedCircleActivations,
        membership: Option<VerifiedMergeMembershipClosure>,
        loaded_predecessor_memberships: &LoadedMergePredecessorMemberships,
        local_store_membership: LocalStoreMembership,
        routing_key: Option<&super::circle::RowRoutingKey>,
    ) -> Result<ApplyOutcome, StorePullError> {
        let root = self.history.root().clone();
        let candidate = &merge_candidate.candidate;
        let commit = candidate.commit();
        let commit_ref = candidate.commit_ref();
        let author = candidate.author();
        let predecessor_membership = &merge_candidate.predecessor_membership;
        let (_, predecessor_state) = self
            .history
            .pull_device_state_for_order(&commit.order)
            .await?;
        verify_merge_membership_state_ref(
            &commit.membership_state,
            predecessor_membership,
            &predecessor_state,
        )?;
        let (authorized_predecessor, recovery_author) = predecessor_state
            .preactivate_recovery_author(commit, &candidate.registrations)
            .map_err(StorePullError::Protocol)?;
        let owner_recovery = self
            .history
            .verify_pull_owner_recovery_activation(commit)
            .await?;
        let state_after = device_operations
            .apply_to(authorized_predecessor.clone(), &commit.device_state)
            .and_then(|state| {
                state.apply_verified_lifecycle(
                    commit,
                    &candidate.registrations,
                    recovery_author.as_ref(),
                    owner_recovery,
                )
            })
            .map_err(StorePullError::Protocol)?;
        let retained_acknowledgement = self
            .history
            .retain_pull_acknowledgement(commit_ref, commit, author)
            .await?;
        let registrations = candidate
            .registrations
            .iter()
            .map(|registration| registration.registration().clone())
            .collect();
        let prepared_history = self
            .history
            .prepare_merge_history_successor(
                &candidate.verified,
                predecessor_membership,
                recovery_author.as_ref(),
                state_after.clone(),
                MergeHistorySuccessorEvidence {
                    registrations,
                    acknowledgement: retained_acknowledgement,
                    membership_proof: membership.as_ref().map(|closure| closure.proof.clone()),
                },
            )
            .await?;
        let activation_head_ref = super::store_commit::StoreDeviceHeadRef {
            head_hash: merge_candidate.activation_head.head_hash(),
            object: merge_candidate.activation_head_object.clone(),
        };
        prepared_history
            .summary
            .open(
                commit,
                commit_ref,
                &merge_candidate.activation_head,
                &activation_head_ref,
                &state_after,
            )
            .map_err(|error| {
                StorePullError::context("open prepared Merge history summary", error)
            })?;
        self.history
            .remember_pull_commit(candidate.verified.clone())?;
        let retractions = Box::pin(self.history.verified_pull_terminal_retractions(
            &merge_candidate.activation_head,
            &merge_candidate.activation_head_object,
            &candidate.verified,
            &authorized_predecessor,
            predecessor_membership,
            &device_operations,
            loaded_predecessor_memberships,
        ))
        .await?;
        let receiver_wall_ms = self.history.pull_receive_wall_ms();
        let materialization = PreparedMergeMaterialization {
            root: root.clone(),
            verified_commit: candidate.verified.clone(),
            activation_head: merge_candidate.activation_head.clone(),
            activation_head_object: merge_candidate.activation_head_object.clone(),
            history_summary: prepared_history.summary,
            membership_objects: membership.as_ref().map(|closure| closure.objects().clone()),
            membership_remote_objects: membership
                .map(VerifiedMergeMembershipClosure::into_remote_objects)
                .unwrap_or_default(),
            registrations: candidate.registrations.clone(),
            package_application: (!packages.is_empty()).then_some(
                crate::database::RetainedPackageApplication::Received { receiver_wall_ms },
            ),
            packages,
            device_operations,
            circle_activations: verified_circle_activations,
        };
        let outcome = self
            .history
            .commit_pull_materialization(
                materialization,
                retractions,
                local_store_membership,
                routing_key.cloned(),
                receiver_wall_ms,
            )
            .await?;
        self.history.resume_merge_retraction_cleanups().await?;
        Ok(outcome)
    }
}

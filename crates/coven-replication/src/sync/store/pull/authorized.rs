//! Causal discovery and atomic materialization for immutable Store commits.

use super::*;
use coven_database::{PreparedMergeMaterialization, PreparedMergeMaterializationPackage};
use coven_foundation::stage_timing::StageTimings;
use coven_protocol::membership::MembershipChain;
use coven_protocol::store_commit::CommitFrontier;
use std::collections::BTreeMap;

pub(crate) struct AuthorizedPull<'operation, 'storage> {
    history: super::PullHistory<'operation, 'storage>,
    package_schema: std::sync::Arc<coven_database::TableSchema>,
    membership: &'operation MembershipChain,
    identity: Option<&'operation UserKeypair>,
    routing_encryption: Option<&'operation coven_keys::encryption::EncryptionService>,
}

impl<'operation, 'storage> AuthorizedPull<'operation, 'storage> {
    pub(crate) async fn load(
        history: super::PullHistory<'operation, 'storage>,
        membership: &'operation MembershipChain,
        identity: Option<&'operation UserKeypair>,
        routing_encryption: Option<&'operation coven_keys::encryption::EncryptionService>,
    ) -> Result<Self, StorePullError> {
        let package_schema = history.package_schema().await.map_err(|error| {
            StorePullError::Database(coven_database::DbError::context(
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
        // Report the breakdown whichever way the pull ends. A pull that failed
        // partway is exactly the one whose stage timings are wanted, and the
        // stages it did reach are still real elapsed time.
        let mut timings = StageTimings::counting("Store pull", self.history.provider_requests());
        let outcome = Box::pin(self.execute_stages(&mut timings)).await;
        let outcome = self.history.finish_checkpoint_preparation(outcome).await;
        timings.report();
        outcome
    }

    async fn execute_stages(
        &mut self,
        timings: &mut StageTimings,
    ) -> Result<StorePullExecution, StorePullError> {
        let membership = self.membership;
        let routing_encryption = self.routing_encryption;
        let store_root_hash = self.history.root().store_root_hash;
        let routing_key = if self.history.has_scoped_graph() {
            let encryption = routing_encryption.ok_or_else(|| {
                StorePullError::InvalidState(
                    "scoped Store pull requires row-routing encryption".to_string(),
                )
            })?;
            Some(
                coven_protocol::circle::derive_row_routing_key(encryption, store_root_hash)
                    .map_err(|error| StorePullError::context("derive row routing key", error))?,
            )
        } else {
            None
        };
        let (mut remote_publication, installation) = timings
            .stage(
                "fetch accepted publications",
                self.history.load_store_publications_for_replay(),
            )
            .await?;
        let accepted_interval = match installation {
            StorePublicationReplayInstallation::Retained(accepted) => accepted,
            StorePublicationReplayInstallation::Checkpoint { expected, accepted } => {
                let selected = remote_publication.accepted_snapshots.pop().ok_or_else(|| {
                    StorePullError::InvalidState(
                        "checkpoint replacement has no verified current snapshot".into(),
                    )
                })?;
                self.history
                    .prepare_checkpoint(
                        expected,
                        selected,
                        membership,
                        self.identity,
                        routing_encryption,
                        routing_key.as_ref(),
                    )
                    .await?;
                accepted
            }
        };
        let mut candidates = BTreeMap::new();
        let visible_commits = remote_publication
            .commits
            .values()
            .map(|published| published.commit.clone())
            .collect::<Vec<_>>();
        let mut held = Vec::new();
        let mut pending = Vec::with_capacity(remote_publication.commits.len());
        for accepted in remote_publication.interval.entries() {
            let coven_protocol::store_commit::StorePublicationPayload::Commit(commit_ref) =
                &accepted.entry().payload
            else {
                continue;
            };
            let verified_commit = remote_publication.commits.get(commit_ref).ok_or_else(|| {
                StorePullError::InvalidState(
                    "accepted Store publication has no authenticated commit".to_string(),
                )
            })?;
            let verified_commit = &verified_commit.commit;
            let commit = verified_commit.value().clone();
            let publication = coven_database::AcceptedStoreCommitPublication::from_verified(
                remote_publication
                    .interval
                    .accepted_commit(verified_commit)
                    .map_err(StorePullError::Protocol)?,
            );
            if commit_ref.coord.sequence() != commit.seq() {
                held.push(HeldStorePosition::commit(
                    commit_ref,
                    HeldStorePositionReason::InvalidObject(
                        "exact commit coordinate differs from signed sequence".to_string(),
                    ),
                ));
                continue;
            }
            let stream_id = commit_stream_id(&commit_ref.coord);
            if let Some(materialized) = timings
                .stage(
                    "check materialized",
                    self.history
                        .exact_materialized_ref(&stream_id, commit_ref.coord.sequence()),
                )
                .await?
            {
                if materialized == *commit_ref {
                    continue;
                }
                held.push(HeldStorePosition::commit(
                    commit_ref,
                    HeldStorePositionReason::HashMismatch {
                        referenced_device_id: stream_id,
                        referenced_commit: commit_ref.clone(),
                        materialized_hash: materialized.commit_hash,
                    },
                ));
                continue;
            }
            if let Some(package) = commit.store_package() {
                if package.schema_version > self.history.schema_version() {
                    held.push(HeldStorePosition::commit(
                        commit_ref,
                        HeldStorePositionReason::NewerSchema {
                            local: self.history.schema_version(),
                            required: package.schema_version,
                        },
                    ));
                    continue;
                }
            }
            pending.push((publication, commit_ref.clone(), commit));
        }
        // A commit names its own package, so nothing orders these reads against
        // each other — only applying the commits is ordered, and it stays so.
        // Left inside the ordered pass, catching up on ten commits cost ten
        // round trips one after another; issued together here, that pass finds
        // each one's bytes already in hand.
        timings
            .stage(
                "prefetch packages",
                self.history.prefetch_store_packages(
                    pending
                        .iter()
                        .map(|(_, commit_ref, commit)| (commit_ref, commit)),
                ),
            )
            .await;
        for (publication, commit_ref, commit) in pending {
            if let Err(error) = timings
                .stage(
                    "verify commits",
                    self.history.verify_refs([commit_ref.clone()]),
                )
                .await
            {
                let reason = match error {
                    StorePullError::Object(error) => held_object_error(error),
                    error => HeldStorePositionReason::InvalidObjectPull(error.into()),
                };
                held.push(HeldStorePosition::commit(&commit_ref, reason));
                continue;
            }
            let verified = self.history.verified_commit(&commit_ref).ok_or_else(|| {
                StorePullError::InvalidState(
                    "Merge candidate is absent from its operation-verified history".to_string(),
                )
            })?;
            if verified.verified.value() != &commit {
                held.push(HeldStorePosition::commit(
                    &commit_ref,
                    HeldStorePositionReason::InvalidObject(
                        "Merge candidate differs from its operation-verified history".to_string(),
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
                .verified_membership_prefix(commit_predecessor_references(&commit))?;
            let verified_commit = verified.verified.clone();
            let package = match timings
                .stage(
                    "load packages",
                    self.history.load_store_package(verified_commit.reference()),
                )
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
                    publication,
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
        let coverage = timings
            .stage("load frontier", self.history.snapshot_coverage())
            .await
            .map_err(|error| {
                StorePullError::Database(coven_database::DbError::context(
                    "load snapshot coverage frontier",
                    error,
                ))
            })?;
        let mut frontier = timings
            .stage("load frontier", self.history.materialized_frontier())
            .await
            .map_err(|error| {
                StorePullError::Database(coven_database::DbError::context(
                    "load materialized frontier",
                    error,
                ))
            })?;
        let mut row_changes = Vec::new();
        let changesets_applied;
        let mut blocked = BTreeMap::new();
        let mut latest_membership = membership.clone();
        let initial_membership = latest_membership.clone();
        let receiver_wall_ms = self.history.receive_wall_ms();
        let mut prepared_materializations = Vec::new();
        let mut prepared_references = Vec::new();
        let mut verified_prefix = VerifiedStreamActivationPrefix::empty();

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
                if let Some(missing) = missing_snapshot_predecessor(
                    &remote_publication.interval,
                    &candidate.publication,
                    &CommitFrontier::from_refs(frontier.clone())?,
                ) {
                    let stream = commit_stream_id(&missing.coord);
                    blocked.insert(
                        key,
                        HeldStorePosition::dependency(
                            candidate.candidate.commit_ref(),
                            &stream,
                            &missing,
                            HeldStorePositionReason::MissingDependency {
                                device_id: stream.clone(),
                                commit: missing.clone(),
                            },
                        ),
                    );
                    continue;
                }
                match timings
                    .stage(
                        "readiness",
                        self.history.readiness(
                            &coverage,
                            &frontier,
                            candidate.candidate.commit_ref(),
                            candidate.candidate.commit(),
                        ),
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
                        match Box::pin(self.prepare_candidate(
                            &candidate,
                            &prepared_materializations,
                            &verified_prefix,
                            &mut latest_membership,
                            routing_key.as_ref(),
                            receiver_wall_ms,
                            timings,
                        ))
                        .await?
                        {
                            Ok(prepared) => {
                                verified_prefix
                                    .include(prepared.circle_activations.stream_activations())?;
                                let stream_id =
                                    commit_stream_id(&candidate.candidate.commit_ref().coord);
                                frontier.insert(
                                    stream_id.clone(),
                                    candidate.candidate.commit_ref().clone(),
                                );
                                prepared_references.push(candidate.candidate.commit_ref().clone());
                                prepared_materializations.push(prepared);
                                blocked.remove(&key);
                                progressed = true;
                            }
                            Err(reason) => {
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
        {
            let local_store_membership =
                LocalStoreMembership::from_membership(&latest_membership, self.identity);
            match timings
                .stage(
                    "materialize accepted interval",
                    self.history.commit_publication_interval(
                        prepared_materializations,
                        accepted_interval,
                        remote_publication.interval,
                        remote_publication
                            .accepted_snapshots
                            .into_iter()
                            .map(|selected| selected.verified)
                            .collect(),
                        local_store_membership,
                        routing_encryption.cloned(),
                        routing_key.clone(),
                        receiver_wall_ms,
                    ),
                )
                .await?
            {
                (coven_database::MaterializationOutcome::Applied(changes), installed) => {
                    row_changes = changes;
                    changesets_applied = u64::try_from(installed.len()).map_err(|_| {
                        StorePullError::InvalidState("Store apply count exceeded u64".to_string())
                    })?;
                }
                (coven_database::MaterializationOutcome::Held(reason), _) => {
                    let reference = prepared_references.first().ok_or_else(|| {
                        StorePullError::InvalidState(
                            "empty Store publication interval was held during installation"
                                .to_string(),
                        )
                    })?;
                    held.push(HeldStorePosition::commit(
                        reference,
                        materialization_hold_reason(reason),
                    ));
                    changesets_applied = 0;
                    latest_membership = initial_membership;
                }
            }
        }
        held.sort_by(|left, right| {
            (left.coordinate.device_id(), left.coordinate.seq())
                .cmp(&(right.coordinate.device_id(), right.coordinate.seq()))
        });
        let local_blob_cleanup_pending = timings
            .stage(
                "drain local blob cleanup",
                self.history.drain_local_blob_cleanup(),
            )
            .await
            .map_err(|error| {
                StorePullError::Database(coven_database::DbError::context(
                    "drain local blob cleanup intents",
                    error,
                ))
            })?;
        Ok(StorePullExecution {
            result: StorePullResult {
                changesets_applied,
                held_positions: held,
                visible_commits,
                row_changes,
                local_blob_cleanup_pending,
                #[cfg(any(test, feature = "test-utils"))]
                frontier: self
                    .history
                    .materialized_frontier()
                    .await
                    .map_err(|error| {
                        StorePullError::Database(coven_database::DbError::context(
                            "read installed frontier after pull",
                            error,
                        ))
                    })?,
            },
            membership: latest_membership,
        })
    }

    async fn prepare_candidate(
        &mut self,
        merge_candidate: &MergeCandidate,
        prepared: &[PreparedMergeMaterialization],
        verified_prefix: &VerifiedStreamActivationPrefix,
        latest_membership: &mut MembershipChain,
        routing_key: Option<&super::circle::RowRoutingKey>,
        receiver_wall_ms: u64,
        timings: &mut StageTimings,
    ) -> Result<Result<PreparedMergeMaterialization, HeldStorePositionReason>, StorePullError> {
        let candidate = &merge_candidate.candidate;
        let commit = candidate.commit();
        let commit_ref = candidate.commit_ref();
        let author = candidate.author();
        let device_operations = merge_candidate.device_operations.clone();
        if !commit.device_exclusion_proposals().is_empty()
            || !commit.device_exclusion_outcomes().is_empty()
        {
            let predecessor_state = self.history.verified_predecessor_state(commit)?;
            let predecessor_cut = commit
                .order
                .predecessor_cut()
                .map_err(StorePullError::Protocol)?;
            let state_ref = StoreDeviceStateRef::from_resolved(
                CommitFrontier(predecessor_cut.0),
                &predecessor_state,
            )
            .map_err(StorePullError::Protocol)?;
            if state_ref != commit.device_state {
                return Err(StorePullError::InvalidState(
                    "Merge exclusion commit differs from its materialized predecessor device state"
                        .to_string(),
                ));
            }
        }
        let membership_objects = self
            .history
            .verified_membership_objects(commit_ref, commit)
            .await?;
        let (local_store_membership, membership_after_candidate) = self
            .local_store_membership_after_candidate(
                latest_membership,
                &merge_candidate.predecessor_membership,
                membership_objects.as_ref(),
            )?;
        let mut available_circles = prepared
            .iter()
            .map(|prepared| &prepared.circle_activations)
            .collect::<Vec<_>>();
        let circle_activations = if commit.control().is_some() {
            merge_candidate.membership_control.clone().ok_or_else(|| {
                super::CirclePackageReadError::Invalid(
                    "Merge membership control is absent from its operation-verified history"
                        .to_string(),
                )
            })
        } else {
            timings
                .stage(
                    "load circle packages",
                    self.history.circles().activations().load_payload(
                        &candidate.verified,
                        self.identity
                            .filter(|_| local_store_membership.allows_circle_access()),
                        routing_key,
                        verified_prefix,
                        &merge_candidate.membership_prefix,
                        &available_circles,
                    ),
                )
                .await
                .map_err(super::CirclePackageReadError::from)
        };
        let verified_circle_activations = match circle_activations {
            Ok(activations) => activations,
            Err(super::CirclePackageReadError::Database(error)) => return Err(error.into()),
            Err(error) => {
                return Ok(Err(HeldStorePositionReason::CirclePackageRead(
                    error.into(),
                )));
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
            self.history.record_circle_close_exclusions(pending).await?;
            return Ok(Err(HeldStorePositionReason::InvalidObject(
                "excluded device awaiting its successor bootstrap to reset".to_string(),
            )));
        }
        available_circles.push(&verified_circle_activations);
        let circle_packages = match timings
            .stage(
                "load circle packages",
                self.history.circles().packages().load_applicable(
                    &candidate.verified,
                    &available_circles,
                    &[],
                    author,
                    local_store_membership,
                ),
            )
            .await
        {
            Ok(packages) => packages,
            Err(super::CirclePackageReadError::Database(error)) => return Err(error.into()),
            Err(error) => {
                return Ok(Err(HeldStorePositionReason::CirclePackageRead(
                    error.into(),
                )));
            }
        };
        let mut packages =
            Vec::with_capacity(usize::from(candidate.package.is_some()) + circle_packages.len());
        if let Some(bytes) = candidate.package.as_ref() {
            let package = match candidate.parse_store_package(bytes) {
                Ok(package) => package,
                Err(reason) => return Ok(Err(reason)),
            };
            match timings
                .stage(
                    "prepare package",
                    self.history
                        .prepare_package(package, self.package_schema.clone()),
                )
                .await?
            {
                Ok(package) => packages.push(package),
                Err(reason) => return Ok(Err(reason)),
            }
        }
        for loaded in &circle_packages {
            let package = match candidate.parse_circle_package(loaded) {
                Ok(package) => package,
                Err(reason) => return Ok(Err(reason)),
            };
            match timings
                .stage(
                    "prepare package",
                    self.history
                        .prepare_package(package, self.package_schema.clone()),
                )
                .await?
            {
                Ok(package) => packages.push(package),
                Err(reason) => return Ok(Err(reason)),
            }
        }
        let materialization = self
            .prepare_materialization(
                merge_candidate,
                packages,
                device_operations,
                verified_circle_activations,
                membership_objects,
                receiver_wall_ms,
            )
            .await?;
        *latest_membership = membership_after_candidate;
        Ok(Ok(materialization))
    }

    fn local_store_membership_after_candidate(
        &self,
        latest: &MembershipChain,
        predecessor: &MembershipChain,
        membership_objects: Option<&VerifiedMergeMembershipClosure>,
    ) -> Result<(LocalStoreMembership, MembershipChain), StorePullError> {
        let candidate = if let Some(membership_objects) = membership_objects {
            let proof = &membership_objects.proof;
            let mut successor = predecessor.clone();
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
        let candidate_state = LocalStoreMembership::from_membership(&candidate, self.identity);
        if candidate.causally_includes(latest) {
            return Ok((candidate_state, candidate));
        }
        if latest.causally_includes(&candidate) {
            let latest_state = LocalStoreMembership::from_membership(latest, self.identity);
            return Ok((
                historical_local_store_membership(latest_state, candidate_state),
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

    async fn prepare_materialization(
        &mut self,
        merge_candidate: &MergeCandidate,
        packages: Vec<PreparedMergeMaterializationPackage>,
        device_operations: VerifiedStoreDeviceOperations,
        verified_circle_activations: VerifiedCircleActivations,
        membership: Option<VerifiedMergeMembershipClosure>,
        receiver_wall_ms: u64,
    ) -> Result<PreparedMergeMaterialization, StorePullError> {
        let root = self.history.root().clone();
        let candidate = &merge_candidate.candidate;
        let commit = candidate.commit();
        let predecessor_membership = &merge_candidate.predecessor_membership;
        let predecessor_state = self.history.verified_predecessor_state(commit)?;
        verify_merge_membership_state_ref(
            &commit.membership_state,
            predecessor_membership,
            &predecessor_state,
        )?;
        let (authorized_predecessor, recovery_author) = predecessor_state
            .clone()
            .preactivate_recovery_author(commit, &candidate.registrations)
            .map_err(StorePullError::Protocol)?;
        let owner_recovery = self
            .history
            .verify_owner_recovery_activation(commit)
            .await?;
        let state_after = device_operations
            .apply_to(authorized_predecessor.clone())
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
            .retain_acknowledgement(&candidate.verified)
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
                &predecessor_state,
                &state_after,
                MergeHistorySuccessorEvidence {
                    registrations,
                    acknowledgement: retained_acknowledgement,
                    membership_proof: membership.as_ref().map(|closure| closure.proof.clone()),
                },
            )
            .await?;
        self.history.remember_commit(candidate.verified.clone())?;
        Ok(PreparedMergeMaterialization {
            root: root.clone(),
            verified_commit: candidate.verified.clone(),
            acceptance: merge_candidate.publication.clone().into(),
            history_evidence: prepared_history.history_evidence,
            membership_objects: membership.as_ref().map(|closure| closure.objects().clone()),
            membership_remote_objects: membership
                .map(VerifiedMergeMembershipClosure::into_remote_objects)
                .unwrap_or_default(),
            registrations: candidate.registrations.clone(),
            package_application: (!packages.is_empty()).then_some(
                coven_database::RetainedPackageApplication::Received { receiver_wall_ms },
            ),
            packages,
            device_operations,
            circle_activations: verified_circle_activations,
        })
    }
}

fn missing_snapshot_predecessor(
    interval: &store_commit::VerifiedStorePublicationInterval,
    candidate: &coven_database::AcceptedStoreCommitPublication,
    ready: &CommitFrontier,
) -> Option<StoreBatchCommitRef> {
    let mut missing = None;
    for entry in interval.entries() {
        if entry.reference().position >= candidate.reference().position {
            break;
        }
        match &entry.entry().payload {
            store_commit::StorePublicationPayload::Commit(reference) => {
                if missing.is_none() && !ready.covers_commit(reference) {
                    missing = Some(reference.clone());
                }
            }
            store_commit::StorePublicationPayload::Snapshot(_) if missing.is_some() => {
                return missing
            }
            store_commit::StorePublicationPayload::Snapshot(_) => {}
        }
    }
    None
}

fn materialization_hold_reason(
    hold: coven_database::MaterializationHold,
) -> HeldStorePositionReason {
    match hold {
        coven_database::MaterializationHold::ForeignKeyDependency => {
            HeldStorePositionReason::ForeignKeyDependency
        }
        coven_database::MaterializationHold::ConstraintConflict(tables) => {
            HeldStorePositionReason::ConstraintConflict(tables)
        }
        coven_database::MaterializationHold::PrivateSharedConflict {
            table,
            row_id,
            commit,
        } => HeldStorePositionReason::PrivateSharedConflict {
            table,
            row_id,
            commit,
        },
    }
}

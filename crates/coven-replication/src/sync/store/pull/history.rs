use super::*;
use crate::sync::store::blob::{RemoteBlobSource, StoreBlobCache};
use crate::sync::store::merge_conflict;
use coven_database::{PreparedMergeMaterialization, PreparedMergeMaterializationPackage};
use coven_protocol::membership::MembershipStatus;
use coven_protocol::store_commit::{StoreDeviceStatus, StreamActivation, StreamAnchorDomain};
use std::collections::BTreeMap;

/// The reads, verifications, and materializations a pull performs, over the
/// five capabilities they need.
pub(crate) struct PullHistory<'operation, 'storage> {
    database: StoreDatabase,
    storage: &'storage dyn CloudSyncObjectStorage,
    history: &'operation mut MergeHistoryVerifier<'storage>,
    blob_source: &'operation RemoteBlobSource<'storage>,
    blob_cache: &'operation StoreBlobCache,
}

impl<'operation, 'storage> PullHistory<'operation, 'storage> {
    pub(crate) fn new(
        database: StoreDatabase,
        storage: &'storage dyn CloudSyncObjectStorage,
        history: &'operation mut MergeHistoryVerifier<'storage>,
        blob_source: &'operation RemoteBlobSource<'storage>,
        blob_cache: &'operation StoreBlobCache,
    ) -> Self {
        Self {
            database,
            storage,
            history,
            blob_source,
            blob_cache,
        }
    }

    pub(crate) fn circles(
        &mut self,
    ) -> crate::sync::store::circles::VerifiedCircleHistory<'_, 'storage> {
        crate::sync::store::circles::VerifiedCircleHistory::new(
            self.database.clone(),
            self.storage,
            self.history,
        )
    }

    pub(crate) fn root(&self) -> &StoreRootRef {
        self.history.verified_root().reference()
    }

    pub(crate) async fn package_schema(
        &self,
    ) -> Result<std::sync::Arc<coven_database::TableSchema>, coven_database::DbError> {
        Ok(std::sync::Arc::new(
            self.database.table_schema_for_apply().await?,
        ))
    }

    pub(crate) async fn drain_local_blob_cleanup(&self) -> Result<bool, coven_database::DbError> {
        self.blob_cache.drain_local_cleanup().await
    }

    pub(crate) async fn prepare_package(
        &self,
        package: coven_protocol::audience_package::AudiencePackage,
        schema: std::sync::Arc<coven_database::TableSchema>,
    ) -> Result<Result<PreparedMergeMaterializationPackage, HeldStorePositionReason>, StorePullError>
    {
        let changeset =
            match coven_database::ValidatedChangeset::new(package.changeset().to_vec(), schema) {
                Ok(changeset) => changeset,
                Err(coven_database::ChangesetIdentityError::Row(error)) => {
                    return Ok(Err(HeldStorePositionReason::InvalidRowIdentity(
                        error.into(),
                    )))
                }
                Err(error) => {
                    return Ok(Err(HeldStorePositionReason::InvalidChangesetIdentity(
                        error.into(),
                    )))
                }
            };
        let changes = match coven_database::walk_changeset(changeset.bytes()) {
            Ok(changes) => changes,
            Err(error) => {
                return Ok(Err(HeldStorePositionReason::ChangesetUnreadable(
                    error.into(),
                )))
            }
        };
        let old_changes = match coven_database::walk_old_changeset(changeset.bytes()) {
            Ok(changes) => changes,
            Err(error) => {
                return Ok(Err(HeldStorePositionReason::ChangesetUnreadable(
                    error.into(),
                )))
            }
        };
        let mut eager = Vec::new();
        for change in &changes {
            if change.op == coven_foundation::changeset::ChangeOp::Delete {
                continue;
            }
            let blob = match self.database.blob_ref_from_change(change) {
                Ok(blob) => blob,
                Err(error) => {
                    return Ok(Err(HeldStorePositionReason::InvalidChangesetBlobDecl(
                        error.into(),
                    )))
                }
            };
            let Some(blob) = blob else {
                continue;
            };
            if blob.fill != coven_protocol::blob::CacheFill::CacheEager {
                continue;
            }
            let row_id = match change.pk() {
                Some(row_id) => row_id,
                None => {
                    return Ok(Err(HeldStorePositionReason::InvalidChangeset(format!(
                        "blob-bearing incoming row {:?} has no primary key",
                        change.table
                    ))))
                }
            };
            let matches = package
                .blob_bindings()
                .iter()
                .filter(|binding| {
                    binding.table() == change.table
                        && binding.row_id() == row_id
                        && binding.blob().locator().namespace() == blob.namespace
                        && binding.blob().locator().blob_id() == blob.id
                })
                .collect::<Vec<_>>();
            let [binding] = matches.as_slice() else {
                return Ok(Err(HeldStorePositionReason::InvalidChangeset(format!(
                    "incoming eager blob row {:?}/{row_id:?} has {} exact locator bindings",
                    change.table,
                    matches.len()
                ))));
            };
            eager.push(binding.blob().clone());
        }
        let mut verified = Vec::new();
        let mut failures = Vec::new();
        let blob_authority =
            coven_protocol::blob::RowBlobAuthority::Remote(package.audience().clone());
        for binding in package.blob_bindings() {
            let stored = binding.blob();
            if verified.iter().any(|candidate| candidate == stored) {
                continue;
            }
            verified.push(stored.clone());
            let locator = stored.locator();
            let retain = eager.iter().any(|download| download == stored);
            if let Err(cause) = self
                .blob_source
                .verify_plaintext(
                    self.blob_cache,
                    &blob_authority,
                    stored,
                    retain,
                    coven_storage::cloud::no_download_progress(),
                )
                .await
            {
                failures.push(BlobDownloadFailure {
                    namespace: locator.namespace().to_string(),
                    id: locator.blob_id().to_string(),
                    cause,
                });
            }
        }
        if !failures.is_empty() {
            let failures = BlobDownloadFailures::new(failures);
            if failures.has_transport_failure() {
                return Err(StorePullError::BlobDownloads(failures));
            }
            return Ok(Err(HeldStorePositionReason::BlobDownloadFailed));
        }
        if let Err(error) = self
            .database
            .validate_local_blob_cleanup_changes(&old_changes, &changes)
        {
            return Ok(Err(HeldStorePositionReason::InvalidChangesetBlobDecl(
                error.into(),
            )));
        }
        Ok(Ok(PreparedMergeMaterializationPackage {
            package,
            changeset,
        }))
    }

    pub(crate) fn has_scoped_graph(&self) -> bool {
        self.database.has_scoped_graph()
    }

    pub(crate) fn schema_version(&self) -> u32 {
        self.database.schema_version()
    }

    pub(crate) async fn unrepresented_device_join_bootstrap_commits(
        &self,
        plan: coven_database::DeviceJoinBootstrapPlan,
    ) -> Result<
        (
            coven_database::DeviceJoinBootstrapPlan,
            Vec<StoreBatchCommitRef>,
        ),
        coven_database::DbError,
    > {
        self.database
            .unrepresented_device_join_bootstrap_commits(plan)
            .await
    }

    pub(crate) fn receive_wall_ms(&self) -> u64 {
        self.database.receive_wall_ms()
    }

    pub(crate) async fn materialized_frontier(
        &self,
    ) -> Result<std::collections::BTreeMap<String, StoreBatchCommitRef>, coven_database::DbError>
    {
        self.database.materialized_frontier().await
    }

    pub(crate) async fn device_state_for_cut(
        &self,
        cut: &StoreHistoryCut,
    ) -> Result<(StoreDeviceStateRef, ResolvedStoreDeviceState), coven_database::DbError> {
        self.database.store_device_state_for_history_cut(cut).await
    }

    pub(crate) async fn device_state_for_order(
        &self,
        order: &coven_protocol::store_commit::StoreCommitOrder,
    ) -> Result<(StoreDeviceStateRef, ResolvedStoreDeviceState), coven_database::DbError> {
        self.database.store_device_state_for_order(order).await
    }

    pub(crate) async fn exact_materialized_ref(
        &self,
        stream_id: &str,
        sequence: u64,
    ) -> Result<Option<StoreBatchCommitRef>, coven_database::DbError> {
        self.database
            .exact_materialized_ref(stream_id, sequence)
            .await
    }

    pub(crate) async fn snapshot_coverage(
        &self,
    ) -> Result<CommitFrontier, coven_database::DbError> {
        self.database.snapshot_coverage_frontier().await
    }

    pub(crate) async fn exclusion_freezes(
        &self,
    ) -> Result<Vec<coven_protocol::store_commit::StoreDeviceProposalAck>, coven_database::DbError>
    {
        self.database.store_device_exclusion_freezes().await
    }

    pub(crate) async fn record_circle_close_exclusions(
        &self,
        exclusions: Vec<coven_protocol::circle_activation::LocalCircleExclusion>,
    ) -> Result<(), coven_database::DbError> {
        self.database
            .record_circle_close_exclusions(exclusions)
            .await
    }

    pub(crate) async fn commit_materialization(
        &self,
        materialization: PreparedMergeMaterialization,
        retractions: Vec<coven_protocol::remote_object::VerifiedCandidateNonactivation>,
        local_store_membership: LocalStoreMembership,
        routing_key: Option<coven_protocol::circle::RowRoutingKey>,
        receiver_wall_ms: u64,
    ) -> Result<coven_database::MaterializationOutcome, coven_database::DbError> {
        self.database
            .apply_received_merge_materialization(
                materialization,
                retractions,
                local_store_membership,
                routing_key,
                receiver_wall_ms,
            )
            .await
    }

    pub(crate) async fn prepare_merge_history_successor(
        &self,
        verified_commit: &VerifiedStoreBatchCommit,
        membership: &MembershipChain,
        recovery_author: Option<&coven_protocol::store_commit::StoreDeviceRegistrationRef>,
        state_after: ResolvedStoreDeviceState,
        evidence: MergeHistorySuccessorEvidence,
    ) -> Result<PreparedMergeHistorySuccessor, StorePullError> {
        crate::sync::store::authorization::history::retained::prepare_merge_history_successor(
            &self.database,
            self.history,
            verified_commit,
            membership,
            recovery_author,
            state_after,
            evidence,
        )
        .await
    }

    /// The retained history this pull replays, and the verified commit graph it
    /// verifies new candidates against.
    ///
    /// The durable rows come first and the verification runs over them: the
    /// database opens each retained materialization from its own canonical
    /// bytes, re-parsing and signature-checking the commit against its activated
    /// registration, and those verified values seed the history verifier's reuse
    /// memos. `verify_refs` then runs exactly as it always has, reaching the
    /// provider only for what those memos do not already cover.
    ///
    /// Ordering it the other way — verify from the provider first, then hand the
    /// proofs to the database — is what made every cycle re-read the whole
    /// retained history: it made the durable authority depend on a fresh remote
    /// verification instead of being that authority.
    pub(crate) async fn prepare_retained_history(
        &mut self,
    ) -> Result<Vec<coven_database::OwnedVerifiedMergeMaterialization>, StorePullError> {
        let retained = self
            .database
            .retained_merge_replay_inputs(self.history.verified_root().reference().clone())
            .await?;
        self.history.admit_retained_history(&retained)?;
        self.history
            .verify_refs(
                retained
                    .iter()
                    .map(|materialization| materialization.commit_ref().clone())
                    .collect::<Vec<_>>(),
            )
            .await?;
        self.resume_merge_retraction_cleanups().await?;
        Ok(retained)
    }

    /// Retire the terminal nonactivations a retracted Merge candidate left
    /// behind, then delete the objects it staged.
    pub(crate) async fn resume_merge_retraction_cleanups(&mut self) -> Result<(), StorePullError> {
        for candidate in self.database.pending_merge_retraction_cleanups().await? {
            let root = self.history.verified_root().reference().clone();
            let verification = self
                .database
                .merge_retraction_cleanup_verification(root, candidate.clone())
                .await?;
            merge_conflict::MergeConflictHistory::new(&self.database, self.storage, self.history)
                .apply_terminal_nonactivation(
                    merge_conflict::TerminalNonactivationCandidate::MergeRetraction {
                        reference: candidate.clone(),
                        verification,
                    },
                )
                .await?;
            let targets = self
                .database
                .merge_retraction_cleanup_targets(candidate.clone())
                .await?;
            crate::sync::store::authorization::delete_candidate_cleanup_targets::<StorePullError>(
                self.storage,
                &self.database,
                targets,
            )
            .await?;
            self.database
                .finish_merge_retraction_cleanup(candidate)
                .await?;
        }
        Ok(())
    }

    pub(crate) async fn load_active_registrations(
        &self,
    ) -> Result<Vec<coven_protocol::store_commit::ReferencedStoreDeviceRegistration>, StorePullError>
    {
        let durable = self
            .database
            .activated_store_device_registration_records()
            .await?;
        let mut verified = Vec::with_capacity(durable.len());
        for expected in durable {
            let reference = expected.reference();
            let opened = self.history.load_registration(reference).await?;
            if &opened.value != expected.value() {
                return Err(StorePullError::InvalidState(format!(
                    "activated Store registration {} differs from its exact remote bytes",
                    reference.device_id
                )));
            }
            if !matches!(
                opened.value.store_commits,
                coven_protocol::store_commit::DeviceStreamAnchor::StoreAnnouncements { .. }
            ) {
                return Err(StorePullError::InvalidState(format!(
                    "activated Store registration {} has no Merge announcement anchor",
                    reference.device_id
                )));
            }
            verified.push(expected);
        }
        Ok(verified)
    }

    pub(crate) async fn discover_owner_recoveries(
        &self,
        membership: &MembershipChain,
    ) -> Result<Vec<coven_protocol::store_commit::ReferencedStoreDeviceRegistration>, StorePullError>
    {
        self.history.discover_owner_recoveries(membership).await
    }

    pub(crate) async fn discover_stream(
        &mut self,
        registration_ref: &coven_protocol::store_commit::StoreDeviceRegistrationRef,
        registration: &StoreDeviceRegistration,
        inactive_accepted_cut: Option<&StoreHistoryCut>,
    ) -> Result<MergeStreamDiscovery, StorePullError> {
        self.history
            .discover_merge_stream(registration_ref, registration, inactive_accepted_cut)
            .await
    }

    pub(crate) async fn verify_refs(
        &mut self,
        references: impl IntoIterator<Item = StoreBatchCommitRef>,
    ) -> Result<(), StorePullError> {
        self.history.verify_refs(references).await
    }

    pub(crate) fn verified_commit(
        &self,
        reference: &StoreBatchCommitRef,
    ) -> Option<VerifiedPullCandidate> {
        self.history.verified_pull_candidate(reference)
    }

    pub(crate) fn verified_predecessor_membership(
        &self,
        reference: &StoreBatchCommitRef,
    ) -> Option<MembershipChain> {
        self.history
            .verified_predecessor_membership(reference)
            .cloned()
    }

    pub(crate) fn verified_membership_prefix(
        &self,
        predecessors: impl IntoIterator<Item = StoreBatchCommitRef>,
    ) -> Result<VerifiedMergeMembershipPrefix, StorePullError> {
        self.history.verified_membership_prefix(predecessors)
    }

    pub(crate) async fn load_store_package(
        &mut self,
        reference: &StoreBatchCommitRef,
    ) -> Result<Option<coven_protocol::objects::VerifiedObject<Vec<u8>>>, StoreObjectError> {
        self.history.load_store_package(reference).await
    }

    pub(crate) async fn materialized_reference_status(
        &mut self,
        coverage: &CommitFrontier,
        stream_id: &str,
        reference: &StoreBatchCommitRef,
    ) -> Result<MaterializedCheck, StorePullError> {
        materialized_reference_status(&self.database, self.history, coverage, stream_id, reference)
            .await
    }

    pub(crate) async fn readiness(
        &mut self,
        coverage: &CommitFrontier,
        frontier: &std::collections::BTreeMap<String, StoreBatchCommitRef>,
        device_state: &ResolvedStoreDeviceState,
        exclusion_freezes: &[coven_protocol::store_commit::StoreDeviceProposalAck],
        commit_ref: &StoreBatchCommitRef,
        commit: &StoreBatchCommit,
    ) -> Result<Readiness, StorePullError> {
        let stream_id = commit_stream_id(&commit_ref.coord);
        if let Some(current) = frontier.get(&stream_id) {
            if commit_ref.coord.sequence() <= current.coord.sequence() {
                match self
                    .materialized_reference_status(coverage, &stream_id, commit_ref)
                    .await?
                {
                    MaterializedCheck::Yes => return Ok(Readiness::AlreadyMaterialized),
                    MaterializedCheck::Missing => {
                        return Ok(Readiness::Held(HeldStorePosition::commit(
                            commit_ref,
                            HeldStorePositionReason::MissingCommit,
                        )))
                    }
                    MaterializedCheck::Held(reason) => {
                        return Ok(Readiness::Held(HeldStorePosition::commit(
                            commit_ref, reason,
                        )))
                    }
                }
            }
            if commit.order.predecessor() != Some(current) {
                let reason = match commit.order.predecessor() {
                    Some(missing) => HeldStorePositionReason::MissingPredecessor(missing.clone()),
                    None => HeldStorePositionReason::InvalidObject(
                        "non-genesis Merge commit omits its exact predecessor".to_string(),
                    ),
                };
                return Ok(Readiness::Held(HeldStorePosition::commit(
                    commit_ref, reason,
                )));
            }
            if commit_ref.coord.sequence() != current.coord.sequence() + 1 {
                return Ok(Readiness::Held(HeldStorePosition::commit(
                    commit_ref,
                    HeldStorePositionReason::InvalidObject(
                        "Merge commit sequence does not immediately follow its materialized frontier"
                            .to_string(),
                    ),
                )));
            }
        } else if commit_ref.coord.sequence() != 1 || commit.order.predecessor().is_some() {
            let reason = match commit.order.predecessor() {
                Some(missing) => HeldStorePositionReason::MissingPredecessor(missing.clone()),
                None => HeldStorePositionReason::InvalidObject(
                    "Merge commit beyond genesis omits its exact predecessor".to_string(),
                ),
            };
            return Ok(Readiness::Held(HeldStorePosition::commit(
                commit_ref, reason,
            )));
        }

        for record in device_state.devices.values() {
            let target_stream = StreamActivation::device_authorized_stream_id(
                self.history.verified_root().reference().store_root_hash,
                &record.registration,
                StreamAnchorDomain::StoreAnnouncements,
            );
            if target_stream.to_string() != stream_id {
                continue;
            }
            let StoreDeviceStatus::Inactive {
                terminals,
                accepted_cut,
            } = &record.status
            else {
                break;
            };
            let target_cut = accepted_cut.commits();
            let terminal_sequence = match target_cut.get(&target_stream) {
                Some(reference) => reference.coord.sequence(),
                None => 0,
            };
            if commit_ref.coord.sequence() > terminal_sequence {
                return Ok(Readiness::Held(HeldStorePosition::commit(
                    commit_ref,
                    HeldStorePositionReason::InactiveDevice {
                        terminals: terminals.clone(),
                        accepted_cut: accepted_cut.clone(),
                    },
                )));
            }
            break;
        }

        for freeze in exclusion_freezes {
            let target_stream = StreamActivation::device_authorized_stream_id(
                self.history.verified_root().reference().store_root_hash,
                &freeze.proposal.target,
                StreamAnchorDomain::StoreAnnouncements,
            );
            if target_stream.to_string() != stream_id {
                continue;
            }
            let target_cut = freeze.target_cut.commits();
            let frozen_sequence = match target_cut.get(&target_stream) {
                Some(reference) => reference.coord.sequence(),
                None => 0,
            };
            if commit_ref.coord.sequence() > frozen_sequence {
                return Ok(Readiness::Held(HeldStorePosition::commit(
                    commit_ref,
                    HeldStorePositionReason::DeviceExclusionFreeze {
                        proposal: freeze.proposal.clone(),
                        target_cut: freeze.target_cut.clone(),
                    },
                )));
            }
        }

        for (required_stream, required_ref) in commit.merge_dependencies() {
            let required_stream = required_stream.to_string();
            match self
                .materialized_reference_status(coverage, &required_stream, required_ref)
                .await?
            {
                MaterializedCheck::Yes => {}
                MaterializedCheck::Missing => {
                    return Ok(Readiness::Held(HeldStorePosition::dependency(
                        commit_ref,
                        &required_stream,
                        required_ref,
                        HeldStorePositionReason::MissingDependency {
                            device_id: required_stream.clone(),
                            commit: required_ref.clone(),
                        },
                    )))
                }
                MaterializedCheck::Held(reason) => {
                    return Ok(Readiness::Held(HeldStorePosition::dependency(
                        commit_ref,
                        &required_stream,
                        required_ref,
                        reason,
                    )))
                }
            }
        }
        Ok(Readiness::Ready)
    }

    pub(crate) async fn verified_membership_objects(
        &mut self,
        commit_ref: &StoreBatchCommitRef,
        commit: &StoreBatchCommit,
    ) -> Result<Option<VerifiedMergeMembershipClosure>, StorePullError> {
        self.history
            .verified_membership_objects(commit_ref, commit)
            .await
    }

    pub(crate) async fn verify_owner_recovery_activation(
        &self,
        commit: &StoreBatchCommit,
    ) -> Result<
        Option<(
            coven_protocol::membership::MembershipGrantId,
            coven_protocol::store_commit::OwnerRecoveryActivationId,
        )>,
        StorePullError,
    > {
        self.history.verify_owner_recovery_activation(commit).await
    }

    pub(crate) async fn retain_acknowledgement(
        &self,
        commit_ref: &StoreBatchCommitRef,
        commit: &StoreBatchCommit,
        author: &StoreDeviceRegistration,
    ) -> Result<Option<coven_protocol::store_commit::RetainedVerifiedActivatedAck>, StorePullError>
    {
        let acknowledgement = self
            .history
            .validate_commit_acknowledgement(commit, author)
            .await
            .map_err(StorePullError::from)?;
        match acknowledgement {
            Some((reference, value)) => self
                .history
                .retain_acknowledgement(commit_ref, commit, author, reference, value)
                .await
                .map(Some),
            None => Ok(None),
        }
    }

    pub(crate) fn remember_commit(
        &mut self,
        commit: VerifiedStoreBatchCommit,
    ) -> Result<(), StorePullError> {
        self.history
            .remember(commit)
            .map_err(StorePullError::Protocol)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn verified_terminal_retractions(
        &mut self,
        activation_head: &StoreDeviceHead,
        activation_head_object: &ExactObjectRef,
        activation_commit: &VerifiedStoreBatchCommit,
        activation_predecessor_state: &ResolvedStoreDeviceState,
        activation_predecessor_membership: &MembershipChain,
        device_operations: &VerifiedStoreDeviceOperations,
        loaded_predecessor_memberships: &LoadedMergePredecessorMemberships,
    ) -> Result<Vec<coven_protocol::remote_object::VerifiedCandidateNonactivation>, StorePullError>
    {
        let root = self.history.verified_root().reference().clone();
        let retained = self
            .database
            .retained_merge_replay_inputs(root.clone())
            .await?;
        let mut verified_retained = BTreeMap::new();
        for materialization in &retained {
            let verified = self
                .history
                .authenticate_bytes(
                    materialization.commit_ref(),
                    &materialization.commit().to_bytes(),
                )
                .await?;
            if verified.value() != materialization.commit() {
                return Err(StorePullError::InvalidState(
                    "retained Merge materialization differs from its authenticated commit"
                        .to_string(),
                ));
            }
            verified_retained.insert(materialization.commit_ref().clone(), verified);
        }
        let activation_commit_ref = activation_commit.reference();
        let activation_commit_value = activation_commit.value();
        let activation_head_ref = coven_protocol::store_commit::StoreDeviceHeadRef {
            head_hash: activation_head.head_hash(),
            object: activation_head_object.clone(),
        };
        let current_membership_ref = &activation_commit_value.membership_state;
        let MembershipStatus::Resolved(current_resolved) =
            activation_predecessor_membership.status()
        else {
            return Err(StorePullError::InvalidState(
                "Merge terminal retraction witness membership is conflicted".to_string(),
            ));
        };
        let mut retractions = Vec::new();
        for materialization in &retained {
            let candidate = verified_retained
                .get(materialization.commit_ref())
                .expect("every retained Merge materialization was authenticated");
            let mut locator = self
                .database
                .author_exclusion_activation_for_candidate(
                    root.clone(),
                    materialization.commit_ref().clone(),
                    candidate.value().author_registration.clone(),
                )
                .await?;
            if locator.is_none() {
                let expected_stream = StreamActivation::device_authorized_stream_id(
                    root.store_root_hash,
                    &candidate.value().author_registration,
                    StreamAnchorDomain::StoreAnnouncements,
                );
                for (exclusion, accepted_cut) in device_operations.exclusions() {
                    if exclusion.proposal.target != candidate.value().author_registration {
                        continue;
                    }
                    let accepted_cut = &accepted_cut.0;
                    let beyond_cutoff =
                        accepted_cut.get(&expected_stream).is_none_or(|reference| {
                            materialization.commit_ref().coord.sequence()
                                > reference.coord.sequence()
                        });
                    if beyond_cutoff {
                        locator = Some(coven_database::AuthorExclusionActivationLocator::verified(
                            exclusion.clone(),
                            accepted_cut.clone(),
                            activation_commit_ref.clone(),
                            activation_head_ref.clone(),
                        ));
                        break;
                    }
                }
            }
            let Some(locator) = locator else {
                let Some(authority) = candidate.value().membership_authority.as_ref() else {
                    continue;
                };
                let predecessor_membership =
                    loaded_predecessor_memberships.membership_for(materialization.commit_ref())?;
                let MembershipStatus::Resolved(predecessor_resolved) =
                    predecessor_membership.status()
                else {
                    return Err(StorePullError::InvalidState(
                        "retained candidate predecessor membership is conflicted".to_string(),
                    ));
                };
                let mut matching = predecessor_resolved
                    .active_grants()
                    .filter(|(_, record)| &record.creation_authority == authority);
                let Some((grant_id, _)) = matching.next() else {
                    return Err(StorePullError::InvalidState(
                        "retained candidate has no exact predecessor grant authority".to_string(),
                    ));
                };
                if matching.next().is_some() {
                    return Err(StorePullError::InvalidState(
                        "retained candidate authority identifies multiple predecessor grants"
                            .to_string(),
                    ));
                }
                if !matches!(
                    current_resolved.grants.get(grant_id),
                    Some(coven_protocol::causal_grants::GrantState::Tombstoned { .. })
                ) {
                    continue;
                }
                let nonactivation = self
                    .history
                    .verify_membership_grant_revocation_nonactivation(
                        grant_id,
                        current_membership_ref,
                        activation_commit_ref,
                        &activation_head_ref,
                        candidate,
                        materialization.activation_head(),
                        materialization.activation_head_object(),
                    )
                    .await?;
                retractions.push(nonactivation);
                continue;
            };
            let nonactivation = self
                .history
                .verify_author_exclusion_nonactivation(
                    &locator,
                    activation_head,
                    activation_head_object,
                    activation_commit,
                    activation_predecessor_state,
                    device_operations,
                    candidate,
                    materialization.activation_head(),
                    materialization.activation_head_object(),
                )
                .await?;
            retractions.push(nonactivation);
        }
        let mut verified_by_reference = retractions
            .into_iter()
            .map(|verified| {
                let reference = verified
                    .candidate_reference()
                    .map_err(StorePullError::RemoteObject)?;
                Ok((reference, verified))
            })
            .collect::<Result<BTreeMap<_, _>, StorePullError>>()?;
        loop {
            let mut additions = Vec::new();
            for materialization in &retained {
                if verified_by_reference.contains_key(materialization.commit_ref()) {
                    continue;
                }
                let candidate = verified_retained
                    .get(materialization.commit_ref())
                    .expect("every retained Merge materialization was authenticated");
                let dependency = commit_predecessor_references(candidate.value())
                    .into_iter()
                    .find_map(|reference| {
                        verified_by_reference
                            .get(&reference)
                            .map(|verified| (reference, verified))
                    });
                let Some((_dependency_reference, dependency)) = dependency else {
                    continue;
                };
                let verified = coven_protocol::remote_object::VerifiedCandidateNonactivation::dependency_retraction(
                    dependency,
                    coven_protocol::store_commit::StoreBatchCommitDeletionTarget {
                        coord: materialization.commit_ref().coord.clone(),
                        object: materialization.commit_ref().object.clone(),
                        canonical_signed_bytes: candidate.value().to_bytes(),
                    },
                    candidate.author(),
                    materialization.activation_head_object().clone(),
                )
                .map_err(StorePullError::RemoteObject)?;
                additions.push((materialization.commit_ref().clone(), verified));
            }
            if additions.is_empty() {
                break;
            }
            for (reference, verified) in additions {
                if verified_by_reference.insert(reference, verified).is_some() {
                    return Err(StorePullError::InvalidState(
                        "transitive Merge retraction constructed duplicate proof".to_string(),
                    ));
                }
            }
        }
        Ok(verified_by_reference.into_values().collect())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn reach_after_remote_commit_test_point(&self, device_id: String, seq: u64) {
        self.database
            .reach_test_point(coven_database::DatabaseTestPoint::PullAfterRemoteCommit {
                device_id,
                seq,
            })
            .await;
    }
}

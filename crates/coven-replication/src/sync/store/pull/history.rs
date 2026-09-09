use super::*;
use crate::sync::store::blob::StoreBlobCache;
use coven_database::PreparedMergeMaterializationPackage;

mod checkpoint;
enum PullDatabase {
    Installed(StoreDatabase),
    Checkpoint {
        receiver: StoreDatabase,
        database: StoreDatabase,
        expected: coven_database::StorePublicationBoundary,
    },
}

/// The reads, verifications, and materializations a pull performs, over the
/// five capabilities they need.
pub(crate) struct PullHistory<'operation, 'storage> {
    database: PullDatabase,
    storage: &'storage dyn CloudSyncObjectStorage,
    history: &'operation mut MergeHistoryVerifier<'storage>,
    blob_cache: &'operation StoreBlobCache,
}

impl<'operation, 'storage> PullHistory<'operation, 'storage> {
    pub(crate) fn new(
        database: StoreDatabase,
        storage: &'storage dyn CloudSyncObjectStorage,
        history: &'operation mut MergeHistoryVerifier<'storage>,
        blob_cache: &'operation StoreBlobCache,
    ) -> Self {
        Self {
            database: PullDatabase::Installed(database),
            storage,
            history,
            blob_cache,
        }
    }

    /// The provider-operation counter of the storage this pull reads through,
    /// so the pull's stage timings can say how many operations each stage cost.
    pub(crate) fn provider_requests(
        &self,
    ) -> Option<std::sync::Arc<dyn coven_foundation::stage_timing::ProviderRequests>> {
        self.storage.provider_requests()
    }

    pub(crate) fn circles(
        &mut self,
    ) -> crate::sync::store::circles::VerifiedCircleHistory<'_, 'storage> {
        let (PullDatabase::Installed(database) | PullDatabase::Checkpoint { database, .. }) =
            &self.database;

        crate::sync::store::circles::VerifiedCircleHistory::new(
            database.clone(),
            self.storage,
            self.history,
        )
    }

    pub(crate) fn root(&self) -> &StoreRootRef {
        self.history.verified_root().reference()
    }

    pub(crate) async fn load_store_publications_for_replay(
        &mut self,
    ) -> Result<
        (
            crate::sync::store::commit_verification::merge_history::VerifiedStorePublication,
            StorePublicationReplayInstallation,
        ),
        StorePullError,
    > {
        let (PullDatabase::Installed(database) | PullDatabase::Checkpoint { database, .. }) =
            &self.database;

        let (publication, installation) = self
            .history
            .load_store_publications_for_replay(database)
            .await?;
        for selected in &publication.accepted_snapshots {
            let snapshot_version = selected.snapshot.meta.schema_version;
            if snapshot_version > database.schema_version() {
                return Err(StorePullError::SnapshotRestoration(Box::new(
                    crate::sync::store::snapshots::SnapshotError::SchemaTooNew {
                        snapshot_version,
                        supported: database.schema_version(),
                    },
                )));
            }
        }
        Ok((publication, installation))
    }

    pub(crate) fn verified_predecessor_state(
        &self,
        commit: &StoreBatchCommit,
    ) -> Result<ResolvedStoreDeviceState, StorePullError> {
        self.history.verified_predecessor_state(commit)
    }

    pub(crate) async fn package_schema(
        &self,
    ) -> Result<std::sync::Arc<coven_database::TableSchema>, coven_database::DbError> {
        let (PullDatabase::Installed(database) | PullDatabase::Checkpoint { database, .. }) =
            &self.database;

        Ok(std::sync::Arc::new(
            database.table_schema_for_apply().await?,
        ))
    }

    pub(crate) async fn drain_local_blob_cleanup(&self) -> Result<bool, coven_database::DbError> {
        self.blob_cache.drain_local_cleanup().await
    }

    /// Read a package's changeset and record what its rows bind.
    ///
    /// Nothing is downloaded here. A binding names its blob's plaintext hash, and
    /// the read that wants the bytes checks them against it, so fetching at pull
    /// time authenticates nothing a read does not authenticate again. What it
    /// cost was the whole library crossing the wire before any of it could be
    /// looked at — a commit binding a large blob held the position until its
    /// bytes arrived, and a blob that was gone held it forever. Rows land now;
    /// the eager cache fills behind them and a lazy blob is fetched when read.
    pub(crate) async fn prepare_package(
        &self,
        package: coven_protocol::audience_package::AudiencePackage,
        schema: std::sync::Arc<coven_database::TableSchema>,
    ) -> Result<Result<PreparedMergeMaterializationPackage, HeldStorePositionReason>, StorePullError>
    {
        let (PullDatabase::Installed(database) | PullDatabase::Checkpoint { database, .. }) =
            &self.database;

        let changeset =
            match coven_database::ValidatedChangeset::new(package.changeset().to_vec(), schema) {
                Ok(changeset) => changeset,
                Err(coven_database::ChangesetIdentityError::Row(error)) => {
                    return Ok(Err(HeldStorePositionReason::InvalidRowIdentity(
                        error.into(),
                    )));
                }
                Err(error) => {
                    return Ok(Err(HeldStorePositionReason::InvalidChangesetIdentity(
                        error.into(),
                    )));
                }
            };
        let changes = match coven_database::walk_changeset(changeset.bytes()) {
            Ok(changes) => changes,
            Err(error) => {
                return Ok(Err(HeldStorePositionReason::ChangesetUnreadable(
                    error.into(),
                )));
            }
        };
        let old_changes = match coven_database::walk_old_changeset(changeset.bytes()) {
            Ok(changes) => changes,
            Err(error) => {
                return Ok(Err(HeldStorePositionReason::ChangesetUnreadable(
                    error.into(),
                )));
            }
        };
        if let Err(error) = database.validate_local_blob_cleanup_changes(&old_changes, &changes) {
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
        let (PullDatabase::Installed(database) | PullDatabase::Checkpoint { database, .. }) =
            &self.database;

        database.has_scoped_graph()
    }

    pub(crate) fn schema_version(&self) -> u32 {
        let (PullDatabase::Installed(database) | PullDatabase::Checkpoint { database, .. }) =
            &self.database;

        database.schema_version()
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
        let (PullDatabase::Installed(database) | PullDatabase::Checkpoint { database, .. }) =
            &self.database;

        database
            .unrepresented_device_join_bootstrap_commits(plan)
            .await
    }

    pub(crate) fn receive_wall_ms(&self) -> u64 {
        let (PullDatabase::Installed(database) | PullDatabase::Checkpoint { database, .. }) =
            &self.database;

        database.receive_wall_ms()
    }

    pub(crate) async fn materialized_frontier(
        &self,
    ) -> Result<std::collections::BTreeMap<String, StoreBatchCommitRef>, coven_database::DbError>
    {
        let (PullDatabase::Installed(database) | PullDatabase::Checkpoint { database, .. }) =
            &self.database;

        database.materialized_frontier().await
    }

    pub(crate) async fn exact_materialized_ref(
        &self,
        stream_id: &str,
        sequence: u64,
    ) -> Result<Option<StoreBatchCommitRef>, coven_database::DbError> {
        let (PullDatabase::Installed(database) | PullDatabase::Checkpoint { database, .. }) =
            &self.database;

        database.exact_materialized_ref(stream_id, sequence).await
    }

    pub(crate) async fn snapshot_coverage(
        &self,
    ) -> Result<CommitFrontier, coven_database::DbError> {
        let (PullDatabase::Installed(database) | PullDatabase::Checkpoint { database, .. }) =
            &self.database;

        database.snapshot_coverage_frontier().await
    }

    pub(crate) async fn record_circle_close_exclusions(
        &self,
        exclusions: Vec<coven_protocol::circle_activation::LocalCircleExclusion>,
    ) -> Result<(), coven_database::DbError> {
        let (PullDatabase::Installed(database) | PullDatabase::Checkpoint { database, .. }) =
            &self.database;

        database.record_circle_close_exclusions(exclusions).await
    }

    pub(crate) async fn commit_publication_interval(
        &mut self,
        materializations: Vec<coven_database::PreparedMergeMaterialization>,
        accepted: coven_database::AcceptedStorePublicationInterval,
        replay: store_commit::VerifiedStorePublicationInterval,
        snapshots: Vec<coven_database::VerifiedStoreSnapshotAuthority>,
        local_store_membership: LocalStoreMembership,
        routing_encryption: Option<coven_keys::encryption::EncryptionService>,
        routing_key: Option<coven_protocol::circle::RowRoutingKey>,
        receiver_wall_ms: u64,
    ) -> Result<
        (
            coven_database::MaterializationOutcome,
            Vec<StoreBatchCommitRef>,
        ),
        coven_database::DbError,
    > {
        let receiver = match &self.database {
            PullDatabase::Installed(database)
            | PullDatabase::Checkpoint {
                receiver: database, ..
            } => database.clone(),
        };
        let database = std::mem::replace(&mut self.database, PullDatabase::Installed(receiver));
        let (outcome, installed) = match database {
            PullDatabase::Installed(database) => {
                database
                    .apply_received_store_publication_interval(
                        materializations,
                        accepted,
                        replay,
                        snapshots,
                        local_store_membership,
                        routing_encryption,
                        routing_key,
                        receiver_wall_ms,
                    )
                    .await?
            }
            PullDatabase::Checkpoint {
                receiver,
                database,
                expected,
            } => {
                let prepared = database.into_prepared_snapshot().await?;
                receiver
                    .install_received_snapshot(
                        prepared,
                        expected,
                        materializations,
                        accepted,
                        snapshots,
                        local_store_membership,
                        routing_encryption,
                        routing_key,
                        receiver_wall_ms,
                    )
                    .await?
            }
        };
        #[cfg(any(test, feature = "test-utils"))]
        if matches!(outcome, coven_database::MaterializationOutcome::Applied(_)) {
            let (PullDatabase::Installed(database) | PullDatabase::Checkpoint { database, .. }) =
                &self.database;
            for reference in &installed {
                let coordinate = &reference.coord;
                database
                    .reach_test_point(coven_database::DatabaseTestPoint::PullAfterRemoteCommit {
                        device_id: coordinate.stream_id.to_string(),
                        seq: coordinate.sequence(),
                    })
                    .await;
            }
        }
        Ok((outcome, installed))
    }

    pub(crate) async fn prepare_merge_history_successor(
        &self,
        verified_commit: &VerifiedStoreBatchCommit,
        membership: &MembershipChain,
        recovery_author: Option<&coven_protocol::store_commit::StoreDeviceRegistrationRef>,
        predecessor_state: &ResolvedStoreDeviceState,
        state_after: &ResolvedStoreDeviceState,
        evidence: MergeHistorySuccessorEvidence,
    ) -> Result<PreparedMergeHistorySuccessor, StorePullError> {
        crate::sync::store::authorization::history::retained::prepare_merge_history_successor(
            self.history,
            verified_commit,
            membership,
            recovery_author,
            predecessor_state,
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
        let (PullDatabase::Installed(database) | PullDatabase::Checkpoint { database, .. }) =
            &self.database;

        let retained =
            crate::sync::store::authorization::history::retained::seed_verifier_from_retained_history(
                database,
                self.history,
            )
            .await?;
        Ok(retained)
    }

    pub(crate) async fn prepare_device_join_history(
        &mut self,
        plan: &coven_database::DeviceJoinBootstrapPlan,
    ) -> Result<(), StorePullError> {
        self.prepare_retained_history().await?;
        for prepared in &plan.commits {
            let publication = plan.publication.accepted_commit(&prepared.commit)?;
            self.history
                .admit_published_commit(publication, prepared.commit.clone())?;
        }
        Ok(())
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

    pub(crate) fn verified_membership_prefix(
        &self,
        predecessors: impl IntoIterator<Item = StoreBatchCommitRef>,
    ) -> Result<VerifiedMergeMembershipPrefix, StorePullError> {
        self.history.verified_membership_prefix(predecessors)
    }

    /// Hold every candidate commit's Store package before the ordered pass.
    pub(crate) async fn prefetch_store_packages<'commits>(
        &self,
        commits: impl IntoIterator<
            Item = (
                &'commits coven_protocol::store_commit::StoreBatchCommitRef,
                &'commits coven_protocol::store_commit::StoreBatchCommit,
            ),
        >,
    ) {
        self.history.prefetch_store_packages(commits).await
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
        let (PullDatabase::Installed(database) | PullDatabase::Checkpoint { database, .. }) =
            &self.database;

        materialized_reference_status(database, self.history, coverage, stream_id, reference).await
    }

    pub(crate) async fn readiness(
        &mut self,
        coverage: &CommitFrontier,
        frontier: &std::collections::BTreeMap<String, StoreBatchCommitRef>,
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
                        )));
                    }
                    MaterializedCheck::Held(reason) => {
                        return Ok(Readiness::Held(HeldStorePosition::commit(
                            commit_ref, reason,
                        )));
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

        let ready_frontier = CommitFrontier::from_refs(frontier.clone())?;
        for (required_stream, required_ref) in commit.merge_dependencies() {
            let required_stream = required_stream.to_string();
            // The frontier includes commits prepared earlier in this atomic pull.
            // Authenticate older dependencies by exact stored references or
            // verified predecessor links, never by sequence numbers alone.
            match self
                .materialized_reference_status(&ready_frontier, &required_stream, required_ref)
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
                    )));
                }
                MaterializedCheck::Held(reason) => {
                    return Ok(Readiness::Held(HeldStorePosition::dependency(
                        commit_ref,
                        &required_stream,
                        required_ref,
                        reason,
                    )));
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
}

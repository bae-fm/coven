use super::*;

impl<'storage> AuthorizedWriterOperation<'storage> {
    pub(crate) async fn history_has_only_acknowledgements(
        &mut self,
        previous: &coven_protocol::store_commit::StoreHistoryCut,
        current: &coven_protocol::store_commit::StoreHistoryCut,
    ) -> Result<bool, crate::sync::store::pull::StorePullError> {
        self.history
            .history_has_only_acknowledgements(previous, current)
            .await
    }

    pub(super) fn membership_objects(&self) -> StoreMembershipObjectVerifier<'_, 'storage> {
        self.history.membership_objects()
    }

    pub(crate) fn store_root(&self) -> &coven_protocol::store_commit::StoreRootRef {
        self.history.root()
    }

    pub(crate) async fn snapshot_publication(
        &self,
    ) -> crate::sync::store::snapshots::AuthorizedSnapshotPublication<'_> {
        crate::sync::store::snapshots::AuthorizedSnapshotPublication::begin(
            &self.database,
            self.storage.as_ref(),
        )
        .await
    }

    pub(crate) async fn resume_snapshot_publication(
        &mut self,
    ) -> Result<
        Option<coven_protocol::store_commit::SnapshotMeta>,
        crate::sync::store::snapshots::SnapshotError,
    > {
        self.snapshots().resume_pending_publication().await
    }

    pub(crate) async fn publish_store_snapshot(
        &mut self,
        pending: &coven_database::DurableSnapshotPublication,
        objects: &crate::sync::store::snapshots::AuthorizedSnapshotPublication<'_>,
    ) -> Result<
        crate::sync::store::authorization::history::publication::StoreSnapshotPublicationAttemptOutcome,
        crate::sync::store::snapshots::SnapshotError,
    >{
        self.writer
            .publish_store_snapshot(&mut self.history, &mut self.membership, pending, objects)
            .await
    }

    pub(crate) async fn capture_current_store_snapshot_cut(
        &self,
    ) -> Result<
        (
            coven_database::CreatedSnapshot,
            coven_protocol::store_commit::CommitFrontier,
        ),
        coven_database::DbError,
    > {
        self.history.capture_current_store_snapshot_cut().await
    }

    pub(crate) fn protocol_root(&self) -> &coven_protocol::store_commit::StoreProtocolRoot {
        &self.history.verified_root_object().value
    }

    /// Select the exact author stream without overwriting its committed prefix.
    /// Streams are persisted per database, so independently restored devices use
    /// different streams; copied state that reuses one exposes an immutable fork.
    pub(super) async fn select_membership_author_stream(
        &self,
        chain: &coven_protocol::membership::MembershipChain,
    ) -> Result<
        coven_protocol::membership::AuthorStreamId,
        crate::sync::store::commit_publication::membership::MembershipMutationError,
    > {
        self.history
            .select_membership_author_stream(chain, &self.writer.author_pubkey())
            .await
    }

    pub(crate) async fn resolve_accepted_snapshot(
        &mut self,
    ) -> Result<
        Result<
            crate::sync::store::commit_verification::merge_history::SelectedStoreSnapshot,
            crate::sync::store::ReplayBaselineDecline,
        >,
        crate::sync::store::acknowledgements::StoreAckError,
    > {
        self.history.resolve_accepted_snapshot().await
    }

    pub(super) async fn stage_verified_blob_plaintext(
        &self,
        authority: &coven_protocol::blob::RowBlobAuthority,
        stored: &coven_protocol::blob::locator::StoredBlobRef,
        destination: &std::path::Path,
    ) -> Result<coven_foundation::local_file::AtomicStagedFile, crate::sync::BlobCacheError> {
        let stage = self
            .store_dir
            .stage_atomic_file(destination)
            .await
            .map_err(crate::sync::BlobCacheError::File)?;
        self.history
            .stage_verified_blob_plaintext(
                authority,
                stored,
                stage,
                coven_storage::cloud::no_download_progress(),
            )
            .await
    }

    pub(super) async fn authorize_retained_preparation(
        &self,
        order: &coven_protocol::store_commit::StoreCommitOrder,
        membership_heads: &[coven_protocol::membership::MembershipHeadRef],
    ) -> Result<
        crate::sync::store::commit_verification::merge_history::MergeOutboundAuthorization,
        crate::sync::store::pull::StorePullError,
    > {
        self.writer
            .authorize_retained_preparation(&self.history, order, membership_heads)
            .await
    }

    /// Seed the verifier from retained history before a walk over it, so the
    /// walk reads nothing from the provider.
    /// Retire the owner's journal for every join whose device has arrived.
    ///
    /// The owner's half of a join ends at a published activation commit, and
    /// until now it ended there permanently: the row sat at
    /// `ActivationPrepared` for the life of the store, still offering to hand
    /// the activation over, because the owner has no artifact by which it could
    /// learn the joining device took it — the same asymmetry that makes the
    /// joiner, not the owner, delete the attempt's transport slots.
    ///
    /// The arrival it can see is the joined device's own first commit. Every
    /// other trace of the join is something the owner wrote: the registration
    /// goes Active from the owner's own activation commit, so it says nothing
    /// about whether the device ever ran. A stream in the materialized frontier
    /// under that device's announcement stream id is a commit the device signed
    /// and this device verified, which it can only have published after
    /// installing the Store.
    ///
    /// Reads nothing from the provider: the stream id is derived from the
    /// registration the journal already holds, and the frontier is the row the
    /// cycle reads anyway. Each retirement is one row delete, so a cycle that
    /// fails partway leaves the rest for the next one to find.
    pub(crate) async fn retire_arrived_device_joins(
        &self,
    ) -> Result<usize, crate::sync::store::DeviceJoinError> {
        let awaiting = self.database.owner_device_joins_awaiting_arrival().await?;
        if awaiting.is_empty() {
            return Ok(0);
        }
        let frontier = self.database.materialized_frontier().await?;
        let store_root_hash = self.store_root().store_root_hash;
        let mut retired = 0;
        for (attempt_id, registration) in awaiting {
            let stream =
                coven_protocol::store_commit::StreamActivation::device_authorized_stream_id(
                    store_root_hash,
                    &registration,
                    coven_protocol::store_commit::StreamAnchorDomain::StoreAnnouncements,
                );
            if !frontier.contains_key(&stream.to_string()) {
                continue;
            }
            self.database
                .retire_device_join(attempt_id, crate::sync::store::DeviceJoinRole::Owner)
                .await?;
            retired += 1;
        }
        Ok(retired)
    }

    pub(crate) async fn seed_retained_history(
        &mut self,
    ) -> Result<(), crate::sync::store::pull::StorePullError> {
        self.history.seed_retained_history().await
    }

    pub(super) async fn prepare_merge_history_successor(
        &self,
        commit: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
        membership: &coven_protocol::membership::MembershipChain,
        recovery_author: Option<&coven_protocol::store_commit::StoreDeviceRegistrationRef>,
        predecessor_state: &coven_protocol::store_commit::ResolvedStoreDeviceState,
        state_after: &coven_protocol::store_commit::ResolvedStoreDeviceState,
        evidence: crate::sync::store::commit_verification::merge_history::MergeHistorySuccessorEvidence,
    ) -> Result<
        crate::sync::store::commit_verification::merge_history::PreparedMergeHistorySuccessor,
        crate::sync::store::pull::StorePullError,
    > {
        self.history
            .prepare_merge_history_successor(
                commit,
                membership,
                recovery_author,
                predecessor_state,
                state_after,
                evidence,
            )
            .await
    }

    pub(super) async fn upload_commit(
        &self,
        candidate: &commit_plan::PreparedStoreOperationCommit,
    ) -> Result<(), StoreError> {
        let stream_id = candidate.reference.coord.stream_id;
        let context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
            candidate.commit.store_root_hash,
            coven_protocol::objects::ProtocolObjectDomain::StoreCommit,
        );
        let prefix = coven_protocol::store_commit::commit_semantic_prefix(
            candidate.commit.candidate_family(),
            &stream_id.to_string(),
            candidate.commit.seq(),
            candidate.commit.commit_hash(),
        );
        self.storage
            .as_ref()
            .create_verified_protocol_object(
                &context,
                &candidate.prepared_commit()?,
                &prefix,
                &candidate.commit.to_bytes(),
            )
            .await
            .map_err(StoreError::prepared_object)
    }

    pub async fn pull(
        &mut self,
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
    ) -> Result<crate::sync::store::StorePullResult, SyncCycleFailure> {
        let membership = self.membership.clone();
        let execution = self
            .writer
            .pull(&mut self.history, &membership, routing_encryption)
            .await
            .map_err(|error| SyncCycleFailure::operation("pull Store commits", error))?;
        self.membership = execution.membership;
        Ok(execution.result)
    }

    pub(crate) fn require_current_owner(
        &self,
        author_pubkey: &str,
    ) -> Result<(), coven_protocol::membership::MembershipError> {
        if self.membership.is_owner_now(author_pubkey) {
            Ok(())
        } else {
            Err(
                coven_protocol::membership::MembershipError::SignerIsNotOwner(
                    author_pubkey.to_string(),
                ),
            )
        }
    }

    pub(crate) async fn prepare_merge_snapshot_history_summary(
        &self,
        coverage: &coven_protocol::store_commit::CommitFrontier,
        membership: &coven_protocol::membership::MembershipChain,
        state: &coven_protocol::store_commit::ResolvedStoreDeviceState,
        publication: &coven_protocol::store_commit::StoreCurrentPublicationRecord,
    ) -> Result<
        coven_protocol::store_commit::RetainedVerifiedMergeHistorySummary,
        crate::sync::store::pull::StorePullError,
    > {
        self.writer
            .prepare_merge_snapshot_history_summary(
                &self.history,
                coverage,
                membership,
                state,
                publication,
            )
            .await
    }

    /// The membership objects a reader of this Store's current frontier would
    /// otherwise fetch one at a time — what a snapshot publishes as its
    /// membership rollup.
    ///
    /// This runs the anchored walk again rather than keeping what an earlier
    /// one read, because the rollup has to describe the whole chain and not
    /// whatever part of it this operation happened to touch. The walk is
    /// served from this verifier's own slot and object memos, so on a device
    /// that has already resolved its membership this costs the terminating
    /// probe per stream and nothing else.
    pub(crate) async fn membership_rollup_parts(
        &mut self,
        membership: &coven_protocol::membership::MembershipChain,
    ) -> Result<
        Vec<coven_protocol::store_commit::MembershipRollupStream>,
        crate::sync::store::membership::AnchoredChainError,
    > {
        self.history.membership_rollup_parts(membership).await
    }

    pub(crate) fn snapshots(
        &mut self,
    ) -> crate::sync::store::snapshots::AuthorizedSnapshots<'_, 'storage> {
        let database = self.database.clone();
        let storage = Arc::clone(self.storage);
        let store_dir = self.store_dir;
        let local_writer = Arc::clone(&self.writer);
        crate::sync::store::snapshots::AuthorizedSnapshots::new(
            self,
            database,
            storage,
            store_dir,
            local_writer,
        )
    }

    pub(crate) fn acknowledgements(
        &mut self,
    ) -> crate::sync::store::acknowledgements::AuthorizedAcknowledgements<'_, 'storage> {
        let database = self.database.clone();
        let storage = Arc::clone(self.storage);
        let local_writer = Arc::clone(&self.writer);
        crate::sync::store::acknowledgements::AuthorizedAcknowledgements::new(
            self,
            database,
            storage,
            local_writer,
        )
    }

    pub(crate) fn reclaim_history(
        &mut self,
    ) -> crate::sync::store::reclaim::ReclaimHistory<'_, 'storage> {
        self.history.reclaim()
    }

    pub(crate) fn owner_promotion(
        &mut self,
    ) -> crate::sync::store::owner_role_promotion::AuthorizedOwnerPromotion<'_, 'storage> {
        let database = self.database.clone();
        let storage = self.storage.clone();
        let root = self.store_root().clone();
        crate::sync::store::owner_role_promotion::AuthorizedOwnerPromotion::new(
            self, database, storage, root,
        )
    }

    pub(crate) fn owner_promotion_history(
        &mut self,
    ) -> crate::sync::store::owner_role_promotion::OwnerPromotionHistory<'_, 'storage> {
        self.history.owner_promotion()
    }

    pub(crate) async fn refresh_authorization_state(
        &self,
        cipher: &dyn coven_storage::CloudSyncCipherStateAccess,
        pending_rotation: &dyn coven_storage::CloudSyncRotationStateAccess,
        master_keys: Option<&dyn coven_keys::keys::MasterKeyCustody>,
    ) -> Result<(), SyncCycleFailure> {
        let result = async {
            if cipher.is_plaintext() {
                tracing::debug!("refresh: plaintext home, nothing to refresh");
                return Ok(());
            }

            let recipient = self.writer.author_pubkey();
            let activated = self
                .membership
                .sealed_key_authority_for(&recipient)
                .map_err(AuthorizationRefreshError::Membership)?;
            if activated.is_empty() {
                tracing::debug!(
                    "refresh: no activated sealed key for this device; keeping the live key"
                );
                return Ok(());
            }

            match crate::sync::store::authorization::open_store_keyring(
                self.writer.as_ref(),
                &self.membership,
            ) {
                Ok(new_encryption) => {
                    let merged = cipher
                        .merged_keyring(&new_encryption)
                        .map_err(AuthorizationRefreshError::InvalidKeyring)?;
                    if merged.merged_key_count() == merged.live_key_count() {
                        if pending_rotation.gate().is_some() {
                            let gate = self
                                .database
                                .complete_peer_rotation_adoption(merged.merged_generation())
                                .await
                                .map_err(AuthorizationRefreshError::Database)?;
                            pending_rotation.install_durable_gate(gate);
                        }
                        tracing::debug!(
                            "refresh: sealed store key is already held by the live keyring"
                        );
                    } else {
                        let gate = self
                            .database
                            .record_peer_rotation(merged.merged_generation())
                            .await
                            .map_err(AuthorizationRefreshError::Database)?;
                        pending_rotation.install_durable_gate(Some(gate));
                        match master_keys {
                            None => {
                                tracing::info!(
                                    committed_generation = merged.merged_generation(),
                                    "refresh: found a rotated store key but this cycle has no \
                                     master-key custody to adopt it; sealing is paused until a \
                                     cycle with custody adopts it"
                                );
                            }
                            Some(master_keys) => {
                                let adopted = cipher
                                    .adopt_key_rotation(&new_encryption, master_keys)
                                    .map_err(AuthorizationRefreshError::KeyAdoption)?;
                                let gate = self
                                    .database
                                    .complete_peer_rotation_adoption(adopted.generation())
                                    .await
                                    .map_err(AuthorizationRefreshError::Database)?;
                                pending_rotation.install_durable_gate(gate);
                                tracing::info!(
                                    fingerprint = adopted.fingerprint(),
                                    "Adopted rotated store key"
                                );
                            }
                        }
                    }
                }
                Err(error) => return Err(AuthorizationRefreshError::SealedKey(error)),
            }

            Ok(())
        }
        .await;

        result.map_err(|error| SyncCycleFailure::operation("refresh authorization state", error))
    }

    pub(crate) fn circles(
        &mut self,
    ) -> crate::sync::store::circles::AuthorizedCircleWriter<'_, 'storage> {
        let database = self.database.clone();
        let storage = Arc::clone(self.storage);
        let root = self.store_root().clone();
        let local_writer = Arc::clone(&self.writer);
        crate::sync::store::circles::AuthorizedCircleWriter::from_parts(
            self,
            database,
            storage,
            root,
            local_writer,
        )
    }

    pub(crate) fn circle_history(
        &mut self,
    ) -> crate::sync::store::commit_publication::circles::VerifiedCircleHistory<'_, 'storage> {
        self.history.circles()
    }

    pub(crate) fn join_history(
        &mut self,
    ) -> crate::sync::store::device_join::history::DeviceJoinHistory<'_, 'storage> {
        self.history.device_join()
    }

    pub(crate) fn device_exclusion_history(
        &mut self,
    ) -> crate::sync::store::device_exclusion::DeviceExclusionHistory<'_, 'storage> {
        self.history.device_exclusion()
    }

    pub(crate) fn device_exclusion(
        &mut self,
    ) -> crate::sync::store::device_exclusion::AuthorizedDeviceExclusion<'_, 'storage> {
        let database = self.database.clone();
        let storage = Arc::clone(self.storage);
        crate::sync::store::device_exclusion::AuthorizedDeviceExclusion::new(
            self, database, storage,
        )
    }

    pub(crate) fn join_operation(
        &mut self,
    ) -> crate::sync::store::commit_publication::device_join::AuthorizedJoin<'_, 'storage> {
        let database = self.database.clone();
        let storage = Arc::clone(self.storage);
        let root = self.store_root().clone();
        let verified_root = self.history.verified_root_object().clone();
        let membership = self.membership.clone();
        let local_writer = Arc::clone(&self.writer);
        crate::sync::store::commit_publication::device_join::AuthorizedJoin::from_parts(
            self,
            database,
            storage,
            root,
            verified_root,
            membership,
            local_writer,
        )
    }

    pub(super) async fn membership_mutation_permit(
        &self,
    ) -> coven_database::store::MembershipMutationPermit {
        self.database.membership_mutation_permit().await
    }

    pub(super) fn writer_pubkey(&self) -> String {
        self.writer.author_pubkey()
    }

    pub(crate) fn local_author_pubkey(&self) -> String {
        self.writer.author_pubkey()
    }

    pub(crate) fn is_local_registration(
        &self,
        registration: &coven_protocol::store_commit::StoreDeviceRegistrationRef,
    ) -> bool {
        self.writer.is_authored_by_registration(registration)
    }

    pub(crate) fn is_current_owner(
        &self,
        membership: &coven_protocol::membership::MembershipChain,
    ) -> bool {
        self.writer.is_current_owner(membership)
    }

    pub(crate) fn matches_local_author(
        &self,
        registration: &coven_protocol::store_commit::StoreDeviceRegistrationRef,
        author_pubkey: &str,
    ) -> bool {
        self.writer.matches_author(registration, author_pubkey)
    }

    pub(crate) fn grant_authorized_stream_id(
        &self,
        grant: &coven_protocol::membership::MembershipGrantId,
        domain: coven_protocol::store_commit::StreamAnchorDomain,
    ) -> coven_protocol::membership::AuthorStreamId {
        self.writer
            .grant_authorized_stream_id(self.store_root().store_root_hash, grant, domain)
    }
}

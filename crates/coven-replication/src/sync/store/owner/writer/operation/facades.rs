use super::*;
use coven_storage::VerifiedObjectWrites;

impl<'storage> AuthorizedWriterOperation<'storage> {
    pub(super) fn membership_objects(&self) -> StoreMembershipObjectVerifier<'_, 'storage> {
        self.history.membership_objects()
    }

    pub(crate) fn store_root(&self) -> &coven_protocol::store_commit::StoreRootRef {
        self.history.root()
    }

    pub(crate) async fn snapshot_publication(&self) -> snapshot::AuthorizedSnapshotPublication<'_> {
        snapshot::AuthorizedSnapshotPublication::begin(&self.database, self.storage.as_ref()).await
    }

    pub(crate) async fn resume_snapshot_publication(
        &self,
    ) -> Result<Option<coven_protocol::store_commit::SnapshotMeta>, snapshot::SnapshotError> {
        self.snapshot_publication().await.resume_store().await
    }

    pub(super) fn protocol_root(&self) -> &coven_protocol::store_commit::StoreProtocolRoot {
        &self.history.verified_root_object().value
    }

    pub(super) fn resolved_membership(
        &self,
    ) -> Result<
        &coven_protocol::membership::MembershipChain,
        crate::sync::store::membership::MembershipOpsError,
    > {
        match self.membership.conflict() {
            Some(conflict) => Err(
                crate::sync::store::membership::MembershipOpsError::SemanticConflict(Box::new(
                    conflict.clone(),
                )),
            ),
            None => Ok(&self.membership),
        }
    }

    pub(super) async fn open_keyring(
        &self,
    ) -> Result<
        coven_keys::encryption::EncryptionService,
        crate::sync::store::owner::writer::membership::InviteError,
    > {
        self.keyrings.open(&self.membership).await
    }

    pub(super) async fn open_keyring_for_membership(
        &self,
        membership: &coven_protocol::membership::MembershipChain,
    ) -> Result<
        coven_keys::encryption::EncryptionService,
        crate::sync::store::owner::writer::membership::InviteError,
    > {
        self.keyrings.open(membership).await
    }

    pub(crate) async fn open_keyring_or_for_membership(
        &self,
        membership: &coven_protocol::membership::MembershipChain,
        initial: &coven_keys::encryption::EncryptionService,
    ) -> Result<
        coven_keys::encryption::EncryptionService,
        crate::sync::store::owner::writer::membership::InviteError,
    > {
        self.keyrings.open_or(membership, initial).await
    }

    pub(crate) async fn prepare_wrapped_key(
        &self,
        recipient: &str,
        value: coven_protocol::wrapped_store_key::WrappedStoreKey,
    ) -> Result<
        coven_protocol::wrapped_store_key::PreparedWrappedStoreKey,
        coven_protocol::objects::StorageError,
    > {
        self.keyrings.prepare(recipient, value).await
    }

    /// Select the exact author stream without overwriting its committed prefix.
    /// Streams are persisted per database, so independently restored devices use
    /// different streams; copied state that reuses one exposes an immutable fork.
    pub(super) async fn select_membership_author_stream(
        &self,
        chain: &coven_protocol::membership::MembershipChain,
    ) -> Result<
        coven_protocol::membership::AuthorStreamId,
        crate::sync::store::owner::writer::membership::InviteError,
    > {
        let author = self.writer.author_pubkey();
        let grant = chain.active_owner_grant(&author).ok_or_else(|| {
            coven_protocol::membership::MembershipError::SignerIsNotOwner(author.clone())
        })?;
        let mut reusable = chain.reusable_author_streams(&author, &grant);
        if let Some(anchored) = chain.membership_stream_id(&grant) {
            reusable.insert(anchored);
        }
        Ok(self
            .database
            .select_membership_author_stream(&author, &grant, reusable)
            .await?)
    }

    pub(super) async fn verify_membership_publication_author(
        &self,
        publication: &PreparedMembershipPublication,
    ) -> Result<
        coven_protocol::store_commit::StoreDeviceRegistration,
        crate::sync::store::membership::InviteError,
    > {
        let author = self
            .history
            .load_registration(&publication.head.body.author_registration)
            .await
            .map_err(|error| {
                crate::sync::store::membership::InviteError::Crypto(error.to_string())
            })?
            .value;
        if !publication.head.verify(&author) {
            return Err(
                crate::sync::store::membership::InviteError::InvalidDurableMutation(
                    "prepared membership head has an invalid certified-device signature"
                        .to_string(),
                ),
            );
        }
        Ok(author)
    }

    pub(super) async fn blocked_candidate_nonactivation(
        &mut self,
        candidate: &coven_database::BlockedMergeCandidate,
    ) -> Result<Option<coven_protocol::remote_object::VerifiedCandidateNonactivation>, StoreError>
    {
        let verified = self
            .history
            .authenticate_blocked_candidate(candidate)
            .await?;
        self.history
            .merge_conflict()
            .excluded_candidate_nonactivation(
                &verified,
                &candidate.head.value,
                &candidate.head.object,
            )
            .await
    }

    pub(super) async fn cleanup_merge_candidate_history(
        &mut self,
        write_id: coven_protocol::write::WriteId,
    ) -> Result<(), crate::sync::store::owner::pull::StorePullError> {
        self.history.cleanup_merge_candidate(write_id).await
    }

    pub(super) async fn select_acknowledgement_snapshot(
        &mut self,
        frontier: &coven_protocol::store_commit::CommitFrontier,
        device_state: &coven_protocol::store_commit::StoreDeviceStateRef,
    ) -> Result<
        Option<coven_protocol::store_commit::StoreSnapshotLocator>,
        acknowledgements::StoreAckError,
    > {
        self.history
            .select_acknowledgement_snapshot(frontier, device_state)
            .await
    }

    pub(super) async fn stage_verified_blob_plaintext(
        &self,
        authority: &coven_protocol::blob::RowBlobAuthority,
        stored: &coven_protocol::blob::locator::StoredBlobRef,
        destination: &std::path::Path,
    ) -> Result<coven_foundation::local_file::AtomicStagedFile, crate::sync::BlobCacheError> {
        self.history
            .stage_verified_blob_plaintext(authority, stored, destination)
            .await
    }

    pub(super) async fn authorize_retained_outbound(
        &self,
        order: &coven_protocol::store_commit::StoreCommitOrder,
        membership_heads: &[coven_protocol::membership::MembershipHeadRef],
    ) -> Result<
        crate::sync::store::owner::writer::verified_history::MergeOutboundAuthorization,
        crate::sync::store::owner::writer::pull::StorePullError,
    > {
        self.writer
            .authorize_retained_outbound(&self.history, order, membership_heads)
            .await
    }

    pub(super) async fn prepare_merge_history_successor(
        &self,
        commit: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
        membership: &coven_protocol::membership::MembershipChain,
        recovery_author: Option<&coven_protocol::store_commit::StoreDeviceRegistrationRef>,
        state_after: coven_protocol::store_commit::ResolvedStoreDeviceState,
        evidence: crate::sync::store::owner::writer::verified_history::MergeHistorySuccessorEvidence,
    ) -> Result<
        crate::sync::store::owner::writer::verified_history::PreparedMergeHistorySuccessor,
        crate::sync::store::owner::writer::pull::StorePullError,
    > {
        self.history
            .prepare_merge_history_successor(
                commit,
                membership,
                recovery_author,
                state_after,
                evidence,
            )
            .await
    }

    pub(super) async fn observe_occupied_merge_head(
        &mut self,
        expected: &coven_protocol::store_commit::StoreDeviceHead,
        expected_commit: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
        slot: &coven_protocol::objects::ObjectSlot,
        semantic_prefix: &str,
    ) -> Result<crate::sync::store::merge_conflict::VerifiedMergeWinner, StoreError> {
        self.history
            .merge_conflict()
            .observe_occupied_merge_head(expected, expected_commit, slot, semantic_prefix)
            .await
    }

    pub(super) async fn upload_commit(
        &self,
        candidate: &operations::PreparedStoreOperationCommit,
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
            .create_protocol_object(&candidate.prepared)
            .await
            .map_err(coven_protocol::objects::StoreObjectError::from)?;
        self.storage
            .verify_readback(
                &context,
                &candidate.reference.object,
                &prefix,
                &candidate.commit.to_bytes(),
            )
            .await
            .map_err(StoreError::readback)
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

    pub(super) fn require_current_owner(&self, author_pubkey: &str) -> Result<(), String> {
        if self.membership.is_owner_now(author_pubkey) {
            Ok(())
        } else {
            Err(format!("author {author_pubkey} is not a current owner"))
        }
    }

    pub(super) fn reclaim_history(
        &mut self,
    ) -> crate::sync::store::owner::writer::history::ReclaimHistory<'_, 'storage> {
        self.history.reclaim()
    }

    pub(crate) fn owner_promotion(
        &mut self,
    ) -> crate::sync::store::owner::writer::owner_promotion::AuthorizedOwnerPromotion<'_, 'storage>
    {
        let database = self.database.clone();
        let storage = self.storage.clone();
        let root = self.store_root().clone();
        let membership = self.membership.clone();
        crate::sync::store::owner::writer::owner_promotion::AuthorizedOwnerPromotion::new(
            self, database, storage, root, membership,
        )
    }

    pub(crate) fn owner_promotion_history(
        &mut self,
    ) -> crate::sync::store::owner::writer::history::OwnerPromotionHistory<'_, 'storage> {
        self.history.owner_promotion()
    }

    pub(crate) async fn refresh_authorization_state(
        &self,
        cipher: &dyn coven_storage::CloudCipherAccess,
        pending_rotation: &dyn coven_storage::CloudRotationAccess,
        master_keys: Option<&dyn coven_keys::keys::MasterKeyCustody>,
    ) -> Result<(), SyncCycleFailure> {
        let result = async {
            if cipher.snapshot().is_plaintext() {
                tracing::debug!("refresh: plaintext home, nothing to refresh");
                return Ok(());
            }

            let recipient = self.writer.author_pubkey();
            let wrapped_keys = self
                .membership
                .wrapped_key_authority_for(&recipient)
                .map_err(AuthorizationRefreshError::Membership)?;
            let live_keyring = match cipher.snapshot() {
                coven_storage::CloudCipher::Encrypted(encryption) => encryption,
                coven_storage::CloudCipher::Plaintext => {
                    return Err(AuthorizationRefreshError::InvalidState(
                        "plaintext home cannot enter encrypted key refresh".to_string(),
                    ));
                }
            };
            if wrapped_keys.is_empty() {
                tracing::debug!(
                    "refresh: no activated wrapped key for this device; keeping the live key"
                );
                return Ok(());
            }

            match self.open_keyring().await {
                Ok(new_encryption) => {
                    let merged = live_keyring
                        .merged_with(&new_encryption)
                        .map_err(AuthorizationRefreshError::InvalidKeyring)?;
                    if merged.key_count() == live_keyring.key_count() {
                        if pending_rotation.gate().is_some() {
                            let gate = self
                                .database
                                .complete_peer_rotation_adoption(live_keyring.current_generation())
                                .await
                                .map_err(AuthorizationRefreshError::Database)?;
                            pending_rotation.install_durable_gate(gate);
                        }
                        tracing::debug!(
                            "refresh: wrapped store key is already held by the live keyring"
                        );
                    } else {
                        let gate = self
                            .database
                            .record_peer_rotation(merged.current_generation())
                            .await
                            .map_err(AuthorizationRefreshError::Database)?;
                        pending_rotation.install_durable_gate(Some(gate));
                        match master_keys {
                            None => {
                                tracing::info!(
                                    committed_generation = merged.current_generation(),
                                    "refresh: found a rotated store key but this cycle has no \
                                     master-key custody to adopt it; sealing is paused until a \
                                     cycle with custody adopts it"
                                );
                            }
                            Some(master_keys) => {
                                let fingerprint = cipher
                                    .adopt_key_rotation(&new_encryption, master_keys)
                                    .map_err(AuthorizationRefreshError::KeyAdoption)?;
                                let adopted_generation = match cipher.snapshot() {
                                    coven_storage::CloudCipher::Encrypted(encryption) => {
                                        encryption.current_generation()
                                    }
                                    coven_storage::CloudCipher::Plaintext => {
                                        return Err(AuthorizationRefreshError::InvalidState(
                                            "encrypted key refresh produced a plaintext cipher"
                                                .to_string(),
                                        ));
                                    }
                                };
                                let gate = self
                                    .database
                                    .complete_peer_rotation_adoption(adopted_generation)
                                    .await
                                    .map_err(AuthorizationRefreshError::Database)?;
                                pending_rotation.install_durable_gate(gate);
                                tracing::info!(%fingerprint, "Adopted rotated store key");
                            }
                        }
                    }
                }
                Err(error) => return Err(AuthorizationRefreshError::WrappedKey(error)),
            }

            Ok(())
        }
        .await;

        result.map_err(|error| SyncCycleFailure::operation("refresh authorization state", error))
    }

    pub(crate) fn circles(
        &mut self,
    ) -> crate::sync::store::owner::writer::AuthorizedCircleWriter<'_, 'storage> {
        let database = self.database.clone();
        let storage = Arc::clone(self.storage);
        let store_dir = self.store_dir;
        let root = self.store_root().clone();
        let membership = self.membership.clone();
        let local_writer = Arc::clone(&self.writer);
        crate::sync::store::owner::writer::AuthorizedCircleWriter::from_parts(
            self,
            database,
            storage,
            store_dir,
            root,
            membership,
            local_writer,
        )
    }

    pub(crate) fn circle_history(
        &mut self,
    ) -> crate::sync::store::owner::writer::circles::VerifiedCircleHistory<'_, 'storage> {
        self.history.circles()
    }

    pub(crate) fn merge_history(
        &mut self,
    ) -> &mut crate::sync::store::owner::verified_history::MergeHistoryVerifier<'storage> {
        self.history.merge_history()
    }

    pub(crate) fn device_exclusion_history(
        &mut self,
    ) -> crate::sync::store::owner::writer::device_exclusion::DeviceExclusionHistory<'_, 'storage>
    {
        self.history.device_exclusion()
    }

    pub(crate) fn device_exclusion(
        &mut self,
    ) -> crate::sync::store::owner::writer::device_exclusion::AuthorizedDeviceExclusion<'_, 'storage>
    {
        let database = self.database.clone();
        let storage = Arc::clone(self.storage);
        crate::sync::store::owner::writer::device_exclusion::AuthorizedDeviceExclusion::new(
            self, database, storage,
        )
    }

    pub(crate) fn join_operation(
        &mut self,
    ) -> crate::sync::store::owner::writer::device_join::AuthorizedJoin<'_, 'storage> {
        let database = self.database.clone();
        let storage = Arc::clone(self.storage);
        let root = self.store_root().clone();
        let protocol_root = self.protocol_root().clone();
        let verified_root = self.history.verified_root_object().clone();
        let membership = self.membership.clone();
        let local_writer = Arc::clone(&self.writer);
        crate::sync::store::owner::writer::device_join::AuthorizedJoin::from_parts(
            self,
            database,
            storage,
            root,
            protocol_root,
            verified_root,
            membership,
            local_writer,
        )
    }

    pub(crate) fn provider_administrator_join(
        &mut self,
    ) -> Result<
        crate::sync::store::owner::writer::device_join::AuthorizedProviderAdministratorJoin<
            '_,
            'storage,
        >,
        crate::sync::store::owner::writer::device_join::DeviceJoinError,
    > {
        self.join_operation().into_provider_administrator()
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

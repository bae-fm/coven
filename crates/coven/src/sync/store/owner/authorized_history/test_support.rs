use super::*;

impl<'storage> AuthorizedStoreHistory<'storage> {
    #[cfg(test)]
    pub(crate) async fn blob_protection_for_test(
        &self,
        authority: &crate::protocol::blob::RowBlobAuthority,
        stored: &crate::protocol::blob::locator::StoredBlobRef,
    ) -> Result<crate::protocol::objects::BlobSpoolProtection, crate::sync::BlobCacheError> {
        self.blob_source
            .protection_for_test(authority, stored)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn open_keyring(
        &self,
        identity: &UserKeypair,
        membership: &MembershipChain,
    ) -> Result<crate::encryption::EncryptionService, crate::sync::store::membership::InviteError>
    {
        self.keyrings.open(identity, membership).await
    }

    #[cfg(test)]
    pub(crate) async fn prepare_wrapped_key(
        &self,
        recipient: &str,
        value: crate::protocol::wrapped_store_key::WrappedStoreKey,
    ) -> Result<
        crate::protocol::wrapped_store_key::PreparedWrappedStoreKey,
        crate::protocol::objects::StorageError,
    > {
        self.keyrings.prepare(recipient, value).await
    }

    #[cfg(test)]
    pub(crate) async fn load_exact_membership_head_for_test(
        &mut self,
        reference: &MembershipHeadRef,
    ) -> Result<
        crate::protocol::membership::AuthorHead,
        crate::sync::store::membership::AnchoredChainError,
    > {
        self.history_verifier
            .load_exact_membership_head(reference)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn load_membership_at_exact_heads_for_test(
        &mut self,
        heads: &[MembershipHeadRef],
        resolutions: &[crate::protocol::membership::StoreMembershipConflictResolutionRef],
    ) -> Result<MembershipChain, crate::sync::store::membership::AnchoredChainError> {
        self.history_verifier
            .load_membership_at_exact_heads(heads, resolutions)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn assert_deep_membership_projection_for_test(
        &mut self,
        heads: &[MembershipHeadRef],
    ) {
        self.history_verifier
            .assert_deep_membership_projection(heads)
            .await;
    }

    #[cfg(test)]
    pub(crate) async fn reach_pull_after_remote_commit_test_point(
        &self,
        device_id: String,
        seq: u64,
    ) {
        self.database
            .reach_test_point(crate::database::DatabaseTestPoint::PullAfterRemoteCommit {
                device_id,
                seq,
            })
            .await;
    }

    #[cfg(test)]
    pub(crate) async fn verify_device_join_attempt_for_test(
        &mut self,
        reference: &crate::protocol::store_commit::DeviceJoinAttemptRef,
        owner: &crate::protocol::store_commit::StoreDeviceRegistration,
    ) -> Result<(), StoreError> {
        self.history_verifier
            .load_verified_device_join_attempt(reference, owner)
            .await?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn exact_next_announcement_slot_for_test(
        &mut self,
        registration_ref: &crate::protocol::store_commit::StoreDeviceRegistrationRef,
        registration: &crate::protocol::store_commit::StoreDeviceRegistration,
        previous: Option<&crate::protocol::store_commit::StoreBatchCommitRef>,
    ) -> Result<
        (
            crate::protocol::objects::ObjectSlot,
            Option<crate::protocol::store_commit::StoreDeviceHeadRef>,
        ),
        StoreError,
    > {
        let previous = match previous {
            Some(reference) => Some(
                self.load_commit(reference)
                    .await
                    .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?,
            ),
            None => None,
        };
        self.history_verifier
            .exact_next_announcement_slot(registration_ref, registration, previous.as_ref())
            .await
    }

    #[cfg(test)]
    pub(crate) async fn load_commit_ancestry_until_for_test(
        &mut self,
        start: crate::protocol::store_commit::StoreBatchCommitRef,
        coverage: &crate::protocol::store_commit::CommitFrontier,
    ) -> Result<
        Vec<(
            crate::protocol::store_commit::StoreBatchCommitRef,
            crate::protocol::store_commit::VerifiedStoreBatchCommit,
        )>,
        StoreError,
    > {
        let mut ancestry = Vec::new();
        let mut cursor = start;
        while !coverage.0.values().any(|covered| covered == &cursor) {
            let commit = self
                .load_commit(&cursor)
                .await
                .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
            let predecessor = commit.order.predecessor().cloned().ok_or_else(|| {
                StoreError::InvalidOutbound(
                    "commit ancestry ended before snapshot coverage".to_string(),
                )
            })?;
            ancestry.push((cursor, commit));
            cursor = predecessor;
        }
        Ok(ancestry)
    }

    #[cfg(test)]
    pub(crate) async fn open_circle_package_for_test(
        &mut self,
        access: &crate::sync::store::circle_controls::CircleEpochAccess,
        commit: &crate::protocol::store_commit::VerifiedStoreBatchCommit,
        reference: &crate::protocol::store_commit::CirclePackageRef,
    ) -> Result<Vec<u8>, StoreError> {
        let reader = crate::sync::store::owner::circles::packages::CirclePackageReader::new(
            &self.database,
            self.storage.as_ref(),
            &mut self.history_verifier,
        );
        let opened = reader
            .open_package(access, commit, reference, commit.author())
            .await
            .map_err(|error| match error {
                crate::sync::store::owner::circles::packages::CirclePackageReadError::Database(
                    error,
                ) => StoreError::Database(error),
                crate::sync::store::owner::circles::packages::CirclePackageReadError::Invalid(
                    error,
                ) => StoreError::InvalidOutbound(error),
            })?;
        Ok(opened.object.value)
    }

    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn pull_readiness_for_test(
        &mut self,
        coverage: &crate::protocol::store_commit::CommitFrontier,
        frontier: &std::collections::BTreeMap<
            String,
            crate::protocol::store_commit::StoreBatchCommitRef,
        >,
        device_state: &crate::protocol::store_commit::ResolvedStoreDeviceState,
        exclusion_freezes: &[crate::protocol::store_commit::StoreDeviceProposalAck],
        commit_ref: &crate::protocol::store_commit::StoreBatchCommitRef,
        commit: &crate::protocol::store_commit::StoreBatchCommit,
    ) -> Result<pull::Readiness, pull::StorePullError> {
        self.pull_readiness(
            coverage,
            frontier,
            device_state,
            exclusion_freezes,
            commit_ref,
            commit,
        )
        .await
    }

    #[cfg(test)]
    pub(crate) async fn verified_merge_membership_prefix_for_test(
        &mut self,
        references: impl IntoIterator<Item = crate::protocol::store_commit::StoreBatchCommitRef>,
        predecessors: impl IntoIterator<Item = crate::protocol::store_commit::StoreBatchCommitRef>,
    ) -> Result<VerifiedMergeMembershipPrefix, pull::StorePullError> {
        self.history_verifier.verify_refs(references).await?;
        self.history_verifier
            .verified_membership_prefix(predecessors)
    }

    #[cfg(test)]
    pub(crate) async fn load_founder_registration_for_test(
        &mut self,
    ) -> Result<
        crate::protocol::objects::VerifiedObject<
            crate::protocol::store_commit::StoreDeviceRegistration,
        >,
        StoreError,
    > {
        Ok(self.history_verifier.load_founder_registration().await?)
    }

    #[cfg(test)]
    pub(crate) async fn prepare_merge_history_successor_for_test(
        &mut self,
        verified_commit: &crate::protocol::store_commit::VerifiedStoreBatchCommit,
        membership: &MembershipChain,
        recovery_author: Option<&crate::protocol::store_commit::StoreDeviceRegistrationRef>,
        evidence: MergeHistorySuccessorEvidence,
    ) -> Result<PreparedMergeHistorySuccessor, StoreError> {
        let (_, state_after) = self
            .database
            .store_device_state_for_order(&verified_commit.value().order)
            .await?;
        self.prepare_merge_history_successor(
            verified_commit,
            membership,
            recovery_author,
            state_after,
            evidence,
        )
        .await
        .map_err(StoreError::from)
    }

    #[cfg(test)]
    pub(crate) async fn prepare_device_join_bootstrap_for_test(
        &mut self,
        coverage: &crate::protocol::store_commit::StoreHistoryCut,
        attempt_activation: &crate::protocol::store_commit::StoreBatchCommitRef,
        membership_state: &crate::protocol::circle_control::StoreMembershipStateRef,
    ) -> Result<crate::database::DeviceJoinBootstrapPlan, StoreError> {
        self.history_verifier
            .prepare_device_join_bootstrap(coverage, attempt_activation, membership_state)
            .await
            .map_err(StoreError::from)
    }

    #[cfg(test)]
    pub(crate) async fn load_store_package_for_test(
        &mut self,
        reference: &crate::protocol::store_commit::StoreBatchCommitRef,
    ) -> Result<Option<crate::protocol::objects::VerifiedObject<Vec<u8>>>, StoreError> {
        Ok(self.history_verifier.load_store_package(reference).await?)
    }

    #[cfg(test)]
    pub(crate) async fn load_store_ack_for_test(
        &mut self,
        reference: &crate::protocol::store_commit::StoreAckRef,
        registration: &crate::protocol::store_commit::StoreDeviceRegistration,
    ) -> Result<crate::protocol::store_commit::StoreAck, StoreError> {
        Ok(self
            .history_verifier
            .load_store_ack(reference, registration)
            .await?
            .value)
    }

    #[cfg(test)]
    pub(crate) async fn load_head_for_test(
        &mut self,
        reference: &crate::protocol::store_commit::StoreDeviceHeadRef,
        registration: &crate::protocol::store_commit::StoreDeviceRegistration,
        commit: &crate::protocol::store_commit::StoreBatchCommitRef,
    ) -> Result<crate::protocol::store_commit::StoreDeviceHead, StoreError> {
        Ok(self
            .history_verifier
            .load_head(reference, registration, commit)
            .await?
            .value)
    }
}

use super::*;

impl<'storage> AuthorizedStoreHistory<'storage> {
    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn blob_key_fingerprint_for_test(
        &self,
        authority: &coven_protocol::blob::RowBlobAuthority,
        stored: &coven_protocol::blob::locator::StoredBlobRef,
    ) -> Result<Option<coven_keys::encryption::KeyFingerprint>, crate::sync::BlobCacheError> {
        self.blob_source
            .key_fingerprint_for_test(authority, stored)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn open_keyring(
        &self,
        identity: &UserKeypair,
        membership: &MembershipChain,
    ) -> Result<
        coven_keys::encryption::EncryptionService,
        crate::sync::store::membership::InviteError,
    > {
        self.keyrings.open(identity, membership).await
    }

    #[cfg(test)]
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

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn load_exact_membership_head_for_test(
        &mut self,
        reference: &MembershipHeadRef,
    ) -> Result<
        coven_protocol::membership::AuthorHead,
        crate::sync::store::membership::AnchoredChainError,
    > {
        self.history_verifier
            .load_exact_membership_head(reference)
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn load_membership_at_exact_heads_for_test(
        &mut self,
        heads: &[MembershipHeadRef],
        resolutions: &[coven_protocol::membership::StoreMembershipConflictResolutionRef],
    ) -> Result<MembershipChain, crate::sync::store::membership::AnchoredChainError> {
        self.history_verifier
            .load_membership_at_exact_heads(heads, resolutions)
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn assert_deep_membership_projection_for_test(
        &mut self,
        heads: &[MembershipHeadRef],
    ) {
        self.history_verifier
            .assert_deep_membership_projection(heads)
            .await;
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn verify_device_join_attempt_for_test(
        &mut self,
        reference: &coven_protocol::store_commit::DeviceJoinAttemptRef,
        owner: &coven_protocol::store_commit::StoreDeviceRegistration,
    ) -> Result<(), StoreError> {
        self.history_verifier
            .load_verified_device_join_attempt(reference, owner)
            .await?;
        Ok(())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn exact_next_announcement_slot_for_test(
        &mut self,
        registration_ref: &coven_protocol::store_commit::StoreDeviceRegistrationRef,
        registration: &coven_protocol::store_commit::StoreDeviceRegistration,
        previous: Option<&coven_protocol::store_commit::StoreBatchCommitRef>,
    ) -> Result<
        (
            coven_protocol::objects::ObjectSlot,
            Option<coven_protocol::store_commit::StoreDeviceHeadRef>,
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

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn load_commit_ancestry_until_for_test(
        &mut self,
        start: coven_protocol::store_commit::StoreBatchCommitRef,
        coverage: &coven_protocol::store_commit::CommitFrontier,
    ) -> Result<
        Vec<(
            coven_protocol::store_commit::StoreBatchCommitRef,
            coven_protocol::store_commit::VerifiedStoreBatchCommit,
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

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn open_circle_package_for_test(
        &mut self,
        access: &coven_protocol::circle_activation::CircleEpochAccess,
        commit: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
        reference: &coven_protocol::store_commit::CirclePackageRef,
    ) -> Result<Vec<u8>, StoreError> {
        let reader = crate::sync::store::circles::packages::CirclePackageReader::new(
            &self.database,
            self.storage.as_ref(),
            &mut self.history_verifier,
        );
        let opened = reader
            .open_package(access, commit, reference, commit.author())
            .await
            .map_err(|error| match error {
                crate::sync::store::circles::packages::CirclePackageReadError::Database(error) => {
                    StoreError::Database(error)
                }
                crate::sync::store::circles::packages::CirclePackageReadError::Invalid(error) => {
                    StoreError::InvalidOutbound(error)
                }
            })?;
        Ok(opened.object.value)
    }

    #[cfg(any(test, feature = "test-utils"))]
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn pull_readiness_for_test(
        &mut self,
        coverage: &coven_protocol::store_commit::CommitFrontier,
        frontier: &std::collections::BTreeMap<
            String,
            coven_protocol::store_commit::StoreBatchCommitRef,
        >,
        device_state: &coven_protocol::store_commit::ResolvedStoreDeviceState,
        exclusion_freezes: &[coven_protocol::store_commit::StoreDeviceProposalAck],
        commit_ref: &coven_protocol::store_commit::StoreBatchCommitRef,
        commit: &coven_protocol::store_commit::StoreBatchCommit,
    ) -> Result<pull::Readiness, pull::StorePullError> {
        self.pull_history()
            .readiness(
                coverage,
                frontier,
                device_state,
                exclusion_freezes,
                commit_ref,
                commit,
            )
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn verified_merge_membership_prefix_for_test(
        &mut self,
        references: impl IntoIterator<Item = coven_protocol::store_commit::StoreBatchCommitRef>,
        predecessors: impl IntoIterator<Item = coven_protocol::store_commit::StoreBatchCommitRef>,
    ) -> Result<VerifiedMergeMembershipPrefix, pull::StorePullError> {
        self.history_verifier.verify_refs(references).await?;
        self.history_verifier
            .verified_membership_prefix(predecessors)
    }

    #[cfg(test)]
    pub(crate) async fn load_founder_registration_for_test(
        &mut self,
    ) -> Result<
        coven_protocol::objects::VerifiedObject<
            coven_protocol::store_commit::StoreDeviceRegistration,
        >,
        StoreError,
    > {
        Ok(self.history_verifier.load_founder_registration().await?)
    }

    #[cfg(test)]
    pub(crate) async fn load_founder_registration_twice_for_test(
        &mut self,
    ) -> Result<(), StoreError> {
        self.history_verifier.load_founder_registration().await?;
        self.history_verifier.load_founder_registration().await?;
        Ok(())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn prepare_merge_history_successor_for_test(
        &mut self,
        verified_commit: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
        membership: &MembershipChain,
        recovery_author: Option<&coven_protocol::store_commit::StoreDeviceRegistrationRef>,
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

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn prepare_device_join_bootstrap_for_test(
        &mut self,
        coverage: &coven_protocol::store_commit::StoreHistoryCut,
        attempt_activation: &coven_protocol::store_commit::StoreBatchCommitRef,
        membership_state: &coven_protocol::circle_control::StoreMembershipStateRef,
    ) -> Result<coven_database::DeviceJoinBootstrapPlan, StoreError> {
        self.history_verifier
            .prepare_device_join_bootstrap(coverage, attempt_activation, membership_state)
            .await
            .map_err(StoreError::from)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn load_store_package_for_test(
        &mut self,
        reference: &coven_protocol::store_commit::StoreBatchCommitRef,
    ) -> Result<Option<coven_protocol::objects::VerifiedObject<Vec<u8>>>, StoreError> {
        Ok(self.history_verifier.load_store_package(reference).await?)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn load_store_ack_for_test(
        &mut self,
        reference: &coven_protocol::store_commit::StoreAckRef,
        registration: &coven_protocol::store_commit::StoreDeviceRegistration,
    ) -> Result<coven_protocol::store_commit::StoreAck, StoreError> {
        Ok(self
            .history_verifier
            .load_store_ack(reference, registration)
            .await?
            .value)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn load_head_for_test(
        &mut self,
        reference: &coven_protocol::store_commit::StoreDeviceHeadRef,
        registration: &coven_protocol::store_commit::StoreDeviceRegistration,
        commit: &coven_protocol::store_commit::StoreBatchCommitRef,
    ) -> Result<coven_protocol::store_commit::StoreDeviceHead, StoreError> {
        Ok(self
            .history_verifier
            .load_head(reference, registration, commit)
            .await?
            .value)
    }
}

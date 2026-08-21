use super::*;

impl Store {
    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn execute_unscoped_host_sql_for_test(
        &self,
        sql: String,
    ) -> Result<(), coven_database::HostWriteError<coven_database::DbError>> {
        let staging = HostWriteBlobStaging::new(
            tokio::runtime::Handle::current(),
            Arc::clone(&self.storage),
            self.root.reference().clone(),
            self.store_dir.clone(),
        );
        self.database
            .run_host_store_write_for_test(None, Some(Box::new(staging)), move |context| {
                context
                    .execute_batch(&sql)
                    .map_err(coven_database::DbError::from)
            })
            .await
            .map(|_| ())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn with_test_storage(&self, storage: Arc<dyn CloudSyncObjectStorage>) -> Self {
        Self::new(
            self.database.clone(),
            storage,
            self.store_dir.clone(),
            self.identity.clone(),
            self.device_id.clone(),
            self.root.clone(),
        )
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn owner_recovery_for_test(
        &self,
    ) -> Result<RestoringStore<'_>, crate::sync::test_helpers::TestError> {
        Ok(self.authorize().await?.bind_restore_for_test())
    }

    #[cfg(test)]
    pub(crate) async fn prepare_wrapped_key_for_test(
        &self,
        recipient: &str,
        value: coven_protocol::wrapped_store_key::WrappedStoreKey,
    ) -> Result<
        coven_protocol::wrapped_store_key::PreparedWrappedStoreKey,
        crate::sync::test_helpers::TestError,
    > {
        let authorization = self.authorize().await?;
        authorization
            .prepare_wrapped_key_for_test(recipient, value)
            .await
            .map_err(Into::into)
    }

    #[cfg(test)]
    pub(crate) async fn membership_keyring_facts_for_test(
        &self,
    ) -> Result<([u8; 32], usize), crate::sync::test_helpers::TestError> {
        let authorization = self.authorize().await?;
        authorization
            .membership_keyring_facts_for_test()
            .await
            .map_err(Into::into)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn blob_key_fingerprint_for_test(
        &self,
        authority: &coven_protocol::blob::RowBlobAuthority,
        stored: &coven_protocol::blob::locator::StoredBlobRef,
    ) -> Result<Option<coven_keys::encryption::KeyFingerprint>, StoreError> {
        self.authorize_history()
            .await
            .map_err(StoreError::from)?
            .blob_key_fingerprint_for_test(authority, stored)
            .await
            .map_err(StoreError::from)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn announcement_stream_id_for_test(
        &self,
    ) -> Result<coven_protocol::membership::AuthorStreamId, StoreError> {
        let writer = self.authorize_writer().await.map_err(StoreError::from)?;
        Ok(writer.announcement_stream_id())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn sign_device_head_for_test(
        &self,
        commit: coven_protocol::store_commit::StoreBatchCommitRef,
        successor: coven_protocol::store_commit::SuccessorLink,
    ) -> Result<coven_protocol::store_commit::StoreDeviceHead, StoreError> {
        let writer = self.authorize_writer().await.map_err(StoreError::from)?;
        writer.sign_device_head_for_test(commit, successor)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn resign_snapshot_meta_for_test(
        &self,
        meta: coven_protocol::store_commit::SnapshotMeta,
    ) -> Result<coven_protocol::store_commit::SnapshotMeta, StoreError> {
        let writer = self.authorize_writer().await.map_err(StoreError::from)?;
        writer.resign_snapshot_meta_for_test(meta)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn parse_local_snapshot_meta_for_test(
        &self,
        bytes: &[u8],
        reference: &coven_protocol::store_commit::StoreSnapshotRef,
    ) -> Result<coven_protocol::store_commit::SnapshotMeta, StoreError> {
        let writer = self.authorize_writer().await.map_err(StoreError::from)?;
        writer.parse_snapshot_meta_for_test(bytes, reference)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn prepare_operation_plan_for_test(
        &self,
    ) -> Result<
        crate::sync::store::commit_publication::operation::commit_plan::StoreOperationCommitPlan,
        StoreError,
    > {
        let mut writer = self.authorize_writer().await.map_err(StoreError::from)?;
        writer.prepare_plan().await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn authorize_retained_outbound_for_test(
        &self,
        order: &coven_protocol::store_commit::StoreCommitOrder,
        candidate_membership_heads: &[coven_protocol::membership::MembershipHeadRef],
    ) -> Result<
        crate::sync::store::commit_verification::merge_history::MergeOutboundAuthorization,
        StoreError,
    > {
        let authorization = self.authorize().await.map_err(StoreError::from)?;
        authorization
            .authorize_retained_outbound_for_test(order, candidate_membership_heads)
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn owner_promotion_target_for_test(
        &self,
    ) -> Result<coven_protocol::store_commit::StoreDeviceRegistrationRef, StoreError> {
        let writer = self.authorize_writer().await.map_err(StoreError::from)?;
        Ok(writer.local_registration_ref_for_test())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn observe_excluded_candidate_head_for_test(
        &self,
        candidate: &coven_protocol::store_commit::StoreDeviceHead,
        candidate_commit: &coven_protocol::store_commit::StoreBatchCommit,
        candidate_object: &coven_protocol::objects::ExactObjectRef,
    ) -> Result<crate::sync::store::merge_conflict::ExcludedCandidateHeadObservation, StoreError>
    {
        let mut history = self.authorize_history().await.map_err(StoreError::from)?;
        let verified_commit = history
            .authenticate_commit_bytes(&candidate.commit, &candidate_commit.to_bytes())
            .await?;
        history
            .merge_conflict()
            .observe_excluded_candidate_head(candidate, &verified_commit, candidate_object)
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn cleanup_merge_candidate_for_test(
        &self,
        write_id: coven_protocol::write::WriteId,
    ) -> Result<(), StoreError> {
        let mut history = self.authorize_history().await.map_err(StoreError::from)?;
        history
            .cleanup_merge_candidate(write_id)
            .await
            .map_err(StoreError::from)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn complete_revoke_rotation_adoption_for_test(
        &self,
        pending_rotation: &dyn coven_storage::CloudSyncRotationStateAccess,
        adopted_generation: u64,
    ) -> Result<(), membership::MembershipMutationError> {
        self.authorize_writer()
            .await
            .expect("authorize Store writer")
            .complete_revoke_rotation_adoption_for_test(pending_rotation, adopted_generation)
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn membership_for_test(
        &self,
    ) -> Result<coven_protocol::membership::MembershipChain, StoreError> {
        self.authorize()
            .await
            .map(|authorization| authorization.membership_for_test())
            .map_err(StoreError::from)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn retained_merge_replay_inputs_for_test(
        &self,
    ) -> Result<Vec<coven_database::OwnedVerifiedMergeMaterialization>, coven_database::DbError>
    {
        self.database
            .retained_merge_replay_inputs(self.root.reference().clone())
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn resolved_store_device_state_for_test(
        &self,
        reference: &coven_protocol::store_commit::StoreDeviceStateRef,
    ) -> Result<coven_protocol::store_commit::ResolvedStoreDeviceState, coven_database::DbError>
    {
        self.database.resolved_store_device_state(reference).await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn retained_merge_materialization_for_test(
        &self,
        reference: coven_protocol::store_commit::StoreBatchCommitRef,
    ) -> Result<coven_database::OwnedVerifiedMergeMaterialization, coven_database::DbError> {
        self.database
            .retained_merge_materialization(self.root.reference().clone(), reference)
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn prepare_conflict_resolution_plan_for_test(
        &self,
        candidate_membership_heads: &[coven_protocol::membership::MembershipHeadRef],
    ) -> Result<(), StoreError> {
        self.authorize_writer()
            .await
            .map_err(StoreError::from)?
            .prepare_conflict_resolution_plan(candidate_membership_heads)
            .await?;
        Ok(())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn load_membership_head_for_test(
        &self,
        reference: &coven_protocol::membership::MembershipHeadRef,
    ) -> Result<coven_protocol::membership::AuthorHead, StoreError> {
        let mut history = self.authorize_history().await.map_err(StoreError::from)?;
        history
            .load_exact_membership_head_for_test(reference)
            .await
            .map_err(StoreError::from)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn load_membership_at_exact_heads_for_test(
        &self,
        heads: &[coven_protocol::membership::MembershipHeadRef],
        resolutions: &[coven_protocol::membership::StoreMembershipConflictResolutionRef],
    ) -> Result<coven_protocol::membership::MembershipChain, StoreError> {
        let mut history = self.authorize_history().await.map_err(StoreError::from)?;
        history
            .load_membership_at_exact_heads_for_test(heads, resolutions)
            .await
            .map_err(StoreError::from)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn project_membership_for_test(
        &self,
        candidate_heads: &[coven_protocol::membership::MembershipHeadRef],
    ) -> Result<coven_protocol::membership::MembershipChain, StoreError> {
        let history = self.authorize_history().await.map_err(StoreError::from)?;
        history
            .project_membership_to_verified_prefix(
                candidate_heads,
                &crate::sync::store::commit_verification::merge_history::VerifiedMergeMembershipPrefix::default(),
            )
            .await
            .map_err(StoreError::from)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn assert_deep_membership_projection_for_test(
        &self,
        heads: &[coven_protocol::membership::MembershipHeadRef],
    ) -> Result<(), StoreError> {
        let mut history = self.authorize_history().await.map_err(StoreError::from)?;
        history
            .assert_deep_membership_projection_for_test(heads)
            .await;
        Ok(())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn verify_device_join_attempt_for_test(
        &self,
        reference: &coven_protocol::store_commit::DeviceJoinAttemptRef,
        owner: &coven_protocol::store_commit::StoreDeviceRegistration,
    ) -> Result<(), StoreError> {
        let mut history = self.authorize_history().await.map_err(StoreError::from)?;
        history
            .verify_device_join_attempt_for_test(reference, owner)
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn exact_next_announcement_slot_for_test(
        &self,
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
        let mut history = self.authorize_history().await.map_err(StoreError::from)?;
        history
            .exact_next_announcement_slot_for_test(registration_ref, registration, previous)
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn load_commit_for_test(
        &self,
        reference: &coven_protocol::store_commit::StoreBatchCommitRef,
    ) -> Result<coven_protocol::store_commit::VerifiedStoreBatchCommit, StoreError> {
        let mut history = self.authorize_history().await.map_err(StoreError::from)?;
        history
            .load_commit(reference)
            .await
            .map_err(StoreError::from)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn load_commit_ancestry_until_for_test(
        &self,
        start: coven_protocol::store_commit::StoreBatchCommitRef,
        coverage: &coven_protocol::store_commit::CommitFrontier,
    ) -> Result<
        Vec<(
            coven_protocol::store_commit::StoreBatchCommitRef,
            coven_protocol::store_commit::VerifiedStoreBatchCommit,
        )>,
        StoreError,
    > {
        let mut history = self.authorize_history().await.map_err(StoreError::from)?;
        history
            .load_commit_ancestry_until_for_test(start, coverage)
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn load_registration_for_test(
        &self,
        reference: &coven_protocol::store_commit::StoreDeviceRegistrationRef,
    ) -> Result<coven_protocol::store_commit::StoreDeviceRegistration, StoreError> {
        let history = self.authorize_history().await.map_err(StoreError::from)?;
        Ok(history.load_registration(reference).await?.value)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn verify_installable_snapshots_for_test(
        &self,
        snapshots: &[coven_database::PublishedStoreSnapshot],
    ) -> Result<(), StoreError> {
        let mut history = self.authorize_history().await.map_err(StoreError::from)?;
        history
            .verify_installable_snapshots_for_test(snapshots)
            .await?;
        Ok(())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn open_circle_package_for_test(
        &self,
        access: &coven_protocol::circle_activation::CircleEpochAccess,
        commit: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
        reference: &coven_protocol::store_commit::CirclePackageRef,
    ) -> Result<Vec<u8>, StoreError> {
        let mut history = self.authorize_history().await.map_err(StoreError::from)?;
        history
            .open_circle_package_for_test(access, commit, reference)
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn pull_readiness_for_test(
        &self,
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
        let mut history = self
            .authorize_history()
            .await
            .expect("authorize Store history");
        history
            .pull_readiness_for_test(
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
        &self,
        references: impl IntoIterator<Item = coven_protocol::store_commit::StoreBatchCommitRef>,
        predecessors: impl IntoIterator<Item = coven_protocol::store_commit::StoreBatchCommitRef>,
    ) -> Result<
        crate::sync::store::commit_verification::merge_history::VerifiedMergeMembershipPrefix,
        pull::StorePullError,
    > {
        let mut history = self
            .authorize_history()
            .await
            .expect("authorize Store history");
        history
            .verified_merge_membership_prefix_for_test(references, predecessors)
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn retained_merge_history_frontier_for_test(
        &self,
        references: Vec<coven_protocol::store_commit::StoreBatchCommitRef>,
    ) -> Result<Vec<coven_database::RetainedMergeHistoryCheckpoint>, coven_database::DbError> {
        self.database
            .retained_merge_history_frontier(self.root.reference().clone(), references)
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn verified_circle_activation_for_test(
        &self,
        circle_id: coven_protocol::circle::CircleId,
        control: coven_protocol::circle::CircleControlCoord,
    ) -> Result<
        Option<coven_protocol::circle_activation::VerifiedCircleReference>,
        coven_database::DbError,
    > {
        self.database
            .verified_circle_activation(self.root.reference().clone(), circle_id, control)
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn finalized_circle_close_outcome_for_test(
        &self,
        circle_id: coven_protocol::circle::CircleId,
    ) -> Result<
        coven_protocol::circle::CircleEpochCloseOutcome,
        crate::sync::store::CircleOperationError,
    > {
        let (successor, _) = self
            .database
            .circle_authoring_context(circle_id, &coven_keys::keys::public_key_hex(&self.identity))
            .await?;
        let coven_protocol::circle::CircleControlState::ActiveEpoch(active) =
            successor.control.value.state()
        else {
            return Err(crate::sync::store::CircleOperationError::InvalidState(
                "finalized Circle control is not active".to_string(),
            ));
        };
        let coven_protocol::circle::CircleEpochOrigin::Closed { close_id, .. } =
            &active.common.origin
        else {
            return Err(crate::sync::store::CircleOperationError::InvalidState(
                "finalized Circle control does not name a close outcome".to_string(),
            ));
        };
        let activation = self
            .database
            .verified_circle_activation(
                self.root.reference().clone(),
                circle_id,
                successor.control.coord,
            )
            .await?
            .ok_or(crate::sync::store::CircleOperationError::MissingState(
                "finalized Circle activation",
            ))?;
        let outcome_ref = activation
            .reference
            .objects()
            .close_outcome
            .as_ref()
            .ok_or(crate::sync::store::CircleOperationError::MissingState(
                "finalized Circle close outcome",
            ))?;
        let context = coven_protocol::objects::ProtocolObjectContext::store_encrypted(
            self.root.reference().store_root_hash,
            coven_protocol::objects::ProtocolObjectDomain::CircleEpochCloseOutcome,
        );
        let prefix = coven_protocol::circle::circle_epoch_close_outcome_semantic_prefix(
            circle_id, *close_id,
        );
        let bytes = self
            .storage
            .read_protocol_object(&context, &outcome_ref.object, &prefix)
            .await
            .map_err(coven_protocol::objects::StoreObjectError::from)?;
        let coven_protocol::circle::CircleEpochCloseSlotValue::Outcome(outcome) =
            coven_protocol::circle::CircleEpochCloseSlotValue::parse(&bytes)?
        else {
            return Err(crate::sync::store::CircleOperationError::InvalidState(
                "finalized Circle close slot holds a cancellation".to_string(),
            ));
        };
        if coven_protocol::circle::CircleEpochCloseOutcomeRef::from_outcome(
            &outcome,
            outcome_ref.object.clone(),
        )? != *outcome_ref
        {
            return Err(crate::sync::store::CircleOperationError::InvalidState(
                "finalized Circle close outcome differs from its exact reference".to_string(),
            ));
        }
        Ok(outcome)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn circle_package_is_retained_for_replay_for_test(
        &self,
        target: coven_protocol::store_commit::CirclePackageRef,
        activation: coven_protocol::store_commit::StoreBatchCommitRef,
    ) -> Result<bool, coven_database::DbError> {
        self.database
            .circle_package_is_retained_for_replay(
                self.root.reference().clone(),
                target,
                activation,
            )
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn load_circle_acknowledgement_for_test(
        &self,
        reference: &coven_protocol::store_commit::CircleAckRef,
    ) -> Result<coven_protocol::store_commit::CircleAck, StoreAckError> {
        self.authorize_history()
            .await
            .map_err(StoreAckError::from)?
            .circles()
            .acknowledgements()
            .load(reference)
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn load_circle_activations_for_test(
        &self,
        commit_ref: &coven_protocol::store_commit::StoreBatchCommitRef,
        commit: &coven_protocol::store_commit::StoreBatchCommit,
        author: &coven_protocol::store_commit::StoreDeviceRegistration,
        routing_key: Option<&coven_protocol::circle::RowRoutingKey>,
    ) -> Result<
        coven_protocol::circle_activation::VerifiedCircleActivations,
        crate::sync::store::CircleOperationError,
    > {
        let verified = coven_protocol::store_commit::VerifiedStoreBatchCommit::parse(
            &commit.to_bytes(),
            self.root.reference().store_root_hash,
            commit_ref,
            author,
        )
        .map_err(crate::sync::store::CircleOperationError::from)?;
        let mut history = self
            .authorize_history()
            .await
            .map_err(crate::sync::store::CircleOperationError::from)?;
        history
            .circles()
            .activations()
            .load(&verified, &self.identity, routing_key)
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn load_applicable_circle_packages_for_test(
        &self,
        verified: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
        activations: &[coven_protocol::circle_activation::VerifiedCircleReference],
        author: &coven_protocol::store_commit::StoreDeviceRegistration,
        local_store_membership: pull::LocalStoreMembership,
    ) -> Result<Vec<pull::LoadedCirclePackage>, circles::CirclePackageReadError> {
        let mut history = self
            .authorize_history()
            .await
            .map_err(circles::CirclePackageReadError::from)?;
        history
            .circles()
            .packages()
            .load_applicable(verified, activations, author, local_store_membership)
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn protocol_root_for_test(&self) -> &StoreProtocolRoot {
        self.root.protocol()
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn root_ref_for_test(&self) -> &coven_protocol::store_commit::StoreRootRef {
        self.root.reference()
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn pending_device_join_observation_for_test(
        &self,
        pending: &crate::sync::store::DeviceJoinJournalDatabase,
        offer: &coven_protocol::store_commit::device_join_exchange::DeviceJoinOffer,
    ) -> Result<crate::sync::store::PendingDeviceJoinObservation<'_>, StorePullError> {
        if &offer.store_root != self.root.reference() {
            return Err(StorePullError::InvalidState(
                "pending device join belongs to another Store root".to_string(),
            ));
        }
        let history_verifier = HistoryConstructionAuthority::for_pending_device_join()
            .bind_verified(self.storage.as_ref(), self.root.clone())
            .await?;
        Ok(crate::sync::store::PendingDeviceJoinObservation::new(
            pending,
            &self.storage,
            history_verifier,
            offer.attempt_id,
        ))
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn open_pending_device_join_for_test(
        &self,
        pending: &crate::sync::store::DeviceJoinJournalDatabase,
        identity: &UserKeypair,
        offer: coven_protocol::store_commit::device_join_exchange::DeviceJoinOffer,
    ) -> Result<crate::sync::store::PendingDeviceJoinAuthority<'_>, DeviceJoinError> {
        let observation = self
            .pending_device_join_observation_for_test(pending, &offer)
            .await?;
        crate::sync::store::PendingDeviceJoinAuthority::open(observation, identity, offer).await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn prepare_snapshot_bootstrap_for_test(
        &self,
        membership_floor: &coven_protocol::membership::MembershipFloor,
        binary_schema_version: u32,
        target_path: &std::path::Path,
        restorer_identity: &UserKeypair,
    ) -> Result<crate::sync::store::PreparedSnapshotBootstrap<'_>, SnapshotError> {
        let history_verifier = HistoryConstructionAuthority::for_snapshot()
            .bind_verified(self.storage.as_ref(), self.root.clone())
            .await?;
        let cancel = tokio::sync::watch::channel(false).1;
        crate::sync::store::PreparedSnapshotBootstrap::prepare(
            &self.storage,
            history_verifier,
            membership_floor,
            binary_schema_version,
            target_path,
            restorer_identity,
            std::sync::Arc::new(|_| {}),
            &cancel,
        )
        .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn export_activated_device_continuation_for_test(
        &self,
    ) -> Result<coven_protocol::recovery::ActivatedContinuation, coven_database::DbError> {
        self.database
            .export_activated_device_continuation(&self.identity)
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn stage_acknowledgement_for_test(
        &self,
        frontier: coven_protocol::store_commit::CommitFrontier,
        sync_time: String,
    ) -> Result<
        crate::sync::store::acknowledgements::StagedStoreAcknowledgement,
        crate::sync::store::acknowledgements::StoreAckError,
    > {
        let mut writer = self
            .authorize_writer()
            .await
            .map_err(crate::sync::store::acknowledgements::StoreAckError::from)?;
        // Every scoped test store routes its rows under this key — the same one
        // `ensure_device_join_snapshot_for_test` captures images with. Staging
        // an acknowledgement now advances the replay baseline, which rebuilds
        // the image by replay, so the key has to be here too. Unscoped stores
        // never look at it.
        let routing_encryption = coven_keys::encryption::EncryptionService::from_key([42; 32]);
        writer
            .acknowledgements()
            .stage_acknowledgement(frontier, sync_time, Some(&routing_encryption))
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn drain_acknowledgements_for_test(
        &self,
    ) -> Result<u64, crate::sync::store::acknowledgements::StoreAckError> {
        let mut writer = self
            .authorize_writer()
            .await
            .map_err(crate::sync::store::acknowledgements::StoreAckError::from)?;
        writer.acknowledgements().drain_acknowledgements().await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn prepare_acknowledgement_activation_for_test(
        &self,
        acknowledgement: coven_protocol::store_commit::StoreAckRef,
        candidate: coven_protocol::prepared_commit::PreparedStoreOperationCommit,
    ) -> Result<(), coven_database::DbError> {
        self.database
            .prepare_acknowledgement_activation(acknowledgement, candidate)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn stage_circle_acknowledgements_for_test(
        &self,
        frontier: &coven_protocol::store_commit::CommitFrontier,
        sync_time: &str,
    ) -> Result<(), crate::sync::store::acknowledgements::StoreAckError> {
        self.authorize_writer()
            .await
            .map_err(crate::sync::store::acknowledgements::StoreAckError::from)?
            .circles()
            .stage_acknowledgements(frontier, sync_time)
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn publish_snapshot_for_test(
        &self,
        snapshot: coven_database::CreatedSnapshot,
        coverage: coven_protocol::store_commit::CommitFrontier,
        created_at: String,
    ) -> Result<
        coven_protocol::store_commit::SnapshotMeta,
        crate::sync::store::snapshots::SnapshotError,
    > {
        let mut writer = self
            .authorize_writer()
            .await
            .map_err(crate::sync::store::snapshots::SnapshotError::from)?;
        writer
            .snapshots()
            .push_store_snapshot(
                snapshot,
                coverage,
                self.database.schema_version(),
                created_at,
            )
            .await
    }

    #[cfg(test)]
    pub(crate) async fn load_founder_registration_for_test(
        &self,
    ) -> Result<
        coven_protocol::objects::VerifiedObject<
            coven_protocol::store_commit::StoreDeviceRegistration,
        >,
        StoreError,
    > {
        let mut history = self.authorize_history().await.map_err(StoreError::from)?;
        history.load_founder_registration_for_test().await
    }

    #[cfg(test)]
    pub(crate) async fn load_founder_registration_twice_for_test(&self) -> Result<(), StoreError> {
        let mut history = self.authorize_history().await.map_err(StoreError::from)?;
        history.load_founder_registration_twice_for_test().await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn prepare_merge_history_successor_for_test(
        &self,
        verified_commit: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
        recovery_author: Option<&coven_protocol::store_commit::StoreDeviceRegistrationRef>,
        evidence: crate::sync::store::commit_verification::merge_history::MergeHistorySuccessorEvidence,
    ) -> Result<
        crate::sync::store::commit_verification::merge_history::PreparedMergeHistorySuccessor,
        StoreError,
    > {
        let mut authorized = self.authorize().await.map_err(StoreError::from)?;
        authorized
            .prepare_merge_history_successor_for_test(verified_commit, recovery_author, evidence)
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn prepare_device_join_bootstrap_for_test(
        &self,
        bootstrap_cut: &coven_protocol::store_commit::StoreHistoryCut,
        attempt_activation: &coven_protocol::store_commit::StoreBatchCommitRef,
        membership_state: &coven_protocol::circle_control::StoreMembershipStateRef,
        installed: &coven_protocol::store_commit::CommitFrontier,
    ) -> Result<coven_database::DeviceJoinBootstrapPlan, StoreError> {
        let mut history = self.authorize_history().await.map_err(StoreError::from)?;
        history
            .prepare_device_join_bootstrap_for_test(
                bootstrap_cut,
                attempt_activation,
                membership_state,
                installed,
            )
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn load_store_package_for_test(
        &self,
        reference: &coven_protocol::store_commit::StoreBatchCommitRef,
    ) -> Result<Option<coven_protocol::objects::VerifiedObject<Vec<u8>>>, StoreError> {
        let mut history = self.authorize_history().await.map_err(StoreError::from)?;
        history.load_store_package_for_test(reference).await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn load_store_ack_for_test(
        &self,
        reference: &coven_protocol::store_commit::StoreAckRef,
        registration: &coven_protocol::store_commit::StoreDeviceRegistration,
    ) -> Result<coven_protocol::store_commit::StoreAck, StoreError> {
        let mut history = self.authorize_history().await.map_err(StoreError::from)?;
        history
            .load_store_ack_for_test(reference, registration)
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn load_head_for_test(
        &self,
        reference: &coven_protocol::store_commit::StoreDeviceHeadRef,
        registration: &coven_protocol::store_commit::StoreDeviceRegistration,
        commit: &coven_protocol::store_commit::StoreBatchCommitRef,
    ) -> Result<coven_protocol::store_commit::StoreDeviceHead, StoreError> {
        let mut history = self.authorize_history().await.map_err(StoreError::from)?;
        history
            .load_head_for_test(reference, registration, commit)
            .await
    }
}

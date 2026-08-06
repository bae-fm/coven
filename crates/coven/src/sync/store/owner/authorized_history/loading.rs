use super::*;

impl<'storage> AuthorizedStoreHistory<'storage> {
    pub(crate) async fn stage_verified_blob_plaintext(
        &self,
        authority: &coven_protocol::blob::RowBlobAuthority,
        stored: &coven_protocol::blob::locator::StoredBlobRef,
        destination: &std::path::Path,
    ) -> Result<coven_foundation::local_file::AtomicStagedFile, crate::sync::BlobCacheError> {
        self.blob_source
            .stage_verified_plaintext(authority, stored, destination)
            .await
    }

    pub(crate) async fn verify_blob_plaintext(
        &self,
        authority: &coven_protocol::blob::RowBlobAuthority,
        stored: &coven_protocol::blob::locator::StoredBlobRef,
        retain: bool,
    ) -> Result<(), crate::sync::store::blob::BlobDownloadFailureCause> {
        self.blob_source
            .verify_plaintext(&self.blob_cache, authority, stored, retain)
            .await
    }

    pub(crate) fn root(&self) -> &StoreRootRef {
        self.history_verifier.verified_root().reference()
    }

    pub(crate) fn verified_root_object(
        &self,
    ) -> &coven_protocol::objects::VerifiedObject<StoreProtocolRoot> {
        self.history_verifier.verified_root().object()
    }

    pub(crate) async fn authenticate_commit_bytes(
        &mut self,
        reference: &StoreBatchCommitRef,
        bytes: &[u8],
    ) -> Result<
        coven_protocol::store_commit::VerifiedStoreBatchCommit,
        coven_protocol::objects::StoreObjectError,
    > {
        self.history_verifier
            .authenticate_bytes(reference, bytes)
            .await
    }

    pub(crate) async fn authenticate_blocked_candidate(
        &mut self,
        candidate: &crate::database::BlockedMergeCandidate,
    ) -> Result<
        coven_protocol::store_commit::VerifiedStoreBatchCommit,
        crate::sync::store::StoreError,
    > {
        self.history_verifier
            .authenticate_blocked_candidate(candidate)
            .await
    }

    pub(crate) async fn load_commit(
        &mut self,
        reference: &StoreBatchCommitRef,
    ) -> Result<coven_protocol::store_commit::VerifiedStoreBatchCommit, pull::StorePullError> {
        self.history_verifier.load_ref(reference).await
    }

    pub(crate) async fn load_registration(
        &self,
        reference: &StoreDeviceRegistrationRef,
    ) -> Result<
        coven_protocol::objects::VerifiedObject<
            coven_protocol::store_commit::StoreDeviceRegistration,
        >,
        coven_protocol::objects::StoreObjectError,
    > {
        self.history_verifier.load_registration(reference).await
    }

    pub(crate) async fn verify_membership_control(
        &mut self,
        verified_commit: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
    ) -> Result<crate::sync::store::circle_controls::activation::VerifiedCircleActivations, String>
    {
        let root = self.history_verifier.verified_root().reference().clone();
        if verified_commit.store_root_hash() != root.store_root_hash {
            return Err(
                "authenticated Merge membership control belongs to another Store root".into(),
            );
        }
        let commit_ref = verified_commit.reference();
        let commit = verified_commit.value();
        self.history_verifier
            .verify_refs(pull::commit_predecessor_references(commit))
            .await
            .map_err(|error| error.to_string())?;
        let predecessor_state = self
            .history_verifier
            .verified_predecessor_state(commit)
            .map_err(|error| error.to_string())?;
        let verified_membership_activations = self
            .history_verifier
            .verified_membership_prefix(pull::commit_predecessor_references(commit))
            .map_err(|error| error.to_string())?;
        let pending_resolution = self
            .history_verifier
            .verify_resolution_activation_acceptance(commit)
            .await
            .map_err(|error| error.to_string())?;
        let predecessor_membership = self
            .history_verifier
            .load_predecessor_membership_at_verified_prefix(
                &commit.membership_state,
                &verified_membership_activations,
                pending_resolution.as_ref(),
            )
            .await
            .map_err(|error| match error {
                RegistrationLoadError::Object(error) => error.to_string(),
                RegistrationLoadError::Invalid(error) => error,
            })?;
        verify_merge_membership_state_ref(
            &commit.membership_state,
            &predecessor_membership,
            &predecessor_state,
        )
        .map_err(|error| error.to_string())?;
        self.history_verifier
            .verify_membership_control_with_retained_history(
                commit_ref,
                commit,
                &predecessor_membership,
                &predecessor_state,
                pending_resolution.as_ref(),
            )
            .await
            .map(|(activations, _)| activations)
    }

    pub(crate) async fn load_local_device_operations(
        &mut self,
        verified_commit: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
        membership: &MembershipChain,
        state_ref: &StoreDeviceStateRef,
        state: ResolvedStoreDeviceState,
    ) -> Result<coven_protocol::store_commit::VerifiedStoreDeviceOperations, pull::StorePullError>
    {
        let resolver =
            crate::sync::store::owner::verification::DeviceStateResolver::Database(&self.database);
        self.history_verifier
            .load_local_device_operations_with_resolver(
                &resolver,
                verified_commit,
                membership,
                state_ref,
                state,
            )
            .await
    }

    pub(crate) async fn retain_acknowledgement(
        &self,
        activating_commit: &StoreBatchCommitRef,
        activating_commit_value: &coven_protocol::store_commit::StoreBatchCommit,
        registration: &coven_protocol::store_commit::StoreDeviceRegistration,
        reference: coven_protocol::store_commit::StoreAckRef,
        value: coven_protocol::store_commit::StoreAck,
    ) -> Result<coven_protocol::store_commit::RetainedVerifiedActivatedAck, pull::StorePullError>
    {
        self.history_verifier
            .retain_acknowledgement(
                activating_commit,
                activating_commit_value,
                registration,
                reference,
                value,
            )
            .await
    }

    pub(crate) async fn derive_local_post_device_state(
        &self,
        commit: &coven_protocol::store_commit::StoreBatchCommit,
        predecessor_state: ResolvedStoreDeviceState,
        registrations: &[coven_protocol::store_commit::ActivatedStoreDeviceRegistration],
        device_operations: coven_protocol::store_commit::VerifiedStoreDeviceOperations,
    ) -> Result<ResolvedStoreDeviceState, pull::StorePullError> {
        self.history_verifier
            .derive_local_post_device_state(
                commit,
                predecessor_state,
                registrations,
                device_operations,
            )
            .await
    }

    pub(crate) async fn load_store_snapshot(
        &self,
        registration_ref: &coven_protocol::store_commit::StoreDeviceRegistrationRef,
        registration: &coven_protocol::store_commit::StoreDeviceRegistration,
        reference: &coven_protocol::store_commit::StoreSnapshotRef,
    ) -> Result<
        (
            coven_protocol::store_commit::StoreSnapshotRef,
            coven_protocol::store_commit::SnapshotMeta,
        ),
        coven_protocol::objects::StoreObjectError,
    > {
        self.history_verifier
            .load_store_snapshot(registration_ref, registration, reference)
            .await
    }

    pub(crate) async fn load_store_snapshot_stream(
        &self,
        registration_ref: &coven_protocol::store_commit::StoreDeviceRegistrationRef,
        registration: &coven_protocol::store_commit::StoreDeviceRegistration,
    ) -> Result<Vec<crate::database::PublishedStoreSnapshot>, snapshot::SnapshotError> {
        self.history_verifier
            .load_store_snapshot_stream(registration_ref, registration)
            .await
    }

    pub(crate) async fn verify_snapshots_for_acknowledgement(
        &mut self,
        snapshots: &[crate::database::PublishedStoreSnapshot],
    ) -> Result<(), pull::StorePullError> {
        self.history_verifier
            .verify_snapshots_for_acknowledgement(snapshots)
            .await
    }

    pub(crate) async fn select_acknowledgement_snapshot(
        &mut self,
        frontier: &CommitFrontier,
        device_state: &StoreDeviceStateRef,
    ) -> Result<Option<coven_protocol::store_commit::StoreSnapshotLocator>, writer::StoreAckError>
    {
        let registrations = self
            .database
            .activated_store_device_registration_records()
            .await?;
        let mut candidates = Vec::new();
        for registration in registrations {
            for snapshot in self
                .history_verifier
                .load_store_snapshot_stream(registration.reference(), registration.value())
                .await?
            {
                if !frontier.covers(&snapshot.meta.coverage)
                    || snapshot.meta.state.devices.state_hash() != device_state.state_hash()
                    || snapshot.meta.state.devices.recovery() != device_state.recovery()
                {
                    continue;
                }
                candidates.push(snapshot);
            }
        }
        if candidates.is_empty() {
            return Ok(None);
        }
        self.verify_snapshots_for_acknowledgement(&candidates)
            .await
            .map_err(|error| snapshot::SnapshotError::UnauthorizedAuthor(error.to_string()))?;
        Ok(
            snapshot::select_maximal_store_snapshot(candidates).map(|snapshot| {
                coven_protocol::store_commit::StoreSnapshotLocator {
                    author_registration: snapshot.meta.author_registration.clone(),
                    snapshot: snapshot.reference,
                }
            }),
        )
    }

    pub(crate) async fn load_current_membership(
        &mut self,
        owner_pubkey: &str,
    ) -> Result<MembershipChain, crate::sync::store::membership::MembershipOpsError> {
        let _membership_load = self.database.membership_load_permit().await;
        let cursors = self
            .database
            .membership_head_cursors()
            .await
            .map_err(|error| {
                crate::sync::store::membership::MembershipOpsError::Database(error.to_string())
            })?;
        let chain = Box::pin(
            self.history_verifier
                .load_exact_anchored_membership(&cursors.head_refs, Some(owner_pubkey)),
        )
        .await?;
        self.database
            .persist_membership_head_cursors(chain.head_refs().to_vec())
            .await
            .map_err(|error| {
                crate::sync::store::membership::MembershipOpsError::Database(error.to_string())
            })?;
        Ok(chain)
    }

    pub(crate) async fn load_and_install_owner_membership(
        &mut self,
        owner_pubkey: &str,
    ) -> Result<MembershipChain, crate::sync::store::membership::AnchoredChainError> {
        let _membership_load = self.database.membership_load_permit().await;
        let cursors = self
            .database
            .membership_head_cursors()
            .await
            .map_err(|error| {
                crate::sync::store::membership::AnchoredChainError::LoadFailed(error.to_string())
            })?;
        let chain = Box::pin(
            self.history_verifier
                .load_exact_anchored_membership(&cursors.head_refs, Some(owner_pubkey)),
        )
        .await?;
        let root = self.history_verifier.verified_root().reference().clone();
        let root_bytes = self.history_verifier.verified_root().object().bytes.clone();
        let protocol_root = self.history_verifier.verified_root().protocol().clone();
        let founder = chain.founder_coord().ok_or_else(|| {
            crate::sync::store::membership::AnchoredChainError::LoadFailed(
                "owner-anchored membership chain is empty".to_string(),
            )
        })?;
        let founder_head_ref = chain
            .head_ref_for_stream(
                &founder.author_pubkey,
                &founder.author_owner_grant,
                founder.stream_id,
            )
            .cloned()
            .ok_or_else(|| {
                crate::sync::store::membership::AnchoredChainError::LoadFailed(
                    "owner-anchored membership chain has no exact founder head".to_string(),
                )
            })?;
        let founder_head = self
            .history_verifier
            .load_exact_membership_head(&founder_head_ref)
            .await?;
        let founder_registration_ref = founder_head.body.author_registration.clone();
        let founder_registration = self
            .history_verifier
            .load_registration(&founder_registration_ref)
            .await
            .map_err(crate::sync::store::membership::AnchoredChainError::from_store_object)?;
        let founder_registration_bytes = founder_registration.bytes;
        let founder_registration = founder_registration.value;
        if founder_registration.author_pubkey != owner_pubkey
            || !matches!(
                founder_registration.origin,
                coven_protocol::store_commit::StoreDeviceRegistrationOrigin::Founder { .. }
            )
        {
            return Err(
                crate::sync::store::membership::AnchoredChainError::LoadFailed(
                    "founder head registration is not activated by the Store root".to_string(),
                ),
            );
        }
        if protocol_root.descriptor.founder_pubkey != owner_pubkey {
            return Err(
                crate::sync::store::membership::AnchoredChainError::LoadFailed(
                    "owner anchor differs from the Store root founder".to_string(),
                ),
            );
        }
        let founder_genesis = coven_protocol::store_commit::ResolvedStoreDeviceState::founder(
            &root,
            founder_registration_ref.clone(),
            &protocol_root.descriptor.founder_pubkey,
            protocol_root.descriptor.founder_grant.clone(),
            &protocol_root.descriptor.founder_recovery,
        )
        .map_err(|error| {
            crate::sync::store::membership::AnchoredChainError::LoadFailed(error.to_string())
        })?;
        self.database
            .install_store_owner_anchor(
                root,
                root_bytes,
                founder_registration_ref,
                founder_registration,
                founder_registration_bytes,
                founder_genesis,
                owner_pubkey.to_string(),
                crate::database::InitialStoreMembershipAuthority {
                    head_refs: chain.head_refs().to_vec(),
                },
            )
            .await
            .map_err(|error| {
                crate::sync::store::membership::AnchoredChainError::LoadFailed(error.to_string())
            })?;
        Ok(chain)
    }

    pub(crate) async fn project_membership_to_verified_prefix(
        &self,
        candidate_heads: &[MembershipHeadRef],
        prefix: &VerifiedMergeMembershipPrefix,
    ) -> Result<MembershipChain, crate::sync::store::membership::AnchoredChainError> {
        self.history_verifier
            .project_membership_to_verified_prefix(candidate_heads, prefix)
            .await
    }
}

use super::*;

impl<'storage> AuthorizedStoreHistory<'storage> {
    pub(crate) async fn stage_verified_blob_plaintext(
        &self,
        authority: &coven_protocol::blob::RowBlobAuthority,
        stored: &coven_protocol::blob::locator::StoredBlobRef,
        stage: coven_foundation::local_file::AtomicStagedFile,
        progress: coven_storage::cloud::DownloadProgress,
    ) -> Result<coven_foundation::local_file::AtomicStagedFile, crate::sync::BlobCacheError> {
        self.blob_source
            .stage_verified_plaintext(authority, stored, stage, progress)
            .await
    }

    /// The membership objects a reader of `membership`'s frontier would have to
    /// fetch, in the form a snapshot publishes them.
    pub(crate) async fn membership_rollup_parts(
        &mut self,
        membership: &MembershipChain,
    ) -> Result<
        (
            Vec<coven_protocol::store_commit::MembershipRollupStream>,
            Vec<coven_protocol::store_commit::MembershipRollupResolution>,
        ),
        crate::sync::store::membership::AnchoredChainError,
    > {
        let owner = self
            .history_verifier
            .verified_root()
            .protocol()
            .descriptor
            .founder_pubkey
            .clone();
        let (_, traversed) = self
            .history_verifier
            .load_exact_anchored_membership_traversal(membership.head_refs(), Some(&owner))
            .await?;
        Ok(traversed.into_rollup_parts())
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
        candidate: &coven_database::BlockedMergeCandidate,
    ) -> Result<
        coven_protocol::store_commit::VerifiedStoreBatchCommit,
        crate::sync::store::StoreError,
    > {
        self.history_verifier
            .authenticate_blocked_candidate(candidate)
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
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
    ) -> Result<coven_protocol::circle_activation::VerifiedCircleActivations, pull::StorePullError>
    {
        let root = self.history_verifier.verified_root().reference().clone();
        if verified_commit.store_root_hash() != root.store_root_hash {
            return Err(pull::StorePullError::InvalidState(
                "authenticated Merge membership control belongs to another Store root".into(),
            ));
        }
        let commit_ref = verified_commit.reference();
        let commit = verified_commit.value();
        self.history_verifier
            .verify_refs(pull::commit_predecessor_references(commit))
            .await?;
        let predecessor_state = self.history_verifier.verified_predecessor_state(commit)?;
        let verified_membership_activations = self
            .history_verifier
            .verified_membership_prefix(pull::commit_predecessor_references(commit))?;
        let pending_resolution = self
            .history_verifier
            .verify_resolution_activation_acceptance(commit)
            .await?;
        let predecessor_membership = self
            .history_verifier
            .load_predecessor_membership_at_verified_prefix(
                &commit.membership_state,
                &verified_membership_activations,
                pending_resolution.as_ref(),
            )
            .await
            .map_err(pull::StorePullError::from)?;
        verify_merge_membership_state_ref(
            &commit.membership_state,
            &predecessor_membership,
            &predecessor_state,
        )?;
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
            crate::sync::store::commit_verification::commit::DeviceStateResolver::Database(
                &self.database,
            );
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

    /// Resolve the snapshot this device has already acknowledged, ready to
    /// stand on.
    ///
    /// `Err` is never the answer to "there is nothing to do" — every way of
    /// having nothing to do is a [`ReplayBaselineDecline`], so the cycle can
    /// say which one it hit instead of printing a silent nothing.
    pub(crate) async fn resolve_acknowledged_snapshot(
        &mut self,
        registration: &StoreDeviceRegistrationRef,
    ) -> Result<
        Result<
            crate::sync::store::commit_verification::merge_history::SelectedInstallableStoreSnapshot,
            crate::sync::store::ReplayBaselineDecline,
        >,
        crate::sync::store::acknowledgements::StoreAckError,
    >{
        use crate::sync::store::ReplayBaselineDecline;

        let Some(locator) = self
            .history_verifier
            .newest_acknowledged_snapshot(registration)
        else {
            return Ok(Err(ReplayBaselineDecline::NoAcknowledgedSnapshot));
        };
        let generation = locator.snapshot.generation;
        // The steady state, answered without asking the provider anything: this
        // baseline was installed from that very snapshot, so there is nothing
        // to load and nothing to stand on again.
        if self
            .history_verifier
            .replay_baseline_stands_on(&locator.snapshot)
        {
            return Ok(Err(ReplayBaselineDecline::BaselineAtCoverage {
                generation,
            }));
        }
        let author = self
            .database
            .activated_store_device_registration_records()
            .await?
            .into_iter()
            .find(|record| record.reference() == &locator.author_registration);
        let Some(author) = author else {
            return Ok(Err(ReplayBaselineDecline::SnapshotAuthorInactive {
                generation,
            }));
        };
        let snapshot = self
            .history_verifier
            .load_acknowledged_snapshot(&locator, author.value())
            .await
            .map_err(crate::sync::store::acknowledgements::StoreAckError::from)?;
        let Some(snapshot) = snapshot else {
            return Ok(Err(ReplayBaselineDecline::SnapshotUnavailable {
                generation,
            }));
        };
        if self
            .history_verifier
            .replay_baseline_covers(&snapshot.meta.coverage)
        {
            return Ok(Err(ReplayBaselineDecline::BaselineAtCoverage {
                generation,
            }));
        }
        let verified = self
            .history_verifier
            .verify_acknowledged_store_snapshot(&snapshot)
            .await
            .map_err(crate::sync::store::snapshots::SnapshotError::from)?;
        let Some(verified) = verified else {
            return Ok(Err(ReplayBaselineDecline::SnapshotRejected { generation }));
        };
        Ok(Ok(
            crate::sync::store::commit_verification::merge_history::SelectedStoreSnapshot {
                snapshot,
                verified,
            },
        ))
    }

    /// The newest snapshot this device could acknowledge next, verified as
    /// installable.
    ///
    /// What a device *may say next* — not what it has already said. A snapshot
    /// whose device state the store has moved past fails these filters while
    /// remaining exactly what this device acknowledged, which is why the
    /// baseline advance is licensed elsewhere.
    pub(crate) async fn select_acknowledgement_snapshot(
        &mut self,
        frontier: &CommitFrontier,
        device_state: &StoreDeviceStateRef,
    ) -> Result<
        Option<
            crate::sync::store::commit_verification::merge_history::SelectedInstallableStoreSnapshot,
        >,
        crate::sync::store::acknowledgements::StoreAckError,
    >{
        let registrations = self
            .database
            .activated_store_device_registration_records()
            .await?;
        let mut published = Vec::new();
        for registration in registrations {
            published.extend(
                self.history_verifier
                    .load_store_snapshot_stream(registration.reference(), registration.value())
                    .await?,
            );
        }
        let candidates = published
            .into_iter()
            .filter(|snapshot| {
                // A snapshot this device's replay baseline already stands past
                // is dropped rather than verified: it would need the history
                // the baseline retired, and acknowledging it would claim less
                // than the device holds.
                frontier.covers(&snapshot.meta.coverage)
                    && !self
                        .history_verifier
                        .replay_baseline_stands_past(&snapshot.meta.coverage)
                    && snapshot.meta.state.devices.state_hash() == device_state.state_hash()
                    && snapshot.meta.state.devices.recovery() == device_state.recovery()
            })
            .collect::<Vec<_>>();
        if candidates.is_empty() {
            return Ok(None);
        }
        Ok(self
            .history_verifier
            .select_maximal_installable_store_snapshot(candidates)
            .await
            .map_err(crate::sync::store::snapshots::SnapshotError::from)?)
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
            .map_err(crate::sync::store::membership::MembershipOpsError::Database)?;
        let chain = Box::pin(
            self.history_verifier
                .load_exact_anchored_membership(&cursors.head_refs, Some(owner_pubkey)),
        )
        .await?;
        self.database
            .persist_membership_head_cursors(chain.head_refs().to_vec())
            .await
            .map_err(crate::sync::store::membership::MembershipOpsError::Database)?;
        Ok(chain)
    }

    /// Load the owner-anchored membership chain and install it as this
    /// device's owner anchor.
    ///
    /// `carried` is a chain this operation already walked and verified for the
    /// same Store root — a joining device walks one to open its cloud home
    /// before it ever opens a database. Walking a membership stream always
    /// starts at its founder anchor and runs to the end, so a second walk
    /// re-reads every head the first one did; when the carried chain already
    /// reaches the durable cursors, it is exactly what the second walk would
    /// produce and is used instead. A chain that falls short of the cursors is
    /// discarded and the walk runs, so this never installs less history than
    /// the device already has.
    pub(crate) async fn load_and_install_owner_membership(
        &mut self,
        owner_pubkey: &str,
        carried: Option<MembershipChain>,
    ) -> Result<MembershipChain, crate::sync::store::membership::AnchoredChainError> {
        let _membership_load = self.database.membership_load_permit().await;
        let cursors = self
            .database
            .membership_head_cursors()
            .await
            .map_err(crate::sync::store::membership::AnchoredChainError::from)?;
        let chain = match carried.filter(|chain| chain.covers_heads(&cursors.head_refs)) {
            Some(chain) => chain,
            None => {
                Box::pin(
                    self.history_verifier
                        .load_exact_anchored_membership(&cursors.head_refs, Some(owner_pubkey)),
                )
                .await?
            }
        };
        let root = self.history_verifier.verified_root().reference().clone();
        let root_object = self.history_verifier.verified_root().object().clone();
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
        if root_object.value.descriptor.founder_pubkey != owner_pubkey {
            return Err(
                crate::sync::store::membership::AnchoredChainError::LoadFailed(
                    "owner anchor differs from the Store root founder".to_string(),
                ),
            );
        }
        let owner_anchor = coven_database::StoreOwnerAnchor::new(
            root,
            root_object,
            founder_registration_ref.clone(),
            founder_registration,
        )
        .map_err(crate::sync::store::membership::AnchoredChainError::from)?;
        self.database
            .install_store_owner_anchor(
                owner_anchor,
                coven_database::InitialStoreMembershipAuthority {
                    head_refs: chain.head_refs().to_vec(),
                },
            )
            .await
            .map_err(crate::sync::store::membership::AnchoredChainError::from)?;
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

use super::*;

impl<'storage> AuthorizedStoreHistory<'storage> {
    pub(crate) async fn candidate_grant_retirement(
        &mut self,
        candidate: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
    ) -> Result<
        Option<(
            MembershipChain,
            coven_protocol::store_commit::StorePublicationRef,
        )>,
        pull::StorePullError,
    > {
        self.history_verifier
            .candidate_grant_retirement(&self.database, candidate)
            .await
    }

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
        Vec<coven_protocol::store_commit::MembershipRollupStream>,
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
        self.history_verifier
            .membership_rollup_parts(traversed)
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
        // Another writer can install the predecessors after this history owner
        // opens. Verify against the same retained authority used by preparation.
        self.seed_retained_history().await?;
        self.history_verifier
            .verify_refs(pull::commit_predecessor_references(commit))
            .await?;
        let predecessor_state = self.history_verifier.verified_predecessor_state(commit)?;
        let verified_membership_activations = self
            .history_verifier
            .verified_membership_prefix(pull::commit_predecessor_references(commit))?;
        let predecessor_membership = self
            .history_verifier
            .load_predecessor_membership_at_verified_prefix(
                &commit.membership_state,
                &verified_membership_activations,
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
            )
            .await
    }

    pub(crate) async fn load_local_device_operations(
        &mut self,
        verified_commit: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
        control_entry: Option<&coven_protocol::membership::MembershipEntry>,
        membership: &MembershipChain,
        state_ref: &StoreDeviceStateRef,
        state: ResolvedStoreDeviceState,
    ) -> Result<coven_protocol::store_commit::VerifiedStoreDeviceOperations, pull::StorePullError>
    {
        self.history_verifier
            .load_local_device_operations(
                verified_commit,
                control_entry,
                membership,
                state_ref,
                state,
            )
            .await
    }

    pub(crate) fn retain_acknowledgement(
        &self,
        activating_commit: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
        reference: coven_protocol::store_commit::StoreAckRef,
        value: coven_protocol::store_commit::StoreAck,
    ) -> Result<coven_protocol::store_commit::RetainedVerifiedActivatedAck, pull::StorePullError>
    {
        self.history_verifier
            .retain_acknowledgement(activating_commit, reference, value)
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

    /// Resolve the latest installed accepted snapshot, ready to stand on.
    ///
    /// `Err` is never the answer to "there is nothing to do" — every way of
    /// having nothing to do is a [`ReplayBaselineDecline`], so the cycle can
    /// say which one it hit instead of printing a silent nothing.
    pub(crate) async fn resolve_accepted_snapshot(
        &mut self,
    ) -> Result<
        Result<
            crate::sync::store::commit_verification::merge_history::SelectedStoreSnapshot,
            crate::sync::store::ReplayBaselineDecline,
        >,
        crate::sync::store::acknowledgements::StoreAckError,
    > {
        use crate::sync::store::ReplayBaselineDecline;

        let Some(snapshot) = self
            .history_verifier
            .load_installed_current_accepted_snapshot(&self.database)
            .await
            .map_err(crate::sync::store::snapshots::SnapshotError::from)?
        else {
            return Ok(Err(ReplayBaselineDecline::NoAcceptedSnapshot));
        };
        if !self
            .database
            .replay_baseline_would_advance(
                snapshot.reference.clone(),
                snapshot.meta.coverage.clone(),
            )
            .await?
        {
            return Ok(Err(ReplayBaselineDecline::BaselineAtCoverage {
                snapshot: snapshot.reference,
            }));
        }
        let verified = match self
            .history_verifier
            .verify_installable_snapshot(&snapshot)
            .await
        {
            Ok(verified) => verified,
            Err(error) => {
                return Err(crate::sync::store::snapshots::SnapshotError::from(error).into());
            }
        };
        Ok(Ok(
            crate::sync::store::commit_verification::merge_history::SelectedStoreSnapshot {
                snapshot,
                verified,
            },
        ))
    }

    pub(crate) async fn load_current_membership(
        &mut self,
        owner_pubkey: &str,
    ) -> Result<MembershipChain, crate::sync::store::membership::MembershipOpsError> {
        let _membership_load = self.database.membership_load_permit().await;
        let (publication, _) = self
            .history_verifier
            .load_store_publications_for_replay(&self.database)
            .await
            .map_err(crate::sync::store::membership::AnchoredChainError::from)?;
        let cursors = self
            .database
            .membership_head_cursors()
            .await
            .map_err(crate::sync::store::membership::MembershipOpsError::Database)?;
        let chain = self
            .history_verifier
            .membership_at_accepted_publication(&publication, &cursors.head_refs)
            .await
            .map_err(crate::sync::store::membership::AnchoredChainError::from)?;
        if !chain.is_founded_by(owner_pubkey) {
            return Err(
                crate::sync::store::membership::AnchoredChainError::FounderMismatch {
                    founder: chain.founder_pubkey().map(str::to_string),
                    owner: owner_pubkey.to_string(),
                }
                .into(),
            );
        }
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
                let installed_owner = self
                    .database
                    .get_protocol_state(coven_protocol::membership::OWNER_PUBKEY_STATE_KEY)
                    .await
                    .map_err(crate::sync::store::membership::AnchoredChainError::from)?;
                // Installing the owner anchor creates the initial replay baseline.
                // Before that installation the rooted founder chain is the authority;
                // a reopened Store must load its accepted history before walking
                // cursors that can name Store-activated membership changes.
                if installed_owner.is_some() {
                    Box::pin(
                        self.history_verifier
                            .load_store_publications_for_replay(&self.database),
                    )
                    .await
                    .map_err(crate::sync::store::membership::AnchoredChainError::from)?;
                }
                Box::pin(
                    self.history_verifier
                        .load_exact_anchored_membership(&cursors.head_refs, Some(owner_pubkey)),
                )
                .await?
            }
        };
        let root = self.history_verifier.verified_root().reference().clone();
        let root_object = self.history_verifier.verified_root().object().clone();
        let founder_registration = self
            .history_verifier
            .load_founder_registration()
            .await
            .map_err(crate::sync::store::membership::AnchoredChainError::from_store_object)?;
        let founder_registration_ref = StoreDeviceRegistrationRef::from_registration(
            &founder_registration.value,
            founder_registration.object.clone(),
        );
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

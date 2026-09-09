use super::*;

struct VerifiedMergeSnapshotState {
    common: VerifiedSnapshotState,
    membership: MembershipChain,
    commit_refs: BTreeSet<StoreBatchCommitRef>,
}

/// One snapshot chosen out of the candidates, with the verification that made
/// it eligible. The two predicates answer different questions, so the evidence
/// they produce has different types and cannot be swapped.
pub(crate) struct SelectedStoreSnapshot {
    pub(crate) snapshot: coven_database::PublishedStoreSnapshot,
    pub(crate) verified: coven_database::VerifiedStoreSnapshotAuthority,
}

impl<'a> MergeHistoryVerifier<'a> {
    async fn verify_snapshot_history_state(
        &mut self,
        frontier: &BTreeMap<protocol_membership::AuthorStreamId, StoreBatchCommitRef>,
        membership_ref: &StoreMembershipStateRef,
    ) -> Result<VerifiedMergeSnapshotState, StorePullError> {
        let authority = self
            .verify_merge_history_authority(frontier, membership_ref)
            .await?;
        let active_registrations = self
            .commit_verifier
            .load_active_registrations(&authority.device_state)
            .await?;
        let commit_refs = verified_merge_commit_closure(&self.history, frontier.values().cloned())?;
        Ok(VerifiedMergeSnapshotState {
            common: VerifiedSnapshotState {
                device_state: authority.device_state,
                active_registrations,
            },
            membership: authority.membership,
            commit_refs,
        })
    }

    async fn verify_snapshot_authority(
        &mut self,
        snapshot: &coven_database::PublishedStoreSnapshot,
    ) -> Result<(StoreHistoryCut, VerifiedMergeSnapshotState), StorePullError> {
        // Verification recomposes the snapshot's history summary, and the
        // composition resumes at this device's baseline. That only reaches the
        // snapshot's coverage when the coverage stands at or above the
        // baseline; a snapshot behind it would need the commits the baseline
        // retired. It is also nothing this device wants: it already stands
        // further along than the snapshot claims.
        if !snapshot
            .meta
            .coverage
            .covers(self.history.baseline.coverage())
        {
            return Err(StorePullError::SnapshotBehindReplayBaseline);
        }
        let frontier = &snapshot.meta.coverage.0;
        let state = self
            .verify_snapshot_history_state(frontier, &snapshot.meta.state.membership)
            .await?;
        self.verify_snapshot_authority_with_state(snapshot, state)
            .await
    }

    async fn verify_snapshot_authority_with_state(
        &self,
        snapshot: &coven_database::PublishedStoreSnapshot,
        state: VerifiedMergeSnapshotState,
    ) -> Result<(StoreHistoryCut, VerifiedMergeSnapshotState), StorePullError> {
        let frontier = &snapshot.meta.coverage.0;
        if state.common.device_state != snapshot.meta.state.devices {
            return Err(StorePullError::InvalidState(
                "Merge snapshot device state differs from its exact verified history".to_string(),
            ));
        }
        let author = state
            .common
            .active_registrations
            .get(&snapshot.meta.author_registration.device_id)
            .filter(|registration| registration.reference() == &snapshot.meta.author_registration)
            .ok_or(StorePullError::SnapshotAuthorInactive)?;
        if !state.membership.is_owner_now(&author.value().author_pubkey) {
            return Err(StorePullError::SnapshotAuthorNotOwner);
        }
        if self.history.baseline.snapshot().is_some_and(|installed| {
            installed.reference == snapshot.reference && installed.meta == snapshot.meta
        }) {
            return Ok((StoreHistoryCut(frontier.clone()), state));
        }
        self.verify_snapshot_finalization(&state).await?;
        let mut canonical = compose_verified_merge_snapshot_history_summary(
            self.root.reference(),
            &snapshot.meta.coverage,
            &state.membership,
            &state.common.device_state,
            &snapshot.meta.author_registration,
            author.value(),
            self.history.baseline.history_summary().cloned(),
            state
                .commit_refs
                .iter()
                .filter_map(|reference| self.history.commits.get(reference)),
        )?;
        let mut requests = Vec::new();
        for reference in &state.commit_refs {
            if self.history.baseline.covers(reference) {
                continue;
            }
            let verified = self.history.commits.get(reference).ok_or_else(|| {
                StorePullError::InvalidState("request retirement lacks its verified commit".into())
            })?;
            if verified
                .verified
                .value()
                .owner_promotion_request()
                .is_none()
            {
                continue;
            }
            let accepted = self.accepted_publication(reference).ok_or_else(|| {
                StorePullError::InvalidState(
                    "request retirement lacks its exact accepted publication".into(),
                )
            })?;
            requests.push((
                verified.verified.value(),
                accepted,
                verified.predecessor_state.clone(),
            ));
        }
        self.retain_pending_owner_promotions(
            &mut canonical,
            requests,
            &state.membership,
            &state.common.device_state,
        )
        .await?;
        let mut join_commits = BTreeMap::new();
        let mut join_publications = BTreeMap::new();
        for reference in &state.commit_refs {
            let Some(verified) = self.history.commits.get(reference) else {
                continue;
            };
            join_commits.insert(
                reference.clone(),
                DeviceJoinBootstrapCommit {
                    reference: reference.clone(),
                    commit: verified.verified.clone(),
                    registrations: verified.registrations.clone(),
                    device_operations: verified.operations.clone(),
                    history_evidence: verified.history_evidence.clone(),
                },
            );
            if let Some(exact) = self.accepted_publication(reference) {
                join_publications.insert(reference.clone(), exact.reference().clone());
            }
        }
        self.retain_pending_device_joins(
            &mut canonical,
            &state.common.device_state,
            join_commits,
            join_publications,
            &snapshot.meta.publication_predecessor,
        )
        .await?;
        if let Some(previous) = snapshot.meta.publication_predecessor.latest_snapshot() {
            let metadata = self.load_snapshot_metadata(&previous.snapshot).await?;
            canonical
                .reclaim
                .include_previous_snapshot(previous, &metadata)
                .map_err(StorePullError::Protocol)?;
            canonical
                .reclaim
                .retire_receipts(state.commit_refs.iter().filter_map(|reference| {
                    self.history
                        .commits
                        .get(reference)
                        .and_then(|commit| commit.verified.value().reclaim_receipt())
                }))
                .map_err(StorePullError::Protocol)?;
        }
        self.retain_snapshot_publication_objects(&mut canonical)?;
        self.verify_snapshot_artifact_retirement(
            &mut canonical.reclaim,
            &snapshot.meta.history_summary.reclaim,
        )
        .await?;
        // Complete each chain the same way the publisher did, so the
        // recomposition is comparable to the summary the snapshot carries.
        for chain in canonical.acknowledgements.values_mut() {
            let (reference, value) = chain
                .latest()
                .ok_or_else(|| {
                    StorePullError::InvalidState(
                        "composed acknowledgement chain is empty".to_string(),
                    )
                })?
                .clone();
            let registration = self
                .commit_verifier
                .load_registration(&reference.registration)
                .await?;
            chain.chain = self
                .load_acknowledgement_proof_chain(reference, value, &registration.value)
                .await
                .map_err(StorePullError::from)?;
        }
        crate::sync::store::commit_verification::merge_history::validate_composed_snapshot_history_summary(
            &canonical,
            &snapshot.meta.coverage,
        )?;
        if snapshot.meta.history_summary != canonical {
            return Err(StorePullError::InvalidState(
                "Merge snapshot history summary differs from its exact verified cut".to_string(),
            ));
        }
        Ok((StoreHistoryCut(frontier.clone()), state))
    }

    // Construct receipt verification on the heap before its caller polls it.
    // This verifier is also reached through bounded-stack Circle publication.
    #[inline(never)]
    fn verify_snapshot_finalization<'verification>(
        &'verification self,
        state: &'verification VerifiedMergeSnapshotState,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), StorePullError>> + Send + 'verification>,
    > {
        Box::pin(async move {
            // The live interval proves acceptance. Its continuing results must
            // name those exact publications before either publisher or receiver
            // can retire the interval in favor of this snapshot.
            for reference in &state.commit_refs {
                if self.history.baseline.covers(reference) {
                    continue;
                }
                let commit = self.history.commits.get(reference).ok_or_else(|| {
                    StorePullError::InvalidState(
                        "snapshot finalization lacks a verified predecessor".into(),
                    )
                })?;
                let Some(proof) = &commit.history_evidence.membership_proof else {
                    continue;
                };
                let Some(AcceptedStoreCommitEvidence::Exact(exact)) =
                    self.accepted_publications.get(reference)
                else {
                    return Err(StorePullError::InvalidState(
                        "retained membership control has no exact accepted publication".into(),
                    ));
                };
                let result = self
                    .commit_verifier
                    .membership_objects()
                    .load_head_acceptance(&proof.head, &proof.head_value)
                    .await?;
                if result.value.publication()? != exact.reference() {
                    return Err(StorePullError::InvalidState(
                        "membership acceptance result names another accepted publication".into(),
                    ));
                }
            }
            Ok(())
        })
    }

    pub(crate) async fn store_snapshot_blob_is_reclaimable(
        &self,
        snapshot: &coven_database::PublishedStoreSnapshot,
        blob: &coven_protocol::blob::locator::StoredBlobRef,
    ) -> Result<bool, StorePullError> {
        let bytes = self
            .commit_verifier
            .load_store_snapshot_image(&snapshot.reference, &snapshot.meta)
            .await?;
        coven_database::SnapshotDatabaseImage::contains_reclaimable_store_blob(
            &bytes,
            &snapshot.meta,
            blob,
        )
        .map_err(|error| {
            StorePullError::Database(coven_database::DbError::context(
                "read accepted Store blob inventory",
                error,
            ))
        })
    }

    /// Establish snapshot authority before opening the encrypted image.
    /// Membership remains anchored in the Store root; signed metadata carries
    /// the device state whose accepted registration and exclusion effects are
    /// verified below.
    pub(super) async fn admit_accepted_snapshot_verification_baseline(
        &mut self,
        snapshot: coven_database::PublishedStoreSnapshot,
    ) -> Result<(), StorePullError> {
        let baseline = coven_database::InstalledReplayBaseline::new(
            snapshot.meta.coverage.clone(),
            BTreeMap::new(),
            Some(OpenedRetainedMergeHistorySummary {
                summary: snapshot.meta.history_summary.clone(),
                post_state: snapshot.meta.state.devices.clone(),
            }),
            Some(snapshot.clone()),
        );
        let opened = baseline.history_summary().ok_or_else(|| {
            StorePullError::InvalidState("accepted snapshot has no replay summary".into())
        })?;
        let membership =
            membership::AcceptedMembershipActivation::new(&self.root, &self.commit_verifier)
                .load_snapshot_membership(&snapshot.meta)
                .await?;
        // The candidate summary is checked only after its membership authority
        // is established from independently discovered, accepted head results.
        let prefix = VerifiedMergeMembershipPrefix::from_retained(&[
            coven_database::RetainedMergeHistoryCheckpoint::Snapshot(opened.clone()),
        ])?;
        prefix.validate_complete_membership(&membership)?;
        self.verify_retained_owner_promotions(
            &opened.summary,
            &prefix,
            &snapshot.meta.publication_predecessor,
        )
        .await?;
        self.verify_retained_device_joins(&opened.summary, &opened.post_state)
            .await?;

        verify_merge_membership_state_ref(
            &snapshot.meta.state.membership,
            &membership,
            &opened.post_state,
        )?;
        let registrations = self
            .commit_verifier
            .load_active_registrations(&opened.post_state)
            .await?;
        let author = registrations
            .get(&snapshot.meta.author_registration.device_id)
            .filter(|author| author.reference() == &snapshot.meta.author_registration)
            .ok_or(StorePullError::SnapshotAuthorInactive)?;
        if !membership.is_owner_now(&author.value().author_pubkey) {
            return Err(StorePullError::SnapshotAuthorNotOwner);
        }
        coven_protocol::store_commit::RetainedReplaySnapshotAuthority {
            store_root: self.root.reference().clone(),
            founder_registration: self.founder.clone(),
            snapshot: snapshot.reference.clone(),
            metadata: snapshot.meta.clone(),
            snapshot_cut: StoreHistoryCut(snapshot.meta.coverage.commits().clone()),
            active_registrations: registrations,
        }
        .validate()
        .map_err(StorePullError::Protocol)?;
        // This changes the verifier's read floor only. Live rows and their replay
        // retention remain owned by the database installation transaction.
        self.admit_installed_baseline(baseline)?;
        Ok(())
    }

    /// Verify one snapshot as installable: the owner's signature over metadata
    /// whose coverage, device state and history summary all recompose from the
    /// verified history. Nothing here consults the other devices.
    pub(crate) async fn verify_installable_snapshot(
        &mut self,
        snapshot: &coven_database::PublishedStoreSnapshot,
    ) -> Result<VerifiedStoreSnapshotAuthority, StorePullError> {
        let authority = self.build_snapshot_authority(snapshot).await?;
        VerifiedStoreSnapshotAuthority::from_authority(authority).map_err(StorePullError::Database)
    }

    async fn build_snapshot_authority(
        &mut self,
        snapshot: &coven_database::PublishedStoreSnapshot,
    ) -> Result<coven_protocol::store_commit::RetainedReplaySnapshotAuthority, StorePullError> {
        let (snapshot_cut, state) = self.verify_snapshot_authority(snapshot).await?;
        Ok(
            coven_protocol::store_commit::RetainedReplaySnapshotAuthority {
                store_root: self.root.reference().clone(),
                founder_registration: self.founder.clone(),
                snapshot: snapshot.reference.clone(),
                metadata: snapshot.meta.clone(),
                snapshot_cut,
                active_registrations: state.common.active_registrations,
            },
        )
    }
}

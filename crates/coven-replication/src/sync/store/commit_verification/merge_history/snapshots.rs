use super::*;

struct VerifiedMergeSnapshotState {
    common: VerifiedSnapshotState,
    membership: MembershipChain,
    commit_refs: BTreeSet<StoreBatchCommitRef>,
}

/// One snapshot chosen out of the candidates, with the verification that made
/// it eligible. The two predicates answer different questions, so the evidence
/// they produce has different types and cannot be swapped.
pub(crate) struct SelectedStoreSnapshot<Verified> {
    pub(crate) snapshot: coven_database::PublishedStoreSnapshot,
    pub(crate) verified: Verified,
}

pub(crate) type SelectedInstallableStoreSnapshot =
    SelectedStoreSnapshot<coven_database::VerifiedStoreSnapshotAuthority>;
pub(crate) type SelectedAcknowledgedStoreSnapshot =
    SelectedStoreSnapshot<coven_database::VerifiedAcknowledgedStoreSnapshot>;

/// Report a candidate that was passed over. A rejection here costs as much as
/// the choice does: a store whose newest snapshot is rejected silently falls
/// back to an older one and pays the difference on every join.
fn report_rejected_snapshot(
    snapshot: &coven_database::PublishedStoreSnapshot,
    error: &StorePullError,
) {
    tracing::info!(
        generation = snapshot.reference.generation,
        snapshot = %snapshot.reference.snapshot_hash,
        coverage_positions = snapshot.meta.coverage.position_count(),
        rejection = %error,
        "Store snapshot is not an eligible candidate"
    );
}

fn report_selected_snapshot(snapshot: &coven_database::PublishedStoreSnapshot, eligible: usize) {
    // The tips themselves, not just how many: the whole point of reading this
    // line is to tell how much history the snapshot spares a joining device.
    let coverage = snapshot
        .meta
        .coverage
        .commits()
        .iter()
        .map(|(stream, reference)| format!("{stream}/{}", reference.coord.sequence()))
        .collect::<Vec<_>>()
        .join(" ");
    tracing::info!(
        generation = snapshot.reference.generation,
        snapshot = %snapshot.reference.snapshot_hash,
        coverage_streams = snapshot.meta.coverage.position_count(),
        %coverage,
        eligible_candidates = eligible,
        "Selected the Store snapshot"
    );
}

/// Whether a rejection disqualifies one candidate or fails the whole selection.
/// An unstable or improperly authored snapshot is a candidate the store may
/// simply not have yet; anything else is a fault worth propagating.
fn disqualifies_one_candidate(error: &StorePullError) -> bool {
    matches!(
        error,
        StorePullError::SnapshotNotStable { .. }
            | StorePullError::SnapshotAuthorInactive
            | StorePullError::SnapshotAuthorNotOwner
    )
}

/// Take the maximal snapshot out of those that passed, or hand back the reason
/// the maximal candidate overall was turned away.
fn take_maximal_eligible<Verified>(
    mut eligible: Vec<SelectedStoreSnapshot<Verified>>,
    maximal_rejection: Option<StorePullError>,
) -> Result<Option<SelectedStoreSnapshot<Verified>>, StorePullError> {
    let selected = crate::sync::store::snapshots::select_maximal_store_snapshot(
        eligible
            .iter()
            .map(|candidate| candidate.snapshot.clone())
            .collect(),
    );
    if let Some(selected) = selected {
        let index = eligible
            .iter()
            .position(|candidate| candidate.snapshot.reference == selected.reference)
            .ok_or_else(|| {
                StorePullError::InvalidState(
                    "Store snapshot selection lost its verified candidate".to_string(),
                )
            })?;
        let count = eligible.len();
        let selected = eligible.swap_remove(index);
        report_selected_snapshot(&selected.snapshot, count);
        return Ok(Some(selected));
    }
    Err(maximal_rejection.ok_or_else(|| {
        StorePullError::InvalidState(
            "Store snapshot candidates produced no eligibility decision".to_string(),
        )
    })?)
}

impl<'a> MergeHistoryVerifier<'a> {
    /// The newest snapshot this device can install as its starting state.
    ///
    /// Eligibility is the owner's signature over a history-consistent image and
    /// nothing else. Whether the store's other devices have caught up to it is
    /// a different question, asked by
    /// [`select_maximal_acknowledged_store_snapshot`](Self::select_maximal_acknowledged_store_snapshot)
    /// on reclaim's behalf; a device that is behind converges through an
    /// ordinary pull whichever image the joiner installed.
    pub(crate) async fn select_maximal_installable_store_snapshot(
        &mut self,
        candidates: Vec<coven_database::PublishedStoreSnapshot>,
    ) -> Result<Option<SelectedInstallableStoreSnapshot>, StorePullError> {
        let Some(maximal_candidate) =
            crate::sync::store::snapshots::select_maximal_store_snapshot(candidates.clone())
        else {
            return Ok(None);
        };
        let maximal_reference = maximal_candidate.reference;
        let mut eligible = Vec::new();
        let mut maximal_rejection = None;
        for snapshot in candidates {
            match self.verify_installable_snapshot(&snapshot).await {
                Ok(verified) => eligible.push(SelectedStoreSnapshot { snapshot, verified }),
                Err(error) if disqualifies_one_candidate(&error) => {
                    report_rejected_snapshot(&snapshot, &error);
                    if snapshot.reference == maximal_reference {
                        maximal_rejection = Some(error);
                    }
                }
                Err(error) => return Err(error),
            }
        }
        take_maximal_eligible(eligible, maximal_rejection)
    }

    /// The newest snapshot every device active at its cut has acknowledged.
    /// Reclaim deletes history behind a snapshot only against this.
    pub(crate) async fn select_maximal_acknowledged_store_snapshot(
        &mut self,
        candidates: Vec<coven_database::PublishedStoreSnapshot>,
    ) -> Result<Option<SelectedAcknowledgedStoreSnapshot>, StorePullError> {
        let Some(maximal_candidate) =
            crate::sync::store::snapshots::select_maximal_store_snapshot(candidates.clone())
        else {
            return Ok(None);
        };
        let maximal_reference = maximal_candidate.reference;
        let mut eligible = Vec::new();
        let mut maximal_rejection = None;
        for snapshot in candidates {
            match self.verify_snapshot_stability(&snapshot).await {
                Ok(verified) => eligible.push(SelectedStoreSnapshot { snapshot, verified }),
                Err(error) if disqualifies_one_candidate(&error) => {
                    report_rejected_snapshot(&snapshot, &error);
                    if snapshot.reference == maximal_reference {
                        maximal_rejection = Some(error);
                    }
                }
                Err(error) => return Err(error),
            }
        }
        take_maximal_eligible(eligible, maximal_rejection)
    }

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
        let commit_refs =
            verified_merge_commit_closure(&self.history.commits, frontier.values().cloned())?;
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
        let expected_device_state = StoreDeviceStateRef::from_resolved(
            snapshot.meta.coverage.clone(),
            &state.common.device_state,
        )
        .map_err(StorePullError::Protocol)?;
        if expected_device_state != snapshot.meta.state.devices {
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
        let mut canonical = compose_verified_merge_snapshot_history_summary(
            self.root.reference(),
            &snapshot.meta.coverage,
            &state.membership,
            &state.common.device_state,
            &snapshot.meta.author_registration,
            author.value(),
            state
                .commit_refs
                .iter()
                .map(|reference| &self.history.commits[reference]),
        )?;
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

    async fn accepted_snapshot_cut(
        &mut self,
        snapshot_frontier: &BTreeMap<protocol_membership::AuthorStreamId, StoreBatchCommitRef>,
        state: &VerifiedMergeSnapshotState,
    ) -> Result<StoreHistoryCut, StorePullError> {
        let root = self.root.reference().clone();
        let mut accepted = snapshot_frontier.clone();
        for registration in state.common.active_registrations.values() {
            let registration_ref = registration.reference();
            let stream_id = store_commit::StreamActivation::device_authorized_stream_id(
                root.store_root_hash,
                registration_ref,
                store_commit::StreamAnchorDomain::StoreAnnouncements,
            );
            let discovery = self
                .discover_merge_stream(registration_ref, registration.value(), None)
                .await?;
            let Some((_, _, latest, _)) = discovery.commits.last() else {
                if accepted.contains_key(&stream_id) {
                    return Err(StorePullError::InvalidState(
                        "accepted Merge snapshot history is absent from its author stream"
                            .to_string(),
                    ));
                }
                continue;
            };
            if let Some(snapshot_tip) = accepted.get(&stream_id) {
                if latest.coord.sequence() < snapshot_tip.coord.sequence()
                    || (latest.coord.sequence() == snapshot_tip.coord.sequence()
                        && latest != snapshot_tip)
                {
                    return Err(StorePullError::InvalidState(
                        "current Merge author stream does not contain the snapshot cut".to_string(),
                    ));
                }
            }
            accepted.insert(stream_id, latest.clone());
        }
        Ok(StoreHistoryCut(accepted))
    }

    async fn activated_snapshot_acknowledgements(
        &mut self,
        frontier: &BTreeMap<protocol_membership::AuthorStreamId, StoreBatchCommitRef>,
    ) -> Result<Vec<VerifiedActivatedStoreAck>, StorePullError> {
        self.verify_refs(frontier.values().cloned()).await?;
        self.activated_snapshot_acknowledgements_from_verified_history(frontier)
    }

    fn activated_snapshot_acknowledgements_from_verified_history(
        &self,
        frontier: &BTreeMap<protocol_membership::AuthorStreamId, StoreBatchCommitRef>,
    ) -> Result<Vec<VerifiedActivatedStoreAck>, StorePullError> {
        verified_merge_commit_closure(&self.history.commits, frontier.values().cloned())?;
        // A retained row carries the one acknowledgement its commit activated, so
        // a device's chain is assembled here, from every commit in the verified
        // closure that acknowledged for it. This is the boundary where the whole
        // chain is wanted — a snapshot's summary states contiguity for devices
        // that will restore from it and have no rows to walk — and folding it
        // once here is what lets the rows stay the size of their own commit.
        let mut acknowledgements = Vec::new();
        for (activating_commit, commit) in &self.history.commits {
            let Some((reference, value)) = commit.acknowledgement.as_ref() else {
                continue;
            };
            acknowledgements.push(VerifiedActivatedStoreAck {
                reference: reference.clone(),
                value: value.clone(),
                activating_commit: activating_commit.clone(),
                activating_commit_value: commit.verified.value().clone(),
            });
        }
        Ok(acknowledgements)
    }

    pub(crate) async fn verify_snapshots_for_acknowledgement(
        &mut self,
        snapshots: &[coven_database::PublishedStoreSnapshot],
    ) -> Result<(), StorePullError> {
        for snapshot in snapshots {
            self.verify_snapshot_authority(snapshot).await?;
        }
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

    /// Verify one snapshot as acknowledged by every device active at its cut.
    /// This is the installable verification plus the unanimity walk, which
    /// reads one acknowledgement chain per active device.
    pub(crate) async fn verify_snapshot_stability(
        &mut self,
        snapshot: &coven_database::PublishedStoreSnapshot,
    ) -> Result<VerifiedAcknowledgedStoreSnapshot, StorePullError> {
        let authority = self.build_snapshot_authority(snapshot).await?;
        let acknowledgements = self
            .activated_snapshot_acknowledgements(&authority.accepted_cut.0)
            .await?;
        let acknowledged = self
            .build_acknowledged_snapshot(authority, acknowledgements)
            .await?;
        VerifiedAcknowledgedStoreSnapshot::from_acknowledged(acknowledged)
            .map_err(StorePullError::Database)
    }

    async fn build_snapshot_authority(
        &mut self,
        snapshot: &coven_database::PublishedStoreSnapshot,
    ) -> Result<coven_protocol::store_commit::RetainedReplaySnapshotAuthority, StorePullError> {
        let (snapshot_cut, state) = self.verify_snapshot_authority(snapshot).await?;
        let accepted_cut = self.accepted_snapshot_cut(&snapshot_cut.0, &state).await?;
        Ok(
            coven_protocol::store_commit::RetainedReplaySnapshotAuthority {
                store_root: self.root.reference().clone(),
                founder_registration: self.founder.clone(),
                snapshot: snapshot.reference.clone(),
                metadata: snapshot.meta.clone(),
                snapshot_cut,
                accepted_cut,
                device_state: state.common.device_state,
                active_registrations: state.common.active_registrations,
            },
        )
    }

    async fn build_acknowledged_snapshot(
        &self,
        authority: coven_protocol::store_commit::RetainedReplaySnapshotAuthority,
        acknowledgements: Vec<VerifiedActivatedStoreAck>,
    ) -> Result<coven_protocol::store_commit::AcknowledgedStoreSnapshot, StorePullError> {
        let mut retained_acknowledgements = BTreeMap::new();
        for (device_id, registration) in &authority.active_registrations {
            let registration_ref = registration.reference();
            let matching = acknowledgements
                .iter()
                .filter(|ack| {
                    ack.value.registration == *registration_ref
                        && ack.value.snapshot.as_ref().is_some_and(|acknowledged| {
                            acknowledged.author_registration
                                == authority.metadata.author_registration
                                && acknowledged.snapshot == authority.snapshot
                        })
                        && ack.value.device_state == authority.metadata.state.devices
                        && ack
                            .value
                            .store_cut
                            .frontier()
                            .covers(&authority.metadata.coverage)
                })
                .max_by_key(|ack| (ack.reference.sequence, ack.activating_commit.clone()))
                .ok_or_else(|| StorePullError::SnapshotNotStable {
                    member: registration.value().author_pubkey.clone(),
                    device_id: device_id.to_string(),
                })?;
            // The whole chain is stated once here, at the snapshot boundary,
            // rather than carried by every retained row. The walk is served from
            // the acknowledgements this verifier already holds, which the
            // retained rows seeded.
            let chain = self
                .load_acknowledgement_proof_chain(
                    matching.reference.clone(),
                    matching.value.clone(),
                    registration.value(),
                )
                .await
                .map_err(StorePullError::from)?;
            retained_acknowledgements.insert(
                *device_id,
                store_commit::RetainedAcknowledgementChain {
                    chain,
                    activating_commit: matching.activating_commit.clone(),
                    activating_commit_value: matching.activating_commit_value.clone(),
                },
            );
        }
        Ok(coven_protocol::store_commit::AcknowledgedStoreSnapshot {
            authority,
            acknowledgements: retained_acknowledgements,
        })
    }
}

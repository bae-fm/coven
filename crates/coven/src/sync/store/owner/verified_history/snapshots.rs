use super::*;

struct VerifiedMergeSnapshotState {
    common: VerifiedSnapshotState,
    membership: MembershipChain,
    checkpoints: Vec<OpenedRetainedMergeHistorySummary>,
}

pub(crate) struct SelectedStableStoreSnapshot {
    pub(crate) snapshot: crate::database::PublishedStoreSnapshot,
    pub(crate) stability: crate::database::VerifiedStoreSnapshotStability,
}

impl<'a> MergeHistoryVerifier<'a> {
    pub(crate) async fn select_maximal_stable_store_snapshot(
        &mut self,
        candidates: Vec<crate::database::PublishedStoreSnapshot>,
    ) -> Result<Option<SelectedStableStoreSnapshot>, StorePullError> {
        let Some(maximal_candidate) =
            super::snapshot::select_maximal_store_snapshot(candidates.clone())
        else {
            return Ok(None);
        };
        let maximal_reference = maximal_candidate.reference;
        let mut stable = Vec::new();
        let mut maximal_rejection = None;
        for snapshot in candidates {
            match self.verify_snapshot_stability(&snapshot).await {
                Ok(stability) => stable.push(SelectedStableStoreSnapshot {
                    snapshot,
                    stability,
                }),
                Err(error) => match &error {
                    StorePullError::SnapshotNotStable { .. }
                    | StorePullError::SnapshotAuthorInactive
                    | StorePullError::SnapshotAuthorNotOwner => {
                        if snapshot.reference == maximal_reference {
                            maximal_rejection = Some(error);
                        }
                    }
                    _ => return Err(error),
                },
            }
        }
        let selected = super::snapshot::select_maximal_store_snapshot(
            stable
                .iter()
                .map(|candidate| candidate.snapshot.clone())
                .collect(),
        );
        if let Some(selected) = selected {
            let index = stable
                .iter()
                .position(|candidate| candidate.snapshot.reference == selected.reference)
                .ok_or_else(|| {
                    StorePullError::Database(
                        "stable Store snapshot selection lost its verified candidate".to_string(),
                    )
                })?;
            return Ok(Some(stable.swap_remove(index)));
        }
        Err(maximal_rejection.ok_or_else(|| {
            StorePullError::Database(
                "Store snapshot candidates produced no stability decision".to_string(),
            )
        })?)
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
        let checkpoints = frontier
            .values()
            .map(|reference| {
                self.history
                    .commits
                    .get(reference)
                    .map(|commit| commit.history.clone())
                    .ok_or_else(|| {
                        StorePullError::Database(
                            "Merge snapshot frontier is absent from its verified history"
                                .to_string(),
                        )
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(VerifiedMergeSnapshotState {
            common: VerifiedSnapshotState {
                device_state: authority.device_state,
                active_registrations,
            },
            membership: authority.membership,
            checkpoints,
        })
    }

    async fn verify_snapshot_authority(
        &mut self,
        snapshot: &crate::database::PublishedStoreSnapshot,
    ) -> Result<(StoreHistoryCut, VerifiedMergeSnapshotState), StorePullError> {
        let frontier = &snapshot.meta.coverage.0;
        let state = self
            .verify_snapshot_history_state(frontier, &snapshot.meta.state.membership)
            .await?;
        let expected_device_state = StoreDeviceStateRef::from_resolved(
            snapshot.meta.coverage.clone(),
            &state.common.device_state,
        )
        .map_err(|error| StorePullError::Database(error.to_string()))?;
        if expected_device_state != snapshot.meta.state.devices {
            return Err(StorePullError::Database(
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
        let canonical = compose_merge_snapshot_history_summary(
            self.root.reference(),
            &snapshot.meta.coverage,
            &state.membership,
            &state.common.device_state,
            &snapshot.meta.author_registration,
            author.value(),
            state.checkpoints.clone(),
        )?;
        if snapshot.meta.history_summary != canonical {
            return Err(StorePullError::Database(
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
                    return Err(StorePullError::Database(
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
                    return Err(StorePullError::Database(
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
        let mut acknowledgements = Vec::new();
        for (activating_commit, commit) in &self.history.commits {
            let Some((reference, value)) = commit.acknowledgement.as_ref() else {
                continue;
            };
            let chain = commit
                .history
                .summary
                .acknowledgements
                .get(&reference.registration.device_id)
                .ok_or_else(|| {
                    StorePullError::Database(
                        "verified acknowledgement history lacks its exact chain".to_string(),
                    )
                })?
                .chain
                .clone();
            acknowledgements.push(VerifiedActivatedStoreAck {
                reference: reference.clone(),
                value: value.clone(),
                chain,
                activating_commit: activating_commit.clone(),
                activating_commit_value: commit.verified.value().clone(),
            });
        }
        Ok(acknowledgements)
    }

    pub(crate) async fn verify_snapshots_for_acknowledgement(
        &mut self,
        snapshots: &[crate::database::PublishedStoreSnapshot],
    ) -> Result<(), StorePullError> {
        for snapshot in snapshots {
            self.verify_snapshot_authority(snapshot).await?;
        }
        Ok(())
    }

    pub(crate) async fn verify_snapshot_stability(
        &mut self,
        snapshot: &crate::database::PublishedStoreSnapshot,
    ) -> Result<VerifiedStoreSnapshotStability, StorePullError> {
        let (snapshot_cut, state) = self.verify_snapshot_authority(snapshot).await?;
        let snapshot_frontier = &snapshot_cut.0;
        let accepted_cut = self
            .accepted_snapshot_cut(snapshot_frontier, &state)
            .await?;
        let acknowledgements = self
            .activated_snapshot_acknowledgements(&accepted_cut.0)
            .await?;
        let mut retained_acknowledgements = BTreeMap::new();
        for (device_id, registration) in &state.common.active_registrations {
            let registration_ref = registration.reference();
            let matching = acknowledgements
                .iter()
                .filter(|ack| {
                    ack.value.registration == *registration_ref
                        && ack.value.snapshot.as_ref().is_some_and(|acknowledged| {
                            acknowledged.author_registration == snapshot.meta.author_registration
                                && acknowledged.snapshot == snapshot.reference
                        })
                        && ack.value.device_state == snapshot.meta.state.devices
                        && ack
                            .value
                            .store_cut
                            .frontier()
                            .covers(&snapshot.meta.coverage)
                })
                .max_by_key(|ack| (ack.reference.sequence, ack.activating_commit.clone()))
                .ok_or_else(|| StorePullError::SnapshotNotStable {
                    member: registration.value().author_pubkey.clone(),
                    device_id: device_id.to_string(),
                })?;
            retained_acknowledgements.insert(
                *device_id,
                store_commit::RetainedVerifiedActivatedAck {
                    chain: matching.chain.clone(),
                    activating_commit: matching.activating_commit.clone(),
                    activating_commit_value: matching.activating_commit_value.clone(),
                },
            );
        }
        let founder = self.commit_verifier.load_founder_registration().await?;
        let authority = crate::database::RetainedReplaySnapshotAuthority {
            store_root: self.root.reference().clone(),
            founder_registration: StoreDeviceRegistrationRef::from_registration(
                &founder.value,
                founder.object,
            ),
            snapshot: snapshot.reference.clone(),
            metadata: snapshot.meta.clone(),
            snapshot_cut,
            accepted_cut,
            device_state: state.common.device_state,
            active_registrations: state.common.active_registrations,
            acknowledgements: retained_acknowledgements,
        };
        VerifiedStoreSnapshotStability::from_authority(authority)
            .map_err(|error| StorePullError::Database(error.to_string()))
    }
}

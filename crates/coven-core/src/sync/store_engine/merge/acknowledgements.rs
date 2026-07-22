use super::*;

pub(super) async fn finish_nonactivating_acknowledgement(
    db: &Database,
    storage: &dyn SyncStorage,
    acknowledgement: crate::sync::store_commit::StoreAckRef,
) -> Result<(), crate::sync::store_outbound::StoreOutboundError> {
    let database = MergeDatabase::new(db);
    if let Some(target) = database
        .acknowledgement_cleanup_target(acknowledgement.clone())
        .await?
    {
        crate::sync::store_objects::delete_exact_object(storage, &target.object).await?;
        db.mark_candidate_cleanup_absent(target.object).await?;
    }
    database
        .complete_nonactivating_acknowledgement(acknowledgement)
        .await?;
    Ok(())
}

impl AuthorizedMergeStoreEngine<'_> {
    pub(in crate::sync::store_engine) async fn stage_and_publish_ack(
        &self,
        identity: &UserKeypair,
        sync_time: &str,
    ) -> Result<(), SyncCycleFailure> {
        Box::pin(self.drain_acknowledgements(identity))
            .await
            .map_err(|error| {
                SyncCycleFailure::operation("publish queued Store acknowledgement", error)
            })?;
        let frontier = CommitFrontier::from_refs(
            crate::WritePolicy::MergeConcurrent,
            self.db()
                .materialized_frontier()
                .await
                .map_err(|error| format!("read Store acknowledgement frontier: {error}"))?,
        )
        .map_err(|error| format!("shape Store acknowledgement frontier: {error}"))?;
        Box::pin(self.stage_acknowledgement(frontier, sync_time.to_owned(), identity))
            .await
            .map_err(|error| format!("stage Store acknowledgement: {error}"))?;
        Box::pin(self.drain_acknowledgements(identity))
            .await
            .map_err(|error| SyncCycleFailure::operation("publish Store acknowledgement", error))?;
        Ok(())
    }

    pub(in crate::sync::store_engine) async fn stage_acknowledgement(
        &self,
        frontier: CommitFrontier,
        sync_time: String,
        identity: &UserKeypair,
    ) -> Result<crate::sync::store_commit::StoreAck, crate::sync::store_ack::StoreAckError> {
        let CommitFrontier::MergeConcurrent(commits) = &frontier else {
            return Err(crate::sync::store_ack::StoreAckError::InvalidOutbound(
                "Merge acknowledgement received a Serial frontier".to_string(),
            ));
        };
        let device_id = self
            .db()
            .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
            .await?
            .ok_or(crate::sync::store_ack::StoreAckError::MissingState(
                crate::database::LOCAL_DEVICE_ID_STATE_KEY,
            ))?;
        let (root, registration_ref, registration, device_signer) =
            crate::sync::store_outbound::load_local_store_authority(
                self.db(),
                &device_id,
                identity,
            )
            .await
            .map_err(|error| {
                crate::sync::store_ack::StoreAckError::InvalidOutbound(error.to_string())
            })?;
        let history_cut =
            crate::sync::store_commit::StoreHistoryCut::merge_concurrent(commits.clone());
        let (device_state, _) = self
            .db()
            .store_device_state_for_history_cut(&history_cut)
            .await?;
        let snapshot = self
            .select_acknowledgement_snapshot(&root, &frontier, &device_state)
            .await?;
        let exclusions = crate::sync::store_commit::StoreAckExclusionState::MergeConcurrent {
            proposal_freezes: self.db().store_device_exclusion_freezes().await?,
        };
        crate::sync::store_ack::stage_resolved_store_ack(
            self.db(),
            self.storage(),
            crate::sync::store_ack::ResolvedStoreAckPlan {
                root,
                registration_ref,
                registration,
                device_signer,
                device_id,
                history_cut,
                device_state,
                snapshot,
                exclusions,
                last_sync: sync_time,
            },
        )
        .await
    }

    async fn select_acknowledgement_snapshot(
        &self,
        root: &StoreRootRef,
        frontier: &CommitFrontier,
        device_state: &crate::sync::store_commit::StoreDeviceStateRef,
    ) -> Result<
        Option<crate::sync::store_commit::StoreSnapshotLocator>,
        crate::sync::store_ack::StoreAckError,
    > {
        let registrations = self
            .db()
            .activated_store_device_registration_records()
            .await?;
        let mut candidates = Vec::new();
        for (registration_ref, registration) in registrations {
            for snapshot in crate::sync::store_snapshot::load_store_snapshot_stream(
                self.storage(),
                root,
                &registration_ref,
                &registration,
            )
            .await?
            {
                if snapshot.meta.coverage.policy() != crate::WritePolicy::MergeConcurrent
                    || !frontier.covers(&snapshot.meta.coverage)
                    || snapshot.meta.state.devices.state_hash() != device_state.state_hash()
                    || snapshot.meta.state.devices.recovery() != device_state.recovery()
                {
                    continue;
                }
                crate::sync::store_pull::verify_store_snapshot_for_acknowledgement(
                    self.storage(),
                    None,
                    root,
                    &snapshot,
                )
                .await
                .map_err(|error| {
                    crate::sync::snapshot::SnapshotError::UnauthorizedAuthor(error.to_string())
                })?;
                candidates.push(snapshot);
            }
        }
        Ok(
            crate::sync::store_snapshot::select_maximal_store_snapshot(candidates).map(
                |snapshot| crate::sync::store_commit::StoreSnapshotLocator {
                    author_registration: snapshot.meta.author_registration,
                    snapshot: snapshot.reference,
                },
            ),
        )
    }

    pub(in crate::sync::store_engine) async fn drain_acknowledgements(
        &self,
        identity: &UserKeypair,
    ) -> Result<u64, crate::sync::store_ack::StoreAckError> {
        let device_id = self
            .db()
            .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
            .await?
            .ok_or(crate::sync::store_ack::StoreAckError::MissingState(
                crate::database::LOCAL_DEVICE_ID_STATE_KEY,
            ))?;
        let mut published = 0_u64;
        while let Some(outbound) = self.db().oldest_outbound_store_ack().await? {
            if let Some(activated) = self
                .db()
                .activated_store_ack(&outbound.reference.registration)
                .await?
            {
                if activated == outbound.reference {
                    self.db()
                        .complete_outbound_store_ack(outbound.reference)
                        .await?;
                    published = published.checked_add(1).ok_or_else(|| {
                        crate::sync::store_ack::StoreAckError::Database(
                            "ack publish count exceeded u64".into(),
                        )
                    })?;
                    continue;
                }
                if activated.sequence >= outbound.reference.sequence {
                    return Err(crate::sync::store_ack::StoreAckError::InvalidOutbound(
                        "queued Store acknowledgement differs from the activated exact ref"
                            .to_string(),
                    ));
                }
            }
            let candidate = match outbound.activation.clone() {
                crate::database::OutboundStoreAckActivation::AwaitingCandidate => {
                    let plan = operations::prepare_plan(
                        self.db(),
                        self.storage(),
                        &self.membership,
                        &device_id,
                        identity,
                    )
                    .await?;
                    plan.common()
                        .validate_acknowledgement(&outbound.ack.value)?;
                    let candidate = Box::pin(operations::prepare_candidate(
                        self.db(),
                        self.storage(),
                        plan,
                        crate::sync::store_outbound::StoreOperationBatch::Acknowledgement {
                            reference: outbound.reference.clone(),
                            value: outbound.ack.value.clone(),
                        },
                    ))
                    .await?;
                    self.database()
                        .prepare_acknowledgement_activation(outbound.reference.clone(), candidate)
                        .await?;
                    continue;
                }
                crate::database::OutboundStoreAckActivation::Prepared(candidate) => candidate,
                crate::database::OutboundStoreAckActivation::Nonactivating(_) => {
                    finish_nonactivating_acknowledgement(
                        self.db(),
                        self.storage(),
                        outbound.reference,
                    )
                    .await?;
                    published = published.checked_add(1).ok_or_else(|| {
                        crate::sync::store_ack::StoreAckError::Database(
                            "ack publish count exceeded u64".into(),
                        )
                    })?;
                    continue;
                }
            };
            let crate::sync::store_outbound::PreparedStoreOperationCommit::MergeConcurrent(
                candidate,
            ) = candidate
            else {
                return Err(crate::sync::store_ack::StoreAckError::InvalidOutbound(
                    "Merge acknowledgement queue contains a Serial candidate".to_string(),
                ));
            };
            let wrapped =
                crate::sync::store_outbound::PreparedStoreOperationCommit::MergeConcurrent(
                    candidate.clone(),
                );
            if !crate::sync::store_ack::publish_acknowledgement_object(
                self.db(),
                self.storage(),
                &device_id,
                &outbound,
                &wrapped,
            )
            .await?
            {
                continue;
            }
            match Box::pin(operations::publish_prepared(
                self.db(),
                self.storage(),
                Box::new(candidate),
                None,
                None,
            ))
            .await?
            {
                crate::sync::store_outbound::StoreOperationPublicationOutcome::Activated(_) => {
                    self.db()
                        .complete_outbound_store_ack(outbound.reference)
                        .await?;
                }
                crate::sync::store_outbound::StoreOperationPublicationOutcome::Nonactivated(_) => {}
                crate::sync::store_outbound::StoreOperationPublicationOutcome::Reprepared => {
                    continue;
                }
                crate::sync::store_outbound::StoreOperationPublicationOutcome::RepreparedCandidate(_)
                | crate::sync::store_outbound::StoreOperationPublicationOutcome::NonactivatedCandidate { .. } => {
                    return Err(crate::sync::store_ack::StoreAckError::InvalidOutbound(
                        "acknowledgement publication returned non-acknowledgement conflict state"
                            .to_string(),
                    ));
                }
            }
            published = published.checked_add(1).ok_or_else(|| {
                crate::sync::store_ack::StoreAckError::Database(
                    "ack publish count exceeded u64".into(),
                )
            })?;
        }
        Ok(published)
    }
}

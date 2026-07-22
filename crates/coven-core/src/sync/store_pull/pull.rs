use super::*;

#[doc(hidden)]
pub struct SerialResolutionCommit {
    pub(crate) commit: StoreBatchCommit,
    pub(crate) commit_ref: super::store_commit::StoreBatchCommitRef,
    pub(crate) packages: Vec<AudiencePackage>,
    pub(crate) changesets: super::gate::SerialInboundChangesets,
    pub(crate) registrations: Vec<(
        StoreDeviceRegistration,
        super::store_commit::StoreDeviceRegistrationActivation,
    )>,
    pub(crate) verified_circle_activations: VerifiedCircleActivations,
    pub(crate) device_operations: VerifiedStoreDeviceOperations,
    pub(crate) authorization_after: SerialAuthorizationState,
}

#[doc(hidden)]
pub struct SerialResolutionPlan {
    pub(super) head: StoreSerialHead,
    pub(super) head_object: super::storage::VersionedObject,
    pub(super) commits: Vec<SerialResolutionCommit>,
    pub(super) verified_suffix: Option<VerifiedSerialAcceptedSuffix>,
}

impl SerialResolutionPlan {
    pub(crate) fn head(&self) -> &StoreSerialHead {
        &self.head
    }

    pub(crate) fn head_object(&self) -> &super::storage::VersionedObject {
        &self.head_object
    }

    pub(crate) fn commits(&self) -> &[SerialResolutionCommit] {
        &self.commits
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        StoreSerialHead,
        super::storage::VersionedObject,
        Vec<SerialResolutionCommit>,
    ) {
        (self.head, self.head_object, self.commits)
    }

    pub(crate) fn verified_suffix(&self) -> Result<VerifiedSerialAcceptedSuffix, StorePullError> {
        self.verified_suffix.clone().ok_or_else(|| {
            StorePullError::Serial("Serial resolution has no accepted successor suffix".to_string())
        })
    }
}

pub(super) enum ApplyOutcome {
    Applied(Vec<RowChange>),
    Held(HeldStorePositionReason),
}

async fn required_pull_root(
    db: &Database,
    requested_hash: ObjectHash,
) -> Result<StoreRootRef, StorePullError> {
    let root = db
        .local_store_root_ref()
        .await
        .map_err(|error| StorePullError::Database(format!("load exact Store root: {error}")))?
        .ok_or_else(|| {
            StorePullError::Database("Store root exact reference is absent".to_string())
        })?;
    if root.store_root_hash != requested_hash {
        return Err(StorePullError::Database(
            "requested Store root differs from the durable exact root reference".to_string(),
        ));
    }
    Ok(root)
}

/// Discover every visible immutable head, then repeatedly materialize any commit
/// whose exact predecessor and dependency positions are already durable.
#[allow(clippy::too_many_arguments)]
pub async fn pull_store_commits(
    db: &Database,
    tables: &[SyncedTable],
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    store_dir: &StoreDir,
    membership: Option<&MembershipChain>,
) -> Result<StorePullResult, StorePullError> {
    let membership = membership.ok_or_else(|| {
        StorePullError::Database("Merge pull has no exact membership state".to_string())
    })?;
    Box::pin(pull_merge_store_commits_with_identity(
        db,
        tables,
        storage,
        store_root_hash,
        store_dir,
        membership,
        None,
    ))
    .await
}

#[allow(clippy::too_many_arguments)]
#[doc(hidden)]
pub async fn pull_store_commits_with_coordination(
    db: &Database,
    tables: &[SyncedTable],
    storage: &dyn SyncStorage,
    serial_coordination: Option<&dyn CoordinationStorage>,
    store_root_hash: ObjectHash,
    store_dir: &StoreDir,
    membership: Option<&MembershipChain>,
) -> Result<StorePullResult, StorePullError> {
    match db.write_policy() {
        crate::WritePolicy::MergeConcurrent => {
            let membership = membership.ok_or_else(|| {
                StorePullError::Database("Merge pull has no exact membership state".to_string())
            })?;
            pull_merge_store_commits_with_identity(
                db,
                tables,
                storage,
                store_root_hash,
                store_dir,
                membership,
                None,
            )
            .await
        }
        crate::WritePolicy::Serial => {
            pull_serial_store_commits_with_identity(
                db,
                tables,
                storage,
                serial_coordination.ok_or_else(|| {
                    StorePullError::Serial("coordination capability is absent".to_string())
                })?,
                store_root_hash,
                store_dir,
                None,
            )
            .await
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub fn pull_store_commits_with_identity<'a>(
    db: &'a Database,
    tables: &'a [SyncedTable],
    storage: &'a dyn SyncStorage,
    serial_coordination: Option<&'a dyn CoordinationStorage>,
    store_root_hash: ObjectHash,
    store_dir: &'a StoreDir,
    membership: Option<&'a MembershipChain>,
    identity: Option<&'a crate::keys::UserKeypair>,
) -> Pin<Box<dyn Future<Output = Result<StorePullResult, StorePullError>> + Send + 'a>> {
    Box::pin(async move {
        match db.write_policy() {
            crate::WritePolicy::MergeConcurrent => {
                let membership = membership.ok_or_else(|| {
                    StorePullError::Database("Merge pull has no exact membership state".to_string())
                })?;
                pull_merge_store_commits_with_identity(
                    db,
                    tables,
                    storage,
                    store_root_hash,
                    store_dir,
                    membership,
                    identity,
                )
                .await
            }
            crate::WritePolicy::Serial => {
                pull_serial_store_commits_with_identity(
                    db,
                    tables,
                    storage,
                    serial_coordination.ok_or_else(|| {
                        StorePullError::Serial("coordination capability is absent".to_string())
                    })?,
                    store_root_hash,
                    store_dir,
                    identity,
                )
                .await
            }
        }
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn pull_serial_store_commits_with_identity<'a>(
    db: &'a Database,
    tables: &'a [SyncedTable],
    storage: &'a dyn SyncStorage,
    coordination: &'a dyn CoordinationStorage,
    store_root_hash: ObjectHash,
    store_dir: &'a StoreDir,
    identity: Option<&'a crate::keys::UserKeypair>,
) -> Pin<Box<dyn Future<Output = Result<StorePullResult, StorePullError>> + Send + 'a>> {
    Box::pin(async move {
        let root = required_pull_root(db, store_root_hash).await?;
        let verified_root = load_store_protocol_root(storage, &root).await?.value;
        if verified_root.descriptor.write_policy != crate::WritePolicy::Serial {
            return Err(StorePullError::Database(
                "durable write policy differs from the signed Store root".to_string(),
            ));
        }
        pull_serial_store_commits(
            db,
            tables,
            storage,
            coordination,
            &root,
            verified_root,
            store_dir,
            identity,
        )
        .await
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn pull_merge_store_commits_with_identity<'a>(
    db: &'a Database,
    tables: &'a [SyncedTable],
    storage: &'a dyn SyncStorage,
    store_root_hash: ObjectHash,
    store_dir: &'a StoreDir,
    membership: &'a MembershipChain,
    identity: Option<&'a crate::keys::UserKeypair>,
) -> Pin<Box<dyn Future<Output = Result<StorePullResult, StorePullError>> + Send + 'a>> {
    Box::pin(async move {
        let root = required_pull_root(db, store_root_hash).await?;
        let verified_root = load_store_protocol_root(storage, &root).await?.value;
        if verified_root.descriptor.write_policy != crate::WritePolicy::MergeConcurrent {
            return Err(StorePullError::Database(
                "durable write policy differs from the signed Store root".to_string(),
            ));
        }
        resume_merge_retraction_cleanups(db, storage, &root).await?;

        let local_frontier = db.materialized_frontier().await.map_err(|error| {
            StorePullError::Database(format!("load discovery device-state frontier: {error}"))
        })?;
        let local_frontier = local_frontier
            .into_values()
            .map(|reference| match reference.coord {
                StoreCommitCoord::MergeConcurrent { stream_id, .. } => Ok((stream_id, reference)),
                StoreCommitCoord::Serial { .. } => Err(StorePullError::Database(
                    "Merge discovery frontier contains a Serial commit".to_string(),
                )),
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        let (_, discovery_device_state) = db
            .store_device_state_for_history_cut(&StoreHistoryCut::MergeConcurrent(local_frontier))
            .await?;

        let mut active = load_active_merge_registrations(db, storage, &root)
            .await
            .map_err(|error| {
                StorePullError::Database(format!("load active Merge registrations: {error}"))
            })?;
        for recovered in
            discover_merge_owner_recoveries(storage, &root, &verified_root, membership).await?
        {
            if active
                .iter()
                .all(|(reference, _)| reference != &recovered.0)
            {
                active.push(recovered);
            }
        }
        let mut candidates = BTreeMap::new();
        let mut visible_heads = Vec::new();
        let mut held = Vec::new();
        for (registration_ref, registration) in active {
            let inactive_cut = match discovery_device_state
                .devices
                .get(&registration_ref.device_id)
            {
                Some(record) if record.registration != registration_ref => {
                    return Err(StorePullError::Database(format!(
                        "discovery device state names another registration for {}",
                        registration_ref.device_id
                    )));
                }
                Some(record) => match &record.status {
                    StoreDeviceStatus::Active => None,
                    StoreDeviceStatus::Inactive { accepted_cut, .. } => Some(accepted_cut),
                },
                None => None,
            };
            let discovered = discover_merge_stream(
                storage,
                &root,
                &registration_ref,
                &registration,
                inactive_cut,
            )
            .await
            .map_err(|error| {
                StorePullError::Database(format!(
                    "discover Merge stream for {}: {error}",
                    registration.device_id
                ))
            })?;
            if let Some(head) = discovered.latest_head {
                visible_heads.push(VerifiedStoreDeviceHead {
                    head,
                    author: registration.clone(),
                });
            }
            if let Some(block) = discovered.block {
                held.push(block.into_position());
            }
            for (activation_head_ref, activation_head, commit_ref, commit) in discovered.commits {
                if commit_ref.coord.sequence() != commit.seq() {
                    held.push(held_commit(
                        &commit_ref,
                        HeldStorePositionReason::InvalidObject(
                            "exact commit coordinate differs from signed sequence".to_string(),
                        ),
                    ));
                    continue;
                }
                let stream_id = commit_stream_id(&commit_ref.coord);
                if let Some(materialized) = db
                    .exact_materialized_ref(&stream_id, commit_ref.coord.sequence())
                    .await?
                {
                    if materialized == commit_ref {
                        continue;
                    }
                    held.push(held_commit(
                        &commit_ref,
                        HeldStorePositionReason::HashMismatch {
                            referenced_device_id: stream_id,
                            referenced_commit: commit_ref.clone(),
                            materialized_hash: materialized.commit_hash,
                        },
                    ));
                    continue;
                }
                if let Some(package) = commit.store_package() {
                    if package.schema_version > db.schema_version() {
                        held.push(held_commit(
                            &commit_ref,
                            HeldStorePositionReason::NewerSchema {
                                local: db.schema_version(),
                                required: package.schema_version,
                            },
                        ));
                        continue;
                    }
                }
                let predecessor_membership = match load_merge_predecessor_membership(
                    storage,
                    &root,
                    &commit.membership_state,
                )
                .await
                {
                    Ok(membership) => membership,
                    Err(RegistrationLoadError::Object(error)) => {
                        held.push(held_commit(&commit_ref, held_object_error(error)));
                        continue;
                    }
                    Err(RegistrationLoadError::Invalid(error)) => {
                        held.push(held_commit(
                            &commit_ref,
                            HeldStorePositionReason::InvalidObject(error),
                        ));
                        continue;
                    }
                };
                let predecessor_authority =
                    RegistrationPredecessorAuthority::MergeConcurrent(&predecessor_membership);
                let requires_accepted_predecessor = commit
                    .device_join_attempt_decisions()
                    .iter()
                    .any(|decision| matches!(decision, DeviceJoinAttemptDecisionRef::Attempt(_)))
                    || !commit.device_join_outcomes().is_empty()
                    || !commit.device_join_cleanup_receipts().is_empty()
                    || commit.device_registrations().iter().any(|activation| {
                        matches!(
                            activation.authority,
                            StoreDeviceRegistrationActivationRef::Join { .. }
                        )
                    });
                let verified_accepted_predecessor = if requires_accepted_predecessor {
                    let predecessor_cut = commit
                        .order
                        .predecessor_cut()
                        .map_err(|error| StorePullError::Database(error.to_string()))?;
                    Some(
                        Box::pin(verify_store_history_state(
                            storage,
                            None,
                            &root,
                            &predecessor_cut,
                            &commit.membership_state,
                        ))
                        .await?,
                    )
                } else {
                    None
                };
                let accepted_predecessor = verified_accepted_predecessor
                    .as_ref()
                    .map(|_| VerifiedAcceptedPredecessor::Exact);
                let registrations = match Box::pin(load_commit_registrations(
                    storage,
                    &root,
                    &commit,
                    &registration,
                    Some(&predecessor_authority),
                    accepted_predecessor.as_ref(),
                ))
                .await
                {
                    Ok(registrations) => registrations,
                    Err(RegistrationLoadError::Object(error)) => {
                        held.push(held_commit(&commit_ref, held_object_error(error)));
                        continue;
                    }
                    Err(RegistrationLoadError::Invalid(error)) => {
                        held.push(held_commit(
                            &commit_ref,
                            HeldStorePositionReason::InvalidObject(error),
                        ));
                        continue;
                    }
                };
                if !membership_authorizes(Some(&predecessor_membership), &commit, &registration) {
                    held.push(held_commit(
                        &commit_ref,
                        HeldStorePositionReason::Unauthorized,
                    ));
                    continue;
                }
                let package = match load_store_package(storage, &commit_ref, &commit).await {
                    Ok(package) => package.map(|package| package.value),
                    Err(error) => {
                        held.push(held_package(&commit_ref, &commit, held_object_error(error)));
                        continue;
                    }
                };
                let key = (
                    commit_stream_id(&commit_ref.coord),
                    commit_ref.coord.sequence(),
                );
                let device_operations = if commit.device_exclusion_proposals().is_empty()
                    && commit.device_exclusion_outcomes().is_empty()
                {
                    CandidateDeviceOperations::Verified(
                        VerifiedStoreDeviceOperations::without_exclusions(&commit)
                            .map_err(|error| StorePullError::Database(error.to_string()))?,
                    )
                } else {
                    CandidateDeviceOperations::MergePending {
                        predecessor_membership: predecessor_membership.clone(),
                    }
                };
                candidates.insert(
                    key,
                    MergeCandidate {
                        activation_head,
                        activation_head_object: activation_head_ref.object,
                        candidate: Candidate {
                            commit_ref,
                            commit,
                            author: registration.clone(),
                            package,
                            registrations,
                            device_operations,
                        },
                        predecessor_membership,
                    },
                );
            }
        }

        let retained = Box::pin(db.retained_merge_replay_inputs()).await?;
        let mut loaded_predecessor_memberships = BTreeMap::new();
        for materialization in retained {
            if materialization.commit().membership_authority.is_none() {
                continue;
            }
            let membership = Box::pin(load_merge_predecessor_membership(
                storage,
                &root,
                &materialization.commit().membership_state,
            ))
            .await
            .map_err(|error| match error {
                RegistrationLoadError::Object(error) => StorePullError::Object(error),
                RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
            })?;
            loaded_predecessor_memberships.insert(materialization.commit_ref().clone(), membership);
        }
        for candidate in candidates.values() {
            loaded_predecessor_memberships.insert(
                candidate.candidate.commit_ref.clone(),
                candidate.predecessor_membership.clone(),
            );
        }
        let loaded_predecessor_memberships = LoadedMergePredecessorMemberships {
            by_commit: loaded_predecessor_memberships,
        };

        let schema: Arc<TableSchema> = {
            let tables = tables.to_vec();
            let gates = db.gates();
            Arc::new(
                db.call(move |conn| {
                    TableSchema::for_apply(
                        conn,
                        &tables,
                        &gates,
                        crate::WritePolicy::MergeConcurrent,
                    )
                })
                .await
                .map_err(|error| {
                    StorePullError::Database(format!("load synced table schema: {error}"))
                })?,
            )
        };
        let coverage = db.snapshot_coverage_frontier().await.map_err(|error| {
            StorePullError::Database(format!("load snapshot coverage frontier: {error}"))
        })?;
        let mut frontier = db.materialized_frontier().await.map_err(|error| {
            StorePullError::Database(format!("load materialized frontier: {error}"))
        })?;
        let mut applied_devices = BTreeSet::new();
        let mut row_changes = Vec::new();
        let mut changesets_applied = 0_u64;
        let mut asset_downloads_failed = false;
        let mut blocked = BTreeMap::new();

        loop {
            let mut progressed = false;
            let keys = candidates.keys().cloned().collect::<Vec<_>>();
            for key in keys {
                let candidate = candidates.get(&key).ok_or_else(|| {
                    StorePullError::Database(
                        "Merge candidate disappeared while evaluating readiness".to_string(),
                    )
                })?;
                let exclusion_freezes = db.store_device_exclusion_freezes().await?;
                let current_frontier = CommitFrontier::from_refs(
                    crate::WritePolicy::MergeConcurrent,
                    frontier.clone(),
                )
                .map_err(|error| StorePullError::Database(error.to_string()))?;
                let CommitFrontier::MergeConcurrent(current_frontier) = current_frontier else {
                    return Err(StorePullError::Database(
                        "Merge pull produced a Serial materialized frontier".to_string(),
                    ));
                };
                let (_, current_device_state) = db
                    .store_device_state_for_history_cut(&StoreHistoryCut::MergeConcurrent(
                        current_frontier,
                    ))
                    .await?;
                match readiness(
                    db,
                    storage,
                    &root,
                    &coverage,
                    &frontier,
                    &current_device_state,
                    &exclusion_freezes,
                    &candidate.candidate.commit_ref,
                    &candidate.candidate.commit,
                )
                .await
                .map_err(|error| {
                    StorePullError::Database(format!(
                        "evaluate Store commit readiness for {}/{}: {error}",
                        key.0, key.1
                    ))
                })? {
                    Readiness::AlreadyMaterialized => {
                        candidates.remove(&key);
                        blocked.remove(&key);
                        progressed = true;
                    }
                    Readiness::Held(held_position) => {
                        blocked.insert(key, held_position);
                    }
                    Readiness::Ready => {
                        let candidate = candidates.remove(&key).ok_or_else(|| {
                            StorePullError::Database(
                                "ready Merge candidate disappeared before apply".to_string(),
                            )
                        })?;
                        match Box::pin(apply_candidate(
                            db,
                            storage,
                            &root,
                            store_dir,
                            schema.clone(),
                            &candidate,
                            &loaded_predecessor_memberships,
                            identity,
                        ))
                        .await?
                        {
                            ApplyOutcome::Applied(changes) => {
                                let stream_id =
                                    commit_stream_id(&candidate.candidate.commit_ref.coord);
                                frontier.insert(
                                    stream_id.clone(),
                                    candidate.candidate.commit_ref.clone(),
                                );
                                applied_devices.insert(stream_id);
                                row_changes.extend(changes);
                                changesets_applied =
                                    changesets_applied.checked_add(1).ok_or_else(|| {
                                        StorePullError::Database(
                                            "Store apply count exceeded u64".to_string(),
                                        )
                                    })?;
                                blocked.remove(&key);
                                progressed = true;
                            }
                            ApplyOutcome::Held(reason) => {
                                if matches!(reason, HeldStorePositionReason::BlobDownloadFailed) {
                                    asset_downloads_failed = true;
                                }
                                let held_position =
                                    held_commit(&candidate.candidate.commit_ref, reason);
                                candidates.insert(key.clone(), candidate);
                                blocked.insert(key, held_position);
                            }
                        }
                    }
                }
            }
            if !progressed {
                break;
            }
        }

        held.extend(blocked.into_values());
        held.sort_by(|left, right| {
            (left.coordinate.device_id(), left.coordinate.seq())
                .cmp(&(right.coordinate.device_id(), right.coordinate.seq()))
        });
        let local_blob_cleanup_pending =
            local_cleanup::drain(db, store_dir).await.map_err(|error| {
                StorePullError::Database(format!("drain local blob cleanup intents: {error}"))
            })?;
        let devices_pulled = u64::try_from(applied_devices.len())
            .map_err(|_| StorePullError::Database("pulled device count exceeds u64".to_string()))?;

        Ok(StorePullResult {
            changesets_applied,
            devices_pulled,
            held_positions: held,
            visible_heads,
            serial_head: None,
            row_changes,
            asset_downloads_failed,
            local_blob_cleanup_pending,
            frontier,
        })
    })
}

use super::*;

pub(crate) enum Readiness {
    Ready,
    AlreadyMaterialized,
    Held(HeldStorePosition),
}

enum MaterializedCheck {
    Yes,
    Missing,
    Held(HeldStorePositionReason),
}

pub(crate) fn held_object_error(error: StoreObjectError) -> HeldStorePositionReason {
    match error {
        StoreObjectError::Storage(source) => HeldStorePositionReason::ObjectUnreadable {
            key: "exact Store object".to_string(),
            detail: source.to_string(),
        },
        StoreObjectError::InvalidObject { key, source, .. } => match *source {
            StoreProtocolError::InvalidSignature => HeldStorePositionReason::InvalidSignature,
            StoreProtocolError::RelocatedSlot { .. }
            | StoreProtocolError::RelocatedPackage { .. }
            | StoreProtocolError::StoreRootMismatch { .. }
            | StoreProtocolError::StoreMismatch { .. }
            | StoreProtocolError::FounderMismatch { .. } => {
                HeldStorePositionReason::WrongSlot(source.to_string())
            }
            source => HeldStorePositionReason::ObjectUnreadable {
                key,
                detail: source.to_string(),
            },
        },
    }
}

pub(crate) async fn readiness(
    database: &StoreDatabase,
    commit_verifier: &mut StoreCommitVerifier<'_>,
    root: &StoreRootRef,
    coverage: &super::store_commit::CommitFrontier,
    frontier: &BTreeMap<String, StoreBatchCommitRef>,
    device_state: &ResolvedStoreDeviceState,
    exclusion_freezes: &[StoreDeviceProposalAck],
    commit_ref: &StoreBatchCommitRef,
    commit: &StoreBatchCommit,
) -> Result<Readiness, StorePullError> {
    let stream_id = commit_stream_id(&commit_ref.coord);
    if let Some(current) = frontier.get(&stream_id) {
        if commit_ref.coord.sequence() <= current.coord.sequence() {
            match reference_is_materialized(
                database,
                commit_verifier,
                coverage,
                &stream_id,
                commit_ref,
            )
            .await?
            {
                MaterializedCheck::Yes => return Ok(Readiness::AlreadyMaterialized),
                MaterializedCheck::Missing => {
                    return Ok(Readiness::Held(held_commit(
                        commit_ref,
                        HeldStorePositionReason::MissingCommit,
                    )))
                }
                MaterializedCheck::Held(reason) => {
                    return Ok(Readiness::Held(held_commit(commit_ref, reason)))
                }
            }
        }
        if commit.order.predecessor() != Some(current) {
            let reason = match commit.order.predecessor() {
                Some(missing) => HeldStorePositionReason::MissingPredecessor(missing.clone()),
                None => HeldStorePositionReason::InvalidObject(
                    "non-genesis Merge commit omits its exact predecessor".to_string(),
                ),
            };
            return Ok(Readiness::Held(held_commit(commit_ref, reason)));
        }
        if commit_ref.coord.sequence() != current.coord.sequence() + 1 {
            return Ok(Readiness::Held(held_commit(
                commit_ref,
                HeldStorePositionReason::InvalidObject(
                    "Merge commit sequence does not immediately follow its materialized frontier"
                        .to_string(),
                ),
            )));
        }
    } else if commit_ref.coord.sequence() != 1 || commit.order.predecessor().is_some() {
        let reason = match commit.order.predecessor() {
            Some(missing) => HeldStorePositionReason::MissingPredecessor(missing.clone()),
            None => HeldStorePositionReason::InvalidObject(
                "Merge commit beyond genesis omits its exact predecessor".to_string(),
            ),
        };
        return Ok(Readiness::Held(held_commit(commit_ref, reason)));
    }

    for record in device_state.devices.values() {
        let target_stream = super::store_commit::StreamActivation::device_authorized_stream_id(
            root.store_root_hash,
            &record.registration,
            super::store_commit::StreamAnchorDomain::StoreAnnouncements,
        );
        if target_stream.to_string() != stream_id {
            continue;
        }
        let StoreDeviceStatus::Inactive {
            terminals,
            accepted_cut,
        } = &record.status
        else {
            break;
        };
        let target_cut = accepted_cut.commits();
        let terminal_sequence = match target_cut.get(&target_stream) {
            Some(reference) => reference.coord.sequence(),
            None => 0,
        };
        if commit_ref.coord.sequence() > terminal_sequence {
            return Ok(Readiness::Held(held_commit(
                commit_ref,
                HeldStorePositionReason::InactiveDevice {
                    terminals: terminals.clone(),
                    accepted_cut: accepted_cut.clone(),
                },
            )));
        }
        break;
    }

    for freeze in exclusion_freezes {
        let target_stream = super::store_commit::StreamActivation::device_authorized_stream_id(
            root.store_root_hash,
            &freeze.proposal.target,
            super::store_commit::StreamAnchorDomain::StoreAnnouncements,
        );
        if target_stream.to_string() != stream_id {
            continue;
        }
        let target_cut = freeze.target_cut.commits();
        let frozen_sequence = match target_cut.get(&target_stream) {
            Some(reference) => reference.coord.sequence(),
            None => 0,
        };
        if commit_ref.coord.sequence() > frozen_sequence {
            return Ok(Readiness::Held(held_commit(
                commit_ref,
                HeldStorePositionReason::DeviceExclusionFreeze {
                    proposal: freeze.proposal.clone(),
                    target_cut: freeze.target_cut.clone(),
                },
            )));
        }
    }

    for (required_stream, required_ref) in commit.merge_dependencies() {
        let required_stream = required_stream.to_string();
        match reference_is_materialized(
            database,
            commit_verifier,
            coverage,
            &required_stream,
            required_ref,
        )
        .await?
        {
            MaterializedCheck::Yes => {}
            MaterializedCheck::Missing => {
                return Ok(Readiness::Held(held_dependency(
                    commit_ref,
                    &required_stream,
                    required_ref,
                    HeldStorePositionReason::MissingDependency {
                        device_id: required_stream.clone(),
                        commit: required_ref.clone(),
                    },
                )))
            }
            MaterializedCheck::Held(reason) => {
                return Ok(Readiness::Held(held_dependency(
                    commit_ref,
                    &required_stream,
                    required_ref,
                    reason,
                )))
            }
        }
    }
    Ok(Readiness::Ready)
}

async fn reference_is_materialized(
    database: &StoreDatabase,
    commit_verifier: &mut StoreCommitVerifier<'_>,
    coverage: &super::store_commit::CommitFrontier,
    stream_id: &str,
    reference: &StoreBatchCommitRef,
) -> Result<MaterializedCheck, StorePullError> {
    if commit_stream_id(&reference.coord) != stream_id {
        return Ok(MaterializedCheck::Held(HeldStorePositionReason::WrongSlot(
            format!(
                "commit reference stream {} differs from dependency stream {stream_id}",
                commit_stream_id(&reference.coord)
            ),
        )));
    }
    if let Some(actual) = database
        .exact_materialized_ref(stream_id, reference.coord.sequence())
        .await?
    {
        if actual != *reference {
            return Ok(MaterializedCheck::Held(
                HeldStorePositionReason::HashMismatch {
                    referenced_device_id: stream_id.to_string(),
                    referenced_commit: reference.clone(),
                    materialized_hash: actual.commit_hash,
                },
            ));
        }
        return Ok(MaterializedCheck::Yes);
    }
    let coverage = coverage.clone().into_refs();
    let Some(covered) = coverage.get(stream_id) else {
        return Ok(MaterializedCheck::Missing);
    };
    if reference.coord.sequence() > covered.coord.sequence() {
        return Ok(MaterializedCheck::Missing);
    }
    let mut cursor = covered.clone();
    loop {
        if cursor == *reference {
            return Ok(MaterializedCheck::Yes);
        }
        if cursor.coord.sequence() <= reference.coord.sequence() {
            return Ok(MaterializedCheck::Held(
                HeldStorePositionReason::HashMismatch {
                    referenced_device_id: stream_id.to_string(),
                    referenced_commit: reference.clone(),
                    materialized_hash: cursor.commit_hash,
                },
            ));
        }
        let verified_commit = match commit_verifier.load_ref(&cursor).await {
            Ok(commit) => commit,
            Err(error) => return Ok(MaterializedCheck::Held(held_object_error(error))),
        };
        let Some(predecessor) = verified_commit.value().order.predecessor() else {
            return Ok(MaterializedCheck::Missing);
        };
        cursor = predecessor.clone();
    }
}

pub(crate) async fn verify_merge_commit_currently_materialized(
    database: &StoreDatabase,
    commit_verifier: &mut StoreCommitVerifier<'_>,
    reference: &StoreBatchCommitRef,
) -> Result<(), StorePullError> {
    let stream_id = reference.coord.stream_id.to_string();
    let coverage = database.snapshot_coverage_frontier().await?;
    match reference_is_materialized(database, commit_verifier, &coverage, &stream_id, reference)
        .await?
    {
        MaterializedCheck::Yes => Ok(()),
        MaterializedCheck::Missing => Err(StorePullError::Database(
            "Merge activation commit is absent from current accepted history".to_string(),
        )),
        MaterializedCheck::Held(reason) => Err(StorePullError::Database(format!(
            "Merge activation commit is not current accepted history: {reason:?}"
        ))),
    }
}

pub(super) async fn apply_candidate(
    database: &StoreDatabase,
    history_verifier: &mut MergeHistoryVerifier<'_>,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    store_dir: &StoreDir,
    schema: Arc<TableSchema>,
    merge_candidate: &MergeCandidate,
    loaded_predecessor_memberships: &LoadedMergePredecessorMemberships,
    identity: Option<&crate::keys::UserKeypair>,
    latest_membership: &mut MembershipChain,
    routing_key: Option<&super::circle::RowRoutingKey>,
    verified_root: &super::store_commit::StoreProtocolRoot,
) -> Result<ApplyOutcome, StorePullError> {
    let db = database.sqlite();
    let candidate = &merge_candidate.candidate;
    let commit = candidate.commit();
    let commit_ref = candidate.commit_ref();
    let author = candidate.author();
    let device_operations = merge_candidate.device_operations.clone();
    if !commit.device_exclusion_proposals().is_empty()
        || !commit.device_exclusion_outcomes().is_empty()
    {
        let (state_ref, _) = database.store_device_state_for_order(&commit.order).await?;
        if state_ref != commit.device_state {
            return Err(StorePullError::Database(
                "Merge exclusion commit differs from its materialized predecessor device state"
                    .to_string(),
            ));
        }
    }
    let membership_objects =
        verified_merge_membership_objects(storage, root, commit_ref, commit).await?;
    let (local_store_membership, membership_after_candidate) =
        local_store_membership_after_candidate(
            latest_membership,
            root,
            &merge_candidate.predecessor_membership,
            membership_objects.as_ref(),
            identity,
        )?;
    let verified_prefix = VerifiedStreamActivationPrefix::empty();
    let circle_activations = if commit.control().is_some() {
        merge_candidate.membership_control.clone().ok_or_else(|| {
            PullCircleActivationError::Invalid(
                "Merge membership control is absent from its operation-verified history"
                    .to_string(),
            )
        })
    } else {
        load_circle_payload_activations(
            database,
            storage,
            root,
            &candidate.verified,
            identity.filter(|_| local_store_membership.allows_circle_access()),
            routing_key,
            &verified_prefix,
            verified_root,
            &merge_candidate.membership_prefix,
        )
        .await
    };
    let verified_circle_activations = match circle_activations {
        Ok(activations) => activations,
        Err(PullCircleActivationError::Database(error)) => return Err(error.into()),
        Err(PullCircleActivationError::Invalid(error)) => {
            return Ok(ApplyOutcome::Held(HeldStorePositionReason::InvalidObject(
                error,
            )))
        }
    };
    // An excluded device that cannot yet read its successor bootstrap records the
    // exclusion now — detection is derived from the verified outcome, not the
    // bootstrap — and holds the successor. Its position advances only once a later
    // pull reads the bootstrap and reseeds; publication stays gated meanwhile.
    if !verified_circle_activations
        .bootstrap_pending_exclusions()
        .is_empty()
    {
        let pending = verified_circle_activations
            .bootstrap_pending_exclusions()
            .to_vec();
        database
            .sqlite()
            .call(move |conn| {
                for exclusion in &pending {
                    crate::sync::store::database::StoreDatabase::record_circle_close_exclusion_on(
                        conn, exclusion,
                    )?;
                }
                Ok(())
            })
            .await?;
        return Ok(ApplyOutcome::Held(HeldStorePositionReason::InvalidObject(
            "excluded device awaiting its successor bootstrap to reset".to_string(),
        )));
    }
    let circle_packages = match load_applicable_circle_packages(
        database,
        history_verifier.commit_verifier(),
        storage,
        root,
        &candidate.verified,
        verified_circle_activations.circles(),
        author,
        local_store_membership,
    )
    .await
    {
        Ok(packages) => packages,
        Err(PullCircleActivationError::Database(error)) => return Err(error.into()),
        Err(PullCircleActivationError::Invalid(error)) => {
            return Ok(ApplyOutcome::Held(HeldStorePositionReason::InvalidObject(
                error,
            )))
        }
    };
    let mut packages =
        Vec::with_capacity(usize::from(candidate.package.is_some()) + circle_packages.len());
    if let Some(bytes) = candidate.package.as_ref() {
        let package = match parse_candidate_store_package(candidate, bytes) {
            Ok(package) => package,
            Err(error) => {
                return Ok(ApplyOutcome::Held(
                    HeldStorePositionReason::InvalidChangeset(error),
                ))
            }
        };
        let protection = storage.store_blob_protection()?;
        match prepare_merge_candidate_package(
            db,
            storage,
            store_dir,
            schema.clone(),
            package,
            protection,
        )
        .await?
        {
            Ok(package) => packages.push(package),
            Err(reason) => return Ok(ApplyOutcome::Held(reason)),
        }
    }
    for loaded in &circle_packages {
        let package = match parse_candidate_circle_package(candidate, loaded) {
            Ok(package) => package,
            Err(error) => {
                return Ok(ApplyOutcome::Held(
                    HeldStorePositionReason::InvalidChangeset(error),
                ))
            }
        };
        match prepare_merge_candidate_package(
            db,
            storage,
            store_dir,
            schema.clone(),
            package,
            loaded.blob_protection.clone(),
        )
        .await?
        {
            Ok(package) => packages.push(package),
            Err(reason) => return Ok(ApplyOutcome::Held(reason)),
        }
    }
    let outcome = Box::pin(commit_candidate(
        database,
        storage,
        root,
        history_verifier,
        merge_candidate,
        packages,
        device_operations,
        verified_circle_activations,
        membership_objects,
        loaded_predecessor_memberships,
        local_store_membership,
        routing_key,
    ))
    .await?;
    if matches!(outcome, ApplyOutcome::Applied(_)) {
        *latest_membership = membership_after_candidate;
    }
    #[cfg(any(test, feature = "test-utils"))]
    if matches!(outcome, ApplyOutcome::Applied(_)) {
        db.reach_test_point(crate::database::DatabaseTestPoint::PullAfterRemoteCommit {
            device_id: commit_stream_id(&commit_ref.coord),
            seq: commit.seq(),
        })
        .await;
    }
    Ok(outcome)
}

fn local_store_membership_after_candidate(
    latest: &MembershipChain,
    root: &StoreRootRef,
    predecessor: &MembershipChain,
    membership_objects: Option<&VerifiedMergeMembershipClosure>,
    identity: Option<&crate::keys::UserKeypair>,
) -> Result<(LocalStoreMembership, MembershipChain), StorePullError> {
    let candidate = if let Some(membership_objects) = membership_objects {
        let proof = &membership_objects.proof;
        let mut successor = predecessor.clone();
        match (&proof.resolution, &proof.resolution_value) {
            (Some(reference), Some(value)) => successor
                .apply_resolutions(root.store_root_hash, &[(reference.clone(), value.clone())])
                .map_err(|error| StorePullError::Database(error.to_string()))?,
            (None, None) => {}
            _ => {
                return Err(StorePullError::Database(
                    "verified Merge membership proof has incomplete resolution evidence"
                        .to_string(),
                ))
            }
        }
        successor
            .add_entry(proof.entry_value.clone())
            .and_then(|()| successor.activate_head_ref(proof.head.clone()))
            .map_err(|error| StorePullError::Database(error.to_string()))?;
        successor
    } else {
        predecessor.clone()
    };
    let candidate_state = LocalStoreMembership::from_membership(&candidate, identity)
        .map_err(StorePullMembershipError::State)
        .map_err(StorePullError::Membership)?;
    if candidate.causally_includes(latest) {
        return Ok((candidate_state, candidate));
    }
    if latest.causally_includes(&candidate) {
        let latest_state = LocalStoreMembership::from_membership(latest, identity)
            .map_err(StorePullMembershipError::State)
            .map_err(StorePullError::Membership);
        return Ok((
            historical_local_store_membership(latest_state?, candidate_state),
            latest.clone(),
        ));
    }
    Err(StorePullError::Membership(
        StorePullMembershipError::Message(
            "latest Store membership and exact candidate membership are causally incomparable"
                .to_string(),
        ),
    ))
}

pub(super) fn historical_local_store_membership(
    latest: LocalStoreMembership,
    candidate: LocalStoreMembership,
) -> LocalStoreMembership {
    if matches!(latest, LocalStoreMembership::Removed)
        || matches!(candidate, LocalStoreMembership::Removed)
    {
        LocalStoreMembership::Removed
    } else if matches!(latest, LocalStoreMembership::Current)
        && matches!(candidate, LocalStoreMembership::Current)
    {
        LocalStoreMembership::Current
    } else if matches!(latest, LocalStoreMembership::IdentityNotSupplied)
        || matches!(candidate, LocalStoreMembership::IdentityNotSupplied)
    {
        LocalStoreMembership::IdentityNotSupplied
    } else {
        LocalStoreMembership::NotYetMember
    }
}

pub(crate) struct PreparedMergeMaterializationPackage {
    pub(crate) package: AudiencePackage,
    pub(crate) changeset: ValidatedChangeset<Vec<u8>>,
}

async fn prepare_merge_candidate_package(
    db: &Database,
    storage: &dyn SyncStorage,
    store_dir: &StoreDir,
    schema: Arc<TableSchema>,
    package: AudiencePackage,
    blob_protection: BlobSpoolProtection,
) -> Result<Result<PreparedMergeMaterializationPackage, HeldStorePositionReason>, StorePullError> {
    let changeset = match ValidatedChangeset::new(package.changeset().to_vec(), schema) {
        Ok(changeset) => changeset,
        Err(super::session::ChangesetIdentityError::Row(error)) => {
            return Ok(Err(HeldStorePositionReason::InvalidRowIdentity {
                table: error.table().to_string(),
                reason: error.to_string(),
            }))
        }
        Err(error) => {
            return Ok(Err(HeldStorePositionReason::InvalidChangeset(
                error.to_string(),
            )))
        }
    };
    let changes = crate::changeset::walk(changeset.bytes())
        .map_err(HeldStorePositionReason::InvalidChangeset);
    let changes = match changes {
        Ok(changes) => changes,
        Err(reason) => return Ok(Err(reason)),
    };
    let old_changes = match crate::changeset::walk_old(changeset.bytes()) {
        Ok(changes) => changes,
        Err(error) => return Ok(Err(HeldStorePositionReason::InvalidChangeset(error))),
    };
    let blob_decls = db.blob_decls();
    let eager = match cache_eager_blobs(&blob_decls, &changes, &package) {
        Ok(eager) => eager,
        Err(error) => {
            return Ok(Err(HeldStorePositionReason::InvalidChangeset(
                error.to_string(),
            )))
        }
    };
    if let Err(failures) = verify_package_blobs(
        db,
        storage,
        store_dir,
        package.blob_bindings(),
        blob_protection,
        &eager,
    )
    .await
    {
        if failures.has_transport_failure() {
            return Err(StorePullError::BlobDownloads(failures));
        }
        return Ok(Err(HeldStorePositionReason::BlobDownloadFailed));
    }
    if let Err(error) = local_blob_cleanup_intents(&blob_decls, &old_changes, &changes) {
        return Ok(Err(HeldStorePositionReason::InvalidChangeset(
            error.to_string(),
        )));
    }
    Ok(Ok(PreparedMergeMaterializationPackage {
        package,
        changeset,
    }))
}

pub(crate) struct PreparedMergeMaterialization {
    pub(crate) root: StoreRootRef,
    pub(crate) verified_commit: VerifiedStoreBatchCommit,
    pub(crate) activation_head: StoreDeviceHead,
    pub(crate) activation_head_object: ExactObjectRef,
    pub(crate) history_summary: RetainedVerifiedMergeHistorySummary,
    pub(crate) membership_objects: Option<crate::database::VerifiedMergeMembershipObjects>,
    pub(crate) membership_remote_objects: Vec<super::remote_object::RemoteObjectRecord>,
    pub(crate) registrations: Vec<(StoreDeviceRegistration, StoreDeviceRegistrationActivation)>,
    pub(crate) packages: Vec<PreparedMergeMaterializationPackage>,
    pub(crate) device_operations: VerifiedStoreDeviceOperations,
    pub(crate) circle_activations: VerifiedCircleActivations,
    pub(crate) package_application: Option<crate::database::RetainedPackageApplication>,
}

pub(crate) struct AppliedMergeMaterialization {
    pub(crate) outcome: ApplyOutcome,
    pub(crate) max_updated_at: Option<super::hlc::Timestamp>,
    pub(crate) write_status_notifications: Vec<(crate::WriteId, crate::WriteStatus)>,
    pub(crate) retained: Option<crate::database::OwnedVerifiedMergeMaterialization>,
}

enum MergeSubsetOutcome {
    Applied(Vec<crate::sync::apply::WinningRow>),
    ConstraintConflict(Vec<String>),
}

impl MergeSubsetOutcome {
    fn extend_winning_rows(
        self,
        winning_rows: &mut Vec<crate::sync::apply::WinningRow>,
    ) -> Result<(), Vec<String>> {
        match self {
            Self::Applied(rows) => {
                winning_rows.extend(rows);
                Ok(())
            }
            Self::ConstraintConflict(tables) => Err(tables),
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn apply_merge_subset_on(
    conn: &rusqlite::Transaction<'_>,
    blob_decls: &BlobDecls,
    gates: &super::gate::Gates,
    routing_key: Option<&super::circle::RowRoutingKey>,
    source: &ValidatedChangeset<Vec<u8>>,
    bytes: Vec<u8>,
    package_audience: Option<&super::circle::Audience>,
    timestamp_policy: IncomingTimestampPolicy,
    changeset_max: &mut Option<super::hlc::Timestamp>,
    returned_changes: &mut Vec<RowChange>,
    package_reported_fk_violation: &mut bool,
) -> Result<MergeSubsetOutcome, DbError> {
    let applied_changeset = source
        .validate_subset(bytes.clone())
        .map_err(|error| DbError::Message(error.to_string()))?;
    let actual_changes = crate::changeset::walk(&bytes).map_err(DbError::Message)?;
    if let Some(receiver_wall_ms) = timestamp_policy.received_wall_ms() {
        advance_max_updated_at(
            changeset_max,
            &actual_changes,
            source.schema(),
            receiver_wall_ms,
        );
    }
    returned_changes.extend(
        actual_changes
            .iter()
            .filter(|change| !super::gate::is_routing_table(&change.table))
            .cloned(),
    );
    let apply =
        resolve_and_apply_changeset_with_policy_on(conn, applied_changeset, timestamp_policy)?;
    if !apply.constraint_conflict_tables.is_empty() {
        return Ok(MergeSubsetOutcome::ConstraintConflict(
            apply.constraint_conflict_tables,
        ));
    }
    *package_reported_fk_violation |= apply.had_fk_violations;
    if let Some(package_audience) = package_audience {
        super::gate::align_inbound_scoped_root_audiences(
            conn,
            &bytes,
            package_audience,
            gates,
            routing_key.ok_or_else(|| {
                DbError::Message(
                    "scoped audience application requires a row-routing key".to_string(),
                )
            })?,
        )
        .map_err(|error| DbError::Message(error.to_string()))?;
    }
    let winning_rows =
        crate::sync::apply::current_winning_rows_with_schema(conn, source.schema(), &bytes)?;
    let old_changes = crate::changeset::walk_old(&bytes).map_err(DbError::Message)?;
    let cleanup = local_blob_cleanup_intents(blob_decls, &old_changes, &actual_changes)
        .map_err(|error| DbError::Message(error.to_string()))?;
    for intent in cleanup {
        local_cleanup::record_obsolete_copy_intents_on(conn, blob_decls, &intent)?;
    }
    Ok(MergeSubsetOutcome::Applied(winning_rows))
}

#[allow(clippy::too_many_arguments)]
fn apply_merge_package_on(
    conn: &rusqlite::Transaction<'_>,
    blob_decls: &BlobDecls,
    gates: &super::gate::Gates,
    routing_key: Option<&super::circle::RowRoutingKey>,
    package: &AudiencePackage,
    changeset: &ValidatedChangeset<Vec<u8>>,
    store_audience_transitions: &super::gate::StoreAudienceTransitions,
    timestamp_policy: IncomingTimestampPolicy,
    changeset_max: &mut Option<super::hlc::Timestamp>,
    returned_changes: &mut Vec<RowChange>,
    package_reported_fk_violation: &mut bool,
) -> Result<MergeSubsetOutcome, DbError> {
    let mut winning_rows = Vec::new();
    match package.audience() {
        PackageAudience::Store if gates.has_scoped_graph() => {
            let routing_key = routing_key.ok_or_else(|| {
                DbError::Message(
                    "scoped Store package application requires a row-routing key".to_string(),
                )
            })?;
            let inbound = super::gate::normalize_inbound_store_changeset(
                conn,
                package.changeset(),
                gates,
                routing_key,
            )
            .map_err(|error| DbError::Message(error.to_string()))?;
            if let Err(tables) = apply_merge_subset_on(
                conn,
                blob_decls,
                gates,
                Some(routing_key),
                changeset,
                inbound.mirror,
                None,
                timestamp_policy,
                changeset_max,
                returned_changes,
                package_reported_fk_violation,
            )?
            .extend_winning_rows(&mut winning_rows)
            {
                return Ok(MergeSubsetOutcome::ConstraintConflict(tables));
            }
            let rows =
                super::gate::filter_inbound_store_rows(conn, &inbound.rows, gates, routing_key)
                    .map_err(|error| DbError::Message(error.to_string()))?;
            if let Err(tables) = apply_merge_subset_on(
                conn,
                blob_decls,
                gates,
                Some(routing_key),
                changeset,
                rows,
                Some(&super::circle::Audience::Store),
                timestamp_policy,
                changeset_max,
                returned_changes,
                package_reported_fk_violation,
            )?
            .extend_winning_rows(&mut winning_rows)
            {
                return Ok(MergeSubsetOutcome::ConstraintConflict(tables));
            }
        }
        PackageAudience::Store => {
            return apply_merge_subset_on(
                conn,
                blob_decls,
                gates,
                None,
                changeset,
                package.changeset().to_vec(),
                None,
                timestamp_policy,
                changeset_max,
                returned_changes,
                package_reported_fk_violation,
            );
        }
        PackageAudience::Circle { circle_id, .. } => {
            let routing_key = routing_key.ok_or_else(|| {
                DbError::Message(
                    "Circle package application requires a row-routing key".to_string(),
                )
            })?;
            let rows = super::gate::filter_inbound_circle_changeset(
                conn,
                package.changeset(),
                *circle_id,
                store_audience_transitions,
                gates,
                routing_key,
            )
            .map_err(|error| DbError::Message(error.to_string()))?;
            return apply_merge_subset_on(
                conn,
                blob_decls,
                gates,
                Some(routing_key),
                changeset,
                rows,
                Some(&super::circle::Audience::Circle(*circle_id)),
                timestamp_policy,
                changeset_max,
                returned_changes,
                package_reported_fk_violation,
            );
        }
    }
    Ok(MergeSubsetOutcome::Applied(winning_rows))
}

pub(crate) fn apply_prepared_merge_materialization_on(
    conn: &rusqlite::Transaction<'_>,
    blob_decls: &BlobDecls,
    gates: &super::gate::Gates,
    synced_tables: &[SyncedTable],
    routing_key: Option<&super::circle::RowRoutingKey>,
    local_store_membership: LocalStoreMembership,
    timestamp_policy: IncomingTimestampPolicy,
    baseline_circle_cuts: Option<
        &BTreeMap<super::circle::CircleId, crate::sync::store_commit::CommitFrontier>,
    >,
    materialization: PreparedMergeMaterialization,
) -> Result<AppliedMergeMaterialization, DbError> {
    let PreparedMergeMaterialization {
        root,
        verified_commit,
        activation_head,
        activation_head_object,
        history_summary,
        membership_objects,
        membership_remote_objects,
        registrations,
        packages,
        device_operations,
        circle_activations,
        package_application,
    } = materialization;
    let commit = verified_commit.value();
    let commit_ref = verified_commit.reference();
    let mut inactive_circles = circle_activations
        .circles()
        .iter()
        .filter_map(|activation| {
            activation
                .local_access
                .as_ref()
                .filter(|access| access.active.is_none())
                .filter(|_| {
                    baseline_circle_cuts
                        .and_then(|cuts| cuts.get(&activation.circle_id))
                        .is_none_or(|cut| !cut.covers_commit(commit_ref))
                })
                .map(|_| activation.circle_id)
        })
        .collect::<BTreeSet<_>>();
    let mut changeset_max = None;
    let mut returned_changes = Vec::new();
    let mut package_reported_fk_violation = false;
    crate::sync::store::database::StoreDatabase::record_activated_store_device_registrations_on(
        conn,
        commit,
        &registrations,
    )?;
    for bootstrap in circle_activations.bootstraps() {
        super::replay::install_circle_bootstrap_remote_objects_on(conn, commit_ref, bootstrap)?;
    }
    let store_transaction = crate::sync::store::database::StoreDatabaseTransaction::new(conn);
    store_transaction
        .record_verified_circle_activations(&verified_commit, circle_activations.circles())?;
    // A Circle whose winning control chain is now Deleted prunes its rows,
    // routes, and blob bindings like an inactive recipient, and drops its live
    // access/roster/metadata caches; the authority spine is retained above.
    for activation in circle_activations.circles() {
        if crate::sync::store::database::StoreDatabase::circle_current_state_is_deleted_on(
            conn,
            activation.circle_id,
        )? {
            inactive_circles.insert(activation.circle_id);
            crate::sync::store::database::StoreDatabase::remove_deleted_circle_caches_on(
                conn,
                activation.circle_id,
            )?;
        }
    }
    let retained_packages = packages
        .iter()
        .map(|prepared| prepared.package.clone())
        .collect::<Vec<_>>();
    let store_audience_transitions = packages
        .iter()
        .find(|prepared| matches!(prepared.package.audience(), PackageAudience::Store))
        .map(|prepared| super::gate::store_audience_transitions(prepared.package.changeset()))
        .transpose()
        .map_err(|error| DbError::Message(error.to_string()))?
        .unwrap_or_default();
    for prepared in packages {
        let PreparedMergeMaterializationPackage { package, changeset } = prepared;
        let winning_rows = match apply_merge_package_on(
            conn,
            blob_decls,
            gates,
            routing_key,
            &package,
            &changeset,
            &store_audience_transitions,
            timestamp_policy,
            &mut changeset_max,
            &mut returned_changes,
            &mut package_reported_fk_violation,
        )? {
            MergeSubsetOutcome::Applied(rows) => rows,
            MergeSubsetOutcome::ConstraintConflict(tables) => {
                return Ok(AppliedMergeMaterialization {
                    outcome: ApplyOutcome::Held(HeldStorePositionReason::ConstraintConflict(
                        tables,
                    )),
                    max_updated_at: None,
                    write_status_notifications: Vec::new(),
                    retained: None,
                });
            }
        };
        let retained = crate::sync::store::database::StoreDatabase::retained_audience_package(
            commit,
            commit_ref,
            package.clone(),
        )?;
        Database::install_pulled_package_activation_on(
            conn,
            commit_ref,
            retained.domain(),
            retained.object(),
            retained.package(),
        )?;
        Database::install_pulled_blob_activations_on(conn, &package, commit_ref)?;
        Database::install_winning_blob_bindings_on(
            conn,
            gates,
            synced_tables,
            &package,
            &BlobActivation {
                coord: commit_ref.coord.clone(),
            },
            &winning_rows,
        )?;
    }
    if gates.has_scoped_graph() && !local_store_membership.retains_circle_rows() {
        let mut statement = conn
            .prepare(
                "SELECT DISTINCT circle_id
                 FROM _coven_audience
                 WHERE circle_id IS NOT NULL
                 ORDER BY circle_id",
            )
            .map_err(DbError::from)?;
        let circles = statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(DbError::from)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(DbError::from)?;
        drop(statement);
        for encoded in circles {
            inactive_circles.insert(encoded.parse().map_err(|error| {
                DbError::Message(format!(
                    "parse materialized Circle audience {encoded}: {error}"
                ))
            })?);
        }
        crate::sync::store::database::StoreDatabase::remove_local_circle_access_on(conn)?;
    }
    let mut removal_session = rusqlite::session::Session::new(conn).map_err(DbError::from)?;
    for table in synced_tables {
        removal_session
            .attach(Some(table.name()))
            .map_err(DbError::from)?;
    }
    super::gate::prune_ineligible_scoped_rows(conn, gates, &inactive_circles)
        .map_err(|error| DbError::Message(error.to_string()))?;
    super::gate::validate_scoped_foreign_key_audiences(conn, gates)
        .map_err(|error| DbError::Message(error.to_string()))?;
    let mut removal_changeset = Vec::new();
    removal_session
        .changeset_strm(&mut removal_changeset)
        .map_err(DbError::from)?;
    drop(removal_session);
    let removed = crate::changeset::walk_old(&removal_changeset).map_err(DbError::Message)?;
    let removal_changes = crate::changeset::walk(&removal_changeset).map_err(DbError::Message)?;
    let removal_cleanup = local_blob_cleanup_intents(blob_decls, &removed, &removal_changes)
        .map_err(|error| DbError::Message(error.to_string()))?;
    returned_changes.extend(removal_changes);
    for intent in removal_cleanup {
        local_cleanup::record_obsolete_copy_intents_on(conn, blob_decls, &intent)?;
    }
    if package_reported_fk_violation {
        let violations: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM pragma_foreign_key_check)",
                [],
                |row| row.get(0),
            )
            .map_err(DbError::from)?;
        if violations {
            return Ok(AppliedMergeMaterialization {
                outcome: ApplyOutcome::Held(HeldStorePositionReason::ForeignKeyDependency),
                max_updated_at: None,
                write_status_notifications: Vec::new(),
                retained: None,
            });
        }
    }
    let verified = VerifiedMergeMaterialization::verify(
        &root,
        &verified_commit,
        &registrations,
        &device_operations,
        &circle_activations,
        &activation_head,
        &activation_head_object,
        &history_summary,
        membership_objects.as_ref(),
        &retained_packages,
        package_application,
    )?;
    Database::install_pulled_merge_membership_activations_on(
        conn,
        commit_ref,
        &membership_remote_objects,
    )?;
    let retained = store_transaction.record_verified_merge_materialization(verified)?;
    Ok(AppliedMergeMaterialization {
        outcome: ApplyOutcome::Applied(returned_changes),
        max_updated_at: changeset_max,
        write_status_notifications: Vec::new(),
        retained: Some(retained),
    })
}

pub(crate) async fn verified_merge_membership_objects(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit_ref: &StoreBatchCommitRef,
    commit: &StoreBatchCommit,
) -> Result<Option<VerifiedMergeMembershipClosure>, StorePullError> {
    let Some(super::store_commit::StoreControl { transition }) = commit.control() else {
        return Ok(None);
    };
    let entry = super::store_objects::load_membership_entry_ref(
        storage,
        root.store_root_hash,
        &transition.body.entry,
    )
    .await
    .map_err(StorePullError::Object)?;
    let author = super::store_objects::load_registration_ref(
        storage,
        root,
        &transition.body.author_registration,
    )
    .await
    .map_err(StorePullError::Object)?
    .value;
    let semantic_prefix = transition
        .head_slot
        .logical_key()
        .strip_suffix(".json")
        .ok_or_else(|| {
            StorePullError::Database(
                "Merge membership head slot has no protocol extension".to_string(),
            )
        })?;
    let context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreMembershipHead,
    );
    let (head_bytes, head_object) = storage
        .read_protocol_slot(&context, &transition.head_slot, semantic_prefix)
        .await
        .map_err(StoreObjectError::from)
        .map_err(StorePullError::Object)?;
    let head: super::membership::AuthorHead = serde_json::from_slice(&head_bytes)
        .map_err(|error| StorePullError::Database(format!("Merge membership head: {error}")))?;
    if !head.verify(&author)
        || serde_json::to_vec(&head).map_err(|error| {
            StorePullError::Database(format!("serialize membership head: {error}"))
        })? != head_bytes
    {
        return Err(StorePullError::Database(
            "Merge membership head is not canonical or has an invalid device signature".to_string(),
        ));
    }
    let head_ref = super::membership::MembershipHeadRef {
        coord: head.entry_coord(),
        head_hash: head.head_hash(),
        object: head_object,
    };
    let objects = crate::database::VerifiedMergeMembershipObjects::verify(
        commit,
        commit_ref,
        &entry.value,
        &head,
        head_ref.clone(),
    )
    .map_err(|error| StorePullError::Database(error.to_string()))?;
    let family = commit.candidate_family();
    let resolution = match &entry.value.change {
        super::membership::MembershipChange::ResolutionActivation { resolution } => {
            Some(resolution.clone())
        }
        _ => None,
    };
    let resolution_loaded = if let Some(resolution) = &resolution {
        let loaded = super::store_objects::load_membership_resolution_ref(
            storage,
            root.store_root_hash,
            resolution,
        )
        .await
        .map_err(StorePullError::Object)?;
        Some((loaded.bytes, loaded.value))
    } else {
        None
    };
    let remote_objects = activated_merge_membership_remote_objects(
        family,
        &objects,
        MembershipAuthorityBytes::identical(entry.bytes),
        MembershipAuthorityBytes::identical(head_bytes),
        resolution_loaded
            .as_ref()
            .map(|(bytes, _)| MembershipAuthorityBytes::identical(bytes.clone())),
        commit_ref,
    )
    .map_err(|error| StorePullError::Database(error.to_string()))?;
    let resolution_value = resolution_loaded.map(|(_, value)| value);
    let proof = super::store_commit::RetainedMergeMembershipProof {
        commit: commit_ref.clone(),
        commit_value: commit.clone(),
        announcement: None,
        entry: transition.body.entry.clone(),
        entry_value: entry.value,
        head: head_ref,
        head_value: head,
        resolution,
        resolution_value,
    };
    Ok(Some(VerifiedMergeMembershipClosure {
        objects,
        remote_objects,
        proof,
    }))
}

pub(crate) struct VerifiedMergeMembershipClosure {
    objects: crate::database::VerifiedMergeMembershipObjects,
    remote_objects: Vec<super::remote_object::RemoteObjectRecord>,
    pub(crate) proof: super::store_commit::RetainedMergeMembershipProof,
}

pub(super) struct MembershipAuthorityBytes {
    canonical: Vec<u8>,
    stored: Vec<u8>,
}

impl MembershipAuthorityBytes {
    fn identical(bytes: Vec<u8>) -> Self {
        Self {
            canonical: bytes.clone(),
            stored: bytes,
        }
    }

    pub(super) fn new(canonical: Vec<u8>, stored: Vec<u8>) -> Self {
        Self { canonical, stored }
    }
}

pub(super) fn activated_merge_membership_remote_objects(
    family: super::store_commit::CandidateFamilyId,
    objects: &crate::database::VerifiedMergeMembershipObjects,
    entry_bytes: MembershipAuthorityBytes,
    head_bytes: MembershipAuthorityBytes,
    resolution_bytes: Option<MembershipAuthorityBytes>,
    commit_ref: &StoreBatchCommitRef,
) -> Result<
    Vec<super::remote_object::RemoteObjectRecord>,
    super::remote_object::RemoteObjectRecordError,
> {
    let mut remotes = vec![
        activate_merge_membership_authority(
            super::remote_object::RemoteObjectRecord::candidate_exclusive_merge_membership_entry(
                family,
                objects.entry().clone(),
                entry_bytes.canonical,
                entry_bytes.stored,
                commit_ref.clone(),
            )?,
            commit_ref,
        )?,
        activate_merge_membership_authority(
            super::remote_object::RemoteObjectRecord::candidate_exclusive_merge_membership_head(
                family,
                objects.head().clone(),
                head_bytes.canonical,
                head_bytes.stored,
                commit_ref.clone(),
            )?,
            commit_ref,
        )?,
    ];
    if let Some(resolution) = objects.resolution() {
        let bytes = resolution_bytes
            .ok_or(super::remote_object::RemoteObjectRecordError::StoredReferenceMismatch)?;
        remotes.push(activate_merge_membership_authority(
            super::remote_object::RemoteObjectRecord::candidate_activated_store_membership_resolution(
                resolution.clone(),
                bytes.canonical,
                bytes.stored,
                commit_ref.clone(),
            )?,
            commit_ref,
        )?);
    } else if resolution_bytes.is_some() {
        return Err(super::remote_object::RemoteObjectRecordError::StoredReferenceMismatch);
    }
    Ok(remotes)
}

fn activate_merge_membership_authority(
    mut remote: super::remote_object::RemoteObjectRecord,
    commit_ref: &StoreBatchCommitRef,
) -> Result<super::remote_object::RemoteObjectRecord, super::remote_object::RemoteObjectRecordError>
{
    remote.mark_uploaded_verified()?;
    remote.into_activated(commit_ref)
}

async fn commit_candidate(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    history_verifier: &mut MergeHistoryVerifier<'_>,
    merge_candidate: &MergeCandidate,
    packages: Vec<PreparedMergeMaterializationPackage>,
    device_operations: VerifiedStoreDeviceOperations,
    verified_circle_activations: VerifiedCircleActivations,
    membership: Option<VerifiedMergeMembershipClosure>,
    loaded_predecessor_memberships: &LoadedMergePredecessorMemberships,
    local_store_membership: LocalStoreMembership,
    routing_key: Option<&super::circle::RowRoutingKey>,
) -> Result<ApplyOutcome, StorePullError> {
    let db = database.sqlite();
    let candidate = &merge_candidate.candidate;
    let commit = candidate.commit();
    let commit_ref = candidate.commit_ref();
    let author = candidate.author();
    let predecessor_membership = &merge_candidate.predecessor_membership;
    let (_, predecessor_state) = database.store_device_state_for_order(&commit.order).await?;
    verify_merge_membership_state_ref(
        &commit.membership_state,
        predecessor_membership,
        &predecessor_state,
    )?;
    let (authorized_predecessor, recovery_author) =
        predecessor_with_recovery_author(predecessor_state, commit, &candidate.registrations)
            .map_err(|error| StorePullError::Database(error.to_string()))?;
    let owner_recovery = verify_commit_owner_recovery_activation(storage, root, commit).await?;
    let state_after = device_operations
        .apply_to(authorized_predecessor.clone(), &commit.device_state)
        .and_then(|state| {
            apply_verified_device_lifecycle(
                state,
                commit,
                &candidate.registrations,
                recovery_author.as_ref(),
                owner_recovery,
            )
        })
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    let acknowledgement = validate_commit_acknowledgement(storage, root, commit, author)
        .await
        .map_err(|error| match error {
            RegistrationLoadError::Object(error) => StorePullError::Object(error),
            RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
        })?;
    let retained_acknowledgement = match acknowledgement {
        Some((acknowledgement_ref, acknowledgement_value)) => Some(
            retain_activated_acknowledgement(
                storage,
                root,
                commit_ref,
                commit,
                author,
                acknowledgement_ref,
                acknowledgement_value,
            )
            .await?,
        ),
        None => None,
    };
    let registrations = commit
        .device_registrations()
        .iter()
        .zip(&candidate.registrations)
        .map(|(activation, (value, _))| RetainedVerifiedRegistration {
            reference: activation.registration.clone(),
            value: value.clone(),
        })
        .collect();
    let history = prepare_merge_history_successor(
        database,
        root,
        commit,
        commit_ref,
        predecessor_membership,
        author,
        recovery_author.as_ref(),
        state_after.clone(),
        MergeHistorySuccessorEvidence {
            registrations,
            acknowledgement: retained_acknowledgement,
            membership_proof: membership.as_ref().map(|closure| closure.proof.clone()),
        },
    )
    .await?;
    let activation_head_ref = super::store_commit::StoreDeviceHeadRef {
        head_hash: merge_candidate.activation_head.head_hash(),
        object: merge_candidate.activation_head_object.clone(),
    };
    history
        .summary
        .open(
            commit,
            commit_ref,
            &merge_candidate.activation_head,
            &activation_head_ref,
            &state_after,
        )
        .map_err(|error| {
            StorePullError::Database(format!("open prepared Merge history summary: {error}"))
        })?;
    history_verifier
        .commit_verifier()
        .remember(candidate.verified.clone())
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    let retractions = Box::pin(verified_terminal_merge_retractions(
        database,
        storage,
        root,
        history_verifier,
        &merge_candidate.activation_head,
        &merge_candidate.activation_head_object,
        &candidate.verified,
        &authorized_predecessor,
        predecessor_membership,
        &device_operations,
        loaded_predecessor_memberships,
    ))
    .await?;
    let receiver_wall_ms = db.receive_wall_ms();
    let materialization = PreparedMergeMaterialization {
        root: root.clone(),
        verified_commit: candidate.verified.clone(),
        activation_head: merge_candidate.activation_head.clone(),
        activation_head_object: merge_candidate.activation_head_object.clone(),
        history_summary: history.summary,
        membership_objects: membership.as_ref().map(|closure| closure.objects.clone()),
        membership_remote_objects: membership
            .map(|closure| closure.remote_objects)
            .unwrap_or_default(),
        registrations: candidate.registrations.clone(),
        package_application: (!packages.is_empty())
            .then_some(crate::database::RetainedPackageApplication::Received { receiver_wall_ms }),
        packages,
        device_operations,
        circle_activations: verified_circle_activations,
    };
    let blob_decls = db.blob_decls();
    let gates = db.gates();
    let synced_tables = db.synced_tables().to_vec();
    let routing_key = routing_key.cloned();
    let activates_circle_epoch_cutoff =
        materialization
            .circle_activations
            .circles()
            .iter()
            .any(|activation| {
                activation
                    .control
                    .value
                    .active_epoch()
                    .is_some_and(|epoch| {
                        matches!(
                            &epoch.common.origin,
                            crate::sync::circle::CircleEpochOrigin::Closed { .. }
                        )
                    })
            });
    let installs_circle_bootstrap = !materialization.circle_activations.bootstraps().is_empty();
    // The local device's own exclusions from the successor outcomes this commit
    // activates — derived from the verified outcome, never unverified storage.
    // A successor commit finalizes exactly one close, so there is at most one.
    let local_exclusions = materialization
        .circle_activations
        .local_exclusions()
        .to_vec();
    #[cfg(any(test, feature = "test-utils"))]
    let materialization_failure = db.merge_materialization_failure_injection();
    let retained_merge_materializations = database.retained_merge_materialization_cache();
    let applied = db
        .call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let materialized_frontier = CommitFrontier::from_refs(
                crate::sync::store::database::StoreDatabase::materialized_frontier_on(&tx, None)?,
            )
            .map_err(|error| DbError::Message(error.to_string()))?;
            let candidate_predecessors = materialization
                .verified_commit
                .value()
                .order
                .predecessor_cut()
                .map_err(|error| DbError::Message(error.to_string()))?
                .frontier();
            let requires_canonical_replay =
                !candidate_predecessors.covers(&materialized_frontier);
            let mut applied = apply_prepared_merge_materialization_on(
                &tx,
                &blob_decls,
                &gates,
                &synced_tables,
                routing_key.as_ref(),
                local_store_membership,
                IncomingTimestampPolicy::Received { receiver_wall_ms },
                None,
                materialization,
            )?;
            if matches!(applied.outcome, ApplyOutcome::Applied(_)) {
                let mut retained_merge_materializations =
                    retained_merge_materializations.lock().map_err(|_| {
                        DbError::Message(
                            "retained Merge materialization cache lock is poisoned".to_string(),
                        )
                    })?;
                let mut transaction_cache = retained_merge_materializations.clone();
                let retained = applied.retained.take().ok_or_else(|| {
                    DbError::Message(
                        "applied Merge materialization omitted its verified retained input"
                            .to_string(),
                    )
                })?;
                transaction_cache.insert_verified(retained)?;
                #[cfg(any(test, feature = "test-utils"))]
                if materialization_failure.reach(
                    crate::database::MergeMaterializationFailurePoint::SummaryMaterialization,
                )? {
                    return Err(DbError::Message(
                        "injected failure after Merge summary materialization".to_string(),
                    ));
                }
                // Detection: record this device's own exclusions from the verified
                // successor outcome. The reseed below rebuilds the Circle projection
                // at the cutoff, dropping beyond-cutoff acceptance the close declared
                // invalid; publication stays gated until its coverage records.
                for exclusion in &local_exclusions {
                    crate::sync::store::database::StoreDatabase::record_circle_close_exclusion_on(
                        &tx, exclusion,
                    )?;
                }
                let retracted = retractions
                    .iter()
                    .map(|retraction| {
                        retraction
                            .candidate_reference()
                            .map_err(|error| DbError::Message(error.to_string()))
                    })
                    .collect::<Result<BTreeSet<_>, _>>()?;
                if !retractions.is_empty() {
                    applied.write_status_notifications =
                        crate::sync::store::database::StoreDatabase::retract_verified_merge_materializations_on(
                            &tx,
                            &mut transaction_cache,
                            retractions,
                        )?;
                    #[cfg(any(test, feature = "test-utils"))]
                    if materialization_failure.reach(
                        crate::database::MergeMaterializationFailurePoint::RetractionDeletion,
                    )? {
                        return Err(DbError::Message(
                            "injected failure after Merge retraction deletion".to_string(),
                        ));
                    }
                }
                if requires_canonical_replay
                    || activates_circle_epoch_cutoff
                    || installs_circle_bootstrap
                    || !retracted.is_empty()
                {
                    let replay = replay_retained_merge_projection_on(
                        &tx,
                        &mut transaction_cache,
                        &blob_decls,
                        &gates,
                        &synced_tables,
                        routing_key.as_ref(),
                        &retracted,
                        None,
                        true,
                        local_store_membership,
                    )?;
                    let projection_changeset = super::retained_replay::replace_live_projection(
                        &tx,
                        &replay,
                        &synced_tables,
                        gates.has_scoped_graph(),
                    )?;
                    #[cfg(any(test, feature = "test-utils"))]
                    if materialization_failure.reach(
                        crate::database::MergeMaterializationFailurePoint::ProjectionReplacement,
                    )? {
                        return Err(DbError::Message(
                            "injected failure after Merge projection replacement".to_string(),
                        ));
                    }
                    crate::sync::store::database::StoreDatabase::replace_store_device_exclusion_freezes_from_replay_on(&tx)?;
                    let old_projection = crate::changeset::walk_old(&projection_changeset)
                        .map_err(DbError::Message)?;
                    let new_projection =
                        crate::changeset::walk(&projection_changeset).map_err(DbError::Message)?;
                    for intent in
                        local_blob_cleanup_intents(&blob_decls, &old_projection, &new_projection)
                            .map_err(|error| DbError::Message(error.to_string()))?
                    {
                        local_cleanup::record_obsolete_copy_intents_on(
                            &tx,
                            &blob_decls,
                            &intent,
                        )?;
                    }
                    if let ApplyOutcome::Applied(rows) = &mut applied.outcome {
                        rows.extend(new_projection);
                    }
                }
                tx.commit().map_err(DbError::from)?;
                *retained_merge_materializations = transaction_cache;
            }
            Ok(applied)
        })
        .await?;
    if let Some(max_applied) = applied.max_updated_at.as_ref() {
        db.hlc().advance_past(max_applied);
    }
    for (write_id, status) in applied.write_status_notifications {
        db.notify_write_status(write_id, status);
    }
    resume_merge_retraction_cleanups(database, storage, root).await?;
    Ok(applied.outcome)
}

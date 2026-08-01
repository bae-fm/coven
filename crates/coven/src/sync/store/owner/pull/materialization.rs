use super::support::advance_max_updated_at;
use super::*;
use crate::blob::local_cleanup::intents_from_changes as local_blob_cleanup_intents;

pub(crate) enum Readiness {
    Ready,
    AlreadyMaterialized,
    Held(HeldStorePosition),
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
    Applied(Vec<crate::database::WinningRow>),
    ConstraintConflict(Vec<String>),
}

impl MergeSubsetOutcome {
    fn extend_winning_rows(
        self,
        winning_rows: &mut Vec<crate::database::WinningRow>,
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
    gates: &crate::database::Gates,
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
    let actual_changes = crate::database::walk_changeset(&bytes).map_err(DbError::Message)?;
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
            .filter(|change| !crate::database::is_routing_table(&change.table))
            .cloned(),
    );
    let store_transaction = MergeMaterializationTransaction::new(conn);
    let apply = store_transaction.apply_changeset(applied_changeset, timestamp_policy)?;
    if !apply.constraint_conflict_tables.is_empty() {
        return Ok(MergeSubsetOutcome::ConstraintConflict(
            apply.constraint_conflict_tables,
        ));
    }
    *package_reported_fk_violation |= apply.had_fk_violations;
    if let Some(package_audience) = package_audience {
        crate::database::align_inbound_scoped_root_audiences(
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
    let winning_rows = store_transaction.current_winning_rows(source.schema(), &bytes)?;
    let old_changes = crate::database::walk_old_changeset(&bytes).map_err(DbError::Message)?;
    let cleanup = local_blob_cleanup_intents(blob_decls, &old_changes, &actual_changes)
        .map_err(|error| DbError::Message(error.to_string()))?;
    for intent in cleanup {
        store_transaction.record_obsolete_blob_cleanup_intent(blob_decls, &intent)?;
    }
    Ok(MergeSubsetOutcome::Applied(winning_rows))
}

#[allow(clippy::too_many_arguments)]
fn apply_merge_package_on(
    conn: &rusqlite::Transaction<'_>,
    blob_decls: &BlobDecls,
    gates: &crate::database::Gates,
    routing_key: Option<&super::circle::RowRoutingKey>,
    package: &AudiencePackage,
    changeset: &ValidatedChangeset<Vec<u8>>,
    store_audience_transitions: &crate::database::StoreAudienceTransitions,
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
            let inbound = crate::database::normalize_inbound_store_changeset(
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
                crate::database::filter_inbound_store_rows(conn, &inbound.rows, gates, routing_key)
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
            let rows = crate::database::filter_inbound_circle_changeset(
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
    gates: &crate::database::Gates,
    synced_tables: &[SyncedTable],
    routing_key: Option<&super::circle::RowRoutingKey>,
    local_store_membership: LocalStoreMembership,
    timestamp_policy: IncomingTimestampPolicy,
    baseline_circle_cuts: Option<
        &BTreeMap<super::circle::CircleId, crate::protocol::store_commit::CommitFrontier>,
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
    crate::database::StoreDatabase::record_activated_store_device_registrations_on(
        conn,
        commit,
        &registrations,
    )?;
    for bootstrap in circle_activations.bootstraps() {
        super::replay::install_circle_bootstrap_remote_objects_on(conn, commit_ref, bootstrap)?;
    }
    let store_transaction = crate::database::MergeMaterializationTransaction::new(conn);
    store_transaction
        .record_verified_circle_activations(&verified_commit, circle_activations.circles())?;
    // A Circle whose winning control chain is now Deleted prunes its rows,
    // routes, and blob bindings like an inactive recipient. Recording the
    // verified activation above already removed its live access cache while
    // retaining the authority spine.
    for activation in circle_activations.circles() {
        if crate::database::StoreDatabase::circle_current_state_is_deleted_on(
            conn,
            activation.circle_id,
        )? {
            inactive_circles.insert(activation.circle_id);
        }
    }
    let retained_packages = packages
        .iter()
        .map(|prepared| prepared.package.clone())
        .collect::<Vec<_>>();
    let store_audience_transitions = packages
        .iter()
        .find(|prepared| matches!(prepared.package.audience(), PackageAudience::Store))
        .map(|prepared| crate::database::store_audience_transitions(prepared.package.changeset()))
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
        let retained =
            crate::database::RetainedAudiencePackage::verify(commit, commit_ref, package.clone())?;
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
        crate::database::StoreDatabase::remove_local_circle_access_on(conn)?;
    }
    let mut removal_session = rusqlite::session::Session::new(conn).map_err(DbError::from)?;
    for table in synced_tables {
        removal_session
            .attach(Some(table.name()))
            .map_err(DbError::from)?;
    }
    crate::database::prune_ineligible_scoped_rows(conn, gates, &inactive_circles)
        .map_err(|error| DbError::Message(error.to_string()))?;
    crate::database::validate_scoped_foreign_key_audiences(conn, gates)
        .map_err(|error| DbError::Message(error.to_string()))?;
    let mut removal_changeset = Vec::new();
    removal_session
        .changeset_strm(&mut removal_changeset)
        .map_err(DbError::from)?;
    drop(removal_session);
    let removed =
        crate::database::walk_old_changeset(&removal_changeset).map_err(DbError::Message)?;
    let removal_changes =
        crate::database::walk_changeset(&removal_changeset).map_err(DbError::Message)?;
    let removal_cleanup = local_blob_cleanup_intents(blob_decls, &removed, &removal_changes)
        .map_err(|error| DbError::Message(error.to_string()))?;
    returned_changes.extend(removal_changes);
    for intent in removal_cleanup {
        store_transaction.record_obsolete_blob_cleanup_intent(blob_decls, &intent)?;
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
    commit_verifier: &StoreCommitVerifier<'_>,
    commit_ref: &StoreBatchCommitRef,
    commit: &StoreBatchCommit,
) -> Result<Option<VerifiedMergeMembershipClosure>, StorePullError> {
    let Some(super::store_commit::StoreControl { transition }) = commit.control() else {
        return Ok(None);
    };
    let entry = commit_verifier
        .membership_objects()
        .load_entry(&transition.body.entry)
        .await
        .map_err(StorePullError::Object)?;
    let coord = &transition.body.entry.coord;
    let loaded_head = commit_verifier
        .membership_objects()
        .load_head_at_slot(
            &transition.head_slot,
            &coord.author_pubkey,
            &coord.author_owner_grant,
            coord.stream_id,
            coord.seq,
        )
        .await
        .map_err(StorePullError::Object)?;
    let head_bytes = loaded_head.bytes;
    let head_object = loaded_head.object;
    let head = loaded_head.value;
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
        let loaded = commit_verifier
            .membership_objects()
            .load_resolution(resolution)
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

impl VerifiedMergeMembershipClosure {
    pub(super) fn objects(&self) -> &crate::database::VerifiedMergeMembershipObjects {
        &self.objects
    }

    pub(super) fn into_remote_objects(self) -> Vec<super::remote_object::RemoteObjectRecord> {
        self.remote_objects
    }
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
        super::remote_object::RemoteObjectRecord::candidate_exclusive_merge_membership_entry(
            family,
            objects.entry().clone(),
            entry_bytes.canonical,
            entry_bytes.stored,
            commit_ref.clone(),
        )?
        .into_observed_activated(commit_ref)?,
        super::remote_object::RemoteObjectRecord::candidate_exclusive_merge_membership_head(
            family,
            objects.head().clone(),
            head_bytes.canonical,
            head_bytes.stored,
            commit_ref.clone(),
        )?
        .into_observed_activated(commit_ref)?,
    ];
    if let Some(resolution) = objects.resolution() {
        let bytes = resolution_bytes
            .ok_or(super::remote_object::RemoteObjectRecordError::StoredReferenceMismatch)?;
        remotes.push(
            super::remote_object::RemoteObjectRecord::candidate_activated_store_membership_resolution(
                resolution.clone(),
                bytes.canonical,
                bytes.stored,
                commit_ref.clone(),
            )?
            .into_observed_activated(commit_ref)?,
        );
    } else if resolution_bytes.is_some() {
        return Err(super::remote_object::RemoteObjectRecordError::StoredReferenceMismatch);
    }
    Ok(remotes)
}

use super::*;
use crate::database::query_mapped_rows;

#[derive(Clone, Default)]
pub(crate) struct RetainedMergeMaterializationCache {
    verified: BTreeMap<(String, u64), OwnedVerifiedMergeMaterialization>,
}

pub(crate) enum RetainedCommitAuthority<'a> {
    StoredBytes,
    Operation(&'a crate::protocol::store_commit::VerifiedStoreBatchCommit),
}

pub(crate) enum RetainedCommitAuthorities<'a> {
    StoredBytes,
    Operation(
        &'a BTreeMap<
            crate::protocol::store_commit::StoreBatchCommitRef,
            crate::protocol::store_commit::VerifiedStoreBatchCommit,
        >,
    ),
}

impl RetainedMergeMaterializationCache {
    pub(crate) fn insert_verified(
        &mut self,
        materialization: OwnedVerifiedMergeMaterialization,
    ) -> Result<(), DbError> {
        let coordinate = materialization.commit_ref().coord.clone();
        let key = (coordinate.stream_id.to_string(), coordinate.sequence());
        match self.verified.entry(key) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(materialization);
            }
            std::collections::btree_map::Entry::Occupied(entry)
                if entry.get().commit_ref() == materialization.commit_ref()
                    && entry.get().input_hash() == materialization.input_hash() => {}
            std::collections::btree_map::Entry::Occupied(_) => {
                return Err(DbError::Message(
                    "retained Merge materialization cache coordinate contains another exact input"
                        .to_string(),
                ))
            }
        }
        Ok(())
    }

    pub(crate) fn verified_by_ref(
        &self,
        reference: &StoreBatchCommitRef,
    ) -> Result<&OwnedVerifiedMergeMaterialization, DbError> {
        let key = (
            reference.coord.stream_id.to_string(),
            reference.coord.sequence(),
        );
        let verified = self.verified.get(&key).ok_or_else(|| {
            DbError::Message(format!(
                "retained Merge materialization cache omits {reference:?}"
            ))
        })?;
        if verified.commit_ref() != reference {
            return Err(DbError::Message(
                "retained Merge materialization cache coordinate contains another commit"
                    .to_string(),
            ));
        }
        Ok(verified)
    }

    pub(crate) fn replay_inputs_on(
        &mut self,
        conn: &Connection,
        root: &crate::protocol::store_commit::StoreRootRef,
    ) -> Result<Vec<OwnedVerifiedMergeMaterialization>, DbError> {
        self.replay_inputs_with_authorities_on(conn, root, RetainedCommitAuthorities::StoredBytes)
    }

    pub(crate) fn replay_inputs_with_verified_commits_on(
        &mut self,
        conn: &Connection,
        root: &crate::protocol::store_commit::StoreRootRef,
        verified: &BTreeMap<
            crate::protocol::store_commit::StoreBatchCommitRef,
            crate::protocol::store_commit::VerifiedStoreBatchCommit,
        >,
    ) -> Result<Vec<OwnedVerifiedMergeMaterialization>, DbError> {
        self.replay_inputs_with_authorities_on(
            conn,
            root,
            RetainedCommitAuthorities::Operation(verified),
        )
    }

    pub(crate) fn replay_inputs_with_authorities_on(
        &mut self,
        conn: &Connection,
        root: &crate::protocol::store_commit::StoreRootRef,
        authorities: RetainedCommitAuthorities<'_>,
    ) -> Result<Vec<OwnedVerifiedMergeMaterialization>, DbError> {
        let rows = query_mapped_rows(
            conn,
            "SELECT device_id, seq, commit_ref, input_hash
                 FROM retained_merge_materializations
                 ORDER BY device_id, seq",
            [],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            },
        )?;

        let mut verified = BTreeMap::new();
        let mut replay_inputs = Vec::with_capacity(rows.len());
        for (stream_id, sequence, encoded_ref, encoded_input_hash) in rows {
            let sequence = Database::sequence_from_sqlite(&stream_id, sequence)?;
            let commit_ref =
                StoreDatabase::parse_stored_commit_ref(&stream_id, sequence, &encoded_ref)?;
            let input_hash = encoded_input_hash.parse::<ObjectHash>().map_err(|error| {
                DbError::context(
                    format!(
                        "retained Merge coordinate {stream_id}/{sequence} input hash is invalid"
                    ),
                    error,
                )
            })?;
            let key = (stream_id.clone(), sequence);
            let materialization = match self.verified.get(&key) {
                Some(cached)
                    if cached.commit_ref() == &commit_ref && cached.input_hash() == input_hash =>
                {
                    cached.clone()
                }
                _ => match &authorities {
                    RetainedCommitAuthorities::StoredBytes => {
                        StoreDatabase::load_retained_merge_materialization_on(
                            conn,
                            root,
                            &stream_id,
                            sequence,
                            &commit_ref,
                            &encoded_input_hash,
                        )?
                    }
                    RetainedCommitAuthorities::Operation(operation_verified) => {
                        let commit = operation_verified.get(&commit_ref).ok_or_else(|| {
                            DbError::Message(format!(
                                "retained Merge commit {commit_ref:?} is absent from the operation-verified history"
                            ))
                        })?;
                        StoreDatabase::load_retained_merge_materialization_with_verified_commit_on(
                            conn,
                            root,
                            &stream_id,
                            sequence,
                            &commit_ref,
                            &encoded_input_hash,
                            commit,
                        )?
                    }
                },
            };
            verified.insert(key, materialization.clone());
            replay_inputs.push(materialization);
        }
        self.verified = verified;
        Ok(replay_inputs)
    }

    pub(crate) fn verified_circle_activation_on(
        &self,
        conn: &Connection,
        circle_id: crate::protocol::circle::CircleId,
        control: &crate::protocol::circle::CircleControlCoord,
    ) -> Result<Option<crate::protocol::circle_activation::VerifiedCircleReference>, DbError> {
        let Some(activation_commit) =
            StoreDatabase::circle_activation_commit_ref_on(conn, circle_id, control)?
        else {
            return Ok(None);
        };
        self.verified_by_ref(&activation_commit)?
            .circle_activation(circle_id, control)
            .map(Some)
    }

    pub(crate) fn circle_replay_epoch_index_on(
        &self,
        conn: &Connection,
    ) -> Result<CircleReplayEpochIndex, DbError> {
        let rows = query_mapped_rows(
            conn,
            "SELECT circle_id, control_coord
                 FROM circle_control_activations
                 ORDER BY circle_id, control_coord",
            [],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )?;
        let mut index = CircleReplayEpochIndex {
            control_epochs: BTreeMap::new(),
            cutoffs: BTreeMap::new(),
        };
        for (encoded_circle_id, encoded_control) in rows {
            let circle_id = encoded_circle_id.parse().map_err(|error| {
                DbError::context(
                    format!("parse Circle replay index id {encoded_circle_id}"),
                    error,
                )
            })?;
            let control = serde_json::from_str(&encoded_control).map_err(|error| {
                DbError::context(
                    format!("parse Circle replay index control for {circle_id}"),
                    error,
                )
            })?;
            let activation = self
                .verified_circle_activation_on(conn, circle_id, &control)?
                .ok_or_else(|| {
                    DbError::Message(format!(
                        "Circle replay index activation for {circle_id} disappeared"
                    ))
                })?;
            index.record_control(circle_id, &activation.control)?;
        }
        Ok(index)
    }

    pub(crate) fn replay_projection_on(
        &mut self,
        live: &rusqlite::Transaction<'_>,
        root: &StoreRootRef,
        blob_decls: &BlobDecls,
        gates: &crate::database::Gates,
        synced_tables: &[SyncedTable],
        routing_key: Option<&crate::protocol::circle::RowRoutingKey>,
        retracted: &BTreeSet<StoreBatchCommitRef>,
        history_cut: Option<&CommitFrontier>,
        include_local_write_overlays: bool,
        local_store_membership: LocalStoreMembership,
    ) -> Result<rusqlite::Connection, DbError> {
        let baseline = StoreDatabase::generation_zero_replay_baseline_on(live)?;
        let replay = baseline.open_image()?;
        replay
            .pragma_update(None, "foreign_keys", "ON")
            .map_err(DbError::from)?;
        let schema = Arc::new(TableSchema::for_apply(&replay, synced_tables, gates)?);
        let circle_bootstraps = StoreDatabase::circle_bootstrap_replay_inputs_on(live)?;
        let mut circle_bootstrap_cuts = BTreeMap::new();
        for (activation_commit, bootstrap) in &circle_bootstraps {
            crate::database::verify_circle_bootstrap_image(
                bootstrap.image_bytes(),
                bootstrap.reference(),
                bootstrap.circle_id(),
                synced_tables,
                routing_key,
            )
            .map_err(|error| {
                DbError::context(
                    format!("verify retained Circle {} bootstrap", bootstrap.circle_id()),
                    error,
                )
            })?;
            let tx = replay.unchecked_transaction().map_err(DbError::from)?;
            crate::database::install_circle_bootstrap_image_on(
                &tx,
                synced_tables,
                activation_commit,
                bootstrap,
            )?;
            tx.commit().map_err(DbError::from)?;
            if circle_bootstrap_cuts
                .insert(
                    bootstrap.circle_id(),
                    bootstrap.reference().coverage.clone(),
                )
                .is_some()
            {
                return Err(DbError::Message(format!(
                    "retained replay has duplicate Circle {} bootstraps",
                    bootstrap.circle_id()
                )));
            }
        }
        let retained = self.replay_inputs_on(live, root)?;
        let circle_epochs = self.circle_replay_epoch_index_on(live)?;
        let active_references = retained
            .iter()
            .filter(|materialization| {
                !retracted.contains(materialization.commit_ref())
                    && history_cut
                        .is_none_or(|cutoff| cutoff.covers_commit(materialization.commit_ref()))
            })
            .map(|materialization| materialization.commit_ref().clone())
            .collect::<BTreeSet<_>>();
        for materialization in retained
            .iter()
            .filter(|materialization| active_references.contains(materialization.commit_ref()))
        {
            let mut dependencies = materialization
                .commit()
                .order
                .dependencies()
                .values()
                .cloned()
                .collect::<BTreeSet<_>>();
            if let Some(predecessor) = materialization.commit().order.predecessor() {
                dependencies.insert(predecessor.clone());
            }
            for dependency in dependencies {
                if retracted.contains(&dependency) {
                    return Err(DbError::Message(format!(
                        "surviving retained Merge commit {:?} depends on retracted commit {:?}",
                        materialization.commit_ref(),
                        dependency
                    )));
                }
                if !active_references.contains(&dependency)
                    && !replay_dependency_is_baseline_covered(&dependency, &baseline.exact_cut)
                {
                    return Err(DbError::Message(format!(
                        "surviving retained Merge commit {:?} has unretained dependency {:?}",
                        materialization.commit_ref(),
                        dependency
                    )));
                }
            }
        }
        let active_accepted_writes = retained
            .iter()
            .filter(|materialization| active_references.contains(materialization.commit_ref()))
            .map(|materialization| materialization.commit().write_id.clone())
            .collect::<BTreeSet<_>>();
        let retracted_writes = retained
            .iter()
            .filter(|materialization| retracted.contains(materialization.commit_ref()))
            .map(|materialization| materialization.commit().write_id.clone())
            .collect::<BTreeSet<_>>();
        let write_overlays = if include_local_write_overlays {
            StoreDatabase::load_merge_replay_write_overlays_on(
                live,
                &active_accepted_writes,
                &retracted_writes,
            )?
        } else {
            Vec::new()
        };
        let mut pending = retained
            .into_iter()
            .filter(|materialization| active_references.contains(materialization.commit_ref()))
            .map(|materialization| (materialization.commit_ref().clone(), materialization))
            .collect::<BTreeMap<_, _>>();
        let mut applied = BTreeSet::new();
        while !pending.is_empty() {
            let ready = pending
                .iter()
                .filter_map(|(reference, materialization)| {
                    let predecessor_ready = materialization
                        .commit()
                        .order
                        .predecessor()
                        .is_none_or(|predecessor| {
                            replay_dependency_is_settled(predecessor, &applied, &baseline.exact_cut)
                        });
                    let dependencies_ready = materialization
                        .commit()
                        .order
                        .dependencies()
                        .values()
                        .all(|dependency| {
                            replay_dependency_is_settled(dependency, &applied, &baseline.exact_cut)
                        });
                    (predecessor_ready && dependencies_ready).then(|| reference.clone())
                })
                .collect::<Vec<_>>();
            if ready.is_empty() {
                return Err(DbError::Message(
                    "retained Merge replay is cyclic or has an unresolved dependency".to_string(),
                ));
            }
            let mut made_progress = false;
            for reference in ready {
                let materialization = pending
                    .get(&reference)
                    .expect("ready retained replay input remains pending")
                    .clone();
                let timestamp_policy = match materialization.package_application() {
                    None => IncomingTimestampPolicy::LocallyAuthored,
                    Some(RetainedPackageApplication::Received { receiver_wall_ms }) => {
                        IncomingTimestampPolicy::Received { receiver_wall_ms }
                    }
                    Some(RetainedPackageApplication::LocallyAuthored) => {
                        IncomingTimestampPolicy::LocallyAuthored
                    }
                };
                let mut retained_packages = Vec::new();
                for package in materialization.packages() {
                    if let crate::protocol::audience_package::PackageAudience::Circle {
                        circle_id,
                        control,
                        ..
                    } = package.audience()
                    {
                        if circle_bootstrap_cuts
                            .get(circle_id)
                            .is_some_and(|cut| cut.covers_commit(materialization.commit_ref()))
                        {
                            continue;
                        }
                        if !circle_epochs.permits(
                            materialization.commit_ref(),
                            *circle_id,
                            control,
                        )? {
                            continue;
                        }
                        if !local_store_membership.retains_circle_rows() {
                            continue;
                        }
                    }
                    retained_packages.push(package.clone());
                }
                let package_application = if retained_packages.is_empty() {
                    None
                } else {
                    Some(materialization.package_application().ok_or_else(|| {
                        DbError::Message(
                            "retained Merge packages lack their application timestamp".to_string(),
                        )
                    })?)
                };
                let packages = retained_packages
                    .into_iter()
                    .map(|package| {
                        let changeset =
                            ValidatedChangeset::new(package.changeset().to_vec(), schema.clone())
                                .map_err(|error| {
                                DbError::context("retained Merge replay changeset", error)
                            })?;
                        Ok(PreparedMergeMaterializationPackage { package, changeset })
                    })
                    .collect::<Result<Vec<_>, DbError>>()?;
                let membership_remote_objects = if let Some(objects) =
                    materialization.membership_objects()
                {
                    let retained_membership_bytes = |object: &ExactObjectRef,
                                                     kind: &str|
                     -> Result<
                        MembershipAuthorityBytes,
                        DbError,
                    > {
                        let object_id = remote_object_id(object);
                        let remote = load_remote_object_on(live, object_id).map_err(|error| {
                            DbError::context(
                                format!(
                                    "load retained Merge membership {kind} {object_id} for replay"
                                ),
                                error,
                            )
                        })?;
                        if remote.object() != object {
                            return Err(DbError::Message(format!(
                                "retained Merge membership {kind} {object_id} has different exact object"
                            )));
                        }
                        let stored = remote
                            .bytes()
                            .stored()
                            .inline_bytes()
                            .ok_or_else(|| {
                                DbError::Message(format!(
                                    "retained Merge membership {kind} {object_id} has no inline stored bytes"
                                ))
                            })?
                            .to_vec();
                        Ok(MembershipAuthorityBytes::new(
                            remote.bytes().canonical_semantic_bytes().to_vec(),
                            stored,
                        ))
                    };
                    let family = materialization.commit().candidate_family();
                    let owner = materialization.commit_ref();
                    let entry_bytes = retained_membership_bytes(&objects.entry().object, "entry")?;
                    let head_bytes = retained_membership_bytes(&objects.head().object, "head")?;
                    let resolution_bytes = objects
                        .resolution()
                        .map(|resolution| {
                            retained_membership_bytes(&resolution.object, "resolution")
                        })
                        .transpose()?;
                    activated_merge_membership_remote_objects(
                        family,
                        objects,
                        entry_bytes,
                        head_bytes,
                        resolution_bytes,
                        owner,
                    )
                    .map_err(|error| DbError::Message(error.to_string()))?
                } else {
                    Vec::new()
                };
                let replay_materialization = PreparedMergeMaterialization {
                    root: materialization.root().clone(),
                    verified_commit: materialization.verified_commit().clone(),
                    activation_head: materialization.activation_head().clone(),
                    activation_head_object: materialization.activation_head_object().clone(),
                    history_summary: materialization.history_summary().clone(),
                    membership_objects: materialization.membership_objects().cloned(),
                    membership_remote_objects,
                    registrations: materialization.registrations().to_vec(),
                    packages,
                    device_operations: materialization.device_operations().clone(),
                    circle_activations: materialization.circle_activations().clone(),
                    package_application,
                };
                let tx = replay.unchecked_transaction().map_err(DbError::from)?;
                let outcome = MergeMaterializationTransaction::new(&tx)
                    .apply_prepared_merge_materialization(
                        blob_decls,
                        gates,
                        synced_tables,
                        routing_key,
                        local_store_membership,
                        timestamp_policy,
                        Some(&circle_bootstrap_cuts),
                        replay_materialization,
                    )
                    .map_err(|error| {
                        DbError::context(
                            format!(
                                "apply retained Merge commit {reference:?} during canonical replay"
                            ),
                            error,
                        )
                    })?;
                match outcome.outcome {
                    ApplyOutcome::Applied(_) => {
                        tx.commit().map_err(DbError::from)?;
                        pending.remove(&reference);
                        applied.insert(reference);
                        made_progress = true;
                    }
                    ApplyOutcome::Held(HeldStorePositionReason::ForeignKeyDependency) => {
                        tx.rollback().map_err(DbError::from)?;
                    }
                    ApplyOutcome::Held(reason) => {
                        tx.rollback().map_err(DbError::from)?;
                        return Err(DbError::Message(format!(
                            "retained Merge replay held accepted commit {reference:?}: {reason:?}"
                        )));
                    }
                }
            }
            if !made_progress {
                return Err(DbError::Message(
                    "retained Merge replay has an unresolved foreign-key dependency".to_string(),
                ));
            }
        }
        for overlay in write_overlays {
            let tx = replay.unchecked_transaction().map_err(DbError::from)?;
            tx.pragma_update(None, "defer_foreign_keys", "ON")
                .map_err(DbError::from)?;
            let partitions = overlay
                .partitions
                .store
                .into_iter()
                .chain(overlay.partitions.circles)
                .chain(overlay.partitions.local);
            for partition in partitions {
                let changeset = ValidatedChangeset::new(partition.changeset, schema.clone())
                    .map_err(|error| {
                        DbError::context(
                            format!("local replay write {} changeset", overlay.write_id),
                            error,
                        )
                    })?;
                let applied = MergeMaterializationTransaction::new(&tx)
                    .apply_changeset(changeset, IncomingTimestampPolicy::LocallyAuthored)?;
                if applied.had_fk_violations || !applied.constraint_conflict_tables.is_empty() {
                    return Err(DbError::Message(format!(
                        "local replay write {} conflicts with accepted history",
                        overlay.write_id
                    )));
                }
            }
            let violations: bool = tx
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM pragma_foreign_key_check)",
                    [],
                    |row| row.get(0),
                )
                .map_err(DbError::from)?;
            if violations {
                return Err(DbError::Message(format!(
                    "local replay write {} violates foreign keys",
                    overlay.write_id
                )));
            }
            tx.commit().map_err(DbError::from)?;
        }
        Ok(replay)
    }
}

fn replay_dependency_is_settled(
    dependency: &StoreBatchCommitRef,
    applied: &BTreeSet<StoreBatchCommitRef>,
    baseline: &CommitFrontier,
) -> bool {
    if applied.contains(dependency) {
        return true;
    }
    replay_dependency_is_baseline_covered(dependency, baseline)
}

fn replay_dependency_is_baseline_covered(
    dependency: &StoreBatchCommitRef,
    baseline: &CommitFrontier,
) -> bool {
    baseline
        .0
        .get(&dependency.coord.stream_id)
        .is_some_and(|covered| {
            covered.coord.sequence() > dependency.coord.sequence
                || (covered.coord.sequence() == dependency.coord.sequence && covered == dependency)
        })
}

pub(crate) struct CircleReplayEpochIndex {
    pub(super) control_epochs: BTreeMap<
        (
            crate::protocol::circle::CircleId,
            crate::protocol::circle::CircleControlCoord,
        ),
        crate::protocol::circle::CircleEpochId,
    >,
    pub(super) cutoffs: BTreeMap<
        (
            crate::protocol::circle::CircleId,
            crate::protocol::circle::CircleEpochId,
        ),
        CommitFrontier,
    >,
}

pub(crate) struct CircleRestoreSelectionIndex {
    pub(crate) circles: Vec<(
        crate::protocol::circle::CircleId,
        Vec<crate::protocol::circle::CircleControlCoord>,
    )>,
    pub(crate) preserved_images: Vec<(
        StoreBatchCommitRef,
        crate::protocol::circle_activation::VerifiedCircleImage,
    )>,
}

impl CircleReplayEpochIndex {
    pub(crate) fn record_control(
        &mut self,
        circle_id: crate::protocol::circle::CircleId,
        control: &crate::protocol::circle::PreparedCircleControl,
    ) -> Result<(), DbError> {
        let control_key = (circle_id, control.coord.clone());
        match self.control_epochs.entry(control_key) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(control.value.epoch_id());
            }
            std::collections::btree_map::Entry::Occupied(entry)
                if *entry.get() == control.value.epoch_id() => {}
            std::collections::btree_map::Entry::Occupied(_) => {
                return Err(DbError::Message(format!(
                    "Circle replay index maps one control for {circle_id} to conflicting epochs"
                )));
            }
        }
        let crate::protocol::circle::CircleEpochOrigin::Closed {
            closed_epoch_id,
            cutoff,
            ..
        } = &control.value.active_common().origin
        else {
            return Ok(());
        };
        match self.cutoffs.entry((circle_id, *closed_epoch_id)) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(cutoff.clone());
            }
            std::collections::btree_map::Entry::Occupied(entry) if entry.get() == cutoff => {}
            std::collections::btree_map::Entry::Occupied(_) => {
                return Err(DbError::Message(format!(
                    "Circle {circle_id} has conflicting cutoffs for epoch {closed_epoch_id}"
                )));
            }
        }
        Ok(())
    }

    pub(crate) fn include_verified_activations(
        &mut self,
        activations: &[crate::protocol::circle_activation::VerifiedCircleReference],
    ) -> Result<(), DbError> {
        for activation in activations {
            self.record_control(activation.circle_id, &activation.control)?;
        }
        Ok(())
    }

    pub(crate) fn permits(
        &self,
        commit_ref: &StoreBatchCommitRef,
        circle_id: crate::protocol::circle::CircleId,
        control: &crate::protocol::circle::CircleControlCoord,
    ) -> Result<bool, DbError> {
        let epoch_id = self
            .control_epochs
            .get(&(circle_id, control.clone()))
            .ok_or_else(|| {
                DbError::Message(format!(
                    "Circle package {} names an unretained control",
                    circle_id
                ))
            })?;
        let Some(cutoff) = self.cutoffs.get(&(circle_id, *epoch_id)) else {
            return Ok(true);
        };
        if cutoff.covers_commit(commit_ref) {
            Ok(true)
        } else if cutoff
            .0
            .get(&commit_ref.coord.stream_id)
            .is_some_and(|accepted| accepted.coord.sequence() == commit_ref.coord.sequence())
        {
            Err(DbError::Message(format!(
                "Circle package {} conflicts with its accepted epoch cutoff",
                circle_id
            )))
        } else {
            Ok(false)
        }
    }
}

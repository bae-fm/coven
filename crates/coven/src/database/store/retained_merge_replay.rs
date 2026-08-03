use crate::blob::locator::{RemoteAudience, StoredBlobRef};
use crate::database::*;
use crate::database::{RetainedReplayAuthority, RetainedReplayBaseline};
use crate::protocol::audience_package::AudiencePackage;
use crate::protocol::membership::{AuthorHead, MembershipEntry};
use crate::protocol::remote_object::{
    remote_object_id, RemoteObjectRecord, RetainedReplayOwner, SharedLiveSetObjectDomain,
};
use crate::protocol::store_commit::{
    CommitFrontier, ObjectHash, RetainedStoreDeviceRegistrationActivations, StoreBatchCommit,
    StoreBatchCommitRef, StoreCommitCoord, StoreDeviceHead, StoreDeviceRegistrationRef,
    StoreRootRef,
};
use crate::storage::{ExactObjectRef, PreparedExactObject};
use crate::sync::{
    activated_merge_membership_remote_objects, ApplyOutcome, HeldStorePositionReason,
    LocalStoreMembership, MembershipAuthorityBytes, PreparedMergeMaterialization,
    PreparedMergeMaterializationPackage, VerifiedCircleActivations,
};
use crate::write::{WriteId, WriteStatus};
use rusqlite::{Connection, OptionalExtension};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use super::candidate_records::PreparedMergeCandidate;
use super::materialization_models::{
    MergeRetractionCleanupInput, RetainedAudiencePackage, RetainedCommitActivationInput,
    RetainedMergeMaterializationInput,
};
use super::store_device_state::load_store_device_snapshot_on;
use super::*;
use crate::database::store::candidate_records::{
    load_author_exclusion_activation_locator_on, parse_prepared_merge_candidate_parts_on,
};

#[derive(Clone, Default)]
pub(crate) struct RetainedMergeMaterializationCache {
    verified: BTreeMap<(String, u64), OwnedVerifiedMergeMaterialization>,
}

enum RetainedCommitAuthority<'a> {
    StoredBytes,
    Operation(&'a crate::protocol::store_commit::VerifiedStoreBatchCommit),
}

enum RetainedCommitAuthorities<'a> {
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

    fn replay_inputs_with_authorities_on(
        &mut self,
        conn: &Connection,
        root: &crate::protocol::store_commit::StoreRootRef,
        authorities: RetainedCommitAuthorities<'_>,
    ) -> Result<Vec<OwnedVerifiedMergeMaterialization>, DbError> {
        let mut statement = conn
            .prepare(
                "SELECT device_id, seq, commit_ref, input_hash
                 FROM retained_merge_materializations
                 ORDER BY device_id, seq",
            )
            .map_err(DbError::from)?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })
            .map_err(DbError::from)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(DbError::from)?;
        drop(statement);

        let mut verified = BTreeMap::new();
        let mut replay_inputs = Vec::with_capacity(rows.len());
        for (stream_id, sequence, encoded_ref, encoded_input_hash) in rows {
            let sequence = Database::sequence_from_sqlite(&stream_id, sequence)?;
            let commit_ref =
                StoreDatabase::parse_stored_commit_ref(&stream_id, sequence, &encoded_ref)?;
            let input_hash = encoded_input_hash.parse::<ObjectHash>().map_err(|error| {
                DbError::Message(format!(
                    "retained Merge coordinate {stream_id}/{sequence} input hash is invalid: {error}"
                ))
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
    ) -> Result<Option<crate::sync::VerifiedCircleReference>, DbError> {
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
        let mut statement = conn
            .prepare(
                "SELECT circle_id, control_coord
                 FROM circle_control_activations
                 ORDER BY circle_id, control_coord",
            )
            .map_err(DbError::from)?;
        let rows = statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(DbError::from)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(DbError::from)?;
        drop(statement);
        let mut index = CircleReplayEpochIndex {
            control_epochs: BTreeMap::new(),
            cutoffs: BTreeMap::new(),
        };
        for (encoded_circle_id, encoded_control) in rows {
            let circle_id = encoded_circle_id.parse().map_err(|error| {
                DbError::Message(format!(
                    "parse Circle replay index id {encoded_circle_id}: {error}"
                ))
            })?;
            let control = serde_json::from_str(&encoded_control).map_err(|error| {
                DbError::Message(format!(
                    "parse Circle replay index control for {circle_id}: {error}"
                ))
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
                DbError::Message(format!(
                    "verify retained Circle {} bootstrap: {error}",
                    bootstrap.circle_id()
                ))
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
                                DbError::Message(format!(
                                    "retained Merge replay changeset: {error}"
                                ))
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
                            DbError::Message(format!(
                                "load retained Merge membership {kind} {object_id} for replay: {error}"
                            ))
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
                        DbError::Message(format!(
                            "apply retained Merge commit {reference:?} during canonical replay: {error}"
                        ))
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
                        DbError::Message(format!(
                            "local replay write {} changeset: {error}",
                            overlay.write_id
                        ))
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
    control_epochs: BTreeMap<
        (
            crate::protocol::circle::CircleId,
            crate::protocol::circle::CircleControlCoord,
        ),
        crate::protocol::circle::CircleEpochId,
    >,
    cutoffs: BTreeMap<
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
    pub(crate) preserved_images: Vec<(StoreBatchCommitRef, crate::sync::VerifiedCircleImage)>,
}

impl CircleReplayEpochIndex {
    fn record_control(
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
        activations: &[crate::sync::VerifiedCircleReference],
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

impl StoreDatabase {
    pub(crate) async fn prepare_circle_restore_selection(
        &self,
        root: crate::protocol::store_commit::StoreRootRef,
    ) -> Result<CircleRestoreSelectionIndex, DbError> {
        self.connection
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                Self::seed_stream_activation_index_from_retained_on(&tx, &root)?;
                let mut statement = tx
                    .prepare(
                        "SELECT circle_id, control_coord FROM circle_control_activations
                         ORDER BY circle_id, control_coord",
                    )
                    .map_err(DbError::from)?;
                let rows = statement
                    .query_map([], |row| {
                        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                    })
                    .map_err(DbError::from)?
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .map_err(DbError::from)?;
                drop(statement);
                let mut circles: Vec<(
                    crate::protocol::circle::CircleId,
                    Vec<crate::protocol::circle::CircleControlCoord>,
                )> = Vec::new();
                for (circle_id, control_coord) in rows {
                    let circle_id: crate::protocol::circle::CircleId =
                        circle_id.parse().map_err(|error| {
                            DbError::Message(format!("parse retained Circle id: {error}"))
                        })?;
                    let control: crate::protocol::circle::CircleControlCoord =
                        serde_json::from_str(&control_coord).map_err(|error| {
                            DbError::Message(format!(
                                "parse retained Circle control coordinate: {error}"
                            ))
                        })?;
                    match circles.last_mut() {
                        Some((last_circle, controls)) if *last_circle == circle_id => {
                            controls.push(control)
                        }
                        _ => circles.push((circle_id, vec![control])),
                    }
                }
                let preserved_images = Self::circle_bootstrap_replay_inputs_on(&tx)?;
                tx.commit().map_err(DbError::from)?;
                Ok(CircleRestoreSelectionIndex {
                    circles,
                    preserved_images,
                })
            })
            .await
    }

    pub(crate) async fn retained_merge_materialization_by_ref(
        &self,
        root: crate::protocol::store_commit::StoreRootRef,
        reference: StoreBatchCommitRef,
    ) -> Result<OwnedVerifiedMergeMaterialization, DbError> {
        self.connection
            .call(move |conn| {
                Self::load_retained_merge_materialization_by_ref_on(conn, &root, &reference)
            })
            .await
    }

    pub(crate) async fn circle_replay_epoch_index(
        &self,
        root: crate::protocol::store_commit::StoreRootRef,
    ) -> Result<CircleReplayEpochIndex, DbError> {
        self.with_retained_merge_materializations(move |conn, cache| {
            cache.replay_inputs_on(conn, &root)?;
            cache.circle_replay_epoch_index_on(conn)
        })
        .await
    }

    pub(crate) fn record_circle_bootstrap_coverage_on(
        conn: &Connection,
        root: &crate::protocol::store_commit::StoreRootRef,
        activation_commit: &StoreBatchCommitRef,
        activations: &VerifiedCircleActivations,
    ) -> Result<(), DbError> {
        for bootstrap in activations.bootstraps() {
            let activation = activations
                .circles()
                .iter()
                .find(|activation| {
                    activation.circle_id == bootstrap.circle_id()
                        && activation.control.coord == *bootstrap.control()
                })
                .ok_or_else(|| {
                    DbError::Message(
                        "verified Circle bootstrap has no activating control".to_string(),
                    )
                })?;
            Self::record_one_circle_bootstrap_coverage_on(
                conn,
                root,
                activation_commit,
                bootstrap,
                &activation.control,
            )?;
        }
        Ok(())
    }

    /// Record (or replace) one Circle's coverage row. The replacement validation
    /// writes the durable image bytes, accepts a strictly newer coverage cut whose
    /// control lineage the retained controls prove, and refuses a regression — the
    /// same rule whether the image is a pull-installed recipient bootstrap or a
    /// restore-installed standalone snapshot. `activation_control` is the verified
    /// control that activates this image: the pull recorder resolves it from the
    /// in-flight activation set; the restore installer resolves it from the
    /// just-installed control indexes.
    pub(crate) fn record_one_circle_bootstrap_coverage_on(
        conn: &Connection,
        root: &crate::protocol::store_commit::StoreRootRef,
        activation_commit: &StoreBatchCommitRef,
        bootstrap: &crate::sync::VerifiedCircleImage,
        activation_control: &crate::protocol::circle::PreparedCircleControl,
    ) -> Result<(), DbError> {
        {
            let circle_id = bootstrap.circle_id().to_string();
            let control_coord = serde_json::to_string(bootstrap.control()).map_err(|error| {
                DbError::Message(format!("serialize Circle bootstrap control: {error}"))
            })?;
            let encoded_commit = serde_json::to_string(activation_commit).map_err(|error| {
                DbError::Message(format!("serialize Circle bootstrap activation: {error}"))
            })?;
            let encoded_cut =
                serde_json::to_string(&bootstrap.reference().coverage).map_err(|error| {
                    DbError::Message(format!("serialize Circle bootstrap coverage: {error}"))
                })?;
            let encoded_ref = serde_json::to_vec(bootstrap.reference()).map_err(|error| {
                DbError::Message(format!("serialize Circle bootstrap reference: {error}"))
            })?;
            let encoded_image_hash = bootstrap.reference().image.image_hash.to_string();
            // The stored image bytes are input to the replay verifier, not trusted
            // for being local: their digest binds the row's image hash at write and
            // again at read, so a corrupted or swapped image fails loud.
            let image_bytes = bootstrap.image_bytes();
            if bootstrap.reference().image.image_hash != ObjectHash::digest(image_bytes) {
                return Err(DbError::Message(
                    "Circle bootstrap image bytes differ from their exact image hash".to_string(),
                ));
            }
            let existing: Option<(String, String, String, String, Vec<u8>)> = conn
                .query_row(
                    "SELECT control_coord, activation_commit, exact_cut, image_hash, bootstrap_ref
                     FROM circle_bootstrap_coverage WHERE circle_id = ?1",
                    [&circle_id],
                    |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                        ))
                    },
                )
                .optional()
                .map_err(DbError::from)?;
            if let Some((prior_control, prior_commit, prior_cut, prior_image_hash, prior_ref)) =
                existing
            {
                let prior_reference: crate::protocol::circle::CircleBootstrapRef =
                    serde_json::from_slice(&prior_ref).map_err(|error| {
                        DbError::Message(format!("parse prior Circle bootstrap reference: {error}"))
                    })?;
                if serde_json::to_vec(&prior_reference).map_err(|error| {
                    DbError::Message(format!(
                        "serialize prior Circle bootstrap reference: {error}"
                    ))
                })? != prior_ref
                    || serde_json::to_string(&prior_reference.coverage).map_err(|error| {
                        DbError::Message(format!(
                            "serialize prior Circle bootstrap coverage: {error}"
                        ))
                    })? != prior_cut
                    || prior_reference.image.image_hash.to_string() != prior_image_hash
                {
                    return Err(DbError::Message(
                        "retained Circle bootstrap row differs from its exact reference"
                            .to_string(),
                    ));
                }
                if (
                    prior_control.as_str(),
                    prior_commit.as_str(),
                    prior_cut.as_str(),
                    prior_image_hash.as_str(),
                    &prior_ref,
                ) == (
                    control_coord.as_str(),
                    encoded_commit.as_str(),
                    encoded_cut.as_str(),
                    encoded_image_hash.as_str(),
                    &encoded_ref,
                ) {
                    // Row already records this exact image — idempotent no-op.
                    return Ok(());
                }
                let prior_control: crate::protocol::circle::CircleControlCoord =
                    serde_json::from_str(&prior_control).map_err(|error| {
                        DbError::Message(format!("parse prior Circle bootstrap control: {error}"))
                    })?;
                let prior_cut: CommitFrontier =
                    serde_json::from_str(&prior_cut).map_err(|error| {
                        DbError::Message(format!("parse prior Circle bootstrap coverage: {error}"))
                    })?;
                let prior_activation = Self::verified_circle_activation_on(
                    conn,
                    root,
                    bootstrap.circle_id(),
                    &prior_control,
                )?
                .ok_or_else(|| {
                    DbError::Message(
                        "prior Circle bootstrap activation is not retained".to_string(),
                    )
                })?;
                if !bootstrap.reference().coverage.covers(&prior_cut)
                    || !Self::verified_circle_control_covers_on(
                        conn,
                        root,
                        bootstrap.circle_id(),
                        activation_control,
                        &prior_activation.control.coord,
                    )?
                {
                    return Err(DbError::Message(format!(
                        "Circle {} bootstrap conflicts with its retained predecessor",
                        bootstrap.circle_id()
                    )));
                }
            }
            conn.execute(
                "INSERT INTO circle_bootstrap_coverage
                 (circle_id, control_coord, activation_commit, exact_cut, image_hash,
                  image_bytes, bootstrap_ref)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(circle_id) DO UPDATE SET
                   control_coord = excluded.control_coord,
                   activation_commit = excluded.activation_commit,
                   exact_cut = excluded.exact_cut,
                   image_hash = excluded.image_hash,
                   image_bytes = excluded.image_bytes,
                   bootstrap_ref = excluded.bootstrap_ref",
                rusqlite::params![
                    circle_id,
                    control_coord,
                    encoded_commit,
                    encoded_cut,
                    encoded_image_hash,
                    image_bytes,
                    encoded_ref,
                ],
            )
            .map_err(DbError::from)?;
        }
        Ok(())
    }

    /// Delete a Circle's preserved coverage row. Restore calls this for every
    /// Circle the restoring identity cannot decrypt: a leftover coverage row would
    /// reconstruct a replay image for a Circle the restorer has no access to,
    /// re-arming the replay for a removed member. Deleting a row that is already
    /// absent is not an error — the identity simply had no coverage to clear.
    pub(crate) fn clear_circle_bootstrap_coverage_on(
        conn: &Connection,
        circle_id: crate::protocol::circle::CircleId,
    ) -> Result<(), DbError> {
        conn.execute(
            "DELETE FROM circle_bootstrap_coverage WHERE circle_id = ?1",
            [circle_id.to_string()],
        )
        .map(|_| ())
        .map_err(DbError::from)
    }

    /// Rebuild the stream-activation index from the retained materializations. A
    /// device restored from a snapshot has the retained authority but not the
    /// per-cycle stream-activation index the pull writes; restore selection must
    /// resolve control-stream authority before any pull, so it seeds the index
    /// from the retained inputs it will otherwise replay from. Idempotent: the
    /// recorder re-verifies each activation against any existing row.
    pub(crate) fn seed_stream_activation_index_from_retained_on(
        conn: &Connection,
        root: &crate::protocol::store_commit::StoreRootRef,
    ) -> Result<(), DbError> {
        let mut statement = conn
            .prepare("SELECT commit_ref FROM retained_merge_materializations ORDER BY commit_ref")
            .map_err(DbError::from)?;
        let encoded_refs = statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(DbError::from)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(DbError::from)?;
        drop(statement);
        for encoded in encoded_refs {
            let reference: StoreBatchCommitRef =
                serde_json::from_str(&encoded).map_err(|error| {
                    DbError::Message(format!(
                        "parse retained materialization commit ref: {error}"
                    ))
                })?;
            let owned =
                Self::load_retained_merge_materialization_by_ref_on(conn, root, &reference)?;
            Self::record_verified_stream_activations_on(
                conn,
                owned.circle_activations().stream_activations(),
                &encoded,
            )?;
        }
        Ok(())
    }

    pub(crate) fn circle_bootstrap_coverage_ref_on(
        conn: &Connection,
        circle_id: crate::protocol::circle::CircleId,
    ) -> Result<Option<crate::protocol::circle::CircleBootstrapCoverageRef>, DbError> {
        let row: Option<(String, String, Vec<u8>)> = conn
            .query_row(
                "SELECT control_coord, activation_commit, bootstrap_ref
                 FROM circle_bootstrap_coverage WHERE circle_id = ?1",
                [circle_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(DbError::from)?;
        let Some((control, activation_commit, bootstrap_ref)) = row else {
            return Ok(None);
        };
        let control: crate::protocol::circle::CircleControlCoord = serde_json::from_str(&control)
            .map_err(|error| {
            DbError::Message(format!("parse Circle coverage control: {error}"))
        })?;
        let activation_commit: StoreBatchCommitRef = serde_json::from_str(&activation_commit)
            .map_err(|error| {
                DbError::Message(format!("parse Circle coverage activation: {error}"))
            })?;
        let bootstrap: crate::protocol::circle::CircleBootstrapRef =
            serde_json::from_slice(&bootstrap_ref).map_err(|error| {
                DbError::Message(format!("parse Circle coverage bootstrap: {error}"))
            })?;
        Ok(Some(crate::protocol::circle::CircleBootstrapCoverageRef {
            circle_id,
            control,
            activation_commit,
            bootstrap,
        }))
    }

    pub(crate) fn circle_bootstrap_replay_inputs_on(
        conn: &Connection,
    ) -> Result<Vec<(StoreBatchCommitRef, crate::sync::VerifiedCircleImage)>, DbError> {
        let mut statement = conn
            .prepare(
                "SELECT circle_id, control_coord, activation_commit, exact_cut,
                        image_hash, image_bytes, bootstrap_ref
                 FROM circle_bootstrap_coverage ORDER BY circle_id",
            )
            .map_err(DbError::from)?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, Vec<u8>>(5)?,
                    row.get::<_, Vec<u8>>(6)?,
                ))
            })
            .map_err(DbError::from)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(DbError::from)?;
        drop(statement);
        let mut bootstraps = Vec::with_capacity(rows.len());
        for (
            circle_id,
            control,
            activation_commit,
            exact_cut,
            image_hash,
            image_bytes,
            encoded_reference,
        ) in rows
        {
            let circle_id: crate::protocol::circle::CircleId =
                circle_id.parse().map_err(|error| {
                    DbError::Message(format!("parse retained Circle bootstrap id: {error}"))
                })?;
            let control = serde_json::from_str(&control).map_err(|error| {
                DbError::Message(format!("parse retained Circle bootstrap control: {error}"))
            })?;
            let activation_commit: StoreBatchCommitRef = serde_json::from_str(&activation_commit)
                .map_err(|error| {
                DbError::Message(format!(
                    "parse retained Circle bootstrap activation: {error}"
                ))
            })?;
            let exact_cut: CommitFrontier = serde_json::from_str(&exact_cut).map_err(|error| {
                DbError::Message(format!("parse retained Circle bootstrap coverage: {error}"))
            })?;
            let reference: crate::protocol::circle::CircleBootstrapRef =
                serde_json::from_slice(&encoded_reference).map_err(|error| {
                    DbError::Message(format!(
                        "parse retained Circle bootstrap reference: {error}"
                    ))
                })?;
            if serde_json::to_vec(&reference).map_err(|error| {
                DbError::Message(format!(
                    "serialize retained Circle bootstrap reference: {error}"
                ))
            })? != encoded_reference
                || reference.coverage != exact_cut
                || reference.image.image_hash.to_string() != image_hash
                || ObjectHash::digest(&image_bytes).to_string() != image_hash
            {
                return Err(DbError::Message(
                    "retained Circle bootstrap row differs from its exact reference".to_string(),
                ));
            }
            // Reconstruct the replay input from the row's own durable bytes — the
            // one representation restore-installed snapshots and pull-installed
            // bootstraps both write. `from_stored_image` re-binds the digest; the
            // replay loop runs the full image verification against the retained
            // control and routing key.
            let bootstrap = crate::sync::VerifiedCircleImage::from_stored_image(
                circle_id,
                control,
                reference,
                image_bytes,
            )
            .map_err(|error| DbError::Message(error.to_string()))?;
            bootstraps.push((activation_commit, bootstrap));
        }
        Ok(bootstraps)
    }

    fn canonical_retained_merge_packages(
        commit: &StoreBatchCommit,
        commit_ref: &StoreBatchCommitRef,
        packages: &[AudiencePackage],
    ) -> Result<Vec<RetainedAudiencePackage>, DbError> {
        let mut by_audience = BTreeMap::new();
        for package in packages {
            let audience = package.audience().remote_audience();
            if by_audience
                .insert(audience.clone(), package.clone())
                .is_some()
            {
                return Err(DbError::Message(format!(
                    "retained Merge commit has duplicate {audience:?} packages"
                )));
            }
        }

        let mut ordered = Vec::new();
        if commit.store_package().is_some() {
            let package = by_audience.remove(&RemoteAudience::Store).ok_or_else(|| {
                DbError::Message("retained Merge commit is missing its Store package".to_string())
            })?;
            ordered.push(RetainedAudiencePackage::verify(
                commit, commit_ref, package,
            )?);
        }
        for reference in commit.circle_packages() {
            let Some(package) = by_audience.remove(&RemoteAudience::Circle(reference.circle_id))
            else {
                continue;
            };
            ordered.push(RetainedAudiencePackage::verify(
                commit, commit_ref, package,
            )?);
        }
        if !by_audience.is_empty() {
            return Err(DbError::Message(
                "retained Merge input carries a package absent from its commit".to_string(),
            ));
        }
        Ok(ordered)
    }

    fn retained_merge_object_ids(
        input: &RetainedMergeMaterializationInput,
    ) -> BTreeSet<ObjectHash> {
        let mut object_ids = BTreeSet::new();
        for retained in &input.packages {
            object_ids.insert(remote_object_id(retained.object()));
            for binding in retained.package().blob_bindings() {
                object_ids.insert(remote_object_id(binding.blob().object()));
            }
        }
        object_ids
    }

    fn validate_retained_package_remote(
        remote: &RemoteObjectRecord,
        retained: &RetainedAudiencePackage,
        owner: &StoreBatchCommitRef,
    ) -> Result<(), DbError> {
        let expected_domain = retained.domain();
        let expected_bytes = retained.package().to_bytes();
        let expected_owner =
            crate::protocol::remote_object::SharedObjectOwner::StoreCommit(owner.clone());
        if !matches!(
            remote,
            RemoteObjectRecord::SharedLiveSet(record)
                if record.identity.domain == expected_domain
                    && record.identity.semantic_hash == ObjectHash::digest(&expected_bytes)
                    && record.identity.object == *retained.object()
                    && record.bytes.canonical_semantic_bytes() == expected_bytes
                    && !matches!(
                        record.bytes.stored(),
                        crate::protocol::remote_object::RemoteStoredRepresentation::Blob { .. }
                    )
                    && matches!(
                        &record.state,
                        crate::protocol::remote_object::OwnedObjectState::UploadedVerified {
                            ownership
                        } if ownership.activated.contains(&expected_owner)
                    )
        ) {
            return Err(DbError::Message(format!(
                "retained package {} differs from its exact activated remote object",
                remote_object_id(retained.object())
            )));
        }
        Ok(())
    }

    fn validate_retained_blob_remote(
        remote: &RemoteObjectRecord,
        stored: &StoredBlobRef,
        owner: &StoreBatchCommitRef,
    ) -> Result<(), DbError> {
        let locator_bytes = stored.locator().to_bytes();
        let expected_owner =
            crate::protocol::remote_object::SharedObjectOwner::StoreCommit(owner.clone());
        if !matches!(
            remote,
            RemoteObjectRecord::SharedLiveSet(record)
                if record.identity.domain == SharedLiveSetObjectDomain::StoredBlob
                    && record.identity.semantic_hash == ObjectHash::digest(&locator_bytes)
                    && record.identity.object == *stored.object()
                    && record.bytes.canonical_semantic_bytes() == locator_bytes
                    && matches!(
                        record.bytes.stored(),
                        crate::protocol::remote_object::RemoteStoredRepresentation::Blob { object }
                            if object == stored.object()
                    )
                    && matches!(
                        &record.state,
                        crate::protocol::remote_object::OwnedObjectState::UploadedVerified {
                            ownership
                        } if ownership.activated.contains(&expected_owner)
                    )
        ) {
            return Err(DbError::Message(format!(
                "retained blob {} differs from its exact activated remote object",
                remote_object_id(stored.object())
            )));
        }
        Ok(())
    }

    fn pin_retained_merge_objects_on(
        conn: &rusqlite::Transaction<'_>,
        input: &RetainedMergeMaterializationInput,
        owner: &RetainedReplayOwner,
    ) -> Result<(), DbError> {
        let commit = owner.commit();
        let mut pinned = BTreeSet::new();
        for retained in &input.packages {
            let object_id = remote_object_id(retained.object());
            let mut remote = load_remote_object_on(conn, object_id)?;
            Self::validate_retained_package_remote(&remote, retained, commit)?;
            remote
                .merge_retained_replay_owner(owner.clone())
                .map_err(|error| {
                    DbError::Message(format!(
                        "pin retained package {object_id} for replay: {error}"
                    ))
                })?;
            update_remote_object_on(conn, object_id, &remote)?;
            index_retained_replay_owner_on(conn, object_id, owner)?;
            pinned.insert(object_id);
            for binding in retained.package().blob_bindings() {
                let stored = binding.blob();
                let object_id = remote_object_id(stored.object());
                if !pinned.insert(object_id) {
                    continue;
                }
                let mut remote = load_remote_object_on(conn, object_id)?;
                Self::validate_retained_blob_remote(&remote, stored, commit)?;
                remote
                    .merge_retained_replay_owner(owner.clone())
                    .map_err(|error| {
                        DbError::Message(format!(
                            "pin retained blob {object_id} for replay: {error}"
                        ))
                    })?;
                update_remote_object_on(conn, object_id, &remote)?;
                index_retained_replay_owner_on(conn, object_id, owner)?;
            }
        }
        Ok(())
    }

    fn validate_retained_merge_pin_closure_on(
        conn: &Connection,
        input: &RetainedMergeMaterializationInput,
        owner: &RetainedReplayOwner,
    ) -> Result<(), DbError> {
        let commit = owner.commit();
        for retained in &input.packages {
            let remote = load_remote_object_on(conn, remote_object_id(retained.object()))?;
            Self::validate_retained_package_remote(&remote, retained, commit)?;
            if !remote
                .retained_replay_owners()
                .any(|actual| actual == owner)
            {
                return Err(DbError::Message(format!(
                    "retained package {} is missing its exact replay owner",
                    remote_object_id(retained.object())
                )));
            }
            for binding in retained.package().blob_bindings() {
                let stored = binding.blob();
                let remote = load_remote_object_on(conn, remote_object_id(stored.object()))?;
                Self::validate_retained_blob_remote(&remote, stored, commit)?;
                if !remote
                    .retained_replay_owners()
                    .any(|actual| actual == owner)
                {
                    return Err(DbError::Message(format!(
                        "retained blob {} is missing its exact replay owner",
                        remote_object_id(stored.object())
                    )));
                }
            }
        }
        let expected = Self::retained_merge_object_ids(input);
        let RetainedReplayOwner::Commit { commit, input_hash } = owner;
        let StoreCommitCoord {
            stream_id,
            sequence,
        } = &commit.coord;
        let stream_id = stream_id.to_string();
        let sequence = Database::sequence_to_sqlite(&stream_id, *sequence)?;
        let commit_ref = serde_json::to_string(commit).map_err(|error| {
            DbError::Message(format!("serialize retained replay commit ref: {error}"))
        })?;
        let input_hash = input_hash.to_string();
        let mut rows = conn
            .prepare(
                "SELECT object_id FROM retained_replay_objects
                 WHERE device_id = ?1 AND seq = ?2 AND commit_ref = ?3 AND input_hash = ?4
                 ORDER BY object_id",
            )
            .map_err(DbError::from)?;
        let actual = rows
            .query_map(
                rusqlite::params![stream_id, sequence, commit_ref, input_hash],
                |row| row.get::<_, String>(0),
            )
            .map_err(DbError::from)?
            .map(|row| {
                let object_id = row.map_err(DbError::from)?;
                object_id.parse().map_err(|error| {
                    DbError::Message(format!("retained replay object id {object_id}: {error}"))
                })
            })
            .collect::<Result<BTreeSet<_>, DbError>>()?;
        if actual != expected {
            return Err(DbError::Message(
                "retained Merge replay ownership differs from its exact object closure".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn remove_retained_replay_ownership_from_snapshot_on(
        conn: &rusqlite::Transaction<'_>,
    ) -> Result<(), DbError> {
        let mut statement = conn
            .prepare("SELECT DISTINCT object_id FROM retained_replay_objects ORDER BY object_id")
            .map_err(DbError::from)?;
        let object_ids = statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(DbError::from)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(DbError::from)?;
        drop(statement);
        for encoded in object_ids {
            let object_id = encoded.parse().map_err(|error| {
                DbError::Message(format!("snapshot retained replay object id: {error}"))
            })?;
            let mut remote = load_remote_object_on(conn, object_id)?;
            remote
                .remove_all_retained_replay_owners()
                .map_err(|error| {
                    DbError::Message(format!(
                        "remove snapshot retained replay owner from {object_id}: {error}"
                    ))
                })?;
            update_remote_object_on(conn, object_id, &remote)?;
        }
        conn.execute("DELETE FROM retained_replay_objects", [])
            .map_err(DbError::from)?;
        Ok(())
    }

    /// The `retained_merge_materializations` commit-refs a Store snapshot image
    /// keeps: author-exclusion activation commits (device-exclusion recovery),
    /// Circle bootstrap-coverage activation commits, and every retained
    /// materialization that still carries a Circle package no bootstrap cut
    /// covers. `retain_snapshot_replay_inputs_on` keeps exactly this set, and
    /// `validate_snapshot_retained_inputs_on` expects exactly it, so the two
    /// share this one derivation.
    pub(crate) fn snapshot_required_retained_refs(
        conn: &Connection,
        root: &crate::protocol::store_commit::StoreRootRef,
    ) -> Result<BTreeSet<String>, DbError> {
        let mut statement = conn
            .prepare(
                "SELECT DISTINCT activation_commit
                 FROM store_author_exclusion_activations
                 ORDER BY activation_commit",
            )
            .map_err(DbError::from)?;
        let references = statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(DbError::from)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(DbError::from)?;
        drop(statement);
        let mut required = references.into_iter().collect::<BTreeSet<_>>();
        let mut bootstrap_statement = conn
            .prepare(
                "SELECT circle_id, activation_commit, exact_cut
                 FROM circle_bootstrap_coverage ORDER BY circle_id",
            )
            .map_err(DbError::from)?;
        let bootstrap_rows = bootstrap_statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .map_err(DbError::from)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(DbError::from)?;
        drop(bootstrap_statement);
        let mut bootstrap_cuts = BTreeMap::new();
        for (circle_id, activation_commit, exact_cut) in bootstrap_rows {
            let circle_id: crate::protocol::circle::CircleId =
                circle_id.parse().map_err(|error| {
                    DbError::Message(format!("snapshot Circle bootstrap id: {error}"))
                })?;
            let cut: CommitFrontier = serde_json::from_str(&exact_cut).map_err(|error| {
                DbError::Message(format!("snapshot Circle bootstrap coverage: {error}"))
            })?;
            if bootstrap_cuts.insert(circle_id, cut).is_some() {
                return Err(DbError::Message(
                    "snapshot has duplicate Circle bootstrap coverage".to_string(),
                ));
            }
            required.insert(activation_commit);
        }
        let mut materialization_statement = conn
            .prepare("SELECT commit_ref FROM retained_merge_materializations ORDER BY commit_ref")
            .map_err(DbError::from)?;
        let materialization_refs = materialization_statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(DbError::from)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(DbError::from)?;
        drop(materialization_statement);
        for encoded in materialization_refs {
            let reference: StoreBatchCommitRef =
                serde_json::from_str(&encoded).map_err(|error| {
                    DbError::Message(format!("snapshot retained Circle commit: {error}"))
                })?;
            let materialization =
                Self::load_retained_merge_materialization_by_ref_on(conn, root, &reference)?;
            let has_uncovered_circle_package = materialization.packages().iter().any(|package| {
                let crate::protocol::audience_package::PackageAudience::Circle {
                    circle_id, ..
                } = package.audience()
                else {
                    return false;
                };
                bootstrap_cuts
                    .get(circle_id)
                    .is_none_or(|cut| !cut.covers_commit(&reference))
            });
            if has_uncovered_circle_package {
                required.insert(encoded);
            }
        }
        Ok(required)
    }

    pub(crate) fn retain_snapshot_replay_inputs_on(
        conn: &rusqlite::Transaction<'_>,
        root: &crate::protocol::store_commit::StoreRootRef,
    ) -> Result<(), DbError> {
        let required = Self::snapshot_required_retained_refs(conn, root)?;
        let mut retained = Vec::with_capacity(required.len());
        for encoded in required {
            let reference: StoreBatchCommitRef =
                serde_json::from_str(&encoded).map_err(|error| {
                    DbError::Message(format!(
                        "snapshot author exclusion activation commit: {error}"
                    ))
                })?;
            Self::load_retained_merge_materialization_by_ref_on(conn, root, &reference)?;
            let StoreCommitCoord {
                stream_id,
                sequence,
            } = &reference.coord;
            let stream_id = stream_id.to_string();
            let sequence_sql = Database::sequence_to_sqlite(&stream_id, *sequence)?;
            let (stored_ref, input_hash, canonical_input): (String, String, Vec<u8>) = conn
                .query_row(
                    "SELECT commit_ref, input_hash, canonical_input
                     FROM retained_merge_materializations
                     WHERE device_id = ?1 AND seq = ?2",
                    rusqlite::params![&stream_id, sequence_sql],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .map_err(DbError::from)?;
            if stored_ref != encoded
                || input_hash != ObjectHash::digest(&canonical_input).to_string()
            {
                return Err(DbError::Message(
                    "snapshot retained replay activation differs from its retained input"
                        .to_string(),
                ));
            }
            let input: RetainedMergeMaterializationInput = serde_json::from_slice(&canonical_input)
                .map_err(|error| {
                    DbError::Message(format!("snapshot retained replay input: {error}"))
                })?;
            retained.push((reference, input_hash, canonical_input, input));
        }
        Self::remove_retained_replay_ownership_from_snapshot_on(conn)?;
        conn.execute("DELETE FROM retained_merge_materializations", [])
            .map_err(DbError::from)?;
        for (reference, input_hash, canonical_input, input) in retained {
            let StoreCommitCoord {
                stream_id,
                sequence,
            } = &reference.coord;
            let stream_id = stream_id.to_string();
            let sequence = Database::sequence_to_sqlite(&stream_id, *sequence)?;
            let encoded_ref = serde_json::to_string(&reference).map_err(|error| {
                DbError::Message(format!(
                    "serialize snapshot author exclusion activation: {error}"
                ))
            })?;
            conn.execute(
                "INSERT INTO retained_merge_materializations
                 (device_id, seq, commit_ref, input_hash, canonical_input)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![
                    &stream_id,
                    sequence,
                    &encoded_ref,
                    &input_hash,
                    &canonical_input
                ],
            )
            .map_err(DbError::from)?;
            let input_hash = input_hash.parse().map_err(|error| {
                DbError::Message(format!(
                    "snapshot author exclusion input hash {input_hash}: {error}"
                ))
            })?;
            let owner = RetainedReplayOwner::Commit {
                commit: reference,
                input_hash,
            };
            Self::pin_retained_merge_objects_on(conn, &input, &owner)?;
            Self::validate_retained_merge_pin_closure_on(conn, &input, &owner)?;
        }
        Ok(())
    }

    pub(crate) fn retain_snapshot_device_states_on(
        conn: &rusqlite::Transaction<'_>,
        root: &crate::protocol::store_commit::StoreRootRef,
        coverage: BTreeMap<String, StoreBatchCommitRef>,
    ) -> Result<(), DbError> {
        let mut required = coverage.into_values().collect::<BTreeSet<_>>();
        let mut statement = conn
            .prepare("SELECT commit_ref FROM retained_merge_materializations ORDER BY commit_ref")
            .map_err(DbError::from)?;
        let retained = statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(DbError::from)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(DbError::from)?;
        drop(statement);
        for encoded in retained {
            let reference: StoreBatchCommitRef =
                serde_json::from_str(&encoded).map_err(|error| {
                    DbError::Message(format!("snapshot retained device-state authority: {error}"))
                })?;
            let materialization =
                Self::load_retained_merge_materialization_by_ref_on(conn, root, &reference)?;
            required.insert(reference);
            required.extend(materialization.commit().order.predecessor.iter().cloned());
            required.extend(
                materialization
                    .commit()
                    .order
                    .dependencies
                    .values()
                    .cloned(),
            );
        }
        conn.execute_batch(
            "CREATE TEMP TABLE snapshot_required_device_states (
                 commit_ref TEXT PRIMARY KEY
             ) STRICT;",
        )
        .map_err(DbError::from)?;
        for reference in &required {
            let encoded = serde_json::to_string(reference).map_err(|error| {
                DbError::Message(format!(
                    "serialize snapshot device-state reference: {error}"
                ))
            })?;
            let present = conn
                .query_row(
                    "SELECT EXISTS(
                         SELECT 1 FROM store_device_state_snapshots WHERE commit_ref = ?1
                     )",
                    [&encoded],
                    |row| row.get::<_, bool>(0),
                )
                .map_err(DbError::from)?;
            if !present {
                return Err(DbError::Message(
                    "snapshot device-state closure is incomplete".to_string(),
                ));
            }
            conn.execute(
                "INSERT INTO snapshot_required_device_states (commit_ref) VALUES (?1)",
                [&encoded],
            )
            .map_err(DbError::from)?;
        }
        conn.execute(
            "DELETE FROM store_device_state_snapshots
             WHERE NOT EXISTS (
                 SELECT 1 FROM snapshot_required_device_states required
                 WHERE required.commit_ref = store_device_state_snapshots.commit_ref
             )",
            [],
        )
        .map_err(DbError::from)?;
        let actual = conn
            .prepare("SELECT commit_ref FROM store_device_state_snapshots ORDER BY commit_ref")
            .map_err(DbError::from)?
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(DbError::from)?
            .collect::<rusqlite::Result<BTreeSet<_>>>()
            .map_err(DbError::from)?;
        let expected = required
            .iter()
            .map(|reference| {
                serde_json::to_string(reference).map_err(|error| {
                    DbError::Message(format!("serialize expected snapshot device state: {error}"))
                })
            })
            .collect::<Result<BTreeSet<_>, _>>()?;
        if actual != expected {
            return Err(DbError::Message(
                "snapshot device-state closure differs from its exact authority".to_string(),
            ));
        }
        conn.execute_batch("DROP TABLE snapshot_required_device_states")
            .map_err(DbError::from)?;
        Ok(())
    }

    pub(crate) fn validate_snapshot_retained_inputs_on(
        conn: &Connection,
        root: &crate::protocol::store_commit::StoreRootRef,
    ) -> Result<(), DbError> {
        // Each recorded author-exclusion activation must still match its
        // exclusion locator, so the image's exclusion table is internally exact.
        let mut statement = conn
            .prepare(
                "SELECT exclusion_ref, activation_commit
                 FROM store_author_exclusion_activations
                 ORDER BY exclusion_ref",
            )
            .map_err(DbError::from)?;
        let stored = statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(DbError::from)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(DbError::from)?;
        drop(statement);
        for (encoded_exclusion, activation_commit) in stored {
            let exclusion = serde_json::from_str(&encoded_exclusion).map_err(|error| {
                DbError::Message(format!("snapshot author exclusion reference: {error}"))
            })?;
            let locator = load_author_exclusion_activation_locator_on(conn, root, &exclusion)?;
            let encoded_locator =
                serde_json::to_string(locator.activation_commit()).map_err(|error| {
                    DbError::Message(format!(
                        "serialize snapshot author exclusion activation: {error}"
                    ))
                })?;
            if encoded_locator != activation_commit {
                return Err(DbError::Message(
                    "snapshot author exclusion activation changed during verification".to_string(),
                ));
            }
        }
        // The image's retained inputs must be exactly the set the retention rule
        // keeps — an extra row is unjustified replay baseline, a missing one is
        // coverage the Circle retained replay needs.
        let expected = Self::snapshot_required_retained_refs(conn, root)?;
        let mut statement = conn
            .prepare("SELECT commit_ref FROM retained_merge_materializations ORDER BY commit_ref")
            .map_err(DbError::from)?;
        let actual = statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(DbError::from)?
            .collect::<rusqlite::Result<BTreeSet<_>>>()
            .map_err(DbError::from)?;
        if actual != expected {
            return Err(DbError::Message(
                "snapshot retained inputs differ from the retention rule".to_string(),
            ));
        }
        Ok(())
    }

    fn open_retained_merge_materialization_input_with_authority_on(
        conn: &Connection,
        root: &crate::protocol::store_commit::StoreRootRef,
        commit_ref: &StoreBatchCommitRef,
        input: &RetainedMergeMaterializationInput,
        input_hash: ObjectHash,
        authority: RetainedCommitAuthority<'_>,
    ) -> Result<OwnedVerifiedMergeMaterialization, DbError> {
        let sequence = &commit_ref.coord.sequence;
        if sequence == &0 {
            return Err(DbError::Message(
                "retained Merge input names sequence zero".to_string(),
            ));
        }
        let unverified: StoreBatchCommit = serde_json::from_slice(input.commit.stored_bytes())
            .map_err(|error| DbError::Message(format!("retained Merge commit: {error}")))?;
        let registrations = input
            .activation
            .registrations
            .verify_for(root, &unverified)
            .map_err(|error| DbError::Message(error.to_string()))?;
        let introduced_registration = |reference: &StoreDeviceRegistrationRef| {
            let mut matches = unverified
                .device_registrations()
                .iter()
                .zip(&registrations)
                .filter(|(activated, _)| &activated.registration == reference)
                .map(|(_, registration)| registration.value());
            let registration = matches.next();
            if matches.next().is_some() {
                return Err(DbError::Message(
                    "retained Merge input introduces one registration more than once".to_string(),
                ));
            }
            Ok(registration)
        };
        let introduced_author = introduced_registration(&unverified.author_registration)?;
        let stored_author;
        let author = match introduced_author {
            Some(author) => author,
            None => {
                stored_author =
                    load_activated_registration_on(conn, root, &unverified.author_registration)?;
                &stored_author
            }
        };
        let verified_commit = match authority {
            RetainedCommitAuthority::StoredBytes => {
                crate::protocol::store_commit::VerifiedStoreBatchCommit::parse(
                    input.commit.stored_bytes(),
                    root.store_root_hash,
                    commit_ref,
                    author,
                )
                .map_err(|error| DbError::Message(format!("retained Merge commit: {error}")))?
            }
            RetainedCommitAuthority::Operation(verified)
                if verified.reference() == commit_ref
                    && verified.author() == author
                    && verified.value().to_bytes() == input.commit.stored_bytes() =>
            {
                verified.clone()
            }
            RetainedCommitAuthority::Operation(_) => {
                return Err(DbError::Message(
                    "retained Merge commit differs from its operation-verified exact commit"
                        .to_string(),
                ))
            }
        };
        let commit = verified_commit.value().clone();
        if commit.to_bytes() != input.commit.stored_bytes() {
            return Err(DbError::Message(
                "retained Merge commit bytes are not canonical".to_string(),
            ));
        }
        let exact_ref = StoreBatchCommitRef::from_commit(
            &commit,
            commit_ref.coord.clone(),
            input.commit.reference().clone(),
        )
        .map_err(|error| DbError::Message(error.to_string()))?;
        if &exact_ref != commit_ref {
            return Err(DbError::Message(
                "retained Merge commit differs from its materialized coordinate".to_string(),
            ));
        }
        let head = StoreDeviceHead::parse_at(
            input.activation_head.stored_bytes(),
            root.store_root_hash,
            author,
            commit_ref,
        )
        .map_err(|error| DbError::Message(format!("retained Merge activation head: {error}")))?;
        if head.to_bytes() != input.activation_head.stored_bytes() {
            return Err(DbError::Message(
                "retained Merge activation head bytes are not canonical".to_string(),
            ));
        }
        let package_values = input
            .packages
            .iter()
            .map(RetainedAudiencePackage::package)
            .cloned()
            .collect::<Vec<_>>();
        let packages =
            Self::canonical_retained_merge_packages(&commit, commit_ref, &package_values)?;
        if packages != input.packages {
            return Err(DbError::Message(
                "retained Merge packages are not in commit order".to_string(),
            ));
        }
        if packages.is_empty() != input.activation.package_application.is_none() {
            return Err(DbError::Message(
                "retained Merge package application does not match its applied packages"
                    .to_string(),
            ));
        }
        let device_operations = input
            .activation
            .device_operations
            .verify_for(root, &commit)
            .map_err(|error| DbError::Message(error.to_string()))?;
        let local_identity = match local_activated_registration_ref_on(conn)? {
            Some(reference) => Some(match introduced_registration(&reference)? {
                Some(registration) => registration.author_pubkey.clone(),
                None => load_activated_registration_on(conn, root, &reference)?.author_pubkey,
            }),
            None => None,
        };
        let circle_activations = VerifiedCircleActivations::parse_retained_for_verified_commit(
            &input.activation.circle_activations,
            &verified_commit,
            local_identity.as_deref(),
        )
        .map_err(|error| DbError::Message(error.to_string()))?;
        if commit.control().is_some() != input.membership_objects.is_some() {
            return Err(DbError::Message(
                "retained Merge membership closure differs from its exact Store control"
                    .to_string(),
            ));
        }
        if let Some(objects) = &input.membership_objects {
            let entry_remote =
                load_remote_object_on(conn, remote_object_id(&objects.entry().object))?;
            let entry: MembershipEntry = serde_json::from_slice(
                entry_remote.bytes().canonical_semantic_bytes(),
            )
            .map_err(|error| DbError::Message(format!("retained membership entry: {error}")))?;
            let head_remote =
                load_remote_object_on(conn, remote_object_id(&objects.head().object))?;
            let head_value: AuthorHead = serde_json::from_slice(
                head_remote.bytes().canonical_semantic_bytes(),
            )
            .map_err(|error| DbError::Message(format!("retained membership head: {error}")))?;
            let verified_objects = VerifiedMergeMembershipObjects::verify(
                &commit,
                commit_ref,
                &entry,
                &head_value,
                objects.head().clone(),
            )?;
            if &verified_objects != objects || entry_remote.object() != &objects.entry().object {
                return Err(DbError::Message(
                    "retained membership objects differ from their exact authority".to_string(),
                ));
            }
            if let Some(reference) = objects.resolution() {
                let remote = load_remote_object_on(conn, remote_object_id(&reference.object))?;
                let resolution: crate::protocol::membership::StoreMembershipConflictResolution =
                    serde_json::from_slice(remote.bytes().canonical_semantic_bytes()).map_err(
                        |error| {
                            DbError::Message(format!("retained membership resolution: {error}"))
                        },
                    )?;
                if !resolution.verify_signature()
                    || resolution.resolution_ref(reference.object.clone()) != *reference
                {
                    return Err(DbError::Message(
                        "retained membership resolution differs from its exact authority"
                            .to_string(),
                    ));
                }
            }
        }
        OwnedVerifiedMergeMaterialization::verify(
            root.clone(),
            verified_commit,
            registrations,
            device_operations,
            circle_activations,
            head,
            input.activation_head.reference().clone(),
            input.history_summary.clone(),
            input.membership_objects.clone(),
            package_values,
            input.activation.package_application,
            input_hash,
        )
    }

    pub(crate) fn retain_merge_materialization_on(
        conn: &rusqlite::Transaction<'_>,
        root: &crate::protocol::store_commit::StoreRootRef,
        materialization: &VerifiedMergeMaterialization<'_>,
    ) -> Result<
        (
            RetainedMergeMaterializationKey,
            OwnedVerifiedMergeMaterialization,
        ),
        DbError,
    > {
        Self::retain_merge_materialization_with_authority_on(
            conn,
            root,
            materialization,
            RetainedCommitAuthority::Operation(materialization.verified_commit()),
        )
    }

    fn retain_merge_materialization_with_authority_on(
        conn: &rusqlite::Transaction<'_>,
        root: &crate::protocol::store_commit::StoreRootRef,
        materialization: &VerifiedMergeMaterialization<'_>,
        authority: RetainedCommitAuthority<'_>,
    ) -> Result<
        (
            RetainedMergeMaterializationKey,
            OwnedVerifiedMergeMaterialization,
        ),
        DbError,
    > {
        let packages = Self::canonical_retained_merge_packages(
            materialization.commit(),
            materialization.commit_ref(),
            materialization.packages(),
        )?;
        let input = RetainedMergeMaterializationInput {
            commit: PreparedExactObject::new(
                materialization.commit_ref().object.clone(),
                materialization.commit().to_bytes(),
            )
            .map_err(|error| DbError::Message(error.to_string()))?,
            activation_head: PreparedExactObject::new(
                materialization.activation_head_object().clone(),
                materialization.activation_head().to_bytes(),
            )
            .map_err(|error| DbError::Message(error.to_string()))?,
            history_summary: materialization.history_summary().clone(),
            membership_objects: materialization.membership_objects().cloned(),
            packages,
            activation: RetainedCommitActivationInput {
                registrations: RetainedStoreDeviceRegistrationActivations::from_verified(
                    root,
                    materialization.commit(),
                    materialization.registrations(),
                )
                .map_err(|error| DbError::Message(error.to_string()))?,
                device_operations: materialization.device_operations().to_retained(),
                circle_activations: materialization
                    .circle_activations()
                    .to_retained()
                    .map_err(|error| DbError::Message(error.to_string()))?,
                package_application: materialization.package_application(),
            },
        };
        let canonical_input = serde_json::to_vec(&input).map_err(|error| {
            DbError::Message(format!("serialize retained Merge materialization: {error}"))
        })?;
        let input_hash = ObjectHash::digest(&canonical_input);
        let verified = Self::open_retained_merge_materialization_input_with_authority_on(
            conn,
            root,
            materialization.commit_ref(),
            &input,
            input_hash,
            authority,
        )?;
        let StoreCommitCoord {
            stream_id,
            sequence,
        } = &materialization.commit_ref().coord;
        let stream_id = stream_id.to_string();
        let sequence = Database::sequence_to_sqlite(&stream_id, *sequence)?;
        let commit_ref_json =
            serde_json::to_string(materialization.commit_ref()).map_err(|error| {
                DbError::Message(format!("serialize retained Merge commit ref: {error}"))
            })?;
        let inserted = conn
            .execute(
                "INSERT INTO retained_merge_materializations
                 (device_id, seq, commit_ref, input_hash, canonical_input)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(device_id, seq) DO NOTHING",
                rusqlite::params![
                    &stream_id,
                    sequence,
                    &commit_ref_json,
                    input_hash.to_string(),
                    &canonical_input
                ],
            )
            .map_err(DbError::from)?;
        if inserted == 0 {
            let stored: (String, String, Vec<u8>) = conn
                .query_row(
                    "SELECT commit_ref, input_hash, canonical_input
                     FROM retained_merge_materializations
                     WHERE device_id = ?1 AND seq = ?2",
                    rusqlite::params![&stream_id, sequence],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .map_err(DbError::from)?;
            if stored
                != (
                    commit_ref_json.clone(),
                    input_hash.to_string(),
                    canonical_input,
                )
            {
                return Err(DbError::Message(format!(
                    "retained Merge coordinate {stream_id}/{} already contains different exact input",
                    materialization.commit_ref().coord.sequence()
                )));
            }
        }
        let replay_owner = RetainedReplayOwner::Commit {
            commit: materialization.commit_ref().clone(),
            input_hash,
        };
        Self::pin_retained_merge_objects_on(conn, &input, &replay_owner)?;
        Self::validate_retained_merge_pin_closure_on(conn, &input, &replay_owner)?;
        Ok((
            RetainedMergeMaterializationKey {
                commit_ref: commit_ref_json,
                input_hash,
            },
            verified,
        ))
    }

    pub(crate) fn load_retained_merge_materialization_on(
        conn: &Connection,
        root: &crate::protocol::store_commit::StoreRootRef,
        stream_id: &str,
        sequence: u64,
        commit_ref: &StoreBatchCommitRef,
        expected_input_hash: &str,
    ) -> Result<OwnedVerifiedMergeMaterialization, DbError> {
        Self::load_retained_merge_materialization_with_authority_on(
            conn,
            root,
            stream_id,
            sequence,
            commit_ref,
            expected_input_hash,
            RetainedCommitAuthority::StoredBytes,
        )
    }

    fn load_retained_merge_materialization_with_verified_commit_on(
        conn: &Connection,
        root: &crate::protocol::store_commit::StoreRootRef,
        stream_id: &str,
        sequence: u64,
        commit_ref: &StoreBatchCommitRef,
        expected_input_hash: &str,
        verified: &crate::protocol::store_commit::VerifiedStoreBatchCommit,
    ) -> Result<OwnedVerifiedMergeMaterialization, DbError> {
        Self::load_retained_merge_materialization_with_authority_on(
            conn,
            root,
            stream_id,
            sequence,
            commit_ref,
            expected_input_hash,
            RetainedCommitAuthority::Operation(verified),
        )
    }

    fn load_retained_merge_materialization_with_authority_on(
        conn: &Connection,
        root: &crate::protocol::store_commit::StoreRootRef,
        stream_id: &str,
        sequence: u64,
        commit_ref: &StoreBatchCommitRef,
        expected_input_hash: &str,
        authority: RetainedCommitAuthority<'_>,
    ) -> Result<OwnedVerifiedMergeMaterialization, DbError> {
        let sequence_sql = Database::sequence_to_sqlite(stream_id, sequence)?;
        let (stored_ref, stored_hash, canonical_input): (String, String, Vec<u8>) = conn
            .query_row(
                "SELECT commit_ref, input_hash, canonical_input
                 FROM retained_merge_materializations
                 WHERE device_id = ?1 AND seq = ?2",
                rusqlite::params![stream_id, sequence_sql],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(DbError::from)?;
        let expected_ref = serde_json::to_string(commit_ref).map_err(|error| {
            DbError::Message(format!("serialize materialized Merge commit ref: {error}"))
        })?;
        if stored_ref != expected_ref {
            return Err(DbError::Message(format!(
                "retained Merge coordinate {stream_id}/{sequence} names another commit"
            )));
        }
        if stored_hash != expected_input_hash
            || stored_hash != ObjectHash::digest(&canonical_input).to_string()
        {
            return Err(DbError::Message(format!(
                "retained Merge coordinate {stream_id}/{sequence} input hash differs from its bytes"
            )));
        }
        let input: RetainedMergeMaterializationInput = serde_json::from_slice(&canonical_input)
            .map_err(|error| {
                DbError::Message(format!("retained Merge materialization input: {error}"))
            })?;
        if serde_json::to_vec(&input).map_err(|error| {
            DbError::Message(format!("serialize retained Merge materialization: {error}"))
        })? != canonical_input
        {
            return Err(DbError::Message(
                "retained Merge materialization input is not canonical".to_string(),
            ));
        }
        let input_hash = stored_hash.parse().map_err(|error| {
            DbError::Message(format!(
                "retained Merge coordinate {stream_id}/{sequence} input hash is invalid: {error}"
            ))
        })?;
        let verified = Self::open_retained_merge_materialization_input_with_authority_on(
            conn, root, commit_ref, &input, input_hash, authority,
        )?;
        Self::validate_retained_merge_pin_closure_on(
            conn,
            &input,
            &RetainedReplayOwner::Commit {
                commit: commit_ref.clone(),
                input_hash,
            },
        )?;
        Ok(verified)
    }

    pub(crate) fn load_retained_merge_materialization_by_ref_on(
        conn: &Connection,
        root: &crate::protocol::store_commit::StoreRootRef,
        reference: &StoreBatchCommitRef,
    ) -> Result<OwnedVerifiedMergeMaterialization, DbError> {
        let StoreCommitCoord {
            stream_id,
            sequence,
        } = &reference.coord;
        let stream_id = stream_id.to_string();
        let sequence_sql = Database::sequence_to_sqlite(&stream_id, *sequence)?;
        let (stored_ref, input_hash): (String, String) = conn
            .query_row(
                "SELECT commit_ref, input_hash FROM retained_merge_materializations
                 WHERE device_id = ?1 AND seq = ?2",
                rusqlite::params![&stream_id, sequence_sql],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(DbError::from)?;
        let stored_ref = Self::parse_stored_commit_ref(&stream_id, *sequence, &stored_ref)?;
        if &stored_ref != reference {
            return Err(DbError::Message(
                "retained Merge materialization coordinate contains another commit".to_string(),
            ));
        }
        Self::load_retained_merge_materialization_on(
            conn,
            root,
            &stream_id,
            *sequence,
            reference,
            &input_hash,
        )
    }

    pub(super) fn load_retained_merge_history_checkpoint_on(
        conn: &Connection,
        root: &crate::protocol::store_commit::StoreRootRef,
        reference: &StoreBatchCommitRef,
    ) -> Result<crate::protocol::store_commit::OpenedRetainedMergeHistorySummary, DbError> {
        let StoreCommitCoord {
            stream_id,
            sequence,
        } = &reference.coord;
        let stream = stream_id.to_string();
        let sequence_sql = Database::sequence_to_sqlite(&stream, *sequence)?;
        let snapshot_reference: Option<String> = conn
            .query_row(
                "SELECT commit_ref FROM snapshot_coverage WHERE device_id = ?1 AND seq = ?2",
                rusqlite::params![&stream, sequence_sql],
                |row| row.get(0),
            )
            .optional()
            .map_err(DbError::from)?;
        if let Some(snapshot_reference) = snapshot_reference {
            let snapshot_reference: StoreBatchCommitRef = serde_json::from_str(&snapshot_reference)
                .map_err(|error| {
                    DbError::Message(format!("snapshot Merge checkpoint commit ref: {error}"))
                })?;
            if &snapshot_reference != reference {
                return Err(DbError::Message(
                    "snapshot Merge checkpoint coordinate contains another commit".to_string(),
                ));
            }
            let baseline = load_generation_zero_replay_baseline_on(conn)?.ok_or_else(|| {
                DbError::Message(
                    "snapshot Merge checkpoint has no retained replay baseline".to_string(),
                )
            })?;
            let RetainedReplayAuthority::StableSnapshot(authority) = baseline.authority else {
                return Err(DbError::Message(
                    "snapshot Merge checkpoint has genesis replay authority".to_string(),
                ));
            };
            let summary = authority.metadata.history_summary;
            summary
                .validate_snapshot_baseline()
                .map_err(|error| DbError::Message(format!("snapshot Merge checkpoint: {error}")))?;
            if summary
                .frontier()
                .map_err(|error| {
                    DbError::Message(format!("snapshot Merge checkpoint frontier: {error}"))
                })?
                .get(stream_id)
                != Some(reference)
            {
                return Err(DbError::Message(
                    "snapshot Merge checkpoint is absent from its signed frontier".to_string(),
                ));
            }
            let state = load_store_device_snapshot_on(conn, reference)?;
            let expected_state = crate::protocol::store_commit::StoreDeviceStateRef::from_resolved(
                CommitFrontier(
                    summary
                        .frontier()
                        .map_err(|error| DbError::Message(error.to_string()))?,
                ),
                &state,
            )
            .map_err(|error| DbError::Message(error.to_string()))?;
            if summary.post_state != expected_state {
                return Err(DbError::Message(
                    "snapshot Merge checkpoint state differs from its signed reference".to_string(),
                ));
            }
            return Ok(
                crate::protocol::store_commit::OpenedRetainedMergeHistorySummary {
                    announcement_frontier: summary.announcement_frontier.clone(),
                    post_state: state,
                    summary,
                },
            );
        }
        let (stored_ref, input_hash): (String, String) = conn
            .query_row(
                "SELECT commit_ref, input_hash FROM retained_merge_materializations \
                 WHERE device_id = ?1 AND seq = ?2",
                rusqlite::params![stream, sequence_sql],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(DbError::from)?;
        let stored_ref: StoreBatchCommitRef =
            serde_json::from_str(&stored_ref).map_err(|error| {
                DbError::Message(format!("retained Merge checkpoint commit ref: {error}"))
            })?;
        if &stored_ref != reference {
            return Err(DbError::Message(
                "retained Merge checkpoint coordinate contains another commit".to_string(),
            ));
        }
        let retained = Self::load_retained_merge_materialization_on(
            conn,
            root,
            &stream_id.to_string(),
            *sequence,
            reference,
            &input_hash,
        )?;
        Self::open_retained_merge_history_checkpoint_on(conn, reference, &retained)
    }

    pub(super) fn open_retained_merge_history_checkpoint_on(
        conn: &Connection,
        reference: &StoreBatchCommitRef,
        retained: &OwnedVerifiedMergeMaterialization,
    ) -> Result<crate::protocol::store_commit::OpenedRetainedMergeHistorySummary, DbError> {
        if retained.commit_ref() != reference {
            return Err(DbError::Message(
                "retained Merge checkpoint materialization names another commit".to_string(),
            ));
        }
        let head_ref = crate::protocol::store_commit::StoreDeviceHeadRef {
            head_hash: retained.activation_head().head_hash(),
            object: retained.activation_head_object().clone(),
        };
        let state = load_store_device_snapshot_on(conn, reference)?;
        retained
            .history_summary()
            .open(
                retained.commit(),
                reference,
                retained.activation_head(),
                &head_ref,
                &state,
            )
            .map_err(|error| DbError::Message(format!("retained Merge checkpoint: {error}")))
    }

    #[cfg(test)]
    pub(crate) fn load_retained_merge_replay_inputs_on(
        conn: &Connection,
        root: &crate::protocol::store_commit::StoreRootRef,
    ) -> Result<Vec<OwnedVerifiedMergeMaterialization>, DbError> {
        let mut statement = conn
            .prepare(
                "SELECT device_id, seq, commit_ref, input_hash
                 FROM retained_merge_materializations
                 ORDER BY device_id, seq",
            )
            .map_err(DbError::from)?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })
            .map_err(DbError::from)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(DbError::from)?;
        drop(statement);
        rows.into_iter()
            .map(|(stream_id, sequence, encoded_ref, input_hash)| {
                let sequence = Database::sequence_from_sqlite(&stream_id, sequence)?;
                let commit_ref = Self::parse_stored_commit_ref(&stream_id, sequence, &encoded_ref)?;
                Self::load_retained_merge_materialization_on(
                    conn,
                    root,
                    &stream_id,
                    sequence,
                    &commit_ref,
                    &input_hash,
                )
            })
            .collect()
    }

    pub(crate) fn load_merge_replay_write_overlays_on(
        conn: &Connection,
        active_accepted_writes: &BTreeSet<WriteId>,
        retracted_writes: &BTreeSet<WriteId>,
    ) -> Result<Vec<MergeReplayWriteOverlay>, DbError> {
        if !active_accepted_writes.is_disjoint(retracted_writes) {
            return Err(DbError::Message(
                "retained replay classifies one write as active and retracted".to_string(),
            ));
        }
        let mut statement = conn
            .prepare(
                "SELECT write_id, status, changeset
                 FROM store_writes
                 ORDER BY ordinal",
            )
            .map_err(DbError::from)?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                ))
            })
            .map_err(DbError::from)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(DbError::from)?;
        drop(statement);
        let mut overlays = Vec::new();
        for (encoded_write_id, raw_status, stored_store_changeset) in rows {
            let write_id = WriteId::from_generated(encoded_write_id.clone());
            let status: WriteStatus = serde_json::from_str(&raw_status).map_err(|error| {
                DbError::Message(format!(
                    "retained replay write {encoded_write_id} status: {error}"
                ))
            })?;
            let partitions = StoreDatabase::store_write_partitions_on(
                conn,
                &encoded_write_id,
                &stored_store_changeset,
            )?;
            let active = active_accepted_writes.contains(&write_id);
            let retracted = retracted_writes.contains(&write_id);
            let partitions = match status {
                WriteStatus::LocalOnly => {
                    if partitions.store.is_some() || !partitions.circles.is_empty() {
                        return Err(DbError::Message(format!(
                            "Local-only write {encoded_write_id} carries a shared partition"
                        )));
                    }
                    PreparedStoreWritePartitions {
                        store: None,
                        circles: Vec::new(),
                        local: partitions.local,
                    }
                }
                WriteStatus::Pending => partitions,
                WriteStatus::Publishing | WriteStatus::Blocked(_) => {
                    if retracted {
                        return Err(DbError::Message(format!(
                            "unresolved write {encoded_write_id} is already terminally retracted"
                        )));
                    }
                    if active {
                        PreparedStoreWritePartitions {
                            store: None,
                            circles: Vec::new(),
                            local: partitions.local,
                        }
                    } else {
                        partitions
                    }
                }
                WriteStatus::Published(_) => {
                    if retracted {
                        PreparedStoreWritePartitions {
                            store: None,
                            circles: Vec::new(),
                            local: None,
                        }
                    } else if active {
                        PreparedStoreWritePartitions {
                            store: None,
                            circles: Vec::new(),
                            local: partitions.local,
                        }
                    } else {
                        return Err(DbError::Message(format!(
                            "published write {encoded_write_id} has no retained replay input"
                        )));
                    }
                }
                WriteStatus::Resolved(_) => PreparedStoreWritePartitions {
                    store: None,
                    circles: Vec::new(),
                    local: None,
                },
            };
            if partitions.store.is_some()
                || !partitions.circles.is_empty()
                || partitions.local.is_some()
            {
                overlays.push(MergeReplayWriteOverlay {
                    write_id,
                    partitions,
                });
            }
        }
        Ok(overlays)
    }

    pub(crate) fn generation_zero_replay_baseline_on(
        conn: &Connection,
    ) -> Result<RetainedReplayBaseline, DbError> {
        load_generation_zero_replay_baseline_on(conn)?.ok_or_else(|| {
            DbError::Message("generation-zero retained replay baseline is absent".to_string())
        })
    }

    pub(crate) fn load_merge_retraction_cleanup_on(
        conn: &Connection,
        candidate: &StoreBatchCommitRef,
    ) -> Result<PreparedMergeCandidate, DbError> {
        let StoreCommitCoord {
            stream_id,
            sequence,
        } = &candidate.coord;
        let stream_id = stream_id.to_string();
        let sequence_sql = Database::sequence_to_sqlite(&stream_id, *sequence)?;
        let encoded_ref = serde_json::to_string(candidate).map_err(|error| {
            DbError::Message(format!("serialize Merge retraction cleanup ref: {error}"))
        })?;
        let (stored_hash, canonical_cleanup): (String, Vec<u8>) = conn
            .query_row(
                "SELECT cleanup_hash, canonical_cleanup
                 FROM merge_retraction_cleanups
                 WHERE device_id = ?1 AND seq = ?2 AND commit_ref = ?3",
                rusqlite::params![&stream_id, sequence_sql, &encoded_ref],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(DbError::from)?;
        if stored_hash != ObjectHash::digest(&canonical_cleanup).to_string() {
            return Err(DbError::Message(
                "Merge retraction cleanup hash differs from its bytes".to_string(),
            ));
        }
        let input: MergeRetractionCleanupInput = serde_json::from_slice(&canonical_cleanup)
            .map_err(|error| {
                DbError::Message(format!("parse Merge retraction cleanup: {error}"))
            })?;
        if serde_json::to_vec(&input).map_err(|error| {
            DbError::Message(format!("serialize Merge retraction cleanup: {error}"))
        })? != canonical_cleanup
        {
            return Err(DbError::Message(
                "Merge retraction cleanup is not canonical".to_string(),
            ));
        }
        let commit =
            DurablePreparedProtocolObject::new(input.commit.stored_bytes().to_vec(), input.commit);
        let head = DurablePreparedProtocolObject::new(
            input.activation_head.stored_bytes().to_vec(),
            input.activation_head,
        );
        let prepared = parse_prepared_merge_candidate_parts_on(conn, &commit, &head)?;
        if &prepared.reference != candidate {
            return Err(DbError::Message(
                "Merge retraction cleanup opens another candidate".to_string(),
            ));
        }
        Ok(prepared)
    }
}

#[cfg(test)]
mod circle_epoch_cutoff_tests {
    use super::*;
    use crate::id_provider::SequentialIdProvider;
    use crate::protocol::causal_grants::AuthorStreamId;
    use crate::protocol::circle::{CircleControlCoord, CircleEpochId, CircleId};
    use crate::protocol::membership::MembershipGrantId;
    use crate::storage::cloud::ObjectSlot;

    fn commit_reference(
        stream_id: AuthorStreamId,
        sequence: u64,
        label: &str,
    ) -> StoreBatchCommitRef {
        let bytes = format!("{label}-stored");
        StoreBatchCommitRef {
            coord: StoreCommitCoord {
                stream_id,
                sequence,
            },
            commit_hash: ObjectHash::digest(format!("{label}-semantic").as_bytes()),
            object: crate::storage::ExactObjectRef::new(
                ObjectSlot::logical(format!("store-v1/commits/{label}.json"))
                    .expect("valid commit slot"),
                bytes.len() as u64,
                ObjectHash::digest(bytes.as_bytes()),
            ),
        }
    }

    #[test]
    fn circle_epoch_cutoff_accepts_exact_history_and_omits_later_packages() {
        let stream_id = AuthorStreamId::from_digest(ObjectHash::digest(b"cutoff stream"));
        let accepted = commit_reference(stream_id, 2, "accepted");
        let later = commit_reference(stream_id, 3, "later");
        let circle_id = CircleId::from_bytes([7; 16]);
        let control = CircleControlCoord {
            device_id: "cutoff-device".to_string(),
            stream_id: AuthorStreamId::from_digest(ObjectHash::digest(b"control stream")),
            author_pubkey: "cutoff-author".to_string(),
            author_owner_grant: MembershipGrantId(ObjectHash::digest(b"owner grant")),
            seq: 1,
            control_hash: ObjectHash::digest(b"control"),
        };
        let epoch_id = CircleEpochId::generate(&SequentialIdProvider::new("cutoff-epoch"));
        let index = CircleReplayEpochIndex {
            control_epochs: BTreeMap::from([((circle_id, control.clone()), epoch_id)]),
            cutoffs: BTreeMap::from([(
                (circle_id, epoch_id),
                CommitFrontier(BTreeMap::from([(stream_id, accepted.clone())])),
            )]),
        };

        assert!(index
            .permits(&accepted, circle_id, &control)
            .expect("accepted commit is valid"));
        assert!(!index
            .permits(&later, circle_id, &control)
            .expect("later commit is excluded"));
    }

    #[test]
    fn circle_epoch_cutoff_rejects_another_commit_at_the_accepted_coordinate() {
        let stream_id = AuthorStreamId::from_digest(ObjectHash::digest(b"collision stream"));
        let accepted = commit_reference(stream_id, 2, "accepted-coordinate");
        let collision = commit_reference(stream_id, 2, "conflicting-coordinate");
        let circle_id = CircleId::from_bytes([8; 16]);
        let control = CircleControlCoord {
            device_id: "collision-device".to_string(),
            stream_id: AuthorStreamId::from_digest(ObjectHash::digest(b"collision control")),
            author_pubkey: "collision-author".to_string(),
            author_owner_grant: MembershipGrantId(ObjectHash::digest(b"collision owner grant")),
            seq: 1,
            control_hash: ObjectHash::digest(b"collision control hash"),
        };
        let epoch_id = CircleEpochId::generate(&SequentialIdProvider::new("collision-epoch"));
        let index = CircleReplayEpochIndex {
            control_epochs: BTreeMap::from([((circle_id, control.clone()), epoch_id)]),
            cutoffs: BTreeMap::from([(
                (circle_id, epoch_id),
                CommitFrontier(BTreeMap::from([(stream_id, accepted)])),
            )]),
        };

        let error = index
            .permits(&collision, circle_id, &control)
            .expect_err("same coordinate with different exact commit must fail");
        assert!(
            error
                .to_string()
                .contains("conflicts with its accepted epoch cutoff"),
            "{error}"
        );
    }
}

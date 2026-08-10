use super::*;
use crate::store::retained_merge_replay::CircleReplayEpochIndex;
use crate::store::store_session::{StoreRecords, StoreTransaction};
use crate::{
    activated_merge_membership_remote_objects, ObjectHash, PreparedMergeMaterialization,
    PreparedMergeMaterializationPackage,
};
use coven_protocol::membership::{ApplyOutcome, HeldStorePositionReason, LocalStoreMembership};
use coven_protocol::store_commit::{CommitFrontier, StoreRootRef};
use coven_protocol::synced_schema::SyncedTable;
use std::collections::BTreeSet;

#[derive(Clone, Default)]
pub(super) struct RetainedReplayCache {
    baseline: Option<RetainedReplayBaseline>,
    verified: BTreeMap<(String, u64), RetainedReplayEntry>,
}

#[derive(Clone)]
struct RetainedReplayEntry {
    materialization: OwnedVerifiedMergeMaterialization,
    history_checkpoint_verified: bool,
}

struct ReplayVerifiedStoreLookup<'lookup, 'root> {
    cache: &'lookup mut RetainedReplayCache,
    registrations: &'lookup mut dyn VerifiedRegistrationLookup,
    root: &'root StoreRootRef,
}

impl VerifiedRegistrationLookup for ReplayVerifiedStoreLookup<'_, '_> {
    fn activated_registration_on(
        &mut self,
        records: StoreRecords<'_>,
        root: &StoreRootRef,
        reference: &StoreDeviceRegistrationRef,
    ) -> Result<coven_protocol::store_commit::StoreDeviceRegistration, DbError> {
        self.registrations
            .activated_registration_on(records, root, reference)
    }
}

impl VerifiedStoreLookup for ReplayVerifiedStoreLookup<'_, '_> {
    fn retained_materialization_by_ref_on(
        &mut self,
        records: StoreRecords<'_>,
        reference: &StoreBatchCommitRef,
    ) -> Result<OwnedVerifiedMergeMaterialization, DbError> {
        if let Some(materialization) = self.cache.cached_by_ref(reference)? {
            return Ok(materialization.clone());
        }
        let materialization = StoreDatabase::load_retained_merge_materialization_by_ref_on(
            records,
            self.root,
            self.registrations,
            reference,
        )?;
        self.cache.insert_verified(materialization.clone())?;
        Ok(materialization)
    }
}

enum RetainedCommitAuthorities<'a> {
    StoredBytes,
    Operation(
        &'a BTreeMap<
            coven_protocol::store_commit::StoreBatchCommitRef,
            coven_protocol::store_commit::VerifiedStoreBatchCommit,
        >,
    ),
}

impl RetainedReplayCache {
    pub(super) fn commit_installed_baseline(&mut self, baseline: RetainedReplayBaseline) {
        match &self.baseline {
            Some(existing) => assert_eq!(
                existing, &baseline,
                "committed retained replay baseline conflicts with connection authority"
            ),
            None => self.baseline = Some(baseline),
        }
    }

    pub(super) fn validate_owner_anchor(
        &self,
        authority: &RetainedReplayGenesisAuthority,
    ) -> Result<(), DbError> {
        let baseline = self.baseline.as_ref().ok_or_else(|| {
            DbError::Message(
                "verified Store owner anchor has no retained replay baseline".to_string(),
            )
        })?;
        let matches = match &baseline.authority {
            RetainedReplayAuthority::Genesis(existing) => existing == authority,
            RetainedReplayAuthority::StableSnapshot(existing) => {
                existing.store_root == authority.store_root
                    && existing.founder_registration == authority.founder_registration
            }
        };
        if !matches {
            return Err(DbError::Message(
                "verified replay baseline differs from the Store owner anchor".to_string(),
            ));
        }
        Ok(())
    }

    pub(super) fn baseline_on(
        &mut self,
        records: StoreRecords<'_>,
    ) -> Result<&RetainedReplayBaseline, DbError> {
        if self.baseline.is_none() {
            self.baseline = Some(StoreDatabase::generation_zero_replay_baseline_on(records)?);
        }
        Ok(self
            .baseline
            .as_ref()
            .expect("retained replay baseline was installed in the cache"))
    }

    pub(super) fn validate_insert_verified(
        &self,
        materialization: &OwnedVerifiedMergeMaterialization,
    ) -> Result<(), DbError> {
        let coordinate = materialization.commit_ref().coord.clone();
        let key = (coordinate.stream_id.to_string(), coordinate.sequence());
        match self.verified.get(&key) {
            None => Ok(()),
            Some(existing)
                if existing.materialization.commit_ref() == materialization.commit_ref()
                    && existing.materialization.input_hash() == materialization.input_hash() =>
            {
                Ok(())
            }
            Some(_) => Err(DbError::Message(
                "retained Merge materialization cache coordinate contains another exact input"
                    .to_string(),
            )),
        }
    }

    pub(super) fn insert_verified(
        &mut self,
        materialization: OwnedVerifiedMergeMaterialization,
    ) -> Result<(), DbError> {
        self.validate_insert_verified(&materialization)?;
        let coordinate = materialization.commit_ref().coord.clone();
        let key = (coordinate.stream_id.to_string(), coordinate.sequence());
        if let std::collections::btree_map::Entry::Vacant(entry) = self.verified.entry(key) {
            entry.insert(RetainedReplayEntry {
                materialization,
                history_checkpoint_verified: false,
            });
        }
        Ok(())
    }

    fn verified_by_ref(
        &self,
        reference: &StoreBatchCommitRef,
    ) -> Result<&OwnedVerifiedMergeMaterialization, DbError> {
        self.cached_by_ref(reference)?.ok_or_else(|| {
            DbError::Message(format!(
                "retained Merge materialization cache omits {reference:?}"
            ))
        })
    }

    pub(super) fn cached_by_ref(
        &self,
        reference: &StoreBatchCommitRef,
    ) -> Result<Option<&OwnedVerifiedMergeMaterialization>, DbError> {
        let key = (
            reference.coord.stream_id.to_string(),
            reference.coord.sequence(),
        );
        let Some(verified) = self.verified.get(&key) else {
            return Ok(None);
        };
        if verified.materialization.commit_ref() != reference {
            return Err(DbError::Message(
                "retained Merge materialization cache coordinate contains another commit"
                    .to_string(),
            ));
        }
        Ok(Some(&verified.materialization))
    }

    pub(super) fn retained_history_checkpoint_on(
        &mut self,
        records: StoreRecords<'_>,
        registrations: &mut dyn VerifiedRegistrationLookup,
        reference: &StoreBatchCommitRef,
    ) -> Result<RetainedMergeHistoryCheckpoint, DbError> {
        let key = (
            reference.coord.stream_id.to_string(),
            reference.coord.sequence(),
        );
        let entry = self.verified.get_mut(&key).ok_or_else(|| {
            DbError::Message(format!(
                "retained Merge materialization cache omits {reference:?}"
            ))
        })?;
        if entry.materialization.commit_ref() != reference {
            return Err(DbError::Message(
                "retained Merge materialization cache coordinate contains another commit"
                    .to_string(),
            ));
        }
        if !entry.history_checkpoint_verified {
            let checkpoint = StoreDatabase::open_retained_merge_history_checkpoint_on(
                records,
                registrations,
                reference,
                &entry.materialization,
            )?;
            entry.history_checkpoint_verified = true;
            return Ok(checkpoint);
        }
        Ok(RetainedMergeHistoryCheckpoint::Commit(Box::new(
            entry.materialization.clone(),
        )))
    }

    pub(super) fn replay_inputs_on(
        &mut self,
        records: StoreRecords<'_>,
        root: &coven_protocol::store_commit::StoreRootRef,
        registrations: &mut dyn VerifiedRegistrationLookup,
    ) -> Result<Vec<OwnedVerifiedMergeMaterialization>, DbError> {
        self.replay_inputs_with_authorities_on(
            records,
            root,
            registrations,
            RetainedCommitAuthorities::StoredBytes,
        )
    }

    pub(super) fn replay_inputs_with_verified_commits_on(
        &mut self,
        records: StoreRecords<'_>,
        root: &coven_protocol::store_commit::StoreRootRef,
        registrations: &mut dyn VerifiedRegistrationLookup,
        verified: &BTreeMap<
            coven_protocol::store_commit::StoreBatchCommitRef,
            coven_protocol::store_commit::VerifiedStoreBatchCommit,
        >,
    ) -> Result<Vec<OwnedVerifiedMergeMaterialization>, DbError> {
        self.replay_inputs_with_authorities_on(
            records,
            root,
            registrations,
            RetainedCommitAuthorities::Operation(verified),
        )
    }

    fn replay_inputs_with_authorities_on(
        &mut self,
        records: StoreRecords<'_>,
        root: &coven_protocol::store_commit::StoreRootRef,
        registrations: &mut dyn VerifiedRegistrationLookup,
        authorities: RetainedCommitAuthorities<'_>,
    ) -> Result<Vec<OwnedVerifiedMergeMaterialization>, DbError> {
        let rows = records.retained_materialization_rows()?;

        self.replay_inputs_from_rows(rows, authorities, |authority, row| match authority {
            RetainedCommitAuthorities::StoredBytes => {
                StoreDatabase::load_retained_merge_materialization_on(
                    records,
                    root,
                    registrations,
                    &row.0,
                    row.1,
                    &row.2,
                    &row.3,
                )
            }
            RetainedCommitAuthorities::Operation(operation_verified) => {
                let commit = operation_verified.get(&row.2).ok_or_else(|| {
                    DbError::Message(format!(
                        "retained Merge commit {:?} is absent from the operation-verified history",
                        row.2
                    ))
                })?;
                StoreDatabase::load_retained_merge_materialization_with_verified_commit_on(
                    records,
                    root,
                    registrations,
                    &row.0,
                    row.1,
                    &row.2,
                    &row.3,
                    commit,
                )
            }
        })
    }

    fn replay_inputs_in_transaction(
        &mut self,
        records: StoreTransaction<'_, '_>,
        root: &StoreRootRef,
        registrations: &mut dyn VerifiedRegistrationLookup,
    ) -> Result<Vec<OwnedVerifiedMergeMaterialization>, DbError> {
        let rows = records.retained_materialization_rows()?;
        self.replay_inputs_from_rows(
            rows,
            RetainedCommitAuthorities::StoredBytes,
            |authority, row| match authority {
                RetainedCommitAuthorities::StoredBytes => records.load_retained_materialization(
                    root,
                    registrations,
                    &row.0,
                    row.1,
                    &row.2,
                    &row.3,
                    None,
                ),
                RetainedCommitAuthorities::Operation(_) => {
                    unreachable!("transaction replay loads durable retained commit authority")
                }
            },
        )
    }

    fn replay_inputs_from_rows(
        &mut self,
        rows: Vec<(String, i64, String, String)>,
        authorities: RetainedCommitAuthorities<'_>,
        mut load: impl FnMut(
            &RetainedCommitAuthorities<'_>,
            &(String, u64, StoreBatchCommitRef, String),
        ) -> Result<OwnedVerifiedMergeMaterialization, DbError>,
    ) -> Result<Vec<OwnedVerifiedMergeMaterialization>, DbError> {
        let rows = rows
            .into_iter()
            .map(|(stream_id, sequence, encoded_ref, encoded_input_hash)| {
                let sequence = Database::sequence_from_sqlite(&stream_id, sequence)?;
                let commit_ref = crate::store::materialized_commit_index::parse_stored_commit_ref(
                    &stream_id,
                    sequence,
                    &encoded_ref,
                )?;
                Ok((stream_id, sequence, commit_ref, encoded_input_hash))
            })
            .collect::<Result<Vec<_>, DbError>>()?;

        let mut verified = BTreeMap::new();
        let mut replay_inputs = Vec::with_capacity(rows.len());
        for (stream_id, sequence, commit_ref, encoded_input_hash) in rows {
            let input_hash = encoded_input_hash.parse::<ObjectHash>().map_err(|error| {
                DbError::context(
                    format!(
                        "retained Merge coordinate {stream_id}/{sequence} input hash is invalid"
                    ),
                    error,
                )
            })?;
            let key = (stream_id.clone(), sequence);
            let (materialization, history_checkpoint_verified) = match self.verified.get(&key) {
                Some(cached)
                    if cached.materialization.commit_ref() == &commit_ref
                        && cached.materialization.input_hash() == input_hash =>
                {
                    (
                        cached.materialization.clone(),
                        cached.history_checkpoint_verified,
                    )
                }
                _ => (
                    load(
                        &authorities,
                        &(
                            stream_id.clone(),
                            sequence,
                            commit_ref.clone(),
                            encoded_input_hash.clone(),
                        ),
                    )?,
                    false,
                ),
            };
            verified.insert(
                key,
                RetainedReplayEntry {
                    materialization: materialization.clone(),
                    history_checkpoint_verified,
                },
            );
            replay_inputs.push(materialization);
        }
        self.verified = verified;
        Ok(replay_inputs)
    }

    pub(super) fn verified_circle_activation_on(
        &self,
        records: StoreRecords<'_>,
        circle_id: coven_protocol::circle::CircleId,
        control: &coven_protocol::circle::CircleControlCoord,
    ) -> Result<Option<coven_protocol::circle_activation::VerifiedCircleReference>, DbError> {
        let Some(activation_commit) = records.circle_activation_commit_ref(circle_id, control)?
        else {
            return Ok(None);
        };
        self.verified_by_ref(&activation_commit)?
            .circle_activation(circle_id, control)
            .map(Some)
    }

    pub(super) fn circle_replay_epoch_index_on(
        &self,
        records: StoreRecords<'_>,
    ) -> Result<CircleReplayEpochIndex, DbError> {
        let rows = records.circle_replay_controls()?;
        Self::circle_replay_epoch_index_from_rows(rows, |circle_id, control| {
            self.verified_circle_activation_on(records, circle_id, control)
        })
    }

    fn circle_replay_epoch_index_in_transaction(
        &self,
        records: StoreTransaction<'_, '_>,
    ) -> Result<CircleReplayEpochIndex, DbError> {
        let rows = records.circle_replay_controls()?;
        Self::circle_replay_epoch_index_from_rows(rows, |circle_id, control| {
            let Some(activation_commit) =
                records.circle_activation_commit_ref(circle_id, control)?
            else {
                return Ok(None);
            };
            self.verified_by_ref(&activation_commit)?
                .circle_activation(circle_id, control)
                .map(Some)
        })
    }

    fn circle_replay_epoch_index_from_rows(
        rows: Vec<(String, String)>,
        mut activation: impl FnMut(
            coven_protocol::circle::CircleId,
            &coven_protocol::circle::CircleControlCoord,
        ) -> Result<
            Option<coven_protocol::circle_activation::VerifiedCircleReference>,
            DbError,
        >,
    ) -> Result<CircleReplayEpochIndex, DbError> {
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
            let verified = activation(circle_id, &control)?.ok_or_else(|| {
                DbError::Message(format!(
                    "Circle replay index activation for {circle_id} disappeared"
                ))
            })?;
            index.record_control(circle_id, &verified.control)?;
        }
        Ok(index)
    }

    pub(super) fn replay_projection_on(
        &mut self,
        transaction_records: StoreTransaction<'_, '_>,
        root: &StoreRootRef,
        registrations: &mut dyn VerifiedRegistrationLookup,
        blob_decls: &BlobDecls,
        gates: &crate::Gates,
        synced_tables: &[SyncedTable],
        routing_key: Option<&coven_protocol::circle::RowRoutingKey>,
        retracted: &BTreeSet<StoreBatchCommitRef>,
        history_cut: Option<&CommitFrontier>,
        include_local_write_overlays: bool,
        local_store_membership: LocalStoreMembership,
    ) -> Result<ReplayProjection, DbError> {
        if self.baseline.is_none() {
            self.baseline = Some(transaction_records.generation_zero_replay_baseline()?);
        }
        let baseline = self
            .baseline
            .as_ref()
            .expect("retained replay baseline was installed in the cache")
            .clone();
        let replay = transaction_records
            .open_replay_projection(&transaction_records.replay_baseline_image_bytes(&baseline)?)?;
        let schema = replay.table_schema(synced_tables, gates)?;
        let circle_bootstraps = transaction_records.claimed_circle_bootstrap_coverage_refs()?;
        let mut circle_bootstrap_cuts = BTreeMap::new();
        for coverage in &circle_bootstraps {
            replay.install_circle_bootstrap(
                &transaction_records.verified_payload(coverage.bootstrap.image.image_hash)?,
                coverage,
                synced_tables,
                routing_key,
            )?;
            if circle_bootstrap_cuts
                .insert(coverage.circle_id, coverage.bootstrap.coverage.clone())
                .is_some()
            {
                return Err(DbError::Message(format!(
                    "retained replay has duplicate Circle {} bootstraps",
                    coverage.circle_id
                )));
            }
        }
        let retained =
            self.replay_inputs_in_transaction(transaction_records, root, registrations)?;
        let circle_epochs = self.circle_replay_epoch_index_in_transaction(transaction_records)?;
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
            transaction_records
                .merge_replay_write_overlays(&active_accepted_writes, &retracted_writes)?
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
                    if let coven_protocol::audience_package::PackageAudience::Circle {
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
                    let family = materialization.commit().candidate_family();
                    let owner = materialization.commit_ref();
                    let entry_bytes = transaction_records
                        .retained_membership_authority_bytes(&objects.entry().object, "entry")?;
                    let head_bytes = transaction_records
                        .retained_membership_authority_bytes(&objects.head().object, "head")?;
                    let resolution_bytes = objects
                        .resolution()
                        .map(|resolution| {
                            transaction_records.retained_membership_authority_bytes(
                                &resolution.object,
                                "resolution",
                            )
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
                    history_evidence: materialization.history_evidence().clone(),
                    membership_objects: materialization.membership_objects().cloned(),
                    membership_remote_objects,
                    registrations: materialization.registrations().to_vec(),
                    packages,
                    device_operations: materialization.device_operations().clone(),
                    circle_activations: materialization.circle_activations().clone(),
                    package_application,
                };
                let outcome = {
                    let mut authority = ReplayVerifiedStoreLookup {
                        cache: self,
                        registrations,
                        root,
                    };
                    replay.apply_materialization(
                        &mut authority,
                        blob_decls,
                        gates,
                        synced_tables,
                        routing_key,
                        local_store_membership,
                        timestamp_policy,
                        &circle_bootstrap_cuts,
                        replay_materialization,
                    )
                }
                .map_err(|error| {
                    DbError::context(
                        format!(
                            "apply retained Merge commit {reference:?} during canonical replay"
                        ),
                        error,
                    )
                })?;
                match outcome {
                    ApplyOutcome::Applied(_) => {
                        pending.remove(&reference);
                        applied.insert(reference);
                        made_progress = true;
                    }
                    ApplyOutcome::Held(HeldStorePositionReason::ForeignKeyDependency) => {}
                    ApplyOutcome::Held(reason) => {
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
            replay.apply_write_overlay(overlay, schema.clone())?;
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

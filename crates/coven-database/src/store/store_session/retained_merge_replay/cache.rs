use super::*;
use crate::store::retained_merge_replay::CircleReplayEpochIndex;
use crate::store::store_session::{StoreRecords, StoreTransaction};
use crate::{
    activated_merge_membership_remote_objects, ObjectHash, PreparedMergeMaterialization,
    PreparedMergeMaterializationPackage,
};
use coven_protocol::membership::LocalStoreMembership;
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

impl RetainedReplayCache {
    /// Forget everything derived from a baseline that has just been superseded.
    ///
    /// Advancing the baseline retires retained materializations, so both halves
    /// of this cache describe a store that no longer exists. It is a cache, so
    /// the repair is to read again, not to patch the entries.
    pub(super) fn forget_superseded_baseline(&mut self) {
        self.baseline = None;
        self.verified.clear();
    }

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
            RetainedReplayAuthority::InstalledSnapshot(existing) => {
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

    /// Retained materializations, each opened from its own stored canonical
    /// bytes.
    ///
    /// A retained row is the durable record that this device verified the
    /// commit: the row was written by the transaction that verified and applied
    /// it, its bytes are pinned by the stored input hash, and opening it
    /// re-parses the commit and re-checks its signature against the activated
    /// registration. So this answers from local state alone, and the entries it
    /// keeps are memos over content-addressed rows rather than remembered
    /// verdicts.
    pub(super) fn replay_inputs_on(
        &mut self,
        records: StoreRecords<'_>,
        root: &coven_protocol::store_commit::StoreRootRef,
        registrations: &mut dyn VerifiedRegistrationLookup,
    ) -> Result<Vec<OwnedVerifiedMergeMaterialization>, DbError> {
        let rows = records.retained_materialization_rows()?;
        self.replay_inputs_from_rows(rows, |row| {
            StoreDatabase::load_retained_merge_materialization_on(
                records,
                root,
                registrations,
                &row.0,
                row.1,
                &row.2,
                &row.3,
            )
        })
    }

    fn replay_inputs_in_transaction(
        &mut self,
        records: StoreTransaction<'_, '_>,
        root: &StoreRootRef,
        registrations: &mut dyn VerifiedRegistrationLookup,
    ) -> Result<Vec<OwnedVerifiedMergeMaterialization>, DbError> {
        let rows = records.retained_materialization_rows()?;
        self.replay_inputs_from_rows(rows, |row| {
            records.load_retained_materialization(
                root,
                registrations,
                &row.0,
                row.1,
                &row.2,
                &row.3,
                None,
            )
        })
    }

    fn replay_inputs_from_rows(
        &mut self,
        rows: Vec<(String, i64, String, String)>,
        mut load: impl FnMut(
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
                    load(&(
                        stream_id.clone(),
                        sequence,
                        commit_ref.clone(),
                        encoded_input_hash.clone(),
                    ))?,
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
        journal: crate::ReplayJournal<'_>,
        local_store_membership: LocalStoreMembership,
    ) -> Result<crate::store::store_session::ReplayProjectionResult, DbError> {
        self.replay_projection_watching_on(
            transaction_records,
            root,
            registrations,
            blob_decls,
            gates,
            synced_tables,
            routing_key,
            retracted,
            history_cut,
            journal,
            local_store_membership,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn replay_projection_watching_on(
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
        journal: crate::ReplayJournal<'_>,
        local_store_membership: LocalStoreMembership,
        watched: Option<&StoreBatchCommitRef>,
    ) -> Result<crate::store::store_session::ReplayProjectionResult, DbError> {
        if self.baseline.is_none() {
            self.baseline = Some(transaction_records.generation_zero_replay_baseline()?);
        }
        let baseline = self
            .baseline
            .as_ref()
            .expect("retained replay baseline was installed in the cache")
            .clone();
        let replay = transaction_records.open_replay_projection(&baseline)?;
        let schema = replay.table_schema(synced_tables, gates)?;
        let mut private_rows = replay.private_rows(gates, &schema)?;
        let circle_bootstraps = transaction_records.claimed_circle_bootstrap_coverage_refs()?;
        let mut circle_bootstrap_cuts = BTreeMap::new();
        // Circles whose bootstrap stands above the horizon this projection
        // stops at. See `deferred_bootstrap_circles` below for why their rows
        // are left out entirely rather than replayed.
        let mut deferred_bootstrap_circles = BTreeSet::new();
        for coverage in &circle_bootstraps {
            // A claimed bootstrap is a fact about now, not about the horizon a
            // projection stops at. A projection that does not reach the commit
            // activating it must leave it out — and must leave that Circle's
            // rows out with it, because the bootstrap image is their authority
            // and every replay from this projection installs it. Carrying rows
            // this projection replayed for the Circle would put them under the
            // bootstrap image that restates them, and the install would insert
            // each one a second time.
            if history_cut.is_some_and(|cut| !cut.covers_commit(&coverage.activation_commit)) {
                deferred_bootstrap_circles.insert(coverage.circle_id);
                continue;
            }
            // A bootstrap the baseline already covers is in the image this
            // replay starts from: the capture that produced the image installed
            // it. Its cut is still recorded below, because the packages under
            // it stay skipped either way.
            if !baseline
                .exact_cut
                .covers_commit(&coverage.activation_commit)
            {
                replay.install_circle_bootstrap(
                    &transaction_records.verified_payload(coverage.bootstrap.image.image_hash)?,
                    coverage,
                    synced_tables,
                    routing_key,
                )?;
            }
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
        // A retained row is not automatically a replay input. The baseline
        // image already states the history its cut covers, so re-applying a
        // commit from under the cut would apply it twice — which is why a
        // dependency under the cut counts as settled below without being
        // applied. Rows under the cut are kept for the authority they carry,
        // read directly by the Circle and exclusion paths, not for replay.
        let active_references = retained
            .iter()
            .filter(|materialization| {
                !retracted.contains(materialization.commit_ref())
                    && !baseline
                        .exact_cut
                        .covers_commit(materialization.commit_ref())
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
                    && !baseline.exact_cut.covers_commit(&dependency)
                {
                    return Err(DbError::Message(format!(
                        "surviving retained Merge commit {:?} has unretained dependency {:?}",
                        materialization.commit_ref(),
                        dependency
                    )));
                }
            }
        }
        let mut active_accepted_writes = BTreeMap::new();
        for materialization in retained
            .iter()
            .filter(|materialization| active_references.contains(materialization.commit_ref()))
        {
            let write_id = materialization.commit().write_id.clone();
            if active_accepted_writes
                .insert(write_id.clone(), materialization.commit_ref().clone())
                .is_some()
            {
                return Err(DbError::Message(format!(
                    "accepted replay history contains more than one commit for write {write_id}"
                )));
            }
        }
        let retracted_writes = retained
            .iter()
            .filter(|materialization| retracted.contains(materialization.commit_ref()))
            .map(|materialization| materialization.commit().write_id.clone())
            .collect::<BTreeSet<_>>();
        let replay_journal = match journal {
            crate::ReplayJournal::Omit => transaction_records.merge_replay_associations(
                &baseline.exact_cut,
                &active_accepted_writes,
                &retracted_writes,
            )?,
            crate::ReplayJournal::Owed => transaction_records.merge_replay_journal(
                &baseline.exact_cut,
                &active_accepted_writes,
                &retracted_writes,
            )?,
            crate::ReplayJournal::Folded(folded) => {
                transaction_records.folded_replay_journal(folded)?
            }
        };
        let mut replay_journal = std::collections::VecDeque::from(replay_journal);
        let mut pending = retained
            .into_iter()
            .filter(|materialization| active_references.contains(materialization.commit_ref()))
            .map(|materialization| (materialization.commit_ref().clone(), materialization))
            .collect::<BTreeMap<_, _>>();
        let mut applied = BTreeSet::new();
        let mut applied_order = Vec::new();
        let mut max_updated_at = None;
        let mut watched_outcome = None;
        {
            let mut authority = ReplayVerifiedStoreLookup {
                cache: self,
                registrations,
                root,
            };
            drain_replay_journal(
                &replay,
                &mut authority,
                root,
                &schema,
                gates,
                &mut private_rows,
                &mut replay_journal,
                &applied,
                &baseline.exact_cut,
                false,
            )?;
        }
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
                        if deferred_bootstrap_circles.contains(circle_id) {
                            continue;
                        }
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
                    .map_err(DbError::from)?
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
                let associated_effect = match replay_journal.front() {
                    Some(crate::MergeReplayWrite::Accepted {
                        effect,
                        observed,
                        commit,
                    }) if commit == &reference => {
                        let predecessor_cut = materialization
                            .commit()
                            .order
                            .predecessor_cut()
                            .map_err(DbError::from)?
                            .frontier();
                        if !predecessor_cut.covers(observed) {
                            return Err(DbError::Message(format!(
                                "accepted Store commit {reference:?} does not cover write {} capture frontier",
                                effect.write_id
                            )));
                        }
                        Some(effect.clone())
                    }
                    _ => {
                        if replay_journal.iter().any(|write| {
                            matches!(
                                write,
                                crate::MergeReplayWrite::Accepted { commit, .. }
                                    if commit == &reference
                            )
                        }) {
                            return Err(DbError::Message(format!(
                                "accepted Store commit {reference:?} crosses an earlier retained local write"
                            )));
                        }
                        None
                    }
                };
                let consumed_associated_write = associated_effect.is_some();
                let applied_materialization = {
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
                        associated_effect,
                        schema.clone(),
                        &mut private_rows,
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
                let applied_max_updated_at = applied_materialization.max_updated_at.clone();
                match applied_materialization.outcome {
                    crate::MaterializationOutcome::Applied(_) => {
                        if let Some(applied_max) = &applied_max_updated_at {
                            if max_updated_at
                                .as_ref()
                                .is_none_or(|current| current < applied_max)
                            {
                                max_updated_at = Some(applied_max.clone());
                            }
                        }
                        if watched == Some(&reference) {
                            watched_outcome =
                                Some(crate::store::store_session::WatchedReplayOutcome::Applied {
                                    max_updated_at: applied_max_updated_at,
                                });
                        }
                        if consumed_associated_write {
                            replay_journal
                                .pop_front()
                                .expect("associated replay write remains at journal head");
                        }
                        pending.remove(&reference);
                        applied.insert(reference.clone());
                        applied_order.push(reference);
                        made_progress = true;
                        let reason = {
                            let mut authority = ReplayVerifiedStoreLookup {
                                cache: self,
                                registrations,
                                root,
                            };
                            drain_replay_journal(
                                &replay,
                                &mut authority,
                                root,
                                &schema,
                                gates,
                                &mut private_rows,
                                &mut replay_journal,
                                &applied,
                                &baseline.exact_cut,
                                false,
                            )?
                        };
                        if let Some(reason) = reason {
                            if watched.is_some() {
                                return Ok(
                                    crate::store::store_session::ReplayProjectionResult::new(
                                        replay,
                                        Some(
                                            crate::store::store_session::WatchedReplayOutcome::Held(
                                                reason,
                                            ),
                                        ),
                                        applied_order,
                                        max_updated_at,
                                    ),
                                );
                            }
                            return Err(DbError::Message(format!(
                                "retained local replay conflicts with accepted Store history: {reason:?}"
                            )));
                        }
                    }
                    crate::MaterializationOutcome::Held(
                        crate::MaterializationHold::ForeignKeyDependency,
                    ) => {
                        if watched == Some(&reference) {
                            watched_outcome =
                                Some(crate::store::store_session::WatchedReplayOutcome::Held(
                                    crate::MaterializationHold::ForeignKeyDependency,
                                ));
                        }
                    }
                    crate::MaterializationOutcome::Held(reason) => {
                        if watched.is_some() {
                            return Ok(crate::store::store_session::ReplayProjectionResult::new(
                                replay,
                                Some(crate::store::store_session::WatchedReplayOutcome::Held(
                                    reason,
                                )),
                                applied_order,
                                max_updated_at,
                            ));
                        }
                        return Err(DbError::Message(format!(
                            "retained Merge replay held accepted commit {reference:?}: {reason:?}"
                        )));
                    }
                }
            }
            if !made_progress {
                if watched.is_some() {
                    return Ok(crate::store::store_session::ReplayProjectionResult::new(
                        replay,
                        Some(crate::store::store_session::WatchedReplayOutcome::Held(
                            crate::MaterializationHold::ForeignKeyDependency,
                        )),
                        applied_order,
                        max_updated_at,
                    ));
                }
                return Err(DbError::Message(
                    "retained Merge replay has an unresolved foreign-key dependency".to_string(),
                ));
            }
        }
        let reason = {
            let mut authority = ReplayVerifiedStoreLookup {
                cache: self,
                registrations,
                root,
            };
            drain_replay_journal(
                &replay,
                &mut authority,
                root,
                &schema,
                gates,
                &mut private_rows,
                &mut replay_journal,
                &applied,
                &baseline.exact_cut,
                true,
            )?
        };
        if let Some(reason) = reason {
            if watched.is_some() {
                return Ok(crate::store::store_session::ReplayProjectionResult::new(
                    replay,
                    Some(crate::store::store_session::WatchedReplayOutcome::Held(
                        reason,
                    )),
                    applied_order,
                    max_updated_at,
                ));
            }
            return Err(DbError::Message(format!(
                "retained local replay conflicts with accepted Store history: {reason:?}"
            )));
        }
        if let Some(write) = replay_journal.front() {
            return Err(DbError::Message(format!(
                "retained local write {} cannot be placed in available Store history",
                write.write_id()
            )));
        }
        if watched.is_some() && watched_outcome.is_none() {
            return Err(DbError::Message(
                "watched Merge materialization was absent from retained replay".to_string(),
            ));
        }
        Ok(crate::store::store_session::ReplayProjectionResult::new(
            replay,
            watched_outcome,
            applied_order,
            max_updated_at,
        ))
    }
}

fn drain_replay_journal(
    replay: &ReplayProjection,
    authority: &mut dyn VerifiedStoreLookup,
    root: &StoreRootRef,
    schema: &std::sync::Arc<TableSchema>,
    gates: &crate::Gates,
    private_rows: &mut crate::store::store_session::merge_materialization_transaction::ReplayRows,
    journal: &mut std::collections::VecDeque<crate::MergeReplayWrite>,
    applied: &BTreeSet<StoreBatchCommitRef>,
    baseline: &CommitFrontier,
    include_unaccepted: bool,
) -> Result<Option<crate::MaterializationHold>, DbError> {
    loop {
        let effect = match journal.front() {
            Some(crate::MergeReplayWrite::Consumed { .. }) => {
                journal.pop_front();
                continue;
            }
            Some(crate::MergeReplayWrite::LocalOnly { effect, observed })
                if replay_frontier_is_settled(observed, applied, baseline) =>
            {
                Some(effect.clone())
            }
            Some(crate::MergeReplayWrite::Unaccepted { effect, observed })
                if include_unaccepted
                    && replay_frontier_is_settled(observed, applied, baseline) =>
            {
                Some(effect.clone())
            }
            Some(crate::MergeReplayWrite::Accepted { .. })
            | Some(crate::MergeReplayWrite::LocalOnly { .. })
            | Some(crate::MergeReplayWrite::Unaccepted { .. }) => None,
            None => return Ok(None),
        };
        let Some(effect) = effect else {
            return Ok(None);
        };
        if let Some(hold) = replay.apply_write_effect(
            authority,
            root,
            effect,
            schema.clone(),
            gates,
            private_rows,
        )? {
            return Ok(Some(hold));
        }
        journal.pop_front();
    }
}

fn replay_frontier_is_settled(
    observed: &CommitFrontier,
    applied: &BTreeSet<StoreBatchCommitRef>,
    baseline: &CommitFrontier,
) -> bool {
    observed
        .commits()
        .values()
        .all(|reference| replay_dependency_is_settled(reference, applied, baseline))
}

fn replay_dependency_is_settled(
    dependency: &StoreBatchCommitRef,
    applied: &BTreeSet<StoreBatchCommitRef>,
    baseline: &CommitFrontier,
) -> bool {
    applied.contains(dependency) || baseline.covers_commit(dependency)
}

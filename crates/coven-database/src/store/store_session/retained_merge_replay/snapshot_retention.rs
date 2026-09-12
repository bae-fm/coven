use super::*;
use crate::store::store_session::StoreRecords;

impl crate::store::store_session::StoreTransaction<'_, '_> {
    pub(crate) fn restore_snapshot_circle_access(
        self,
        authority: &mut crate::store::VerifiedStoreAuthority,
        root: &coven_protocol::store_commit::StoreRootRef,
        access: &[crate::StagedCircleAccess],
    ) -> Result<(), DbError> {
        let records = StoreRecords::new(self.transaction, self.store_dir);
        let materializations = authority.retained_replay_inputs_on(records, root)?;
        let baseline = authority.retained_replay_baseline_on(records)?.clone();
        let mut replacements = BTreeMap::new();
        for staged in access {
            let key = (
                staged.activating_commit.clone(),
                staged.activation.circle_id,
                staged.activation.control.coord.clone(),
            );
            if let Some(previous) = replacements.insert(key, staged) {
                if previous.activation != staged.activation
                    || previous.leaf_bootstrap != staged.leaf_bootstrap
                    || previous.local_exclusion != staged.local_exclusion
                {
                    return Err(DbError::Message(
                        "snapshot recipient access repeats conflicting context".to_string(),
                    ));
                }
            }
        }
        clear_retained_replay_index_on(self.transaction)?;
        self.transaction
            .execute("DELETE FROM circle_access_cache", [])
            .map_err(DbError::from)?;
        self.transaction
            .execute("DELETE FROM retained_merge_materializations", [])
            .map_err(DbError::from)?;
        // Cached canonical input hashes belong to the public image. Rebuild them
        // through the retained-input owner after adding this recipient's context.
        *authority = crate::store::VerifiedStoreAuthority::for_replay_baseline(baseline);
        for materialization in materializations {
            let original = materialization.circle_activations();
            let mut circles = original.circles().to_vec();
            let mut bootstraps = original.bootstraps().to_vec();
            for circle in &mut circles {
                let key = (
                    materialization.commit_ref().clone(),
                    circle.circle_id,
                    circle.control.coord.clone(),
                );
                if let Some(staged) = replacements.remove(&key) {
                    if circle.reference != staged.activation.reference
                        || circle.control != staged.activation.control
                    {
                        return Err(DbError::Message(
                            "snapshot recipient context changes its accepted Circle control"
                                .to_string(),
                        ));
                    }
                    *circle = staged.activation.clone();
                    bootstraps.retain(|image| {
                        image.circle_id() != circle.circle_id
                            || image.control() != &circle.control.coord
                    });
                    bootstraps.extend(staged.leaf_bootstrap.iter().cloned());
                }
            }
            let circles =
                coven_protocol::circle_activation::VerifiedCircleActivations::from_verified_parts(
                    circles,
                    original.stream_activations().clone(),
                    bootstraps,
                    original.local_exclusions().to_vec(),
                    original.bootstrap_pending_exclusions().to_vec(),
                );
            if circles.clone().without_local_access() != original.clone().without_local_access() {
                return Err(DbError::Message(
                    "snapshot recipient context changes public activation evidence".to_string(),
                ));
            }
            let restored = crate::VerifiedMergeMaterialization::verify(
                root,
                materialization.verified_commit(),
                materialization.registrations(),
                materialization.device_operations(),
                &circles,
                materialization.acceptance(),
                materialization.history_evidence(),
                materialization.membership_objects(),
                materialization.packages(),
                materialization.package_application(),
            )?;
            self.retain_merge_materialization(authority, root, &restored)?;
            for activation in circles.circles() {
                self.record_circle_access(activation)?;
                let current = crate::store::circle_operations::circle_current_state_on(
                    self.transaction,
                    activation.circle_id,
                )?
                .ok_or_else(|| {
                    DbError::Message("snapshot Circle activation has no current state".to_string())
                })?;
                if current
                    .resolved_control()
                    .is_none_or(|current| current.coordinate() != &activation.control.coord)
                {
                    tracing::debug!(circle_id = %activation.circle_id, control = ?activation.control.coord,
                        "retain historical Circle access without changing the current control");
                    continue;
                }
                for exclusion in access
                    .iter()
                    .filter_map(|staged| staged.local_exclusion.as_ref())
                {
                    if exclusion.circle_id == activation.circle_id
                        && exclusion.successor_control == activation.control.coord
                    {
                        if &exclusion.activating_commit != materialization.commit_ref() {
                            return Err(DbError::Message(
                                "snapshot Circle exclusion names another activating commit"
                                    .to_string(),
                            ));
                        }
                        crate::store::circle_operations::record_circle_close_exclusion_on(
                            self.transaction,
                            exclusion,
                        )?;
                    }
                }
                let restored =
                    coven_protocol::circle_activation::CircleCurrentState::from_verified(
                        materialization.commit().candidate_family(),
                        activation,
                    )?;
                if current.without_local_access() != restored.clone().without_local_access() {
                    return Err(DbError::Message(
                        "snapshot recipient context changes current Circle authority".to_string(),
                    ));
                }
                self.transaction.execute(
                    "UPDATE circle_current_state SET state = ?2 WHERE circle_id = ?1",
                    rusqlite::params![
                        activation.circle_id.to_string(),
                        serde_json::to_vec(&restored)?
                    ],
                )?;
            }
        }
        if !replacements.is_empty() {
            return Err(DbError::Message(
                "snapshot recipient context names an absent retained activation".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn project_shared_snapshot_replay_inputs(
        self,
        authority: &mut crate::store::VerifiedStoreAuthority,
        root: &coven_protocol::store_commit::StoreRootRef,
    ) -> Result<(), DbError> {
        let records = StoreRecords::new(self.transaction, self.store_dir);
        let materializations = authority.retained_replay_inputs_on(records, root)?;
        let baseline = authority.retained_replay_baseline_on(records)?.clone();
        // This transaction belongs to the unpublished image copy. Replace its
        // replay ownership with the public projection without touching the
        // publisher's payload claims or private replay baseline.
        clear_retained_replay_index_on(self.transaction)?;
        self.transaction
            .execute_batch(
                "DELETE FROM retained_merge_materializations;
                 DELETE FROM circle_bootstrap_coverage;",
            )
            .map_err(DbError::from)?;
        crate::store::circle_operations::remove_local_circle_access_on(self.transaction)?;
        // The rewritten inputs have different canonical hashes and no recipient
        // context. Keep the baseline authority, but discard the input cache.
        *authority = crate::store::VerifiedStoreAuthority::for_replay_baseline(baseline);
        for materialization in materializations {
            let circles = materialization
                .circle_activations()
                .clone()
                .without_local_access();
            let packages = materialization
                .packages()
                .iter()
                .filter(|package| {
                    matches!(
                        package.audience(),
                        coven_protocol::audience_package::PackageAudience::Store
                    )
                })
                .cloned()
                .collect::<Vec<_>>();
            let application = if packages.is_empty() {
                None
            } else {
                materialization.package_application()
            };
            let projected = crate::VerifiedMergeMaterialization::verify(
                root,
                materialization.verified_commit(),
                materialization.registrations(),
                materialization.device_operations(),
                &circles,
                materialization.acceptance(),
                materialization.history_evidence(),
                materialization.membership_objects(),
                &packages,
                application,
            )?;
            self.retain_merge_materialization(authority, root, &projected)?;
        }
        Ok(())
    }
}

impl StoreDatabase {
    /// The `retained_merge_materializations` commit-refs a Store snapshot image
    /// keeps: device-exclusion activation commits (retained authority controls),
    /// Circle bootstrap-coverage activation commits, the activation commit
    /// behind every Circle control the database still indexes, and every
    /// retained materialization that still carries a Circle package no
    /// bootstrap cut covers. `StoreTransaction::retain_snapshot_replay_inputs`
    /// keeps exactly this set, `validate_snapshot_retained_inputs_on` expects
    /// exactly it, and advancing a replay baseline retires everything at or
    /// under the new cut that is not in it — so all three share this one
    /// derivation.
    pub(crate) fn snapshot_required_retained_refs(
        records: StoreRecords<'_>,
        authority: &mut dyn VerifiedStoreLookup,
        root: &coven_protocol::store_commit::StoreRootRef,
        coverage: &CommitFrontier,
    ) -> Result<BTreeSet<String>, DbError> {
        let rows = records.snapshot_retention_rows()?;
        let mut required = BTreeSet::new();
        for references in authority
            .pending_device_join_retention_on(records, root, coverage)?
            .into_values()
        {
            for reference in references {
                required.insert(serde_json::to_string(&reference)?);
            }
        }
        for (encoded_exclusion, encoded_commit) in rows.exclusion_activations {
            let exclusion = serde_json::from_str(&encoded_exclusion)
                .map_err(|error| DbError::context("snapshot author exclusion reference", error))?;
            let activation =
                load_device_exclusion_activation_on(records, authority, root, &exclusion)?;
            let exact_commit = serde_json::to_string(&activation).map_err(|error| {
                DbError::context("serialize snapshot author exclusion commit", error)
            })?;
            if exact_commit != encoded_commit {
                return Err(DbError::Message(
                    "snapshot author exclusion commit changed during verification".to_string(),
                ));
            }
            required.insert(serde_json::to_string(&activation).map_err(|error| {
                DbError::context("serialize snapshot author exclusion activation", error)
            })?);
        }
        // `circle_control_activations` is rebuilt by replay from the commits it
        // applies, so every control it names is a claim that the commit which
        // activated it is still replayable. Building the Circle replay epoch
        // index resolves all of them, and refuses a control whose activation
        // was dropped. Keeping those commits is what makes that claim true; a
        // control whose activation is already gone is a superseded epoch and
        // stays gone.
        required.extend(records.circle_control_activation_refs()?);
        let mut bootstrap_cuts = BTreeMap::new();
        for (circle_id, activation_commit, exact_cut) in rows.circle_bootstraps {
            let circle_id: coven_protocol::circle::CircleId = circle_id
                .parse()
                .map_err(|error| DbError::context("snapshot Circle bootstrap id", error))?;
            let cut: CommitFrontier = serde_json::from_str(&exact_cut)
                .map_err(|error| DbError::context("snapshot Circle bootstrap coverage", error))?;
            if bootstrap_cuts.insert(circle_id, cut).is_some() {
                return Err(DbError::Message(
                    "snapshot has duplicate Circle bootstrap coverage".to_string(),
                ));
            }
            required.insert(activation_commit);
        }
        for encoded in rows.materialization_refs {
            let reference: StoreBatchCommitRef = serde_json::from_str(&encoded)
                .map_err(|error| DbError::context("snapshot retained Circle commit", error))?;
            let materialization =
                authority.retained_materialization_by_ref_on(records, &reference)?;
            if materialization.root() != root {
                return Err(DbError::Message(
                    "snapshot retained materialization belongs to another Store root".to_string(),
                ));
            }
            let has_uncovered_circle_package = materialization.packages().iter().any(|package| {
                let coven_protocol::audience_package::PackageAudience::Circle { circle_id, .. } =
                    package.audience()
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

    pub(crate) fn validate_snapshot_retained_inputs_on(
        records: StoreRecords<'_>,
        authority: &mut dyn VerifiedStoreLookup,
        root: &coven_protocol::store_commit::StoreRootRef,
        coverage: &CommitFrontier,
    ) -> Result<(), DbError> {
        // The image's retained inputs must be exactly the set the retention rule
        // keeps — an extra row is unjustified replay baseline, a missing one is
        // coverage the Circle retained replay needs.
        let expected = Self::snapshot_required_retained_refs(records, authority, root, coverage)?;
        let actual = records.retained_materialization_refs()?;
        if actual != expected {
            return Err(DbError::Message(
                "snapshot retained inputs differ from the retention rule".to_string(),
            ));
        }
        Ok(())
    }
}

use super::*;
use crate::store::store_session::{StoreRecords, StoreTransaction};
use coven_protocol::audience_package::PackageAudience;
use coven_protocol::reclaim::AudienceBlobBindingPackage;
use std::collections::BTreeMap;

impl StoreTransaction<'_, '_> {
    pub(crate) fn install_snapshot_circle_packages(
        self,
        root: &coven_protocol::store_commit::StoreRootRef,
        selected: &crate::StagedCircleRestore,
        authority: &mut crate::store::VerifiedStoreAuthority,
        tables: &[SyncedTable],
        receiver_wall_ms: u64,
    ) -> Result<(), DbError> {
        let records = StoreRecords::new(self.transaction, self.store_dir);
        let baseline = authority.retained_replay_baseline_on(records)?.clone();
        let crate::RetainedReplayAuthority::InstalledSnapshot(snapshot) = &baseline.authority
        else {
            return Err(DbError::Message(
                "Circle package restoration requires snapshot authority".into(),
            ));
        };
        if &snapshot.store_root != root {
            return Err(DbError::Message(
                "Circle package restoration belongs to another Store".into(),
            ));
        }
        let cuts = selected.coverage_cuts()?;
        authority.retained_replay_inputs_on(records, root)?;
        let epochs = authority.circle_replay_epoch_index_on(records)?;
        let expected = StoreDatabase::snapshot_circle_packages_after(snapshot, &epochs, &cuts)?;
        let Some(staged) = &selected.packages else {
            if !expected.is_empty() {
                return Err(DbError::Message(
                    "snapshot Circle restoration omits accepted packages".into(),
                ));
            }
            return Ok(());
        };
        let mut expected = expected
            .into_iter()
            .map(|retained| (retained.package.object().clone(), retained))
            .collect::<BTreeMap<_, _>>();
        let mut verified = BTreeMap::new();
        for (commit_ref, packages) in &staged.packages {
            if packages.is_empty() {
                return Err(DbError::Message(
                    "snapshot Circle restoration contains an empty commit".into(),
                ));
            }
            let mut commit_packages = Vec::new();
            let mut exact_commit = None;
            for package in packages {
                let PackageAudience::Circle { circle_id, .. } = package.audience() else {
                    return Err(DbError::Message(
                        "snapshot Circle restoration contains a Store package".into(),
                    ));
                };
                let entry = expected.values().find(|entry| {
                    &entry.activation == commit_ref
                        && matches!(&entry.package, AudienceBlobBindingPackage::Circle(reference) if reference.circle_id == *circle_id)
                }).cloned().ok_or_else(|| DbError::Message(
                    "Circle package is absent from the authenticated snapshot restoration cut".into()
                ))?;
                expected.remove(entry.package.object());
                let retained = crate::RetainedAudiencePackage::verify(
                    &entry.commit,
                    commit_ref,
                    package.clone(),
                )?;
                exact_commit = Some(entry.commit);
                commit_packages.push(retained);
            }
            verified.insert(
                commit_ref.clone(),
                (
                    exact_commit.ok_or_else(|| {
                        DbError::Message(
                            "snapshot Circle restoration contains no verified commit".into(),
                        )
                    })?,
                    commit_packages,
                ),
            );
        }
        if !expected.is_empty() {
            return Err(DbError::Message(
                "snapshot Circle restoration omits accepted packages".into(),
            ));
        }
        let gates = crate::Gates::from_tables(self.transaction, tables)?;
        let blobs = crate::BlobDecls::from_tables(self.transaction, tables)?;
        let schema = std::sync::Arc::new(crate::TableSchema::for_apply(
            self.transaction,
            tables,
            &gates,
        )?);
        let materialization = MergeMaterializationTransaction::from_store(self);
        while !verified.is_empty() {
            let next = verified
                .iter()
                .find(|(_, (commit, _))| {
                    commit
                        .order
                        .predecessor()
                        .into_iter()
                        .chain(commit.order.dependencies().values())
                        .all(|dependency| {
                            !verified.keys().any(|needed| {
                                needed.coord.stream_id == dependency.coord.stream_id
                                    && needed.coord.sequence() <= dependency.coord.sequence()
                            })
                        })
                })
                .map(|(reference, _)| reference.clone())
                .ok_or_else(|| {
                    DbError::Message("snapshot Circle package dependency cycle".into())
                })?;
            let (_, packages) = verified.remove(&next).ok_or_else(|| {
                DbError::Message("snapshot Circle package ordering lost its selected commit".into())
            })?;
            for retained in packages {
                let package = retained.package();
                let PackageAudience::Circle { circle_id, .. } = package.audience() else {
                    return Err(DbError::Message(
                        "verified Circle package changed audience".into(),
                    ));
                };
                materialization.record_package_activation(&next, &retained)?;
                let source =
                    crate::ValidatedChangeset::new(package.changeset().to_vec(), schema.clone())?;
                let rows = crate::gate::filter_snapshot_circle_changeset(
                    self.transaction,
                    package.changeset(),
                    *circle_id,
                    &gates,
                    &staged.routing_key,
                )?;
                let winning = match materialization.apply_merge_subset(
                    &blobs,
                    &gates,
                    Some(&staged.routing_key),
                    &source,
                    rows,
                    Some(&coven_protocol::circle::Audience::Circle(*circle_id)),
                    crate::IncomingTimestampPolicy::Received { receiver_wall_ms },
                    &mut None,
                    &mut Vec::new(),
                )? {
                    MergeSubsetOutcome::Applied(winning) => winning,
                    MergeSubsetOutcome::ConstraintConflict(tables) => {
                        return Err(DbError::Message(format!(
                            "snapshot Circle package has constraint conflicts: {tables:?}"
                        )));
                    }
                    MergeSubsetOutcome::Held(hold) => {
                        return Err(DbError::Message(format!(
                            "snapshot Circle package cannot be installed: {hold:?}"
                        )));
                    }
                };
                materialization.install_winning_blob_bindings(
                    &gates,
                    tables,
                    package,
                    &crate::BlobActivation {
                        coord: next.coord.clone(),
                    },
                    &winning,
                )?;
            }
        }
        crate::validate_scoped_foreign_key_audiences(self.transaction, &gates)?;
        if materialization.has_foreign_key_violations()? {
            return Err(DbError::Message(
                "snapshot Circle packages have incomplete foreign keys".into(),
            ));
        }
        Ok(())
    }
}

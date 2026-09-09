use super::*;
use coven_protocol::audience_package::AudiencePackage;
use coven_protocol::reclaim::AudienceBlobBindingPackage;
use coven_protocol::store_commit::{CirclePackageRef, StoreBatchCommit, StoreBatchCommitRef};
use std::collections::BTreeMap;

impl CircleSnapshotReader<'_, '_> {
    pub(super) async fn select_packages_after_bases(
        &mut self,
        restore: &coven_database::StagedCircleRestore,
        routing_key: Option<&coven_protocol::circle::RowRoutingKey>,
        local_membership: coven_protocol::membership::LocalStoreMembership,
    ) -> Result<Option<coven_database::StagedCirclePackageRestore>, SnapshotError> {
        let cuts = restore.coverage_cuts()?;
        let inputs = self.database.circle_snapshot_package_inputs(cuts).await?;
        if inputs.is_empty() {
            return Ok(None);
        }
        let routing_key = routing_key.cloned().ok_or_else(|| {
            SnapshotError::BootstrapState(
                "snapshot Circle packages require the receiving Store routing key".into(),
            )
        })?;
        let mut commits =
            BTreeMap::<StoreBatchCommitRef, (StoreBatchCommit, Vec<CirclePackageRef>)>::new();
        for input in inputs {
            let AudienceBlobBindingPackage::Circle(reference) = input.package else {
                return Err(SnapshotError::BootstrapState(
                    "Circle restoration selected a Store package".into(),
                ));
            };
            let entry = commits
                .entry(input.activation)
                .or_insert_with(|| (input.commit, Vec::new()));
            entry.1.push(reference);
        }
        let mut packages = BTreeMap::new();
        for (reference, (commit, wanted)) in commits {
            let verified = self
                .history
                .authenticate_bytes(&reference, &commit.to_bytes())
                .await?;
            let author = verified.author().clone();
            let loaded = crate::sync::store::circles::packages::CirclePackageReader::new(
                self.database,
                self.storage,
                self.history,
            )
            .load_selected(
                &verified,
                &wanted,
                &[],
                &restore.access,
                &author,
                local_membership,
            )
            .await
            .map_err(crate::sync::store::pull::StorePullError::from)?;
            if loaded.len() != wanted.len() {
                return Err(SnapshotError::BootstrapState(
                    "snapshot Circle package restoration lacks access to its accepted suffix"
                        .into(),
                ));
            }
            let mut decoded = Vec::new();
            for loaded in loaded {
                let package = AudiencePackage::parse(&loaded.bytes)?;
                coven_database::RetainedAudiencePackage::verify(
                    &commit,
                    &reference,
                    package.clone(),
                )?;
                decoded.push(package);
            }
            packages.insert(reference, decoded);
        }
        Ok(Some(coven_database::StagedCirclePackageRestore {
            routing_key,
            packages,
        }))
    }
}

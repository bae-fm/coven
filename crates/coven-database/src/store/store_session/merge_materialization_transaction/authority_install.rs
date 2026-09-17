use super::*;

impl MergeMaterializationTransaction<'_, '_> {
    pub(crate) fn record_prepared_materialization_authority(
        &self,
        materialization: &PreparedMergeMaterialization,
    ) -> Result<(), DbError> {
        let packages = materialization
            .packages
            .iter()
            .map(|prepared| prepared.package.clone())
            .collect::<Vec<_>>();
        let verified = VerifiedMergeMaterialization::verify(
            &materialization.root,
            &materialization.verified_commit,
            &materialization.registrations,
            &materialization.device_operations,
            &materialization.circle_activations,
            &materialization.acceptance,
            &materialization.history_evidence,
            materialization.membership_objects.as_ref(),
            &packages,
            materialization.package_application,
        )?;
        self.record_verified_materialization_authority(
            &verified,
            &materialization.membership_remote_objects,
        )
    }

    pub(crate) fn record_verified_materialization_authority(
        &self,
        materialization: &VerifiedMergeMaterialization<'_>,
        membership_remote_objects: &[coven_protocol::remote_object::ClosedRemoteObject],
    ) -> Result<(), DbError> {
        let conn = self.store.transaction;
        let commit = materialization.commit();
        let commit_ref = materialization.commit_ref();
        crate::store::record_activated_store_device_registrations_on(
            conn,
            commit,
            materialization.registrations(),
        )?;
        for bootstrap in materialization.circle_activations().bootstraps() {
            crate::install_circle_bootstrap_remote_objects_on(conn, commit_ref, bootstrap)?;
        }
        self.record_verified_circle_activations(
            materialization.verified_commit(),
            materialization.circle_activations().circles(),
        )?;
        for package in materialization.packages() {
            let retained =
                crate::RetainedAudiencePackage::verify(commit, commit_ref, package.clone())?;
            self.record_package_activation(commit_ref, &retained)?;
        }
        crate::install_pulled_merge_membership_activations_on(
            conn,
            self.store.store_dir,
            commit_ref,
            membership_remote_objects,
        )
    }

    pub(crate) fn record_package_activation(
        &self,
        commit: &StoreBatchCommitRef,
        retained: &crate::RetainedAudiencePackage,
    ) -> Result<(), DbError> {
        crate::install_pulled_package_activation_on(
            self.store.transaction,
            self.store.store_dir,
            commit,
            retained.domain(),
            retained.object(),
            retained.package(),
        )?;
        Database::install_pulled_blob_activations_on(
            self.store.transaction,
            retained.package(),
            commit,
        )
    }

    pub(crate) fn retain_prepared_merge_materialization(
        &self,
        registrations_lookup: &mut dyn VerifiedStoreLookup,
        materialization: &PreparedMergeMaterialization,
    ) -> Result<OwnedVerifiedMergeMaterialization, DbError> {
        let retained_packages = materialization
            .packages
            .iter()
            .map(|prepared| prepared.package.clone())
            .collect::<Vec<_>>();
        let verified = VerifiedMergeMaterialization::verify(
            &materialization.root,
            &materialization.verified_commit,
            &materialization.registrations,
            &materialization.device_operations,
            &materialization.circle_activations,
            &materialization.acceptance,
            &materialization.history_evidence,
            materialization.membership_objects.as_ref(),
            &retained_packages,
            materialization.package_application,
        )?;
        self.record_verified_merge_materialization(registrations_lookup, verified)
    }
}

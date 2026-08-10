use super::{StoreRecordTransaction, StoreRecords};
use crate::{
    install_snapshot_replay_baseline_on, install_store_founder_state_on,
    install_store_root_authority_on, validate_snapshot_object_owners_on, CircleRestoreSelection,
    Database, DbError, ResolvedStoreDeviceState, StoreDatabase, StoreDeviceRegistrationRef,
    SyncedTable, VerifiedSnapshotBootstrapInstall,
};

impl StoreRecordTransaction<'_, '_> {
    pub(crate) fn install_verified_snapshot_bootstrap(
        self,
        install: &VerifiedSnapshotBootstrapInstall,
        schema_version: u32,
        routing_hash: crate::ObjectHash,
        synced_tables: &[SyncedTable],
    ) -> Result<(), DbError> {
        let conn = self.transaction;
        let root = coven_protocol::store_commit::StoreRootRef {
            store_root_id: install.store_root.value.descriptor.store_root_id(),
            store_root_hash: install.store_root.semantic_hash,
            object: install.store_root.object.clone(),
        };
        let founder_reference = StoreDeviceRegistrationRef::from_registration(
            &install.founder.value,
            install.founder.object.clone(),
        );
        let genesis = ResolvedStoreDeviceState::founder(
            &root,
            founder_reference.clone(),
            &install.store_root.value.descriptor.founder_pubkey,
            install.store_root.value.descriptor.founder_grant.clone(),
            &install.store_root.value.descriptor.founder_recovery,
        )
        .map_err(|error| DbError::Message(error.to_string()))?;
        validate_snapshot_object_owners_on(conn, &root, &install.snapshot.meta)?;
        install_store_root_authority_on(conn, &root, &install.store_root.bytes)?;
        install_store_founder_state_on(
            conn,
            &root,
            &founder_reference,
            &install.founder.value,
            &install.founder.bytes,
            &genesis,
        )?;
        crate::set_protocol_state_on(
            conn,
            coven_protocol::membership::OWNER_PUBKEY_STATE_KEY,
            &install.store_root.value.descriptor.founder_pubkey,
        )?;
        install.membership.install_on(conn)?;
        conn.execute("DELETE FROM snapshot_coverage", [])
            .map_err(DbError::from)?;
        for (stream_id, reference) in install.snapshot.meta.coverage.clone().into_refs() {
            let encoded = serde_json::to_string(&reference)
                .map_err(|error| DbError::context("serialize snapshot exact commit ref", error))?;
            conn.execute(
                "INSERT INTO snapshot_coverage
                 (device_id, seq, commit_ref, snapshot_hash) VALUES (?1, ?2, ?3, ?4)",
                (
                    &stream_id,
                    Database::sequence_to_sqlite(&stream_id, reference.coord.sequence())?,
                    encoded,
                    install.snapshot.reference.snapshot_hash.to_string(),
                ),
            )
            .map_err(DbError::from)?;
        }
        install_snapshot_replay_baseline_on(
            StoreRecords::new(self.transaction, self.store_dir),
            schema_version,
            routing_hash,
            install.stability.clone(),
        )?;
        self.install_selected_snapshot_circles(install, &root, synced_tables)
    }

    fn install_selected_snapshot_circles(
        self,
        install: &VerifiedSnapshotBootstrapInstall,
        root: &coven_protocol::store_commit::StoreRootRef,
        synced_tables: &[SyncedTable],
    ) -> Result<(), DbError> {
        let CircleRestoreSelection::Selected(circle_installs) = &install.circle_selection else {
            return Ok(());
        };
        #[cfg(any(test, feature = "test-utils"))]
        if install.fail_circle_install {
            return Err(DbError::Message(
                "injected Circle install failure after Store install".to_string(),
            ));
        }
        self.clear_imported_circle_bootstrap_coverage()?;
        let mut verified_authority = crate::store::VerifiedStoreAuthority::default();
        for selected in circle_installs {
            let activation = StoreDatabase::verified_circle_activation_on(
                StoreRecords::new(self.transaction, self.store_dir),
                &mut verified_authority,
                root,
                selected.image.circle_id(),
                selected.image.control(),
            )?
            .ok_or_else(|| {
                DbError::Message(format!(
                    "restored Circle {} image names a control absent from the installed control indexes",
                    selected.image.circle_id()
                ))
            })?;
            crate::install_circle_bootstrap_image_on(
                self.transaction,
                synced_tables,
                &selected.activation_commit,
                &selected.image,
            )?;
            self.record_one_circle_bootstrap_coverage(
                &mut verified_authority,
                root,
                &selected.activation_commit,
                &selected.image,
                &activation.control,
            )?;
        }
        Ok(())
    }
}

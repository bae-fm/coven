use super::{StoreRecords, StoreTransaction};
use crate::store::retained_replay::install_snapshot_replay_baseline_on;
use crate::{
    install_store_founder_state_on, install_store_root_authority_on,
    validate_snapshot_object_owners_on, CircleRestoreSelection, Database, DbError,
    ResolvedStoreDeviceState, StoreDatabase, StoreDeviceRegistrationRef, SyncedTable,
    VerifiedSnapshotBootstrapInstall,
};

#[cfg(any(test, feature = "test-utils"))]
impl StoreRecords<'_> {
    pub(crate) fn capture_snapshot(
        self,
        image: crate::SnapshotDatabaseImage,
        root: &coven_protocol::store_commit::StoreRootRef,
        tables: &[SyncedTable],
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
        audience: &coven_protocol::circle::Audience,
    ) -> Result<crate::CreatedSnapshot, crate::SnapshotImageError> {
        image.capture_on(
            self.conn,
            self.store_dir,
            crate::store::VerifiedStoreAuthority::default(),
            root,
            tables,
            routing_encryption,
            audience,
        )
    }
}

impl StoreTransaction<'_, '_> {
    pub(crate) fn record_circle_access(
        self,
        activation: &coven_protocol::circle_activation::VerifiedCircleReference,
    ) -> Result<(), DbError> {
        let conn = self.transaction;
        let circle_id = activation.circle_id.to_string();
        let control_coord = serde_json::to_string(&activation.control.coord)?;
        if let Some(access) = &activation.local_access {
            let disposition = match access.leaf.value.disposition {
                coven_protocol::circle::CircleAccessDisposition::Active { .. } => "active",
                coven_protocol::circle::CircleAccessDisposition::Inactive => "inactive",
            };
            conn.execute(
                "INSERT INTO circle_access_cache
             (circle_id, control_coord, owner_pubkey, disposition)
             VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![
                    &circle_id,
                    &control_coord,
                    &access.leaf.value.owner_pubkey,
                    disposition,
                ],
            )
            .map_err(DbError::from)?;
        }
        Ok(())
    }

    pub(crate) fn install_verified_snapshot_bootstrap(
        self,
        install: &VerifiedSnapshotBootstrapInstall,
        schema_version: u32,
        routing_hash: crate::ObjectHash,
        synced_tables: &[SyncedTable],
        receiver_wall_ms: u64,
    ) -> Result<(), DbError> {
        // Live, this one call is the bulk of a joining device's snapshot
        // install, and none of its steps is obviously the one: the checks are
        // local and small, the coverage rows number one per stream, and the
        // baseline capture reports its own breakdown below. They are named so
        // the next run says which.
        let mut timings =
            coven_foundation::stage_timing::StageTimings::start("Store snapshot install");
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
        .map_err(DbError::from)?;
        timings.mark("validate the snapshot owners", || {
            validate_snapshot_object_owners_on(
                conn,
                &root,
                &install.snapshot.reference,
                &install.snapshot.meta,
            )
        })?;
        timings.mark("install the root and founder", || {
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
            )
        })?;
        timings.mark("install the membership", || {
            install.membership.install_on(conn)
        })?;
        let installed_publication =
            crate::store::store_session::observed_store_publication::load_store_current_publication_on(
                conn,
            )?;
        if installed_publication.record() != &install.snapshot.meta.publication_predecessor {
            return Err(DbError::Message(
                "snapshot image Store publication boundary differs from its signed metadata"
                    .to_string(),
            ));
        }
        let coverage_started = coven_foundation::clock::Stopwatch::start();
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
        timings.record("record the coverage", coverage_started.elapsed(), 0);
        let blob_decls =
            crate::BlobDecls::from_tables(conn, synced_tables).map_err(DbError::from)?;
        let circles_started = coven_foundation::clock::Stopwatch::start();
        self.install_selected_snapshot_circles(
            install,
            &root,
            schema_version,
            routing_hash,
            synced_tables,
            &blob_decls,
            receiver_wall_ms,
        )?;
        timings.record("install the circles", circles_started.elapsed(), 0);
        let baseline_started = coven_foundation::clock::Stopwatch::start();
        install_snapshot_replay_baseline_on(
            crate::store::store_session::StoreRecords::new(self.transaction, self.store_dir),
            schema_version,
            routing_hash,
            install.authority.clone(),
            &blob_decls,
        )?;
        timings.record("capture the replay baseline", baseline_started.elapsed(), 0);
        timings.report();
        Ok(())
    }

    fn install_selected_snapshot_circles(
        self,
        install: &VerifiedSnapshotBootstrapInstall,
        root: &coven_protocol::store_commit::StoreRootRef,
        schema_version: u32,
        routing_hash: crate::ObjectHash,
        synced_tables: &[SyncedTable],
        blob_decls: &crate::BlobDecls,
        receiver_wall_ms: u64,
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
        let prepared = super::retained_replay::PreparedRetainedReplayBaseline::new(
            install.authority.metadata.coverage.clone(),
            schema_version,
            routing_hash,
            crate::RetainedReplayAuthority::InstalledSnapshot(install.authority.clone()),
            crate::connection_io::serialize_database_image(self.transaction)?,
        );
        let mut verified_authority = prepared.verify_authority(self.store_dir, blob_decls)?;
        self.install_snapshot_circle_restore(
            root,
            circle_installs,
            &mut verified_authority,
            synced_tables,
            receiver_wall_ms,
        )
        .map(|_| ())
    }

    pub(crate) fn restore_device_join_snapshot_circles(
        self,
        root: &coven_protocol::store_commit::StoreRootRef,
        selection: &crate::StagedCircleRestore,
        synced_tables: &[SyncedTable],
        blob_decls: &crate::BlobDecls,
        receiver_wall_ms: u64,
    ) -> Result<Option<coven_protocol::hlc::Timestamp>, DbError> {
        let baseline = self.load_replay_baseline()?;
        if !matches!(&baseline.authority, crate::RetainedReplayAuthority::InstalledSnapshot(authority) if &authority.store_root == root)
        {
            return Err(DbError::Message(
                "recipient Circle restoration requires the installed Store snapshot".to_string(),
            ));
        }
        let has_tail: bool = self.transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM materialized_commits)",
            [],
            |row| row.get(0),
        )?;
        if has_tail {
            return Err(DbError::Message(
                "recipient Circle restoration must precede snapshot tail installation".to_string(),
            ));
        }
        let mut authority =
            crate::store::VerifiedStoreAuthority::for_replay_baseline(baseline.clone());
        let floor = self.install_snapshot_circle_restore(
            root,
            selection,
            &mut authority,
            synced_tables,
            receiver_wall_ms,
        )?;
        StoreRecords::new(self.transaction, self.store_dir).replace_retained_replay_image(
            &baseline,
            baseline.schema_version,
            &crate::connection_io::serialize_database_image(self.transaction)?,
            blob_decls,
        )?;
        Ok(floor)
    }

    pub(crate) fn install_snapshot_circle_restore(
        self,
        root: &coven_protocol::store_commit::StoreRootRef,
        circle_installs: &crate::StagedCircleRestore,
        verified_authority: &mut crate::store::VerifiedStoreAuthority,
        synced_tables: &[SyncedTable],
        receiver_wall_ms: u64,
    ) -> Result<Option<coven_protocol::hlc::Timestamp>, DbError> {
        self.restore_snapshot_circle_access(verified_authority, root, &circle_installs.access)?;
        let mut floor = None;
        super::clock_floor::observe_circle_metadata(
            &mut floor,
            circle_installs
                .access
                .iter()
                .map(|access| &access.activation),
            crate::IncomingTimestampPolicy::Received { receiver_wall_ms },
        )?;
        self.clear_imported_circle_bootstrap_coverage()?;
        circle_installs.coverage_cuts()?;
        for base in &circle_installs.bases {
            let selected = match base {
                crate::StagedCircleBase::Image(selected) => selected,
                crate::StagedCircleBase::Founder { circle_id, control } => {
                    let current = crate::store::circle_operations::circle_current_state_on(
                        self.transaction,
                        *circle_id,
                    )?
                    .ok_or_else(|| {
                        DbError::Message(
                            "Circle founder restore base has no installed current control".into(),
                        )
                    })?;
                    let current = current.authoring_state().ok_or_else(|| {
                        DbError::Message(
                            "Circle founder restore base requires current active recipient access"
                                .into(),
                        )
                    })?;
                    let founding = StoreDatabase::verified_circle_activation_on(
                        StoreRecords::new(self.transaction, self.store_dir),
                        verified_authority,
                        root,
                        *circle_id,
                        control,
                    )?
                    .ok_or_else(|| {
                        DbError::Message(
                            "Circle founder restore base has no retained founding control".into(),
                        )
                    })?;
                    if current.control.value.epoch_id() != founding.control.value.epoch_id()
                        || !StoreDatabase::verified_circle_control_covers_on(
                            StoreRecords::new(self.transaction, self.store_dir),
                            verified_authority,
                            root,
                            *circle_id,
                            &current.control,
                            control,
                        )?
                    {
                        return Err(DbError::Message(
                            "Circle founder restore base differs from the installed current epoch lineage".into(),
                        ));
                    }
                    continue;
                }
            };
            let activation = StoreDatabase::verified_circle_activation_on(
                crate::store::store_session::StoreRecords::new(self.transaction, self.store_dir),
                verified_authority,
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
                verified_authority,
                root,
                &selected.activation_commit,
                &selected.image,
                &activation.control,
            )?;
        }
        self.install_snapshot_circle_packages(
            root,
            circle_installs,
            verified_authority,
            synced_tables,
            receiver_wall_ms,
        )?;
        // Circle images arrive after the Store database's open-time clock seed.
        // Their row registers must join the same atomic floor as their metadata.
        let row_floor = crate::connection_io::scan_max_updated_at(
            self.transaction,
            synced_tables,
            receiver_wall_ms.saturating_add(coven_protocol::hlc::MAX_FUTURE_SKEW_MS),
        )?
        .map(|raw| {
            coven_protocol::hlc::Timestamp::parse(&raw)
                .ok_or_else(|| DbError::Message(format!("invalid restored row clock: {raw:?}")))
        })
        .transpose()?;
        if row_floor > floor {
            floor = row_floor;
        }
        if let Some(floor) = &floor {
            self.raise_clock_floor(floor)?;
        }
        Ok(floor)
    }
}

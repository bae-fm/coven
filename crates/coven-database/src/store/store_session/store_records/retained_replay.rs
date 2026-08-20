use std::collections::{BTreeMap, BTreeSet};

use super::{StoreRecords, StoreTransaction};
use crate::store::materialization_models::{
    RetainedCommitActivationInput, RetainedMergeMaterializationInput,
};
use crate::store::verified_store_authority::{VerifiedRegistrationLookup, VerifiedStoreLookup};
use crate::{
    Database, DbError, ObjectHash, OwnedVerifiedMergeMaterialization, RetainedReplayOwner,
    StoreDatabase,
};
use coven_protocol::store_commit::RetainedStoreDeviceRegistrationActivations;

pub(crate) struct RetainedReplayBaselineRow {
    pub(crate) generation: i64,
    pub(crate) exact_cut: String,
    pub(crate) schema_version: i64,
    pub(crate) routing_hash: String,
    pub(crate) image_payload_hash: String,
    pub(crate) authority_hash: String,
}

pub(crate) struct SnapshotRetentionRows {
    pub(crate) exclusion_activation_commits: Vec<String>,
    pub(crate) circle_bootstraps: Vec<(String, String, String)>,
    pub(crate) materialization_refs: Vec<String>,
}

struct PreparedRetainedReplayBaseline {
    baseline: crate::RetainedReplayBaseline,
    image_bytes: Vec<u8>,
}

impl PreparedRetainedReplayBaseline {
    fn new(
        exact_cut: coven_protocol::store_commit::CommitFrontier,
        schema_version: u32,
        routing_hash: ObjectHash,
        authority: crate::RetainedReplayAuthority,
        image_bytes: Vec<u8>,
    ) -> Self {
        Self {
            baseline: crate::RetainedReplayBaseline {
                generation: crate::GENERATION_ZERO,
                exact_cut,
                schema_version,
                routing_hash,
                image_payload_hash: ObjectHash::digest(&image_bytes),
                authority,
            },
            image_bytes,
        }
    }
}

impl StoreRecords<'_> {
    pub(crate) fn snapshot_retention_rows(self) -> Result<SnapshotRetentionRows, DbError> {
        let exclusions = crate::query_mapped_rows(
            self.conn,
            "SELECT DISTINCT activation_commit
             FROM store_author_exclusion_activations
             ORDER BY activation_commit",
            [],
            |row| row.get::<_, String>(0),
        )?;
        let bootstraps = crate::query_mapped_rows(
            self.conn,
            "SELECT circle_id, activation_commit, exact_cut
             FROM circle_bootstrap_coverage ORDER BY circle_id",
            [],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )?;
        let materializations = crate::query_mapped_rows(
            self.conn,
            "SELECT commit_ref FROM retained_merge_materializations ORDER BY commit_ref",
            [],
            |row| row.get::<_, String>(0),
        )?;
        Ok(SnapshotRetentionRows {
            exclusion_activation_commits: exclusions,
            circle_bootstraps: bootstraps,
            materialization_refs: materializations,
        })
    }

    pub(crate) fn snapshot_exclusion_activation_rows(
        self,
    ) -> Result<Vec<(String, String)>, DbError> {
        Ok(crate::query_mapped_rows(
            self.conn,
            "SELECT exclusion_ref, activation_commit
             FROM store_author_exclusion_activations
             ORDER BY exclusion_ref",
            [],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )?)
    }

    pub(crate) fn retained_materialization_refs(self) -> Result<BTreeSet<String>, DbError> {
        Ok(crate::query_mapped_rows(
            self.conn,
            "SELECT commit_ref FROM retained_merge_materializations ORDER BY commit_ref",
            [],
            |row| row.get::<_, String>(0),
        )?
        .into_iter()
        .collect())
    }

    pub(crate) fn remote_object(
        self,
        object_id: ObjectHash,
    ) -> Result<coven_protocol::remote_object::RemoteObjectRecord, DbError> {
        crate::load_remote_object_on(self.conn, object_id)
    }

    pub(crate) fn retained_materialization_row(
        self,
        stream_id: &str,
        sequence: i64,
    ) -> Result<(String, String, Vec<u8>), DbError> {
        self.conn
            .query_row(
                "SELECT commit_ref, input_hash, canonical_input
                 FROM retained_merge_materializations
                 WHERE device_id = ?1 AND seq = ?2",
                rusqlite::params![stream_id, sequence],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(DbError::from)
    }

    pub(crate) fn retained_materialization_identity(
        self,
        stream_id: &str,
        sequence: i64,
    ) -> Result<(String, String), DbError> {
        self.conn
            .query_row(
                "SELECT commit_ref, input_hash FROM retained_merge_materializations
                 WHERE device_id = ?1 AND seq = ?2",
                rusqlite::params![stream_id, sequence],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(DbError::from)
    }

    pub(crate) fn validate_retained_merge_pin_closure(
        self,
        input: &RetainedMergeMaterializationInput,
        owner: &RetainedReplayOwner,
    ) -> Result<(), DbError> {
        crate::store::retained_merge_replay::validate_retained_merge_pin_closure_on(
            self.conn, input, owner,
        )
    }

    pub(crate) fn snapshot_coverage_reference(
        self,
        stream_id: &str,
        sequence: i64,
    ) -> Result<Option<String>, DbError> {
        use rusqlite::OptionalExtension;

        self.conn
            .query_row(
                "SELECT commit_ref FROM snapshot_coverage WHERE device_id = ?1 AND seq = ?2",
                rusqlite::params![stream_id, sequence],
                |row| row.get(0),
            )
            .optional()
            .map_err(DbError::from)
    }

    pub(crate) fn store_device_snapshot(
        self,
        reference: &coven_protocol::store_commit::StoreBatchCommitRef,
    ) -> Result<coven_protocol::store_commit::ResolvedStoreDeviceState, DbError> {
        crate::store::store_device_state::load_store_device_snapshot_on(self.conn, reference)
    }

    pub(crate) fn store_write_status_rows(self) -> Result<Vec<(String, String)>, DbError> {
        Ok(crate::query_mapped_rows(
            self.conn,
            "SELECT write_id, status FROM store_writes ORDER BY ordinal",
            [],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )?)
    }

    pub(crate) fn merge_retraction_cleanup_objects(
        self,
        candidate: &coven_protocol::store_commit::StoreBatchCommitRef,
    ) -> Result<
        (
            crate::DurablePreparedProtocolObject,
            crate::DurablePreparedProtocolObject,
        ),
        DbError,
    > {
        crate::store::retained_merge_replay::load_merge_retraction_cleanup_objects_on(
            self.conn, candidate,
        )
    }

    pub(crate) fn retained_materialization_rows(
        self,
    ) -> Result<Vec<(String, i64, String, String)>, DbError> {
        Ok(crate::query_mapped_rows(
            self.conn,
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
        )?)
    }

    pub(crate) fn circle_activation_commit_ref(
        self,
        circle_id: coven_protocol::circle::CircleId,
        control: &coven_protocol::circle::CircleControlCoord,
    ) -> Result<Option<coven_protocol::store_commit::StoreBatchCommitRef>, DbError> {
        crate::store::circle_authority::circle_activation_commit_ref_on(
            self.conn, circle_id, control,
        )
    }

    pub(crate) fn circle_replay_controls(self) -> Result<Vec<(String, String)>, DbError> {
        Ok(crate::query_mapped_rows(
            self.conn,
            "SELECT circle_id, control_coord
             FROM circle_control_activations
             ORDER BY circle_id, control_coord",
            [],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )?)
    }

    pub(crate) fn retained_replay_baseline_row(
        self,
    ) -> Result<Option<RetainedReplayBaselineRow>, DbError> {
        use rusqlite::OptionalExtension;

        self.conn
            .query_row(
                "SELECT generation, exact_cut, schema_version,
                        routing_hash, image_payload_hash, authority_hash
                 FROM retained_replay_baselines WHERE singleton = 1",
                [],
                |row| {
                    Ok(RetainedReplayBaselineRow {
                        generation: row.get(0)?,
                        exact_cut: row.get(1)?,
                        schema_version: row.get(2)?,
                        routing_hash: row.get(3)?,
                        image_payload_hash: row.get(4)?,
                        authority_hash: row.get(5)?,
                    })
                },
            )
            .optional()
            .map_err(DbError::from)
    }

    pub(crate) fn validate_replay_authority(
        self,
        baseline: &crate::RetainedReplayBaseline,
    ) -> Result<(), DbError> {
        crate::store_authority_records::validate_replay_authority_on(self.conn, baseline)
    }

    pub(crate) fn insert_retained_replay_baseline_row(
        self,
        baseline: &crate::RetainedReplayBaseline,
        authority_hash: ObjectHash,
    ) -> Result<(), DbError> {
        self.conn
            .execute(
                "INSERT INTO retained_replay_baselines
                 (singleton, generation, exact_cut, schema_version,
                  routing_hash, image_payload_hash, authority_hash)
                 VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6)",
                rusqlite::params![
                    i64::try_from(baseline.generation).map_err(|_| {
                        DbError::Message(
                            "retained replay generation exceeds SQLite INTEGER".to_string(),
                        )
                    })?,
                    serde_json::to_string(&baseline.exact_cut).map_err(|error| {
                        DbError::context("serialize retained replay exact cut", error)
                    })?,
                    i64::from(baseline.schema_version),
                    baseline.routing_hash.to_string(),
                    baseline.image_payload_hash.to_string(),
                    authority_hash.to_string(),
                ],
            )
            .map_err(DbError::from)?;
        crate::payload_store::set_payload_owner_claims_on(
            self.conn,
            crate::payload_store::RETAINED_REPLAY_BASELINE_OWNER_KEY,
            &BTreeSet::from([baseline.image_payload_hash, authority_hash]),
        )
    }

    pub(crate) fn accepted_history_count(self) -> Result<i64, DbError> {
        self.conn
            .query_row(
                "SELECT (SELECT COUNT(*) FROM materialized_commits)
                        + (SELECT COUNT(*) FROM snapshot_coverage)",
                [],
                |row| row.get(0),
            )
            .map_err(DbError::from)
    }

    pub(super) fn install_generation_zero_replay_baseline_records(
        self,
        schema_version: u32,
        routing_hash: ObjectHash,
        authority: crate::RetainedReplayGenesisAuthority,
    ) -> Result<crate::RetainedReplayBaseline, DbError> {
        let image = crate::store::retained_replay::project_generation_zero_image(self.conn)?;
        let prepared = PreparedRetainedReplayBaseline::new(
            coven_protocol::store_commit::CommitFrontier(Default::default()),
            schema_version,
            routing_hash,
            crate::RetainedReplayAuthority::Genesis(authority),
            image.database_bytes()?,
        );
        image.validate(&prepared.baseline, self.store_dir)?;
        // A founder's generation-zero image is the empty store, so this reports
        // the same stages over nothing much; it costs a line, not a pass.
        let mut timings =
            coven_foundation::stage_timing::StageTimings::start("Retained replay baseline capture");
        let outcome = self.install_prepared_replay_baseline(prepared, &mut timings);
        timings.report();
        outcome
    }

    pub(super) fn install_snapshot_replay_baseline_records(
        self,
        schema_version: u32,
        routing_hash: ObjectHash,
        authority: coven_protocol::store_commit::RetainedReplaySnapshotAuthority,
    ) -> Result<crate::RetainedReplayBaseline, DbError> {
        // Capturing the baseline copies the whole store: the open database is
        // serialized, hashed, compressed and written as a payload, then read
        // back and compared. Each of those is a pass over the image, so they
        // are named apart rather than as one number that only says "big store".
        let mut timings =
            coven_foundation::stage_timing::StageTimings::start("Retained replay baseline capture");
        let image_bytes = timings.mark("serialize the image", || {
            crate::connection_io::serialize_database_image(self.conn)
        })?;
        let prepared = PreparedRetainedReplayBaseline::new(
            authority.metadata.coverage.clone(),
            schema_version,
            routing_hash,
            crate::RetainedReplayAuthority::InstalledSnapshot(authority),
            image_bytes,
        );
        timings.mark("validate the image", || {
            prepared
                .baseline
                .validate_open_image(self.conn, self.store_dir)
        })?;
        let outcome = self.install_prepared_replay_baseline(prepared, &mut timings);
        timings.report();
        outcome
    }

    fn install_prepared_replay_baseline(
        self,
        prepared: PreparedRetainedReplayBaseline,
        timings: &mut coven_foundation::stage_timing::StageTimings,
    ) -> Result<crate::RetainedReplayBaseline, DbError> {
        let PreparedRetainedReplayBaseline {
            baseline,
            image_bytes,
        } = prepared;
        self.validate_replay_authority(&baseline)?;
        let installed_image_hash = timings.mark("store the image payload", || {
            self.install_payload(&image_bytes)
                .map_err(|error| DbError::context("install retained replay image", error))
        })?;
        if installed_image_hash != baseline.image_payload_hash {
            return Err(DbError::Message(
                "installed retained replay image has a different content address".to_string(),
            ));
        }
        let (authority_bytes, authority_hash) =
            timings.mark("store the authority payload", || {
                let authority_bytes = baseline.canonical_authority_bytes()?;
                let authority_hash = self.install_payload(&authority_bytes).map_err(|error| {
                    DbError::context("install retained replay authority", error)
                })?;
                self.insert_retained_replay_baseline_row(&baseline, authority_hash)?;
                Ok::<_, DbError>((authority_bytes, authority_hash))
            })?;
        let installed_image = timings.mark("read back the image payload", || {
            self.verified_payload(installed_image_hash)
                .map_err(|error| DbError::context("read installed retained replay image", error))
        })?;
        if installed_image != image_bytes {
            return Err(DbError::Message(
                "installed retained replay image differs from its prepared bytes".to_string(),
            ));
        }
        let installed_authority = self
            .verified_payload(authority_hash)
            .map_err(|error| DbError::context("read installed retained replay authority", error))?;
        if installed_authority != authority_bytes {
            return Err(DbError::Message(
                "installed retained replay authority differs from its canonical bytes".to_string(),
            ));
        }
        let installed = crate::store::retained_replay::load_replay_baseline_metadata_on(self)?
            .ok_or_else(|| {
                DbError::Message("installed retained replay baseline is absent".to_string())
            })?;
        self.validate_replay_authority(&installed)?;
        if installed != baseline {
            return Err(DbError::Message(
                "installed retained replay baseline differs from its verified image".to_string(),
            ));
        }
        Ok(installed)
    }

    pub(crate) fn validate_replay_baseline_image(
        self,
        baseline: &crate::RetainedReplayBaseline,
    ) -> Result<(), DbError> {
        baseline.validate_image(self.conn, self.store_dir)
    }
}

impl StoreTransaction<'_, '_> {
    pub(crate) fn generation_zero_replay_baseline(
        self,
    ) -> Result<crate::RetainedReplayBaseline, DbError> {
        StoreDatabase::generation_zero_replay_baseline_on(
            crate::store::store_session::StoreRecords::new(self.transaction, self.store_dir),
        )
    }

    pub(crate) fn replay_baseline_image_bytes(
        self,
        baseline: &crate::RetainedReplayBaseline,
    ) -> Result<Vec<u8>, DbError> {
        baseline.image_bytes(self.transaction, self.store_dir)
    }

    pub(crate) fn claimed_circle_bootstrap_coverage_refs(
        self,
    ) -> Result<Vec<coven_protocol::circle::CircleBootstrapCoverageRef>, DbError> {
        crate::store::store_session::StoreRecords::new(self.transaction, self.store_dir)
            .claimed_circle_bootstrap_coverage_refs()
    }

    pub(crate) fn verified_payload(self, hash: ObjectHash) -> Result<Vec<u8>, DbError> {
        crate::store::store_session::StoreRecords::new(self.transaction, self.store_dir)
            .verified_payload(hash)
            .map_err(DbError::from)
    }

    pub(crate) fn retained_materialization_rows(
        self,
    ) -> Result<Vec<(String, i64, String, String)>, DbError> {
        crate::store::store_session::StoreRecords::new(self.transaction, self.store_dir)
            .retained_materialization_rows()
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn load_retained_materialization(
        self,
        root: &coven_protocol::store_commit::StoreRootRef,
        registrations: &mut dyn VerifiedRegistrationLookup,
        stream_id: &str,
        sequence: u64,
        commit_ref: &coven_protocol::store_commit::StoreBatchCommitRef,
        expected_input_hash: &str,
        verified: Option<&coven_protocol::store_commit::VerifiedStoreBatchCommit>,
    ) -> Result<OwnedVerifiedMergeMaterialization, DbError> {
        let records =
            crate::store::store_session::StoreRecords::new(self.transaction, self.store_dir);
        match verified {
            Some(verified) => {
                StoreDatabase::load_retained_merge_materialization_with_verified_commit_on(
                    records,
                    root,
                    registrations,
                    stream_id,
                    sequence,
                    commit_ref,
                    expected_input_hash,
                    verified,
                )
            }
            None => StoreDatabase::load_retained_merge_materialization_on(
                records,
                root,
                registrations,
                stream_id,
                sequence,
                commit_ref,
                expected_input_hash,
            ),
        }
    }

    pub(crate) fn circle_replay_controls(self) -> Result<Vec<(String, String)>, DbError> {
        crate::store::store_session::StoreRecords::new(self.transaction, self.store_dir)
            .circle_replay_controls()
    }

    pub(crate) fn circle_activation_commit_ref(
        self,
        circle_id: coven_protocol::circle::CircleId,
        control: &coven_protocol::circle::CircleControlCoord,
    ) -> Result<Option<coven_protocol::store_commit::StoreBatchCommitRef>, DbError> {
        crate::store::store_session::StoreRecords::new(self.transaction, self.store_dir)
            .circle_activation_commit_ref(circle_id, control)
    }

    pub(crate) fn merge_replay_write_overlays(
        self,
        active_accepted_writes: &BTreeSet<coven_protocol::write::WriteId>,
        retracted_writes: &BTreeSet<coven_protocol::write::WriteId>,
    ) -> Result<Vec<crate::MergeReplayWriteOverlay>, DbError> {
        StoreDatabase::load_merge_replay_write_overlays_on(
            crate::store::store_session::StoreRecords::new(self.transaction, self.store_dir),
            active_accepted_writes,
            retracted_writes,
        )
    }

    pub(crate) fn retained_membership_authority_bytes(
        self,
        object: &coven_protocol::objects::ExactObjectRef,
        kind: &str,
    ) -> Result<crate::MembershipAuthorityBytes, DbError> {
        let object_id = coven_protocol::remote_object::remote_object_id(object);
        let remote =
            crate::load_remote_object_on(self.transaction, object_id).map_err(|error| {
                DbError::context(
                    format!("load retained Merge membership {kind} {object_id} for replay"),
                    error,
                )
            })?;
        if remote.object() != object {
            return Err(DbError::Message(format!(
                "retained Merge membership {kind} {object_id} has different exact object"
            )));
        }
        let coven_protocol::remote_object::SemanticPayload::Spooled(semantic_hash) =
            remote.semantic_payload()
        else {
            return Err(DbError::Message(format!(
                "retained Merge membership {kind} {object_id} names no stored plaintext"
            )));
        };
        let stored_hash = remote.stored_payload().ok_or_else(|| {
            DbError::Message(format!(
                "retained Merge membership {kind} {object_id} names no stored ciphertext"
            ))
        })?;
        Ok(crate::MembershipAuthorityBytes::new(
            crate::store::store_session::StoreRecords::new(self.transaction, self.store_dir)
                .payload(semantic_hash)?,
            crate::store::store_session::StoreRecords::new(self.transaction, self.store_dir)
                .payload(stored_hash)?,
        ))
    }

    pub(crate) fn open_replay_projection(
        self,
        image: &[u8],
    ) -> Result<crate::store::ReplayProjection, DbError> {
        crate::store::ReplayProjection::from_image(image, self.store_dir.clone())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn replay_projection_with_authority(
        self,
        authority: &mut crate::store::VerifiedStoreAuthority,
        expected_root: &coven_protocol::store_commit::StoreRootRef,
        blob_decls: &crate::BlobDecls,
        gates: &crate::Gates,
        synced_tables: &[coven_protocol::synced_schema::SyncedTable],
        routing_key: Option<&coven_protocol::circle::RowRoutingKey>,
        retracted: &BTreeSet<coven_protocol::store_commit::StoreBatchCommitRef>,
        history_cut: Option<&coven_protocol::store_commit::CommitFrontier>,
        include_local_write_overlays: bool,
        local_store_membership: coven_protocol::membership::LocalStoreMembership,
    ) -> Result<crate::store::ReplayProjection, DbError> {
        let root = authority.required_root_authority_on(
            crate::store::store_session::StoreRecords::new(self.transaction, self.store_dir),
        )?;
        if &root != expected_root {
            return Err(DbError::Message(
                "retained replay projection belongs to another Store root".to_string(),
            ));
        }
        authority.replay_projection_for_root_on(
            self,
            &root,
            blob_decls,
            gates,
            synced_tables,
            routing_key,
            retracted,
            history_cut,
            include_local_write_overlays,
            local_store_membership,
        )
    }

    pub(crate) fn retain_merge_materialization(
        self,
        registrations: &mut dyn VerifiedRegistrationLookup,
        root: &coven_protocol::store_commit::StoreRootRef,
        materialization: &crate::VerifiedMergeMaterialization<'_>,
    ) -> Result<
        (
            crate::RetainedMergeMaterializationKey,
            OwnedVerifiedMergeMaterialization,
        ),
        DbError,
    > {
        let packages = crate::store::retained_merge_replay::canonical_retained_merge_packages(
            materialization.commit(),
            materialization.commit_ref(),
            materialization.packages(),
        )?;
        let input = RetainedMergeMaterializationInput {
            commit: coven_protocol::objects::PreparedExactObject::new(
                materialization.commit_ref().object.clone(),
                materialization.commit().to_bytes(),
            )
            .map_err(DbError::from)?,
            activation_head: coven_protocol::objects::PreparedExactObject::new(
                materialization.activation_head_object().clone(),
                materialization.activation_head().to_bytes(),
            )
            .map_err(DbError::from)?,
            history_evidence: materialization.history_evidence().clone(),
            membership_objects: materialization.membership_objects().cloned(),
            packages,
            activation: RetainedCommitActivationInput {
                registrations: RetainedStoreDeviceRegistrationActivations::from_verified(
                    root,
                    materialization.commit(),
                    materialization.registrations(),
                )
                .map_err(DbError::from)?,
                device_operations: materialization.device_operations().to_retained(),
                circle_activations: materialization
                    .circle_activations()
                    .to_retained()
                    .map_err(DbError::from)?,
                package_application: materialization.package_application(),
            },
        };
        let canonical_input = serde_json::to_vec(&input)
            .map_err(|error| DbError::context("serialize retained Merge materialization", error))?;
        let input_hash = ObjectHash::digest(&canonical_input);
        let verified =
            StoreDatabase::open_retained_merge_materialization_input_with_verified_commit_on(
                crate::store::store_session::StoreRecords::new(self.transaction, self.store_dir),
                root,
                registrations,
                materialization.commit_ref(),
                &input,
                input_hash,
                materialization.verified_commit(),
            )?;
        let stream_id = materialization.commit_ref().coord.stream_id.to_string();
        let sequence =
            Database::sequence_to_sqlite(&stream_id, materialization.commit_ref().coord.sequence)?;
        let commit_ref_json = serde_json::to_string(materialization.commit_ref())
            .map_err(|error| DbError::context("serialize retained Merge commit ref", error))?;
        let inserted = self
            .transaction
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
            let stored: (String, String, Vec<u8>) = self
                .transaction
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
        crate::store::retained_merge_replay::pin_retained_merge_objects_on(
            self.transaction,
            &input,
            &replay_owner,
        )?;
        crate::store::retained_merge_replay::validate_retained_merge_pin_closure_on(
            self.transaction,
            &input,
            &replay_owner,
        )?;
        Ok((
            crate::RetainedMergeMaterializationKey {
                commit_ref: commit_ref_json,
                input_hash,
            },
            verified,
        ))
    }

    pub(crate) fn retain_snapshot_replay_inputs(
        self,
        authority: &mut dyn VerifiedStoreLookup,
        root: &coven_protocol::store_commit::StoreRootRef,
    ) -> Result<(), DbError> {
        let conn = self.transaction;
        let required = StoreDatabase::snapshot_required_retained_refs(
            crate::store::store_session::StoreRecords::new(self.transaction, self.store_dir),
            authority,
            root,
        )?;
        let mut retained = Vec::with_capacity(required.len());
        for encoded in required {
            let reference: coven_protocol::store_commit::StoreBatchCommitRef =
                serde_json::from_str(&encoded).map_err(|error| {
                    DbError::context("snapshot author exclusion activation commit", error)
                })?;
            StoreDatabase::load_retained_merge_materialization_by_ref_on(
                crate::store::store_session::StoreRecords::new(self.transaction, self.store_dir),
                root,
                authority,
                &reference,
            )?;
            let stream_id = reference.coord.stream_id.to_string();
            let sequence_sql = Database::sequence_to_sqlite(&stream_id, reference.coord.sequence)?;
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
                .map_err(|error| DbError::context("snapshot retained replay input", error))?;
            retained.push((reference, input_hash, canonical_input, input));
        }
        crate::store::retained_merge_replay::remove_retained_replay_ownership_from_snapshot_on(
            conn,
        )?;
        conn.execute("DELETE FROM retained_merge_materializations", [])
            .map_err(DbError::from)?;
        for (reference, input_hash, canonical_input, input) in retained {
            let stream_id = reference.coord.stream_id.to_string();
            let sequence = Database::sequence_to_sqlite(&stream_id, reference.coord.sequence)?;
            let encoded_ref = serde_json::to_string(&reference).map_err(|error| {
                DbError::context("serialize snapshot author exclusion activation", error)
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
                DbError::context(
                    format!("snapshot author exclusion input hash {input_hash}"),
                    error,
                )
            })?;
            let owner = RetainedReplayOwner::Commit {
                commit: reference,
                input_hash,
            };
            crate::store::retained_merge_replay::pin_retained_merge_objects_on(
                conn, &input, &owner,
            )?;
            crate::store::retained_merge_replay::validate_retained_merge_pin_closure_on(
                conn, &input, &owner,
            )?;
        }
        Ok(())
    }

    pub(crate) fn retain_snapshot_device_states(
        self,
        authority: &mut dyn VerifiedStoreLookup,
        root: &coven_protocol::store_commit::StoreRootRef,
        coverage: BTreeMap<String, coven_protocol::store_commit::StoreBatchCommitRef>,
    ) -> Result<(), DbError> {
        let conn = self.transaction;
        let mut required = coverage.into_values().collect::<BTreeSet<_>>();
        let retained = crate::query_mapped_rows(
            conn,
            "SELECT commit_ref FROM retained_merge_materializations ORDER BY commit_ref",
            [],
            |row| row.get::<_, String>(0),
        )?;
        for encoded in retained {
            let reference: coven_protocol::store_commit::StoreBatchCommitRef =
                serde_json::from_str(&encoded).map_err(|error| {
                    DbError::context("snapshot retained device-state authority", error)
                })?;
            let materialization = StoreDatabase::load_retained_merge_materialization_by_ref_on(
                crate::store::store_session::StoreRecords::new(self.transaction, self.store_dir),
                root,
                authority,
                &reference,
            )?;
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
                DbError::context("serialize snapshot device-state reference", error)
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
                    DbError::context("serialize expected snapshot device state", error)
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
}

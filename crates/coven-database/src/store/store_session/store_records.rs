use coven_foundation::store_dir::StoreDir;
use coven_protocol::store_commit::ObjectHash;
use coven_protocol::write::{WriteId, WriteStatus};
use rusqlite::Connection;

use super::payload_store::{
    read_payload_blocking, read_verified_payload_blocking, write_payload_blocking,
    PayloadStoreError,
};
use super::StoreTransaction;
#[cfg(any(test, feature = "test-utils"))]
use crate::StoreDatabase;
use crate::{AudiencePartition, CirclePartitionControl, Database, DbError};

mod circle_bootstrap;
mod retained_replay;
mod snapshot_install;

/// One Store's row connection and matching payload storage.
///
/// Payload records may hold bytes in SQLite or name a file beside it, so record
/// operations carry the connection and directory as one scoped value.
#[derive(Clone, Copy)]
pub(crate) struct StoreRecords<'store> {
    conn: &'store Connection,
    store_dir: &'store StoreDir,
}

impl<'store> StoreRecords<'store> {
    pub(super) fn new(conn: &'store Connection, store_dir: &'store StoreDir) -> Self {
        Self { conn, store_dir }
    }

    pub(crate) fn payload(&self, hash: ObjectHash) -> Result<Vec<u8>, PayloadStoreError> {
        read_payload_blocking(self.conn, self.store_dir, hash)
    }

    pub(crate) fn verified_payload(&self, hash: ObjectHash) -> Result<Vec<u8>, PayloadStoreError> {
        read_verified_payload_blocking(self.conn, self.store_dir, hash)
    }

    pub(crate) fn install_payload(&self, bytes: &[u8]) -> Result<ObjectHash, PayloadStoreError> {
        write_payload_blocking(self.conn, self.store_dir, bytes)
    }

    pub(super) fn install_generation_zero_replay_baseline(
        self,
        schema_version: u32,
        routing_hash: ObjectHash,
        authority: crate::RetainedReplayGenesisAuthority,
    ) -> Result<crate::RetainedReplayBaseline, DbError> {
        self.install_generation_zero_replay_baseline_records(
            schema_version,
            routing_hash,
            authority,
        )
    }

    pub(super) fn install_snapshot_replay_baseline(
        self,
        schema_version: u32,
        routing_hash: ObjectHash,
        authority: crate::RetainedReplaySnapshotAuthority,
    ) -> Result<crate::RetainedReplayBaseline, DbError> {
        self.install_snapshot_replay_baseline_records(schema_version, routing_hash, authority)
    }

    pub(crate) fn store_write_partitions(
        self,
        write_id: &str,
    ) -> Result<crate::PreparedStoreWritePartitions, DbError> {
        let mut statement = self
            .conn
            .prepare(
                "SELECT audience, control_coord, changeset_hash
                 FROM store_write_partitions
                 WHERE write_id = ?1
                 ORDER BY CASE audience WHEN 'store' THEN 0 WHEN 'local' THEN 2 ELSE 1 END,
                          audience, control_coord",
            )
            .map_err(DbError::from)?;
        let rows = statement
            .query_map([write_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .map_err(DbError::from)?;
        let mut store = None;
        let mut circles = Vec::new();
        let mut local = None;
        for row in rows {
            let (audience, control, changeset_hash) = row.map_err(DbError::from)?;
            let changeset = self.payload(changeset_hash.parse()?)?;
            if audience == "store" {
                if control.is_some() {
                    return Err(DbError::Message(format!(
                        "pending write {write_id} Store partition carries a Circle control"
                    )));
                }
                if store.is_some() {
                    return Err(DbError::Message(format!(
                        "pending write {write_id} carries more than one Store partition"
                    )));
                }
                store = Some(AudiencePartition {
                    audience: coven_protocol::circle::Audience::Store,
                    control: None,
                    changeset,
                });
                continue;
            }
            if audience == "local" {
                if control.is_some() {
                    return Err(DbError::Message(format!(
                        "pending write {write_id} Local partition carries a Circle control"
                    )));
                }
                if local.is_some() {
                    return Err(DbError::Message(format!(
                        "pending write {write_id} carries more than one Local partition"
                    )));
                }
                local = Some(AudiencePartition {
                    audience: coven_protocol::circle::Audience::Local,
                    control: None,
                    changeset,
                });
                continue;
            }
            let circle_id = audience
                .parse::<coven_protocol::circle::CircleId>()
                .map_err(|error| {
                    DbError::context(
                        format!("pending write {write_id} has invalid audience {audience:?}"),
                        error,
                    )
                })?;
            let control_json = control.ok_or_else(|| {
                DbError::Message(format!(
                    "pending write {write_id} Circle {circle_id} has no control coordinate"
                ))
            })?;
            let control =
                CirclePartitionControl::from_stored_json(control_json).map_err(|error| {
                    DbError::Message(format!(
                        "pending write {write_id} Circle {circle_id} control coordinate: {error}"
                    ))
                })?;
            circles.push(AudiencePartition {
                audience: coven_protocol::circle::Audience::Circle(circle_id),
                control: Some(control),
                changeset,
            });
        }
        drop(statement);
        Ok(crate::PreparedStoreWritePartitions {
            store,
            circles,
            local,
        })
    }

    pub(super) fn store_root_authority(
        self,
    ) -> Result<
        Option<(
            coven_protocol::store_commit::StoreRootRef,
            coven_protocol::store_commit::StoreProtocolRoot,
        )>,
        DbError,
    > {
        crate::load_store_root_authority_on(self.conn)
    }

    pub(super) fn activated_registration(
        self,
        root: &coven_protocol::store_commit::StoreRootRef,
        reference: &coven_protocol::store_commit::StoreDeviceRegistrationRef,
    ) -> Result<coven_protocol::store_commit::StoreDeviceRegistration, DbError> {
        crate::load_activated_registration_on(self.conn, root, reference)
    }

    pub(super) fn local_activated_registration_ref(
        self,
    ) -> Result<Option<coven_protocol::store_commit::StoreDeviceRegistrationRef>, DbError> {
        crate::local_activated_registration_ref_on(self.conn)
    }

    pub(super) fn has_local_device(self) -> Result<bool, DbError> {
        Ok(crate::get_protocol_state_on(self.conn, crate::LOCAL_DEVICE_ID_STATE_KEY)?.is_some())
    }

    pub(super) fn current_store_device_state(
        self,
    ) -> Result<coven_protocol::store_commit::ResolvedStoreDeviceState, DbError> {
        let frontier =
            crate::store::materialized_commit_index::materialized_frontier_on(self.conn, None)?
                .into_values()
                .map(|reference| (reference.coord.stream_id, reference))
                .collect::<std::collections::BTreeMap<_, _>>();
        let (_, state) = super::store_device_state::store_device_state_for_history_cut_on(
            self.conn,
            &coven_protocol::store_commit::StoreHistoryCut(frontier),
        )?;
        Ok(state)
    }

    pub(super) fn author_exclusion_activation_row(
        self,
        exclusion: &str,
    ) -> Result<Option<(String, String, String)>, DbError> {
        use rusqlite::OptionalExtension;

        self.conn
            .query_row(
                "SELECT accepted_cut, activation_commit, activation_head
                 FROM store_author_exclusion_activations
                 WHERE exclusion_ref = ?1",
                [exclusion],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(DbError::from)
    }

    pub(super) fn materialized_commit_ref(
        self,
        stream_id: &str,
        sequence: u64,
    ) -> Result<Option<coven_protocol::store_commit::StoreBatchCommitRef>, DbError> {
        crate::store::materialized_commit_index::materialized_commit_ref_on(
            self.conn, stream_id, sequence,
        )
    }

    pub(super) fn declared_store_device_state(
        self,
        reference: &coven_protocol::store_commit::StoreDeviceStateRef,
    ) -> Result<coven_protocol::store_commit::ResolvedStoreDeviceState, DbError> {
        super::store_device_state::load_declared_store_device_state_on(self.conn, reference)
    }

    pub(super) fn transaction<R>(
        self,
        operation: impl FnOnce(
            StoreTransaction<'_, '_>,
        ) -> Result<super::StoreTransactionOutcome<R>, DbError>,
    ) -> Result<R, DbError> {
        let transaction = self.conn.unchecked_transaction().map_err(DbError::from)?;
        let outcome = operation(StoreTransaction::new(&transaction, self.store_dir));
        match outcome {
            Ok(super::StoreTransactionOutcome::Commit(value)) => {
                transaction.commit().map_err(DbError::from)?;
                Ok(value)
            }
            Ok(super::StoreTransactionOutcome::Rollback(value)) => {
                transaction.rollback().map_err(DbError::from)?;
                Ok(value)
            }
            Err(error) => Err(error),
        }
    }

    pub(super) fn host_sql_read<F, R, E>(self, read: F) -> Result<Result<R, E>, DbError>
    where
        F: for<'connection> FnOnce(super::SqlReadContext<'connection>) -> Result<R, E>,
    {
        let authorization = super::host_sql_transaction::HostSqlAuthorization::begin(self.conn)?;
        Ok(authorization.run(|| read(super::SqlReadContext::new(self.conn))))
    }

    pub(super) fn protocol_state(self, key: &str) -> Result<Option<String>, DbError> {
        crate::get_protocol_state_on(self.conn, key)
    }

    pub(super) fn required_protocol_state(self, key: &str) -> Result<String, DbError> {
        crate::required_protocol_state_on(self.conn, key)
    }

    pub(super) fn set_protocol_state(self, key: &str, value: &str) -> Result<(), DbError> {
        crate::set_protocol_state_on(self.conn, key, value)
    }

    pub(super) fn write_status(self, write_id: &WriteId) -> Result<WriteStatus, DbError> {
        let raw: String = self
            .conn
            .query_row(
                "SELECT status FROM store_writes WHERE write_id = ?1",
                [write_id.as_str()],
                |row| row.get(0),
            )
            .map_err(DbError::from)?;
        serde_json::from_str(&raw)
            .map_err(|error| DbError::context(format!("write {write_id} status"), error))
    }

    pub(super) fn materialized_frontier(
        self,
    ) -> Result<
        std::collections::BTreeMap<String, coven_protocol::store_commit::StoreBatchCommitRef>,
        DbError,
    > {
        super::materialized_commit_index::materialized_frontier_on(self.conn, None)
    }

    pub(super) fn retained_merge_materialization_refs(
        self,
    ) -> Result<Vec<coven_protocol::store_commit::StoreBatchCommitRef>, DbError> {
        let rows = crate::query_mapped_rows(
            self.conn,
            "SELECT device_id, seq, commit_ref
             FROM retained_merge_materializations
             ORDER BY device_id, seq",
            [],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )?;
        rows.into_iter()
            .map(|(stream_id, sequence, encoded_ref)| {
                let sequence = Database::sequence_from_sqlite(&stream_id, sequence)?;
                super::materialized_commit_index::parse_stored_commit_ref(
                    &stream_id,
                    sequence,
                    &encoded_ref,
                )
            })
            .collect()
    }

    pub(super) fn snapshot_coverage_frontier(
        self,
    ) -> Result<coven_protocol::store_commit::CommitFrontier, DbError> {
        let rows = crate::query_mapped_rows(
            self.conn,
            "SELECT device_id, seq, commit_ref FROM snapshot_coverage",
            [],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )?;
        let mut frontier = std::collections::BTreeMap::new();
        for (device_id, sequence, encoded_ref) in rows {
            let sequence = Database::sequence_from_sqlite(&device_id, sequence)?;
            let reference = super::materialized_commit_index::parse_stored_commit_ref(
                &device_id,
                sequence,
                &encoded_ref,
            )?;
            frontier.insert(device_id, reference);
        }
        coven_protocol::store_commit::CommitFrontier::from_refs(frontier)
            .map_err(|error| DbError::context("snapshot coverage frontier", error))
    }

    pub(super) fn store_device_state_for_history_cut(
        self,
        cut: &coven_protocol::store_commit::StoreHistoryCut,
    ) -> Result<
        (
            coven_protocol::store_commit::StoreDeviceStateRef,
            coven_protocol::store_commit::ResolvedStoreDeviceState,
        ),
        DbError,
    > {
        super::store_device_state::store_device_state_for_history_cut_on(self.conn, cut)
    }

    pub(super) fn store_device_exclusion_freezes(
        self,
        root: &coven_protocol::store_commit::StoreRootRef,
    ) -> Result<Vec<coven_protocol::store_commit::StoreDeviceProposalAck>, DbError> {
        Ok(
            super::store_device_state::load_store_device_exclusion_freezes_on(self.conn, root)?
                .into_values()
                .collect(),
        )
    }

    pub(super) fn activated_registration_references(
        self,
    ) -> Result<Vec<coven_protocol::store_commit::StoreDeviceRegistrationRef>, DbError> {
        let rows = crate::query_mapped_rows(
            self.conn,
            "SELECT device_id, registration_hash, registration_object
             FROM store_device_registration_activations ORDER BY device_id",
            [],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )?;
        rows.into_iter()
            .map(|(device_id, registration_hash, object)| {
                let device_id = device_id
                    .parse()
                    .map_err(|error| DbError::context("activated Store device id", error))?;
                let registration_hash = registration_hash.parse().map_err(|error| {
                    DbError::context("activated Store device registration hash", error)
                })?;
                let reference = serde_json::from_str::<
                    coven_protocol::store_commit::StoreDeviceRegistrationRef,
                >(&object)
                .map_err(|error| {
                    DbError::context("activated Store device exact reference", error)
                })?;
                if reference.device_id != device_id
                    || reference.registration_hash != registration_hash
                {
                    return Err(DbError::Message(
                        "activated Store registration columns differ from its exact reference"
                            .to_string(),
                    ));
                }
                Ok(reference)
            })
            .collect()
    }

    pub(super) fn activated_registration_authority(
        self,
        reference: &coven_protocol::store_commit::StoreDeviceRegistrationRef,
    ) -> Result<String, DbError> {
        self.conn
            .query_row(
                "SELECT activation_authority FROM store_device_registration_activations
                 WHERE device_id = ?1 AND registration_hash = ?2",
                (
                    reference.device_id.to_string(),
                    reference.registration_hash.to_string(),
                ),
                |row| row.get(0),
            )
            .map_err(DbError::from)
    }

    pub(super) fn activated_registration_row_for_device(
        self,
        device_id: coven_protocol::store_commit::StoreDeviceId,
    ) -> Result<Option<(String, String)>, DbError> {
        use rusqlite::OptionalExtension;
        self.conn
            .query_row(
                "SELECT registration_object, activation_authority
                 FROM store_device_registration_activations WHERE device_id = ?1",
                [device_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(DbError::from)
    }

    pub(super) fn registered_stream_activation(
        self,
        activation_id: coven_protocol::store_commit::StreamActivationId,
    ) -> Result<Option<coven_protocol::store_commit::RegisteredStreamActivation>, DbError> {
        use rusqlite::OptionalExtension;
        let key = activation_id.as_hash().to_string();
        let stored = self
            .conn
            .query_row(
                "SELECT activation_id, author_stream_id, activation, activating_commit
                 FROM stream_activations WHERE activation_id = ?1",
                [key],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                },
            )
            .optional()
            .map_err(DbError::from)?;
        let Some((activation_id, author_stream_id, activation, activating_commit)) = stored else {
            return Ok(None);
        };
        let activation_id = coven_protocol::store_commit::StreamActivationId::from_digest(
            activation_id
                .parse()
                .map_err(|error| DbError::context("stored stream activation id", error))?,
        );
        let author_stream_id = author_stream_id
            .parse()
            .map_err(|error| DbError::Message(format!("stored author stream id: {error}")))?;
        let activation = serde_json::from_slice(&activation)
            .map_err(|error| DbError::context("stored stream activation descriptor", error))?;
        let activating_commit = serde_json::from_str(&activating_commit)
            .map_err(|error| DbError::context("stored stream activation commit ref", error))?;
        coven_protocol::store_commit::RegisteredStreamActivation::from_stored(
            activation_id,
            author_stream_id,
            activation,
            activating_commit,
        )
        .map(Some)
        .map_err(|error| DbError::Message(error.to_string()))
    }

    pub(super) fn stage_owner_recovery_publication(
        self,
        registration_hash: &str,
        encoded: &str,
    ) -> Result<(), DbError> {
        let transaction = self.conn.unchecked_transaction().map_err(DbError::from)?;
        transaction
            .execute(
                "INSERT INTO local_owner_recovery_publication
                     (singleton, registration_hash, publication)
                 VALUES (1, ?1, ?2)
                 ON CONFLICT(singleton) DO NOTHING",
                (registration_hash, encoded),
            )
            .map_err(DbError::from)?;
        let stored: (String, String) = transaction
            .query_row(
                "SELECT registration_hash, publication
                 FROM local_owner_recovery_publication WHERE singleton = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(DbError::from)?;
        if stored != (registration_hash.to_string(), encoded.to_string()) {
            return Err(DbError::Message(
                "Owner recovery publication journal owns different exact objects".into(),
            ));
        }
        transaction.commit().map_err(DbError::from)
    }

    pub(super) fn owner_recovery_publication_row(
        self,
    ) -> Result<Option<(String, String)>, DbError> {
        use rusqlite::OptionalExtension;
        self.conn
            .query_row(
                "SELECT registration_hash, publication
                 FROM local_owner_recovery_publication WHERE singleton = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(DbError::from)
    }

    pub(super) fn begin_protocol_state(self, key: &str, value: &str) -> Result<String, DbError> {
        let transaction = self.conn.unchecked_transaction().map_err(DbError::from)?;
        transaction
            .execute(
                "INSERT INTO protocol_state (key, value) VALUES (?1, ?2)
                 ON CONFLICT(key) DO NOTHING",
                (key, value),
            )
            .map_err(DbError::from)?;
        let actual = crate::required_protocol_state_on(&transaction, key)?;
        transaction.commit().map_err(DbError::from)?;
        Ok(actual)
    }

    pub(super) fn compare_exchange_protocol_state(
        self,
        key: &str,
        previous: &str,
        next: &str,
    ) -> Result<bool, DbError> {
        let changed = self
            .conn
            .execute(
                "UPDATE protocol_state SET value = ?1 WHERE key = ?2 AND value = ?3",
                (next, key, previous),
            )
            .map_err(DbError::from)?;
        Ok(changed == 1)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(super) fn scoped_snapshot_counts(self) -> Result<(i64, i64, i64), DbError> {
        self.conn
            .query_row(
                "SELECT
                     (SELECT COUNT(*) FROM documents),
                     (SELECT COUNT(*) FROM paragraphs),
                     (SELECT COUNT(*) FROM _coven_row_routes)",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(DbError::from)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(super) fn migrated_scoped_snapshot_facts(self) -> Result<(i64, i64, String), DbError> {
        self.conn
            .query_row(
                "SELECT
                     (SELECT COUNT(*) FROM documents),
                     (SELECT COUNT(*) FROM _coven_row_routes),
                     (SELECT ordinary FROM documents)",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(DbError::from)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(super) fn circle_bootstrap_coverage_ref(
        self,
        circle_id: coven_protocol::circle::CircleId,
    ) -> Result<Option<coven_protocol::circle::CircleBootstrapCoverageRef>, DbError> {
        super::retained_merge_replay::circle_bootstrap_coverage_ref_on(self.conn, circle_id)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(super) fn circle_control_activation_count(
        self,
        circle_id: coven_protocol::circle::CircleId,
    ) -> Result<i64, DbError> {
        self.conn
            .query_row(
                "SELECT COUNT(*) FROM circle_control_activations WHERE circle_id = ?1",
                [circle_id.to_string()],
                |row| row.get(0),
            )
            .map_err(DbError::from)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(super) fn generation_zero_replay_baseline(
        self,
    ) -> Result<crate::RetainedReplayBaseline, DbError> {
        StoreDatabase::generation_zero_replay_baseline_on(self)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(super) fn replace_generation_zero_replay_authority(
        self,
        authority_bytes: &[u8],
    ) -> Result<(), DbError> {
        let transaction = self.conn.unchecked_transaction().map_err(DbError::from)?;
        let authority_hash = super::payload_store::write_payload_blocking(
            &transaction,
            self.store_dir,
            authority_bytes,
        )
        .map_err(|error| DbError::Message(format!("install retained replay authority: {error}")))?;
        transaction
            .execute(
                "UPDATE retained_replay_baselines SET authority_hash = ?1
                 WHERE singleton = 1",
                [authority_hash.to_string()],
            )
            .map_err(DbError::from)?;
        let image_payload_hash: String = transaction
            .query_row(
                "SELECT image_payload_hash FROM retained_replay_baselines WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .map_err(DbError::from)?;
        super::payload_store::set_payload_owner_claims_on(
            &transaction,
            super::payload_store::RETAINED_REPLAY_BASELINE_OWNER_KEY,
            &std::collections::BTreeSet::from([image_payload_hash.parse()?, authority_hash]),
        )?;
        transaction.commit().map_err(DbError::from)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(super) fn circle_bootstrap_replay_inputs(
        self,
    ) -> Result<
        Vec<(
            coven_protocol::store_commit::StoreBatchCommitRef,
            coven_protocol::circle_activation::VerifiedCircleImage,
        )>,
        DbError,
    > {
        StoreDatabase::circle_bootstrap_replay_inputs_on(self)
    }
}

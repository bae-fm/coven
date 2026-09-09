use super::clock_floor;
use super::retained_replay::load_generation_zero_replay_baseline_on;
use super::verified_store_authority::VerifiedRegistrationLookup;
use crate::PublishedStoreSnapshot;
use coven_foundation::store_dir::StoreDir;
use coven_protocol::store_commit::ObjectHash;
use coven_protocol::store_commit::{SnapshotMeta, StoreRootRef, StoreSnapshotRef};
use coven_protocol::write::{WriteId, WriteStatus};
use rusqlite::{Connection, OptionalExtension};

use super::payload_store::{
    read_payload_blocking, read_verified_payload_blocking, write_payload_blocking,
    PayloadStoreError,
};
use super::StoreTransaction;
#[cfg(any(test, feature = "test-utils"))]
use crate::StoreDatabase;
use crate::{AudiencePartition, CirclePartitionControl, Database, DbError};

mod baseline_advance;
pub(crate) use baseline_advance::replay_baseline_advances_on;
pub use baseline_advance::AdvancedReplayBaseline;
mod circle_bootstrap;
mod covered_store_write;
pub use covered_store_write::{CoveredStoreWrite, CoveredStoreWriteCompletion};
mod received_snapshot;
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

    pub(crate) fn rebased_store_write(
        self,
        write_id: &WriteId,
    ) -> Result<Option<crate::write_models::RebasedStoreWrite>, DbError> {
        let encoded: Option<String> = self.conn.query_row(
            "SELECT rebased FROM store_writes WHERE write_id = ?1",
            [write_id.as_str()],
            |row| row.get(0),
        )?;
        encoded
            .map(|encoded| {
                serde_json::from_str(&encoded)
                    .map_err(|error| DbError::context("read rebased Store write", error))
            })
            .transpose()
    }

    pub(crate) fn effective_store_write_base(
        self,
        write_id: &WriteId,
        captured: &str,
    ) -> Result<crate::StoreWriteBase, DbError> {
        match self.rebased_store_write(write_id)? {
            Some(rebased) => Ok(rebased.base),
            None => serde_json::from_str(captured)
                .map_err(|error| DbError::context("read captured Store write base", error)),
        }
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
        authority: coven_protocol::store_commit::RetainedReplaySnapshotAuthority,
        blob_decls: &crate::BlobDecls,
    ) -> Result<crate::RetainedReplayBaseline, DbError> {
        self.install_snapshot_replay_baseline_records(
            schema_version,
            routing_hash,
            authority,
            blob_decls,
        )
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
            let control = CirclePartitionControl::from_stored_json(control_json)?;
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

    pub(super) fn author_exclusion_activation_row(
        self,
        exclusion: &str,
    ) -> Result<Option<String>, DbError> {
        use rusqlite::OptionalExtension;

        self.conn
            .query_row(
                "SELECT activation_commit
                 FROM store_author_exclusion_activations
                 WHERE exclusion_ref = ?1",
                [exclusion],
                |row| row.get(0),
            )
            .optional()
            .map_err(DbError::from)
    }

    pub(super) fn store_publication_entries(
        self,
    ) -> Result<
        Vec<
            coven_protocol::objects::ExactProtocolObject<
                coven_protocol::store_commit::StorePublicationEntry,
            >,
        >,
        DbError,
    > {
        super::observed_store_publication::load_store_publication_entries_on(self.conn)
    }

    pub(super) fn accepted_store_commit(
        self,
        commit: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
        publisher_signing_pubkey: &str,
    ) -> Result<crate::AcceptedStoreCommitPublication, DbError> {
        super::observed_store_publication::load_accepted_store_commit_on(
            self.conn,
            commit,
            publisher_signing_pubkey,
        )
    }

    /// Cumulative coverage settles the reserved logical edit, independently of
    /// which of its candidate hashes was accepted. It cannot authenticate an
    /// arbitrary historical commit supplied by a caller.
    pub(super) fn snapshot_covers_reserved_write(
        self,
        write_id: &WriteId,
        candidate: &coven_protocol::store_commit::StoreBatchCommitRef,
        snapshot: &coven_protocol::store_commit::RetainedReplaySnapshotAuthority,
    ) -> Result<bool, DbError> {
        snapshot.validate()?;
        if snapshot
            .metadata
            .coverage
            .commits()
            .get(&candidate.coord.stream_id)
            .is_none_or(|covered| covered.coord.sequence() < candidate.coord.sequence())
        {
            return Ok(false);
        }
        let active = super::active_store_publication::load_active_store_publication_on(self.conn)?
            .ok_or_else(|| {
                DbError::Message(
                    "covered unresolved write has no durable publication reservation".into(),
                )
            })?;
        let (reserved_id, registration, coord) = active.commit_reservation().ok_or_else(|| {
            DbError::Message("covered unresolved write has no reserved author position".into())
        })?;
        let stream = coven_protocol::store_commit::StreamActivation::device_authorized_stream_id(
            snapshot.store_root.store_root_hash,
            registration,
            coven_protocol::store_commit::StreamAnchorDomain::StoreAnnouncements,
        );
        if active.owner() != &crate::ActiveStorePublicationOwner::StoreWrite(write_id.clone())
            || reserved_id != write_id
            || coord != &candidate.coord
            || stream != coord.stream_id
            || active.attempt()?.entry.author_registration != *registration
            || active.attempt()?.entry.payload
                != coven_protocol::store_commit::StorePublicationPayload::Commit(candidate.clone())
        {
            return Err(DbError::Message(
                "covered unresolved write differs from its durable publication reservation".into(),
            ));
        }
        Ok(true)
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
        crate::store::host_sql_reads::HostSqlReads::new(self.conn).read(read)
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

    /// One write's durable status.
    ///
    /// A write is in the journal only while something about it is still worth
    /// holding, and the two ways a row leaves both mean the same thing. A
    /// capture that partitioned into nothing is never journalled, and a
    /// replay-baseline advance that absorbs a local-only write drops its row —
    /// while a write that reached the cloud, or was reversed, keeps its receipt
    /// through the advance. So an absent row says the write never left this
    /// device, which is what `LocalOnly` says.
    pub(super) fn write_status(self, write_id: &WriteId) -> Result<WriteStatus, DbError> {
        use rusqlite::OptionalExtension;
        let raw: Option<String> = self
            .conn
            .query_row(
                "SELECT status FROM store_writes WHERE write_id = ?1",
                [write_id.as_str()],
                |row| row.get(0),
            )
            .optional()
            .map_err(DbError::from)?;
        let Some(raw) = raw else {
            return Ok(WriteStatus::LocalOnly);
        };
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
        let frontier = super::materialized_commit_index::snapshot_coverage_on(self.conn)?;
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
            .map_err(|error| DbError::context("stored author stream id", error))?;
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
        .map_err(DbError::from)
    }

    pub(super) fn stage_owner_recovery_publication(
        self,
        registration_hash: &str,
        encoded: &str,
    ) -> Result<(), DbError> {
        self.conn
            .execute(
                "INSERT INTO local_owner_recovery_publication
                     (singleton, registration_hash, publication)
                 VALUES (1, ?1, ?2)
                 ON CONFLICT(singleton) DO NOTHING",
                (registration_hash, encoded),
            )
            .map_err(DbError::from)?;
        let stored: (String, String) = self
            .conn
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
        Ok(())
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

    pub(super) fn published_store_snapshot(
        self,
        root: &StoreRootRef,
        lookup: &mut dyn VerifiedRegistrationLookup,
    ) -> Result<Option<PublishedStoreSnapshot>, DbError> {
        self.conn
            .query_row(
                "SELECT publication_position, snapshot_ref, meta_bytes \
             FROM published_store_snapshot ORDER BY publication_position DESC LIMIT 1",
                [],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(DbError::from)?
            .map(|row| self.parse_published_store_snapshot(row, root, lookup))
            .transpose()
    }

    pub(super) fn published_store_snapshots(
        self,
        root: &StoreRootRef,
        lookup: &mut dyn VerifiedRegistrationLookup,
    ) -> Result<Vec<PublishedStoreSnapshot>, DbError> {
        let mut statement = self
            .conn
            .prepare(
                "SELECT publication_position, snapshot_ref, meta_bytes \
                 FROM published_store_snapshot ORDER BY publication_position DESC",
            )
            .map_err(DbError::from)?;
        let snapshots = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                ))
            })
            .map_err(DbError::from)?
            .map(|row| {
                self.parse_published_store_snapshot(row.map_err(DbError::from)?, root, lookup)
            })
            .collect();
        snapshots
    }

    fn parse_published_store_snapshot(
        self,
        (position, reference, bytes): (i64, String, Vec<u8>),
        root: &StoreRootRef,
        lookup: &mut dyn VerifiedRegistrationLookup,
    ) -> Result<PublishedStoreSnapshot, DbError> {
        let position = u64::try_from(position).map_err(|_| {
            DbError::Message("published Store snapshot position is negative".to_string())
        })?;
        let reference: StoreSnapshotRef = serde_json::from_str(&reference)
            .map_err(|error| DbError::context("published Store snapshot ref", error))?;
        let unverified: SnapshotMeta = serde_json::from_slice(&bytes)
            .map_err(|error| DbError::context("published Store snapshot author", error))?;
        let author_ref = &unverified.author_registration;
        let author = lookup.activated_registration_on(self, root, author_ref)?;
        let meta = SnapshotMeta::parse_at(&bytes, root.store_root_hash, &reference, &author)
            .map_err(|error| DbError::context("published Store snapshot", error))?;
        if &meta.author_registration != author_ref
            || meta.publication_predecessor.next_position()?.get() != position
        {
            return Err(DbError::Message(
                "published Store snapshot differs from its accepted publication position"
                    .to_string(),
            ));
        }
        Ok(PublishedStoreSnapshot { reference, meta })
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
        .map_err(|error| DbError::context("install retained replay authority", error))?;
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
    pub(super) fn received_snapshot_image_with_local_rows(
        self,
        local_image: &[u8],
        gates: &crate::Gates,
        covered_suffix: &[crate::MergeReplayWriteEffect],
    ) -> Result<Vec<u8>, DbError> {
        self.received_snapshot_image_with_local_rows_records(local_image, gates, covered_suffix)
    }
}

impl StoreTransaction<'_, '_> {
    pub(super) fn import_snapshot_device_states(
        self,
        source: StoreRecords<'_>,
    ) -> Result<(), DbError> {
        self.import_snapshot_device_state_records(source)
    }

    pub(super) fn import_received_snapshot_inputs(
        self,
        source: StoreRecords<'_>,
        inputs: &[crate::OwnedVerifiedMergeMaterialization],
        baseline: &crate::RetainedReplayBaseline,
    ) -> Result<(), DbError> {
        self.import_received_snapshot_inputs_records(source, inputs, baseline)
    }

    pub(super) fn import_received_snapshot_blob_inventory(
        self,
        source: StoreRecords<'_>,
    ) -> Result<(), DbError> {
        self.import_received_snapshot_blob_inventory_records(source)
    }

    pub(super) fn replace_received_snapshot_baseline(
        self,
        source: StoreRecords<'_>,
        baseline: &crate::RetainedReplayBaseline,
        image: Vec<u8>,
        folded: &[crate::SettledStoreWrite],
        retained_inputs: &[crate::OwnedVerifiedMergeMaterialization],
        blob_decls: &crate::BlobDecls,
    ) -> Result<crate::RetainedReplayBaseline, DbError> {
        self.replace_received_snapshot_baseline_records(
            source,
            baseline,
            image,
            folded,
            retained_inputs,
            blob_decls,
        )
    }
}

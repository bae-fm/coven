use coven_foundation::store_dir::StoreDir;
use coven_protocol::remote_object::{remote_object_id, RemoteObjectRecord};
use coven_protocol::store_commit::ObjectHash;
use coven_protocol::write::{AffectedRow, WriteId, WriteResolution, WriteStatus};
use rusqlite::Connection;
use tracing::warn;

use super::candidate_records::{
    load_merge_candidate_head_cleanup_on, parse_prepared_merge_candidate_on,
    MergeCandidateHeadCleanup,
};
use super::payload_spool::{
    read_payload_blocking, read_verified_payload_blocking, write_payload_blocking,
    PayloadSpoolError,
};
use super::publication_state::PreparedStoreWriteState;
use super::{StoreRecords, StoreTransaction};
use crate::{
    candidate_graph_exact_objects, is_routing_table, load_remote_object_on, AudiencePartition,
    CirclePartitionControl, Database, DbError, StoreWriteBase, StoreWriteBlobFacts,
};

mod circle_bootstrap;
mod retained_replay;
mod snapshot_install;

impl<'store> StoreRecords<'store> {
    pub(super) fn new(conn: &'store Connection, store_dir: &'store StoreDir) -> Self {
        Self { conn, store_dir }
    }

    pub(crate) fn payload(&self, hash: ObjectHash) -> Result<Vec<u8>, PayloadSpoolError> {
        read_payload_blocking(self.store_dir, hash)
    }

    pub(crate) fn verified_payload(&self, hash: ObjectHash) -> Result<Vec<u8>, PayloadSpoolError> {
        read_verified_payload_blocking(self.store_dir, hash)
    }

    pub(crate) fn install_payload(&self, bytes: &[u8]) -> Result<ObjectHash, PayloadSpoolError> {
        write_payload_blocking(self.store_dir, bytes)
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
}

struct UnpublishedWriteCleanup {
    removable: Vec<ObjectHash>,
    candidate: Option<coven_protocol::store_commit::StoreBatchCommitRef>,
}

impl<'store, 'connection> StoreTransaction<'store, 'connection> {
    pub(super) fn new(
        transaction: &'store rusqlite::Transaction<'connection>,
        store_dir: &'store StoreDir,
    ) -> Self {
        Self {
            transaction,
            records: StoreRecords::new(transaction, store_dir),
        }
    }

    pub(crate) fn install_payload(&self, bytes: &[u8]) -> Result<ObjectHash, PayloadSpoolError> {
        self.records.install_payload(bytes)
    }

    pub(crate) fn payload(&self, hash: ObjectHash) -> Result<Vec<u8>, PayloadSpoolError> {
        self.records.payload(hash)
    }

    pub(crate) fn store_write_partitions(
        self,
        write_id: &str,
    ) -> Result<crate::PreparedStoreWritePartitions, DbError> {
        self.records.store_write_partitions(write_id)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn begin_blocked_merge_candidate_nonactivation(
        self,
        retained: &mut dyn super::verified_store_authority::VerifiedStoreLookup,
        root: &coven_protocol::store_commit::StoreRootRef,
        write_id: &WriteId,
        candidate: &super::candidate_records::PreparedMergeCandidate,
        nonactivation: &super::candidate_records::BlockedMergeCandidateNonactivation,
        include_indexed_blobs: bool,
        extra_objects: &[coven_protocol::objects::ExactObjectRef],
    ) -> Result<(), DbError> {
        if let super::candidate_records::BlockedMergeCandidateNonactivation::Terminal {
            durable,
            ..
        } = nonactivation
        {
            super::candidate_records::validate_terminal_candidate_authority_on(
                self.records,
                retained,
                root,
                candidate,
                durable,
            )?;
        }
        match nonactivation {
            super::candidate_records::BlockedMergeCandidateNonactivation::Merge(durable) => {
                super::candidate_records::begin_merge_candidate_nonactivation_on(
                    self.transaction,
                    write_id,
                    candidate,
                    durable,
                    include_indexed_blobs,
                    extra_objects,
                )
            }
            super::candidate_records::BlockedMergeCandidateNonactivation::Terminal {
                durable,
                head_nonactivation,
            } => {
                super::candidate_records::begin_merge_candidate_nonactivation_with_verified_head_on(
                    self.transaction,
                    write_id,
                    candidate,
                    durable,
                    include_indexed_blobs,
                    extra_objects,
                    head_nonactivation,
                )
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn advance_owner_promotion_journal(
        self,
        journal_key: String,
        target_key: String,
        previous_value: String,
        next_value: String,
        remote_objects: Vec<coven_protocol::remote_object::ClosedRemoteObject>,
    ) -> Result<(), DbError> {
        super::owner_promotion::advance_owner_promotion_journal_on(
            self.transaction,
            self.records.store_dir,
            journal_key,
            target_key,
            previous_value,
            next_value,
            remote_objects,
        )
    }

    pub(super) fn replace_tables_from_projection(
        self,
        source: &super::ReplayProjection,
        tables: &[String],
    ) -> Result<(), DbError> {
        super::replay_projection::replace_tables_from_projection_on(
            source,
            self.transaction,
            tables,
        )
    }

    pub(crate) fn insert_store_write(
        self,
        write_id: &WriteId,
        partitions: &[AudiencePartition],
        changeset_hash: ObjectHash,
        base: &StoreWriteBase,
        blob_facts: &StoreWriteBlobFacts,
        rows_changed: u64,
    ) -> Result<WriteStatus, DbError> {
        let tx = self.records.conn;
        let remote_partitions = partitions
            .iter()
            .filter(|partition| partition.audience != coven_protocol::circle::Audience::Local)
            .collect::<Vec<_>>();
        let affected_rows = if remote_partitions.is_empty() {
            if partitions.is_empty() && rows_changed == 0 {
                warn!("journaled sql transaction changed nothing; pure reads belong on sql_read");
                #[cfg(debug_assertions)]
                warn!(
                    "zero-change sql transaction backtrace:\n{}",
                    std::backtrace::Backtrace::force_capture()
                );
            }
            Vec::new()
        } else {
            let mut affected = Vec::new();
            for partition in &remote_partitions {
                affected.extend(
                    crate::walk_changeset(&partition.changeset)
                        .map_err(|error| {
                            DbError::Message(format!("read affected write rows: {error}"))
                        })?
                        .into_iter()
                        .filter(|row| !is_routing_table(&row.table))
                        .map(|row| {
                            let primary_key = row.pk().map(str::to_owned).ok_or_else(|| {
                                DbError::Message(format!(
                                    "shared write row in {:?} has no primary key",
                                    row.table
                                ))
                            })?;
                            Ok(AffectedRow {
                                table: row.table,
                                primary_key,
                            })
                        })
                        .collect::<Result<Vec<_>, DbError>>()?,
                );
            }
            affected.sort();
            affected.dedup();
            affected
        };
        let status = if remote_partitions.is_empty() {
            WriteStatus::LocalOnly
        } else {
            WriteStatus::Pending
        };
        let base = serde_json::to_string(base)
            .map_err(|error| DbError::context("serialize pending Store base", error))?;
        let status_json = serde_json::to_string(&status)
            .map_err(|error| DbError::context("serialize write status", error))?;
        let affected_rows = serde_json::to_string(&affected_rows)
            .map_err(|error| DbError::context("serialize affected rows", error))?;
        let blob_facts_json = serde_json::to_string(blob_facts)
            .map_err(|error| DbError::context("serialize Store write blob facts", error))?;
        tx.execute(
            "INSERT INTO store_writes
             (write_id, status, affected_rows, changeset_hash, base, blob_facts)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![
                write_id.as_str(),
                status_json,
                affected_rows,
                changeset_hash.to_string(),
                base,
                blob_facts_json,
            ],
        )
        .map_err(DbError::from)?;
        let mut payloads = std::collections::BTreeSet::from([changeset_hash]);
        for partition in partitions {
            let audience = match partition.audience {
                coven_protocol::circle::Audience::Store => "store".to_string(),
                coven_protocol::circle::Audience::Local => "local".to_string(),
                coven_protocol::circle::Audience::Circle(circle_id) => circle_id.to_string(),
            };
            let control = partition
                .control
                .as_ref()
                .map(CirclePartitionControl::stored_json);
            let partition_hash = self.install_payload(&partition.changeset)?;
            payloads.insert(partition_hash);
            tx.execute(
                "INSERT INTO store_write_partitions
                 (write_id, audience, control_coord, changeset_hash)
                 VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![
                    write_id.as_str(),
                    audience,
                    control,
                    partition_hash.to_string()
                ],
            )
            .map_err(DbError::from)?;
        }
        crate::payload_spool::set_payload_owner_claims_on(
            tx,
            &crate::payload_spool::store_write_owner_key(write_id),
            &payloads,
        )?;
        if status == WriteStatus::Pending {
            for fact in &blob_facts.blobs {
                if fact.blob.provenance != coven_protocol::blob::Provenance::HostProvided {
                    continue;
                }
                tx.execute(
                    "INSERT OR IGNORE INTO store_write_blob_leases
                     (write_id, namespace, blob_id) VALUES (?1, ?2, ?3)",
                    (write_id.as_str(), &fact.blob.namespace, &fact.blob.id),
                )
                .map_err(DbError::from)?;
            }
        }
        Ok(status)
    }

    fn unpublished_write_cleanup(
        self,
        authority: &mut super::VerifiedStoreAuthority,
        write_id: &WriteId,
    ) -> Result<UnpublishedWriteCleanup, DbError> {
        let tx = self.records.conn;
        let raw_prepared: Option<String> = tx
            .query_row(
                "SELECT prepared FROM store_writes WHERE write_id = ?1",
                [write_id.as_str()],
                |row| row.get(0),
            )
            .map_err(DbError::from)?;
        let mut removable = Vec::new();
        let mut candidate = None;
        if let Some(raw_prepared) = raw_prepared.as_deref() {
            let prepared: PreparedStoreWriteState = serde_json::from_str(raw_prepared)
                .map_err(|error| DbError::context("resolved prepared write", error))?;
            let merge = parse_prepared_merge_candidate_on(self.records, authority, &prepared)?;
            removable.push(remote_object_id(&merge.reference.object));
            match load_merge_candidate_head_cleanup_on(tx, &merge.head_object, &merge.reference)? {
                MergeCandidateHeadCleanup::Remote { .. } => {
                    removable.push(remote_object_id(&merge.head_object))
                }
                MergeCandidateHeadCleanup::ProtocolInert => {}
            }
            removable.extend(
                candidate_graph_exact_objects(&merge.commit)?
                    .iter()
                    .map(remote_object_id),
            );
            candidate = Some(merge.reference);
        }
        let mut statement = tx
            .prepare("SELECT remote_object_id FROM store_write_blobs WHERE write_id = ?1")
            .map_err(DbError::from)?;
        let indexed = statement
            .query_map([write_id.as_str()], |row| row.get::<_, String>(0))
            .map_err(DbError::from)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(DbError::from)?;
        drop(statement);
        for encoded in indexed {
            removable.push(
                encoded
                    .parse()
                    .map_err(|error| DbError::context("resolved remote object id", error))?,
            );
        }
        Ok(UnpublishedWriteCleanup {
            removable,
            candidate,
        })
    }

    fn unpublished_write_cleanup_complete(
        self,
        cleanup: &UnpublishedWriteCleanup,
    ) -> Result<bool, DbError> {
        let Some(candidate) = &cleanup.candidate else {
            return Ok(true);
        };
        for object_id in &cleanup.removable {
            let remote = load_remote_object_on(self.records.conn, *object_id)?;
            if !remote
                .candidate_cleanup_complete(candidate)
                .map_err(|error| {
                    DbError::context(format!("validate candidate cleanup for {object_id}"), error)
                })?
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(super) fn unpublished_write_cleanup_is_complete(
        self,
        authority: &mut super::VerifiedStoreAuthority,
        write_id: &WriteId,
    ) -> Result<bool, DbError> {
        let cleanup = self.unpublished_write_cleanup(authority, write_id)?;
        self.unpublished_write_cleanup_complete(&cleanup)
    }

    pub(super) fn resolve_unpublished_writes(
        self,
        authority: &mut super::VerifiedStoreAuthority,
        write_ids: &[WriteId],
        resolution: &WriteResolution,
    ) -> Result<(), DbError> {
        let tx = self.records.conn;
        let status = WriteStatus::Resolved(resolution.clone());
        for write_id in write_ids {
            let cleanup = self.unpublished_write_cleanup(authority, write_id)?;
            if !self.unpublished_write_cleanup_complete(&cleanup)? {
                return Err(DbError::Message(format!(
                    "candidate cleanup for write {write_id} is incomplete"
                )));
            }
            tx.execute(
                "DELETE FROM store_write_blob_leases WHERE write_id = ?1",
                [write_id.as_str()],
            )
            .map_err(DbError::from)?;
            tx.execute(
                "DELETE FROM store_write_packages WHERE write_id = ?1",
                [write_id.as_str()],
            )
            .map_err(DbError::from)?;
            tx.execute(
                "DELETE FROM store_write_blobs WHERE write_id = ?1",
                [write_id.as_str()],
            )
            .map_err(DbError::from)?;
            for object_id in cleanup.removable {
                let remote = load_remote_object_on(tx, object_id)?;
                let absent = matches!(
                    remote,
                    RemoteObjectRecord::CandidateCommit(
                        coven_protocol::remote_object::CandidateCommitRecord {
                            state:
                                coven_protocol::remote_object::CandidateCommitState::AbsentVerified { .. },
                            ..
                        }
                    ) | RemoteObjectRecord::CandidateExclusive(
                        coven_protocol::remote_object::CandidateObjectRecord {
                            state:
                                coven_protocol::remote_object::CandidateObjectState::AbsentVerified { .. },
                            ..
                        }
                    ) | RemoteObjectRecord::RetainedAuthority(
                        coven_protocol::remote_object::RetainedAuthorityRecord {
                            state:
                                coven_protocol::remote_object::RetainedAuthorityObjectState::UncreatedVerified { .. },
                            ..
                        }
                    )
                );
                if absent {
                    crate::remote_object_records::delete_remote_object_on(tx, object_id)?;
                }
            }
            tx.execute(
                "UPDATE store_writes SET prepared = NULL WHERE write_id = ?1",
                [write_id.as_str()],
            )
            .map_err(DbError::from)?;
            Database::set_write_status_on(tx, write_id, &status)?;
        }
        Ok(())
    }
}

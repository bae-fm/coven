use super::*;
use coven_protocol::store_commit::{
    ReferencedStoreDeviceRegistration, StoreDeviceRegistrationRef, StoreRootRef,
};

mod provider_probe;

pub(crate) mod acknowledgements;
pub(crate) mod blob_bindings;
pub(crate) mod blob_outbox;
pub(crate) mod blob_transitions;
pub(crate) mod candidate_lifecycle;
pub mod candidate_records;
pub(crate) mod circle_acknowledgements;
pub(crate) mod circle_authority;
pub(crate) mod circle_controls;
pub(crate) mod circle_operation_discard;
pub(crate) mod circle_operations;
pub(crate) mod circle_snapshot_publication;
pub(crate) mod device_continuation;
pub(crate) mod device_exclusion;
pub(crate) mod device_join_challenges;
pub(crate) mod device_registration_journal;
pub(crate) mod host_write_capture;
pub(crate) mod host_write_operation;
pub(crate) mod local_blob_cleanup;
pub(crate) mod materialization;
pub(crate) mod materialized_commit_index;
pub(crate) mod membership_mutations;
pub(crate) mod membership_rotation;
pub(crate) mod merge_materialization_transaction;
pub(crate) mod owner_promotion;
pub(crate) mod owner_recovery_publication;
pub(crate) mod payload_store;
pub(crate) mod pending_publication;
pub(crate) mod preparation;
pub(crate) mod prepared_remote_objects;
pub(crate) mod publication;
pub(crate) mod pull_replay;
pub mod reclaim;
pub(crate) mod replay_projection;
pub(crate) mod retained_merge_replay;
pub(crate) mod retained_replay;
pub(crate) mod snapshot_image;
use snapshot_image::snapshot_image_db_error;
pub(crate) mod snapshot_publication;
pub(crate) mod store_acknowledgements;
pub(crate) mod store_authority;
pub(crate) mod store_records;
pub(crate) mod stream_activation_records;
#[cfg(any(test, feature = "test-utils"))]
pub(crate) mod test_support;
pub(crate) mod verified_store_authority;
pub(crate) mod write_lifecycle;

/// One Store's row connection and matching payload storage.
///
/// Payload records may hold bytes in SQLite or name a file beside it, so record
/// operations carry the connection and directory as one scoped value.
#[derive(Clone, Copy)]
pub(crate) struct StoreRecords<'store> {
    conn: &'store rusqlite::Connection,
    store_dir: &'store coven_foundation::store_dir::StoreDir,
}

/// One Store transaction and its matching row-and-payload capability.
///
/// Payloads land before the row naming them commits, while ownership claims
/// land in this transaction. Keeping both borrows together prevents a record
/// mutation from using another Store's payload directory.
#[derive(Clone, Copy)]
pub(crate) struct StoreTransaction<'store, 'connection> {
    transaction: &'store rusqlite::Transaction<'connection>,
    records: StoreRecords<'store>,
}

/// One Store SQL transaction and the authority facts staged beside it.
///
/// StoreSession owns commit and rollback. Operations borrow this capability so
/// no workflow can promote verified cache state independently from its rows.
struct VerifiedStoreTransaction<'transaction, 'connection, 'authority> {
    store: StoreTransaction<'transaction, 'connection>,
    authority: &'authority mut verified_store_authority::VerifiedStoreAuthorityTransaction,
    gates: &'authority crate::Gates,
    synced_tables: &'authority [coven_protocol::synced_schema::SyncedTable],
    blob_decls: &'authority crate::BlobDecls,
    #[cfg(any(test, feature = "test-utils"))]
    merge_materialization_failure:
        &'authority std::sync::Mutex<Option<crate::MergeMaterializationFailurePoint>>,
}

enum StoreTransactionOutcome<T> {
    Commit(T),
    Rollback(T),
}

/// One connection-thread Store operation.
///
/// The Store implementation can combine its row-and-payload capability with
/// authority verified by the same connection. Code outside this module cannot
/// obtain either retained dependency.
pub(crate) struct StoreSession<'session> {
    records: StoreRecords<'session>,
    verified_store_authority: &'session mut VerifiedStoreAuthority,
    gates: &'session crate::Gates,
    synced_tables: &'session [coven_protocol::synced_schema::SyncedTable],
    schema_version: u32,
    sync_routing_hash: coven_protocol::store_commit::ObjectHash,
    hlc: &'session std::sync::Arc<coven_protocol::hlc::Hlc>,
    blob_decls: &'session crate::BlobDecls,
    #[cfg(any(test, feature = "test-utils"))]
    merge_materialization_failure:
        &'session std::sync::Mutex<Option<crate::MergeMaterializationFailurePoint>>,
}

pub(crate) fn install_verified_snapshot_bootstrap_on(
    transaction: &rusqlite::Transaction<'_>,
    store_dir: &coven_foundation::store_dir::StoreDir,
    install: &crate::VerifiedSnapshotBootstrapInstall,
    schema_version: u32,
    routing_hash: coven_protocol::store_commit::ObjectHash,
    synced_tables: &[coven_protocol::synced_schema::SyncedTable],
) -> Result<(), DbError> {
    StoreTransaction::new(transaction, store_dir).install_verified_snapshot_bootstrap(
        install,
        schema_version,
        routing_hash,
        synced_tables,
    )
}

#[cfg(any(test, feature = "test-utils"))]
pub(crate) fn circle_bootstrap_replay_inputs_for_test(
    connection: &rusqlite::Connection,
    store_dir: &coven_foundation::store_dir::StoreDir,
) -> Result<
    Vec<(
        StoreBatchCommitRef,
        coven_protocol::circle_activation::VerifiedCircleImage,
    )>,
    DbError,
> {
    StoreDatabase::circle_bootstrap_replay_inputs_on(StoreRecords::new(connection, store_dir))
}

#[cfg(any(test, feature = "test-utils"))]
pub(crate) fn retained_merge_replay_inputs_for_test(
    connection: &rusqlite::Connection,
    store_dir: &coven_foundation::store_dir::StoreDir,
    root: &coven_protocol::store_commit::StoreRootRef,
) -> Result<Vec<OwnedVerifiedMergeMaterialization>, DbError> {
    let mut authority = VerifiedStoreAuthority::default();
    StoreDatabase::load_retained_merge_replay_inputs_on(
        StoreRecords::new(connection, store_dir),
        root,
        &mut authority,
    )
}

impl<'session> StoreSession<'session> {
    pub(crate) fn new(
        conn: &'session rusqlite::Connection,
        store_dir: &'session coven_foundation::store_dir::StoreDir,
        verified_store_authority: &'session mut VerifiedStoreAuthority,
        gates: &'session crate::Gates,
        synced_tables: &'session [coven_protocol::synced_schema::SyncedTable],
        schema_version: u32,
        sync_routing_hash: coven_protocol::store_commit::ObjectHash,
        hlc: &'session std::sync::Arc<coven_protocol::hlc::Hlc>,
        blob_decls: &'session crate::BlobDecls,
        #[cfg(any(test, feature = "test-utils"))]
        merge_materialization_failure: &'session std::sync::Mutex<
            Option<crate::MergeMaterializationFailurePoint>,
        >,
    ) -> Self {
        Self {
            records: StoreRecords::new(conn, store_dir),
            verified_store_authority,
            gates,
            synced_tables,
            schema_version,
            sync_routing_hash,
            hlc,
            blob_decls,
            #[cfg(any(test, feature = "test-utils"))]
            merge_materialization_failure,
        }
    }

    pub(super) fn required_root_authority(&mut self) -> Result<StoreRootRef, DbError> {
        self.verified_store_authority
            .required_root_authority_on(self.records)
    }

    pub(super) fn root_authority(
        &mut self,
    ) -> Result<
        Option<(
            StoreRootRef,
            coven_protocol::store_commit::StoreProtocolRoot,
        )>,
        DbError,
    > {
        self.verified_store_authority
            .root_authority_on(self.records)
    }

    pub(super) fn activated_registration(
        &mut self,
        reference: &StoreDeviceRegistrationRef,
    ) -> Result<ReferencedStoreDeviceRegistration, DbError> {
        let root = self.required_root_authority()?;
        let registration = self.verified_store_authority.activated_registration_on(
            self.records,
            &root,
            reference,
        )?;
        ReferencedStoreDeviceRegistration::verified(reference.clone(), registration)
            .map_err(|error| DbError::Message(error.to_string()))
    }

    pub(super) fn local_store_authority(
        &mut self,
    ) -> Result<ReferencedStoreDeviceRegistration, DbError> {
        self.verified_store_authority
            .local_store_authority_on(self.records)
    }

    fn verified_store_transaction<R>(
        &mut self,
        operation: impl FnOnce(
            &mut VerifiedStoreTransaction<'_, '_, '_>,
        ) -> Result<StoreTransactionOutcome<R>, DbError>,
    ) -> Result<R, DbError> {
        let transaction = self
            .records
            .conn
            .unchecked_transaction()
            .map_err(DbError::from)?;
        let store = StoreTransaction::new(&transaction, self.records.store_dir);
        let mut authority =
            store.begin_verified_authority_transaction(self.verified_store_authority)?;
        let outcome = {
            let mut capability = VerifiedStoreTransaction {
                store,
                authority: &mut authority,
                gates: self.gates,
                synced_tables: self.synced_tables,
                blob_decls: self.blob_decls,
                #[cfg(any(test, feature = "test-utils"))]
                merge_materialization_failure: self.merge_materialization_failure,
            };
            operation(&mut capability)
        };
        match outcome {
            Ok(StoreTransactionOutcome::Commit(value)) => {
                transaction.commit().map_err(DbError::from)?;
                self.verified_store_authority.commit_transaction(authority);
                Ok(value)
            }
            Ok(StoreTransactionOutcome::Rollback(value)) => {
                transaction.rollback().map_err(DbError::from)?;
                Ok(value)
            }
            Err(error) => Err(error),
        }
    }

    pub(super) fn read<F, R, E>(&self, read: F) -> Result<Result<R, E>, DbError>
    where
        F: for<'connection> FnOnce(SqlReadContext<'connection>) -> Result<R, E>,
    {
        let authorization = host_sql_transaction::HostSqlAuthorization::begin(self.records.conn)?;
        Ok(authorization.run(|| read(SqlReadContext::new(self.records.conn))))
    }

    pub(super) fn protocol_state(&self, key: &str) -> Result<Option<String>, DbError> {
        crate::get_protocol_state_on(self.records.conn, key)
    }

    pub(super) fn set_protocol_state(&self, key: &str, value: &str) -> Result<(), DbError> {
        crate::set_protocol_state_on(self.records.conn, key, value)
    }

    pub(super) fn write_status(
        &self,
        write_id: &coven_protocol::write::WriteId,
    ) -> Result<coven_protocol::write::WriteStatus, DbError> {
        let raw: String = self
            .records
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

    pub(super) fn begin_store_creation_attempt(
        &self,
        value: &str,
    ) -> Result<coven_protocol::store_creation::StoreCreationAttempt, DbError> {
        let tx = self
            .records
            .conn
            .unchecked_transaction()
            .map_err(DbError::from)?;
        tx.execute(
            "INSERT INTO protocol_state (key, value) VALUES (?1, ?2)
             ON CONFLICT(key) DO NOTHING",
            (
                coven_protocol::store_creation::STORE_CREATION_ATTEMPT_STATE_KEY,
                value,
            ),
        )
        .map_err(DbError::from)?;
        let actual = crate::required_protocol_state_on(
            &tx,
            coven_protocol::store_creation::STORE_CREATION_ATTEMPT_STATE_KEY,
        )?;
        tx.commit().map_err(DbError::from)?;
        serde_json::from_str(&actual)
            .map_err(|error| DbError::context("parse Store creation attempt", error))
    }

    pub(super) fn load_store_creation_attempt(
        &self,
    ) -> Result<Option<coven_protocol::store_creation::StoreCreationAttempt>, DbError> {
        self.protocol_state(coven_protocol::store_creation::STORE_CREATION_ATTEMPT_STATE_KEY)?
            .map(|value| {
                serde_json::from_str(&value)
                    .map_err(|error| DbError::context("parse Store creation attempt", error))
            })
            .transpose()
    }

    pub(super) fn advance_store_creation_attempt(
        &self,
        previous: &str,
        next: &str,
    ) -> Result<(), DbError> {
        let changed = self
            .records
            .conn
            .execute(
                "UPDATE protocol_state SET value = ?1 WHERE key = ?2 AND value = ?3",
                (
                    next,
                    coven_protocol::store_creation::STORE_CREATION_ATTEMPT_STATE_KEY,
                    previous,
                ),
            )
            .map_err(DbError::from)?;
        if changed != 1 {
            return Err(DbError::Message(
                "Store creation attempt advance lost its exact predecessor".to_string(),
            ));
        }
        Ok(())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(super) fn scoped_snapshot_counts(&self) -> Result<(i64, i64, i64), DbError> {
        self.records
            .conn
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
    pub(super) fn migrated_scoped_snapshot_facts(&self) -> Result<(i64, i64, String), DbError> {
        self.records
            .conn
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
        &self,
        circle_id: coven_protocol::circle::CircleId,
    ) -> Result<Option<coven_protocol::circle::CircleBootstrapCoverageRef>, DbError> {
        retained_merge_replay::circle_bootstrap_coverage_ref_on(self.records.conn, circle_id)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(super) fn circle_control_activation_count(
        &self,
        circle_id: coven_protocol::circle::CircleId,
    ) -> Result<i64, DbError> {
        self.records
            .conn
            .query_row(
                "SELECT COUNT(*) FROM circle_control_activations WHERE circle_id = ?1",
                [circle_id.to_string()],
                |row| row.get(0),
            )
            .map_err(DbError::from)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(super) fn generation_zero_replay_baseline(
        &self,
    ) -> Result<crate::RetainedReplayBaseline, DbError> {
        StoreDatabase::generation_zero_replay_baseline_on(StoreRecords::new(
            self.records.conn,
            self.records.store_dir,
        ))
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(super) fn replace_generation_zero_replay_authority(
        &self,
        authority_bytes: &[u8],
    ) -> Result<(), DbError> {
        let transaction = self
            .records
            .conn
            .unchecked_transaction()
            .map_err(DbError::from)?;
        let authority_hash = payload_store::write_payload_blocking(
            &transaction,
            self.records.store_dir,
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
        let image_hash: String = transaction
            .query_row(
                "SELECT image_hash FROM retained_replay_baselines WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .map_err(DbError::from)?;
        payload_store::set_payload_owner_claims_on(
            &transaction,
            payload_store::RETAINED_REPLAY_BASELINE_OWNER_KEY,
            &std::collections::BTreeSet::from([image_hash.parse()?, authority_hash]),
        )?;
        transaction.commit().map_err(DbError::from)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(super) fn circle_bootstrap_replay_inputs(
        &self,
    ) -> Result<
        Vec<(
            StoreBatchCommitRef,
            coven_protocol::circle_activation::VerifiedCircleImage,
        )>,
        DbError,
    > {
        StoreDatabase::circle_bootstrap_replay_inputs_on(StoreRecords::new(
            self.records.conn,
            self.records.store_dir,
        ))
    }
}

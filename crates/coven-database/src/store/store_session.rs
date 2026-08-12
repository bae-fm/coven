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
pub(crate) use store_records::StoreRecords;
mod store_transaction;
pub(crate) mod stream_activation_records;
#[cfg(any(test, feature = "test-utils"))]
pub(crate) mod test_support;
pub(crate) mod verified_store_authority;
pub(crate) mod write_lifecycle;

/// One Store transaction and its matching row-and-payload capability.
///
/// Payloads land before the row naming them commits, while ownership claims
/// land in this transaction. Keeping both borrows together prevents a record
/// mutation from using another Store's payload directory.
#[derive(Clone, Copy)]
pub(crate) struct StoreTransaction<'store, 'connection> {
    transaction: &'store rusqlite::Transaction<'connection>,
    store_dir: &'store coven_foundation::store_dir::StoreDir,
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
            conn,
            store_dir,
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
            .required_root_authority_on(StoreRecords::new(self.conn, self.store_dir))
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
            .root_authority_on(StoreRecords::new(self.conn, self.store_dir))
    }

    pub(super) fn activated_registration(
        &mut self,
        reference: &StoreDeviceRegistrationRef,
    ) -> Result<ReferencedStoreDeviceRegistration, DbError> {
        let root = self.required_root_authority()?;
        let registration = self.verified_store_authority.activated_registration_on(
            StoreRecords::new(self.conn, self.store_dir),
            &root,
            reference,
        )?;
        ReferencedStoreDeviceRegistration::verified(reference.clone(), registration)
            .map_err(DbError::from)
    }

    pub(super) fn local_store_authority(
        &mut self,
    ) -> Result<ReferencedStoreDeviceRegistration, DbError> {
        self.verified_store_authority
            .local_store_authority_on(StoreRecords::new(self.conn, self.store_dir))
    }

    fn verified_store_transaction<R>(
        &mut self,
        operation: impl FnOnce(
            &mut VerifiedStoreTransaction<'_, '_, '_>,
        ) -> Result<StoreTransactionOutcome<R>, DbError>,
    ) -> Result<R, DbError> {
        let mut committed_authority = None;
        let value = StoreRecords::new(self.conn, self.store_dir).transaction(|store| {
            let mut authority =
                store.begin_verified_authority_transaction(self.verified_store_authority)?;
            let mut capability = VerifiedStoreTransaction {
                store,
                authority: &mut authority,
                gates: self.gates,
                synced_tables: self.synced_tables,
                blob_decls: self.blob_decls,
                #[cfg(any(test, feature = "test-utils"))]
                merge_materialization_failure: self.merge_materialization_failure,
            };
            match operation(&mut capability)? {
                StoreTransactionOutcome::Commit(value) => {
                    committed_authority = Some(authority);
                    Ok(StoreTransactionOutcome::Commit(value))
                }
                StoreTransactionOutcome::Rollback(value) => {
                    Ok(StoreTransactionOutcome::Rollback(value))
                }
            }
        })?;
        if let Some(authority) = committed_authority {
            self.verified_store_authority.commit_transaction(authority);
        }
        Ok(value)
    }

    pub(super) fn read<F, R, E>(&self, read: F) -> Result<Result<R, E>, DbError>
    where
        F: for<'connection> FnOnce(SqlReadContext<'connection>) -> Result<R, E>,
    {
        StoreRecords::new(self.conn, self.store_dir).host_sql_read(read)
    }

    pub(super) fn read_tracked<F, R, E>(
        &self,
        read: F,
    ) -> Result<(Result<R, E>, crate::live_query::QueryDependencies), DbError>
    where
        F: for<'connection> FnOnce(SqlReadContext<'connection>) -> Result<R, E>,
    {
        StoreRecords::new(self.conn, self.store_dir).host_sql_read_tracked(read)
    }

    pub(super) fn protocol_state(&self, key: &str) -> Result<Option<String>, DbError> {
        StoreRecords::new(self.conn, self.store_dir).protocol_state(key)
    }

    pub(super) fn set_protocol_state(&self, key: &str, value: &str) -> Result<(), DbError> {
        StoreRecords::new(self.conn, self.store_dir).set_protocol_state(key, value)
    }

    pub(super) fn write_status(
        &self,
        write_id: &coven_protocol::write::WriteId,
    ) -> Result<coven_protocol::write::WriteStatus, DbError> {
        StoreRecords::new(self.conn, self.store_dir).write_status(write_id)
    }

    pub(super) fn begin_store_creation_attempt(
        &self,
        value: &str,
    ) -> Result<coven_protocol::store_creation::StoreCreationAttempt, DbError> {
        let actual = StoreRecords::new(self.conn, self.store_dir).begin_protocol_state(
            coven_protocol::store_creation::STORE_CREATION_ATTEMPT_STATE_KEY,
            value,
        )?;
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
        if !StoreRecords::new(self.conn, self.store_dir).compare_exchange_protocol_state(
            coven_protocol::store_creation::STORE_CREATION_ATTEMPT_STATE_KEY,
            previous,
            next,
        )? {
            return Err(DbError::Message(
                "Store creation attempt advance lost its exact predecessor".to_string(),
            ));
        }
        Ok(())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(super) fn scoped_snapshot_counts(&self) -> Result<(i64, i64, i64), DbError> {
        StoreRecords::new(self.conn, self.store_dir).scoped_snapshot_counts()
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(super) fn migrated_scoped_snapshot_facts(&self) -> Result<(i64, i64, String), DbError> {
        StoreRecords::new(self.conn, self.store_dir).migrated_scoped_snapshot_facts()
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(super) fn circle_bootstrap_coverage_ref(
        &self,
        circle_id: coven_protocol::circle::CircleId,
    ) -> Result<Option<coven_protocol::circle::CircleBootstrapCoverageRef>, DbError> {
        StoreRecords::new(self.conn, self.store_dir).circle_bootstrap_coverage_ref(circle_id)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(super) fn circle_control_activation_count(
        &self,
        circle_id: coven_protocol::circle::CircleId,
    ) -> Result<i64, DbError> {
        StoreRecords::new(self.conn, self.store_dir).circle_control_activation_count(circle_id)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(super) fn generation_zero_replay_baseline(
        &self,
    ) -> Result<crate::RetainedReplayBaseline, DbError> {
        StoreRecords::new(self.conn, self.store_dir).generation_zero_replay_baseline()
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(super) fn replace_generation_zero_replay_authority(
        &self,
        authority_bytes: &[u8],
    ) -> Result<(), DbError> {
        StoreRecords::new(self.conn, self.store_dir)
            .replace_generation_zero_replay_authority(authority_bytes)
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
        StoreRecords::new(self.conn, self.store_dir).circle_bootstrap_replay_inputs()
    }
}

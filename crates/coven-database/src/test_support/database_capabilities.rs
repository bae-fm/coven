use crate::{Database, DatabaseTestTable, DbError};

impl Database {
    pub async fn vacuum_into_for_test(&self, destination: String) -> Result<(), DbError> {
        self.test_sql(move |database| {
            database
                .execute("VACUUM INTO ?1", [destination])
                .map(|_| ())
                .map_err(DbError::from)
        })
        .await
    }

    pub async fn table_row_count_for_test(&self, table: DatabaseTestTable) -> Result<i64, DbError> {
        self.test_sql(move |database| database.table_row_count(table))
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn install_blob_binding_for_test(
        &self,
        object_id: String,
        state: String,
        locator_hash: String,
        table: &str,
        row_id: &str,
        column: &str,
        row_stamp: &str,
        audience: String,
    ) -> Result<(), DbError> {
        let table = table.to_string();
        let row_id = row_id.to_string();
        let column = column.to_string();
        let row_stamp = row_stamp.to_string();
        self.test_sql(move |database| {
            database.install_blob_binding(
                &object_id,
                &state,
                &locator_hash,
                &table,
                &row_id,
                &column,
                &row_stamp,
                &audience,
            )
        })
        .await
    }

    pub async fn protocol_state_prefix_count_for_test(&self, prefix: &str) -> Result<i64, DbError> {
        let prefix = prefix.to_string();
        self.test_sql(move |database| database.protocol_state_prefix_count(&prefix))
            .await
    }

    pub async fn exact_row_blob_locator_count_for_test(
        &self,
        table: &str,
        row_id: &str,
        column: &str,
        stamp: &str,
    ) -> Result<i64, DbError> {
        let table = table.to_string();
        let row_id = row_id.to_string();
        let column = column.to_string();
        let stamp = stamp.to_string();
        self.test_sql(move |database| {
            database.exact_row_blob_locator_count(&table, &row_id, &column, &stamp)
        })
        .await
    }

    pub async fn exact_upload_outbox_count_for_test(
        &self,
        table: &str,
        row_id: &str,
        column: &str,
        stamp: &str,
    ) -> Result<i64, DbError> {
        let table = table.to_string();
        let row_id = row_id.to_string();
        let column = column.to_string();
        let stamp = stamp.to_string();
        self.test_sql(move |database| {
            database.exact_upload_outbox_count(&table, &row_id, &column, &stamp)
        })
        .await
    }

    pub async fn install_outbound_preparation_failure_for_test(&self) -> Result<(), DbError> {
        self.test_sql(|database| database.install_outbound_preparation_failure_trigger())
            .await
    }

    pub async fn remove_outbound_preparation_failure_for_test(&self) -> Result<(), DbError> {
        self.test_sql(|database| {
            database
                .execute_batch("DROP TRIGGER fail_outbound_preparation")
                .map_err(DbError::from)
        })
        .await
    }

    pub async fn staged_circle_acknowledgement_object_for_test(
        &self,
    ) -> Result<coven_protocol::objects::PreparedExactObject, DbError> {
        self.test_sql(|database| database.staged_circle_acknowledgement_object())
            .await
    }

    pub async fn install_owner_anchor_failure_for_test(&self) -> Result<(), DbError> {
        self.test_sql(|database| {
            database
                .execute_batch(
                    "CREATE TEMP TRIGGER fail_owner_anchor_baseline
                     BEFORE INSERT ON retained_replay_baselines
                     BEGIN
                         SELECT RAISE(ABORT, 'injected owner anchor failure');
                     END",
                )
                .map_err(DbError::from)
        })
        .await
    }

    pub async fn remove_owner_anchor_failure_for_test(&self) -> Result<(), DbError> {
        self.test_sql(|database| {
            database
                .execute_batch("DROP TRIGGER fail_owner_anchor_baseline")
                .map_err(DbError::from)
        })
        .await
    }

    pub async fn corrupt_store_device_registration_bytes_for_test(
        &self,
        registration: coven_protocol::store_commit::StoreDeviceRegistrationRef,
    ) -> Result<(), DbError> {
        self.test_sql(move |database| {
            database.corrupt_store_device_registration_bytes(&registration)
        })
        .await
    }

    pub async fn validate_retained_merge_replay_for_test(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
    ) -> Result<(), DbError> {
        self.test_sql(move |database| database.load_retained_merge_replay_inputs(&root).map(drop))
            .await
    }

    pub async fn replace_retained_merge_input_for_test(
        &self,
        stream_id: String,
        canonical_input: Vec<u8>,
    ) -> Result<(), DbError> {
        self.test_sql(move |database| {
            database.replace_retained_merge_input(&stream_id, &canonical_input)
        })
        .await
    }

    pub async fn insert_invalid_materialized_commit_for_test(&self) -> Result<(), DbError> {
        self.test_sql(|database| database.insert_invalid_materialized_commit())
            .await
    }

    pub async fn retained_materialization_input_for_test(
        &self,
        stream_id: String,
        sequence: u64,
    ) -> Result<(Vec<u8>, String, String), DbError> {
        self.test_sql(move |database| database.retained_materialization_input(&stream_id, sequence))
            .await
    }

    pub async fn retained_canonical_input_for_test(
        &self,
        stream_id: String,
        sequence: u64,
    ) -> Result<Vec<u8>, DbError> {
        self.test_sql(move |database| database.retained_canonical_input(&stream_id, sequence))
            .await
    }

    pub async fn corrupt_retained_materialization_input_for_test(
        &self,
        stream_id: String,
        sequence: u64,
    ) -> Result<(), DbError> {
        self.test_sql(move |database| {
            database.corrupt_retained_materialization_input(&stream_id, sequence)
        })
        .await
    }

    pub async fn insert_retained_replay_object_for_test(
        &self,
        owner: coven_protocol::remote_object::RetainedReplayOwner,
        object: coven_protocol::objects::ExactObjectRef,
    ) -> Result<(), DbError> {
        self.test_sql(move |database| database.insert_retained_replay_object(&owner, &object))
            .await
    }

    pub async fn retained_merge_input_hash_for_test(
        &self,
        stream_id: String,
        sequence: u64,
    ) -> Result<coven_protocol::store_commit::ObjectHash, DbError> {
        self.test_sql(move |database| {
            database
                .retained_merge_input(&stream_id, sequence)
                .map(|(input_hash, _)| input_hash)
        })
        .await
    }

    pub async fn materialized_commit_exists_for_test(
        &self,
        stream_id: String,
        sequence: u64,
    ) -> Result<bool, DbError> {
        self.test_sql(move |database| database.materialized_commit_exists(&stream_id, sequence))
            .await
    }

    pub async fn remove_materialized_note_for_test(
        &self,
        stream_id: String,
        sequence: u64,
        row_id: String,
    ) -> Result<(), DbError> {
        self.test_sql(move |database| {
            database.transaction(|transaction| {
                transaction.delete_materialized_commit(&stream_id, sequence)?;
                transaction
                    .execute("DELETE FROM notes WHERE id = ?1", [row_id])
                    .map(|_| ())
                    .map_err(DbError::from)
            })
        })
        .await
    }

    pub async fn write_retains_prepared_for_test(
        &self,
        write_id: coven_protocol::write::WriteId,
    ) -> Result<bool, DbError> {
        self.test_sql(move |database| database.write_retains_prepared(&write_id))
            .await
    }

    pub async fn install_outbound_completion_failure_for_test(&self) -> Result<(), DbError> {
        self.test_sql(|database| database.install_outbound_completion_failure_trigger())
            .await
    }

    pub async fn remove_outbound_completion_failure_for_test(&self) -> Result<(), DbError> {
        self.test_sql(|database| {
            database
                .execute_batch("DROP TRIGGER fail_outbound_completion")
                .map_err(DbError::from)
        })
        .await
    }

    pub async fn replace_store_root_hash_for_test(
        &self,
        value: Option<String>,
    ) -> Result<(), DbError> {
        self.test_sql(move |database| database.replace_store_root_hash(value.as_deref()))
            .await
    }

    pub async fn delete_device_state_snapshot_for_test(
        &self,
        commit_ref: String,
    ) -> Result<(), DbError> {
        self.test_sql(move |database| database.delete_device_state_snapshot(&commit_ref))
            .await
    }

    pub async fn delete_retained_materialization_without_foreign_keys_for_test(
        &self,
        reference: coven_protocol::store_commit::StoreBatchCommitRef,
    ) -> Result<(), DbError> {
        self.test_sql(move |database| {
            database.delete_retained_materialization_without_foreign_keys(&reference)
        })
        .await
    }

    pub async fn replace_device_state_snapshot_for_test(
        &self,
        commit_ref: String,
        state: coven_protocol::store_commit::ResolvedStoreDeviceState,
    ) -> Result<(), DbError> {
        self.test_sql(move |database| database.replace_device_state_snapshot(&commit_ref, &state))
            .await
    }

    pub async fn forge_device_in_state_snapshots_for_test(
        &self,
        forged_device_id: coven_protocol::store_commit::StoreDeviceId,
    ) -> Result<(), DbError> {
        self.test_sql(move |database| database.forge_device_in_state_snapshots(forged_device_id))
            .await
    }

    pub async fn delete_exact_materialized_commit_for_test(
        &self,
        reference: coven_protocol::store_commit::StoreBatchCommitRef,
    ) -> Result<(), DbError> {
        self.test_sql(move |database| database.delete_exact_materialized_commit(&reference))
            .await
    }

    pub async fn install_protocol_state_key_insert_failure_for_test(
        &self,
        rejected_key: String,
    ) -> Result<(), DbError> {
        self.test_sql(move |database| {
            database
                .execute_batch(&format!(
                    "CREATE TRIGGER reject_protocol_state_key
                     BEFORE INSERT ON protocol_state
                     WHEN NEW.key = '{rejected_key}'
                     BEGIN SELECT RAISE(ABORT, 'forced cursor failure'); END;"
                ))
                .map_err(DbError::from)
        })
        .await
    }

    pub async fn install_protocol_state_insert_failure_for_test(&self) -> Result<(), DbError> {
        self.test_sql(|database| database.install_protocol_state_insert_failure_trigger())
            .await
    }

    pub async fn apply_changeset_for_test(
        &self,
        bytes: Vec<u8>,
        tables: Vec<coven_protocol::synced_schema::SyncedTable>,
        receiver_wall_ms: u64,
    ) -> Result<crate::ApplyResult, DbError> {
        self.test_sql(move |database| database.apply_changeset(&bytes, &tables, receiver_wall_ms))
            .await
    }

    pub async fn apply_changesets_atomically_for_test(
        &self,
        changesets: Vec<Vec<u8>>,
        tables: Vec<coven_protocol::synced_schema::SyncedTable>,
        receiver_wall_ms: u64,
    ) -> Result<(Vec<crate::ApplyResult>, bool), DbError> {
        self.test_sql(move |database| {
            database.apply_changesets_atomically(changesets, &tables, receiver_wall_ms)
        })
        .await
    }

    pub async fn store_write_partitions_in_audience_order_for_test(
        &self,
    ) -> Result<Vec<(String, Option<String>, Vec<u8>)>, DbError> {
        self.test_sql(|database| database.store_write_partitions_in_audience_order())
            .await
    }

    pub async fn first_store_write_partition_hash_for_test(
        &self,
        write_id: coven_protocol::write::WriteId,
    ) -> Result<coven_protocol::store_commit::ObjectHash, DbError> {
        self.test_sql(move |database| database.first_store_write_partition_hash(write_id.as_str()))
            .await
    }

    pub async fn plant_control_on_local_partition_for_test(&self) -> Result<(), DbError> {
        self.test_sql(|database| database.plant_control_on_the_local_partition())
            .await
    }

    pub async fn store_write_row_for_test(
        &self,
        write_id: coven_protocol::write::WriteId,
    ) -> Result<(String, Vec<u8>), DbError> {
        self.test_sql(move |database| database.store_write_row(write_id.as_str()))
            .await
    }

    pub async fn row_and_private_routing_presence_for_test(
        &self,
        table: &str,
        row_id: &str,
    ) -> Result<(bool, bool, bool), DbError> {
        let table = table.to_string();
        let row_id = row_id.to_string();
        self.test_sql(move |database| database.row_and_private_routing_presence(&table, &row_id))
            .await
    }

    pub async fn store_write_row_and_only_partition_for_test(
        &self,
        write_id: coven_protocol::write::WriteId,
    ) -> Result<((String, Vec<u8>), (String, Option<String>, Vec<u8>)), DbError> {
        self.test_sql(move |database| {
            Ok((
                database.store_write_row(write_id.as_str())?,
                database.only_store_write_partition(write_id.as_str())?,
            ))
        })
        .await
    }

    pub async fn store_write_partition_changesets_for_test(
        &self,
        write_id: coven_protocol::write::WriteId,
    ) -> Result<Vec<(String, Vec<u8>)>, DbError> {
        self.test_sql(move |database| database.store_write_partition_changesets(write_id.as_str()))
            .await
    }

    pub async fn apply_coven_routing_schema_for_test(&self) -> Result<(), DbError> {
        self.test_sql(|database| database.apply_coven_routing_schema())
            .await
    }

    pub async fn circle_current_state_for_test(
        &self,
        circle_id: coven_protocol::circle::CircleId,
    ) -> Result<Option<coven_protocol::circle_activation::CircleCurrentState>, DbError> {
        self.test_sql(move |database| database.circle_current_state(circle_id))
            .await
    }

    pub async fn replace_circle_operation_prepared_for_test(
        &self,
        operation_id: coven_protocol::circle::CircleOperationId,
        substitute: coven_protocol::circle_journal::CircleOperationJournal,
    ) -> Result<(), DbError> {
        self.test_sql(move |database| {
            database.replace_circle_operation_prepared(&operation_id, &substitute)
        })
        .await
    }

    pub async fn circle_state_table_counts_for_test(&self) -> Result<(i64, i64), DbError> {
        self.test_sql(|database| database.circle_state_table_counts())
            .await
    }

    pub async fn document_circle_route_for_test(
        &self,
        row_id: &str,
    ) -> Result<(String, String, String), DbError> {
        let row_id = row_id.to_string();
        self.test_sql(move |database| database.document_circle_route(&row_id))
            .await
    }

    pub async fn corrupt_live_document_route_id_for_test(
        &self,
        row_id: &str,
    ) -> Result<(), DbError> {
        let row_id = row_id.to_string();
        self.test_sql(move |database| database.corrupt_live_document_route_id(&row_id))
            .await
    }

    pub async fn materialization_graph_counts_for_test(&self) -> Result<(i64, i64, i64), DbError> {
        self.test_sql(|database| {
            Ok((
                database.table_row_count(DatabaseTestTable::named("materialized_commits"))?,
                database
                    .table_row_count(DatabaseTestTable::named("retained_merge_materializations"))?,
                database.table_row_count(DatabaseTestTable::named("retained_replay_objects"))?,
            ))
        })
        .await
    }

    pub async fn scoped_routing_counts_for_test(
        &self,
        circle_id: coven_protocol::circle::CircleId,
    ) -> Result<(i64, i64), DbError> {
        self.test_sql(move |database| database.scoped_routing_counts(circle_id))
            .await
    }

    pub async fn cleanup_intent_copy_identities_for_test(&self) -> Result<Vec<String>, DbError> {
        self.test_sql(|database| database.cleanup_intent_copy_identities())
            .await
    }

    pub async fn insert_cleanup_intent_for_test(
        &self,
        namespace: String,
        blob_id: String,
        copy_identity: String,
    ) -> Result<(), DbError> {
        self.test_sql(move |database| {
            database.insert_cleanup_intent(&namespace, &blob_id, &copy_identity)
        })
        .await
    }
}

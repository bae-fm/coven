use crate::{Database, DatabaseTestTable, DbError};

impl Database {
    pub async fn install_malformed_store_root_authority_for_test(&self) -> Result<(), DbError> {
        self.test_sql(|database| {
            database
                .execute(
                    "INSERT INTO store_protocol_root_authority
                     (singleton, store_root_hash, store_protocol_root_bytes, store_root_object)
                     VALUES (1, ?1, X'00', '{}')",
                    ["00".repeat(32)],
                )
                .map(|_| ())
                .map_err(DbError::from)
        })
        .await
    }

    pub async fn install_exact_store_root_authority_for_test(
        &self,
        reference: coven_protocol::store_commit::StoreRootRef,
        bytes: Vec<u8>,
    ) -> Result<(), DbError> {
        self.test_sql(move |database| {
            database.install_exact_store_root_authority(&reference, &bytes)
        })
        .await
    }

    pub async fn seed_existing_store_write_for_test(
        &self,
        changeset_hash: String,
    ) -> Result<(), DbError> {
        self.test_sql(move |database| {
            database
                .execute(
                    "INSERT INTO store_writes
                     (write_id, status, affected_rows, changeset_hash, base, blob_facts)
                     VALUES (
                        'existing-write', '\"pending\"', '[]', ?1,
                        '{\"dependencies\":{}}',
                        '{\"blobs\":[]}'
                     )",
                    [changeset_hash],
                )
                .map(|_| ())
                .map_err(DbError::from)
        })
        .await
    }

    pub async fn host_identity_rollback_state_for_test(
        &self,
    ) -> Result<(i64, Vec<String>), DbError> {
        self.test_sql(|database| {
            let row_count = database
                .query_row("SELECT COUNT(*) FROM things", [], |row| row.get(0))
                .map_err(DbError::from)?;
            let write_hashes = database
                .query(
                    "SELECT changeset_hash FROM store_writes
                     WHERE changeset_hash IS NOT NULL ORDER BY ordinal",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .map_err(DbError::from)?;
            Ok((row_count, write_hashes))
        })
        .await
    }

    pub async fn seed_thing_for_test(&self, row_id: String) -> Result<(), DbError> {
        self.test_sql(move |database| {
            database
                .execute(
                    "INSERT INTO things VALUES (?1, 'base', '0000000001000-0000-writer')",
                    [row_id],
                )
                .map(|_| ())
                .map_err(DbError::from)
        })
        .await
    }

    pub async fn store_write_hashes_for_test(&self) -> Result<Vec<String>, DbError> {
        self.test_sql(|database| {
            database
                .query(
                    "SELECT changeset_hash FROM store_writes
                     WHERE changeset_hash IS NOT NULL ORDER BY ordinal",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .map_err(DbError::from)
        })
        .await
    }

    pub async fn thing_and_store_write_state_for_test(
        &self,
    ) -> Result<((String, String), Vec<String>), DbError> {
        self.test_sql(|database| {
            let row = database
                .query_row("SELECT id, body FROM things", [], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .map_err(DbError::from)?;
            let write_hashes = database
                .query(
                    "SELECT changeset_hash FROM store_writes
                     WHERE changeset_hash IS NOT NULL ORDER BY ordinal",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .map_err(DbError::from)?;
            Ok((row, write_hashes))
        })
        .await
    }

    pub async fn make_remote_retain_pinned_column_for_test(
        &self,
    ) -> Result<Option<(i64, Option<String>)>, DbError> {
        self.test_sql(|database| {
            let rows = database
                .query("PRAGMA table_info(blob_make_remote_intents)", [], |row| {
                    Ok((
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, Option<String>>(4)?,
                    ))
                })
                .map_err(DbError::from)?;
            Ok(rows
                .into_iter()
                .find_map(|(name, not_null, default_value)| {
                    (name == "retain_pinned").then_some((not_null, default_value))
                }))
        })
        .await
    }

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

    pub async fn install_replay_image_corruption_for_test(&self) -> Result<(), DbError> {
        self.test_sql(|database| {
            database
                .execute_batch(
                    "CREATE TEMP TRIGGER corrupt_owner_anchor_replay_image
                     AFTER INSERT ON retained_replay_baselines
                     BEGIN
                         UPDATE payload_storage
                         SET compressed_bytes = X'00', compressed_size = 1
                         WHERE payload_hash = NEW.image_payload_hash
                           AND storage = 'inline';
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

    pub async fn record_verified_circle_activations_for_test(
        &self,
        commit: coven_protocol::store_commit::VerifiedStoreBatchCommit,
        activations: Vec<coven_protocol::circle_activation::VerifiedCircleReference>,
    ) -> Result<(), DbError> {
        self.test_sql(move |database| {
            database.record_verified_circle_activations(&commit, &activations)
        })
        .await
    }

    pub async fn circle_access_owner_for_test(
        &self,
        circle_id: coven_protocol::circle::CircleId,
    ) -> Result<String, DbError> {
        self.test_sql(move |database| database.circle_access_owner(circle_id))
            .await
    }

    pub async fn clear_circle_access_cache_for_test(&self) -> Result<(), DbError> {
        self.test_sql(|database| {
            database.clear_table(DatabaseTestTable::named("circle_access_cache"))
        })
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

    pub async fn persist_exact_remote_object_for_test(
        &self,
        remote: coven_protocol::remote_object::ClosedRemoteObject,
        context: String,
    ) -> Result<(), DbError> {
        self.test_sql(move |database| database.persist_exact_remote_object(&remote, &context))
            .await
    }

    pub async fn remote_object_by_id_for_test(
        &self,
        object_id: coven_protocol::store_commit::ObjectHash,
    ) -> Result<coven_protocol::remote_object::RemoteObjectRecord, DbError> {
        self.test_sql(move |database| database.load_remote_object(object_id))
            .await
    }

    pub async fn install_reclaimed_store_package_for_test(
        &self,
        operation: crate::DurableStoreReclaimOperation,
        package: crate::ReclaimedStorePackage,
    ) -> Result<(), DbError> {
        self.test_sql(move |database| {
            database.transaction(|transaction| {
                transaction.insert_store_reclaim_operation(&operation)?;
                transaction.record_reclaimed_store_package(&package)
            })
        })
        .await
    }

    pub async fn reclaimed_store_package_for_test(
        &self,
        object_id: coven_protocol::store_commit::ObjectHash,
    ) -> Result<Option<crate::ReclaimedStorePackage>, DbError> {
        self.test_sql(move |database| database.load_reclaimed_store_package(object_id))
            .await
    }

    pub async fn record_reclaimed_store_package_for_test(
        &self,
        package: crate::ReclaimedStorePackage,
    ) -> Result<(), DbError> {
        self.test_sql(move |database| database.record_reclaimed_store_package(&package))
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

    pub async fn seed_distinct_cleanup_bindings_for_test(
        &self,
        removed_locator: coven_protocol::store_commit::ObjectHash,
        live_locator: coven_protocol::store_commit::ObjectHash,
        removed_object: coven_protocol::store_commit::ObjectHash,
        live_object: coven_protocol::store_commit::ObjectHash,
    ) -> Result<(), DbError> {
        self.test_sql(move |database| {
            database
                .execute_batch(&format!(
                    "INSERT INTO notes (id, title, shared, _updated_at, created_at)
                     VALUES ('parent', 'parent', 1, '0000000001000-0000-test', '2026-01-01');
                     INSERT INTO note_photos
                        (id, note_id, kind, size, hash, blob_id, _updated_at, created_at)
                     VALUES
                        ('removed-row', 'parent', 'cover', 5, '{hash}', 'shared-id',
                         '0000000001000-0000-test', '2026-01-01'),
                        ('live-row', 'parent', 'cover', 5, '{hash}', 'shared-id',
                         '0000000001001-0000-test', '2026-01-01');",
                    hash = coven_protocol::blob::content_hash(b"bytes"),
                ))
                .map_err(DbError::from)?;
            for (object, locator) in [
                (removed_object, removed_locator),
                (live_object, live_locator),
            ] {
                database
                    .execute(
                        "INSERT INTO remote_objects (object_id, state) VALUES (?1, '{}')",
                        [object.to_string()],
                    )
                    .map_err(DbError::from)?;
                database
                    .execute(
                        "INSERT INTO blob_locators (remote_object_id, locator_hash)
                         VALUES (?1, ?2)",
                        (object.to_string(), locator.to_string()),
                    )
                    .map_err(DbError::from)?;
            }
            for (row_id, row_stamp, object) in [
                ("removed-row", "0000000001000-0000-test", removed_object),
                ("live-row", "0000000001001-0000-test", live_object),
            ] {
                database
                    .execute(
                        "INSERT INTO row_blob_locators
                         (table_name, row_id, column_name, row_stamp,
                          audience_authority, remote_object_id)
                         VALUES ('note_photos', ?1, 'blob_id', ?2, '\"store\"', ?3)",
                        (row_id, row_stamp, object.to_string()),
                    )
                    .map_err(DbError::from)?;
            }
            database
                .execute("DELETE FROM note_photos WHERE id = 'removed-row'", [])
                .map(|_| ())
                .map_err(DbError::from)
        })
        .await
    }
}

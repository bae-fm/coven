use super::{DbError, StoreDatabase};
use crate::protocol::store_commit::{
    ObjectHash, StoreBatchCommitRef, StoreDeviceExclusionRef, StoreDeviceHeadRef,
};
use crate::WriteId;
use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug)]
pub(crate) enum AuthorExclusionLocatorTamper {
    Missing,
    ExclusionReference,
    AcceptedCut,
    ActivationCommit,
    ActivationHead,
}

struct PreparedWriteTransfer {
    write: (String, String, Vec<u8>, Vec<u8>, String, String, String),
    partitions: Vec<(String, Option<String>, Vec<u8>)>,
    packages: Vec<(String, String)>,
    blobs: Vec<(String, String, String, Option<String>)>,
    remotes: Vec<(String, String)>,
}

impl StoreDatabase {
    pub(crate) async fn seed_local_release_rows_for_test(
        &self,
        note_id: &str,
        photo_id: &str,
        cloud_path: &str,
        bytes: &[u8],
    ) {
        let note_id = note_id.to_string();
        let photo_id = photo_id.to_string();
        let cloud_path = cloud_path.to_string();
        let size = i64::try_from(bytes.len()).expect("test blob size fits SQLite");
        let hash = crate::protocol::blob::content_hash(bytes);
        self.connection
            .call(move |connection| {
                connection
                    .execute(
                        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at)
                         VALUES (?1, 'Release', NULL, ?2, '0000000001000-0000-A', '2026-01-01')",
                        rusqlite::params![note_id, 0_i64],
                    )
                    .map_err(DbError::from)?;
                connection
                    .execute(
                        "INSERT INTO note_photos
                         (id, note_id, kind, size, hash, _updated_at, created_at, cloud_path)
                         VALUES (?1, ?2, 'image', ?3, ?4,
                                 '0000000001000-0000-A', '2026-01-01', ?5)",
                        rusqlite::params![photo_id, note_id, size, hash, cloud_path],
                    )
                    .map(|_| ())
                    .map_err(DbError::from)
            })
            .await
            .expect("seed exact release rows");
    }

    pub(crate) async fn register_external_blob_for_test(
        &self,
        table: &str,
        row_id: &str,
        path: &std::path::Path,
    ) {
        let reference = self
            .row_blob_ref(table, row_id)
            .await
            .expect("load exact Local row blob reference");
        let path = path.to_path_buf();
        self.connection
            .call(move |connection| {
                crate::database::DatabaseTestSql::new(connection)
                    .register_external_blob(&reference, &path)
            })
            .await
            .expect("register exact external blob reference");
    }

    pub(crate) async fn enqueue_blob_upload_for_test(
        &self,
        root_table: &str,
        root_id: &str,
        reference: &crate::protocol::blob::RowBlobRef,
        source_path: &std::path::Path,
        created_at: &str,
    ) -> Result<(), DbError> {
        let reference = reference.clone();
        let root_table = root_table.to_string();
        let root_id = root_id.to_string();
        let source_path = source_path.to_path_buf();
        let created_at = created_at.to_string();
        self.connection
            .call(move |connection| {
                crate::database::DatabaseTestSql::new(connection).enqueue_blob_upload(
                    &root_table,
                    &root_id,
                    &reference,
                    &source_path,
                    false,
                    &created_at,
                )
            })
            .await
    }

    pub(crate) async fn insert_fixture_position_for_test(
        &self,
        note_id: &str,
    ) -> Result<(), DbError> {
        let note_id = note_id.to_string();
        self.run_host_store_write_for_test(None, None, move |transaction| {
            transaction
                .execute(
                    "INSERT INTO notes (id, title, shared, _updated_at, created_at)
                     VALUES (?1, 'fixture position', 1,
                             '0000000001000-0000-A', '2026-01-01')",
                    [note_id],
                )
                .map(|_| ())
                .map_err(DbError::from)
        })
        .await
        .map(|_| ())
    }

    pub(crate) async fn run_host_store_write_for_test<R>(
        &self,
        routing_encryption: Option<crate::encryption::EncryptionService>,
        blob_staging: Option<Box<dyn crate::database::AudienceBlobMoveStaging>>,
        operation: impl for<'transaction, 'connection> FnOnce(
                crate::database::DatabaseTestTransaction<'transaction, 'connection>,
            ) -> Result<R, DbError>
            + Send
            + 'static,
    ) -> Result<crate::WriteReceipt<R>, DbError>
    where
        R: Send + 'static,
    {
        let synced_tables = self.synced_tables().to_vec();
        let gates = self.gates();
        let blob_decls = self.blob_decls();
        let write_id = self.new_store_write_id();
        self.connection
            .call(move |connection| {
                super::host_write_capture::CapturedStoreWriteTransaction::begin_host(
                    connection,
                    &synced_tables,
                    &gates,
                    &blob_decls,
                    routing_encryption.as_ref(),
                    blob_staging.as_deref(),
                    write_id,
                )?
                .execute(|transaction| {
                    operation(crate::database::DatabaseTestTransaction::new(transaction))
                })
            })
            .await
    }

    pub(crate) async fn run_prepared_blob_transition_write_for_test<R>(
        &self,
        routing_encryption: Option<crate::encryption::EncryptionService>,
        operation: impl for<'transaction, 'connection> FnOnce(
                crate::database::DatabaseTestTransaction<'transaction, 'connection>,
            ) -> Result<R, DbError>
            + Send
            + 'static,
    ) -> Result<crate::WriteReceipt<R>, DbError>
    where
        R: Send + 'static,
    {
        let synced_tables = self.synced_tables().to_vec();
        let gates = self.gates();
        let blob_decls = self.blob_decls();
        let write_id = self.new_store_write_id();
        self.connection
            .call(move |connection| {
                super::host_write_capture::CapturedStoreWriteTransaction::begin_prepared_blob_transition(
                    connection,
                    &synced_tables,
                    &gates,
                    &blob_decls,
                    routing_encryption.as_ref(),
                    write_id,
                )?
                .execute(|transaction| {
                    operation(crate::database::DatabaseTestTransaction::new(transaction))
                })
            })
            .await
    }

    pub(crate) async fn cleanup_intent_count_for_test(
        &self,
        namespace: &str,
        blob_id: &str,
    ) -> Result<i64, DbError> {
        let namespace = namespace.to_string();
        let blob_id = blob_id.to_string();
        self.connection
            .call(move |connection| {
                connection
                    .query_row(
                        "SELECT COUNT(*) FROM local_cleanup_intents
                         WHERE namespace = ?1 AND blob_id = ?2",
                        (&namespace, &blob_id),
                        |row| row.get(0),
                    )
                    .map_err(DbError::from)
            })
            .await
    }

    pub(crate) async fn coven_table_exists_for_test(
        &self,
        table: crate::database::DatabaseTestTable,
    ) -> Result<bool, DbError> {
        self.connection
            .call(move |connection| {
                connection
                    .query_row(
                        "SELECT EXISTS(
                            SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1
                         )",
                        [table.0],
                        |row| row.get(0),
                    )
                    .map_err(DbError::from)
            })
            .await
    }

    pub(crate) async fn install_store_write_failure_trigger_for_test(&self) -> Result<(), DbError> {
        self.connection
            .call(move |connection| {
                connection
                    .execute_batch(
                        "CREATE TRIGGER fail_store_write_journal
                         BEFORE INSERT ON store_writes
                         BEGIN
                           SELECT RAISE(ABORT, 'injected Store write journal failure');
                         END;",
                    )
                    .map_err(DbError::from)
            })
            .await
    }

    pub(crate) async fn remove_store_write_failure_trigger_for_test(&self) -> Result<(), DbError> {
        self.connection
            .call(move |connection| {
                connection
                    .execute_batch("DROP TRIGGER fail_store_write_journal")
                    .map_err(DbError::from)
            })
            .await
    }

    pub(crate) async fn write_blob_facts_for_test(
        &self,
        write_id: WriteId,
    ) -> Result<String, DbError> {
        self.connection
            .call(move |connection| {
                connection
                    .query_row(
                        "SELECT blob_facts FROM store_writes WHERE write_id = ?1",
                        [write_id.as_str()],
                        |row| row.get(0),
                    )
                    .map_err(DbError::from)
            })
            .await
    }

    pub(crate) async fn install_test_active_circle(
        &self,
        label: String,
    ) -> Result<crate::protocol::circle::CircleId, DbError> {
        self.connection
            .call(move |connection| {
                let database = crate::database::DatabaseTestSql::new(connection);
                let (circle_id, _) = database.install_test_active_circle(&label);
                Ok(circle_id)
            })
            .await
    }

    pub(crate) async fn install_test_active_circle_state(
        &self,
        label: String,
    ) -> Result<crate::protocol::circle_activation::CircleCurrentState, DbError> {
        self.connection
            .call(move |connection| {
                let database = crate::database::DatabaseTestSql::new(connection);
                database.apply_coven_routing_schema()?;
                let (circle_id, _) = database.install_test_active_circle(&label);
                database.circle_current_state(circle_id)?.ok_or_else(|| {
                    DbError::Message("installed active circle has no current state".to_string())
                })
            })
            .await
    }

    pub(crate) async fn install_test_inactive_circle_state(
        &self,
        label: String,
    ) -> Result<crate::protocol::circle_activation::CircleCurrentState, DbError> {
        self.connection
            .call(move |connection| {
                let database = crate::database::DatabaseTestSql::new(connection);
                database.apply_coven_routing_schema()?;
                let (circle_id, _) = database.install_test_inactive_circle(&label);
                database.circle_current_state(circle_id)?.ok_or_else(|| {
                    DbError::Message("installed inactive circle has no current state".to_string())
                })
            })
            .await
    }

    pub(crate) async fn insert_write_status_for_test(
        &self,
        write_id: WriteId,
        status: crate::WriteStatus,
    ) -> Result<(), DbError> {
        let base = serde_json::json!({ "dependencies": {} }).to_string();
        let status = serde_json::to_string(&status)
            .map_err(|error| DbError::context("serialize write status", error))?;
        self.connection
            .call(move |connection| {
                connection
                    .execute(
                        r#"INSERT INTO store_writes
                         (write_id, status, affected_rows, changeset, inverse_changeset, base, blob_facts)
                         VALUES (?1, ?2, '[]', X'', X'', ?3, '{"blobs":[]}')"#,
                        (write_id.as_str(), status, base),
                    )
                    .map(|_| ())
                    .map_err(DbError::from)
            })
            .await
    }

    pub(crate) async fn delete_write_for_test(&self, write_id: WriteId) -> Result<(), DbError> {
        self.connection
            .call(move |connection| {
                connection
                    .execute(
                        "DELETE FROM store_writes WHERE write_id = ?1",
                        [write_id.as_str()],
                    )
                    .map(|_| ())
                    .map_err(DbError::from)
            })
            .await
    }

    pub(crate) fn arm_test_pause(
        &self,
        point: crate::database::DatabaseTestPoint,
    ) -> (
        std::sync::Arc<tokio::sync::Notify>,
        std::sync::Arc<tokio::sync::Notify>,
    ) {
        self.test_access.arm(point)
    }

    pub(crate) async fn write_changeset_for_test(
        &self,
        write_id: &WriteId,
    ) -> Result<Vec<u8>, DbError> {
        let write_id = write_id.clone();
        self.connection
            .call(move |connection| {
                connection
                    .query_row(
                        "SELECT changeset FROM store_writes WHERE write_id = ?1",
                        [write_id.as_str()],
                        |row| row.get(0),
                    )
                    .map_err(DbError::from)
            })
            .await
    }

    pub(crate) async fn write_blob_lease_count_for_test(
        &self,
        write_id: &WriteId,
    ) -> Result<i64, DbError> {
        let write_id = write_id.clone();
        self.connection
            .call(move |connection| {
                connection
                    .query_row(
                        "SELECT COUNT(*) FROM store_write_blob_leases WHERE write_id = ?1",
                        [write_id.as_str()],
                        |row| row.get(0),
                    )
                    .map_err(DbError::from)
            })
            .await
    }

    pub(crate) async fn latest_materialized_commit_coordinate_for_test(
        &self,
    ) -> Result<(String, u64), DbError> {
        self.connection
            .call(move |connection| {
                let (device_id, sequence): (String, i64) = connection
                    .query_row(
                        "SELECT device_id, seq
                         FROM materialized_commits
                         ORDER BY seq DESC
                         LIMIT 1",
                        [],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .map_err(DbError::from)?;
                let sequence = u64::try_from(sequence).map_err(|error| {
                    DbError::context(
                        format!("materialized commit sequence {sequence} is invalid"),
                        error,
                    )
                })?;
                Ok((device_id, sequence))
            })
            .await
    }

    pub(crate) async fn compare_circle_bootstrap_replay_with_missing_coverage_for_test(
        &self,
        root: crate::protocol::store_commit::StoreRootRef,
        routing_key: crate::protocol::circle::RowRoutingKey,
        historical_id: String,
        late_id: String,
    ) -> Result<(i64, i64, i64, i64), DbError> {
        let blob_decls = self.blob_decls();
        let gates = self.gates();
        let tables = self.synced_tables().to_vec();
        self.with_retained_merge_materializations(
            move |connection, retained_merge_materializations| {
                let transaction = connection.unchecked_transaction().map_err(DbError::from)?;
                let retained = retained_merge_materializations.replay_projection_on(
                    &transaction,
                    &root,
                    &blob_decls,
                    &gates,
                    &tables,
                    Some(&routing_key),
                    &BTreeSet::new(),
                    None,
                    false,
                    crate::protocol::membership::LocalStoreMembership::Current,
                )?;
                let retained_count = retained.query_row(
                    "SELECT COUNT(*) FROM documents WHERE id = ?1",
                    [historical_id.as_str()],
                    |row| row.get::<_, i64>(0),
                )?;
                let retained_late_count = retained.query_row(
                    "SELECT COUNT(*) FROM documents WHERE id = ?1",
                    [late_id.as_str()],
                    |row| row.get::<_, i64>(0),
                )?;
                transaction
                    .execute("DELETE FROM circle_bootstrap_coverage", [])
                    .map_err(DbError::from)?;
                let sabotaged = retained_merge_materializations.replay_projection_on(
                    &transaction,
                    &root,
                    &blob_decls,
                    &gates,
                    &tables,
                    Some(&routing_key),
                    &BTreeSet::new(),
                    None,
                    false,
                    crate::protocol::membership::LocalStoreMembership::Current,
                )?;
                let sabotaged_count = sabotaged.query_row(
                    "SELECT COUNT(*) FROM documents WHERE id = ?1",
                    [historical_id.as_str()],
                    |row| row.get::<_, i64>(0),
                )?;
                let sabotaged_late_count = sabotaged.query_row(
                    "SELECT COUNT(*) FROM documents WHERE id = ?1",
                    [late_id.as_str()],
                    |row| row.get::<_, i64>(0),
                )?;
                transaction.rollback().map_err(DbError::from)?;
                Ok((
                    retained_count,
                    retained_late_count,
                    sabotaged_count,
                    sabotaged_late_count,
                ))
            },
        )
        .await
    }

    pub(crate) async fn circle_bootstrap_coverage_count_for_test(
        &self,
        circle_id: crate::protocol::circle::CircleId,
    ) -> Result<i64, DbError> {
        self.connection
            .call(move |connection| {
                connection
                    .query_row(
                        "SELECT COUNT(*) FROM circle_bootstrap_coverage WHERE circle_id = ?1",
                        [circle_id.to_string()],
                        |row| row.get(0),
                    )
                    .map_err(DbError::from)
            })
            .await
    }

    pub(crate) async fn reject_changed_circle_bootstrap_image_hash_for_test(
        &self,
        circle_id: crate::protocol::circle::CircleId,
        root: crate::protocol::store_commit::StoreRootRef,
        activation_commit: &StoreBatchCommitRef,
    ) -> Result<String, DbError> {
        let activation_commit = activation_commit.clone();
        self.connection
            .call(move |connection| {
                let transaction = connection.unchecked_transaction().map_err(DbError::from)?;
                transaction
                    .execute(
                        "UPDATE circle_bootstrap_coverage
                         SET image_hash = ?2
                         WHERE circle_id = ?1",
                        rusqlite::params![
                            circle_id.to_string(),
                            ObjectHash::digest(b"corrupt Circle bootstrap image hash").to_string(),
                        ],
                    )
                    .map_err(DbError::from)?;
                let retained = StoreDatabase::load_retained_merge_materialization_by_ref_on(
                    &transaction,
                    &root,
                    &activation_commit,
                )?;
                let error = StoreDatabase::record_circle_bootstrap_coverage_on(
                    &transaction,
                    &root,
                    &activation_commit,
                    retained.circle_activations(),
                )
                .expect_err("changed image hash must conflict with its exact reference");
                transaction.rollback().map_err(DbError::from)?;
                Ok(error.to_string())
            })
            .await
    }

    pub(crate) async fn transfer_prepared_write_to_for_test(
        &self,
        destination: &Self,
        write_id: &WriteId,
    ) -> Result<(), DbError> {
        let source_write_id = write_id.clone();
        let transfer = self
            .connection
            .call(move |connection| {
                let write = connection
                    .query_row(
                        "SELECT status, affected_rows, changeset, inverse_changeset,
                                base, blob_facts, prepared
                         FROM store_writes WHERE write_id = ?1",
                        [source_write_id.as_str()],
                        |row| {
                            Ok((
                                row.get(0)?,
                                row.get(1)?,
                                row.get(2)?,
                                row.get(3)?,
                                row.get(4)?,
                                row.get(5)?,
                                row.get(6)?,
                            ))
                        },
                    )
                    .map_err(DbError::from)?;
                let partitions = {
                    let mut statement = connection
                        .prepare(
                            "SELECT audience, control_coord, changeset
                             FROM store_write_partitions WHERE write_id = ?1 ORDER BY audience",
                        )
                        .map_err(DbError::from)?;
                    let rows = statement
                        .query_map([source_write_id.as_str()], |row| {
                            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                        })
                        .map_err(DbError::from)?
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(DbError::from)?;
                    rows
                };
                let packages = {
                    let mut statement = connection
                        .prepare(
                            "SELECT audience, remote_object_id
                             FROM store_write_packages WHERE write_id = ?1 ORDER BY audience",
                        )
                        .map_err(DbError::from)?;
                    let rows = statement
                        .query_map([source_write_id.as_str()], |row| {
                            Ok((row.get(0)?, row.get(1)?))
                        })
                        .map_err(DbError::from)?
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(DbError::from)?;
                    rows
                };
                let blobs = {
                    let mut statement = connection
                        .prepare(
                            "SELECT audience, locator_hash, remote_object_id, spool_path
                             FROM store_write_blobs WHERE write_id = ?1
                             ORDER BY audience, remote_object_id",
                        )
                        .map_err(DbError::from)?;
                    let rows = statement
                        .query_map([source_write_id.as_str()], |row| {
                            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                        })
                        .map_err(DbError::from)?
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(DbError::from)?;
                    rows
                };
                let remotes = {
                    let mut statement = connection
                        .prepare("SELECT object_id, state FROM remote_objects ORDER BY object_id")
                        .map_err(DbError::from)?;
                    let rows = statement
                        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                        .map_err(DbError::from)?
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(DbError::from)?;
                    rows
                };
                Ok(PreparedWriteTransfer {
                    write,
                    partitions,
                    packages,
                    blobs,
                    remotes,
                })
            })
            .await?;

        let destination_write_id = write_id.clone();
        destination
            .connection
            .call(move |connection| {
                let transaction = connection.unchecked_transaction().map_err(DbError::from)?;
                for (object_id, state) in transfer.remotes {
                    let imported = transaction
                        .execute(
                            "INSERT INTO remote_objects (object_id, state) VALUES (?1, ?2)
                             ON CONFLICT(object_id) DO UPDATE SET state = excluded.state
                             WHERE remote_objects.state = excluded.state",
                            (object_id, state),
                        )
                        .map_err(DbError::from)?;
                    if imported != 1 {
                        return Err(DbError::Message(
                            "prepared write remote object conflicts with restored state"
                                .to_string(),
                        ));
                    }
                }
                transaction
                    .execute(
                        "INSERT INTO store_writes
                         (write_id, status, affected_rows, changeset, inverse_changeset,
                          base, blob_facts, prepared)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                        rusqlite::params![
                            destination_write_id.as_str(),
                            transfer.write.0,
                            transfer.write.1,
                            transfer.write.2,
                            transfer.write.3,
                            transfer.write.4,
                            transfer.write.5,
                            transfer.write.6,
                        ],
                    )
                    .map_err(DbError::from)?;
                for (audience, control, changeset) in transfer.partitions {
                    transaction
                        .execute(
                            "INSERT INTO store_write_partitions
                             (write_id, audience, control_coord, changeset)
                             VALUES (?1, ?2, ?3, ?4)",
                            rusqlite::params![
                                destination_write_id.as_str(),
                                audience,
                                control,
                                changeset
                            ],
                        )
                        .map_err(DbError::from)?;
                }
                for (audience, object_id) in transfer.packages {
                    transaction
                        .execute(
                            "INSERT INTO store_write_packages
                             (write_id, audience, remote_object_id) VALUES (?1, ?2, ?3)",
                            rusqlite::params![destination_write_id.as_str(), audience, object_id],
                        )
                        .map_err(DbError::from)?;
                }
                for (audience, locator_hash, object_id, spool_path) in transfer.blobs {
                    transaction
                        .execute(
                            "INSERT INTO store_write_blobs
                             (write_id, audience, locator_hash, remote_object_id, spool_path)
                             VALUES (?1, ?2, ?3, ?4, ?5)",
                            rusqlite::params![
                                destination_write_id.as_str(),
                                audience,
                                locator_hash,
                                object_id,
                                spool_path
                            ],
                        )
                        .map_err(DbError::from)?;
                }
                transaction.commit().map_err(DbError::from)
            })
            .await
    }

    pub(crate) async fn author_exclusion_activation_evidence_for_test(
        &self,
        exclusion: &StoreDeviceExclusionRef,
    ) -> Result<(String, String), DbError> {
        let exclusion = serde_json::to_string(exclusion)
            .map_err(|error| DbError::context("serialize exclusion ref", error))?;
        self.connection
            .call(move |connection| {
                connection
                    .query_row(
                        "SELECT accepted_cut, activation_head
                         FROM store_author_exclusion_activations
                         WHERE exclusion_ref = ?1",
                        [&exclusion],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .map_err(DbError::from)
            })
            .await
    }

    pub(crate) async fn tamper_author_exclusion_locator_for_test(
        &self,
        exclusion: &StoreDeviceExclusionRef,
        candidate: &StoreBatchCommitRef,
        tamper: AuthorExclusionLocatorTamper,
    ) -> Result<(), DbError> {
        let exclusion = exclusion.clone();
        let candidate = candidate.clone();
        self.connection
            .call(move |connection| {
                let exact = serde_json::to_string(&exclusion).map_err(|error| {
                    DbError::context("serialize exact exclusion reference", error)
                })?;
                let affected = match tamper {
                    AuthorExclusionLocatorTamper::Missing => connection.execute(
                        "DELETE FROM store_author_exclusion_activations
                         WHERE exclusion_ref = ?1",
                        [&exact],
                    ),
                    AuthorExclusionLocatorTamper::ExclusionReference => {
                        let mut wrong = exclusion;
                        wrong.outcome_hash = ObjectHash::digest(b"wrong exclusion reference");
                        let wrong = serde_json::to_string(&wrong).map_err(|error| {
                            DbError::context("serialize wrong exclusion reference", error)
                        })?;
                        connection.execute(
                            "UPDATE store_author_exclusion_activations
                             SET exclusion_ref = ?1 WHERE exclusion_ref = ?2",
                            (&wrong, &exact),
                        )
                    }
                    AuthorExclusionLocatorTamper::AcceptedCut => {
                        let cut: String = connection
                            .query_row(
                                "SELECT accepted_cut
                                 FROM store_author_exclusion_activations
                                 WHERE exclusion_ref = ?1",
                                [&exact],
                                |row| row.get(0),
                            )
                            .map_err(DbError::from)?;
                        let mut cut: std::collections::BTreeMap<
                            crate::protocol::causal_grants::AuthorStreamId,
                            StoreBatchCommitRef,
                        > = serde_json::from_str(&cut).map_err(|error| {
                            DbError::context("parse exclusion accepted cut", error)
                        })?;
                        cut.insert(
                            crate::protocol::causal_grants::AuthorStreamId::from_digest(
                                ObjectHash::digest(b"wrong exclusion accepted-cut stream"),
                            ),
                            candidate.clone(),
                        );
                        let wrong = serde_json::to_string(&cut).map_err(|error| {
                            DbError::context("serialize wrong exclusion accepted cut", error)
                        })?;
                        connection.execute(
                            "UPDATE store_author_exclusion_activations
                             SET accepted_cut = ?1 WHERE exclusion_ref = ?2",
                            (&wrong, &exact),
                        )
                    }
                    AuthorExclusionLocatorTamper::ActivationCommit => {
                        let wrong = serde_json::to_string(&candidate).map_err(|error| {
                            DbError::context("serialize wrong exclusion activation commit", error)
                        })?;
                        connection.execute(
                            "UPDATE store_author_exclusion_activations
                             SET activation_commit = ?1 WHERE exclusion_ref = ?2",
                            (&wrong, &exact),
                        )
                    }
                    AuthorExclusionLocatorTamper::ActivationHead => {
                        let head: String = connection
                            .query_row(
                                "SELECT activation_head
                                 FROM store_author_exclusion_activations
                                 WHERE exclusion_ref = ?1",
                                [&exact],
                                |row| row.get(0),
                            )
                            .map_err(DbError::from)?;
                        let mut head: StoreDeviceHeadRef =
                            serde_json::from_str(&head).map_err(|error| {
                                DbError::context("parse exclusion activation head", error)
                            })?;
                        head.head_hash = ObjectHash::digest(b"wrong exclusion activation head");
                        let wrong = serde_json::to_string(&head).map_err(|error| {
                            DbError::context("serialize wrong exclusion activation head", error)
                        })?;
                        connection.execute(
                            "UPDATE store_author_exclusion_activations
                             SET activation_head = ?1 WHERE exclusion_ref = ?2",
                            (&wrong, &exact),
                        )
                    }
                }
                .map_err(DbError::from)?;
                if affected != 1 {
                    return Err(DbError::Message(format!(
                        "locator tamper {tamper:?} changed {affected} rows"
                    )));
                }
                Ok(())
            })
            .await
    }
}

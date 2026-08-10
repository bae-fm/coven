use super::{DbError, StoreDatabase, StoreSession};
use coven_protocol::store_commit::{
    ObjectHash, StoreBatchCommitRef, StoreDeviceExclusionRef, StoreDeviceHeadRef,
};
use coven_protocol::write::WriteId;
use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug)]
pub enum AuthorExclusionLocatorTamper {
    Missing,
    ExclusionReference,
    AcceptedCut,
    ActivationCommit,
    ActivationHead,
}

struct PreparedWriteTransfer {
    write: (String, String, String, String, String, String),
    partitions: Vec<(String, Option<String>, String)>,
    packages: Vec<(String, String)>,
    blobs: Vec<(String, String, String, Option<String>)>,
    remotes: Vec<(String, String)>,
    payload_claims: Vec<(String, BTreeSet<ObjectHash>)>,
    payloads: Vec<(ObjectHash, Vec<u8>)>,
}

impl StoreSession<'_> {
    pub(crate) fn capture_test_changeset(&self, statements: &[String]) -> Result<Vec<u8>, DbError> {
        let tables = self
            .synced_tables
            .iter()
            .map(|table| table.name().to_string())
            .collect::<Vec<_>>();
        crate::DatabaseTestSql::for_store(self.conn, self.store_dir)
            .capture_changeset(&tables, statements)
    }

    pub(crate) fn apply_test_changeset(&self, bytes: &[u8]) -> Result<crate::ApplyResult, DbError> {
        crate::resolve_and_apply_changeset(
            self.conn,
            self.store_dir,
            bytes,
            self.synced_tables,
            self.hlc.wall_now_ms(),
        )
    }

    fn export_prepared_write(&self, write_id: &WriteId) -> Result<PreparedWriteTransfer, DbError> {
        let connection = self.conn;
        let write = connection
            .query_row(
                "SELECT status, affected_rows, changeset_hash,
                        base, blob_facts, prepared
                 FROM store_writes WHERE write_id = ?1",
                [write_id.as_str()],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                },
            )
            .map_err(DbError::from)?;
        let partitions = {
            let mut statement = connection
                .prepare(
                    "SELECT audience, control_coord, changeset_hash
                     FROM store_write_partitions WHERE write_id = ?1 ORDER BY audience",
                )
                .map_err(DbError::from)?;
            let rows = statement
                .query_map([write_id.as_str()], |row| {
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
                .query_map([write_id.as_str()], |row| Ok((row.get(0)?, row.get(1)?)))
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
                .query_map([write_id.as_str()], |row| {
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
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .map_err(DbError::from)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(DbError::from)?;
            rows
        };
        let mut owner_keys = vec![crate::payload_store::store_write_owner_key(write_id)];
        for (object_id, _) in &remotes {
            let object_id = object_id
                .parse()
                .map_err(|error| DbError::context("parse transferred remote object id", error))?;
            owner_keys.push(crate::payload_store::remote_object_owner_key(object_id));
        }
        let mut payload_claims = Vec::new();
        let mut payload_hashes = BTreeSet::new();
        for owner_key in owner_keys {
            let claims = {
                let mut statement = connection
                    .prepare(
                        "SELECT payload_hash FROM payload_owners
                         WHERE owner_key = ?1 ORDER BY payload_hash",
                    )
                    .map_err(DbError::from)?;
                let claims = statement
                    .query_map([&owner_key], |row| row.get::<_, String>(0))
                    .map_err(DbError::from)?
                    .map(|encoded| {
                        encoded.map_err(DbError::from)?.parse().map_err(|error| {
                            DbError::context("parse transferred payload hash", error)
                        })
                    })
                    .collect::<Result<BTreeSet<ObjectHash>, DbError>>()?;
                claims
            };
            payload_hashes.extend(claims.iter().copied());
            if !claims.is_empty() {
                payload_claims.push((owner_key, claims));
            }
        }
        let payloads = payload_hashes
            .into_iter()
            .map(|hash| {
                Ok((
                    hash,
                    crate::store::store_session::StoreRecords::new(self.conn, self.store_dir)
                        .payload(hash)?,
                ))
            })
            .collect::<Result<Vec<_>, DbError>>()?;
        Ok(PreparedWriteTransfer {
            write,
            partitions,
            packages,
            blobs,
            remotes,
            payload_claims,
            payloads,
        })
    }

    fn import_prepared_write(
        &self,
        write_id: &WriteId,
        transfer: PreparedWriteTransfer,
    ) -> Result<(), DbError> {
        let transaction = self.conn.unchecked_transaction().map_err(DbError::from)?;
        for (expected_hash, bytes) in &transfer.payloads {
            let actual_hash =
                crate::payload_store::write_payload_blocking(&transaction, self.store_dir, bytes)?;
            if actual_hash != *expected_hash {
                return Err(DbError::Message(format!(
                    "transferred payload expected {expected_hash} but stored as {actual_hash}"
                )));
            }
        }
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
                    "prepared write remote object conflicts with restored state".to_string(),
                ));
            }
        }
        transaction
            .execute(
                "INSERT INTO store_writes
                 (write_id, status, affected_rows, changeset_hash,
                  base, blob_facts, prepared)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                rusqlite::params![
                    write_id.as_str(),
                    transfer.write.0,
                    transfer.write.1,
                    transfer.write.2,
                    transfer.write.3,
                    transfer.write.4,
                    transfer.write.5,
                ],
            )
            .map_err(DbError::from)?;
        for (audience, control, changeset_hash) in transfer.partitions {
            transaction
                .execute(
                    "INSERT INTO store_write_partitions
                     (write_id, audience, control_coord, changeset_hash)
                     VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![write_id.as_str(), audience, control, changeset_hash],
                )
                .map_err(DbError::from)?;
        }
        for (audience, object_id) in transfer.packages {
            transaction
                .execute(
                    "INSERT INTO store_write_packages
                     (write_id, audience, remote_object_id) VALUES (?1, ?2, ?3)",
                    rusqlite::params![write_id.as_str(), audience, object_id],
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
                        write_id.as_str(),
                        audience,
                        locator_hash,
                        object_id,
                        spool_path
                    ],
                )
                .map_err(DbError::from)?;
        }
        for (owner_key, claims) in transfer.payload_claims {
            crate::payload_store::set_payload_owner_claims_on(&transaction, &owner_key, &claims)?;
        }
        transaction.commit().map_err(DbError::from)
    }

    fn seed_local_release_rows_for_test(
        &self,
        note_id: &str,
        photo_id: &str,
        cloud_path: &str,
        size: i64,
        hash: String,
    ) -> Result<(), DbError> {
        self.conn
            .execute(
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at)
                 VALUES (?1, 'Release', NULL, ?2, '0000000001000-0000-A', '2026-01-01')",
                rusqlite::params![note_id, 0_i64],
            )
            .map_err(DbError::from)?;
        self.conn
            .execute(
                "INSERT INTO note_photos
                 (id, note_id, kind, size, hash, _updated_at, created_at, cloud_path)
                 VALUES (?1, ?2, 'image', ?3, ?4,
                         '0000000001000-0000-A', '2026-01-01', ?5)",
                rusqlite::params![photo_id, note_id, size, hash, cloud_path],
            )
            .map(|_| ())
            .map_err(DbError::from)
    }

    fn register_external_blob_for_test(
        &self,
        reference: &coven_protocol::blob::RowBlobRef,
        path: &std::path::Path,
    ) -> Result<(), DbError> {
        crate::DatabaseTestSql::new(self.conn).register_external_blob(reference, path)
    }

    fn enqueue_blob_upload_for_test(
        &self,
        root_table: &str,
        root_id: &str,
        reference: &coven_protocol::blob::RowBlobRef,
        source_path: &std::path::Path,
        created_at: &str,
    ) -> Result<(), DbError> {
        crate::DatabaseTestSql::new(self.conn).enqueue_blob_upload(
            root_table,
            root_id,
            reference,
            source_path,
            false,
            created_at,
        )
    }

    fn cleanup_intent_count_for_test(
        &self,
        namespace: &str,
        blob_id: &str,
    ) -> Result<i64, DbError> {
        self.conn
            .query_row(
                "SELECT COUNT(*) FROM local_cleanup_intents
                 WHERE namespace = ?1 AND blob_id = ?2",
                (namespace, blob_id),
                |row| row.get(0),
            )
            .map_err(DbError::from)
    }

    fn coven_table_exists_for_test(
        &self,
        table: crate::DatabaseTestTable,
    ) -> Result<bool, DbError> {
        self.conn
            .query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1
                 )",
                [table.0],
                |row| row.get(0),
            )
            .map_err(DbError::from)
    }

    fn install_store_write_failure_trigger_for_test(&self) -> Result<(), DbError> {
        self.conn
            .execute_batch(
                "CREATE TRIGGER fail_store_write_journal
                 BEFORE INSERT ON store_writes
                 BEGIN
                   SELECT RAISE(ABORT, 'injected Store write journal failure');
                 END;",
            )
            .map_err(DbError::from)
    }

    fn remove_store_write_failure_trigger_for_test(&self) -> Result<(), DbError> {
        self.conn
            .execute_batch("DROP TRIGGER fail_store_write_journal")
            .map_err(DbError::from)
    }

    fn write_blob_facts_for_test(&self, write_id: &WriteId) -> Result<String, DbError> {
        self.conn
            .query_row(
                "SELECT blob_facts FROM store_writes WHERE write_id = ?1",
                [write_id.as_str()],
                |row| row.get(0),
            )
            .map_err(DbError::from)
    }

    fn install_test_active_circle(&self, label: &str) -> coven_protocol::circle::CircleId {
        self.install_test_active_circle_with_control(label).0
    }

    fn install_test_active_circle_with_control(
        &self,
        label: &str,
    ) -> (
        coven_protocol::circle::CircleId,
        coven_protocol::circle::CircleControlCoord,
    ) {
        let database = crate::DatabaseTestSql::new(self.conn);
        database.install_test_active_circle(label)
    }

    fn install_test_inactive_circle(&self, label: &str) -> coven_protocol::circle::CircleId {
        let database = crate::DatabaseTestSql::new(self.conn);
        let (circle_id, _) = database.install_test_inactive_circle(label);
        circle_id
    }

    fn insert_write_status_for_test(
        &self,
        write_id: &WriteId,
        status: &str,
        base: &str,
    ) -> Result<(), DbError> {
        let transaction = self.conn.unchecked_transaction().map_err(DbError::from)?;
        let changeset_hash =
            crate::payload_store::write_payload_blocking(&transaction, self.store_dir, b"")?;
        let owner_key = crate::payload_store::store_write_owner_key(write_id);
        transaction
            .execute(
                r#"INSERT INTO store_writes
                 (write_id, status, affected_rows, changeset_hash, base, blob_facts)
                 VALUES (?1, ?2, '[]', ?3, ?4, '{"blobs":[]}')"#,
                (write_id.as_str(), status, changeset_hash.to_string(), base),
            )
            .map_err(DbError::from)?;
        crate::payload_store::set_payload_owner_claims_on(
            &transaction,
            &owner_key,
            &BTreeSet::from([changeset_hash]),
        )?;
        transaction.commit().map_err(DbError::from)
    }

    fn delete_write_for_test(&self, write_id: &WriteId) -> Result<(), DbError> {
        let transaction = self.conn.unchecked_transaction().map_err(DbError::from)?;
        crate::payload_store::release_payload_owner_on(
            &transaction,
            &crate::payload_store::store_write_owner_key(write_id),
        )?;
        transaction
            .execute(
                "DELETE FROM store_writes WHERE write_id = ?1",
                [write_id.as_str()],
            )
            .map_err(DbError::from)?;
        transaction.commit().map_err(DbError::from)
    }

    fn store_write_partition_for_test(&self, write_id: &WriteId) -> Result<Vec<u8>, DbError> {
        let encoded: String = self
            .conn
            .query_row(
                "SELECT changeset_hash FROM store_write_partitions
                 WHERE write_id = ?1 AND audience = 'store'",
                [write_id.as_str()],
                |row| row.get(0),
            )
            .map_err(DbError::from)?;
        let hash = encoded
            .parse()
            .map_err(|error| DbError::context("parse captured changeset hash", error))?;
        Ok(
            crate::store::store_session::StoreRecords::new(self.conn, self.store_dir)
                .payload(hash)?,
        )
    }

    fn write_blob_lease_count_for_test(&self, write_id: &WriteId) -> Result<i64, DbError> {
        self.conn
            .query_row(
                "SELECT COUNT(*) FROM store_write_blob_leases WHERE write_id = ?1",
                [write_id.as_str()],
                |row| row.get(0),
            )
            .map_err(DbError::from)
    }

    fn latest_materialized_commit_coordinate_for_test(&self) -> Result<(String, u64), DbError> {
        let (device_id, sequence): (String, i64) = self
            .conn
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
    }

    fn compare_circle_bootstrap_replay_with_missing_coverage_for_test(
        &mut self,
        root: &coven_protocol::store_commit::StoreRootRef,
        routing_key: &coven_protocol::circle::RowRoutingKey,
        historical_id: &str,
        late_id: &str,
    ) -> Result<(i64, i64, i64, i64), DbError> {
        let transaction = self.conn.unchecked_transaction().map_err(DbError::from)?;
        let retained =
            crate::store::store_session::StoreTransaction::new(&transaction, self.store_dir)
                .replay_projection_with_authority(
                    self.verified_store_authority,
                    root,
                    self.blob_decls,
                    self.gates,
                    self.synced_tables,
                    Some(routing_key),
                    &BTreeSet::new(),
                    None,
                    false,
                    coven_protocol::membership::LocalStoreMembership::Current,
                )?;
        let retained_count = retained.document_count(historical_id)?;
        let retained_late_count = retained.document_count(late_id)?;
        transaction
            .execute("DELETE FROM circle_bootstrap_coverage", [])
            .map_err(DbError::from)?;
        let sabotaged =
            crate::store::store_session::StoreTransaction::new(&transaction, self.store_dir)
                .replay_projection_with_authority(
                    self.verified_store_authority,
                    root,
                    self.blob_decls,
                    self.gates,
                    self.synced_tables,
                    Some(routing_key),
                    &BTreeSet::new(),
                    None,
                    false,
                    coven_protocol::membership::LocalStoreMembership::Current,
                )?;
        let sabotaged_count = sabotaged.document_count(historical_id)?;
        let sabotaged_late_count = sabotaged.document_count(late_id)?;
        transaction.rollback().map_err(DbError::from)?;
        Ok((
            retained_count,
            retained_late_count,
            sabotaged_count,
            sabotaged_late_count,
        ))
    }

    fn circle_bootstrap_coverage_count_for_test(
        &self,
        circle_id: coven_protocol::circle::CircleId,
    ) -> Result<i64, DbError> {
        self.conn
            .query_row(
                "SELECT COUNT(*) FROM circle_bootstrap_coverage WHERE circle_id = ?1",
                [circle_id.to_string()],
                |row| row.get(0),
            )
            .map_err(DbError::from)
    }

    fn reject_missing_circle_bootstrap_payload_claim_for_test(
        &self,
        circle_id: coven_protocol::circle::CircleId,
    ) -> Result<String, DbError> {
        let transaction = self.conn.unchecked_transaction().map_err(DbError::from)?;
        transaction
            .execute(
                "DELETE FROM payload_owners WHERE owner_key = ?1",
                [crate::payload_store::circle_bootstrap_coverage_owner_key(
                    circle_id,
                )],
            )
            .map_err(DbError::from)?;
        let error =
            crate::store::store_session::StoreTransaction::new(&transaction, self.store_dir)
                .circle_bootstrap_replay_inputs()
                .expect_err("Circle bootstrap replay must require its payload claim");
        transaction.rollback().map_err(DbError::from)?;
        Ok(error.to_string())
    }

    fn reject_changed_circle_bootstrap_image_hash_for_test(
        &mut self,
        circle_id: coven_protocol::circle::CircleId,
        root: &coven_protocol::store_commit::StoreRootRef,
        activation_commit: &StoreBatchCommitRef,
    ) -> Result<String, DbError> {
        let transaction = self.conn.unchecked_transaction().map_err(DbError::from)?;
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
        let retained =
            crate::store::store_session::StoreTransaction::new(&transaction, self.store_dir)
                .load_retained_merge_materialization_by_ref(
                    root,
                    self.verified_store_authority,
                    activation_commit,
                )?;
        let error =
            crate::store::store_session::StoreTransaction::new(&transaction, self.store_dir)
                .record_circle_bootstrap_coverage(
                    self.verified_store_authority,
                    root,
                    activation_commit,
                    retained.circle_activations(),
                )
                .expect_err("changed image hash must conflict with its exact reference");
        transaction.rollback().map_err(DbError::from)?;
        Ok(error.to_string())
    }

    fn author_exclusion_activation_evidence_for_test(
        &self,
        exclusion: &str,
    ) -> Result<(String, String), DbError> {
        self.conn
            .query_row(
                "SELECT accepted_cut, activation_head
                 FROM store_author_exclusion_activations
                 WHERE exclusion_ref = ?1",
                [exclusion],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(DbError::from)
    }

    fn tamper_author_exclusion_locator_for_test(
        &self,
        exclusion: StoreDeviceExclusionRef,
        candidate: &StoreBatchCommitRef,
        tamper: AuthorExclusionLocatorTamper,
    ) -> Result<(), DbError> {
        let connection = self.conn;
        let exact = serde_json::to_string(&exclusion)
            .map_err(|error| DbError::context("serialize exact exclusion reference", error))?;
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
                    coven_protocol::causal_grants::AuthorStreamId,
                    StoreBatchCommitRef,
                > = serde_json::from_str(&cut)
                    .map_err(|error| DbError::context("parse exclusion accepted cut", error))?;
                cut.insert(
                    coven_protocol::causal_grants::AuthorStreamId::from_digest(ObjectHash::digest(
                        b"wrong exclusion accepted-cut stream",
                    )),
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
                let wrong = serde_json::to_string(candidate).map_err(|error| {
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
                let mut head: StoreDeviceHeadRef = serde_json::from_str(&head)
                    .map_err(|error| DbError::context("parse exclusion activation head", error))?;
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
    }
}

mod database;

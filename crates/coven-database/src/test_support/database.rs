use crate::{Database, DatabaseTestSql, DbError};
use rusqlite::OptionalExtension;

impl Database {
    /// The directory this database opened under, and with it the payload spool
    /// its rows name. Tests that reach past the async API into `test_sql` need
    /// the same pair the real read paths carry.
    pub fn store_dir_for_test(&self) -> &coven_foundation::store_dir::StoreDir {
        &self.state.store_dir
    }

    pub async fn remove_store_protocol_root_for_test(&self) {
        self.test_sql(|database| database.remove_store_protocol_root())
            .await
            .expect("remove exact Store root authority");
    }

    pub async fn tamper_retained_recovery_registration_for_test(
        &self,
        reference: &coven_protocol::store_commit::StoreBatchCommitRef,
        tamper: crate::RetainedRegistrationTamper,
    ) {
        let reference = reference.clone();
        self.test_sql(move |database| {
            database.tamper_retained_recovery_registration(&reference, tamper)
        })
        .await
        .expect("install tampered retained recovery registration");
    }

    pub async fn execute_test_sql(&self, sql: &str) {
        let sql = sql.to_string();
        self.test_sql(move |database| database.execute_batch(&sql).map_err(DbError::from))
            .await
            .unwrap_or_else(|error| panic!("test SQL execution failed: {error}"));
    }

    pub async fn execute_test_host_write(&self, sql: &str) {
        let sql = sql.to_string();
        crate::StoreDatabase::new(self)
            .run_prepared_blob_transition_write_for_test(None, move |transaction| {
                transaction.execute_batch(&sql).map_err(DbError::from)
            })
            .await
            .unwrap_or_else(|error| panic!("test host write failed: {error}"));
    }

    pub async fn add_local_photo_for_test(
        &self,
        note_id: &str,
        photo_id: &str,
        cloud_path: &str,
        bytes: &[u8],
        source: &std::path::Path,
    ) {
        let note_id = note_id.to_string();
        let photo_id = photo_id.to_string();
        let cloud_path = cloud_path.to_string();
        let size = i64::try_from(bytes.len()).expect("test blob size fits SQLite");
        let hash = coven_protocol::blob::content_hash(bytes);
        self.execute_test_host_write(&format!(
            "INSERT INTO note_photos
             (id, note_id, kind, size, hash, _updated_at, created_at, cloud_path)
             VALUES ('{photo_id}', '{note_id}', 'image', {size}, '{hash}',
                     '0000000001000-0000-A', '2026-01-01', '{cloud_path}')"
        ))
        .await;
        crate::StoreDatabase::new(self)
            .register_external_blob_for_test("note_photos", &photo_id, source)
            .await;
    }

    pub async fn insert_local_upload_rows_for_test(
        &self,
        root_id: &str,
        rows: &[(&str, &[u8])],
    ) -> Result<(), DbError> {
        let root_id = root_id.to_string();
        let rows = rows
            .iter()
            .map(|(id, bytes)| {
                (
                    id.to_string(),
                    i64::try_from(bytes.len()).expect("test blob size fits SQLite"),
                    coven_protocol::blob::content_hash(bytes),
                )
            })
            .collect::<Vec<_>>();
        self.test_sql(move |database| {
            database
                .execute(
                    "INSERT INTO notes (id, title, shared, _updated_at, created_at)
                     VALUES (?1, 'upload', 0, '0000000001000-0000-test', '2024-01-01')",
                    [&root_id],
                )
                .map_err(DbError::from)?;
            for (id, size, hash) in rows {
                database
                    .execute(
                        "INSERT INTO note_photos
                         (id, note_id, kind, size, hash, _updated_at, created_at)
                         VALUES (?1, ?2, 'attach', ?3, ?4,
                                 '0000000001000-0000-test', '2024-01-01')",
                        rusqlite::params![id, root_id, size, hash],
                    )
                    .map_err(DbError::from)?;
            }
            Ok(())
        })
        .await
    }

    pub async fn seed_stuck_blob_upload_for_test(&self, created_at: &str) -> Result<(), DbError> {
        let hash = coven_protocol::blob::content_hash(b"x");
        self.execute_test_sql(&format!(
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('pending-root', 'Pending', NULL, 0, \
                     '0000000000001-0000-M', '2026-01-01'); \
             INSERT INTO note_photos \
                    (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('pending-blob', 'pending-root', 'cover', 1, '{hash}', \
                     '0000000000001-0000-M', '2026-01-01')"
        ))
        .await;
        let row = self.row_blob_ref("note_photos", "pending-blob").await?;
        let created_at = created_at.to_string();
        self.test_sql(move |database| {
            database.enqueue_blob_upload(
                "notes",
                "pending-root",
                &row,
                std::path::Path::new("/nonexistent/pending-blob"),
                false,
                &created_at,
            )
        })
        .await
    }

    pub async fn query_test_text(&self, sql: &str) -> String {
        let sql = sql.to_string();
        self.test_sql(move |database| {
            database
                .query_row(&sql, [], |row| row.get::<_, String>(0))
                .map_err(DbError::from)
        })
        .await
        .unwrap_or_else(|error| panic!("test text query failed: {error}"))
    }

    pub async fn test_row_exists(&self, sql: &str) -> bool {
        let sql = sql.to_string();
        self.test_sql(move |database| {
            database
                .query_row(&sql, [], |_| Ok(()))
                .optional()
                .map(|row| row.is_some())
                .map_err(DbError::from)
        })
        .await
        .unwrap_or_else(|error| panic!("test row-existence query failed: {error}"))
    }

    pub async fn capture_test_changeset(&self, statements: &[&str]) -> Vec<u8> {
        let statements = statements
            .iter()
            .map(|statement| statement.to_string())
            .collect::<Vec<_>>();
        let tables = self
            .synced_tables()
            .iter()
            .map(|table| table.name().to_string())
            .collect::<Vec<_>>();
        self.test_sql(move |database| database.capture_changeset(&tables, &statements))
            .await
            .unwrap_or_else(|error| panic!("test changeset capture failed: {error}"))
    }

    pub async fn capture_test_changeset_for_tables(&self, tables: &[&str], sql: &str) -> Vec<u8> {
        let tables = tables
            .iter()
            .map(|table| table.to_string())
            .collect::<Vec<_>>();
        let sql = sql.to_string();
        self.test_sql(move |database| database.capture_changeset(&tables, &[sql]))
            .await
            .unwrap_or_else(|error| panic!("raw test changeset capture failed: {error}"))
    }

    async fn apply_test_changeset_result(
        &self,
        bytes: &[u8],
    ) -> Result<crate::ApplyResult, DbError> {
        let bytes = bytes.to_vec();
        let tables = self.synced_tables().to_vec();
        let receiver_wall_ms = self.receive_wall_ms();
        let store_dir = self.state.store_dir.clone();
        self.test_sql(move |database| {
            database.apply_changeset(&store_dir, &bytes, &tables, receiver_wall_ms)
        })
        .await
    }

    pub async fn try_apply_test_changeset(&self, bytes: &[u8]) -> Result<(), DbError> {
        self.apply_test_changeset_result(bytes).await.map(|_| ())
    }

    pub async fn apply_test_changeset(&self, bytes: &[u8]) {
        self.try_apply_test_changeset(bytes)
            .await
            .expect("apply test changeset");
    }

    pub async fn apply_test_changeset_reporting_foreign_key_violations(
        &self,
        bytes: &[u8],
    ) -> Result<bool, DbError> {
        self.apply_test_changeset_result(bytes)
            .await
            .map(|result| result.had_fk_violations)
    }

    pub async fn plant_blob_row_for_test(&self, blob_id: &str, remote: bool, bytes: &[u8]) {
        self.plant_blob_row_with_facts_for_test(
            blob_id,
            remote,
            bytes.len() as u64,
            Some(&coven_protocol::blob::content_hash(bytes)),
        )
        .await;
    }

    pub async fn plant_blob_row_with_facts_for_test(
        &self,
        blob_id: &str,
        remote: bool,
        size: u64,
        hash: Option<&str>,
    ) {
        let note = format!("note-{blob_id}");
        let blob_id = blob_id.to_string();
        let hash = hash.map(str::to_string);
        self.test_sql(move |database| {
            database
                .execute(
                    "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
                     VALUES (?1, 'read-test', ?2, '0000000001000-0000-dev1', '2026-01-01')",
                    (note.as_str(), remote as i64),
                )
                .map_err(DbError::from)?;
            database
                .execute(
                    "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
                     VALUES (?1, ?2, 'attach', ?3, ?4, '0000000001000-0000-dev1', '2026-01-01')",
                    rusqlite::params![blob_id.as_str(), note.as_str(), size as i64, hash],
                )
                .map_err(DbError::from)?;
            Ok(())
        })
        .await
        .expect("plant test blob row");
    }

    pub async fn set_blob_remote_for_test(&self, blob_id: &str, remote: bool) {
        let note = format!("note-{blob_id}");
        self.test_sql(move |database| {
            database
                .execute(
                    "UPDATE notes SET shared = ?1 WHERE id = ?2",
                    (remote as i64, note.as_str()),
                )
                .map(|_| ())
                .map_err(DbError::from)
        })
        .await
        .expect("change test blob locality");
    }

    pub async fn run_scoped_host_write_for_test(&self, sql: String) {
        crate::StoreDatabase::new(self)
            .run_host_store_write_for_test(
                Some(coven_keys::encryption::EncryptionService::from_key(
                    [42; 32],
                )),
                None,
                move |transaction| transaction.execute_batch(&sql).map_err(DbError::from),
            )
            .await
            .expect("commit scoped host write");
    }

    pub async fn scoped_routing_state_for_test(
        &self,
        row_id: &str,
    ) -> crate::ScopedRoutingStateForTest {
        let row_id = row_id.to_string();
        self.test_sql(move |database| {
            let (row, route, mirror) = database.scoped_note_routing_state(&row_id, [42; 32])?;
            Ok(crate::ScopedRoutingStateForTest { row, route, mirror })
        })
        .await
        .expect("read scoped routing state")
    }

    pub async fn circle_control_activation_count_for_test(
        &self,
        circle_id: coven_protocol::circle::CircleId,
    ) -> i64 {
        self.test_sql(move |database| database.circle_control_activation_count(circle_id))
            .await
            .expect("count Circle control activations")
    }

    pub async fn row_blob_binding_count_for_test(&self, row_id: &str) -> i64 {
        let row_id = row_id.to_string();
        self.test_sql(move |database| database.row_blob_binding_count(&row_id))
            .await
            .expect("count row blob bindings")
    }

    pub async fn bind_circle_row_blob_for_test(&self, row_id: &str) {
        let row_id = row_id.to_string();
        let object_id = "0".repeat(64);
        self.test_sql(move |database| {
            database.install_blob_binding(
                &object_id,
                "{}",
                &"1".repeat(64),
                "notes",
                &row_id,
                "attachment",
                "0000000002000-0000-owner",
                "{}",
            )
        })
        .await
        .expect("bind Circle row blob");
    }

    pub async fn table_has_rows_for_test(
        &self,
        table: crate::DatabaseTestTable,
    ) -> Result<bool, DbError> {
        self.test_sql(move |database| database.table_has_rows(table))
            .await
    }

    pub async fn store_partition_changesets_for_test(&self) -> Result<Vec<Vec<u8>>, DbError> {
        self.test_sql(|database| database.store_partition_changesets())
            .await
    }

    pub async fn has_store_partition_for_test(&self) -> Result<bool, DbError> {
        self.test_sql(|database| database.has_store_partition())
            .await
    }

    pub async fn make_remote_intent_exists_for_test(
        &self,
        root_table: &str,
        root_id: &str,
    ) -> Result<bool, DbError> {
        let root_table = root_table.to_string();
        let root_id = root_id.to_string();
        self.test_sql(move |database| database.make_remote_intent_exists(&root_table, &root_id))
            .await
    }

    pub async fn published_blob_drop_intent_exists_for_test(
        &self,
        blob_id: &str,
    ) -> Result<bool, DbError> {
        let blob_id = blob_id.to_string();
        self.test_sql(move |database| database.published_blob_drop_intent_exists(&blob_id))
            .await
    }

    pub async fn insert_published_blob_drop_intent_for_test(
        &self,
        sequence: u64,
        namespace: &str,
        blob_id: &str,
        bytes: &[u8],
        locator_hash: coven_protocol::store_commit::ObjectHash,
        disposition: coven_protocol::blob::DeferredLocalBlobDisposition,
    ) -> Result<(), DbError> {
        let drop = coven_protocol::blob::DeferredLocalBlobDrop {
            namespace: namespace.to_string(),
            id: blob_id.to_string(),
            size: bytes.len() as u64,
            plaintext_hash: coven_protocol::store_commit::ObjectHash::digest(bytes),
            locator_hash,
            disposition,
        };
        self.test_sql(move |database| database.insert_published_blob_drop_intent(sequence, &drop))
            .await
    }

    pub async fn remote_object_for_test(
        &self,
        object: coven_protocol::objects::ExactObjectRef,
    ) -> Result<coven_protocol::remote_object::RemoteObjectRecord, DbError> {
        self.test_sql(move |database| database.remote_object(&object))
            .await
    }

    pub async fn retained_store_package_pin_for_test(
        &self,
        commit: &coven_protocol::store_commit::StoreBatchCommitRef,
    ) -> Result<
        (
            coven_protocol::remote_object::RetainedReplayOwner,
            coven_protocol::store_commit::StorePackageRef,
            coven_protocol::remote_object::RemoteObjectRecord,
        ),
        DbError,
    > {
        let stream_id = commit.coord.stream_id.to_string();
        let sequence = commit.coord.sequence();
        let (input_hash, canonical_input) = self
            .test_sql(move |database| database.retained_merge_input(&stream_id, sequence))
            .await?;
        let retained: serde_json::Value = serde_json::from_slice(&canonical_input)
            .map_err(|error| DbError::context("parse retained package input", error))?;
        let reference: coven_protocol::store_commit::StorePackageRef = serde_json::from_value(
            retained["packages"][0]["store"]["reference"].clone(),
        )
        .map_err(|error| DbError::context("parse retained Store package reference", error))?;
        let remote = self
            .remote_object_for_test(reference.object.clone())
            .await?;
        Ok((
            coven_protocol::remote_object::RetainedReplayOwner::Commit {
                commit: commit.clone(),
                input_hash,
            },
            reference,
            remote,
        ))
    }

    pub async fn remote_objects_for_test(
        &self,
    ) -> Result<Vec<coven_protocol::remote_object::RemoteObjectRecord>, DbError> {
        self.test_sql(|database| database.remote_objects()).await
    }

    pub async fn remote_object_exists_for_test(
        &self,
        object: coven_protocol::objects::ExactObjectRef,
    ) -> Result<bool, DbError> {
        self.test_sql(move |database| database.remote_object_exists(&object))
            .await
    }

    pub async fn remote_object_id_exists_for_test(
        &self,
        object_id: coven_protocol::store_commit::ObjectHash,
    ) -> Result<bool, DbError> {
        self.test_sql(move |database| database.remote_object_id_exists(object_id))
            .await
    }

    pub async fn replace_remote_object_for_test(
        &self,
        object: coven_protocol::objects::ExactObjectRef,
        remote: coven_protocol::remote_object::RemoteObjectRecord,
    ) -> Result<(), DbError> {
        self.test_sql(move |database| database.replace_remote_object(&object, &remote))
            .await
    }

    pub async fn delete_remote_object_for_test(
        &self,
        object: coven_protocol::objects::ExactObjectRef,
    ) -> Result<(), DbError> {
        self.test_sql(move |database| database.delete_remote_object(&object))
            .await
    }

    pub async fn enqueue_blob_delete_for_test(
        &self,
        stored: &coven_protocol::blob::locator::StoredBlobRef,
        created_at: &str,
    ) -> Result<(), DbError> {
        let stored = stored.clone();
        let created_at = created_at.to_string();
        self.test_sql(move |database| database.enqueue_blob_delete(&stored, &created_at))
            .await
    }

    pub async fn delete_outbox_attempt_for_test(
        &self,
        id: i64,
    ) -> Result<Option<crate::OutboxAttempt>, DbError> {
        self.test_sql(move |database| database.delete_outbox_attempt(id))
            .await
    }

    pub async fn insert_local_blob_row_for_test(
        &self,
        root_id: &str,
        row_id: &str,
        blob_id: &str,
        cloud_path: Option<&str>,
        bytes: &[u8],
    ) -> Result<(), DbError> {
        let root_id = root_id.to_string();
        let row_id = row_id.to_string();
        let blob_id = blob_id.to_string();
        let cloud_path = cloud_path.map(str::to_string);
        let size = i64::try_from(bytes.len()).expect("test blob size fits SQLite");
        let hash = coven_protocol::store_commit::ObjectHash::digest(bytes).to_string();
        crate::StoreDatabase::new(self)
            .run_host_store_write_for_test(None, None, move |transaction| {
                transaction
                    .execute(
                        "INSERT INTO notes
                         (id, title, body, shared, _updated_at, created_at)
                         VALUES (?1, 'blob root', NULL, 0, '0000000001000-0000-dev1', '2026-01-01')",
                        [root_id.as_str()],
                    )
                    .map_err(DbError::from)?;
                transaction
                    .execute(
                        "INSERT INTO note_photos
                         (id, note_id, kind, size, hash, cloud_path, blob_id, _updated_at, created_at)
                         VALUES (?1, ?2, 'cover', ?3, ?4, ?5, ?6,
                                 '0000000001000-0000-dev1', '2026-01-01')",
                        rusqlite::params![row_id, root_id, size, hash, cloud_path, blob_id],
                    )
                    .map_err(DbError::from)?;
                Ok(())
            })
            .await
            .map(|_| ())
    }

    pub async fn capture_circle_document_for_test(
        &self,
        row_id: &str,
        circle_id: coven_protocol::circle::CircleId,
        stamp: &str,
    ) -> Result<coven_protocol::write::WriteId, DbError> {
        let routing = coven_keys::encryption::EncryptionService::from_key([42; 32]);
        let audience_value = circle_id.to_string();
        let row_id = row_id.to_string();
        let stamp = stamp.to_string();
        let receipt = crate::StoreDatabase::new(self)
            .run_host_store_write_for_test(Some(routing), None, move |transaction| {
                transaction
                    .execute(
                        "INSERT INTO documents (id, audience, _updated_at)
                             VALUES (?1, ?2, ?3)",
                        rusqlite::params![row_id, audience_value, stamp],
                    )
                    .map(|_| ())
                    .map_err(DbError::from)
            })
            .await?;
        Ok(receipt.write_id)
    }

    pub async fn circle_document_present_for_test(&self, row_id: &str) -> Result<bool, DbError> {
        let row_id = row_id.to_string();
        self.test_sql(move |database| {
            database
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM documents WHERE id = ?1)",
                    [row_id],
                    |row| row.get::<_, bool>(0),
                )
                .map_err(DbError::from)
        })
        .await
    }

    pub async fn local_store_device_id_for_test(
        &self,
    ) -> Result<coven_protocol::store_commit::StoreDeviceId, DbError> {
        self.get_protocol_state(crate::LOCAL_DEVICE_ID_STATE_KEY)
            .await?
            .ok_or_else(|| DbError::Message("local device id is not installed".to_string()))?
            .parse()
            .map_err(|error| DbError::context("parse local device id", error))
    }

    pub async fn capture_document_for_test(
        &self,
        row_id: &str,
        audience: Option<coven_protocol::circle::CircleId>,
        stamp: &str,
    ) -> Result<coven_protocol::write::WriteId, DbError> {
        let routing = coven_keys::encryption::EncryptionService::from_key([42; 32]);
        let audience = audience.map(|circle_id| circle_id.to_string());
        let row_id = row_id.to_string();
        let stamp = stamp.to_string();
        let receipt = crate::StoreDatabase::new(self)
            .run_host_store_write_for_test(Some(routing), None, move |transaction| {
                transaction
                    .execute(
                        "INSERT INTO documents (id, audience, _updated_at)
                         VALUES (?1, ?2, ?3)",
                        rusqlite::params![row_id, audience, stamp],
                    )
                    .map(|_| ())
                    .map_err(DbError::from)
            })
            .await?;
        Ok(receipt.write_id)
    }

    pub async fn capture_document_with_file_for_test(
        &self,
        document_id: &str,
        file_id: &str,
        audience: Option<coven_protocol::circle::CircleId>,
        bytes: &[u8],
        stamp: &str,
    ) -> Result<coven_protocol::write::WriteId, DbError> {
        let routing = coven_keys::encryption::EncryptionService::from_key([42; 32]);
        let document_id = document_id.to_string();
        let file_id = file_id.to_string();
        let audience = audience.map(|circle_id| circle_id.to_string());
        let size = i64::try_from(bytes.len()).expect("test blob size fits SQLite");
        let hash = coven_protocol::blob::content_hash(bytes);
        let stamp = stamp.to_string();
        let receipt = crate::StoreDatabase::new(self)
            .run_host_store_write_for_test(Some(routing), None, move |transaction| {
                transaction
                    .execute(
                        "INSERT INTO documents (id, audience, _updated_at)
                         VALUES (?1, ?2, ?3)",
                        rusqlite::params![document_id, audience, stamp],
                    )
                    .map_err(DbError::from)?;
                transaction
                    .execute(
                        "INSERT INTO document_files
                         (id, document_id, size, hash, _updated_at)
                         VALUES (?1, ?2, ?3, ?4, ?5)",
                        rusqlite::params![file_id, document_id, size, hash, stamp],
                    )
                    .map(|_| ())
                    .map_err(DbError::from)
            })
            .await?;
        Ok(receipt.write_id)
    }

    pub async fn document_file_stamp_for_test(&self, file_id: &str) -> Result<String, DbError> {
        let file_id = file_id.to_string();
        self.test_sql(move |database| {
            database
                .query_row(
                    "SELECT _updated_at FROM document_files WHERE id = ?1",
                    [file_id],
                    |row| row.get::<_, String>(0),
                )
                .map_err(DbError::from)
        })
        .await
    }

    pub async fn release_retained_replay_ownership_for_test(&self) -> Result<(), DbError> {
        self.test_sql(|database| {
            database.transaction(|transaction| {
                transaction.remove_retained_replay_ownership_from_snapshot()
            })
        })
        .await
    }

    pub async fn insert_browsable_blob_row_for_test(
        &self,
        blob_id: &str,
        cloud_path: &str,
        bytes: &[u8],
    ) -> Result<(), DbError> {
        let note = format!("note-{blob_id}");
        let blob_id = blob_id.to_string();
        let cloud_path = cloud_path.to_string();
        let size = i64::try_from(bytes.len()).expect("test blob size fits SQLite");
        let hash = coven_protocol::blob::content_hash(bytes);
        self.test_sql(move |database| {
            database
                .execute(
                    "INSERT INTO notes (id, title, shared, _updated_at, created_at)
                     VALUES (?1, 'browsable-test', 1, '0000000001000-0000-dev1', '2026-01-01')",
                    [note.as_str()],
                )
                .map_err(DbError::from)?;
            database
                .execute(
                    "INSERT INTO note_photos
                     (id, note_id, kind, size, hash, _updated_at, created_at)
                     VALUES (?1, ?2, ?3, ?4, ?5,
                             '0000000001000-0000-dev1', '2026-01-01')",
                    rusqlite::params![blob_id, note, cloud_path, size, hash],
                )
                .map_err(DbError::from)?;
            Ok(())
        })
        .await
    }

    pub async fn bind_stored_blob_to_row_for_test(
        &self,
        stored: &coven_protocol::blob::locator::StoredBlobRef,
        table: &str,
        id: &str,
        owner: coven_protocol::store_commit::StoreBatchCommitRef,
    ) -> Result<(), DbError> {
        let locator = stored.locator().clone();
        let record =
            coven_protocol::remote_object::RemoteObjectRecord::activated_blob(stored, owner)
                .map_err(|error| DbError::Message(error.to_string()))?
                .into_record();
        let object_id = record.object_id().to_string();
        let state =
            serde_json::to_string(&record).map_err(|error| DbError::Message(error.to_string()))?;
        let locator_hash = locator.locator_hash().to_string();
        let authority =
            serde_json::to_string(&coven_protocol::audience_package::PackageAudience::Store)
                .map_err(|error| DbError::Message(error.to_string()))?;
        let id_for_insert = id.to_string();
        let table_for_insert = table.to_string();
        let stamp_table = table.to_string();
        let stamp_id = id.to_string();
        self.test_sql(move |database| {
            let row_stamp = database
                .query_row(
                    &format!(
                        "SELECT _updated_at FROM {} WHERE id = ?1",
                        crate::quote_ident(&stamp_table)
                    ),
                    [stamp_id],
                    |row| row.get::<_, String>(0),
                )
                .map_err(DbError::from)?;
            database.install_blob_binding(
                &object_id,
                &state,
                &locator_hash,
                &table_for_insert,
                &id_for_insert,
                "id",
                &row_stamp,
                &authority,
            )
        })
        .await
    }

    pub async fn store_package_is_retained_for_replay_for_test(
        &self,
        package: coven_protocol::store_commit::StorePackageRef,
        activation: coven_protocol::store_commit::StoreBatchCommitRef,
    ) -> Result<bool, DbError> {
        let database = crate::StoreDatabase::new(self);
        let root = database
            .local_store_root_ref()
            .await?
            .ok_or_else(|| DbError::Message("test Store root is not installed".to_string()))?;
        database
            .store_package_is_retained_for_replay(root, package, activation)
            .await
    }

    pub async fn test_sql<F, R>(&self, operation: F) -> Result<R, DbError>
    where
        F: for<'connection> FnOnce(DatabaseTestSql<'connection>) -> Result<R, DbError>
            + Send
            + 'static,
        R: Send + 'static,
    {
        let store_dir = self.state.store_dir.clone();
        self.connection
            .call(move |connection| operation(DatabaseTestSql::for_store(connection, &store_dir)))
            .await
    }
}

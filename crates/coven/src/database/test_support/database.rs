use crate::database::{Database, DatabaseTestSql, DbError};

impl Database {
    pub(crate) async fn table_has_rows_for_test(
        &self,
        table: crate::database::DatabaseTestTable,
    ) -> Result<bool, DbError> {
        self.test_sql(move |database| database.table_has_rows(table))
            .await
    }

    pub(crate) async fn store_partition_changesets_for_test(
        &self,
    ) -> Result<Vec<Vec<u8>>, DbError> {
        self.test_sql(|database| database.store_partition_changesets())
            .await
    }

    pub(crate) async fn has_store_partition_for_test(&self) -> Result<bool, DbError> {
        self.test_sql(|database| database.has_store_partition())
            .await
    }

    pub(crate) async fn make_remote_intent_exists_for_test(
        &self,
        root_table: &str,
        root_id: &str,
    ) -> Result<bool, DbError> {
        let root_table = root_table.to_string();
        let root_id = root_id.to_string();
        self.test_sql(move |database| database.make_remote_intent_exists(&root_table, &root_id))
            .await
    }

    pub(crate) async fn published_blob_drop_intent_exists_for_test(
        &self,
        blob_id: &str,
    ) -> Result<bool, DbError> {
        let blob_id = blob_id.to_string();
        self.test_sql(move |database| database.published_blob_drop_intent_exists(&blob_id))
            .await
    }

    pub(crate) async fn insert_published_blob_drop_intent_for_test(
        &self,
        sequence: u64,
        namespace: &str,
        blob_id: &str,
        bytes: &[u8],
        locator_hash: crate::protocol::store_commit::ObjectHash,
        disposition: crate::sync::cycle::DeferredLocalBlobDisposition,
    ) -> Result<(), DbError> {
        let drop = crate::sync::cycle::DeferredLocalBlobDrop {
            namespace: namespace.to_string(),
            id: blob_id.to_string(),
            size: bytes.len() as u64,
            plaintext_hash: crate::protocol::store_commit::ObjectHash::digest(bytes),
            locator_hash,
            disposition,
        };
        self.test_sql(move |database| database.insert_published_blob_drop_intent(sequence, &drop))
            .await
    }

    pub(crate) async fn remote_object_for_test(
        &self,
        object: crate::storage::ExactObjectRef,
    ) -> Result<crate::protocol::remote_object::RemoteObjectRecord, DbError> {
        self.test_sql(move |database| database.remote_object(&object))
            .await
    }

    pub(crate) async fn remote_objects_for_test(
        &self,
    ) -> Result<Vec<crate::protocol::remote_object::RemoteObjectRecord>, DbError> {
        self.test_sql(|database| database.remote_objects()).await
    }

    pub(crate) async fn remote_object_exists_for_test(
        &self,
        object: crate::storage::ExactObjectRef,
    ) -> Result<bool, DbError> {
        self.test_sql(move |database| database.remote_object_exists(&object))
            .await
    }

    pub(crate) async fn remote_object_id_exists_for_test(
        &self,
        object_id: crate::protocol::store_commit::ObjectHash,
    ) -> Result<bool, DbError> {
        self.test_sql(move |database| database.remote_object_id_exists(object_id))
            .await
    }

    pub(crate) async fn replace_remote_object_for_test(
        &self,
        object: crate::storage::ExactObjectRef,
        remote: crate::protocol::remote_object::RemoteObjectRecord,
    ) -> Result<(), DbError> {
        self.test_sql(move |database| database.replace_remote_object(&object, &remote))
            .await
    }

    pub(crate) async fn delete_remote_object_for_test(
        &self,
        object: crate::storage::ExactObjectRef,
    ) -> Result<(), DbError> {
        self.test_sql(move |database| database.delete_remote_object(&object))
            .await
    }

    pub(crate) async fn enqueue_blob_delete_for_test(
        &self,
        stored: &crate::blob::locator::StoredBlobRef,
        created_at: &str,
    ) -> Result<(), DbError> {
        let stored = stored.clone();
        let created_at = created_at.to_string();
        self.test_sql(move |database| database.enqueue_blob_delete(&stored, &created_at))
            .await
    }

    pub(crate) async fn delete_outbox_attempt_for_test(
        &self,
        id: i64,
    ) -> Result<Option<crate::database::OutboxAttempt>, DbError> {
        self.test_sql(move |database| database.delete_outbox_attempt(id))
            .await
    }

    pub(crate) async fn insert_local_blob_row_for_test(
        &self,
        root_id: &str,
        row_id: &str,
        blob_id: &str,
        cloud_path: Option<&str>,
        bytes: &[u8],
    ) -> Result<(), DbError> {
        let tables = self.synced_tables().to_vec();
        let write_id = self.new_write_id();
        let root_id = root_id.to_string();
        let row_id = row_id.to_string();
        let blob_id = blob_id.to_string();
        let cloud_path = cloud_path.map(str::to_string);
        let size = i64::try_from(bytes.len()).expect("test blob size fits SQLite");
        let hash = crate::protocol::store_commit::ObjectHash::digest(bytes).to_string();
        self.test_sql(move |database| {
            database.run_internal_store_write(&tables, None, write_id, |transaction| {
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
        })
        .await
    }

    pub(crate) async fn store_package_is_retained_for_replay_for_test(
        &self,
        package: crate::protocol::store_commit::StorePackageRef,
        activation: crate::protocol::store_commit::StoreBatchCommitRef,
    ) -> Result<bool, DbError> {
        let database = crate::database::StoreDatabase::new(self);
        let root = database
            .local_store_root_ref()
            .await?
            .ok_or_else(|| DbError::Message("test Store root is not installed".to_string()))?;
        database
            .store_package_is_retained_for_replay(root, package, activation)
            .await
    }

    pub(crate) async fn test_sql<F, R>(&self, operation: F) -> Result<R, DbError>
    where
        F: for<'connection> FnOnce(DatabaseTestSql<'connection>) -> Result<R, DbError>
            + Send
            + 'static,
        R: Send + 'static,
    {
        self.connection
            .call(move |connection| operation(DatabaseTestSql::new(connection)))
            .await
    }
}

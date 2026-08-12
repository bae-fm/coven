use super::*;

impl StoreDatabase {
    pub async fn seed_prepared_audience_write_for_test(
        &self,
        write_id: WriteId,
        changeset_hash: ObjectHash,
    ) -> Result<(), DbError> {
        self.call_store(move |session| {
            session.seed_prepared_audience_write_for_test(&write_id, changeset_hash)
        })
        .await
    }

    pub async fn persist_prepared_audience_objects_for_test(
        &self,
        write_id: WriteId,
        remotes: Vec<coven_protocol::remote_object::RemoteObjectRecord>,
        packages: Vec<crate::PreparedAudiencePackage>,
        blobs: Vec<crate::PreparedAudienceBlob>,
    ) -> Result<(), DbError> {
        self.call_store(move |session| {
            session
                .persist_prepared_audience_objects_for_test(&write_id, &remotes, &packages, &blobs)
        })
        .await
    }

    pub async fn seed_local_release_rows_for_test(
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
        let hash = coven_protocol::blob::content_hash(bytes);
        self.call_store(move |session| {
            session.seed_local_release_rows_for_test(&note_id, &photo_id, &cloud_path, size, hash)
        })
        .await
        .expect("seed exact release rows");
    }

    pub async fn register_external_blob_for_test(
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
        self.call_store(move |session| session.register_external_blob_for_test(&reference, &path))
            .await
            .expect("register exact external blob reference");
    }

    pub async fn enqueue_blob_upload_for_test(
        &self,
        root_table: &str,
        root_id: &str,
        reference: &coven_protocol::blob::RowBlobRef,
        source_path: &std::path::Path,
        created_at: &str,
    ) -> Result<(), DbError> {
        let reference = reference.clone();
        let root_table = root_table.to_string();
        let root_id = root_id.to_string();
        let source_path = source_path.to_path_buf();
        let created_at = created_at.to_string();
        self.call_store(move |session| {
            session.enqueue_blob_upload_for_test(
                &root_table,
                &root_id,
                &reference,
                &source_path,
                &created_at,
            )
        })
        .await
    }

    pub async fn insert_fixture_position_for_test(
        &self,
        note_id: &str,
    ) -> Result<(), crate::HostWriteError<DbError>> {
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

    pub async fn run_host_store_write_for_test<R>(
        &self,
        routing_encryption: Option<coven_keys::encryption::EncryptionService>,
        blob_staging: Option<Box<dyn crate::AudienceBlobMoveStaging>>,
        operation: impl for<'context, 'connection> FnOnce(
                crate::SqlContext<'context, 'connection>,
            ) -> Result<R, DbError>
            + Send
            + 'static,
    ) -> Result<coven_protocol::write::WriteReceipt<R>, crate::HostWriteError<DbError>>
    where
        R: Send + 'static,
    {
        crate::StoreRowWrites::new(self.clone())
            .execute(
                crate::HostWriteOperation::new(crate::WriteBatch::new(), operation),
                routing_encryption,
                blob_staging,
            )
            .await
    }

    pub async fn cleanup_intent_count_for_test(
        &self,
        namespace: &str,
        blob_id: &str,
    ) -> Result<i64, DbError> {
        let namespace = namespace.to_string();
        let blob_id = blob_id.to_string();
        self.call_store(move |session| session.cleanup_intent_count_for_test(&namespace, &blob_id))
            .await
    }

    pub async fn coven_table_exists_for_test(
        &self,
        table: crate::DatabaseTestTable,
    ) -> Result<bool, DbError> {
        self.call_store(move |session| session.coven_table_exists_for_test(table))
            .await
    }

    pub async fn install_store_write_failure_trigger_for_test(&self) -> Result<(), DbError> {
        self.call_store(|session| session.install_store_write_failure_trigger_for_test())
            .await
    }

    pub async fn remove_store_write_failure_trigger_for_test(&self) -> Result<(), DbError> {
        self.call_store(|session| session.remove_store_write_failure_trigger_for_test())
            .await
    }

    pub async fn write_blob_facts_for_test(&self, write_id: WriteId) -> Result<String, DbError> {
        self.call_store(move |session| session.write_blob_facts_for_test(&write_id))
            .await
    }

    pub async fn install_test_active_circle(
        &self,
        label: String,
    ) -> Result<coven_protocol::circle::CircleId, DbError> {
        self.call_store(move |session| Ok(session.install_test_active_circle(&label)))
            .await
    }

    pub async fn install_test_active_circles(
        &self,
        labels: Vec<String>,
    ) -> Result<Vec<coven_protocol::circle::CircleId>, DbError> {
        self.call_store(move |session| {
            Ok(labels
                .iter()
                .map(|label| session.install_test_active_circle(label))
                .collect())
        })
        .await
    }

    pub async fn install_test_inactive_circle(
        &self,
        label: String,
    ) -> Result<coven_protocol::circle::CircleId, DbError> {
        self.call_store(move |session| Ok(session.install_test_inactive_circle(&label)))
            .await
    }

    pub async fn install_test_active_circle_with_control(
        &self,
        label: String,
    ) -> Result<
        (
            coven_protocol::circle::CircleId,
            coven_protocol::circle::CircleControlCoord,
        ),
        DbError,
    > {
        self.call_store(move |session| Ok(session.install_test_active_circle_with_control(&label)))
            .await
    }

    pub async fn insert_write_status_for_test(
        &self,
        write_id: WriteId,
        status: coven_protocol::write::WriteStatus,
    ) -> Result<(), DbError> {
        let base = serde_json::json!({ "dependencies": {} }).to_string();
        let status = serde_json::to_string(&status)
            .map_err(|error| DbError::context("serialize write status", error))?;
        self.call_store(move |session| {
            session.insert_write_status_for_test(&write_id, &status, &base)
        })
        .await
    }

    pub async fn delete_write_for_test(&self, write_id: WriteId) -> Result<(), DbError> {
        self.call_store(move |session| session.delete_write_for_test(&write_id))
            .await
    }

    pub async fn store_write_partition_for_test(
        &self,
        write_id: &WriteId,
    ) -> Result<Vec<u8>, DbError> {
        let write_id = write_id.clone();
        self.call_store(move |session| session.store_write_partition_for_test(&write_id))
            .await
    }

    pub async fn write_blob_lease_count_for_test(
        &self,
        write_id: &WriteId,
    ) -> Result<i64, DbError> {
        let write_id = write_id.clone();
        self.call_store(move |session| session.write_blob_lease_count_for_test(&write_id))
            .await
    }

    pub async fn latest_materialized_commit_coordinate_for_test(
        &self,
    ) -> Result<(String, u64), DbError> {
        self.call_store(|session| session.latest_materialized_commit_coordinate_for_test())
            .await
    }

    pub async fn compare_circle_bootstrap_replay_with_missing_coverage_for_test(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        routing_key: coven_protocol::circle::RowRoutingKey,
        historical_id: String,
        late_id: String,
    ) -> Result<(i64, i64, i64, i64), DbError> {
        self.call_store(move |session| {
            session.compare_circle_bootstrap_replay_with_missing_coverage_for_test(
                &root,
                &routing_key,
                &historical_id,
                &late_id,
            )
        })
        .await
    }

    pub async fn circle_bootstrap_coverage_count_for_test(
        &self,
        circle_id: coven_protocol::circle::CircleId,
    ) -> Result<i64, DbError> {
        self.call_store(move |session| session.circle_bootstrap_coverage_count_for_test(circle_id))
            .await
    }

    pub async fn reject_missing_circle_bootstrap_payload_claim_for_test(
        &self,
        circle_id: coven_protocol::circle::CircleId,
    ) -> Result<String, DbError> {
        self.call_store(move |session| {
            session.reject_missing_circle_bootstrap_payload_claim_for_test(circle_id)
        })
        .await
    }

    pub async fn reject_changed_circle_bootstrap_image_hash_for_test(
        &self,
        circle_id: coven_protocol::circle::CircleId,
        root: coven_protocol::store_commit::StoreRootRef,
        activation_commit: &StoreBatchCommitRef,
    ) -> Result<String, DbError> {
        let activation_commit = activation_commit.clone();
        self.call_store(move |session| {
            session.reject_changed_circle_bootstrap_image_hash_for_test(
                circle_id,
                &root,
                &activation_commit,
            )
        })
        .await
    }

    pub async fn circle_bootstrap_failure_state_for_test(
        &self,
        blob_id: String,
        circle_id: coven_protocol::circle::CircleId,
        control: String,
        remote_object_id: String,
    ) -> Result<(bool, bool, bool, bool), DbError> {
        self.call_store(move |session| {
            session.circle_bootstrap_failure_state_for_test(
                &blob_id,
                circle_id,
                &control,
                remote_object_id,
            )
        })
        .await
    }

    pub async fn circle_bootstrap_replay_for_control_for_test(
        &self,
        circle_id: coven_protocol::circle::CircleId,
        control: coven_protocol::circle::CircleControlCoord,
    ) -> Result<Option<coven_protocol::circle_activation::VerifiedCircleImage>, DbError> {
        self.call_store(move |session| {
            session.circle_bootstrap_replay_for_control_for_test(circle_id, &control)
        })
        .await
    }

    pub async fn forge_circle_close_exclusion_for_test(
        &self,
        circle_id: coven_protocol::circle::CircleId,
    ) -> Result<(), DbError> {
        self.call_store(move |session| session.forge_circle_close_exclusion_for_test(circle_id))
            .await
    }

    pub async fn transfer_prepared_write_to_for_test(
        &self,
        destination: &Self,
        write_id: &WriteId,
    ) -> Result<(), DbError> {
        let source_write_id = write_id.clone();
        let transfer = self
            .call_store(move |session| session.export_prepared_write(&source_write_id))
            .await?;

        let destination_write_id = write_id.clone();
        destination
            .call_store(move |session| {
                session.import_prepared_write(&destination_write_id, transfer)
            })
            .await
    }
}

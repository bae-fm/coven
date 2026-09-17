use super::*;
use crate::store::store_session::StoreRecords;
use coven_protocol::store_commit::StoreBatchCommitRef;

impl StoreDatabase {
    pub async fn store_write_payload_claims_for_test(
        &self,
        write_id: &coven_protocol::write::WriteId,
    ) -> Result<Vec<ObjectHash>, DbError> {
        let owner_key = store_write_owner_key(write_id);
        self.call_store(move |session| session.payload_owner_claims(&owner_key))
            .await
    }

    pub async fn circle_operation_payload_claims_for_test(
        &self,
        operation_id: &coven_protocol::circle::CircleOperationId,
    ) -> Result<Vec<ObjectHash>, DbError> {
        let owner_key = circle_operation_owner_key(operation_id.as_str());
        self.call_store(move |session| session.payload_owner_claims(&owner_key))
            .await
    }

    pub async fn retained_replay_payload_claims_for_test(
        &self,
    ) -> Result<Vec<ObjectHash>, DbError> {
        self.call_store(|session| session.payload_owner_claims(RETAINED_REPLAY_BASELINE_OWNER_KEY))
            .await
    }

    pub async fn outbound_store_snapshot_payload_claims_for_test(
        &self,
    ) -> Result<Vec<ObjectHash>, DbError> {
        self.call_store(|session| session.payload_owner_claims(OUTBOUND_STORE_SNAPSHOT_OWNER_KEY))
            .await
    }

    pub async fn install_payload_for_test(&self, bytes: Vec<u8>) -> Result<ObjectHash, DbError> {
        self.call_store(move |session| session.install_payload_for_test(&bytes))
            .await
    }

    pub async fn payload_for_test(&self, hash: ObjectHash) -> Result<Vec<u8>, DbError> {
        self.call_store(move |session| session.payload_for_test(hash))
            .await
    }

    pub async fn has_payload_for_test(&self, hash: ObjectHash) -> Result<bool, DbError> {
        self.call_store(move |session| session.has_payload_for_test(hash))
            .await
    }

    pub async fn corrupt_payload_for_test(
        &self,
        hash: ObjectHash,
        bytes: Vec<u8>,
    ) -> Result<(), DbError> {
        self.call_store(move |session| session.corrupt_payload_for_test(hash, &bytes))
            .await
    }

    pub async fn remove_payload_bytes_for_test(&self, hash: ObjectHash) -> Result<(), DbError> {
        self.call_store(move |session| session.remove_payload_bytes_for_test(hash))
            .await
    }

    pub async fn replace_replay_baseline_device_state_for_test(
        &self,
        reference: StoreBatchCommitRef,
        state: Option<coven_protocol::store_commit::ResolvedStoreDeviceState>,
    ) -> Result<(), DbError> {
        self.call_store(move |session| {
            session.replace_replay_baseline_device_state_for_test(reference, state)
        })
        .await
    }

    pub async fn assert_replay_baseline_rejects_altered_coverage_for_test(
        &self,
    ) -> Result<(), DbError> {
        self.call_store(|session| {
            let baseline = crate::StoreDatabase::load_replay_baseline_on(StoreRecords::new(
                session.conn,
                session.store_dir,
            ))?;
            let original = baseline.image_bytes(session.conn)?;
            for (sql, expected) in [
                (
                    "UPDATE snapshot_coverage SET seq = seq + 1",
                    "snapshot replay coverage sequence differs from its exact reference",
                ),
                (
                    "DELETE FROM snapshot_coverage",
                    "snapshot replay image coverage differs from its baseline",
                ),
            ] {
                let mut image = Connection::open_in_memory()?;
                crate::connection_io::deserialize_database_image_into(&mut image, &original)?;
                assert_eq!(image.execute(sql, [])?, 1, "alter the covered stream");
                let bytes = crate::connection_io::serialize_database_image(&image)?;
                let mut altered = baseline.clone();
                altered.image_payload_hash = session.install_payload_for_test(&bytes)?;
                let error = altered
                    .validate_image(session.conn, session.store_dir)
                    .expect_err("valid authority must not admit altered image coverage");
                assert!(matches!(error, DbError::Message(message) if message == expected));
            }
            Ok(())
        })
        .await
    }

    /// A baseline image carries the rows that name payloads and never the
    /// payloads themselves: a device resolves those names in its own catalog.
    /// Plant each payload table into the stored image in turn and the baseline
    /// refuses it — without that refusal an image would carry the image it
    /// replaces, once per capture.
    pub async fn assert_replay_baseline_rejects_carried_payload_rows_for_test(
        &self,
    ) -> Result<(), DbError> {
        self.call_store(|session| {
            let baseline = crate::StoreDatabase::load_replay_baseline_on(StoreRecords::new(
                session.conn,
                session.store_dir,
            ))?;
            let original = baseline.image_bytes(session.conn)?;
            let planted = ObjectHash::digest(b"a payload no image may carry").to_string();
            let catalog = "INSERT INTO payload_storage
                 (payload_hash, payload_size, compressed_size, chunk_count)
                 VALUES (?1, 1, 1, 1)";
            for (table, plant) in [
                ("payload_storage", Vec::new()),
                (
                    "payload_chunks",
                    vec![
                        "INSERT INTO payload_chunks (payload_hash, ordinal, bytes)
                          VALUES (?1, 0, X'00')",
                    ],
                ),
                (
                    "payload_owners",
                    vec![
                        "INSERT INTO payload_owners (payload_hash, owner_key)
                          VALUES (?1, 'planted')",
                    ],
                ),
            ] {
                let mut image = Connection::open_in_memory()?;
                crate::connection_io::deserialize_database_image_into(&mut image, &original)?;
                assert_eq!(
                    image.execute(catalog, [&planted])?,
                    1,
                    "plant the catalog row {table} needs"
                );
                for statement in plant {
                    assert_eq!(image.execute(statement, [&planted])?, 1, "plant {table}");
                }
                let bytes = crate::connection_io::serialize_database_image(&image)?;
                let mut altered = baseline.clone();
                altered.image_payload_hash = session.install_payload_for_test(&bytes)?;

                let error = altered
                    .validate_image(session.conn, session.store_dir)
                    .expect_err("an image carrying payload rows is not a baseline");

                let DbError::Message(message) = &error else {
                    panic!("carried payload rows are refused by name: {error}");
                };
                assert!(
                    message.starts_with("retained replay image carries payload rows"),
                    "{message}"
                );
                assert!(message.contains(table), "{message} does not name {table}");
            }
            Ok(())
        })
        .await
    }

    /// The retention rule keeps the Store commit every inherited Circle entry
    /// names as its introduction, so a restored device can still prove where
    /// that entry entered accepted history. Drop that commit's retained
    /// materialization from the image and `validate_snapshot_retained_inputs_on`
    /// refuses the image: the inherited entry would have no introduction to
    /// resolve.
    pub async fn assert_replay_baseline_requires_an_entry_introduction_for_test(
        &self,
        introduction: StoreBatchCommitRef,
    ) -> Result<(), DbError> {
        self.call_store(move |session| {
            let baseline = crate::StoreDatabase::load_replay_baseline_on(StoreRecords::new(
                session.conn,
                session.store_dir,
            ))?;
            let original = baseline.image_bytes(session.conn)?;
            let encoded = serde_json::to_string(&introduction)?;
            let mut image = Connection::open_in_memory()?;
            crate::connection_io::deserialize_database_image_into(&mut image, &original)?;
            image.pragma_update(None, "defer_foreign_keys", "ON")?;
            assert_eq!(
                image.execute(
                    "DELETE FROM retained_merge_materializations WHERE commit_ref = ?1",
                    [&encoded],
                )?,
                1,
                "the image carries the introducing commit before the edit"
            );
            image.execute(
                "DELETE FROM retained_replay_objects WHERE commit_ref = ?1",
                [&encoded],
            )?;
            let bytes = crate::connection_io::serialize_database_image(&image)?;
            let mut altered = baseline.clone();
            altered.image_payload_hash = session.install_payload_for_test(&bytes)?;
            let error = altered
                .validate_image(session.conn, session.store_dir)
                .expect_err("an image missing an entry introduction must be refused");
            assert!(
                matches!(&error, DbError::Message(message)
                    if message == "snapshot retained inputs differ from the retention rule"),
                "{error}"
            );
            Ok(())
        })
        .await
    }

    pub async fn assert_installed_baseline_rejects_altered_coverage_for_test(
        &self,
    ) -> Result<(), DbError> {
        self.call_store(|session| {
            let original = session.installed_replay_baseline()?;
            assert_eq!(original.coverage().position_count(), 1);
            let connection = session.conn;
            let transaction = connection.unchecked_transaction()?;
            assert_eq!(transaction.execute("DELETE FROM snapshot_coverage", [])?, 1);
            let altered = session.installed_replay_baseline();
            transaction.rollback()?;
            let error = altered.expect_err("live coverage must match its installed authority");
            assert!(matches!(error, DbError::Message(message)
                if message == "installed snapshot coverage differs from its replay authority"));
            let restored = session.installed_replay_baseline()?;
            assert_eq!(restored.coverage(), original.coverage());
            assert_eq!(
                restored
                    .covered_states()
                    .collect::<std::collections::BTreeMap<_, _>>(),
                original
                    .covered_states()
                    .collect::<std::collections::BTreeMap<_, _>>(),
            );
            Ok(())
        })
        .await
    }

    pub async fn downgrade_replay_baseline_coven_schema_to_v0_for_test(
        &self,
        include_routing: bool,
    ) -> Result<ObjectHash, DbError> {
        self.call_store(move |session| {
            session.downgrade_replay_baseline_coven_schema_to_v0_for_test(include_routing)
        })
        .await
    }
}

impl StoreSession<'_> {
    fn payload_owner_claims(&self, owner_key: &str) -> Result<Vec<ObjectHash>, DbError> {
        Ok(payload_owner_claims_on(self.conn, owner_key)?
            .into_iter()
            .collect())
    }

    fn install_payload_for_test(&self, bytes: &[u8]) -> Result<ObjectHash, DbError> {
        let transaction = self.conn.unchecked_transaction().map_err(DbError::from)?;
        let hash = PayloadStore::new(&transaction)
            .install(bytes)
            .map_err(DbError::from)?;
        transaction.commit().map_err(DbError::from)?;
        Ok(hash)
    }

    fn payload_for_test(&self, hash: ObjectHash) -> Result<Vec<u8>, DbError> {
        crate::store::store_session::StoreRecords::new(self.conn, self.store_dir)
            .payload(hash)
            .map_err(DbError::from)
    }

    fn has_payload_for_test(&self, hash: ObjectHash) -> Result<bool, DbError> {
        Ok(PayloadStore::new(self.conn)
            .stored(hash)
            .map_err(DbError::from)?
            .is_some())
    }

    /// Replace one payload's stored bytes with a compression of `bytes`,
    /// leaving its address alone: a reader that trusts the address without
    /// checking the content it gets back is what this catches.
    fn corrupt_payload_for_test(&self, hash: ObjectHash, bytes: &[u8]) -> Result<(), DbError> {
        let transaction = self.conn.unchecked_transaction().map_err(DbError::from)?;
        PayloadStore::new(&transaction)
            .replace_content_for_test(hash, bytes)
            .map_err(DbError::from)?;
        transaction.commit().map_err(DbError::from)
    }

    /// Take one payload's bytes away while leaving the rows that name it, so a
    /// reader meets storage its catalog row promised and cannot produce.
    fn remove_payload_bytes_for_test(&self, hash: ObjectHash) -> Result<(), DbError> {
        let transaction = self.conn.unchecked_transaction().map_err(DbError::from)?;
        if PayloadStore::new(&transaction)
            .stored(hash)
            .map_err(DbError::from)?
            .is_none()
        {
            return Err(DbError::Message(format!(
                "cannot remove absent test payload {hash}"
            )));
        }
        transaction
            .execute(
                "DELETE FROM payload_chunks WHERE payload_hash = ?1",
                [hash.to_string()],
            )
            .map_err(DbError::from)?;
        transaction.commit().map_err(DbError::from)
    }

    fn replace_replay_baseline_device_state_for_test(
        &mut self,
        reference: StoreBatchCommitRef,
        state: Option<coven_protocol::store_commit::ResolvedStoreDeviceState>,
    ) -> Result<(), DbError> {
        let records = StoreRecords::new(self.conn, self.store_dir);
        let baseline = crate::store::retained_replay::load_replay_baseline_metadata_on(records)?
            .ok_or_else(|| DbError::Message("retained replay baseline is absent".to_string()))?;
        let mut image = rusqlite::Connection::open_in_memory().map_err(DbError::from)?;
        crate::connection_io::deserialize_database_image_into(
            &mut image,
            &baseline.image_bytes(self.conn)?,
        )
        .map_err(|error| DbError::context("open test replay image", error))?;
        let reference = serde_json::to_string(&reference)
            .map_err(|error| DbError::context("encode test device-state reference", error))?;
        let database = crate::DatabaseTestSql::new(&image);
        match state {
            Some(state) => database.replace_device_state_snapshot(&reference, &state)?,
            None => database.delete_device_state_snapshot(&reference)?,
        }
        let bytes = crate::connection_io::serialize_database_image(&image)?;
        let transaction = self.conn.unchecked_transaction().map_err(DbError::from)?;
        StoreRecords::new(&transaction, self.store_dir).replace_retained_replay_image(
            &baseline,
            baseline.schema_version,
            &bytes,
            self.blob_decls,
        )?;
        transaction.commit().map_err(DbError::from)?;
        self.verified_store_authority
            .forget_superseded_replay_baseline();
        Ok(())
    }

    fn downgrade_replay_baseline_coven_schema_to_v0_for_test(
        &self,
        include_routing: bool,
    ) -> Result<ObjectHash, DbError> {
        let records = crate::store::store_session::StoreRecords::new(self.conn, self.store_dir);
        let baseline = crate::store::retained_replay::load_replay_baseline_metadata_on(records)?
            .ok_or_else(|| DbError::Message("retained replay baseline is absent".to_string()))?;
        let authority_hash = ObjectHash::digest(&baseline.canonical_authority_bytes()?);
        let mut image = rusqlite::Connection::open_in_memory().map_err(DbError::from)?;
        crate::connection_io::deserialize_database_image_into(
            &mut image,
            &baseline.image_bytes(self.conn)?,
        )
        .map_err(|error| DbError::context("open retained replay database image", error))?;
        crate::coven_schema::downgrade_coven_schema_to_v0_for_test(&image, include_routing)?;
        let image_bytes = crate::connection_io::serialize_database_image(&image)?;

        let transaction = self.conn.unchecked_transaction().map_err(DbError::from)?;
        let records = crate::store::store_session::StoreRecords::new(&transaction, self.store_dir);
        let image_hash = records
            .install_payload(&image_bytes)
            .map_err(DbError::from)?;
        let updated = transaction
            .execute(
                "UPDATE retained_replay_baselines
                 SET image_payload_hash = ?1
                 WHERE singleton = 1 AND image_payload_hash = ?2",
                rusqlite::params![
                    image_hash.to_string(),
                    baseline.image_payload_hash.to_string()
                ],
            )
            .map_err(DbError::from)?;
        if updated != 1 {
            return Err(DbError::Message(format!(
                "replacing the retained replay image changed {updated} rows"
            )));
        }
        set_payload_owner_claims_on(
            &transaction,
            RETAINED_REPLAY_BASELINE_OWNER_KEY,
            &std::collections::BTreeSet::from([image_hash, authority_hash]),
        )?;
        transaction.commit().map_err(DbError::from)?;
        Ok(image_hash)
    }
}

impl crate::CreatedSnapshot {
    pub async fn device_state_for_test(
        &self,
        reference: &StoreBatchCommitRef,
    ) -> Result<coven_protocol::store_commit::ResolvedStoreDeviceState, DbError> {
        let bytes = self.read_image().await.map_err(DbError::from)?;
        let mut image = rusqlite::Connection::open_in_memory().map_err(DbError::from)?;
        crate::connection_io::deserialize_database_image_into(&mut image, &bytes)
            .map_err(|error| DbError::context("open captured test snapshot", error))?;
        crate::store::store_device_state::load_store_device_snapshot_on(&image, reference)
    }
}

impl PayloadStore<'_> {
    /// Store a compression of `bytes` under `hash`, whatever `bytes` hashes to.
    ///
    /// Only a test can put a payload and its address out of step; the writer
    /// refuses to, which is the property the tests reading this back check.
    fn replace_content_for_test(
        self,
        hash: ObjectHash,
        bytes: &[u8],
    ) -> Result<(), PayloadStoreError> {
        if self.stored(hash)?.is_none() {
            return Err(PayloadStoreError::Storage {
                hash,
                error: "no catalog row to replace".to_string(),
            });
        }
        self.conn
            .execute(
                "DELETE FROM payload_chunks WHERE payload_hash = ?1",
                [hash.to_string()],
            )
            .map_err(|source| PayloadStoreError::Database { hash, source })?;
        let mut encoder =
            lz4_flex::frame::FrameEncoder::new(PayloadChunkSink::new(self.conn, hash));
        encoder
            .write_all(bytes)
            .map_err(|source| PayloadStoreError::CompressionIo { hash, source })?;
        let (compressed_size, chunk_count) = encoder
            .finish()
            .map_err(|source| PayloadStoreError::CompressionFrame { hash, source })?
            .finish()
            .map_err(|source| PayloadStoreError::CompressionIo { hash, source })?;
        self.conn
            .execute(
                "UPDATE payload_storage
                 SET payload_size = ?2, compressed_size = ?3, chunk_count = ?4
                 WHERE payload_hash = ?1",
                rusqlite::params![
                    hash.to_string(),
                    bytes.len() as i64,
                    compressed_size as i64,
                    chunk_count as i64
                ],
            )
            .map(drop)
            .map_err(|source| PayloadStoreError::Database { hash, source })
    }
}

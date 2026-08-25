use super::*;

impl StoreDatabase {
    /// The payloads still owed a deletion. Empty once every obligation this
    /// store committed has been discharged.
    pub async fn owed_payload_cleanup(&self) -> Result<Vec<ObjectHash>, DbError> {
        self.call_store(|session| session.owed_payload_cleanup())
            .await
    }

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
    fn owed_payload_cleanup(&self) -> Result<Vec<ObjectHash>, DbError> {
        payload_cleanup_hashes_on(self.conn)
    }

    fn payload_owner_claims(&self, owner_key: &str) -> Result<Vec<ObjectHash>, DbError> {
        Ok(payload_owner_claims_on(self.conn, owner_key)?
            .into_iter()
            .collect())
    }

    fn install_payload_for_test(&self, bytes: &[u8]) -> Result<ObjectHash, DbError> {
        let transaction = self.conn.unchecked_transaction().map_err(DbError::from)?;
        let hash = PayloadStore::new(&transaction, self.store_dir)
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
        Ok(PayloadStore::new(self.conn, self.store_dir)
            .stored(hash)
            .map_err(DbError::from)?
            .is_some())
    }

    fn corrupt_payload_for_test(&self, hash: ObjectHash, bytes: &[u8]) -> Result<(), DbError> {
        let compressed = compress_payload(hash, bytes).map_err(DbError::from)?;
        match PayloadStore::new(self.conn, self.store_dir)
            .stored(hash)
            .map_err(DbError::from)?
        {
            Some(StoredPayload::Inline { .. }) => {
                self.conn
                    .execute(
                        "UPDATE payload_storage
                         SET payload_size = ?2, compressed_bytes = ?3, compressed_size = ?4
                         WHERE payload_hash = ?1",
                        rusqlite::params![
                            hash.to_string(),
                            bytes.len() as i64,
                            &compressed,
                            compressed.len() as i64
                        ],
                    )
                    .map_err(DbError::from)?;
            }
            Some(StoredPayload::File { .. }) => {
                std::fs::write(self.store_dir.payload_spool_path(hash), &compressed)
                    .map_err(|error| DbError::context("corrupt test payload file", error))?;
                self.conn
                    .execute(
                        "UPDATE payload_storage
                         SET payload_size = ?2, compressed_size = ?3
                         WHERE payload_hash = ?1",
                        rusqlite::params![
                            hash.to_string(),
                            bytes.len() as i64,
                            compressed.len() as i64
                        ],
                    )
                    .map_err(DbError::from)?;
            }
            None => {
                return Err(DbError::Message(format!(
                    "cannot corrupt absent test payload {hash}"
                )));
            }
        }
        Ok(())
    }

    fn remove_payload_bytes_for_test(&self, hash: ObjectHash) -> Result<(), DbError> {
        match PayloadStore::new(self.conn, self.store_dir)
            .stored(hash)
            .map_err(DbError::from)?
        {
            Some(StoredPayload::Inline { compressed, .. }) => {
                self.conn
                    .execute(
                        "UPDATE payload_storage
                         SET storage = 'file', compressed_bytes = NULL, compressed_size = ?2
                         WHERE payload_hash = ?1",
                        rusqlite::params![hash.to_string(), compressed.len() as i64],
                    )
                    .map_err(DbError::from)?;
                match std::fs::remove_file(self.store_dir.payload_spool_path(hash)) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => {
                        return Err(DbError::context("remove test payload file", error));
                    }
                }
            }
            Some(StoredPayload::File { .. }) => {
                std::fs::remove_file(self.store_dir.payload_spool_path(hash))
                    .map_err(|error| DbError::context("remove test payload file", error))?;
            }
            None => {
                return Err(DbError::Message(format!(
                    "cannot remove absent test payload {hash}"
                )));
            }
        }
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
            &baseline.image_bytes(self.conn, self.store_dir)?,
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
        pay_owed_payload_deletions_on(self.conn, self.store_dir)?;
        Ok(image_hash)
    }
}

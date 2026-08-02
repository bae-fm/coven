use super::test_support::{
    author_exclusion_activation_evidence, clear_table, table_row_count, OutboxAttempt,
    RetainedRegistrationTamper,
};
use super::{Connection, DatabaseTestTable, DatabaseTestTransaction, DbError, ExternalBlobRecords};
use rusqlite::OptionalExtension;

/// Test-only SQL access to Coven's retained database connection.
///
/// Tests may exercise or corrupt durable database state, but the connection
/// itself remains private so test modules cannot pass it into unrelated
/// helpers or retain it beyond one database-thread operation.
pub(crate) struct DatabaseTestSql<'connection> {
    connection: &'connection Connection,
}

impl DatabaseTestSql<'_> {
    pub(super) fn new(connection: &Connection) -> DatabaseTestSql<'_> {
        DatabaseTestSql { connection }
    }

    pub(crate) fn execute<P>(&self, sql: &str, params: P) -> rusqlite::Result<usize>
    where
        P: rusqlite::Params,
    {
        self.connection.execute(sql, params)
    }

    pub(crate) fn execute_batch(&self, sql: &str) -> rusqlite::Result<()> {
        self.connection.execute_batch(sql)
    }

    pub(crate) fn query_row<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<T>
    where
        P: rusqlite::Params,
        F: FnOnce(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    {
        self.connection.query_row(sql, params, map)
    }

    pub(crate) fn query<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<Vec<T>>
    where
        P: rusqlite::Params,
        F: FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    {
        let mut statement = self.connection.prepare(sql)?;
        let values = statement.query_map(params, map)?.collect();
        values
    }

    pub(crate) fn set_foreign_keys(&self, enabled: bool) -> rusqlite::Result<()> {
        self.connection.pragma_update(None, "foreign_keys", enabled)
    }

    pub(crate) fn author_exclusion_activation_evidence(
        &self,
    ) -> Result<(String, String, String, String), DbError> {
        author_exclusion_activation_evidence(self.connection)
    }

    pub(crate) fn table_row_count(&self, table: DatabaseTestTable) -> Result<i64, DbError> {
        table_row_count(self.connection, table)
    }

    pub(crate) fn scoped_store_state_counts(&self) -> Result<[i64; 4], DbError> {
        Ok([
            self.table_row_count(DatabaseTestTable::named("store_writes"))?,
            self.table_row_count(DatabaseTestTable::named("store_write_partitions"))?,
            self.table_row_count(DatabaseTestTable::named("_coven_row_routes"))?,
            self.table_row_count(DatabaseTestTable::named("_coven_audience"))?,
        ])
    }

    pub(crate) fn table_has_rows(&self, table: DatabaseTestTable) -> Result<bool, DbError> {
        self.connection
            .query_row(
                &format!("SELECT EXISTS(SELECT 1 FROM {})", table.0),
                [],
                |row| row.get(0),
            )
            .map_err(DbError::from)
    }

    pub(crate) fn clear_table(&self, table: DatabaseTestTable) -> Result<(), DbError> {
        clear_table(self.connection, table)
    }

    pub(crate) fn remote_object(
        &self,
        object: &crate::storage::ExactObjectRef,
    ) -> Result<crate::protocol::remote_object::RemoteObjectRecord, DbError> {
        let object_id = crate::protocol::remote_object::remote_object_id(object);
        let state: String = self
            .connection
            .query_row(
                "SELECT state FROM remote_objects WHERE object_id = ?1",
                [object_id.to_string()],
                |row| row.get(0),
            )
            .map_err(DbError::from)?;
        serde_json::from_str(&state).map_err(|error| DbError::Message(error.to_string()))
    }

    pub(crate) fn remote_objects(
        &self,
    ) -> Result<Vec<crate::protocol::remote_object::RemoteObjectRecord>, DbError> {
        self.query(
            "SELECT state FROM remote_objects ORDER BY object_id",
            [],
            |row| row.get::<_, String>(0),
        )?
        .into_iter()
        .map(|state| {
            serde_json::from_str(&state)
                .map_err(|error| DbError::Message(format!("parse remote object: {error}")))
        })
        .collect()
    }

    pub(crate) fn replace_remote_object(
        &self,
        object: &crate::storage::ExactObjectRef,
        remote: &crate::protocol::remote_object::RemoteObjectRecord,
    ) -> Result<(), DbError> {
        let object_id = crate::protocol::remote_object::remote_object_id(object);
        let state = serde_json::to_string(remote)
            .map_err(|error| DbError::Message(format!("serialize test remote object: {error}")))?;
        let updated = self
            .connection
            .execute(
                "UPDATE remote_objects SET state = ?2 WHERE object_id = ?1",
                rusqlite::params![object_id.to_string(), state],
            )
            .map_err(DbError::from)?;
        if updated != 1 {
            return Err(DbError::Message(
                "test remote object disappeared".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn remote_object_exists(
        &self,
        object: &crate::storage::ExactObjectRef,
    ) -> Result<bool, DbError> {
        let object_id = crate::protocol::remote_object::remote_object_id(object);
        self.connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM remote_objects WHERE object_id = ?1)",
                [object_id.to_string()],
                |row| row.get(0),
            )
            .map_err(DbError::from)
    }

    pub(crate) fn remote_object_id_exists(
        &self,
        object_id: crate::protocol::store_commit::ObjectHash,
    ) -> Result<bool, DbError> {
        self.connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM remote_objects WHERE object_id = ?1)",
                [object_id.to_string()],
                |row| row.get(0),
            )
            .map_err(DbError::from)
    }

    pub(crate) fn delete_remote_object(
        &self,
        object: &crate::storage::ExactObjectRef,
    ) -> Result<(), DbError> {
        let object_id = crate::protocol::remote_object::remote_object_id(object);
        let deleted = self
            .connection
            .execute(
                "DELETE FROM remote_objects WHERE object_id = ?1",
                [object_id.to_string()],
            )
            .map_err(DbError::from)?;
        if deleted != 1 {
            return Err(DbError::Message(
                "test remote object disappeared".to_string(),
            ));
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn install_blob_binding(
        &self,
        object_id: &str,
        remote_state: &str,
        locator_hash: &str,
        table: &str,
        row_id: &str,
        column: &str,
        row_stamp: &str,
        audience_authority: &str,
    ) -> Result<(), DbError> {
        self.transaction(|transaction| {
            transaction
                .execute(
                    "INSERT INTO remote_objects (object_id, state) VALUES (?1, ?2)",
                    rusqlite::params![object_id, remote_state],
                )
                .map_err(DbError::from)?;
            transaction
                .execute(
                    "INSERT INTO blob_locators (locator_hash, remote_object_id) VALUES (?1, ?2)",
                    rusqlite::params![locator_hash, object_id],
                )
                .map_err(DbError::from)?;
            transaction
                .execute(
                    "INSERT INTO row_blob_locators
                     (table_name, row_id, column_name, row_stamp, audience_authority,
                      remote_object_id)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    rusqlite::params![
                        table,
                        row_id,
                        column,
                        row_stamp,
                        audience_authority,
                        object_id
                    ],
                )
                .map(|_| ())
                .map_err(DbError::from)
        })
    }

    pub(crate) fn row_blob_binding_count(&self, row_id: &str) -> Result<i64, DbError> {
        self.connection
            .query_row(
                "SELECT COUNT(*) FROM row_blob_locators WHERE row_id = ?1",
                [row_id],
                |row| row.get(0),
            )
            .map_err(DbError::from)
    }

    pub(crate) fn circle_control_activation_count(
        &self,
        circle_id: crate::protocol::circle::CircleId,
    ) -> Result<i64, DbError> {
        self.connection
            .query_row(
                "SELECT COUNT(*) FROM circle_control_activations WHERE circle_id = ?1",
                [circle_id.to_string()],
                |row| row.get(0),
            )
            .map_err(DbError::from)
    }

    pub(crate) fn document_circle_route(
        &self,
        row_id: &str,
    ) -> Result<(String, String, String), DbError> {
        self.connection
            .query_row(
                "SELECT document.audience, route.routing_id, route._updated_at
                 FROM documents AS document
                 JOIN _coven_row_routes AS route
                   ON route.table_name = 'documents' AND route.row_id = document.id
                 WHERE document.id = ?1",
                [row_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(DbError::from)
    }

    pub(crate) fn corrupt_live_document_route_id(&self, row_id: &str) -> Result<(), DbError> {
        self.connection
            .execute(
                "UPDATE _coven_row_routes
                 SET routing_id =
                     '0000000000000000000000000000000000000000000000000000000000000000'
                 WHERE table_name = 'documents' AND row_id = ?1",
                [row_id],
            )
            .map(|_| ())
            .map_err(DbError::from)
    }

    pub(crate) fn replace_circle_operation_payload(
        &self,
        operation_id: &crate::protocol::circle::CircleOperationId,
        payload: &[u8],
    ) -> Result<(), DbError> {
        self.connection
            .execute(
                "UPDATE circle_operations SET payload = ?2 WHERE operation_id = ?1",
                rusqlite::params![operation_id.as_str(), payload],
            )
            .map(|_| ())
            .map_err(DbError::from)
    }

    pub(crate) fn circle_bootstrap_failure_state(
        &self,
        blob_id: &str,
        circle_id: crate::protocol::circle::CircleId,
        control_coord: &str,
        remote_object_id: String,
    ) -> Result<(bool, bool, bool, bool), DbError> {
        self.connection
            .query_row(
                "SELECT
                   EXISTS(SELECT 1 FROM documents WHERE id = ?1),
                   EXISTS(SELECT 1 FROM circle_bootstrap_coverage WHERE circle_id = ?2),
                   EXISTS(
                       SELECT 1 FROM circle_control_activations
                       WHERE circle_id = ?2 AND control_coord = ?3
                   ),
                   EXISTS(SELECT 1 FROM remote_objects WHERE object_id = ?4)",
                rusqlite::params![
                    blob_id,
                    circle_id.to_string(),
                    control_coord,
                    remote_object_id
                ],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .map_err(DbError::from)
    }

    pub(crate) fn forge_circle_close_exclusion(
        &self,
        circle_id: crate::protocol::circle::CircleId,
    ) -> Result<(), DbError> {
        self.connection
            .execute(
                "INSERT INTO circle_close_exclusions
                 (circle_id, close_id, excluded_registration, successor_control, activating_commit)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![
                    circle_id.to_string(),
                    "\"0000000000000000000000000000000000000000000000000000000000000000\"",
                    "{\"forged\":\"registration\"}",
                    "{\"forged\":\"control\"}",
                    "{\"forged\":\"commit\"}",
                ],
            )
            .map(|_| ())
            .map_err(DbError::from)
    }

    pub(crate) fn circle_state_table_counts(&self) -> Result<(i64, i64), DbError> {
        Ok((
            table_row_count(
                self.connection,
                DatabaseTestTable::named("circle_current_state"),
            )?,
            table_row_count(
                self.connection,
                DatabaseTestTable::named("circle_access_cache"),
            )?,
        ))
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn scoped_note_routing_state(
        &self,
        row_id: &str,
        generation_one_key: [u8; 32],
    ) -> Result<
        (
            Option<(Option<String>, String, String)>,
            Option<(String, String)>,
            Option<(Option<String>, String)>,
        ),
        DbError,
    > {
        let routing_id = self
            .row_routing_id(generation_one_key, "notes", row_id)?
            .to_string();
        let row = self
            .connection
            .query_row(
                "SELECT audience, body, _updated_at FROM notes WHERE id = ?1",
                [row_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(DbError::from)?;
        let route = self
            .connection
            .query_row(
                "SELECT routing_id, _updated_at FROM _coven_row_routes
                 WHERE table_name = 'notes' AND row_id = ?1",
                [row_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(DbError::from)?;
        let mirror = self
            .connection
            .query_row(
                "SELECT circle_id, _updated_at FROM _coven_audience WHERE routing_id = ?1",
                [&routing_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(DbError::from)?;
        Ok((row, route, mirror))
    }

    pub(crate) fn row_routing_id(
        &self,
        generation_one_key: [u8; 32],
        table: &str,
        row_id: &str,
    ) -> Result<crate::protocol::circle::RowRoutingId, DbError> {
        let root_hash = self.store_root_hash()?;
        let encryption = crate::encryption::EncryptionService::from_key(generation_one_key);
        let key = crate::protocol::circle::derive_row_routing_key(&encryption, root_hash)
            .map_err(|error| DbError::Message(error.to_string()))?;
        Ok(crate::protocol::circle::row_routing_id(&key, table, row_id))
    }

    pub(crate) fn retained_merge_input(
        &self,
        stream_id: &str,
        sequence: u64,
    ) -> Result<(crate::protocol::store_commit::ObjectHash, Vec<u8>), DbError> {
        let sequence = i64::try_from(sequence).map_err(|error| {
            DbError::Message(format!(
                "retained Merge sequence {sequence} is invalid: {error}"
            ))
        })?;
        let (input_hash, canonical_input): (String, Vec<u8>) = self
            .connection
            .query_row(
                "SELECT input_hash, canonical_input FROM retained_merge_materializations
                 WHERE device_id = ?1 AND seq = ?2",
                rusqlite::params![stream_id, sequence],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(DbError::from)?;
        Ok((
            input_hash.parse().map_err(|error| {
                DbError::Message(format!("parse retained package input hash: {error}"))
            })?,
            canonical_input,
        ))
    }

    pub(crate) fn tamper_retained_recovery_registration(
        &self,
        reference: &crate::protocol::store_commit::StoreBatchCommitRef,
        tamper: RetainedRegistrationTamper,
    ) -> Result<(), DbError> {
        let stream_id = reference.coord.stream_id.to_string();
        let sequence = i64::try_from(reference.coord.sequence())
            .map_err(|error| DbError::Message(format!("recovery sequence is invalid: {error}")))?;
        self.transaction(|transaction| {
            let (commit_ref, canonical_input): (String, Vec<u8>) = transaction
                .query_row(
                    "SELECT commit_ref, canonical_input
                     FROM retained_merge_materializations
                     WHERE device_id = ?1 AND seq = ?2",
                    (&stream_id, sequence),
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(DbError::from)?;
            let mut input: serde_json::Value = serde_json::from_slice(&canonical_input)
                .map_err(|error| DbError::Message(error.to_string()))?;
            let registration = input
                .get_mut("activation")
                .and_then(|value| value.get_mut("registrations"))
                .and_then(|value| value.get_mut("registrations"))
                .and_then(serde_json::Value::as_array_mut)
                .and_then(|values| values.first_mut())
                .ok_or_else(|| DbError::Message("retained recovery registration absent".into()))?;
            match tamper {
                RetainedRegistrationTamper::CanonicalRegistration => registration
                    .get_mut("canonical_registration")
                    .and_then(serde_json::Value::as_array_mut)
                    .ok_or_else(|| {
                        DbError::Message("canonical recovery registration bytes absent".into())
                    })?
                    .push(serde_json::Value::from(b' ')),
                RetainedRegistrationTamper::ActivationAuthority => {
                    let recovery = registration
                        .get_mut("authority")
                        .and_then(|value| value.get_mut("recovery"))
                        .and_then(serde_json::Value::as_object_mut)
                        .ok_or_else(|| {
                            DbError::Message("retained recovery authority absent".into())
                        })?;
                    recovery.insert(
                        "recovery_id".to_string(),
                        serde_json::Value::String("0".repeat(64)),
                    );
                }
            }
            let canonical_input =
                serde_json::to_vec(&input).map_err(|error| DbError::Message(error.to_string()))?;
            let input_hash =
                crate::protocol::store_commit::ObjectHash::digest(&canonical_input).to_string();
            transaction
                .execute(
                    "DELETE FROM materialized_commits WHERE device_id = ?1 AND seq = ?2",
                    (&stream_id, sequence),
                )
                .map_err(DbError::from)?;
            transaction
                .execute(
                    "UPDATE retained_merge_materializations
                     SET input_hash = ?3, canonical_input = ?4
                     WHERE device_id = ?1 AND seq = ?2",
                    rusqlite::params![&stream_id, sequence, &input_hash, &canonical_input],
                )
                .map_err(DbError::from)?;
            transaction
                .execute(
                    "INSERT INTO materialized_commits
                     (device_id, seq, commit_ref, retained_commit_ref, retained_input_hash)
                     VALUES (?1, ?2, ?3, ?3, ?4)",
                    rusqlite::params![&stream_id, sequence, &commit_ref, &input_hash],
                )
                .map(|_| ())
                .map_err(DbError::from)
        })
    }

    pub(crate) fn replace_retained_merge_input(
        &self,
        stream_id: &str,
        canonical_input: &[u8],
    ) -> Result<(), DbError> {
        self.transaction(|transaction| {
            transaction.defer_foreign_keys().map_err(DbError::from)?;
            let stored_hash: String = transaction
                .query_row(
                    "SELECT input_hash FROM retained_merge_materializations
                     WHERE device_id = ?1 AND seq = 1",
                    [stream_id],
                    |row| row.get(0),
                )
                .map_err(DbError::from)?;
            let old_hash = stored_hash.parse().map_err(|error| {
                DbError::Message(format!("stored retained input hash: {error}"))
            })?;
            let new_hash = crate::protocol::store_commit::ObjectHash::digest(canonical_input);
            let rows = transaction
                .query(
                    "SELECT indexed.object_id, remote.state
                     FROM retained_replay_objects AS indexed
                     JOIN remote_objects AS remote ON remote.object_id = indexed.object_id
                     WHERE indexed.device_id = ?1 AND indexed.seq = 1
                     ORDER BY indexed.object_id",
                    [stream_id],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                )
                .map_err(DbError::from)?;
            if rows.is_empty() {
                return Err(DbError::Message(
                    "retained Merge input has no indexed replay objects".to_string(),
                ));
            }
            for (object_id, state) in rows {
                let mut remote: crate::protocol::remote_object::RemoteObjectRecord =
                    serde_json::from_str(&state).map_err(|error| {
                        DbError::Message(format!(
                            "parse retained replay object {object_id}: {error}"
                        ))
                    })?;
                let crate::protocol::remote_object::RemoteObjectRecord::SharedLiveSet(record) =
                    &mut remote
                else {
                    return Err(DbError::Message(format!(
                        "retained replay object {object_id} is not shared"
                    )));
                };
                let crate::protocol::remote_object::OwnedObjectState::UploadedVerified {
                    ownership,
                } = &mut record.state
                else {
                    return Err(DbError::Message(format!(
                        "retained replay object {object_id} is not activated"
                    )));
                };
                let old_owner = ownership
                    .activated
                    .iter()
                    .find_map(|owner| match owner {
                        crate::protocol::remote_object::SharedObjectOwner::RetainedReplay(
                            crate::protocol::remote_object::RetainedReplayOwner::Commit {
                                commit,
                                input_hash,
                            },
                        ) if *input_hash == old_hash => Some((owner.clone(), commit.clone())),
                        _ => None,
                    })
                    .ok_or_else(|| {
                        DbError::Message(format!(
                            "retained replay object {object_id} lacks its indexed owner"
                        ))
                    })?;
                ownership.activated.remove(&old_owner.0);
                ownership.activated.insert(
                    crate::protocol::remote_object::SharedObjectOwner::RetainedReplay(
                        crate::protocol::remote_object::RetainedReplayOwner::Commit {
                            commit: old_owner.1,
                            input_hash: new_hash,
                        },
                    ),
                );
                transaction
                    .execute(
                        "UPDATE remote_objects SET state = ?2 WHERE object_id = ?1",
                        rusqlite::params![
                            object_id,
                            serde_json::to_string(&remote).map_err(|error| DbError::Message(
                                format!("serialize rebound retained replay object: {error}")
                            ))?
                        ],
                    )
                    .map_err(DbError::from)?;
            }
            transaction
                .execute(
                    "UPDATE retained_merge_materializations
                     SET input_hash = ?2, canonical_input = ?3
                     WHERE device_id = ?1 AND seq = 1",
                    rusqlite::params![stream_id, new_hash.to_string(), canonical_input],
                )
                .map_err(DbError::from)?;
            transaction
                .execute(
                    "UPDATE materialized_commits SET retained_input_hash = ?2
                     WHERE device_id = ?1 AND seq = 1",
                    rusqlite::params![stream_id, new_hash.to_string()],
                )
                .map_err(DbError::from)?;
            transaction
                .execute(
                    "UPDATE retained_replay_objects SET input_hash = ?2
                     WHERE device_id = ?1 AND seq = 1",
                    rusqlite::params![stream_id, new_hash.to_string()],
                )
                .map(|_| ())
                .map_err(DbError::from)
        })
    }

    pub(crate) fn corrupt_store_device_registration_bytes(
        &self,
        reference: &crate::protocol::store_commit::StoreDeviceRegistrationRef,
    ) -> Result<(), DbError> {
        self.connection
            .execute(
                "UPDATE store_device_registration_activations
                 SET registration_bytes = X'00'
                 WHERE device_id = ?1 AND registration_hash = ?2",
                (
                    reference.device_id.to_string(),
                    reference.registration_hash.to_string(),
                ),
            )
            .map(|_| ())
            .map_err(DbError::from)
    }

    pub(crate) fn insert_invalid_materialized_commit(&self) -> Result<(), DbError> {
        self.connection
            .execute(
                "INSERT INTO materialized_commits (device_id, seq, commit_ref)
                 VALUES ('invalid-device', -1, '{}')",
                [],
            )
            .map(|_| ())
            .map_err(DbError::from)
    }

    pub(crate) fn retained_materialization_input(
        &self,
        stream_id: &str,
        sequence: u64,
    ) -> Result<(Vec<u8>, String, String), DbError> {
        let sequence = i64::try_from(sequence)
            .map_err(|error| DbError::Message(format!("invalid sequence: {error}")))?;
        self.connection
            .query_row(
                "SELECT canonical_input, input_hash, commit_ref
                 FROM retained_merge_materializations
                 WHERE device_id = ?1 AND seq = ?2",
                rusqlite::params![stream_id, sequence],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(DbError::from)
    }

    pub(crate) fn corrupt_retained_materialization_input(
        &self,
        stream_id: &str,
        sequence: u64,
    ) -> Result<(), DbError> {
        let sequence = i64::try_from(sequence)
            .map_err(|error| DbError::Message(format!("invalid sequence: {error}")))?;
        self.connection
            .execute(
                "UPDATE retained_merge_materializations SET canonical_input = x'7b7d'
                 WHERE device_id = ?1 AND seq = ?2",
                rusqlite::params![stream_id, sequence],
            )
            .map(|_| ())
            .map_err(DbError::from)
    }

    pub(crate) fn insert_retained_replay_object(
        &self,
        owner: &crate::protocol::remote_object::RetainedReplayOwner,
        object: &crate::storage::ExactObjectRef,
    ) -> Result<(), DbError> {
        let crate::protocol::remote_object::RetainedReplayOwner::Commit { commit, input_hash } =
            owner;
        let sequence = i64::try_from(commit.coord.sequence())
            .map_err(|error| DbError::Message(format!("invalid sequence: {error}")))?;
        self.connection
            .execute(
                "INSERT INTO retained_replay_objects
                 (device_id, seq, commit_ref, input_hash, object_id)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![
                    commit.coord.stream_id.to_string(),
                    sequence,
                    serde_json::to_string(commit)
                        .map_err(|error| DbError::Message(error.to_string()))?,
                    input_hash.to_string(),
                    crate::protocol::remote_object::remote_object_id(object).to_string(),
                ],
            )
            .map(|_| ())
            .map_err(DbError::from)
    }

    pub(crate) fn materialized_commit_exists(
        &self,
        stream_id: &str,
        sequence: u64,
    ) -> Result<bool, DbError> {
        let sequence = i64::try_from(sequence)
            .map_err(|error| DbError::Message(format!("invalid sequence: {error}")))?;
        self.connection
            .query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM materialized_commits WHERE device_id = ?1 AND seq = ?2
                 )",
                rusqlite::params![stream_id, sequence],
                |row| row.get(0),
            )
            .map_err(DbError::from)
    }

    pub(crate) fn scoped_routing_counts(
        &self,
        circle_id: crate::protocol::circle::CircleId,
    ) -> Result<(i64, i64), DbError> {
        let routes = table_row_count(
            self.connection,
            DatabaseTestTable::named("_coven_row_routes"),
        )?;
        let mirrors = self.connection.query_row(
            "SELECT COUNT(*) FROM _coven_audience WHERE circle_id = ?1",
            [circle_id.to_string()],
            |row| row.get(0),
        )?;
        Ok((routes, mirrors))
    }

    pub(crate) fn cleanup_intent_copy_identities(&self) -> Result<Vec<String>, DbError> {
        self.query(
            "SELECT copy_identity FROM local_cleanup_intents ORDER BY copy_identity",
            [],
            |row| row.get(0),
        )
        .map_err(DbError::from)
    }

    pub(crate) fn insert_cleanup_intent(
        &self,
        namespace: &str,
        blob_id: &str,
        copy_identity: &str,
    ) -> Result<(), DbError> {
        self.connection
            .execute(
                "INSERT INTO local_cleanup_intents (namespace, blob_id, copy_identity)
                 VALUES (?1, ?2, ?3)",
                rusqlite::params![namespace, blob_id, copy_identity],
            )
            .map(|_| ())
            .map_err(DbError::from)
    }

    pub(crate) fn install_retracted_device_state_failure_trigger(&self) -> Result<(), DbError> {
        self.connection
            .execute_batch(
                "CREATE TRIGGER delete_retracted_device_state_early
                 AFTER DELETE ON materialized_commits
                 BEGIN
                   DELETE FROM store_device_state_snapshots WHERE commit_ref = OLD.commit_ref;
                 END;",
            )
            .map_err(DbError::from)
    }

    pub(crate) fn latest_local_write_facts(&self) -> Result<(String, i64, i64), DbError> {
        self.connection
            .query_row(
                "SELECT status,
                        (SELECT COUNT(*) FROM store_write_partitions p
                         WHERE p.write_id = w.write_id),
                        (SELECT COALESCE(SUM(length(changeset)), 0)
                         FROM store_write_partitions p WHERE p.write_id = w.write_id)
                 FROM store_writes w ORDER BY ordinal DESC LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(DbError::from)
    }

    pub(crate) fn prepared_write_count(&self, write_id: &crate::WriteId) -> Result<i64, DbError> {
        self.connection
            .query_row(
                "SELECT COUNT(*) FROM store_writes WHERE write_id = ?1 AND prepared IS NOT NULL",
                [write_id.as_str()],
                |row| row.get(0),
            )
            .map_err(DbError::from)
    }

    pub(crate) fn install_indexed_shared_blobs(
        &self,
        write_id: &crate::WriteId,
        records: Vec<crate::protocol::remote_object::RemoteObjectRecord>,
    ) -> Result<(), DbError> {
        self.transaction(|transaction| {
            for (index, record) in records.into_iter().enumerate() {
                let object_id = record.object_id();
                transaction
                    .execute(
                        "INSERT INTO remote_objects (object_id, state) VALUES (?1, ?2)",
                        rusqlite::params![
                            object_id.to_string(),
                            serde_json::to_string(&record)
                                .map_err(|error| DbError::Message(error.to_string()))?
                        ],
                    )
                    .map_err(DbError::from)?;
                transaction
                    .execute(
                        "INSERT INTO store_write_blobs
                         (write_id, audience, locator_hash, remote_object_id, spool_path)
                         VALUES (?1, 'store', ?2, ?3, NULL)",
                        rusqlite::params![
                            write_id.as_str(),
                            crate::protocol::store_commit::ObjectHash::digest(
                                format!("indexed shared blob {index}").as_bytes()
                            )
                            .to_string(),
                            object_id.to_string(),
                        ],
                    )
                    .map_err(DbError::from)?;
            }
            Ok(())
        })
    }

    pub(crate) fn staged_circle_acknowledgement_object(
        &self,
    ) -> Result<crate::storage::PreparedExactObject, DbError> {
        let encoded: String = self
            .connection
            .query_row(
                "SELECT prepared_object FROM outbound_circle_acks",
                [],
                |row| row.get(0),
            )
            .map_err(DbError::from)?;
        serde_json::from_str(&encoded).map_err(|error| DbError::Message(error.to_string()))
    }

    pub(crate) fn forge_device_in_state_snapshots(
        &self,
        forged_device_id: crate::protocol::store_commit::StoreDeviceId,
    ) -> Result<(), DbError> {
        let rows = self.query(
            "SELECT commit_ref, state FROM store_device_state_snapshots",
            [],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )?;
        for (commit, encoded) in rows {
            let state: crate::protocol::store_commit::ResolvedStoreDeviceState =
                serde_json::from_str(&encoded)
                    .map_err(|error| DbError::Message(error.to_string()))?;
            let mut forged_registration = state
                .devices
                .values()
                .next()
                .ok_or_else(|| {
                    DbError::Message("Store device snapshot has no registration".into())
                })?
                .registration
                .clone();
            forged_registration.device_id = forged_device_id;
            let forged = state
                .activate_registration(forged_registration, None)
                .map_err(|error| DbError::Message(error.to_string()))?;
            self.connection
                .execute(
                    "UPDATE store_device_state_snapshots SET state = ?1 WHERE commit_ref = ?2",
                    rusqlite::params![
                        serde_json::to_string(&forged)
                            .map_err(|error| DbError::Message(error.to_string()))?,
                        commit,
                    ],
                )
                .map_err(DbError::from)?;
        }
        Ok(())
    }

    pub(crate) fn remove_store_protocol_root(&self) -> Result<(), DbError> {
        clear_table(
            self.connection,
            DatabaseTestTable::named("store_protocol_root_authority"),
        )
    }

    pub(crate) fn retained_canonical_input(
        &self,
        stream_id: &str,
        sequence: u64,
    ) -> Result<Vec<u8>, DbError> {
        self.retained_materialization_input(stream_id, sequence)
            .map(|value| value.0)
    }

    pub(crate) fn write_retains_prepared(
        &self,
        write_id: &crate::WriteId,
    ) -> Result<bool, DbError> {
        self.connection
            .query_row(
                "SELECT prepared IS NOT NULL FROM store_writes WHERE write_id = ?1",
                [write_id.as_str()],
                |row| row.get(0),
            )
            .map_err(DbError::from)
    }

    pub(crate) fn install_outbound_completion_failure_trigger(&self) -> Result<(), DbError> {
        self.connection
            .execute_batch(
                "CREATE TEMP TRIGGER fail_outbound_completion
                 BEFORE UPDATE OF prepared ON store_writes
                 WHEN OLD.prepared IS NOT NULL AND NEW.prepared IS NULL
                 BEGIN SELECT RAISE(ABORT, 'injected completion failure'); END;",
            )
            .map_err(DbError::from)
    }

    pub(crate) fn replace_store_root_hash(&self, value: Option<&str>) -> Result<(), DbError> {
        match value {
            Some(value) => self.connection.execute(
                "UPDATE store_protocol_root_authority SET store_root_hash = ?1",
                [value],
            ),
            None => self
                .connection
                .execute("DELETE FROM store_protocol_root_authority", []),
        }
        .map(|_| ())
        .map_err(DbError::from)
    }

    pub(crate) fn delete_exact_materialized_commit(
        &self,
        reference: &crate::protocol::store_commit::StoreBatchCommitRef,
    ) -> Result<(), DbError> {
        let sequence = i64::try_from(reference.coord.sequence())
            .map_err(|error| DbError::Message(format!("invalid sequence: {error}")))?;
        let encoded = serde_json::to_string(reference)
            .map_err(|error| DbError::Message(error.to_string()))?;
        let removed = self
            .connection
            .execute(
                "DELETE FROM materialized_commits
                 WHERE device_id = ?1 AND seq = ?2 AND commit_ref = ?3",
                rusqlite::params![reference.coord.stream_id.to_string(), sequence, encoded],
            )
            .map_err(DbError::from)?;
        if removed != 1 {
            return Err(DbError::Message(
                "exact materialized commit was not removed".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn delete_retained_materialization_without_foreign_keys(
        &self,
        reference: &crate::protocol::store_commit::StoreBatchCommitRef,
    ) -> Result<(), DbError> {
        let sequence = i64::try_from(reference.coord.sequence())
            .map_err(|error| DbError::Message(format!("invalid sequence: {error}")))?;
        self.set_foreign_keys(false).map_err(DbError::from)?;
        let result = self
            .connection
            .execute(
                "DELETE FROM retained_merge_materializations WHERE device_id = ?1 AND seq = ?2",
                rusqlite::params![reference.coord.stream_id.to_string(), sequence],
            )
            .map(|_| ())
            .map_err(DbError::from);
        self.set_foreign_keys(true).map_err(DbError::from)?;
        result
    }

    pub(crate) fn delete_device_state_snapshot(&self, commit_ref: &str) -> Result<(), DbError> {
        let deleted = self
            .connection
            .execute(
                "DELETE FROM store_device_state_snapshots WHERE commit_ref = ?1",
                [commit_ref],
            )
            .map_err(DbError::from)?;
        if deleted != 1 {
            return Err(DbError::Message(
                "checkpoint state sabotage found no exact row".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn replace_device_state_snapshot(
        &self,
        commit_ref: &str,
        state: &crate::protocol::store_commit::ResolvedStoreDeviceState,
    ) -> Result<(), DbError> {
        let encoded =
            serde_json::to_string(state).map_err(|error| DbError::Message(error.to_string()))?;
        let updated = self
            .connection
            .execute(
                "UPDATE store_device_state_snapshots SET state = ?1 WHERE commit_ref = ?2",
                rusqlite::params![encoded, commit_ref],
            )
            .map_err(DbError::from)?;
        if updated != 1 {
            return Err(DbError::Message(
                "checkpoint state forgery found no exact row".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn register_external_blob(
        &self,
        reference: &crate::blob::RowBlobRef,
        path: &std::path::Path,
    ) -> Result<(), DbError> {
        ExternalBlobRecords::new(self.connection).register(reference, path)
    }

    pub(crate) fn clear_external_blob(
        &self,
        reference: &crate::blob::RowBlobRef,
    ) -> Result<(), DbError> {
        ExternalBlobRecords::new(self.connection).clear(reference)
    }

    pub(crate) fn make_remote_intent_exists(
        &self,
        root_table: &str,
        root_id: &str,
    ) -> Result<bool, DbError> {
        self.connection
            .query_row(
                "SELECT 1 FROM blob_make_remote_intents
                 WHERE root_table = ?1 AND root_id = ?2",
                (root_table, root_id),
                |_| Ok(()),
            )
            .optional()
            .map(|value| value.is_some())
            .map_err(DbError::from)
    }

    pub(crate) fn enqueue_blob_upload(
        &self,
        root_table: &str,
        root_id: &str,
        row: &crate::blob::RowBlobRef,
        source_path: &std::path::Path,
        retain_pinned: bool,
        created_at: &str,
    ) -> Result<(), DbError> {
        crate::database::CloudOutboxRecords::new(self.connection).enqueue_upload(
            root_table,
            root_id,
            row,
            source_path,
            retain_pinned,
            created_at,
        )
    }

    pub(crate) fn enqueue_blob_delete(
        &self,
        stored: &crate::blob::locator::StoredBlobRef,
        created_at: &str,
    ) -> Result<(), DbError> {
        crate::database::CloudOutboxRecords::new(self.connection).enqueue_delete(stored, created_at)
    }

    pub(crate) fn delete_outbox_attempt(&self, id: i64) -> Result<Option<OutboxAttempt>, DbError> {
        self.connection
            .query_row(
                "SELECT attempt_count, last_error, last_attempt_at FROM cloud_outbox
                 WHERE id = ?1 AND operation = 'delete'",
                [id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(DbError::from)
    }

    pub(crate) fn upload_outbox_attempt(
        &self,
        row_id: &str,
    ) -> Result<Option<OutboxAttempt>, DbError> {
        self.connection
            .query_row(
                "SELECT attempt_count, last_error, last_attempt_at
                 FROM cloud_outbox WHERE operation = 'upload' AND row_id = ?1",
                [row_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(DbError::from)
    }

    pub(crate) fn corrupt_delete_outbox_attempt_time(&self, id: i64) -> Result<(), DbError> {
        self.connection
            .execute(
                "UPDATE cloud_outbox SET last_attempt_at = 'not-a-timestamp', attempt_count = 1
                 WHERE id = ?1 AND operation = 'delete'",
                [id],
            )
            .map(|_| ())
            .map_err(DbError::from)
    }

    pub(crate) fn corrupt_upload_outbox_attempt_time(&self, id: i64) -> Result<(), DbError> {
        self.connection
            .execute(
                "UPDATE cloud_outbox SET last_attempt_at = 'not-a-timestamp', attempt_count = 1
                 WHERE id = ?1 AND operation = 'upload'",
                [id],
            )
            .map(|_| ())
            .map_err(DbError::from)
    }

    pub(crate) fn store_partition_changesets(&self) -> Result<Vec<Vec<u8>>, DbError> {
        self.query(
            "SELECT partition.changeset
             FROM store_write_partitions AS partition
             JOIN store_writes AS write USING (write_id)
             WHERE partition.audience = 'store'
             ORDER BY write.ordinal DESC",
            [],
            |row| row.get(0),
        )
        .map_err(DbError::from)
    }

    pub(crate) fn has_store_partition(&self) -> Result<bool, DbError> {
        self.connection
            .query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM store_write_partitions AS partition
                    JOIN store_writes AS write USING (write_id)
                    WHERE partition.audience = 'store'
                 )",
                [],
                |row| row.get(0),
            )
            .map_err(DbError::from)
    }

    pub(crate) fn insert_published_blob_drop_intent(
        &self,
        seq: u64,
        drop: &crate::sync::cycle::DeferredLocalBlobDrop,
    ) -> Result<(), DbError> {
        self.connection
            .execute(
                "INSERT INTO published_blob_drop_intents
                 (seq, namespace, blob_id, size, plaintext_hash, locator_hash, disposition)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                rusqlite::params![
                    i64::try_from(seq).map_err(|_| DbError::Message(format!(
                        "test published blob drop sequence {seq} exceeds SQLite integer range"
                    )))?,
                    &drop.namespace,
                    &drop.id,
                    i64::try_from(drop.size).map_err(|_| DbError::Message(format!(
                        "test published blob drop size {} exceeds SQLite integer range",
                        drop.size
                    )))?,
                    drop.plaintext_hash.to_string(),
                    drop.locator_hash.to_string(),
                    drop.disposition.as_db()
                ],
            )
            .map(|_| ())
            .map_err(DbError::from)
    }

    pub(crate) fn insert_blob_row(
        &self,
        row_id: &str,
        stamp: &str,
        bytes: &[u8],
    ) -> Result<(), DbError> {
        let size = i64::try_from(bytes.len())
            .map_err(|error| DbError::Message(format!("test blob size is invalid: {error}")))?;
        self.connection
            .execute(
                "INSERT INTO photos (id, size, hash, cloud_path, _updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![
                    row_id,
                    size,
                    crate::protocol::store_commit::ObjectHash::digest(bytes).to_string(),
                    format!("photos/{row_id}.bin"),
                    stamp,
                ],
            )
            .map(|_| ())
            .map_err(DbError::from)
    }

    pub(crate) fn published_blob_drop_intent_count(
        &self,
        seq: i64,
        namespace: &str,
        blob_id: &str,
    ) -> Result<i64, DbError> {
        self.connection
            .query_row(
                "SELECT COUNT(*) FROM published_blob_drop_intents
                 WHERE seq = ?1 AND namespace = ?2 AND blob_id = ?3",
                rusqlite::params![seq, namespace, blob_id],
                |row| row.get(0),
            )
            .map_err(DbError::from)
    }

    pub(crate) fn published_blob_drop_intent_exists(&self, blob_id: &str) -> Result<bool, DbError> {
        self.connection
            .query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM published_blob_drop_intents WHERE blob_id = ?1
                 )",
                [blob_id],
                |row| row.get(0),
            )
            .map_err(DbError::from)
    }

    pub(crate) fn apply_coven_routing_schema(&self) -> Result<(), DbError> {
        crate::database::apply_coven_routing_schema(self.connection).map_err(DbError::from)
    }

    pub(crate) fn circle_current_state(
        &self,
        circle_id: crate::protocol::circle::CircleId,
    ) -> Result<Option<crate::sync::store::CircleCurrentState>, DbError> {
        crate::database::StoreDatabase::circle_current_state_on(self.connection, circle_id)
    }

    pub(crate) fn circle_state_counts(
        &self,
        circle_id: crate::protocol::circle::CircleId,
    ) -> Result<(i64, i64, i64), DbError> {
        let circle_id = circle_id.to_string();
        let activated = self.connection.query_row(
            "SELECT COUNT(*) FROM circle_control_activations WHERE circle_id = ?1",
            [&circle_id],
            |row| row.get(0),
        )?;
        let active_access = self.connection.query_row(
            "SELECT COUNT(*) FROM circle_access_cache
             WHERE circle_id = ?1 AND disposition = 'active'",
            [&circle_id],
            |row| row.get(0),
        )?;
        let pending = self.connection.query_row(
            "SELECT COUNT(*) FROM circle_operations WHERE circle_id = ?1",
            [&circle_id],
            |row| row.get(0),
        )?;
        Ok((activated, active_access, pending))
    }

    pub(crate) fn circle_access_owner(
        &self,
        circle_id: crate::protocol::circle::CircleId,
    ) -> Result<String, DbError> {
        self.connection
            .query_row(
                "SELECT owner_pubkey FROM circle_access_cache WHERE circle_id = ?1",
                [circle_id.to_string()],
                |row| row.get(0),
            )
            .map_err(DbError::from)
    }

    pub(crate) fn store_device_registration_activation(
        &self,
        device_id: &str,
    ) -> Result<crate::protocol::store_commit::StoreDeviceRegistrationActivation, DbError> {
        let authority = self
            .connection
            .query_row(
                "SELECT activation_authority FROM store_device_registration_activations
                 WHERE device_id = ?1",
                [device_id],
                |row| row.get::<_, String>(0),
            )
            .map_err(DbError::from)?;
        serde_json::from_str(&authority).map_err(|error| DbError::Message(error.to_string()))
    }

    pub(crate) fn latest_published_store_snapshot(&self) -> Result<(i64, Vec<u8>), DbError> {
        self.connection
            .query_row(
                "SELECT generation, meta_bytes FROM published_store_snapshot
                 ORDER BY generation DESC LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(DbError::from)
    }

    pub(crate) fn latest_published_store_snapshot_bytes(&self) -> Result<Vec<u8>, DbError> {
        self.connection
            .query_row(
                "SELECT meta_bytes FROM published_store_snapshot
                 ORDER BY generation DESC LIMIT 1",
                [],
                |row| row.get(0),
            )
            .map_err(DbError::from)
    }

    pub(crate) fn materialized_commits_without_device_state_count(&self) -> Result<i64, DbError> {
        self.connection
            .query_row(
                "SELECT COUNT(*) FROM materialized_commits AS commits
                 LEFT JOIN store_device_state_snapshots AS states
                   ON states.commit_ref = commits.commit_ref
                 WHERE states.commit_ref IS NULL",
                [],
                |row| row.get(0),
            )
            .map_err(DbError::from)
    }

    pub(crate) fn store_device_state_snapshot_refs(
        &self,
    ) -> Result<Vec<crate::protocol::store_commit::StoreBatchCommitRef>, DbError> {
        let encoded = self.query(
            "SELECT commit_ref FROM store_device_state_snapshots",
            [],
            |row| row.get::<_, String>(0),
        )?;
        encoded
            .into_iter()
            .map(|encoded| {
                serde_json::from_str(&encoded).map_err(|error| {
                    DbError::Message(format!(
                        "parse test Store device state snapshot reference: {error}"
                    ))
                })
            })
            .collect()
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn install_circle_current_state(
        &self,
        circle_id: crate::protocol::circle::CircleId,
        control_coord: &str,
        stream_id: &str,
        commit_hash: crate::protocol::store_commit::ObjectHash,
        control_bytes: &[u8],
        owner_pubkey: Option<&str>,
        state: &[u8],
    ) -> Result<(), DbError> {
        self.connection
            .execute(
                "INSERT INTO circle_control_activations
                 (circle_id, control_coord, stream_id, seq, commit_hash, control_bytes)
                 VALUES (?1, ?2, ?3, 1, ?4, ?5)",
                rusqlite::params![
                    circle_id.to_string(),
                    control_coord,
                    stream_id,
                    commit_hash.to_string(),
                    control_bytes,
                ],
            )
            .map_err(DbError::from)?;
        if let Some(owner_pubkey) = owner_pubkey {
            self.connection
                .execute(
                    "INSERT INTO circle_access_cache
                     (circle_id, control_coord, owner_pubkey, disposition)
                     VALUES (?1, ?2, ?3, 'active')",
                    rusqlite::params![circle_id.to_string(), control_coord, owner_pubkey],
                )
                .map_err(DbError::from)?;
        }
        self.connection
            .execute(
                "INSERT INTO circle_current_state (circle_id, state) VALUES (?1, ?2)",
                rusqlite::params![circle_id.to_string(), state],
            )
            .map_err(DbError::from)?;
        Ok(())
    }

    pub(crate) fn store_root_hash(
        &self,
    ) -> Result<crate::protocol::store_commit::ObjectHash, DbError> {
        let encoded = self
            .connection
            .query_row(
                "SELECT store_root_hash FROM store_protocol_root_authority WHERE singleton = 1",
                [],
                |row| row.get::<_, String>(0),
            )
            .map_err(DbError::from)?;
        encoded
            .parse()
            .map_err(|error| DbError::Message(format!("stored Store root hash: {error}")))
    }

    pub(crate) fn required_protocol_state(&self, key: &str) -> Result<String, DbError> {
        crate::database::required_protocol_state_on(self.connection, key)
    }

    pub(crate) fn protocol_state_prefix_count(&self, prefix: &str) -> Result<i64, DbError> {
        self.connection
            .query_row(
                "SELECT COUNT(*) FROM protocol_state WHERE key LIKE (?1 || '%')",
                [prefix],
                |row| row.get(0),
            )
            .map_err(DbError::from)
    }

    pub(crate) fn exact_row_blob_locator_count(
        &self,
        table: &str,
        row_id: &str,
        column: &str,
        row_stamp: &str,
    ) -> Result<i64, DbError> {
        self.connection
            .query_row(
                "SELECT COUNT(*) FROM row_blob_locators
                 WHERE table_name = ?1 AND row_id = ?2 AND column_name = ?3 AND row_stamp = ?4",
                (table, row_id, column, row_stamp),
                |row| row.get(0),
            )
            .map_err(DbError::from)
    }

    pub(crate) fn exact_upload_outbox_count(
        &self,
        table: &str,
        row_id: &str,
        column: &str,
        row_stamp: &str,
    ) -> Result<i64, DbError> {
        self.connection
            .query_row(
                "SELECT COUNT(*) FROM cloud_outbox
                 WHERE operation = 'upload' AND table_name = ?1 AND row_id = ?2
                   AND column_name = ?3 AND row_stamp = ?4",
                (table, row_id, column, row_stamp),
                |row| row.get(0),
            )
            .map_err(DbError::from)
    }

    pub(crate) fn install_outbound_preparation_failure_trigger(&self) -> Result<(), DbError> {
        self.connection
            .execute_batch(
                "CREATE TEMP TRIGGER fail_outbound_preparation
                 BEFORE UPDATE OF prepared ON store_writes
                 WHEN OLD.prepared IS NULL AND NEW.prepared IS NOT NULL
                 BEGIN SELECT RAISE(ABORT, 'injected Store preparation failure'); END;",
            )
            .map_err(DbError::from)
    }

    pub(crate) fn install_protocol_state_insert_failure_trigger(&self) -> Result<(), DbError> {
        self.connection
            .execute_batch(
                "CREATE TRIGGER block_protocol_state_insert
                 BEFORE INSERT ON protocol_state
                 BEGIN SELECT RAISE(ABORT, 'forced set_protocol_state failure'); END;",
            )
            .map_err(DbError::from)
    }

    pub(crate) fn install_test_store_root_authority(
        &self,
        label: &str,
    ) -> Result<crate::protocol::store_commit::ObjectHash, DbError> {
        use crate::protocol::store_commit::{
            GrantStreamAnchor, ObjectHash, StoreCreationDescriptor, StoreCreationId,
            StoreProtocolRoot, STORE_PROTOCOL_VERSION,
        };
        use crate::storage::cloud::ObjectSlot;
        use crate::storage::{ExactObjectRef, S3EndpointBinding, StoreProviderBinding};

        let keypair_bytes: [u8; crate::keys::SIGN_SECRETKEYBYTES] = hex::decode(concat!(
            "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60",
            "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"
        ))
        .expect("fixed test signing key is hexadecimal")
        .try_into()
        .expect("fixed test signing key is 64 bytes");
        let signer = crate::keys::UserKeypair::from_signing_key_bytes(&keypair_bytes)
            .expect("fixed test signing key is valid");
        let sync_routing_hash: ObjectHash = self
            .required_protocol_state(crate::database::SYNC_ROUTING_HASH_STATE_KEY)?
            .parse()
            .map_err(|error| DbError::Message(format!("test Store sync-routing hash: {error}")))?;
        let root_slot = ObjectSlot::logical(
            crate::protocol::store_commit::STORE_PROTOCOL_ROOT_LOGICAL_KEY.to_string(),
        )
        .map_err(|error| DbError::Message(error.to_string()))?;
        let descriptor = StoreCreationDescriptor {
            version: STORE_PROTOCOL_VERSION,
            creation_id: StoreCreationId::from_random_bytes(
                *ObjectHash::digest(label.as_bytes()).as_bytes(),
            ),
            provider: StoreProviderBinding::S3 {
                endpoint: S3EndpointBinding::Custom {
                    origin: "https://test.invalid".to_string(),
                },
                region: "test-region".to_string(),
                bucket: format!("{label}-bucket"),
                key_prefix: None,
            },
            schema_version: 1,
            sync_routing_hash,
            founder_pubkey: crate::keys::public_key_hex(&signer),
            founder_grant: crate::protocol::causal_grants::MembershipGrantId::from_test_label(
                &format!("{label} founder grant"),
            ),
            root_slot: root_slot.clone(),
            founder_registration: ObjectSlot::logical(format!(
                "store-v1/test/{label}/registration.json"
            ))
            .map_err(|error| DbError::Message(error.to_string()))?,
            founder_provider_admin:
                crate::protocol::provider::FounderProviderAdminGrant::from_test_label(label),
            founder_membership: GrantStreamAnchor::StoreMembership {
                first_slot: ObjectSlot::logical(format!("store-v1/test/{label}/membership/1.json"))
                    .map_err(|error| DbError::Message(error.to_string()))?,
            },
            founder_recovery: GrantStreamAnchor::OwnerRecovery {
                first_slot: ObjectSlot::logical(format!("store-v1/test/{label}/recovery/1.json"))
                    .map_err(|error| DbError::Message(error.to_string()))?,
            },
        };
        let root = StoreProtocolRoot::signed(descriptor, &signer)
            .map_err(|error| DbError::Message(error.to_string()))?;
        let bytes = root.to_bytes();
        let hash = root.object_hash();
        let object = ExactObjectRef::new(root_slot, bytes.len() as u64, ObjectHash::digest(&bytes));
        self.install_store_root_authority(hash, &bytes, &object)?;
        Ok(hash)
    }

    pub(crate) fn install_store_root_authority(
        &self,
        hash: crate::protocol::store_commit::ObjectHash,
        bytes: &[u8],
        object: &crate::storage::ExactObjectRef,
    ) -> Result<(), DbError> {
        self.connection
            .execute(
                "INSERT INTO store_protocol_root_authority
                 (singleton, store_root_hash, store_protocol_root_bytes, store_root_object)
                 VALUES (1, ?1, ?2, ?3)
                 ON CONFLICT(singleton) DO NOTHING",
                rusqlite::params![
                    hash.to_string(),
                    bytes,
                    serde_json::to_string(object).map_err(|error| {
                        DbError::Message(format!("serialize test Store root object: {error}"))
                    })?,
                ],
            )
            .map(|_| ())
            .map_err(DbError::from)
    }

    pub(crate) fn circle_bootstrap_coverage(
        &self,
        circle_id: crate::protocol::circle::CircleId,
    ) -> Result<Option<crate::protocol::circle::CircleBootstrapCoverageRef>, DbError> {
        crate::database::StoreDatabase::circle_bootstrap_coverage_ref_on(self.connection, circle_id)
    }

    pub(crate) fn circle_bootstrap_replay_inputs(
        &self,
    ) -> Result<
        Vec<(
            crate::protocol::store_commit::StoreBatchCommitRef,
            crate::sync::VerifiedCircleImage,
        )>,
        DbError,
    > {
        crate::database::StoreDatabase::circle_bootstrap_replay_inputs_on(self.connection)
    }

    pub(crate) fn materialized_frontier(
        &self,
    ) -> Result<
        std::collections::BTreeMap<String, crate::protocol::store_commit::StoreBatchCommitRef>,
        DbError,
    > {
        crate::database::StoreDatabase::materialized_frontier_on(self.connection, None)
    }

    pub(crate) fn load_retained_merge_replay_inputs(
        &self,
        root: &crate::protocol::store_commit::StoreRootRef,
    ) -> Result<Vec<crate::database::OwnedVerifiedMergeMaterialization>, DbError> {
        crate::database::StoreDatabase::load_retained_merge_replay_inputs_on(self.connection, root)
    }

    pub(crate) fn record_verified_circle_activations(
        &self,
        commit: &crate::protocol::store_commit::VerifiedStoreBatchCommit,
        activations: &[crate::sync::VerifiedCircleReference],
    ) -> Result<(), DbError> {
        crate::database::record_verified_circle_activations_for_test(
            self.connection,
            commit,
            activations,
        )
    }

    pub(crate) fn apply_changeset(
        &self,
        bytes: &[u8],
        tables: &[crate::sync::session::SyncedTable],
        receiver_wall_ms: u64,
    ) -> Result<crate::database::ApplyResult, DbError> {
        crate::database::resolve_and_apply_changeset(
            self.connection,
            bytes,
            tables,
            receiver_wall_ms,
        )
    }

    pub(crate) fn apply_changesets_atomically(
        &self,
        changesets: Vec<Vec<u8>>,
        tables: &[crate::sync::session::SyncedTable],
        receiver_wall_ms: u64,
    ) -> Result<(Vec<crate::database::ApplyResult>, bool), DbError> {
        let schema = std::sync::Arc::new(crate::database::TableSchema::from_db(
            self.connection,
            tables,
        )?);
        let transaction = self
            .connection
            .unchecked_transaction()
            .map_err(DbError::from)?;
        let mut results = Vec::with_capacity(changesets.len());
        for changeset in changesets {
            let changeset = crate::database::ValidatedChangeset::new(changeset, schema.clone())
                .map_err(|error| DbError::Message(error.to_string()))?;
            results.push(crate::database::resolve_and_apply_changeset_with_schema_on(
                &transaction,
                changeset,
                receiver_wall_ms,
            )?);
        }
        let foreign_key_violations = transaction
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM pragma_foreign_key_check)",
                [],
                |row| row.get(0),
            )
            .map_err(DbError::from)?;
        transaction.commit().map_err(DbError::from)?;
        Ok((results, foreign_key_violations))
    }

    pub(crate) fn capture_changeset(
        &self,
        tables: &[String],
        statements: &[String],
    ) -> Result<Vec<u8>, DbError> {
        let mut session =
            rusqlite::session::Session::new(self.connection).map_err(DbError::from)?;
        for table in tables {
            session
                .attach(Some(table.as_str()))
                .map_err(DbError::from)?;
        }
        for statement in statements {
            self.connection
                .execute_batch(statement)
                .map_err(DbError::from)?;
        }
        let mut bytes = Vec::new();
        session.changeset_strm(&mut bytes).map_err(DbError::from)?;
        Ok(bytes)
    }

    pub(crate) fn transaction<R>(
        &self,
        operation: impl FnOnce(DatabaseTestTransaction<'_, '_>) -> Result<R, DbError>,
    ) -> Result<R, DbError> {
        let transaction = self
            .connection
            .unchecked_transaction()
            .map_err(DbError::from)?;
        let result = operation(DatabaseTestTransaction::new(&transaction))?;
        transaction.commit().map_err(DbError::from)?;
        Ok(result)
    }

    pub(crate) fn rolled_back_transaction<R>(
        &self,
        operation: impl FnOnce(DatabaseTestTransaction<'_, '_>) -> Result<R, DbError>,
    ) -> Result<R, DbError> {
        let transaction = self
            .connection
            .unchecked_transaction()
            .map_err(DbError::from)?;
        let result = operation(DatabaseTestTransaction::new(&transaction))?;
        transaction.rollback().map_err(DbError::from)?;
        Ok(result)
    }

    pub(crate) fn run_internal_store_write<R, E>(
        &self,
        synced_tables: &[crate::sync::session::SyncedTable],
        routing_encryption: Option<&crate::encryption::EncryptionService>,
        write_id: crate::WriteId,
        operation: impl FnOnce(DatabaseTestTransaction<'_, '_>) -> Result<R, E>,
    ) -> Result<R, E>
    where
        E: From<DbError>,
    {
        crate::database::StoreDatabase::run_internal_store_write_transaction_on(
            self.connection,
            synced_tables,
            routing_encryption,
            write_id,
            |transaction| operation(DatabaseTestTransaction::new(transaction)),
        )
    }

    pub(crate) fn run_prepared_blob_transition_write<R, E>(
        &self,
        synced_tables: &[crate::sync::session::SyncedTable],
        routing_encryption: Option<&crate::encryption::EncryptionService>,
        write_id: crate::WriteId,
        operation: impl FnOnce(DatabaseTestTransaction<'_, '_>) -> Result<R, E>,
    ) -> Result<R, E>
    where
        E: From<DbError>,
    {
        crate::database::StoreDatabase::run_prepared_blob_transition_transaction_on(
            self.connection,
            synced_tables,
            routing_encryption,
            write_id,
            |transaction| operation(DatabaseTestTransaction::new(transaction)),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn run_host_store_write<R, E>(
        &self,
        synced_tables: &[crate::sync::session::SyncedTable],
        gates: &crate::database::Gates,
        blob_decls: &crate::database::BlobDecls,
        routing_encryption: Option<&crate::encryption::EncryptionService>,
        blob_staging: Option<&crate::sync::HostWriteBlobStaging>,
        write_id: crate::WriteId,
        operation: impl FnOnce(DatabaseTestTransaction<'_, '_>) -> Result<R, E>,
    ) -> Result<crate::WriteReceipt<R>, E>
    where
        E: From<DbError>,
    {
        crate::database::StoreDatabase::run_store_write_transaction_on(
            self.connection,
            synced_tables,
            gates,
            blob_decls,
            routing_encryption,
            blob_staging,
            write_id,
            |transaction| operation(DatabaseTestTransaction::new(transaction)),
        )
    }
}

use super::table_row_count;
use crate::{Connection, DatabaseTestTable, DbError};

pub struct DatabaseImageTest {
    connection: Connection,
}

impl DatabaseImageTest {
    pub fn open(path: &std::path::Path) -> Result<Self, DbError> {
        Ok(Self {
            connection: Connection::open(path).map_err(DbError::from)?,
        })
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, DbError> {
        let mut connection = Connection::open_in_memory().map_err(DbError::from)?;
        crate::connection_io::deserialize_database_image_into(&mut connection, bytes)?;
        Ok(Self { connection })
    }

    pub fn execute<P>(&self, sql: &str, params: P) -> rusqlite::Result<usize>
    where
        P: rusqlite::Params,
    {
        self.connection.execute(sql, params)
    }

    pub fn execute_batch(&self, sql: &str) -> rusqlite::Result<()> {
        self.connection.execute_batch(sql)
    }

    pub fn query_row<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<T>
    where
        P: rusqlite::Params,
        F: FnOnce(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    {
        self.connection.query_row(sql, params, map)
    }

    pub fn query<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<Vec<T>>
    where
        P: rusqlite::Params,
        F: FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    {
        let mut statement = self.connection.prepare(sql)?;
        let values = statement.query_map(params, map)?.collect();
        values
    }

    pub fn apply_coven_schema(&self) -> Result<(), DbError> {
        crate::apply_coven_schema(&self.connection).map_err(DbError::from)
    }

    pub fn downgrade_coven_schema_to_v0(&self, include_routing: bool) -> Result<(), DbError> {
        crate::coven_schema::downgrade_coven_schema_to_v0_for_test(
            &self.connection,
            include_routing,
        )
    }

    pub fn validate_uninitialized_coven_schema_v0(
        &self,
        include_routing: bool,
    ) -> Result<(), crate::CovenMigrationError> {
        crate::coven_migration::validate_uninitialized_coven_schema_v0_for_test(
            &self.connection,
            include_routing,
        )
    }

    pub fn validate_current_initialized_coven_schema(
        &self,
        include_routing: bool,
    ) -> Result<(), crate::OpenError> {
        crate::database_open::load_coven_metadata(&self.connection)?;
        crate::validate_coven_schema_for_reader(&self.connection, include_routing)?;
        Ok(())
    }

    /// The payload rows this image carries, by table and count.
    ///
    /// A serialized image that travels carries the rows that name payloads and
    /// never the payloads themselves, so the only shape this may report is an
    /// empty one.
    pub fn carried_payload_rows(&self) -> Result<Vec<(&'static str, i64)>, DbError> {
        crate::payload_store::payload_rows_in_image(&self.connection)
    }

    /// The content hash of the retained replay baseline image this database
    /// stands on, if it has one.
    pub fn replay_baseline_image_hash(&self) -> Result<Option<String>, DbError> {
        use rusqlite::OptionalExtension as _;
        self.connection
            .query_row(
                "SELECT image_payload_hash FROM retained_replay_baselines WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .optional()
            .map_err(DbError::from)
    }

    /// How many payloads this database holds, and how many bytes their chunks
    /// take up.
    pub fn payload_totals(&self) -> Result<(i64, i64), DbError> {
        self.connection
            .query_row(
                "SELECT (SELECT COUNT(*) FROM payload_storage),
                        (SELECT COALESCE(SUM(length(bytes)), 0) FROM payload_chunks)",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(DbError::from)
    }

    pub fn payload(&self, encoded_hash: String) -> Result<Vec<u8>, DbError> {
        let hash = encoded_hash
            .parse()
            .map_err(|error| DbError::context("parse image payload hash", error))?;
        crate::payload_store::read_payload_blocking(&self.connection, hash).map_err(DbError::from)
    }

    pub fn scoped_routing_id(&self, table: &str, row_id: &str) -> String {
        crate::DatabaseTestSql::new(&self.connection)
            .row_routing_id([7; 32], table, row_id)
            .expect("derive test row-routing id")
            .to_string()
    }

    pub fn seed_active_circle(&self, label: &str) -> (String, String) {
        let database = crate::DatabaseTestSql::new(&self.connection);
        database
            .install_test_store_root_authority("scoped-routing-root")
            .expect("install scoped-routing Store root authority");
        let (circle_id, control) = database.install_test_active_circle(label);
        (
            circle_id.to_string(),
            serde_json::to_string(&control).expect("serialize active Circle control"),
        )
    }

    pub fn seed_inactive_circle(&self, label: &str) -> String {
        let database = crate::DatabaseTestSql::new(&self.connection);
        database
            .install_test_store_root_authority("scoped-routing-root")
            .expect("install scoped-routing Store root authority");
        database.install_test_inactive_circle(label).0.to_string()
    }

    pub fn coven_table_row_count(&self, table: DatabaseTestTable) -> Result<i64, DbError> {
        table_row_count(&self.connection, table)
    }

    pub fn install_audience_mirror(
        &self,
        routing_id: &str,
        circle_id: Option<&str>,
        row_stamp: &str,
    ) -> Result<(), DbError> {
        self.connection
            .execute(
                "INSERT INTO _coven_audience (routing_id, circle_id, _updated_at)
                 VALUES (?1, ?2, ?3)",
                rusqlite::params![routing_id, circle_id, row_stamp],
            )
            .map(|_| ())
            .map_err(DbError::from)
    }

    /// Rewrite one audience mirror to a routing id no row derives, so the mirror
    /// names no row and its row has no mirror. The caller supplies the routing
    /// id because deriving it needs the Store root the image does not carry.
    pub fn corrupt_mirror_id(&self, routing_id: &str) -> Result<(), DbError> {
        let updated = self
            .connection
            .execute(
                "UPDATE _coven_audience
                 SET routing_id =
                     '0000000000000000000000000000000000000000000000000000000000000000'
                 WHERE routing_id = ?1",
                [routing_id],
            )
            .map_err(DbError::from)?;
        if updated != 1 {
            return Err(DbError::Message(format!(
                "image holds no audience mirror for {routing_id}"
            )));
        }
        Ok(())
    }

    pub fn replace_first_circle_audience(&self, circle_id: Option<&str>) -> Result<(), DbError> {
        self.connection
            .execute(
                "UPDATE _coven_audience SET circle_id = ?1
                 WHERE routing_id = (
                     SELECT routing_id FROM _coven_audience
                     WHERE circle_id IS NOT NULL ORDER BY routing_id LIMIT 1
                 )",
                [circle_id],
            )
            .map(|_| ())
            .map_err(DbError::from)
    }

    /// Bind an image's exact commit to canonical device state, including when
    /// constructing a signed image whose authority the receiver must reject.
    pub fn replace_store_device_snapshot(
        &self,
        reference: &coven_protocol::store_commit::StoreBatchCommitRef,
        state: &coven_protocol::store_commit::ResolvedStoreDeviceState,
    ) -> Result<(), DbError> {
        let transaction = self.connection.unchecked_transaction()?;
        transaction.execute(
            "DELETE FROM store_device_state_snapshots WHERE commit_ref = ?1",
            [serde_json::to_string(reference).map_err(DbError::from)?],
        )?;
        crate::store::record_store_device_snapshot_on(&transaction, reference, state)?;
        transaction.commit().map_err(DbError::from)
    }

    pub fn store_device_state_snapshot_refs(&self) -> Result<Vec<String>, DbError> {
        self.query(
            "SELECT commit_ref FROM store_device_state_snapshots ORDER BY commit_ref",
            [],
            |row| row.get(0),
        )
        .map_err(DbError::from)
    }

    pub fn materialization_graph_counts(&self) -> Result<(i64, i64, i64), DbError> {
        Ok((
            table_row_count(
                &self.connection,
                DatabaseTestTable::named("materialized_commits"),
            )?,
            table_row_count(
                &self.connection,
                DatabaseTestTable::named("retained_merge_materializations"),
            )?,
            table_row_count(
                &self.connection,
                DatabaseTestTable::named("retained_replay_objects"),
            )?,
        ))
    }

    pub fn retained_materialization_bytes(&self) -> Result<Vec<Vec<u8>>, DbError> {
        self.query(
            "SELECT canonical_input FROM retained_merge_materializations",
            [],
            |row| row.get(0),
        )
        .map_err(DbError::from)
    }

    pub fn circle_states_containing(&self, text: &str) -> Result<i64, DbError> {
        self.connection
            .query_row(
                "SELECT COUNT(*) FROM circle_current_state
                 WHERE instr(CAST(state AS TEXT), ?1) > 0",
                [text],
                |row| row.get(0),
            )
            .map_err(DbError::from)
    }

    pub fn remote_object(
        &self,
        object: &coven_protocol::objects::ExactObjectRef,
    ) -> Result<coven_protocol::remote_object::RemoteObjectRecord, DbError> {
        crate::load_remote_object_on(
            &self.connection,
            coven_protocol::remote_object::remote_object_id(object),
        )
    }

    pub fn row_blob_remote_object(
        &self,
        table: &str,
        row_id: &str,
        column: &str,
        row_stamp: &str,
    ) -> Result<coven_protocol::remote_object::RemoteObjectRecord, DbError> {
        let (object_id, encoded): (String, String) = self.connection.query_row(
            "SELECT remote.object_id, remote.state FROM row_blob_locators AS binding
             JOIN remote_objects AS remote ON remote.object_id = binding.remote_object_id
             WHERE binding.table_name = ?1 AND binding.row_id = ?2
               AND binding.column_name = ?3 AND binding.row_stamp = ?4",
            [table, row_id, column, row_stamp],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        let remote: coven_protocol::remote_object::RemoteObjectRecord =
            serde_json::from_str(&encoded)
                .map_err(|error| DbError::context("decode image row blob ownership", error))?;
        if remote.object_id().to_string() != object_id {
            return Err(DbError::Message(
                "image row blob binding differs from its remote object identity".into(),
            ));
        }
        Ok(remote)
    }

    pub fn snapshot_blob_graph(
        &self,
    ) -> Result<
        (
            String,
            String,
            String,
            String,
            String,
            coven_protocol::remote_object::RemoteObjectRecord,
        ),
        DbError,
    > {
        let (table, row_id, column, row_stamp, locator_hash, remote_state) = self
            .connection
            .query_row(
                "SELECT binding.table_name, binding.row_id, binding.column_name,
                        binding.row_stamp, locator.locator_hash, remote.state
                 FROM row_blob_locators AS binding
                 JOIN blob_locators AS locator
                   ON locator.remote_object_id = binding.remote_object_id
                 JOIN remote_objects AS remote
                   ON remote.object_id = locator.remote_object_id",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get::<_, String>(5)?,
                    ))
                },
            )
            .map_err(DbError::from)?;
        let remote = serde_json::from_str(&remote_state)
            .map_err(|error| DbError::context("parse snapshot remote blob", error))?;
        Ok((table, row_id, column, row_stamp, locator_hash, remote))
    }

    pub fn install_snapshot_blob_binding(
        &self,
        binding: &coven_protocol::audience_package::RowBlobLocatorBinding,
        remote: &coven_protocol::remote_object::RemoteObjectRecord,
    ) -> Result<(), DbError> {
        let object_id = remote.object_id().to_string();
        self.connection
            .execute(
                "INSERT INTO remote_objects (object_id, state) VALUES (?1, ?2)",
                rusqlite::params![
                    object_id,
                    serde_json::to_string(remote).map_err(DbError::from)?
                ],
            )
            .map_err(DbError::from)?;
        self.connection
            .execute(
                "INSERT INTO blob_locators (remote_object_id, locator_hash) VALUES (?1, ?2)",
                rusqlite::params![
                    object_id,
                    binding.blob().locator().locator_hash().to_string()
                ],
            )
            .map_err(DbError::from)?;
        self.connection
            .execute(
                "INSERT INTO row_blob_locators
                 (table_name, row_id, column_name, row_stamp, audience_authority, remote_object_id)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                rusqlite::params![
                    binding.table(),
                    binding.row_id(),
                    binding.column(),
                    binding.row_stamp(),
                    serde_json::to_string(
                        &coven_protocol::audience_package::PackageAudience::Store
                    )
                    .map_err(DbError::from)?,
                    object_id,
                ],
            )
            .map(|_| ())
            .map_err(DbError::from)
    }

    /// Move one retained materialization inside a captured snapshot image onto
    /// another commit: its canonical input carries `commit` instead, and every
    /// row that named the reference it replaces names `commit_ref`.
    ///
    /// Device-state mappings are deliberately left where they are. An image may
    /// only carry states its signed causal cut names, so rewriting them would
    /// make the image fail that check instead of the one a hostile carried
    /// history is supposed to fail.
    pub fn replace_retained_materialization_commit(
        &self,
        stream_id: &str,
        sequence: u64,
        commit_ref: &str,
        commit_hash: &str,
        commit: coven_protocol::objects::PreparedExactObject,
    ) -> Result<(), DbError> {
        let sequence_sql = i64::try_from(sequence)
            .map_err(|error| DbError::context("retained materialization sequence", error))?;
        let (replaced, stored): (String, Vec<u8>) = self
            .connection
            .query_row(
                "SELECT commit_ref, canonical_input FROM retained_merge_materializations
                 WHERE device_id = ?1 AND seq = ?2",
                rusqlite::params![stream_id, sequence_sql],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(DbError::from)?;
        let mut input: crate::store::materialization_models::RetainedMergeMaterializationInput =
            serde_json::from_slice(&stored)
                .map_err(|error| DbError::context("retained materialization input", error))?;
        input.commit = commit;
        // Everything a retained input holds names the commit it materializes.
        // Its Circle activations carry that reference inside their own opaque
        // canonical bytes, so they are moved before the input is serialized;
        // every other carrier is a field of the input and moves with it.
        input.activation.circle_activations = rename_reference(
            &input.activation.circle_activations,
            replaced.as_bytes(),
            commit_ref.as_bytes(),
        );
        let canonical_input = serde_json::to_vec(&input)
            .map_err(|error| DbError::context("serialize retained materialization input", error))?;
        let canonical_input =
            rename_reference(&canonical_input, replaced.as_bytes(), commit_ref.as_bytes());
        self.rewrite_retained_materialization(
            stream_id,
            sequence_sql,
            &replaced,
            commit_ref,
            commit_hash,
            &canonical_input,
        )
    }

    fn rewrite_retained_materialization(
        &self,
        stream_id: &str,
        sequence: i64,
        replaced: &str,
        commit_ref: &str,
        commit_hash: &str,
        canonical_input: &[u8],
    ) -> Result<(), DbError> {
        let input_hash =
            coven_protocol::store_commit::ObjectHash::digest(canonical_input).to_string();
        let transaction = self.connection.unchecked_transaction()?;
        transaction
            .pragma_update(None, "defer_foreign_keys", "ON")
            .map_err(DbError::from)?;
        transaction
            .execute(
                "UPDATE retained_merge_materializations
                 SET commit_ref = ?3, input_hash = ?4, canonical_input = ?5
                 WHERE device_id = ?1 AND seq = ?2",
                rusqlite::params![
                    stream_id,
                    sequence,
                    commit_ref,
                    &input_hash,
                    canonical_input
                ],
            )
            .map_err(DbError::from)?;
        transaction
            .execute(
                "UPDATE retained_replay_objects SET commit_ref = ?3, input_hash = ?4
                 WHERE device_id = ?1 AND seq = ?2",
                rusqlite::params![stream_id, sequence, commit_ref, &input_hash],
            )
            .map_err(DbError::from)?;
        transaction
            .execute(
                "UPDATE circle_control_activations SET commit_hash = ?3
                 WHERE stream_id = ?1 AND seq = ?2",
                rusqlite::params![stream_id, sequence, commit_hash],
            )
            .map_err(DbError::from)?;
        transaction
            .execute(
                "UPDATE circle_bootstrap_coverage SET activation_commit = ?2
                 WHERE activation_commit = ?1",
                rusqlite::params![replaced, commit_ref],
            )
            .map_err(DbError::from)?;
        transaction
            .execute(
                "UPDATE store_author_exclusion_activations SET activation_commit = ?2
                 WHERE activation_commit = ?1",
                rusqlite::params![replaced, commit_ref],
            )
            .map_err(DbError::from)?;
        transaction.commit().map_err(DbError::from)
    }

    pub fn create_interrupted_coven_schema(&self) -> Result<(), DbError> {
        self.connection
            .execute_batch(
                "CREATE TABLE protocol_state (
                     key TEXT PRIMARY KEY,
                     value TEXT NOT NULL
                 ) STRICT;",
            )
            .map_err(DbError::from)
    }

    pub fn into_bytes(self) -> Result<Vec<u8>, DbError> {
        self.connection
            .serialize(rusqlite::MAIN_DB)
            .map(|bytes| bytes.to_vec())
            .map_err(DbError::from)
    }
}

/// Replace every occurrence of one canonical reference's serialized bytes with
/// another's. Both come from the same serializer, so the result stays canonical.
fn rename_reference(bytes: &[u8], replaced: &[u8], replacement: &[u8]) -> Vec<u8> {
    let mut renamed = Vec::with_capacity(bytes.len());
    let mut rest = bytes;
    while let Some(at) = rest
        .windows(replaced.len())
        .position(|window| window == replaced)
    {
        renamed.extend_from_slice(&rest[..at]);
        renamed.extend_from_slice(replacement);
        rest = &rest[at + replaced.len()..];
    }
    renamed.extend_from_slice(rest);
    renamed
}

use super::{author_exclusion_activation_evidence, table_row_count};
use crate::database::{Connection, DatabaseTestTable, DbError};

pub(crate) struct DatabaseImageTest {
    connection: Connection,
}

impl DatabaseImageTest {
    pub(crate) fn open(path: &std::path::Path) -> Result<Self, DbError> {
        Ok(Self {
            connection: Connection::open(path).map_err(DbError::from)?,
        })
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self, DbError> {
        Ok(Self {
            connection: crate::database::open_database_image(bytes)?,
        })
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

    pub(crate) fn apply_coven_schema(&self) -> Result<(), DbError> {
        crate::database::apply_coven_schema(&self.connection).map_err(DbError::from)
    }

    pub(crate) fn scoped_routing_id(&self, table: &str, row_id: &str) -> String {
        crate::database::DatabaseTestSql::new(&self.connection)
            .row_routing_id([7; 32], table, row_id)
            .expect("derive test row-routing id")
            .to_string()
    }

    pub(crate) fn seed_active_circle(&self, label: &str) -> (String, String) {
        let database = crate::database::DatabaseTestSql::new(&self.connection);
        database
            .install_test_store_root_authority("scoped-routing-root")
            .expect("install scoped-routing Store root authority");
        let (circle_id, control) = database.install_test_active_circle(label);
        (
            circle_id.to_string(),
            serde_json::to_string(&control).expect("serialize active Circle control"),
        )
    }

    pub(crate) fn seed_inactive_circle(&self, label: &str) -> String {
        let database = crate::database::DatabaseTestSql::new(&self.connection);
        database
            .install_test_store_root_authority("scoped-routing-root")
            .expect("install scoped-routing Store root authority");
        database.install_test_inactive_circle(label).0.to_string()
    }

    pub(crate) fn coven_table_row_count(&self, table: DatabaseTestTable) -> Result<i64, DbError> {
        table_row_count(&self.connection, table)
    }

    pub(crate) fn install_row_route(
        &self,
        routing_id: &str,
        table: &str,
        row_id: &str,
        row_stamp: &str,
    ) -> Result<(), DbError> {
        self.connection
            .execute(
                "INSERT INTO _coven_row_routes
                 (routing_id, table_name, row_id, _updated_at) VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![routing_id, table, row_id, row_stamp],
            )
            .map(|_| ())
            .map_err(DbError::from)
    }

    pub(crate) fn install_audience_mirror(
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

    pub(crate) fn corrupt_document_route_id(&self) -> Result<(), DbError> {
        self.connection
            .execute(
                "UPDATE _coven_row_routes
                 SET routing_id =
                     '0000000000000000000000000000000000000000000000000000000000000000'
                 WHERE table_name = 'documents'",
                [],
            )
            .map(|_| ())
            .map_err(DbError::from)
    }

    pub(crate) fn replace_first_circle_audience(
        &self,
        circle_id: Option<&str>,
    ) -> Result<(), DbError> {
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

    pub(crate) fn store_device_state_snapshot_refs(&self) -> Result<Vec<String>, DbError> {
        self.query(
            "SELECT commit_ref FROM store_device_state_snapshots ORDER BY commit_ref",
            [],
            |row| row.get(0),
        )
        .map_err(DbError::from)
    }

    pub(crate) fn materialization_graph_counts(&self) -> Result<(i64, i64, i64), DbError> {
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

    pub(crate) fn author_exclusion_activation_evidence(
        &self,
    ) -> Result<(String, String, String, String), DbError> {
        author_exclusion_activation_evidence(&self.connection)
    }

    pub(crate) fn snapshot_blob_graph(
        &self,
    ) -> Result<
        (
            String,
            String,
            String,
            String,
            String,
            crate::protocol::remote_object::RemoteObjectRecord,
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

    pub(crate) fn install_snapshot_blob_binding(
        &self,
        binding: &crate::protocol::audience_package::RowBlobLocatorBinding,
        remote: &crate::protocol::remote_object::RemoteObjectRecord,
    ) -> Result<(), DbError> {
        let object_id = remote.object_id().to_string();
        self.connection
            .execute(
                "INSERT INTO remote_objects (object_id, state) VALUES (?1, ?2)",
                rusqlite::params![
                    object_id,
                    serde_json::to_string(remote)
                        .map_err(|error| DbError::Message(error.to_string()))?
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
                        &crate::protocol::audience_package::PackageAudience::Store
                    )
                    .map_err(|error| DbError::Message(error.to_string()))?,
                    object_id,
                ],
            )
            .map(|_| ())
            .map_err(DbError::from)
    }

    pub(crate) fn create_interrupted_coven_schema(&self) -> Result<(), DbError> {
        self.connection
            .execute_batch(
                "CREATE TABLE protocol_state (
                     key TEXT PRIMARY KEY,
                     value TEXT NOT NULL
                 ) STRICT;",
            )
            .map_err(DbError::from)
    }

    pub(crate) fn into_bytes(self) -> Result<Vec<u8>, DbError> {
        self.connection
            .serialize(rusqlite::MAIN_DB)
            .map(|bytes| bytes.to_vec())
            .map_err(DbError::from)
    }
}

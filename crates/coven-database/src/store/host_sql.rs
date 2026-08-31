use crate::Gates;
use crate::{CloudOutboxRecords, Database, DbError, ExternalBlobRecords, PreparedExternalBlob};
use coven_protocol::blob::Provenance;

use coven_protocol::hlc::UpdatedAtStamper;
use coven_protocol::synced_schema::SyncedTable;

/// Host SQL against Coven's retained database connection.
///
/// The connection remains private. This context exposes query operations while
/// preventing callers from changing connection configuration or starting an
/// independent transaction.
///
/// ```compile_fail
/// fn cannot_write(sql: coven::SqlReadContext<'_>) {
///     sql.execute("DELETE FROM notes", []);
/// }
/// ```
pub struct SqlReadContext<'connection> {
    connection: &'connection rusqlite::Connection,
    dependencies: Option<crate::live_query::ReadDependencyCapture>,
}

impl<'connection> SqlReadContext<'connection> {
    pub(crate) fn new(connection: &'connection rusqlite::Connection) -> Self {
        Self {
            connection,
            dependencies: None,
        }
    }

    pub(crate) fn tracking(
        connection: &'connection rusqlite::Connection,
        dependencies: crate::live_query::ReadDependencyCapture,
    ) -> Self {
        Self {
            connection,
            dependencies: Some(dependencies),
        }
    }

    pub fn query_row<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<T>
    where
        P: rusqlite::Params,
        F: FnOnce(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    {
        let mut statement = self.prepare_tracked(sql)?;
        if let Err(error) = params.__bind_in(&mut statement) {
            self.finish_statement(None);
            return Err(error);
        }
        self.finish_statement(statement.expanded_sql());
        let mut rows = statement.raw_query();
        match rows.next()? {
            Some(row) => map(row),
            None => Err(rusqlite::Error::QueryReturnedNoRows),
        }
    }

    pub fn query<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<Vec<T>>
    where
        P: rusqlite::Params,
        F: FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    {
        let mut statement = self.prepare_tracked(sql)?;
        if let Err(error) = params.__bind_in(&mut statement) {
            self.finish_statement(None);
            return Err(error);
        }
        self.finish_statement(statement.expanded_sql());
        let mut rows = statement.raw_query();
        let mut map = map;
        let mut values = Vec::new();
        while let Some(row) = rows.next()? {
            values.push(map(row)?);
        }
        Ok(values)
    }

    fn prepare_tracked(&self, sql: &str) -> rusqlite::Result<rusqlite::Statement<'connection>> {
        if let Some(dependencies) = &self.dependencies {
            dependencies.begin_statement();
        }
        match self.connection.prepare(sql) {
            Ok(statement) => Ok(statement),
            Err(error) => {
                self.finish_statement(None);
                Err(error)
            }
        }
    }

    fn finish_statement(&self, expanded_sql: Option<String>) {
        if let Some(dependencies) = &self.dependencies {
            dependencies.finish_statement(expanded_sql);
        }
    }
}

/// Host SQL inside one journaled write transaction.
///
/// The underlying transaction remains private so host SQL cannot remove
/// Coven's authorizer or address Coven-owned attached schemas.
pub struct SqlContext<'context, 'connection> {
    transaction: &'context rusqlite::Transaction<'connection>,
    stamper: UpdatedAtStamper,
    tables: &'context [SyncedTable],
    gates: &'context Gates,
}

impl<'context, 'connection> SqlContext<'context, 'connection> {
    pub(crate) fn new(
        transaction: &'context rusqlite::Transaction<'connection>,
        stamper: UpdatedAtStamper,
        tables: &'context [SyncedTable],
        gates: &'context Gates,
    ) -> Self {
        Self {
            transaction,
            stamper,
            tables,
            gates,
        }
    }

    fn blob_table(&self, table: &str) -> Result<&SyncedTable, DbError> {
        let declared = self
            .tables
            .iter()
            .find(|candidate| candidate.name() == table)
            .ok_or_else(|| DbError::Message(format!("undeclared synced table {table:?}")))?;
        if declared.blob().is_none() {
            return Err(DbError::Message(format!(
                "synced table {table:?} has no blob declaration"
            )));
        }
        Ok(declared)
    }

    pub fn execute<P>(&self, sql: &str, params: P) -> rusqlite::Result<usize>
    where
        P: rusqlite::Params,
    {
        self.transaction.execute(sql, params)
    }

    pub fn execute_batch(&self, sql: &str) -> rusqlite::Result<()> {
        self.transaction.execute_batch(sql)
    }

    pub fn query_row<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<T>
    where
        P: rusqlite::Params,
        F: FnOnce(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    {
        self.transaction.query_row(sql, params, map)
    }

    pub fn query<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<Vec<T>>
    where
        P: rusqlite::Params,
        F: FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    {
        let mut statement = self.transaction.prepare(sql)?;
        let values = statement.query_map(params, map)?.collect();
        values
    }

    pub fn stamp(&self) -> String {
        self.stamper.stamp()
    }

    pub fn register_external_blob(
        &self,
        table: &str,
        row_id: &str,
        prepared: PreparedExternalBlob,
    ) -> Result<(), DbError> {
        crate::observe_host_sql_write();
        crate::with_coven_sql_authority(|| {
            let declared = self.blob_table(table)?;
            let blob = declared.blob().expect("blob_table requires a declaration");
            if blob.provenance != Provenance::UserProvided {
                return Err(DbError::Message(format!(
                    "table {table:?} declares host-provided blobs, which Coven copies; \
                     an external file registration on it would never be read"
                )));
            }
            prepared.validate_current()?;
            let table_ident = crate::quote_ident(table);
            let size_ident = crate::quote_ident(&blob.size_column);
            let hash_ident = crate::quote_ident(&blob.hash_column);
            let select =
                format!("SELECT {size_ident}, {hash_ident} FROM {table_ident} WHERE id = ?1");
            let (declared_size, declared_hash) =
                self.transaction.query_row(&select, [row_id], |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, Option<String>>(1)?))
                })?;
            let declared_size = u64::try_from(declared_size).map_err(|_| {
                DbError::Message(format!(
                    "external blob row {table:?}/{row_id:?} has negative size"
                ))
            })?;
            if declared_size != prepared.size() {
                return Err(DbError::Message(format!(
                    "external blob row {table:?}/{row_id:?} declares {declared_size} bytes, but Coven read {}",
                    prepared.size()
                )));
            }
            match declared_hash {
                Some(hash) if hash != prepared.hash() => {
                    return Err(DbError::Message(format!(
                        "external blob row {table:?}/{row_id:?} already declares different content"
                    )));
                }
                Some(_) => {}
                None => {
                    let update = format!(
                        "UPDATE {table_ident} SET {hash_ident} = ?1 \
                         WHERE id = ?2 AND {hash_ident} IS NULL"
                    );
                    let updated = self
                        .transaction
                        .execute(&update, rusqlite::params![prepared.hash(), row_id])?;
                    if updated != 1 {
                        return Err(DbError::Message(format!(
                            "external blob row {table:?}/{row_id:?} changed before registration"
                        )));
                    }
                }
            }
            let reference =
                Database::row_blob_ref_on(self.transaction, self.gates, declared, row_id)?;
            ExternalBlobRecords::new(self.transaction).register(&reference, prepared.path())
        })
    }

    pub fn enqueue_blob_delete(
        &self,
        blob: &coven_protocol::blob::RowBlobRef,
    ) -> Result<(), DbError> {
        crate::observe_host_sql_write();
        let stored = blob.stored().ok_or_else(|| {
            DbError::Message(format!(
                "blob {:?} in {:?} has no cloud object to remove",
                blob.blob().id,
                blob.blob().namespace
            ))
        })?;
        crate::with_coven_sql_authority(|| {
            CloudOutboxRecords::new(self.transaction).enqueue_delete(stored, &self.stamp())
        })
    }

    pub fn clear_external_blob(&self, table: &str, row_id: &str) -> Result<(), DbError> {
        crate::observe_host_sql_write();
        crate::with_coven_sql_authority(|| {
            let declared = self.blob_table(table)?;
            let reference =
                Database::row_blob_ref_on(self.transaction, self.gates, declared, row_id)?;
            ExternalBlobRecords::new(self.transaction).clear(&reference)
        })
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn materialized_sequence(&self, stream_id: &str) -> Result<Option<u64>, DbError> {
        use rusqlite::OptionalExtension;

        crate::with_coven_sql_authority(|| {
            self.transaction
                .query_row(
                    "SELECT seq FROM materialized_commits WHERE device_id = ?1",
                    [stream_id],
                    |row| row.get::<_, i64>(0),
                )
                .optional()
                .map_err(DbError::from)?
                .map(|sequence| {
                    u64::try_from(sequence)
                        .map_err(|error| DbError::context("invalid sequence", error))
                })
                .transpose()
        })
    }
}

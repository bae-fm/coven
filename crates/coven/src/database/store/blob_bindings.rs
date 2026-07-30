use rusqlite::OptionalExtension;

use super::*;

impl StoreDatabase {
    pub(crate) async fn stored_blob_reference_state(
        &self,
        stored: crate::blob::locator::StoredBlobRef,
    ) -> Result<crate::database::StoredBlobReferenceState, DbError> {
        let gates = self.gates();
        let tables = self.synced_tables().to_vec();
        self.connection
            .call(move |connection| {
                Database::stored_blob_reference_state_on(connection, &gates, &tables, &stored)
            })
            .await
    }

    #[doc(hidden)]
    pub(crate) async fn row_blob_ref(
        &self,
        table: &str,
        row_id: &str,
    ) -> Result<crate::blob::RowBlobRef, DbError> {
        let table = self
            .synced_tables()
            .iter()
            .find(|candidate| candidate.name() == table)
            .cloned()
            .ok_or_else(|| DbError::Message(format!("undeclared synced table {table:?}")))?;
        if table.blob().is_none() {
            return Err(DbError::Message(format!(
                "synced table {:?} has no blob declaration",
                table.name()
            )));
        }
        let row_id = row_id.to_string();
        let gates = self.gates();
        self.connection
            .call(move |connection| Database::row_blob_ref_on(connection, &gates, &table, &row_id))
            .await
    }

    pub(crate) async fn row_blob_refs_for_root(
        &self,
        root_table: &str,
        root_id: &str,
    ) -> Result<Vec<crate::blob::RowBlobRef>, DbError> {
        let root_table = root_table.to_string();
        let root_id = root_id.to_string();
        let gates = self.gates();
        let tables = self.synced_tables().to_vec();
        self.connection
            .call(move |connection| {
                Database::row_blob_refs_for_root_on(
                    connection,
                    &gates,
                    &tables,
                    &root_table,
                    &root_id,
                )
            })
            .await
    }

    pub(crate) async fn validate_row_blob_ref(
        &self,
        reference: &crate::blob::RowBlobRef,
    ) -> Result<(), DbError> {
        let current = self
            .row_blob_ref(reference.table(), reference.row_id())
            .await?;
        if &current != reference {
            return Err(DbError::Message(format!(
                "row blob reference {:?}/{:?}/{:?} at {:?} is stale",
                reference.table(),
                reference.row_id(),
                reference.column(),
                reference.row_stamp()
            )));
        }
        Ok(())
    }

    pub(crate) async fn external_blob_for_row(
        &self,
        reference: &crate::blob::RowBlobRef,
    ) -> Result<Option<crate::database::ExternalBlob>, DbError> {
        let table = reference.table().to_string();
        let row_id = reference.row_id().to_string();
        let column = reference.column().to_string();
        let row_stamp = reference.row_stamp().to_string();
        let namespace = reference.blob().namespace.clone();
        let blob_id = reference.blob().id.clone();
        let expected_size = reference.plaintext_size();
        let expected_hash = reference.plaintext_hash();
        self.connection
            .call(move |connection| {
                let row = connection
                    .query_row(
                        "SELECT path, plaintext_size, plaintext_hash, namespace, blob_id
                         FROM local_blob_refs
                         WHERE table_name = ?1 AND row_id = ?2 AND column_name = ?3
                           AND row_stamp = ?4",
                        rusqlite::params![table, row_id, column, row_stamp],
                        |row| {
                            Ok((
                                row.get::<_, String>(0)?,
                                row.get::<_, i64>(1)?,
                                row.get::<_, String>(2)?,
                                row.get::<_, String>(3)?,
                                row.get::<_, String>(4)?,
                            ))
                        },
                    )
                    .optional()
                    .map_err(DbError::from)?;
                let Some((path, size, hash, stored_namespace, stored_blob_id)) = row else {
                    return Ok(None);
                };
                let size = u64::try_from(size).map_err(|_| {
                    DbError::Message(format!("external blob {blob_id} has negative size"))
                })?;
                let hash: crate::protocol::store_commit::ObjectHash =
                    hash.parse().map_err(|error| {
                        DbError::Message(format!("external blob {blob_id} hash: {error}"))
                    })?;
                if size != expected_size
                    || hash != expected_hash
                    || stored_namespace != namespace
                    || stored_blob_id != blob_id
                {
                    return Err(DbError::Message(format!(
                        "external blob row {table}/{row_id}/{column} differs from its row reference"
                    )));
                }
                Ok(Some(crate::database::ExternalBlob {
                    path: std::path::PathBuf::from(path),
                    size,
                }))
            })
            .await
    }

    #[doc(hidden)]
    pub(crate) async fn external_blob(
        &self,
        table: &str,
        row_id: &str,
    ) -> Result<Option<crate::database::ExternalBlob>, DbError> {
        let reference = self.row_blob_ref(table, row_id).await?;
        self.external_blob_for_row(&reference).await
    }
}

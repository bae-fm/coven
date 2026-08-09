use super::*;
use crate::{ExternalBlob, ExternalBlobRecords};

impl StoreDatabase {
    pub async fn eager_row_blob_refs(
        &self,
    ) -> Result<Vec<coven_protocol::blob::RowBlobRef>, DbError> {
        let tables = self.synced_tables().to_vec();
        let gates = self.gates.clone();
        self.connection
            .call_store(move |session| {
                let connection = session.records.conn;
                let mut references = Vec::new();
                for table in &tables {
                    let Some(declaration) = table.blob() else {
                        continue;
                    };
                    if declaration.fill != coven_protocol::blob::CacheFill::CacheEager {
                        continue;
                    }
                    let sql = format!(
                        "SELECT id FROM {} WHERE {} IS NOT NULL ORDER BY id",
                        crate::quote_ident(table.name()),
                        crate::quote_ident(&declaration.id_column),
                    );
                    let mut statement = connection.prepare(&sql).map_err(DbError::from)?;
                    let row_ids = statement
                        .query_map([], |row| row.get::<_, String>(0))
                        .map_err(DbError::from)?
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(DbError::from)?;
                    drop(statement);
                    for row_id in row_ids {
                        references.push(Database::row_blob_ref_on(
                            connection, &gates, table, &row_id,
                        )?);
                    }
                }
                Ok(references)
            })
            .await
    }

    pub async fn stored_blob_reference_state(
        &self,
        stored: coven_protocol::blob::locator::StoredBlobRef,
    ) -> Result<crate::StoredBlobReferenceState, DbError> {
        let gates = self.gates.clone();
        let tables = self.synced_tables().to_vec();
        self.connection
            .call_store(move |session| {
                let connection = session.records.conn;
                Database::stored_blob_reference_state_on(connection, &gates, &tables, &stored)
            })
            .await
    }

    #[doc(hidden)]
    pub async fn row_blob_ref(
        &self,
        table: &str,
        row_id: &str,
    ) -> Result<coven_protocol::blob::RowBlobRef, DbError> {
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
        let gates = self.gates.clone();
        self.connection
            .call_store(move |session| {
                let connection = session.records.conn;
                Database::row_blob_ref_on(connection, &gates, &table, &row_id)
            })
            .await
    }

    pub async fn row_blob_refs_for_root(
        &self,
        root_table: &str,
        root_id: &str,
    ) -> Result<Vec<coven_protocol::blob::RowBlobRef>, DbError> {
        let root_table = root_table.to_string();
        let root_id = root_id.to_string();
        let gates = self.gates.clone();
        let tables = self.synced_tables().to_vec();
        self.connection
            .call_store(move |session| {
                let connection = session.records.conn;
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

    pub async fn validate_row_blob_ref(
        &self,
        reference: &coven_protocol::blob::RowBlobRef,
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

    pub async fn external_blob_for_row(
        &self,
        reference: &coven_protocol::blob::RowBlobRef,
    ) -> Result<Option<ExternalBlob>, DbError> {
        let reference = reference.clone();
        self.connection
            .call_store(move |session| {
                ExternalBlobRecords::new(session.records.conn).load(&reference)
            })
            .await
    }

    #[doc(hidden)]
    pub async fn external_blob(
        &self,
        table: &str,
        row_id: &str,
    ) -> Result<Option<ExternalBlob>, DbError> {
        let reference = self.row_blob_ref(table, row_id).await?;
        self.external_blob_for_row(&reference).await
    }
}

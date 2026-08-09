use super::*;
use crate::{ExternalBlob, ExternalBlobRecords};

impl StoreSession<'_> {
    fn eager_row_blob_refs(&self) -> Result<Vec<coven_protocol::blob::RowBlobRef>, DbError> {
        let mut references = Vec::new();
        for table in self.synced_tables {
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
            let mut statement = self.records.conn.prepare(&sql).map_err(DbError::from)?;
            let row_ids = statement
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(DbError::from)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(DbError::from)?;
            drop(statement);
            for row_id in row_ids {
                references.push(Database::row_blob_ref_on(
                    self.records.conn,
                    self.gates,
                    table,
                    &row_id,
                )?);
            }
        }
        Ok(references)
    }

    fn stored_blob_reference_state(
        &self,
        stored: &coven_protocol::blob::locator::StoredBlobRef,
    ) -> Result<crate::StoredBlobReferenceState, DbError> {
        Database::stored_blob_reference_state_on(
            self.records.conn,
            self.gates,
            self.synced_tables,
            stored,
        )
    }

    fn row_blob_ref(
        &self,
        table: &coven_protocol::synced_schema::SyncedTable,
        row_id: &str,
    ) -> Result<coven_protocol::blob::RowBlobRef, DbError> {
        Database::row_blob_ref_on(self.records.conn, self.gates, table, row_id)
    }

    fn row_blob_refs_for_root(
        &self,
        root_table: &str,
        root_id: &str,
    ) -> Result<Vec<coven_protocol::blob::RowBlobRef>, DbError> {
        Database::row_blob_refs_for_root_on(
            self.records.conn,
            self.gates,
            self.synced_tables,
            root_table,
            root_id,
        )
    }

    fn external_blob_for_row(
        &self,
        reference: &coven_protocol::blob::RowBlobRef,
    ) -> Result<Option<ExternalBlob>, DbError> {
        ExternalBlobRecords::new(self.records.conn).load(reference)
    }
}

impl StoreDatabase {
    pub async fn eager_row_blob_refs(
        &self,
    ) -> Result<Vec<coven_protocol::blob::RowBlobRef>, DbError> {
        self.connection
            .call_store(|session| session.eager_row_blob_refs())
            .await
    }

    pub async fn stored_blob_reference_state(
        &self,
        stored: coven_protocol::blob::locator::StoredBlobRef,
    ) -> Result<crate::StoredBlobReferenceState, DbError> {
        self.connection
            .call_store(move |session| session.stored_blob_reference_state(&stored))
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
        self.connection
            .call_store(move |session| session.row_blob_ref(&table, &row_id))
            .await
    }

    pub async fn row_blob_refs_for_root(
        &self,
        root_table: &str,
        root_id: &str,
    ) -> Result<Vec<coven_protocol::blob::RowBlobRef>, DbError> {
        let root_table = root_table.to_string();
        let root_id = root_id.to_string();
        self.connection
            .call_store(move |session| session.row_blob_refs_for_root(&root_table, &root_id))
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
            .call_store(move |session| session.external_blob_for_row(&reference))
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

use crate::{Database, DatabaseTestTable, DbError};

impl Database {
    pub async fn table_row_count_for_test(&self, table: DatabaseTestTable) -> Result<i64, DbError> {
        self.test_sql(move |database| database.table_row_count(table))
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn install_blob_binding_for_test(
        &self,
        object_id: String,
        state: String,
        locator_hash: String,
        table: &str,
        row_id: &str,
        column: &str,
        row_stamp: &str,
        audience: String,
    ) -> Result<(), DbError> {
        let table = table.to_string();
        let row_id = row_id.to_string();
        let column = column.to_string();
        let row_stamp = row_stamp.to_string();
        self.test_sql(move |database| {
            database.install_blob_binding(
                &object_id,
                &state,
                &locator_hash,
                &table,
                &row_id,
                &column,
                &row_stamp,
                &audience,
            )
        })
        .await
    }

    pub async fn protocol_state_prefix_count_for_test(&self, prefix: &str) -> Result<i64, DbError> {
        let prefix = prefix.to_string();
        self.test_sql(move |database| database.protocol_state_prefix_count(&prefix))
            .await
    }

    pub async fn exact_row_blob_locator_count_for_test(
        &self,
        table: &str,
        row_id: &str,
        column: &str,
        stamp: &str,
    ) -> Result<i64, DbError> {
        let table = table.to_string();
        let row_id = row_id.to_string();
        let column = column.to_string();
        let stamp = stamp.to_string();
        self.test_sql(move |database| {
            database.exact_row_blob_locator_count(&table, &row_id, &column, &stamp)
        })
        .await
    }

    pub async fn exact_upload_outbox_count_for_test(
        &self,
        table: &str,
        row_id: &str,
        column: &str,
        stamp: &str,
    ) -> Result<i64, DbError> {
        let table = table.to_string();
        let row_id = row_id.to_string();
        let column = column.to_string();
        let stamp = stamp.to_string();
        self.test_sql(move |database| {
            database.exact_upload_outbox_count(&table, &row_id, &column, &stamp)
        })
        .await
    }

    pub async fn install_outbound_preparation_failure_for_test(&self) -> Result<(), DbError> {
        self.test_sql(|database| database.install_outbound_preparation_failure_trigger())
            .await
    }

    pub async fn remove_outbound_preparation_failure_for_test(&self) -> Result<(), DbError> {
        self.test_sql(|database| {
            database
                .execute_batch("DROP TRIGGER fail_outbound_preparation")
                .map_err(DbError::from)
        })
        .await
    }
}

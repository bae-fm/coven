use super::*;

impl StoreDatabase {
    pub async fn row_blob_bindings_for_test(&self) -> Result<String, DbError> {
        self.call_store(|session| session.row_blob_bindings_for_test())
            .await
    }

    pub async fn replace_blob_row_stamp_for_test(
        &self,
        table: &str,
        row_id: &str,
        stamp: &str,
    ) -> Result<(), DbError> {
        let table = table.to_string();
        let row_id = row_id.to_string();
        let stamp = stamp.to_string();
        self.call_store(move |session| {
            session.replace_blob_row_stamp_for_test(&table, &row_id, &stamp)
        })
        .await
    }

    pub async fn replace_blob_row_facts_for_test(
        &self,
        table: &str,
        row_id: &str,
        size: i64,
        hash: &str,
        stamp: &str,
    ) -> Result<(), DbError> {
        let table = table.to_string();
        let row_id = row_id.to_string();
        let hash = hash.to_string();
        let stamp = stamp.to_string();
        self.call_store(move |session| {
            session.replace_blob_row_facts_for_test(&table, &row_id, size, &hash, &stamp)
        })
        .await
    }

    pub async fn complete_note_blob_transition_to_remote_for_test(
        &self,
        reference: coven_protocol::blob::RowBlobRef,
        note_id: String,
    ) -> Result<(), DbError> {
        self.call_store(move |session| {
            session.complete_note_blob_transition_to_remote_for_test(&reference, &note_id)
        })
        .await
    }

    pub async fn plant_blob_namespace_collision_for_test(
        &self,
        id: &str,
        local_hash: &str,
        remote_hash: &str,
    ) -> Result<(), DbError> {
        let id = id.to_string();
        let local_hash = local_hash.to_string();
        let remote_hash = remote_hash.to_string();
        self.call_store(move |session| {
            session.plant_blob_namespace_collision_for_test(&id, &local_hash, &remote_hash)
        })
        .await
    }

    pub async fn plant_note_cover_blob_row_for_test(
        &self,
        id: &str,
        note_id: &str,
        size: i64,
        hash: &str,
    ) -> Result<(), DbError> {
        let id = id.to_string();
        let note_id = note_id.to_string();
        let hash = hash.to_string();
        self.call_store(move |session| {
            session.plant_note_cover_blob_row_for_test(&id, &note_id, size, &hash)
        })
        .await
    }
}

impl StoreSession<'_> {
    fn row_blob_bindings_for_test(&self) -> Result<String, DbError> {
        self.conn
            .query_row(
                "SELECT json_group_array(json_array(
                    table_name, row_id, column_name, row_stamp, audience_authority, remote_object_id))
                 FROM (SELECT * FROM row_blob_locators
                       ORDER BY table_name, row_id, column_name, row_stamp)",
                [],
                |row| row.get(0),
            )
            .map_err(DbError::from)
    }
}

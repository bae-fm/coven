use crate::database::{Database, DatabaseTestSql, DbError};

impl Database {
    pub(crate) async fn table_has_rows_for_test(
        &self,
        table: crate::database::DatabaseTestTable,
    ) -> Result<bool, DbError> {
        self.test_sql(move |database| database.table_has_rows(table))
            .await
    }

    pub(crate) async fn store_partition_changesets_for_test(
        &self,
    ) -> Result<Vec<Vec<u8>>, DbError> {
        self.test_sql(|database| database.store_partition_changesets())
            .await
    }

    pub(crate) async fn has_store_partition_for_test(&self) -> Result<bool, DbError> {
        self.test_sql(|database| database.has_store_partition())
            .await
    }

    pub(crate) async fn make_remote_intent_exists_for_test(
        &self,
        root_table: &str,
        root_id: &str,
    ) -> Result<bool, DbError> {
        let root_table = root_table.to_string();
        let root_id = root_id.to_string();
        self.test_sql(move |database| database.make_remote_intent_exists(&root_table, &root_id))
            .await
    }

    pub(crate) async fn published_blob_drop_intent_exists_for_test(
        &self,
        blob_id: &str,
    ) -> Result<bool, DbError> {
        let blob_id = blob_id.to_string();
        self.test_sql(move |database| database.published_blob_drop_intent_exists(&blob_id))
            .await
    }

    pub(crate) async fn insert_published_blob_drop_intent_for_test(
        &self,
        sequence: u64,
        namespace: &str,
        blob_id: &str,
        bytes: &[u8],
        locator_hash: crate::protocol::store_commit::ObjectHash,
        disposition: crate::sync::cycle::DeferredLocalBlobDisposition,
    ) -> Result<(), DbError> {
        let drop = crate::sync::cycle::DeferredLocalBlobDrop {
            namespace: namespace.to_string(),
            id: blob_id.to_string(),
            size: bytes.len() as u64,
            plaintext_hash: crate::protocol::store_commit::ObjectHash::digest(bytes),
            locator_hash,
            disposition,
        };
        self.test_sql(move |database| database.insert_published_blob_drop_intent(sequence, &drop))
            .await
    }

    pub(crate) async fn remote_object_for_test(
        &self,
        object: crate::storage::ExactObjectRef,
    ) -> Result<crate::protocol::remote_object::RemoteObjectRecord, DbError> {
        self.test_sql(move |database| database.remote_object(&object))
            .await
    }

    pub(crate) async fn remote_objects_for_test(
        &self,
    ) -> Result<Vec<crate::protocol::remote_object::RemoteObjectRecord>, DbError> {
        self.test_sql(|database| database.remote_objects()).await
    }

    pub(crate) async fn remote_object_exists_for_test(
        &self,
        object: crate::storage::ExactObjectRef,
    ) -> Result<bool, DbError> {
        self.test_sql(move |database| database.remote_object_exists(&object))
            .await
    }

    pub(crate) async fn remote_object_id_exists_for_test(
        &self,
        object_id: crate::protocol::store_commit::ObjectHash,
    ) -> Result<bool, DbError> {
        self.test_sql(move |database| database.remote_object_id_exists(object_id))
            .await
    }

    pub(crate) async fn replace_remote_object_for_test(
        &self,
        object: crate::storage::ExactObjectRef,
        remote: crate::protocol::remote_object::RemoteObjectRecord,
    ) -> Result<(), DbError> {
        self.test_sql(move |database| database.replace_remote_object(&object, &remote))
            .await
    }

    pub(crate) async fn delete_remote_object_for_test(
        &self,
        object: crate::storage::ExactObjectRef,
    ) -> Result<(), DbError> {
        self.test_sql(move |database| database.delete_remote_object(&object))
            .await
    }

    pub(crate) async fn test_sql<F, R>(&self, operation: F) -> Result<R, DbError>
    where
        F: for<'connection> FnOnce(DatabaseTestSql<'connection>) -> Result<R, DbError>
            + Send
            + 'static,
        R: Send + 'static,
    {
        self.connection
            .call(move |connection| operation(DatabaseTestSql::new(connection)))
            .await
    }
}

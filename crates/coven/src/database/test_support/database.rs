use crate::database::{Database, DatabaseTestSql, DbError};

impl Database {
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

use super::*;

impl AuthorizedWriterOperation<'_> {
    pub(crate) async fn drain_local_blob_cleanup(&self) -> Result<bool, coven_database::DbError> {
        self.history.drain_local_blob_cleanup().await
    }

    pub(crate) async fn persist_hlc_high_water(&self) -> Result<(), coven_database::DbError> {
        self.database.persist_hlc_high_water().await
    }
}

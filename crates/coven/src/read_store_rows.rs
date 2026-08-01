use crate::database::StoreDatabase;
use crate::{CovenError, CovenResult, SqlReadContext};

#[derive(Clone)]
pub(crate) struct ReadStoreRows {
    database: StoreDatabase,
}

impl ReadStoreRows {
    pub(crate) fn new(database: StoreDatabase) -> Self {
        Self { database }
    }

    pub(crate) async fn read<F, R>(&self, read: F) -> CovenResult<R>
    where
        F: for<'connection> FnOnce(SqlReadContext<'connection>) -> CovenResult<R> + Send + 'static,
        R: Send + 'static,
    {
        self.database.read(read).await.map_err(CovenError::from)?
    }
}

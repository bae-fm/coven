use crate::database::StoreDatabase;
use crate::{CovenError, CovenResult};

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
        F: FnOnce(&rusqlite::Connection) -> CovenResult<R> + Send + 'static,
        R: Send + 'static,
    {
        self.database.read(read).await.map_err(CovenError::from)?
    }
}

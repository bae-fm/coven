//! The store's live queries: each runs on the store's readers, and runs again
//! when a commit changes what it read (§4).

use super::StoreDatabase;
use crate::sqlite::StoreSql;
use crate::write_failure::Hosted;
use crate::{DbError, LiveQuery, LostValue, ReconfigurableLiveQuery, SqlReadContext};
use coven_foundation::task::CarriesBug;
use std::sync::Arc;

impl StoreDatabase {
    /// Observe the tables and columns a query reads, and the rows its
    /// conditions allow. Its results fail with the closure's own error type.
    pub fn subscribe<F, R, E>(&self, query: F) -> LiveQuery<R, E>
    where
        F: Fn(SqlReadContext<'_>) -> Result<R, E> + Send + Sync + 'static,
        R: Send + 'static,
        E: From<DbError> + CarriesBug + Send + 'static,
    {
        LiveQuery::new(self.subscribe_reconfigurable((), move |(), sql| query(sql)))
    }

    /// Observe a query whose request the caller can replace through a shared
    /// handle.
    pub fn subscribe_reconfigurable<Q, F, R, E>(
        &self,
        initial_request: Q,
        query: F,
    ) -> ReconfigurableLiveQuery<Q, R, E>
    where
        Q: Clone + PartialEq + Send + Sync + 'static,
        F: Fn(&Q, SqlReadContext<'_>) -> Result<R, E> + Send + Sync + 'static,
        R: Send + 'static,
        E: From<DbError> + CarriesBug + Send + 'static,
    {
        self.live(initial_request, move |request, reader| {
            query(request, SqlReadContext::new(reader))
        })
    }

    /// Follow `query`, coven's own SQL on its tables, live: it runs again
    /// after each commit that changes what it read.
    pub(crate) fn follow<F, R, E>(&self, query: F) -> LiveQuery<R, E>
    where
        F: Fn(&StoreSql) -> Result<R, E> + Send + Sync + 'static,
        R: Send + 'static,
        E: From<DbError> + CarriesBug + Send + 'static,
    {
        LiveQuery::new(self.live((), move |(), reader| query(reader)))
    }

    /// A live query that runs `query` on the store's readers for each
    /// request.
    fn live<Q, F, R, E>(&self, initial_request: Q, query: F) -> ReconfigurableLiveQuery<Q, R, E>
    where
        Q: Clone + PartialEq + Send + Sync + 'static,
        F: Fn(&Q, &StoreSql) -> Result<R, E> + Send + Sync + 'static,
        R: Send + 'static,
        E: From<DbError> + CarriesBug + Send + 'static,
    {
        // A subscription retains a database only for its active read.
        // Idle queries and undelivered results cannot keep a closing lock held.
        let database = Arc::downgrade(&self.inner);
        ReconfigurableLiveQuery::new(
            initial_request,
            Arc::new(move |request, completion| {
                let database = database.upgrade().ok_or(DbError::StoreClosed)?;
                let slot = coven_foundation::locks::read_unpoisoned(&database);
                let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                inner.call(|inner| {
                    inner.access.with_reader(|reader, call| {
                        let _constants = crate::transaction_constants::keep_constants(reader, call);
                        reader
                            .live_read(
                                &mut |reads, lookups| completion.finish_judging(reads, lookups),
                                |reader| query(request, reader).map_err(Hosted),
                            )
                            .map_err(Hosted::into_inner)
                    })
                })
            }),
            |error| E::from(DbError::from(error)),
            &self.observer,
            self.bugs.clone(),
        )
    }

    /// Observe the decoded lost cells and removed rows.
    pub fn subscribe_lost_values<E>(&self) -> LiveQuery<Vec<LostValue>, E>
    where
        E: From<DbError> + coven_foundation::task::CarriesBug + Send + 'static,
    {
        self.follow(|db| crate::loss_record::lost_values(db).map_err(E::from))
    }
}

#[cfg(test)]
#[path = "database_live_queries_tests.rs"]
mod tests;

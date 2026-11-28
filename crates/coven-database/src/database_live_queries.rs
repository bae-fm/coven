//! The store's live queries: each runs on the store's readers, and runs again
//! when a commit changes what it read (§4).

use super::StoreDatabase;
use crate::live_reads::{LiveReads, Run};
use crate::{DbError, LiveQuery, LostValue, ReconfigurableLiveQuery};
use coven_foundation::task::CarriesBug;
use std::sync::Arc;

impl StoreDatabase {
    /// Observe the rows `reads` returns: coven runs the reads again for each
    /// commit that changes what they read. Its results fail with `E`, which
    /// holds a database failure.
    pub fn subscribe<S, E>(&self, reads: S) -> LiveQuery<S::Rows, E>
    where
        S: LiveReads,
        E: From<DbError> + CarriesBug + Send + 'static,
    {
        LiveQuery::new(self.live((), move |(), run| Ok(reads.read(run)?)))
    }

    /// Observe the rows the reads `reads` makes of each request return,
    /// where the caller replaces the request through a shared handle. A
    /// request `reads` can't make reads of fails its run with `reads`'s
    /// error.
    pub fn subscribe_reconfigurable<Q, S, E>(
        &self,
        initial_request: Q,
        reads: impl Fn(&Q) -> Result<S, E> + Send + Sync + 'static,
    ) -> ReconfigurableLiveQuery<Q, S::Rows, E>
    where
        Q: Clone + PartialEq + Send + Sync + 'static,
        S: LiveReads,
        E: From<DbError> + CarriesBug + Send + 'static,
    {
        self.live(initial_request, move |request, run| Ok(reads(request)?.read(run)?))
    }

    /// A live query that runs `read` on the store's readers for each
    /// request.
    fn live<Q, R, E>(
        &self,
        initial_request: Q,
        read: impl Fn(&Q, Run<'_>) -> Result<R, E> + Send + Sync + 'static,
    ) -> ReconfigurableLiveQuery<Q, R, E>
    where
        Q: Clone + PartialEq + Send + Sync + 'static,
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
                        let mut ran = false;
                        let result = read(request, &mut |statements, each| {
                            ran = true;
                            let judge = &mut |reads, lookups: &mut sqlive::Lookups<'_>| {
                                completion.finish_judging(reads, lookups)
                            };
                            Ok(reader.live_read(judge, statements, each)?)
                        });
                        // Reads that failed before they ran read nothing,
                        // which the query's judging still hears.
                        if !ran {
                            let judge = &mut |reads, lookups: &mut sqlive::Lookups<'_>| {
                                completion.finish_judging(reads, lookups)
                            };
                            reader
                                .live_read(judge, &[], |_, _| Ok(()))
                                .map_err(|error| E::from(DbError::from(error)))?;
                        }
                        result
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
        self.subscribe(crate::loss_record::lost_values_read())
    }
}

#[cfg(test)]
#[path = "database_live_queries_tests.rs"]
mod tests;

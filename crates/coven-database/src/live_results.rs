//! A live query's results for code on an async executor.

use crate::{DbError, LiveQuery};
use coven_foundation::locks::{into_unpoisoned, lock_unpoisoned};
use coven_foundation::task::{blocking_completing, CarriesBug};
use std::sync::{Arc, Mutex};

/// The query's next result, with the query to ask again: it waits for
/// `changed()` on the executor, runs `run_changed` on tokio's blocking pool,
/// and waits again when a write-caused result equals the last, so no pool
/// thread waits. `None` once the query has ended (E15). A pool that has begun
/// shutting down answers `Some(Err(DbError::Shutdown))`, and a panic outside
/// the run itself answers `Some(Err(DbError::Bug))`.
///
/// Dropping this future loses the query, since the pool's thread holds it; a
/// run in flight ends on its own thread, and a bug in its result goes to the
/// store's bug sink (E15).
pub async fn next_result<T, E>(
    mut query: LiveQuery<T, E>,
) -> (LiveQuery<T, E>, Option<Result<T, E>>)
where
    T: Clone + PartialEq + Send + 'static,
    E: From<DbError> + CarriesBug + Send + 'static,
{
    loop {
        if query.has_ended() {
            return (query, None);
        }
        query.changed().await;
        let bugs = query.bugs().clone();
        // The slot keeps the query when the pool drops the work unstarted or
        // the work panics, so either answers with the query.
        let slot = Arc::new(Mutex::new(query));
        let held = Arc::clone(&slot);
        let result = blocking_completing(
            "live query",
            bugs,
            crate::commit_task::bind(move || match lock_unpoisoned(&held).run_changed() {
                None => Ok(None),
                Some(result) => result.map(Some),
            }),
        )
        .await;
        query = match Arc::try_unwrap(slot) {
            Ok(slot) => into_unpoisoned(slot),
            Err(_) => unreachable!("the pool has dropped or ended the work"),
        };
        match result {
            Ok(Ok(None)) => {}
            Ok(Ok(Some(value))) => return (query, Some(Ok(value))),
            Ok(Err(error)) => return (query, Some(Err(error))),
            Err(failed) => return (query, Some(Err(E::from(DbError::from(failed))))),
        }
    }
}

#[cfg(test)]
#[path = "live_results_tests.rs"]
mod tests;

//! How a call runs on the store: on the blocking pool, to its end even when
//! its caller stops waiting, with its panic, or a bug its error carries, then
//! going to the store's bug sink (E15).

use coven_foundation::locks::read_unpoisoned;
use coven_foundation::task::{
    blocking_completing, BugSinkRef, CarriesBug, CompletingTask, TaskFailed,
};
use std::sync::{Arc, RwLock};

use crate::{CovenResult, DbError};

/// Run `run`, named `operation`, on the open store on the blocking pool. It
/// runs to its end even when its caller stops waiting, and then its panic,
/// or a bug its error carries, goes to `bugs` (E15).
pub(crate) fn start_call<I, R, E>(
    operation: &'static str,
    bugs: &BugSinkRef,
    inner: Arc<RwLock<Option<I>>>,
    run: impl FnOnce(&I) -> Result<R, E> + Send + 'static,
) -> CompletingTask<Result<R, E>>
where
    I: Send + Sync + 'static,
    R: Send + 'static,
    E: From<DbError> + CarriesBug + Send + 'static,
{
    blocking_completing(
        operation,
        bugs.clone(),
        crate::commit_scope::bind(move || {
            let slot = read_unpoisoned(&inner);
            run(slot.as_ref().ok_or(DbError::StoreClosed)?)
        }),
    )
}

/// The result of a database call run on the blocking pool. A panic in it is
/// `DbError::Bug`; the pool dropping it unstarted, as it does once its
/// runtime begins shutting down, is `DbError::Shutdown`, and the call
/// changed nothing (E15).
pub(crate) fn finish_blocking<R, E: From<DbError>>(
    joined: Result<Result<R, E>, TaskFailed>,
) -> Result<R, E> {
    joined.map_err(|failed| E::from(DbError::from(failed)))?
}

/// Run `process` on the blocking pool; once its caller stops waiting, its
/// panic, or a bug its error carries, goes to `bugs`.
pub(crate) async fn process<F, T>(bugs: &BugSinkRef, process: F) -> CovenResult<T>
where
    F: FnOnce() -> CovenResult<T> + Send + 'static,
    T: Send + 'static,
{
    finish_blocking(blocking_completing("process a read", bugs.clone(), process).await)
}

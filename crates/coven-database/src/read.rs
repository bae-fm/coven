//! Borrowed reads and their connection-scoped SQL capability.

use crate::sqlite::StoreSql;
use crate::{database, DbError, Mode, StoreDatabase, Writer};
use coven_foundation::task::CarriesBug;
use sqlive::rusqlite::{Params, Row};
use std::{
    any::Any,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

/// SQL access to one read-only snapshot. The connection and internal tables
/// remain private; even SQL with RETURNING cannot write through this context.
/// `M` is the kind of handle it reads on, whose compiled reads alone it runs.
pub struct SqlReadContext<'connection, M: Mode = Writer> {
    database: &'connection StoreSql,
    mode: PhantomData<M>,
}

impl<'connection, M: Mode> SqlReadContext<'connection, M> {
    pub(crate) fn new(database: &'connection StoreSql) -> Self {
        Self { database, mode: PhantomData }
    }

    /// Map the first result row of an ordinary SQL read.
    pub fn query_row<T, P, F>(&self, sql: &str, params: P, map: F) -> sqlive::rusqlite::Result<T>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> sqlive::rusqlite::Result<T>,
    {
        self.database.app_query_row(sqlive::Recorded::Yes, sql, params, map)
    }

    /// Map all result rows of an ordinary SQL read.
    pub fn query<T, P, F>(&self, sql: &str, params: P, map: F) -> sqlive::rusqlite::Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> sqlive::rusqlite::Result<T>,
    {
        self.database.app_query(sqlive::Recorded::Yes, sql, params, map)
    }

    /// Map all result rows of a compiled read, with the scope plan it
    /// brings (`SqlRead::query_compiled`).
    pub fn query_compiled<T, P, F>(
        &self,
        statement: sqlive::CompiledStatement<M>,
        params: P,
        map: F,
    ) -> sqlive::rusqlite::Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> sqlive::rusqlite::Result<T>,
    {
        self.database.app_query_compiled(sqlive::Recorded::Yes, statement, params, map)
    }
}

impl SqlReadContext<'_, Writer> {
    /// What `reads` returns, read once in this snapshot: the reads a live
    /// query runs, run as a one-shot read, which records nothing.
    pub fn reads<S: crate::LiveReads>(&self, reads: &S) -> Result<S::Rows, DbError> {
        reads.read(&mut |statements, each| {
            for (index, statement) in statements.iter().enumerate() {
                self.database.once_rows(statement, |row| each(index, row))?;
            }
            Ok(())
        })
    }
}

/// A read that starts when polled and retains its store borrow until it
/// ends, on the store's writer or a read-only handle.
#[must_use = "reads run when awaited"]
pub struct PendingRead<'a, F, M: Mode = Writer> {
    database: &'a StoreDatabase<M>,
    state: ReadState<F>,
}

// The public type names the closure, rather than its result. Only poll creates
// the running slot, as RunningRead<R, E> for that closure.
enum ReadState<F> {
    Pending(F),
    Running(Box<dyn Any + Send>),
    Finished,
}

impl<'a, F, R, E, M: Mode> PendingRead<'a, F, M>
where
    F: FnOnce(SqlReadContext<'_, M>) -> Result<R, E> + Send + 'static,
    R: Send + 'static,
    E: From<DbError> + CarriesBug + Send + 'static,
{
    pub(crate) fn new(database: &'a StoreDatabase<M>, read: F) -> Self {
        Self { database, state: ReadState::Pending(read) }
    }

    /// Process the value on another worker after releasing the read
    /// connection. It fails with the read's own error type.
    pub async fn process<P, T>(self, process: P) -> Result<T, E>
    where
        P: FnOnce(R) -> Result<T, E> + Send + 'static,
        T: Send + 'static,
    {
        let bugs = self.database.bugs().clone();
        let result = self.await?;
        database::process(&bugs, move || process(result)).await
    }
}

// No field relies on a pinned address. The task itself is a movable handle.
impl<F, M: Mode> Unpin for PendingRead<'_, F, M> {}

impl<F, R, E, M: Mode> Future for PendingRead<'_, F, M>
where
    F: FnOnce(SqlReadContext<'_, M>) -> Result<R, E> + Send + 'static,
    R: Send + 'static,
    E: From<DbError> + CarriesBug + Send + 'static,
{
    type Output = Result<R, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut task: Box<dyn Any + Send> =
            match std::mem::replace(&mut this.state, ReadState::Finished) {
                ReadState::Pending(read) => Box::new(this.database.start_read(read)),
                ReadState::Running(task) => task,
                ReadState::Finished => panic!("read polled after completion"),
            };
        let running =
            task.downcast_mut::<RunningRead<R, E>>().expect("read task matches its closure result");
        match Pin::new(running).poll(cx) {
            Poll::Pending => {
                this.state = ReadState::Running(task);
                Poll::Pending
            }
            Poll::Ready(result) => Poll::Ready(result),
        }
    }
}

/// One read's call on the blocking pool, which its database started for the
/// `PendingRead` that awaits it.
pub(crate) struct RunningRead<R, E> {
    call: coven_foundation::task::CompletingTask<Result<R, E>>,
}

impl<R, E> RunningRead<R, E> {
    pub(crate) fn new(call: coven_foundation::task::CompletingTask<Result<R, E>>) -> Self {
        Self { call }
    }
}

impl<R, E: From<DbError>> Future for RunningRead<R, E> {
    type Output = Result<R, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().call).poll(cx).map(database::finish_blocking)
    }
}

#[cfg(test)]
#[path = "read_tests.rs"]
pub(crate) mod tests;

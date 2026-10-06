//! Borrowed reads and their connection-scoped SQL capability.

use crate::{
    database, sqlite::DatabaseConnection, CovenReadHandle, CovenResult, Database, LostValue,
};
use rusqlite::{Params, Row};
use std::{
    any::Any,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

/// SQL access to one read-only snapshot. The connection and internal tables
/// remain private; even SQL with RETURNING cannot write through this context.
pub struct SqlReadContext<'connection> {
    database: &'connection DatabaseConnection,
}

impl<'connection> SqlReadContext<'connection> {
    pub(crate) fn new(database: &'connection DatabaseConnection) -> Self {
        Self { database }
    }

    /// Map the first result row of an ordinary SQL read.
    pub fn query_row<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<T>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> rusqlite::Result<T>,
    {
        self.database.app_query_row(sql, params, map)
    }

    /// Map all result rows of an ordinary SQL read.
    pub fn query<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> rusqlite::Result<T>,
    {
        self.database.app_query(sql, params, map)
    }

    pub(crate) fn file_ref(
        &self,
        schema: &crate::write_schema::WriteSchema,
        table: &str,
        key: &crate::RowKey,
    ) -> Result<crate::FileRef, crate::DbError> {
        crate::file_ref::read(self.database, schema, table, key)
    }

    pub(crate) fn user_file(
        &self,
        schema: &crate::write_schema::WriteSchema,
        table: &str,
        key: &crate::RowKey,
    ) -> Result<Option<crate::UserFile>, crate::DbError> {
        crate::user_file::read(self.database, schema, table, key)
    }

    pub(crate) fn lost_values(&self) -> CovenResult<Vec<LostValue>> {
        self.database.lost_values()
    }
}

/// A read that starts when polled and retains its store borrow until it ends.
#[must_use = "reads run when awaited"]
pub struct Read<'a, F> {
    database: ReadOwner<'a>,
    state: ReadState<F>,
}

pub(crate) enum ReadOwner<'a> {
    Writer(&'a Database),
    Reader(&'a CovenReadHandle),
}

// The public type names the closure, rather than its result. Only poll creates
// the running slot, as JoinHandle<CovenResult<R>> for that closure.
enum ReadState<F> {
    Pending(F),
    Running(Box<dyn Any + Send>),
    Finished,
}

impl<'a, F, R> Read<'a, F>
where
    F: FnOnce(SqlReadContext<'_>) -> CovenResult<R> + Send + 'static,
    R: Send + 'static,
{
    pub(crate) fn new(database: ReadOwner<'a>, read: F) -> Self {
        Self {
            database,
            state: ReadState::Pending(read),
        }
    }

    /// Process the value on another worker after releasing the read connection.
    pub async fn process<P, T>(self, process: P) -> CovenResult<T>
    where
        P: FnOnce(R) -> CovenResult<T> + Send + 'static,
        T: Send + 'static,
    {
        let result = self.await?;
        database::process(move || process(result)).await
    }
}

// No field relies on a pinned address. The task itself is a movable handle.
impl<F> Unpin for Read<'_, F> {}

impl<F, R> Future for Read<'_, F>
where
    F: FnOnce(SqlReadContext<'_>) -> CovenResult<R> + Send + 'static,
    R: Send + 'static,
{
    type Output = CovenResult<R>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut task: Box<dyn Any + Send> =
            match std::mem::replace(&mut this.state, ReadState::Finished) {
                ReadState::Pending(read) => Box::new(match this.database {
                    ReadOwner::Writer(database) => database.start_read(read),
                    ReadOwner::Reader(database) => database.start_read(read),
                }),
                ReadState::Running(task) => task,
                ReadState::Finished => panic!("read polled after completion"),
            };
        let handle = task
            .downcast_mut::<tokio::task::JoinHandle<CovenResult<R>>>()
            .expect("read task matches its closure result");
        match Pin::new(handle).poll(cx) {
            Poll::Pending => {
                this.state = ReadState::Running(task);
                Poll::Pending
            }
            Poll::Ready(result) => Poll::Ready(database::finish_blocking(result)),
        }
    }
}

#[cfg(test)]
#[path = "read_tests.rs"]
mod tests;

//! Borrowed reads and their connection-scoped SQL capability.

use crate::{database, sqlite::DatabaseConnection, CovenResult, Database, LostValue, Mode, Writer};
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

    /// Where this device keeps the bytes of a file on this device only.
    pub(crate) fn device_only_source(
        &self,
        device: coven_foundation::id_source::DeviceId,
        reference: &crate::FileRef,
    ) -> Result<Option<crate::file_ref::LocalSource>, crate::DbError> {
        crate::file_ref::device_only_source(self.database, device, reference)
    }

    pub(crate) fn file_ref(
        &self,
        file: coven_foundation::id_source::FileId,
    ) -> Result<crate::FileRef, crate::DbError> {
        crate::file_ref::read(self.database, file)
    }

    pub(crate) fn user_file(
        &self,
        file: coven_foundation::id_source::FileId,
    ) -> Result<Option<crate::UserFile>, crate::DbError> {
        crate::user_file::read(self.database, file)
    }

    pub(crate) fn lost_values(&self) -> CovenResult<Vec<LostValue>> {
        self.database.lost_values()
    }

    pub(crate) fn pending<P: crate::PendingValue>(&self) -> CovenResult<Vec<P>> {
        Ok(crate::pending::read(self.database)?)
    }

    pub(crate) fn pin_progress(
        &self,
        files: &[crate::FileRef],
        device: coven_foundation::id_source::DeviceId,
    ) -> CovenResult<crate::PinProgress> {
        Ok(crate::cache_progress::pin_progress(self.database, files, device)?)
    }

    pub(crate) fn files_pinned(
        &self,
        files: &[coven_foundation::id_source::FileId],
    ) -> CovenResult<Vec<Option<bool>>> {
        Ok(crate::cache_progress::files_pinned(self.database, files)?)
    }

    pub(crate) fn unsent(&self) -> CovenResult<crate::UnsentWork> {
        Ok(crate::file_queue::unsent(self.database)?)
    }

    pub(crate) fn sync_facts<P: crate::PendingValue + Clone>(
        &self,
    ) -> CovenResult<crate::SyncFacts<P>> {
        Ok(crate::sync_facts::read(self.database)?)
    }

    /// `coven_sync_status`'s row, as coven reads it.
    #[cfg(test)]
    pub(crate) fn sync_status_row(&self) -> CovenResult<crate::SyncStatusRow> {
        Ok(crate::internal_schema::sync_status::read(self.database)?)
    }

    pub(crate) fn upload_queue(&self) -> CovenResult<crate::UploadQueueState> {
        Ok(crate::file_queue::state(self.database)?)
    }

    pub(crate) fn device_reset(&self) -> CovenResult<Option<coven_foundation::files::DeviceReset>> {
        Ok(crate::device_reset::read(self.database)?)
    }

    pub(crate) fn store_name(&self) -> CovenResult<String> {
        Ok(crate::store_state_tables::name(self.database)?)
    }

    pub(crate) fn named_store(&self) -> CovenResult<(String, coven_merge::Slot)> {
        Ok(crate::store_state_tables::named(self.database)?)
    }

    pub(crate) fn members(
        &self,
        device: coven_foundation::id_source::DeviceId,
    ) -> CovenResult<Vec<crate::MemberInfo>> {
        Ok(crate::members::members(self.database, device)?)
    }

    pub(crate) fn circles(
        &self,
        device: coven_foundation::id_source::DeviceId,
    ) -> CovenResult<Vec<crate::Circle>> {
        Ok(crate::members::circles(self.database, device)?)
    }

    pub(crate) fn circle_members(
        &self,
        device: coven_foundation::id_source::DeviceId,
        circle: coven_foundation::id_source::CircleId,
    ) -> CovenResult<Vec<crate::CircleMemberInfo>> {
        Ok(crate::members::circle_members(self.database, device, circle)?)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn store_state(&self) -> CovenResult<crate::StoreState> {
        Ok(crate::store_state_tables::read(self.database)?)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn retry_wait(
        &self,
        subject: crate::RetrySubject,
    ) -> CovenResult<Option<crate::RetryWait>> {
        Ok(crate::retry::find(self.database, subject)?)
    }
}

/// A read that starts when polled and retains its store borrow until it
/// ends, on the store's writer or a read-only handle.
#[must_use = "reads run when awaited"]
pub struct Read<'a, F, M: Mode = Writer> {
    database: &'a Database<M>,
    state: ReadState<F>,
}

// The public type names the closure, rather than its result. Only poll creates
// the running slot, as RunningRead<R> for that closure.
enum ReadState<F> {
    Pending(F),
    Running(Box<dyn Any + Send>),
    Finished,
}

impl<'a, F, R, M: Mode> Read<'a, F, M>
where
    F: FnOnce(SqlReadContext<'_>) -> CovenResult<R> + Send + 'static,
    R: Send + 'static,
{
    pub(crate) fn new(database: &'a Database<M>, read: F) -> Self {
        Self { database, state: ReadState::Pending(read) }
    }

    /// Process the value on another worker after releasing the read connection.
    pub async fn process<P, T>(self, process: P) -> CovenResult<T>
    where
        P: FnOnce(R) -> CovenResult<T> + Send + 'static,
        T: Send + 'static,
    {
        let bugs = self.database.bugs().clone();
        let result = self.await?;
        database::process(&bugs, move || process(result)).await
    }
}

// No field relies on a pinned address. The task itself is a movable handle.
impl<F, M: Mode> Unpin for Read<'_, F, M> {}

impl<F, R, M: Mode> Future for Read<'_, F, M>
where
    F: FnOnce(SqlReadContext<'_>) -> CovenResult<R> + Send + 'static,
    R: Send + 'static,
{
    type Output = CovenResult<R>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut task: Box<dyn Any + Send> =
            match std::mem::replace(&mut this.state, ReadState::Finished) {
                ReadState::Pending(read) => Box::new(this.database.start_read(read)),
                ReadState::Running(task) => task,
                ReadState::Finished => panic!("read polled after completion"),
            };
        let running =
            task.downcast_mut::<RunningRead<R>>().expect("read task matches its closure result");
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
/// `Read` that awaits it.
pub(crate) struct RunningRead<R> {
    call: coven_foundation::task::CompletingTask<CovenResult<R>>,
}

impl<R> RunningRead<R> {
    pub(crate) fn new(call: coven_foundation::task::CompletingTask<CovenResult<R>>) -> Self {
        Self { call }
    }
}

impl<R> Future for RunningRead<R> {
    type Output = CovenResult<R>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().call).poll(cx).map(database::finish_blocking)
    }
}

#[cfg(test)]
#[path = "read_tests.rs"]
mod tests;

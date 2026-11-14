//! Borrowed reads and their connection-scoped SQL capability.

use crate::{database, sqlite::DatabaseConnection, Database, DbError, LostValue, Mode, Writer};
use coven_foundation::task::CarriesBug;
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

    /// The journal's rows of `kind`, oldest first, as this read sees them:
    /// the internal query a status that follows one operation live runs.
    pub fn operations_of_kind(&self, kind: &str) -> Result<Vec<crate::OperationRecord>, DbError> {
        crate::operation::of_kind(self.database, kind)
    }

    /// Where this device keeps the bytes of `file`, while its record says
    /// it is on this device only.
    pub(crate) fn device_only_source(
        &self,
        device: coven_foundation::id_source::DeviceId,
        file: coven_foundation::id_source::FileId,
    ) -> Result<Option<crate::file_ref::LocalSource>, crate::DbError> {
        crate::file_ref::device_only_source(self.database, device, file)
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

    pub(crate) fn lost_values(&self) -> Result<Vec<LostValue>, DbError> {
        self.database.lost_values()
    }

    pub(crate) fn pending<P: crate::PendingValue>(&self) -> Result<Vec<P>, DbError> {
        crate::pending::read(self.database)
    }

    pub(crate) fn pin_progress(
        &self,
        files: &[crate::FileRef],
        device: coven_foundation::id_source::DeviceId,
    ) -> Result<crate::PinProgress, DbError> {
        crate::cache_progress::pin_progress(self.database, files, device)
    }

    pub(crate) fn cache_budgets(&self) -> Result<Vec<crate::NamespaceBytes>, DbError> {
        crate::database::local_bytes::budgets(self.database)
    }

    /// Every namespace's counters and the partials' sum, in one snapshot.
    pub(crate) fn cache_bytes(&self) -> Result<(Vec<crate::NamespaceBytes>, u64), DbError> {
        Ok((
            crate::database::local_bytes::budgets(self.database)?,
            crate::database::local_bytes::partials(self.database)?,
        ))
    }

    pub(crate) fn files_pinned(
        &self,
        files: &[coven_foundation::id_source::FileId],
    ) -> Result<Vec<Option<bool>>, DbError> {
        crate::cache_progress::files_pinned(self.database, files)
    }

    pub(crate) fn unsent(&self) -> Result<crate::UnsentWork, DbError> {
        crate::file_queue::unsent(self.database)
    }

    pub(crate) fn sync_facts<P: crate::PendingValue + Clone>(
        &self,
    ) -> Result<crate::SyncFacts<P>, DbError> {
        crate::sync_facts::read(self.database)
    }

    pub(crate) fn queue_facts<P: crate::PendingValue + Clone>(
        &self,
    ) -> Result<crate::QueueFacts<P>, DbError> {
        crate::sync_facts::read_queue(self.database)
    }

    /// `coven_sync_status`'s row, as coven reads it.
    #[cfg(test)]
    pub(crate) fn sync_status_row(&self) -> Result<crate::SyncStatusRow, DbError> {
        crate::internal_schema::sync_status::read(self.database)
    }

    pub(crate) fn upload_queue(&self) -> Result<crate::UploadQueueState, DbError> {
        crate::file_queue::state(self.database)
    }

    pub(crate) fn device_reset(
        &self,
    ) -> Result<Option<coven_foundation::files::DeviceReset>, DbError> {
        crate::device_reset::read(self.database)
    }

    pub(crate) fn store_name(&self) -> Result<String, DbError> {
        crate::store_state_tables::name(self.database)
    }

    pub(crate) fn named_store(&self) -> Result<(String, coven_merge::Slot), DbError> {
        crate::store_state_tables::named(self.database)
    }

    pub(crate) fn members(
        &self,
        device: coven_foundation::id_source::DeviceId,
    ) -> Result<Vec<crate::MemberInfo>, DbError> {
        crate::members::members(self.database, device)
    }

    pub(crate) fn circles(
        &self,
        device: coven_foundation::id_source::DeviceId,
    ) -> Result<Vec<crate::Circle>, DbError> {
        crate::members::circles(self.database, device)
    }

    pub(crate) fn circle_members(
        &self,
        device: coven_foundation::id_source::DeviceId,
        circle: coven_foundation::id_source::CircleId,
    ) -> Result<Vec<crate::CircleMemberInfo>, DbError> {
        crate::members::circle_members(self.database, device, circle)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn store_state(&self) -> Result<crate::StoreState, DbError> {
        crate::store_state_tables::read(self.database)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn retry_wait(
        &self,
        subject: crate::RetrySubject,
    ) -> Result<Option<crate::RetryWait>, DbError> {
        crate::retry::find(self.database, subject)
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
// the running slot, as RunningRead<R, E> for that closure.
enum ReadState<F> {
    Pending(F),
    Running(Box<dyn Any + Send>),
    Finished,
}

impl<'a, F, R, E, M: Mode> Read<'a, F, M>
where
    F: FnOnce(SqlReadContext<'_>) -> Result<R, E> + Send + 'static,
    R: Send + 'static,
    E: From<DbError> + CarriesBug + Send + 'static,
{
    pub(crate) fn new(database: &'a Database<M>, read: F) -> Self {
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
impl<F, M: Mode> Unpin for Read<'_, F, M> {}

impl<F, R, E, M: Mode> Future for Read<'_, F, M>
where
    F: FnOnce(SqlReadContext<'_>) -> Result<R, E> + Send + 'static,
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
/// `Read` that awaits it.
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

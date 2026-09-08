use std::future::{Future, IntoFuture};
use std::pin::Pin;

use coven_database::store::StoreReads;

use crate::{CovenError, CovenResult, SqlReadContext};

/// A database read that starts when awaited.
///
/// Constructed by [`CovenHandle::read`](crate::CovenHandle::read) or
/// [`CovenReadHandle::read`](crate::CovenReadHandle::read). Await it to receive
/// the fetched values, or attach [`process`](Self::process) to compute a result
/// on separate workers after releasing the database connection.
///
/// ```no_run
/// # async fn example(handle: &coven::CovenHandle) -> coven::CovenResult<()> {
/// let titles = handle.read(|sql| {
///     Ok(sql.query("SELECT body FROM notes", [], |row| row.get::<_, String>(0))?)
/// }).process(|mut titles| {
///     titles.sort();
///     Ok(titles)
/// }).await?;
/// # Ok(())
/// # }
/// ```
///
/// Use [`IntoFuture::into_future`] when passing the read to an API that requires
/// a [`Future`] rather than an awaitable value.
#[must_use = "reads do not execute until awaited"]
pub struct Read<'a, F> {
    database: &'a StoreReads,
    fetch: F,
}

impl<'a, F, Raw> Read<'a, F>
where
    F: for<'connection> FnOnce(SqlReadContext<'connection>) -> CovenResult<Raw> + Send + 'static,
    Raw: Send + 'static,
{
    pub(crate) fn new(database: &'a StoreReads, fetch: F) -> Self {
        Self { database, fetch }
    }

    /// Process the fetched values on bounded workers after the read transaction
    /// ends. Fetch every database input in the read closure; the processor
    /// receives owned values without a SQL context. A failed read skips it.
    pub async fn process<P, R>(self, process: P) -> CovenResult<R>
    where
        P: FnOnce(Raw) -> CovenResult<R> + Send + 'static,
        R: Send + 'static,
    {
        let database = self.database;
        let raw = self.await?;
        database.process(move || process(raw)).await
    }
}

impl<'a, F, R> IntoFuture for Read<'a, F>
where
    F: for<'connection> FnOnce(SqlReadContext<'connection>) -> CovenResult<R> + Send + 'static,
    R: Send + 'static,
{
    type Output = CovenResult<R>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            self.database
                .read(self.fetch)
                .await
                .map_err(CovenError::from)?
        })
    }
}

use std::sync::Arc;

use crate::{CovenError, CovenResult, SqlReadContext};
use coven_database::{QueryDependencies, StoreDatabase, StoreRowWrites};

type Query<T> =
    dyn for<'connection> Fn(SqlReadContext<'connection>) -> CovenResult<T> + Send + Sync;

/// A query over the store's reader that runs initially and whenever a committed
/// row change can affect its result.
///
/// Construct one with [`CovenHandle::subscribe`](crate::CovenHandle::subscribe),
/// then call [`next`](Self::next) for the initial value and each later value.
/// Coven records the tables and columns SQLite reads. For single-table queries
/// without subqueries, primary-key equality, `IN`, and range predicates also
/// record their bound key values; other SQL safely falls back to
/// table-and-column invalidation. Virtual-table dependencies return an error
/// because SQLite sessions do not report their row changes.
pub struct LiveQuery<T> {
    _writer: StoreRowWrites,
    reader: StoreDatabase,
    changes: tokio::sync::broadcast::Receiver<Arc<coven_database::CommittedChanges>>,
    dependencies: QueryDependencies,
    pending: bool,
    query: Arc<Query<T>>,
}

impl<T> LiveQuery<T>
where
    T: Send + 'static,
{
    pub(crate) fn new<F>(writer: StoreRowWrites, reader: StoreDatabase, query: F) -> Self
    where
        F: for<'connection> Fn(SqlReadContext<'connection>) -> CovenResult<T>
            + Send
            + Sync
            + 'static,
    {
        let changes = writer.subscribe_committed_changes();
        Self {
            _writer: writer,
            reader,
            changes,
            dependencies: QueryDependencies::unknown(),
            pending: true,
            query: Arc::new(query),
        }
    }

    /// Return the query's initial value, or wait for a committed change that can
    /// affect it and return the value after that commit.
    ///
    /// Query errors are values in the sequence; they do not end the
    /// subscription. Cancelling a `next` future does not consume a pending
    /// relevant change. If this subscriber falls behind the bounded change
    /// stream, it reruns conservatively.
    pub async fn next(&mut self) -> CovenResult<T> {
        while !self.pending {
            match self.changes.recv().await {
                Ok(changes) if self.dependencies.is_affected_by(&changes) => {
                    self.pending = true;
                }
                Ok(_) => {}
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    self.pending = true;
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    panic!("the live query retains its committed-change sender")
                }
            }
        }

        loop {
            match self.changes.try_recv() {
                Ok(_) | Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => {}
                Err(tokio::sync::broadcast::error::TryRecvError::Empty) => break,
                Err(tokio::sync::broadcast::error::TryRecvError::Closed) => {
                    panic!("the live query retains its committed-change sender")
                }
            }
        }

        let query = self.query.clone();
        let outcome = self
            .reader
            .read_tracked(move |sql| query(sql))
            .await
            .map_err(CovenError::from);
        match outcome {
            Ok((result, dependencies)) => {
                self.dependencies = dependencies;
                self.pending = false;
                result
            }
            Err(error) => {
                self.dependencies = QueryDependencies::unknown();
                self.pending = false;
                Err(error)
            }
        }
    }
}

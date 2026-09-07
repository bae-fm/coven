use super::{host_sql_reads::HostSqlReads, SqlReadContext};
use crate::{DbError, QueryDependencies};
use coven_foundation::bounded_workers::BoundedWorkers;
use rusqlite::{Connection, OpenFlags};
use std::{num::NonZeroUsize, path::Path};

const WORKERS: usize = 4;
const QUEUED: NonZeroUsize = NonZeroUsize::new(64).expect("positive queue capacity");

/// Application reads over owned read-only connections, with a separate bounded
/// executor for processing owned results after their transaction ends.
#[derive(Clone)]
pub struct StoreReads {
    connections: BoundedWorkers<Connection>,
    processing: BoundedWorkers<()>,
}

impl StoreReads {
    /// Open application readers after the store's schema has been validated.
    /// All connections open before any worker or read capability is exposed.
    pub fn open(path: &Path) -> Result<Self, DbError> {
        let connections = (0..WORKERS)
            .map(|_| {
                let connection = Connection::open_with_flags(
                    path,
                    OpenFlags::SQLITE_OPEN_READ_ONLY
                        | OpenFlags::SQLITE_OPEN_NO_MUTEX
                        | OpenFlags::SQLITE_OPEN_URI,
                )?;
                connection.pragma_update(None, "foreign_keys", "ON")?;
                Ok(connection)
            })
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(DbError::from)?;
        Ok(Self {
            connections: BoundedWorkers::start(connections, QUEUED, "coven-read")
                .map_err(|e| DbError::context("start read workers", e))?,
            processing: BoundedWorkers::start(vec![(); WORKERS], QUEUED, "coven-read-processing")
                .map_err(|e| DbError::context("start read processing workers", e))?,
        })
    }

    pub async fn read<F, R, E>(&self, read: F) -> Result<Result<R, E>, DbError>
    where
        F: for<'connection> FnOnce(SqlReadContext<'connection>) -> Result<R, E> + Send + 'static,
        R: Send + 'static,
        E: Send + 'static,
    {
        self.connections
            .call(move |connection| HostSqlReads::new(connection).read(read))
            .await
    }

    pub async fn read_tracked<F, R, E>(
        &self,
        read: F,
    ) -> Result<(Result<R, E>, QueryDependencies), DbError>
    where
        F: for<'connection> FnOnce(SqlReadContext<'connection>) -> Result<R, E> + Send + 'static,
        R: Send + 'static,
        E: Send + 'static,
    {
        self.connections
            .call(move |connection| HostSqlReads::new(connection).read_tracked(read))
            .await
    }

    pub async fn read_processed<F, P, Raw, R, E>(
        &self,
        read: F,
        process: P,
    ) -> Result<Result<R, E>, DbError>
    where
        F: for<'connection> FnOnce(SqlReadContext<'connection>) -> Result<Raw, E> + Send + 'static,
        P: FnOnce(Raw) -> Result<R, E> + Send + 'static,
        Raw: Send + 'static,
        R: Send + 'static,
        E: Send + 'static,
    {
        match self.read(read).await? {
            Ok(raw) => Ok(self.processing.call(move |()| process(raw)).await),
            Err(error) => Ok(Err(error)),
        }
    }

    pub async fn read_tracked_processed<F, P, Raw, R, E>(
        &self,
        read: F,
        process: P,
    ) -> Result<(Result<R, E>, QueryDependencies), DbError>
    where
        F: for<'connection> FnOnce(SqlReadContext<'connection>) -> Result<Raw, E> + Send + 'static,
        P: FnOnce(Raw) -> Result<R, E> + Send + 'static,
        Raw: Send + 'static,
        R: Send + 'static,
        E: Send + 'static,
    {
        let (result, dependencies) = self.read_tracked(read).await?;
        let result = match result {
            Ok(raw) => self.processing.call(move |()| process(raw)).await,
            Err(error) => Err(error),
        };
        Ok((result, dependencies))
    }
}

use crate::DbError;
use rusqlite::Connection;

/// One host read transaction and its connection-local authorization/tracking.
pub(super) struct HostSqlReads<'store> {
    conn: &'store Connection,
}

impl<'store> HostSqlReads<'store> {
    pub(super) fn new(conn: &'store Connection) -> Self {
        Self { conn }
    }
    pub(super) fn read<F, R, E>(self, read: F) -> Result<Result<R, E>, DbError>
    where
        F: for<'connection> FnOnce(super::SqlReadContext<'connection>) -> Result<R, E>,
    {
        self.transaction(|connection| {
            let authorization =
                super::host_sql_transaction::HostSqlAuthorization::begin(connection)?;
            Ok(authorization.run(|| read(super::SqlReadContext::new(connection))))
        })
    }

    pub(super) fn read_tracked<F, R, E>(
        self,
        read: F,
    ) -> Result<(Result<R, E>, crate::live_query::QueryDependencies), DbError>
    where
        F: for<'connection> FnOnce(super::SqlReadContext<'connection>) -> Result<R, E>,
    {
        self.transaction(|connection| {
            let dependencies = crate::live_query::ReadDependencyCapture::default();
            let authorization =
                super::host_sql_transaction::HostSqlAuthorization::begin_tracking_reads(
                    connection,
                    dependencies.clone(),
                )?;
            let outcome = authorization.run(|| {
                read(super::SqlReadContext::tracking(
                    connection,
                    dependencies.clone(),
                ))
            });
            Ok((outcome, dependencies.dependencies(connection)?))
        })
    }

    fn transaction<R>(
        self,
        operation: impl FnOnce(&Connection) -> Result<R, DbError>,
    ) -> Result<R, DbError> {
        let transaction = self.conn.unchecked_transaction().map_err(DbError::from)?;
        let outcome = operation(&transaction)?;
        transaction.rollback().map_err(DbError::from)?;
        Ok(outcome)
    }
}

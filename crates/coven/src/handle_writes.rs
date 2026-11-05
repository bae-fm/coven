//! Writes: the app's SQL and coven's steps as one write, and the app bytes
//! staged before one (§4, §17.2, E3).
use crate::coven_write::CovenWrite;
use crate::*;

impl CovenHandle {
    /// Runs one write: the app's SQL and coven's steps, in one transaction.
    /// It fails with the closure's own error type, which keeps a failed
    /// rollback or byte removal beside the write's failure
    /// (`WriteFailure`).
    pub async fn write<F, R, E>(&self, write: F) -> Result<R, E>
    where
        F: FnOnce(CovenWrite<'_>) -> Result<R, E> + Send + 'static,
        R: Send + 'static,
        E: WriteFailure + Send + 'static,
    {
        let (steps, files) = (self.steps.clone(), self.files.clone());
        self.database.write_as(move |sql| write(CovenWrite::new(sql, &steps, &files))).await
    }

    /// Runs one bulk write: SQL that changes device-local tables alone, run
    /// as plain SQLite with no record of each row change, so emptying a table
    /// takes one step and filling one runs at SQLite's own speed (§4). A
    /// statement that would write a synced table, itself or through a
    /// trigger or foreign-key action, fails the write with
    /// `SyncedTableInBulkWrite`. Triggers still run. After commit, every live
    /// query that read a table the write's statements may change runs again,
    /// whatever rows it read; a small change that should rerun only the live
    /// queries reading its rows belongs in `write`.
    pub async fn write_bulk<F, R, E>(&self, sql: F) -> Result<R, E>
    where
        F: FnOnce(BulkSqlContext<'_>) -> Result<R, E> + Send + 'static,
        R: Send + 'static,
        E: WriteFailure + Send + 'static,
    {
        self.database.write_bulk(sql).await
    }

    /// Stages app bytes for a write that puts them in rows (§17.2): `stage`
    /// stages each file and returns what that write needs, such as the ids
    /// staging gave them, and the write takes it with
    /// `CovenWrite::put_staged`. The bytes are streamed to coven's own copies,
    /// synced and flushed before this returns; close waits for staging and
    /// its cleanup, including cancellation. The staged files hold nothing
    /// close waits for: dropped uncommitted, or after the store closes, they
    /// are removed by the next write or open. An undeclared namespace fails
    /// with `UndeclaredNamespace`, a user one with `NamespaceKind`.
    pub async fn stage_files<F, T, E>(&self, stage: F) -> Result<StagedFiles<T>, E>
    where
        F: FnOnce(&mut WriteBatch<'_>) -> Result<T, E> + Send + 'static,
        T: Send + 'static,
        E: WriteFailure + Send + 'static,
    {
        self.database.stage_files(stage).await
    }
}

#[cfg(test)]
impl CovenHandle {
    /// Stage app bytes, then run a write that puts them in its rows, as an
    /// app does with `stage_files` and `put_staged`, for tests.
    pub(crate) async fn test_staged_write<F, S, T, R, E>(&self, stage: F, write: S) -> Result<R, E>
    where
        F: FnOnce(&mut WriteBatch<'_>) -> Result<T, E> + Send + 'static,
        S: FnOnce(CovenWrite<'_>, T) -> Result<R, E> + Send + 'static,
        T: Send + 'static,
        R: Send + 'static,
        E: WriteFailure + Send + 'static,
    {
        let staged = self.stage_files(stage).await?;
        self.write(move |sql| {
            let value = sql.put_staged(staged)?;
            write(sql, value)
        })
        .await
    }
}

#[cfg(test)]
#[path = "handle_writes_tests.rs"]
mod tests;

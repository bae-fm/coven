//! Writes: the app's SQL and coven's steps as one write, and the app bytes
//! staged before one (§4, §17.2, E3).
use crate::coven_write::CovenWrite;
use crate::*;

impl CovenHandle {
    /// Runs one write: the app's SQL and coven's steps, in one transaction.
    pub async fn write<F, R>(&self, write: F) -> CovenResult<R>
    where
        F: FnOnce(CovenWrite<'_>) -> CovenResult<R> + Send + 'static,
        R: Send + 'static,
    {
        let storage = self.storage.clone();
        self.database.write_as(move |sql| write(CovenWrite::new(sql, &storage))).await
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
    pub async fn write_bulk<F, R>(&self, sql: F) -> CovenResult<R>
    where
        F: FnOnce(BulkSqlContext<'_>) -> CovenResult<R> + Send + 'static,
        R: Send + 'static,
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
    pub async fn stage_files<F, T>(&self, stage: F) -> CovenResult<StagedFiles<T>>
    where
        F: FnOnce(&mut WriteBatch<'_>) -> CovenResult<T> + Send + 'static,
        T: Send + 'static,
    {
        self.database.stage_files(stage).await
    }
}

#[cfg(test)]
impl CovenHandle {
    /// Stage app bytes, then run a write that puts them in its rows, as an
    /// app does with `stage_files` and `put_staged`, for tests.
    pub(crate) async fn test_staged_write<F, S, T, R>(&self, stage: F, write: S) -> CovenResult<R>
    where
        F: FnOnce(&mut WriteBatch<'_>) -> CovenResult<T> + Send + 'static,
        S: FnOnce(CovenWrite<'_>, T) -> CovenResult<R> + Send + 'static,
        T: Send + 'static,
        R: Send + 'static,
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

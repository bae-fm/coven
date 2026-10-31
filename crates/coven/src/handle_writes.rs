//! Writes: the app's SQL, with the files it stages, as one write (§4, §17.2,
//! E3).
use crate::*;

impl CovenHandle {
    /// Runs one write.
    pub async fn write<F, R>(&self, sql: F) -> CovenResult<R>
    where
        F: FnOnce(SqlContext<'_, '_>) -> CovenResult<R> + Send + 'static,
        R: Send + 'static,
    {
        self.database
            .write_with_files(|_| Ok(()), move |context, ()| sql(context))
            .await
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

    /// Stage files, then run the write's SQL with what staging returned, as
    /// one write (§17.2). `stage` stages each file and returns what the SQL
    /// needs, such as the ids staging gave them; the SQL puts those ids in
    /// the rows that reference the files. App bytes are streamed to coven's
    /// own copy before the transaction's writer is taken; close waits for
    /// staging and its cleanup, including cancellation. The write fails with
    /// `FileUnknown` if a row references a file coven has no record of in the
    /// row's audience, `FileUnreferenced` if a staged file has no row, and
    /// `FileAbsent` or `NotThisDevice` if a new file over an existing file's
    /// bytes names a file coven has no record of, or one on another device
    /// only.
    pub async fn write_with_files<F, S, T, R>(&self, stage: F, sql: S) -> CovenResult<R>
    where
        F: FnOnce(&mut WriteBatch<'_>) -> CovenResult<T> + Send + 'static,
        S: FnOnce(SqlContext<'_, '_>, T) -> CovenResult<R> + Send + 'static,
        T: Send + 'static,
        R: Send + 'static,
    {
        self.database.write_with_files(stage, sql).await
    }
}

#[cfg(test)]
#[path = "handle_writes_tests.rs"]
mod tests;

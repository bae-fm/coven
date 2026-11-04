//! Transaction-scoped app SQL. The connection remains inside its owner.

use rusqlite::{Params, Row};

/// App SQL inside one local write, and coven's steps beside it (§4).
/// Returning an error rolls back every statement and step. This borrowed
/// context cannot change the transaction or access coven's tables.
pub struct SqlContext<'connection, 'write> {
    files: &'write crate::file_write::FileWrite<'connection>,
}

impl<'connection, 'write> SqlContext<'connection, 'write> {
    pub(crate) fn new(files: &'write crate::file_write::FileWrite<'connection>) -> Self {
        Self { files }
    }

    /// Record a move of these files of this device, on it only, to the
    /// cloud (§17.3). It survives restarts; once every file is stored, one
    /// write switches them all to the cloud, changing no app row.
    pub fn move_to_cloud(
        &self,
        files: Vec<crate::MoveFile>,
    ) -> Result<crate::MoveId, crate::DbError> {
        self.files.step(|| self.files.move_to_cloud(files))
    }

    /// Cancel a recorded move: every file stays on this device only, and
    /// the move's record ends (§17.3).
    pub fn cancel_move(&self, id: crate::MoveId) -> Result<(), crate::DbError> {
        self.files.step(|| self.files.cancel_move(id))
    }

    /// Release the pins on these files' stored bytes and evict each of their
    /// namespaces down to its budget, in this write (§17.8). A file this
    /// device has no record of, or one on one device only, has no pin to
    /// release. The evicted bytes go once the write has committed.
    pub fn unpin(
        &self,
        files: &[coven_foundation::id_source::FileId],
    ) -> Result<(), crate::DbError> {
        self.files.step(|| self.files.unpin(files))
    }

    /// Remove a stored file's cached copies, pinned or not, in this write,
    /// with no storage operation (§17.8). A file this device has no record
    /// of, or one on one device only, has none. The bytes go once the write
    /// has committed.
    pub fn evict_file(
        &self,
        file: coven_foundation::id_source::FileId,
    ) -> Result<(), crate::DbError> {
        self.files.step(|| self.files.evict(file))
    }

    /// Set a declared namespace's cache budget and evict its unpinned
    /// entries down to it, in this write (§17.8). An undeclared namespace
    /// fails with `UndeclaredNamespace`. The evicted bytes go once the write
    /// has committed.
    pub fn set_cache_budget(&self, namespace: &str, bytes: u64) -> Result<(), crate::DbError> {
        self.files.step(|| self.files.set_cache_budget(namespace, bytes))
    }

    /// Dismiss lost values the app has dealt with, in this write (§8.4): its
    /// record carries the acknowledgements and the removed rows' deletes,
    /// with their cascades, after the app's own changes, so every device
    /// drops them. A removed row is deleted for good. A value no longer lost
    /// as it was, such as a row restored since, is left alone. The write
    /// fails with `DismissedRowChanged` if it also changes a row its
    /// dismissals delete.
    pub fn dismiss_lost_values(&self, values: &[crate::LostValue]) -> Result<(), crate::DbError> {
        self.files.step(|| {
            self.files.dismiss(values);
            Ok(())
        })
    }

    /// Pause trailing uploads, or resume them, in this write (§17.9). A
    /// paused upload keeps its place and its provider session.
    pub fn set_uploads_paused(&self, paused: bool) -> Result<(), crate::DbError> {
        self.files.step(|| self.files.set_uploads_paused(paused))
    }

    /// Move files only on unmetered networks, or on any, in this write
    /// (§17.9).
    pub fn set_unmetered_only(&self, unmetered_only: bool) -> Result<(), crate::DbError> {
        self.files.step(|| self.files.set_unmetered_only(unmetered_only))
    }

    /// Run `effect` once this write has committed, as the in-memory side of
    /// one of coven's steps: nothing runs for a write that rolls back. The
    /// write returns only once every effect has run.
    pub fn after_commit(&self, effect: impl FnOnce() + 'static) {
        self.files.after_commit(Box::new(effect));
    }

    /// Put files `stage_files` staged into this write, and return what its
    /// batch returned, such as their ids, for this write's rows to name
    /// (§17.2). Files staged by another open store, or by this one before it
    /// closed, fail with `PreparedElsewhere`. A file no row names fails the
    /// write with `FileUnreferenced`.
    pub fn put_staged<T>(&self, staged: crate::StagedFiles<T>) -> Result<T, crate::DbError> {
        self.files.step(|| self.files.put_staged(staged))
    }

    /// Stage a user's original, read whole once already, as a new file in a
    /// user namespace, and return its id for a row to reference (§17.1,
    /// §17.3). Coven records the original's path, size, modification time and
    /// the hashes its read found; it never reads it here. An undeclared
    /// namespace fails with `UndeclaredNamespace`, an app one with
    /// `NamespaceKind`.
    pub fn put_user_file(
        &self,
        namespace: &str,
        original: crate::PreparedUserFile,
        place: crate::Place,
    ) -> Result<coven_foundation::id_source::FileId, crate::DbError> {
        self.files.step(|| self.files.put_user_file(namespace, original, place))
    }

    /// Stage a new file over an existing file's bytes, for a row in any
    /// audience, and return its id (§17.2). The write fails with
    /// `FileAbsent` if this device has no record of `from`, and with
    /// `NotThisDevice` if `from` is on another device only.
    pub fn existing_file(
        &self,
        from: coven_foundation::id_source::FileId,
    ) -> coven_foundation::id_source::FileId {
        self.files.existing_file(from)
    }

    /// Pin a prepared pin's files in this write (§17.8): each whole copy
    /// becomes the file's pinned cache entry, and each copy already kept is
    /// pinned. Files prepared in another open store, or in this one before
    /// it closed, fail with `PreparedElsewhere`; a file whose record left
    /// its stored bytes since with `NoLongerStored`; and a kept copy evicted
    /// since with `PinChanged`.
    pub fn pin(&self, prepared: crate::PreparedPin) -> Result<(), crate::DbError> {
        self.files.step(|| self.files.pin(prepared))
    }

    /// End the request for the pin coven keeps for `file`, in this write
    /// (§17.8).
    pub(crate) fn end_pin_request(
        &self,
        file: coven_foundation::id_source::FileId,
    ) -> Result<(), crate::DbError> {
        self.files.step(|| self.files.end_pin_request(file))
    }

    /// Switch a prepared call's files to device-only on this device, each to
    /// its copy, which becomes the original this device records, in this
    /// write (§17.3). Each must still be one this device may make
    /// device-only, and still stored where its copy came from
    /// (`NotInCloud`); copies prepared in another open store, or in this
    /// one before it closed, fail with `PreparedElsewhere`. A write that
    /// also removes such a file's last reference fails with
    /// `DeviceOnlyUnreferenced`.
    pub fn make_device_only(
        &self,
        prepared: crate::PreparedDeviceOnly,
    ) -> Result<(), crate::DbError> {
        self.files.step(|| self.files.make_prepared_device_only(prepared))
    }

    /// Acknowledge a finished operation the app's writes journaled, in this
    /// write: it leaves `coven_operations` (§11.4). One still running fails
    /// with `OperationRunning`; one not listed there with
    /// `OperationUnknown`.
    pub fn acknowledge_operation(
        &self,
        operation: crate::OperationId,
    ) -> Result<(), crate::DbError> {
        self.journal_step(|journal| journal.acknowledge(operation))
    }

    /// Run one of sync's steps on the journal in this write (§4, §11). A
    /// failed step fails the write, even if the closure goes on and returns
    /// success, since it may have made only part of its change.
    pub fn journal_step<T, E>(
        &self,
        step: impl FnOnce(&crate::Journal<'_>) -> Result<T, E>,
    ) -> Result<T, E> {
        self.files.step(|| step(&crate::Journal::new(self.files)))
    }

    /// Execute one app statement with parameters.
    pub fn execute<P: Params>(&self, sql: &str, params: P) -> rusqlite::Result<usize> {
        self.files.execute(sql, params)
    }

    /// Execute app statements within this write's transaction.
    pub fn execute_batch(&self, sql: &str) -> rusqlite::Result<()> {
        self.files.execute_batch(sql)
    }

    /// Map one app row, including changes already made by this write.
    pub fn query_row<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<T>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> rusqlite::Result<T>,
    {
        self.files.query_row(sql, params, map)
    }

    /// Map all app rows produced by a query within this write.
    pub fn query<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> rusqlite::Result<T>,
    {
        self.files.query(sql, params, map)
    }
}

/// App SQL inside one bulk write (§4): it changes device-local tables
/// alone, and may read any table app SQL reads. Returning an error rolls back
/// every statement. This borrowed context cannot change the transaction,
/// access coven's tables or write a synced one.
pub struct BulkSqlContext<'connection> {
    database: &'connection crate::sqlite::DatabaseConnection,
}

impl<'connection> BulkSqlContext<'connection> {
    pub(crate) fn new(database: &'connection crate::sqlite::DatabaseConnection) -> Self {
        Self { database }
    }

    /// Execute one app statement with parameters.
    pub fn execute<P: Params>(&self, sql: &str, params: P) -> rusqlite::Result<usize> {
        self.database.app_execute(sql, params)
    }

    /// Execute app statements within this write's transaction.
    pub fn execute_batch(&self, sql: &str) -> rusqlite::Result<()> {
        self.database.app_batch(sql)
    }

    /// Map one app row, including changes already made by this write.
    pub fn query_row<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<T>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> rusqlite::Result<T>,
    {
        self.database.app_query_row(sql, params, map)
    }

    /// Map all app rows produced by a query within this write.
    pub fn query<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> rusqlite::Result<T>,
    {
        self.database.app_query(sql, params, map)
    }
}

#[cfg(test)]
#[path = "write_tests.rs"]
pub(crate) mod tests;

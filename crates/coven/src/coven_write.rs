//! The app's write: its SQL and coven's steps in one transaction (§4, E3).

use crate::*;
use coven_database::SqlContext;

/// One write's SQL and coven's steps, in one transaction (§4, E3). A step
/// runs where it is called: the write's later SQL sees what it changed, and
/// it commits or rolls back with the app's statements. A failed step fails
/// the write, even if the closure goes on and returns success. Returning an
/// error rolls back every statement and step.
pub struct CovenWrite<'write> {
    sql: SqlContext<'write, 'write>,
    /// The store's operation owner, whose steps run in this write.
    storage: &'write crate::storage::StorageConnections,
}

impl<'write> CovenWrite<'write> {
    pub(crate) fn new(
        sql: SqlContext<'write, 'write>,
        storage: &'write crate::storage::StorageConnections,
    ) -> Self {
        Self { sql, storage }
    }

    /// Puts files `CovenHandle::stage_files` staged into this write, and
    /// returns what its batch returned, such as their ids, for this write's
    /// rows to reference (§17.2). The write makes each file's record, in the
    /// audience of the rows that name it, and queues its upload for the
    /// cloud. Files staged by another open store, or by this one before it
    /// closed, fail with `PreparedElsewhere`; a file no row names fails the
    /// write with `FileUnreferenced`.
    pub fn put_staged<T>(&self, staged: StagedFiles<T>) -> Result<T, DbError> {
        self.sql.put_staged(staged)
    }

    /// Stages a user's original, read whole once already, as a new file in
    /// a user namespace and returns its id for a row to reference: on this
    /// device only, or for the cloud, with its upload queued. Coven records
    /// the original's path, size, modification time and the hashes its read
    /// found, which its upload checks, now or when the file moves to the
    /// cloud; it never changes the original, nor reads it here (§17.1,
    /// §17.3). An undeclared namespace fails with `UndeclaredNamespace`, an
    /// app one with `NamespaceKind`.
    pub fn put_user_file(
        &self,
        namespace: &str,
        original: PreparedUserFile,
        place: Place,
    ) -> Result<FileId, DbError> {
        self.sql.put_user_file(namespace, original, place)
    }

    /// Stages a new file over an existing file's bytes, for a row in any
    /// audience, and returns its id for that row to reference. The write
    /// makes it in the audience of the rows that reference it, with
    /// `from`'s namespace, size, place and key; nothing is uploaded again. A
    /// file on this device only gives a new file on this device only, over
    /// the same original or coven's copy. The write fails with `FileAbsent`
    /// if this device has no record of `from`, and with `NotThisDevice` if
    /// `from` is on another device only (§17.2).
    pub fn existing_file(&self, from: FileId) -> FileId {
        self.sql.existing_file(from)
    }

    /// Records a move of these files to the cloud (§17.3).
    pub fn move_to_cloud(&self, files: Vec<MoveFile>) -> Result<MoveId, DbError> {
        self.sql.move_to_cloud(files)
    }

    /// Cancels a move: every file stays on this device only (§17.3).
    pub fn cancel_move(&self, id: MoveId) -> Result<(), DbError> {
        self.sql.cancel_move(id)
    }

    /// Releases the pins on these files' stored bytes, then evicts each of
    /// their namespaces down to its budget (§17.8). A file this device has
    /// no record of, or one on one device only, has no pin to release. A
    /// pin coven keeps for a switch to the cloud stays (§17.8). The evicted
    /// bytes go once the write has committed.
    pub fn unpin(&self, files: &[FileId]) -> Result<(), DbError> {
        self.sql.unpin(files)
    }

    /// Removes a stored file's cached copies, pinned or not, without
    /// changing storage or a local original (§17.8). A file this device has
    /// no record of, or one on one device only, has none. The bytes go once
    /// the write has committed.
    pub fn evict_file(&self, file: FileId) -> Result<(), DbError> {
        self.sql.evict_file(file)
    }

    /// Sets a declared namespace's cache budget and evicts its unpinned
    /// files and chunks, least recently read first, down to it (§17.8). An
    /// undeclared namespace fails with `UndeclaredNamespace`. The evicted
    /// bytes go once the write has committed.
    pub fn set_cache_budget(&self, namespace: &str, max_bytes: u64) -> Result<(), DbError> {
        self.sql.set_cache_budget(namespace, max_bytes)
    }

    /// Pauses trailing uploads, or resumes them (§17.9). A paused upload
    /// keeps its place, including a provider upload session in progress,
    /// and stops between confirmed parts; the setting persists. Bound
    /// uploads run on, since writes wait for them (§17.4).
    pub fn set_uploads_paused(&self, paused: bool) -> Result<(), DbError> {
        self.storage.set_uploads_paused(&self.sql, paused)
    }

    /// Dismisses lost values the app has dealt with (§8.4): the write's
    /// record carries the acknowledgements, and the removed rows' deletes
    /// with their cascades, after the app's own changes, so every device
    /// drops them from `_coven_lost`. A removed row is deleted for good and
    /// never comes back. A value no longer lost as it was is left alone.
    /// Setting a value and dismissing its loss in one write keeps the two
    /// together; a write that also changes a row its dismissals delete fails
    /// with `DismissedRowChanged`.
    pub fn dismiss_lost_values(&self, values: &[LostValue]) -> Result<(), DbError> {
        self.sql.dismiss_lost_values(values)
    }

    /// Keeps a prepared pin's files whole on this device, regardless of the
    /// cache budget (§17.8): each whole copy becomes the file's pinned cache
    /// entry, and each copy already kept is pinned, all in this write.
    /// Files prepared in another open store, or in this one before it
    /// closed, fail with `PreparedElsewhere`; a file whose record left its
    /// stored bytes since with `NoLongerStored`; and a copy already kept
    /// that was evicted since with `PinChanged`, which preparing again
    /// downloads.
    pub fn pin(&self, prepared: PreparedPin) -> Result<(), DbError> {
        self.sql.pin(prepared)
    }

    /// Makes an operation's retry or undo due now, from its recorded
    /// progress (E6): its pending record and retry wait end in this write.
    /// It fails with `NotPending` for an operation that has finished or
    /// never existed, and with `Pending` and the record for one whose
    /// record never retries, or only after an update not installed yet. It
    /// can't bypass a provider cooldown. Once the write commits, a pass runs
    /// the operation as soon as storage is connected.
    pub fn retry_pending_operation(&self, operation: OperationId) -> Result<(), OperationError> {
        self.storage.retry_pending_operation(&self.sql, operation)
    }

    /// Execute one app statement with parameters.
    pub fn execute<P: Params>(&self, sql: &str, params: P) -> SqliteResult<usize> {
        self.sql.execute(sql, params)
    }

    /// Execute app statements within this write's transaction.
    pub fn execute_batch(&self, sql: &str) -> SqliteResult<()> {
        self.sql.execute_batch(sql)
    }

    /// Map one app row, including changes already made by this write.
    pub fn query_row<T, P, F>(&self, sql: &str, params: P, map: F) -> SqliteResult<T>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> SqliteResult<T>,
    {
        self.sql.query_row(sql, params, map)
    }

    /// Map all app rows produced by a query within this write.
    pub fn query<T, P, F>(&self, sql: &str, params: P, map: F) -> SqliteResult<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> SqliteResult<T>,
    {
        self.sql.query(sql, params, map)
    }
}

impl SqlRead for CovenWrite<'_> {
    fn query_row<T, P, F>(&self, sql: &str, params: P, map: F) -> SqliteResult<T>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> SqliteResult<T>,
    {
        CovenWrite::query_row(self, sql, params, map)
    }

    fn query<T, P, F>(&self, sql: &str, params: P, map: F) -> SqliteResult<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> SqliteResult<T>,
    {
        CovenWrite::query(self, sql, params, map)
    }
}

#[cfg(test)]
#[path = "coven_write_tests.rs"]
mod tests;

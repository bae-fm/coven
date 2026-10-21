//! Pin progress as a query over committed cache coverage (§17.6, E8).
//! Nothing here counts bytes in flight.

use crate::{
    file_queue, sqlite::DatabaseConnection, write_schema::WriteSchema, CovenResult, Database,
    DbError, FileRef, LiveQuery, RowKey,
};
use coven_foundation::id_source::DeviceId;

/// Verified cache coverage of requested files (§17.6, E8).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PinProgress {
    /// Files whose bytes have all been kept.
    pub files_completed: u64,
    /// Files requested, including those already present.
    pub files_total: u64,
    /// Verified plaintext bytes present in the committed cache for the
    /// requested files.
    pub bytes_cached: u64,
    /// Total plaintext size of the requested files, including cached bytes.
    pub bytes_total: u64,
}

/// Verified plaintext bytes of `file` the cache holds: a checked whole file,
/// or each authenticated chunk without its tag. A file this device still has
/// queued is wholly local.
#[expect(
    clippy::map_err_ignore,
    reason = "a stored value that won't convert or decode is DamagedDatabase"
)]
pub(crate) fn covered(
    db: &DatabaseConnection,
    file: &FileRef,
    device: DeviceId,
) -> Result<u64, DbError> {
    if file.uploader() == device && file_queue::queued(db, file)? {
        return Ok(file.plaintext_size());
    }
    let id = crate::database::file_database::cache_id(file)?;
    if db.query_row(
        "SELECT EXISTS(SELECT 1 FROM _coven_cache
         WHERE namespace=?1 AND file_id=?2 AND chunk=?3)",
        (
            file.namespace(),
            &id,
            crate::internal_schema::CACHE_COMPLETE_FILE,
        ),
        |r| r.get::<_, bool>(0),
    )? {
        return Ok(file.plaintext_size());
    }
    let cached = db.query_row(
        "SELECT coalesce(sum(size-chunks*?3),0) FROM _coven_cache
         WHERE namespace=?1 AND file_id=?2 AND chunk>=0",
        (
            file.namespace(),
            &id,
            coven_crypto::FILE_CHUNK_TAG_LEN as i64,
        ),
        |r| r.get::<_, i64>(0),
    )?;
    let cached = u64::try_from(cached).map_err(|_| DbError::DamagedDatabase)?;
    if cached > file.plaintext_size() {
        return Err(DbError::DamagedDatabase);
    }
    Ok(cached)
}

/// Requested files' verified coverage. A pin completes with its checked
/// whole file; a file still queued here is already whole on this device.
pub(crate) fn pin_progress(
    db: &DatabaseConnection,
    files: &[FileRef],
    device: DeviceId,
) -> Result<PinProgress, DbError> {
    let mut progress = PinProgress {
        files_completed: 0,
        files_total: files.len() as u64,
        bytes_cached: 0,
        bytes_total: 0,
    };
    let too_large = || DbError::TooLarge {
        field: "requested file bytes",
        actual: u64::MAX,
        maximum: u64::MAX,
    };
    for file in files {
        let cached = covered(db, file, device)?;
        progress.bytes_cached = progress
            .bytes_cached
            .checked_add(cached)
            .ok_or_else(too_large)?;
        progress.bytes_total = progress
            .bytes_total
            .checked_add(file.plaintext_size())
            .ok_or_else(too_large)?;
        if file.uploader() == device && file_queue::queued(db, file)?
            || crate::database::file_database::cache_pinned(db, file)?
        {
            progress.files_completed += 1;
        }
    }
    Ok(progress)
}

/// Pin answers in key order; a key with no row carrying a file is `None`.
pub(crate) fn rows_pinned(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    table: &str,
    keys: &[RowKey],
) -> Result<Vec<Option<bool>>, DbError> {
    keys.iter()
        .map(|key| match crate::file_ref::read(db, schema, table, key) {
            Ok(file) => Ok(Some(crate::database::file_database::cache_pinned(
                db, &file,
            )?)),
            Err(DbError::FileAbsent { .. }) => Ok(None),
            Err(error) => Err(error),
        })
        .collect()
}

impl Database {
    /// Verified cache coverage for these fixed references, in one snapshot.
    pub async fn pin_progress(&self, files: Vec<FileRef>) -> CovenResult<PinProgress> {
        let device = self.local_device()?;
        self.read(move |sql| sql.pin_progress(&files, device)).await
    }
    /// The same pre-built cache query with the same fixed references.
    pub fn subscribe_pin_progress(&self, files: Vec<FileRef>) -> LiveQuery<PinProgress> {
        let device = self.local_device().ok();
        self.subscribe(move |sql| sql.pin_progress(&files, device.ok_or(DbError::StoreClosed)?))
    }
    /// Whether each row's file is pinned, one answer per key in order.
    pub async fn rows_pinned(
        &self,
        table: &str,
        keys: Vec<RowKey>,
    ) -> CovenResult<Vec<Option<bool>>> {
        let table = table.to_owned();
        self.read_schema(move |schema, sql| sql.rows_pinned(schema, &table, &keys))
            .await
    }
    /// The same internal query, live.
    pub fn subscribe_rows_pinned(
        &self,
        table: &str,
        keys: Vec<RowKey>,
    ) -> LiveQuery<Vec<Option<bool>>> {
        let table = table.to_owned();
        LiveQuery::new(self.subscribe_schema((), move |(), schema, sql| {
            sql.rows_pinned(schema, &table, &keys)
        }))
    }
}

#[cfg(test)]
#[path = "cache_progress_tests.rs"]
mod tests;

//! Pin progress as a query over committed cache coverage (§17.8, E8).
//! Nothing here counts bytes in flight.

use crate::{sqlite::DatabaseConnection, CovenResult, Database, DbError, FileRef, LiveQuery};
use coven_foundation::id_source::{DeviceId, FileId};

/// Verified cache coverage of requested files (§17.8, E8).
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
/// or each authenticated chunk without its tag. A file only on this device
/// is wholly local; one only on another device has none here.
#[expect(
    clippy::map_err_ignore,
    reason = "a stored value that won't convert or decode is DamagedDatabase"
)]
pub(crate) fn covered(
    db: &DatabaseConnection,
    file: &FileRef,
    device: DeviceId,
) -> Result<u64, DbError> {
    match file.place() {
        crate::FilePlace::DeviceOnly { device: on } if on == device => return Ok(file.size()),
        crate::FilePlace::DeviceOnly { .. } => return Ok(0),
        crate::FilePlace::Cloud { .. } => {}
    }
    let id = crate::database::file_database::cache_id(file)?;
    if db.query_row(
        "SELECT EXISTS(SELECT 1 FROM _coven_cache
         WHERE namespace=?1 AND file_id=?2 AND chunk=?3)",
        (file.namespace(), &id, crate::internal_schema::CACHE_COMPLETE_FILE),
        |r| r.get::<_, bool>(0),
    )? {
        return Ok(file.size());
    }
    let cached = db.query_row(
        "SELECT coalesce(sum(size-chunks*?3),0) FROM _coven_cache
         WHERE namespace=?1 AND file_id=?2 AND chunk>=0",
        (file.namespace(), &id, coven_crypto::FILE_CHUNK_TAG_LEN as i64),
        |r| r.get::<_, i64>(0),
    )?;
    let cached = u64::try_from(cached).map_err(|_| DbError::DamagedDatabase)?;
    if cached > file.size() {
        return Err(DbError::DamagedDatabase);
    }
    Ok(cached)
}

/// Requested files' verified coverage. A pin completes with its checked
/// whole file; a file only on this device is already whole here.
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
    let too_large =
        || DbError::TooLarge { field: "requested file bytes", actual: u64::MAX, maximum: u64::MAX };
    for file in files {
        let cached = covered(db, file, device)?;
        progress.bytes_cached = progress.bytes_cached.checked_add(cached).ok_or_else(too_large)?;
        progress.bytes_total =
            progress.bytes_total.checked_add(file.size()).ok_or_else(too_large)?;
        let whole = match file.place() {
            crate::FilePlace::DeviceOnly { device: on } => on == device,
            crate::FilePlace::Cloud { .. } => crate::database::file_database::cache_kept(db, file)?,
        };
        if whole {
            progress.files_completed += 1;
        }
    }
    Ok(progress)
}

/// Pin answers in order; a file this device has no record of is `None`.
/// The answer is whether `coven_pins` lists the file, so the app's SQL and
/// this call agree. It reads no merge state, so a live query of it reruns
/// only for those tables' changes.
pub(crate) fn files_pinned(
    db: &DatabaseConnection,
    files: &[FileId],
) -> Result<Vec<Option<bool>>, DbError> {
    files
        .iter()
        .map(|file| {
            let (known, pinned): (bool, bool) = db.query_row(
                "SELECT EXISTS(SELECT 1 FROM main.coven_files WHERE id=?1),
                        EXISTS(SELECT 1 FROM coven_pins WHERE file=?1)",
                [file],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            Ok(known.then_some(pinned))
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
    /// Whether each file is pinned, one answer per file in order.
    pub async fn files_pinned(&self, files: Vec<FileId>) -> CovenResult<Vec<Option<bool>>> {
        self.read(move |sql| sql.files_pinned(&files)).await
    }
    /// The same internal query, live.
    pub fn subscribe_files_pinned(&self, files: Vec<FileId>) -> LiveQuery<Vec<Option<bool>>> {
        self.subscribe(move |sql| sql.files_pinned(&files))
    }
}

#[cfg(test)]
#[path = "cache_progress_tests.rs"]
mod tests;

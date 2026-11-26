//! Pin progress as a query over committed cache coverage (§17.8, E8).
//! Nothing here counts bytes in flight.

use crate::sqlite::StoreSql;
use crate::{DbError, FileRef, LiveQuery, StoreDatabase};
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

/// What the cache holds of one stored file.
struct Held {
    /// Whether it holds a whole copy.
    complete: bool,
    /// The bytes of its authenticated chunks, without their tags.
    chunks: i64,
    /// Whether the whole copy is the one the file's content hash names.
    kept: bool,
}

/// What the cache holds of each of `files`, stored files with their content
/// hashes, in order: one statement reads the files as a JSON array.
fn held(
    db: &StoreSql,
    files: &[(&FileRef, &coven_crypto::ContentHash)],
) -> Result<Vec<Held>, DbError> {
    let listed = files
        .iter()
        .map(|(file, content_hash)| {
            let hash: String = content_hash.as_bytes().iter().map(|b| format!("{b:02X}")).collect();
            Ok(serde_json::json!({
                "namespace": file.namespace(),
                "object": crate::database::file_database::object_path_of(file)?,
                "hash": hash,
            }))
        })
        .collect::<Result<Vec<_>, DbError>>()?;
    let listed = serde_json::to_string(&listed)
        .map_err(|_unencodable| DbError::Invariant { what: "cache keys encode as JSON" })?;
    Ok(db.host_live_read(
        "SELECT EXISTS(SELECT 1 FROM _coven_cache
                       WHERE namespace=json_extract(j.value,'$.namespace')
                         AND object_path=json_extract(j.value,'$.object') AND chunk=?3),
                coalesce((SELECT sum(size-chunks*?2) FROM _coven_cache
                          WHERE namespace=json_extract(j.value,'$.namespace')
                            AND object_path=json_extract(j.value,'$.object') AND chunk>=0),0),
                EXISTS(SELECT 1 FROM _coven_cache
                       WHERE namespace=json_extract(j.value,'$.namespace')
                         AND object_path=json_extract(j.value,'$.object') AND chunk=?3
                         AND checked_hash=unhex(json_extract(j.value,'$.hash')))
         FROM json_each(?1) AS j ORDER BY j.key",
        (
            listed,
            coven_crypto::FILE_CHUNK_TAG_LEN as i64,
            crate::internal_schema::CACHE_COMPLETE_FILE,
        ),
        |r| Ok(Held { complete: r.get(0)?, chunks: r.get(1)?, kept: r.get(2)? }),
    )?)
}

/// Verified plaintext bytes of the stored file `file` the cache holds, given
/// what it holds: a checked whole file, or each authenticated chunk without
/// its tag.
#[expect(
    clippy::map_err_ignore,
    reason = "a stored value that won't convert or decode is DamagedDatabase"
)]
fn covered_by(held: &Held, file: &FileRef) -> Result<u64, DbError> {
    if held.complete {
        return Ok(file.size());
    }
    let cached = u64::try_from(held.chunks).map_err(|_| DbError::DamagedDatabase)?;
    if cached > file.size() {
        return Err(DbError::DamagedDatabase);
    }
    Ok(cached)
}

/// Verified plaintext bytes of `file` the cache holds: a checked whole file,
/// or each authenticated chunk without its tag. A file only on this device
/// is wholly local; one only on another device has none here.
#[cfg(any(test, feature = "test-utils"))]
pub(crate) fn covered(db: &StoreSql, file: &FileRef, device: DeviceId) -> Result<u64, DbError> {
    match file.place() {
        crate::FilePlace::DeviceOnly { device: on } if on == device => Ok(file.size()),
        crate::FilePlace::DeviceOnly { .. } => Ok(0),
        crate::FilePlace::Cloud { content_hash, .. } => {
            let held = held(db, &[(file, content_hash)])?.pop().ok_or(DbError::Invariant {
                what: "the statement returns a row for each stored file",
            })?;
            covered_by(&held, file)
        }
    }
}

/// Requested files' verified coverage, from one statement over the stored
/// files. A pin completes with its checked whole file; a file only on this
/// device is already whole here.
pub(crate) fn pin_progress(
    db: &StoreSql,
    files: &[FileRef],
    device: DeviceId,
) -> Result<PinProgress, DbError> {
    let stored: Vec<(&FileRef, &coven_crypto::ContentHash)> = files
        .iter()
        .filter_map(|file| match file.place() {
            crate::FilePlace::Cloud { content_hash, .. } => Some((file, content_hash)),
            crate::FilePlace::DeviceOnly { .. } => None,
        })
        .collect();
    let mut held = held(db, &stored)?.into_iter();
    let mut progress = PinProgress {
        files_completed: 0,
        files_total: files.len() as u64,
        bytes_cached: 0,
        bytes_total: 0,
    };
    for file in files {
        let (cached, whole) = match file.place() {
            crate::FilePlace::DeviceOnly { device: on } if on == device => (file.size(), true),
            crate::FilePlace::DeviceOnly { .. } => (0, false),
            crate::FilePlace::Cloud { .. } => {
                let held = held.next().ok_or(DbError::Invariant {
                    what: "the statement returns a row for each stored file",
                })?;
                (covered_by(&held, file)?, held.kept)
            }
        };
        progress.bytes_cached =
            crate::sqlite::add_total("requested file bytes cached", progress.bytes_cached, cached)?;
        progress.bytes_total =
            crate::sqlite::add_total("requested file bytes", progress.bytes_total, file.size())?;
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
pub(crate) fn files_pinned(db: &StoreSql, files: &[FileId]) -> Result<Vec<Option<bool>>, DbError> {
    // The requested files as a JSON array of their canonical texts, which one
    // statement reads a row of for each element, repeats included.
    let listed =
        serde_json::to_string(&files.iter().map(|file| file.0.to_string()).collect::<Vec<_>>())
            .map_err(|_unencodable| DbError::Invariant { what: "file ids encode as JSON" })?;
    Ok(db.host_live_read(
        "SELECT EXISTS(SELECT 1 FROM main.coven_files WHERE id=j.value),
                EXISTS(SELECT 1 FROM coven_pins WHERE file=j.value)
         FROM json_each(?1) AS j ORDER BY j.key",
        [listed],
        |r| r.get::<_, bool>(0)?.then(|| r.get::<_, bool>(1)).transpose(),
    )?)
}

impl StoreDatabase {
    /// Verified cache coverage for these fixed references, in one snapshot.
    pub async fn pin_progress(&self, files: Vec<FileRef>) -> Result<PinProgress, DbError> {
        let device = self.local_device()?;
        self.read_committed(move |db| pin_progress(db, &files, device)).await
    }
    /// The same pre-built cache query with the same fixed references.
    pub fn subscribe_pin_progress<E>(&self, files: Vec<FileRef>) -> LiveQuery<PinProgress, E>
    where
        E: From<DbError> + coven_foundation::task::CarriesBug + Send + 'static,
    {
        let device = self.local_device().ok();
        self.follow(move |db| {
            pin_progress(db, &files, device.ok_or(DbError::StoreClosed)?).map_err(E::from)
        })
    }
    /// Whether each file is pinned, one answer per file in order.
    pub async fn files_pinned(&self, files: Vec<FileId>) -> Result<Vec<Option<bool>>, DbError> {
        self.read_committed(move |db| files_pinned(db, &files)).await
    }
    /// The same internal query, live.
    pub fn subscribe_files_pinned<E>(&self, files: Vec<FileId>) -> LiveQuery<Vec<Option<bool>>, E>
    where
        E: From<DbError> + coven_foundation::task::CarriesBug + Send + 'static,
    {
        self.follow(move |db| files_pinned(db, &files).map_err(E::from))
    }
}

#[cfg(test)]
#[path = "cache_progress_tests.rs"]
mod tests;

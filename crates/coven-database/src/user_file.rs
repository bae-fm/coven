//! Preparing an original does no database work and never changes the file.

use crate::file_hashes::{FileHasher, FileHashes};
use crate::sqlite::StoreSql;
use crate::DbError;
use coven_foundation::files::{FileLease, ObservationError, UserOriginal};
use std::{
    path::{Path, PathBuf},
    time::SystemTime,
};

/// An original read whole once before entering a write: its path, size and
/// modification time, and the content hash and chunk hashes of the bytes
/// that read found, which coven records and checks the original against.
#[derive(Debug)]
pub struct PreparedUserFile {
    pub(crate) original: UserFile,
    pub(crate) hashes: FileHashes,
}

impl PreparedUserFile {
    /// Its size in bytes when it was read, which coven records and checks
    /// the original against.
    pub fn size(&self) -> u64 {
        self.original.size
    }

    /// Its modification time when it was read: the exact value coven
    /// records and checks the original against, at the precision the
    /// operating system gives.
    pub fn modified_at(&self) -> SystemTime {
        self.original.modified_at
    }
}

/// The facts recorded for the user's original, which coven never changes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UserFile {
    /// The user's original path.
    pub path: PathBuf,
    /// Its recorded size in bytes.
    pub size: u64,
    /// Its recorded modification time.
    pub modified_at: SystemTime,
}

/// Read a user's original once, recording its whole-file hash and each upload
/// chunk's hash from the same bytes, and reporting total bytes read. Changes
/// during the read are refused; later ones fail its upload, whether it is
/// staged for the cloud or moved there later. The read holds one place of
/// `lease` until it ends (§17.3).
pub async fn prepare_user_file(
    path: &Path,
    lease: &FileLease,
    progress: impl Fn(u64) + Send + Sync,
) -> Result<PreparedUserFile, DbError> {
    let mut hash = FileHasher::new();
    let mut read = 0;
    let observed = UserOriginal::at(path.to_owned())
        .observe(lease.place().await, |bytes| {
            hash.update(bytes);
            read += bytes.len() as u64;
            progress(read);
        })
        .await?;
    Ok(PreparedUserFile {
        original: UserFile {
            path: observed.path().to_owned(),
            size: observed.size(),
            modified_at: observed.modified_at(),
        },
        hashes: hash.finish(),
    })
}

impl From<ObservationError> for DbError {
    fn from(error: ObservationError) -> Self {
        match error {
            ObservationError::Missing(path) => Self::UserFileMissing { path },
            ObservationError::Changed(path) => Self::UserFileChanged { path },
            ObservationError::File(error) => error.into(),
        }
    }
}

/// A time as `coven_user_files` lists a modification time, in
/// `modified_seconds` and `modified_nanos`: whole seconds since the Unix
/// epoch, rounded down, and the nanoseconds past that second, from 0 through
/// 999,999,999 (§17.3). Half a second before the epoch is (-1, 500,000,000).
/// An app compares a file's time on disk with the view's through it.
pub fn unix_seconds_and_nanos(time: SystemTime) -> (i64, u32) {
    let (seconds, nanos) = match time.duration_since(std::time::UNIX_EPOCH) {
        Ok(after) => (i128::from(after.as_secs()), after.subsec_nanos()),
        Err(before) => {
            let before = before.duration();
            match before.subsec_nanos() {
                0 => (-i128::from(before.as_secs()), 0),
                nanos => (-i128::from(before.as_secs()) - 1, 1_000_000_000 - nanos),
            }
        }
    };
    (
        i64::try_from(seconds)
            .expect("every platform's time fits: Unix keeps i64 seconds, Windows's ends in 30828"),
        nanos,
    )
}

pub(crate) fn encode_time(time: SystemTime) -> Vec<u8> {
    let (sign, elapsed) = match time.duration_since(std::time::UNIX_EPOCH) {
        Ok(elapsed) => (0, elapsed),
        Err(error) => (1, error.duration()),
    };
    let mut bytes = vec![sign];
    bytes.extend(elapsed.as_secs().to_be_bytes());
    bytes.extend(elapsed.subsec_nanos().to_be_bytes());
    bytes
}

/// Record `$original` as `$file`'s original on this device, with the
/// content hash and chunk hashes in `$hashes`, running each statement on
/// `$db`: a store's open write, or a test's connection outside any store.
/// Both callers expand this one body, so a test records an original as a
/// store does.
macro_rules! record_original {
    ($db:expr, $file:expr, $original:expr, $hashes:expr) => {{
        let (db, file, original, hashes) = ($db, $file, $original, $hashes);
        let size = crate::sqlite::integer("file size", original.size)?;
        db.execute(
            "INSERT INTO _coven_user_files(file,path,size,modified_at,hash) VALUES(?1,?2,?3,?4,?5)",
            sqlive::rusqlite::params![
                file,
                coven_foundation::files::path_bytes(&original.path),
                size,
                encode_time(original.modified_at),
                hashes.content.as_bytes().as_slice()
            ],
        )?;
        for (index, hash) in hashes.chunks().iter().enumerate() {
            db.execute(
                "INSERT INTO _coven_user_chunks(file,chunk,hash) VALUES(?1,?2,?3)",
                sqlive::rusqlite::params![file, index as i64, hash.as_bytes().as_slice()],
            )?;
        }
        Ok(())
    }};
}

/// Record `original` as `file`'s original on this device, with the content
/// hash and chunk hashes of the bytes its read found (§17.3).
pub(crate) fn record(
    db: &StoreSql,
    file: coven_foundation::id_source::FileId,
    original: &UserFile,
    hashes: &FileHashes,
) -> Result<(), DbError> {
    record_original!(db, file, original, hashes)
}

/// Record `original` as `file`'s original, with the content hash and chunk
/// hashes in `hashes`, on `connection`, a test's connection outside any store
/// (`test_utils::record_user_file`).
#[cfg(any(test, feature = "test-utils"))]
pub(crate) fn record_outside_store(
    connection: &sqlive::rusqlite::Connection,
    file: coven_foundation::id_source::FileId,
    original: &UserFile,
    hashes: &FileHashes,
) -> Result<(), DbError> {
    record_original!(connection, file, original, hashes)
}

/// Forget `file`'s original here, and its chunk hashes with it: the file
/// never touches it again (§17.3).
pub(crate) fn forget(
    db: &StoreSql,
    file: coven_foundation::id_source::FileId,
) -> Result<(), DbError> {
    db.execute("DELETE FROM _coven_user_files WHERE file=?1", [file])?;
    Ok(())
}

/// The new file `to` over `from`'s bytes shares `from`'s original here and
/// its chunk hashes (§17.2).
pub(crate) fn share(
    db: &StoreSql,
    from: coven_foundation::id_source::FileId,
    to: coven_foundation::id_source::FileId,
) -> Result<(), DbError> {
    db.execute(
        "INSERT INTO _coven_user_files(file,path,size,modified_at,hash)
         SELECT ?2,path,size,modified_at,hash FROM _coven_user_files WHERE file=?1",
        sqlive::rusqlite::params![from, to],
    )?;
    db.execute(
        "INSERT INTO _coven_user_chunks(file,chunk,hash)
         SELECT ?2,chunk,hash FROM _coven_user_chunks WHERE file=?1",
        sqlive::rusqlite::params![from, to],
    )?;
    Ok(())
}

/// `file` is stored, and no reset can switch it back: a stored file never
/// moves again, so its original's chunk hashes go; the original stays
/// recorded (§17.3, §17.11).
pub(crate) fn stored(
    db: &StoreSql,
    file: coven_foundation::id_source::FileId,
) -> Result<(), DbError> {
    db.execute("DELETE FROM _coven_user_chunks WHERE file=?1", [file])?;
    Ok(())
}

/// The content hash and chunk hashes recorded with `file`'s original here,
/// if it has one. Its chunk hashes are kept only while the file is on this
/// device only: they go once it is stored (§17.3).
pub(crate) fn hashes(
    db: &StoreSql,
    file: coven_foundation::id_source::FileId,
) -> Result<Option<FileHashes>, DbError> {
    let Some(content) = db
        .query("SELECT hash FROM _coven_user_files WHERE file=?1", [file], |r| {
            r.get::<_, Vec<u8>>(0)
        })?
        .pop()
    else {
        return Ok(None);
    };
    let chunks = db.query(
        "SELECT hash FROM _coven_user_chunks WHERE file=?1 ORDER BY chunk",
        [file],
        |r| r.get::<_, Vec<u8>>(0),
    )?;
    Ok(Some(FileHashes::stored(content, chunks)?))
}

/// The user's original this device recorded for `file`, if it has one and
/// the file's record is here and doesn't put the file on another device
/// only, as `coven_user_files` lists it: one kept only for a reset that may
/// bring its record back isn't (§17.11).
#[expect(
    clippy::map_err_ignore,
    reason = "a stored value that won't convert or decode is DamagedDatabase"
)]
pub(crate) fn read(
    db: &StoreSql,
    file: coven_foundation::id_source::FileId,
) -> Result<Option<UserFile>, DbError> {
    db.query(
        "SELECT u.path,u.size,u.modified_at FROM _coven_user_files AS u
         WHERE u.file=?1
         AND (NOT EXISTS(SELECT 1 FROM _coven_undoable_files AS k WHERE k.file=u.file)
           OR EXISTS(SELECT 1 FROM main.coven_files AS f WHERE f.id=u.file
             AND coalesce(coven_file_device(f.coordinates)=_coven_device(),1)))",
        [file],
        |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, i64>(1)?, row.get::<_, Vec<u8>>(2)?)),
    )?
    .into_iter()
    .next()
    .map(|(path, size, modified_at)| {
        Ok(UserFile {
            path: decode_path(path)?,
            size: u64::try_from(size).map_err(|_| DbError::DamagedDatabase)?,
            modified_at: decode_time(&modified_at)?,
        })
    })
    .transpose()
}

pub(crate) fn decode_time(bytes: &[u8]) -> Result<SystemTime, DbError> {
    if bytes.len() != 13 {
        return Err(DbError::DamagedDatabase);
    }
    let seconds = u64::from_be_bytes(bytes[1..9].try_into().expect("checked time width"));
    let nanos = u32::from_be_bytes(bytes[9..].try_into().expect("checked time width"));
    if nanos >= 1_000_000_000 {
        return Err(DbError::DamagedDatabase);
    }
    let elapsed = std::time::Duration::new(seconds, nanos);
    match bytes[0] {
        0 => std::time::UNIX_EPOCH.checked_add(elapsed),
        1 => std::time::UNIX_EPOCH.checked_sub(elapsed),
        _ => None,
    }
    .ok_or(DbError::DamagedDatabase)
}

/// A path `path_bytes` recorded; bytes it never gives are a damaged
/// database.
pub(crate) fn decode_path(bytes: Vec<u8>) -> Result<PathBuf, DbError> {
    coven_foundation::files::path_from_bytes(bytes)
        .map_err(|coven_foundation::files::PathBytesError| DbError::DamagedDatabase)
}

#[cfg(test)]
#[path = "user_file_tests.rs"]
pub(crate) mod tests;

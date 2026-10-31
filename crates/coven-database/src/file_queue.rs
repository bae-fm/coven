//! Durable file upload facts; provider session bytes remain opaque to SQLite.

use crate::file_location::FileLocation;
use crate::{sqlite::DatabaseConnection, DbError};
use coven_crypto::{ContentHash, SecretBytes};
use coven_format::file::FileHeader;
use coven_foundation::id_source::{DeviceId, FileId};
use std::time::SystemTime;

/// One durable upload of one stored object; its plaintext chunk hashes are
/// owned child rows. The location carries the path and key its staging
/// fixed. Failures live in `_coven_pending` under the object's subject, not
/// here.
#[derive(Debug)]
pub struct FileUpload {
    /// Local queue identity, stable across retries.
    pub id: i64,
    /// Where the bytes go, with their key, fixed by the staging write.
    pub location: FileLocation,
    /// The plaintext size.
    pub size: u64,
    /// The content hash of the bytes, from their first read.
    pub content_hash: ContentHash,
    /// The files the upload's completion switches to the cloud: the one its
    /// write staged, and each a move gave its rows over the same bytes.
    pub files: Vec<FileId>,
    /// When this device queued the file.
    pub queued_at: SystemTime,
    /// The persisted automatic retry wait, if the last attempt armed one.
    pub retry: Option<crate::RetryWait>,
    /// The move it belongs to, whose files switch to the cloud together.
    pub of: crate::MoveId,
    /// Whether its bytes are stored: it waits for the rest of its move.
    pub stored: bool,
    /// For a bound upload, the first write it holds back: the write that
    /// staged its file, or the first write another bound upload queued then
    /// held. That write and every later one of this device wait while a file
    /// waits for the upload (§17.4). `None` for a trailing upload.
    pub holds: Option<u64>,
    /// Opaque provider session, interpreted only by storage. It records each
    /// provider-confirmed part before progress reports it.
    pub session: Option<SecretBytes>,
    /// The bytes the session records as confirmed, written with it; `None`
    /// while the upload has no session, as one sent in one request never does.
    pub progress: Option<UploadProgress>,
    /// The whole encrypted size the upload sends, in one request or in
    /// parts, which the file's size fixes: known before any attempt starts.
    pub bytes_total: u64,
}

/// Bytes the provider confirmed in a recorded upload session. Sending bytes
/// alone does not advance it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UploadProgress {
    /// Provider-confirmed ciphertext bytes.
    pub bytes_confirmed: u64,
    /// Whole ciphertext size.
    pub bytes_total: u64,
}
impl Clone for FileUpload {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            location: self.location.clone(),
            size: self.size,
            content_hash: self.content_hash,
            files: self.files.clone(),
            queued_at: self.queued_at,
            retry: self.retry,
            of: self.of,
            stored: self.stored,
            holds: self.holds,
            session: self
                .session
                .as_ref()
                .map(|session| SecretBytes::new(session.as_bytes().to_vec())),
            progress: self.progress,
            bytes_total: self.bytes_total,
        }
    }
}
impl PartialEq for FileUpload {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.location == other.location
            && self.size == other.size
            && self.content_hash == other.content_hash
            && self.files == other.files
            && self.queued_at == other.queued_at
            && self.retry == other.retry
            && self.of == other.of
            && self.stored == other.stored
            && self.holds == other.holds
            && self.session.as_ref().map(SecretBytes::as_bytes)
                == other.session.as_ref().map(SecretBytes::as_bytes)
            && self.progress == other.progress
            && self.bytes_total == other.bytes_total
    }
}

/// The queue as the app observes it: the pause setting and every queued file
/// with its recorded session, from one committed state, and the files this
/// process is sending as it was read (E7).
#[derive(Clone, Debug, PartialEq)]
pub struct UploadQueueState {
    /// Whether the app paused uploads; persisted across reopening.
    pub paused: bool,
    /// Queued uploads, oldest first.
    pub uploads: Vec<FileUpload>,
    /// The files being sent as the queue was read, as `coven_sending` lists
    /// them.
    pub sending: std::collections::BTreeSet<FileId>,
}

/// What this device committed and has not uploaded: writes and files (E1).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UnsentWork {
    /// Committed writes waiting to upload.
    pub writes: u64,
    /// Staged files waiting to upload.
    pub files: u64,
}

impl UnsentWork {
    /// Whether anything waits to upload.
    pub fn is_empty(&self) -> bool {
        self.writes == 0 && self.files == 0
    }
}

pub(crate) fn unsent(db: &DatabaseConnection) -> Result<UnsentWork, DbError> {
    db.query_row(
        "SELECT (SELECT count(*) FROM _coven_uploads),(SELECT count(*) FROM _coven_file_uploads)",
        [],
        |r| {
            Ok(UnsentWork {
                writes: r.get::<_, i64>(0)? as u64,
                files: r.get::<_, i64>(1)? as u64,
            })
        },
    )
}

pub(crate) fn state(db: &DatabaseConnection) -> Result<UploadQueueState, DbError> {
    Ok(UploadQueueState {
        paused: paused(db)?,
        uploads: read(db)?,
        sending: db
            .query("SELECT file FROM coven_sending", [], |r| {
                r.get::<_, FileId>(0)
            })?
            .into_iter()
            .collect(),
    })
}

pub(crate) fn paused(db: &DatabaseConnection) -> Result<bool, DbError> {
    db.query_row(
        "SELECT EXISTS(SELECT 1 FROM _coven_upload_pause)",
        [],
        |r| r.get(0),
    )
}

pub(crate) fn set_paused(db: &DatabaseConnection, paused: bool) -> Result<(), DbError> {
    if paused {
        db.internal_execute(
            "INSERT INTO _coven_upload_pause(paused) VALUES(1) ON CONFLICT DO NOTHING",
            [],
        )?;
    } else {
        db.internal_execute("DELETE FROM _coven_upload_pause", [])?;
    }
    Ok(())
}

/// The uploads some file still waits for, oldest first: the queue the app
/// sees and the worker serves (§17.9). One whose files are all gone waits
/// only for retirement.
pub(crate) fn read(db: &DatabaseConnection) -> Result<Vec<FileUpload>, DbError> {
    let rows = db.query(
        "SELECT u.id,u.location,u.size,u.hash,u.queued_at,u.session,u.bytes_confirmed,
                u.bytes_total,r.failures,r.not_before,r.wait,u.move,u.stored,u.holds
         FROM _coven_file_uploads AS u LEFT JOIN _coven_retries AS r ON r.upload=u.id
         WHERE EXISTS(SELECT 1 FROM _coven_upload_files AS l WHERE l.upload=u.id)
         ORDER BY u.id",
        [],
        upload_row,
    )?;
    decode_uploads(db, rows)
}

/// Every queued upload, those no file waits for included, oldest first.
#[cfg(any(test, feature = "test-utils"))]
pub(crate) fn read_all(db: &DatabaseConnection) -> Result<Vec<FileUpload>, DbError> {
    let rows = db.query(
        "SELECT u.id,u.location,u.size,u.hash,u.queued_at,u.session,u.bytes_confirmed,
                u.bytes_total,r.failures,r.not_before,r.wait,u.move,u.stored,u.holds
         FROM _coven_file_uploads AS u LEFT JOIN _coven_retries AS r ON r.upload=u.id ORDER BY u.id",
        [],
        upload_row,
    )?;
    decode_uploads(db, rows)
}

type UploadRow = (
    (i64, String, i64, Vec<u8>, Vec<u8>),
    (Option<Vec<u8>>, Option<i64>, Option<i64>),
    (Option<i64>, Option<Vec<u8>>, Option<i64>),
    (i64, bool, Option<Vec<u8>>),
);

fn upload_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<UploadRow> {
    Ok((
        (r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?),
        (r.get(5)?, r.get(6)?, r.get(7)?),
        (r.get(8)?, r.get(9)?, r.get(10)?),
        (r.get(11)?, r.get(12)?, r.get(13)?),
    ))
}

#[expect(
    clippy::map_err_ignore,
    reason = "a stored value that won't convert or decode is DamagedDatabase"
)]
fn decode_uploads(
    db: &DatabaseConnection,
    rows: Vec<UploadRow>,
) -> Result<Vec<FileUpload>, DbError> {
    rows.into_iter()
        .map(
            |((id, location, size, hash, queued), session, retry, (of, stored, holds))| {
                let retry = match retry {
                    (None, None, None) => None,
                    (Some(failures), Some(not_before), Some(wait)) => Some(crate::RetryWait {
                        failures: u32::try_from(failures).map_err(|_| DbError::DamagedDatabase)?,
                        not_before: crate::user_file::decode_time(&not_before)?,
                        wait: std::time::Duration::from_millis(
                            u64::try_from(wait).map_err(|_| DbError::DamagedDatabase)?,
                        ),
                    }),
                    _ => return Err(DbError::DamagedDatabase),
                };
                let (session, progress) = match session {
                    (None, None, None) => (None, None),
                    (Some(session), Some(confirmed), Some(total)) => (
                        Some(SecretBytes::new(session)),
                        Some(UploadProgress {
                            bytes_confirmed: u64::try_from(confirmed)
                                .map_err(|_| DbError::DamagedDatabase)?,
                            bytes_total: u64::try_from(total)
                                .map_err(|_| DbError::DamagedDatabase)?,
                        }),
                    ),
                    _ => return Err(DbError::DamagedDatabase),
                };
                let size = u64::try_from(size).map_err(|_| DbError::DamagedDatabase)?;
                // Queueing refused a file whose layout doesn't fit.
                let bytes_total = FileHeader::new(size)
                    .encrypted_size()
                    .map_err(|_| DbError::DamagedDatabase)?;
                Ok(FileUpload {
                    id,
                    location: FileLocation::decode(&location)?,
                    size,
                    content_hash: ContentHash::from_bytes(
                        hash.try_into().map_err(|_| DbError::DamagedDatabase)?,
                    ),
                    files: db.query(
                        "SELECT file FROM _coven_upload_files WHERE upload=?1 ORDER BY file",
                        [id],
                        |r| r.get::<_, FileId>(0),
                    )?,
                    queued_at: crate::user_file::decode_time(&queued)?,
                    retry,
                    of: crate::MoveId(of),
                    stored,
                    holds: holds.map(crate::write_encoding::counter),
                    session,
                    progress,
                    bytes_total,
                })
            },
        )
        .collect()
}

/// The first write this device's bound uploads hold, while a file waits
/// for one (§17.4): no write from it on may be sent. An upload no file waits
/// for holds nothing, since nothing a write names waits for its bytes.
pub(crate) fn first_held(db: &DatabaseConnection) -> Result<Option<u64>, DbError> {
    db.query_row(
        "SELECT min(u.holds) FROM _coven_file_uploads AS u WHERE u.holds IS NOT NULL
         AND EXISTS(SELECT 1 FROM _coven_upload_files AS l WHERE l.upload=u.id)",
        [],
        |r| r.get::<_, Option<Vec<u8>>>(0),
    )
    .map(|holds| holds.map(crate::write_encoding::counter))
}

/// The queued upload of this stored object, by its unique identity index.
pub(crate) fn upload_id(
    db: &DatabaseConnection,
    uploader: DeviceId,
    object: FileId,
) -> Result<Option<i64>, DbError> {
    Ok(db
        .query(
            "SELECT id FROM _coven_file_uploads WHERE device=?1 AND file=?2",
            (uploader.0.to_be_bytes(), object.0.as_bytes()),
            |row| row.get(0),
        )?
        .pop())
}

/// The upload a file waits for on this device, if it waits for one.
pub(crate) fn upload_of(db: &DatabaseConnection, file: FileId) -> Result<Option<i64>, DbError> {
    Ok(db
        .query(
            "SELECT upload FROM _coven_upload_files WHERE file=?1",
            [file],
            |row| row.get(0),
        )?
        .pop())
}

/// The queue ids of the bound uploads (§17.4).
pub(crate) fn bound(db: &DatabaseConnection) -> Result<std::collections::BTreeSet<i64>, DbError> {
    Ok(db
        .query(
            "SELECT id FROM _coven_file_uploads WHERE holds IS NOT NULL",
            [],
            |r| r.get(0),
        )?
        .into_iter()
        .collect())
}

/// Whether a bound upload stores `file`, holding this device's writes until
/// it is stored (§17.4).
pub(crate) fn held_upload_of(db: &DatabaseConnection, file: FileId) -> Result<bool, DbError> {
    db.query_row(
        "SELECT EXISTS(SELECT 1 FROM _coven_upload_files AS l
             JOIN _coven_file_uploads AS u ON u.id=l.upload
             WHERE l.file=?1 AND u.holds IS NOT NULL)",
        [file],
        |row| row.get(0),
    )
}

/// The first-read hash of one queued plaintext chunk. A missing hash is
/// damage, found before any byte of that chunk is encrypted.
#[expect(
    clippy::map_err_ignore,
    reason = "a stored value that won't convert is damage, a computed one a broken invariant"
)]
pub(crate) fn chunk_hash(
    db: &DatabaseConnection,
    location: &FileLocation,
    index: u64,
) -> Result<ContentHash, DbError> {
    let index = i64::try_from(index).map_err(|_| DbError::Invariant {
        what: "a chunk index fits SQLite",
    })?;
    let hash = db
        .query_row(
            "SELECT hash FROM _coven_file_chunks WHERE device=?1 AND file=?2 AND chunk=?3",
            (
                location.uploader().0.to_be_bytes(),
                location.object().0.as_bytes(),
                index,
            ),
            |r| r.get::<_, Vec<u8>>(0),
        )
        .map_err(|error| match error {
            DbError::Sqlite(rusqlite::Error::QueryReturnedNoRows) => DbError::DamagedDatabase,
            error => error,
        })?;
    Ok(ContentHash::from_bytes(
        hash.try_into().map_err(|_| DbError::DamagedDatabase)?,
    ))
}

/// Queued uploads no file waits for any more, with their locations.
pub(crate) fn unlinked(db: &DatabaseConnection) -> Result<Vec<(i64, FileLocation)>, DbError> {
    db.query(
        "SELECT u.id,u.location FROM _coven_file_uploads AS u
         WHERE NOT EXISTS(SELECT 1 FROM _coven_upload_files AS l WHERE l.upload=u.id)
         ORDER BY u.id",
        [],
        |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)),
    )?
    .into_iter()
    .map(|(id, location)| Ok((id, FileLocation::decode(&location)?)))
    .collect()
}

/// Retire an upload whose move switched its files to the cloud (§17.3):
/// their copies, needed only while they were on this device only, go with
/// it, except one a pin request keeps for a file kept here.
pub(crate) fn switched(db: &DatabaseConnection, id: i64) -> Result<(), DbError> {
    // A file kept here is pinned by the write that switched it: its request
    // keeps coven's copy until the cache copy is made from it (§17.8).
    for file in db.query(
        "SELECT file FROM _coven_upload_files WHERE upload=?1 AND keep=1",
        [id],
        |r| r.get::<_, FileId>(0),
    )? {
        if crate::file_table::Record::read(db, file)?
            .is_some_and(|record| matches!(record.place, crate::file_location::Place::Cloud(_)))
        {
            crate::file_pins::request(db, file)?;
        }
    }
    for path in db.query(
        "DELETE FROM _coven_device_files
         WHERE file IN (SELECT file FROM _coven_upload_files WHERE upload=?1) RETURNING path",
        [id],
        |r| r.get::<_, String>(0),
    )? {
        release_copy(db, &path)?;
    }
    retire(db, id)
}

/// Retire an upload in one transaction (§17.9): the queue row, its chunk
/// hashes, and its links to the files it would switch. Coven's copy it
/// names is recorded for removal after the caller commits once nothing else
/// names it; the stored path becomes a deletion candidate if nothing else
/// protects it (§17.10).
pub(crate) fn retire(db: &DatabaseConnection, id: i64) -> Result<(), DbError> {
    let copy: Option<String> = db
        .query_row(
            "SELECT copy FROM _coven_file_uploads WHERE id=?1",
            [id],
            |r| r.get(0),
        )
        .map_err(|error| match error {
            DbError::Sqlite(rusqlite::Error::QueryReturnedNoRows) => DbError::Invariant {
                what: "a retired upload is queued",
            },
            error => error,
        })?;
    db.internal_execute("DELETE FROM _coven_upload_files WHERE upload=?1", [id])?;
    db.internal_execute("DELETE FROM _coven_file_uploads WHERE id=?1", [id])?;
    if let Some(copy) = copy {
        release_copy(db, &copy)?;
    }
    Ok(())
}

/// Record coven's copy at `path` for removal once nothing names it: no file
/// on this device only, no queued upload and no pin waiting to be made from
/// it (Appendix C.3).
pub(crate) fn release_copy(db: &DatabaseConnection, path: &str) -> Result<(), DbError> {
    let named: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM _coven_device_files WHERE path=?1)
             OR EXISTS(SELECT 1 FROM _coven_file_uploads WHERE copy=?1)
             OR EXISTS(SELECT 1 FROM _coven_pin_requests WHERE copy=?1)",
        [path],
        |r| r.get(0),
    )?;
    if named {
        return Ok(());
    }
    db.internal_execute("DELETE FROM _coven_copy_chunks WHERE path=?1", [path])?;
    db.internal_execute(
        "INSERT INTO _coven_file_removals(path) VALUES(?1) ON CONFLICT DO NOTHING",
        [path],
    )?;
    Ok(())
}

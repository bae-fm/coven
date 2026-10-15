//! Durable file upload facts; provider session bytes remain opaque to SQLite.

use crate::{sqlite::DatabaseConnection, DbError, FileRef};
use coven_crypto::{ContentHash, SecretBytes};
use coven_format::file::FileHeader;
use coven_foundation::files::FileName;
use std::time::SystemTime;

/// One durable upload; its plaintext chunk hashes are owned child rows.
/// The captured reference carries the path and key fixed by the attaching write.
/// Failures live in `_coven_pending` under the file's subject, not here.
#[derive(Debug)]
pub struct FileUpload {
    /// Local queue identity, stable across retries.
    pub id: i64,
    /// The exact row version queued by its attaching write, with its fixed
    /// uploader, object id and key.
    pub file: FileRef,
    /// When this device queued the file.
    pub queued_at: SystemTime,
    /// The persisted automatic retry wait, if the last attempt armed one.
    pub retry: Option<crate::RetryWait>,
    /// Opaque provider session, interpreted only by storage. It records each
    /// provider-confirmed part before progress reports it.
    pub session: Option<SecretBytes>,
}
impl Clone for FileUpload {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            file: self.file.clone(),
            queued_at: self.queued_at,
            retry: self.retry,
            session: self
                .session
                .as_ref()
                .map(|session| SecretBytes::new(session.as_bytes().to_vec())),
        }
    }
}
impl PartialEq for FileUpload {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.file == other.file
            && self.queued_at == other.queued_at
            && self.retry == other.retry
            && self.session.as_ref().map(SecretBytes::as_bytes)
                == other.session.as_ref().map(SecretBytes::as_bytes)
    }
}

/// The queue as the app observes it: the pause setting and every queued file
/// with its recorded session, from one committed state (E7).
#[derive(Clone, Debug, PartialEq)]
pub struct UploadQueueState {
    /// Whether the app paused uploads; persisted across reopening.
    pub paused: bool,
    /// Queued files, oldest first.
    pub uploads: Vec<FileUpload>,
}

pub(crate) fn state(db: &DatabaseConnection) -> Result<UploadQueueState, DbError> {
    Ok(UploadQueueState {
        paused: paused(db)?,
        uploads: read(db)?,
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

/// Where a queued upload reads its bytes, fixed by the attaching write and
/// kept by the queue whatever later happens to the row (§16.5).
pub(crate) enum Source {
    /// Coven's own copy of app-provided bytes.
    Copy(FileName),
    /// The user's original: its encoded path and recorded modification time.
    Original { path: Vec<u8>, modified_at: Vec<u8> },
}

/// The source facts and first-read chunk hashes an attachment queues.
pub(crate) struct Attachment {
    pub(crate) source: Source,
    pub(crate) chunks: Vec<ContentHash>,
}

/// Queue one attached file under its fixed reference, with its source and one
/// hash per plaintext chunk, in the attaching transaction (§16.1, §16.6).
pub(crate) fn enqueue(
    db: &DatabaseConnection,
    file: &FileRef,
    attachment: &Attachment,
    now: SystemTime,
) -> Result<(), DbError> {
    if attachment.chunks.len() as u64 != FileHeader::new(file.plaintext_size()).chunk_count() {
        return Err(DbError::Invariant {
            what: "an attachment hashes every plaintext chunk",
        });
    }
    let device = file.uploader().0.to_be_bytes();
    let id = file.id().0;
    let (original, modified_at) = match &attachment.source {
        Source::Original { path, modified_at } => (Some(path), Some(modified_at)),
        Source::Copy(_) => (None, None),
    };
    db.internal_execute(
        "INSERT INTO _coven_file_uploads(reference,device,file,original,modified_at,queued_at)
         VALUES(?1,?2,?3,?4,?5,?6)",
        (
            file.encode()?,
            device,
            id.as_bytes(),
            original,
            modified_at,
            crate::user_file::encode_time(now),
        ),
    )?;
    if let Source::Copy(name) = &attachment.source {
        db.internal_execute(
            "INSERT INTO _coven_device_files(device,file,path) VALUES(?1,?2,?3)",
            (device, id.as_bytes(), name.as_str()),
        )?;
    }
    for (index, hash) in attachment.chunks.iter().enumerate() {
        db.internal_execute(
            "INSERT INTO _coven_file_chunks(device,file,chunk,hash) VALUES(?1,?2,?3,?4)",
            (
                device,
                id.as_bytes(),
                index as i64,
                hash.as_bytes().as_slice(),
            ),
        )?;
    }
    Ok(())
}

pub(crate) fn read(db: &DatabaseConnection) -> Result<Vec<FileUpload>, DbError> {
    let rows = db.query(
        "SELECT u.id,u.reference,u.queued_at,u.session,r.failures,r.not_before,r.wait
         FROM _coven_file_uploads AS u LEFT JOIN _coven_retries AS r ON r.upload=u.id ORDER BY u.id",
        [],
        |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, Vec<u8>>(1)?,
                r.get::<_, Vec<u8>>(2)?,
                r.get::<_, Option<Vec<u8>>>(3)?,
                (
                    r.get::<_, Option<i64>>(4)?,
                    r.get::<_, Option<Vec<u8>>>(5)?,
                    r.get::<_, Option<i64>>(6)?,
                ),
            ))
        },
    )?;
    rows.into_iter()
        .map(|(id, reference, queued, session, retry)| {
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
            Ok(FileUpload {
                id,
                file: FileRef::decode(&reference)?,
                queued_at: crate::user_file::decode_time(&queued)?,
                retry,
                session: session.map(SecretBytes::new),
            })
        })
        .collect()
}

/// Whether this device's queue still holds the file, so its local source
/// remains the authoritative copy (§16.1).
pub(crate) fn queued(db: &DatabaseConnection, file: &FileRef) -> Result<bool, DbError> {
    db.query_row(
        "SELECT EXISTS(SELECT 1 FROM _coven_file_uploads WHERE device=?1 AND file=?2)",
        (file.uploader().0.to_be_bytes(), file.id().0.as_bytes()),
        |r| r.get(0),
    )
}

/// The first-read hash of one queued plaintext chunk. A missing hash is
/// damage, found before any byte of that chunk is encrypted.
pub(crate) fn chunk_hash(
    db: &DatabaseConnection,
    file: &FileRef,
    index: u64,
) -> Result<ContentHash, DbError> {
    let index = i64::try_from(index).map_err(|_| DbError::Invariant {
        what: "a chunk index fits SQLite",
    })?;
    let hash = db
        .query_row(
            "SELECT hash FROM _coven_file_chunks WHERE device=?1 AND file=?2 AND chunk=?3",
            (
                file.uploader().0.to_be_bytes(),
                file.id().0.as_bytes(),
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

/// Retire a confirmed upload in one transaction (§16.5): the queue row, its
/// chunk hashes, and the owned copy kept only until upload. The app row and
/// every `FileRef` stay unchanged, whatever happened to the row meanwhile.
/// A released copy no other queued file shares is recorded for removal after
/// the caller commits.
pub(crate) fn complete(db: &DatabaseConnection, id: i64) -> Result<FileRef, DbError> {
    let (device, file, reference) = db
        .query_row(
            "SELECT device,file,reference FROM _coven_file_uploads WHERE id=?1",
            [id],
            |r| {
                Ok((
                    r.get::<_, Vec<u8>>(0)?,
                    r.get::<_, Vec<u8>>(1)?,
                    r.get::<_, Vec<u8>>(2)?,
                ))
            },
        )
        .map_err(|error| match error {
            DbError::Sqlite(rusqlite::Error::QueryReturnedNoRows) => DbError::Invariant {
                what: "a completed upload is queued",
            },
            error => error,
        })?;
    for path in db.query(
        "DELETE FROM _coven_device_files WHERE device=?1 AND file=?2 RETURNING path",
        (&device, &file),
        |r| r.get::<_, String>(0),
    )? {
        db.internal_execute(
            "INSERT INTO _coven_file_removals(path)
             SELECT ?1 WHERE NOT EXISTS(SELECT 1 FROM _coven_device_files WHERE path=?1)
             ON CONFLICT DO NOTHING",
            [path],
        )?;
    }
    db.internal_execute("DELETE FROM _coven_file_uploads WHERE id=?1", [id])?;
    FileRef::decode(&reference)
}

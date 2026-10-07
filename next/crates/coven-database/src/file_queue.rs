//! Durable file upload facts; provider session bytes remain opaque to SQLite.

use crate::{sqlite::DatabaseConnection, write_schema::WriteSchema, DbError, FileRef, Uploads};
use coven_crypto::SecretBytes;
use coven_foundation::files::FileName;
use std::time::SystemTime;

/// One durable upload, including the fixed encrypted source after preparation.
#[derive(Debug)]
pub struct FileUpload {
    /// Local queue identity, stable across retries.
    pub id: i64,
    /// The exact row version requested for upload.
    pub file: FileRef,
    /// When this device queued the file.
    pub queued_at: SystemTime,
    /// Failed attempts so far.
    pub attempts: u64,
    /// When the latest attempt began.
    pub last_attempt_at: Option<SystemTime>,
    /// Sync's typed failure recording, interpreted only by sync.
    pub failure: Option<SecretBytes>,
    /// Encrypted bytes and their fixed identity, once durably prepared.
    pub fixed: Option<FixedFileUpload>,
    /// Opaque provider session, interpreted only by storage.
    pub session: Option<SecretBytes>,
    /// The provider has confirmed complete publication.
    pub stored: bool,
    /// The row changed; this stored object is retained for later deletion.
    pub unused: bool,
}

/// The immutable disk source and uploaded identity of a prepared file.
#[derive(Debug)]
pub struct FixedFileUpload {
    /// The name in coven's owned file area.
    pub name: FileName,
    /// Sync's encoded file id and key; never printed.
    pub identity: SecretBytes,
}

pub(crate) fn enqueue(
    db: &DatabaseConnection,
    file: &FileRef,
    now: SystemTime,
) -> Result<(), DbError> {
    db.internal_execute("INSERT INTO coven_file_uploads(reference,queued_at) VALUES(?1,?2) ON CONFLICT(reference) DO NOTHING", (file.encode()?, crate::user_file::encode_time(now)))?;
    Ok(())
}

pub(crate) fn attached(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    keys: impl Iterator<Item = crate::write_rows::AppKey>,
    now: SystemTime,
) -> Result<(), DbError> {
    for (table, key) in keys {
        let Some(file) = &schema.declaration(&table).files else {
            continue;
        };
        if file.uploads != Uploads::WhenAttached {
            continue;
        }
        match crate::file_ref::read(
            db,
            schema,
            &table,
            &crate::file_row::key(&(table.clone(), key))?,
        ) {
            Ok(reference) => enqueue(db, &reference, now)?,
            Err(DbError::FileAbsent { .. }) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

pub(crate) fn read(db: &DatabaseConnection) -> Result<Vec<FileUpload>, DbError> {
    let rows = db.query("SELECT id,reference,queued_at,attempts,last_attempt_at,failure,path,fixed,session,stored,unused FROM coven_file_uploads ORDER BY id",[],|r| Ok((r.get::<_,i64>(0)?,r.get::<_,Vec<u8>>(1)?,r.get::<_,Vec<u8>>(2)?,r.get::<_,i64>(3)?,r.get::<_,Option<Vec<u8>>>(4)?,r.get::<_,Option<Vec<u8>>>(5)?,r.get::<_,Option<String>>(6)?,r.get::<_,Option<Vec<u8>>>(7)?,r.get::<_,Option<Vec<u8>>>(8)?,r.get::<_,bool>(9)?,r.get::<_,bool>(10)?)))?;
    rows.into_iter()
        .map(
            |(
                id,
                reference,
                queued,
                attempts,
                last,
                failure,
                path,
                fixed,
                session,
                stored,
                unused,
            )| {
                let fixed = match (path, fixed) {
                    (None, None) => None,
                    (Some(path), Some(identity)) => Some(FixedFileUpload {
                        name: FileName::new(path).map_err(|_| DbError::DamagedDatabase)?,
                        identity: SecretBytes::new(identity),
                    }),
                    _ => return Err(DbError::DamagedDatabase),
                };
                Ok(FileUpload {
                    id,
                    file: FileRef::decode(&reference)?,
                    queued_at: crate::user_file::decode_time(&queued)?,
                    attempts: u64::try_from(attempts).map_err(|_| DbError::DamagedDatabase)?,
                    last_attempt_at: last
                        .as_deref()
                        .map(crate::user_file::decode_time)
                        .transpose()?,
                    failure: failure.map(SecretBytes::new),
                    fixed,
                    session: session.map(SecretBytes::new),
                    stored,
                    unused,
                })
            },
        )
        .collect()
}

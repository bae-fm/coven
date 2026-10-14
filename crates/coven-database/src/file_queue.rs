//! Durable file upload facts; provider session bytes remain opaque to SQLite.

use crate::{sqlite::DatabaseConnection, write_schema::WriteSchema, DbError, FileRef};
use coven_crypto::SecretBytes;
use coven_format::file::FileHeader;
use std::time::SystemTime;

/// One durable upload; its plaintext chunk hashes are owned child rows.
/// The captured reference carries the path and key fixed by the attaching write.
#[derive(Debug)]
pub struct FileUpload {
    /// Local queue identity, stable across retries.
    pub id: i64,
    /// The exact row version queued by its attaching write, with its fixed
    /// uploader, object id and key.
    pub file: FileRef,
    /// When this device queued the file.
    pub queued_at: SystemTime,
    /// Failed attempts so far.
    pub attempts: u64,
    /// When the latest attempt began.
    pub last_attempt_at: Option<SystemTime>,
    /// Sync's typed failure recording, interpreted only by sync.
    pub failure: Option<SecretBytes>,
    /// Opaque provider session, interpreted only by storage.
    pub session: Option<SecretBytes>,
}

fn enqueue(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    file: &FileRef,
    now: SystemTime,
) -> Result<(), DbError> {
    let (_, declaration) = crate::file_row::declaration(schema, file.table())?;
    let (key, values) = crate::file_row::lookup(db, schema, file.table(), file.key())?;
    let identity =
        crate::file_row::identity(declaration, &values)?.ok_or(DbError::DamagedDatabase)?;
    let inserted = db.internal_execute(
        "INSERT INTO _coven_file_uploads(reference,device,file,queued_at) VALUES(?1,?2,?3,?4)
         ON CONFLICT(reference) DO NOTHING",
        (
            file.encode()?,
            file.uploader().0.to_be_bytes(),
            file.id().0.as_bytes(),
            crate::user_file::encode_time(now),
        ),
    )?;
    if inserted == 1 {
        let id: i64 = db.query_row("SELECT last_insert_rowid()", [], |r| r.get(0))?;
        let count = db.internal_execute(
            "INSERT INTO _coven_file_upload_chunks(upload,chunk,hash)
             SELECT ?1,chunk,hash FROM _coven_file_chunks
             WHERE table_name=?2 AND key=?3 AND column_name=?4 AND identity=?5",
            (id, &key.0, &key.1, file.column(), identity),
        )?;
        let header = FileHeader::new(file.plaintext_size());
        let end: Option<i64> = db.query_row(
            "SELECT max(chunk)+1 FROM _coven_file_upload_chunks WHERE upload=?1",
            [id],
            |r| r.get(0),
        )?;
        if count as u64 != header.chunk_count() || end != (count != 0).then_some(count as i64) {
            return Err(DbError::DamagedDatabase);
        }
    }
    Ok(())
}

pub(crate) fn attached(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    keys: impl Iterator<Item = crate::write_rows::AppKey>,
    device: coven_foundation::id_source::DeviceId,
    now: SystemTime,
) -> Result<(), DbError> {
    for (table, key) in keys {
        match crate::file_ref::read(
            db,
            schema,
            &table,
            &crate::file_row::key(&(table.clone(), key))?,
        ) {
            Ok(reference) => {
                // Reattaching identical bytes can change only local file records,
                // leaving no row-write part on which to check membership.
                crate::write_commit::check_membership(db, device, &reference.audience())?;
                enqueue(db, schema, &reference, now)?;
            }
            Err(DbError::FileAbsent { .. }) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

pub(crate) fn read(db: &DatabaseConnection) -> Result<Vec<FileUpload>, DbError> {
    let rows = db.query("SELECT id,reference,queued_at,attempts,last_attempt_at,failure,session FROM _coven_file_uploads ORDER BY id", [], |r| {
        Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?, r.get::<_, Vec<u8>>(2)?, r.get::<_, i64>(3)?, r.get::<_, Option<Vec<u8>>>(4)?, r.get::<_, Option<Vec<u8>>>(5)?, r.get::<_, Option<Vec<u8>>>(6)?))
    })?;
    rows.into_iter()
        .map(
            |(id, reference, queued, attempts, last, failure, session)| {
                let file = FileRef::decode(&reference)?;
                Ok(FileUpload {
                    id,
                    file,
                    queued_at: crate::user_file::decode_time(&queued)?,
                    attempts: u64::try_from(attempts).map_err(|_| DbError::DamagedDatabase)?,
                    last_attempt_at: last
                        .as_deref()
                        .map(crate::user_file::decode_time)
                        .transpose()?,
                    failure: failure.map(SecretBytes::new),
                    session: session.map(SecretBytes::new),
                })
            },
        )
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

/// Retire a confirmed upload in one transaction (§16.5): the queue row, its
/// chunk hashes, and the owned copy kept only until upload. The app row and
/// every `FileRef` stay unchanged, whatever happened to the row meanwhile.
/// The released copy is recorded for removal after the caller commits.
pub(crate) fn complete(db: &DatabaseConnection, id: i64) -> Result<(), DbError> {
    let reference = db
        .query_row(
            "SELECT reference FROM _coven_file_uploads WHERE id=?1",
            [id],
            |r| r.get::<_, Vec<u8>>(0),
        )
        .map_err(|error| match error {
            DbError::Sqlite(rusqlite::Error::QueryReturnedNoRows) => DbError::DamagedDatabase,
            error => error,
        })?;
    let file = FileRef::decode(&reference)?;
    let identity = file.identity()?;
    let (table, key) = file.app_key();
    for path in db.query(
        "DELETE FROM _coven_device_files
         WHERE table_name=?1 AND key=?2 AND column_name=?3 AND identity=?4 RETURNING path",
        (&table, &key, file.column(), &identity),
        |r| r.get::<_, String>(0),
    )? {
        db.internal_execute(
            "INSERT INTO _coven_file_removals(path) VALUES(?1) ON CONFLICT DO NOTHING",
            [path],
        )?;
    }
    db.internal_execute(
        "DELETE FROM _coven_file_chunks
         WHERE table_name=?1 AND key=?2 AND column_name=?3 AND identity=?4",
        (&table, &key, file.column(), &identity),
    )?;
    if db.internal_execute("DELETE FROM _coven_file_uploads WHERE id=?1", [id])? != 1 {
        return Err(DbError::DamagedDatabase);
    }
    Ok(())
}

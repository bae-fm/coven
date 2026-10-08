//! Durable file upload facts; provider session bytes remain opaque to SQLite.

use crate::{sqlite::DatabaseConnection, write_schema::WriteSchema, DbError, FileRef};
use coven_crypto::SecretBytes;
use coven_format::file::FileHeader;
use std::time::SystemTime;

/// One durable upload; its plaintext chunk hashes are owned child rows.
#[derive(Debug)]
pub struct FileUpload {
    /// Local queue identity, stable across retries.
    pub id: i64,
    /// The exact row version queued by its attaching write.
    pub file: FileRef,
    /// When this device queued the file.
    pub queued_at: SystemTime,
    /// Failed attempts so far.
    pub attempts: u64,
    /// When the latest attempt began.
    pub last_attempt_at: Option<SystemTime>,
    /// Sync's typed failure recording, interpreted only by sync.
    pub failure: Option<SecretBytes>,
    /// Sync's encoded independent file id and key, fixed before contacting storage.
    pub identity: Option<SecretBytes>,
    /// Opaque provider session, interpreted only by storage.
    pub session: Option<SecretBytes>,
    /// The provider has confirmed complete publication.
    pub stored: bool,
    /// The row changed; this stored object is retained for later deletion.
    pub unused: bool,
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
    let inserted = db.internal_execute("INSERT INTO _coven_file_uploads(reference,queued_at) VALUES(?1,?2) ON CONFLICT(reference) DO NOTHING", (file.encode()?, crate::user_file::encode_time(now)))?;
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
    now: SystemTime,
) -> Result<(), DbError> {
    for (table, key) in keys {
        match crate::file_ref::read(
            db,
            schema,
            &table,
            &crate::file_row::key(&(table.clone(), key))?,
        ) {
            Ok(reference) => enqueue(db, schema, &reference, now)?,
            Err(DbError::FileAbsent { .. }) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

pub(crate) fn read(db: &DatabaseConnection) -> Result<Vec<FileUpload>, DbError> {
    let rows = db.query("SELECT id,reference,queued_at,attempts,last_attempt_at,failure,identity,session,stored,unused FROM _coven_file_uploads ORDER BY id", [], |r| {
        Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?, r.get::<_, Vec<u8>>(2)?, r.get::<_, i64>(3)?, r.get::<_, Option<Vec<u8>>>(4)?, r.get::<_, Option<Vec<u8>>>(5)?, r.get::<_, Option<Vec<u8>>>(6)?, r.get::<_, Option<Vec<u8>>>(7)?, r.get::<_, bool>(8)?, r.get::<_, bool>(9)?))
    })?;
    rows.into_iter()
        .map(
            |(
                id,
                reference,
                queued,
                attempts,
                last,
                failure,
                identity,
                session,
                stored,
                unused,
            )| {
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
                    identity: identity.map(SecretBytes::new),
                    session: session.map(SecretBytes::new),
                    stored,
                    unused,
                })
            },
        )
        .collect()
}

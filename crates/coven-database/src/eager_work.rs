//! Eager-fill work from changed file references, kept in the transaction
//! that changes them, so no pass rescans every eager row (§17.6, §6.7).

use crate::FileRefJournal;
use crate::{
    file_queue, file_ref, sqlite::DatabaseConnection, write_rows::AppKey,
    write_schema::WriteSchema, CacheFill, DbError, FileRef,
};
use coven_foundation::id_source::DeviceId;

/// Note an eager row's current file. A different fixed file than the one last
/// noted becomes eager work; editing other columns, or evicting a completed
/// fill, requests nothing again. A file this device still has queued is
/// already local and waits for its upload's completion instead.
pub(crate) fn note(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    device: DeviceId,
    key: &AppKey,
) -> Result<(), DbError> {
    let Some(file) = &schema.declaration(&key.0).files else {
        return Ok(());
    };
    if file.fill != CacheFill::CacheEager {
        return Ok(());
    }
    let reference = match file_ref::read(db, schema, &key.0, &crate::file_row::key(key)?) {
        Ok(reference) => reference,
        Err(DbError::FileAbsent { .. } | DbError::Sqlite(rusqlite::Error::QueryReturnedNoRows)) => {
            db.internal_execute(
                "DELETE FROM _coven_eager_rows WHERE table_name=?1 AND key=?2",
                (&key.0, &key.1),
            )?;
            return Ok(());
        }
        Err(error) => return Err(error),
    };
    let identity = crate::database::file_database::cache_id(&reference)?;
    let previous = db
        .query(
            "SELECT file FROM _coven_eager_rows WHERE table_name=?1 AND key=?2",
            (&key.0, &key.1),
            |r| r.get::<_, String>(0),
        )?
        .pop();
    if previous.as_deref() == Some(identity.as_str()) {
        // The same file under a newer row version: pending work keeps the
        // reference a download validates against.
        db.internal_execute(
            "UPDATE _coven_eager_work SET reference=?3 WHERE namespace=?1 AND file_id=?2",
            (reference.namespace(), &identity, reference.encode()?),
        )?;
        return Ok(());
    }
    db.internal_execute(
        "INSERT INTO _coven_eager_rows(table_name,key,file) VALUES(?1,?2,?3)
         ON CONFLICT(table_name,key) DO UPDATE SET file=excluded.file",
        (&key.0, &key.1, &identity),
    )?;
    if reference.uploader() != device || !file_queue::queued(db, &reference)? {
        request(db, &reference)?;
    }
    Ok(())
}

/// Make one fixed reference eager work.
pub(crate) fn request(db: &DatabaseConnection, reference: &FileRef) -> Result<(), DbError> {
    db.internal_execute(
        "INSERT INTO _coven_eager_work(namespace,file_id,reference) VALUES(?1,?2,?3)
         ON CONFLICT(namespace,file_id) DO UPDATE SET reference=excluded.reference",
        (
            reference.namespace(),
            crate::database::file_database::cache_id(reference)?,
            reference.encode()?,
        ),
    )?;
    Ok(())
}

/// Pending eager work: changed references and temporary failures awaiting
/// their retry wait.
pub(crate) fn read(db: &DatabaseConnection) -> Result<Vec<FileRef>, DbError> {
    db.query(
        "SELECT reference FROM _coven_eager_work ORDER BY namespace,file_id",
        [],
        |r| r.get::<_, Vec<u8>>(0),
    )?
    .iter()
    .map(|bytes| FileRef::decode(bytes))
    .collect()
}

/// The work is settled: downloaded, permanently refused or cancelled.
pub(crate) fn settle(db: &DatabaseConnection, reference: &FileRef) -> Result<(), DbError> {
    db.internal_execute(
        "DELETE FROM _coven_eager_work WHERE namespace=?1 AND file_id=?2",
        (
            reference.namespace(),
            crate::database::file_database::cache_id(reference)?,
        ),
    )?;
    Ok(())
}

/// An upload this device completed leaves its file to the cache: if an eager
/// row still names it, it becomes eager work like a changed reference.
pub(crate) fn uploaded(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    reference: &FileRef,
) -> Result<(), DbError> {
    let Some(file) = schema
        .declarations
        .iter()
        .find(|declaration| declaration.name == reference.table())
        .and_then(|declaration| declaration.files.as_ref())
    else {
        return Ok(());
    };
    if file.fill != CacheFill::CacheEager {
        return Ok(());
    }
    let current = match file_ref::read(db, schema, reference.table(), reference.key()) {
        Ok(current) => current,
        Err(DbError::FileAbsent { .. } | DbError::Sqlite(rusqlite::Error::QueryReturnedNoRows)) => {
            return Ok(())
        }
        Err(error) => return Err(error),
    };
    if crate::database::file_database::cache_id(&current)?
        == crate::database::file_database::cache_id(reference)?
    {
        request(db, &current)?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "eager_work_tests.rs"]
mod tests;

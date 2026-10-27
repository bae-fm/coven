//! Pending attachments (§17.2): this device's record of a file it attached,
//! until the write completing its upload names the file in the row, or a
//! newer change to the row's file abandons it. Abandonments stay listed until
//! the app dismisses them.

use crate::{
    file_row, merge_store::MergeStore, sqlite::DatabaseConnection, write_rows::AppKey,
    write_schema::WriteSchema, DbError, FileRef, FileRefJournal, RowKey,
};
use coven_format::value::Value;
use coven_foundation::id_source::FileId;
use coven_merge::RowId;

/// Why an attachment was abandoned (§17.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AbandonReason {
    /// The row now names another file, or another file was attached to it.
    Replaced,
    /// The row's file was cleared or its file columns changed.
    Cleared,
    /// The row was deleted, moved to another audience or taken out.
    RowGone,
}

impl AbandonReason {
    fn text(self) -> &'static str {
        match self {
            Self::Replaced => "replaced",
            Self::Cleared => "cleared",
            Self::RowGone => "gone",
        }
    }

    fn parse(text: &str) -> Result<Self, DbError> {
        match text {
            "replaced" => Ok(Self::Replaced),
            "cleared" => Ok(Self::Cleared),
            "gone" => Ok(Self::RowGone),
            _ => Err(DbError::DamagedDatabase),
        }
    }
}

/// Reads the `reason` column of `coven_abandoned` (api.md E4).
impl rusqlite::types::FromSql for AbandonReason {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        Self::parse(value.as_str()?)
            .map_err(|error| rusqlite::types::FromSqlError::Other(Box::new(error)))
    }
}

/// A file this device attached whose row took a newer change before its
/// upload completed, so no row ever named it (§17.2, E8). Its stored bytes,
/// if any, are deleted under §17.8.
#[derive(Clone, Debug, PartialEq)]
pub struct AbandonedAttachment {
    /// The row's table.
    pub table: String,
    /// The row's key.
    pub key: RowKey,
    /// The file's fixed id; this device uploaded it.
    pub file: FileId,
    /// Why the row didn't take it.
    pub reason: AbandonReason,
}

/// The row as a pending attachment compares it: where it is, its merged
/// generation, and the writes that set its four file cells.
struct Observed {
    row: RowId,
    generation: u64,
    setters: Vec<u8>,
    has_file: bool,
}

/// The row's current state, or `None` when it isn't in the app's table.
fn observe(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    key: &AppKey,
) -> Result<Option<Observed>, DbError> {
    let (_, file) = file_row::declaration(schema, &key.0)?;
    let visible = crate::write_rows::AppView::after(db, schema);
    let Some(app) = visible.row(key)? else {
        return Ok(None);
    };
    let row = crate::write_rows::row_id(key, &app);
    let stored = MergeStore::new(db, &visible).row(&row)?;
    let setters = file
        .columns()
        .into_iter()
        .map(|column| match stored.state.cells().get(column) {
            Some(cell) => Ok(Value::Blob(coven_format::merge_fields::encode_write_id(
                &cell.write,
            )?)),
            None => Ok(Value::Null),
        })
        .collect::<Result<Vec<_>, DbError>>()?;
    Ok(Some(Observed {
        has_file: app.values[&file.hash] != Value::Null,
        generation: stored.state.generation(),
        setters: coven_format::key::encode_key(&setters)?,
        row,
    }))
}

/// One pending attachment and the row state it was made against.
pub(crate) struct Link {
    pub(crate) upload: i64,
    pub(crate) key: AppKey,
    audience: String,
    generation: u64,
    setters: Vec<u8>,
}

/// A link row: upload, table, key, audience, generation and setters.
type LinkRow = (i64, String, Vec<u8>, String, Vec<u8>, Vec<u8>);

fn link(row: &rusqlite::Row<'_>) -> rusqlite::Result<LinkRow> {
    Ok((
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
        row.get(5)?,
    ))
}

#[expect(
    clippy::map_err_ignore,
    reason = "a stored value that won't convert is DamagedDatabase"
)]
fn links(
    db: &DatabaseConnection,
    sql: &str,
    params: impl rusqlite::Params,
) -> Result<Option<Link>, DbError> {
    db.query(sql, params, link)?
        .pop()
        .map(|(upload, table, key, audience, generation, setters)| {
            Ok(Link {
                upload,
                key: (table, key),
                audience,
                generation: u64::from_be_bytes(
                    generation
                        .try_into()
                        .map_err(|_| DbError::DamagedDatabase)?,
                ),
                setters,
            })
        })
        .transpose()
}

fn for_row(db: &DatabaseConnection, key: &AppKey) -> Result<Option<Link>, DbError> {
    links(
        db,
        "SELECT upload,table_name,key,audience,generation,setters FROM _coven_pending_attachments
         WHERE table_name=?1 AND key=?2",
        (&key.0, &key.1),
    )
}

pub(crate) fn for_upload(db: &DatabaseConnection, upload: i64) -> Result<Option<Link>, DbError> {
    links(
        db,
        "SELECT upload,table_name,key,audience,generation,setters FROM _coven_pending_attachments
         WHERE upload=?1",
        [upload],
    )
}

/// Whether the row is still the one the attachment was made against: there,
/// in the same audience and generation, its file cells set by the same
/// writes. Otherwise, why not.
pub(crate) fn review(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    link: &Link,
) -> Result<Option<AbandonReason>, DbError> {
    Ok(match observe(db, schema, &link.key)? {
        None => Some(AbandonReason::RowGone),
        Some(row)
            if crate::write_encoding::audience_text(&row.row.audience) != link.audience
                || row.generation != link.generation =>
        {
            Some(AbandonReason::RowGone)
        }
        Some(row) if row.setters != link.setters => Some(if row.has_file {
            AbandonReason::Replaced
        } else {
            AbandonReason::Cleared
        }),
        Some(_) => None,
    })
}

/// The queued reference a link waits for.
fn queued(db: &DatabaseConnection, upload: i64) -> Result<FileRef, DbError> {
    let bytes = db
        .query(
            "SELECT reference FROM _coven_file_uploads WHERE id=?1",
            [upload],
            |r| r.get::<_, Vec<u8>>(0),
        )?
        .pop()
        .ok_or(DbError::Invariant {
            what: "a pending attachment's upload is queued",
        })?;
    FileRef::decode(&bytes)
}

/// Abandon the attachment and record why. Its upload stays queued until a
/// step that knows no attempt is in flight retires it (§17.7).
pub(crate) fn abandon(
    db: &DatabaseConnection,
    link: &Link,
    reason: AbandonReason,
) -> Result<(), DbError> {
    let file = queued(db, link.upload)?;
    db.internal_execute(
        "INSERT INTO _coven_abandoned_attachments(table_name,key,device,file,reason)
         VALUES(?1,?2,?3,?4,?5) ON CONFLICT DO NOTHING",
        (
            &link.key.0,
            &link.key.1,
            file.uploader().0.to_be_bytes(),
            file.id().0.as_bytes(),
            reason.text(),
        ),
    )?;
    db.internal_execute(
        "DELETE FROM _coven_pending_attachments WHERE upload=?1",
        [link.upload],
    )?;
    Ok(())
}

/// Abandon the row's pending attachment, if it has one, for this reason.
pub(crate) fn abandon_row(
    db: &DatabaseConnection,
    key: &AppKey,
    reason: AbandonReason,
) -> Result<(), DbError> {
    match for_row(db, key)? {
        Some(link) => abandon(db, &link, reason),
        None => Ok(()),
    }
}

/// Record the attachment queued as `upload` against the row as the attaching
/// write left it, replacing the row's earlier pending attachment (§17.2).
pub(crate) fn record(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    key: &AppKey,
    upload: i64,
) -> Result<(), DbError> {
    abandon_row(db, key, AbandonReason::Replaced)?;
    let row = observe(db, schema, key)?.ok_or(DbError::Invariant {
        what: "an attachment's row is in the app's table",
    })?;
    db.internal_execute(
        "INSERT INTO _coven_pending_attachments(upload,table_name,key,audience,generation,setters)
         VALUES(?1,?2,?3,?4,?5,?6)",
        (
            upload,
            &key.0,
            &key.1,
            crate::write_encoding::audience_text(&row.row.audience),
            row.generation.to_be_bytes().as_slice(),
            &row.setters,
        ),
    )?;
    Ok(())
}

/// The transaction that changes a row abandons its pending attachment when
/// the change touched the row's file (§17.2).
pub(crate) fn review_row(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    key: &AppKey,
) -> Result<(), DbError> {
    let Some(link) = for_row(db, key)? else {
        return Ok(());
    };
    match review(db, schema, &link)? {
        Some(reason) => abandon(db, &link, reason),
        None => Ok(()),
    }
}

/// Whether any pending attachment no longer matches its row, so a review
/// would abandon it. Reads only.
pub(crate) fn any_stale(db: &DatabaseConnection, schema: &WriteSchema) -> Result<bool, DbError> {
    let uploads = db.query(
        "SELECT upload FROM _coven_pending_attachments ORDER BY upload",
        [],
        |r| r.get::<_, i64>(0),
    )?;
    for upload in uploads {
        if let Some(link) = for_upload(db, upload)? {
            if review(db, schema, &link)?.is_some() {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

/// Review every pending attachment, as the upload worker does before it
/// serves the queue: a change no transaction reviewed, such as a reloaded
/// snapshot, abandons it there.
pub(crate) fn review_all(db: &DatabaseConnection, schema: &WriteSchema) -> Result<(), DbError> {
    let uploads = db.query(
        "SELECT upload FROM _coven_pending_attachments ORDER BY upload",
        [],
        |r| r.get::<_, i64>(0),
    )?;
    for upload in uploads {
        if let Some(link) = for_upload(db, upload)? {
            if let Some(reason) = review(db, schema, &link)? {
                abandon(db, &link, reason)?;
            }
        }
    }
    Ok(())
}

/// The row's current pending attachment, as the file it names on this
/// device (§17.2).
pub(crate) fn current_reference(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    key: &AppKey,
) -> Result<Option<FileRef>, DbError> {
    let Some(link) = for_row(db, key)? else {
        return Ok(None);
    };
    if review(db, schema, &link)?.is_some() {
        return Ok(None);
    }
    queued(db, link.upload).map(Some)
}

/// This device's abandoned attachments, oldest first, as `coven_abandoned`
/// lists them, so the call and the app's SQL agree.
pub(crate) fn abandoned(db: &DatabaseConnection) -> Result<Vec<AbandonedAttachment>, DbError> {
    db.query(
        "SELECT table_name,key,file,reason FROM coven_abandoned ORDER BY position",
        [],
        |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, Vec<u8>>(1)?,
                r.get::<_, FileId>(2)?,
                r.get::<_, AbandonReason>(3)?,
            ))
        },
    )?
    .into_iter()
    .map(|(table, key, file, reason)| {
        let key = file_row::key(&(table.clone(), key))?;
        Ok(AbandonedAttachment {
            table,
            key,
            file,
            reason,
        })
    })
    .collect()
}

/// Forget these records; ones already gone are ignored.
pub(crate) fn dismiss(
    db: &DatabaseConnection,
    records: &[AbandonedAttachment],
) -> Result<(), DbError> {
    for record in records {
        db.internal_execute(
            "DELETE FROM _coven_abandoned_attachments WHERE file=?1",
            [record.file.0.as_bytes()],
        )?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "pending_attachment_tests.rs"]
mod tests;

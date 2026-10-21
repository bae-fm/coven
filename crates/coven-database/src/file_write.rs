//! A write's file state: bytes first, SQL commit, then obsolete owned bytes.

use crate::{
    file_row,
    sqlite::DatabaseConnection,
    write_capture::CapturedRow,
    write_rows::{AppKey, AppView},
    write_schema::WriteSchema,
    DbError, PreparedUserFile, Provenance, RowKey,
};
use coven_crypto::ContentHash;
use coven_format::value::Value;
use coven_foundation::{
    files::FileName,
    id_source::{DeviceId, IdSource},
};
use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
};

pub(crate) struct StagedFile {
    pub(crate) namespace: String,
    pub(crate) id: String,
    pub(crate) name: FileName,
    pub(crate) size: u64,
    pub(crate) hashes: crate::file_hashes::FileHashes,
}

#[path = "file_write_sources.rs"]
pub(crate) mod sources;

#[cfg(test)]
#[path = "file_write_tests.rs"]
pub(crate) mod tests;

/// A file this write attached: the row's pending attachment once it commits
/// (§17.2).
struct AttachedFile {
    id: Value,
    size: u64,
    hash: ContentHash,
    reference: file_row::FixedReference,
    queued: crate::file_queue::Attachment,
}

/// What the transaction completing an upload does once its write applied:
/// retire the upload, and keep the file here as its pin asks (§17.7).
/// It learns the queued file, whether it was pinned, and whether the
/// completion wrote the row.
pub(crate) type AfterCompletion =
    Box<dyn FnOnce(&DatabaseConnection, &crate::FileRef, bool, bool) -> Result<(), DbError>>;

struct Completion {
    upload: i64,
    /// The row the completing write names the file in, if it does.
    written: Option<AppKey>,
    after: AfterCompletion,
}

pub(crate) struct FileWrite<'a> {
    database: &'a DatabaseConnection,
    schema: &'a WriteSchema,
    device: DeviceId,
    ids: &'a dyn IdSource,
    staged: Vec<StagedFile>,
    attached: RefCell<BTreeMap<AppKey, AttachedFile>>,
    cleared: RefCell<BTreeSet<AppKey>>,
    completion: RefCell<Option<Completion>>,
    obsolete: RefCell<BTreeSet<FileName>>,
    changed_reference: RefCell<Option<(String, RowKey)>>,
}

impl<'a> FileWrite<'a> {
    pub(crate) fn new(
        database: &'a DatabaseConnection,
        schema: &'a WriteSchema,
        device: DeviceId,
        ids: &'a dyn IdSource,
        staged: Vec<StagedFile>,
    ) -> Self {
        Self {
            database,
            schema,
            device,
            ids,
            staged,
            attached: RefCell::new(BTreeMap::new()),
            cleared: RefCell::new(BTreeSet::new()),
            completion: RefCell::new(None),
            obsolete: RefCell::new(BTreeSet::new()),
            changed_reference: RefCell::new(None),
        }
    }

    pub(crate) fn execute<P: rusqlite::Params>(
        &self,
        sql: &str,
        params: P,
    ) -> rusqlite::Result<usize> {
        self.database.app_execute(sql, params)
    }

    pub(crate) fn execute_batch(&self, sql: &str) -> rusqlite::Result<()> {
        self.database.app_batch(sql)
    }

    pub(crate) fn query_row<T, P: rusqlite::Params>(
        &self,
        sql: &str,
        params: P,
        map: impl FnOnce(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    ) -> rusqlite::Result<T> {
        self.database.app_query_row(sql, params, map)
    }

    pub(crate) fn query<T, P: rusqlite::Params>(
        &self,
        sql: &str,
        params: P,
        map: impl FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    ) -> rusqlite::Result<Vec<T>> {
        self.database.app_query(sql, params, map)
    }

    pub(crate) fn attach_app_files(&self) -> Result<(), DbError> {
        let db = self.database;
        for staged in &self.staged {
            let mut attached = false;
            for declaration in &self.schema.declarations {
                let Some(file) = &declaration.files else {
                    continue;
                };
                if file.namespace != staged.namespace || file.provenance != Provenance::AppProvided
                {
                    continue;
                }
                let table = self.schema.table(&declaration.name);
                let rows = db.query(
                    &format!(
                        "SELECT * FROM main.{} WHERE {}=?1",
                        crate::sql::identifier(&table.name),
                        crate::sql::identifier(&file.id)
                    ),
                    [&staged.id],
                    |r| crate::write_rows::read_row(table, r),
                )?;
                for values in rows {
                    let key = (
                        table.name.clone(),
                        crate::write_rows::row_key(table, &values)?,
                    );
                    // The queue takes the copy and its hashes when the write
                    // commits; until then the staged name protects the bytes.
                    self.attached.borrow_mut().insert(
                        key,
                        AttachedFile {
                            id: values[&file.id].clone(),
                            size: staged.size,
                            hash: staged.hashes.content,
                            reference: file_row::new_reference(self.device, self.ids)?,
                            queued: crate::file_queue::Attachment {
                                source: crate::file_queue::Source::Copy(staged.name.clone()),
                                chunks: staged.hashes.chunks().to_vec(),
                            },
                        },
                    );
                    attached = true;
                }
            }
            if !attached {
                return Err(DbError::FileUnreferenced {
                    namespace: staged.namespace.clone(),
                    id: staged.id.clone(),
                });
            }
        }
        Ok(())
    }

    /// Each attached row keeps the file it had before the write until the
    /// upload completes (§17.2); a row the write deleted takes no attachment.
    pub(crate) fn keep_files(
        &self,
        captured: &BTreeMap<AppKey, CapturedRow>,
    ) -> Result<(), DbError> {
        let db = self.database;
        let before = AppView::before(db, self.schema, captured)?;
        let keys = self.attached.borrow().keys().cloned().collect::<Vec<_>>();
        for key in keys {
            let table = self.schema.table(&key.0);
            if crate::write_rows::read_values(db, table, &key.1)?.is_some() {
                let old = before.row(&key)?;
                file_row::keep(db, self.schema, &key, old.as_ref().map(|row| &row.values))?;
                // A shared trigger removing the row refuses the attachment.
                if crate::write_rows::read_values(db, table, &key.1)?.is_none() {
                    return Err(DbError::FileRowRemoved {
                        table: key.0.clone(),
                        key: file_row::key(&key)?,
                    });
                }
            }
        }
        Ok(())
    }

    pub(crate) fn register(
        &self,
        table: &str,
        key: RowKey,
        prepared: PreparedUserFile,
    ) -> Result<(), DbError> {
        let db = self.database;
        let (_, file) = file_row::declaration(self.schema, table)?;
        if file.provenance != Provenance::UserProvided {
            return Err(DbError::FileTableNotUserProvided {
                table: table.into(),
            });
        }
        let (key, values) = file_row::lookup(db, self.schema, table, &key)?;
        file_row::check_size(&values, file, prepared.observed.size())?;
        if values[&file.id] == Value::Null {
            return Err(DbError::FileIdMissing {
                column: file.id.clone(),
            });
        }
        let reference = file_row::new_reference(self.device, self.ids)?;
        let size =
            i64::try_from(prepared.observed.size()).map_err(|_overflow| DbError::TooLarge {
                field: "file size",
                actual: prepared.observed.size(),
                maximum: i64::MAX as u64,
            })?;
        let identity = file_row::identity_values(
            &values[&file.id],
            &Value::Integer(size),
            &Value::Blob(prepared.hashes.content.as_bytes().to_vec()),
            &reference,
        )?;
        db.internal_execute(
            "INSERT INTO _coven_user_files
                 (table_name,key,column_name,identity,path,size,modified_at)
             VALUES(?1,?2,?3,?4,?5,?6,?7)
             ON CONFLICT(table_name,key,column_name) DO UPDATE SET
                 identity=excluded.identity,path=excluded.path,
                 size=excluded.size,modified_at=excluded.modified_at",
            rusqlite::params![
                key.0,
                key.1,
                file.id,
                identity,
                crate::user_file::encode_path(prepared.observed.path()),
                prepared.observed.size().to_be_bytes().as_slice(),
                crate::user_file::encode_time(prepared.observed.modified_at())
            ],
        )?;
        self.attached.borrow_mut().insert(
            key,
            AttachedFile {
                id: values[&file.id].clone(),
                size: prepared.observed.size(),
                hash: prepared.hashes.content,
                reference,
                queued: crate::file_queue::Attachment {
                    source: crate::file_queue::Source::Original {
                        path: crate::user_file::encode_path(prepared.observed.path()),
                        modified_at: crate::user_file::encode_time(prepared.observed.modified_at()),
                    },
                    chunks: prepared.hashes.chunks().to_vec(),
                },
            },
        );
        Ok(())
    }

    pub(crate) fn clear(&self, table: &str, key: RowKey) -> Result<(), DbError> {
        let db = self.database;
        let (_, file) = file_row::declaration(self.schema, table)?;
        if file.provenance != Provenance::UserProvided {
            return Err(DbError::FileTableNotUserProvided {
                table: table.into(),
            });
        }
        let (key, _) = file_row::lookup(db, self.schema, table, &key)?;
        file_row::clear(db, self.schema, &key)?;
        // Clearing the row's file abandons a file attached earlier (§17.2),
        // even when the row had none yet, and forgets one attached in this
        // write before the clear.
        crate::pending_attachment::abandon_row(
            db,
            &key,
            crate::pending_attachment::AbandonReason::Cleared,
        )?;
        self.attached.borrow_mut().remove(&key);
        self.cleared.borrow_mut().insert(key);
        Ok(())
    }

    pub(crate) fn clear_null_ids(
        &self,
        captured: &BTreeMap<AppKey, CapturedRow>,
    ) -> Result<(), DbError> {
        let db = self.database;
        for key in captured.keys() {
            let Some(file) = &self.schema.declaration(&key.0).files else {
                continue;
            };
            if let Some(values) =
                crate::write_rows::read_values(db, self.schema.table(&key.0), &key.1)?
            {
                if values[&file.id] == Value::Null && values[&file.hash] != Value::Null {
                    file_row::clear(db, self.schema, key)?;
                    self.cleared.borrow_mut().insert(key.clone());
                }
            }
        }
        Ok(())
    }

    pub(crate) fn validate(&self, captured: &BTreeMap<AppKey, CapturedRow>) -> Result<(), DbError> {
        let db = self.database;
        let before = AppView::before(db, self.schema, captured)?;
        let after = AppView::after(db, self.schema);
        for key in captured.keys() {
            let Some(file) = &self.schema.declaration(&key.0).files else {
                continue;
            };
            let old = before.row(key)?;
            let Some(new) = after.row(key)? else {
                continue;
            };
            let reference = |r: &crate::write_rows::AppRow| {
                (
                    r.values[&file.hash].clone(),
                    r.values[&file.location].clone(),
                )
            };
            let none = (Value::Null, Value::Null);
            let had = old.as_ref().map_or(none.clone(), reference);
            let has = reference(&new);
            // The completing write names the file, all four columns of it.
            if self
                .completion
                .borrow()
                .as_ref()
                .is_some_and(|completion| completion.written.as_ref() == Some(key))
            {
                continue;
            }
            // Only coven writes the content hash and reference: a completing
            // write names a file, and a clear removes one (§17.2).
            if has != had && !(has == none && self.cleared.borrow().contains(key)) {
                return Err(DbError::FileColumnWrite {
                    table: key.0.clone(),
                    column: file.hash.clone(),
                });
            }
            if (has.0 == Value::Null) != (has.1 == Value::Null) {
                return Err(DbError::FileHashLocationMismatch {
                    table: key.0.clone(),
                    key: file_row::key(key)?,
                });
            }
            let attached = self.attached.borrow();
            let attachment = attached.get(key);
            if has.0 != Value::Null {
                if new.values[&file.id] == Value::Null {
                    return Err(DbError::FileIdMissing {
                        column: file.id.clone(),
                    });
                }
                // The row keeps its file whole until a completing write
                // replaces all four columns (§17.2).
                let old = old.as_ref().expect("a kept file was the row's before");
                if [&file.id, &file.size]
                    .iter()
                    .any(|c| old.values[*c] != new.values[*c])
                {
                    return Err(DbError::FileBytesRequired {
                        table: key.0.clone(),
                        key: file_row::key(key)?,
                    });
                }
                if attachment.is_some() && file.write_once {
                    return Err(DbError::FileWriteOnce {
                        table: key.0.clone(),
                        key: file_row::key(key)?,
                    });
                }
            } else if let Some(attachment) = attachment {
                // A pending file counts as the row's file here.
                if file.write_once
                    && crate::pending_attachment::current_reference(db, self.schema, key)?.is_some()
                {
                    return Err(DbError::FileWriteOnce {
                        table: key.0.clone(),
                        key: file_row::key(key)?,
                    });
                }
                if attachment.id == Value::Null {
                    return Err(DbError::FileIdMissing {
                        column: file.id.clone(),
                    });
                }
                // A row the write inserted keeps the app's id and size, which
                // must still be the attached file's.
                if old.is_none() {
                    if file.provenance == Provenance::UserProvided {
                        file_row::check_size(&new.values, file, attachment.size)?;
                    }
                    if new.values[&file.id] != attachment.id {
                        return Err(DbError::FileAttachmentChanged {
                            table: key.0.clone(),
                            key: file_row::key(key)?,
                        });
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) fn validate_file_ref(&self, reference: &crate::FileRef) -> Result<(), DbError> {
        let result = crate::file_ref::validate(self.database, self.schema, reference);
        if let Err(DbError::FileRefChanged { table, key }) = &result {
            *self.changed_reference.borrow_mut() = Some((table.clone(), key.clone()));
        }
        result
    }

    /// Queue each file this write attached and record it as its row's
    /// pending attachment, atomically with the write (§17.2). A file
    /// attached and then removed by the same write never reaches the queue;
    /// its staged copy is released when the write commits.
    pub(crate) fn queue_attached(&self, now: std::time::SystemTime) -> Result<(), DbError> {
        let db = self.database;
        for (key, attached) in self.attached.borrow().iter() {
            let (_, file) = file_row::declaration(self.schema, &key.0)?;
            let visible = AppView::after(db, self.schema);
            let Some(app) = visible.row(key)? else {
                continue;
            };
            let row = crate::write_rows::row_id(key, &app);
            let generation = crate::merge_store::MergeStore::new(db, &visible)
                .row(&row)?
                .state
                .generation();
            let reference = crate::FileRef::pending(
                row,
                file_row::key(key)?,
                file.id.clone(),
                file.namespace.clone(),
                generation,
                attached.id.clone(),
                attached.size,
                attached.hash,
                attached.reference.clone(),
            );
            // The attaching write may record no row change at all, leaving
            // no row-write part on which to check membership.
            crate::write_commit::check_membership(db, self.device, &reference.audience())?;
            let upload = crate::file_queue::enqueue(db, &reference, &attached.queued, now)?;
            crate::pending_attachment::record(db, self.schema, key, upload)?;
        }
        self.finish_completion()
    }

    /// The app phase of the transaction completing upload `upload` (§17.2,
    /// §17.7): if the row's pending attachment is still current, write the
    /// file's four columns into the row as this write; otherwise abandon it.
    /// `after` runs once the write has applied.
    pub(crate) fn complete_upload(
        &self,
        upload: i64,
        after: AfterCompletion,
    ) -> Result<(), DbError> {
        let db = self.database;
        let (reference, _) = crate::file_queue::entry(db, upload)?;
        let mut written = None;
        if let Some(link) = crate::pending_attachment::for_upload(db, upload)? {
            match crate::pending_attachment::review(db, self.schema, &link)? {
                None => {
                    let (id, size, hash, location) = reference.columns();
                    // The link goes first, so the review this write's own
                    // row change gets finds nothing to abandon.
                    db.internal_execute(
                        "DELETE FROM _coven_pending_attachments WHERE upload=?1",
                        [upload],
                    )?;
                    file_row::write(db, self.schema, &link.key, id, size, hash, location)?;
                    // A shared trigger removing the row refuses the write;
                    // the upload stays queued, its failure visible.
                    if crate::write_rows::read_values(
                        db,
                        self.schema.table(&link.key.0),
                        &link.key.1,
                    )?
                    .is_none()
                    {
                        return Err(DbError::FileRowRemoved {
                            table: link.key.0.clone(),
                            key: file_row::key(&link.key)?,
                        });
                    }
                    written = Some(link.key.clone());
                }
                Some(reason) => crate::pending_attachment::abandon(db, &link, reason)?,
            }
        }
        *self.completion.borrow_mut() = Some(Completion {
            upload,
            written,
            after,
        });
        Ok(())
    }

    /// Retire a completing upload after its write applied: the queue row
    /// goes, its path a deletion candidate unless a row now names it.
    fn finish_completion(&self) -> Result<(), DbError> {
        let Some(completion) = self.completion.borrow_mut().take() else {
            return Ok(());
        };
        let (reference, pinned) = crate::file_queue::complete(self.database, completion.upload)?;
        (completion.after)(
            self.database,
            &reference,
            pinned,
            completion.written.is_some(),
        )
    }

    pub(crate) fn before_commit(&self) -> Result<(), DbError> {
        if let Some((table, key)) = self.changed_reference.borrow().as_ref() {
            return Err(DbError::FileRefChanged {
                table: table.clone(),
                key: key.clone(),
            });
        }
        let db = self.database;
        for file in &self.staged {
            self.obsolete.borrow_mut().insert(file.name.clone());
        }
        for name in self.obsolete.borrow().iter() {
            if db.query_row(
                "SELECT EXISTS(SELECT 1 FROM _coven_device_files WHERE path=?1)",
                [name.as_str()],
                |r| r.get::<_, bool>(0),
            )? {
                db.internal_execute(
                    "DELETE FROM _coven_file_removals WHERE path=?1",
                    [name.as_str()],
                )?;
            } else {
                db.internal_execute(
                    "INSERT INTO _coven_file_removals(path) VALUES(?1) ON CONFLICT DO NOTHING",
                    [name.as_str()],
                )?;
            }
        }
        Ok(())
    }
}

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
    files::{FileName, StoreDir},
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

#[cfg(test)]
#[path = "file_write_tests.rs"]
pub(crate) mod tests;

struct AttachedFile {
    id: Value,
    size: u64,
    hash: ContentHash,
    queued: crate::file_queue::Attachment,
}

pub(crate) struct FileWrite<'a> {
    database: &'a DatabaseConnection,
    directory: &'a StoreDir,
    schema: &'a WriteSchema,
    device: DeviceId,
    ids: &'a dyn IdSource,
    staging: &'a std::sync::Mutex<BTreeSet<FileName>>,
    staged: Vec<StagedFile>,
    originals: RefCell<Vec<PreparedUserFile>>,
    attached: RefCell<BTreeMap<AppKey, AttachedFile>>,
    obsolete: RefCell<BTreeSet<FileName>>,
    changed_reference: RefCell<Option<(String, RowKey)>>,
}

impl<'a> FileWrite<'a> {
    pub(crate) fn new(
        database: &'a DatabaseConnection,
        directory: &'a StoreDir,
        schema: &'a WriteSchema,
        device: DeviceId,
        ids: &'a dyn IdSource,
        staging: &'a std::sync::Mutex<BTreeSet<FileName>>,
        staged: Vec<StagedFile>,
    ) -> Self {
        Self {
            database,
            directory,
            schema,
            device,
            ids,
            staging,
            staged,
            originals: RefCell::new(Vec::new()),
            attached: RefCell::new(BTreeMap::new()),
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
                    file_row::set(
                        db,
                        self.schema,
                        &key,
                        Some((staged.hashes.content, staged.size)),
                        self.device,
                        self.ids,
                    )?;
                    if crate::write_rows::read_values(db, table, &key.1)?.is_none() {
                        return Err(DbError::FileRowRemoved {
                            table: key.0.clone(),
                            key: file_row::key(&key)?,
                        });
                    }
                    // The queue takes the copy and its hashes when the write
                    // commits; until then the staged name protects the bytes.
                    self.attached.borrow_mut().insert(
                        key,
                        AttachedFile {
                            id: values[&file.id].clone(),
                            size: staged.size,
                            hash: staged.hashes.content,
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
        prepared.observed.validate()?;
        let (key, values) = file_row::lookup(db, self.schema, table, &key)?;
        file_row::check_size(&values, file, prepared.observed.size())?;
        file_row::set(
            db,
            self.schema,
            &key,
            Some((prepared.hashes.content, prepared.observed.size())),
            self.device,
            self.ids,
        )?;
        let Some(attached_values) =
            crate::write_rows::read_values(db, self.schema.table(&key.0), &key.1)?
        else {
            return Err(DbError::FileRowRemoved {
                table: key.0.clone(),
                key: file_row::key(&key)?,
            });
        };
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
                file_row::identity(file, &attached_values)?,
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
                queued: crate::file_queue::Attachment {
                    source: crate::file_queue::Source::Original {
                        path: crate::user_file::encode_path(prepared.observed.path()),
                        modified_at: crate::user_file::encode_time(prepared.observed.modified_at()),
                    },
                    chunks: prepared.hashes.chunks().to_vec(),
                },
            },
        );
        self.originals.borrow_mut().push(prepared);
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
        file_row::set(db, self.schema, &key, None, self.device, self.ids)?;
        self.forget_original(&key, &file.id)?;
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
                    file_row::set(db, self.schema, key, None, self.device, self.ids)?;
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
            let new = after.row(key)?;
            let has_file = |r: &crate::write_rows::AppRow| r.values[&file.hash] != Value::Null;
            if let Some(new) = &new {
                if has_file(new) {
                    if new.values[&file.id] == Value::Null {
                        return Err(DbError::FileIdMissing {
                            column: file.id.clone(),
                        });
                    }
                    if let Some(attached) = self.attached.borrow().get(key) {
                        file_row::check_size(&new.values, file, attached.size)?;
                        if new.values[&file.id] != attached.id
                            || new.values[&file.hash]
                                != Value::Blob(attached.hash.as_bytes().to_vec())
                        {
                            return Err(DbError::FileAttachmentChanged {
                                table: key.0.clone(),
                                key: file_row::key(key)?,
                            });
                        }
                    }
                }
                if has_file(new) != (new.values[&file.location] != Value::Null) {
                    return Err(DbError::FileHashLocationMismatch {
                        table: key.0.clone(),
                        key: file_row::key(key)?,
                    });
                }
                if let Some(old) = old.as_ref().filter(|r| has_file(r)) {
                    if has_file(new) {
                        let different = [&file.id, &file.size, &file.hash]
                            .iter()
                            .any(|c| old.values[*c] != new.values[*c]);
                        if file.write_once && different {
                            return Err(DbError::FileWriteOnce {
                                table: key.0.clone(),
                                key: file_row::key(key)?,
                            });
                        }
                        if different && !self.attached.borrow().contains_key(key) {
                            return Err(DbError::FileBytesRequired {
                                table: key.0.clone(),
                                key: file_row::key(key)?,
                            });
                        }
                    }
                } else if has_file(new) && !self.attached.borrow().contains_key(key) {
                    return Err(DbError::FileColumnWrite {
                        table: key.0.clone(),
                        column: file.hash.clone(),
                    });
                }
            }
            if new.as_ref().is_none_or(|r| !has_file(r)) {
                self.forget_original(key, &file.id)?;
            }
        }
        Ok(())
    }

    /// Retain an original's recorded facts while a current row still names it,
    /// including rows temporarily removed by constraints and rows a circle
    /// deletion hides: that deletion is not a row delete and can still drop
    /// (§14.7). Historical generations cannot keep them. Audience moves keep a
    /// file's identity. The upload queue keeps its own source facts and owned
    /// copies independently of rows (§16.5).
    pub(crate) fn retain_rows(&self, keys: BTreeSet<AppKey>) -> Result<(), DbError> {
        let db = self.database;
        // Each file identity is checked once; retaining its app row would keep
        // every downloaded payload alive until this whole write finishes.
        let visible = AppView::after(db, self.schema).without_row_cache();
        let store = crate::merge_store::MergeStore::new(db, &visible).without_row_cache();
        for key in keys {
            crate::file_protection::refresh(db, self.schema, &key)?;
            let Some(file) = &self.schema.declaration(&key.0).files else {
                continue;
            };
            if file.provenance != Provenance::UserProvided {
                continue;
            }
            let identities = db.query(
                "SELECT identity FROM _coven_user_files
                 WHERE table_name=?1 AND key=?2 AND column_name=?3",
                (&key.0, &key.1, &file.id),
                |r| r.get::<_, Vec<u8>>(0),
            )?;
            if identities.is_empty() {
                continue;
            }
            let mut retained = BTreeSet::new();
            for (audience, generation) in db.query(
                "SELECT audience,max(generation) FROM _coven_rows
                 WHERE table_name=?1 AND key=?2 GROUP BY audience",
                (&key.0, &key.1),
                |r| {
                    Ok((
                        crate::write_encoding::audience(&r.get::<_, String>(0)?)?,
                        crate::write_encoding::counter(r.get(1)?),
                    ))
                },
            )? {
                if generation % 2 == 0 {
                    continue;
                }
                let row = store.row(&coven_merge::RowId {
                    table: key.0.clone(),
                    key: key.1.clone(),
                    audience,
                })?;
                let values = row
                    .state
                    .cells()
                    .iter()
                    .map(|(column, cell)| (column.clone(), cell.value.value.clone()))
                    .collect();
                if let Some(identity) = file_row::identity(file, &values)? {
                    retained.insert(identity);
                }
            }
            if identities
                .iter()
                .all(|identity| !retained.contains(identity))
            {
                self.forget_original(&key, &file.id)?;
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

    /// Queue each file this write attached, atomically with the write (§16.1).
    /// A file attached and then removed by the same write never reaches the
    /// queue; its staged copy is released when the write commits.
    pub(crate) fn queue_attached(&self, now: std::time::SystemTime) -> Result<(), DbError> {
        for (key, attached) in self.attached.borrow().iter() {
            let reference = match crate::file_ref::read(
                self.database,
                self.schema,
                &key.0,
                &file_row::key(key)?,
            ) {
                Ok(reference) => reference,
                Err(DbError::FileAbsent { .. }) => continue,
                Err(error) => return Err(error),
            };
            if reference.content_hash() != attached.hash
                || reference.plaintext_size() != attached.size
            {
                return Err(DbError::DamagedDatabase);
            }
            // Reattaching identical bytes can change only local file records,
            // leaving no row-write part on which to check membership.
            crate::write_commit::check_membership(
                self.database,
                self.device,
                &reference.audience(),
            )?;
            crate::file_queue::enqueue(self.database, &reference, &attached.queued, now)?;
        }
        Ok(())
    }

    pub(crate) fn before_commit(&self) -> Result<(), DbError> {
        if let Some((table, key)) = self.changed_reference.borrow().as_ref() {
            return Err(DbError::FileRefChanged {
                table: table.clone(),
                key: key.clone(),
            });
        }
        let db = self.database;
        for original in self.originals.borrow().iter() {
            original.observed.validate()?;
        }
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

    fn forget_original(&self, key: &AppKey, column: &str) -> Result<(), DbError> {
        self.database.internal_execute(
            "DELETE FROM _coven_user_files WHERE table_name=?1 AND key=?2 AND column_name=?3",
            (&key.0, &key.1, column),
        )?;
        Ok(())
    }

    pub(crate) fn finish<R, E: crate::WriteFailure>(&self, result: Result<R, E>) -> Result<R, E> {
        crate::file_removals::FileRemovals::new(
            self.database,
            self.directory,
            &self.staging.lock().expect("file staging lock poisoned"),
        )
        .finish(result)
    }

    pub(crate) fn rollback(&self) -> Result<(), Vec<DbError>> {
        let failures = crate::file_removals::FileRemovals::new(
            self.database,
            self.directory,
            &self.staging.lock().expect("file staging lock poisoned"),
        )
        .remove_unused();
        if failures.is_empty() {
            Ok(())
        } else {
            Err(failures)
        }
    }
}

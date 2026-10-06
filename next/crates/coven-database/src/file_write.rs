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
    id_source::DeviceId,
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
    pub(crate) hash: ContentHash,
}

#[cfg(test)]
#[path = "file_write_tests.rs"]
pub(crate) mod tests;

struct AttachedFile {
    id: Value,
    size: u64,
    hash: ContentHash,
}

pub(crate) struct FileWrite<'a> {
    database: &'a DatabaseConnection,
    directory: &'a StoreDir,
    schema: &'a WriteSchema,
    device: DeviceId,
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
        staging: &'a std::sync::Mutex<BTreeSet<FileName>>,
        staged: Vec<StagedFile>,
    ) -> Self {
        Self {
            database,
            directory,
            schema,
            device,
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
                    self.forget_owned(&key, &file.id)?;
                    file_row::set(
                        db,
                        self.schema,
                        &key,
                        Some((staged.hash, staged.size)),
                        self.device,
                    )?;
                    let Some(attached_values) = crate::write_rows::read_values(db, table, &key.1)?
                    else {
                        return Err(DbError::FileRowRemoved {
                            table: key.0.clone(),
                            key: file_row::key(&key)?,
                        });
                    };
                    db.internal_execute(
                        "INSERT INTO coven_device_files
                             (table_name,key,column_name,identity,path)
                         VALUES(?1,?2,?3,?4,?5)",
                        (
                            &key.0,
                            &key.1,
                            &file.id,
                            file_row::identity(file, &attached_values)?,
                            staged.name.as_str(),
                        ),
                    )?;
                    self.attached.borrow_mut().insert(
                        key,
                        AttachedFile {
                            id: values[&file.id].clone(),
                            size: staged.size,
                            hash: staged.hash,
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
            Some((prepared.hash, prepared.observed.size())),
            self.device,
        )?;
        let mut attached_values = values.clone();
        attached_values.insert(
            file.hash.clone(),
            Value::Blob(prepared.hash.as_bytes().to_vec()),
        );
        db.internal_execute(
            "INSERT INTO coven_user_files
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
                hash: prepared.hash,
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
        file_row::set(db, self.schema, &key, None, self.device)?;
        db.internal_execute(
            "DELETE FROM coven_user_files
             WHERE table_name=?1 AND key=?2 AND column_name=?3",
            (&key.0, &key.1, &file.id),
        )?;
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
                    file_row::set(db, self.schema, key, None, self.device)?;
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
                self.forget_owned(key, &file.id)?;
                db.internal_execute(
                    "DELETE FROM coven_user_files
                     WHERE table_name=?1 AND key=?2 AND column_name=?3",
                    (&key.0, &key.1, &file.id),
                )?;
            }
        }
        Ok(())
    }

    /// Retain originals and owned copies while a current row still names them,
    /// including rows temporarily removed by constraints. Historical generations
    /// and deleted circles cannot keep a file alive. Audience moves keep its identity.
    pub(crate) fn retain_rows(
        &self,
        keys: BTreeSet<AppKey>,
        deleted: &BTreeSet<coven_foundation::id_source::CircleId>,
    ) -> Result<(), DbError> {
        let db = self.database;
        let visible = AppView::after(db, self.schema);
        let store = crate::merge_store::MergeStore::new(db, &visible);
        for key in keys {
            let Some(file) = &self.schema.declaration(&key.0).files else {
                continue;
            };
            let local_table = match file.provenance {
                Provenance::AppProvided => "coven_device_files",
                Provenance::UserProvided => "coven_user_files",
            };
            let identities = db.query(
                &format!(
                    "SELECT identity FROM {local_table}
                     WHERE table_name=?1 AND key=?2 AND column_name=?3"
                ),
                (&key.0, &key.1, &file.id),
                |r| r.get::<_, Vec<u8>>(0),
            )?;
            if identities.is_empty() {
                continue;
            }
            let mut retained = BTreeSet::new();
            for (audience, generation) in db.query(
                "SELECT audience,max(generation) FROM coven_rows
                 WHERE table_name=?1 AND key=?2 GROUP BY audience",
                (&key.0, &key.1),
                |r| {
                    Ok((
                        crate::write_encoding::audience(&r.get::<_, String>(0)?)?,
                        crate::write_encoding::counter(r.get(1)?),
                    ))
                },
            )? {
                if generation % 2 == 0
                    || matches!(&audience, coven_merge::Audience::Circle(id) if deleted.contains(id))
                {
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
                    if file.provenance == Provenance::AppProvided
                        && crate::file_location::StoredLocation::decode(&values[&file.location])?
                            .public()
                            != crate::FileLocation::OnDevice(self.device)
                    {
                        continue;
                    }
                    retained.insert(identity);
                }
            }
            if identities
                .iter()
                .all(|identity| !retained.contains(identity))
            {
                match file.provenance {
                    Provenance::AppProvided => self.forget_owned(&key, &file.id)?,
                    Provenance::UserProvided => {
                        db.internal_execute(
                            "DELETE FROM coven_user_files
                             WHERE table_name=?1 AND key=?2 AND column_name=?3",
                            (&key.0, &key.1, &file.id),
                        )?;
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
                "SELECT EXISTS(SELECT 1 FROM coven_device_files WHERE path=?1)",
                [name.as_str()],
                |r| r.get::<_, bool>(0),
            )? {
                db.internal_execute(
                    "DELETE FROM coven_file_removals WHERE path=?1",
                    [name.as_str()],
                )?;
            } else {
                db.internal_execute(
                    "INSERT INTO coven_file_removals(path) VALUES(?1) ON CONFLICT DO NOTHING",
                    [name.as_str()],
                )?;
            }
        }
        Ok(())
    }

    fn forget_owned(&self, key: &AppKey, column: &str) -> Result<(), DbError> {
        let db = self.database;
        let paths = db.query(
            "DELETE FROM coven_device_files
             WHERE table_name=?1 AND key=?2 AND column_name=?3 RETURNING path",
            (&key.0, &key.1, column),
            |r| r.get::<_, String>(0),
        )?;
        for path in paths {
            self.obsolete
                .borrow_mut()
                .insert(FileName::new(path).map_err(|_| DbError::DamagedDatabase)?);
        }
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

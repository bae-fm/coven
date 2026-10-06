//! A write's file state: bytes first, SQL commit, then obsolete owned bytes.

use crate::{
    file_row,
    sqlite::DatabaseConnection,
    write_capture::CapturedRow,
    write_rows::{AppKey, AppView},
    write_schema::WriteSchema,
    DbError, FileSource, PreparedUserFile, Provenance, RowKey, WriteBatch,
};
use coven_crypto::{ContentHash, ContentHasher};
use coven_format::value::Value;
use coven_foundation::{
    files::{FileArea, FileName, StoreDir},
    id_source::DeviceId,
};
use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
};
use tokio::io::AsyncReadExt;

struct StagedFile {
    namespace: String,
    id: String,
    name: FileName,
    size: u64,
    hash: ContentHash,
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
    ids: &'a dyn coven_foundation::id_source::IdSource,
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
        ids: &'a dyn coven_foundation::id_source::IdSource,
    ) -> Self {
        Self {
            database,
            directory,
            schema,
            device,
            ids,
            staged: Vec::new(),
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

    pub(crate) fn stage(
        &mut self,
        batch: WriteBatch,
        runtime: &tokio::runtime::Handle,
    ) -> Result<(), DbError> {
        let mut identities = BTreeSet::new();
        for (namespace, id, source) in batch.files {
            if !identities.insert((namespace.clone(), id.clone())) {
                return Err(file_row::invalid("a batch supplies the same file twice"));
            }
            if !self.schema.declarations.iter().any(|d| {
                d.files.as_ref().is_some_and(|f| {
                    f.namespace == namespace && f.provenance == Provenance::AppProvided
                })
            }) {
                return Err(file_row::invalid(format!(
                    "{namespace} is not an app-provided file namespace"
                )));
            }
            let name =
                FileName::new(self.ids.new_id().to_string()).expect("UUID is a portable filename");
            let recorded = self.database.internal_execute(
                "INSERT INTO coven_file_removals(path) SELECT ?1 WHERE NOT EXISTS(SELECT 1 FROM coven_device_files WHERE path=?1)",
                [name.as_str()],
            )?;
            if recorded != 1 {
                return Err(file_row::invalid("the id source reused a kept file's name"));
            }
            let (size, hash) = self
                .directory
                .file(FileArea::AppProvided, &name)
                .create(|out| {
                    let mut hash = ContentHasher::new();
                    let mut size = 0;
                    match source {
                        FileSource::Bytes(bytes) => {
                            for bytes in bytes.chunks(64 * 1024) {
                                out.write_all(bytes)?;
                                hash.update(bytes);
                                size += bytes.len() as u64;
                            }
                        }
                        FileSource::Stream(mut reader) => runtime.block_on(async {
                            let mut buffer = [0; 64 * 1024];
                            loop {
                                let read = reader.read(&mut buffer).await?;
                                if read == 0 {
                                    break;
                                }
                                out.write_all(&buffer[..read])?;
                                hash.update(&buffer[..read]);
                                size += read as u64;
                            }
                            Ok::<_, std::io::Error>(())
                        })?,
                    }
                    Ok((size, hash.finish()))
                })?;
            self.staged.push(StagedFile {
                namespace,
                id,
                name,
                size,
                hash,
            });
        }
        Ok(())
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
                    let attached_values = crate::write_rows::read_values(db, table, &key.1)?
                        .ok_or_else(|| {
                            file_row::invalid("the row was removed while attaching its file")
                        })?;
                    db.internal_execute("INSERT INTO coven_device_files(table_name,key,column_name,identity,path) VALUES(?1,?2,?3,?4,?5)", (&key.0, &key.1, &file.id, file_row::identity(file, &attached_values)?, staged.name.as_str()))?;
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
                return Err(file_row::invalid(format!(
                    "no row refers to {}/{}",
                    staged.namespace, staged.id
                )));
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
            return Err(file_row::invalid(format!(
                "{table} does not declare user-provided files"
            )));
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
        db.internal_execute("INSERT INTO coven_user_files(table_name,key,column_name,identity,path,size,modified_at) VALUES(?1,?2,?3,?4,?5,?6,?7) ON CONFLICT(table_name,key,column_name) DO UPDATE SET identity=excluded.identity,path=excluded.path,size=excluded.size,modified_at=excluded.modified_at", rusqlite::params![key.0, key.1, file.id, file_row::identity(file, &attached_values)?, crate::user_file::encode_path(prepared.observed.path()), prepared.observed.size().to_be_bytes().as_slice(), crate::user_file::encode_time(prepared.observed.modified_at())])?;
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
            return Err(file_row::invalid(format!(
                "{table} does not declare user-provided files"
            )));
        }
        let (key, _) = file_row::lookup(db, self.schema, table, &key)?;
        file_row::set(db, self.schema, &key, None, self.device)?;
        db.internal_execute(
            "DELETE FROM coven_user_files WHERE table_name=?1 AND key=?2 AND column_name=?3",
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
                        return Err(file_row::invalid("an attached file must have an id"));
                    }
                    if let Some(attached) = self.attached.borrow().get(key) {
                        file_row::check_size(&new.values, file, attached.size)?;
                        if new.values[&file.id] != attached.id
                            || new.values[&file.hash]
                                != Value::Blob(attached.hash.as_bytes().to_vec())
                        {
                            return Err(file_row::invalid("row no longer names the attached file"));
                        }
                    }
                }
                if has_file(new) != (new.values[&file.location] != Value::Null) {
                    return Err(file_row::invalid(
                        "hash and location must both be NULL or both identify a file",
                    ));
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
                            return Err(file_row::invalid(
                                "changing a row's file requires supplying its bytes",
                            ));
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
                db.internal_execute("DELETE FROM coven_user_files WHERE table_name=?1 AND key=?2 AND column_name=?3", (&key.0, &key.1, &file.id))?;
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
            let identities = db.query(&format!("SELECT identity FROM {local_table} WHERE table_name=?1 AND key=?2 AND column_name=?3"), (&key.0, &key.1, &file.id), |r| r.get::<_, Vec<u8>>(0))?;
            if identities.is_empty() {
                continue;
            }
            let mut retained = BTreeSet::new();
            for (audience, generation) in db.query("SELECT audience,max(generation) FROM coven_rows WHERE table_name=?1 AND key=?2 GROUP BY audience", (&key.0, &key.1), |r| Ok((crate::write_encoding::audience(&r.get::<_,String>(0)?)?, crate::write_encoding::counter(r.get(1)?))))? {
                if generation % 2 == 0 || matches!(&audience, coven_merge::Audience::Circle(id) if deleted.contains(id)) { continue; }
                let row = store.row(&coven_merge::RowId { table:key.0.clone(), key:key.1.clone(), audience })?;
                let values = row.state.cells().iter().map(|(column,cell)| (column.clone(),cell.value.value.clone())).collect();
                if let Some(identity) = file_row::identity(file, &values)? {
                    if file.provenance == Provenance::AppProvided
                        && crate::file_location::StoredLocation::decode(&values[&file.location])?.public()
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
                        db.internal_execute("DELETE FROM coven_user_files WHERE table_name=?1 AND key=?2 AND column_name=?3", (&key.0, &key.1, &file.id))?;
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
        let paths = db.query("DELETE FROM coven_device_files WHERE table_name=?1 AND key=?2 AND column_name=?3 RETURNING path", (&key.0, &key.1, column), |r| r.get::<_, String>(0))?;
        for path in paths {
            self.obsolete
                .borrow_mut()
                .insert(FileName::new(path).map_err(|_| DbError::DamagedDatabase)?);
        }
        Ok(())
    }

    pub(crate) fn finish<R>(&self, result: Result<R, DbError>) -> Result<R, DbError> {
        crate::file_removals::FileRemovals::new(self.database, self.directory).finish(result)
    }

    pub(crate) fn rollback(&self) -> Result<(), Vec<DbError>> {
        let failures =
            crate::file_removals::FileRemovals::new(self.database, self.directory).remove_unused();
        if failures.is_empty() {
            Ok(())
        } else {
            Err(failures)
        }
    }
}

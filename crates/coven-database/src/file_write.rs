//! A write's files (§17.2): the files it stages, the records it makes,
//! changes and deletes for them, and the uploads it queues, all in the
//! write's own transaction.

use crate::{
    file_location::{FileLocation, Place},
    file_table::{self, Record},
    sqlite::DatabaseConnection,
    write_rows::{AppKey, AppValues, AppView},
    write_schema::WriteSchema,
    DbError,
};
use coven_format::value::Value;
use coven_foundation::{
    files::{FileName, ObservedFile},
    id_source::{DeviceId, FileId, IdSource},
};
use coven_merge::Audience;
use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
};

#[path = "file_write_references.rs"]
mod references;
pub(crate) use references::{deletions, unreferenced};

#[path = "file_write_uploads.rs"]
mod uploads;

#[path = "file_write_moves.rs"]
mod moves;
pub use moves::MoveFile;

#[path = "file_write_bound.rs"]
mod bound;

#[path = "file_write_device_only.rs"]
mod device_only;
pub(crate) use device_only::permitted;
pub use device_only::DeviceOnlyCopy;

#[cfg(test)]
#[path = "file_write_tests.rs"]
pub(crate) mod tests;

/// One file a write's batch staged: its new id, namespace, size, where its
/// bytes are, the hashes from its first read, which an upload checks, and
/// where it is to be. An original observed without being read has none.
pub(crate) struct StagedFile {
    pub(crate) id: FileId,
    pub(crate) namespace: String,
    pub(crate) size: u64,
    pub(crate) source: StagedSource,
    pub(crate) hashes: Option<crate::file_hashes::FileHashes>,
    pub(crate) place: crate::Place,
}

/// Where a staged file's bytes are until its upload: coven's copy of the
/// app's bytes, or the user's original.
pub(crate) enum StagedSource {
    Copy(FileName),
    Original(ObservedFile),
}

/// The reference error of a row naming no file of its audience (§17.2).
pub(crate) fn unknown(
    table: &crate::schema::TableSchema,
    values: &AppValues,
    column: &str,
) -> DbError {
    let column = crate::write_rows::column_name(table, column).to_owned();
    DbError::FileUnknown {
        table: table.name.clone(),
        file: match values.get(&column) {
            Some(Value::Text(text)) => text.clone(),
            Some(value) => format!("{:?}", crate::write_encoding::sql_value(value)),
            None => String::new(),
        },
        column,
    }
}

pub(crate) struct FileWrite<'a> {
    database: &'a DatabaseConnection,
    schema: &'a WriteSchema,
    device: DeviceId,
    ids: &'a dyn IdSource,
    staged: Vec<StagedFile>,
    /// The staged files rows reference, by id, as their records are made.
    made: RefCell<BTreeSet<FileId>>,
    /// The staged files a bound reference names, whose uploads hold this
    /// write (§17.4).
    held: RefCell<BTreeSet<FileId>>,
    /// The new files the write's moves made for rows removal rules took out.
    taken_out: RefCell<Copied>,
    /// The records this write deletes, once it has decided them; `None` in a
    /// transaction applying other devices' writes.
    deleted: RefCell<Option<BTreeSet<FileId>>>,
    completion: RefCell<Option<uploads::Completion>>,
    /// The moves this write recorded, whose uploads it queues.
    moving: RefCell<Vec<moves::Moving>>,
    /// The files this write makes device-only here, each with the copy that
    /// becomes its original (§17.3).
    local: RefCell<Vec<DeviceOnlyCopy>>,
    obsolete: RefCell<BTreeSet<FileName>>,
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
            made: RefCell::new(BTreeSet::new()),
            held: RefCell::new(BTreeSet::new()),
            taken_out: RefCell::new(BTreeMap::new()),
            deleted: RefCell::new(None),
            completion: RefCell::new(None),
            moving: RefCell::new(Vec::new()),
            local: RefCell::new(Vec::new()),
            obsolete: RefCell::new(BTreeSet::new()),
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

    /// The records a write made without SQL deletes, its others' last
    /// reference gone (§17.2).
    pub(crate) fn deleting(&self, files: BTreeSet<FileId>) {
        *self.deleted.borrow_mut() = Some(files);
    }

    /// The new files the write's moves made for rows removal rules took out,
    /// which their inserts name (§17.2).
    pub(crate) fn taken_out_copies(&self) -> Copied {
        self.taken_out.borrow().clone()
    }

    /// Coven's part of a local write, after its SQL and while its row
    /// operations are still recorded (§17.2): make each staged file's record
    /// in the audience of the rows naming it, give rows moved to another
    /// audience new files there, refuse a reference naming no file of its
    /// row's audience, a change to a write-once one and a bound one naming
    /// a file it can't wait for (§17.4), and switch each move whose uploads
    /// this write completed (§17.3).
    pub(crate) fn finish_app(
        &self,
        events: impl Fn() -> Vec<crate::sqlite::Event>,
    ) -> Result<(), DbError> {
        self.finish_uploads()?;
        let captured = crate::write_capture::capture(self.schema, &events())?;
        self.check_write_once(&captured)?;
        self.make_staged(&captured)?;
        self.check_bound(&captured)?;
        let captured = crate::write_capture::capture(self.schema, &events())?;
        let copies = self.copy_moved(&captured)?;
        self.taken_out.borrow_mut().clone_from(&copies.taken_out);
        let captured = crate::write_capture::capture(self.schema, &events())?;
        self.check_references(&captured)
    }

    /// The local write's record, deleting each file record whose last
    /// reference it removes (§17.2). `build` makes a record of changes and
    /// steps; merging the record without the deletes, which persists
    /// nothing, says which references remain. Each delete is a step after
    /// the write's others.
    pub(crate) fn delete_unreferenced(
        &self,
        store: &crate::merge_store::MergeStore<'_>,
        mut changes: BTreeMap<coven_merge::RowId, coven_format::write::RowChange>,
        mut steps: BTreeMap<coven_merge::RowId, Vec<coven_format::write::Step>>,
        build: impl Fn(
            BTreeMap<coven_merge::RowId, coven_format::write::RowChange>,
            BTreeMap<coven_merge::RowId, Vec<coven_format::write::Step>>,
        ) -> Result<Option<coven_format::write::WriteRecord>, DbError>,
    ) -> Result<Option<coven_format::write::WriteRecord>, DbError> {
        let db = self.database;
        let Some(record) = build(changes.clone(), steps.clone())? else {
            *self.deleted.borrow_mut() = Some(BTreeSet::new());
            return Ok(None);
        };
        let unreferenced = references::unreferenced(db, self.schema, store, &record)?;
        if unreferenced.is_empty() {
            *self.deleted.borrow_mut() = Some(unreferenced);
            return Ok(Some(record));
        }
        let sequence = steps
            .values()
            .flatten()
            .map(|step| step.sequence + 1)
            .max()
            .unwrap_or(0);
        for (change, step) in references::deletions(db, store, &unreferenced, sequence)? {
            if changes.contains_key(&change.row) || steps.contains_key(&change.row) {
                return Err(DbError::Invariant {
                    what: "a write deleting a file's record makes no other change to it",
                });
            }
            steps.insert(change.row.clone(), vec![step]);
            changes.insert(change.row.clone(), change);
        }
        *self.deleted.borrow_mut() = Some(unreferenced);
        build(changes, steps)
    }

    /// A write-once reference, once it names a file, never changes (§17.2).
    fn check_write_once(
        &self,
        captured: &BTreeMap<AppKey, crate::write_capture::CapturedRow>,
    ) -> Result<(), DbError> {
        for (key, row) in captured {
            for file in &self.schema.declaration(&key.0).files {
                if !file.write_once {
                    continue;
                }
                let Some(old) = row.old.get(&file.column).filter(|v| **v != Value::Null) else {
                    continue;
                };
                let table = self.schema.table(&key.0);
                let new = crate::write_rows::read_values(self.database, table, &key.1)?;
                if new.as_ref().and_then(|values| values.get(&file.column)) != Some(old) {
                    return Err(DbError::FileWriteOnce {
                        table: key.0.clone(),
                        key: crate::write_rows::app_row_key(key)?,
                    });
                }
            }
        }
        Ok(())
    }

    /// Make the record of each staged file a row names, device-only on this
    /// device until its upload completes, in the audience of the rows naming
    /// it. A staged file no row names fails the write.
    fn make_staged(
        &self,
        captured: &BTreeMap<AppKey, crate::write_capture::CapturedRow>,
    ) -> Result<(), DbError> {
        if self.staged.is_empty() {
            return Ok(());
        }
        let db = self.database;
        let staged: BTreeMap<FileId, &StagedFile> =
            self.staged.iter().map(|file| (file.id, file)).collect();
        let view = AppView::after(db, self.schema);
        let mut audiences = BTreeMap::<FileId, Audience>::new();
        for key in captured.keys() {
            let columns = self.schema.file_columns(&key.0);
            if columns.is_empty() {
                continue;
            }
            let table = self.schema.table(&key.0);
            let Some(values) = crate::write_rows::read_values(db, table, &key.1)? else {
                continue;
            };
            for column in columns {
                let Some(file) = values.get(column).and_then(file_table::id) else {
                    continue;
                };
                if !staged.contains_key(&file) {
                    continue;
                }
                let audience = view.audience_of(table, &values)?;
                match audiences.get(&file) {
                    Some(first) if *first != audience => {
                        return Err(unknown(table, &values, column));
                    }
                    Some(_) => {}
                    None => {
                        audiences.insert(file, audience);
                    }
                }
            }
        }
        for file in &self.staged {
            let Some(audience) = audiences.remove(&file.id) else {
                return Err(DbError::FileUnreferenced {
                    namespace: file.namespace.clone(),
                    file: file.id,
                });
            };
            crate::write_commit::check_membership(db, self.device, &audience)?;
            Record {
                id: file.id,
                audience,
                size: file.size,
                hash: None,
                place: Place::DeviceOnly(self.device),
                namespace: file.namespace.clone(),
                shares: None,
            }
            .insert(db)?;
            self.made.borrow_mut().insert(file.id);
        }
        Ok(())
    }

    /// Give each row the write moves to another audience a new file there
    /// for each file it names in its old one, over the same bytes, and name
    /// it in the row (§15.4, §17.2). A row removal rules took out keeps its
    /// values in coven's records; its new files are named in the move's
    /// insert instead.
    fn copy_moved(
        &self,
        captured: &BTreeMap<AppKey, crate::write_capture::CapturedRow>,
    ) -> Result<Copies, DbError> {
        let db = self.database;
        let mut copies = Copies::default();
        let before = AppView::before(db, self.schema, captured)?;
        let after = AppView::after(db, self.schema);
        let store = crate::merge_store::MergeStore::new(db, &before);
        let (moved, _) = crate::write_record::moved(
            db,
            self.schema,
            &before,
            &after,
            &store,
            captured.keys().cloned(),
        )?;
        for (old, new) in moved {
            let columns = self.schema.file_columns(&old.table);
            if columns.is_empty() {
                continue;
            }
            let table = self.schema.table(&old.table);
            let state = store.row(&old)?;
            let shown = state.loss.is_none();
            let values: AppValues = if shown {
                match crate::write_rows::read_values(db, table, &old.key)? {
                    Some(values) => values,
                    None => continue,
                }
            } else {
                state
                    .state
                    .cells()
                    .iter()
                    .map(|(name, cell)| (name.clone(), cell.value.value.clone()))
                    .collect()
            };
            for column in columns {
                let Some(file) = values.get(column).and_then(file_table::id) else {
                    continue;
                };
                let Some(record) = Record::read(db, file)? else {
                    continue;
                };
                if record.audience != old.audience || record.audience == new.audience {
                    continue;
                }
                let copy = match copies.made.get(&(file, new.audience.clone())) {
                    Some(copy) => *copy,
                    None => {
                        let copy = self.copy(&record, new.audience.clone())?;
                        copies.made.insert((file, new.audience.clone()), copy);
                        copy
                    }
                };
                if shown {
                    let changed = db.internal_execute(
                        &format!(
                            "UPDATE main.{} SET {}=?1 WHERE {}",
                            crate::sql::identifier(&table.name),
                            crate::sql::identifier(column),
                            crate::write_rows::key_predicate(table)
                        ),
                        rusqlite::params_from_iter(
                            std::iter::once(rusqlite::types::Value::Text(copy.0.to_string()))
                                .chain(crate::write_rows::key_parameters(&old.key)?),
                        ),
                    )?;
                    if changed != 1 {
                        return Err(rusqlite::Error::StatementChangedRows(changed).into());
                    }
                } else {
                    copies.taken_out.insert((file, new.audience.clone()), copy);
                }
            }
        }
        Ok(copies)
    }

    /// A new file in `audience` over `record`'s bytes: the same size,
    /// namespace, place and content hash, naming `record` as the file whose
    /// bytes it shares. A file on this device only shares its bytes here and
    /// any upload (§17.2).
    fn copy(&self, record: &Record, audience: Audience) -> Result<FileId, DbError> {
        let db = self.database;
        crate::write_commit::check_membership(db, self.device, &audience)?;
        let id = FileId(self.ids.new_id());
        Record {
            id,
            audience,
            shares: Some(record.id),
            ..record.clone()
        }
        .insert(db)?;
        if record.place == Place::DeviceOnly(self.device) {
            link(db, self.device, id, record.id)?;
        }
        Ok(id)
    }

    /// Each file reference the write's rows hold names a file of the row's
    /// own audience (§17.2).
    fn check_references(
        &self,
        captured: &BTreeMap<AppKey, crate::write_capture::CapturedRow>,
    ) -> Result<(), DbError> {
        let db = self.database;
        let view = AppView::after(db, self.schema);
        // Coven's own moves rewrote their shown rows' references, so each
        // such row is among the write's rows.
        for key in captured.keys() {
            let columns = self.schema.file_columns(&key.0);
            if columns.is_empty() {
                continue;
            }
            let table = self.schema.table(&key.0);
            let Some(values) = crate::write_rows::read_values(db, table, &key.1)? else {
                continue;
            };
            let audience = view.audience_of(table, &values)?;
            for column in columns {
                match values.get(column) {
                    None | Some(Value::Null) => {}
                    Some(value) => {
                        let record = match file_table::id(value) {
                            Some(file) => Record::read(db, file)?,
                            None => None,
                        };
                        if record.is_none_or(|record| record.audience != audience) {
                            return Err(unknown(table, &values, column));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Bring the protection of the rows a transaction touched up to date
    /// (§17.2, §17.10). A local write already deleted the records whose last
    /// reference it removed, and every file left without one is among them.
    /// Other devices' writes can leave a file with none, when two of them
    /// each removed one of its references; this device's next write of its
    /// own deletes that record.
    pub(crate) fn retain_rows(&self, keys: BTreeSet<AppKey>) -> Result<(), DbError> {
        let db = self.database;
        let deleted = self.deleted.borrow();
        let Some(deleted) = deleted.as_ref() else {
            for key in &keys {
                crate::file_protection::refresh_received(db, self.schema, key)?;
            }
            return settle_unreferenced(db);
        };
        for key in &keys {
            for file in crate::file_protection::refresh(db, self.schema, key)? {
                if Record::read(db, file)?.is_some() {
                    return Err(DbError::Invariant {
                        what: "a write removing a file's last reference deletes its record",
                    });
                }
            }
        }
        for file in deleted {
            if db.query_row(
                "SELECT EXISTS(SELECT 1 FROM _coven_file_references WHERE file=?1)",
                [file],
                |r| r.get::<_, bool>(0),
            )? {
                return Err(DbError::Invariant {
                    what: "a write deletes only a record nothing references",
                });
            }
        }
        settle_unreferenced(db)
    }

    /// `retain_rows` for one row of a transaction that only applies other
    /// devices' changes, which deletes no record of its own; settle the
    /// files left unreferenced once every row is done.
    pub(crate) fn retain_received(&self, key: &AppKey) -> Result<(), DbError> {
        if self.deleted.borrow().is_some() {
            return Err(DbError::Invariant {
                what: "a transaction applying received changes deletes no record of its own",
            });
        }
        crate::file_protection::refresh_received(self.database, self.schema, key)
    }

    /// Settle the files the rows `retain_received` visited left unreferenced.
    pub(crate) fn settle_received(&self) -> Result<(), DbError> {
        settle_unreferenced(self.database)
    }

    /// Keep the bytes of each staged file a row names on this device, and
    /// queue the uploads of those staged for the cloud, as one move, and of
    /// each move this write recorded (§17.3, §17.9). A file a bound
    /// reference names is a move of its own, whose upload holds `write`,
    /// this write's number, or the first write another queued bound upload
    /// holds: that upload's completion follows that write, so the two can go
    /// out together only once both files are stored (§17.4).
    pub(crate) fn queue_uploads(
        &self,
        now: std::time::SystemTime,
        write: Option<u64>,
    ) -> Result<(), DbError> {
        let db = self.database;
        let mut group = None;
        for file in &self.staged {
            if !self.made.borrow().contains(&file.id) {
                continue;
            }
            // A file made and deleted in this one write keeps nothing.
            if Record::read(db, file.id)?.is_none() {
                continue;
            }
            uploads::keep_source(db, file)?;
            if file.place == crate::Place::DeviceOnly {
                continue;
            }
            let hashes = file.hashes.as_ref().ok_or(DbError::Invariant {
                what: "a file staged for the cloud has its first read's hashes",
            })?;
            // The trailing files one write stages for the cloud are one
            // move; a bound file switches on its own, with its held write.
            let holds = if self.held.borrow().contains(&file.id) {
                let write = write.ok_or(DbError::Invariant {
                    what: "a write setting a bound reference is recorded",
                })?;
                Some(crate::file_queue::first_held(db)?.map_or(write, |first| first.min(write)))
            } else {
                None
            };
            let of = match (holds, group) {
                (Some(_), _) => crate::file_moves::create(db)?,
                (None, Some(of)) => of,
                (None, None) => *group.insert(crate::file_moves::create(db)?),
            };
            let location = FileLocation::new(self.device, self.ids)?;
            uploads::enqueue(
                db,
                uploads::Queued {
                    files: &[(file.id, file.place == crate::Place::CloudKeptHere)],
                    size: file.size,
                    source: &file.source,
                    hashes,
                    of,
                    holds,
                },
                &location,
                now,
            )?;
        }
        self.queue_moves(now)?;
        self.keep_device_only()?;
        self.finish_completion()
    }

    pub(crate) fn before_commit(&self) -> Result<(), DbError> {
        let db = self.database;
        for file in &self.staged {
            if let StagedSource::Copy(name) = &file.source {
                self.obsolete.borrow_mut().insert(name.clone());
            }
        }
        // A staged copy's cleanup record went as its file came to name it;
        // a copy nothing names goes.
        for name in self.obsolete.borrow().iter() {
            crate::file_queue::release_copy(db, name.as_str())?;
        }
        Ok(())
    }
}

/// The new files applied writes made on this device only over another
/// file's bytes (§17.2), each with the file whose bytes it shares, as their
/// records' inserts say.
pub(crate) struct SharedCopies {
    device: DeviceId,
    made: BTreeMap<FileId, FileId>,
}

impl SharedCopies {
    pub(crate) fn new(db: &DatabaseConnection) -> Result<Self, DbError> {
        Ok(Self {
            device: db.query_row("SELECT _coven_device()", [], |r| r.get(0))?,
            made: BTreeMap::new(),
        })
    }

    /// Note one applied row change.
    pub(crate) fn note(&mut self, row: &coven_format::write::RowChange) {
        let coven_merge::Operation::Insert(columns) = &row.change.operation else {
            return;
        };
        if !file_table::is_files(&row.row.table) {
            return;
        }
        let here = columns
            .get(file_table::COORDINATES)
            .and_then(|value| Place::decode(&value.value).ok())
            == Some(Place::DeviceOnly(self.device));
        let shares = columns
            .get(file_table::SHARES)
            .and_then(|value| file_table::id(&value.value));
        if let (true, Some(shares), Ok(file)) =
            (here, shares, file_table::file_of_key(&row.row.key))
        {
            self.made.insert(file, shares);
        }
    }

    /// Give each new file the bytes of the file it shares.
    pub(crate) fn link(self, db: &DatabaseConnection) -> Result<(), DbError> {
        for (copy, shares) in self.made {
            link(db, self.device, copy, shares)?;
        }
        Ok(())
    }
}

/// Give each file an install shows on this device only over another
/// file's bytes the bytes of the file it shares (§17.2), before the install
/// settles what its records leave: a snapshot carries the same records a
/// write does, and a source the install deletes still has its bytes here.
pub(crate) fn link_installed(db: &DatabaseConnection) -> Result<(), DbError> {
    let device: DeviceId = db.query_row("SELECT _coven_device()", [], |r| r.get(0))?;
    let shown = db.query(
        "SELECT f.id,f.shares FROM (
           SELECT key FROM temp._coven_install_changed WHERE table_name=?1 AND kind!='delete'
           UNION SELECT key FROM temp._coven_install_region WHERE table_name=?1) AS c
         CROSS JOIN main.coven_files AS f ON f.id=_coven_file_of_key(c.key)
         WHERE f.coordinates=?2 AND f.shares IS NOT NULL ORDER BY f.id",
        rusqlite::params![
            crate::internal_schema::FILES,
            crate::write_encoding::sql_value(&Place::DeviceOnly(device).value())
        ],
        |r| Ok((r.get::<_, FileId>(0)?, r.get::<_, FileId>(1)?)),
    )?;
    for (copy, shares) in shown {
        link(db, device, copy, shares)?;
    }
    Ok(())
}

/// Give `copy`, a new file on this device only that shares `source`'s
/// bytes, those bytes here (§17.2): `source`'s original or coven's copy,
/// and its upload if one is queued, whose completion then switches both.
/// A source this device has stored already leaves `copy` waiting for this
/// device's own write switching it to the same stored bytes. A source whose
/// record is gone, as when the write or install bringing `copy` deletes it,
/// still has its bytes here until that transaction settles the records it
/// left. A copy with bytes here already keeps them.
pub(crate) fn link(
    db: &DatabaseConnection,
    device: DeviceId,
    copy: FileId,
    source: FileId,
) -> Result<(), DbError> {
    if crate::file_ref::file_source(db, copy)?.is_some() {
        return Ok(());
    }
    match Record::read(db, source)? {
        Some(Record {
            place: Place::Cloud(location),
            hash: Some(hash),
            ..
        }) => {
            db.internal_execute(
                "INSERT INTO _coven_moved_copies(file,location,hash) VALUES(?1,?2,?3)
                 ON CONFLICT DO NOTHING",
                rusqlite::params![copy, location.text(), hash.as_bytes().as_slice()],
            )?;
            Ok(())
        }
        Some(Record {
            place: Place::DeviceOnly(on),
            ..
        }) if on != device => Ok(()),
        Some(_) | None => uploads::share(db, source, copy),
    }
}

/// A file referenced again, or whose record is gone, no longer waits for
/// its record's delete.
fn settle_unreferenced(db: &DatabaseConnection) -> Result<(), DbError> {
    db.internal_execute(
        "DELETE FROM _coven_unreferenced_files WHERE EXISTS(
             SELECT 1 FROM _coven_file_references AS r WHERE r.file=_coven_unreferenced_files.file)
         OR NOT EXISTS(SELECT 1 FROM main.coven_files AS f WHERE f.id=_coven_unreferenced_files.file)",
        [],
    )?;
    Ok(())
}

/// New files moves made, by the file each shares bytes with and the
/// audience it was made in.
pub(crate) type Copied = BTreeMap<(FileId, Audience), FileId>;

/// The new files a write's moves made, and those a taken-out row's insert
/// names instead of the files of its old audience.
#[derive(Default)]
pub(crate) struct Copies {
    made: Copied,
    pub(crate) taken_out: Copied,
}

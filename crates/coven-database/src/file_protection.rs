//! What a device keeps a file for: the stored paths its records name, which
//! storage keeps until nothing can name them (§17.9), and each reference
//! the app's rows hold to a record, which keeps the record (§17.2).

use crate::file_location::Place;
use crate::file_table::{self, COORDINATES};
use crate::{sqlite::DatabaseConnection, write_rows::AppKey, write_schema::WriteSchema, DbError};
use coven_format::{
    file_reference::FileReference,
    loss::{Loss, LossValues},
    value::Value,
};
use coven_foundation::id_source::{DeviceId, FileId};
use rusqlite::params;
use std::collections::BTreeSet;

/// The column naming a stored path: a record's coordinates.
pub(crate) fn location(table: &str) -> Option<&'static str> {
    file_table::is_files(table).then_some(COORDINATES)
}

/// The stored path a coordinates value names, if it names one.
pub(crate) fn value(
    value: &Value,
    references: &mut BTreeSet<(DeviceId, FileId)>,
) -> Result<(), DbError> {
    if let Place::Cloud(location) = Place::decode(value)? {
        references.insert((location.uploader(), location.object()));
    }
    Ok(())
}

/// The stored paths a loss keeps. A record's own lost coordinates keep
/// nothing: nothing restores coven's records, so bytes a lost switch to
/// the cloud names go once nothing else names them.
pub(crate) fn loss(loss: &Loss, references: &mut BTreeSet<(DeviceId, FileId)>) {
    if !loss.frozen {
        return;
    }
    // Frozen values keep their original column names after declarations
    // disappear. Any canonical stored location remains protective; a
    // non-file string coinciding with that encoding can only retain extra
    // bytes, never authorize deletion. Other strings are not locations.
    match &loss.values {
        LossValues::Cell { cell, .. } => potential_reference(&cell.value.value, references),
        LossValues::Row(cells) => {
            for cell in cells.values() {
                potential_reference(&cell.value.value, references);
            }
        }
    }
}

/// The files a loss of an app row references through its file columns,
/// each cell of it `kept` keeps, by its column and setter. A row that removal
/// rules took out keeps its merged cells, so its own lost record adds none.
pub(crate) fn loss_files(
    schema: &WriteSchema,
    loss: &Loss,
    kept: impl Fn(&str, coven_merge::WriteId) -> bool,
    files: &mut BTreeSet<(FileId, String)>,
) {
    if matches!(loss.cause, coven_format::loss::LossCause::Rules(_)) {
        return;
    }
    let columns = schema.file_columns(&loss.row.table);
    let mut add = |column: &str, cell: &coven_merge::Cell<Value>| {
        if let Some(name) = columns.iter().find(|c| c.eq_ignore_ascii_case(column)) {
            if let Some(file) =
                file_table::id(&cell.value.value).filter(|_| kept(column, cell.write))
            {
                files.insert((file, (*name).to_owned()));
            }
        }
    };
    match &loss.values {
        LossValues::Cell { column, cell } => add(column, cell),
        LossValues::Row(cells) => {
            for (column, cell) in cells {
                add(column, cell);
            }
        }
    }
}

/// The files one audience's merged state of an app row references through
/// `columns`, its file columns: its cells, and, when `state` holds them, its
/// live lost values.
pub(crate) fn state_files(
    columns: &[&str],
    state: &coven_merge::RowState<Value>,
    kept: impl Fn(&str, coven_merge::WriteId) -> bool,
    files: &mut BTreeSet<(FileId, String)>,
) {
    for column in columns {
        if let Some(file) = state
            .cells()
            .get(*column)
            .and_then(|cell| file_table::id(&cell.value.value))
        {
            files.insert((file, (*column).to_owned()));
        }
    }
    for (key, lost) in state.lost() {
        let Some(column) = columns.iter().find(|c| **c == key.column) else {
            continue;
        };
        if let Some(file) = file_table::id(&lost.value.value).filter(|_| kept(column, key.write)) {
            files.insert((file, (*column).to_owned()));
        }
    }
}

/// A loss may arrive without a displayed row change (schema exclusion or
/// reload): its paths and file references are kept at once.
pub(crate) fn retain_loss(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    record: &Loss,
) -> Result<(), DbError> {
    let mut paths = BTreeSet::new();
    loss(record, &mut paths);
    for (device, file) in &paths {
        db.internal_execute("INSERT INTO _coven_row_file_refs(device,file,table_name,key) VALUES(?1,?2,?3,?4) ON CONFLICT DO NOTHING",params![device.0.to_be_bytes(),file.0.as_bytes(),record.row.table,record.row.key])?;
    }
    let mut files = BTreeSet::new();
    loss_files(schema, record, |_, _| true, &mut files);
    for (file, column) in files {
        db.internal_execute(
            "INSERT INTO _coven_file_references(file,table_name,key,column_name)
             VALUES(?1,?2,?3,?4) ON CONFLICT DO NOTHING",
            params![file, record.row.table, record.row.key, column],
        )?;
    }
    Ok(())
}

/// Bring one row's protection up to date with its merged state: a record's
/// stored paths and stored file, or an app row's file references. All its
/// audiences and retained losses contribute; display removal alone releases
/// nothing. Returns the files the row referenced before and no row
/// references now.
pub(crate) fn refresh(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    key: &AppKey,
) -> Result<BTreeSet<FileId>, DbError> {
    if file_table::is_files(&key.0) {
        refresh_record(db, schema, key)?;
        return Ok(BTreeSet::new());
    }
    if schema.file_columns(&key.0).is_empty()
        && !db.query_row(
            "SELECT EXISTS(SELECT 1 FROM _coven_file_references WHERE table_name=?1 AND key=?2)",
            params![key.0, key.1],
            |r| r.get::<_, bool>(0),
        )?
    {
        return Ok(BTreeSet::new());
    }
    let files = row_files(db, schema, key)?;
    let previous: BTreeSet<(FileId, String)> = db
        .query(
            "SELECT file,column_name FROM _coven_file_references WHERE table_name=?1 AND key=?2",
            params![key.0, key.1],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?
        .into_iter()
        .collect();
    let mut released = BTreeSet::new();
    for (file, column) in previous.difference(&files) {
        db.internal_execute(
            "DELETE FROM _coven_file_references
             WHERE file=?1 AND table_name=?2 AND key=?3 AND column_name=?4",
            params![file, key.0, key.1, column],
        )?;
        if !db.query_row(
            "SELECT EXISTS(SELECT 1 FROM _coven_file_references WHERE file=?1)",
            [file],
            |r| r.get::<_, bool>(0),
        )? {
            released.insert(*file);
        }
    }
    for (file, column) in files.difference(&previous) {
        db.internal_execute(
            "INSERT INTO _coven_file_references(file,table_name,key,column_name)
             VALUES(?1,?2,?3,?4)",
            params![file, key.0, key.1, column],
        )?;
        released.remove(file);
    }
    Ok(released)
}

/// `refresh` in a transaction applying other devices' changes: a file those
/// changes left with no reference, while this device still has its record,
/// waits for this device's own write to delete it (§17.2). That happens when
/// two devices each removed one of its references, neither seeing the other.
pub(crate) fn refresh_received(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    key: &AppKey,
) -> Result<(), DbError> {
    for file in refresh(db, schema, key)? {
        if file_table::Record::read(db, file)?.is_some() {
            db.internal_execute(
                "INSERT INTO _coven_unreferenced_files(file) VALUES(?1) ON CONFLICT DO NOTHING",
                [file],
            )?;
        }
    }
    Ok(())
}

/// The files an app row references through its file columns: each
/// audience's merged cells, and every retained loss of the row.
pub(crate) fn row_files(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    key: &AppKey,
) -> Result<BTreeSet<(FileId, String)>, DbError> {
    let mut files = BTreeSet::new();
    let columns = schema.file_columns(&key.0);
    if !columns.is_empty() {
        let visible = crate::write_rows::AppView::after(db, schema).without_row_cache();
        let store = crate::merge_store::MergeStore::new(db, &visible).without_row_cache();
        db.for_each(
            "SELECT DISTINCT table_name,key,audience FROM _coven_rows WHERE table_name=?1 AND key=?2",
            params![key.0, key.1],
            |r| {
                let row = crate::row_queries::read_identity(r)?;
                // Its live lost values are among the loss records below.
                let state = store.row_without_losses(&row)?.state;
                state_files(&columns, &state, |_, _| true, &mut files);
                Ok::<_, DbError>(())
            },
        )?;
    }
    db.for_each(
        &format!(
            "{} WHERE l.table_name=?1 AND l.key=?2",
            crate::loss_record::SELECT
        ),
        params![key.0, key.1],
        |r| {
            loss_files(
                schema,
                &crate::loss_record::read(r)?,
                |_, _| true,
                &mut files,
            );
            Ok::<_, DbError>(())
        },
    )?;
    Ok(files)
}

/// A record's stored paths, from each audience's merged cells and its
/// frozen losses, and its stored file as the cache and pins find it. A
/// record gone for good, deleted or in a deleted circle, which never comes
/// back (§15.9), has its original forgotten (§17.2).
fn refresh_record(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    key: &AppKey,
) -> Result<(), DbError> {
    let mut references = BTreeSet::new();
    let visible = crate::write_rows::AppView::after(db, schema);
    let displayed = visible.row(key)?;
    let store = crate::merge_store::MergeStore::new(db, &visible);
    let deleted = crate::store_state_tables::deleted_circles(db)?;
    let mut kept = false;
    db.for_each(
        "SELECT DISTINCT table_name,key,audience FROM _coven_rows WHERE table_name=?1 AND key=?2",
        params![key.0, key.1],
        |r| {
            let row = crate::row_queries::read_identity(r)?;
            let state = store.row_without_losses(&row)?.state;
            kept |= state.present()
                && !matches!(&row.audience, coven_merge::Audience::Circle(circle) if deleted.contains_key(circle));
            if let Some(cell) = state.cells().get(COORDINATES) {
                value(&cell.value.value, &mut references)?;
            }
            Ok::<_, DbError>(())
        },
    )?;
    db.for_each(
        &format!(
            "{} WHERE l.table_name=?1 AND l.key=?2",
            crate::loss_record::SELECT
        ),
        params![key.0, key.1],
        |r| {
            loss(&crate::loss_record::read(r)?, &mut references);
            Ok::<_, DbError>(())
        },
    )?;
    // Change only the references that differ: deleting a kept one would make
    // its file a retention candidate (§17.9) although nothing released it.
    let previous: BTreeSet<_> = db
        .query(
            "SELECT device,file FROM _coven_row_file_refs WHERE table_name=?1 AND key=?2",
            params![key.0, key.1],
            |r| {
                Ok((
                    DeviceId(u64::from_be_bytes(r.get(0)?)),
                    FileId(uuid::Uuid::from_bytes(r.get(1)?)),
                ))
            },
        )?
        .into_iter()
        .collect();
    for (device, file) in previous.difference(&references) {
        db.internal_execute(
            "DELETE FROM _coven_row_file_refs WHERE device=?1 AND file=?2 AND table_name=?3 AND key=?4",
            params![device.0.to_be_bytes(), file.0.as_bytes(), key.0, key.1],
        )?;
    }
    for (device, file) in references.difference(&previous) {
        db.internal_execute(
            "INSERT INTO _coven_row_file_refs(device,file,table_name,key) VALUES(?1,?2,?3,?4)",
            params![device.0.to_be_bytes(), file.0.as_bytes(), key.0, key.1],
        )?;
    }
    let id = file_table::file_of_key(&key.1)?;
    let stored = match displayed {
        Some(row) => match crate::file_table::Record::decode(&row.values)? {
            crate::file_table::Record {
                place: Place::Cloud(location),
                hash: Some(hash),
                namespace,
                ..
            } => Some((namespace, location, hash)),
            _ => None,
        },
        None => None,
    };
    match stored {
        Some((namespace, location, hash)) => db.internal_execute(
            "INSERT INTO _coven_stored_files(file,namespace,device,object,cache_id,hash) VALUES(?1,?2,?3,?4,?5,?6)
             ON CONFLICT(file) DO UPDATE SET namespace=excluded.namespace,device=excluded.device,object=excluded.object,cache_id=excluded.cache_id,hash=excluded.hash
             WHERE namespace!=excluded.namespace OR device!=excluded.device OR object!=excluded.object OR hash!=excluded.hash",
            params![
                id,
                namespace,
                location.uploader().0.to_be_bytes(),
                location.object().0.as_bytes(),
                crate::database::file_database::file_cache_id(location.uploader(), location.object()),
                hash.as_bytes().as_slice()
            ],
        )?,
        None => db.internal_execute("DELETE FROM _coven_stored_files WHERE file=?1", [id])?,
    };
    if !kept {
        // The record's file is gone from this device: its original is
        // forgotten, never touched, and an upload no other file shares is
        // left for the upload worker to retire (§17.8).
        db.internal_execute("DELETE FROM _coven_user_files WHERE file=?1", [id])?;
        db.internal_execute("DELETE FROM _coven_upload_files WHERE file=?1", [id])?;
    }
    Ok(())
}

/// Schema changes can rename or drop every file column. Rebuild every
/// affected row's file references inside that migration.
pub(crate) fn migrate(
    db: &DatabaseConnection,
    tables: &[crate::SyncedTable],
) -> Result<BTreeSet<FileId>, DbError> {
    let schema = WriteSchema::read(db, tables.to_vec())?;
    let mut released = BTreeSet::new();
    let keys = db.query(
        "SELECT DISTINCT table_name,key FROM _coven_file_references",
        [],
        |r| Ok((r.get::<_, String>(0)?, r.get::<_, Vec<u8>>(1)?)),
    )?;
    let mut refreshed = BTreeSet::new();
    for key in keys {
        released.extend(refresh(db, &schema, &key)?);
        refreshed.insert(key);
    }
    for declaration in tables.iter().filter(|table| !table.files.is_empty()) {
        let keys = db.query(
            "SELECT key FROM _coven_rows WHERE table_name=?1 UNION SELECT key FROM _coven_lost WHERE table_name=?1",
            [&declaration.name],
            |r| r.get::<_, Vec<u8>>(0),
        )?;
        for key in keys {
            let key = (declaration.name.clone(), key);
            if refreshed.insert(key.clone()) {
                released.extend(refresh(db, &schema, &key)?);
            }
        }
    }
    Ok(released)
}

// A historical value need not belong to a current file declaration. Recognize
// the complete canonical encoding; all other text is ordinary application data.
fn potential_reference(value: &Value, references: &mut BTreeSet<(DeviceId, FileId)>) {
    if let Value::Text(text) = value {
        if let Ok(reference) = FileReference::decode(text) {
            references.insert((reference.device, reference.id));
        }
    }
}

/// Queue creation and interpretation changes retain paths in the same transaction.
/// Original paths survive schema changes until publication removes the queue row.
pub(crate) fn queued(
    db: &DatabaseConnection,
    record: &coven_format::write::WriteRecord,
) -> Result<(), DbError> {
    let mut references = BTreeSet::new();
    for row in record.parts.iter().flat_map(|part| &part.rows) {
        if let coven_merge::Operation::Insert(columns) | coven_merge::Operation::Update(columns) =
            &row.change.operation
        {
            for column in columns.values() {
                potential_reference(&column.value, &mut references);
            }
        }
        for value in row.old.values() {
            potential_reference(value, &mut references);
        }
    }
    for (device, file) in references {
        db.internal_execute("INSERT INTO _coven_queued_file_refs(device,file,writer,number) VALUES(?1,?2,?3,?4) ON CONFLICT DO NOTHING",params![device.0.to_be_bytes(),file.0.as_bytes(),record.header.position.device.0.to_be_bytes(),record.header.position.number.to_be_bytes()])?;
    }
    Ok(())
}

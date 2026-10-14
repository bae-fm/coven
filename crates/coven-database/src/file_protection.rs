//! Fixed remote paths named by current rows, losses and reversible inputs.

use crate::{sqlite::DatabaseConnection, write_schema::WriteSchema, DbError};
use coven_format::{
    file_reference::FileReference,
    loss::{Loss, LossValues},
    value::Value,
};
use coven_foundation::id_source::{DeviceId, FileId};
use rusqlite::params;
use std::collections::BTreeSet;

pub(crate) fn location<'a>(schema: &'a WriteSchema, table: &str) -> Option<&'a str> {
    schema
        .declarations
        .iter()
        .find(|declaration| declaration.name.eq_ignore_ascii_case(table))
        .and_then(|declaration| declaration.files.as_ref())
        .map(|file| file.location.as_str())
}

pub(crate) fn value(
    value: &Value,
    references: &mut BTreeSet<(DeviceId, FileId)>,
) -> Result<(), DbError> {
    if *value == Value::Null {
        return Ok(());
    }
    if let crate::file_location::StoredLocation::Uploaded(text) =
        crate::file_location::StoredLocation::decode(value)?
    {
        let reference = FileReference::decode(text.as_str())?;
        references.insert((reference.device, reference.id));
    }
    Ok(())
}

pub(crate) fn loss(
    schema: &WriteSchema,
    loss: &Loss,
    references: &mut BTreeSet<(DeviceId, FileId)>,
) -> Result<(), DbError> {
    if loss.retired {
        // Frozen values keep their original column names after declarations
        // disappear. Any canonical fixed file reference remains protective;
        // a non-file string coinciding with that encoding can only retain extra
        // bytes, never authorize deletion. Other strings are not file locations.
        match &loss.values {
            LossValues::Cell { cell, .. } => potential_reference(&cell.value.value, references),
            LossValues::Row(cells) => {
                for cell in cells.values() {
                    potential_reference(&cell.value.value, references);
                }
            }
        }
        return Ok(());
    }
    let Some(location) = location(schema, &loss.row.table) else {
        return Ok(());
    };
    match &loss.values {
        LossValues::Cell { column, cell } if column == location => {
            value(&cell.value.value, references)?
        }
        LossValues::Row(cells) => {
            if let Some(cell) = cells.get(location) {
                value(&cell.value.value, references)?;
            }
        }
        LossValues::Cell { .. } => (),
    }
    Ok(())
}

/// A loss may arrive without a displayed row change (schema exclusion or reload).
pub(crate) fn retain_loss(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    record: &Loss,
) -> Result<(), DbError> {
    let mut references = BTreeSet::new();
    loss(schema, record, &mut references)?;
    for (device, file) in &references {
        db.internal_execute("INSERT INTO _coven_row_file_refs(device,file,table_name,key) VALUES(?1,?2,?3,?4) ON CONFLICT DO NOTHING",params![device.0.to_be_bytes(),file.0.as_bytes(),record.row.table,record.row.key])?;
    }
    if !references.is_empty() {
        for input in db.query(
            "SELECT id FROM _coven_data_inputs WHERE audience=?1",
            [crate::write_encoding::audience_text(&record.row.audience)],
            |r| r.get::<_, i64>(0),
        )? {
            retain_input(db, input, &references)?;
        }
    }
    Ok(())
}

pub(crate) fn retain_input(
    db: &DatabaseConnection,
    input: i64,
    references: &BTreeSet<(DeviceId, FileId)>,
) -> Result<(), DbError> {
    for (device, file) in references {
        db.internal_execute("INSERT INTO _coven_input_file_refs(device,file,input) VALUES(?1,?2,?3) ON CONFLICT DO NOTHING",
            params![device.0.to_be_bytes(),file.0.as_bytes(),input])?;
    }
    Ok(())
}

/// Refresh only a changed app identity. All its audiences and retained losses
/// contribute; display removal alone cannot release their uploaded paths.
pub(crate) fn refresh(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    key: &crate::write_rows::AppKey,
) -> Result<(), DbError> {
    let location = location(schema, &key.0);
    if location.is_none()
        && !db.query_row(
            "SELECT EXISTS(SELECT 1 FROM _coven_row_file_refs WHERE table_name=?1 AND key=?2)",
            params![key.0, key.1],
            |r| r.get::<_, bool>(0),
        )?
    {
        return Ok(());
    }
    let mut references = BTreeSet::new();
    let mut audiences = BTreeSet::new();
    if let Some(location) = location {
        let visible = crate::write_rows::AppView::after(db, schema);
        if let Some(row) = visible.row(key)? {
            if let Some(file) = row.values.get(location) {
                value(file, &mut references)?;
            }
        }
        let store = crate::merge_store::MergeStore::new(db, &visible);
        db.for_each(
        "SELECT DISTINCT table_name,key,audience FROM _coven_rows WHERE table_name=?1 AND key=?2",
        params![key.0, key.1],
        |r| {
            let row = crate::row_queries::read_identity(r)?;
            audiences.insert(crate::write_encoding::audience_text(&row.audience));
            let state = store.row_without_losses(&row)?.state;
            if let Some(cell) = state.cells().get(location) {
                value(&cell.value.value, &mut references)?;
            }
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
            let record = crate::loss_record::read(r)?;
            audiences.insert(crate::write_encoding::audience_text(&record.row.audience));
            loss(schema, &record, &mut references)
        },
    )?;
    db.internal_execute(
        "DELETE FROM _coven_row_file_refs WHERE table_name=?1 AND key=?2",
        params![key.0, key.1],
    )?;
    for (device, file) in &references {
        db.internal_execute(
            "INSERT INTO _coven_row_file_refs(device,file,table_name,key) VALUES(?1,?2,?3,?4)",
            params![device.0.to_be_bytes(), file.0.as_bytes(), key.0, key.1],
        )?;
    }
    // Later changes can be replayed from any retained origin of this audience.
    // Its file protection ends with that input, in the same transaction.
    for audience in audiences {
        for input in db.query(
            "SELECT id FROM _coven_data_inputs WHERE audience=?1",
            [audience],
            |r| r.get::<_, i64>(0),
        )? {
            retain_input(db, input, &references)?;
        }
    }
    Ok(())
}

/// Schema changes can rename every file column. Rebuild affected declarations
/// inside that migration, keeping frozen references from removed declarations.
pub(crate) fn migrate(
    db: &DatabaseConnection,
    tables: &[crate::SyncedTable],
) -> Result<(), DbError> {
    if !tables.iter().any(|table| table.files.is_some()) {
        return Ok(());
    }
    let schema = WriteSchema::read(db, tables.to_vec())?;
    for table in tables.iter().filter(|table| table.files.is_some()) {
        let keys = db.query("SELECT key FROM _coven_rows WHERE table_name=?1 UNION SELECT key FROM _coven_lost WHERE table_name=?1 UNION SELECT key FROM _coven_row_file_refs WHERE table_name=?1",[&table.name],|r| r.get::<_,Vec<u8>>(0))?;
        for key in keys {
            refresh(db, &schema, &(table.name.clone(), key))?;
        }
    }
    Ok(())
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

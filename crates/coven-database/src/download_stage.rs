//! Stage a write's merged values in SQLite before changing app-visible rows.
use crate::merge_store::MergeStore;
use crate::write_encoding::{decoded, encoded};
use crate::write_rows::AppView;
use crate::{sqlite::DatabaseConnection, write_schema::WriteSchema, DbError};
use coven_format::{
    dismissal::WriteFrame,
    merge_fields,
    write::{OriginalRow, PartRecord, WriteDisposition, WriteHeader, WritePart, WriteRecord},
};
use coven_merge::{Audience, RowId};
use rusqlite::params;
use std::collections::{BTreeMap, BTreeSet};

/// The staging of one transaction's merged values: the rows its writes
/// staged one at a time, and the audiences a reload replaces whole, whose
/// other rows are staged in the snapshot's install tables. The tables stay on
/// the writer's connection, empty between transactions: creating or dropping
/// a temporary table expires every statement prepared on the connection, so
/// SQLite would parse and plan each again in every slot. A transaction that
/// rolls back takes its staged rows with it, and the tables too if it made
/// them.
pub(crate) fn begin(database: &DatabaseConnection) -> Result<(), DbError> {
    database.batch("CREATE TEMP TABLE IF NOT EXISTS _coven_download_rows(table_name TEXT NOT NULL,key BLOB NOT NULL,audience TEXT NOT NULL,columns BLOB,PRIMARY KEY(table_name,key,audience)) WITHOUT ROWID;
        CREATE TEMP TABLE IF NOT EXISTS _coven_download_dismissals(id INTEGER PRIMARY KEY,record BLOB NOT NULL);
        CREATE TEMP TABLE IF NOT EXISTS _coven_download_replaced(audience TEXT PRIMARY KEY NOT NULL) WITHOUT ROWID;
        CREATE TEMP TABLE IF NOT EXISTS _coven_received_records(write_id BLOB NOT NULL,audience TEXT NOT NULL,table_name TEXT NOT NULL,key BLOB NOT NULL,record BLOB NOT NULL,PRIMARY KEY(write_id,audience,table_name,key)) WITHOUT ROWID;")?;
    let staged: bool = database.query_row(
        "SELECT EXISTS(SELECT 1 FROM temp._coven_download_rows)
             OR EXISTS(SELECT 1 FROM temp._coven_download_dismissals)
             OR EXISTS(SELECT 1 FROM temp._coven_download_replaced)
             OR EXISTS(SELECT 1 FROM temp._coven_received_records)",
        [],
        |r| r.get(0),
    )?;
    if staged {
        return Err(DbError::Invariant {
            what: "a transaction's staging begins with no rows another left",
        });
    }
    Ok(())
}

/// The reload replaces `audience` with the snapshot it installed: the rows
/// no write stages are read from the install tables until it commits.
pub(crate) fn replace(database: &DatabaseConnection, audience: &Audience) -> Result<(), DbError> {
    database.internal_execute(
        "INSERT INTO temp._coven_download_replaced(audience) VALUES(?1) ON CONFLICT DO NOTHING",
        [audience],
    )?;
    Ok(())
}

/// Whether a reload replaces `audience` with the snapshot it installed.
pub(crate) fn replaced(
    database: &DatabaseConnection,
    audience: &Audience,
) -> Result<bool, DbError> {
    database.query_row(
        "SELECT EXISTS(SELECT 1 FROM temp._coven_download_replaced WHERE audience=?1)",
        [audience],
        |r| r.get(0),
    )
}

/// A present row's staged values: those a write staged for it, else those
/// of the snapshot that replaces its audience, else none staged.
pub(crate) fn values(
    database: &DatabaseConnection,
    schema: &crate::schema::Schema,
    row: &RowId,
) -> Result<Option<crate::write_rows::AppValues>, DbError> {
    let audience = &row.audience;
    let (bytes, replaced): (Option<Vec<u8>>, bool) = database.query_row(
        "SELECT (SELECT columns FROM temp._coven_download_rows WHERE table_name=?1 AND key=?2 AND audience=?3),
                EXISTS(SELECT 1 FROM temp._coven_download_replaced WHERE audience=?3)",
        params![row.table, row.key, audience],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    match bytes {
        Some(bytes) => Ok(Some(
            merge_fields::decode_columns(&bytes)?
                .into_iter()
                .map(|(name, value)| (name, value.value))
                .collect(),
        )),
        None if replaced => crate::snapshot_install::values(database, schema, row).map(Some),
        None => Ok(None),
    }
}

/// Keep one streamed frame: its original record, checked and retained as
/// evidence, or a dismissal for after the write's rows. Nothing merges until
/// the whole write is judged, since a foreign-key action and its causes can
/// be anywhere in it (§9.6).
pub(crate) fn ignored(
    database: &DatabaseConnection,
    header: &WriteHeader,
    audience: &Audience,
) -> Result<bool, DbError> {
    Ok(crate::write_boundary::WriteBoundary::ignore(
        &crate::write_boundary::WriteBoundary::judging(database)?,
        header,
        audience,
        crate::sequence::write_slot(database, header.position)?,
    ))
}

pub(crate) fn frame(
    database: &DatabaseConnection,
    form: &crate::write_sources::KeptForm<'_>,
    header: &WriteHeader,
    ignored: bool,
    frame: WriteFrame,
) -> Result<(), DbError> {
    let record = match &frame {
        WriteFrame::Change(row) => PartRecord::Change(row),
        WriteFrame::Execution(execution) => PartRecord::Execution(execution),
        WriteFrame::Dismissal(dismissal) => {
            if ignored {
                return Ok(());
            }
            crate::dismissal::validate_past(database, header, dismissal)?;
            database.internal_execute(
                "INSERT INTO temp._coven_download_dismissals(record) VALUES(?1)",
                [encoded(dismissal.encode())?],
            )?;
            return Ok(());
        }
    };
    if !ignored {
        crate::write_sources::validate(database, header, record)?;
    }
    crate::write_sources::retain(database, form, header, record)
}

/// Stage one received record whole until its write is judged and applied:
/// the history keeps only its kept form (D7.3).
pub(crate) fn stage_record(
    database: &DatabaseConnection,
    header: &WriteHeader,
    record: PartRecord<'_>,
) -> Result<(), DbError> {
    let row = record.row();
    database.internal_execute(
        "INSERT INTO temp._coven_received_records(write_id,audience,table_name,key,record) VALUES(?1,?2,?3,?4,?5)",
        params![
            merge_fields::encode_write_id(&header.position)?,
            &row.audience,
            row.table,
            row.key,
            record.encode()?
        ],
    )?;
    Ok(())
}

/// Release a received write's staged records once it is applied or consumed.
pub(crate) fn release(database: &DatabaseConnection, header: &WriteHeader) -> Result<(), DbError> {
    database.internal_execute(
        "DELETE FROM temp._coven_received_records WHERE write_id=?1",
        [merge_fields::encode_write_id(&header.position)?],
    )?;
    Ok(())
}

/// Where a write's whole records are while it is judged and applied: a
/// received write's in this slot's staging, and this device's migration
/// write's in its own table, since its marker carries none (§18.4).
pub(crate) fn records_table(header: &WriteHeader) -> &'static str {
    match header.disposition {
        WriteDisposition::Apply => "temp._coven_received_records",
        WriteDisposition::Migration => "_coven_migration_records",
    }
}

/// Judge the whole streamed write, then stage each opened part's admitted
/// changes one record at a time, as they were staged. A write in the local
/// captured prefix also stages its captured parts, but only in audiences
/// `selected` admits: a reload replays its slots only into the audiences it
/// loads, and the others already hold the write. An ordinary write's parts
/// come whole only from its slot, so each captured part it stages is one its
/// slot opened; the migration write's come from its own records (§18.4).
///
/// `ahead` is the breaking version a rebuild's write above its database's
/// version needs: every part of it is frozen as lost values.
///
/// A write landing in `slot` marks where each change it replaces stopped
/// being current, and where each of its own that never became current
/// stopped (§16.7). A queued write folded over the counted state has no slot
/// and marks nothing: what it replaces is current still in the counted state.
pub(crate) fn stage(
    database: &DatabaseConnection,
    schema: &WriteSchema,
    header: &WriteHeader,
    opened: &[Audience],
    slot: Option<coven_merge::Slot>,
    ahead: Option<u32>,
    selected: impl Fn(&Audience) -> Result<bool, DbError>,
) -> Result<(), DbError> {
    let mut captured = Vec::new();
    for audience in crate::captured_changes::audiences(database, header)? {
        if selected(&audience)? {
            if header.disposition == WriteDisposition::Apply && !opened.contains(&audience) {
                return Err(DbError::Invariant {
                    what: "a captured write's selected part is opened from its slot",
                });
            }
            captured.push(audience);
        }
    }
    let audiences: Vec<_> = opened
        .iter()
        .chain(captured.iter())
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let opened = &audiences;
    let interpreter =
        crate::interpretation::Interpreter::new(database, schema, header, opened, ahead)?;
    let write = merge_fields::encode_write_id(&header.position)?;
    for audience in opened {
        // One record at a time, past the last: admitting it updates its row.
        let mut last: Option<(String, Vec<u8>)> = None;
        let next = format!(
            "SELECT table_name,key,record FROM {records} WHERE write_id=?1 AND audience=?2
             AND (?3 IS NULL OR (table_name,key)>(?3,?4)) ORDER BY table_name,key LIMIT 1",
            records = records_table(header)
        );
        while let Some((table, key, bytes)) = database
            .query(
                &next,
                params![
                    write,
                    audience,
                    last.as_ref().map(|(t, _)| t),
                    last.as_ref().map(|(_, k)| k)
                ],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, Vec<u8>>(1)?, r.get::<_, Vec<u8>>(2)?)),
            )?
            .pop()
        {
            let (table, key) = &*last.insert((table, key));
            let mut part = WritePart {
                audience: audience.clone(),
                rows: Vec::new(),
                executions: Vec::new(),
                dismissals: Vec::new(),
            };
            match OriginalRow::decode(&bytes)? {
                OriginalRow::Change(row) => part.rows.push(row),
                OriginalRow::Execution(execution) => part.executions.push(execution),
            }
            // A change of a write landing in a slot that isn't current once
            // staged never became current: it stops at its own slot.
            let Some(part) = interpreter.part(part)? else {
                if let Some(slot) = slot {
                    crate::forget_records::stopped_at(
                        database,
                        header.position,
                        audience,
                        table,
                        key,
                        slot,
                    )?;
                }
                continue;
            };
            let record = WriteRecord { header: header.clone(), parts: vec![part] };
            let store = MergeStore::from_staged(database, &schema.schema);
            let updates = store.apply(&record)?;
            if let Some(slot) = slot {
                for (row, update) in &updates {
                    let before = store.row(row)?.state;
                    crate::forget_records::stopped(
                        database,
                        header.position,
                        &before,
                        &update.state,
                        slot,
                    )?;
                    crate::forget_entries::changed(database, &before, &update.state)?;
                }
            }
            crate::write_commit::commit(database, &record, &store, &updates)?;
            if let Some(slot) = slot {
                if !crate::forget_records::applied(
                    header.position,
                    updates.values().map(|update| &update.state),
                ) {
                    crate::forget_records::stopped_at(
                        database,
                        header.position,
                        audience,
                        table,
                        key,
                        slot,
                    )?;
                }
            }
            for (row, update) in updates {
                put_values(database, &row, &update.state)?;
            }
        }
    }
    crate::captured_changes::losses(database, schema, header)?;
    let dismissals = database.query(
        "SELECT record FROM temp._coven_download_dismissals ORDER BY id",
        [],
        |r| decoded(coven_format::dismissal::Dismissal::decode(&r.get::<_, Vec<u8>>(0)?)),
    )?;
    let mut parts = BTreeMap::<Audience, WritePart>::new();
    for dismissal in dismissals {
        parts
            .entry(dismissal.row.audience.clone())
            .or_insert_with(|| WritePart {
                audience: dismissal.row.audience.clone(),
                rows: Vec::new(),
                executions: Vec::new(),
                dismissals: Vec::new(),
            })
            .dismissals
            .push(dismissal);
    }
    // Rows whose lost values another device dismissed keep only what still
    // protects their files; a file left with no reference waits for this
    // device's own write to delete its record (§17.2).
    for key in crate::dismissal::apply(
        database,
        &WriteRecord { header: header.clone(), parts: parts.into_values().collect() },
    )? {
        crate::file_protection::refresh_received(database, schema, &key)?;
    }
    database.internal_execute("DELETE FROM temp._coven_download_dismissals", [])?;
    Ok(())
}

pub(crate) fn put_values(
    database: &DatabaseConnection,
    row: &RowId,
    state: &coven_merge::RowState<coven_format::value::Value>,
) -> Result<(), DbError> {
    database.internal_execute(
        "DELETE FROM temp._coven_download_rows WHERE table_name=?1 AND key=?2 AND audience=?3",
        params![row.table, row.key, &row.audience],
    )?;
    retain_values(database, row, state)
}

/// Keep raw values before app materialization can replace or cascade them.
/// Already staged rows retain the result produced by their merge frame.
pub(crate) fn retain_values(
    database: &DatabaseConnection,
    row: &RowId,
    state: &coven_merge::RowState<coven_format::value::Value>,
) -> Result<(), DbError> {
    let columns = if state.present() {
        Some(encoded(merge_fields::encode_columns(
            &state.cells().iter().map(|(name, cell)| (name.clone(), cell.value.clone())).collect(),
        ))?)
    } else {
        None
    };
    database.internal_execute("INSERT INTO temp._coven_download_rows(table_name,key,audience,columns) VALUES(?1,?2,?3,?4) ON CONFLICT DO NOTHING",params![row.table,row.key,&row.audience,columns])?;
    Ok(())
}

/// The rows staged so far in this transaction.
pub(crate) fn staged_rows(database: &DatabaseConnection) -> Result<Vec<RowId>, DbError> {
    database.query(
        "SELECT table_name,key,audience FROM temp._coven_download_rows",
        [],
        crate::row_queries::read_identity,
    )
}

/// Install app rows once after the counted and queued folds have completed.
pub(crate) fn materialize(
    database: &DatabaseConnection,
    before: &DatabaseConnection,
    schema: &WriteSchema,
    deleted: &BTreeMap<coven_foundation::id_source::CircleId, coven_merge::Slot>,
    touched: BTreeSet<RowId>,
    files: &crate::file_write::FileWrite<'_>,
) -> Result<(), DbError> {
    let region = rules(database, before, schema, deleted, touched)?;
    files.retain_rows(
        region.into_iter().map(|row| (row.table, row.key)).collect(),
        &crate::file_protection::Merged::new(),
    )?;
    finish(database)
}

/// Run the removal rules over the rows `touched` and the staged rows reach,
/// in both the committed state and this transaction's, and materialize the
/// app rows of that region. Returns the region.
pub(crate) fn rules(
    database: &DatabaseConnection,
    before: &DatabaseConnection,
    schema: &WriteSchema,
    deleted: &BTreeMap<coven_foundation::id_source::CircleId, coven_merge::Slot>,
    mut touched: BTreeSet<RowId>,
) -> Result<BTreeSet<RowId>, DbError> {
    touched.extend(database.query("SELECT table_name,key,audience FROM temp._coven_download_rows ORDER BY table_name,key,audience",[],crate::row_queries::read_identity)?);
    let prior = AppView::after(before, schema).without_row_cache();
    let old_store = MergeStore::from_schema(before, &schema.schema).with_recent_rows();
    let empty = BTreeMap::new();
    let old = crate::removal_view::DatabaseRemovalView::new(
        before, &old_store, schema, &prior, &empty, deleted, None,
    )?;
    let current = MergeStore::from_staged(database, &schema.schema).with_recent_rows();
    crate::write_apply::WriteApply::new(database, schema, &current, &prior, &prior, deleted)
        .replace(&old, touched)
}

/// Empty the staging once the transaction's rows are materialized.
pub(crate) fn finish(database: &DatabaseConnection) -> Result<(), DbError> {
    database.batch(
        "DELETE FROM temp._coven_download_rows; DELETE FROM temp._coven_download_dismissals; DELETE FROM temp._coven_download_replaced",
    )
}

/// The counted projection's losses must be captured before the local overlay.
/// A removal rule can carry the region to a row no write of the slot reached,
/// which still holds the queue's fold, so what this records for it can be
/// neither the counted nor the final outcome. Every row of the region is
/// staged, so materializing works each one out again under the state the
/// queue leaves (§6.2).
pub(crate) fn count(
    database: &DatabaseConnection,
    schema: &WriteSchema,
    touched: BTreeSet<RowId>,
) -> Result<(), DbError> {
    let app = AppView::after(database, schema).without_row_cache();
    let store = MergeStore::from_staged(database, &schema.schema).with_recent_rows();
    let empty = BTreeMap::new();
    let deleted = crate::store_state_tables::deleted_circles(database)?;
    let view = crate::removal_view::DatabaseRemovalView::new(
        database, &store, schema, &app, &empty, &deleted, None,
    )?;
    view.prime(touched.iter().cloned())?;
    let result =
        coven_merge::recompute(&view, &view, touched).map_err(|e| e.into_db_error(None))?;
    store.retain_materialization_values(&result.region)?;
    crate::removal::record(database, &view, &result, &BTreeMap::new())
}

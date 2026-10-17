//! Stage a write's merged values in SQLite before changing app-visible rows.
use crate::merge_store::MergeStore;
use crate::write_encoding::{audience_text, decoded, encoded};
use crate::write_rows::AppView;
use crate::{sqlite::DatabaseConnection, write_schema::WriteSchema, DbError};
use coven_format::{
    dismissal::WriteFrame,
    merge_fields,
    snapshot_rows::OriginalRow,
    write::{PartRecord, WriteHeader, WritePart, WriteRecord},
};
use coven_merge::{Audience, RowId};
use rusqlite::params;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) fn begin(database: &DatabaseConnection) -> Result<(), DbError> {
    database.batch("CREATE TEMP TABLE _coven_download_rows(table_name TEXT NOT NULL,key BLOB NOT NULL,audience TEXT NOT NULL,columns BLOB,PRIMARY KEY(table_name,key,audience)) WITHOUT ROWID;
        CREATE TEMP TABLE _coven_download_dismissals(id INTEGER PRIMARY KEY,record BLOB NOT NULL);")
}

pub(crate) fn values(
    database: &DatabaseConnection,
    row: &RowId,
) -> Result<Option<crate::write_rows::AppValues>, DbError> {
    let bytes: Option<Vec<u8>> = database.query_row("SELECT (SELECT columns FROM temp._coven_download_rows WHERE table_name=?1 AND key=?2 AND audience=?3)",params![row.table,row.key,audience_text(&row.audience)],|row|row.get(0))?;
    bytes
        .map(|bytes| {
            Ok(merge_fields::decode_columns(&bytes)?
                .into_iter()
                .map(|(name, value)| (name, value.value))
                .collect())
        })
        .transpose()
}

/// Keep one streamed frame: its original record, checked and retained as
/// evidence, or a dismissal for after the write's rows. Nothing merges until
/// the whole write is judged, since a foreign-key action and its causes can
/// be anywhere in it (§8.8).
pub(crate) fn ignored(
    database: &DatabaseConnection,
    header: &WriteHeader,
    audience: &Audience,
) -> Result<bool, DbError> {
    Ok(matches!(
        crate::write_boundary::WriteBoundary::part_verdict(
            &crate::write_boundary::WriteBoundary::judging(database)?,
            header,
            audience,
            crate::sequence::write_slot(database, header.position)?
        ),
        Some(coven_format::snapshot_rows::ChangeVerdict::Ignored(_))
    ))
}

pub(crate) fn frame(
    database: &DatabaseConnection,
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
    crate::write_sources::retain(database, header, record)
}

/// Judge the whole streamed write, then stage each opened part's admitted
/// changes one record at a time, as they were retained. A write in the local
/// captured prefix also stages its captured parts, but only in audiences
/// `selected` admits: a reload replays its slots only into the audiences it
/// loads, and the others already hold the write.
pub(crate) fn stage(
    database: &DatabaseConnection,
    schema: &WriteSchema,
    header: &WriteHeader,
    opened: &[Audience],
    selected: impl Fn(&Audience) -> Result<bool, DbError>,
) -> Result<(), DbError> {
    let mut captured = Vec::new();
    for audience in crate::captured_changes::audiences(database, header)? {
        if selected(&audience)? {
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
    let interpreter = crate::interpretation::Interpreter::new(database, schema, header, opened)?;
    let write = merge_fields::encode_write_id(&header.position)?;
    for audience in opened {
        // One record at a time, past the last: admitting it updates its row.
        let mut last: Option<(String, Vec<u8>)> = None;
        while let Some((table, key, bytes)) = database
            .query(
                "SELECT table_name,key,record FROM _coven_authored_changes WHERE write_id=?1 AND audience=?2
                 AND (?3 IS NULL OR (table_name,key)>(?3,?4)) ORDER BY table_name,key LIMIT 1",
                params![
                    write,
                    audience_text(audience),
                    last.as_ref().map(|(t, _)| t),
                    last.as_ref().map(|(_, k)| k)
                ],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, Vec<u8>>(1)?, r.get::<_, Vec<u8>>(2)?)),
            )?
            .pop()
        {
            last = Some((table, key));
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
            let Some(part) = interpreter.part(database, schema, part)? else {
                continue;
            };
            let record = WriteRecord {
                header: header.clone(),
                parts: vec![part],
            };
            let store = MergeStore::from_staged(database, &schema.schema);
            let updates = store.apply(&record)?;
            crate::write_commit::commit(database, &record, &store, &updates)?;
            for (row, update) in updates {
                put_values(database, &row, &update.state)?;
            }
        }
    }
    crate::captured_changes::losses(database, schema, header)?;
    let dismissals = database.query(
        "SELECT record FROM temp._coven_download_dismissals ORDER BY id",
        [],
        |r| {
            decoded(coven_format::dismissal::Dismissal::decode(
                &r.get::<_, Vec<u8>>(0)?,
            ))
        },
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
    crate::dismissal::apply(
        database,
        schema,
        &WriteRecord {
            header: header.clone(),
            parts: parts.into_values().collect(),
        },
    )?;
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
        params![row.table, row.key, audience_text(&row.audience)],
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
            &state
                .cells()
                .iter()
                .map(|(name, cell)| (name.clone(), cell.value.clone()))
                .collect(),
        ))?)
    } else {
        None
    };
    database.internal_execute("INSERT INTO temp._coven_download_rows(table_name,key,audience,columns) VALUES(?1,?2,?3,?4) ON CONFLICT DO NOTHING",params![row.table,row.key,audience_text(&row.audience),columns])?;
    Ok(())
}

/// Install app rows once after the counted and queued folds have completed.
pub(crate) fn materialize(
    database: &DatabaseConnection,
    before: &DatabaseConnection,
    schema: &WriteSchema,
    deleted: &BTreeMap<coven_foundation::id_source::CircleId, coven_merge::Slot>,
    mut touched: BTreeSet<RowId>,
    files: &crate::file_write::FileWrite<'_>,
) -> Result<(), DbError> {
    touched.extend(database.query("SELECT table_name,key,audience FROM temp._coven_download_rows ORDER BY table_name,key,audience",[],crate::row_queries::read_identity)?);
    let prior = AppView::after(before, schema).without_row_cache();
    let old_store = MergeStore::from_schema(before, &schema.schema).without_row_cache();
    let empty = BTreeMap::new();
    let old = crate::removal_view::DatabaseRemovalView::new(
        before, &old_store, schema, &prior, &empty, deleted, None,
    )?;
    let current = MergeStore::from_staged(database, &schema.schema).without_row_cache();
    let affected =
        crate::write_apply::WriteApply::new(database, schema, &current, &prior, &prior, deleted)
            .replace(&old, touched)?;
    files.retain_rows(affected)?;
    database
        .batch("DROP TABLE temp._coven_download_rows; DROP TABLE temp._coven_download_dismissals")
}

/// The counted projection's losses must be captured before the local overlay.
pub(crate) fn count(database: &DatabaseConnection, schema: &WriteSchema) -> Result<(), DbError> {
    let touched = database
        .query(
            "SELECT DISTINCT table_name,key,audience FROM _coven_rows",
            [],
            crate::row_queries::read_identity,
        )?
        .into_iter()
        .collect::<BTreeSet<_>>();
    let app = AppView::after(database, schema).without_row_cache();
    let store = MergeStore::from_staged(database, &schema.schema).without_row_cache();
    let empty = BTreeMap::new();
    let deleted = crate::store_state_tables::deleted_circles(database)?;
    let view = crate::removal_view::DatabaseRemovalView::new(
        database, &store, schema, &app, &empty, &deleted, None,
    )?;
    view.prime(touched.iter().cloned())?;
    let result =
        coven_merge::recompute(&view, &view, touched).map_err(|e| e.into_db_error(None))?;
    store.retain_materialization_values(&result.region)?;
    crate::removal::record(database, &view, &result)
}

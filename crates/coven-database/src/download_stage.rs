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
pub(crate) fn frame(
    database: &DatabaseConnection,
    header: &WriteHeader,
    audience: &Audience,
    frame: WriteFrame,
) -> Result<(), DbError> {
    crate::retained_writes::frame(database, header, audience, &frame)?;
    let record = match &frame {
        WriteFrame::Change(row) => PartRecord::Change(row),
        WriteFrame::Execution(execution) => PartRecord::Execution(execution),
        WriteFrame::Dismissal(dismissal) => {
            dismissal.validate_past(header)?;
            database.internal_execute(
                "INSERT INTO temp._coven_download_dismissals(record) VALUES(?1)",
                [encoded(dismissal.encode())?],
            )?;
            return Ok(());
        }
    };
    crate::write_sources::validate(database, header, record)?;
    crate::write_sources::retain(database, header, record)
}

/// Judge the whole streamed write, then stage each opened part's admitted
/// changes one record at a time, as they were retained.
fn stage(
    database: &DatabaseConnection,
    schema: &WriteSchema,
    header: &WriteHeader,
    opened: &[Audience],
) -> Result<(), DbError> {
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
                retain_values(database, &row, &update.state)?;
            }
        }
    }
    Ok(())
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

pub(crate) fn finish(
    database: &DatabaseConnection,
    before: &DatabaseConnection,
    schema: &WriteSchema,
    header: &WriteHeader,
    opened: &[Audience],
    deleted: &BTreeMap<coven_foundation::id_source::CircleId, crate::EntryId>,
    files: &crate::file_write::FileWrite<'_>,
) -> Result<(), DbError> {
    stage(database, schema, header, opened)?;
    let touched = database.query("SELECT table_name,key,audience FROM temp._coven_download_rows ORDER BY table_name,key,audience",[],crate::row_queries::read_identity)?.into_iter().collect::<BTreeSet<_>>();
    if !touched.is_empty() {
        let prior = AppView::after(before, schema).without_row_cache();
        let old_store = MergeStore::from_schema(before, &schema.schema).without_row_cache();
        let empty = BTreeMap::new();
        let old = crate::removal_view::DatabaseRemovalView::new(
            before, &old_store, schema, &prior, &empty, deleted, None,
        )?;
        let current = MergeStore::from_staged(database, &schema.schema).without_row_cache();
        let affected = crate::write_apply::WriteApply::new(
            database, schema, &current, &prior, &prior, deleted,
        )
        .replace(&old, touched)?;
        files.retain_rows(affected)?;
    }
    database.for_each::<_, crate::DbError>(
        "SELECT record FROM temp._coven_download_dismissals ORDER BY id",
        [],
        |row| {
            let dismissal = decoded(coven_format::dismissal::Dismissal::decode(
                &row.get::<_, Vec<u8>>(0)?,
            ))?;
            crate::download::apply_opened(
                database,
                schema,
                crate::DownloadedWrite {
                    header: header.clone(),
                    parts: vec![crate::DownloadedPart::Opened(WritePart {
                        audience: dismissal.row.audience.clone(),
                        rows: Vec::new(),
                        executions: Vec::new(),
                        dismissals: vec![dismissal],
                    })],
                },
                deleted,
            )?;
            Ok(())
        },
    )?;
    // An empty migration or a write containing only skipped parts still advances.
    let visible = AppView::after(database, schema);
    let store = MergeStore::new(database, &visible);
    let record = WriteRecord {
        header: header.clone(),
        parts: Vec::new(),
    };
    let updates = store.apply(&record)?;
    crate::write_commit::commit(database, &record, &store, &updates)?;
    database
        .batch("DROP TABLE temp._coven_download_rows; DROP TABLE temp._coven_download_dismissals")
}

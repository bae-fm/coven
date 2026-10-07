//! Stage a write's merged values in SQLite before changing app-visible rows.
use crate::merge_store::MergeStore;
use crate::write_encoding::{audience_text, decoded, encoded};
use crate::write_rows::AppView;
use crate::{sqlite::DatabaseConnection, write_schema::WriteSchema, DbError};
use coven_format::{
    dismissal::WriteFrame,
    merge_fields,
    write::{WriteHeader, WritePart, WriteRecord},
};
use coven_merge::{Audience, RowId};
use rusqlite::params;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) fn begin(database: &DatabaseConnection) -> Result<(), DbError> {
    database.batch("CREATE TEMP TABLE coven_download_rows(table_name TEXT NOT NULL,key BLOB NOT NULL,audience TEXT NOT NULL,columns BLOB,PRIMARY KEY(table_name,key,audience)) WITHOUT ROWID;
        CREATE TEMP TABLE coven_download_dismissals(id INTEGER PRIMARY KEY,record BLOB NOT NULL);")
}

pub(crate) fn values(
    database: &DatabaseConnection,
    row: &RowId,
) -> Result<Option<crate::write_rows::AppValues>, DbError> {
    let bytes: Option<Vec<u8>> = database.query_row("SELECT (SELECT columns FROM temp.coven_download_rows WHERE table_name=?1 AND key=?2 AND audience=?3)",params![row.table,row.key,audience_text(&row.audience)],|row|row.get(0))?;
    bytes
        .map(|bytes| {
            Ok(merge_fields::decode_columns(&bytes)?
                .into_iter()
                .map(|(name, value)| (name, value.value))
                .collect())
        })
        .transpose()
}

pub(crate) fn frame(
    database: &DatabaseConnection,
    schema: &WriteSchema,
    header: &WriteHeader,
    audience: &Audience,
    frame: WriteFrame,
) -> Result<(), DbError> {
    let mut part = WritePart {
        audience: audience.clone(),
        rows: Vec::new(),
        dismissals: Vec::new(),
    };
    match frame {
        WriteFrame::Change(row) => part.rows.push(row),
        WriteFrame::Dismissal(dismissal) => {
            dismissal.validate_past(header)?;
            database.internal_execute(
                "INSERT INTO temp.coven_download_dismissals(record) VALUES(?1)",
                [encoded(dismissal.encode())?],
            )?;
            return Ok(());
        }
    }
    let record = crate::download::accepted(
        database,
        schema,
        crate::DownloadedWrite {
            header: header.clone(),
            parts: vec![crate::DownloadedPart::Opened(part)],
        },
    )?;
    let store = MergeStore::from_staged(database, &schema.schema);
    let updates = store.apply(&record)?;
    crate::write_commit::commit(database, &record, &store, &updates)?;
    for (row, update) in updates {
        retain_values(database, &row, &update.state)?;
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
    database.internal_execute("INSERT INTO temp.coven_download_rows(table_name,key,audience,columns) VALUES(?1,?2,?3,?4) ON CONFLICT DO NOTHING",params![row.table,row.key,audience_text(&row.audience),columns])?;
    Ok(())
}

pub(crate) fn finish(
    database: &DatabaseConnection,
    before: &DatabaseConnection,
    schema: &WriteSchema,
    header: &WriteHeader,
    deleted: &BTreeSet<coven_foundation::id_source::CircleId>,
    files: &crate::file_write::FileWrite<'_>,
) -> Result<(), DbError> {
    let touched = database.query("SELECT table_name,key,audience FROM temp.coven_download_rows ORDER BY table_name,key,audience",[],crate::row_queries::read_identity)?.into_iter().collect::<BTreeSet<_>>();
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
        files.retain_rows(affected, deleted)?;
    }
    database.visit(
        "SELECT record FROM temp.coven_download_dismissals ORDER BY id",
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
    database.batch("DROP TABLE temp.coven_download_rows; DROP TABLE temp.coven_download_dismissals")
}

//! The counted data beneath this installation's provisional write queue.
//!
//! While originals await slots, preserve the published merge state separately.
//! Incoming slots rebuild from it, then fold the current queue once. App rows
//! are materialized only after both folds, in the enclosing transaction.
//!
//! The kept state holds only what no removal rule decides. Whether a kept row
//! shows, and its removal loss, are worked out under the schema that reads
//! them (`removals`); a breaking migration can change those rules (§6.2).

use crate::removal_schema::TableRules;
use crate::removal_view::DatabaseRemovalView;
use crate::schema::Schema;
use crate::write_encoding::{audience_text, decoded, encoded};
use crate::{
    merge_store::MergeStore, sqlite::DatabaseConnection, write_schema::WriteSchema, DbError,
};
use coven_format::{loss::Loss, merge_fields, snapshot_rows::MergeRow, value::Value};
use coven_merge::{RowId, RowState, RowUpdate, Rule};
use rusqlite::params;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) fn exists(db: &DatabaseConnection) -> Result<bool, DbError> {
    db.query_row("SELECT EXISTS(SELECT 1 FROM _coven_published)", [], |r| r.get(0))
}

/// Preserve each affected row once, before provisional changes reach it.
pub(crate) fn capture_rows<'a>(
    db: &DatabaseConnection,
    store: &MergeStore<'_>,
    rows: impl IntoIterator<Item = &'a RowId>,
) -> Result<(), DbError> {
    db.internal_execute(
        "INSERT INTO _coven_published(singleton) VALUES(1) ON CONFLICT DO NOTHING",
        [],
    )?;
    for row in rows {
        let audience = audience_text(&row.audience);
        let kept:bool=db.query_row("SELECT EXISTS(SELECT 1 FROM _coven_published_rows WHERE table_name=?1 AND key=?2 AND audience=?3)",params![row.table,row.key,audience],|r|r.get(0))?;
        if kept {
            continue;
        }
        #[cfg(any(test, feature = "test-utils"))]
        db.record_work(|work| work.published_rows += 1);
        put_row(db, store.row_without_losses(row)?.state)?;
        keep_losses(db, row)?;
        db.internal_execute("INSERT INTO _coven_published_creations SELECT * FROM _coven_creations WHERE table_name=?1 AND key=?2 AND audience=?3",params![row.table,row.key,audience])?;
        db.internal_execute("INSERT INTO _coven_published_file_refs SELECT device,file FROM _coven_row_file_refs WHERE table_name=?1 AND key=?2 ON CONFLICT DO NOTHING",params![row.table,row.key])?;
    }
    Ok(())
}

/// Keep a row's record in the counted state, under the row it names.
pub(crate) fn put_row(db: &DatabaseConnection, state: RowState<Value>) -> Result<(), DbError> {
    let row = state.row().clone();
    db.internal_execute(
        "INSERT INTO _coven_published_rows(table_name,key,audience,record) VALUES(?1,?2,?3,?4)",
        params![
            row.table,
            row.key,
            audience_text(&row.audience),
            encoded(merge_fields::encode_merge_row(&MergeRow { state }))?
        ],
    )?;
    Ok(())
}

/// Keep a loss in the counted state, beside its row's record.
pub(crate) fn put_loss(db: &DatabaseConnection, loss: &Loss) -> Result<(), DbError> {
    db.internal_execute(
        "INSERT INTO _coven_published_losses(table_name,key,audience,record) VALUES(?1,?2,?3,?4)",
        params![
            loss.row.table,
            loss.row.key,
            audience_text(&loss.row.audience),
            encoded(merge_fields::encode_loss(loss))?
        ],
    )?;
    Ok(())
}

/// Keep `row`'s losses beside its record. Whether the row shows, and the
/// removal loss that holds it out, are the current rules' outcome: the
/// counted removal view works them out again under whatever schema reads
/// them (`removals`). Only what no rule decides is kept: frozen losses, and
/// the cells concurrent writes displaced.
fn keep_losses(db: &DatabaseConnection, row: &RowId) -> Result<(), DbError> {
    let audience = audience_text(&row.audience);
    db.for_each(&format!("{} WHERE l.table_name=?1 AND l.key=?2 AND l.audience=?3 AND NOT (l.replacement_kind='rules' AND l.frozen=0)",crate::loss_record::SELECT),params![row.table,row.key,audience],|r| {
        put_loss(db, &crate::loss_record::read(r)?)
    })
}

/// Preserve once, before a queued migration write's losses reach them, the
/// losses of `rows` whose table that migration dropped: the counted state
/// holds nothing else of them (§18.7).
pub(crate) fn capture_dropped<'a>(
    db: &DatabaseConnection,
    rows: impl IntoIterator<Item = &'a RowId>,
) -> Result<(), DbError> {
    db.internal_execute(
        "INSERT INTO _coven_published(singleton) VALUES(1) ON CONFLICT DO NOTHING",
        [],
    )?;
    for row in rows {
        let audience = audience_text(&row.audience);
        let kept: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM _coven_published_dropped WHERE table_name=?1 AND key=?2 AND audience=?3)
             OR EXISTS(SELECT 1 FROM _coven_published_losses WHERE table_name=?1 AND key=?2 AND audience=?3)",
            params![row.table, row.key, audience],
            |r| r.get(0),
        )?;
        if kept {
            continue;
        }
        db.internal_execute(
            "INSERT INTO _coven_published_dropped(table_name,key,audience) VALUES(?1,?2,?3)",
            params![row.table, row.key, audience],
        )?;
        keep_losses(db, row)?;
    }
    Ok(())
}

/// Before a migration's statement changes `tables`, by lowercase name, keep
/// the counted state of their rows: the migration carries it through the new
/// schema with them (§18.1). A row kept already, by an earlier statement or a
/// queued write, keeps what it kept.
pub(crate) fn before_migrating(
    db: &DatabaseConnection,
    schema: &crate::schema::Schema,
    tables: &BTreeSet<String>,
) -> Result<(), DbError> {
    let store = MergeStore::from_schema(db, schema).without_row_cache();
    for table in tables.iter().filter_map(|table| schema.tables.get(table)) {
        let rows = db.query(
            "SELECT table_name,key,audience FROM _coven_rows WHERE table_name=?1
             UNION SELECT table_name,key,audience FROM _coven_lost WHERE table_name=?1",
            [&table.name],
            crate::row_queries::read_identity,
        )?;
        capture_rows(db, &store, &rows)?;
    }
    Ok(())
}

/// The counted state kept aside for `row`, if the queue reached it.
pub(crate) fn kept_state(
    db: &DatabaseConnection,
    row: &RowId,
) -> Result<Option<RowState<Value>>, DbError> {
    let Some(bytes) = db
        .query(
            "SELECT record FROM _coven_published_rows WHERE table_name=?1 AND key=?2 AND audience=?3",
            params![row.table, row.key, audience_text(&row.audience)],
            |r| r.get::<_, Vec<u8>>(0),
        )?
        .pop()
    else {
        return Ok(None);
    };
    let oracle = crate::write_metadata::DatabaseMetadata::new(db);
    let merged = merge_fields::decode_merge_row(&bytes, &oracle)
        .map_err(crate::write_metadata::MetadataFailure::stored)?;
    Ok(Some(merged.state))
}

/// The removal rules that take each kept row out of the counted projection
/// under `schema`, whose synced tables have `rules`: the rows the queue
/// reached, as the slots alone leave them (§6.2). Every other row's removal
/// is the one its stored state records.
pub(crate) fn removals(
    db: &DatabaseConnection,
    schema: &Schema,
    rules: &BTreeMap<String, TableRules>,
) -> Result<BTreeMap<RowId, BTreeSet<Rule>>, DbError> {
    if !exists(db)? {
        return Ok(BTreeMap::new());
    }
    let kept: BTreeSet<RowId> = db
        .query(
            "SELECT table_name,key,audience FROM _coven_published_rows",
            [],
            crate::row_queries::read_identity,
        )?
        .into_iter()
        .collect();
    let store = MergeStore::from_schema(db, schema).with_recent_rows();
    let deleted = crate::store_state_tables::deleted_circles(db)?;
    let view = DatabaseRemovalView::counted(db, &store, schema, rules, &deleted);
    view.prime(kept.iter().cloned())?;
    let mut result =
        coven_merge::recompute(&view, &view, kept.clone()).map_err(|e| e.into_db_error(None))?;
    result.removed.retain(|row, _| kept.contains(row));
    Ok(result.removed)
}

/// The removal losses of the kept rows the counted projection takes out
/// under `schema` and its synced tables' `rules`.
pub(crate) fn removal_losses(
    db: &DatabaseConnection,
    schema: &Schema,
    rules: &BTreeMap<String, TableRules>,
) -> Result<Vec<Loss>, DbError> {
    let mut losses = Vec::new();
    for (row, rules) in removals(db, schema, rules)? {
        let state = kept_state(db, &row)?
            .ok_or(DbError::Invariant { what: "a kept row's removal names a kept record" })?;
        losses.push(Loss::removed(&state, rules));
    }
    Ok(losses)
}

/// The removal losses of the kept rows under `before`, the schema a
/// migration run starts from, read before any of its statements runs. A
/// table the run drops keeps them frozen (`published_migration::carry`):
/// once its rules are gone, nothing could work them out again.
pub(crate) fn removal_losses_before_migrating(
    db: &DatabaseConnection,
    before: &Schema,
) -> Result<Vec<Loss>, DbError> {
    if !exists(db)? {
        return Ok(Vec::new());
    }
    // Every synced table has its columns registered (§18.1).
    let mut rules = BTreeMap::new();
    for name in
        db.query("SELECT DISTINCT lower(table_name) FROM _coven_columns WHERE synced=1", [], |r| {
            r.get::<_, String>(0)
        })?
    {
        if let Some(table) = before.tables.get(&name) {
            rules.insert(table.name.clone(), TableRules::read(db, table)?);
        }
    }
    crate::write_schema::with_rule_evaluators(db, before, &rules, || {
        removal_losses(db, before, &rules)
    })
}

pub(crate) fn clear(db: &DatabaseConnection) -> Result<(), DbError> {
    for table in [
        "_coven_published_rows",
        "_coven_published_losses",
        "_coven_published_creations",
        "_coven_published_dropped",
        "_coven_published_file_refs",
        "_coven_published",
    ] {
        db.internal_execute(&format!("DELETE FROM {table}"), [])?;
    }
    Ok(())
}

/// Restore only merge metadata, staged values and the kept losses; the
/// previously visible app tables remain untouched until the final
/// materialization, which works out each restored row's removal under the
/// current schema and records its removal loss.
pub(crate) fn restore(
    db: &DatabaseConnection,
    schema: &WriteSchema,
) -> Result<BTreeSet<RowId>, DbError> {
    if !exists(db)? {
        return Ok(BTreeSet::new());
    }
    let rows=db.query("SELECT table_name,key,audience FROM _coven_published_rows UNION SELECT table_name,key,audience FROM _coven_published_losses UNION SELECT table_name,key,audience FROM _coven_published_dropped",[],crate::row_queries::read_identity)?;
    let mut touched = BTreeSet::new();
    #[cfg(any(test, feature = "test-utils"))]
    db.record_work(|work| work.published_rows += rows.len());
    for row in rows {
        let audience = audience_text(&row.audience);
        crate::write_commit::delete_children(db, &crate::write_commit::StoredRows::Row(&row))?;
        for table in ["_coven_rows", "_coven_lost", "_coven_creations"] {
            db.internal_execute(
                &format!("DELETE FROM {table} WHERE table_name=?1 AND key=?2 AND audience=?3"),
                params![row.table, row.key, audience],
            )?;
        }
        if schema.schema.tables.contains_key(&row.table.to_ascii_lowercase()) {
            touched.insert(row);
        }
    }
    // The oracle reads the originals' metadata, which provisional application
    // never changes. It is independent of the rows being restored.
    let oracle = crate::write_metadata::DatabaseMetadata::new(db);
    db.for_each(
        "SELECT record FROM _coven_published_rows ORDER BY audience,table_name,key",
        [],
        |r| {
            let bytes: Vec<u8> = r.get(0)?;
            let merged = merge_fields::decode_merge_row(&bytes, &oracle)
                .map_err(crate::write_metadata::MetadataFailure::stored)?;
            let row = merged.state.row().clone();
            if !schema.schema.tables.contains_key(&row.table.to_ascii_lowercase()) {
                return Ok::<_, DbError>(());
            }
            crate::download_stage::put_values(db, &row, &merged.state)?;
            crate::write_commit::persist(
                db,
                &BTreeMap::from([(
                    row.clone(),
                    RowUpdate { state: merged.state, lost_changes: Vec::new() },
                )]),
                |row| {
                    Ok(crate::merge_store::StoredRow {
                        state: RowState::new(row.clone()),
                        ordinal: None,
                        loss: None,
                        lost_ids: BTreeMap::new(),
                    })
                },
                |id| crate::snapshot_state::ordinal(db, id),
            )?;
            if schema.schema.tables.contains_key(&row.table.to_ascii_lowercase()) {
                touched.insert(row);
            }
            Ok::<_, DbError>(())
        },
    )?;
    db.for_each("SELECT record FROM _coven_published_losses ORDER BY id", [], |r| {
        let loss = decoded(merge_fields::decode_loss(&r.get::<_, Vec<u8>>(0)?))?;
        crate::loss_record::keep(db, schema, &loss)
    })?;
    db.internal_execute(
        "INSERT INTO _coven_creations SELECT * FROM _coven_published_creations",
        [],
    )?;
    clear(db)?;
    Ok(touched)
}

#[cfg(test)]
#[path = "published_tests.rs"]
pub(crate) mod tests;

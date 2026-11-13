//! The counted data beneath this installation's provisional write queue.
//!
//! While originals await slots, preserve the published merge state separately.
//! An incoming slot puts back the kept rows it reaches, then folds again the
//! queued writes that reach them (§6.2). App rows are materialized only after
//! both folds, in the enclosing transaction.
//!
//! The kept state holds only what no removal rule decides. Whether a kept row
//! shows, and its removal loss, are worked out under the schema that reads
//! them (`removals`); a breaking migration can change those rules (§6.2).

use crate::removal_schema::TableRules;
use crate::removal_view::DatabaseRemovalView;
use crate::schema::Schema;
use crate::write_encoding::{decoded, encoded};
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

/// How many rows one statement of `capture_rows` covers.
const CAPTURE_CHUNK: usize = 500;

/// A `VALUES` list of `count` row identities, each `(table, key, audience)`.
fn identity_values(count: usize) -> String {
    vec!["(?,?,?)"; count].join(",")
}

/// The parameters of `identity_values` for `rows`.
fn identity_parameters<'r>(rows: &[&'r RowId]) -> Vec<&'r dyn rusqlite::ToSql> {
    rows.iter()
        .flat_map(|row| [&row.table as &dyn rusqlite::ToSql, &row.key, &row.audience])
        .collect()
}

/// Preserve each affected row once, before provisional changes reach it. A
/// row kept already keeps what it kept: the first capture wins. Rows are kept
/// aside a chunk at a time, so a write's rows share each statement.
pub(crate) fn capture_rows<'a>(
    db: &DatabaseConnection,
    store: &MergeStore<'_>,
    rows: impl IntoIterator<Item = &'a RowId>,
) -> Result<(), DbError> {
    db.internal_execute(
        "INSERT INTO _coven_published(singleton) VALUES(1) ON CONFLICT DO NOTHING",
        [],
    )?;
    let rows: BTreeSet<&RowId> = rows.into_iter().collect();
    let rows: Vec<&RowId> = rows.into_iter().collect();
    for chunk in rows.chunks(CAPTURE_CHUNK) {
        let kept: BTreeSet<RowId> = db
            .query(
                &format!(
                    "WITH k(t,k,a) AS (VALUES {}) SELECT k.t,k.k,k.a FROM k CROSS JOIN _coven_published_rows p ON p.table_name=k.t AND p.key=k.k AND p.audience=k.a",
                    identity_values(chunk.len())
                ),
                rusqlite::params_from_iter(identity_parameters(chunk)),
                crate::row_queries::read_identity,
            )?
            .into_iter()
            .collect();
        let new: Vec<&RowId> = chunk.iter().copied().filter(|row| !kept.contains(*row)).collect();
        if new.is_empty() {
            continue;
        }
        #[cfg(any(test, feature = "test-utils"))]
        db.record_work(|work| work.published_rows += new.len());
        let states = new
            .iter()
            .map(|row| Ok(store.row_without_losses(row)?.state))
            .collect::<Result<Vec<_>, DbError>>()?;
        put_rows(db, &states)?;
        keep_losses(db, &new)?;
        db.internal_execute(
            &format!(
                "WITH k(t,k,a) AS (VALUES {}) INSERT INTO _coven_published_creations SELECT c.* FROM k CROSS JOIN _coven_creations c ON c.table_name=k.t AND c.key=k.k AND c.audience=k.a",
                identity_values(new.len())
            ),
            rusqlite::params_from_iter(identity_parameters(&new)),
        )?;
        db.internal_execute(
            &format!(
                "WITH k(t,k,a) AS (VALUES {}) INSERT INTO _coven_published_file_refs(table_name,key,audience,device,object) SELECT f.table_name,f.key,k.a,f.device,f.object FROM k CROSS JOIN _coven_row_file_refs f ON f.table_name=k.t AND f.key=k.k WHERE true ON CONFLICT DO NOTHING",
                identity_values(new.len())
            ),
            rusqlite::params_from_iter(identity_parameters(&new)),
        )?;
    }
    Ok(())
}

/// Keep a row's record in the counted state, under the row it names.
pub(crate) fn put_row(db: &DatabaseConnection, state: RowState<Value>) -> Result<(), DbError> {
    put_rows(db, &[state])
}

/// Keep each state's record in the counted state, under the row it names, and
/// name the writes they hold.
fn put_rows(db: &DatabaseConnection, states: &[RowState<Value>]) -> Result<(), DbError> {
    use crate::insert_rows::{insert_rows, value};
    let mut ordinals = BTreeMap::new();
    let mut names = Vec::new();
    for state in states {
        let row = state.row();
        let writes: BTreeSet<_> = state
            .generations()
            .values()
            .copied()
            .chain(state.cells().values().map(|cell| cell.write))
            .collect();
        for write in writes {
            let ordinal = match ordinals.get(&write) {
                Some(ordinal) => *ordinal,
                None => {
                    let ordinal = crate::snapshot_state::ordinal(db, write)?;
                    ordinals.insert(write, ordinal);
                    ordinal
                }
            };
            names.push(vec![
                value(&row.table)?,
                value(&row.key)?,
                value(&row.audience)?,
                value(&ordinal)?,
            ]);
        }
    }
    insert_rows(
        db,
        4,
        &names,
        |values| {
            format!("INSERT INTO _coven_published_writes(table_name,key,audience,write_id) VALUES {values} ON CONFLICT DO NOTHING")
        },
        |_| Ok(()),
    )?;
    let mut records = Vec::with_capacity(states.len());
    for state in states {
        let row = state.row();
        records.push(vec![
            value(&row.table)?,
            value(&row.key)?,
            value(&row.audience)?,
            value(&encoded(merge_fields::encode_merge_row(&MergeRow { state: state.clone() }))?)?,
        ]);
    }
    insert_rows(
        db,
        4,
        &records,
        |values| {
            format!(
                "INSERT INTO _coven_published_rows(table_name,key,audience,record) VALUES {values}"
            )
        },
        |_| Ok(()),
    )?;
    Ok(())
}

/// Keep a loss in the counted state, beside its row's record.
pub(crate) fn put_loss(db: &DatabaseConnection, loss: &Loss) -> Result<(), DbError> {
    name_writes(db, &loss.row, crate::forget_entries::active_writes(loss))?;
    db.internal_execute(
        "INSERT INTO _coven_published_losses(table_name,key,audience,record) VALUES(?1,?2,?3,?4)",
        params![
            loss.row.table,
            loss.row.key,
            &loss.row.audience,
            encoded(merge_fields::encode_loss(loss))?
        ],
    )?;
    Ok(())
}

/// Record that the counted state kept for `row` names `writes`, whose
/// entries it reads when it is put back (§16.7).
fn name_writes(
    db: &DatabaseConnection,
    row: &RowId,
    writes: impl IntoIterator<Item = coven_merge::WriteId>,
) -> Result<(), DbError> {
    for write in writes {
        db.internal_execute(
            "INSERT INTO _coven_published_writes(table_name,key,audience,write_id) VALUES(?1,?2,?3,?4) ON CONFLICT DO NOTHING",
            params![row.table, row.key, &row.audience, crate::snapshot_state::ordinal(db, write)?],
        )?;
    }
    Ok(())
}

/// Keep the losses of `rows` beside their records. Whether a row shows, and
/// the removal loss that holds it out, are the current rules' outcome: the
/// counted removal view works them out again under whatever schema reads
/// them (`removals`). Only what no rule decides is kept: frozen losses, and
/// the cells concurrent writes displaced.
fn keep_losses(db: &DatabaseConnection, rows: &[&RowId]) -> Result<(), DbError> {
    for chunk in rows.chunks(CAPTURE_CHUNK) {
        db.for_each(
            &format!(
                "WITH k(t,k,a) AS (VALUES {}) {} WHERE (l.audience,l.table_name,l.key) IN (SELECT a,t,k FROM k) AND NOT (l.replacement_kind='rules' AND l.frozen=0)",
                identity_values(chunk.len()),
                crate::loss_record::SELECT
            ),
            rusqlite::params_from_iter(identity_parameters(chunk)),
            |r| put_loss(db, &crate::loss_record::read(r)?),
        )?;
    }
    Ok(())
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
        let audience = &row.audience;
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
        keep_losses(db, &[row])?;
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
            params![row.table, row.key, &row.audience],
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
    crate::forget_entries::unnamed_by_published(db, None)?;
    for table in [
        "_coven_published_writes",
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

/// Put back every kept row (`put_back`) and release the kept state: a slot
/// that changes membership, cut-offs or boundaries changes how every queued
/// write applies, a reload replaces the state beneath them all, and once no
/// queued write is left nothing waits to be folded over the counted state.
pub(crate) fn restore(
    db: &DatabaseConnection,
    schema: &WriteSchema,
) -> Result<BTreeSet<RowId>, DbError> {
    if !exists(db)? {
        return Ok(BTreeSet::new());
    }
    let rows=db.query("SELECT table_name,key,audience FROM _coven_published_rows UNION SELECT table_name,key,audience FROM _coven_published_losses UNION SELECT table_name,key,audience FROM _coven_published_dropped",[],crate::row_queries::read_identity)?;
    let mut touched = BTreeSet::new();
    for row in rows {
        if put_back(db, schema, &row)? {
            touched.insert(row);
        }
    }
    clear(db)?;
    Ok(touched)
}

/// Put back those of `rows` the counted projection keeps, and release what
/// it kept of them: a slot of writes reaches only these (§6.2). A row with
/// nothing kept holds the counted state already. Returns the rows put back
/// whose table the current schema has.
pub(crate) fn restore_rows<'a>(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    rows: impl IntoIterator<Item = &'a RowId>,
) -> Result<BTreeSet<RowId>, DbError> {
    let mut touched = BTreeSet::new();
    for row in rows {
        let audience = &row.audience;
        let kept: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM _coven_published_rows WHERE table_name=?1 AND key=?2 AND audience=?3)
             OR EXISTS(SELECT 1 FROM _coven_published_losses WHERE table_name=?1 AND key=?2 AND audience=?3)
             OR EXISTS(SELECT 1 FROM _coven_published_dropped WHERE table_name=?1 AND key=?2 AND audience=?3)",
            params![row.table, row.key, audience],
            |r| r.get(0),
        )?;
        if !kept {
            continue;
        }
        if put_back(db, schema, row)? {
            touched.insert(row.clone());
        }
        crate::forget_entries::unnamed_by_published(db, Some(row))?;
        for table in [
            "_coven_published_writes",
            "_coven_published_rows",
            "_coven_published_losses",
            "_coven_published_creations",
            "_coven_published_dropped",
            "_coven_published_file_refs",
        ] {
            db.internal_execute(
                &format!("DELETE FROM {table} WHERE table_name=?1 AND key=?2 AND audience=?3"),
                params![row.table, row.key, audience],
            )?;
        }
    }
    Ok(touched)
}

/// Put back `row`'s kept merge metadata, staged values, losses and creation
/// in place of its current ones; the previously visible app row remains
/// untouched until the final materialization, which works out the row's
/// removal under the current schema and records its removal loss. Returns
/// whether the current schema has the row's table.
fn put_back(db: &DatabaseConnection, schema: &WriteSchema, row: &RowId) -> Result<bool, DbError> {
    #[cfg(any(test, feature = "test-utils"))]
    db.record_work(|work| work.published_rows += 1);
    crate::write_commit::forget(db, &crate::write_commit::StoredRows::Row(row))?;
    crate::loss_record::forget_row(db, row)?;
    crate::ownership::forget_row(db, row)?;
    let present = schema.schema.tables.contains_key(&row.table.to_ascii_lowercase());
    if present {
        if let Some(state) = kept_state(db, row)? {
            crate::download_stage::put_values(db, row, &state)?;
            crate::write_commit::persist(
                db,
                &BTreeMap::from([(row.clone(), RowUpdate { state, lost_changes: Vec::new() })]),
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
        }
    }
    db.for_each(
        "SELECT record FROM _coven_published_losses WHERE table_name=?1 AND key=?2 AND audience=?3 ORDER BY id",
        params![row.table, row.key, &row.audience],
        |r| {
            let loss = decoded(merge_fields::decode_loss(&r.get::<_, Vec<u8>>(0)?))?;
            crate::loss_record::keep(db, schema, &loss)
        },
    )?;
    crate::ownership::restore_published(db, row)?;
    Ok(present)
}

#[cfg(test)]
#[path = "published_tests.rs"]
pub(crate) mod tests;

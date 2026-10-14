//! Rows a breaking migration deletes through the ordinary generation path
//! (§17.1): those removal rules hide independently of non-final circle
//! deletions. Each becomes a delete at its generation in the migration write,
//! with one frozen whole-row loss keeping its written values, old names and
//! setters; a row hidden only through a non-final deletion is left out.
use crate::entry_inputs::FINAL;
use crate::merge_store::MergeStore;
use crate::migration_names::MigrationMatch;
use crate::removal_view::DatabaseRemovalView;
use crate::sqlite::DatabaseConnection;
use crate::write_rows::AppView;
use crate::write_schema::WriteSchema;
use crate::DbError;
use coven_format::loss::{Loss, LossCause, LossValues};
use coven_format::value::Value;
use coven_format::write::RowChange;
use coven_foundation::id_source::CircleId;
use coven_merge::{Change, Operation, RowId, RowState};
use std::collections::{BTreeMap, BTreeSet};

/// The rows removal rules hide before the migration, under their old names.
pub(crate) struct Hidden {
    /// Merge state under the old identity and column names, by new identity.
    rows: BTreeMap<RowId, (RowId, RowState<Value>)>,
    /// Removed-row losses naming a column the migration drops, by new identity.
    dropped: BTreeMap<RowId, Loss>,
}

impl Hidden {
    /// Read every hidden row before identities move. A hidden row of a dropped
    /// table has no generation left to delete; its losses are frozen with the
    /// table's (`migration_state::carry`).
    pub(crate) fn before(
        db: &DatabaseConnection,
        before: &crate::schema::Schema,
        names: &MigrationMatch,
    ) -> Result<Self, DbError> {
        let store = MergeStore::from_schema(db, before);
        let mut rows = BTreeMap::new();
        let mut dropped = BTreeMap::new();
        for (id, row) in db.query(
            "SELECT id,table_name,key,audience FROM _coven_lost WHERE frozen=0 AND replacement_kind='rules'",
            [],
            |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    RowId {
                        table: r.get(1)?,
                        key: r.get(2)?,
                        audience: crate::write_encoding::audience(&r.get::<_, String>(3)?)?,
                    },
                ))
            },
        )? {
            let Some(table) = names.tables.get(&row.table) else {
                continue;
            };
            let state = store.row_without_losses(&row)?.state;
            let new = RowId {
                table: table.clone(),
                ..row.clone()
            };
            if state
                .cells()
                .keys()
                .any(|column| !names.columns.contains_key(&(row.table.clone(), column.clone())))
            {
                let loss = db.query_row(
                    &format!("{} WHERE l.id=?1", crate::loss_record::SELECT),
                    [id],
                    crate::loss_record::read,
                )?;
                dropped.insert(new.clone(), crate::migration_state::frozen(loss));
            }
            rows.insert(new, (row, state));
        }
        Ok(Self { rows, dropped })
    }

    /// Lean `Migration.freezeDropped` for a row the migration leaves hidden:
    /// its removed-row loss names a dropped column, so a frozen copy keeps the
    /// whole row and its cause while the active one follows the new schema.
    pub(crate) fn freeze_dropped(
        &self,
        db: &DatabaseConnection,
        schema: &WriteSchema,
        selected: &BTreeSet<RowId>,
    ) -> Result<(), DbError> {
        for (row, loss) in &self.dropped {
            if !selected.contains(row) {
                crate::loss_record::put(db, loss, None)?;
                crate::file_protection::retain_loss(db, schema, loss)?;
            }
        }
        Ok(())
    }

    /// Lean `migrationRemoves`: rerun the three removal passes without
    /// non-final deleted-circle causes. Without such a deletion the current
    /// hidden set is that answer. With one, a row the deletions decide is
    /// left out, and a row that loses only once they are absent is included.
    pub(crate) fn select(
        &self,
        db: &DatabaseConnection,
        schema: &WriteSchema,
        visible: &AppView<'_>,
    ) -> Result<BTreeSet<RowId>, DbError> {
        let deleted = crate::store_log_tables::deleted_circles(db)?;
        let final_deleted = final_deletions(db, &deleted)?;
        let current: BTreeSet<_> = self.rows.keys().cloned().collect();
        if final_deleted.len() == deleted.len() {
            return Ok(current);
        }
        let removed = |deleted: &BTreeMap<CircleId, crate::EntryId>| {
            let store = MergeStore::new(db, visible);
            let updates = BTreeMap::new();
            let view =
                DatabaseRemovalView::new(db, &store, schema, visible, &updates, deleted, None)?;
            Ok::<_, DbError>(
                coven_merge::removals(&view)
                    .map_err(|error| error.into_db_error(None))?
                    .removed
                    .into_keys()
                    .collect::<BTreeSet<_>>(),
            )
        };
        let all = removed(&deleted)?;
        let independent = removed(&final_deleted)?;
        let decided: BTreeSet<_> = all.difference(&independent).cloned().collect();
        Ok(current
            .difference(&decided)
            .cloned()
            .chain(independent.difference(&all).cloned())
            .collect())
    }

    /// The migration write's delete of one selected row at its generation,
    /// naming the generation and every setter it replaces.
    pub(crate) fn delete(
        db: &DatabaseConnection,
        store: &MergeStore<'_>,
        row: &RowId,
    ) -> Result<Option<RowChange>, DbError> {
        let state = store.row(row)?.state;
        let generation = state.generation();
        if generation.is_multiple_of(2) {
            return Ok(None);
        }
        let old = state
            .cells()
            .iter()
            .map(|(column, cell)| (column.clone(), cell.value.value.clone()))
            .collect();
        crate::write_sources::authored(
            db,
            store,
            row.clone(),
            Change {
                generation,
                operation: Operation::Delete,
            },
            old,
        )
        .map(Some)
    }

    /// One frozen whole-row loss per deleted row, caused by this breaking
    /// version and naming the migration write: the written values under
    /// their old names, with their original setters and no live parent links.
    pub(crate) fn freeze(
        &self,
        db: &DatabaseConnection,
        schema: &WriteSchema,
        store: &MergeStore<'_>,
        row: &RowId,
        migration: coven_merge::WriteId,
        version: u32,
    ) -> Result<(), DbError> {
        let (original, state) = match self.rows.get(row) {
            Some((original, state)) => (original.clone(), state.clone()),
            None => (row.clone(), store.row_without_losses(row)?.state),
        };
        let mut cells = state.cells().clone();
        for cell in cells.values_mut() {
            cell.value.parents.clear();
        }
        let loss = Loss {
            row: original,
            generation: state.generation(),
            frozen: true,
            values: LossValues::Row(cells),
            cause: LossCause::Excluded {
                write: migration,
                version,
            },
        };
        crate::loss_record::put(db, &loss, None)?;
        crate::file_protection::retain_loss(db, schema, &loss)
    }
}

/// Deleted circles whose deleting entry is final (§9).
fn final_deletions(
    db: &DatabaseConnection,
    deleted: &BTreeMap<CircleId, crate::EntryId>,
) -> Result<BTreeMap<CircleId, crate::EntryId>, DbError> {
    let mut kept = BTreeMap::new();
    for (circle, entry) in deleted {
        let final_entry: bool = db.query_row(
            &format!(
                "SELECT EXISTS(SELECT 1 FROM _coven_store_log l WHERE l.device=?1 AND l.number=?2 AND {FINAL})"
            ),
            rusqlite::params![
                entry.device.0.to_be_bytes().as_slice(),
                entry.number.to_be_bytes().as_slice()
            ],
            |r| r.get(0),
        )?;
        if final_entry {
            kept.insert(*circle, *entry);
        }
    }
    Ok(kept)
}

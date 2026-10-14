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
}

impl Hidden {
    /// Read every hidden row before identities move. A hidden row of a dropped
    /// table has no generation left to delete: its losses are frozen as they
    /// are, with their causes, before the table's merge state goes.
    pub(crate) fn before(
        db: &DatabaseConnection,
        before: &crate::schema::Schema,
        names: &MigrationMatch,
    ) -> Result<Self, DbError> {
        let store = MergeStore::from_schema(db, before);
        let mut rows = BTreeMap::new();
        for row in db.query(
            "SELECT DISTINCT table_name,key,audience FROM _coven_lost WHERE retired=0 AND replacement_kind='rules'",
            [],
            crate::row_queries::read_identity,
        )? {
            let Some(table) = names.tables.get(&row.table) else {
                crate::migration_state::freeze_losses(db, &row)?;
                continue;
            };
            let state = store.row_without_losses(&row)?.state;
            let new = RowId {
                table: table.clone(),
                ..row.clone()
            };
            rows.insert(new, (row, state));
        }
        Ok(Self { rows })
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
            retired: true,
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

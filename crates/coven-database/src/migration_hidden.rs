//! Rows a breaking migration deletes through the ordinary generation path
//! (§18.7), including rows hidden by irreversible circle deletions. Each becomes a delete at its generation in the migration write,
//! with one frozen whole-row loss keeping its written values, old names and
//! setters.
use crate::merge_store::MergeStore;
use crate::migration_names::MigrationMatch;
use crate::sqlite::DatabaseConnection;
use crate::write_schema::WriteSchema;
use crate::DbError;
use coven_format::loss::{Loss, LossCause, LossValues};
use coven_format::value::Value;
use coven_format::write::RowChange;
use coven_merge::{Change, Operation, RowId, RowState};
use std::collections::{BTreeMap, BTreeSet};

/// The rows removal rules hide before the migration, under their old names,
/// for the migration run on this connection.
pub(crate) struct Hidden<'c> {
    database: &'c DatabaseConnection,
    /// Merge state under the old identity and column names, by new identity.
    rows: BTreeMap<RowId, (RowId, RowState<Value>)>,
}

impl<'c> Hidden<'c> {
    /// Read every hidden row before identities move. A hidden row of a dropped
    /// table has no generation left to delete; its losses are frozen with the
    /// table's (`migration_state::carry`).
    pub(crate) fn before(
        db: &'c DatabaseConnection,
        before: &crate::schema::Schema,
        names: &MigrationMatch,
    ) -> Result<Self, DbError> {
        let store = MergeStore::from_schema(db, before);
        let mut rows = BTreeMap::new();
        for row in db.query(
            "SELECT table_name,key,audience FROM _coven_lost WHERE frozen=0 AND replacement_kind='rules'",
            [],
            crate::row_queries::read_identity,
        )? {
            let Some(table) = names.tables.get(&row.table) else {
                continue;
            };
            let state = store.row_without_losses(&row)?.state;
            let new = RowId {
                table: table.clone(),
                ..row.clone()
            };
            rows.insert(new, (row, state));
        }
        Ok(Self { database: db, rows })
    }

    /// Rows hidden by the counted removal rules before migration.
    pub(crate) fn select(&self) -> BTreeSet<RowId> {
        self.rows.keys().cloned().collect()
    }

    /// The migration write's delete of one selected row at its generation,
    /// naming the generation and every setter it replaces.
    pub(crate) fn delete(
        &self,
        store: &MergeStore<'_>,
        row: &RowId,
    ) -> Result<Option<RowChange>, DbError> {
        let state = store.row(row)?.state;
        let generation = state.generation();
        if !state.present() {
            return Ok(None);
        }
        let old = state
            .cells()
            .iter()
            .map(|(column, cell)| (column.clone(), cell.value.value.clone()))
            .collect();
        crate::write_sources::authored(
            self.database,
            store,
            row.clone(),
            Change { generation, operation: Operation::Delete },
            old,
        )
        .map(Some)
    }

    /// One frozen whole-row loss per deleted row, caused by this breaking
    /// version and naming the migration write: the written values under
    /// their old names, with their original setters and no live parent links.
    pub(crate) fn freeze(
        &self,
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
        let loss = Loss {
            row: original,
            generation: state.generation(),
            frozen: false,
            values: LossValues::Row(state.cells().clone()),
            cause: LossCause::Excluded { write: migration, version },
        }
        .freeze();
        let db = self.database;
        crate::migration_run::keep_loss(db, migration, &loss)?;
        crate::loss_record::keep(db, schema, &loss)
    }
}

#[cfg(test)]
#[path = "migration_hidden_tests.rs"]
mod tests;

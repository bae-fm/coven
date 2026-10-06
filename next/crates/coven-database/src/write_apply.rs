//! The shared merge, removal, and materialization transaction body.
use crate::merge_store::MergeStore;
use crate::removal_view::DatabaseRemovalView;
use crate::sqlite::DatabaseConnection;
use crate::write_rows::AppView;
use crate::write_schema::WriteSchema;
use crate::DbError;
use coven_format::write::WriteRecord;
use coven_foundation::id_source::CircleId;
use coven_merge::RowId;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) struct WriteApply<'a> {
    database: &'a DatabaseConnection,
    schema: &'a WriteSchema,
    store: &'a MergeStore<'a>,
    before: &'a AppView<'a>,
    visible: &'a AppView<'a>,
    deleted: &'a BTreeSet<CircleId>,
}

impl<'a> WriteApply<'a> {
    pub(crate) fn new(
        database: &'a DatabaseConnection,
        schema: &'a WriteSchema,
        store: &'a MergeStore<'a>,
        before: &'a AppView<'a>,
        visible: &'a AppView<'a>,
        deleted: &'a BTreeSet<CircleId>,
    ) -> Self {
        Self {
            database,
            schema,
            store,
            before,
            visible,
            deleted,
        }
    }

    pub(crate) fn apply(
        &self,
        record: Option<&WriteRecord>,
        mut touched: BTreeSet<RowId>,
    ) -> Result<BTreeSet<crate::write_rows::AppKey>, DbError> {
        let updates = match record {
            Some(record) => self.store.apply(record)?,
            None => BTreeMap::new(),
        };
        touched.extend(updates.keys().cloned());
        let empty = BTreeMap::new();
        let old = DatabaseRemovalView::new(
            self.database,
            self.store,
            self.schema,
            self.before,
            &empty,
            self.deleted,
            None,
        )?;
        let new = DatabaseRemovalView::new(
            self.database,
            self.store,
            self.schema,
            self.visible,
            &updates,
            self.deleted,
            record.map(|r| (r.header.position, r.header.timestamp)),
        )?;
        old.prime(touched.iter().cloned())?;
        new.prime(touched.iter().cloned())?;
        let write = record.map(|record| record.header.position);
        let removal = coven_merge::recompute(&old, &new, touched.clone())
            .map_err(|error| error.into_db_error(write))?;
        let fingerprint = coven_merge::recompute_fingerprint(&old, &new, touched)
            .map_err(|error| error.into_db_error(write))?;
        if let Some(record) = record {
            crate::write_commit::commit(self.database, record, self.store, &updates)?;
        }
        self.database.batch("PRAGMA defer_foreign_keys=ON")?;
        crate::removal::materialize(self.database, self.schema, self.visible, &new, &removal)?;
        crate::fingerprint::update(self.database, &new, &fingerprint)?;
        Ok(removal
            .region
            .into_iter()
            .map(|row| (row.table, row.key))
            .collect())
    }
}

#[cfg(test)]
#[path = "write_apply_tests.rs"]
mod tests;

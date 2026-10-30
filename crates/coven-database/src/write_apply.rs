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
    deleted: &'a BTreeMap<CircleId, crate::Slot>,
}

impl<'a> WriteApply<'a> {
    pub(crate) fn new(
        database: &'a DatabaseConnection,
        schema: &'a WriteSchema,
        store: &'a MergeStore<'a>,
        before: &'a AppView<'a>,
        visible: &'a AppView<'a>,
        deleted: &'a BTreeMap<CircleId, crate::Slot>,
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
        if let Some(record) = record {
            crate::published::capture_rows(
                self.database,
                self.store,
                record
                    .parts
                    .iter()
                    .flat_map(|p| p.dismissals.iter().map(|d| &d.row)),
            )?;
        }
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
        let affected = self.finish(
            &old,
            &new,
            touched,
            record.map(|record| record.header.position),
            || match record {
                Some(record) => {
                    crate::write_commit::commit(self.database, record, self.store, &updates)
                }
                None => Ok(BTreeMap::new()),
            },
        )?;
        let mut affected = affected;
        if let Some(record) = record {
            affected.extend(crate::dismissal::apply(self.database, record)?);
        }
        Ok(affected)
    }

    pub(crate) fn replace(
        &self,
        old: &DatabaseRemovalView<'_>,
        touched: BTreeSet<RowId>,
    ) -> Result<BTreeSet<crate::write_rows::AppKey>, DbError> {
        let updates = BTreeMap::new();
        let new = DatabaseRemovalView::new(
            self.database,
            self.store,
            self.schema,
            self.visible,
            &updates,
            self.deleted,
            None,
        )?;
        self.finish(old, &new, touched, None, || Ok(BTreeMap::new()))
    }

    fn finish(
        &self,
        old: &DatabaseRemovalView<'_>,
        new: &DatabaseRemovalView<'_>,
        touched: BTreeSet<RowId>,
        write: Option<coven_merge::WriteId>,
        persist: impl FnOnce() -> Result<BTreeMap<RowId, i64>, DbError>,
    ) -> Result<BTreeSet<crate::write_rows::AppKey>, DbError> {
        old.prime(touched.iter().cloned())?;
        new.prime(touched.iter().cloned())?;
        let removal = coven_merge::recompute(old, new, touched.clone())
            .map_err(|error| error.into_db_error(write))?;
        self.store.retain_materialization_values(&removal.region)?;
        if write.is_some() {
            crate::published::capture_rows(self.database, self.store, &removal.region)?;
        }
        let stored = persist()?;
        crate::removal::materialize(
            self.database,
            self.schema,
            self.visible,
            new,
            &removal,
            &stored,
        )?;
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

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
        let affected = self.finish(
            &old,
            &new,
            touched,
            record.map(|record| record.header.position),
            || {
                if let Some(record) = record {
                    crate::write_commit::commit(self.database, record, self.store, &updates)?;
                }
                Ok(())
            },
        )?;
        if let Some(record) = record {
            let dismissed = crate::dismissal::apply(self.database, record)?;
            if !dismissed.is_empty() {
                // Read the committed merge effects and the acknowledged losses
                // inside this same transaction, without cached pre-dismissal rows.
                let visible = AppView::after(self.database, self.schema);
                let store = MergeStore::new(self.database, &visible);
                let view = DatabaseRemovalView::new(
                    self.database,
                    &store,
                    self.schema,
                    &visible,
                    &empty,
                    self.deleted,
                    None,
                )?;
                let result = coven_merge::recompute_fingerprint(&view, &view, dismissed)
                    .map_err(|error| error.into_db_error(Some(record.header.position)))?;
                crate::fingerprint::update(self.database, &view, &result)?;
            }
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
        self.finish(old, &new, touched, None, || Ok(()))
    }

    fn finish(
        &self,
        old: &DatabaseRemovalView<'_>,
        new: &DatabaseRemovalView<'_>,
        touched: BTreeSet<RowId>,
        write: Option<coven_merge::WriteId>,
        persist: impl FnOnce() -> Result<(), DbError>,
    ) -> Result<BTreeSet<crate::write_rows::AppKey>, DbError> {
        old.prime(touched.iter().cloned())?;
        new.prime(touched.iter().cloned())?;
        let removal = coven_merge::recompute(old, new, touched.clone())
            .map_err(|error| error.into_db_error(write))?;
        let fingerprint = coven_merge::recompute_fingerprint(old, new, touched)
            .map_err(|error| error.into_db_error(write))?;
        persist()?;
        self.database.batch("PRAGMA defer_foreign_keys=ON")?;
        crate::removal::materialize(self.database, self.schema, self.visible, new, &removal)?;
        crate::fingerprint::update(self.database, new, &fingerprint)?;
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

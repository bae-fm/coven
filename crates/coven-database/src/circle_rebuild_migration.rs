//! A rebuild's migration, reading forward and export (§18.3): the steps that
//! turn the circle loaded at its version into the circle at the app's.

use super::{removed_after, RebuildDatabase, RebuildError};
use crate::database::{Database, DatabaseBuilder};
use crate::{DbError, SnapshotStaging};
use coven_foundation::id_source::KeyId;

/// The staging a rebuild's circle is exported into stopped taking frames.
#[derive(Debug, thiserror::Error)]
#[error("the snapshot's staging stopped taking frames")]
pub struct ExportStopped;

impl coven_foundation::task::CarriesBug for ExportStopped {
    fn bug(&self) -> Option<&coven_foundation::task::Bug> {
        None
    }
}

impl RebuildDatabase {
    /// Migrate the rebuild's store, which holds only the store, up to
    /// `version`, the version of the circle about to load, with no migration
    /// write: the store's changes come only from the store, so the rows are
    /// the ones every device that raised the store to `version` made, and
    /// the device's own marker belongs to the migration past it (§18.3). The
    /// store is closed and opened again with the app's migrations before the
    /// first breaking one past `version`.
    ///
    /// A failure removes the rebuild's folder, as `open_rebuild` does.
    pub async fn migrate_store_to(
        &mut self,
        circle: coven_merge::Audience,
        version: u32,
    ) -> Result<(), RebuildError> {
        let Some(database) = self.database.take() else {
            return Err(DbError::StoreClosed.into());
        };
        let result = match database.close().await {
            Err(error) => Err(RebuildError::from(error)),
            Ok(()) => match super::first_past(&self.inputs, &circle, version) {
                Err(error) => Err(error),
                Ok(first) => {
                    super::at_version(&self.inputs, first, &self.scratch, &self.clock, &self.ids)
                        .loading_store()
                        .open()
                        .await
                        .map_err(RebuildError::from)
                }
            },
        };
        self.parent.local_bytes_changed();
        match result {
            Ok(database) => {
                self.database = Some(database);
                Ok(())
            }
            Err(operation) => Err(removed_after(&self.real, operation)),
        }
    }

    /// Run the app's migrations the rebuild's store hasn't, in one run, the
    /// breaking ones making the migration write `header` and not a new one:
    /// the device's migration write for this rebuild, which read the circle
    /// through the slot the store has applied (§18.3). The store is closed
    /// and opened again at the app's newest version; the per-audience check
    /// of the run's reads (§18.3) runs here as on any breaking open.
    ///
    /// A failure removes the rebuild's folder, as `open_rebuild` does; the
    /// caller's `discard` after it finds nothing left.
    pub async fn migrate(
        &mut self,
        header: coven_format::write::WriteHeader,
    ) -> Result<(), RebuildError> {
        let Some(database) = self.database.take() else {
            return Err(DbError::StoreClosed.into());
        };
        let closed = database.close().await;
        let result = match closed {
            Err(error) => Err(RebuildError::from(error)),
            Ok(()) => DatabaseBuilder::new(self.scratch.clone())
                .synced_tables(self.inputs.tables.clone())
                .migrations(self.inputs.migrations.clone())
                .namespaces(self.inputs.namespaces.clone())
                .sqlite_settings(self.inputs.sqlite.clone())
                .sql_functions(self.inputs.functions.clone())
                .clock(self.clock.clone())
                .id_source(self.ids.clone())
                .rebuilding(header)
                .open()
                .await
                .map_err(RebuildError::from),
        };
        self.parent.local_bytes_changed();
        match result {
            Ok(database) => {
                self.database = Some(database);
                Ok(())
            }
            Err(operation) => Err(removed_after(&self.real, operation)),
        }
    }

    /// Stage the circle's counted state at the slot the rebuild's store has
    /// applied into `into` as the snapshot `id`, sealed under `key`, then
    /// remove the rebuild (§18.3). The rows are the circle's, migrated, as
    /// a device that held the circle and migrated would write them; the
    /// raise captures the rebuild's store made are not consulted, since it
    /// sends nothing. Returns the staging, finished, for the caller to load;
    /// on a failure the rebuild is removed, nothing stays staged in `into`
    /// and the failure is returned.
    pub async fn export(
        mut self,
        id: coven_format::admin::SnapshotId,
        key: KeyId,
        into: &Database,
    ) -> Result<SnapshotStaging, RebuildError> {
        let exported = self.stage_export(id, key, into).await;
        let removed = self.discard_inner().await;
        match (exported, removed) {
            (Ok(staging), Ok(())) => Ok(staging),
            (Ok(staging), Err(error)) => Err(refused(staging, error).await),
            (Err(error), Ok(())) => Err(error),
            (Err(error), Err(cleanup)) => Err(RebuildError::Cleanup {
                operation: Box::new(error),
                cleanup: Box::new(cleanup),
            }),
        }
    }

    async fn stage_export(
        &self,
        id: coven_format::admin::SnapshotId,
        key: KeyId,
        into: &Database,
    ) -> Result<SnapshotStaging, RebuildError> {
        let Some(database) = &self.database else {
            return Err(DbError::StoreClosed.into());
        };
        let audience = id.audience.clone();
        let (prefixes, prefix) = tokio::sync::oneshot::channel();
        let (frames, mut received) = tokio::sync::mpsc::channel::<Vec<u8>>(8);
        let writing = database.write_counted_snapshot(
            id.clone(),
            move |header| {
                let prefix = coven_format::sealed_snapshot::SnapshotObjectPrefix {
                    audience,
                    key,
                    covers: header.covers,
                    purpose: coven_format::sequence::Purpose::Ordinary,
                };
                prefixes.send(prefix).ok().ok_or(ExportStopped)
            },
            move |frame| frames.blocking_send(frame).ok().ok_or(ExportStopped),
        );
        let staging = async {
            let Ok(prefix) = prefix.await else {
                return Ok(None);
            };
            let mut staging = into.stage_snapshot(id, prefix).await?;
            let mut batch = Vec::new();
            let fed = async {
                while let Some(frame) = received.recv().await {
                    batch.extend(frame);
                    if batch.len() >= coven_format::chunks::CHUNK_SIZE {
                        staging.stage(std::mem::take(&mut batch)).await?;
                    }
                }
                if !batch.is_empty() {
                    staging.stage(batch).await?;
                }
                staging.finish().await
            }
            .await;
            match fed {
                Ok(()) => Ok(Some(staging)),
                Err(error) => {
                    Err(crate::database::input_database::staging::refuse(vec![staging], error)
                        .await)
                }
            }
        };
        let (written, staged) = tokio::join!(writing, staging);
        match (written, staged) {
            (Ok(_), Ok(Some(staging))) => Ok(staging),
            (Ok(_), Ok(None)) => Err(DbError::Invariant {
                what: "a snapshot's writer names its prefix before its first frame",
            }
            .into()),
            (Ok(_), Err(error)) => Err(error.into()),
            (Err(error), Ok(staging)) => {
                let error = RebuildError::from(error);
                match staging {
                    Some(staging) => Err(refused(staging, error).await),
                    None => Err(error),
                }
            }
            (Err(error), Err(_)) => Err(error.into()),
        }
    }
}

/// `error` after discarding `staging`.
async fn refused(staging: SnapshotStaging, error: RebuildError) -> RebuildError {
    match staging.discard().await {
        Ok(()) => error,
        Err(cleanup) => RebuildError::Cleanup {
            operation: Box::new(error),
            cleanup: Box::new(RebuildError::Database(cleanup)),
        },
    }
}

#[cfg(test)]
#[path = "circle_rebuild_migration_tests.rs"]
mod tests;

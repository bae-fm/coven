//! The database a rebuild of a circle runs in: a coven store of its own, in
//! the folder `rebuild` of the store's folder, at the version the circle is
//! at, holding this device's copy of the store and the circle as they were
//! (§18.3). The real store is changed only by installing what the rebuild
//! makes, never by running it.

use super::builder::OpenInputs;
use super::{Database, DatabaseBuilder};
use crate::migration::MigrationKind;
use crate::{DbError, OpenError, SnapshotWriteError};
use coven_foundation::clock::ClockRef;
use coven_foundation::files::{FileError, SettingsError, StoreDir, StoreDirAccess};
use coven_foundation::id_source::IdSourceRef;
use coven_merge::Audience;

/// Why a rebuild could not run.
#[derive(Debug, thiserror::Error)]
pub enum RebuildError {
    /// The app has no breaking migration past the circle's version: nothing
    /// to rebuild it for.
    #[error("{circle} at version {version} is not below a breaking migration")]
    NotBelow {
        /// The circle.
        circle: Audience,
        /// Its version.
        version: u32,
    },
    /// The rebuild's store could not open at the circle's version or migrate.
    #[error(transparent)]
    Open(#[from] OpenError),
    /// The circle's state could not be read out of the rebuild's store.
    #[error(transparent)]
    Export(#[from] SnapshotWriteError<ExportStopped>),
    /// The rebuild's store failed.
    #[error(transparent)]
    Database(#[from] DbError),
    /// The rebuild's folder could not be made.
    #[error("the rebuild's store: {0}")]
    Directory(#[from] SettingsError),
    /// The rebuild's folder could not be removed.
    #[error("the rebuild's folder: {0}")]
    Folder(#[from] FileError),
    /// A rebuild failed, and so did removing what it left.
    #[error("{operation}; removing what it left also failed: {cleanup}")]
    Cleanup {
        /// The failure that ended the rebuild.
        operation: Box<RebuildError>,
        /// The failure of the removal that followed.
        cleanup: Box<RebuildError>,
    },
}

/// A rebuild's store, open at the version its circle is at. Its owner is the
/// only one that writes to it or closes it; `discard` closes it and removes
/// its folder.
pub struct RebuildDatabase {
    /// None once `discard` took it to close.
    database: Option<Database>,
    /// The real store, whose folder holds the rebuild's.
    real: StoreDir,
    /// The rebuild's own store, which `migrate` opens again.
    scratch: StoreDir,
    /// What the real store opened with.
    inputs: OpenInputs,
    clock: ClockRef,
    ids: IdSourceRef,
    /// The real store's handle, told when the folder's bytes change: they
    /// are on this device's disk, so its storage figures count them (§17.8).
    parent: Database,
}

impl Database {
    /// Open a database of its own for rebuilding `circle`, which is at
    /// breaking version `version`: the app's migrations through the version
    /// before the first breaking one past `version`, declared as that
    /// migration's `starts_from` (§18.3). A circle with no breaking
    /// migration past its version is `NotBelow`. A leftover rebuild from a
    /// crash is replaced; on a failure nothing is left behind.
    pub async fn open_rebuild(
        &self,
        circle: Audience,
        version: u32,
    ) -> Result<RebuildDatabase, RebuildError> {
        let (real, inputs, clock, ids) = self
            .call(|inner| {
                Ok::<_, DbError>((
                    inner.access.directory.clone(),
                    inner.open_inputs.clone(),
                    inner.clock.clone(),
                    inner.ids.clone(),
                ))
            })
            .await?;
        let Some(first) = inputs
            .migrations
            .iter()
            .position(|m| m.version > version && matches!(m.kind, MigrationKind::Breaking(_)))
        else {
            return Err(RebuildError::NotBelow { circle, version });
        };
        let scratch = real.rebuild_directory().await?;
        let opened = DatabaseBuilder::new(scratch.clone())
            .synced_tables(inputs.migrations[first].starts_from.clone())
            .migrations(inputs.migrations[..first].to_vec())
            .namespaces(inputs.namespaces.clone())
            .sqlite_settings(inputs.sqlite.clone())
            .sql_functions(inputs.functions.clone())
            .clock(clock.clone())
            .id_source(ids.clone())
            .loading_store()
            .open()
            .await;
        match opened {
            Ok(database) => {
                self.local_bytes_changed();
                Ok(RebuildDatabase {
                    database: Some(database),
                    real,
                    scratch,
                    inputs,
                    clock,
                    ids,
                    parent: self.clone(),
                })
            }
            Err(operation) => Err(removed_after(&real, operation.into())),
        }
    }
}

impl RebuildDatabase {
    /// Begin staging a snapshot into the rebuild's store (§16.2).
    pub async fn stage_snapshot(
        &self,
        id: coven_format::admin::SnapshotId,
        prefix: coven_format::sealed_snapshot::SnapshotObjectPrefix,
    ) -> Result<crate::SnapshotStaging, DbError> {
        let Some(database) = &self.database else {
            return Err(DbError::StoreClosed);
        };
        database.stage_snapshot(id, prefix).await
    }

    /// Load snapshots staged here and the slots after them into the
    /// rebuild's store, as a device loads them (§16.2).
    ///
    /// The store's parts in slots after `store_through` aren't read: past the
    /// slot before the store's raise beyond the circle's version, the store
    /// holds no rows at the circle's version. A write above the rebuild's
    /// version is refused, as every device refuses a part ahead of its
    /// audience (§18.3, §18.5).
    pub async fn load<P: crate::PendingValue, W>(
        &self,
        reload: crate::SnapshotReload<crate::SnapshotStaging, crate::DownloadedWriteStream<W>>,
        store_through: u64,
    ) -> Result<Vec<crate::LoadedSnapshot>, DbError>
    where
        W: std::io::Read + Send + 'static,
    {
        let Some(database) = &self.database else {
            return Err(DbError::StoreClosed);
        };
        let loaded = database.load_through::<P, W>(reload, None, Some(store_through), None).await;
        self.parent.local_bytes_changed();
        loaded
    }

    /// Close the rebuild's store and remove its folder, reporting the first
    /// failure (both, as `Cleanup`, when both fail).
    pub async fn discard(mut self) -> Result<(), RebuildError> {
        self.discard_inner().await
    }

    async fn discard_inner(&mut self) -> Result<(), RebuildError> {
        let closed = match self.database.take() {
            Some(database) => database.close().await.map_err(RebuildError::from),
            None => Ok(()),
        };
        let removed = self.real.remove_rebuild_directory();
        self.parent.local_bytes_changed();
        match (closed, removed) {
            (Ok(()), Ok(())) => Ok(()),
            (Ok(()), Err(cleanup)) => Err(cleanup.into()),
            (Err(operation), Ok(())) => Err(operation),
            (Err(operation), Err(cleanup)) => Err(RebuildError::Cleanup {
                operation: Box::new(operation),
                cleanup: Box::new(cleanup.into()),
            }),
        }
    }
}

/// `operation` after removing the rebuild's folder.
fn removed_after(real: &StoreDir, operation: RebuildError) -> RebuildError {
    match real.remove_rebuild_directory() {
        Ok(()) => operation,
        Err(cleanup) => RebuildError::Cleanup {
            operation: Box::new(operation),
            cleanup: Box::new(cleanup.into()),
        },
    }
}

#[path = "circle_rebuild_migration.rs"]
mod migration;
pub use migration::ExportStopped;

#[cfg(test)]
#[path = "circle_rebuild_tests.rs"]
pub(crate) mod tests;

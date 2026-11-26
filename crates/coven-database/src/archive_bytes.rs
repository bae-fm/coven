//! The archives that resets kept in the store's directory, with their
//! sizes, in `coven_archive_bytes` (§17.8, §19.3): coven writes each
//! archive's row as the store opens after the swap that kept it, and
//! deletes the row in the step that moves the archive out of the store's
//! directory to delete it.

use crate::database::{process, StoreDatabase};
use crate::sqlite::StoreSql;
use crate::DbError;
use coven_foundation::files::{StoreDir, StoreDirAccess};

/// Record each archive in `directory` that has no row yet, with its size,
/// as the store opens, failing the handle if the write fails. A reset's
/// swap moves the old installation into its archive while no database is
/// open and the store refuses opens. The opening of the swapped store comes
/// next and records the archive. When the process stops between the two,
/// the next reset's opening of the swapped store records it, so no reader
/// sees an archive without its row. Nothing writes into an archive once a
/// reset keeps it, so its size stays right.
pub(crate) fn record_opened(writer: &mut StoreSql, directory: &StoreDir) -> Result<(), DbError> {
    let recorded =
        writer.query("SELECT archive FROM coven_archive_bytes", [], |r| r.get::<_, String>(0))?;
    let mut kept = Vec::new();
    for name in directory.discarded_devices()? {
        if !recorded.contains(&name) {
            let bytes = directory.discarded_device_size(&name)?;
            kept.push((name, crate::sqlite::integer("archive size", bytes)?));
        }
    }
    if kept.is_empty() {
        return Ok(());
    }
    crate::pending::state_transaction(writer, |db| {
        for (name, bytes) in &kept {
            db.execute(
                "INSERT INTO coven_archive_bytes(archive,bytes) VALUES(?1,?2)",
                (name, bytes),
            )?;
        }
        Ok(())
    })
}

/// Delete the archive `name`'s row, in the transaction under way.
fn forget_archive(db: &StoreSql, name: &str) -> Result<(), DbError> {
    db.execute("DELETE FROM coven_archive_bytes WHERE archive=?1", [name])?;
    Ok(())
}

/// Move the archive `name` out of `directory` and delete its row, in one
/// step, on `writer`, the store's writer, which the caller holds.
fn retire_archive(writer: &mut StoreSql, directory: &StoreDir, name: &str) -> Result<(), DbError> {
    crate::database::library_bytes::uncount(
        writer,
        || directory.retire_discarded_device(name),
        || directory.return_discarded_device(name),
        |db| forget_archive(db, name),
    )
}

impl StoreDatabase {
    /// Delete every archive that resets kept in the store's directory
    /// (§19.3). For each archive, one step moves it into the removal folder
    /// and deletes its row; then the method deletes the removal folder on
    /// the blocking pool. On a failure the method returns the error. Each
    /// archive is then either still in the store's directory with its row,
    /// or in the removal folder, which the next call deletes.
    pub async fn delete_discarded_devices(&self) -> Result<(), DbError> {
        let directory = self
            .call(|inner| {
                let directory = &inner.access.directory;
                inner.access.with_writer(|writer, _| {
                    let names = writer.query(
                        "SELECT archive FROM coven_archive_bytes ORDER BY archive",
                        [],
                        |r| r.get::<_, String>(0),
                    )?;
                    for name in names {
                        retire_archive(writer, directory, &name)?;
                    }
                    Ok::<_, DbError>(())
                })?;
                Ok::<_, DbError>(directory.clone())
            })
            .await?;
        process(&self.bugs, move || Ok(directory.remove_retired_devices()?)).await
    }

    /// Delete the archive `name` the way `delete_discarded_devices` deletes
    /// each archive. A restore calls it to delete the archive of the
    /// installation it replaced (§13.1).
    pub async fn remove_discarded_device(&self, name: String) -> Result<(), DbError> {
        let directory = self
            .call(move |inner| {
                let directory = &inner.access.directory;
                inner.access.with_writer(|writer, _| retire_archive(writer, directory, &name))?;
                Ok::<_, DbError>(directory.clone())
            })
            .await?;
        process(&self.bugs, move || Ok(directory.remove_retired_devices()?)).await
    }
}

//! The archives resets kept in the store's directory, with their sizes, in
//! `coven_archive_bytes` (§17.8, §19.3): each archive's row is written as
//! the store opens after the swap that kept it, and deleted in the step
//! that moves the archive out of the store's directory to remove it.

use crate::database::{process, StoreDatabase};
use crate::sqlite::StoreSql;
use crate::DbError;
use coven_foundation::files::{StoreDir, StoreDirAccess};

/// Record each archive in `directory` that has no row yet, with its size,
/// as the store opens, failing the handle if the write fails. A reset's
/// swap moves the old installation into its archive while no database is
/// open and the store refuses opens, and the opening of the swapped store
/// follows it, or the next reset's after a stop, so no reader sees an
/// archive without its row. Nothing writes into an archive once it is
/// kept, so its size stays right.
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

/// The archives' sizes together, while there is one.
pub(crate) fn kept(db: &StoreSql) -> Result<Option<u64>, DbError> {
    let (archives, bytes) =
        db.query_row("SELECT count(*),coalesce(sum(bytes),0) FROM coven_archive_bytes", [], |r| {
            Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?))
        })?;
    if archives == 0 {
        return Ok(None);
    }
    Ok(Some(u64::try_from(bytes).map_err(|_negative| DbError::DamagedDatabase)?))
}

/// Move the archive `name` out of `directory` and delete its row, on
/// `writer`, the store's writer, which the caller holds. When the row's
/// deletion fails, the archive moves back and the failure returns, so the
/// archive and its row stay as they were; when the move back fails too,
/// both failures return. An archive already moved deletes only its row.
fn retire_archive(writer: &mut StoreSql, directory: &StoreDir, name: &str) -> Result<(), DbError> {
    let moved = directory.retire_discarded_device(name)?;
    let deleted = writer.transaction(|db| {
        db.execute("DELETE FROM coven_archive_bytes WHERE archive=?1", [name])?;
        Ok::<_, DbError>(())
    });
    match deleted {
        Ok(()) => Ok(()),
        Err(error) if !moved => Err(error),
        Err(error) => match directory.return_discarded_device(name) {
            Ok(()) => Err(error),
            Err(returning) => Err(DbError::FileCleanup {
                write: Err(Box::new(error)),
                failures: vec![DbError::Disk(returning)],
            }),
        },
    }
}

impl StoreDatabase {
    /// Remove every archive resets kept in the store's directory (§19.3):
    /// each leaves the directory with its row in one step, then their
    /// folder goes, on the blocking pool. A failure returns; what it left
    /// is either still an archive with its row, or in the folder being
    /// removed, which the next call removes.
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

    /// Remove the archive `name` the way `delete_discarded_devices` removes
    /// each: a restore's of the installation it replaced (§13.1).
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

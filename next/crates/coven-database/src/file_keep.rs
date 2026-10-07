//! Download reservations and the conditional file-location write (§16, §18).

use super::{finish_blocking, read_only, FileDatabase, FileDatabaseAccess};
use crate::{
    file_ref, file_row, DbError, FileLocation, FileRef, NewOperation, OperationId, OperationUpdate,
    PreparedUserFile, Provenance,
};
use coven_foundation::files::{DownloadLocation, FileName};
use std::path::PathBuf;

impl FileDatabase {
    /// Read the declared provenance only after validating the captured reference.
    pub async fn provenance(&self, file: &FileRef) -> Result<Provenance, DbError> {
        let file = file.clone();
        self.run(move |db, schema, _, _| {
            db.transaction(|db| {
                file_ref::validate(db, schema, &file)?;
                Ok(file_row::declaration(schema, file.table())?
                    .1
                    .provenance
                    .clone())
            })
        })
        .await
    }

    /// Record all requested operations and their disk names in one transaction.
    /// User destinations have already been resolved and checked by foundation;
    /// exclusive publication will check them again after downloading.
    pub async fn start_keeps(
        &self,
        requests: Vec<(FileRef, Option<PathBuf>, NewOperation)>,
    ) -> Result<(), DbError> {
        let FileDatabaseAccess::Writer(database) = &self.access else {
            return Err(read_only());
        };
        let database = database.clone();
        finish_blocking(tokio::task::spawn_blocking(move || {
            let slot = database.inner.read().expect("database lock poisoned");
            let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
            let writer = inner.writer.lock().expect("writer lock poisoned");
            writer.transaction(|db| {
                for (file, destination, operation) in requests {
                    file_ref::validate(db, &inner.write_schema, &file)?;
                    if file.location() != FileLocation::Uploaded { return Err(DbError::DamagedDatabase); }
                    let (_, declaration) = file_row::declaration(&inner.write_schema, file.table())?;
                    if (declaration.provenance == Provenance::UserProvided) != destination.is_some() {
                        return Err(DbError::DamagedDatabase);
                    }
                    let name = FileName::new(inner.ids.new_id().to_string()).expect("UUID download name");
                    if db.query_row("SELECT EXISTS(SELECT 1 FROM coven_device_files WHERE path=?1 UNION ALL SELECT 1 FROM coven_file_uploads WHERE path=?1 UNION ALL SELECT 1 FROM coven_cache WHERE path=?1 UNION ALL SELECT 1 FROM coven_file_removals WHERE path=?1)", [name.as_str()], |r| r.get::<_, bool>(0))? {
                        return Err(DbError::FileNameReused { name });
                    }
                    let operation = crate::operation::insert(db, &operation)?;
                    let area = if destination.is_some() { "user" } else { "files" };
                    db.internal_execute("INSERT INTO coven_file_removals(path,area,destination,operation) VALUES(?1,?2,?3,?4)",
                        (name.as_str(), area, destination.as_deref().map(crate::user_file::encode_path), operation.0))?;
                }
                Ok(())
            })
        }).await)
    }

    /// The name still owned by an unfinished download operation.
    pub async fn keep_location(&self, operation: OperationId) -> Result<DownloadLocation, DbError> {
        self.run(move |db, _, _, _| {
            let (name, area, destination) = db.query_row(
                "SELECT path,area,destination FROM coven_file_removals WHERE operation=?1",
                [operation.0],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, Option<Vec<u8>>>(2)?,
                    ))
                },
            )?;
            let name = FileName::new(name).map_err(|_| DbError::DamagedDatabase)?;
            match (area.as_str(), destination) {
                ("files", None) => Ok(DownloadLocation::AppProvided(name)),
                ("user", Some(path)) => Ok(DownloadLocation::UserProvided {
                    path: crate::user_file::decode_path(path)?,
                    name,
                }),
                _ => Err(DbError::DamagedDatabase),
            }
        })
        .await
    }

    /// Keep close from overtaking a file step's outstanding blocking disk work.
    pub async fn keep_lease(&self) -> Result<tokio::sync::OwnedRwLockReadGuard<()>, DbError> {
        let FileDatabaseAccess::Writer(database) = &self.access else {
            return Err(read_only());
        };
        let lease = database.file_tasks.clone().read_owned().await;
        if database
            .inner
            .read()
            .expect("database lock poisoned")
            .is_none()
        {
            return Err(DbError::StoreClosed);
        }
        Ok(lease)
    }

    /// Commit the file-location write and completed step together. All four file
    /// columns use the ordinary write/merge path. No automatic upload is queued:
    /// keeping is an explicit change of location, including WhenAttached tables.
    pub async fn finish_keep(
        &self,
        file: &FileRef,
        prepared: Option<PreparedUserFile>,
        update: OperationUpdate,
    ) -> Result<(), DbError> {
        let FileDatabaseAccess::Writer(database) = &self.access else {
            return Err(read_only());
        };
        let database = database.clone();
        let file = file.clone();
        finish_blocking(tokio::task::spawn_blocking(move || {
            let slot = database.inner.read().expect("database lock poisoned");
            let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
            let writer = inner.writer.lock().expect("writer lock poisoned");
            let files = crate::file_write::FileWrite::new(&writer, &inner.directory, &inner.write_schema, inner.device, &inner.staging, Vec::new());
            let result = writer.local_write(&inner.write_schema, inner.device, inner.clock.now(), &files, |_| {
                file_ref::validate(&writer, &inner.write_schema, &file)?;
                let (name, area) = writer.query_row("SELECT path,area FROM coven_file_removals WHERE operation=?1", [update.id.0], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
                let name = FileName::new(name).map_err(|_| DbError::DamagedDatabase)?;
                files.keep_file(&file, &name, prepared)?;
                match area.as_str() {
                    "files" => { writer.internal_execute("DELETE FROM coven_file_removals WHERE operation=?1", [update.id.0])?; }
                    "user" => { writer.internal_execute("UPDATE coven_file_removals SET area='user-staging',operation=NULL WHERE operation=?1", [update.id.0])?; }
                    _ => return Err(DbError::DamagedDatabase),
                }
                crate::operation::advance(&writer, &update)
            });
            files.finish(result)
        }).await)
    }

    /// Make a stale or discarded download unused in the transaction recording
    /// that decision. A cleanup failure retains its removal record and reaches
    /// the operation; retrying never reattaches the abandoned copy.
    pub async fn abandon_keep(&self, update: OperationUpdate) -> Result<(), DbError> {
        self.require_writer()?;
        self.run(move |db, _, _, _| {
            db.transaction(|db| {
                crate::operation::advance(db, &update)?;
                db.internal_execute(
                    "UPDATE coven_file_removals SET operation=NULL WHERE operation=?1",
                    [update.id.0],
                )?;
                Ok::<_, DbError>(())
            })
        })
        .await?;
        self.clean_files().await
    }

    /// Record a completed download after its bytes and publication are durable.
    pub async fn advance_keep(&self, update: OperationUpdate) -> Result<(), DbError> {
        let FileDatabaseAccess::Writer(database) = &self.access else {
            return Err(read_only());
        };
        database.advance_operation(update).await
    }

    /// Delete a finished operation only after its recorded cleanup succeeds.
    pub async fn complete_keep(&self, id: OperationId) -> Result<(), DbError> {
        let FileDatabaseAccess::Writer(database) = &self.access else {
            return Err(read_only());
        };
        self.clean_files().await?;
        database.finish_operation(id).await
    }

    /// Finish recorded removals, preserving active staged writes and downloads.
    pub async fn clean_files(&self) -> Result<(), DbError> {
        let FileDatabaseAccess::Writer(database) = &self.access else {
            return Err(read_only());
        };
        let database = database.clone();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                let slot = database.inner.read().expect("database lock poisoned");
                let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                let writer = inner.writer.lock().expect("writer lock poisoned");
                let active = inner.staging.lock().expect("staging lock poisoned");
                crate::file_removals::FileRemovals::new(&writer, &inner.directory, &active)
                    .finish(Ok::<_, DbError>(()))
            })
            .await,
        )
    }
}

//! Pending deletion remains durable until both the bytes and their record are gone.

use std::collections::BTreeSet;

use crate::{sqlite::DatabaseConnection, DbError};
use coven_foundation::files::{FileArea, FileName, StoreDir};

pub(crate) struct FileRemovals<'a> {
    database: &'a DatabaseConnection,
    directory: &'a StoreDir,
    active: &'a BTreeSet<FileName>,
}

impl<'a> FileRemovals<'a> {
    pub(crate) fn new(
        database: &'a DatabaseConnection,
        directory: &'a StoreDir,
        active: &'a BTreeSet<FileName>,
    ) -> Self {
        Self {
            database,
            directory,
            active,
        }
    }

    pub(crate) fn finish<R, E: crate::WriteFailure>(&self, result: Result<R, E>) -> Result<R, E> {
        let failures = self.remove_unused();
        if failures.is_empty() {
            result
        } else {
            Err(E::with_cleanup(result.map(|_| ()), failures))
        }
    }

    pub(crate) fn remove_unused(&self) -> Vec<DbError> {
        // BEGIN refuses an outstanding transaction after a failed rollback.
        // Only committed pending records can authorize deleting any bytes.
        let mut failures = Vec::new();
        let result = self.database.transaction(|database| {
            let names = database.query(
                "SELECT path,area,destination,reference FROM _coven_file_removals WHERE operation IS NULL ORDER BY area,path",
                [],
                |r| Ok((r.get::<_, String>(0)?,r.get::<_, String>(1)?,r.get::<_, Option<Vec<u8>>>(2)?,r.get::<_, Option<Vec<u8>>>(3)?)),
            )?;
            for (name,area,destination,reference) in names {
                let result = (|| {
                    let name = FileName::new(name).map_err(|_| DbError::DamagedDatabase)?;
                    if self.active.contains(&name) {
                        return Ok(());
                    }
                    if database.query_row(
                        "SELECT EXISTS(SELECT 1 FROM _coven_device_files WHERE path=?1 UNION ALL SELECT 1 FROM _coven_cache WHERE path=?1)",
                        [name.as_str()],
                        |r| r.get::<_, bool>(0),
                    )? {
                        return Err(DbError::DamagedDatabase);
                    }
                    match area.as_str() {
                        "files" => self.directory.file(FileArea::AppProvided, &name).remove()?,
                        "cache" => self.directory.file(FileArea::Cache, &name).remove()?,
                        "user" => {
                            let location = coven_foundation::files::DownloadLocation::UserProvided {
                                path: crate::user_file::decode_path(destination.ok_or(DbError::DamagedDatabase)?)?,
                                name: name.clone(),
                            };
                            let download = self.directory.download(&location)?;
                            let file = crate::FileRef::decode(&reference.ok_or(DbError::DamagedDatabase)?)?;
                            download.remove_unused(|reader| file.matches_content(reader))?;
                        }
                        _ => return Err(DbError::DamagedDatabase),
                    }
                    database.internal_execute(
                        "DELETE FROM _coven_file_removals WHERE path=?1 AND area=?2",
                        (name.as_str(), &area),
                    )?;
                    Ok(())
                })();
                if let Err(error) = result {
                    failures.push(error);
                }
            }
            Ok(())
        });
        if let Err(error) = result {
            failures.push(error);
        }
        failures
    }
}

#[cfg(test)]
#[path = "file_removals_tests.rs"]
mod tests;

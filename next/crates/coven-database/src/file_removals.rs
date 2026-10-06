//! Pending deletion remains durable until both the bytes and their record are gone.

use crate::{sqlite::DatabaseConnection, DbError};
use coven_foundation::files::{FileArea, FileName, StoreDir};

pub(crate) struct FileRemovals<'a> {
    database: &'a DatabaseConnection,
    directory: &'a StoreDir,
}

impl<'a> FileRemovals<'a> {
    pub(crate) fn new(database: &'a DatabaseConnection, directory: &'a StoreDir) -> Self {
        Self {
            database,
            directory,
        }
    }

    pub(crate) fn finish<R>(&self, result: Result<R, DbError>) -> Result<R, DbError> {
        let failures = self.remove_unused();
        if failures.is_empty() {
            result
        } else {
            Err(DbError::FileCleanup {
                write: result.map(|_| ()).map_err(Box::new),
                failures,
            })
        }
    }

    pub(crate) fn remove_unused(&self) -> Vec<DbError> {
        // BEGIN refuses an outstanding transaction after a failed rollback.
        // Only committed pending records can authorize deleting any bytes.
        let mut failures = Vec::new();
        let result = self.database.transaction(|database| {
            let names = database.query(
                "SELECT path FROM coven_file_removals ORDER BY path",
                [],
                |r| r.get::<_, String>(0),
            )?;
            for name in names {
                let result = (|| {
                    let name = FileName::new(name).map_err(|_| DbError::DamagedDatabase)?;
                    if database.query_row(
                        "SELECT EXISTS(SELECT 1 FROM coven_device_files WHERE path=?1)",
                        [name.as_str()],
                        |r| r.get::<_, bool>(0),
                    )? {
                        return Err(DbError::DamagedDatabase);
                    }
                    self.directory.file(FileArea::AppProvided, &name).remove()?;
                    database.internal_execute(
                        "DELETE FROM coven_file_removals WHERE path=?1",
                        [name.as_str()],
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

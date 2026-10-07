//! Explicit database replacement. A durable marker excludes ordinary opens
//! until the caller has committed the snapshot reload.

use super::{atomic_file, lock, AtomicFile, FileError, FileName, StoreLock, StoreLockError};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

const FILES: [&str; 4] = [
    "store.db",
    "store.db-wal",
    "store.db-shm",
    "store.db-journal",
];
const MARKER: &str = "database-recovery.json";

#[derive(Serialize, Deserialize)]
struct Journal {
    backup: String,
    files: [bool; 4],
    phase: Phase,
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
enum Phase {
    Archiving,
    Building,
    Loading,
}

/// Owns an explicitly requested recovery. Dropping it keeps the damaged files
/// and the marker; only another explicit recovery may continue that state.
pub struct DatabaseRecovery {
    marker: AtomicFile,
    source: PathBuf,
    journal: Journal,
}

impl DatabaseRecovery {
    pub(super) fn begin(
        directory: &Path,
        id: crate::id_source::StoreId,
        writer: &StoreLock,
        name: &FileName,
    ) -> Result<Self, StoreLockError> {
        writer.verify(directory, id)?;
        let _readers = lock::exclude_readers(directory, id)?;
        let marker = AtomicFile::new(directory.join(MARKER));
        let mut journal = match marker.read_optional()? {
            Some(bytes) => serde_json::from_slice::<Journal>(&bytes)
                .map_err(|e| invalid(directory, e.to_string()))?,
            None => {
                let mut files = [false; 4];
                for (present, file) in files.iter_mut().zip(FILES) {
                    *present = AtomicFile::new(directory.join(file)).exists()?;
                }
                let backup = format!("damaged-database-{}", name.as_str());
                let target = directory.join(&backup);
                std::fs::create_dir(&target)
                    .map_err(|e| FileError::at("create damaged database archive", &target, e))?;
                atomic_file::sync_publication(&target)?;
                let journal = Journal {
                    backup,
                    files,
                    phase: Phase::Archiving,
                };
                marker.replace(&serde_json::to_vec(&journal).expect("recovery journal"))?;
                journal
            }
        };
        FileName::new(journal.backup.clone()).map_err(|e| invalid(directory, e.to_string()))?;
        if !journal.backup.starts_with("damaged-database-") {
            return Err(invalid(directory, "invalid recovery archive name".into()).into());
        }
        let backup = directory.join(&journal.backup);
        if journal.phase == Phase::Archiving {
            for (present, file) in journal.files.iter().zip(FILES) {
                if !present {
                    continue;
                }
                let from = directory.join(file);
                let to = backup.join(file);
                match (
                    AtomicFile::new(from.clone()).exists()?,
                    AtomicFile::new(to.clone()).exists()?,
                ) {
                    (true, false) => atomic_file::rename_new(&from, &to)
                        .map_err(|e| FileError::at("archive damaged database", &from, e))?,
                    (false, true) => {
                        tracing::debug!(file, "database file already archived by this recovery")
                    }
                    _ => {
                        return Err(invalid(
                            directory,
                            format!("recovery source and archive disagree for {file}"),
                        )
                        .into())
                    }
                }
                atomic_file::sync_publication(&from)?;
                atomic_file::sync_publication(&to)?;
            }
            journal.phase = Phase::Building;
            marker.replace(&serde_json::to_vec(&journal).expect("recovery journal"))?;
        }
        // Before the replacement schema and salvage have committed, no sync
        // operation may have started. An explicit retry can rebuild that state.
        if journal.phase == Phase::Building {
            for file in FILES {
                AtomicFile::new(directory.join(file)).remove()?;
            }
        } else if !AtomicFile::new(directory.join(FILES[0])).exists()? {
            return Err(invalid(directory, "prepared recovery database is missing".into()).into());
        }
        Ok(Self {
            marker,
            source: backup.join(FILES[0]),
            journal,
        })
    }

    pub(super) fn check(
        directory: &Path,
        id: crate::id_source::StoreId,
    ) -> Result<(), StoreLockError> {
        if AtomicFile::new(directory.join(MARKER)).exists()? {
            return Err(StoreLockError::RecoveryPending(id));
        }
        Ok(())
    }

    /// Whether the replacement still needs its initial schema and salvage.
    pub fn needs_salvage(&self) -> bool {
        self.journal.phase == Phase::Building
    }

    /// Record that the replacement schema and readable work have committed.
    /// An explicit retry now resumes this database and its operation journal.
    pub fn prepared(&mut self) -> Result<(), FileError> {
        self.journal.phase = Phase::Loading;
        self.marker
            .replace(&serde_json::to_vec(&self.journal).expect("recovery journal"))
    }

    /// Exact archived SQLite filename, including its adjacent WAL. Database
    /// code may open it read-only to salvage waiting writes.
    pub fn source_database_path(&self) -> &Path {
        &self.source
    }

    /// Publish a replacement only after the authenticated reload commits.
    pub fn finish(&self) -> Result<(), FileError> {
        self.marker.remove()
    }
}

fn invalid(directory: &Path, message: String) -> FileError {
    FileError::at(
        "read database recovery journal",
        &directory.join(MARKER),
        std::io::Error::new(std::io::ErrorKind::InvalidData, message),
    )
}

#[cfg(test)]
#[path = "recovery_tests.rs"]
mod tests;

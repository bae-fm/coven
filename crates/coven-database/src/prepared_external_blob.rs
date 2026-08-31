use std::path::{Path, PathBuf};
use std::time::SystemTime;

use coven_foundation::atomic_file::FileError;

use crate::DbError;

/// A user-owned file whose plaintext size and SHA-256 digest Coven read in one
/// pass. Its content facts stay private and can only be consumed by external
/// blob registration.
pub struct PreparedExternalBlob {
    path: PathBuf,
    size: u64,
    hash: String,
    snapshot: FileSnapshot,
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct FileSnapshot {
    size: u64,
    modified: SystemTime,
}

impl PreparedExternalBlob {
    pub(crate) fn validate_current(&self) -> Result<(), DbError> {
        let metadata = std::fs::metadata(&self.path)
            .map_err(|source| FileError::at("stat prepared external blob", &self.path, source))?;
        let current = snapshot(&self.path, &metadata)?;
        if current != self.snapshot {
            return Err(changed(&self.path));
        }
        Ok(())
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn size(&self) -> u64 {
        self.size
    }

    pub(crate) fn hash(&self) -> &str {
        &self.hash
    }
}

/// Stream a user-owned file once and prepare its opaque content identity for
/// registration. `progress` receives the cumulative bytes consumed after each
/// read.
pub async fn prepare_external_blob(
    path: &Path,
    progress: impl Fn(u64) + Send + Sync,
) -> Result<PreparedExternalBlob, DbError> {
    let before_metadata = tokio::fs::metadata(path)
        .await
        .map_err(|source| FileError::at("stat external blob before preparation", path, source))?;
    if !before_metadata.is_file() {
        return Err(FileError::NotFile {
            subject: "external blob",
            path: path.to_path_buf(),
        }
        .into());
    }
    let before = snapshot(path, &before_metadata)?;
    let (size, digest) =
        coven_foundation::local_file::file_facts_with_progress(path, progress).await?;
    let after_metadata = tokio::fs::metadata(path)
        .await
        .map_err(|source| FileError::at("stat external blob after preparation", path, source))?;
    let after = snapshot(path, &after_metadata)?;
    if before != after || size != after.size {
        return Err(changed(path));
    }
    Ok(PreparedExternalBlob {
        path: path.to_path_buf(),
        size,
        hash: hex::encode(digest),
        snapshot: after,
    })
}

fn snapshot(path: &Path, metadata: &std::fs::Metadata) -> Result<FileSnapshot, DbError> {
    let modified = metadata
        .modified()
        .map_err(|source| FileError::at("read external blob modification time", path, source))?;
    Ok(FileSnapshot {
        size: metadata.len(),
        modified,
    })
}

fn changed(path: &Path) -> DbError {
    DbError::Message(format!(
        "external blob changed while Coven prepared it: {}",
        path.display()
    ))
}

use std::path::{Path, PathBuf};

use coven_core::sync::snapshot::{SnapshotError, SnapshotFiles};

static NATIVE_SNAPSHOT_FILES: NativeSnapshotFiles = NativeSnapshotFiles;

pub(crate) fn install_snapshot_files() {
    coven_core::sync::snapshot::register_snapshot_files(&NATIVE_SNAPSHOT_FILES);
}

struct NativeSnapshotFiles;

impl SnapshotFiles for NativeSnapshotFiles {
    fn prepare_snapshot_path(&self, temp_dir: &Path) -> Result<PathBuf, SnapshotError> {
        let snapshot_path = temp_dir.join("snapshot.db");
        match std::fs::remove_file(&snapshot_path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(SnapshotError::Io(e)),
        }
        Ok(snapshot_path)
    }

    fn cleanup_snapshot_path(&self, path: &Path) {
        if let Err(e) = std::fs::remove_file(path) {
            if e.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(error = %e, path = %path.display(), "failed to remove temp snapshot");
            }
        }
    }

    fn read_and_remove_snapshot(&self, path: &Path) -> Result<Vec<u8>, SnapshotError> {
        let bytes = std::fs::read(path)?;
        self.cleanup_snapshot_path(path);
        Ok(bytes)
    }

    fn write_snapshot_db(&self, target_path: &Path, plaintext: &[u8]) -> Result<(), SnapshotError> {
        if let Some(parent) = target_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(target_path, plaintext)?;
        Ok(())
    }
}

use std::io::Write as _;
use std::path::{Path, PathBuf};

pub(crate) const TEMP_FILE_PREFIX: &str = ".tmp.";

/// One local file whose complete contents are installed with a durable rename.
pub(crate) struct AtomicFile {
    path: PathBuf,
}

impl AtomicFile {
    pub(crate) fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub(crate) fn read_optional(&self) -> Result<Option<Vec<u8>>, String> {
        match std::fs::read(&self.path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(format!("read {}: {error}", self.path.display())),
        }
    }

    pub(crate) fn replace(&self, bytes: &[u8]) -> Result<(), String> {
        let parent = self.path.parent().ok_or_else(|| {
            format!(
                "atomic file path has no parent directory: {}",
                self.path.display()
            )
        })?;
        std::fs::create_dir_all(parent)
            .map_err(|error| format!("create parent directory {}: {error}", parent.display()))?;
        let named = tempfile::Builder::new()
            .prefix(TEMP_FILE_PREFIX)
            .tempfile_in(parent)
            .map_err(|error| {
                format!("create temporary file under {}: {error}", parent.display())
            })?;
        let (mut file, temp) = named.into_parts();
        let temp = temp
            .keep()
            .map_err(|error| format!("retain temporary file path: {error}"))?;
        let write = (|| {
            file.write_all(bytes)
                .map_err(|error| format!("write temporary file {}: {error}", temp.display()))?;
            file.sync_all()
                .map_err(|error| format!("fsync temporary file {}: {error}", temp.display()))?;
            drop(file);
            std::fs::rename(&temp, &self.path).map_err(|error| {
                format!(
                    "rename temporary file {} to {}: {error}",
                    temp.display(),
                    self.path.display()
                )
            })?;
            sync_parent(&self.path)
        })();
        if let Err(operation) = write {
            return match std::fs::remove_file(&temp) {
                Ok(()) => Err(operation),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Err(operation),
                Err(cleanup) => Err(format!(
                    "{operation}; remove failed temporary file {}: {cleanup}",
                    temp.display()
                )),
            };
        }
        Ok(())
    }

    pub(crate) fn remove(&self) -> Result<(), String> {
        match std::fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(format!("remove {}: {error}", self.path.display())),
        }
    }

    #[cfg(test)]
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

fn sync_parent(path: &Path) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| format!("path has no parent directory: {}", path.display()))?;
    std::fs::File::open(parent)
        .map_err(|error| format!("open parent directory {}: {error}", parent.display()))?
        .sync_all()
        .map_err(|error| format!("fsync parent directory {}: {error}", parent.display()))
}

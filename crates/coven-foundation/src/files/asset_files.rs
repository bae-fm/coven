//! File-backed assets retained for the lifetime of a native provider operation.

use std::{
    io::{self, Read, Write},
    path::Path,
    sync::Arc,
};

struct Workspace(Option<tempfile::TempDir>);
impl Drop for Workspace {
    fn drop(&mut self) {
        if let Some(directory) = self.0.take() {
            if let Err(error) = directory.close() {
                tracing::error!(%error, "could not remove native asset directory");
            }
        }
    }
}

/// Disk access for a native provider whose payload API requires local files.
pub struct AssetFiles {
    workspace: Arc<Workspace>,
}
impl AssetFiles {
    /// Create a private staging directory at the provider's composition root.
    pub fn new() -> io::Result<Self> {
        Ok(Self { workspace: Arc::new(Workspace(Some(tempfile::tempdir()?))) })
    }
    /// Stage encrypted bytes; the returned owner keeps its directory alive.
    pub fn stage(&self, bytes: &[u8]) -> io::Result<AssetFile> {
        let directory = self.workspace.0.as_ref().expect("live asset directory");
        let mut file = AssetFile {
            file: Some(tempfile::NamedTempFile::new_in(directory.path())?),
            _workspace: self.workspace.clone(),
        };
        file.file.as_mut().expect("live asset file").write_all(bytes)?;
        Ok(file)
    }
    /// Read a framework-owned asset during its callback, refusing oversized data.
    pub fn read(&self, path: &Path, limit: usize) -> io::Result<Vec<u8>> {
        let file = std::fs::File::open(path)?;
        let mut bytes = Vec::new();
        file.take(limit as u64 + 1).read_to_end(&mut bytes)?;
        if bytes.len() > limit {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "native asset exceeds part bound",
            ));
        }
        Ok(bytes)
    }
}

/// An asset file whose lifetime extends through the native completion callback.
pub struct AssetFile {
    file: Option<tempfile::NamedTempFile>,
    _workspace: Arc<Workspace>,
}
impl AssetFile {
    /// The path passed to the native asset constructor.
    pub fn path(&self) -> &Path {
        self.file.as_ref().expect("live asset file").path()
    }
    /// Remove staging bytes and return any filesystem failure to the operation.
    pub fn close(mut self) -> io::Result<()> {
        self.file.take().expect("live asset file").close()
    }
}
impl Drop for AssetFile {
    fn drop(&mut self) {
        if let Some(file) = self.file.take() {
            if let Err(error) = file.close() {
                tracing::error!(%error, "could not remove native asset file");
            }
        }
    }
}

#[cfg(test)]
#[path = "asset_files_tests.rs"]
mod tests;

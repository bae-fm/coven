//! Storage-side adapters over the foundation staged-file machinery in
//! [`coven_foundation::local_file`]: typed plaintext chunk sources whose remote errors
//! stay classified, and exact-fact reads expressed in protocol hashes.

use std::path::Path;

use async_trait::async_trait;
use tokio::io::AsyncReadExt;

pub(crate) struct PlaintextReader(
    Box<dyn coven_foundation::local_file::PlaintextChunkReader<Error = PlaintextChunkError>>,
);

impl PlaintextReader {
    pub(crate) async fn next_chunk(&mut self, max: usize) -> Result<Vec<u8>, String> {
        self.0
            .next_chunk(max)
            .await
            .map_err(|error| error.to_string())
    }

    #[cfg(test)]
    pub(crate) fn from_test_reader(
        reader: impl coven_foundation::local_file::PlaintextChunkReader<Error = PlaintextChunkError>
            + 'static,
    ) -> Self {
        Self(Box::new(reader))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum PlaintextChunkError {
    #[error(transparent)]
    Remote(#[from] crate::protocol::objects::StorageError),
    #[error("invalid remote content: {0}")]
    InvalidContent(String),
    #[error("local plaintext source: {0}")]
    Local(String),
}

pub(super) async fn open_reader(path: &Path) -> Result<PlaintextReader, String> {
    let file = tokio::fs::File::open(path)
        .await
        .map_err(|e| format!("open local blob {} for streaming: {e}", path.display()))?;
    Ok(PlaintextReader(Box::new(FilePlaintextReader {
        file,
        path: path.to_path_buf(),
    })))
}

/// Stream the exact stored size and SHA-256 identity of a local file.
pub(super) async fn exact_file_facts(
    path: &Path,
) -> Result<(u64, crate::protocol::store_commit::ObjectHash), String> {
    let (size, digest) = coven_foundation::local_file::file_facts(path).await?;
    Ok((
        size,
        crate::protocol::store_commit::ObjectHash::from_digest(digest),
    ))
}

struct FilePlaintextReader {
    file: tokio::fs::File,
    path: std::path::PathBuf,
}

#[async_trait]
impl coven_foundation::local_file::PlaintextChunkReader for FilePlaintextReader {
    type Error = PlaintextChunkError;

    async fn next_chunk(&mut self, max: usize) -> Result<Vec<u8>, PlaintextChunkError> {
        debug_assert!(max > 0, "next_chunk max must be positive");
        let mut buf = vec![0u8; max];
        let mut filled = 0;
        while filled < max {
            let read = self.file.read(&mut buf[filled..]).await.map_err(|e| {
                PlaintextChunkError::Local(format!("read local blob {}: {e}", self.path.display()))
            })?;
            if read == 0 {
                break;
            }
            filled += read;
        }
        buf.truncate(filled);
        Ok(buf)
    }
}

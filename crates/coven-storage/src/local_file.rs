//! Storage-side adapters over the foundation staged-file machinery in
//! [`coven_foundation::local_file`]: typed plaintext chunk sources whose remote errors
//! stay classified, and exact-fact reads expressed in protocol hashes.

use std::path::Path;

use async_trait::async_trait;
use tokio::io::AsyncReadExt;

pub struct PlaintextReader(
    Box<dyn coven_foundation::local_file::PlaintextChunkReader<Error = PlaintextChunkError>>,
);

impl PlaintextReader {
    pub(crate) async fn next_chunk(&mut self, max: usize) -> Result<Vec<u8>, PlaintextChunkError> {
        self.0.next_chunk(max).await
    }

    #[cfg(test)]
    pub(crate) fn from_test_reader(
        reader: impl coven_foundation::local_file::PlaintextChunkReader<Error = PlaintextChunkError>
            + 'static,
    ) -> Self {
        Self(Box::new(reader))
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum PlaintextChunkError {
    #[error(transparent)]
    Remote(#[from] coven_protocol::objects::StorageError),
    #[error("invalid remote content: {0}")]
    InvalidContent(String),
    #[error("decrypt remote content {context}: {source}")]
    Decryption {
        context: String,
        #[source]
        source: coven_keys::encryption::EncryptionError,
    },
    #[error("local plaintext source: {0}")]
    Local(coven_foundation::atomic_file::FileError),
}

impl From<PlaintextChunkError> for coven_protocol::objects::StorageError {
    fn from(error: PlaintextChunkError) -> Self {
        match error {
            PlaintextChunkError::Remote(error) => error,
            PlaintextChunkError::InvalidContent(message) => Self::InvalidContent(message),
            PlaintextChunkError::Decryption { context, source } => {
                Self::Decryption { context, source }
            }
            PlaintextChunkError::Local(error) => Self::LocalFilesystem(error),
        }
    }
}

pub(crate) async fn open_reader(
    path: &Path,
) -> Result<PlaintextReader, coven_foundation::atomic_file::FileError> {
    let file = tokio::fs::File::open(path).await.map_err(|source| {
        coven_foundation::atomic_file::FileError::Path {
            operation: "open local blob for streaming",
            path: path.to_path_buf(),
            source,
        }
    })?;
    Ok(PlaintextReader(Box::new(FilePlaintextReader {
        file,
        path: path.to_path_buf(),
    })))
}

/// Stream the exact stored size and SHA-256 identity of a local file.
pub(crate) async fn exact_file_facts(
    path: &Path,
) -> Result<(u64, coven_protocol::store_commit::ObjectHash), coven_foundation::atomic_file::FileError>
{
    let (size, digest) = coven_foundation::local_file::file_facts(path).await?;
    Ok((
        size,
        coven_protocol::store_commit::ObjectHash::from_digest(digest),
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
            let read = self.file.read(&mut buf[filled..]).await.map_err(|source| {
                PlaintextChunkError::Local(coven_foundation::atomic_file::FileError::Path {
                    operation: "read local blob",
                    path: self.path.clone(),
                    source,
                })
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

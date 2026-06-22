//! Local managed blob storage: plaintext files at content-addressed paths.
use crate::storage::local::storage_path;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Storage not configured")]
    NotConfigured,
    #[error("Cloud storage error: {0}")]
    Cloud(String),
    #[error("Database error: {0}")]
    Database(String),
    /// The file id does not form a safe content-addressed path (see
    /// [`crate::library_dir::BlobPathError`]). For local managed storage the id is
    /// device-generated, so this is a programmer error surfaced loudly, never a
    /// silent mis-shard.
    #[error("invalid blob id: {0}")]
    InvalidId(#[from] crate::library_dir::BlobPathError),
}

/// Progress callback type: (bytes_written, total_bytes)
pub type ProgressCallback = Box<dyn Fn(usize, usize) + Send + Sync>;

/// Storage implementation for managed local storage.
///
/// Writes files to `library_dir/storage/ab/cd/{file_id}` as plaintext.
/// Local files are never encrypted -- encryption only happens when uploading
/// to the cloud home.
#[derive(Clone)]
pub struct BlobStore {
    library_dir: crate::library_dir::LibraryDir,
}

impl BlobStore {
    /// Create storage for managed local blobs.
    pub fn new_local(library_dir: crate::library_dir::LibraryDir) -> Self {
        Self { library_dir }
    }

    /// Write bytes to local storage without creating a DB record.
    ///
    /// Uses the given `file_id` for the hash-based storage path.
    pub async fn store_bytes(
        &self,
        file_id: &str,
        data: &[u8],
        on_progress: ProgressCallback,
    ) -> Result<(), StorageError> {
        use tokio::io::AsyncWriteExt;

        let total_bytes = data.len();
        on_progress(0, total_bytes);

        let rel_path = storage_path(file_id)?;
        let path = self.library_dir.join(&rel_path);

        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let batch_size = 1_048_576;
        let file = tokio::fs::File::create(&path).await?;
        let mut writer = tokio::io::BufWriter::new(file);
        let mut bytes_written = 0usize;

        for chunk in data.chunks(batch_size) {
            writer.write_all(chunk).await?;
            bytes_written += chunk.len();
            on_progress(bytes_written.min(total_bytes), total_bytes);
        }

        writer.flush().await?;

        Ok(())
    }

    /// Stream a source file from disk into local storage without buffering the
    /// whole thing in memory. Progress is reported in 1 MiB batches to match
    /// the cadence of `store_bytes`.
    pub async fn store_from_path(
        &self,
        file_id: &str,
        source: &std::path::Path,
        on_progress: ProgressCallback,
    ) -> Result<(), StorageError> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let total_bytes = tokio::fs::metadata(source).await?.len() as usize;
        on_progress(0, total_bytes);

        let rel_path = storage_path(file_id)?;
        let path = self.library_dir.join(&rel_path);

        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let batch_size = 1_048_576;
        let mut reader = tokio::fs::File::open(source).await?;
        let dest = tokio::fs::File::create(&path).await?;
        let mut writer = tokio::io::BufWriter::new(dest);
        let mut buf = vec![0u8; batch_size];
        let mut bytes_written = 0usize;

        // Fill `buf` up to `batch_size` per iteration so progress fires once per
        // full batch. A single `read` on `tokio::fs::File` (even via `BufReader`)
        // can return far less than the requested length, so without the inner
        // fill loop we'd report progress on every short read.
        loop {
            let mut filled = 0usize;
            while filled < batch_size {
                let n = reader.read(&mut buf[filled..]).await?;
                if n == 0 {
                    break;
                }
                filled += n;
            }
            if filled == 0 {
                break;
            }
            writer.write_all(&buf[..filled]).await?;
            bytes_written += filled;
            on_progress(bytes_written.min(total_bytes), total_bytes);
        }

        writer.flush().await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tempfile::TempDir;

    #[tokio::test]
    async fn store_from_path_copies_bytes_and_reports_1mib_cadence() {
        let temp = TempDir::new().unwrap();
        let library_dir = crate::library_dir::LibraryDir::new(temp.path());
        let storage = BlobStore::new_local(library_dir);

        // 2.5 MiB — two full batches plus a partial tail.
        let total = 2_621_440usize;
        let source_bytes: Vec<u8> = (0..total).map(|i| (i % 256) as u8).collect();
        let source_path = temp.path().join("source.bin");
        tokio::fs::write(&source_path, &source_bytes).await.unwrap();

        let calls: Arc<Mutex<Vec<(usize, usize)>>> = Arc::new(Mutex::new(Vec::new()));
        let calls_clone = calls.clone();
        let file_id = "abcdef1234567890";
        storage
            .store_from_path(
                file_id,
                &source_path,
                Box::new(move |written, total| {
                    calls_clone.lock().unwrap().push((written, total));
                }),
            )
            .await
            .unwrap();

        let dest_path = temp.path().join(storage_path(file_id).expect("valid id"));
        let dest_bytes = tokio::fs::read(&dest_path).await.unwrap();
        assert_eq!(dest_bytes, source_bytes, "destination equals source");

        let calls = calls.lock().unwrap();
        assert_eq!(
            &*calls,
            &[
                (0, total),
                (1_048_576, total),
                (2_097_152, total),
                (total, total),
            ],
            "progress fires once per 1 MiB batch (plus initial and final partial)",
        );
    }
}

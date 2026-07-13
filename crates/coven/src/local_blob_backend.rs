use std::path::{Path, PathBuf};

use async_trait::async_trait;
use coven_core::local_blob::{PlatformLocalBlobBackend, PlatformPlaintextReader, TEMP_BLOB_PREFIX};
use tokio::io::AsyncReadExt;

static NATIVE_LOCAL_BLOB_BACKEND: NativeLocalBlobBackend = NativeLocalBlobBackend;

pub(crate) fn install_platform_backend() {
    coven_core::local_blob::register_platform_local_blob_backend(&NATIVE_LOCAL_BLOB_BACKEND);
}

struct NativeLocalBlobBackend;

struct NativePlaintextReader {
    file: tokio::fs::File,
    path: PathBuf,
}

#[async_trait]
impl PlatformPlaintextReader for NativePlaintextReader {
    async fn next_chunk(&mut self, max: usize) -> Result<Vec<u8>, String> {
        debug_assert!(max > 0, "next_chunk max must be positive");
        let mut buf = vec![0u8; max];
        let mut filled = 0;
        while filled < max {
            let n = self
                .file
                .read(&mut buf[filled..])
                .await
                .map_err(|e| format!("read local blob {}: {e}", self.path.display()))?;
            if n == 0 {
                break;
            }
            filled += n;
        }
        buf.truncate(filled);
        Ok(buf)
    }
}

#[async_trait]
impl PlatformLocalBlobBackend for NativeLocalBlobBackend {
    async fn open_reader(&self, path: &Path) -> Result<Box<dyn PlatformPlaintextReader>, String> {
        let file = tokio::fs::File::open(path)
            .await
            .map_err(|e| format!("open local blob {} for streaming: {e}", path.display()))?;
        Ok(Box::new(NativePlaintextReader {
            file,
            path: path.to_path_buf(),
        }))
    }

    async fn file_len(&self, path: &Path) -> Result<u64, String> {
        tokio::fs::metadata(path)
            .await
            .map(|m| m.len())
            .map_err(|e| format!("stat local blob {}: {e}", path.display()))
    }

    async fn copy_atomic(&self, src: &Path, dst: &Path) -> Result<(), String> {
        use tokio::io::AsyncWriteExt;

        let parent = dst
            .parent()
            .ok_or_else(|| format!("blob path has no parent dir: {}", dst.display()))?;
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|e| format!("create parent dir for {}: {e}", dst.display()))?;
        // A temp sibling in the destination directory keeps the rename within one
        // filesystem, so readers see either the previous destination or the whole
        // copied file.
        let tmp = parent.join(format!("{TEMP_BLOB_PREFIX}{}", uuid::Uuid::new_v4()));

        let copy = async {
            let mut input = tokio::fs::File::open(src)
                .await
                .map_err(|e| format!("open pin source {}: {e}", src.display()))?;
            let mut out = tokio::fs::File::create(&tmp)
                .await
                .map_err(|e| format!("create temp pin {}: {e}", tmp.display()))?;
            // Stream src into the temp file in bounded windows, then fsync the
            // temp before its name can become the destination.
            let mut buf = vec![0u8; 1 << 20];
            loop {
                let n = input
                    .read(&mut buf)
                    .await
                    .map_err(|e| format!("read pin source {}: {e}", src.display()))?;
                if n == 0 {
                    break;
                }
                out.write_all(&buf[..n])
                    .await
                    .map_err(|e| format!("write temp pin {}: {e}", tmp.display()))?;
            }
            out.sync_all()
                .await
                .map_err(|e| format!("fsync temp pin {}: {e}", tmp.display()))?;
            Ok::<(), String>(())
        }
        .await;
        if let Err(e) = copy {
            // The destination still points at the old file or is absent; remove
            // only the temp debris left by the failed copy.
            if let Err(rm) = tokio::fs::remove_file(&tmp).await {
                tracing::warn!(
                    "could not remove temp pin {} after a failed copy: {rm}",
                    tmp.display()
                );
            }
            return Err(e);
        }
        if let Err(e) = tokio::fs::rename(&tmp, dst).await {
            // A failed rename leaves the destination unchanged; remove only the
            // temp file the caller should not observe.
            if let Err(rm) = tokio::fs::remove_file(&tmp).await {
                tracing::warn!(
                    "could not remove temp pin {} after a failed rename: {rm}",
                    tmp.display()
                );
            }
            return Err(format!(
                "rename temp pin {} -> {}: {e}",
                tmp.display(),
                dst.display()
            ));
        }
        Ok(())
    }

    async fn write_stream_atomic(
        &self,
        path: &Path,
        source: &mut dyn PlatformPlaintextReader,
    ) -> Result<u64, String> {
        use tokio::io::AsyncWriteExt;

        let parent = path
            .parent()
            .ok_or_else(|| format!("blob path has no parent dir: {}", path.display()))?;
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|e| format!("create parent dir for {}: {e}", path.display()))?;
        let tmp = parent.join(format!("{TEMP_BLOB_PREFIX}{}", uuid::Uuid::new_v4()));

        let write_tmp = async {
            let mut file = tokio::fs::File::create(&tmp)
                .await
                .map_err(|e| format!("create temp blob {}: {e}", tmp.display()))?;
            let mut written = 0u64;
            loop {
                let chunk = source.next_chunk(1 << 20).await?;
                if chunk.is_empty() {
                    break;
                }
                file.write_all(&chunk)
                    .await
                    .map_err(|e| format!("write temp blob {}: {e}", tmp.display()))?;
                written += chunk.len() as u64;
            }
            file.sync_all()
                .await
                .map_err(|e| format!("fsync temp blob {}: {e}", tmp.display()))?;
            Ok::<u64, String>(written)
        }
        .await;
        let written = match write_tmp {
            Ok(written) => written,
            Err(e) => {
                if let Err(rm) = tokio::fs::remove_file(&tmp).await {
                    tracing::warn!(
                        "could not remove temp blob {} after a failed streamed write: {rm}",
                        tmp.display()
                    );
                }
                return Err(e);
            }
        };

        if let Err(e) = tokio::fs::rename(&tmp, path).await {
            if let Err(rm) = tokio::fs::remove_file(&tmp).await {
                tracing::warn!(
                    "could not remove temp blob {} after a failed streamed rename: {rm}",
                    tmp.display()
                );
            }
            return Err(format!(
                "rename temp blob {} -> {}: {e}",
                tmp.display(),
                path.display()
            ));
        }
        Ok(written)
    }

    async fn read(&self, path: &Path) -> Result<Vec<u8>, String> {
        tokio::fs::read(path)
            .await
            .map_err(|e| format!("read local blob {}: {e}", path.display()))
    }

    async fn read_range(&self, path: &Path, offset: u64, len: u64) -> Result<Vec<u8>, String> {
        use tokio::io::{AsyncReadExt, AsyncSeekExt};

        if len == 0 {
            return Ok(Vec::new());
        }
        let mut file = tokio::fs::File::open(path)
            .await
            .map_err(|e| format!("open local blob {} for ranged read: {e}", path.display()))?;
        file.seek(std::io::SeekFrom::Start(offset))
            .await
            .map_err(|e| format!("seek local blob {} to {offset}: {e}", path.display()))?;
        let mut buf = vec![0u8; len as usize];
        file.read_exact(&mut buf).await.map_err(|e| {
            format!(
                "read {len} bytes at {offset} from local blob {}: {e}",
                path.display()
            )
        })?;
        Ok(buf)
    }

    async fn write_atomic(&self, path: &Path, bytes: &[u8]) -> Result<(), String> {
        use tokio::io::AsyncWriteExt;

        let parent = path
            .parent()
            .ok_or_else(|| format!("blob path has no parent dir: {}", path.display()))?;
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|e| format!("create parent dir for {}: {e}", path.display()))?;
        // A temp sibling in the same directory keeps the final rename on one
        // filesystem. The destination name does not point at the new bytes until
        // the temp file has been fully written and fsynced.
        let tmp = parent.join(format!("{TEMP_BLOB_PREFIX}{}", uuid::Uuid::new_v4()));

        let write_tmp = async {
            let mut file = tokio::fs::File::create(&tmp)
                .await
                .map_err(|e| format!("create temp blob {}: {e}", tmp.display()))?;
            file.write_all(bytes)
                .await
                .map_err(|e| format!("write temp blob {}: {e}", tmp.display()))?;
            file.sync_all()
                .await
                .map_err(|e| format!("fsync temp blob {}: {e}", tmp.display()))?;
            Ok::<(), String>(())
        }
        .await;
        if let Err(e) = write_tmp {
            // A failed temp write leaves the destination untouched; remove only
            // the temp debris so a retry starts from the same visible state.
            if let Err(rm) = tokio::fs::remove_file(&tmp).await {
                tracing::warn!(
                    "could not remove temp blob {} after a failed write: {rm}",
                    tmp.display()
                );
            }
            return Err(e);
        }

        if let Err(e) = tokio::fs::rename(&tmp, path).await {
            // A failed rename leaves the old destination visible; remove only the
            // temp file that never became authoritative.
            if let Err(rm) = tokio::fs::remove_file(&tmp).await {
                tracing::warn!(
                    "could not remove temp blob {} after a failed rename: {rm}",
                    tmp.display()
                );
            }
            return Err(format!(
                "rename temp blob {} -> {}: {e}",
                tmp.display(),
                path.display()
            ));
        }
        // No parent-directory fsync. Cache correctness needs atomic visibility:
        // a reader sees the old file or the whole new one, never a torn file.
        // The cache is a mirror of cloud blobs, so losing the post-rename
        // directory entry to a crash means the blob is absent and can be fetched
        // again; preserving that directory entry is not part of this contract.
        Ok(())
    }

    async fn exists(&self, path: &Path) -> Result<bool, String> {
        tokio::fs::try_exists(path)
            .await
            .map_err(|e| format!("check local blob {}: {e}", path.display()))
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<(), String> {
        tokio::fs::rename(from, to)
            .await
            .map_err(|e| format!("rename {} -> {}: {e}", from.display(), to.display()))
    }

    async fn remove_file(&self, path: &Path) -> Result<bool, String> {
        match tokio::fs::remove_file(path).await {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(format!("remove file {}: {e}", path.display())),
        }
    }

    async fn remove_dir_all(&self, path: &Path) -> Result<bool, String> {
        match tokio::fs::remove_dir_all(path).await {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(format!("remove dir tree {}: {e}", path.display())),
        }
    }

    async fn create_dir_all(&self, path: &Path) -> Result<(), String> {
        tokio::fs::create_dir_all(path)
            .await
            .map_err(|e| format!("create dir tree {}: {e}", path.display()))
    }

    async fn sync_parent_dir(&self, path: &Path) -> Result<(), String> {
        let parent = path
            .parent()
            .ok_or_else(|| format!("path has no parent dir: {}", path.display()))?;
        let dir = tokio::fs::File::open(parent)
            .await
            .map_err(|e| format!("open parent dir {} to fsync: {e}", parent.display()))?;
        dir.sync_all()
            .await
            .map_err(|e| format!("fsync parent dir {}: {e}", parent.display()))
    }

    async fn walk_files(&self, dir: &Path) -> Result<Vec<(PathBuf, u64, u64)>, String> {
        let mut files = Vec::new();
        let mut stack = vec![dir.to_path_buf()];
        while let Some(d) = stack.pop() {
            let mut read_dir = match tokio::fs::read_dir(&d).await {
                Ok(rd) => rd,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    tracing::debug!("walk_files: {} absent, skipping", d.display());
                    continue;
                }
                Err(e) => return Err(format!("read dir {}: {e}", d.display())),
            };

            loop {
                let entry = match read_dir.next_entry().await {
                    Ok(Some(entry)) => entry,
                    Ok(None) => break,
                    Err(e) => return Err(format!("read dir entry under {}: {e}", d.display())),
                };
                let path = entry.path();
                let metadata = match entry.metadata().await {
                    Ok(metadata) => metadata,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        tracing::debug!(
                            "walk_files: {} vanished between listing and stat, skipping",
                            path.display()
                        );
                        continue;
                    }
                    Err(e) => return Err(format!("stat entry {}: {e}", path.display())),
                };
                if metadata.is_dir() {
                    stack.push(path);
                } else {
                    files.push((path, mtime_millis(&metadata)?, metadata.len()));
                }
            }
        }
        Ok(files)
    }
}

fn mtime_millis(metadata: &std::fs::Metadata) -> Result<u64, String> {
    Ok(metadata
        .modified()
        .map_err(|e| format!("modified time: {e}"))?
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| format!("modified time predates Unix epoch: {e}"))?
        .as_millis() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The durable variant runs the real native atomic write plus a real
    /// parent-directory fsync and leaves a correct, readable file. The fsync
    /// itself isn't observable from a normal filesystem, so this pins the same
    /// contract the make_local durability path relies on: after the durable
    /// write returns, the destination holds exactly the written bytes.
    #[tokio::test]
    async fn write_atomic_durable_leaves_a_readable_file() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let path = tmp.path().join("nested").join("sync_staging.bin");
        let bytes = b"packed outgoing changeset".to_vec();

        NativeLocalBlobBackend
            .write_atomic_durable(&path, &bytes)
            .await
            .expect("durable write");

        let read_back = NativeLocalBlobBackend.read(&path).await.expect("read back");
        assert_eq!(read_back, bytes);
    }
}

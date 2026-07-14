use std::path::{Path, PathBuf};

use async_trait::async_trait;
use tokio::io::AsyncReadExt;

use crate::local_blob::{PlatformLocalBlobBackend, PlatformPlaintextReader, TEMP_BLOB_PREFIX};

pub(crate) static TEST_LOCAL_BLOB_BACKEND: TestLocalBlobBackend = TestLocalBlobBackend;

pub(crate) struct TestLocalBlobBackend;

struct TestPlaintextReader {
    file: tokio::fs::File,
    path: PathBuf,
}

#[async_trait]
impl PlatformPlaintextReader for TestPlaintextReader {
    async fn next_chunk(&mut self, max: usize) -> Result<Vec<u8>, String> {
        let mut buf = vec![0u8; max];
        let n = self
            .file
            .read(&mut buf)
            .await
            .map_err(|e| format!("read local blob {}: {e}", self.path.display()))?;
        buf.truncate(n);
        Ok(buf)
    }
}

#[async_trait]
impl PlatformLocalBlobBackend for TestLocalBlobBackend {
    async fn open_reader(&self, path: &Path) -> Result<Box<dyn PlatformPlaintextReader>, String> {
        let file = tokio::fs::File::open(path)
            .await
            .map_err(|e| format!("open local blob {} for streaming: {e}", path.display()))?;
        Ok(Box::new(TestPlaintextReader {
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
        let bytes = self.read(src).await?;
        self.write_atomic(dst, &bytes).await
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
                if let Err(cleanup) = tokio::fs::remove_file(&tmp).await {
                    tracing::warn!(
                        "failed to remove temp blob {} after write failure: {cleanup}",
                        tmp.display()
                    );
                }
                return Err(e);
            }
        };
        if let Err(e) = tokio::fs::rename(&tmp, path).await {
            if let Err(cleanup) = tokio::fs::remove_file(&tmp).await {
                tracing::warn!(
                    "failed to remove temp blob {} after rename failure: {cleanup}",
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
        let parent = path
            .parent()
            .ok_or_else(|| format!("blob path has no parent dir: {}", path.display()))?;
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|e| format!("create parent dir for {}: {e}", path.display()))?;
        let tmp = parent.join(format!("{TEMP_BLOB_PREFIX}{}", uuid::Uuid::new_v4()));
        tokio::fs::write(&tmp, bytes)
            .await
            .map_err(|e| format!("write local blob {}: {e}", tmp.display()))?;
        tokio::fs::rename(&tmp, path).await.map_err(|e| {
            format!(
                "rename temp blob {} -> {}: {e}",
                tmp.display(),
                path.display()
            )
        })
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
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(format!("read dir {}: {e}", d.display())),
            };
            while let Some(entry) = read_dir
                .next_entry()
                .await
                .map_err(|e| format!("read dir entry under {}: {e}", d.display()))?
            {
                let path = entry.path();
                let metadata = entry
                    .metadata()
                    .await
                    .map_err(|e| format!("stat entry {}: {e}", path.display()))?;
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
mod durable_write_tests {
    use super::*;
    use std::sync::Mutex;

    /// Records what the default [`PlatformLocalBlobBackend::write_atomic_durable`]
    /// calls, so the test can assert the variant makes the directory entry
    /// durable — it performs the atomic write and then fsyncs the same path's
    /// parent. Every other method is unreachable through the durable variant.
    #[derive(Default)]
    struct RecordingBackend {
        wrote: Mutex<Option<(PathBuf, Vec<u8>)>>,
        synced_parent_of: Mutex<Option<PathBuf>>,
    }

    #[async_trait]
    impl PlatformLocalBlobBackend for RecordingBackend {
        async fn write_atomic(&self, path: &Path, bytes: &[u8]) -> Result<(), String> {
            *self.wrote.lock().unwrap() = Some((path.to_path_buf(), bytes.to_vec()));
            Ok(())
        }

        async fn sync_parent_dir(&self, path: &Path) -> Result<(), String> {
            *self.synced_parent_of.lock().unwrap() = Some(path.to_path_buf());
            Ok(())
        }

        async fn open_reader(
            &self,
            _path: &Path,
        ) -> Result<Box<dyn PlatformPlaintextReader>, String> {
            unimplemented!("not exercised by write_atomic_durable")
        }
        async fn file_len(&self, _path: &Path) -> Result<u64, String> {
            unimplemented!("not exercised by write_atomic_durable")
        }
        async fn copy_atomic(&self, _src: &Path, _dst: &Path) -> Result<(), String> {
            unimplemented!("not exercised by write_atomic_durable")
        }
        async fn write_stream_atomic(
            &self,
            _path: &Path,
            _source: &mut dyn PlatformPlaintextReader,
        ) -> Result<u64, String> {
            unimplemented!("not exercised by write_atomic_durable")
        }
        async fn read(&self, _path: &Path) -> Result<Vec<u8>, String> {
            unimplemented!("not exercised by write_atomic_durable")
        }
        async fn read_range(
            &self,
            _path: &Path,
            _offset: u64,
            _len: u64,
        ) -> Result<Vec<u8>, String> {
            unimplemented!("not exercised by write_atomic_durable")
        }
        async fn exists(&self, _path: &Path) -> Result<bool, String> {
            unimplemented!("not exercised by write_atomic_durable")
        }
        async fn rename(&self, _from: &Path, _to: &Path) -> Result<(), String> {
            unimplemented!("not exercised by write_atomic_durable")
        }
        async fn remove_file(&self, _path: &Path) -> Result<bool, String> {
            unimplemented!("not exercised by write_atomic_durable")
        }
        async fn remove_dir_all(&self, _path: &Path) -> Result<bool, String> {
            unimplemented!("not exercised by write_atomic_durable")
        }
        async fn create_dir_all(&self, _path: &Path) -> Result<(), String> {
            unimplemented!("not exercised by write_atomic_durable")
        }
        async fn walk_files(&self, _dir: &Path) -> Result<Vec<(PathBuf, u64, u64)>, String> {
            unimplemented!("not exercised by write_atomic_durable")
        }
    }

    #[tokio::test]
    async fn durable_write_fsyncs_the_written_file_s_parent() {
        let backend = RecordingBackend::default();
        let path = Path::new("/store/upload_staging.bin");

        backend
            .write_atomic_durable(path, b"packed changeset")
            .await
            .expect("durable write");

        assert_eq!(
            *backend.wrote.lock().unwrap(),
            Some((path.to_path_buf(), b"packed changeset".to_vec())),
            "the durable variant performs the atomic write"
        );
        assert_eq!(
            *backend.synced_parent_of.lock().unwrap(),
            Some(path.to_path_buf()),
            "the durable variant fsyncs the written file's parent so the rename's \
             directory entry survives power loss"
        );
    }
}

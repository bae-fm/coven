use std::path::{Path, PathBuf};

use async_trait::async_trait;
use tokio::io::AsyncReadExt;

use crate::local_blob::{PlatformLocalBlobBackend, PlatformPlaintextReader};

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
        let tmp = parent.join(format!(".tmp.{}", uuid::Uuid::new_v4()));
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

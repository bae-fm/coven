use std::path::{Path, PathBuf};

use async_trait::async_trait;

pub struct PlaintextReader(Box<dyn PlatformPlaintextReader>);

impl PlaintextReader {
    pub async fn next_chunk(&mut self, max: usize) -> Result<Vec<u8>, String> {
        self.0.next_chunk(max).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait PlatformPlaintextReader: crate::MaybeThreadSafe {
    async fn next_chunk(&mut self, max: usize) -> Result<Vec<u8>, String>;
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait PlatformLocalBlobBackend: crate::MaybeThreadSafe {
    async fn open_reader(&self, path: &Path) -> Result<Box<dyn PlatformPlaintextReader>, String>;
    async fn file_len(&self, path: &Path) -> Result<u64, String>;
    async fn copy_atomic(&self, src: &Path, dst: &Path) -> Result<(), String>;
    async fn read(&self, path: &Path) -> Result<Vec<u8>, String>;
    async fn read_range(&self, path: &Path, offset: u64, len: u64) -> Result<Vec<u8>, String>;
    async fn write_atomic(&self, path: &Path, bytes: &[u8]) -> Result<(), String>;
    async fn exists(&self, path: &Path) -> Result<bool, String>;
    async fn rename(&self, from: &Path, to: &Path) -> Result<(), String>;
    async fn remove_file(&self, path: &Path) -> Result<bool, String>;
    #[cfg(test)]
    async fn remove_dir_all(&self, path: &Path) -> Result<bool, String>;
    async fn create_dir_all(&self, path: &Path) -> Result<(), String>;
    async fn sync_parent_dir(&self, _path: &Path) -> Result<(), String> {
        Ok(())
    }
    async fn walk_files(&self, dir: &Path) -> Result<Vec<(PathBuf, u64, u64)>, String>;
}

pub async fn open_reader(path: &Path) -> Result<PlaintextReader, String> {
    backend().open_reader(path).await.map(PlaintextReader)
}

pub async fn file_len(path: &Path) -> Result<u64, String> {
    backend().file_len(path).await
}

pub async fn copy_atomic(src: &Path, dst: &Path) -> Result<(), String> {
    backend().copy_atomic(src, dst).await
}

pub async fn read(path: &Path) -> Result<Vec<u8>, String> {
    backend().read(path).await
}

pub async fn read_range(path: &Path, offset: u64, len: u64) -> Result<Vec<u8>, String> {
    backend().read_range(path, offset, len).await
}

pub async fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), String> {
    backend().write_atomic(path, bytes).await
}

pub async fn exists(path: &Path) -> Result<bool, String> {
    backend().exists(path).await
}

pub async fn rename(from: &Path, to: &Path) -> Result<(), String> {
    backend().rename(from, to).await
}

pub async fn remove_file(path: &Path) -> Result<bool, String> {
    backend().remove_file(path).await
}

#[cfg(test)]
pub async fn remove_dir_all(path: &Path) -> Result<bool, String> {
    backend().remove_dir_all(path).await
}

pub async fn create_dir_all(path: &Path) -> Result<(), String> {
    backend().create_dir_all(path).await
}

pub async fn sync_parent_dir(path: &Path) -> Result<(), String> {
    backend().sync_parent_dir(path).await
}

pub async fn walk_files(dir: &Path) -> Result<Vec<(PathBuf, u64, u64)>, String> {
    backend().walk_files(dir).await
}

#[cfg(not(target_arch = "wasm32"))]
static PLATFORM_BACKEND: std::sync::OnceLock<&'static dyn PlatformLocalBlobBackend> =
    std::sync::OnceLock::new();

#[cfg(target_arch = "wasm32")]
thread_local! {
    static PLATFORM_BACKEND: std::cell::Cell<Option<&'static dyn PlatformLocalBlobBackend>> =
        std::cell::Cell::new(None);
}

#[cfg(not(target_arch = "wasm32"))]
pub fn register_platform_local_blob_backend(backend: &'static dyn PlatformLocalBlobBackend) {
    let _ = PLATFORM_BACKEND.set(backend);
}

#[cfg(target_arch = "wasm32")]
pub fn register_platform_local_blob_backend(backend: &'static dyn PlatformLocalBlobBackend) {
    PLATFORM_BACKEND.with(|slot| slot.set(Some(backend)));
}

#[cfg(not(target_arch = "wasm32"))]
fn backend() -> &'static dyn PlatformLocalBlobBackend {
    if let Some(backend) = PLATFORM_BACKEND.get().copied() {
        backend
    } else {
        #[cfg(any(test, feature = "test-utils"))]
        {
            &crate::local_blob_tests::TEST_LOCAL_BLOB_BACKEND
        }
        #[cfg(not(any(test, feature = "test-utils")))]
        {
            panic!("platform local blob backend is not registered")
        }
    }
}

#[cfg(target_arch = "wasm32")]
fn backend() -> &'static dyn PlatformLocalBlobBackend {
    PLATFORM_BACKEND
        .with(|slot| slot.get())
        .expect("platform local blob backend is not registered")
}

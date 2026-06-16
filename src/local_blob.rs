//! The device-local plaintext file behind each [`crate::blob::BlobRef`].
//!
//! coven reads this file on push (then encrypts and uploads it) and writes it on
//! pull (after downloading and decrypting). The host chooses where it lives via
//! [`BlobRef::local_path`](crate::blob::BlobRef::local_path); this module is just
//! the read / write / exists primitives over whatever storage the target platform
//! has.
//!
//! - **Native** uses the filesystem through `tokio::fs`, so a large blob's read or
//!   write runs on the blocking pool instead of stalling the sync loop.
//! - **wasm** has no `std::fs`, so it uses the browser's Origin Private File System,
//!   reached through the dedicated DB Worker's *synchronous* access handles — the
//!   same stable OPFS API the SQLite VFS runs on. Getting a handle is async (it is
//!   awaited here); the read/write through it is synchronous. An absolute coven path
//!   like `/coven/images/ab/cd/<id>` maps to nested OPFS directories ending in the
//!   file, so the on-disk layout the host names is preserved under the OPFS root.

use std::path::Path;

/// Read the whole local file at `path`. `Err` if it is missing or unreadable — a
/// caller (the push read, the outbox drain) treats that as a failed upload, never
/// as empty bytes.
pub async fn read(path: &Path) -> Result<Vec<u8>, String> {
    imp::read(path).await
}

/// Write `bytes` to `path`, creating any missing parent directories. Overwrites an
/// existing file exactly — no stale tail survives from a longer previous version.
pub async fn write(path: &Path, bytes: &[u8]) -> Result<(), String> {
    imp::write(path, bytes).await
}

/// Whether a local file already exists at `path`. The pull skip-check (a blob whose
/// file is already on disk is not re-downloaded) and the push presence-check both
/// read it; a backend error counts as "absent" so the caller re-attempts rather
/// than skipping.
pub async fn exists(path: &Path) -> bool {
    imp::exists(path).await
}

#[cfg(not(target_arch = "wasm32"))]
mod imp {
    use std::path::Path;

    pub async fn read(path: &Path) -> Result<Vec<u8>, String> {
        tokio::fs::read(path)
            .await
            .map_err(|e| format!("read local blob {}: {e}", path.display()))
    }

    pub async fn write(path: &Path, bytes: &[u8]) -> Result<(), String> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| format!("create parent dir for {}: {e}", path.display()))?;
        }
        tokio::fs::write(path, bytes)
            .await
            .map_err(|e| format!("write local blob {}: {e}", path.display()))
    }

    pub async fn exists(path: &Path) -> bool {
        tokio::fs::try_exists(path).await.unwrap_or(false)
    }
}

#[cfg(target_arch = "wasm32")]
mod imp {
    use std::path::{Component, Path};

    use wasm_bindgen::{JsCast, JsValue};
    use wasm_bindgen_futures::JsFuture;
    use web_sys::{
        FileSystemDirectoryHandle, FileSystemFileHandle, FileSystemGetDirectoryOptions,
        FileSystemGetFileOptions, FileSystemReadWriteOptions, FileSystemSyncAccessHandle,
        WorkerGlobalScope,
    };

    /// Stringify a rejected JS value (a `DOMException` from OPFS, usually) without
    /// dropping its message — `{:?}` alone would print an opaque object.
    fn err_str(e: &JsValue) -> String {
        e.as_string()
            .or_else(|| {
                e.dyn_ref::<js_sys::Error>()
                    .map(|er| String::from(er.message()))
            })
            .unwrap_or_else(|| format!("{e:?}"))
    }

    /// The non-empty path segments, dropping the root and any `.`/`..` — OPFS is a
    /// rooted tree with no parent traversal, so only `Normal` components map to it.
    fn segments(path: &Path) -> Vec<String> {
        path.components()
            .filter_map(|c| match c {
                Component::Normal(s) => s.to_str().map(str::to_owned),
                _ => None,
            })
            .collect()
    }

    /// The OPFS root directory handle. Requires a dedicated Worker: the sync access
    /// handles every read/write needs exist only off the main thread.
    async fn root() -> Result<FileSystemDirectoryHandle, String> {
        let global = js_sys::global()
            .dyn_into::<WorkerGlobalScope>()
            .map_err(|_| {
                "OPFS blob storage requires a dedicated Worker global scope".to_string()
            })?;
        let dir = JsFuture::from(global.navigator().storage().get_directory())
            .await
            .map_err(|e| format!("open OPFS root: {}", err_str(&e)))?;
        dir.dyn_into::<FileSystemDirectoryHandle>()
            .map_err(|_| "OPFS root is not a directory handle".to_string())
    }

    /// Walk `path` to its file handle. With `create`, missing directories and the
    /// file are made along the way; without it, a missing component is an `Err` the
    /// caller maps to not-found.
    async fn file_handle(path: &Path, create: bool) -> Result<FileSystemFileHandle, String> {
        let segs = segments(path);
        let (name, dirs) = segs
            .split_last()
            .ok_or_else(|| format!("empty OPFS path: {}", path.display()))?;

        let mut dir = root().await?;
        let dir_opts = FileSystemGetDirectoryOptions::new();
        dir_opts.set_create(create);
        for d in dirs {
            let next = JsFuture::from(dir.get_directory_handle_with_options(d, &dir_opts))
                .await
                .map_err(|e| {
                    format!(
                        "OPFS directory {d:?} in {}: {}",
                        path.display(),
                        err_str(&e)
                    )
                })?;
            dir = next
                .dyn_into::<FileSystemDirectoryHandle>()
                .map_err(|_| "OPFS directory handle has the wrong type".to_string())?;
        }

        let file_opts = FileSystemGetFileOptions::new();
        file_opts.set_create(create);
        let fh = JsFuture::from(dir.get_file_handle_with_options(name, &file_opts))
            .await
            .map_err(|e| format!("OPFS file {name:?} in {}: {}", path.display(), err_str(&e)))?;
        fh.dyn_into::<FileSystemFileHandle>()
            .map_err(|_| "OPFS file handle has the wrong type".to_string())
    }

    /// A synchronous access handle for `fh`. Holding one locks the file, so every
    /// caller closes it before returning (even on the read/write error path).
    async fn sync_access(fh: &FileSystemFileHandle) -> Result<FileSystemSyncAccessHandle, String> {
        let h = JsFuture::from(fh.create_sync_access_handle())
            .await
            .map_err(|e| format!("OPFS open sync access handle: {}", err_str(&e)))?;
        h.dyn_into::<FileSystemSyncAccessHandle>()
            .map_err(|_| "OPFS sync access handle has the wrong type".to_string())
    }

    /// Read/write options positioned at byte `offset`.
    fn at(offset: f64) -> FileSystemReadWriteOptions {
        let o = FileSystemReadWriteOptions::new();
        o.set_at(offset);
        o
    }

    pub async fn read(path: &Path) -> Result<Vec<u8>, String> {
        let fh = file_handle(path, false).await?;
        let sah = sync_access(&fh).await?;
        let out = read_all(&sah, path);
        sah.close();
        out
    }

    /// Read the whole file through an open handle. Split out so `read` always
    /// `close()`s the handle afterward — an open handle locks the file against the
    /// next open.
    fn read_all(sah: &FileSystemSyncAccessHandle, path: &Path) -> Result<Vec<u8>, String> {
        let size = sah
            .get_size()
            .map_err(|e| format!("OPFS size of {}: {}", path.display(), err_str(&e)))?
            as usize;
        let mut buf = vec![0u8; size];
        let read = sah
            .read_with_u8_array_and_options(&mut buf, &at(0.0))
            .map_err(|e| format!("OPFS read {}: {}", path.display(), err_str(&e)))?
            as usize;
        buf.truncate(read);
        Ok(buf)
    }

    pub async fn write(path: &Path, bytes: &[u8]) -> Result<(), String> {
        let fh = file_handle(path, true).await?;
        let sah = sync_access(&fh).await?;
        let out = write_all(&sah, bytes, path);
        sah.close();
        out
    }

    /// Overwrite the file's whole contents through an open handle, truncating first
    /// so a shorter payload leaves no stale tail. Split out so `write` always
    /// `close()`s the handle afterward.
    fn write_all(
        sah: &FileSystemSyncAccessHandle,
        bytes: &[u8],
        path: &Path,
    ) -> Result<(), String> {
        sah.truncate_with_f64(0.0)
            .map_err(|e| format!("OPFS truncate {}: {}", path.display(), err_str(&e)))?;
        let written = sah
            .write_with_u8_array_and_options(bytes, &at(0.0))
            .map_err(|e| format!("OPFS write {}: {}", path.display(), err_str(&e)))?
            as usize;
        if written != bytes.len() {
            return Err(format!(
                "OPFS short write {}: wrote {written} of {} bytes",
                path.display(),
                bytes.len()
            ));
        }
        sah.flush()
            .map_err(|e| format!("OPFS flush {}: {}", path.display(), err_str(&e)))
    }

    pub async fn exists(path: &Path) -> bool {
        file_handle(path, false).await.is_ok()
    }
}

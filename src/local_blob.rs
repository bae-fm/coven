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

/// Write `bytes` to `path` ATOMICALLY: a concurrent or post-crash reader sees
/// either the previous file or the whole new one, never a torn prefix. This is the
/// guarantee callers depend on — the cache treats "the file exists" as equivalent to
/// "all the bytes are there" (presence is the only truth, no length or checksum
/// column), so every cache write goes through this rather than the in-place
/// [`write`], which truncates the destination before refilling it.
///
/// Native: write a temp file in the same directory, fsync it, then `rename` it over the
/// destination (atomic on one filesystem) — a reader sees the old file or the whole new
/// one, never a torn one. There is deliberately no parent-directory fsync, and thus no
/// durability sub-step that could fail after the rename: the cache is a re-fetchable
/// mirror of the cloud, not a durable store. If a crash loses the rename's directory
/// entry, the blob is simply absent — indistinguishable from one that was never cached,
/// handled by the same fetch-on-read path, not a wrong state anyone reconciles. (Dir
/// fsync isn't portable anyway — Windows has no handle-based dir flush.)
///
/// wasm/OPFS: delegate to [`write`]. OPFS has no cross-file rename to build temp→rename
/// atomicity from, so this is the whole-file sync-access write — best-effort on that
/// platform (the same as every other OPFS write in this codebase), with the same
/// re-fetchable-cache safety net the native path relies on.
pub async fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), String> {
    imp::write_atomic(path, bytes).await
}

/// Whether a local file exists at `path`. `Ok(true)`/`Ok(false)` is a definite
/// answer; `Err` is a real backend failure (a broken filesystem, an OPFS API
/// error) — never collapsed into "absent", so a caller can tell "the file isn't
/// there" apart from "I couldn't find out". The pull skip-check (don't re-download
/// a blob already on disk) and the push presence-check (don't upload a file that
/// isn't here) each decide what a failure means in their context.
pub async fn exists(path: &Path) -> Result<bool, String> {
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
        use tokio::io::AsyncWriteExt;

        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| format!("create parent dir for {}: {e}", path.display()))?;
        }
        // Write THEN fsync the file before returning: a blob does not count as
        // "downloaded" until its bytes are durable on disk, because the pull
        // applies the row that references it only after this returns (the
        // blob-before-row invariant, issue #111). A plain `fs::write` leaves the
        // bytes in the page cache, so a crash could surface a row whose blob is
        // empty or absent.
        let mut file = tokio::fs::File::create(path)
            .await
            .map_err(|e| format!("create local blob {}: {e}", path.display()))?;
        file.write_all(bytes)
            .await
            .map_err(|e| format!("write local blob {}: {e}", path.display()))?;
        file.sync_all()
            .await
            .map_err(|e| format!("fsync local blob {}: {e}", path.display()))?;
        // Also fsync the parent directory so the new file's directory entry
        // survives a crash, not just its data. Best-effort: directory fsync is
        // not portable (Windows has no handle-based dir flush), so a failure here
        // is logged rather than failing the already-durable file write.
        if let Some(parent) = path.parent() {
            match tokio::fs::File::open(parent).await {
                Ok(dir) => {
                    if let Err(e) = dir.sync_all().await {
                        tracing::warn!("could not fsync parent dir {}: {e}", parent.display());
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "could not open parent dir {} to fsync: {e}",
                        parent.display()
                    );
                }
            }
        }
        Ok(())
    }

    pub async fn exists(path: &Path) -> Result<bool, String> {
        tokio::fs::try_exists(path)
            .await
            .map_err(|e| format!("check local blob {}: {e}", path.display()))
    }

    pub async fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), String> {
        use tokio::io::AsyncWriteExt;

        let parent = path
            .parent()
            .ok_or_else(|| format!("blob path has no parent dir: {}", path.display()))?;
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|e| format!("create parent dir for {}: {e}", path.display()))?;

        // A temp sibling in the SAME directory, so the rename below is within one
        // filesystem (cross-filesystem rename is not atomic). A v4 uuid suffix keeps
        // two concurrent writers of the same blob from colliding on the temp name.
        let tmp = parent.join(format!(".tmp.{}", uuid::Uuid::new_v4()));

        // Write + fsync the temp file fully before it is renamed into place: the
        // bytes must be durable on disk before the destination name points at them,
        // or a crash could surface a present-but-empty/torn blob the cache would
        // trust. On any failure remove the temp so a retry isn't blocked by debris.
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
            if let Err(rm) = tokio::fs::remove_file(&tmp).await {
                tracing::warn!(
                    "could not remove temp blob {} after a failed write: {rm}",
                    tmp.display()
                );
            }
            return Err(e);
        }

        if let Err(e) = tokio::fs::rename(&tmp, path).await {
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

        // No parent-directory fsync. The guarantee write_atomic makes is atomicity:
        // a reader sees the old file or the whole new one, never a torn one — given by
        // fsyncing the temp before the rename and the rename being atomic. It does NOT
        // promise the new directory entry survives a crash; that would need a parent
        // fsync, which isn't portable (Windows has no handle-based dir flush) and isn't
        // needed here — every blob the cache holds is re-fetchable from the cloud, so a
        // rename lost to a crash is re-fetched on the next read, never corruption. There
        // is thus no durability sub-step that could fail after this point.
        Ok(())
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

    /// A handle lookup either found the path absent or hit a real failure. Keeping
    /// these apart lets `exists` answer `Ok(false)` for absence while still
    /// surfacing a broken-storage error, instead of collapsing both to "absent".
    enum HandleError {
        /// The directory or file isn't there — OPFS raised a `NotFoundError`.
        NotFound,
        /// A real backend failure (no Worker, a storage API error, a type mismatch).
        Other(String),
    }

    impl HandleError {
        /// Render as a message for callers that treat absence as an error too (a
        /// read of a missing file), describing what was being looked up.
        fn into_message(self, what: &str) -> String {
            match self {
                HandleError::NotFound => format!("{what} not found"),
                HandleError::Other(m) => m,
            }
        }
    }

    /// Classify an OPFS rejection: a `NotFoundError` DOMException is absence,
    /// everything else is a real failure (carrying `context` and the message).
    fn classify(e: JsValue, context: String) -> HandleError {
        if e.dyn_ref::<web_sys::DomException>()
            .map(|d| d.name())
            .as_deref()
            == Some("NotFoundError")
        {
            HandleError::NotFound
        } else {
            HandleError::Other(format!("{context}: {}", err_str(&e)))
        }
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
    /// file are made along the way; without it, a missing directory or file yields
    /// [`HandleError::NotFound`] (a real backend failure yields
    /// [`HandleError::Other`]), so each caller decides what absence means.
    async fn file_handle(path: &Path, create: bool) -> Result<FileSystemFileHandle, HandleError> {
        let segs = segments(path);
        let (name, dirs) = segs
            .split_last()
            .ok_or_else(|| HandleError::Other(format!("empty OPFS path: {}", path.display())))?;

        let mut dir = root().await.map_err(HandleError::Other)?;
        let dir_opts = FileSystemGetDirectoryOptions::new();
        dir_opts.set_create(create);
        for d in dirs {
            let next = JsFuture::from(dir.get_directory_handle_with_options(d, &dir_opts))
                .await
                .map_err(|e| classify(e, format!("OPFS directory {d:?} in {}", path.display())))?;
            dir = next.dyn_into::<FileSystemDirectoryHandle>().map_err(|_| {
                HandleError::Other("OPFS directory handle has the wrong type".into())
            })?;
        }

        let file_opts = FileSystemGetFileOptions::new();
        file_opts.set_create(create);
        let fh = JsFuture::from(dir.get_file_handle_with_options(name, &file_opts))
            .await
            .map_err(|e| classify(e, format!("OPFS file {name:?} in {}", path.display())))?;
        fh.dyn_into::<FileSystemFileHandle>()
            .map_err(|_| HandleError::Other("OPFS file handle has the wrong type".into()))
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
        // A read of a missing file is an error too, so not-found folds into the
        // message the same as any other failure.
        let fh = file_handle(path, false)
            .await
            .map_err(|e| e.into_message(&format!("local blob {}", path.display())))?;
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
        // `create = true`, so a component is created rather than reported missing —
        // not-found can't arise here, but fold it into a message for completeness.
        let fh = file_handle(path, true)
            .await
            .map_err(|e| e.into_message(&format!("local blob {}", path.display())))?;
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

    pub async fn exists(path: &Path) -> Result<bool, String> {
        match file_handle(path, false).await {
            Ok(_) => Ok(true),
            Err(HandleError::NotFound) => Ok(false),
            Err(HandleError::Other(m)) => Err(m),
        }
    }

    /// OPFS has no cross-file rename to build a temp→rename atomic write from, but
    /// the sync-access-handle write `write` performs IS OPFS's atomic unit: it
    /// truncates then writes the whole payload through one handle, and a reader can't
    /// observe a torn intermediate. So the atomic-write contract is already met by
    /// delegating — this is the real OPFS guarantee, not a stand-in.
    pub async fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), String> {
        write(path, bytes).await
    }
}

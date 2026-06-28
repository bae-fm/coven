//! The device-local plaintext-file primitives behind coven's blob cache.
//!
//! coven reads a blob file on push (then encrypts and uploads it) and writes it on
//! pull (after downloading and decrypting); the cache ([`crate::blob::cache`])
//! decides where each file lives (`storage/pinned/<id>` or `storage/cache/<id>`,
//! built from the validated blob id). This module is just the read / write / exists
//! primitives over whatever storage the target platform has.
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

/// Read exactly `len` bytes starting at byte `offset` from the local file at
/// `path`. The cache stores plaintext, so a ranged read of a cached blob seeks and
/// reads the slice straight off disk — no decryption — the local analogue of
/// [`crate::sync::cloud_storage::BlobRangeReader::read`] for a cache hit.
///
/// `Err` if the file is missing, unreadable, or shorter than `offset + len`: the
/// read is exact, never a silent short read. A cached blob's file is the whole
/// plaintext (cache writes are whole-file and atomic), so a caller that has
/// already checked the requested range against the blob's plaintext length never
/// trips the short-file case; if it somehow does, the file is torn and the loud
/// error is correct. `len == 0` returns an empty vec without opening past the
/// seek.
pub async fn read_range(path: &Path, offset: u64, len: u64) -> Result<Vec<u8>, String> {
    imp::read_range(path, offset, len).await
}

/// Write `bytes` to `path`, creating any missing parent directories. Overwrites an
/// existing file exactly — no stale tail survives from a longer previous version.
/// Test-only, wasm: production cache writes go through [`write_atomic`]; only the
/// OPFS round-trip test exercises the in-place write directly.
#[cfg(all(test, target_arch = "wasm32"))]
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

/// Move the file at `from` to `to` within coven's storage. The cache's pin/unpin
/// promote a blob between `storage/cache/<id>` and `storage/pinned/<id>`; the
/// destination's parent directories must already exist (the cache creates them via
/// [`create_dir_all`] first).
///
/// Native: a `tokio::fs::rename`, atomic within one filesystem — a reader never
/// sees the blob in both folders or neither. wasm/OPFS has no cross-directory
/// rename, so this is copy-then-delete and is NOT atomic; see the wasm `imp` for
/// why a transient duplicate is benign for a re-fetchable cache.
pub async fn rename(from: &Path, to: &Path) -> Result<(), String> {
    imp::rename(from, to).await
}

/// Remove the file at `path`. `Ok(true)` if it was there and is now gone,
/// `Ok(false)` if it was already absent — the expected case when a blob lives in
/// only one cache folder, or a sweep races a concurrent delete. `Err` is a real
/// backend failure, never collapsed into "absent", so a caller can tell "nothing
/// to remove" apart from "couldn't remove it".
pub async fn remove_file(path: &Path) -> Result<bool, String> {
    imp::remove_file(path).await
}

/// Remove the directory tree at `path` and everything under it. `Ok(true)` if it
/// was there and is now gone, `Ok(false)` if it was already absent (an empty cache
/// `clear_cache` is asked to drop). `Err` is a real backend failure — a tree the
/// caller asked to clear must actually be gone, never reported clear over a failed
/// delete.
/// Test-only: the production cache sweeps individual files via [`remove_file`];
/// only the test-only `clear_cache` and the OPFS round-trip test drop whole trees.
#[cfg(test)]
pub async fn remove_dir_all(path: &Path) -> Result<bool, String> {
    imp::remove_dir_all(path).await
}

/// Create the directory at `path` and any missing parents. Used before a [`rename`]
/// into a cache folder a blob has never lived in yet (its `{ab}/{cd}` shard).
pub async fn create_dir_all(path: &Path) -> Result<(), String> {
    imp::create_dir_all(path).await
}

/// Enumerate every file in the tree rooted at `dir`, returning `(path, recency,
/// size)` per file — the input to a budget eviction sweep over `storage/cache/`.
/// `recency` is milliseconds since the Unix epoch (native: the file's modification
/// time; wasm: `File.lastModified`), larger meaning more recently written, so the
/// caller evicts the smallest `recency` first. `size` is the file's byte length.
///
/// An absent `dir` is an empty result, not an error: nothing has been cached yet,
/// so there is nothing to measure. A directory or file that vanishes mid-walk (a
/// concurrent `clear_cache`/sweep) is dropped from the result, logged at debug —
/// the one legitimate skip. Every other failure to read a directory or stat a file
/// is surfaced: a cache that cannot be fully measured fails loudly rather than
/// under-counting and silently drifting over budget.
pub async fn walk_files(dir: &Path) -> Result<Vec<(std::path::PathBuf, u64, u64)>, String> {
    imp::walk_files(dir).await
}

#[cfg(not(target_arch = "wasm32"))]
mod imp {
    use std::path::Path;

    pub async fn read(path: &Path) -> Result<Vec<u8>, String> {
        tokio::fs::read(path)
            .await
            .map_err(|e| format!("read local blob {}: {e}", path.display()))
    }

    pub async fn read_range(path: &Path, offset: u64, len: u64) -> Result<Vec<u8>, String> {
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
        // `read_exact` errors if fewer than `len` bytes remain rather than
        // returning a short buffer, so a request past the file's end fails loudly
        // instead of silently truncating the served range.
        let mut buf = vec![0u8; len as usize];
        file.read_exact(&mut buf).await.map_err(|e| {
            format!(
                "read {len} bytes at {offset} from local blob {}: {e}",
                path.display()
            )
        })?;
        Ok(buf)
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

    pub async fn rename(from: &Path, to: &Path) -> Result<(), String> {
        tokio::fs::rename(from, to)
            .await
            .map_err(|e| format!("rename {} -> {}: {e}", from.display(), to.display()))
    }

    pub async fn remove_file(path: &Path) -> Result<bool, String> {
        match tokio::fs::remove_file(path).await {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(format!("remove file {}: {e}", path.display())),
        }
    }

    #[cfg(test)]
    pub async fn remove_dir_all(path: &Path) -> Result<bool, String> {
        match tokio::fs::remove_dir_all(path).await {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(format!("remove dir tree {}: {e}", path.display())),
        }
    }

    pub async fn create_dir_all(path: &Path) -> Result<(), String> {
        tokio::fs::create_dir_all(path)
            .await
            .map_err(|e| format!("create dir tree {}: {e}", path.display()))
    }

    pub async fn walk_files(dir: &Path) -> Result<Vec<(std::path::PathBuf, u64, u64)>, String> {
        // Descend the tree with an explicit stack and collect only leaf files. The
        // cache stores blobs under a two-level shard (`{ab}/{cd}/<id>`), so the walk
        // is shallow but recursive.
        let mut files = Vec::new();
        let mut stack = vec![dir.to_path_buf()];
        while let Some(d) = stack.pop() {
            let mut read_dir = match tokio::fs::read_dir(&d).await {
                Ok(rd) => rd,
                // No dir (the whole tree, or a shard removed mid-walk) — nothing more
                // to measure down this branch. A legitimate skip, logged so it is not
                // silent.
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    tracing::debug!(
                        "walk_files: {} absent, skipping (empty tree or concurrently-removed dir)",
                        d.display()
                    );
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
                    // Vanished between listing and stat (a concurrent clear/sweep) —
                    // it no longer occupies the tree, so it drops out of the measure.
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
                    let recency = mtime_millis(&metadata, &path)?;
                    files.push((path, recency, metadata.len()));
                }
            }
        }
        Ok(files)
    }

    /// Milliseconds since the Unix epoch from a file's modification time — the
    /// recency key eviction sorts on. A pre-epoch mtime (impossible for a file the
    /// cache wrote, but bad data if it occurs) fails loudly rather than wrapping to
    /// a bogus key.
    fn mtime_millis(metadata: &std::fs::Metadata, path: &Path) -> Result<u64, String> {
        let mtime = metadata
            .modified()
            .map_err(|e| format!("modified time of {}: {e}", path.display()))?;
        let millis = mtime
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| {
                format!(
                    "modified time of {} predates the Unix epoch: {e}",
                    path.display()
                )
            })?
            .as_millis();
        Ok(millis as u64)
    }
}

#[cfg(target_arch = "wasm32")]
mod imp {
    use std::path::{Component, Path, PathBuf};

    use wasm_bindgen::{JsCast, JsValue};
    use wasm_bindgen_futures::JsFuture;
    use web_sys::{
        File, FileSystemDirectoryHandle, FileSystemFileHandle, FileSystemGetDirectoryOptions,
        FileSystemGetFileOptions, FileSystemReadWriteOptions, FileSystemRemoveOptions,
        FileSystemSyncAccessHandle, WorkerGlobalScope,
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

    /// Walk a chain of directory `segs` from the OPFS root to the innermost
    /// directory handle. With `create`, missing directories are made along the way;
    /// without it, a missing directory yields [`HandleError::NotFound`] (a real
    /// backend failure yields [`HandleError::Other`]). `path` is carried only for
    /// error messages. The shared directory descent behind [`file_handle`],
    /// [`dir_handle`], and the remove operations — none re-derive it.
    async fn walk_dirs(
        segs: &[String],
        path: &Path,
        create: bool,
    ) -> Result<FileSystemDirectoryHandle, HandleError> {
        let mut dir = root().await.map_err(HandleError::Other)?;
        let dir_opts = FileSystemGetDirectoryOptions::new();
        dir_opts.set_create(create);
        for d in segs {
            let next = JsFuture::from(dir.get_directory_handle_with_options(d, &dir_opts))
                .await
                .map_err(|e| classify(e, format!("OPFS directory {d:?} in {}", path.display())))?;
            dir = next.dyn_into::<FileSystemDirectoryHandle>().map_err(|_| {
                HandleError::Other("OPFS directory handle has the wrong type".into())
            })?;
        }
        Ok(dir)
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

        let dir = walk_dirs(dirs, path, create).await?;

        let file_opts = FileSystemGetFileOptions::new();
        file_opts.set_create(create);
        let fh = JsFuture::from(dir.get_file_handle_with_options(name, &file_opts))
            .await
            .map_err(|e| classify(e, format!("OPFS file {name:?} in {}", path.display())))?;
        fh.dyn_into::<FileSystemFileHandle>()
            .map_err(|_| HandleError::Other("OPFS file handle has the wrong type".into()))
    }

    /// Walk `path` as a chain of directories to its directory handle — the sibling
    /// of [`file_handle`] for a directory target. Used by the tree walk and the
    /// remove operations, which act on a directory rather than a file.
    async fn dir_handle(
        path: &Path,
        create: bool,
    ) -> Result<FileSystemDirectoryHandle, HandleError> {
        walk_dirs(&segments(path), path, create).await
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

    pub async fn read_range(path: &Path, offset: u64, len: u64) -> Result<Vec<u8>, String> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let fh = file_handle(path, false)
            .await
            .map_err(|e| e.into_message(&format!("local blob {}", path.display())))?;
        let sah = sync_access(&fh).await?;
        let out = read_range_at(&sah, offset, len, path);
        sah.close();
        out
    }

    /// Read exactly `len` bytes at `offset` through an open handle. Split out so
    /// `read_range` always `close()`s the handle afterward. A sync-access read
    /// positioned at `offset` returns however many bytes are available there; if
    /// that is short of `len` the file doesn't cover the range, so this errors
    /// rather than returning a truncated slice — matching the native `read_exact`.
    fn read_range_at(
        sah: &FileSystemSyncAccessHandle,
        offset: u64,
        len: u64,
        path: &Path,
    ) -> Result<Vec<u8>, String> {
        let mut buf = vec![0u8; len as usize];
        let read = sah
            .read_with_u8_array_and_options(&mut buf, &at(offset as f64))
            .map_err(|e| {
                format!(
                    "OPFS ranged read {} at {offset}: {}",
                    path.display(),
                    err_str(&e)
                )
            })? as usize;
        if read != len as usize {
            return Err(format!(
                "OPFS short ranged read {}: got {read} of {len} bytes at {offset}",
                path.display()
            ));
        }
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

    /// OPFS has no cross-directory rename, so a move is copy-then-delete: read the
    /// source's whole contents, write them to the destination (creating its parent
    /// directories), then remove the source. Unlike the native `rename` this is NOT
    /// atomic — a crash between the write and the delete leaves the blob in both
    /// places — but every blob the cache moves is re-fetchable from the cloud and a
    /// read checks both folders, so a transient duplicate is benign (it serves the
    /// same bytes from either folder), the same best-effort posture every other OPFS
    /// write here takes.
    pub async fn rename(from: &Path, to: &Path) -> Result<(), String> {
        let from_fh = file_handle(from, false)
            .await
            .map_err(|e| e.into_message(&format!("rename source {}", from.display())))?;
        let sah = sync_access(&from_fh).await?;
        let bytes = read_all(&sah, from);
        sah.close();
        let bytes = bytes?;

        // `write` creates the destination's parent directories and the file, then
        // truncate-writes the whole payload through one sync-access handle.
        write(to, &bytes).await?;

        // The destination now holds the bytes; drop the original. (`remove_file`
        // reports an already-absent source as `Ok(false)`, which the `?` discards —
        // the move still succeeded because the destination has the bytes.)
        remove_file(from).await?;
        Ok(())
    }

    pub async fn remove_file(path: &Path) -> Result<bool, String> {
        remove_entry(path, false).await
    }

    #[cfg(test)]
    pub async fn remove_dir_all(path: &Path) -> Result<bool, String> {
        remove_entry(path, true).await
    }

    /// Remove the entry `path` names from its parent directory, returning whether it
    /// was there (`Ok(true)` removed, `Ok(false)` already absent). `recursive`
    /// removes a non-empty directory tree (OPFS `removeEntry({recursive: true})`).
    /// A missing parent directory means the entry is already gone, so it folds into
    /// `Ok(false)` alongside a `NotFoundError` on the entry itself; every other OPFS
    /// failure is surfaced.
    async fn remove_entry(path: &Path, recursive: bool) -> Result<bool, String> {
        let segs = segments(path);
        let (name, dirs) = match segs.split_last() {
            Some(split) => split,
            None => return Err(format!("empty OPFS path: {}", path.display())),
        };

        // An absent parent directory means the entry can't be there either.
        let dir = match walk_dirs(dirs, path, false).await {
            Ok(dir) => dir,
            Err(HandleError::NotFound) => return Ok(false),
            Err(HandleError::Other(m)) => return Err(m),
        };

        let promise = if recursive {
            let opts = FileSystemRemoveOptions::new();
            opts.set_recursive(true);
            dir.remove_entry_with_options(name, &opts)
        } else {
            dir.remove_entry(name)
        };
        match JsFuture::from(promise).await {
            Ok(_) => Ok(true),
            Err(e) => match classify(e, format!("OPFS remove {}", path.display())) {
                HandleError::NotFound => Ok(false),
                HandleError::Other(m) => Err(m),
            },
        }
    }

    pub async fn create_dir_all(path: &Path) -> Result<(), String> {
        // `create = true`, so every component is made rather than reported missing —
        // not-found can't arise, but fold it into a message for completeness.
        dir_handle(path, true)
            .await
            .map(|_| ())
            .map_err(|e| e.into_message(&format!("OPFS create dir tree {}", path.display())))
    }

    pub async fn walk_files(dir: &Path) -> Result<Vec<(PathBuf, u64, u64)>, String> {
        // An absent root directory means nothing has been cached yet — an empty
        // result, the same posture the native walk takes on a missing dir.
        let start = match dir_handle(dir, false).await {
            Ok(start) => start,
            Err(HandleError::NotFound) => return Ok(Vec::new()),
            Err(HandleError::Other(m)) => return Err(m),
        };

        let mut files = Vec::new();
        // Each stack item carries the directory's reconstructed path (so leaf paths
        // match what the cache's path-builders produce) alongside its handle.
        let mut stack = vec![(dir.to_path_buf(), start)];
        while let Some((dir_path, handle)) = stack.pop() {
            // `entries()` yields `[name, handle]` pairs, async — drive the iterator
            // by awaiting each `next()` until it reports done.
            let iter = handle.entries();
            loop {
                let next = iter
                    .next()
                    .map_err(|e| format!("OPFS iterate {}: {}", dir_path.display(), err_str(&e)))?;
                let next = JsFuture::from(next)
                    .await
                    .map_err(|e| format!("OPFS iterate {}: {}", dir_path.display(), err_str(&e)))?;
                let next: js_sys::IteratorNext = next.unchecked_into();
                if next.done() {
                    break;
                }

                let pair = next.value().dyn_into::<js_sys::Array>().map_err(|_| {
                    format!(
                        "OPFS directory entry of {} is not a [name, handle] pair",
                        dir_path.display()
                    )
                })?;
                let name = pair.get(0).as_string().ok_or_else(|| {
                    format!(
                        "OPFS directory entry name in {} is not a string",
                        dir_path.display()
                    )
                })?;
                let child = dir_path.join(&name);
                let entry = pair.get(1);

                if let Some(subdir) = entry.dyn_ref::<FileSystemDirectoryHandle>() {
                    stack.push((child, subdir.clone()));
                } else if let Ok(fh) = entry.dyn_into::<FileSystemFileHandle>() {
                    // `File.size` and `File.lastModified` give eviction the bytes and
                    // the recency key without opening (and locking) a sync-access
                    // handle. lastModified is milliseconds since the Unix epoch — the
                    // same unit the native mtime path yields.
                    let file = JsFuture::from(fh.get_file()).await.map_err(|e| {
                        format!("OPFS getFile {}: {}", child.display(), err_str(&e))
                    })?;
                    let file = file.dyn_into::<File>().map_err(|_| {
                        format!("OPFS getFile of {} did not return a File", child.display())
                    })?;
                    let size = file.size() as u64;
                    let recency = file.last_modified() as u64;
                    files.push((child, recency, size));
                } else {
                    return Err(format!(
                        "OPFS entry {} is neither a file nor a directory handle",
                        child.display()
                    ));
                }
            }
        }
        Ok(files)
    }
}

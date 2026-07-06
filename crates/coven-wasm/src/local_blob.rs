//! The OPFS backend for coven's device-local plaintext-file primitives.
//!
//! coven reads a blob file on push (then encrypts and uploads it) and writes it on
//! pull (after downloading and decrypting); the cache ([`coven_core::blob::cache`])
//! decides where each file lives (`storage/pinned/<id>` or `storage/cache/<id>`,
//! built from the validated blob id). `coven_core::local_blob` is the public
//! facade; this module registers its browser implementation over the Origin
//! Private File System.
//!
//! wasm has no `std::fs`, so this module uses OPFS through the dedicated DB
//! Worker's *synchronous* access handles — the same stable OPFS API the SQLite
//! VFS runs on. Getting a handle is async (it is awaited here); the read/write
//! through it is synchronous. An absolute coven path like
//! `/coven/images/ab/cd/<id>` maps to nested OPFS directories ending in the file,
//! so the on-disk layout the host names is preserved under the OPFS root.

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use coven_core::local_blob::{PlatformLocalBlobBackend, PlatformPlaintextReader};

static OPFS_LOCAL_BLOB_BACKEND: OpfsLocalBlobBackend = OpfsLocalBlobBackend;

pub fn install_platform_backend() {
    coven_core::local_blob::register_platform_local_blob_backend(&OPFS_LOCAL_BLOB_BACKEND);
}

struct OpfsLocalBlobBackend;

#[async_trait(?Send)]
impl PlatformPlaintextReader for imp::PlaintextReader {
    async fn next_chunk(&mut self, max: usize) -> Result<Vec<u8>, String> {
        imp::PlaintextReader::next_chunk(self, max).await
    }
}

#[async_trait(?Send)]
impl PlatformLocalBlobBackend for OpfsLocalBlobBackend {
    async fn open_reader(&self, path: &Path) -> Result<Box<dyn PlatformPlaintextReader>, String> {
        imp::open_reader(path)
            .await
            .map(|reader| Box::new(reader) as Box<dyn PlatformPlaintextReader>)
    }

    async fn file_len(&self, path: &Path) -> Result<u64, String> {
        imp::file_len(path).await
    }

    async fn copy_atomic(&self, src: &Path, dst: &Path) -> Result<(), String> {
        imp::copy_atomic(src, dst).await
    }

    async fn write_stream_atomic(
        &self,
        path: &Path,
        source: &mut dyn PlatformPlaintextReader,
    ) -> Result<u64, String> {
        imp::write_stream_atomic(path, source).await
    }

    async fn read(&self, path: &Path) -> Result<Vec<u8>, String> {
        imp::read(path).await
    }

    async fn read_range(&self, path: &Path, offset: u64, len: u64) -> Result<Vec<u8>, String> {
        imp::read_range(path, offset, len).await
    }

    async fn write_atomic(&self, path: &Path, bytes: &[u8]) -> Result<(), String> {
        imp::write_atomic(path, bytes).await
    }

    async fn exists(&self, path: &Path) -> Result<bool, String> {
        imp::exists(path).await
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<(), String> {
        imp::rename(from, to).await
    }

    async fn remove_file(&self, path: &Path) -> Result<bool, String> {
        imp::remove_file(path).await
    }

    async fn remove_dir_all(&self, path: &Path) -> Result<bool, String> {
        imp::remove_dir_all(path).await
    }

    async fn create_dir_all(&self, path: &Path) -> Result<(), String> {
        imp::create_dir_all(path).await
    }

    async fn walk_files(&self, dir: &Path) -> Result<Vec<(PathBuf, u64, u64)>, String> {
        imp::walk_files(dir).await
    }
}

#[cfg(target_arch = "wasm32")]
mod imp {
    use std::path::{Component, Path, PathBuf};

    use coven_core::local_blob::PlatformPlaintextReader;
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

    /// wasm incremental reader: a held-open OPFS sync-access handle read at a
    /// running offset. The handle locks the file, so it is closed on drop.
    pub struct PlaintextReader {
        sah: FileSystemSyncAccessHandle,
        offset: u64,
        path: std::path::PathBuf,
    }

    impl Drop for PlaintextReader {
        fn drop(&mut self) {
            self.sah.close();
        }
    }

    impl PlaintextReader {
        pub async fn next_chunk(&mut self, max: usize) -> Result<Vec<u8>, String> {
            let mut buf = vec![0u8; max];
            let mut filled = 0usize;
            while filled < max {
                let read = self
                    .sah
                    .read_with_u8_array_and_options(
                        &mut buf[filled..],
                        &at((self.offset + filled as u64) as f64),
                    )
                    .map_err(|e| {
                        format!(
                            "OPFS streaming read {}: {}",
                            self.path.display(),
                            err_str(&e)
                        )
                    })? as usize;
                if read == 0 {
                    break;
                }
                filled += read;
            }
            self.offset += filled as u64;
            buf.truncate(filled);
            Ok(buf)
        }
    }

    pub async fn open_reader(path: &Path) -> Result<PlaintextReader, String> {
        let fh = file_handle(path, false)
            .await
            .map_err(|e| e.into_message(&format!("local blob {}", path.display())))?;
        let sah = sync_access(&fh).await?;
        Ok(PlaintextReader {
            sah,
            offset: 0,
            path: path.to_path_buf(),
        })
    }

    pub async fn file_len(path: &Path) -> Result<u64, String> {
        let fh = file_handle(path, false)
            .await
            .map_err(|e| e.into_message(&format!("local blob {}", path.display())))?;
        let sah = sync_access(&fh).await?;
        let size = sah
            .get_size()
            .map_err(|e| format!("OPFS size of {}: {}", path.display(), err_str(&e)));
        sah.close();
        size.map(|s| s as u64)
    }

    pub async fn copy_atomic(src: &Path, dst: &Path) -> Result<(), String> {
        let mut source = open_reader(src).await?;
        write_stream_atomic(dst, &mut source).await.map(|_| ())
    }

    pub async fn write_stream_atomic(
        path: &Path,
        source: &mut dyn PlatformPlaintextReader,
    ) -> Result<u64, String> {
        let fh = file_handle(path, true)
            .await
            .map_err(|e| e.into_message(&format!("local blob {}", path.display())))?;
        let sah = sync_access(&fh).await?;
        let result = write_stream(&sah, source, path).await;
        sah.close();
        match result {
            Ok(written) => Ok(written),
            Err(e) => {
                if let Err(cleanup) = remove_file(path).await {
                    tracing::warn!(
                        "failed to remove partial OPFS blob {} after streamed write failure: {cleanup}",
                        path.display()
                    );
                }
                Err(e)
            }
        }
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
    /// rather than returning a truncated slice.
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

    async fn write_stream(
        sah: &FileSystemSyncAccessHandle,
        source: &mut dyn PlatformPlaintextReader,
        path: &Path,
    ) -> Result<u64, String> {
        sah.truncate_with_f64(0.0)
            .map_err(|e| format!("OPFS truncate {}: {}", path.display(), err_str(&e)))?;
        let mut offset = 0u64;
        loop {
            let chunk = source.next_chunk(1 << 20).await?;
            if chunk.is_empty() {
                break;
            }
            let written = sah
                .write_with_u8_array_and_options(&chunk, &at(offset as f64))
                .map_err(|e| format!("OPFS write {}: {}", path.display(), err_str(&e)))?
                as usize;
            if written != chunk.len() {
                return Err(format!(
                    "OPFS short write {}: wrote {written} of {} bytes",
                    path.display(),
                    chunk.len()
                ));
            }
            offset += written as u64;
        }
        sah.flush()
            .map_err(|e| format!("OPFS flush {}: {}", path.display(), err_str(&e)))?;
        Ok(offset)
    }

    pub async fn exists(path: &Path) -> Result<bool, String> {
        match file_handle(path, false).await {
            Ok(_) => Ok(true),
            Err(HandleError::NotFound) => Ok(false),
            Err(HandleError::Other(m)) => Err(m),
        }
    }

    /// OPFS has no cross-file rename to build a temp→rename write from, so the
    /// wasm backend uses the same sync-access-handle write path as [`write`].
    pub async fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), String> {
        write(path, bytes).await
    }

    /// OPFS has no cross-directory rename, so a move is copy-then-delete. This is
    /// NOT atomic — a crash between the write and the delete leaves the blob in
    /// both places — but every blob the cache moves is re-fetchable from the cloud
    /// and a read checks both folders, so a transient duplicate is benign because
    /// either folder serves the same bytes.
    pub async fn rename(from: &Path, to: &Path) -> Result<(), String> {
        copy_atomic(from, to).await?;

        // The destination now holds the bytes; drop the original. (`remove_file`
        // reports an already-absent source as `Ok(false)`, which the `?` discards —
        // the move still succeeded because the destination has the bytes.)
        remove_file(from).await?;
        Ok(())
    }

    pub async fn remove_file(path: &Path) -> Result<bool, String> {
        remove_entry(path, false).await
    }

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
        // result.
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
                    // same unit the public API returns.
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

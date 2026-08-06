use std::ops::Deref;
use std::path::{Path, PathBuf};
use tracing::{debug, info};

use crate::config::{Config, ConfigError};

/// Why a string is not a safe path token.
///
/// An untrusted string becomes a path component in several places: a blob's
/// `id`/`namespace` (interpolated into its on-disk file path and cloud object
/// key), and a `store_id`/`sid` from an unsigned invite or restore code (the
/// name of a directory under `stores/`). All arrive from outside — an incoming
/// changeset authored by any write-capable member, or a pasted code anyone can
/// craft — so an unconstrained one could climb out of the directory it is joined
/// onto (`..`, a path separator, an absolute leading slash) and make a pulling or
/// joining device read, write, or recursively delete an arbitrary location, or —
/// too short / not aligned to a char boundary — crash a blob's partition-prefix
/// slice. A string that trips any of these is bad data, refused before a path is
/// built or used.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathTokenError {
    /// The token is empty — no file name to write, no key to form.
    Empty,
    /// The token contains a path separator (`/` or `\`), so joining it onto a
    /// directory would descend into (or, with a leading separator, replace) the
    /// path rather than name a single child.
    Separator,
    /// The token is exactly `..`, which names the parent of the directory it is
    /// joined onto rather than a child. A trailing `..` component is normalized
    /// away when the path is resolved, so the join lands on the parent.
    ParentDir,
    /// The token is exactly `.`, which names the directory it is joined onto
    /// itself rather than a child. Like `..`, a trailing `.` component is
    /// normalized away, so `stores/.` resolves to `stores`'s parent (the
    /// data dir) — an escape just as `..` is.
    CurDir,
    /// The token contains a NUL byte, which truncates the path at the OS boundary.
    NulByte,
    /// The token contains a `:`, which on Windows names an alternate data stream
    /// (`file:stream`) or a drive-relative reference (`c:dir`) rather than a child.
    Colon,
    /// The dash-stripped id is too short, or splits a multi-byte char, to take the
    /// two leading byte-pairs the `{ab}/{cd}` partition prefix needs.
    Unindexable,
}

#[derive(Debug)]
pub enum RequiredLocalBlobPathError {
    Path(PathTokenError),
    Missing { namespace: String, id: String },
    Io(String),
}

#[derive(Debug)]
pub enum CachedLocatorRemovalError {
    Path(PathTokenError),
    Io(String),
}

#[derive(Debug)]
pub enum LocalBlobRemovalError {
    Path(PathTokenError),
    Io(String),
}

#[derive(Debug)]
pub enum LocalBlobStoreError {
    Path(PathTokenError),
    Io(String),
}

impl std::fmt::Display for LocalBlobStoreError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Path(error) => write!(formatter, "local blob path: {error}"),
            Self::Io(error) => write!(formatter, "local blob file: {error}"),
        }
    }
}

impl std::error::Error for LocalBlobStoreError {}

impl From<PathTokenError> for LocalBlobStoreError {
    fn from(error: PathTokenError) -> Self {
        Self::Path(error)
    }
}

#[derive(Debug)]
pub enum StoreBlobFileError {
    Path(PathTokenError),
    Io(String),
    Integrity {
        path: PathBuf,
        expected_size: u64,
        actual_size: u64,
        expected_hash: crate::object_hash::ObjectHash,
        actual_hash: crate::object_hash::ObjectHash,
    },
}

pub struct CachedBlobFile {
    path: PathBuf,
    recency: u64,
    size: u64,
}

impl CachedBlobFile {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn recency(&self) -> u64 {
        self.recency
    }

    pub fn size(&self) -> u64 {
        self.size
    }
}

impl std::fmt::Display for LocalBlobRemovalError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Path(error) => write!(formatter, "local blob path: {error}"),
            Self::Io(error) => write!(formatter, "local blob file: {error}"),
        }
    }
}

impl std::fmt::Display for CachedLocatorRemovalError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Path(error) => write!(formatter, "blob cache path: {error}"),
            Self::Io(error) => write!(formatter, "blob cache file: {error}"),
        }
    }
}

impl std::fmt::Display for StoreBlobFileError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Path(error) => write!(formatter, "store blob path: {error}"),
            Self::Io(error) => write!(formatter, "store blob file: {error}"),
            Self::Integrity {
                path,
                expected_size,
                actual_size,
                expected_hash,
                actual_hash,
            } => write!(
                formatter,
                "store blob {} has size/hash {actual_size}/{actual_hash}, expected {expected_size}/{expected_hash}",
                path.display()
            ),
        }
    }
}

impl From<PathTokenError> for StoreBlobFileError {
    fn from(error: PathTokenError) -> Self {
        Self::Path(error)
    }
}

impl std::fmt::Display for PathTokenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PathTokenError::Empty => write!(f, "path token is empty"),
            PathTokenError::Separator => write!(f, "path token contains a path separator"),
            PathTokenError::ParentDir => write!(f, "path token contains a parent reference"),
            PathTokenError::CurDir => write!(f, "path token is a current-directory reference"),
            PathTokenError::NulByte => write!(f, "path token contains a NUL byte"),
            PathTokenError::Colon => write!(f, "path token contains a colon"),
            PathTokenError::Unindexable => {
                write!(
                    f,
                    "id is too short or misaligned to form a partition prefix"
                )
            }
        }
    }
}

impl std::error::Error for PathTokenError {}

/// Reject a single untrusted path token (a blob `id`/`namespace`, or a
/// `store_id`/`sid`) that could escape the directory it is joined onto. A safe
/// token names exactly one child: no separator, no `..`, no `.`, no NUL, no `:` (a
/// Windows stream/drive reference), non-empty. The single gate every path builder
/// and every code decoder runs an untrusted token through, so traversal is refused
/// before any on-disk or cloud path is formed — and a decoded id is a safe single
/// component by the time any consumer joins it onto a directory.
///
/// Both `.` and `..` are refused: each is a directory-relative reference that a
/// trailing path component normalizes away, so joining either onto `dir` resolves
/// to `dir` itself or its parent rather than to a child of `dir`.
pub fn validate_path_token(token: &str) -> Result<(), PathTokenError> {
    if token.is_empty() {
        return Err(PathTokenError::Empty);
    }
    if token.contains('\0') {
        return Err(PathTokenError::NulByte);
    }
    if token.contains('/') || token.contains('\\') {
        return Err(PathTokenError::Separator);
    }
    if token.contains(':') {
        return Err(PathTokenError::Colon);
    }
    if token == ".." {
        return Err(PathTokenError::ParentDir);
    }
    if token == "." {
        return Err(PathTokenError::CurDir);
    }
    Ok(())
}

/// Reject an untrusted `cloud_path` (the consumer's readable object key under the
/// plain scheme, e.g. `"Artist - Album/cover.jpg"`) that could escape its
/// namespace prefix in the bucket. Unlike a path token, an interior `/` is
/// legitimate — the readable path is nested — but every segment still has to be
/// a canonical path token. Empty, `.`, `..`, colon/platform-prefix, backslash,
/// and NUL forms are refused before an object key is built. The `cloud_path`
/// never feeds a local file path, only the cloud object key, so this guards the
/// keyspace, not the disk.
pub fn validate_cloud_path(cloud_path: &str) -> Result<(), PathTokenError> {
    if cloud_path.starts_with('/') {
        return Err(PathTokenError::Separator);
    }
    for segment in cloud_path.split('/') {
        validate_path_token(segment)?;
    }
    Ok(())
}

/// Default name of the parent directory a store lives under — overridden
/// per host via [`StoreLayout::stores_dirname`].
const DEFAULT_STORES_DIRNAME: &str = "stores";
/// Default name of a store's own database file — overridden per host via
/// [`StoreLayout::db_filename`].
const DEFAULT_DB_FILENAME: &str = "store.db";

/// The host's on-disk layout for stores: where they live and what the
/// database file is called. One rule shared by create, open, join, and
/// restore, so a host that wants `libraries/<id>/library.db` instead of
/// coven's default `stores/<id>/store.db` names it once here rather than
/// each flow hardwiring (or working around) coven's own choice.
#[derive(Clone, Debug)]
pub struct StoreLayout {
    app_dir: PathBuf,
    stores_dirname: String,
    db_filename: String,
}

impl StoreLayout {
    pub fn new(app_dir: impl Into<PathBuf>) -> Self {
        Self {
            app_dir: app_dir.into(),
            stores_dirname: DEFAULT_STORES_DIRNAME.to_string(),
            db_filename: DEFAULT_DB_FILENAME.to_string(),
        }
    }

    pub fn stores_dirname(mut self, name: impl Into<String>) -> Self {
        self.stores_dirname = name.into();
        self
    }

    pub fn db_filename(mut self, name: impl Into<String>) -> Self {
        self.db_filename = name.into();
        self
    }

    /// The stores parent dir (for host listing/discovery).
    pub fn stores_root(&self) -> PathBuf {
        self.app_dir.join(&self.stores_dirname)
    }

    /// The one `(app_dir, store_id) -> StoreDir` rule, named with this
    /// layout's directory and database filename. Callers validate an
    /// untrusted `store_id` ([`validate_path_token`]) BEFORE calling, as
    /// every join/restore/create flow already does.
    pub fn store_dir(&self, store_id: &str) -> StoreDir {
        StoreDir {
            path: self.stores_root().join(store_id),
            db_filename: self.db_filename.clone(),
        }
    }

    /// Create a store directory, generate a device id, and save `config.yaml`.
    ///
    /// The caller is responsible for encryption-key setup and calling
    /// [`Config::save_to_config_yaml`](crate::config::Config::save_to_config_yaml)
    /// afterward.
    pub fn create_store(
        &self,
        store_id: String,
        store_name: String,
        ids: &dyn crate::id_provider::IdProvider,
    ) -> Result<Config, ConfigError> {
        validate_path_token(&store_id)
            .map_err(|error| ConfigError::Config(format!("invalid store id: {error}")))?;
        let store_dir = self.store_dir(&store_id);
        if store_dir.exists() {
            return Err(ConfigError::StoreExists(store_id));
        }
        std::fs::create_dir_all(&*store_dir)?;

        let device_id = ids.new_id();
        let config = Config::with_defaults(store_id, device_id, store_dir, store_name);
        config.save_to_config_yaml()?;

        info!("Created store at {}", config.store_dir.display());
        Ok(config)
    }
}

/// Typed wrapper for a store directory path.
///
/// Centralizes the on-disk layout so callers use methods instead of
/// ad-hoc `path.join("images")` etc.
#[derive(Clone, Debug, PartialEq)]
pub struct StoreDir {
    path: PathBuf,
    db_filename: String,
}

impl StoreDir {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            db_filename: DEFAULT_DB_FILENAME.to_string(),
        }
    }

    pub fn db_path(&self) -> PathBuf {
        self.path.join(&self.db_filename)
    }

    pub fn config_path(&self) -> PathBuf {
        self.path.join("config.yaml")
    }

    /// The two-level partition shard for `id`: `{ab}/{cd}/{id}`, where `{ab}`/`{cd}`
    /// are the first two byte-pairs of the dash-stripped id. The single home for the
    /// partition scheme — every blob path (cloud key and on-disk file, hashed or
    /// pinned/cache) is this shard under some root.
    ///
    /// `id` is validated as a single path token and must be long enough (and
    /// char-boundary aligned) to take the two leading byte-pairs. An id that fails
    /// is bad data — it could escape the directory or crash the slice — so this
    /// returns [`PathTokenError`] rather than interpolating it or panicking; the
    /// caller refuses the blob.
    pub(crate) fn id_shard(id: &str) -> Result<String, PathTokenError> {
        validate_path_token(id)?;
        let hex = id.replace('-', "");
        if !(hex.is_char_boundary(2) && hex.is_char_boundary(4)) {
            return Err(PathTokenError::Unindexable);
        }
        Ok(format!("{}/{}/{id}", &hex[..2], &hex[2..4]))
    }

    /// Content-addressed relative path `{prefix}/{ab}/{cd}/{id}`, partitioning by
    /// the first two byte-pairs of the dash-stripped id. The single home for the
    /// partition scheme — shared by the local blob store and the cloud layout.
    ///
    /// Both `prefix` and `id` are validated as single path tokens, and the id must
    /// be long enough (and char-boundary aligned) to take the two leading
    /// byte-pairs the prefix needs. An id that fails is bad data — it could escape
    /// the directory or crash the slice — so this returns [`PathTokenError`] rather
    /// than interpolating it or panicking; the caller refuses the blob.
    pub fn hashed_path(prefix: &str, id: &str) -> Result<String, PathTokenError> {
        validate_path_token(prefix)?;
        Ok(format!("{prefix}/{}", Self::id_shard(id)?))
    }

    /// The cloud object key for a Hashed-scheme blob under the device that
    /// uploaded it: `{namespace}/{uploader}/{ab}/{cd}/{id}`. The `{uploader}`
    /// segment is what aligns the blob keyspace to the storage-access rule (a
    /// member writes only under its own public key), so a bucket ACL can scope each
    /// member to `{namespace}/{self}/`. Only the *cloud* key carries it; the local
    /// cache keeps the un-prefixed `{namespace}/{ab}/{cd}/{id}` layout because it is
    /// per-device. `namespace` and `uploader` are validated as single path tokens;
    /// the id must be indexable (see `id_shard`).
    pub fn uploader_hashed_key(
        namespace: &str,
        uploader: &str,
        id: &str,
    ) -> Result<String, PathTokenError> {
        validate_path_token(namespace)?;
        validate_path_token(uploader)?;
        Ok(format!("{namespace}/{uploader}/{}", Self::id_shard(id)?))
    }

    pub fn storage_dir(&self) -> PathBuf {
        self.path.join("storage")
    }

    /// Immutable stored bytes prepared for one blob locator. The locator hash is
    /// the file name, so retries reopen the same exact spool rather than sealing
    /// the plaintext again with fresh randomness.
    pub fn outbound_blob_spool_path(
        &self,
        locator_hash: crate::object_hash::ObjectHash,
    ) -> PathBuf {
        self.storage_dir()
            .join("outbound-blobs")
            .join(locator_hash.to_string())
    }

    pub async fn remove_outbound_blob_spool(
        &self,
        locator_hash: crate::object_hash::ObjectHash,
    ) -> Result<(), String> {
        let path = self.outbound_blob_spool_path(locator_hash);
        match tokio::fs::remove_file(&path).await {
            Ok(()) => crate::atomic_file::sync_parent_dir(&path).await,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(format!(
                "remove exact blob spool {}: {error}",
                path.display()
            )),
        }
    }

    /// A kept (budget-exempt) cache copy of a **Remote** blob:
    /// `storage/pinned/<namespace>/{ab}/{cd}/<locator-hash>`. The kept sibling of
    /// [`Self::cache_blob_path`] — same per-namespace shard layout, in the `pinned`
    /// folder instead of `cache`. The cache's truth is the folder a blob's file lives
    /// in, not a table; a file here is a Remote blob's cache copy the user pinned for
    /// offline (kept from eviction). `Err` if `namespace` is unsafe.
    pub fn pinned_blob_path(
        &self,
        namespace: &str,
        locator_hash: crate::object_hash::ObjectHash,
    ) -> Result<PathBuf, PathTokenError> {
        self.cache_folder_blob_path("pinned", namespace, &locator_hash.to_string())
    }

    pub async fn populate_pinned_blob_from_file(
        &self,
        namespace: &str,
        locator_hash: crate::object_hash::ObjectHash,
        expected_size: u64,
        expected_hash: crate::object_hash::ObjectHash,
        source: &Path,
    ) -> Result<(), StoreBlobFileError> {
        let destination = self
            .pinned_blob_path(namespace, locator_hash)
            .map_err(StoreBlobFileError::Path)?;
        self.populate_exact_blob_from_file(destination, expected_size, expected_hash, source)
            .await
    }

    pub async fn populate_cached_blob_from_file(
        &self,
        namespace: &str,
        locator_hash: crate::object_hash::ObjectHash,
        expected_size: u64,
        expected_hash: crate::object_hash::ObjectHash,
        source: &Path,
    ) -> Result<PathBuf, StoreBlobFileError> {
        let destination = self
            .cache_blob_path(namespace, locator_hash)
            .map_err(StoreBlobFileError::Path)?;
        self.populate_exact_blob_from_file(
            destination.clone(),
            expected_size,
            expected_hash,
            source,
        )
        .await?;
        Ok(destination)
    }

    async fn populate_exact_blob_from_file(
        &self,
        destination: PathBuf,
        expected_size: u64,
        expected_hash: crate::object_hash::ObjectHash,
        source: &Path,
    ) -> Result<(), StoreBlobFileError> {
        let staged = crate::local_file::AtomicStagedFile::create(&destination)
            .await
            .map_err(StoreBlobFileError::Io)?;
        let (staged, actual_size, actual_digest) = staged
            .copy_from(source)
            .await
            .map_err(StoreBlobFileError::Io)?;
        let actual_hash = crate::object_hash::ObjectHash::from_digest(actual_digest);
        if actual_size != expected_size || actual_hash != expected_hash {
            return Err(StoreBlobFileError::Integrity {
                path: source.to_path_buf(),
                expected_size,
                actual_size,
                expected_hash,
                actual_hash,
            });
        }
        match staged.commit_new().await {
            Ok(()) => Ok(()),
            Err(crate::local_file::CommitNewFileError::DestinationExists(path)) => {
                let (actual_size, actual_hash) = exact_file_facts(&path)
                    .await
                    .map_err(StoreBlobFileError::Io)?;
                if actual_size == expected_size && actual_hash == expected_hash {
                    Ok(())
                } else {
                    Err(StoreBlobFileError::Integrity {
                        path,
                        expected_size,
                        actual_size,
                        expected_hash,
                        actual_hash,
                    })
                }
            }
            Err(error) => Err(StoreBlobFileError::Io(error.to_string())),
        }
    }

    pub async fn pinned_blob_is_exact(
        &self,
        namespace: &str,
        locator_hash: crate::object_hash::ObjectHash,
        expected_size: u64,
        expected_hash: crate::object_hash::ObjectHash,
    ) -> Result<bool, StoreBlobFileError> {
        let path = self
            .pinned_blob_path(namespace, locator_hash)
            .map_err(StoreBlobFileError::Path)?;
        match file_exists(&path).await {
            Ok(false) => Ok(false),
            Err(error) => Err(StoreBlobFileError::Io(error)),
            Ok(true) => {
                let (actual_size, actual_hash) = exact_file_facts(&path)
                    .await
                    .map_err(StoreBlobFileError::Io)?;
                if actual_size == expected_size && actual_hash == expected_hash {
                    Ok(true)
                } else {
                    Err(StoreBlobFileError::Integrity {
                        path,
                        expected_size,
                        actual_size,
                        expected_hash,
                        actual_hash,
                    })
                }
            }
        }
    }

    pub async fn remote_blob_is_exact(
        &self,
        namespace: &str,
        locator_hash: crate::object_hash::ObjectHash,
        expected_size: u64,
        expected_hash: crate::object_hash::ObjectHash,
    ) -> Result<bool, StoreBlobFileError> {
        for path in [
            self.pinned_blob_path(namespace, locator_hash)?,
            self.cache_blob_path(namespace, locator_hash)?,
        ] {
            if file_is_exact(&path, expected_size, expected_hash).await? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub async fn cached_blob_is_exact(
        &self,
        namespace: &str,
        locator_hash: crate::object_hash::ObjectHash,
        expected_size: u64,
        expected_hash: crate::object_hash::ObjectHash,
    ) -> Result<bool, StoreBlobFileError> {
        let path = self.cache_blob_path(namespace, locator_hash)?;
        file_is_exact(&path, expected_size, expected_hash).await
    }

    /// An opportunistic (evictable) cache copy of a **Remote** blob:
    /// `storage/cache/<namespace>/{ab}/{cd}/<locator-hash>`. A file here is a cached-but-unpinned
    /// blob — fetched on read or eagerly on pull, droppable by the budget sweep. The
    /// folder it lives in, not a table, is what makes it evictable rather than kept.
    /// Segmented by `namespace` so each namespace's budget evicts only its own
    /// subtree (see [`Self::cache_namespace_dir`]). `Err` if
    /// `namespace` is unsafe.
    pub fn cache_blob_path(
        &self,
        namespace: &str,
        locator_hash: crate::object_hash::ObjectHash,
    ) -> Result<PathBuf, PathTokenError> {
        self.cache_folder_blob_path("cache", namespace, &locator_hash.to_string())
    }

    pub fn remote_blob_paths(
        &self,
        namespace: &str,
        locator_hash: crate::object_hash::ObjectHash,
    ) -> Result<(PathBuf, PathBuf), PathTokenError> {
        Ok((
            self.pinned_blob_path(namespace, locator_hash)?,
            self.cache_blob_path(namespace, locator_hash)?,
        ))
    }

    pub async fn remove_cached_locator(
        &self,
        namespace: &str,
        locator_hash: crate::object_hash::ObjectHash,
    ) -> Result<(), CachedLocatorRemovalError> {
        for path in [
            self.pinned_blob_path(namespace, locator_hash)
                .map_err(CachedLocatorRemovalError::Path)?,
            self.cache_blob_path(namespace, locator_hash)
                .map_err(CachedLocatorRemovalError::Path)?,
        ] {
            remove_file(&path)
                .await
                .map_err(CachedLocatorRemovalError::Io)?;
        }
        Ok(())
    }

    /// `storage/<folder>/<namespace>/{ab}/{cd}/<locator-hash>` — the single blob-path builder
    /// behind [`Self::cache_blob_path`] (`folder` = `cache`) and
    /// [`Self::pinned_blob_path`] (`folder` = `pinned`), which differ only by the
    /// folder token. Composes the per-namespace dir
    /// ([`Self::cache_folder_namespace_dir`]) with the locator-hash shard, so the layout lives
    /// in one place. `namespace` and the locator hash are validated.
    fn cache_folder_blob_path(
        &self,
        folder: &str,
        namespace: &str,
        id: &str,
    ) -> Result<PathBuf, PathTokenError> {
        Ok(self
            .cache_folder_namespace_dir(folder, namespace)?
            .join(Self::id_shard(id)?))
    }

    /// `storage/<folder>/<namespace>` for a cache folder (`cache` evictable / `pinned`
    /// kept), `namespace` validated as a single path token. The per-namespace dir both
    /// cache folders compose onto; [`Self::cache_namespace_dir`] is the evictable case
    /// the budget sweep walks.
    fn cache_folder_namespace_dir(
        &self,
        folder: &str,
        namespace: &str,
    ) -> Result<PathBuf, PathTokenError> {
        validate_path_token(namespace)?;
        Ok(self.storage_dir().join(folder).join(namespace))
    }

    /// coven's own copy of a **host-provided Local** blob:
    /// `storage/local/<namespace>/<id>`. This is NOT a cache copy — it is the blob's
    /// home while its release is Local (a host-provided blob has no user path). It
    /// is never evicted: the budget sweep walks only [`Self::cache_dir`], never
    /// `storage/local`. Both `namespace` and `id` are validated as single path
    /// tokens (the blob columns come from a row any write-capable member authored),
    /// so neither can escape the store. `Err` if either is unsafe.
    pub fn local_blob_path(&self, namespace: &str, id: &str) -> Result<PathBuf, PathTokenError> {
        validate_path_token(namespace)?;
        validate_path_token(id)?;
        Ok(self
            .path
            .join("storage")
            .join("local")
            .join(namespace)
            .join(id))
    }

    pub async fn require_local_blob_path(
        &self,
        namespace: &str,
        id: &str,
    ) -> Result<PathBuf, RequiredLocalBlobPathError> {
        let path = self
            .local_blob_path(namespace, id)
            .map_err(RequiredLocalBlobPathError::Path)?;
        match file_exists(&path).await {
            Ok(true) => Ok(path),
            Ok(false) => Err(RequiredLocalBlobPathError::Missing {
                namespace: namespace.to_string(),
                id: id.to_string(),
            }),
            Err(error) => Err(RequiredLocalBlobPathError::Io(error)),
        }
    }

    pub async fn local_blob_path_if_present(
        &self,
        namespace: &str,
        id: &str,
        expected_size: u64,
    ) -> Result<Option<PathBuf>, LocalBlobStoreError> {
        let path = self.local_blob_path(namespace, id)?;
        if !file_exists(&path).await.map_err(LocalBlobStoreError::Io)? {
            return Ok(None);
        }
        let actual_size = tokio::fs::metadata(&path)
            .await
            .map_err(|error| {
                LocalBlobStoreError::Io(format!("stat local blob {}: {error}", path.display()))
            })?
            .len();
        if actual_size != expected_size {
            return Err(LocalBlobStoreError::Io(format!(
                "local blob {} has {actual_size} bytes, expected {expected_size}",
                path.display()
            )));
        }
        Ok(Some(path))
    }

    pub async fn remove_local_blob(
        &self,
        namespace: &str,
        id: &str,
    ) -> Result<bool, LocalBlobRemovalError> {
        let path = self
            .local_blob_path(namespace, id)
            .map_err(LocalBlobRemovalError::Path)?;
        remove_file(&path).await.map_err(LocalBlobRemovalError::Io)
    }

    /// The evictable-cache root, `storage/cache`, holding every namespace's subtree.
    /// The per-namespace budget sweep walks only one namespace's subtree under it —
    /// see [`Self::cache_namespace_dir`].
    pub fn cache_dir(&self) -> PathBuf {
        self.storage_dir().join("cache")
    }

    /// One namespace's evictable-cache subtree, `storage/cache/<namespace>`. The
    /// cache budget enforcement walks only this tree, so
    /// a namespace evicts against its own budget without touching another namespace's
    /// files. `namespace` is validated as a single path token; `Err` if it is unsafe.
    pub fn cache_namespace_dir(&self, namespace: &str) -> Result<PathBuf, PathTokenError> {
        self.cache_folder_namespace_dir("cache", namespace)
    }

    pub async fn cached_blob_files(
        &self,
        namespace: &str,
    ) -> Result<Vec<CachedBlobFile>, StoreBlobFileError> {
        let directory = self
            .cache_namespace_dir(namespace)
            .map_err(StoreBlobFileError::Path)?;
        walk_files(&directory)
            .await
            .map_err(StoreBlobFileError::Io)
            .map(|files| {
                files
                    .into_iter()
                    .map(|(path, recency, size)| CachedBlobFile {
                        path,
                        recency,
                        size,
                    })
                    .collect()
            })
    }

    pub async fn remove_cached_blob_file(
        &self,
        file: &CachedBlobFile,
    ) -> Result<bool, StoreBlobFileError> {
        remove_file(file.path())
            .await
            .map_err(StoreBlobFileError::Io)
    }

    /// Remove unpublished local, cached, and pinned blob files left by an
    /// earlier process. Files created at or after `process_start` belong to the
    /// current process and are left untouched.
    pub fn remove_orphaned_blob_temps(
        &self,
        process_start: std::time::SystemTime,
    ) -> std::io::Result<()> {
        let storage = self.storage_dir();
        for folder in ["local", "cache", "pinned"] {
            self.remove_orphaned_temps_in_dir(&storage.join(folder), process_start)?;
        }
        Ok(())
    }

    fn remove_orphaned_temps_in_dir(
        &self,
        dir: &Path,
        process_start: std::time::SystemTime,
    ) -> std::io::Result<()> {
        let entries = match std::fs::read_dir(dir) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                debug!(
                    path = %dir.display(),
                    store_dir = %self.display(),
                    "blob directory absent during orphaned temp cleanup"
                );
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                self.remove_orphaned_temps_in_dir(&path, process_start)?;
            } else if file_type.is_file()
                && crate::local_file::AtomicStagedFile::is_staging_path(&path)
            {
                let modified = entry.metadata()?.modified()?;
                if modified >= process_start {
                    debug!(
                        path = %path.display(),
                        "leaving fresh blob temp created at or after process start"
                    );
                    continue;
                }
                match std::fs::remove_file(&path) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        debug!(
                            path = %path.display(),
                            "file already absent during local blob cleanup"
                        );
                    }
                    Err(error) => return Err(error),
                }
            } else if file_type.is_file()
                && path.file_name().and_then(|name| name.to_str()).is_none()
            {
                debug!(
                    path = %path.display(),
                    "skipping blob path with non-utf8 file name during orphaned temp cleanup"
                );
            }
        }
        Ok(())
    }

    /// Create the store directory tree if it is absent.
    pub fn ensure_created(&self) -> std::io::Result<()> {
        std::fs::create_dir_all(&self.path)
    }

    /// Remove the complete store directory tree. Absence is success: the tree
    /// is already gone.
    pub fn remove_tree(&self) -> std::io::Result<()> {
        match std::fs::remove_dir_all(&self.path) {
            Err(error) if error.kind() != std::io::ErrorKind::NotFound => Err(error),
            _ => Ok(()),
        }
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn store_local_blob(
        &self,
        namespace: &str,
        id: &str,
        bytes: &[u8],
    ) -> Result<(), LocalBlobStoreError> {
        let destination = self.local_blob_path(namespace, id)?;
        let mut staged = crate::local_file::AtomicStagedFile::create(&destination)
            .await
            .map_err(LocalBlobStoreError::Io)?;
        staged
            .write_bytes(bytes)
            .await
            .map_err(LocalBlobStoreError::Io)?;
        staged.commit().await.map_err(LocalBlobStoreError::Io)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn read_local_blob(
        &self,
        namespace: &str,
        id: &str,
        expected_size: u64,
    ) -> Result<Option<Vec<u8>>, LocalBlobStoreError> {
        let Some(path) = self
            .local_blob_path_if_present(namespace, id, expected_size)
            .await?
        else {
            return Ok(None);
        };
        tokio::fs::read(&path).await.map(Some).map_err(|error| {
            LocalBlobStoreError::Io(format!("read local blob {}: {error}", path.display()))
        })
    }
}

async fn file_exists(path: &Path) -> Result<bool, String> {
    match tokio::fs::metadata(path).await {
        Ok(metadata) => Ok(metadata.is_file()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(format!("stat store blob {}: {error}", path.display())),
    }
}

async fn exact_file_facts(path: &Path) -> Result<(u64, crate::object_hash::ObjectHash), String> {
    use sha2::{Digest, Sha256};
    use tokio::io::AsyncReadExt;

    let mut file = tokio::fs::File::open(path)
        .await
        .map_err(|error| format!("open store blob {}: {error}", path.display()))?;
    let mut size = 0_u64;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 1 << 20];
    loop {
        let read = file
            .read(&mut buffer)
            .await
            .map_err(|error| format!("read store blob {}: {error}", path.display()))?;
        if read == 0 {
            break;
        }
        size = size
            .checked_add(read as u64)
            .ok_or_else(|| format!("store blob size overflow: {}", path.display()))?;
        hasher.update(&buffer[..read]);
    }
    Ok((
        size,
        crate::object_hash::ObjectHash::from_digest(hasher.finalize().into()),
    ))
}

async fn file_is_exact(
    path: &Path,
    expected_size: u64,
    expected_hash: crate::object_hash::ObjectHash,
) -> Result<bool, StoreBlobFileError> {
    if !file_exists(path).await.map_err(StoreBlobFileError::Io)? {
        return Ok(false);
    }
    let (size, hash) = exact_file_facts(path)
        .await
        .map_err(StoreBlobFileError::Io)?;
    Ok(size == expected_size && hash == expected_hash)
}

async fn remove_file(path: &Path) -> Result<bool, String> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(format!("remove store blob {}: {error}", path.display())),
    }
}

async fn walk_files(path: &Path) -> Result<Vec<(PathBuf, u64, u64)>, String> {
    let mut files = Vec::new();
    let mut pending = vec![path.to_path_buf()];
    while let Some(directory) = pending.pop() {
        let mut entries = match tokio::fs::read_dir(&directory).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(format!(
                    "read store blob directory {}: {error}",
                    directory.display()
                ))
            }
        };
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|error| format!("read store blob directory entry: {error}"))?
        {
            let entry_path = entry.path();
            let metadata = match entry.metadata().await {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => {
                    return Err(format!("stat store blob {}: {error}", entry_path.display()))
                }
            };
            if metadata.is_dir() {
                pending.push(entry_path);
            } else if !crate::local_file::AtomicStagedFile::is_staging_path(&entry_path) {
                let recency = metadata
                    .modified()
                    .map_err(|error| format!("read store blob modification time: {error}"))?
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_err(|error| format!("store blob modification time: {error}"))?
                    .as_millis() as u64;
                files.push((entry_path, recency, metadata.len()));
            }
        }
    }
    Ok(files)
}

/// The single-writer store lock: an exclusive advisory lock on
/// `<store>/.coven-lock`, held for the life of a full open handle (and its
/// running sync loop). A second full open of the same store is refused with
/// [`StoreOpenGuardError::AlreadyOpen`] while the lock is held — the invariant
/// that keeps two writers from racing the same db and blob store.
///
/// # Read-only opens take no lock
///
/// A read-only open deliberately does **not** touch this lock. The lock is
/// exclusive, so a shared lock on the same file would block against a writer
/// that already holds it (and vice versa) — a reader could never coexist with
/// the writer it exists to read alongside. But a read-only open needs no lock
/// at all: the lock guards against a second *writer*, and a read-only handle
/// holds a `SQLITE_OPEN_READONLY` connection that cannot write. So a read-only
/// open skips the guard entirely. Cross-process safety comes from WAL mode (a
/// reader sees committed rows while the writer commits more), not from this
/// lock; the blob cache a reader may populate is per-device scratch written
/// atomically (temp + rename), so a reader and the writer touching the same
/// cache file never tear it. This lets one writer and any number of read-only
/// readers coexist on one store.
pub struct StoreOpenGuard {
    _file: std::fs::File,
}

#[derive(Debug)]
pub enum StoreOpenGuardError {
    AlreadyOpen { store_dir: PathBuf },
    MalformedPath(String),
    Io(std::io::Error),
}

impl StoreOpenGuard {
    pub fn acquire(store_dir: &StoreDir) -> Result<Self, StoreOpenGuardError> {
        let db_path = store_dir.db_path();
        let Some(dir) = db_path.parent() else {
            return Err(StoreOpenGuardError::MalformedPath(format!(
                "store database path has no parent: {}",
                db_path.display()
            )));
        };
        std::fs::create_dir_all(dir).map_err(StoreOpenGuardError::Io)?;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(dir.join(".coven-lock"))
            .map_err(StoreOpenGuardError::Io)?;
        match file.try_lock() {
            Ok(()) => Ok(Self { _file: file }),
            Err(std::fs::TryLockError::WouldBlock) => Err(StoreOpenGuardError::AlreadyOpen {
                store_dir: dir.to_path_buf(),
            }),
            Err(std::fs::TryLockError::Error(error)) => Err(StoreOpenGuardError::Io(error)),
        }
    }

    /// Acquire the guard for a test, panicking on refusal.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn acquire_for_test(store_dir: &StoreDir) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self::acquire(store_dir).expect("acquire store open guard"))
    }
}

impl Deref for StoreDir {
    type Target = Path;

    fn deref(&self) -> &Path {
        &self.path
    }
}

impl AsRef<Path> for StoreDir {
    fn as_ref(&self) -> &Path {
        &self.path
    }
}

impl From<PathBuf> for StoreDir {
    fn from(path: PathBuf) -> Self {
        Self {
            path,
            db_filename: DEFAULT_DB_FILENAME.to_string(),
        }
    }
}

/// A temp dir plus a [`StoreDir`] rooted at it. The returned `TempDir` must be
/// held for the directory to outlive the test.
#[cfg(any(test, feature = "test-utils"))]
pub fn temp_store_dir() -> (tempfile::TempDir, StoreDir) {
    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = StoreDir::new(tmp.path());
    (tmp, dir)
}

#[cfg(test)]
#[path = "store_dir_tests.rs"]
mod tests;

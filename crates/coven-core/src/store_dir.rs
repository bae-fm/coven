use std::ops::Deref;
use std::path::{Component, Path, PathBuf};
use tracing::info;

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
/// legitimate — the readable path is nested — but a `..` component, a leading
/// separator (an absolute key), a `\`, or a NUL still escape or truncate, so they
/// are refused. The `cloud_path` never feeds a local file path, only the cloud
/// object key, so this guards the keyspace, not the disk.
pub fn validate_cloud_path(cloud_path: &str) -> Result<(), PathTokenError> {
    if cloud_path.is_empty() {
        return Err(PathTokenError::Empty);
    }
    if cloud_path.contains('\0') {
        return Err(PathTokenError::NulByte);
    }
    if cloud_path.contains('\\') {
        return Err(PathTokenError::Separator);
    }
    if cloud_path.starts_with('/') {
        return Err(PathTokenError::Separator);
    }
    if Path::new(cloud_path)
        .components()
        .any(|c| c == Component::ParentDir)
    {
        return Err(PathTokenError::ParentDir);
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
    /// the id must be indexable (see [`Self::id_shard`]).
    pub fn uploader_hashed_key(
        namespace: &str,
        uploader: &str,
        id: &str,
    ) -> Result<String, PathTokenError> {
        validate_path_token(namespace)?;
        validate_path_token(uploader)?;
        Ok(format!("{namespace}/{uploader}/{}", Self::id_shard(id)?))
    }

    /// Parse a hashed blob cloud key `{namespace}/{uploader}/{ab}/{cd}/{id}` back
    /// into `(namespace, uploader, id)`, or `None` when it is not one — a wrong
    /// segment count, or a shard that does not rebuild (e.g. a plain
    /// `{namespace}/{cloud_path}` key that happens to have five segments). The
    /// inverse of [`Self::uploader_hashed_key`], and the single place the layout is
    /// parsed, shared by the GC, the blob→row lookup, and share authorization.
    pub fn parse_uploader_hashed_key(cloud_key: &str) -> Option<(String, String, String)> {
        let mut parts = cloud_key.split('/');
        let namespace = parts.next()?;
        let uploader = parts.next()?;
        let _ab = parts.next()?;
        let _cd = parts.next()?;
        let id = parts.next()?;
        if parts.next().is_some() {
            return None;
        }
        match Self::uploader_hashed_key(namespace, uploader, id) {
            Ok(rebuilt) if rebuilt == cloud_key => {
                Some((namespace.to_string(), uploader.to_string(), id.to_string()))
            }
            _ => None,
        }
    }

    pub fn storage_dir(&self) -> PathBuf {
        self.path.join("storage")
    }

    /// A kept (budget-exempt) cache copy of a **Remote** blob:
    /// `storage/pinned/<namespace>/{ab}/{cd}/<id>`. The kept sibling of
    /// [`Self::cache_blob_path`] — same per-namespace shard layout, in the `pinned`
    /// folder instead of `cache`. The cache's truth is the folder a blob's file lives
    /// in, not a table; a file here is a Remote blob's cache copy the user pinned for
    /// offline (kept from eviction). `Err` if `namespace`/`id` is unsafe or `id` is
    /// not indexable (see [`Self::id_shard`]).
    pub fn pinned_blob_path(&self, namespace: &str, id: &str) -> Result<PathBuf, PathTokenError> {
        self.cache_folder_blob_path("pinned", namespace, id)
    }

    /// An opportunistic (evictable) cache copy of a **Remote** blob:
    /// `storage/cache/<namespace>/{ab}/{cd}/<id>`. A file here is a cached-but-unpinned
    /// blob — fetched on read or eagerly on pull, droppable by the budget sweep. The
    /// folder it lives in, not a table, is what makes it evictable rather than kept.
    /// Segmented by `namespace` so each namespace's budget evicts only its own
    /// subtree (see [`Self::cache_namespace_dir`]). `Err` if
    /// `namespace`/`id` is unsafe or `id` is not indexable (see [`Self::id_shard`]).
    pub fn cache_blob_path(&self, namespace: &str, id: &str) -> Result<PathBuf, PathTokenError> {
        self.cache_folder_blob_path("cache", namespace, id)
    }

    /// `storage/<folder>/<namespace>/{ab}/{cd}/<id>` — the single blob-path builder
    /// behind [`Self::cache_blob_path`] (`folder` = `cache`) and
    /// [`Self::pinned_blob_path`] (`folder` = `pinned`), which differ only by the
    /// folder token. Composes the per-namespace dir
    /// ([`Self::cache_folder_namespace_dir`]) with the id shard, so the layout lives
    /// in one place. `namespace`/`id` validated.
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

    /// The evictable-cache root, `storage/cache`, holding every namespace's subtree.
    /// The per-namespace budget sweep walks only one namespace's subtree under it —
    /// see [`Self::cache_namespace_dir`].
    pub fn cache_dir(&self) -> PathBuf {
        self.storage_dir().join("cache")
    }

    /// One namespace's evictable-cache subtree, `storage/cache/<namespace>`. The
    /// budget sweep ([`crate::blob::cache::evict_to_budget`]) walks only this tree, so
    /// a namespace evicts against its own budget without touching another namespace's
    /// files. `namespace` is validated as a single path token; `Err` if it is unsafe.
    pub fn cache_namespace_dir(&self, namespace: &str) -> Result<PathBuf, PathTokenError> {
        self.cache_folder_namespace_dir("cache", namespace)
    }

    /// Create a store directory, generate a device_id, and save config.yaml.
    ///
    /// The caller is responsible for encryption key setup and calling
    /// `Config::save_active_store()` afterward.
    ///
    /// `store_id` is device-generated here (trusted), but it still has to name a
    /// single directory under `layout`'s stores dir, so it passes the same
    /// [`validate_path_token`] gate the untrusted join/restore ids pass — a
    /// malformed id fails loudly rather than forming a stray path.
    ///
    /// Refuses with [`ConfigError::StoreExists`] if the directory already
    /// exists — the same refusal join and restore give their own `stores/<id>`,
    /// so a `store_id` collision (e.g. a device-generated id reused, or two
    /// concurrent creates) fails loudly here too rather than overwriting an
    /// existing store's directory.
    pub fn create(
        layout: &StoreLayout,
        store_id: String,
        store_name: String,
        ids: &dyn crate::id_provider::IdProvider,
    ) -> Result<Config, ConfigError> {
        validate_path_token(&store_id)
            .map_err(|e| ConfigError::Config(format!("invalid store id: {e}")))?;
        let store_dir = layout.store_dir(&store_id);
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A normal id partitions by its first two dash-stripped byte-pairs, with the
    /// full id (dashes kept) as the file name — the layout the cloud and local
    /// stores share.
    #[test]
    fn hashed_path_partitions_a_normal_id() {
        let id = "a1b2c3d4-e5f6-7890-abcd-ef1234567890";
        assert_eq!(
            StoreDir::hashed_path("images", id).expect("valid id"),
            format!("images/a1/b2/{id}"),
        );
    }

    /// An id too short to take the `{ab}/{cd}` prefix cannot index a two-byte
    /// shard, so it is rejected as `Unindexable` rather than slicing past its end.
    #[test]
    fn hashed_path_refuses_a_short_id_instead_of_panicking() {
        assert_eq!(
            StoreDir::hashed_path("images", "a"),
            Err(PathTokenError::Unindexable),
        );
    }

    /// An id whose dash-stripped form splits a multi-byte char at the prefix
    /// boundary is unindexable too, not a panic.
    #[test]
    fn hashed_path_refuses_a_misaligned_multibyte_id() {
        // 'é' is two bytes; "aé" puts a char boundary failure at byte 2.
        assert_eq!(
            StoreDir::hashed_path("images", "aé"),
            Err(PathTokenError::Unindexable),
        );
    }

    /// An id or namespace carrying a separator, a `..`, or a NUL is a traversal
    /// attempt and is refused before any path is built.
    #[test]
    fn hashed_path_refuses_traversal_tokens() {
        assert_eq!(
            StoreDir::hashed_path("images", "ab/../../etc/passwd"),
            Err(PathTokenError::Separator),
        );
        assert_eq!(
            StoreDir::hashed_path("images", ".."),
            Err(PathTokenError::ParentDir),
        );
        assert_eq!(
            StoreDir::hashed_path("images", "a\0b"),
            Err(PathTokenError::NulByte),
        );
        assert_eq!(
            StoreDir::hashed_path("im/ages", "abcd"),
            Err(PathTokenError::Separator),
        );
    }

    /// `validate_path_token` accepts an ordinary single token and rejects each
    /// escape shape: separators (`/` and `\`), a leading slash (an absolute path),
    /// a bare `..` and a bare `.` (both directory-relative references that
    /// normalize away to land off a child), a NUL, a `:` (a Windows
    /// alternate-data-stream / drive-relative reference), and the empty string.
    #[test]
    fn validate_path_token_accepts_safe_and_rejects_escapes() {
        assert_eq!(validate_path_token("abc123"), Ok(()));
        assert_eq!(validate_path_token(""), Err(PathTokenError::Empty));
        assert_eq!(validate_path_token("a/b"), Err(PathTokenError::Separator));
        assert_eq!(validate_path_token("a\\b"), Err(PathTokenError::Separator));
        assert_eq!(validate_path_token("/abs"), Err(PathTokenError::Separator));
        assert_eq!(validate_path_token(".."), Err(PathTokenError::ParentDir));
        assert_eq!(validate_path_token("."), Err(PathTokenError::CurDir));
        assert_eq!(validate_path_token("a\0b"), Err(PathTokenError::NulByte));
        assert_eq!(validate_path_token("foo:bar"), Err(PathTokenError::Colon));
        assert_eq!(validate_path_token("c:"), Err(PathTokenError::Colon));
    }

    /// A lone `.` is rejected just as `..` is: a trailing `.` component is
    /// normalized away when the path resolves, so `stores/.` would land on
    /// `stores`'s parent (the data dir) rather than name a child of `stores/`
    /// — an escape. The unit under test is the rejection itself.
    #[test]
    fn validate_path_token_rejects_lone_current_dir() {
        assert_eq!(validate_path_token("."), Err(PathTokenError::CurDir));
    }

    /// A `cloud_path` is a readable object key, so an interior `/` is legitimate
    /// (nested path), but a `..` component, a leading slash, a `\`, or a NUL still
    /// escape its namespace prefix and are refused.
    #[test]
    fn validate_cloud_path_allows_nesting_but_rejects_escapes() {
        assert_eq!(validate_cloud_path("Artist - Album/cover.jpg"), Ok(()));
        assert_eq!(validate_cloud_path(""), Err(PathTokenError::Empty));
        assert_eq!(
            validate_cloud_path("../escape"),
            Err(PathTokenError::ParentDir),
        );
        assert_eq!(
            validate_cloud_path("a/../../escape"),
            Err(PathTokenError::ParentDir),
        );
        assert_eq!(validate_cloud_path("/abs"), Err(PathTokenError::Separator));
        assert_eq!(validate_cloud_path("a\\b"), Err(PathTokenError::Separator),);
    }
}

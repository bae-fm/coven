use std::ops::Deref;
use std::path::{Component, Path, PathBuf};
use tracing::info;

use crate::config::{Config, ConfigError};

/// Why a string is not a safe blob path token.
///
/// A blob's `id`/`namespace` is interpolated into both its on-disk file path and
/// its cloud object key. Those values arrive in an incoming changeset authored by
/// any write-capable member, so an unconstrained one could climb out of the
/// library directory (`..`, a path separator, an absolute leading slash) and make
/// every pulling device write attacker-chosen bytes to an arbitrary file, or — too
/// short / not aligned to a char boundary — crash the partition-prefix slice. A
/// token that trips any of these is bad data, refused before a path is built.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlobPathError {
    /// The token is empty — no file name to write, no key to form.
    Empty,
    /// The token contains a path separator (`/` or `\`), so joining it onto a
    /// directory would descend into (or, with a leading separator, replace) the
    /// path rather than name a single child.
    Separator,
    /// The token is or contains a `..` component, which climbs to the parent.
    ParentDir,
    /// The token contains a NUL byte, which truncates the path at the OS boundary.
    NulByte,
    /// The token contains a `:`, which on Windows names an alternate data stream
    /// (`file:stream`) or a drive-relative reference (`c:dir`) rather than a child.
    Colon,
    /// The dash-stripped id is too short, or splits a multi-byte char, to take the
    /// two leading byte-pairs the `{ab}/{cd}` partition prefix needs.
    Unindexable,
}

impl std::fmt::Display for BlobPathError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BlobPathError::Empty => write!(f, "blob path token is empty"),
            BlobPathError::Separator => write!(f, "blob path token contains a path separator"),
            BlobPathError::ParentDir => write!(f, "blob path token contains a parent reference"),
            BlobPathError::NulByte => write!(f, "blob path token contains a NUL byte"),
            BlobPathError::Colon => write!(f, "blob path token contains a colon"),
            BlobPathError::Unindexable => {
                write!(
                    f,
                    "blob id is too short or misaligned to form a partition prefix"
                )
            }
        }
    }
}

impl std::error::Error for BlobPathError {}

/// Reject a single untrusted path token (a blob `id` or `namespace`) that could
/// escape the directory it is joined onto. A safe token names exactly one child:
/// no separator, no `..`, no NUL, no `:` (a Windows stream/drive reference),
/// non-empty. The single gate every path builder runs an untrusted token through,
/// so traversal is refused before any on-disk or cloud path is formed.
pub fn validate_path_token(token: &str) -> Result<(), BlobPathError> {
    if token.is_empty() {
        return Err(BlobPathError::Empty);
    }
    if token.contains('\0') {
        return Err(BlobPathError::NulByte);
    }
    if token.contains('/') || token.contains('\\') {
        return Err(BlobPathError::Separator);
    }
    if token.contains(':') {
        return Err(BlobPathError::Colon);
    }
    if token == ".." {
        return Err(BlobPathError::ParentDir);
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
pub fn validate_cloud_path(cloud_path: &str) -> Result<(), BlobPathError> {
    if cloud_path.is_empty() {
        return Err(BlobPathError::Empty);
    }
    if cloud_path.contains('\0') {
        return Err(BlobPathError::NulByte);
    }
    if cloud_path.contains('\\') {
        return Err(BlobPathError::Separator);
    }
    if cloud_path.starts_with('/') {
        return Err(BlobPathError::Separator);
    }
    if Path::new(cloud_path)
        .components()
        .any(|c| c == Component::ParentDir)
    {
        return Err(BlobPathError::ParentDir);
    }
    Ok(())
}

/// Whether `path` contains a parent-directory (`..`) component anywhere, which
/// would let it climb above the directory its host placed it under. A blob's
/// local file path is `host_dir` joined with a validated single-token id, so a
/// legitimate one never contains `..`; one that does was either built from an
/// unvalidated id or by a host that traversed, and writing through it could land
/// outside the library. A pure lexical check (no filesystem access, so no
/// symlink/TOCTOU dependency): the write boundary refuses such a path independent
/// of how it was built, the second line of defense behind token validation.
pub fn path_escapes_root(path: &Path) -> bool {
    path.components().any(|c| c == Component::ParentDir)
}

/// Typed wrapper for a library directory path.
///
/// Centralizes the on-disk layout so callers use methods instead of
/// ad-hoc `path.join("images")` etc.
#[derive(Clone, Debug)]
pub struct LibraryDir {
    path: PathBuf,
}

impl LibraryDir {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn db_path(&self) -> PathBuf {
        self.path.join("library.db")
    }

    pub fn config_path(&self) -> PathBuf {
        self.path.join("config.yaml")
    }

    pub fn images_dir(&self) -> PathBuf {
        self.path.join("images")
    }

    /// Content-addressed relative path `{prefix}/{ab}/{cd}/{id}`, partitioning by
    /// the first two byte-pairs of the dash-stripped id. The single home for the
    /// partition scheme — shared by the local blob store and the cloud layout.
    ///
    /// Both `prefix` and `id` are validated as single path tokens, and the id must
    /// be long enough (and char-boundary aligned) to take the two leading
    /// byte-pairs the prefix needs. An id that fails is bad data — it could escape
    /// the directory or crash the slice — so this returns [`BlobPathError`] rather
    /// than interpolating it or panicking; the caller refuses the blob.
    pub fn hashed_path(prefix: &str, id: &str) -> Result<String, BlobPathError> {
        validate_path_token(prefix)?;
        validate_path_token(id)?;
        let hex = id.replace('-', "");
        if !(hex.is_char_boundary(2) && hex.is_char_boundary(4)) {
            return Err(BlobPathError::Unindexable);
        }
        Ok(format!("{prefix}/{}/{}/{id}", &hex[..2], &hex[2..4]))
    }

    /// Hash-based image path: `images/{ab}/{cd}/{id}`. `Err` if `id` is not a safe,
    /// indexable blob token (see [`Self::hashed_path`]).
    pub fn image_path(&self, id: &str) -> Result<PathBuf, BlobPathError> {
        Ok(self.path.join(Self::hashed_path("images", id)?))
    }

    pub fn torrents_dir(&self) -> PathBuf {
        self.path.join("torrents")
    }

    /// Hash-based torrent file path: `torrents/{ab}/{cd}/{id}`. `Err` if
    /// `torrent_id` is not a safe, indexable blob token (see [`Self::hashed_path`]).
    pub fn torrent_file_path(&self, torrent_id: &str) -> Result<PathBuf, BlobPathError> {
        Ok(self.path.join(Self::hashed_path("torrents", torrent_id)?))
    }

    pub fn storage_dir(&self) -> PathBuf {
        self.path.join("storage")
    }

    /// Hash-based storage path: `storage/{ab}/{cd}/{file_id}`. `Err` if `file_id`
    /// is not a safe, indexable blob token (see [`Self::hashed_path`]).
    pub fn storage_file_path(&self, file_id: &str) -> Result<PathBuf, BlobPathError> {
        Ok(self.path.join(Self::hashed_path("storage", file_id)?))
    }

    pub fn pending_deletions_path(&self) -> PathBuf {
        self.path.join("pending_deletions.json")
    }

    pub fn playback_state_path(&self) -> PathBuf {
        self.path.join("playback_state.json")
    }

    /// All asset directories that should be synced/created.
    pub fn asset_dirs(&self) -> Vec<PathBuf> {
        vec![self.images_dir(), self.storage_dir(), self.torrents_dir()]
    }

    /// Create a library directory, generate a device_id, and save config.yaml.
    ///
    /// The caller is responsible for encryption key setup and calling
    /// `Config::save_active_library()` afterward.
    pub fn create(
        data_dir: &Path,
        library_id: String,
        library_name: String,
        ids: &dyn crate::id_provider::IdProvider,
    ) -> Result<Config, ConfigError> {
        let library_dir = LibraryDir::new(data_dir.join("libraries").join(&library_id));
        std::fs::create_dir_all(&*library_dir)?;

        let device_id = ids.new_id();
        let config = Config::with_defaults(library_id, device_id, library_dir, library_name);
        config.save_to_config_yaml()?;

        info!("Created library at {}", config.library_dir.display());
        Ok(config)
    }
}

impl Deref for LibraryDir {
    type Target = Path;

    fn deref(&self) -> &Path {
        &self.path
    }
}

impl AsRef<Path> for LibraryDir {
    fn as_ref(&self) -> &Path {
        &self.path
    }
}

impl From<PathBuf> for LibraryDir {
    fn from(path: PathBuf) -> Self {
        Self { path }
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
            LibraryDir::hashed_path("images", id).expect("valid id"),
            format!("images/a1/b2/{id}"),
        );
    }

    /// An id too short to take the `{ab}/{cd}` prefix cannot index a two-byte
    /// shard, so it is rejected as `Unindexable` rather than slicing past its end.
    #[test]
    fn hashed_path_refuses_a_short_id_instead_of_panicking() {
        assert_eq!(
            LibraryDir::hashed_path("images", "a"),
            Err(BlobPathError::Unindexable),
        );
    }

    /// An id whose dash-stripped form splits a multi-byte char at the prefix
    /// boundary is unindexable too, not a panic.
    #[test]
    fn hashed_path_refuses_a_misaligned_multibyte_id() {
        // 'é' is two bytes; "aé" puts a char boundary failure at byte 2.
        assert_eq!(
            LibraryDir::hashed_path("images", "aé"),
            Err(BlobPathError::Unindexable),
        );
    }

    /// An id or namespace carrying a separator, a `..`, or a NUL is a traversal
    /// attempt and is refused before any path is built.
    #[test]
    fn hashed_path_refuses_traversal_tokens() {
        assert_eq!(
            LibraryDir::hashed_path("images", "ab/../../etc/passwd"),
            Err(BlobPathError::Separator),
        );
        assert_eq!(
            LibraryDir::hashed_path("images", ".."),
            Err(BlobPathError::ParentDir),
        );
        assert_eq!(
            LibraryDir::hashed_path("images", "a\0b"),
            Err(BlobPathError::NulByte),
        );
        assert_eq!(
            LibraryDir::hashed_path("im/ages", "abcd"),
            Err(BlobPathError::Separator),
        );
    }

    /// `validate_path_token` accepts an ordinary single token and rejects each
    /// escape shape: separators (`/` and `\`), a leading slash (an absolute path),
    /// a bare `..`, a NUL, a `:` (a Windows alternate-data-stream / drive-relative
    /// reference), and the empty string.
    #[test]
    fn validate_path_token_accepts_safe_and_rejects_escapes() {
        assert_eq!(validate_path_token("abc123"), Ok(()));
        assert_eq!(validate_path_token(""), Err(BlobPathError::Empty));
        assert_eq!(validate_path_token("a/b"), Err(BlobPathError::Separator));
        assert_eq!(validate_path_token("a\\b"), Err(BlobPathError::Separator));
        assert_eq!(validate_path_token("/abs"), Err(BlobPathError::Separator));
        assert_eq!(validate_path_token(".."), Err(BlobPathError::ParentDir));
        assert_eq!(validate_path_token("a\0b"), Err(BlobPathError::NulByte));
        assert_eq!(validate_path_token("foo:bar"), Err(BlobPathError::Colon));
        assert_eq!(validate_path_token("c:"), Err(BlobPathError::Colon));
    }

    /// A `cloud_path` is a readable object key, so an interior `/` is legitimate
    /// (nested path), but a `..` component, a leading slash, a `\`, or a NUL still
    /// escape its namespace prefix and are refused.
    #[test]
    fn validate_cloud_path_allows_nesting_but_rejects_escapes() {
        assert_eq!(validate_cloud_path("Artist - Album/cover.jpg"), Ok(()));
        assert_eq!(validate_cloud_path(""), Err(BlobPathError::Empty));
        assert_eq!(
            validate_cloud_path("../escape"),
            Err(BlobPathError::ParentDir),
        );
        assert_eq!(
            validate_cloud_path("a/../../escape"),
            Err(BlobPathError::ParentDir),
        );
        assert_eq!(validate_cloud_path("/abs"), Err(BlobPathError::Separator));
        assert_eq!(validate_cloud_path("a\\b"), Err(BlobPathError::Separator),);
    }

    /// `path_escapes_root` flags any local path that carries a `..` component (it
    /// could land above its directory) and passes a plain `dir/child` path.
    #[test]
    fn path_escapes_root_flags_parent_components() {
        assert!(!path_escapes_root(Path::new("/lib/images/ab/cd/id")));
        assert!(path_escapes_root(Path::new("/lib/images/../../etc/passwd")));
        assert!(path_escapes_root(Path::new("a/../b")));
    }
}

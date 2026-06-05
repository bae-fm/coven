use std::ops::Deref;
use std::path::{Path, PathBuf};
use tracing::info;

use crate::config::{Config, ConfigError};

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
    pub fn hashed_path(prefix: &str, id: &str) -> String {
        let hex = id.replace('-', "");
        format!("{prefix}/{}/{}/{id}", &hex[..2], &hex[2..4])
    }

    /// Hash-based image path: `images/{ab}/{cd}/{id}`
    pub fn image_path(&self, id: &str) -> PathBuf {
        self.path.join(Self::hashed_path("images", id))
    }

    pub fn torrents_dir(&self) -> PathBuf {
        self.path.join("torrents")
    }

    /// Hash-based torrent file path: `torrents/{ab}/{cd}/{id}`
    pub fn torrent_file_path(&self, torrent_id: &str) -> PathBuf {
        self.path.join(Self::hashed_path("torrents", torrent_id))
    }

    pub fn storage_dir(&self) -> PathBuf {
        self.path.join("storage")
    }

    /// Hash-based storage path: `storage/{ab}/{cd}/{file_id}`
    pub fn storage_file_path(&self, file_id: &str) -> PathBuf {
        self.path.join(Self::hashed_path("storage", file_id))
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

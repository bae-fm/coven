//! The bytes a store keeps in its directory (§17.8): everything the store's
//! own files hold, apart from the cache, whose bytes its database counts,
//! and what resets kept, whose sizes its database records (§19.3). A
//! rebuild's folder (§18.3) is one of the store's own files while it lives,
//! its cache folder with it: the figures show what is on the disk.

use super::{device_reset, FileError};
use std::fs;
use std::path::Path;

/// What a store's directory holds, in file sizes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DirectoryBytes {
    /// Every file in the directory but the cache's and what resets kept.
    pub library: u64,
}

/// Measure the store's directory `store`, leaving out the folder named
/// `cache`.
pub(super) fn measure(store: &Path, cache: &str) -> Result<DirectoryBytes, FileError> {
    let mut library = 0u64;
    let entries = fs::read_dir(store)
        .map_err(|source| FileError::at("list the store's directory", store, source))?;
    for entry in entries {
        let entry =
            entry.map_err(|source| FileError::at("list the store's directory", store, source))?;
        let name = entry.file_name();
        if name == cache
            || name == device_reset::AREA
            || name.to_str().is_some_and(device_reset::is_kept_by_resets)
        {
            continue;
        }
        library = library.saturating_add(device_reset::tree_size(&entry.path())?);
    }
    Ok(DirectoryBytes { library })
}

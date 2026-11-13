//! The bytes a store keeps in its directory (§17.8, §19.3): everything the
//! store's own files hold, apart from the cache, whose bytes its database
//! counts, and the installations resets kept. A rebuild's folder (§18.3) is
//! one of the store's own files while it lives, its cache folder with it: the
//! figures show what is on the disk.

use super::{device_reset, FileError};
use std::fs;
use std::path::Path;

/// What a store's directory holds, in file sizes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DirectoryBytes {
    /// Every file in the directory but the cache's and the archives'.
    pub library: u64,
    /// The archives resets kept together, while there is one.
    pub archives: Option<u64>,
}

/// Measure the store's directory `store`, leaving out the folder named
/// `cache`.
pub(super) fn measure(store: &Path, cache: &str) -> Result<DirectoryBytes, FileError> {
    let archives = device_reset::archives(store)?;
    let mut library = 0u64;
    let entries = fs::read_dir(store)
        .map_err(|source| FileError::at("list the store's directory", store, source))?;
    for entry in entries {
        let entry =
            entry.map_err(|source| FileError::at("list the store's directory", store, source))?;
        let path = entry.path();
        let name = entry.file_name();
        if name == cache || name == device_reset::AREA || archives.contains(&path) {
            continue;
        }
        library = library.saturating_add(size_of(&path)?);
    }
    let archives = if archives.is_empty() {
        None
    } else {
        let mut total = 0u64;
        for archive in &archives {
            total = total.saturating_add(size_of(archive)?);
        }
        Some(total)
    };
    Ok(DirectoryBytes { library, archives })
}

/// The size of a file, or of everything under a folder. Links are not
/// followed, and what disappears while the walk runs counts for nothing.
fn size_of(path: &Path) -> Result<u64, FileError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(source) => return Err(FileError::at("measure a store file", path, source)),
    };
    if metadata.is_file() {
        return Ok(metadata.len());
    }
    if !metadata.is_dir() {
        return Ok(0);
    }
    let entries = match fs::read_dir(path) {
        Ok(entries) => entries,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(source) => return Err(FileError::at("list a store folder", path, source)),
    };
    let mut total = 0u64;
    for entry in entries {
        let entry = entry.map_err(|source| FileError::at("list a store folder", path, source))?;
        total = total.saturating_add(size_of(&entry.path())?);
    }
    Ok(total)
}

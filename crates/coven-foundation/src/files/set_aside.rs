//! Folders coven deletes in two steps (§17.8, §19.3): one rename moves a
//! folder aside, in the step that deletes the row counting its bytes, and
//! coven then deletes the folder it moved aside. No row names that folder,
//! so a deletion that stops partway leaves no row wrong.

use std::fs;
use std::path::Path;

use super::{atomic_file, FileError};

/// Move `folder` to `aside` in one rename, creating the folder that holds
/// `aside` when it is missing, then sync the folders that held and now hold
/// it. Returns whether there was a `folder` to move.
pub(super) fn move_aside(folder: &Path, aside: &Path) -> Result<bool, FileError> {
    if !atomic_file::exists("inspect a folder to delete", folder)? {
        return Ok(false);
    }
    let holder = parent(aside);
    if !atomic_file::exists("inspect the folder of what coven deletes", holder)? {
        fs::create_dir(holder).map_err(|source| {
            FileError::at("create the folder of what coven deletes", holder, source)
        })?;
    }
    atomic_file::rename_new(folder, aside)
        .map_err(|source| FileError::at("move a folder aside to delete it", folder, source))?;
    sync_folders(parent(folder), holder)?;
    Ok(true)
}

/// Move `aside` back to `folder`, where `move_aside` found it, then sync
/// both folders that hold them.
pub(super) fn move_back(folder: &Path, aside: &Path) -> Result<(), FileError> {
    atomic_file::rename_new(aside, folder)
        .map_err(|source| FileError::at("move a folder back from deletion", aside, source))?;
    sync_folders(parent(folder), parent(aside))
}

/// Delete `aside` and everything in it, then sync the folder that held it.
/// When `aside` is absent, the call does nothing and succeeds. A deletion
/// that stops partway leaves the rest for the next call.
pub(super) fn delete(aside: &Path) -> Result<(), FileError> {
    match fs::remove_dir_all(aside) {
        Ok(()) => atomic_file::sync_publication(aside),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(FileError::at("delete a folder moved aside", aside, source)),
    }
}

/// Sync `from` and `to`, the folders on each side of a rename. On Windows
/// a rename writes through, so the function does nothing there.
#[cfg_attr(not(unix), expect(unused_variables, reason = "Windows renames write through"))]
fn sync_folders(from: &Path, to: &Path) -> Result<(), FileError> {
    #[cfg(unix)]
    for folder in [from, to] {
        atomic_file::sync_directory(folder)
            .map_err(|source| FileError::at("sync a folder after a rename", folder, source))?;
    }
    Ok(())
}

/// The folder that holds `path`, which is always inside a store's directory.
fn parent(path: &Path) -> &Path {
    path.parent().expect("coven moves only folders inside a store's directory")
}

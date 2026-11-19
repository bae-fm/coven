//! The files that keep a directory in place on Windows, which refuses to
//! move or delete a directory while a file in it is open. Elsewhere an open
//! file does not stop a rename or a removal.

use std::io;
use std::path::{Path, PathBuf};

use super::FileError;

/// The failure of `operation` on `directory`: `FileError::HeldOpen` naming the
/// files in it that are open when there are any, else `FileError::Io`.
pub(super) fn directory_failure(
    operation: &'static str,
    directory: &Path,
    source: io::Error,
) -> FileError {
    let held = files_held_open(directory);
    if held.is_empty() {
        FileError::at(operation, directory, source)
    } else {
        FileError::HeldOpen {
            operation,
            path: directory.to_owned(),
            held,
            source: Box::new(source),
        }
    }
}

/// The files under `directory` that refuse an exclusive open. Empty on other
/// platforms, and when the walk itself fails.
#[cfg(windows)]
fn files_held_open(directory: &Path) -> Vec<PathBuf> {
    use std::os::windows::fs::OpenOptionsExt;
    walk_files(directory, &|path| {
        std::fs::OpenOptions::new().read(true).share_mode(0).open(path).is_err()
    })
}

#[cfg(not(windows))]
fn files_held_open(_: &Path) -> Vec<PathBuf> {
    Vec::new()
}

/// The files under `directory`, in subdirectories too, for which `held` is
/// true. A directory that can't be read has no files listed.
#[cfg_attr(
    all(not(windows), not(test)),
    expect(dead_code, reason = "only Windows asks which files are open")
)]
fn walk_files(directory: &Path, held: &dyn Fn(&Path) -> bool) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(directory) else { return Vec::new() };
    let mut files = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            files.extend(walk_files(&path, held));
        } else if held(&path) {
            files.push(path);
        }
    }
    files
}

#[cfg(test)]
#[path = "held_open_tests.rs"]
mod tests;

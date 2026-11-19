//! The files that keep a directory in place on Windows, which refuses to
//! move or delete a directory while a file in it is open. Elsewhere an open
//! file does not stop a rename or a removal.

use std::io;
use std::path::{Path, PathBuf};

use super::FileError;

/// The failure of `operation` on `directory`: `FileError::HeldOpen` naming the
/// files in it that are open when there are any, else `FileError::Io`. When
/// looking for the open files itself fails, the `Io` error says so.
pub(super) fn directory_failure(
    operation: &'static str,
    directory: &Path,
    source: io::Error,
) -> FileError {
    match files_held_open(directory) {
        Ok(held) if held.is_empty() => FileError::at(operation, directory, source),
        Ok(held) => FileError::HeldOpen {
            operation,
            path: directory.to_owned(),
            held,
            source: Box::new(source),
        },
        Err(walk) => FileError::at(
            operation,
            directory,
            io::Error::new(
                source.kind(),
                format!("{source}; listing the open files in it failed: {walk}"),
            ),
        ),
    }
}

/// The files under `directory` that refuse an exclusive open because another
/// holder has them open. Empty on other platforms, where an open file does
/// not stop a rename or a removal.
#[cfg(windows)]
fn files_held_open(directory: &Path) -> io::Result<Vec<PathBuf>> {
    use std::os::windows::fs::OpenOptionsExt;
    /// The error Windows gives an open that another holder's open excludes.
    const ERROR_SHARING_VIOLATION: i32 = 32;
    walk_files(directory, &|path| match std::fs::OpenOptions::new()
        .read(true)
        .share_mode(0)
        .open(path)
    {
        Ok(_) => Ok(false),
        Err(error) if error.raw_os_error() == Some(ERROR_SHARING_VIOLATION) => Ok(true),
        Err(error) => Err(error),
    })
}

#[cfg(not(windows))]
fn files_held_open(_: &Path) -> io::Result<Vec<PathBuf>> {
    Ok(Vec::new())
}

/// The files under `directory`, in subdirectories too, for which `held` is
/// true. A directory or file that can't be read fails the walk.
#[cfg(any(windows, test))]
fn walk_files(
    directory: &Path,
    held: &dyn Fn(&Path) -> io::Result<bool>,
) -> io::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(directory)? {
        let path = entry?.path();
        if path.is_dir() {
            files.extend(walk_files(&path, held)?);
        } else if held(&path)? {
            files.push(path);
        }
    }
    Ok(files)
}

#[cfg(test)]
#[path = "held_open_tests.rs"]
mod tests;

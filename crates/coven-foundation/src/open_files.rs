//! Which files under a directory this process still holds open.
//!
//! A closed store must hold no file in its directory. POSIX lets a directory
//! with open files be deleted, so a leftover handle goes unseen on macOS and
//! Linux and fails the delete only on Windows. Listing the process's open file
//! descriptors catches it on every platform that can list them.

use std::path::{Path, PathBuf};

/// Panic naming every file under `dir` this process has open. Windows does
/// not list handles here; deleting the directory is the check there.
pub fn assert_no_open_files_under(dir: &Path) {
    let dir = dir
        .canonicalize()
        .unwrap_or_else(|error| panic!("canonicalize {}: {error}", dir.display()));
    let open: Vec<PathBuf> = open_file_paths()
        .into_iter()
        .filter(|path| path.starts_with(&dir))
        .collect();
    assert!(
        open.is_empty(),
        "files still open under {}: {open:#?}",
        dir.display()
    );
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
fn open_file_paths() -> Vec<PathBuf> {
    use std::os::fd::BorrowedFd;
    use std::os::unix::ffi::OsStringExt;

    let descriptors: Vec<std::os::fd::RawFd> = std::fs::read_dir("/dev/fd")
        .expect("list /dev/fd")
        .filter_map(|entry| entry.ok()?.file_name().to_str()?.parse().ok())
        .collect();
    descriptors
        .into_iter()
        .filter_map(|fd| {
            // SAFETY: the descriptor is only asked for its path. One closed
            // since the listing (the listing's own among them) fails the call.
            let fd = unsafe { BorrowedFd::borrow_raw(fd) };
            let path = rustix::fs::getpath(fd).ok()?;
            Some(PathBuf::from(std::ffi::OsString::from_vec(
                path.into_bytes(),
            )))
        })
        .collect()
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn open_file_paths() -> Vec<PathBuf> {
    std::fs::read_dir("/proc/self/fd")
        .expect("list /proc/self/fd")
        .filter_map(|entry| std::fs::read_link(entry.ok()?.path()).ok())
        .collect()
}

#[cfg(not(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "linux",
    target_os = "android"
)))]
fn open_file_paths() -> Vec<PathBuf> {
    Vec::new()
}

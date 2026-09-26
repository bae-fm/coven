//! Which files under a directory this process still holds open.
//!
//! A closed store must hold no file in its directory. POSIX lets a directory
//! with open files be deleted, so a leftover handle goes unseen on macOS and
//! Linux and fails the delete only on Windows. Listing the process's open file
//! descriptors, or handles on Windows, catches it on every platform.

use std::path::{Path, PathBuf};

/// Every file under `dir` this process has open, once per open descriptor or
/// handle.
pub fn open_files_under(dir: &Path) -> Vec<PathBuf> {
    let dir = dir
        .canonicalize()
        .unwrap_or_else(|error| panic!("canonicalize {}: {error}", dir.display()));
    open_file_paths()
        .into_iter()
        .filter(|path| path.starts_with(&dir))
        .collect()
}

/// Panic naming every file under `dir` this process has open.
pub fn assert_no_open_files_under(dir: &Path) {
    let open = open_files_under(dir);
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

/// Windows gives no listing of a process's handles, so every handle value is
/// asked in turn (they are multiples of four) until as many valid ones have
/// answered as the process holds. A disk file's handle names its path.
#[cfg(windows)]
fn open_file_paths() -> Vec<PathBuf> {
    use std::os::windows::ffi::OsStringExt;
    use windows_sys::Win32::Foundation::{GetHandleInformation, HANDLE};
    use windows_sys::Win32::Storage::FileSystem::{
        GetFileType, GetFinalPathNameByHandleW, FILE_TYPE_DISK, VOLUME_NAME_DOS,
    };
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, GetProcessHandleCount};

    let mut held = 0u32;
    // SAFETY: the pseudo-handle for this process needs no closing, and the
    // count is written to a local.
    if unsafe { GetProcessHandleCount(GetCurrentProcess(), &mut held) } == 0 {
        panic!(
            "count this process's handles: {}",
            std::io::Error::last_os_error()
        );
    }
    let mut paths = Vec::new();
    let mut answered = 0u32;
    let mut value = 4usize;
    let mut name = vec![0u16; 32_768];
    // Handle values stay far below this; it only bounds a count that moved
    // while the values were being asked.
    while answered < held && value < 1 << 24 {
        let handle = value as HANDLE;
        let mut flags = 0u32;
        // SAFETY: an invalid value only fails these calls; none of them
        // reads or writes through the handle, and it is never closed here.
        if unsafe { GetHandleInformation(handle, &mut flags) } != 0 {
            answered += 1;
            if unsafe { GetFileType(handle) } == FILE_TYPE_DISK {
                let length = unsafe {
                    GetFinalPathNameByHandleW(
                        handle,
                        name.as_mut_ptr(),
                        name.len() as u32,
                        VOLUME_NAME_DOS,
                    )
                } as usize;
                if length > 0 && length < name.len() {
                    paths.push(PathBuf::from(std::ffi::OsString::from_wide(
                        &name[..length],
                    )));
                }
            }
        }
        value += 4;
    }
    paths
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lists_each_open_descriptor_under_the_directory_until_it_closes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a");
        std::fs::write(&path, b"a").unwrap();
        let path = path.canonicalize().unwrap();

        let first = std::fs::File::open(&path).unwrap();
        let second = std::fs::File::open(&path).unwrap();
        assert_eq!(
            open_files_under(dir.path()),
            vec![path.clone(), path.clone()]
        );
        drop(first);
        assert_eq!(open_files_under(dir.path()), vec![path]);
        drop(second);
        assert_no_open_files_under(dir.path());
    }
}

//! Which files under a directory this process still holds open.
//!
//! A closed store must hold no file in its directory. POSIX lets a directory
//! with open files be deleted, so a leftover handle goes unseen on macOS and
//! Linux and fails the delete only on Windows. Listing the process's open file
//! descriptors, or handles on Windows, catches it on every platform.

use std::path::{Path, PathBuf};

/// Every file under `dir` this process has open, once per open descriptor or
/// handle. Directories are not files: a handle on one, the kind resolving a
/// path takes for a moment on Windows, holds no file open.
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
            // SAFETY: the descriptor is only asked for its type and path. One
            // closed since the listing (the listing's own among them) fails the
            // call.
            let fd = unsafe { BorrowedFd::borrow_raw(fd) };
            if rustix::fs::FileType::from_raw_mode(rustix::fs::fstat(fd).ok()?.st_mode)
                == rustix::fs::FileType::Directory
            {
                return None;
            }
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
        .filter_map(|entry| {
            let link = entry.ok()?.path();
            // The link's own metadata is the descriptor's, followed through.
            if std::fs::metadata(&link).ok()?.is_dir() {
                return None;
            }
            std::fs::read_link(link).ok()
        })
        .collect()
}

/// The disk files this process holds open for their data, read from one
/// snapshot of its handles that the kernel takes at once.
///
/// Paths are named after the snapshot, one handle at a time, and a handle
/// value freed meanwhile can be taken by a new handle. So only values that
/// were file handles in the snapshot are named: a file opened since then under
/// a value that held something else is never counted beside the one it
/// replaced, and the list never names more files than were open at the
/// snapshot. A handle opened only to read attributes, as `std::fs::metadata`
/// and `canonicalize` do here and no Unix descriptor stands for, holds no file
/// open, and neither does one on a directory.
#[cfg(windows)]
fn open_file_paths() -> Vec<PathBuf> {
    use std::os::windows::ffi::OsStringExt;
    use windows_sys::Win32::Storage::FileSystem::{
        GetFileInformationByHandle, GetFileType, GetFinalPathNameByHandleW,
        BY_HANDLE_FILE_INFORMATION, FILE_APPEND_DATA, FILE_ATTRIBUTE_DIRECTORY, FILE_READ_DATA,
        FILE_TYPE_DISK, FILE_WRITE_DATA, VOLUME_NAME_DOS,
    };

    const DATA_ACCESS: u32 = FILE_READ_DATA | FILE_WRITE_DATA | FILE_APPEND_DATA;
    let file_type = windows_handles::file_object_type();
    let mut name = vec![0u16; 32_768];
    windows_handles::snapshot()
        .into_iter()
        .filter(|entry| {
            entry.object_type_index == file_type && entry.granted_access & DATA_ACCESS != 0
        })
        .filter_map(|entry| {
            let handle = entry.handle_value;
            // SAFETY: none of these reads or writes through the handle or
            // closes it; one closed since the snapshot only fails them.
            unsafe {
                if GetFileType(handle) != FILE_TYPE_DISK {
                    return None;
                }
                let mut information: BY_HANDLE_FILE_INFORMATION = std::mem::zeroed();
                if GetFileInformationByHandle(handle, &mut information) == 0
                    || information.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY != 0
                {
                    return None;
                }
                let length = GetFinalPathNameByHandleW(
                    handle,
                    name.as_mut_ptr(),
                    name.len() as u32,
                    VOLUME_NAME_DOS,
                ) as usize;
                (length > 0 && length < name.len())
                    .then(|| PathBuf::from(std::ffi::OsString::from_wide(&name[..length])))
            }
        })
        .collect()
}

#[cfg(windows)]
mod windows_handles {
    use windows_sys::Wdk::System::Threading::{
        NtQueryInformationProcess, ProcessHandleInformation,
    };
    use windows_sys::Win32::Foundation::{HANDLE, STATUS_INFO_LENGTH_MISMATCH};
    use windows_sys::Win32::System::Threading::GetCurrentProcess;

    /// `PROCESS_HANDLE_TABLE_ENTRY_INFO`, one handle of a snapshot.
    #[repr(C)]
    #[derive(Clone, Copy)]
    pub(super) struct HandleEntry {
        pub(super) handle_value: HANDLE,
        handle_count: usize,
        pointer_count: usize,
        pub(super) granted_access: u32,
        pub(super) object_type_index: u32,
        handle_attributes: u32,
        reserved: u32,
    }

    /// Every handle of this process at one instant.
    pub(super) fn snapshot() -> Vec<HandleEntry> {
        // `PROCESS_HANDLE_SNAPSHOT_INFORMATION`: a handle count and a reserved
        // word, then the entries. Grown until the snapshot fits; usize words
        // keep the entries aligned.
        let header = 2 * std::mem::size_of::<usize>();
        let mut buffer: Vec<usize> = vec![0; 4096];
        loop {
            let bytes = std::mem::size_of_val(buffer.as_slice()) as u32;
            let mut needed = 0u32;
            // SAFETY: the pseudo-handle for this process needs no closing, and
            // the call writes at most `bytes` into the buffer it is given.
            let status = unsafe {
                NtQueryInformationProcess(
                    GetCurrentProcess(),
                    ProcessHandleInformation,
                    buffer.as_mut_ptr().cast(),
                    bytes,
                    &mut needed,
                )
            };
            if status == STATUS_INFO_LENGTH_MISMATCH {
                let words =
                    (needed as usize).max(bytes as usize * 2) / std::mem::size_of::<usize>() + 1;
                buffer = vec![0; words];
                continue;
            }
            assert!(
                status >= 0,
                "snapshot this process's handles: status {status:#x}"
            );
            break;
        }
        let count = buffer[0];
        // SAFETY: the snapshot holds `count` entries right after its header,
        // inside the buffer the call filled.
        unsafe {
            std::slice::from_raw_parts(
                buffer
                    .as_ptr()
                    .cast::<u8>()
                    .add(header)
                    .cast::<HandleEntry>(),
                count,
            )
        }
        .to_vec()
    }

    /// The kernel's type number for file objects, read once off a file this
    /// opens for the purpose: it names the type in every later snapshot.
    pub(super) fn file_object_type() -> u32 {
        use std::os::windows::io::AsRawHandle;
        static FILE_TYPE: std::sync::OnceLock<u32> = std::sync::OnceLock::new();
        *FILE_TYPE.get_or_init(|| {
            let probe = std::fs::File::open(std::env::current_exe().expect("this executable"))
                .expect("open this executable");
            let value = probe.as_raw_handle() as HANDLE;
            snapshot()
                .into_iter()
                .find(|entry| entry.handle_value == value)
                .expect("an open file is in its process's snapshot")
                .object_type_index
        })
    }
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

    /// A handle on a directory is not a file held open: resolving a path
    /// takes one for a moment on Windows, on any thread.
    #[test]
    fn a_directory_handle_is_not_an_open_file() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("inner")).unwrap();
        let path = dir.path().join("a");
        std::fs::write(&path, b"a").unwrap();
        let path = path.canonicalize().unwrap();

        let directories = [
            open_directory(dir.path()),
            open_directory(&dir.path().join("inner")),
        ];
        let file = std::fs::File::open(&path).unwrap();
        assert_eq!(open_files_under(dir.path()), vec![path]);
        drop((directories, file));
        assert_no_open_files_under(dir.path());
    }

    fn open_directory(path: &Path) -> std::fs::File {
        let mut options = std::fs::OpenOptions::new();
        options.read(true);
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            // A directory opens only with backup semantics.
            options.custom_flags(0x0200_0000);
        }
        options.open(path).unwrap()
    }
}

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
/// Panics if the directory cannot be resolved or enumeration fails. Descriptors
/// closed while being inspected may disappear from the result.
pub fn open_files_under(dir: &Path) -> Vec<PathBuf> {
    let dir = dir
        .canonicalize()
        .unwrap_or_else(|error| panic!("canonicalize {}: {error}", dir.display()));
    #[cfg(not(windows))]
    let open = open_file_paths()
        .into_iter()
        .filter(|path| path.starts_with(&dir))
        .collect();
    #[cfg(windows)]
    let open = open_file_paths_under(&dir);
    open
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
        .map(|entry| {
            entry
                .expect("read /dev/fd entry")
                .file_name()
                .to_str()
                .expect("descriptor name is UTF-8")
                .parse()
                .expect("descriptor name is a number")
        })
        .collect();
    descriptors
        .into_iter()
        .filter_map(|fd| {
            // SAFETY: the descriptor is only asked for its type and path. One
            // closed since the listing (the listing's own among them) fails the
            // call.
            let fd = unsafe { BorrowedFd::borrow_raw(fd) };
            let stat = match rustix::fs::fstat(fd) {
                Ok(stat) => stat,
                Err(rustix::io::Errno::BADF) => {
                    tracing::debug!(?fd, "descriptor closed during open-file enumeration");
                    return None;
                }
                Err(error) => panic!("inspect descriptor {fd:?}: {error}"),
            };
            if rustix::fs::FileType::from_raw_mode(stat.st_mode) == rustix::fs::FileType::Directory
            {
                return None;
            }
            let path = match rustix::fs::getpath(fd) {
                Ok(path) => path,
                Err(rustix::io::Errno::BADF | rustix::io::Errno::NOENT) => {
                    // Closed descriptors and anonymous pipes, sockets or
                    // event descriptors have no filesystem path. Named pipes do.
                    tracing::debug!(?fd, "descriptor has no current file path");
                    return None;
                }
                Err(error) => panic!("resolve descriptor {fd:?}: {error}"),
            };
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
            let link = entry.expect("read /proc/self/fd entry").path();
            // The link's own metadata is the descriptor's, followed through.
            let metadata = match std::fs::metadata(&link) {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    tracing::debug!(path = %link.display(), "descriptor closed during open-file enumeration");
                    return None;
                }
                Err(error) => panic!("inspect descriptor {}: {error}", link.display()),
            };
            if metadata.is_dir() {
                return None;
            }
            match std::fs::read_link(&link) {
                Ok(path) => Some(path),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    tracing::debug!(path = %link.display(), "descriptor closed during open-file enumeration");
                    None
                }
                Err(error) => panic!("resolve descriptor {}: {error}", link.display()),
            }
        })
        .collect()
}

/// The disk files under `dir` this process holds open for their data, read
/// from one snapshot of its handles that the kernel takes at once.
///
/// Paths are named after the snapshot, one handle at a time, and a handle
/// value freed meanwhile can be taken by a new handle. So only values that
/// were file handles in the snapshot are named: a file opened since then under
/// a value that held something else is never counted beside the one it
/// replaced, and the list never names more files than were open at the
/// snapshot. A handle opened only to read attributes, as `std::fs::metadata`
/// and `canonicalize` do here and no Unix descriptor stands for, holds no file
/// open, and neither does one on a directory. Directories are told apart
/// before naming: one being deleted can't be named while it moves out of its
/// parent, and a watched directory's handle stays open through that.
///
/// Handles are named by their NT device path (`\Device\HarddiskVolume3\...`),
/// which needs no drive-letter lookup; CI runners hold handles whose drive-letter
/// name is refused. The directory gets its NT path the same way, and matches
/// are reported under `dir`'s own spelling.
#[cfg(windows)]
fn open_file_paths_under(dir: &Path) -> Vec<PathBuf> {
    use windows_sys::Win32::Foundation::ERROR_ACCESS_DENIED;
    use windows_sys::Win32::Storage::FileSystem::{
        GetFileType, FILE_APPEND_DATA, FILE_READ_DATA, FILE_TYPE_DISK, FILE_TYPE_UNKNOWN,
        FILE_WRITE_DATA,
    };

    const DATA_ACCESS: u32 = FILE_READ_DATA | FILE_WRITE_DATA | FILE_APPEND_DATA;
    let nt_dir = nt_directory_name(dir);
    let file_type = windows_handles::file_object_type();
    let mut name = vec![0u16; 32_768];
    windows_handles::snapshot()
        .into_iter()
        .filter(|entry| {
            entry.object_type_index == file_type && entry.granted_access & DATA_ACCESS != 0
        })
        .filter_map(|entry| {
            let handle = entry.handle_value;
            // SAFETY: the call only reads the handle's type; one closed since
            // the snapshot only fails it.
            unsafe {
                let kind = GetFileType(handle);
                if kind == FILE_TYPE_UNKNOWN {
                    let error = std::io::Error::last_os_error();
                    // UNKNOWN with NO_ERROR is a successful report of an
                    // unknown type, which is not a disk file to enumerate.
                    if error.raw_os_error() != Some(0) {
                        closed_handle_or_panic(
                            "inspect file type",
                            handle,
                            entry.granted_access,
                            error,
                        );
                    }
                    return None;
                }
                if kind != FILE_TYPE_DISK {
                    return None;
                }
            }
            match standard_information(handle) {
                Ok(information) if information.Directory => return None,
                Ok(_) => {}
                Err(error) => {
                    closed_handle_or_panic(
                        "inspect file handle",
                        handle,
                        entry.granted_access,
                        error,
                    );
                    return None;
                }
            }
            let nt_name = match nt_handle_name(handle, &mut name) {
                Ok(nt_name) => nt_name,
                Err(error) => {
                    // A file deleted while open has no name left under any
                    // directory, so it is skipped. A failed query answers no,
                    // so the failure stays loud.
                    if error.raw_os_error() == Some(ERROR_ACCESS_DENIED as i32)
                        && standard_information(handle)
                            .is_ok_and(|information| information.DeletePending)
                    {
                        tracing::debug!(?handle, "skipping a deleted file still open");
                        return None;
                    }
                    closed_handle_or_panic(
                        "resolve file handle",
                        handle,
                        entry.granted_access,
                        error,
                    );
                    return None;
                }
            };
            Some(dir.join(nt_suffix_under(&nt_name, &nt_dir)?))
        })
        .collect()
}

/// The NT device path of an open handle: normalized when the system allows it,
/// otherwise the name the handle was opened with.
#[cfg(windows)]
fn nt_handle_name(
    handle: windows_sys::Win32::Foundation::HANDLE,
    buffer: &mut [u16],
) -> std::io::Result<String> {
    use windows_sys::Win32::Foundation::ERROR_ACCESS_DENIED;
    use windows_sys::Win32::Storage::FileSystem::{
        GetFinalPathNameByHandleW, FILE_NAME_NORMALIZED, FILE_NAME_OPENED, VOLUME_NAME_NT,
    };

    let mut last = None;
    for flags in [
        VOLUME_NAME_NT | FILE_NAME_NORMALIZED,
        VOLUME_NAME_NT | FILE_NAME_OPENED,
    ] {
        // SAFETY: the call writes at most `buffer.len()` units into `buffer`
        // and only reads the handle's name; a closed handle only fails it.
        let length = unsafe {
            GetFinalPathNameByHandleW(handle, buffer.as_mut_ptr(), buffer.len() as u32, flags)
        } as usize;
        if length != 0 {
            assert!(length < buffer.len(), "open file path exceeds its buffer");
            return Ok(String::from_utf16_lossy(&buffer[..length]));
        }
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() != Some(ERROR_ACCESS_DENIED as i32) {
            return Err(error);
        }
        tracing::debug!(?handle, flags, "file name refused; trying the opened name");
        last = Some(error);
    }
    Err(last.expect("at least one naming attempt ran"))
}

/// The NT device path of `dir`, through a handle that opens no file data.
#[cfg(windows)]
fn nt_directory_name(dir: &Path) -> String {
    use std::os::windows::fs::OpenOptionsExt;
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_BACKUP_SEMANTICS;

    let directory = std::fs::OpenOptions::new()
        .access_mode(0)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(dir)
        .unwrap_or_else(|error| panic!("open {}: {error}", dir.display()));
    let mut buffer = vec![0u16; 32_768];
    nt_handle_name(directory.as_raw_handle() as _, &mut buffer)
        .unwrap_or_else(|error| panic!("resolve {}: {error}", dir.display()))
}

/// The part of `nt_name` below `nt_dir`, compared without case as Windows
/// names are; None for a name outside it.
#[cfg(windows)]
fn nt_suffix_under(nt_name: &str, nt_dir: &str) -> Option<PathBuf> {
    let dir = nt_dir.trim_end_matches('\\');
    let head = nt_name.get(..dir.len())?;
    let rest = &nt_name[dir.len()..];
    (head.eq_ignore_ascii_case(dir) && rest.starts_with('\\')).then(|| PathBuf::from(&rest[1..]))
}

/// Whether `handle`'s file is a directory, and whether it is deleted and
/// waiting for its last handle to close. Unlike its attributes, this needs no
/// access right, so a handle opened for data alone answers it too.
#[cfg(windows)]
fn standard_information(
    handle: windows_sys::Win32::Foundation::HANDLE,
) -> std::io::Result<windows_sys::Win32::Storage::FileSystem::FILE_STANDARD_INFO> {
    use windows_sys::Win32::Storage::FileSystem::{
        FileStandardInfo, GetFileInformationByHandleEx, FILE_STANDARD_INFO,
    };
    // SAFETY: the call only reads the handle's standard information into a
    // buffer of the size it is given; a closed handle only fails it.
    unsafe {
        let mut information: FILE_STANDARD_INFO = std::mem::zeroed();
        if GetFileInformationByHandleEx(
            handle,
            FileStandardInfo,
            (&mut information as *mut FILE_STANDARD_INFO).cast(),
            std::mem::size_of::<FILE_STANDARD_INFO>() as u32,
        ) == 0
        {
            return Err(std::io::Error::last_os_error());
        }
        Ok(information)
    }
}

#[cfg(windows)]
fn closed_handle_or_panic(
    operation: &str,
    handle: windows_sys::Win32::Foundation::HANDLE,
    granted_access: u32,
    error: std::io::Error,
) {
    assert_eq!(
        error.raw_os_error(),
        Some(windows_sys::Win32::Foundation::ERROR_INVALID_HANDLE as i32),
        "{operation} {handle:?} (granted access {granted_access:#x}): {error}"
    );
    tracing::debug!(
        ?handle,
        operation,
        "handle closed during open-file enumeration"
    );
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
#[path = "open_files_tests.rs"]
mod tests;

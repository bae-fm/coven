//! Publish an owned file under another name without replacing an existing file.

#[cfg(unix)]
use super::sync_directory;
use super::{remove as remove_file, FileError};
use std::{fs, io, path::Path};

pub(super) fn publish(source: &Path, destination: &Path) -> Result<(), FileError> {
    match fs::hard_link(source, destination) {
        Ok(()) => (),
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
            if !same_file(source, destination)? {
                return Err(exists(destination));
            }
        }
        Err(e) => return Err(FileError::at("publish download", destination, e)),
    }
    #[cfg(unix)]
    sync_directory(destination.parent().expect("resolved parent")).map_err(|source| {
        FileError::AfterReplace {
            path: destination.to_owned(),
            source,
        }
    })?;
    #[cfg(windows)]
    // FlushFileBuffers (File::sync_all) also flushes file metadata:
    // https://learn.microsoft.com/en-us/windows/win32/fileio/file-caching
    fs::OpenOptions::new()
        .write(true)
        .open(destination)
        .and_then(|file| file.sync_all())
        .map_err(|source| FileError::AfterReplace {
            path: destination.to_owned(),
            source,
        })?;
    Ok(())
}

pub(super) fn remove(source: &Path, destination: &Path) -> Result<(), FileError> {
    if same_file(source, destination)? {
        remove_file(destination)?;
    }
    Ok(())
}

fn exists(path: &Path) -> FileError {
    FileError::at(
        "publish download",
        path,
        io::Error::new(io::ErrorKind::AlreadyExists, "destination exists"),
    )
}

fn same_file(a: &Path, b: &Path) -> Result<bool, FileError> {
    let metadata = |path: &Path| match fs::symlink_metadata(path) {
        Ok(metadata) => Ok(Some(metadata)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(FileError::at("check download identity", path, e)),
    };
    let (Some(a_meta), Some(b_meta)) = (metadata(a)?, metadata(b)?) else {
        return Ok(false);
    };
    if !a_meta.is_file() || !b_meta.is_file() {
        return Ok(false);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Ok(a_meta.dev() == b_meta.dev() && a_meta.ino() == b_meta.ino())
    }
    #[cfg(windows)]
    {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Storage::FileSystem::{
            GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION,
        };
        let identity = |path: &Path| -> Result<_, FileError> {
            let file = fs::File::open(path)
                .map_err(|e| FileError::at("open download identity", path, e))?;
            let mut info = std::mem::MaybeUninit::<BY_HANDLE_FILE_INFORMATION>::zeroed();
            // SAFETY: the live handle and writable ABI-sized output remain valid for this call.
            if unsafe { GetFileInformationByHandle(file.as_raw_handle(), info.as_mut_ptr()) } == 0 {
                return Err(FileError::at(
                    "read download identity",
                    path,
                    io::Error::last_os_error(),
                ));
            }
            // SAFETY: success initialized the entire information structure.
            let info = unsafe { info.assume_init() };
            Ok((
                info.dwVolumeSerialNumber,
                info.nFileIndexHigh,
                info.nFileIndexLow,
            ))
        };
        Ok(identity(a)? == identity(b)?)
    }
}

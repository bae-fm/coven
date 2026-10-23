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

#[cfg(unix)]
#[test]
fn a_named_pipe_keeps_its_filesystem_path() {
    use std::os::unix::fs::OpenOptionsExt;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("pipe");
    // rustix does not expose mkfifo on Apple platforms.
    let status = std::process::Command::new("mkfifo")
        .arg(&path)
        .status()
        .unwrap();
    assert!(status.success(), "mkfifo failed: {status}");
    let pipe = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(rustix::fs::OFlags::NONBLOCK.bits() as i32)
        .open(&path)
        .unwrap();
    assert_eq!(
        open_files_under(dir.path()),
        vec![path.canonicalize().unwrap()]
    );
    drop(pipe);
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

/// A directory deleted while its handle stays open is still not an open
/// file: on Windows it cannot be named while it moves out of its parent, so
/// listing other directories meanwhile must not stop to name it.
#[test]
fn a_directory_deleted_while_open_is_not_an_open_file() {
    let listed = tempfile::tempdir().unwrap();
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let deleters: Vec<_> = (0..4)
        .map(|_| {
            let stop = stop.clone();
            std::thread::spawn(move || {
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let deleted = tempfile::tempdir().unwrap();
                    std::fs::create_dir(deleted.path().join("inner")).unwrap();
                    let directories = [
                        open_directory(deleted.path()),
                        open_directory(&deleted.path().join("inner")),
                    ];
                    deleted.close().unwrap();
                    drop(directories);
                }
            })
        })
        .collect();
    let listings = std::panic::catch_unwind(|| {
        (0..500)
            .map(|_| open_files_under(listed.path()))
            .collect::<Vec<_>>()
    });
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for deleter in deleters {
        deleter.join().unwrap();
    }
    let listings = listings.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    assert!(listings.iter().all(Vec::is_empty), "{listings:?}");
}

#[test]
fn directory_filter_includes_descendants_but_not_similarly_named_siblings() {
    let root = tempfile::tempdir().unwrap();
    let selected = root.path().join("store");
    let sibling = root.path().join("store-other");
    std::fs::create_dir_all(selected.join("nested")).unwrap();
    std::fs::create_dir(&sibling).unwrap();
    let inside = selected.join("nested/file");
    let outside = sibling.join("file");
    let handles = [
        std::fs::File::create(&inside).unwrap(),
        std::fs::File::create(&outside).unwrap(),
    ];
    assert_eq!(
        open_files_under(&selected),
        vec![inside.canonicalize().unwrap()]
    );
    drop(handles);
    assert_no_open_files_under(&selected);
}

#[test]
fn assertion_names_a_file_that_is_still_open() {
    let dir = tempfile::tempdir().unwrap();
    let file = std::fs::File::create(dir.path().join("still-open")).unwrap();
    let panic = std::panic::catch_unwind(|| assert_no_open_files_under(dir.path())).unwrap_err();
    let message = panic.downcast_ref::<String>().unwrap();
    assert!(message.contains("still-open"), "{message}");
    drop(file);
    assert_no_open_files_under(dir.path());
}

#[test]
fn a_missing_directory_is_not_reported_as_having_no_open_files() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("absent");
    assert!(std::panic::catch_unwind(|| open_files_under(&missing)).is_err());
    assert!(std::panic::catch_unwind(|| assert_no_open_files_under(&missing)).is_err());
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

#[cfg(windows)]
mod probe {
    use super::*;
    use std::collections::BTreeMap;
    use std::os::windows::fs::OpenOptionsExt;
    use std::os::windows::io::AsRawHandle;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use windows_sys::Win32::Foundation::HANDLE;
    use windows_sys::Win32::Storage::FileSystem::*;

    fn name(handle: HANDLE, flags: u32) -> Result<String, i32> {
        let mut buffer = vec![0u16; 32_768];
        let length = unsafe {
            GetFinalPathNameByHandleW(handle, buffer.as_mut_ptr(), buffer.len() as u32, flags)
        } as usize;
        if length == 0 {
            Err(std::io::Error::last_os_error().raw_os_error().unwrap())
        } else {
            Ok(String::from_utf16_lossy(&buffer[..length]))
        }
    }

    fn standard(handle: HANDLE) -> String {
        let mut standard: FILE_STANDARD_INFO = unsafe { std::mem::zeroed() };
        if unsafe {
            GetFileInformationByHandleEx(
                handle,
                FileStandardInfo,
                (&mut standard as *mut FILE_STANDARD_INFO).cast(),
                std::mem::size_of::<FILE_STANDARD_INFO>() as u32,
            )
        } != 0
        {
            format!(
                "pending {} dir {} links {}",
                standard.DeletePending, standard.Directory, standard.NumberOfLinks
            )
        } else {
            format!("std ERR {}", std::io::Error::last_os_error())
        }
    }

    fn watch(path: &Path) -> std::fs::File {
        std::fs::OpenOptions::new()
            .access_mode(FILE_LIST_DIRECTORY)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OVERLAPPED)
            .open(path)
            .unwrap()
    }

    fn legacy_delete(path: &Path, directory: bool) -> std::io::Result<()> {
        let mut options = std::fs::OpenOptions::new();
        options.access_mode(DELETE);
        if directory {
            options.custom_flags(FILE_FLAG_BACKUP_SEMANTICS);
        }
        let file = options.open(path)?;
        let info = FILE_DISPOSITION_INFO { DeleteFile: true };
        if unsafe {
            SetFileInformationByHandle(
                file.as_raw_handle() as HANDLE,
                FileDispositionInfo,
                (&info as *const FILE_DISPOSITION_INFO).cast(),
                std::mem::size_of::<FILE_DISPOSITION_INFO>() as u32,
            )
        } == 0
        {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }

    fn churn(mode: &str, stop: &AtomicBool) {
        while !stop.load(Ordering::Relaxed) {
            let root = tempfile::tempdir().unwrap();
            let r = root.path().to_path_buf();
            let x = r.join("x");
            let y = x.join("y");
            std::fs::create_dir_all(&y).unwrap();
            std::fs::write(y.join("f"), b"f").unwrap();
            let handles = (
                watch(&y),
                watch(&x),
                watch(&r),
                std::fs::File::open(y.join("f")).unwrap(),
                std::fs::OpenOptions::new()
                    .access_mode(FILE_READ_DATA)
                    .open(y.join("f"))
                    .unwrap(),
            );
            match mode {
                "posix" => {
                    std::fs::remove_dir_all(&r).unwrap();
                }
                "legacy" => {
                    std::fs::create_dir(r.join("z")).unwrap();
                    let z = watch(&r.join("z"));
                    legacy_delete(&y.join("f"), false).unwrap();
                    legacy_delete(&r.join("z"), true).unwrap();
                    std::thread::yield_now();
                    drop(z);
                }
                "rename" => {
                    for n in 0..5 {
                        if let Err(error) = std::fs::rename(&y, x.join(format!("y{n}")))
                            .and_then(|()| std::fs::rename(x.join(format!("y{n}")), &y))
                        {
                            eprintln!("RENAME y {error}");
                        }
                        if let Err(error) = std::fs::rename(&x, r.join("x2"))
                            .and_then(|()| std::fs::rename(r.join("x2"), &x))
                        {
                            eprintln!("RENAME x {error}");
                            break;
                        }
                    }
                }
                _ => unreachable!(),
            }
            drop(handles);
            drop(root);
        }
    }

    #[test]
    fn probe_races() {
        let file_type = windows_handles::file_object_type();
        for mode in ["posix", "legacy", "rename"] {
            let stop = Arc::new(AtomicBool::new(false));
            let failures: Arc<Mutex<BTreeMap<String, (usize, Vec<String>)>>> = Default::default();
            let named = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let mut threads = Vec::new();
            for _ in 0..4 {
                let stop = stop.clone();
                threads.push(std::thread::spawn(move || churn(mode, &stop)));
            }
            for _ in 0..2 {
                let stop = stop.clone();
                let failures = failures.clone();
                let named = named.clone();
                threads.push(std::thread::spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        for entry in windows_handles::snapshot() {
                            if entry.object_type_index != file_type || entry.granted_access & 3 == 0 {
                                continue;
                            }
                            let handle = entry.handle_value;
                            if unsafe { GetFileType(handle) } != FILE_TYPE_DISK {
                                continue;
                            }
                            let before = standard(handle);
                            if before.contains("dir true") {
                                continue;
                            }
                            match name(handle, VOLUME_NAME_NT | FILE_NAME_NORMALIZED) {
                                Ok(_) => {
                                    named.fetch_add(1, Ordering::Relaxed);
                                }
                                Err(code) => {
                                    let after = standard(handle);
                                    let opened = name(handle, VOLUME_NAME_NT | FILE_NAME_OPENED);
                                    let again = name(handle, VOLUME_NAME_NT | FILE_NAME_NORMALIZED);
                                    let mut failures = failures.lock().unwrap();
                                    let slot = failures
                                        .entry(format!("file normalized error {code} (before {before}; after {})", after.split(" links").next().unwrap()))
                                        .or_default();
                                    slot.0 += 1;
                                    if slot.1.len() < 6 {
                                        slot.1.push(format!(
                                            "{handle:?} access {:#x}: {after}; opened {opened:?}; normalized again {again:?}",
                                            entry.granted_access
                                        ));
                                    }
                                }
                            }
                        }
                    }
                }));
            }
            {
                let stop = stop.clone();
                let failures = failures.clone();
                threads.push(std::thread::spawn(move || {
                    let other = tempfile::tempdir().unwrap();
                    while !stop.load(Ordering::Relaxed) {
                        if let Err(panic) =
                            std::panic::catch_unwind(|| open_files_under(other.path()))
                        {
                            let message =
                                panic.downcast_ref::<String>().cloned().unwrap_or_default();
                            let mut failures = failures.lock().unwrap();
                            let slot = failures.entry("open_files_under panic".into()).or_default();
                            slot.0 += 1;
                            if slot.1.len() < 6 {
                                slot.1.push(message);
                            }
                        }
                    }
                }));
            }
            std::thread::sleep(std::time::Duration::from_secs(20));
            stop.store(true, Ordering::Relaxed);
            for thread in threads {
                thread.join().unwrap();
            }
            eprintln!("RACE {mode}: named {}", named.load(Ordering::Relaxed));
            for (key, (count, examples)) in failures.lock().unwrap().iter() {
                eprintln!("RACE {mode}: {key}: {count}");
                for example in examples {
                    eprintln!("    {example}");
                }
            }
        }
        panic!("probe output above");
    }
}

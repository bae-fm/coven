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
    use std::os::windows::fs::OpenOptionsExt;
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Foundation::HANDLE;
    use windows_sys::Win32::Storage::FileSystem::*;

    fn name(handle: HANDLE, flags: u32) -> String {
        let mut buffer = vec![0u16; 32_768];
        let length = unsafe {
            GetFinalPathNameByHandleW(handle, buffer.as_mut_ptr(), buffer.len() as u32, flags)
        } as usize;
        if length == 0 {
            format!("ERR {}", std::io::Error::last_os_error())
        } else {
            String::from_utf16_lossy(&buffer[..length])
        }
    }

    fn describe(label: &str, handle: HANDLE) {
        let access = windows_handles::snapshot()
            .into_iter()
            .find(|entry| entry.handle_value == handle)
            .map(|entry| {
                format!(
                    "{:#x} type {}",
                    entry.granted_access, entry.object_type_index
                )
            });
        let kind = unsafe { GetFileType(handle) };
        let mut information: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };
        let info = if unsafe { GetFileInformationByHandle(handle, &mut information) } != 0 {
            format!(
                "attrs {:#x} links {}",
                information.dwFileAttributes, information.nNumberOfLinks
            )
        } else {
            format!("ERR {}", std::io::Error::last_os_error())
        };
        let mut standard: FILE_STANDARD_INFO = unsafe { std::mem::zeroed() };
        let std_info = if unsafe {
            GetFileInformationByHandleEx(
                handle,
                FileStandardInfo,
                (&mut standard as *mut FILE_STANDARD_INFO).cast(),
                std::mem::size_of::<FILE_STANDARD_INFO>() as u32,
            )
        } != 0
        {
            format!(
                "delete_pending {} directory {} links {}",
                standard.DeletePending, standard.Directory, standard.NumberOfLinks
            )
        } else {
            format!("ERR {}", std::io::Error::last_os_error())
        };
        let other = tempfile::tempdir().unwrap();
        let listed = std::panic::catch_unwind(|| open_files_under(other.path()))
            .map(|v| format!("{v:?}"))
            .unwrap_or_else(|p| p.downcast_ref::<String>().cloned().unwrap_or_default());
        eprintln!(
            "PROBE {label}: access {access:?} filetype {kind}\n  nt|norm {}\n  nt|opened {}\n  dos|norm {}\n  dos|opened {}\n  info {info}\n  std {std_info}\n  open_files_under(other) {listed}",
            name(handle, VOLUME_NAME_NT | FILE_NAME_NORMALIZED),
            name(handle, VOLUME_NAME_NT | FILE_NAME_OPENED),
            name(handle, VOLUME_NAME_DOS | FILE_NAME_NORMALIZED),
            name(handle, VOLUME_NAME_DOS | FILE_NAME_OPENED),
        );
    }

    /// The handle notify's Windows watcher holds on a watched directory.
    fn watch(path: &Path) -> std::fs::File {
        std::fs::OpenOptions::new()
            .access_mode(FILE_LIST_DIRECTORY)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OVERLAPPED)
            .open(path)
            .unwrap()
    }

    fn h(file: &std::fs::File) -> HANDLE {
        file.as_raw_handle() as HANDLE
    }

    #[test]
    fn probe_handle_states() {
        let root = tempfile::tempdir().unwrap();
        let r = root.path();

        // A: watched empty directory removed.
        std::fs::create_dir(r.join("a")).unwrap();
        let a = watch(&r.join("a"));
        describe("A watched dir before", h(&a));
        eprintln!("A remove_dir {:?}", std::fs::remove_dir(r.join("a")));
        describe("A watched dir after remove_dir", h(&a));

        // B: watched directory whose parent tree is removed.
        std::fs::create_dir_all(r.join("b/inner")).unwrap();
        let b = watch(&r.join("b/inner"));
        eprintln!(
            "B remove_dir_all {:?}",
            std::fs::remove_dir_all(r.join("b"))
        );
        describe("B watched inner dir after parent remove_dir_all", h(&b));

        // F: watched directory holding files, removed whole.
        std::fs::create_dir(r.join("f")).unwrap();
        std::fs::write(r.join("f/x"), b"x").unwrap();
        let f = watch(&r.join("f"));
        eprintln!(
            "F remove_dir_all {:?}",
            std::fs::remove_dir_all(r.join("f"))
        );
        describe("F watched dir after remove_dir_all", h(&f));

        // C: open file removed.
        std::fs::create_dir(r.join("c")).unwrap();
        std::fs::write(r.join("c/f"), b"c").unwrap();
        let c = std::fs::File::open(r.join("c/f")).unwrap();
        describe("C file before", h(&c));
        eprintln!("C remove_file {:?}", std::fs::remove_file(r.join("c/f")));
        describe("C file after remove_file", h(&c));
        eprintln!("C remove_dir parent {:?}", std::fs::remove_dir(r.join("c")));
        describe("C file after parent remove_dir", h(&c));

        // D: open file whose directory is removed whole.
        std::fs::create_dir(r.join("d")).unwrap();
        std::fs::write(r.join("d/f"), b"d").unwrap();
        let d = std::fs::File::open(r.join("d/f")).unwrap();
        eprintln!(
            "D remove_dir_all {:?}",
            std::fs::remove_dir_all(r.join("d"))
        );
        describe("D file after dir remove_dir_all", h(&d));

        // E: file opened for read data only, its directory removed whole.
        std::fs::create_dir(r.join("e")).unwrap();
        std::fs::write(r.join("e/f"), b"e").unwrap();
        let e = std::fs::OpenOptions::new()
            .access_mode(FILE_READ_DATA)
            .open(r.join("e/f"))
            .unwrap();
        describe("E read-data file before", h(&e));
        eprintln!(
            "E remove_dir_all {:?}",
            std::fs::remove_dir_all(r.join("e"))
        );
        describe("E read-data file after dir remove_dir_all", h(&e));

        // G: a read_dir in progress.
        std::fs::create_dir(r.join("g")).unwrap();
        std::fs::write(r.join("g/1"), b"1").unwrap();
        std::fs::write(r.join("g/2"), b"2").unwrap();
        let before: Vec<_> = windows_handles::snapshot()
            .iter()
            .map(|e| e.handle_value as usize)
            .collect();
        let mut listing = std::fs::read_dir(r.join("g")).unwrap();
        listing.next();
        for entry in windows_handles::snapshot() {
            if !before.contains(&(entry.handle_value as usize)) {
                describe("G read_dir handle", entry.handle_value);
            }
        }
        eprintln!(
            "G remove_dir_all {:?}",
            std::fs::remove_dir_all(r.join("g"))
        );
        for entry in windows_handles::snapshot() {
            if !before.contains(&(entry.handle_value as usize)) {
                describe("G read_dir handle after remove_dir_all", entry.handle_value);
            }
        }
        drop(listing);

        // H: whole root removed with watches on it.
        let root_path = r.to_path_buf();
        let top = watch(&root_path);
        drop(root);
        eprintln!("H root exists after drop {}", root_path.exists());
        describe("H watched root after tempdir drop", h(&top));

        drop((a, b, c, d, e, f, top));
        panic!("probe output above");
    }
}

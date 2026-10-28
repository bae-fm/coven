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

/// A handle closed while the files are listed leaves its number free, and the
/// next object opened on any thread can take it: a listing part-way through
/// that number then reads the new object. Pipes and sockets are not files
/// under the directory, so the listing passes over them as it does a closed
/// handle.
#[test]
fn a_handle_number_taken_by_another_object_while_listing_is_passed_over() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a");
    std::fs::write(&path, b"a").unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let churn = std::thread::spawn({
        let stop = stop.clone();
        move || {
            while !stop.load(Ordering::Relaxed) {
                drop(std::fs::File::open(&path).unwrap());
                drop(std::io::pipe().unwrap());
                drop(std::net::UdpSocket::bind("127.0.0.1:0").unwrap());
            }
        }
    });
    let listed = std::panic::catch_unwind(|| {
        for _ in 0..5_000 {
            open_files_under(dir.path());
        }
    });
    stop.store(true, Ordering::Relaxed);
    churn.join().unwrap();
    if let Err(panic) = listed {
        std::panic::resume_unwind(panic);
    }
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

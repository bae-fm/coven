use super::*;

#[test]
fn the_walk_lists_the_held_files_of_every_subfolder() {
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(root.path().join("a/b")).unwrap();
    for name in ["held", "free", "a/held-too", "a/b/free", "a/b/held-deep"] {
        std::fs::write(root.path().join(name), b"x").unwrap();
    }
    let mut listed = walk_files(root.path(), &|path| {
        Ok(path.file_name().unwrap().to_string_lossy().starts_with("held"))
    })
    .unwrap();
    listed.sort();
    let names: Vec<_> = listed
        .iter()
        .map(|path| path.strip_prefix(root.path()).unwrap().to_string_lossy().replace('\\', "/"))
        .collect();
    assert_eq!(names, ["a/b/held-deep", "a/held-too", "held"]);
}

#[test]
fn a_failure_with_no_held_file_is_an_io_failure_naming_the_directory() {
    let root = tempfile::tempdir().unwrap();
    let error = directory_failure(
        "remove store directory",
        root.path(),
        io::Error::from(io::ErrorKind::PermissionDenied),
    );
    assert!(matches!(error, FileError::Io { path, .. } if path == root.path()));
}

#[test]
fn a_failure_naming_held_files_says_which_are_open() {
    let error = FileError::HeldOpen {
        operation: "remove store directory",
        path: PathBuf::from("/stores/one"),
        held: vec![PathBuf::from("/stores/one/db"), PathBuf::from("/stores/one/log")],
        source: Box::new(io::Error::from(io::ErrorKind::PermissionDenied)),
    };
    let text = error.to_string();
    assert!(text.starts_with("remove store directory /stores/one: "), "{text}");
    assert!(text.ends_with("open files: /stores/one/db, /stores/one/log"), "{text}");
}

#[test]
fn a_walk_that_cannot_read_a_file_fails_instead_of_skipping_it() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("file"), b"x").unwrap();
    let error = walk_files(root.path(), &|_| Err(io::Error::other("denied"))).unwrap_err();
    assert_eq!(error.to_string(), "denied");
}

use super::*;

#[test]
fn publication_and_abandonment_resume_without_touching_another_file() {
    let root = tempfile::tempdir().unwrap();
    let stage = root.path().join("staged");
    let destination = root.path().join("destination");
    let download = download(&root, stage.clone(), destination.clone());
    download.staged.replace(b"download").unwrap();
    download.publish().unwrap();
    download.publish().unwrap();
    assert_eq!(fs::read(&destination).unwrap(), b"download");
    download.remove_unused().unwrap();
    download.remove_unused().unwrap();
    assert!(!destination.exists());
    download.staged.replace(b"download").unwrap();
    fs::write(&destination, b"other").unwrap();
    assert!(
        matches!(download.publish(), Err(FileError::Io { source, .. }) if source.kind() == io::ErrorKind::AlreadyExists)
    );
    download.remove_unused().unwrap();
    assert_eq!(fs::read(&destination).unwrap(), b"other");
    assert!(!stage.exists());
}

#[test]
fn accepting_a_user_original_releases_only_the_temporary_link() {
    let root = tempfile::tempdir().unwrap();
    let stage = root.path().join("stage");
    let path = DownloadFile::check_destination(&root.path().join("original")).unwrap();
    let download = download(&root, stage.clone(), path.clone());
    download.staged.replace(b"accepted").unwrap();
    download.publish().unwrap();
    download.remove_staging().unwrap();
    download.remove_staging().unwrap();
    assert!(!stage.exists());
    assert_eq!(fs::read(&path).unwrap(), b"accepted");
    assert!(DownloadFile::check_destination(&path).is_err());
}

#[cfg(unix)]
#[test]
fn dangling_symlinks_are_existing_destinations() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("link");
    std::os::unix::fs::symlink(root.path().join("absent"), &path).unwrap();
    assert!(
        matches!(DownloadFile::check_destination(&path), Err(FileError::Io { source, .. }) if source.kind() == io::ErrorKind::AlreadyExists)
    );
}

fn download(root: &tempfile::TempDir, stage: PathBuf, destination: PathBuf) -> DownloadFile {
    let store = crate::files::StoreLayout::new(root.path().into())
        .create_store_dir(
            crate::id_source::StoreId(uuid::Uuid::from_u128(1)),
            "Download",
            &crate::id_source::UuidIds,
        )
        .unwrap();
    DownloadFile::new(
        AtomicFile::new(stage),
        Some(destination),
        store.lock_read_only().unwrap(),
    )
}

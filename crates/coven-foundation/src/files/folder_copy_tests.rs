use super::*;

fn relative(path: &str) -> RelativePath {
    RelativePath::new(path).unwrap()
}

#[tokio::test]
async fn a_plan_makes_each_missing_folder_once_outermost_first() {
    let root = tempfile::tempdir().unwrap();
    let folder = root.path().join("Music");
    fs::create_dir(&folder).unwrap();
    fs::create_dir(folder.join("Existing")).unwrap();
    let plan = plan_copies(
        folder.clone(),
        vec![
            relative("Abbey Road/Disc 1/01 Come Together.flac"),
            relative("Abbey Road/Disc 1/02 Something.flac"),
            relative("Existing/cover.jpg"),
        ],
    )
    .await
    .unwrap();
    assert_eq!(
        plan,
        CopyPlan {
            folders: vec![
                folder.join("Abbey Road"),
                folder.join("Abbey Road").join("Disc 1")
            ],
            files: vec![
                folder
                    .join("Abbey Road")
                    .join("Disc 1")
                    .join("01 Come Together.flac"),
                folder
                    .join("Abbey Road")
                    .join("Disc 1")
                    .join("02 Something.flac"),
                folder.join("Existing").join("cover.jpg"),
            ],
        }
    );
    // Planning makes nothing.
    assert!(!folder.join("Abbey Road").exists());
}

#[tokio::test]
async fn a_plan_refuses_paths_it_cant_write_without_harm() {
    let root = tempfile::tempdir().unwrap();
    let folder = root.path().join("Music");
    fs::create_dir(&folder).unwrap();
    fs::write(folder.join("taken.flac"), b"the person's").unwrap();
    fs::write(folder.join("file"), b"not a folder").unwrap();
    let refused = |paths: Vec<&'static str>| {
        let folder = folder.clone();
        async move {
            plan_copies(folder, paths.into_iter().map(relative).collect())
                .await
                .unwrap_err()
        }
    };
    assert!(matches!(
        refused(vec!["taken.flac"]).await,
        CopyPathError::Exists(path) if path == folder.join("taken.flac")
    ));
    assert!(matches!(
        refused(vec!["file/track.flac"]).await,
        CopyPathError::NotAFolder(path) if path == folder.join("file")
    ));
    assert!(matches!(
        refused(vec!["album/track.flac", "album/track.flac"]).await,
        CopyPathError::Exists(path) if path == folder.join("album").join("track.flac")
    ));
    assert!(matches!(
        refused(vec!["album", "album/track.flac"]).await,
        CopyPathError::Exists(path) if path == folder.join("album")
    ));
    assert!(matches!(
        refused(vec!["album/track.flac", "album"]).await,
        CopyPathError::Exists(path) if path == folder.join("album")
    ));
    assert!(matches!(
        plan_copies("Music".into(), vec![relative("track.flac")]).await,
        Err(CopyPathError::NotAFolder(path)) if path == Path::new("Music")
    ));
    assert!(matches!(
        plan_copies(root.path().join("missing"), vec![relative("track.flac")]).await,
        Err(CopyPathError::NotAFolder(_))
    ));
}

#[cfg(unix)]
#[tokio::test]
async fn a_plan_refuses_a_link_below_the_folder_but_not_the_folder_itself() {
    let root = tempfile::tempdir().unwrap();
    let real = root.path().join("real");
    fs::create_dir_all(real.join("inside")).unwrap();
    let folder = root.path().join("chosen");
    std::os::unix::fs::symlink(&real, &folder).unwrap();
    std::os::unix::fs::symlink(real.join("inside"), real.join("linked")).unwrap();
    let plan = plan_copies(folder.clone(), vec![relative("inside/track.flac")])
        .await
        .unwrap();
    assert_eq!(plan.files, vec![folder.join("inside").join("track.flac")]);
    assert!(matches!(
        plan_copies(folder.clone(), vec![relative("linked/track.flac")]).await,
        Err(CopyPathError::Link(path)) if path == folder.join("linked")
    ));
    std::os::unix::fs::symlink(real.join("nowhere"), real.join("dangling.flac")).unwrap();
    assert!(matches!(
        plan_copies(folder, vec![relative("dangling.flac")]).await,
        Err(CopyPathError::Exists(_))
    ));
}

#[tokio::test]
async fn a_copy_appears_at_its_path_only_whole() {
    let root = tempfile::tempdir().unwrap();
    let album = root.path().join("album");
    create_folder(album.clone()).await.unwrap();
    assert!(matches!(
        create_folder(album.clone()).await,
        Err(FileError::Io { .. })
    ));
    let temporary = album.join(".coven-1.download");
    let path = album.join("track.flac");
    let mut writer = create_copy(temporary.clone()).await.unwrap();
    writer.append(b"first ").await.unwrap();
    writer.append(b"second").await.unwrap();
    assert!(!path.exists());
    writer.publish(&path).await.unwrap();
    assert_eq!(fs::read(&path).unwrap(), b"first second");
    assert!(!temporary.exists());
    // Publishing never replaces what is at the path.
    let mut writer = create_copy(album.join(".coven-2.download")).await.unwrap();
    writer.append(b"other").await.unwrap();
    assert!(writer.publish(&path).await.is_err());
    assert_eq!(fs::read(&path).unwrap(), b"first second");
    assert!(create_copy(temporary.clone()).await.is_ok());
    assert!(
        create_copy(temporary).await.is_err(),
        "a copy is a new file"
    );
}

#[tokio::test]
async fn removal_takes_only_what_is_as_coven_left_it() {
    let root = tempfile::tempdir().unwrap();
    let album = root.path().join("album");
    fs::create_dir(&album).unwrap();
    let kept = album.join("kept.flac");
    let changed = album.join("changed.flac");
    fs::write(&kept, b"coven's").unwrap();
    fs::write(&changed, b"coven's").unwrap();
    let facts = |path: &Path| {
        let metadata = fs::metadata(path).unwrap();
        (metadata.len(), metadata.modified().unwrap())
    };
    let (size, modified) = facts(&kept);
    let (_, changed_at) = facts(&changed);
    fs::write(&changed, b"the person's edit").unwrap();
    remove_copy(&changed, size, changed_at).unwrap();
    assert_eq!(fs::read(&changed).unwrap(), b"the person's edit");
    // A folder holding anything stays.
    remove_folder(&album).unwrap();
    assert!(album.exists());
    remove_copy(&kept, size, modified).unwrap();
    assert!(!kept.exists());
    remove_copy(&kept, size, modified).unwrap();
    fs::remove_file(&changed).unwrap();
    remove_folder(&album).unwrap();
    assert!(!album.exists());
    remove_folder(&album).unwrap();
    remove_temporary(&album.join(".coven-1.download")).unwrap();
}

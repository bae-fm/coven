use super::*;

/// The path of the names in `path`, written joined by `/` for short.
fn relative(path: &str) -> RelativePath {
    RelativePath::new(path.split('/')).unwrap()
}

#[tokio::test]
async fn a_plan_makes_each_missing_folder_once_outermost_first() {
    let root = tempfile::tempdir().unwrap();
    let folder = root.path().join("Music");
    fs::create_dir(&folder).unwrap();
    fs::create_dir(folder.join("Existing")).unwrap();
    let plan = CopyFolder::open(folder.clone())
        .await
        .unwrap()
        .plan(vec![
            relative("Abbey Road/Disc 1/01 Come Together.flac"),
            relative("Abbey Road/Disc 1/02 Something.flac"),
            relative("Existing/cover.jpg"),
        ])
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
            CopyFolder::open(folder)
                .await
                .unwrap()
                .plan(paths.into_iter().map(relative).collect())
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
        CopyPathError::Clash { path, other }
            if path == relative("album/track.flac") && other == path
    ));
    assert!(matches!(
        refused(vec!["album", "album/track.flac"]).await,
        CopyPathError::Clash { path, other } if path == relative("album") && other == path
    ));
    assert!(matches!(
        refused(vec!["album/track.flac", "album"]).await,
        CopyPathError::Clash { path, other } if path == relative("album") && other == path
    ));
    assert!(matches!(
        CopyFolder::open("Music".into()).await,
        Err(CopyPathError::NotAFolder(path)) if path == Path::new("Music")
    ));
    assert!(matches!(
        CopyFolder::open(root.path().join("missing")).await,
        Err(CopyPathError::NotAFolder(_))
    ));
}

/// Two paths that differ only in case or Unicode form are one name on the
/// default macOS and Windows volumes, so a plan refuses them on every
/// volume, before anything is made, naming both. Folders on the way count
/// too, existing or not.
#[tokio::test]
async fn a_plan_refuses_paths_that_differ_only_in_case_or_unicode_form() {
    let root = tempfile::tempdir().unwrap();
    let folder = root.path().join("Music");
    fs::create_dir_all(folder.join("Existing")).unwrap();
    let chosen = &CopyFolder::open(folder.clone()).await.unwrap();
    let clash = |paths: [&'static str; 2]| async move {
        match chosen.plan(paths.map(relative).to_vec()).await {
            Err(CopyPathError::Clash { path, other }) => (path, other),
            planned => panic!("{paths:?}: {planned:?}"),
        }
    };
    assert_eq!(
        clash(["Abbey Road/Something.flac", "Abbey Road/something.FLAC"]).await,
        (
            relative("Abbey Road/something.FLAC"),
            relative("Abbey Road/Something.flac")
        )
    );
    assert_eq!(
        clash(["Abbey Road/one.flac", "abbey road/two.flac"]).await,
        (relative("abbey road"), relative("Abbey Road"))
    );
    assert_eq!(
        clash(["Existing/one.flac", "existing/two.flac"]).await,
        (relative("existing"), relative("Existing"))
    );
    assert_eq!(
        clash(["Album", "album/track.flac"]).await,
        (relative("album"), relative("Album"))
    );
    // "Björk" composed, and with its "ö" as "o" and a combining diaeresis.
    assert_eq!(
        clash(["Bj\u{f6}rk/Joga.flac", "Bjo\u{308}rk/Joga.flac"]).await,
        (relative("Bjo\u{308}rk"), relative("Bj\u{f6}rk"))
    );
    // Full case folding: "ß" folds as "ss".
    assert_eq!(
        clash(["Straße.flac", "STRASSE.flac"]).await,
        (relative("STRASSE.flac"), relative("Straße.flac"))
    );
    assert!(
        fs::read_dir(&folder).unwrap().count() == 1,
        "planning makes nothing"
    );
    // Names that differ by more than case are separate.
    assert_eq!(
        chosen
            .plan(vec![
                relative("Abbey Road/one.flac"),
                relative("Abbey Road 2/one.flac")
            ])
            .await
            .unwrap()
            .files
            .len(),
        2
    );
}

/// Each name is written as the one rule for every platform makes it, so
/// the plan holds the names coven writes.
#[tokio::test]
async fn a_plan_writes_each_name_valid_everywhere() {
    let root = tempfile::tempdir().unwrap();
    let folder = root.path().to_owned();
    let plan = CopyFolder::open(folder.clone())
        .await
        .unwrap()
        .plan(vec![
            RelativePath::new(["AC/DC", "Back in Black.", r"Caf\xE9.flac"]).unwrap(),
            RelativePath::new(["AC/DC", "Back in Black", "CON.flac"]).unwrap(),
        ])
        .await
        .unwrap();
    let ac_dc = folder.join("AC／DC");
    assert_eq!(
        plan,
        CopyPlan {
            folders: vec![
                ac_dc.clone(),
                ac_dc.join("Back in Black．"),
                ac_dc.join("Back in Black"),
            ],
            files: vec![
                ac_dc.join("Back in Black．").join("Caf＼xE9.flac"),
                ac_dc.join("Back in Black").join("ＣＯＮ.flac"),
            ],
        }
    );
}

/// Two names the rule writes as one name clash, as names differing only in
/// case do, and the refusal names both as the app named them.
#[tokio::test]
async fn a_plan_refuses_paths_that_map_to_one_name() {
    let root = tempfile::tempdir().unwrap();
    let chosen = &CopyFolder::open(root.path().to_owned()).await.unwrap();
    let clash = |paths: [&[&str]; 2]| {
        let paths = paths.map(|names| RelativePath::new(names.iter().copied()).unwrap());
        async move {
            match chosen.plan(paths.to_vec()).await {
                Err(CopyPathError::Clash { path, other }) => {
                    assert_eq!((&path, &other), (&paths[1], &paths[0]));
                }
                planned => panic!("{paths:?}: {planned:?}"),
            }
        }
    };
    clash([&["Why?.flac"], &["Why？.flac"]]).await;
    clash([&["CON.flac"], &["ＣＯＮ.flac"]]).await;
    clash([&["AC/DC"], &["AC／DC"]]).await;
    // Mapped, then compared ignoring case.
    clash([&["AC:DC"], &["ac：dc"]]).await;
    let (one, two) = (
        format!("{}1.flac", "a".repeat(260)),
        format!("{}2.flac", "a".repeat(260)),
    );
    clash([&[one.as_str()], &[two.as_str()]]).await;
    // A folder on the way counts, named up to the name that clashes.
    match chosen
        .plan(vec![
            RelativePath::new(["Album.", "one.flac"]).unwrap(),
            RelativePath::new(["Album．", "two.flac"]).unwrap(),
        ])
        .await
    {
        Err(CopyPathError::Clash { path, other }) => assert_eq!(
            (path, other),
            (
                RelativePath::new(["Album．"]).unwrap(),
                RelativePath::new(["Album."]).unwrap()
            )
        ),
        planned => panic!("{planned:?}"),
    }
    // `Album.` and `Album` are two names everywhere.
    assert_eq!(
        chosen
            .plan(vec![
                RelativePath::new(["Album.", "one.flac"]).unwrap(),
                RelativePath::new(["Album", "one.flac"]).unwrap(),
            ])
            .await
            .unwrap()
            .folders,
        vec![root.path().join("Album．"), root.path().join("Album")]
    );
}

/// What a plan doesn't foresee, the volume refuses: a rename never
/// replaces what is at the path, under any name the volume holds as the
/// same. On a volume that ignores case, a copy published as `track.flac`
/// beside the person's `Track.flac` fails, and theirs stays.
#[tokio::test]
async fn a_rename_onto_a_name_the_volume_holds_as_taken_fails_and_replaces_nothing() {
    let root = tempfile::tempdir().unwrap();
    let chosen = CopyFolder::open(root.path().to_owned()).await.unwrap();
    let theirs = root.path().join("Track.flac");
    fs::write(&theirs, b"the person's").unwrap();
    let ignores_case = root.path().join("track.flac").exists();
    let mut writer = chosen
        .create_copy(root.path().join(".coven-1.download"))
        .await
        .unwrap();
    writer.append(b"coven's").await.unwrap();
    let published = writer.publish(&root.path().join("track.flac")).await;
    assert_eq!(published.is_err(), ignores_case, "{published:?}");
    assert_eq!(fs::read(&theirs).unwrap(), b"the person's");
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
    let chosen = CopyFolder::open(folder.clone()).await.unwrap();
    let plan = chosen
        .plan(vec![relative("inside/track.flac")])
        .await
        .unwrap();
    assert_eq!(plan.files, vec![folder.join("inside").join("track.flac")]);
    assert!(matches!(
        chosen.plan(vec![relative("linked/track.flac")]).await,
        Err(CopyPathError::Link(path)) if path == folder.join("linked")
    ));
    std::os::unix::fs::symlink(real.join("nowhere"), real.join("dangling.flac")).unwrap();
    assert!(matches!(
        chosen.plan(vec![relative("dangling.flac")]).await,
        Err(CopyPathError::Exists(_))
    ));
}

#[tokio::test]
async fn a_copy_appears_at_its_path_only_whole() {
    let root = tempfile::tempdir().unwrap();
    let chosen = CopyFolder::open(root.path().to_owned()).await.unwrap();
    let album = root.path().join("album");
    assert!(chosen.create_folder(album.clone()).await.unwrap());
    // A folder already there is someone else's: the call didn't make it.
    assert!(!chosen.create_folder(album.clone()).await.unwrap());
    fs::write(root.path().join("file"), b"not a folder").unwrap();
    assert!(chosen
        .create_folder(root.path().join("file"))
        .await
        .is_err());
    let temporary = album.join(".coven-1.download");
    let path = album.join("track.flac");
    let mut writer = chosen.create_copy(temporary.clone()).await.unwrap();
    writer.append(b"first ").await.unwrap();
    writer.append(b"second").await.unwrap();
    assert!(!path.exists());
    writer.publish(&path).await.unwrap();
    assert_eq!(fs::read(&path).unwrap(), b"first second");
    assert!(!temporary.exists());
    // Publishing never replaces what is at the path.
    let mut writer = chosen
        .create_copy(album.join(".coven-2.download"))
        .await
        .unwrap();
    writer.append(b"other").await.unwrap();
    assert!(writer.publish(&path).await.is_err());
    assert_eq!(fs::read(&path).unwrap(), b"first second");
    assert!(chosen.create_copy(temporary.clone()).await.is_ok());
    assert!(
        chosen.create_copy(temporary).await.is_err(),
        "a copy is a new file"
    );
    assert!(
        chosen
            .create_copy(Path::new("/elsewhere/.coven-3.download").to_owned())
            .await
            .is_err(),
        "a path outside the folder is refused"
    );
}

/// A copy lands however long its whole path is. Windows calls given a plain
/// path stop at 260 characters, so the rename into place goes by a `\\?\`
/// path, as std's own calls do.
#[tokio::test]
async fn a_copy_lands_past_the_windows_path_limit() {
    let root = tempfile::tempdir().unwrap();
    let chosen = CopyFolder::open(root.path().to_owned()).await.unwrap();
    let mut names = Vec::new();
    while root.path().join(names.join("/")).as_os_str().len() < 300 {
        names.push("A Long Folder Name For A Deep Album");
    }
    names.push("01 Come Together.flac");
    let plan = chosen.plan(vec![relative(&names.join("/"))]).await.unwrap();
    for folder in plan.folders {
        assert!(chosen.create_folder(folder).await.unwrap());
    }
    let path = &plan.files[0];
    assert!(path.as_os_str().len() > 300);
    let mut writer = chosen
        .create_copy(path.with_file_name(".coven-1.download"))
        .await
        .unwrap();
    writer.append(b"bytes").await.unwrap();
    writer.publish(path).await.unwrap();
    assert_eq!(fs::read(path).unwrap(), b"bytes");
}

/// A folder swapped for a link after planning can't send a write through
/// it: every create and rename is relative to the opened folder, one name
/// at a time, never following a link.
#[cfg(unix)]
#[tokio::test]
async fn a_folder_swapped_for_a_link_after_planning_redirects_nothing() {
    let root = tempfile::tempdir().unwrap();
    let music = root.path().join("music");
    let elsewhere = root.path().join("elsewhere");
    fs::create_dir_all(music.join("album")).unwrap();
    fs::create_dir(&elsewhere).unwrap();
    let chosen = CopyFolder::open(music.clone()).await.unwrap();
    let plan = chosen
        .plan(vec![
            relative("album/track.flac"),
            relative("album/new/track.flac"),
        ])
        .await
        .unwrap();
    assert_eq!(plan.folders, vec![music.join("album").join("new")]);
    fs::remove_dir(music.join("album")).unwrap();
    std::os::unix::fs::symlink(&elsewhere, music.join("album")).unwrap();
    assert!(chosen.create_folder(plan.folders[0].clone()).await.is_err());
    assert!(chosen
        .create_copy(music.join("album").join(".coven-1.download"))
        .await
        .is_err());
    assert!(fs::read_dir(&elsewhere).unwrap().next().is_none());
    // A writer opened before the swap renames within the folder it opened.
    fs::remove_file(music.join("album")).unwrap();
    fs::create_dir(music.join("album")).unwrap();
    let mut writer = chosen
        .create_copy(music.join("album").join(".coven-2.download"))
        .await
        .unwrap();
    writer.append(b"bytes").await.unwrap();
    fs::rename(music.join("album"), root.path().join("moved")).unwrap();
    std::os::unix::fs::symlink(&elsewhere, music.join("album")).unwrap();
    writer
        .publish(&music.join("album").join("track.flac"))
        .await
        .unwrap();
    assert!(fs::read_dir(&elsewhere).unwrap().next().is_none());
    assert_eq!(
        fs::read(root.path().join("moved").join("track.flac")).unwrap(),
        b"bytes"
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

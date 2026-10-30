//! Making cloud files device-only (§17.3): the copies and the one write
//! switching them, who may, the paths refused, what a failing or cancelled
//! call leaves, what other devices drop when the switch arrives, the stored
//! bytes going, and two devices making one file device-only at once.
use crate::tests::{wait_for_empty_queue, Device, Network};
use crate::*;
use coven_foundation::files::StoreDirAccess;
use coven_storage::Storage;
use std::path::Path;
use tokio::sync::watch;

fn tables() -> Vec<SyncedTable> {
    vec![
        SyncedTable::new("tracks", RowIdentity::SharedKey).file_column(FileColumn::new("file")),
        SyncedTable::new("owned_tracks", RowIdentity::SharedKey)
            .owner_only()
            .file_column(FileColumn::new("file")),
        SyncedTable::new("covers", RowIdentity::SharedKey).file_column(FileColumn::new("file")),
        SyncedTable::new("gifts", RowIdentity::IndependentUuid)
            .audience_column("audience")
            .file_column(FileColumn::new("file")),
    ]
}

const SCHEMA: &str = "CREATE TABLE tracks(id TEXT NOT NULL PRIMARY KEY, file TEXT);
    CREATE TABLE owned_tracks(id TEXT NOT NULL PRIMARY KEY, file TEXT);
    CREATE TABLE covers(id TEXT NOT NULL PRIMARY KEY, file TEXT);
    CREATE TABLE gifts(id TEXT NOT NULL PRIMARY KEY, file TEXT, audience TEXT NOT NULL);";

/// Ana's Household store on S3, with Carol joined as a member on device 1.
async fn household() -> Network {
    let mut network = Network::with_schema(
        CloudProvider::S3,
        tables(),
        vec![Migration::addition(1, "tracks", SCHEMA)],
    )
    .await;
    network
        .join_as(
            1,
            InviteTarget::NewMember {
                role: MemberRole::Member,
                name: "Carol".into(),
            },
        )
        .await;
    network.sync(0).await;
    network
}

/// Stage `bytes` as the user's original `name` under `originals`, as
/// `source` says, in the row `insert` writes for it. Its upload isn't
/// waited for.
async fn stage(
    device: &Device,
    name: &str,
    originals: &Path,
    bytes: Vec<u8>,
    source: fn(PreparedUserFile) -> UserSource,
    insert: impl FnOnce(SqlContext<'_, '_>, FileId) -> CovenResult<()> + Send + 'static,
) -> FileId {
    let path = originals.join(name);
    std::fs::write(&path, &bytes).unwrap();
    let prepared = prepare_user_file(&path, |_| {}).await.unwrap();
    device
        .handle
        .write_with_files(
            move |batch| Ok(batch.put_user_file("tracks", source(prepared))),
            move |sql, file| {
                insert(sql, file)?;
                Ok(file)
            },
        )
        .await
        .unwrap()
}

/// Import `bytes` as the user's original `name` under `originals`, for the
/// cloud, in row `name` of `table`, and wait for its upload.
async fn import(
    device: &Device,
    table: &'static str,
    name: &str,
    originals: &Path,
    bytes: Vec<u8>,
) -> FileId {
    let row = name.to_owned();
    let file = stage(
        device,
        name,
        originals,
        bytes,
        UserSource::Cloud,
        move |sql, file| {
            sql.execute(
                &format!("INSERT INTO {table}(id,file) VALUES(?1,?2)"),
                (&row, file),
            )?;
            Ok(())
        },
    )
    .await;
    wait_for_empty_queue(&device.handle).await;
    file
}

/// The path of the names in `relative`, written joined by `/` for short.
fn path(relative: &str) -> RelativePath {
    RelativePath::new(relative.split('/')).unwrap()
}

/// A cancel that never comes.
fn uncancelled() -> watch::Receiver<bool> {
    watch::channel(false).1
}

/// Each name directly in `folder`, sorted.
fn names(folder: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(folder)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    names
}

/// How many cache files the device keeps.
fn cached(device: &Device) -> usize {
    let cache = device
        .layout
        .store_dir(&device.store)
        .database_path()
        .parent()
        .unwrap()
        .join("cache");
    std::fs::read_dir(cache).map_or(0, Iterator::count)
}

fn stored_path(store: StoreId, file: &FileRef) -> ObjectPath {
    match file.place() {
        FilePlace::Cloud { location, .. } => {
            ObjectPath::file(store, location.uploader(), location.object())
        }
        FilePlace::DeviceOnly { .. } => panic!("the file is not stored"),
    }
}

async fn member(device: &Device) -> MemberId {
    device
        .handle
        .members()
        .await
        .unwrap()
        .into_iter()
        .find(|member| member.is_self)
        .unwrap()
        .id
}

/// Carol makes two of Ana's tracks local in her music folder. Each copy is
/// whole at its path, one write switches both, and no app row changes. When
/// it reaches Ana's laptop, her pin and cached bytes go, her record of her
/// own original goes and the original stays, and her laptop, the uploader,
/// deletes the stored bytes.
#[tokio::test]
async fn copies_land_and_every_file_switches_in_one_write() {
    let network = household().await;
    let (ana, carol) = (&network.devices[0], &network.devices[1]);
    let originals = tempfile::tempdir().unwrap();
    let one = import(ana, "tracks", "one", originals.path(), vec![1; 100_000]).await;
    let two = import(ana, "tracks", "two", originals.path(), vec![2; 3000]).await;
    network.sync(0).await;
    network.sync(1).await;
    let stored = [
        ana.handle.file_ref(one).await.unwrap(),
        ana.handle.file_ref(two).await.unwrap(),
    ];
    ana.handle.pin(&stored[..1]).await.unwrap();
    assert_eq!(
        ana.handle.read_file(&stored[1]).await.unwrap(),
        vec![2; 3000]
    );
    assert_eq!(ana.handle.files_pinned(&[one]).await.unwrap(), [Some(true)]);
    assert!(cached(ana) > 0);

    let music = tempfile::tempdir().unwrap();
    let queued = carol.handle.test_queued_writes().await.unwrap().len();
    carol
        .handle
        .make_device_only(
            music.path(),
            vec![
                (one, path("Abbey Road/01 Come Together.flac")),
                (two, path("Abbey Road/02 Something.flac")),
            ],
            &uncancelled(),
        )
        .await
        .unwrap();
    let album = music.path().join("Abbey Road");
    assert_eq!(
        names(&album),
        ["01 Come Together.flac", "02 Something.flac"],
        "no temporary name is left"
    );
    let device = carol.handle.device_id();
    for (file, name, bytes) in [
        (one, "01 Come Together.flac", vec![1; 100_000]),
        (two, "02 Something.flac", vec![2; 3000]),
    ] {
        assert_eq!(std::fs::read(album.join(name)).unwrap(), bytes);
        let local = carol.handle.file_ref(file).await.unwrap();
        assert_eq!(local.place(), FilePlace::DeviceOnly { device });
        assert_eq!(
            carol.handle.user_file(file).await.unwrap().unwrap().path,
            album.join(name)
        );
        assert_eq!(carol.handle.read_file(&local).await.unwrap(), bytes);
    }
    let writes = carol.handle.test_queued_writes().await.unwrap();
    assert_eq!(writes.len(), queued + 1, "one write switches both files");
    let changed: Vec<&str> = writes[queued]
        .parts
        .iter()
        .flat_map(|part| &part.rows)
        .map(|row| row.row.table.as_str())
        .collect();
    assert_eq!(
        changed,
        ["coven_files", "coven_files"],
        "no app row changes"
    );

    network.sync(1).await;
    network.sync(0).await;
    for file in [one, two] {
        let elsewhere = ana.handle.file_ref(file).await.unwrap();
        assert_eq!(elsewhere.place(), FilePlace::DeviceOnly { device });
        assert!(matches!(
            ana.handle.read_file(&elsewhere).await,
            Err(FileReadError::DeviceOnly { .. })
        ));
        assert_eq!(ana.handle.user_file(file).await.unwrap(), None);
    }
    assert_eq!(
        std::fs::read(originals.path().join("one")).unwrap(),
        vec![1; 100_000],
        "an original the person gave coven is never touched"
    );
    assert_eq!(
        ana.handle.files_pinned(&[one, two]).await.unwrap(),
        [Some(false), Some(false)]
    );
    assert_eq!(cached(ana), 0, "the switch takes the cached bytes");
    // No record names the stored bytes now: on Ana's laptop, which uploaded
    // them, they are deletion candidates nothing local protects.
    let candidates: Vec<(DeviceId, FileId)> = ana
        .handle
        .database
        .file_candidates([ana.handle.device_id()].into())
        .await
        .unwrap()
        .into_iter()
        .filter(|candidate| !candidate.protected)
        .map(|candidate| (candidate.device, candidate.file))
        .collect();
    for file in &stored {
        let FilePlace::Cloud { location, .. } = file.place() else {
            unreachable!()
        };
        assert!(candidates.contains(&(location.uploader(), location.object())));
    }
    // The history naming them stays retained until a snapshot covers it and
    // the floor passes it; then Ana's laptop, which uploaded them, deletes
    // them.
    let objects = stored.clone().map(|file| stored_path(ana.store, &file));
    for _ in 0..4 {
        network
            .clock
            .set(network.clock.now() + std::time::Duration::from_secs(31 * 24 * 3600));
        network.sync(0).await;
        network.sync(1).await;
    }
    network.sync(0).await;
    for object in &objects {
        assert!(
            ana.storage.status(object).await.unwrap().is_none(),
            "the uploader deletes stored bytes no record names"
        );
    }
    network.close().await;
}

/// Ana imports a track whose name isn't UTF-8, as Linux allows. Its name
/// travels as the text that shows it, which Carol's app names her copy by:
/// one name, its backslash written as `＼` as on every platform. Each
/// device's SQL shows its own original's path the way `path_text` does.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_name_that_isnt_utf8_travels_as_the_text_that_shows_it() {
    use std::os::unix::ffi::OsStrExt;
    let network = household().await;
    let (ana, carol) = (&network.devices[0], &network.devices[1]);
    let originals = tempfile::tempdir().unwrap();
    let original = originals
        .path()
        .join(std::ffi::OsStr::from_bytes(b"Caf\xE9.flac"));
    std::fs::write(&original, b"latin-1 track").unwrap();
    let prepared = prepare_user_file(&original, |_| {}).await.unwrap();
    let name = path_text(Path::new(original.file_name().unwrap()));
    assert_eq!(name, r"Caf\xE9.flac");
    let file = ana
        .handle
        .write_with_files(
            move |batch| Ok(batch.put_user_file("tracks", UserSource::Cloud(prepared))),
            move |sql, file| {
                sql.execute("INSERT INTO tracks(id,file) VALUES(?1,?2)", (&name, file))?;
                Ok(file)
            },
        )
        .await
        .unwrap();
    wait_for_empty_queue(&ana.handle).await;
    network.sync(0).await;
    network.sync(1).await;
    let shown = |device: &Device| {
        let handle = device.handle.clone();
        async move {
            handle
                .read(|sql| {
                    Ok(sql.query(
                        "SELECT u.path,coven_path_text(u.path) FROM tracks AS t
                         JOIN coven_user_files AS u ON u.file=t.file",
                        [],
                        |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, String>(1)?)),
                    )?)
                })
                .await
                .unwrap()
        }
    };
    assert_eq!(
        shown(ana).await,
        [(
            original.as_os_str().as_bytes().to_vec(),
            path_text(&original)
        )]
    );
    let synced: String = carol
        .handle
        .read(|sql| Ok(sql.query_row("SELECT id FROM tracks", [], |r| r.get(0))?))
        .await
        .unwrap();
    let music = tempfile::tempdir().unwrap();
    carol
        .handle
        .make_device_only(
            music.path(),
            vec![(file, RelativePath::new([synced]).unwrap())],
            &uncancelled(),
        )
        .await
        .unwrap();
    assert_eq!(names(music.path()), ["Caf＼xE9.flac"]);
    let copy = music.path().join("Caf＼xE9.flac");
    assert_eq!(std::fs::read(&copy).unwrap(), b"latin-1 track");
    assert_eq!(
        carol.handle.user_file(file).await.unwrap().unwrap().path,
        copy
    );
    assert_eq!(
        shown(carol).await,
        [(
            copy.as_os_str().as_bytes().to_vec(),
            copy.to_str().unwrap().to_owned()
        )]
    );
    network.close().await;
}

/// A synced name lands as the one rule writes it, alike on every platform:
/// none splits at a `\`, names a stream at a `:` or opens a device. Each
/// copy is recorded as the original at the path written.
#[tokio::test]
async fn each_synced_name_lands_as_a_name_valid_everywhere() {
    let network = household().await;
    let (ana, carol) = (&network.devices[0], &network.devices[1]);
    let originals = tempfile::tempdir().unwrap();
    let mut tracks = Vec::new();
    for (synced, local, byte) in [
        (r"Caf\xE9.flac", "Caf＼xE9.flac", 1),
        ("Live: Tokyo.flac", "Live： Tokyo.flac", 2),
        ("CON.flac", "ＣＯＮ.flac", 3),
    ] {
        let file = import(ana, "tracks", local, originals.path(), vec![byte; 300]).await;
        tracks.push((file, synced, local, byte));
    }
    network.sync(0).await;
    network.sync(1).await;
    let music = tempfile::tempdir().unwrap();
    let album = |name| RelativePath::new(["AC/DC", "So Long...", name]).unwrap();
    let files = tracks
        .iter()
        .map(|&(file, synced, ..)| (file, album(synced)));
    carol
        .handle
        .make_device_only(music.path(), files.collect(), &uncancelled())
        .await
        .unwrap();
    assert_eq!(names(music.path()), ["AC／DC"]);
    let folder = music.path().join("AC／DC").join("So Long..．");
    let written: Vec<&str> = tracks.iter().map(|&(_, _, local, _)| local).collect();
    assert_eq!(names(&folder), written, "sorted as they are listed");
    for (file, _, local, byte) in tracks {
        assert_eq!(std::fs::read(folder.join(local)).unwrap(), vec![byte; 300]);
        let recorded = carol.handle.user_file(file).await.unwrap().unwrap();
        assert_eq!(recorded.path, folder.join(local));
    }
    network.close().await;
}

/// The owner rule decides who may: a row of an `owner_only` table another
/// member created refuses the call, an admin included, and every row
/// referencing the file counts.
#[tokio::test]
async fn only_a_member_who_may_change_every_referencing_row_may() {
    let network = household().await;
    let (ana, carol) = (&network.devices[0], &network.devices[1]);
    let (anas, carols) = (member(ana).await, member(carol).await);
    let originals = tempfile::tempdir().unwrap();
    let ana_track = import(ana, "owned_tracks", "ana", originals.path(), vec![3; 500]).await;
    network.sync(0).await;
    let carol_originals = tempfile::tempdir().unwrap();
    let carol_track = import(
        carol,
        "owned_tracks",
        "carol",
        carol_originals.path(),
        vec![4; 500],
    )
    .await;
    network.sync(1).await;
    network.sync(0).await;
    network.sync(1).await;
    let music = tempfile::tempdir().unwrap();
    let refused = |device: &Device, file: FileId| {
        let handle = device.handle.clone();
        let folder = music.path().to_owned();
        async move {
            handle
                .make_device_only(&folder, vec![(file, path("track.flac"))], &uncancelled())
                .await
        }
    };
    assert!(matches!(
        refused(carol, ana_track).await,
        Err(DeviceOnlyError::Database(DbError::NotOwner { table, owner, .. }))
            if table == "owned_tracks" && owner == anas
    ));
    // Ana is an admin, and gets no exception.
    assert!(matches!(
        refused(ana, carol_track).await,
        Err(DeviceOnlyError::Database(DbError::NotOwner { owner, .. })) if owner == carols
    ));
    assert!(names(music.path()).is_empty(), "nothing was downloaded");
    // Ana's row naming Carol's file too refuses Carol, until it goes.
    ana.handle
        .write(move |sql| {
            sql.execute(
                "INSERT INTO owned_tracks(id,file) VALUES('also',?1)",
                [carol_track],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    network.sync(0).await;
    network.sync(1).await;
    assert!(matches!(
        refused(carol, carol_track).await,
        Err(DeviceOnlyError::Database(DbError::NotOwner { owner, .. })) if owner == anas
    ));
    ana.handle
        .write(|sql| {
            sql.execute("DELETE FROM owned_tracks WHERE id='also'", [])?;
            Ok(())
        })
        .await
        .unwrap();
    network.sync(0).await;
    network.sync(1).await;
    refused(carol, carol_track).await.unwrap();
    // Ana may make her own track device-only, wherever it was uploaded from.
    let anas_music = tempfile::tempdir().unwrap();
    ana.handle
        .make_device_only(
            anas_music.path(),
            vec![(ana_track, path("mine.flac"))],
            &uncancelled(),
        )
        .await
        .unwrap();
    assert_eq!(
        std::fs::read(anas_music.path().join("mine.flac")).unwrap(),
        vec![3; 500]
    );
    network.close().await;
}

/// The circle rule decides who may by what the member can see. A circle's
/// records reach only its members, so Carol, outside Gifts, has no record
/// of its track: it is `FileAbsent` to her, as a file she never received.
/// Once in Gifts she may. Removed from it, she keeps the records she had
/// (§15.8), and the call is refused with `NotInCircle`.
#[tokio::test]
async fn only_a_member_of_the_files_circle_may() {
    let network = household().await;
    let (ana, carol) = (&network.devices[0], &network.devices[1]);
    let gifts = ana.handle.circles().create("Gifts").await.unwrap();
    network.sync(0).await;
    network.sync(1).await;
    let originals = tempfile::tempdir().unwrap();
    let mut tracks = Vec::new();
    for (row, name) in [(1, "one"), (2, "two")] {
        let audience = gifts.to_string();
        tracks.push(
            stage(
                ana,
                name,
                originals.path(),
                vec![row; 900],
                UserSource::Cloud,
                move |sql, file| {
                    sql.execute(
                        "INSERT INTO gifts(id,file,audience) VALUES(?1,?2,?3)",
                        (
                            format!("00000000-0000-7000-8000-00000000000{row}"),
                            file,
                            audience,
                        ),
                    )?;
                    Ok(())
                },
            )
            .await,
        );
    }
    wait_for_empty_queue(&ana.handle).await;
    network.sync(0).await;
    network.sync(1).await;
    let [one, two] = tracks[..] else {
        unreachable!()
    };
    let music = tempfile::tempdir().unwrap();
    let call = |file: FileId, name: &'static str| {
        let handle = carol.handle.clone();
        let folder = music.path().to_owned();
        async move {
            handle
                .make_device_only(&folder, vec![(file, path(name))], &uncancelled())
                .await
        }
    };
    assert!(matches!(
        call(one, "one.flac").await,
        Err(DeviceOnlyError::Database(DbError::FileAbsent { file })) if file == one
    ));
    let carols = member(carol).await;
    ana.handle
        .circles()
        .add_member(gifts, &carols)
        .await
        .unwrap();
    network.sync(0).await;
    network.sync(1).await;
    call(one, "one.flac").await.unwrap();
    assert_eq!(
        carol.handle.file_ref(one).await.unwrap().place(),
        FilePlace::DeviceOnly {
            device: carol.handle.device_id()
        }
    );
    ana.handle
        .circles()
        .remove_member(gifts, &carols)
        .await
        .unwrap();
    network.sync(0).await;
    network.sync(1).await;
    assert!(
        matches!(
            carol.handle.file_ref(two).await.unwrap().place(),
            FilePlace::Cloud { .. }
        ),
        "Carol keeps the records she had"
    );
    assert!(matches!(
        call(two, "two.flac").await,
        Err(DeviceOnlyError::Database(DbError::NotInCircle(circle))) if circle == gifts
    ));
    assert_eq!(
        names(music.path()),
        ["one.flac"],
        "nothing more was downloaded"
    );
    network.close().await;
}

/// A path is refused before anything is downloaded: one through a symbolic
/// link below the folder, where something is already, or differing from
/// another only in case, is refused. So is an app file, a file not in the cloud, and one file named
/// twice. The folder is left as it was.
#[tokio::test]
async fn refused_paths_and_files_leave_the_folder_alone() {
    let network = household().await;
    let (ana, carol) = (&network.devices[0], &network.devices[1]);
    let originals = tempfile::tempdir().unwrap();
    let track = import(ana, "tracks", "track", originals.path(), vec![5; 700]).await;
    let other = import(ana, "tracks", "other", originals.path(), vec![6; 700]).await;
    let local_path = originals.path().join("local");
    std::fs::write(&local_path, b"on Ana's laptop only").unwrap();
    let observed = observe_user_file(&local_path).await.unwrap();
    let local = ana
        .handle
        .write_with_files(
            move |batch| Ok(batch.put_user_file("tracks", UserSource::DeviceOnly(observed))),
            |sql, file| {
                sql.execute("INSERT INTO tracks(id,file) VALUES('local',?1)", [file])?;
                Ok(file)
            },
        )
        .await
        .unwrap();
    let cover = ana
        .handle
        .write_with_files(
            |batch| Ok(batch.put_file("files", b"cover".to_vec(), Place::Cloud)),
            |sql, file| {
                sql.execute("INSERT INTO covers(id,file) VALUES('cover',?1)", [file])?;
                Ok(file)
            },
        )
        .await
        .unwrap();
    wait_for_empty_queue(&ana.handle).await;
    network.sync(0).await;
    network.sync(1).await;
    let music = tempfile::tempdir().unwrap();
    std::fs::write(music.path().join("taken.flac"), b"the person's").unwrap();
    let before = names(music.path());
    let call = |files: Vec<(FileId, RelativePath)>| {
        let handle = carol.handle.clone();
        let folder = music.path().to_owned();
        async move {
            handle
                .make_device_only(&folder, files, &uncancelled())
                .await
        }
    };
    assert!(matches!(
        call(vec![(track, path("taken.flac"))]).await,
        Err(DeviceOnlyError::Path(CopyPathError::Exists(taken)))
            if taken == music.path().join("taken.flac")
    ));
    assert_eq!(
        std::fs::read(music.path().join("taken.flac")).unwrap(),
        b"the person's"
    );
    #[cfg(unix)]
    {
        let elsewhere = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(elsewhere.path(), music.path().join("linked")).unwrap();
        assert!(matches!(
            call(vec![(track, path("linked/track.flac"))]).await,
            Err(DeviceOnlyError::Path(CopyPathError::Link(link)))
                if link == music.path().join("linked")
        ));
        assert!(names(elsewhere.path()).is_empty());
        std::fs::remove_file(music.path().join("linked")).unwrap();
    }
    assert!(matches!(
        call(vec![(cover, path("cover.jpg"))]).await,
        Err(DeviceOnlyError::Database(DbError::NamespaceKind { namespace, .. }))
            if namespace == "files"
    ));
    assert!(matches!(
        call(vec![(local, path("local.flac"))]).await,
        Err(DeviceOnlyError::Database(DbError::NotInCloud { file })) if file == local
    ));
    assert!(matches!(
        call(vec![(track, path("a.flac")), (track, path("b.flac"))]).await,
        Err(DeviceOnlyError::RepeatedFile { file }) if file == track
    ));
    // Two paths that differ only in case are one name on the default macOS
    // and Windows volumes: refused before anything is made, on every volume.
    assert!(matches!(
        call(vec![(track, path("Album/Track.flac")), (other, path("album/track.flac"))]).await,
        Err(DeviceOnlyError::Path(CopyPathError::Clash { path, other }))
            if path == self::path("album") && other == self::path("Album")
    ));
    assert_eq!(names(music.path()), before);
    assert!(matches!(
        carol.handle.file_ref(track).await.unwrap().place(),
        FilePlace::Cloud { .. }
    ));
    network.close().await;
}

/// A failed download, or a cancel while one waits for bytes, removes every
/// copy the call made, the ones already whole included, and every folder it
/// made; a folder that was there stays. No file switches.
#[tokio::test]
async fn a_failure_or_cancel_before_the_switch_leaves_nothing() {
    let network = household().await;
    let (ana, carol) = (&network.devices[0], &network.devices[1]);
    let originals = tempfile::tempdir().unwrap();
    let one = import(ana, "tracks", "one", originals.path(), vec![6; 2000]).await;
    let two = import(ana, "tracks", "two", originals.path(), vec![7; 200_000]).await;
    network.sync(0).await;
    network.sync(1).await;
    let music = tempfile::tempdir().unwrap();
    std::fs::create_dir(music.path().join("Existing")).unwrap();
    let files = || {
        vec![
            (one, path("Existing/New Album/one.flac")),
            (two, path("Existing/New Album/two.flac")),
        ]
    };
    let unswitched = || async {
        for file in [one, two] {
            assert!(matches!(
                carol.handle.file_ref(file).await.unwrap().place(),
                FilePlace::Cloud { .. }
            ));
            assert_eq!(carol.handle.user_file(file).await.unwrap(), None);
        }
        assert_eq!(names(music.path()), ["Existing"]);
        assert!(names(&music.path().join("Existing")).is_empty());
    };
    // The second track's stored bytes are damaged.
    let damaged = stored_path(carol.store, &carol.handle.file_ref(two).await.unwrap());
    let size = carol.storage.status(&damaged).await.unwrap().unwrap().size;
    let original = carol.storage.read(&damaged).await.unwrap();
    carol
        .storage
        .corrupt_byte(&damaged, size as usize - 1)
        .await
        .unwrap();
    assert!(matches!(
        carol
            .handle
            .make_device_only(music.path(), files(), &uncancelled())
            .await,
        Err(DeviceOnlyError::Download { file, error: FileReadError::Integrity { .. } })
            if file == two
    ));
    unswitched().await;
    carol.storage.delete(&damaged).await.unwrap();
    carol.storage.create(&damaged, &original).await.unwrap();
    // A cancel while the second track's bytes are on their way.
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    carol
        .storage
        .hold_body_at(damaged.clone(), 70_000, entered, resumed)
        .await;
    let (stop, cancel) = watch::channel(false);
    let call = {
        let handle = carol.handle.clone();
        let folder = music.path().to_owned();
        let files = files();
        tokio::spawn(async move { handle.make_device_only(&folder, files, &cancel).await })
    };
    entering.await.unwrap();
    stop.send(true).unwrap();
    assert!(matches!(
        call.await.unwrap(),
        Err(DeviceOnlyError::Cancelled)
    ));
    drop(resume);
    unswitched().await;
    // The app drops the call's future while bytes are on their way: the
    // copies stay recorded, and the store's next write removes them.
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    carol
        .storage
        .hold_body_at(damaged.clone(), 70_000, entered, resumed)
        .await;
    let call = {
        let handle = carol.handle.clone();
        let folder = music.path().to_owned();
        let files = files();
        tokio::spawn(async move {
            handle
                .make_device_only(&folder, files, &uncancelled())
                .await
        })
    };
    entering.await.unwrap();
    call.abort();
    assert!(call.await.unwrap_err().is_cancelled());
    drop(resume);
    assert!(!names(&music.path().join("Existing").join("New Album")).is_empty());
    carol
        .handle
        .write(|sql| {
            sql.execute("INSERT INTO tracks(id) VALUES('next')", [])?;
            Ok(())
        })
        .await
        .unwrap();
    unswitched().await;
    // With nothing in the way, the same call succeeds.
    carol
        .handle
        .make_device_only(music.path(), files(), &uncancelled())
        .await
        .unwrap();
    assert_eq!(
        names(&music.path().join("Existing").join("New Album")),
        ["one.flac", "two.flac"]
    );
    network.close().await;
}

/// Ana and Carol make the same tracks device-only at once. Carol's write is
/// the later one, so it wins on both devices. Ana's download lost: her copy
/// she didn't touch goes, with the folder coven made for it once empty, and
/// the one she edited meanwhile is hers and stays.
#[tokio::test]
async fn two_devices_at_once_the_later_write_wins_and_the_losing_copy_goes() {
    let network = household().await;
    let (ana, carol) = (&network.devices[0], &network.devices[1]);
    let originals = tempfile::tempdir().unwrap();
    let one = import(ana, "tracks", "one", originals.path(), vec![8; 3000]).await;
    let two = import(ana, "tracks", "two", originals.path(), vec![9; 3000]).await;
    let three = import(ana, "tracks", "three", originals.path(), vec![10; 3000]).await;
    network.sync(0).await;
    network.sync(1).await;
    let anas_music = tempfile::tempdir().unwrap();
    ana.handle
        .make_device_only(
            anas_music.path(),
            vec![
                (one, path("Album/one.flac")),
                (two, path("Edited/two.flac")),
                (three, path("Edited/three.flac")),
            ],
            &uncancelled(),
        )
        .await
        .unwrap();
    network
        .clock
        .set(network.clock.now() + std::time::Duration::from_secs(10));
    let carols_music = tempfile::tempdir().unwrap();
    carol
        .handle
        .make_device_only(
            carols_music.path(),
            vec![
                (one, path("one.flac")),
                (two, path("two.flac")),
                (three, path("three.flac")),
            ],
            &uncancelled(),
        )
        .await
        .unwrap();
    // Ana edits one of her copies before Carol's switch arrives.
    let edited = anas_music.path().join("Edited").join("two.flac");
    std::fs::write(&edited, b"Ana's edit").unwrap();
    network.sync(0).await;
    network.sync(1).await;
    network.sync(0).await;
    let winner = carol.handle.device_id();
    for device in [ana, carol] {
        for file in [one, two, three] {
            assert_eq!(
                device.handle.file_ref(file).await.unwrap().place(),
                FilePlace::DeviceOnly { device: winner }
            );
        }
    }
    for file in [one, two, three] {
        assert_eq!(ana.handle.user_file(file).await.unwrap(), None);
    }
    assert_eq!(names(anas_music.path()), ["Edited"]);
    assert_eq!(names(&anas_music.path().join("Edited")), ["two.flac"]);
    assert_eq!(std::fs::read(&edited).unwrap(), b"Ana's edit");
    assert_eq!(
        names(carols_music.path()),
        ["one.flac", "three.flac", "two.flac"]
    );
    assert_eq!(
        carol.handle.user_file(two).await.unwrap().unwrap().path,
        carols_music.path().join("two.flac")
    );
    let read = carol.handle.file_ref(two).await.unwrap();
    assert_eq!(carol.handle.read_file(&read).await.unwrap(), vec![9; 3000]);
    // Ana's switch is lost values of each record, on both devices.
    for device in [ana, carol] {
        let lost = device.handle.lost_values().await.unwrap();
        assert_eq!(lost.len(), 6, "a coordinates and a hash cell of each");
        assert!(lost.iter().all(|value| value.table == "coven_files"
            && matches!(&value.lost, Lost::Cell(cell) if cell.set_by.device == ana.handle.device_id())));
    }
    network.close().await;
}

/// Ana imports a track to keep on her laptop, and her laptop is still
/// downloading it to keep when Carol makes it device-only. Carol's switch
/// arriving ends Ana's request to keep it, and the download that finishes
/// after it keeps nothing: no pin, no cached bytes no record names.
#[tokio::test]
async fn a_switch_ends_a_request_to_keep_the_file_and_the_keeping_in_flight() {
    let network = household().await;
    let (ana, carol) = (&network.devices[0], &network.devices[1]);
    ana.handle.set_uploads_paused(true).await.unwrap();
    let originals = tempfile::tempdir().unwrap();
    let track = stage(
        ana,
        "kept",
        originals.path(),
        vec![12; 100_000],
        UserSource::CloudKeptHere,
        |sql, file| {
            sql.execute("INSERT INTO tracks(id,file) VALUES('kept',?1)", [file])?;
            Ok(())
        },
    )
    .await;
    let requests = || async {
        coven_database::FileDatabase::new(ana.handle.database.clone())
            .pin_requests()
            .await
            .unwrap()
    };
    let queued = ana.handle.database.upload_queue().await.unwrap();
    let location = &queued.uploads[0].location;
    let object = ObjectPath::file(ana.store, location.uploader(), location.object());
    // The download keeping it is held after its first read.
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    ana.storage.hold_next_read(object, entered, resumed).await;
    let mut downloads = ana.handle.subscribe_download_transfers();
    ana.handle.set_uploads_paused(false).await.unwrap();
    entering.await.unwrap();
    assert_eq!(requests().await[0].file, track);
    network.sync(0).await;
    network.sync(1).await;
    let music = tempfile::tempdir().unwrap();
    carol
        .handle
        .make_device_only(
            music.path(),
            vec![(track, path("kept.flac"))],
            &uncancelled(),
        )
        .await
        .unwrap();
    network.sync(1).await;
    network.sync(0).await;
    assert_eq!(
        ana.handle.file_ref(track).await.unwrap().place(),
        FilePlace::DeviceOnly {
            device: carol.handle.device_id()
        }
    );
    assert!(requests().await.is_empty(), "the switch ends the request");
    resume.send(()).unwrap();
    // Until the download in flight ends.
    loop {
        let changes = downloads.next().await.unwrap();
        if changes
            .iter()
            .any(|change| matches!(change, TransferChange::Removed { file } if *file == track))
        {
            break;
        }
    }
    assert_eq!(
        ana.handle.files_pinned(&[track]).await.unwrap(),
        [Some(false)]
    );
    assert_eq!(
        cached(ana),
        0,
        "the download keeps no bytes no record names"
    );
    assert!(ana
        .handle
        .pending()
        .await
        .unwrap()
        .iter()
        .all(|record| !matches!(record.subject, PendingSubject::Keep { .. })));
    network.close().await;
}

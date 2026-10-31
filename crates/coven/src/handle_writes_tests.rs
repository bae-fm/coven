//! Writes and the files they stage (§17.2): a new file over an existing
//! file's bytes in another audience keeps a record of its own, so the
//! stored bytes stay readable there when the first file is made
//! device-only.
use crate::tests::{wait_for_empty_queue, Device, Network};
use crate::*;
use coven_storage::Storage;

fn tables() -> Vec<SyncedTable> {
    vec![
        SyncedTable::new("tracks", RowIdentity::SharedKey).file_column(FileColumn::new("file")),
        SyncedTable::new("gifts", RowIdentity::IndependentUuid)
            .audience_column("audience")
            .file_column(FileColumn::new("file")),
    ]
}

const SCHEMA: &str = "CREATE TABLE tracks(id TEXT NOT NULL PRIMARY KEY, file TEXT);
    CREATE TABLE gifts(id TEXT NOT NULL PRIMARY KEY, file TEXT, audience TEXT NOT NULL);";

/// How many cache files the device keeps.
fn cached(device: &Device) -> usize {
    use coven_foundation::files::StoreDirAccess;
    let cache = device
        .layout
        .store_dir(&device.store)
        .database_path()
        .parent()
        .unwrap()
        .join("cache");
    std::fs::read_dir(cache).map_or(0, Iterator::count)
}

/// Ana imports a track to the store, then puts it in a Gifts row as a new
/// file over its bytes. Carol, outside Gifts, makes the store's track
/// device-only on her laptop. Gifts' file keeps its own record, stored at
/// the same location: after the switch is published and history moves past
/// it, Ana's laptop, the bytes' uploader, still keeps them in storage, and
/// Ana reads Gifts' file by downloading them.
#[tokio::test]
async fn making_the_store_file_device_only_leaves_the_circles_readable() {
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
    let (ana, carol) = (&network.devices[0], &network.devices[1]);
    let gifts = ana.handle.circles().create("Gifts").await.unwrap();
    let originals = tempfile::tempdir().unwrap();
    let original = originals.path().join("track.flac");
    std::fs::write(&original, vec![7; 100_000]).unwrap();
    let prepared = prepare_user_file(&original, |_| {}).await.unwrap();
    let track = ana
        .handle
        .write_with_files(
            move |batch| Ok(batch.put_user_file("tracks", UserSource::Cloud(prepared))),
            |sql, file| {
                sql.execute("INSERT INTO tracks(id,file) VALUES('one',?1)", [file])?;
                Ok(file)
            },
        )
        .await
        .unwrap();
    wait_for_empty_queue(&ana.handle).await;
    let audience = gifts.to_string();
    let copy = ana
        .handle
        .write_with_files(
            move |batch| Ok(batch.existing_file(track)),
            move |sql, file| {
                sql.execute(
                    "INSERT INTO gifts(id,file,audience)
                     VALUES('00000000-0000-7000-8000-000000000001',?1,?2)",
                    (file, audience),
                )?;
                Ok(file)
            },
        )
        .await
        .unwrap();
    network.sync(0).await;
    network.sync(1).await;
    let shared = ana.handle.file_ref(copy).await.unwrap();
    let FilePlace::Cloud { location, .. } = shared.place() else {
        panic!("Gifts' file is stored");
    };
    let object = ObjectPath::file(ana.store, location.uploader(), location.object());

    let music = tempfile::tempdir().unwrap();
    carol
        .handle
        .make_device_only(
            music.path(),
            vec![(track, RelativePath::new(["track.flac"]).unwrap())],
            &tokio::sync::watch::channel(false).1,
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
    // The history naming the store's stored track stays retained until a
    // snapshot covers it and the floor passes it, when its uploader would
    // delete bytes no record names.
    for _ in 0..4 {
        network
            .clock
            .set(network.clock.now() + std::time::Duration::from_secs(31 * 24 * 3600));
        network.sync(0).await;
        network.sync(1).await;
    }
    network.sync(0).await;
    assert!(
        ana.storage.status(&object).await.unwrap().is_some(),
        "Gifts' record keeps the stored bytes"
    );
    let shared = ana.handle.file_ref(copy).await.unwrap();
    assert!(
        matches!(shared.place(), FilePlace::Cloud { location: now, .. } if now == location),
        "Gifts' file stays stored where it was"
    );
    assert_eq!(ana.handle.user_file(copy).await.unwrap(), None);
    assert_eq!(cached(ana), 0, "nothing here holds the bytes");
    assert_eq!(
        ana.handle.read_file(&shared).await.unwrap(),
        vec![7; 100_000]
    );
    network.close().await;
}

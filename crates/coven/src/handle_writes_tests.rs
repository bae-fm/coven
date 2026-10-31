//! Writes and the files they stage (§17.2): a new file over an existing
//! file's bytes in another audience keeps a record of its own, so the
//! stored bytes stay readable there when the first file is made
//! device-only; and one made by a copy or a move another device hasn't sent
//! still reads them after the first file's delete, since stored bytes stay
//! until every other active device has written past the delete or switch
//! that left them unnamed (§17.10).
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
    // delete bytes no record names. Carol writes again, so her switch holds
    // nothing back (§17.10): only Gifts' record keeps the bytes.
    carol
        .handle
        .write(|sql| {
            sql.execute("INSERT INTO tracks(id) VALUES('later')", [])?;
            Ok(())
        })
        .await
        .unwrap();
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

/// Ana's store with Ben, a member in Gifts with her, and Ana's track,
/// stored, in the store's row 'one' on both devices.
async fn shared_track() -> (Network, CircleId, FileId) {
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
                name: "Ben".into(),
            },
        )
        .await;
    network.sync(0).await;
    let ana = &network.devices[0];
    let ben = ana
        .handle
        .members()
        .await
        .unwrap()
        .into_iter()
        .find(|member| !member.is_self)
        .unwrap()
        .id;
    let gifts = ana.handle.circles().create("Gifts").await.unwrap();
    ana.handle.circles().add_member(gifts, &ben).await.unwrap();
    let track = ana
        .handle
        .write_with_files(
            |batch| {
                Ok(batch.put_file(
                    "files",
                    FileSource::Stream(Box::pin(std::io::Cursor::new(vec![9; 4096]))),
                    Place::Cloud,
                ))
            },
            |sql, file| {
                sql.execute("INSERT INTO tracks(id,file) VALUES('one',?1)", [file])?;
                Ok(file)
            },
        )
        .await
        .unwrap();
    wait_for_empty_queue(&ana.handle).await;
    network.sync(0).await;
    network.sync(1).await;
    (network, gifts, track)
}

/// Where storage holds `file`'s bytes, by this device's record of it.
async fn stored_object(device: &Device, file: FileId) -> ObjectPath {
    let file = device.handle.file_ref(file).await.unwrap();
    let FilePlace::Cloud { location, .. } = file.place() else {
        panic!("the file is stored");
    };
    ObjectPath::file(device.store, location.uploader(), location.object())
}

/// Snapshots cover the history so far and the floor passes it, so no slot
/// is left to name a file the history deleted.
async fn pass_history(network: &Network, devices: &[usize]) {
    for _ in 0..4 {
        network
            .clock
            .set(network.clock.now() + std::time::Duration::from_secs(31 * 24 * 3600));
        for &device in devices {
            network.sync(device).await;
        }
    }
    network.sync(devices[0]).await;
}

/// Ben, offline, puts Ana's track in a Gifts row as a new file over its
/// bytes, while Ana deletes the track, and her delete is published first.
/// The floor passes her delete while Ben is away, but he hasn't published a
/// write that saw it, so the bytes stay, and once he is back his Gifts file
/// reads them. Then Ben deletes his Gifts row and writes again: once both
/// devices have published past every delete and history passes them,
/// nothing names the bytes and Ana deletes them.
#[tokio::test]
async fn a_concurrent_copy_reads_bytes_whose_record_was_deleted() {
    let (network, gifts, track) = shared_track().await;
    let (ana, ben) = (&network.devices[0], &network.devices[1]);
    let object = stored_object(ana, track).await;
    ben.storage.set_online(false);
    let audience = gifts.to_string();
    let copy = ben
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
    ana.handle
        .write(|sql| {
            sql.execute("DELETE FROM tracks WHERE id='one'", [])?;
            Ok(())
        })
        .await
        .unwrap();
    pass_history(&network, &[0]).await;
    assert!(
        ana.storage.status(&object).await.unwrap().is_some(),
        "Ben hasn't published a write that saw the delete"
    );
    ben.storage.set_online(true);
    network.sync(1).await;
    network.sync(0).await;
    let shared = ben.handle.file_ref(copy).await.unwrap();
    assert_eq!(ben.handle.read_file(&shared).await.unwrap(), vec![9; 4096]);

    ben.handle
        .write(|sql| {
            sql.execute("DELETE FROM gifts", [])?;
            Ok(())
        })
        .await
        .unwrap();
    network.sync(1).await;
    pass_history(&network, &[0, 1]).await;
    assert!(
        ana.storage.status(&object).await.unwrap().is_some(),
        "Ben hasn't published a write that saw his own delete"
    );
    ben.handle
        .write(|sql| {
            sql.execute("INSERT INTO tracks(id) VALUES('two')", [])?;
            Ok(())
        })
        .await
        .unwrap();
    network.sync(1).await;
    network.sync(0).await;
    assert!(
        ana.storage.status(&object).await.unwrap().is_none(),
        "both devices read past every delete and nothing names the bytes"
    );
    network.close().await;
}

/// The same for a move: Ana's photo is in a store row of `gifts`, and Ben,
/// offline, moves the row into Gifts, which gives it a new file over the
/// same bytes, while Ana deletes the row and her delete is published first.
/// The bytes stay while Ben is away, and his moved row's file reads them.
#[tokio::test]
async fn a_concurrent_move_reads_bytes_whose_record_was_deleted() {
    let (network, gifts, _) = shared_track().await;
    let (ana, ben) = (&network.devices[0], &network.devices[1]);
    let row = "00000000-0000-7000-8000-000000000002";
    let photo = ana
        .handle
        .write_with_files(
            |batch| {
                Ok(batch.put_file(
                    "files",
                    FileSource::Stream(Box::pin(std::io::Cursor::new(vec![5; 4096]))),
                    Place::Cloud,
                ))
            },
            move |sql, file| {
                sql.execute(
                    "INSERT INTO gifts(id,file,audience) VALUES(?1,?2,'store')",
                    (row, file),
                )?;
                Ok(file)
            },
        )
        .await
        .unwrap();
    wait_for_empty_queue(&ana.handle).await;
    network.sync(0).await;
    network.sync(1).await;
    let object = stored_object(ana, photo).await;
    ben.storage.set_online(false);
    let audience = gifts.to_string();
    ben.handle
        .write(move |sql| {
            sql.execute("UPDATE gifts SET audience=?1 WHERE id=?2", (audience, row))?;
            Ok(())
        })
        .await
        .unwrap();
    ana.handle
        .write(move |sql| {
            sql.execute("DELETE FROM gifts WHERE id=?1", [row])?;
            Ok(())
        })
        .await
        .unwrap();
    pass_history(&network, &[0]).await;
    assert!(
        ana.storage.status(&object).await.unwrap().is_some(),
        "Ben hasn't published a write that saw the delete"
    );
    ben.storage.set_online(true);
    network.sync(1).await;
    network.sync(0).await;
    let moved: FileId = ben
        .handle
        .read(move |sql| {
            Ok(sql.query_row("SELECT file FROM gifts WHERE id=?1", [row], |r| r.get(0))?)
        })
        .await
        .unwrap();
    assert_ne!(moved, photo, "the moved row has a file of its own");
    let moved = ben.handle.file_ref(moved).await.unwrap();
    assert_eq!(ben.handle.read_file(&moved).await.unwrap(), vec![5; 4096]);
    network.close().await;
}

/// The same out of a circle: Ana's photo is in a Gifts row, and Ben,
/// offline, puts it in a store row as a new file over its bytes, while Ana
/// deletes the Gifts row and her delete is published first. The bytes stay
/// while Ben is away, and his store file reads them, on his phone and on
/// Ana's laptop.
#[tokio::test]
async fn a_concurrent_copy_out_of_a_circle_reads_bytes_whose_record_was_deleted() {
    let (network, gifts, _) = shared_track().await;
    let (ana, ben) = (&network.devices[0], &network.devices[1]);
    let row = "00000000-0000-7000-8000-000000000003";
    let audience = gifts.to_string();
    let photo = ana
        .handle
        .write_with_files(
            |batch| {
                Ok(batch.put_file(
                    "files",
                    FileSource::Stream(Box::pin(std::io::Cursor::new(vec![6; 4096]))),
                    Place::Cloud,
                ))
            },
            move |sql, file| {
                sql.execute(
                    "INSERT INTO gifts(id,file,audience) VALUES(?1,?2,?3)",
                    (row, file, audience),
                )?;
                Ok(file)
            },
        )
        .await
        .unwrap();
    wait_for_empty_queue(&ana.handle).await;
    network.sync(0).await;
    network.sync(1).await;
    let object = stored_object(ana, photo).await;
    ben.storage.set_online(false);
    let copy = ben
        .handle
        .write_with_files(
            move |batch| Ok(batch.existing_file(photo)),
            |sql, file| {
                sql.execute("INSERT INTO tracks(id,file) VALUES('two',?1)", [file])?;
                Ok(file)
            },
        )
        .await
        .unwrap();
    ana.handle
        .write(move |sql| {
            sql.execute("DELETE FROM gifts WHERE id=?1", [row])?;
            Ok(())
        })
        .await
        .unwrap();
    pass_history(&network, &[0]).await;
    assert!(
        ana.storage.status(&object).await.unwrap().is_some(),
        "Ben hasn't published a write that saw the delete"
    );
    ben.storage.set_online(true);
    network.sync(1).await;
    network.sync(0).await;
    let shared = ben.handle.file_ref(copy).await.unwrap();
    assert_eq!(ben.handle.read_file(&shared).await.unwrap(), vec![6; 4096]);
    let shared = ana.handle.file_ref(copy).await.unwrap();
    assert_eq!(ana.handle.read_file(&shared).await.unwrap(), vec![6; 4096]);
    network.close().await;
}

/// Ana deletes her track while Ben is away. Ben, idle, has published no
/// write that saw the delete, so the bytes stay after history passes it.
/// Once Ana removes him, nothing he publishes counts, and her next pass
/// deletes them.
#[tokio::test]
async fn a_removed_device_holds_no_bytes() {
    let (network, _, track) = shared_track().await;
    let (ana, ben) = (&network.devices[0], &network.devices[1]);
    let object = stored_object(ana, track).await;
    ben.storage.set_online(false);
    ana.handle
        .write(|sql| {
            sql.execute("DELETE FROM tracks WHERE id='one'", [])?;
            Ok(())
        })
        .await
        .unwrap();
    pass_history(&network, &[0]).await;
    assert!(
        ana.storage.status(&object).await.unwrap().is_some(),
        "an idle device holds the bytes"
    );
    let removed = ana
        .handle
        .members()
        .await
        .unwrap()
        .into_iter()
        .find(|member| !member.is_self)
        .unwrap()
        .id;
    ana.handle.remove_member(&removed).await.unwrap();
    network.sync(0).await;
    assert!(
        ana.storage.status(&object).await.unwrap().is_none(),
        "a removed device holds nothing"
    );
    network.close().await;
}

/// Ben makes Ana's imported track device-only on his laptop, and his switch
/// is published. History passes it, but the switch read only through the
/// slot before its own, so the stored bytes wait for Ben's next write, and
/// then Ana's laptop, their uploader, deletes them.
#[tokio::test]
async fn a_switch_holds_the_bytes_until_its_device_writes_again() {
    let (network, _, _) = shared_track().await;
    let (ana, ben) = (&network.devices[0], &network.devices[1]);
    let originals = tempfile::tempdir().unwrap();
    let original = originals.path().join("track.flac");
    std::fs::write(&original, vec![3; 50_000]).unwrap();
    let prepared = prepare_user_file(&original, |_| {}).await.unwrap();
    let track = ana
        .handle
        .write_with_files(
            move |batch| Ok(batch.put_user_file("tracks", UserSource::Cloud(prepared))),
            |sql, file| {
                sql.execute("INSERT INTO tracks(id,file) VALUES('imported',?1)", [file])?;
                Ok(file)
            },
        )
        .await
        .unwrap();
    wait_for_empty_queue(&ana.handle).await;
    network.sync(0).await;
    network.sync(1).await;
    let object = stored_object(ana, track).await;
    let music = tempfile::tempdir().unwrap();
    ben.handle
        .make_device_only(
            music.path(),
            vec![(track, RelativePath::new(["track.flac"]).unwrap())],
            &tokio::sync::watch::channel(false).1,
        )
        .await
        .unwrap();
    network.sync(1).await;
    pass_history(&network, &[0, 1]).await;
    assert!(
        ana.storage.status(&object).await.unwrap().is_some(),
        "Ben hasn't published a write that saw his switch"
    );
    ben.handle
        .write(|sql| {
            sql.execute("INSERT INTO tracks(id) VALUES('later')", [])?;
            Ok(())
        })
        .await
        .unwrap();
    network.sync(1).await;
    network.sync(0).await;
    assert!(
        ana.storage.status(&object).await.unwrap().is_none(),
        "the uploader deletes stored bytes no record names"
    );
    network.close().await;
}

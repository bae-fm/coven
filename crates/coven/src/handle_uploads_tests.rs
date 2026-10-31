//! The upload queue with bound references (§17.4): a write setting one, and
//! every later write, wait for its file to be stored, and other devices get
//! the row and the stored file in one slot. A bound upload fails as a
//! trailing one does, and the app's pause leaves it running. The app finds
//! each trailing upload's move in `coven_uploads` and can cancel it, but
//! never a bound upload's (§17.3).
use crate::tests::{file_stored, upload_path, wait_for_sent, Device, Network};
use crate::*;
use coven_storage::test_utils::Faults;
use std::time::Duration;

fn tables() -> Vec<SyncedTable> {
    vec![
        SyncedTable::new("covers", RowIdentity::SharedKey)
            .file_column(FileColumn::new("file").bound()),
        SyncedTable::new("photos", RowIdentity::SharedKey).file_column(FileColumn::new("file")),
        SyncedTable::new("tracks", RowIdentity::SharedKey).file_column(FileColumn::new("file")),
        SyncedTable::new("notes", RowIdentity::SharedKey),
    ]
}

const SCHEMA: &str = "CREATE TABLE covers(id TEXT NOT NULL PRIMARY KEY, file TEXT);
    CREATE TABLE photos(id TEXT NOT NULL PRIMARY KEY, file TEXT);
    CREATE TABLE tracks(id TEXT NOT NULL PRIMARY KEY, file TEXT);
    CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL);";

/// Ana's Household store on S3, with Ben joined on device 1.
async fn household() -> Network {
    let mut network = Network::with_schema(
        CloudProvider::S3,
        tables(),
        vec![Migration::addition(1, "covers", SCHEMA)],
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
    network.sync(1).await;
    network
}

/// Stage `bytes` in the app's `files` namespace for the cloud, in row `id`
/// of `table`.
async fn stage(device: &Device, table: &'static str, id: &str, bytes: Vec<u8>) -> FileId {
    let id = id.to_owned();
    device
        .handle
        .write_with_files(
            move |batch| Ok(batch.put_file("files", bytes, Place::Cloud)),
            move |sql, file| {
                sql.execute(
                    &format!("INSERT INTO {table}(id,file) VALUES(?1,?2)"),
                    (&id, file),
                )?;
                Ok(file)
            },
        )
        .await
        .unwrap()
}

async fn note(device: &Device, id: &str) {
    let id = id.to_owned();
    device
        .handle
        .write(move |sql| {
            sql.execute("INSERT INTO notes(id,title) VALUES(?1,?1)", [&id])?;
            Ok(())
        })
        .await
        .unwrap();
}

/// The notes the device shows.
async fn notes(device: &Device) -> Vec<String> {
    device
        .handle
        .read(|sql| {
            Ok(sql.query("SELECT id FROM notes ORDER BY id", [], |r| {
                r.get::<_, String>(0)
            })?)
        })
        .await
        .unwrap()
}

/// Each cover the device shows, with whether its file's record says it is
/// stored.
async fn covers(device: &Device) -> Vec<(String, bool)> {
    device
        .handle
        .read(|sql| {
            Ok(sql.query(
                "SELECT c.id,coven_file_device(f.coordinates) IS NULL
                 FROM covers AS c JOIN coven_files AS f ON f.id=c.file ORDER BY c.id",
                [],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, bool>(1)?)),
            )?)
        })
        .await
        .unwrap()
}

/// Stop the device's sync, so no upload or pass starts until it resumes.
async fn stopped(device: &Device) {
    let mut status = device.handle.subscribe_sync_status();
    device.handle.stop_sync();
    status
        .wait_for(|s| matches!(s, SyncStatus::Stopped))
        .await
        .unwrap();
}

/// Wait until the device records `failure` for an upload.
async fn upload_failed(device: &Device, failure: StorageFailure) {
    let mut pending = device.handle.subscribe(|sql| {
        Ok(sql.query(
            "SELECT record FROM coven_pending ORDER BY position",
            [],
            |r| r.get::<_, PendingRecord>(0),
        )?)
    });
    tokio::time::timeout(Duration::from_secs(10), async {
        while !pending
            .next()
            .await
            .expect("the live query answers")
            .unwrap()
            .iter()
            .any(|record| {
                matches!(record.subject, PendingSubject::Upload { .. })
                    && matches!(record.reason, PendingReason::Storage { failure: f, .. } if f == failure)
            })
        {}
    })
    .await
    .unwrap();
}

/// Ana's cover write and a later note wait while the cover's upload fails
/// for want of space, which only the person clears: her passes send
/// neither, and Ben sees neither. Once she frees space and retries, the
/// completion joins them, and Ben's next pass applies all three in one slot,
/// the cover row arriving with its stored file.
#[tokio::test]
async fn a_held_write_and_every_later_one_wait_for_the_file_and_arrive_with_it() {
    let network = household().await;
    let (ana, ben) = (&network.devices[0], &network.devices[1]);
    stopped(ana).await;
    let cover = stage(ana, "covers", "album", vec![5; 4096]).await;
    note(ana, "rating").await;
    let mut faults = Faults::none();
    faults.create_failures.insert(
        upload_path(ana, cover).await,
        (StorageFailure::QuotaExceeded, 507),
    );
    ana.storage.set_faults(faults).await;
    ana.handle.start_sync().await.unwrap();
    upload_failed(ana, StorageFailure::QuotaExceeded).await;
    network.sync(0).await;
    assert_eq!(ana.handle.test_queued_writes().await.unwrap().len(), 2);
    network.sync(1).await;
    assert!(covers(ben).await.is_empty());
    assert!(notes(ben).await.is_empty());

    let slot = ben.handle.applied_slot().await;
    ana.storage.set_faults(Faults::none()).await;
    ana.handle.retry_uploads_now().await.unwrap();
    wait_for_sent(&ana.handle).await;
    network.sync(1).await;
    assert_eq!(covers(ben).await, [("album".to_owned(), true)]);
    assert_eq!(notes(ben).await, ["rating"]);
    assert_eq!(ben.handle.applied_slot().await, slot + 1);
    assert!(file_stored(ben, cover).await);
    network.close().await;
}

/// A network failure of a bound upload retries under backoff by itself, as
/// a trailing upload's does, uploads paused or not: the writes wait
/// meanwhile, and go once it succeeds, with no call of the app's.
#[tokio::test]
async fn a_failing_bound_upload_retries_by_itself_while_its_writes_wait() {
    let network = household().await;
    let (ana, ben) = (&network.devices[0], &network.devices[1]);
    stopped(ana).await;
    ana.handle.set_uploads_paused(true).await.unwrap();
    let cover = stage(ana, "covers", "album", vec![6; 4096]).await;
    let mut faults = Faults::none();
    faults.create_failures.insert(
        upload_path(ana, cover).await,
        (StorageFailure::Network, 503),
    );
    ana.storage.set_faults(faults).await;
    ana.handle.start_sync().await.unwrap();
    upload_failed(ana, StorageFailure::Network).await;
    network.sync(0).await;
    assert_eq!(ana.handle.test_queued_writes().await.unwrap().len(), 1);
    ana.storage.set_faults(Faults::none()).await;
    wait_for_sent(&ana.handle).await;
    network.sync(1).await;
    assert_eq!(covers(ben).await, [("album".to_owned(), true)]);
    network.close().await;
}

/// Ana pauses uploads. Her photo, a trailing file, waits with its row
/// already sent; her cover, bound, uploads anyway, since its write waits
/// for it, and Ben gets the cover while the photo stays on Ana's device.
#[tokio::test]
async fn the_pause_holds_trailing_uploads_and_leaves_bound_ones_running() {
    let network = household().await;
    let (ana, ben) = (&network.devices[0], &network.devices[1]);
    ana.handle.set_uploads_paused(true).await.unwrap();
    let photo = stage(ana, "photos", "beach", vec![7; 4096]).await;
    let cover = stage(ana, "covers", "album", vec![8; 4096]).await;
    wait_for_sent(&ana.handle).await;
    assert!(file_stored(ana, cover).await);
    let queue = ana.handle.uploads().await.unwrap();
    assert!(queue.paused);
    assert_eq!(
        queue.files.iter().map(|q| q.file).collect::<Vec<_>>(),
        [photo]
    );
    let paused: bool = ana
        .handle
        .read(move |sql| {
            Ok(sql.query_row(
                "SELECT paused FROM coven_uploads WHERE file=?1",
                [photo],
                |r| r.get(0),
            )?)
        })
        .await
        .unwrap();
    assert!(paused);
    network.sync(1).await;
    assert_eq!(covers(ben).await, [("album".to_owned(), true)]);
    assert!(matches!(
        ben.handle.file_ref(photo).await.unwrap().place(),
        FilePlace::DeviceOnly { .. }
    ));
    network.close().await;
}

/// Each queued file of `table`'s rows, by row id, with the move
/// `coven_uploads` names for it.
async fn moves(device: &Device, table: &'static str) -> Vec<(String, Option<MoveId>)> {
    device
        .handle
        .read(move |sql| {
            Ok(sql.query(
                &format!(
                    "SELECT t.id,u.move FROM {table} AS t JOIN coven_uploads AS u ON u.file=t.file
                     ORDER BY t.id"
                ),
                [],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, Option<MoveId>>(1)?)),
            )?)
        })
        .await
        .unwrap()
}

/// Ana imports an album of two tracks to the cloud in one write, which
/// gives back only the files' ids. Her app finds the import's move by
/// joining its tracks with `coven_uploads`, and cancels the import while it
/// uploads: the queue empties, and both tracks stay on her laptop only.
#[tokio::test]
async fn the_app_finds_a_cloud_imports_move_in_coven_uploads_and_cancels_it() {
    let network = household().await;
    let ana = &network.devices[0];
    stopped(ana).await;
    let originals = tempfile::tempdir().unwrap();
    let mut prepared = Vec::new();
    for name in ["one", "two"] {
        let path = originals.path().join(name);
        std::fs::write(&path, name.repeat(1000)).unwrap();
        prepared.push((name, prepare_user_file(&path, |_| {}).await.unwrap()));
    }
    let files = ana
        .handle
        .write_with_files(
            move |batch| {
                Ok(prepared
                    .into_iter()
                    .map(|(name, prepared)| {
                        (
                            name,
                            batch.put_user_file("tracks", UserSource::Cloud(prepared)),
                        )
                    })
                    .collect::<Vec<_>>())
            },
            |sql, files| {
                for (name, file) in &files {
                    sql.execute("INSERT INTO tracks(id,file) VALUES(?1,?2)", (name, file))?;
                }
                Ok(files.into_iter().map(|(_, file)| file).collect::<Vec<_>>())
            },
        )
        .await
        .unwrap();
    let queued = moves(ana, "tracks").await;
    let [(_, Some(id)), (_, Some(other))] = queued[..] else {
        panic!("both tracks name a move: {queued:?}");
    };
    assert_eq!(id, other, "one write's cloud files are one move");
    ana.handle
        .write(move |sql| Ok(sql.cancel_move(id)?))
        .await
        .unwrap();
    assert!(moves(ana, "tracks").await.is_empty());
    for file in files {
        assert!(matches!(
            ana.handle.file_ref(file).await.unwrap().place(),
            FilePlace::DeviceOnly { .. }
        ));
    }
    network.close().await;
}

/// One write stages a bound cover and a trailing photo. `coven_uploads`
/// names the photo's move, which cancels, and no move for the cover, whose
/// cancel would let its held write go out naming a file no other device can
/// read.
#[tokio::test]
async fn coven_uploads_names_no_move_for_a_bound_upload() {
    let network = household().await;
    let ana = &network.devices[0];
    stopped(ana).await;
    ana.handle
        .write_with_files(
            |batch| {
                Ok((
                    batch.put_file("files", vec![1; 4096], Place::Cloud),
                    batch.put_file("files", vec![2; 4096], Place::Cloud),
                ))
            },
            |sql, (cover, photo)| {
                sql.execute("INSERT INTO covers(id,file) VALUES('album',?1)", [cover])?;
                sql.execute("INSERT INTO photos(id,file) VALUES('beach',?1)", [photo])?;
                Ok(())
            },
        )
        .await
        .unwrap();
    assert_eq!(moves(ana, "covers").await, [("album".to_owned(), None)]);
    let queued = moves(ana, "photos").await;
    let [(_, Some(id))] = queued[..] else {
        panic!("the photo names a move: {queued:?}");
    };
    ana.handle
        .write(move |sql| Ok(sql.cancel_move(id)?))
        .await
        .unwrap();
    assert!(moves(ana, "photos").await.is_empty());
    assert_eq!(moves(ana, "covers").await, [("album".to_owned(), None)]);
    network.close().await;
}

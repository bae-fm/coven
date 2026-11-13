//! The upload queue with bound references (§17.4): a write setting one, and
//! every later write, wait for its file to be stored, and other devices get
//! the row and the stored file in one slot. A bound upload fails as a
//! trailing one does, and the app's pause leaves it running. The app finds
//! each trailing upload's move in `coven_uploads` and can cancel it, but
//! never a bound upload's (§17.3).
use crate::tests::{file_stored, stage, upload_path, wait_for_sent, Device, Network};
use crate::*;
use coven_foundation::clock::HANG_GUARD;
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
    network.join_as(1, MemberRole::Member).await;
    network.sync(0).await;
    network.sync(1).await;
    network
}

async fn note(device: &Device, id: &str) {
    let id = id.to_owned();
    device
        .handle
        .write(move |sql| {
            sql.execute("INSERT INTO notes(id,title) VALUES(?1,?1)", [&id])?;
            Ok::<_, CovenError>(())
        })
        .await
        .unwrap();
}

/// The notes the device shows.
async fn notes(device: &Device) -> Vec<String> {
    device
        .handle
        .read(|sql| {
            Ok::<_, CovenError>(
                sql.query("SELECT id FROM notes ORDER BY id", [], |r| r.get::<_, String>(0))?,
            )
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
            Ok::<_, CovenError>(sql.query(
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
/// Whether device `index` has applied the store audience's raise to
/// `number`.
async fn raised_to(network: &Network, index: usize, number: u32) -> bool {
    let state = network.devices[index].handle.store_state().await;
    state.schema.get(&Audience::Store).is_some_and(|version| version.number == number)
}

/// How many writes device `index` holds queued.
async fn queued_writes(network: &Network, index: usize) -> usize {
    network.devices[index].handle.test_queued_writes().await.unwrap().len()
}

/// Wait until device `index`'s sync has nothing left that wakes it, then
/// run `passes` more passes, as an app's pull to refresh does, each followed
/// by the same wait. A pass's own commits ask for no pass (§6.7), so what
/// one of them starts waits for a pass the test runs.
async fn settle(network: &Network, index: usize, passes: usize) {
    let handle = &network.devices[index].handle;
    handle.test_parked().await;
    for _ in 0..passes {
        network.sync(index).await;
        handle.test_parked().await;
    }
}

async fn stopped(device: &Device) {
    device.stop_sync().await;
}

/// Wait until the device records `failure` for an upload.
async fn upload_failed(device: &Device, failure: StorageFailure) {
    let mut pending: crate::LiveQuery<_, crate::CovenError> = device.handle.subscribe(|sql| {
        Ok(sql.query("SELECT record FROM coven_pending ORDER BY position", [], |r| {
            r.get::<_, PendingRecord>(0)
        })?)
    });
    tokio::time::timeout(HANG_GUARD, async {
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
    faults
        .create_failures
        .insert(upload_path(ana, cover).await, (StorageFailure::QuotaExceeded, 507));
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

/// Ana's cover can't upload, and the account stays full: her write and the
/// rating after it wait for it. She gives up the cover by deleting its row,
/// which leaves the file with no reference, so its record goes, the upload
/// retires and the hold ends (§17.4). Her passes send the cover's write, the
/// rating and the delete in one slot; Ben sees the rating and no cover, and
/// the file never reached storage.
#[tokio::test]
async fn deleting_the_row_of_a_cover_that_cannot_upload_ends_its_hold() {
    let network = household().await;
    let (ana, ben) = (&network.devices[0], &network.devices[1]);
    stopped(ana).await;
    let cover = stage(ana, "covers", "album", vec![5; 4096]).await;
    note(ana, "rating").await;
    let mut faults = Faults::none();
    faults
        .create_failures
        .insert(upload_path(ana, cover).await, (StorageFailure::QuotaExceeded, 507));
    ana.storage.set_faults(faults).await;
    ana.handle.start_sync().await.unwrap();
    upload_failed(ana, StorageFailure::QuotaExceeded).await;
    network.sync(0).await;
    assert_eq!(ana.handle.test_queued_writes().await.unwrap().len(), 2);

    ana.handle
        .write(|sql| {
            sql.execute("DELETE FROM covers WHERE id='album'", [])?;
            Ok::<_, CovenError>(())
        })
        .await
        .unwrap();
    wait_for_sent(&ana.handle).await;
    assert!(ana.handle.uploads().await.unwrap().files.is_empty());
    let slot = ben.handle.applied_slot().await;
    network.sync(1).await;
    assert!(covers(ben).await.is_empty());
    assert_eq!(notes(ben).await, ["rating"]);
    assert_eq!(ben.handle.applied_slot().await, slot + 1);
    assert!(ana.handle.file_ref(cover).await.is_err());
    network.close().await;
}

/// A network failure of a bound upload retries under backoff by itself, as
/// a trailing upload's does, uploads paused or not: the writes wait
/// meanwhile, and go once it succeeds, with no call of the app's. The
/// backoff passes as the test moves the clock to its end.
#[tokio::test]
async fn a_failing_bound_upload_retries_by_itself_while_its_writes_wait() {
    let network = household().await;
    let (ana, ben) = (&network.devices[0], &network.devices[1]);
    stopped(ana).await;
    ana.handle.set_uploads_paused(true).await.unwrap();
    let cover = stage(ana, "covers", "album", vec![6; 4096]).await;
    let mut faults = Faults::none();
    faults.create_failures.insert(upload_path(ana, cover).await, (StorageFailure::Network, 503));
    ana.storage.set_faults(faults).await;
    ana.handle.start_sync().await.unwrap();
    upload_failed(ana, StorageFailure::Network).await;
    // Once sync has nothing left to do, the write still waits for its cover.
    ana.handle.test_parked().await;
    assert_eq!(ana.handle.test_queued_writes().await.unwrap().len(), 1);
    ana.storage.set_faults(Faults::none()).await;
    network.end_wait(crate::tests::upload_wait(&ana.handle, cover, |_| true).await);
    wait_for_sent(&ana.handle).await;
    network.sync(1).await;
    assert_eq!(covers(ben).await, [("album".to_owned(), true)]);
    network.assert_moved_only_by_the_test();
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
    assert_eq!(queue.files.iter().map(|q| q.file).collect::<Vec<_>>(), [photo]);
    let paused: bool = ana
        .handle
        .read(move |sql| {
            Ok::<_, CovenError>(sql.query_row(
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
            Ok::<_, CovenError>(sql.query(
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
        .write(move |sql| {
            let mut files = Vec::new();
            for (name, prepared) in prepared {
                let file = sql.put_user_file("tracks", prepared, Place::Cloud)?;
                sql.execute("INSERT INTO tracks(id,file) VALUES(?1,?2)", (name, file))?;
                files.push(file);
            }
            Ok::<_, CovenError>(files)
        })
        .await
        .unwrap();
    let queued = moves(ana, "tracks").await;
    let [(_, Some(id)), (_, Some(other))] = queued[..] else {
        panic!("both tracks name a move: {queued:?}");
    };
    assert_eq!(id, other, "one write's cloud files are one move");
    ana.handle.write(move |sql| Ok::<_, CovenError>(sql.cancel_move(id)?)).await.unwrap();
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
        .test_staged_write(
            |batch| {
                Ok((
                    batch.put_file("files", vec![1; 4096], Place::Cloud),
                    batch.put_file("files", vec![2; 4096], Place::Cloud),
                ))
            },
            |sql, (cover, photo)| {
                sql.execute("INSERT INTO covers(id,file) VALUES('album',?1)", [cover])?;
                sql.execute("INSERT INTO photos(id,file) VALUES('beach',?1)", [photo])?;
                Ok::<_, CovenError>(())
            },
        )
        .await
        .unwrap();
    assert_eq!(moves(ana, "covers").await, [("album".to_owned(), None)]);
    let queued = moves(ana, "photos").await;
    let [(_, Some(id))] = queued[..] else {
        panic!("the photo names a move: {queued:?}");
    };
    ana.handle.write(move |sql| Ok::<_, CovenError>(sql.cancel_move(id)?)).await.unwrap();
    assert!(moves(ana, "photos").await.is_empty());
    assert_eq!(moves(ana, "covers").await, [("album".to_owned(), None)]);
    network.close().await;
}

/// The household's migrations, then a breaking version 2 that dismisses late
/// old-schema writes.
fn raised() -> Vec<Migration> {
    vec![
        Migration::addition(1, "covers", SCHEMA),
        Migration::breaking(
            2,
            "notes by title",
            "CREATE INDEX notes_by_title ON notes(title)",
            OldWrites::Drop,
            tables(),
        ),
    ]
}

/// Close device `index` and open it again with [`raised`], as an update of
/// its app does, then start its sync.
async fn update(network: &mut Network, index: usize) {
    let device = &network.devices[index];
    device.handle.close().await.unwrap();
    let handle =
        network.reopen(device).migrations(raised()).open(device.store, |_| ()).await.unwrap();
    handle.start_sync().await.unwrap();
    network.devices[index].handle = handle;
}

/// Ana's cover write is held when her app updates with a breaking migration,
/// so its marker comes after the held write (§17.4). While the cover's upload
/// fails for want of space, her raise waits for the held write without a
/// failure of its own; the upload's record says why. Once the cover is
/// stored, the held write and the marker go out, the raise follows, and the
/// completion after it. Ben, still on the old app, stops at the slot holding
/// the held write and the marker; once his app updates he loads Ana's raise,
/// and the cover reaches him with its stored file (§18.5).
#[tokio::test]
async fn a_raise_waits_for_a_held_write_and_follows_it() {
    let mut network = household().await;
    let ana = &network.devices[0];
    stopped(ana).await;
    let cover = stage(ana, "covers", "album", vec![5; 4096]).await;
    let mut faults = Faults::none();
    faults
        .create_failures
        .insert(upload_path(ana, cover).await, (StorageFailure::QuotaExceeded, 507));
    ana.storage.set_faults(faults).await;
    update(&mut network, 0).await;
    let ana = &network.devices[0];
    upload_failed(ana, StorageFailure::QuotaExceeded).await;
    network.sync(0).await;
    assert_eq!(ana.handle.test_queued_writes().await.unwrap().len(), 2);
    assert!(ana.handle.store_state().await.schema.is_empty());

    ana.storage.set_faults(Faults::none()).await;
    // The failed attempt's record is committed before the attempt ends, and
    // a retry asked while it ends is answered by it; the upload worker
    // parking says it has ended.
    ana.handle.test_parked().await;
    ana.handle.retry_uploads_now().await.unwrap();
    // The stored cover lets the loop send what waited for it; the step its
    // own commits start waits for a pass.
    settle(&network, 0, 1).await;
    assert_eq!(queued_writes(&network, 0).await, 0);
    assert!(raised_to(&network, 0, 2).await);
    assert!(ana.handle.test_queued_writes().await.unwrap().is_empty());

    let ben = &network.devices[1];
    let mut last = ben.handle.test_last_pass();
    ben.handle.sync_now();
    last.wait_for(|last| {
        matches!(
            last,
            Some(LastPass::Blocked {
                reason: PendingReason::UpdateRequired(RequiredUpdate::AppSchema { version: 2 }),
                ..
            })
        )
    })
    .await;
    assert!(covers(ben).await.is_empty());
    update(&mut network, 1).await;
    network.sync(1).await;
    let ben = &network.devices[1];
    assert_eq!(covers(ben).await, [("album".to_owned(), true)]);
    assert!(file_stored(ben, cover).await);
    network.sync(0).await;
    network.close().await;
}
/// The household's migrations with a breaking version 2 whose late
/// old-schema writes convert unchanged.
fn converting() -> Vec<Migration> {
    vec![
        Migration::addition(1, "covers", SCHEMA),
        Migration::breaking(
            2,
            "notes by title",
            "CREATE INDEX notes_by_title ON notes(title)",
            OldWrites::Convert(std::sync::Arc::new(|_| Ok(()))),
            tables(),
        ),
    ]
}

/// Ben, on the old app, writes a note while his sync is stopped and Ana
/// raises the store to version 2. His pass stops at the raise, which his app
/// can't apply, and sends nothing, so Ana never sees the note. After his app
/// updates, his write goes out unchanged, at schema version 1, and Ana
/// applies it (§18.3).
#[tokio::test]
async fn an_old_app_holds_its_unsent_writes_and_sends_them_unchanged_after_updating() {
    let mut network = household().await;
    let ben = &network.devices[1];
    stopped(ben).await;
    note(ben, "offline").await;
    let held = ben.handle.test_queued_writes().await.unwrap();
    assert_eq!(held.len(), 1);
    assert_eq!(held[0].header.schema_version, 1);

    let device = &network.devices[0];
    device.handle.close().await.unwrap();
    let handle =
        network.reopen(device).migrations(converting()).open(device.store, |_| ()).await.unwrap();
    handle.start_sync().await.unwrap();
    network.devices[0].handle = handle;
    settle(&network, 0, 0).await;
    assert!(raised_to(&network, 0, 2).await);

    let ben = &network.devices[1];
    let mut last = ben.handle.test_last_pass();
    ben.handle.start_sync().await.unwrap();
    ben.handle.sync_now();
    last.wait_for(|last| {
        matches!(
            last,
            Some(LastPass::Blocked {
                reason: PendingReason::UpdateRequired(RequiredUpdate::AppSchema { version: 2 }),
                ..
            })
        )
    })
    .await;
    assert_eq!(ben.handle.test_queued_writes().await.unwrap(), held);
    network.sync(0).await;
    assert!(notes(&network.devices[0]).await.is_empty());

    let device = &network.devices[1];
    device.handle.close().await.unwrap();
    let handle =
        network.reopen(device).migrations(converting()).open(device.store, |_| ()).await.unwrap();
    handle.start_sync().await.unwrap();
    network.devices[1].handle = handle;
    settle(&network, 1, 0).await;
    assert_eq!(queued_writes(&network, 1).await, 0);
    network.sync(0).await;
    assert_eq!(notes(&network.devices[0]).await, ["offline"]);
    network.close().await;
}
/// While Ana's raise waits for her held cover write, her history ages past
/// grace, which asks for a snapshot of it. The raise's own snapshot is the
/// next one for the store: no ordinary snapshot, written at version 2 her
/// store's history doesn't hold yet, goes out ahead of it, so Cara, joining
/// on the old app meanwhile, loads the store instead of finding a snapshot
/// she can't read (§18.3, §18.5).
#[tokio::test]
async fn a_waiting_raise_keeps_ordinary_snapshots_behind_it() {
    let mut network = household().await;
    let ana = &network.devices[0];
    stopped(ana).await;
    let cover = stage(ana, "covers", "album", vec![5; 4096]).await;
    let mut faults = Faults::none();
    faults
        .create_failures
        .insert(upload_path(ana, cover).await, (StorageFailure::QuotaExceeded, 507));
    ana.storage.set_faults(faults).await;
    update(&mut network, 0).await;
    let ana = &network.devices[0];
    upload_failed(ana, StorageFailure::QuotaExceeded).await;
    network.sync(0).await;
    // Moving the clock ends Ben's idle wait too, and his pass may snapshot
    // his own history; only Ana's ordinary snapshots wait for her raise.
    let own = async |ana: &Device| {
        let device = ana.handle.device_id();
        crate::tests::stored(&ana.storage, ana.store, |path| {
            path.snapshot_id().is_some_and(|snapshot| snapshot.device == device)
        })
        .await
        .len()
    };
    let before = own(ana).await;
    network.clock.set(network.clock.now() + Duration::from_secs(31 * 24 * 3600));
    network.sync(0).await;
    let ana = &network.devices[0];
    assert!(ana.handle.store_state().await.schema.is_empty());
    assert_eq!(own(ana).await, before);
    let joined = network.try_join_as(2, MemberRole::Member).await;
    assert!(joined.is_ok(), "{joined:?}");
    let ana = &network.devices[0];

    ana.storage.set_faults(Faults::none()).await;
    // The failed attempt's record is committed before the attempt ends, and
    // a retry asked while it ends is answered by it; the upload worker
    // parking says it has ended.
    ana.handle.test_parked().await;
    ana.handle.retry_uploads_now().await.unwrap();
    // The stored cover lets the loop send what waited for it; the step its
    // own commits start waits for a pass.
    settle(&network, 0, 1).await;
    assert_eq!(queued_writes(&network, 0).await, 0);
    assert!(raised_to(&network, 0, 2).await);
    let ben = &network.devices[1];
    let mut last = ben.handle.test_last_pass();
    ben.handle.sync_now();
    last.wait_for(|last| {
        matches!(
            last,
            Some(LastPass::Blocked {
                reason: PendingReason::UpdateRequired(RequiredUpdate::AppSchema { version: 2 }),
                ..
            })
        )
    })
    .await;
    network.close().await;
}

/// The household's migrations with a breaking version 2 that retitles every
/// note.
fn retitled() -> Vec<Migration> {
    vec![
        Migration::addition(1, "covers", SCHEMA),
        Migration::breaking(
            2,
            "retitle notes",
            "UPDATE notes SET title='migrated'",
            OldWrites::Drop,
            tables(),
        ),
    ]
}

/// Close device `index` and open it again with [`retitled`], then start its
/// sync.
async fn retitle(network: &mut Network, index: usize) {
    let device = &network.devices[index];
    device.handle.close().await.unwrap();
    let handle =
        network.reopen(device).migrations(retitled()).open(device.store, |_| ()).await.unwrap();
    handle.start_sync().await.unwrap();
    network.devices[index].handle = handle;
}

/// Each note the device shows, with its title.
async fn titles(device: &Device) -> Vec<(String, String)> {
    device
        .handle
        .read(|sql| {
            Ok::<_, CovenError>(sql.query("SELECT id,title FROM notes ORDER BY id", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })?)
        })
        .await
        .unwrap()
}

/// Ana's app updates offline, its migration retitling notes `m` and `n`,
/// and in one write she edits `n`'s title and deletes `m`, so both changes
/// name her migration write as a setter. Ben's app updates too, and his raise
/// is established first, so Ana's migration write never counts and no
/// history holds its changes. Ana loads Ben's raise without getting stuck,
/// and both changes, built on data that never counted, become lost values on
/// both devices, with her migration write as their dropped source (§9.5,
/// §18.8).
#[tokio::test]
async fn changes_on_a_migration_another_raise_replaced_are_lost_everywhere() {
    let mut network = household().await;
    note(&network.devices[0], "m").await;
    note(&network.devices[0], "n").await;
    wait_for_sent(&network.devices[0].handle).await;
    network.sync(1).await;

    network.devices[0].storage.set_online(false);
    retitle(&mut network, 0).await;
    let ana = &network.devices[0];
    ana.handle
        .write(|sql| {
            sql.execute("UPDATE notes SET title='edited' WHERE id='n'", [])?;
            sql.execute("DELETE FROM notes WHERE id='m'", [])?;
            Ok::<_, CovenError>(())
        })
        .await
        .unwrap();
    let queued = ana.handle.test_queued_writes().await.unwrap();
    let [marker, edit] = [&queued[0], &queued[1]].map(|write| write.header.position);

    retitle(&mut network, 1).await;
    settle(&network, 1, 0).await;
    assert!(raised_to(&network, 1, 2).await);

    // Back online, Ana's offline backoff holds her sync until a pass the
    // test asks for, and the step that pass's commits start waits for a
    // second.
    network.devices[0].storage.set_online(true);
    settle(&network, 0, 2).await;
    assert_eq!(queued_writes(&network, 0).await, 0);
    network.sync(1).await;
    network.sync(0).await;
    let (ana, ben) = (&network.devices[0], &network.devices[1]);
    assert!(ana.handle.test_queued_writes().await.unwrap().is_empty());
    assert!(ana.handle.pending().await.unwrap().is_empty());
    assert_eq!(ana.handle.applied_slot().await, ben.handle.applied_slot().await);
    let text = |value: &str| types::Value::Text(value.into());
    let source = |key: &str| ChangeSource {
        write: marker,
        table: "notes".into(),
        key: RowKey::from(key),
        audience: Audience::Store,
    };
    for device in [ana, ben] {
        assert_eq!(
            titles(device).await,
            [("m".to_owned(), "migrated".to_owned()), ("n".to_owned(), "migrated".to_owned())]
        );
        let lost: Vec<_> = device
            .handle
            .lost_values()
            .await
            .unwrap()
            .into_iter()
            .map(|lost| {
                let Lost::Row(cells) = lost.lost else {
                    panic!("a frozen row change: {lost:?}");
                };
                let cells: Vec<_> =
                    cells.into_iter().map(|cell| (cell.column, cell.value, cell.set_by)).collect();
                (lost.table, lost.key, cells, lost.replaced_by)
            })
            .collect();
        let dependent =
            |key: &str| Replacement::DataDependency { write: edit, sources: vec![source(key)] };
        assert_eq!(
            lost,
            [
                (
                    "notes".to_owned(),
                    RowKey::from("m"),
                    vec![
                        ("id".to_owned(), text("m"), edit),
                        ("title".to_owned(), text("migrated"), edit)
                    ],
                    dependent("m")
                ),
                (
                    "notes".to_owned(),
                    RowKey::from("n"),
                    vec![("title".to_owned(), text("edited"), edit)],
                    dependent("n")
                )
            ]
        );
    }
    assert_eq!(ana.handle.lost_values().await.unwrap(), ben.handle.lost_values().await.unwrap());
    // Ana's failed passes while offline backed off on held waits: only the
    // test's passes moved the clock.
    network.assert_moved_only_by_the_test();
    network.close().await;
}

/// Stopping sync stops both lanes, though the handle is connected, as
/// unlocking its store key leaves it: neither the cover nor the photo
/// uploads, and an early retry says sync is stopped. Starting sync sends
/// both (§17.4).
#[tokio::test]
async fn both_lanes_wait_while_sync_is_stopped() {
    let network = household().await;
    let ana = &network.devices[0];
    stopped(ana).await;
    ana.handle.unlock_store_key().await.unwrap();
    let cover = stage(ana, "covers", "abbey-road", vec![1; 1024]).await;
    let photo = stage(ana, "photos", "studio", vec![2; 2048]).await;
    assert_eq!(ana.handle.retry_uploads_now().await.unwrap(), DrainOutcome::NotSyncing);
    ana.handle.test_parked().await;
    assert!(!file_stored(ana, cover).await, "a stopped sync sent the cover");
    assert!(!file_stored(ana, photo).await, "a stopped sync sent the photo");
    ana.handle.start_sync().await.unwrap();
    crate::tests::wait_for_empty_queue(&ana.handle).await;
    assert!(file_stored(ana, cover).await);
    assert!(file_stored(ana, photo).await);
    network.close().await;
}

/// Ana's and Ben's apps update to the version that retitles notes, and
/// Ana's raise is established. Carol's phone joins with the updated app:
/// its new installation runs every migration from the start, the breaking
/// one included, over tables no device's rows are in yet. That makes no
/// write of its own, so its first pass claims nothing before it was
/// registered, and it shows the notes at the store's version (§18.3).
#[tokio::test]
async fn a_device_joins_with_a_breaking_migration_declared() {
    let mut network = household().await;
    note(&network.devices[0], "n").await;
    wait_for_sent(&network.devices[0].handle).await;
    network.sync(1).await;
    retitle(&mut network, 0).await;
    settle(&network, 0, 0).await;
    assert!(raised_to(&network, 0, 2).await);
    retitle(&mut network, 1).await;
    network.sync(1).await;
    network.update_app(retitled());
    network.join_as(2, MemberRole::Member).await;
    network.sync(2).await;
    let (ben, carol) = (&network.devices[1], &network.devices[2]);
    assert!(carol.handle.pending().await.unwrap().is_empty());
    assert!(carol.handle.test_queued_writes().await.unwrap().is_empty());
    assert_eq!(carol.handle.store_state().await.schema[&Audience::Store].number, 2);
    assert_eq!(titles(carol).await, titles(ben).await);
    assert_eq!(titles(carol).await, [("n".to_owned(), "migrated".to_owned())]);
    network.close().await;
}

/// Ana's app updates offline, its migration retitling note `n`, and she
/// edits `n`'s title in a write that also adds a cover, so the edit names
/// her migration write as the title's setter and waits for the cover's
/// upload. Ben's raise is established first. Ana's marker goes out, her
/// edit waits on the refused upload, and Ben's next snapshot covers the
/// marker's slot. Carol's phone then joins from that snapshot: it holds
/// neither the marker nor the migration write's changes, and the edit,
/// arriving after the snapshot, becomes the same lost value on her phone as
/// on Ana's and Ben's instead of waiting forever (§9.5, §18.8).
#[tokio::test]
async fn a_device_joining_past_a_replaced_migration_loses_changes_built_on_it() {
    let mut network = household().await;
    note(&network.devices[0], "n").await;
    wait_for_sent(&network.devices[0].handle).await;
    network.sync(1).await;

    network.devices[0].storage.set_online(false);
    retitle(&mut network, 0).await;
    let ana = &network.devices[0];
    let cover = ana
        .handle
        .test_staged_write(
            |batch| Ok::<_, CovenError>(batch.put_file("files", vec![9; 4096], Place::Cloud)),
            |sql, file| {
                sql.execute("UPDATE notes SET title='edited' WHERE id='n'", [])?;
                sql.execute("INSERT INTO covers(id,file) VALUES('album',?1)", [file])?;
                Ok(file)
            },
        )
        .await
        .unwrap();

    retitle(&mut network, 1).await;
    settle(&network, 1, 0).await;
    assert!(raised_to(&network, 1, 2).await);

    let ana = &network.devices[0];
    let mut faults = Faults::none();
    faults
        .create_failures
        .insert(upload_path(ana, cover).await, (StorageFailure::QuotaExceeded, 507));
    ana.storage.set_faults(faults).await;
    ana.storage.set_online(true);
    // Back online, Ana's offline backoff holds her sync until a pass the
    // test asks for, which sends the marker; the edit waits on the refused
    // upload.
    settle(&network, 0, 1).await;
    assert_eq!(ana.handle.test_queued_writes().await.unwrap().len(), 1);
    network.sync(1).await;
    let marker_slot = network.devices[1].handle.applied_slot().await;

    let ben = &network.devices[1];
    let before = crate::tests::snapshots(&ben.storage, ben.store).await.len();
    let title = "x".repeat(1024 * 1024 + 1);
    ben.handle
        .write(move |sql| {
            sql.execute("INSERT INTO notes(id,title) VALUES('long',?1)", [title])?;
            Ok::<_, CovenError>(())
        })
        .await
        .unwrap();
    // A pass after the write sends it, which takes Ben's sequence past
    // the snapshot threshold, and makes a snapshot.
    network.sync(1).await;
    assert!(crate::tests::snapshots(&ben.storage, ben.store).await.len() > before);
    network.sync(1).await;

    let ana = &network.devices[0];
    ana.storage.set_faults(Faults::none()).await;
    // The retry answers once the upload's attempt has ended, whether it
    // stored the file or the loop had already; the edit that waited for it
    // is sent by the pass the test then runs.
    ana.handle.retry_uploads_now().await.unwrap();
    settle(&network, 0, 1).await;
    assert_eq!(queued_writes(&network, 0).await, 0);
    network.sync(1).await;
    assert!(network.devices[1].handle.applied_slot().await > marker_slot + 1);

    network.update_app(retitled());
    network.join_as(2, MemberRole::Member).await;
    network.sync(2).await;
    network.sync(1).await;
    let (ben, carol) = (&network.devices[1], &network.devices[2]);
    assert!(carol.handle.pending().await.unwrap().is_empty());
    assert_eq!(carol.handle.applied_slot().await, ben.handle.applied_slot().await);
    assert_eq!(titles(carol).await, titles(ben).await);
    let lost = ben.handle.lost_values().await.unwrap();
    assert_eq!(lost.len(), 1, "{lost:?}");
    assert_eq!(carol.handle.lost_values().await.unwrap(), lost);
    network.close().await;
}

/// Ana sets up Household with the app that already retitles notes in a
/// breaking version 2. Ben's phone runs the app before that version: the
/// store his join publishes tells him to update as it loads, as it would for
/// a store Ana raised to version 2 later, instead of failing on the snapshot
/// it can't read (§13.2, §18.3).
#[tokio::test]
async fn a_store_created_at_a_breaking_version_asks_an_older_app_to_update() {
    let mut network = Network::with_schema(CloudProvider::S3, tables(), retitled()).await;
    network.sync(0).await;
    assert_eq!(network.devices[0].handle.store_state().await.schema[&Audience::Store].number, 2);
    network.update_app(vec![Migration::addition(1, "covers", SCHEMA)]);
    let joined = network.try_join_as(1, MemberRole::Member).await;
    let Err(BootstrapError::Published { handle, source }) = joined else {
        panic!("the join publishes the store before it loads: {joined:?}");
    };
    assert!(
        matches!(
            &*source,
            BootstrapError::Sync(SyncError::Pending(record))
                if record.reason == PendingReason::UpdateRequired(RequiredUpdate::AppSchema { version: 2 })
        ),
        "{source:?}"
    );
    handle.close().await.unwrap();
    network.close().await;
}

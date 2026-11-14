//! The sync state an app shows (§19.1, E5), as `subscribe_sync_status`
//! follows it: synced means data and bound files are in storage, a queued
//! bound file names what it waits for, a queued write waits for the next
//! pass, a trailing file never reaches the state and shows what it waits for
//! or its failure as its phase in the upload queue, offline with a bound
//! file or a write queued is offline with unsent changes, a location
//! without credentials is signed out, and the last synced time survives
//! every state and every reopen without going backward.
use crate::tests::{file_stored, stage, upload_path, wait_for_sent, Device, Network};
use crate::*;
use coven_storage::test_utils::{Faults, MemoryRequest, MemoryStorage};
use coven_storage::ObjectPath;
use std::num::NonZeroU64;
use std::time::Duration;

fn tables() -> Vec<SyncedTable> {
    vec![
        SyncedTable::new("covers", RowIdentity::SharedKey)
            .file_column(FileColumn::new("file").bound()),
        SyncedTable::new("photos", RowIdentity::SharedKey).file_column(FileColumn::new("file")),
        SyncedTable::new("notes", RowIdentity::SharedKey),
    ]
}

const SCHEMA: &str = "CREATE TABLE covers(id TEXT NOT NULL PRIMARY KEY, file TEXT);
    CREATE TABLE photos(id TEXT NOT NULL PRIMARY KEY, file TEXT);
    CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL);";

/// Ana's Household store on S3.
async fn household() -> Network {
    Network::with_schema(
        CloudProvider::S3,
        tables(),
        vec![Migration::addition(1, "covers", SCHEMA)],
    )
    .await
}

async fn state(device: &Device, expected: impl Fn(&SyncState) -> bool) -> SyncStatus {
    device.handle.test_sync_status().wait_for(|status| expected(&status.state)).await
}

/// What each file in the upload queue is doing, oldest first.
async fn phases(device: &Device) -> Vec<UploadPhase> {
    device.handle.uploads().await.unwrap().files.into_iter().map(|file| file.phase).collect()
}

/// Wait until the upload queue is empty: every upload stored its file.
async fn wait_for_uploads(device: &Device) {
    let mut unsent =
        coven_database::test_utils::Followed::new(device.handle.subscribe_unsent_work());
    unsent.wait_for(|work| work.files == 0).await;
}

/// Ana pauses uploads and takes a photo: its row goes out with the photo on
/// her phone only, and she is synced, since a photo is a trailing file that
/// no state names. Her upload queue says why the photo waits. Once she
/// resumes, the photo uploads, and she stays synced (§17.4, §17.9, §19.1).
#[tokio::test]
async fn a_paused_photo_never_holds_the_state_and_waits_in_its_phase() {
    let network = household().await;
    let ana = &network.devices[0];
    ana.handle.set_uploads_paused(true).await.unwrap();
    let photo = stage(ana, "photos", "beach", vec![7; 4096]).await;
    wait_for_sent(&ana.handle).await;
    state(ana, |state| *state == SyncState::Synced).await;
    assert_eq!(phases(ana).await, [UploadPhase::Waiting(UploadWait::Paused)]);
    assert!(!file_stored(ana, photo).await);
    ana.handle.set_uploads_paused(false).await.unwrap();
    wait_for_uploads(ana).await;
    assert!(file_stored(ana, photo).await);
    // The completion is a write, which the next pass sends.
    state(ana, |state| *state == SyncState::Synced).await;
    network.close().await;
}

/// Ben moves files only on unmetered networks. On cellular his photo waits
/// for the metered network, in its phase, while his cover, bound, uploads,
/// and his state is synced once it has; the app reads the same from
/// `coven_network` and `coven_uploads`. Once his phone joins Wi-Fi the photo
/// uploads with no call of his app's (§17.9, §19.1).
#[tokio::test]
async fn a_photo_waits_for_an_unmetered_network_and_a_cover_does_not() {
    let network = household().await;
    let ben = &network.devices[0];
    ben.handle.set_unmetered_only(true).await.unwrap();
    ben.app.join_network(true);
    let photo = stage(ben, "photos", "beach", vec![7; 4096]).await;
    let cover = stage(ben, "covers", "album", vec![8; 4096]).await;
    wait_for_sent(&ben.handle).await;
    // The cover is stored; the photo alone waits in the queue.
    let mut unsent = coven_database::test_utils::Followed::new(ben.handle.subscribe_unsent_work());
    unsent.wait_for(|work| work.files == 1).await;
    assert!(file_stored(ben, cover).await);
    state(ben, |state| *state == SyncState::Synced).await;
    assert_eq!(phases(ben).await, [UploadPhase::Waiting(UploadWait::Metered)]);
    let shown: (bool, bool, bool, bool) = ben
        .handle
        .read(move |sql| {
            Ok::<_, CovenError>(sql.query_row(
                "SELECT n.usable,n.metered,n.unmetered_only,u.metered
                 FROM coven_network AS n, coven_uploads AS u WHERE u.file=?1",
                [photo],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(shown, (true, true, true, true));
    assert_eq!(ben.handle.retry_uploads_now().await.unwrap(), DrainOutcome::Metered);
    ben.app.join_network(false);
    wait_for_uploads(ben).await;
    assert!(file_stored(ben, photo).await);
    // The completion is a write, which the next pass sends.
    state(ben, |state| *state == SyncState::Synced).await;
    network.close().await;
}

/// Ben's phone stops answering what its network is after his store opened:
/// it may be cellular, so his photo waits as it would on a metered network,
/// in its phase, and `coven_network` says it can't tell. Once the phone
/// answers again, unmetered, the photo uploads (§6.7, §17.9).
#[tokio::test]
async fn a_photo_waits_while_coven_cannot_tell_the_network() {
    let network = household().await;
    let ben = &network.devices[0];
    ben.handle.set_unmetered_only(true).await.unwrap();
    ben.app.network_cannot_tell();
    let photo = stage(ben, "photos", "beach", vec![7; 4096]).await;
    wait_for_sent(&ben.handle).await;
    state(ben, |state| *state == SyncState::Synced).await;
    assert_eq!(phases(ben).await, [UploadPhase::Waiting(UploadWait::Metered)]);
    let shown: (Option<bool>, bool) = ben
        .handle
        .read(|sql| {
            Ok::<_, CovenError>(sql.query_row(
                "SELECT usable,metered FROM coven_network",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(shown, (None, true));
    ben.app.set_network_metered(false);
    wait_for_uploads(ben).await;
    assert!(file_stored(ben, photo).await);
    network.close().await;
}

/// A cover is bound, so the pause doesn't hold it. While sync is stopped it
/// waits on that; once a server error fails its upload, it waits on its own
/// backoff, paused or not, and the error names nothing of its own (§17.4,
/// §6.7).
#[tokio::test]
async fn a_bound_file_waits_on_its_retry_while_uploads_are_paused() {
    let network = household().await;
    let ana = &network.devices[0];
    ana.handle.stop_sync().await.unwrap();
    ana.handle.set_uploads_paused(true).await.unwrap();
    let cover = stage(ana, "covers", "album", vec![6; 4096]).await;
    state(ana, |state| *state == SyncState::WaitingToSync(SyncWait::NotSyncing)).await;
    let mut faults = Faults::none();
    faults.create_failures.insert(upload_path(ana, cover).await, (StorageFailure::Network, 503));
    ana.storage.set_faults(faults).await;
    ana.handle.start_sync().await.unwrap();
    let waiting =
        state(ana, |state| matches!(state, SyncState::WaitingToSync(SyncWait::Retrying { .. })))
            .await;
    let SyncState::WaitingToSync(SyncWait::Retrying { at }) = waiting.state else {
        unreachable!("waited for a retry")
    };
    assert!(at > network.clock.now(), "it retries after its backoff: {at:?}");
    ana.storage.set_faults(Faults::none()).await;
    // The pause doesn't hold the cover, so the retry tries it, whenever it
    // is asked: before or after the run that found the cover waiting ends.
    assert_eq!(
        ana.handle.retry_uploads_now().await.unwrap(),
        DrainOutcome::Drained { uploaded: 1 }
    );
    assert!(file_stored(ana, cover).await);
    state(ana, |state| *state == SyncState::Synced).await;
    network.close().await;
}

/// With sync running, a test refuses exactly one file upload: the photo's
/// first attempt is refused and its next stores it, with no fault left to
/// clear, while every request of the passes beside it goes through.
#[tokio::test]
async fn one_refused_file_upload_leaves_the_passes_alone() {
    let network = household().await;
    let ana = &network.devices[0];
    let before = ana.storage.request_trace().len();
    ana.storage
        .set_faults(Faults {
            refuse_file_uploads: 1,
            failure: StorageFailure::Network,
            response: Some(500),
            ..Faults::none()
        })
        .await;
    let photo = stage(ana, "photos", "beach", vec![7; 4096]).await;
    // The refused upload's backoff passes as the test moves the clock to
    // its end.
    network.end_wait(crate::tests::upload_wait(&ana.handle, photo, |_| true).await);
    wait_for_sent(&ana.handle).await;
    state(ana, |state| *state == SyncState::Synced).await;
    assert!(file_stored(ana, photo).await);
    let file = ana.handle.file_ref(photo).await.unwrap();
    let FilePlace::Cloud { location, .. } = file.place() else { unreachable!("stored") };
    let path = location.path(ana.store);
    let creates = ana.storage.request_trace()[before..]
        .iter()
        .filter(|request| matches!(request, MemoryRequest::Create(created) if *created == path))
        .count();
    assert_eq!(creates, 2, "one refused create, then the one that stored it");
    network.assert_moved_only_by_the_test();
    network.close().await;
}

/// Dan's track sits on a drive he unplugs before it uploads: his state stays
/// synced, since a track is a trailing file, and the track's phase is failed,
/// naming its move's record, while the track is gone. Once the drive is back
/// and the track uploads, his queue is empty (§17.3, §17.9, §19.1).
#[tokio::test]
async fn a_missing_original_fails_its_phase_not_the_state_until_it_uploads() {
    let network = household().await;
    let dan = &network.devices[0];
    let drive = tempfile::tempdir().unwrap();
    let path = drive.path().join("track.flac");
    std::fs::write(&path, vec![9; 4096]).unwrap();
    let prepared =
        prepare_user_file(&path, &crate::FileLease::for_test().await, |_| {}).await.unwrap();
    dan.handle.set_uploads_paused(true).await.unwrap();
    let track = dan
        .handle
        .write(move |sql| {
            let track = sql.put_user_file("tracks", prepared, Place::Cloud)?;
            sql.execute("INSERT INTO photos(id,file) VALUES('track',?1)", [track])?;
            Ok::<_, CovenError>(track)
        })
        .await
        .unwrap();
    let aside = drive.path().join("unplugged");
    std::fs::rename(&path, &aside).unwrap();
    dan.handle.set_uploads_paused(false).await.unwrap();
    crate::tests::upload_wait(&dan.handle, track, |_| true).await;
    state(dan, |state| *state == SyncState::Synced).await;
    let shown = phases(dan).await;
    let [UploadPhase::Failed(record)] = shown.as_slice() else {
        panic!("the move's record fails the track's phase: {shown:?}");
    };
    assert!(matches!(record.subject, PendingSubject::Move { .. }), "{record:?}");
    assert_eq!(
        record.reason,
        PendingReason::Original { path: path.clone(), failure: FileSourceFailure::Missing }
    );
    std::fs::rename(&aside, &path).unwrap();
    dan.handle.retry_uploads_now().await.unwrap();
    wait_for_uploads(dan).await;
    // The completion is a write, which the next pass sends.
    state(dan, |state| *state == SyncState::Synced).await;
    network.close().await;
}

/// Ben's phone loses its network: his state is offline. He takes a photo,
/// and his state is offline with unsent changes, with no pass reaching
/// storage; the last synced time stays where it was (§19.1).
#[tokio::test]
async fn a_photo_taken_offline_shows_unsent_changes() {
    let network = household().await;
    let ben = &network.devices[0];
    let synced = ben.handle.sync_status().await.unwrap().last_synced;
    assert!(synced.is_some(), "setup and its passes synced");
    ben.storage
        .set_faults(Faults {
            fail_next: usize::MAX,
            failure: StorageFailure::Network,
            response: None,
            ..Faults::none()
        })
        .await;
    network.clock.set(network.clock.now() + Duration::from_secs(1));
    let answer = ben.handle.retry_now().await.unwrap().await;
    assert!(matches!(answer, RetryOutcome::Failed(PassFailure::Offline { .. })), "{answer:?}");
    let offline = state(ben, |state| matches!(state, SyncState::Offline(_))).await;
    assert_eq!(offline.last_synced, synced);
    stage(ben, "photos", "train", vec![3; 4096]).await;
    let unsent = state(ben, |state| matches!(state, SyncState::OfflineWithUnsentChanges(_))).await;
    assert_eq!(unsent.last_synced, synced);
    network.close().await;
}

/// A provider that answers every request with a server error fails Ben's pass
/// with an answer, not with silence: his state is waiting to sync, retrying
/// at the sync scope's backoff, never failed and never offline. A pass the
/// provider never answers is offline, as before (§6.7, §19.1).
#[tokio::test]
async fn a_server_error_on_a_pass_waits_to_sync_and_no_answer_is_offline() {
    let network = household().await;
    let ben = &network.devices[0];
    let failing = |response| Faults {
        fail_next: usize::MAX,
        failure: StorageFailure::Network,
        response,
        ..Faults::none()
    };
    ben.storage.set_faults(failing(Some(503))).await;
    network.clock.set(network.clock.now() + Duration::from_secs(1));
    let answer = ben.handle.retry_now().await.unwrap().await;
    assert!(matches!(answer, RetryOutcome::Failed(_)), "{answer:?}");
    let waiting =
        state(ben, |state| matches!(state, SyncState::WaitingToSync(SyncWait::Retrying { .. })))
            .await;
    let SyncState::WaitingToSync(SyncWait::Retrying { at }) = waiting.state else {
        unreachable!("waited for a retry")
    };
    assert!(at > network.clock.now(), "it retries after its backoff: {at:?}");
    ben.storage.set_faults(failing(None)).await;
    network.clock.set(at);
    let answer = ben.handle.retry_now().await.unwrap().await;
    assert!(matches!(answer, RetryOutcome::Failed(PassFailure::Offline { .. })), "{answer:?}");
    state(ben, |state| matches!(state, SyncState::Offline(_))).await;
    network.close().await;
}

/// A store with no storage set up is not synced from its first value. One
/// whose credentials are gone from the keychain is signed out from its
/// first value on reopening, naming its provider with no cause, and keeps
/// the last synced time, which a clock set back never moves back (§19.1).
#[tokio::test]
async fn the_first_value_is_the_state_and_the_last_synced_time_is_kept() {
    let app_dir = tempfile::tempdir().unwrap();
    let app = Coven::in_memory(app_dir.path().into());
    let directory = app.create_closed_store("Notes", std::sync::Arc::new(UuidIds)).await.unwrap();
    let alone = app
        .builder()
        .synced_tables(tables())
        .migrations(vec![Migration::addition(1, "covers", SCHEMA)])
        .open(directory.id(), |_| ())
        .await
        .unwrap();
    assert_eq!(
        alone.test_sync_status().next().await,
        SyncStatus { state: SyncState::NotSynced, last_synced: None }
    );
    alone.close().await.unwrap();

    let mut network = household().await;
    let synced = network.devices[0].handle.sync_status().await.unwrap().last_synced;
    network.clock.set(network.clock.now() - Duration::from_secs(500));
    network.pass(0).await;
    assert_eq!(network.devices[0].handle.sync_status().await.unwrap().last_synced, synced);
    let device = &network.devices[0];
    device.handle.close().await.unwrap();
    device.app.lose_storage_credentials(device.store).unwrap();
    let reopened = network.reopen(device).open(device.store, |_| ()).await.unwrap();
    assert_eq!(
        reopened.test_sync_status().next().await,
        SyncStatus {
            state: SyncState::SignedOut { provider: CloudProvider::S3, cause: None },
            last_synced: synced,
        }
    );
    network.devices[0].handle = reopened;
    network.close().await;
}

/// Dan writes a note before he sets up storage. Once setup finishes, the note
/// is next in line for the first pass, here held before its first request, so
/// the state says syncing, sending, and never synced with the note only on his
/// device. Once that pass sends it, he is synced (§19.1).
#[tokio::test]
async fn a_write_before_setup_is_sending_until_the_first_pass_sends_it() {
    let root = tempfile::tempdir().unwrap();
    let app = Coven::in_memory(root.path().into());
    let storage = std::sync::Arc::new(MemoryStorage::builder().build().unwrap());
    let directory =
        app.create_closed_store("Household", std::sync::Arc::new(UuidIds)).await.unwrap();
    let dan = app
        .builder()
        .storage_connector(storage.clone())
        .synced_tables(tables())
        .migrations(vec![Migration::addition(1, "covers", SCHEMA)])
        .open(directory.id(), |_| ())
        .await
        .unwrap();
    dan.write(|sql| {
        sql.execute("INSERT INTO notes(id,title) VALUES('milk','Buy milk')", [])?;
        Ok::<_, CovenError>(())
    })
    .await
    .unwrap();
    // Setup claims slot 1 and its key copy at slot 2; the first pass after
    // it begins by reading slot 3.
    let next = ObjectPath::slot(directory.id(), NonZeroU64::new(3).unwrap());
    let (started, held) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    storage.hold_next_request_of(MemoryRequest::Read(next, None), started, resumed).await;
    dan.setup_s3_storage(
        storage.config(),
        "member-key".into(),
        SecretText::new("secret".into()),
        |_| {},
    )
    .await
    .unwrap();
    held.await.unwrap();
    assert_eq!(dan.sync_status().await.unwrap().state, SyncState::Syncing(SyncActivity::Sending));
    assert_eq!(dan.unsent_work().await.unwrap().writes, 1);
    resume.send(()).unwrap();
    dan.test_sync_status().wait_for(|status| status.state == SyncState::Synced).await;
    assert_eq!(dan.unsent_work().await.unwrap().writes, 0);
    dan.close().await.unwrap();
}

/// Ana removes Ben's device while his phone is stopped, and he keeps writing:
/// two writes wait in his send queue. His next pass applies the removal, and
/// his state says an admin removed his device and that two writes were never
/// sent; the queue still holds them (§14.1, §19.1).
#[tokio::test]
async fn a_removed_device_names_its_removal_and_what_it_never_sent() {
    let network = crate::tests::Network::new(2).await;
    let (ana, ben) = (&network.devices[0], &network.devices[1]);
    ben.stop_sync().await;
    for id in ["a", "b"] {
        ben.handle
            .write(move |sql| {
                sql.execute("INSERT INTO parents(id,label,value) VALUES(?1,?1,1)", [id])?;
                Ok::<_, CovenError>(())
            })
            .await
            .unwrap();
    }
    let (removal, _) = ana.handle.remove_device(ben.handle.device_id()).await.unwrap();
    crate::tests::landed(&ana.handle, removal).await;
    ben.handle.start_sync().await.unwrap();
    let removed = state(ben, |state| matches!(state, SyncState::Removed { .. })).await;
    assert_eq!(
        removed.state,
        SyncState::Removed { removal: Removal::Device, unsent: UnsentWork { writes: 2, files: 0 } }
    );
    assert_eq!(ben.handle.unsent_work().await.unwrap().writes, 2);
    network.close().await;
}

//! The sync state an app shows (§19.1, E5), as `subscribe_sync_status`
//! follows it: synced means data and files are in storage, a queued file
//! names what it waits for, one whose original is missing names failed,
//! offline with a file or a write queued is offline with unsent changes, a
//! location without credentials is signed out, and the last synced time
//! survives every state and every reopen without going backward.
use crate::tests::{file_stored, stage, upload_path, wait_for_sent, Device, Network};
use crate::*;
use coven_storage::test_utils::Faults;
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

/// Ana pauses uploads and takes a photo: its row goes out with the photo on
/// her phone only, so she waits to sync, on the pause. Once she resumes,
/// the photo uploads and she is synced (§17.4).
#[tokio::test]
async fn a_paused_photo_waits_to_sync_until_it_uploads() {
    let network = household().await;
    let ana = &network.devices[0];
    ana.handle.set_uploads_paused(true).await.unwrap();
    stage(ana, "photos", "beach", vec![7; 4096]).await;
    wait_for_sent(&ana.handle).await;
    state(ana, |state| *state == SyncState::WaitingToSync(SyncWait::Paused)).await;
    ana.handle.set_uploads_paused(false).await.unwrap();
    state(ana, |state| *state == SyncState::Synced).await;
    assert!(ana.handle.uploads().await.unwrap().files.is_empty());
    network.close().await;
}

/// Ben moves files only on unmetered networks. On cellular his photo waits
/// to sync, for the metered network, while his cover, bound, uploads; the
/// app reads the same from `coven_network` and `coven_uploads`. Once his
/// phone joins Wi-Fi the photo uploads with no call of his app's, and he is
/// synced (§17.9, §19.1).
#[tokio::test]
async fn a_photo_waits_for_an_unmetered_network_and_a_cover_does_not() {
    let network = household().await;
    let ben = &network.devices[0];
    ben.handle.set_unmetered_only(true).await.unwrap();
    ben.app.join_network(true);
    let photo = stage(ben, "photos", "beach", vec![7; 4096]).await;
    let cover = stage(ben, "covers", "album", vec![8; 4096]).await;
    wait_for_sent(&ben.handle).await;
    state(ben, |state| *state == SyncState::WaitingToSync(SyncWait::Metered)).await;
    assert!(file_stored(ben, cover).await);
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
    state(ben, |state| *state == SyncState::Synced).await;
    assert!(file_stored(ben, photo).await);
    network.close().await;
}

/// Ben's phone stops answering what its network is after his store opened:
/// it may be cellular, so his photo waits as it would on a metered network,
/// and `coven_network` says it can't tell. Once the phone answers again,
/// unmetered, the photo uploads (§6.7, §17.9).
#[tokio::test]
async fn a_photo_waits_while_coven_cannot_tell_the_network() {
    let network = household().await;
    let ben = &network.devices[0];
    ben.handle.set_unmetered_only(true).await.unwrap();
    ben.app.network_cannot_tell();
    let photo = stage(ben, "photos", "beach", vec![7; 4096]).await;
    wait_for_sent(&ben.handle).await;
    state(ben, |state| *state == SyncState::WaitingToSync(SyncWait::Metered)).await;
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
    state(ben, |state| *state == SyncState::Synced).await;
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
    // The upload's backoff holds until the test lets it go.
    network.polling.hold_waits();
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
    network.close().await;
}

/// Dan's track sits on a drive he unplugs before it uploads: his state is
/// failed, naming the track's move, and stays so while the track is gone.
/// Once the drive is back and the track uploads, he is synced (§17.3).
#[tokio::test]
async fn a_missing_original_fails_naming_its_move_until_it_uploads() {
    let network = household().await;
    let dan = &network.devices[0];
    let drive = tempfile::tempdir().unwrap();
    let path = drive.path().join("track.flac");
    std::fs::write(&path, vec![9; 4096]).unwrap();
    let prepared = prepare_user_file(&path, |_| {}).await.unwrap();
    dan.handle.set_uploads_paused(true).await.unwrap();
    dan.handle
        .write(move |sql| {
            let track = sql.put_user_file("tracks", prepared, Place::Cloud)?;
            sql.execute("INSERT INTO photos(id,file) VALUES('track',?1)", [track])?;
            Ok::<_, CovenError>(())
        })
        .await
        .unwrap();
    let aside = drive.path().join("unplugged");
    std::fs::rename(&path, &aside).unwrap();
    dan.handle.set_uploads_paused(false).await.unwrap();
    let failed = state(dan, |state| matches!(state, SyncState::Failed(_))).await;
    let SyncState::Failed(SyncCause::Pending(record)) = failed.state else {
        panic!("the move's record names failed: {failed:?}");
    };
    assert!(matches!(record.subject, PendingSubject::Move { .. }), "{record:?}");
    assert_eq!(
        record.reason,
        PendingReason::Original { path: path.clone(), failure: FileSourceFailure::Missing }
    );
    std::fs::rename(&aside, &path).unwrap();
    dan.handle.retry_uploads_now().await.unwrap();
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

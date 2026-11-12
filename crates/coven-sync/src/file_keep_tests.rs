//! Files kept here (§17.8): the write switching a file kept here to the
//! cloud pins it, and coven makes the pin itself. An app file's cache copy
//! comes from coven's copy with no request to storage; a user's file
//! downloads, and its original is never read for the cache.

use super::*;
use coven_foundation::{clock::FixedClock, id_source::FileId};
use coven_storage::test_utils::MemoryRequest;

/// Stage `bytes` as row `id` of `files`, kept here once stored.
async fn kept_app_file(f: &Fixture, id: &str, bytes: Vec<u8>) -> FileId {
    let id = id.to_owned();
    f.database
        .staged_write::<_, _, _, _, coven_database::DbError>(
            move |batch| Ok(batch.put_file("files", bytes, coven_database::Place::CloudKeptHere)),
            move |sql, file| {
                sql.execute("INSERT INTO files(id,file) VALUES(?1,?2)", (id, file))?;
                Ok(file)
            },
        )
        .await
        .unwrap()
}

/// Stage a user's original of these bytes as row `id` of `files`, kept here
/// once stored.
async fn kept_original(f: &Fixture, id: &str, bytes: &[u8]) -> FileId {
    let path = f.root.path().join(id);
    std::fs::write(&path, bytes).unwrap();
    let prepared = coven_database::prepare_user_file(&path, |_| {}).await.unwrap();
    let id = id.to_owned();
    f.database
        .write_as::<_, _, coven_database::DbError>(move |sql| {
            let file =
                sql.put_user_file("files", prepared, coven_database::Place::CloudKeptHere)?;
            sql.execute("INSERT INTO files(id,file) VALUES(?1,?2)", (id, file))?;
            Ok(file)
        })
        .await
        .unwrap()
}

async fn pinned(f: &Fixture, file: FileId) -> bool {
    f.database.files_pinned(vec![file]).await.unwrap() == [Some(true)]
}

fn reads(f: &Fixture, from: usize) -> usize {
    f.storage.request_trace()[from..]
        .iter()
        .filter(|request| matches!(request, MemoryRequest::Read(..)))
        .count()
}

async fn keep_records(f: &Fixture) -> Vec<crate::PendingRecord> {
    f.database
        .pending::<crate::PendingRecord>()
        .await
        .unwrap()
        .into_iter()
        .filter(|record| matches!(record.subject, crate::PendingSubject::Keep { .. }))
        .collect()
}

/// The switch pins an app file kept here, and coven makes the cache copy
/// from its own copy: no read reaches storage, and coven's copy goes once
/// the cache holds the file.
#[tokio::test]
async fn an_app_file_kept_here_is_pinned_from_coven_copy_without_storage() {
    let f = Fixture::new(Provenance::AppProvided).await;
    f.files.stop_background_for_counts().await;
    let bytes = vec![47; CHUNK * 2 + 9];
    let file = kept_app_file(&f, "kept", bytes.clone()).await;
    assert!(!pinned(&f, file).await);
    f.drain().await;
    // The write that switched it asked for the pin, keeping coven's copy.
    let requests = f.files.inner.database.pin_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].file, file);
    let copy = requests[0].copy.clone().expect("coven's copy is kept for the pin");
    let copy =
        f.directory.area_path(coven_foundation::files::FileArea::AppProvided).join(copy.as_str());
    assert!(copy.exists());
    let before = f.storage.request_trace().len();
    f.files.inner.keep_pinned().await.unwrap();
    assert!(pinned(&f, file).await);
    assert_eq!(reads(&f, before), 0);
    assert!(!copy.exists());
    let reference = f.database.file_ref(file).await.unwrap();
    assert_eq!(f.files.read_file(&reference).await.unwrap(), bytes);
    assert_eq!(reads(&f, before), 0);
    assert!(f.files.inner.database.pin_requests().await.unwrap().is_empty());
    assert!(keep_records(&f).await.is_empty());
    f.close().await;
}

/// A user's file kept here is pinned by the switch and downloaded from
/// storage: its original is never read for the cache, so it stays as it was
/// and may even be gone.
#[tokio::test]
async fn a_user_file_kept_here_downloads_and_never_reads_its_original() {
    let f = Fixture::new(Provenance::UserProvided).await;
    f.files.stop_background_for_counts().await;
    let bytes = vec![48; CHUNK + 5];
    let file = kept_original(&f, "song", &bytes).await;
    f.drain().await;
    let original = f.root.path().join("song");
    let kept = f.root.path().join("kept-aside");
    std::fs::rename(&original, &kept).unwrap();
    let before = f.storage.request_trace().len();
    f.files.inner.keep_pinned().await.unwrap();
    assert!(pinned(&f, file).await);
    assert!(reads(&f, before) > 0);
    assert_eq!(std::fs::read(&kept).unwrap(), bytes);
    assert!(f.files.inner.database.pin_requests().await.unwrap().is_empty());
    f.close().await;
}

/// Making a file kept here device-only on this device ends its request in
/// the write switching it (§17.3, §17.8): a request still waiting after a
/// failed attempt ends with its record, and nothing is pinned. The copy in
/// the person's folder is the file's original here now.
#[tokio::test]
async fn making_a_kept_file_device_only_here_ends_its_request() {
    let f = Fixture::new(Provenance::UserProvided).await;
    f.files.stop_background_for_counts().await;
    let bytes = vec![53; CHUNK + 7];
    let file = kept_original(&f, "song", &bytes).await;
    f.drain().await;
    f.storage.set_online(false);
    f.files.inner.keep_pinned().await.unwrap();
    assert_eq!(keep_records(&f).await.len(), 1);
    f.storage.set_online(true);
    let music = tempfile::tempdir().unwrap();
    let prepared = f
        .files
        .prepare_device_only(
            music.path(),
            vec![(file, coven_foundation::files::RelativePath::new(["song.flac"]).unwrap())],
            &tokio::sync::watch::channel(false).1,
            tokio::sync::watch::channel(None).0,
        )
        .await
        .unwrap();
    f.database.write(move |sql| sql.make_device_only(prepared)).await.unwrap();
    assert!(f.files.inner.database.pin_requests().await.unwrap().is_empty());
    assert!(keep_records(&f).await.is_empty());
    assert!(!pinned(&f, file).await);
    assert_eq!(std::fs::read(music.path().join("song.flac")).unwrap(), bytes);
    f.close().await;
}

/// A wait belongs to its request (§17.8): one held because its deadline
/// couldn't be saved, or one waiting for a new connection after a failure
/// only a person can clear, ends as the file leaves the cloud, and the
/// request a later move back records is made at once.
#[tokio::test]
async fn a_kept_pins_wait_ends_with_its_request() {
    for cause in ["unsaved", "sign-in"] {
        let start = SystemTime::UNIX_EPOCH + Duration::from_secs(1000);
        let clock = Arc::new(FixedClock::new(start));
        let f = Fixture::with_clock(Provenance::UserProvided, clock.clone()).await;
        f.files.stop_background_for_counts().await;
        let bytes = vec![58; CHUNK + 9];
        let file = kept_original(&f, "song", &bytes).await;
        f.drain().await;
        match cause {
            "unsaved" => {
                // A clock at the end of its range has no instant for the
                // wait's deadline, so the wait can't be saved.
                f.storage.set_online(false);
                clock.set(end_of_time());
                assert!(f.files.inner.keep_pinned().await.is_err());
                clock.set(start);
                f.storage.set_online(true);
            }
            _ => {
                f.storage
                    .set_faults(coven_storage::test_utils::Faults {
                        fail_next: 1,
                        failure: coven_storage::StorageFailure::Authentication,
                        ..coven_storage::test_utils::Faults::none()
                    })
                    .await;
                f.files.inner.keep_pinned().await.unwrap();
            }
        }
        // The wait holds the request: storage answers, and nothing is made.
        f.files.inner.keep_pinned().await.unwrap();
        assert!(!pinned(&f, file).await, "{cause}");
        device_only_and_back(&f, file).await;
        f.files.inner.keep_pinned().await.unwrap();
        assert!(pinned(&f, file).await, "{cause}");
        f.close().await;
    }
}

/// The latest time this platform's clock can name: on Windows long before
/// Unix's `i64::MAX` seconds past the epoch.
fn end_of_time() -> SystemTime {
    latest_reached(SystemTime::UNIX_EPOCH, |at, step| at.checked_add(step))
}

/// The latest point `add` reaches from `start`, found by halving the step
/// until it is zero. A step counts only if it moves the point: Windows'
/// clock counts 100 ns, so past its end an addition under 100 ns "succeeds"
/// without moving, and a search that took it as progress never ended.
fn latest_reached<T: Copy + PartialOrd>(start: T, add: impl Fn(T, Duration) -> Option<T>) -> T {
    let (mut end, mut step) = (start, Duration::MAX);
    while !step.is_zero() {
        match add(end, step) {
            Some(later) if later > end => end = later,
            _ => step /= 2,
        }
    }
    end
}

/// A clock counting 100 ns ticks to `last`, as Windows' does, adds a step
/// by dropping its sub-tick part: the search still ends, at the last tick.
#[test]
fn the_latest_time_is_found_on_a_clock_of_whole_ticks() {
    const TICK_NANOS: u128 = 100;
    let last: u64 = 9_223_372_036_854_775_807 / 1000;
    let add = |at: u64, step: Duration| {
        let ticks = u64::try_from(step.as_nanos() / TICK_NANOS).ok()?;
        at.checked_add(ticks).filter(|later| *later <= last)
    };
    assert_eq!(latest_reached(0, add), last);
}

/// Make the user's file kept here device-only on this device, which ends its
/// request, then move it back to the cloud kept here, which records another.
async fn device_only_and_back(f: &Fixture, file: FileId) {
    let music = tempfile::tempdir().unwrap();
    let prepared = f
        .files
        .prepare_device_only(
            music.path(),
            vec![(file, coven_foundation::files::RelativePath::new(["song.flac"]).unwrap())],
            &tokio::sync::watch::channel(false).1,
            tokio::sync::watch::channel(None).0,
        )
        .await
        .unwrap();
    f.database.write(move |sql| sql.make_device_only(prepared)).await.unwrap();
    assert!(f.files.inner.database.pin_requests().await.unwrap().is_empty());
    f.database
        .write(move |sql| {
            sql.move_to_cloud(vec![coven_database::MoveFile::UserKeptHere(file)]).map(drop)
        })
        .await
        .unwrap();
    f.drain().await;
    assert_eq!(f.files.inner.database.pin_requests().await.unwrap().len(), 1);
}

/// A failure is logged once per request (§19.1): a later request for the
/// same file logs its first failure, though it is the one the ended request
/// last logged.
#[tokio::test]
async fn a_later_requests_first_failure_is_logged() {
    let f = Fixture::new(Provenance::UserProvided).await;
    f.files.stop_background_for_counts().await;
    let file = kept_original(&f, "song", &vec![59; CHUNK + 3]).await;
    f.drain().await;
    let sign_in = || coven_storage::test_utils::Faults {
        fail_next: 1,
        failure: coven_storage::StorageFailure::Authentication,
        ..coven_storage::test_utils::Faults::none()
    };
    f.storage.set_faults(sign_in()).await;
    let before = f.files.inner.waits.failures_logged();
    f.files.inner.keep_pinned().await.unwrap();
    assert_eq!(f.files.inner.waits.failures_logged(), before + 1);
    device_only_and_back(&f, file).await;
    f.storage.set_faults(sign_in()).await;
    f.files.inner.keep_pinned().await.unwrap();
    assert!(!pinned(&f, file).await);
    assert_eq!(f.files.inner.waits.failures_logged(), before + 2);
    f.close().await;
}

/// The files `coven_pin_requests` lists, as the app's SQL reads them.
async fn requested(f: &Fixture) -> Vec<FileId> {
    f.database
        .read(|sql| {
            Ok::<_, DbError>(sql.query("SELECT file FROM coven_pin_requests", [], |r| r.get(0))?)
        })
        .await
        .unwrap()
}

/// The app's unpin of a file kept here ends coven's pin request in its write
/// (§17.8): `coven_pin_requests` loses the file, coven's copy the request
/// kept goes, and coven never makes the pin.
#[tokio::test]
async fn unpinning_a_kept_file_ends_its_request() {
    let f = Fixture::new(Provenance::AppProvided).await;
    f.files.stop_background_for_counts().await;
    let file = kept_app_file(&f, "kept", vec![56; CHUNK + 3]).await;
    f.drain().await;
    let copy = f.files.inner.database.pin_requests().await.unwrap()[0].copy.clone().unwrap();
    let copy =
        f.directory.area_path(coven_foundation::files::FileArea::AppProvided).join(copy.as_str());
    assert!(copy.exists());
    assert_eq!(requested(&f).await, [file]);
    f.database.write(move |sql| sql.unpin(&[file])).await.unwrap();
    assert!(requested(&f).await.is_empty());
    assert!(f.files.inner.database.pin_requests().await.unwrap().is_empty());
    assert!(!copy.exists());
    let before = f.storage.request_trace().len();
    f.files.inner.keep_pinned().await.unwrap();
    assert!(!pinned(&f, file).await);
    assert_eq!(reads(&f, before), 0);
    f.close().await;
}

/// An unpin while coven downloads the pin it keeps ends the request, and the
/// download's write makes no pin: a pin is made only while its request
/// waits (§17.8).
#[tokio::test]
async fn unpinning_a_kept_file_mid_download_keeps_nothing() {
    let f = Fixture::new(Provenance::UserProvided).await;
    f.files.stop_background_for_counts().await;
    let file = kept_original(&f, "song", &vec![57; CHUNK + 5]).await;
    f.drain().await;
    let (started, starting) = tokio::sync::oneshot::channel();
    let (resume, held) = tokio::sync::oneshot::channel();
    f.storage.hold_next_request(started, held).await;
    let mut keeping = Box::pin(f.files.inner.keep_pinned());
    tokio::select! {
        result = &mut keeping => panic!("the kept pin ended before its download: {result:?}"),
        result = tokio::time::timeout(HANG_GUARD, starting) => result.unwrap().unwrap(),
    }
    f.database.write(move |sql| sql.unpin(&[file])).await.unwrap();
    resume.send(()).unwrap();
    keeping.await.unwrap();
    assert!(!pinned(&f, file).await);
    assert!(requested(&f).await.is_empty());
    assert!(f.files.inner.database.pin_requests().await.unwrap().is_empty());
    assert!(keep_records(&f).await.is_empty());
    f.files.inner.keep_pinned().await.unwrap();
    assert!(!pinned(&f, file).await);
    f.close().await;
}

/// A failed pin coven keeps is its own record, which retries by itself,
/// and survives reopening; once storage answers, it is made.
#[tokio::test]
async fn a_failed_kept_pin_retries_by_itself_and_survives_reopening() {
    let clock = Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH));
    let mut f = Fixture::with_clock(Provenance::UserProvided, clock.clone()).await;
    f.files.stop_background_for_counts().await;
    let file = kept_original(&f, "song", &vec![49; CHUNK]).await;
    f.drain().await;
    f.storage.set_online(false);
    f.files.inner.keep_pinned().await.unwrap();
    let records = keep_records(&f).await;
    let [record] = records.as_slice() else {
        panic!("{records:?}");
    };
    assert_eq!(record.subject, crate::PendingSubject::Keep { file });
    assert_eq!(record.retry(), crate::Retry::Automatic);
    let requests = f.files.inner.database.pin_requests().await.unwrap();
    assert_eq!(requests[0].retry.unwrap().failures, 1);
    // `coven_pending` names the file.
    let named: Vec<Option<FileId>> = f
        .database
        .read(|sql| {
            Ok::<_, DbError>(sql.query("SELECT file FROM coven_pending", [], |r| r.get(0))?)
        })
        .await
        .unwrap();
    assert_eq!(named, [Some(file)]);
    f.reopen().await;
    f.files.stop_background_for_counts().await;
    f.storage.set_online(true);
    // Its wait holds until its deadline.
    f.files.inner.keep_pinned().await.unwrap();
    assert!(!pinned(&f, file).await);
    clock.set(SystemTime::UNIX_EPOCH + Duration::from_secs(2));
    f.files.inner.keep_pinned().await.unwrap();
    assert!(pinned(&f, file).await);
    assert!(keep_records(&f).await.is_empty());
    f.close().await;
}

/// When coven's copy can't make the cache copy, the pin downloads instead.
#[tokio::test]
async fn a_damaged_copy_falls_back_to_downloading_the_kept_pin() {
    let f = Fixture::new(Provenance::AppProvided).await;
    f.files.stop_background_for_counts().await;
    let bytes = vec![50; CHUNK + 1];
    let file = kept_app_file(&f, "kept", bytes.clone()).await;
    f.drain().await;
    let request = f.files.inner.database.pin_requests().await.unwrap().remove(0);
    let copy = request.copy.expect("an app file's request keeps coven's copy");
    let path =
        f.directory.area_path(coven_foundation::files::FileArea::AppProvided).join(copy.as_str());
    std::fs::write(&path, vec![51; bytes.len()]).unwrap();
    let before = f.storage.request_trace().len();
    f.files.inner.keep_pinned().await.unwrap();
    assert!(pinned(&f, file).await);
    assert!(reads(&f, before) > 0);
    let reference = f.database.file_ref(file).await.unwrap();
    assert_eq!(f.files.read_file(&reference).await.unwrap(), bytes);
    f.close().await;
}

/// Coven's copy is damaged: the file's pin records why while the download
/// that replaces it runs, and the pin made by the download clears the record.
#[tokio::test]
async fn a_damaged_copy_is_recorded_against_the_file_while_it_downloads() {
    let f = Fixture::new(Provenance::AppProvided).await;
    f.files.stop_background_for_counts().await;
    let bytes = vec![50; CHUNK + 1];
    let file = kept_app_file(&f, "kept", bytes.clone()).await;
    f.drain().await;
    let request = f.files.inner.database.pin_requests().await.unwrap().remove(0);
    let copy = request.copy.expect("an app file's request keeps coven's copy");
    let path =
        f.directory.area_path(coven_foundation::files::FileArea::AppProvided).join(copy.as_str());
    std::fs::write(&path, vec![51; bytes.len()]).unwrap();
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    f.storage.gate_reads(gate.clone()).await;
    let inner = f.files.inner.clone();
    let keeping = tokio::spawn(async move { inner.keep_pinned().await });
    let recorded = tokio::time::timeout(coven_foundation::clock::HANG_GUARD, async {
        loop {
            let records = keep_records(&f).await;
            if !records.is_empty() {
                break records;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the damaged copy is recorded while the download waits");
    assert!(matches!(
        recorded.as_slice(),
        [crate::PendingRecord { subject: crate::PendingSubject::Keep { file: named }, .. }]
            if *named == file
    ));
    assert!(!pinned(&f, file).await);
    gate.add_permits(16);
    keeping.await.unwrap().unwrap();
    assert!(pinned(&f, file).await);
    assert!(keep_records(&f).await.is_empty());
    f.close().await;
}

/// A pin coven keeps waits as an upload does (§6.7): a network report ends
/// a wait whose attempt got no provider response and leaves one a provider
/// answered, which the person's retry ends.
#[tokio::test]
async fn a_network_report_and_the_persons_retry_end_a_kept_pins_wait() {
    let clock = Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH + Duration::from_secs(1000)));
    let f = Fixture::with_clock(Provenance::UserProvided, clock.clone()).await;
    f.files.stop_background_for_counts().await;
    let file = kept_original(&f, "song", &vec![52; CHUNK]).await;
    f.drain().await;
    let wait = |f: &Fixture| {
        let database = f.files.inner.database.clone();
        async move { database.pin_requests().await.unwrap().remove(0).retry }
    };
    // No provider response: a network report ends the wait.
    f.storage.set_online(false);
    f.files.inner.keep_pinned().await.unwrap();
    assert_eq!(wait(&f).await.unwrap().wait, Duration::from_secs(1));
    f.storage.set_online(true);
    f.files.inner.keep_pinned().await.unwrap();
    assert!(!pinned(&f, file).await);
    f.files.inner.waits.report_network();
    f.files.inner.keep_pinned().await.unwrap();
    assert!(pinned(&f, file).await);

    // A provider's answer decides the wait: a report leaves it, and the
    // person's retry ends it.
    let other = kept_original(&f, "other", &vec![53; CHUNK]).await;
    f.drain().await;
    f.storage
        .set_faults(coven_storage::test_utils::Faults {
            fail_next: 1,
            failure: coven_storage::StorageFailure::Network,
            response: Some(503),
            ..coven_storage::test_utils::Faults::none()
        })
        .await;
    f.files.inner.keep_pinned().await.unwrap();
    let answered = wait(&f).await.unwrap();
    f.files.inner.waits.report_network();
    f.files.inner.keep_pinned().await.unwrap();
    assert_eq!(wait(&f).await.unwrap(), answered);
    assert!(!pinned(&f, other).await);
    f.files.inner.waits.retry_requested().await.unwrap();
    f.files.inner.keep_pinned().await.unwrap();
    assert!(pinned(&f, other).await);
    f.close().await;
}

/// While the app moves files only on unmetered networks, a metered network
/// holds a user's file kept here, which downloads, with no record and no
/// wait; an app file kept from coven's own copy asks storage for nothing,
/// so it is made. The user's file is made once the network is unmetered
/// (§17.8).
#[tokio::test]
async fn a_metered_network_holds_a_kept_pins_download_but_not_one_from_coven_s_copy() {
    let f = Fixture::new(Provenance::UserProvided).await;
    f.files.stop_background_for_counts().await;
    let song = kept_original(&f, "song", &vec![54; CHUNK]).await;
    f.drain().await;
    f.set_unmetered_only(true).await;
    f.network.set_metered(true);
    let from = f.storage.request_trace().len();
    f.files.inner.keep_pinned().await.unwrap();
    assert!(!pinned(&f, song).await);
    assert_eq!(reads(&f, from), 0);
    assert!(keep_records(&f).await.is_empty());
    assert!(f.files.inner.database.pin_requests().await.unwrap()[0].retry.is_none());
    f.network.set_metered(false);
    f.files.inner.keep_pinned().await.unwrap();
    assert!(pinned(&f, song).await);
    f.close().await;

    let f = Fixture::new(Provenance::AppProvided).await;
    f.files.stop_background_for_counts().await;
    let photo = kept_app_file(&f, "photo", vec![55; CHUNK]).await;
    f.drain().await;
    f.set_unmetered_only(true).await;
    f.network.set_metered(true);
    let from = f.storage.request_trace().len();
    f.files.inner.keep_pinned().await.unwrap();
    assert!(pinned(&f, photo).await);
    assert_eq!(reads(&f, from), 0);
    f.close().await;
}

/// The kept pin's attempts receive the same files as `Keep.reads` describes:
/// an app file whose copy checks out, one whose copy doesn't, and a user's
/// file, whose original is moved away first. Whether storage was read
/// matches the model, and every pin is made.
#[tokio::test]
#[ignore = "requires COVEN_IO_LEAN"]
async fn io_differential_keep() {
    let mut inputs = Vec::new();
    let mut actual = Vec::new();
    for (kind, checks) in [("app", true), ("app", false), ("user", true)] {
        let provenance = match kind {
            "app" => Provenance::AppProvided,
            _ => Provenance::UserProvided,
        };
        let f = Fixture::new(provenance).await;
        f.files.stop_background_for_counts().await;
        let bytes = vec![54; CHUNK + 2];
        let file = match kind {
            "app" => kept_app_file(&f, "kept", bytes.clone()).await,
            _ => kept_original(&f, "kept", &bytes).await,
        };
        f.drain().await;
        if kind == "app" && !checks {
            let copy =
                f.files.inner.database.pin_requests().await.unwrap()[0].copy.clone().unwrap();
            let path = f
                .directory
                .area_path(coven_foundation::files::FileArea::AppProvided)
                .join(copy.as_str());
            std::fs::write(&path, vec![55; bytes.len()]).unwrap();
        }
        if kind == "user" {
            std::fs::rename(f.root.path().join("kept"), f.root.path().join("aside")).unwrap();
        }
        let before = f.storage.request_trace().len();
        f.files.inner.keep_pinned().await.unwrap();
        assert!(pinned(&f, file).await);
        actual.push(serde_json::json!({"storage": reads(&f, before) > 0}));
        inputs.push(serde_json::json!({"check": "keep", "kind": kind, "checks": checks}));
        f.close().await;
    }
    assert_eq!(actual, crate::tests::io_runner(&inputs), "{inputs:?}");
}

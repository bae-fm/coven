//! An upload no file waits for any more has its provider session aborted
//! before it retires (§17.9).

use super::*;
use coven_foundation::clock::{Clock, FixedClock};
use coven_storage::{test_utils::Faults, CloudProvider};

/// A song too large for one request, staged for row `song`, whose first
/// part fails: its upload keeps the provider session it began.
async fn left_in_parts(provider: CloudProvider, clock: &Arc<FixedClock>) -> (Fixture, FileRef) {
    let f = Fixture::with_provider(Provenance::AppProvided, clock.clone(), CHUNK as u64, provider)
        .await;
    // Only the explicit drains below attempt uploads.
    f.files.stop_background_for_counts().await;
    f.set_uploads_paused(false).await;
    f.storage.set_faults(Faults { drop_part: true, ..Faults::none() }).await;
    let song = f.attach("files", "song", vec![7; CHUNK * 3]).await;
    f.files.inner.drain_uploads().await.unwrap();
    assert!(f.upload_of(&song).await.unwrap().session.is_some(), "{provider:?}");
    assert_eq!(f.storage.unfinished_uploads().await.len(), 1, "{provider:?}");
    (f, song)
}

async fn delete_song(f: &Fixture) {
    f.database
        .write(|sql| {
            sql.execute("DELETE FROM files WHERE id='song'", [])?;
            Ok(())
        })
        .await
        .unwrap();
}

/// Ana deletes the song while its upload holds a provider session. The
/// worker's next run, once the failed part's wait has passed, aborts the
/// session and then retires the upload, on every provider: retiring it
/// alone would leave the parts at the provider for good (§17.9).
#[tokio::test]
async fn retiring_an_upload_aborts_its_session_first() {
    for provider in [
        CloudProvider::S3,
        CloudProvider::Dropbox,
        CloudProvider::OneDrive,
        CloudProvider::CloudKit,
    ] {
        let clock = Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH + Duration::from_secs(1000)));
        let (f, _) = left_in_parts(provider, &clock).await;
        delete_song(&f).await;
        clock.set(clock.now() + Duration::from_secs(60));
        f.files.inner.drain_uploads().await.unwrap();
        assert!(f.storage.unfinished_uploads().await.is_empty(), "{provider:?}");
        assert!(f.files.inner.database.all_uploads().await.unwrap().is_empty(), "{provider:?}");
        f.close().await;
    }
}

/// The abort fails. The upload stays queued, its session still recorded,
/// and the failure is the song's pending record. The run after its wait
/// aborts the session and retires the upload, and the record goes with it.
#[tokio::test]
async fn a_failed_abort_keeps_the_upload_until_its_retry_aborts_it() {
    let clock = Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH + Duration::from_secs(1000)));
    let (f, song) = left_in_parts(CloudProvider::Dropbox, &clock).await;
    let [subject, _] = f.subjects(&song).await;
    let reason = |f: &Fixture| {
        let subject = subject.clone();
        let database = f.database.clone();
        async move {
            database
                .pending::<crate::PendingRecord>()
                .await
                .unwrap()
                .into_iter()
                .find(|record| record.subject == subject)
                .map(|record| record.reason)
        }
    };
    delete_song(&f).await;
    clock.set(clock.now() + Duration::from_secs(60));
    f.storage.set_faults(Faults { abort_failures: 1, ..Faults::none() }).await;
    f.files.inner.drain_uploads().await.unwrap();
    assert_eq!(f.storage.unfinished_uploads().await.len(), 1);
    let left = f.files.inner.database.all_uploads().await.unwrap();
    assert!(left.len() == 1 && left[0].session.is_some());
    assert!(matches!(
        reason(&f).await,
        Some(crate::PendingReason::Storage { failure: coven_storage::StorageFailure::Network, .. })
    ));
    let wait = left[0].retry.as_ref().unwrap().wait;
    f.files.inner.drain_uploads().await.unwrap();
    assert_eq!(f.storage.unfinished_uploads().await.len(), 1, "it waits out its retry");
    clock.set(clock.now() + wait);
    f.files.inner.drain_uploads().await.unwrap();
    assert!(f.storage.unfinished_uploads().await.is_empty());
    assert!(f.files.inner.database.all_uploads().await.unwrap().is_empty());
    assert_eq!(reason(&f).await, None);
    f.close().await;
}

/// With no storage connected, an upload whose session needs aborting stays
/// queued, while one without a session retires (§17.9).
#[tokio::test]
async fn without_storage_only_an_upload_with_no_session_retires() {
    let clock = Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH + Duration::from_secs(1000)));
    let (f, _) = left_in_parts(CloudProvider::Dropbox, &clock).await;
    f.storage.set_faults(Faults { fail_next: 1, ..Faults::none() }).await;
    f.attach("files", "photo", b"photo".to_vec()).await;
    f.files.inner.drain_uploads().await.unwrap();
    f.database
        .write(|sql| {
            sql.execute("DELETE FROM files", [])?;
            Ok(())
        })
        .await
        .unwrap();
    f.files.set_storage(None, async { Ok(()) }).await.unwrap();
    f.files.inner.drain_uploads().await.unwrap_err();
    let left = f.files.inner.database.all_uploads().await.unwrap();
    assert!(left.len() == 1 && left[0].session.is_some());
    assert_eq!(f.storage.unfinished_uploads().await.len(), 1);
    f.close().await;
}

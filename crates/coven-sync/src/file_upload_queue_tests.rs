//! What each upload is doing as the app's SQL reads it from
//! `coven_upload_phases` (§17.9, api.md E4): the phase `uploads()` gives each
//! queued file, with when its wait ends and the record a failure names, in a
//! live query that runs again as the pause, the network or a send changes it.

use super::*;
use coven_database::test_utils::Followed;
use coven_foundation::id_source::FileId;
use coven_storage::test_utils::Faults;
use coven_storage::StorageFailure;

/// One file's row of `coven_upload_phases`.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Viewed {
    phase: String,
    wait: Option<String>,
    at: Option<i64>,
    pending: Option<i64>,
}

/// Every row of `coven_upload_phases`, by file.
fn viewed(
    sql: coven_database::SqlReadContext<'_>,
) -> Result<BTreeMap<FileId, Viewed>, coven_database::DbError> {
    Ok(sql
        .query("SELECT file,phase,wait,at,pending FROM coven_upload_phases", [], |r| {
            Ok((
                r.get(0)?,
                Viewed { phase: r.get(1)?, wait: r.get(2)?, at: r.get(3)?, pending: r.get(4)? },
            ))
        })?
        .into_iter()
        .collect())
}

/// A phase's kind and its wait's, in the names the view gives them.
fn names(phase: &UploadPhase) -> (&'static str, Option<&'static str>) {
    match phase {
        UploadPhase::Uploading => ("uploading", None),
        UploadPhase::Stored => ("stored", None),
        UploadPhase::Failed(_) => ("failed", None),
        UploadPhase::Waiting(wait) => (
            "waiting",
            Some(match wait {
                UploadWait::NotSyncing => "not_syncing",
                UploadWait::CoolingDown { .. } => "cooling_down",
                UploadWait::Sync => "sync",
                UploadWait::Paused => "paused",
                UploadWait::Metered => "metered",
                UploadWait::KeysChanging => "keys_changing",
                UploadWait::Retrying { .. } => "retrying",
                UploadWait::Queued => "queued",
            }),
        ),
    }
}

impl Fixture {
    /// Each queued file's phase in the view's names, as `uploads()` gives
    /// it; the view shows the same, every file there once.
    async fn phases(&self) -> BTreeMap<FileId, (&'static str, Option<&'static str>)> {
        let given: BTreeMap<_, _> = self
            .queue()
            .await
            .files
            .iter()
            .map(|upload| (upload.file, names(&upload.phase)))
            .collect();
        let shown: BTreeMap<_, _> = self
            .database
            .read(viewed)
            .await
            .unwrap()
            .into_iter()
            .map(|(file, row)| (file, (row.phase, row.wait)))
            .collect();
        let named: BTreeMap<_, _> = given
            .iter()
            .map(|(file, (phase, wait))| (*file, ((*phase).to_owned(), wait.map(str::to_owned))))
            .collect();
        assert_eq!(shown, named);
        given
    }
}

/// Ana's photo is a trailing file and her cover a bound one. While she
/// pauses uploads, the view says the photo waits for the pause and the
/// cover for its turn; on a metered network, with files moving only on
/// unmetered ones, the photo waits for the network, and her live query
/// hears that with no write; after a failed attempt both wait for their
/// retry, at the time `coven_uploads.retry_at` holds; while the cover is
/// sent the view says so. Each time, `uploads()` gives the same phases (§17.4, §17.9).
#[tokio::test]
async fn the_view_shows_each_phase_that_uploads_gives_and_follows_it() {
    let f = Fixture::new(Provenance::AppProvided).await;
    f.files.stop_background_for_counts().await;
    f.set_uploads_paused(true).await;
    let photo = f.attach("files", "photo", vec![7; 64]).await.id();
    let cover = f.attach("covers", "cover", vec![8; 64]).await.id();
    let mut shown = Followed::new(f.database.subscribe(viewed));
    let wait = |row: &Viewed| row.wait.clone();
    shown.wait_for(|rows| rows.get(&photo).and_then(wait).as_deref() == Some("paused")).await;
    assert_eq!(
        f.phases().await,
        BTreeMap::from([
            (photo, ("waiting", Some("paused"))),
            (cover, ("waiting", Some("queued")))
        ])
    );
    f.set_uploads_paused(false).await;
    f.set_unmetered_only(true).await;
    shown.wait_for(|rows| rows.get(&photo).and_then(wait).as_deref() == Some("queued")).await;
    // The network alone changes the photo's phase, as the store's watch
    // tells the database.
    let network = |metered| coven_foundation::network_watch::NetworkState {
        usable: Some(true),
        metered,
        joined: 1,
    };
    f.database.record_network(network(true));
    shown.wait_for(|rows| rows.get(&photo).and_then(wait).as_deref() == Some("metered")).await;
    assert_eq!(
        f.phases().await,
        BTreeMap::from([
            (photo, ("waiting", Some("metered"))),
            (cover, ("waiting", Some("queued")))
        ])
    );
    f.database.record_network(network(false));
    shown.wait_for(|rows| rows.get(&photo).and_then(wait).as_deref() == Some("queued")).await;
    f.storage
        .set_faults(Faults { fail_next: 2, failure: StorageFailure::Network, ..Faults::none() })
        .await;
    f.files.inner.drain_uploads().await.unwrap();
    let retrying = shown
        .wait_for(|rows| rows.values().all(|row| row.wait.as_deref() == Some("retrying")))
        .await;
    assert_eq!(
        f.phases().await,
        BTreeMap::from([
            (photo, ("waiting", Some("retrying"))),
            (cover, ("waiting", Some("retrying")))
        ])
    );
    let retry_at: BTreeMap<FileId, Option<i64>> = f
        .database
        .read(|sql| {
            Ok::<_, coven_database::DbError>(sql.query(
                "SELECT file,retry_at FROM coven_uploads",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?)
        })
        .await
        .unwrap()
        .into_iter()
        .collect();
    assert!(retry_at.values().all(Option::is_some));
    assert_eq!(
        retrying.iter().map(|(file, row)| (*file, row.at)).collect::<BTreeMap<_, _>>(),
        retry_at
    );
    // A send in flight shows as uploading, and the live query hears it start
    // and end. The pause holds the photo, so the cover alone is sent.
    f.set_uploads_paused(true).await;
    f.storage.set_faults(Faults::none()).await;
    let (started, starting) = tokio::sync::oneshot::channel();
    let (release, held) = tokio::sync::oneshot::channel::<()>();
    f.storage.hold_next_request(started, held).await;
    let files = f.files.clone();
    let drain = tokio::spawn(async move { files.inner.drain_uploads_retrying().await });
    starting.await.unwrap();
    shown.wait_for(|rows| rows.get(&cover).is_some_and(|row| row.phase == "uploading")).await;
    assert_eq!(
        f.phases().await,
        BTreeMap::from([(photo, ("waiting", Some("paused"))), (cover, ("uploading", None))])
    );
    release.send(()).unwrap();
    drain.await.unwrap().unwrap();
    shown.wait_for(|rows| rows.keys().eq([&photo])).await;
    f.close().await;
}

/// Ana's song is staged for the cloud from her drive, which she unplugs
/// before it uploads: the view says the song's upload failed, naming its
/// move's record in `coven_pending`, as `uploads()` does (§17.3, §17.9).
#[tokio::test]
async fn a_failed_upload_names_the_record_that_says_why() {
    let f = Fixture::new(Provenance::UserProvided).await;
    f.files.stop_background_for_counts().await;
    let song = f.original("song", &[9; 64]).await.id();
    std::fs::remove_file(f.root.path().join("song")).unwrap();
    f.set_uploads_paused(false).await;
    f.files.inner.drain_uploads().await.unwrap();
    assert_eq!(f.phases().await, BTreeMap::from([(song, ("failed", None))]));
    let UploadPhase::Failed(record) = f.queue().await.files.remove(0).phase else {
        panic!("the song's upload failed");
    };
    let (pending, recorded) = f
        .database
        .read(move |sql| {
            Ok::<_, coven_database::DbError>(sql.query_row(
                "SELECT v.pending,p.record FROM coven_upload_phases AS v
                 JOIN coven_pending AS p ON p.position=v.pending WHERE v.file=?1",
                [song],
                |r| Ok((r.get::<_, i64>(0)?, r.get::<_, crate::PendingRecord>(1)?)),
            )?)
        })
        .await
        .unwrap();
    assert!(pending > 0);
    assert_eq!(recorded.subject, record.subject);
    f.close().await;
}

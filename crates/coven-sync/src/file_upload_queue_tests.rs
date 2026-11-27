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
    let wait = match phase {
        UploadPhase::Waiting(wait) => Some(wait.kind()),
        UploadPhase::Uploading | UploadPhase::Stored | UploadPhase::Failed(_) => None,
    };
    (phase.kind(), wait)
}

impl Fixture {
    /// Each queued file's phase as `uploads()` gives it; the view shows the
    /// same, every file there once, in the phases' and waits' own names.
    async fn phases(&self) -> BTreeMap<FileId, UploadPhase> {
        let given: BTreeMap<_, _> = self
            .queue()
            .await
            .files
            .into_iter()
            .map(|upload| (upload.file, upload.phase))
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
            .map(|(file, phase)| {
                let (phase, wait) = names(phase);
                (*file, (phase.to_owned(), wait.map(str::to_owned)))
            })
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
    let waits = |rows: &BTreeMap<FileId, Viewed>, file: FileId, wait: UploadWait| {
        rows.get(&file).and_then(|row| row.wait.as_deref()) == Some(wait.kind())
    };
    shown.wait_for(|rows| waits(rows, photo, UploadWait::Paused)).await;
    assert_eq!(
        f.phases().await,
        BTreeMap::from([
            (photo, UploadPhase::Waiting(UploadWait::Paused)),
            (cover, UploadPhase::Waiting(UploadWait::Queued))
        ])
    );
    f.set_uploads_paused(false).await;
    f.set_unmetered_only(true).await;
    shown.wait_for(|rows| waits(rows, photo, UploadWait::Queued)).await;
    // The network alone changes the photo's phase, as the store's watch
    // tells the database.
    let network = |metered| coven_foundation::network_watch::NetworkState {
        usable: Some(true),
        metered,
        joined: 1,
    };
    f.database.record_network(network(true));
    shown.wait_for(|rows| waits(rows, photo, UploadWait::Metered)).await;
    assert_eq!(
        f.phases().await,
        BTreeMap::from([
            (photo, UploadPhase::Waiting(UploadWait::Metered)),
            (cover, UploadPhase::Waiting(UploadWait::Queued))
        ])
    );
    f.database.record_network(network(false));
    shown.wait_for(|rows| waits(rows, photo, UploadWait::Queued)).await;
    f.storage
        .set_faults(Faults { fail_next: 2, failure: StorageFailure::Network, ..Faults::none() })
        .await;
    f.files.inner.drain_uploads().await.unwrap();
    // A retry's kind is the same whatever its time.
    let retrying = shown
        .wait_for(|rows| {
            [photo, cover]
                .into_iter()
                .all(|file| waits(rows, file, UploadWait::Retrying { at: SystemTime::UNIX_EPOCH }))
        })
        .await;
    let phases = f.phases().await;
    assert_eq!(phases.len(), 2);
    assert!(phases
        .values()
        .all(|phase| matches!(phase, UploadPhase::Waiting(UploadWait::Retrying { .. }))));
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
    shown
        .wait_for(|rows| {
            rows.get(&cover).is_some_and(|row| row.phase == UploadPhase::Uploading.kind())
        })
        .await;
    assert_eq!(
        f.phases().await,
        BTreeMap::from([
            (photo, UploadPhase::Waiting(UploadWait::Paused)),
            (cover, UploadPhase::Uploading)
        ])
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
    let mut phases = f.phases().await;
    let (Some(UploadPhase::Failed(record)), true) = (phases.remove(&song), phases.is_empty())
    else {
        panic!("the song's upload failed, and nothing else is queued");
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

/// A live query of `coven_upload_phases` that selects only each file runs
/// again as the network changes: the view's subqueries keep rows by an OFFSET,
/// so the query depends on every column of each table the view reads (§4).
#[tokio::test]
async fn a_read_of_the_phases_files_alone_follows_the_network() {
    let f = Fixture::new(Provenance::AppProvided).await;
    f.files.stop_background_for_counts().await;
    let photo = f.attach("files", "photo", vec![7; 64]).await.id();
    let mut files: coven_database::LiveQuery<Vec<FileId>, coven_database::DbError> = f
        .database
        .subscribe(|sql| Ok(sql.query("SELECT file FROM coven_upload_phases", [], |r| r.get(0))?));
    assert_eq!(files.next_inline().await.unwrap().unwrap(), [photo]);
    f.database.record_network(coven_foundation::network_watch::NetworkState {
        usable: Some(true),
        metered: true,
        joined: 1,
    });
    assert!(files.is_marked_for_rerun());
}

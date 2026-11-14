//! Making cloud files device-only (§17.3): what a call whose future the app
//! drops leaves in the person's folder.

use super::*;
use crate::files::file_device_only::DeviceOnlyPoint;
use coven_foundation::files::RelativePath;

/// The app drops the call's future while a copy, renamed to its path, is
/// not yet recorded placed. The step finishes holding the copy's name, so
/// the next write's removal can't take the copy's record from under it, and
/// once it is placed the copy goes like any the call left (Appendix C.3).
#[tokio::test]
async fn a_dropped_call_leaves_no_copy_unrecorded() {
    let mut f = Fixture::new(Provenance::UserProvided).await;
    f.files.stop_background_for_counts().await;
    let file = f.original("song", &vec![61; CHUNK + 7]).await;
    f.drain().await;
    let music = tempfile::tempdir().unwrap();
    let copy = music.path().join("song.flac");
    let (reached, reaching) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    *f.files.inner.device_only_gate.lock().unwrap() =
        Some((DeviceOnlyPoint::Renamed, reached, resumed));
    let call = {
        let (files, folder) = (f.files.clone(), music.path().to_owned());
        tokio::spawn(async move {
            files
                .prepare_device_only(
                    &folder,
                    vec![(file.id(), RelativePath::new(["song.flac"]).unwrap())],
                    &tokio::sync::watch::channel(false).1,
                    tokio::sync::watch::channel(None).0,
                )
                .await
        })
    };
    reaching.await.unwrap();
    assert!(copy.exists(), "the copy is renamed to its path");
    call.abort();
    assert!(call.await.unwrap_err().is_cancelled());
    // A write's removal meanwhile.
    f.database
        .write(|sql| {
            sql.execute("INSERT INTO files(id) VALUES('next')", [])?;
            Ok(())
        })
        .await
        .unwrap();
    // A step already dropped with its call no longer waits.
    resume.send(()).ok();
    // Closing waits for a step still running; opening removes what the
    // call left.
    f.reopen().await;
    assert!(!copy.exists(), "the copy is left with no record");
    f.close().await;
}

/// The app drops the call's future while a copy's bytes are being
/// appended. The append finishes holding the copy's name, so a write's
/// removal meanwhile leaves the copy's file alone: it never removes a file
/// an append still writes, which Windows would refuse, leaving the record
/// for a later open. Once the append ends, the copy goes like any the call
/// left.
#[tokio::test]
async fn a_dropped_calls_append_finishes_before_its_copy_is_removed() {
    let mut f = Fixture::new(Provenance::UserProvided).await;
    f.files.stop_background_for_counts().await;
    let file = f.original("song", &vec![62; CHUNK + 7]).await;
    f.drain().await;
    let music = tempfile::tempdir().unwrap();
    let (reached, reaching) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    *f.files.inner.device_only_gate.lock().unwrap() =
        Some((DeviceOnlyPoint::Appended, reached, resumed));
    let call = {
        let (files, folder) = (f.files.clone(), music.path().to_owned());
        tokio::spawn(async move {
            files
                .prepare_device_only(
                    &folder,
                    vec![(file.id(), RelativePath::new(["song.flac"]).unwrap())],
                    &tokio::sync::watch::channel(false).1,
                    tokio::sync::watch::channel(None).0,
                )
                .await
        })
    };
    reaching.await.unwrap();
    let temporary = || {
        std::fs::read_dir(music.path())
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| path.extension().is_some_and(|e| e == "download"))
    };
    assert!(temporary().is_some(), "the copy's bytes are being written");
    call.abort();
    assert!(call.await.unwrap_err().is_cancelled());
    f.database
        .write(|sql| {
            sql.execute("INSERT INTO files(id) VALUES('next')", [])?;
            Ok(())
        })
        .await
        .unwrap();
    assert!(temporary().is_some(), "a write removed a copy an append still writes");
    // A step already dropped with its call no longer waits.
    resume.send(()).ok();
    f.reopen().await;
    assert!(temporary().is_none());
    assert_eq!(std::fs::read_dir(music.path()).unwrap().count(), 0);
    f.close().await;
}

/// The call waits for a place before it reads anything or makes anything in
/// the person's folder; once a place comes back, it downloads.
#[tokio::test]
async fn a_device_only_call_waits_for_a_place_before_it_reads() {
    let f = Fixture::new(Provenance::UserProvided).await;
    f.files.stop_background_for_counts().await;
    let file = f.original("song", &vec![63; CHUNK + 7]).await;
    f.drain().await;
    let music = tempfile::tempdir().unwrap();
    let budget = f.files.inner.files.clone();
    let watch = budget.watch();
    let everything = budget.lease(budget.size()).await.unwrap();
    let waited = watch.lease_waits_from_now(1);
    let call = {
        let (files, folder) = (f.files.clone(), music.path().to_owned());
        tokio::spawn(async move {
            files
                .prepare_device_only(
                    &folder,
                    vec![(file.id(), RelativePath::new(["song.flac"]).unwrap())],
                    &tokio::sync::watch::channel(false).1,
                    tokio::sync::watch::channel(None).0,
                )
                .await
        })
    };
    coven_foundation::clock::within(waited).await.unwrap();
    assert!(!call.is_finished());
    assert_eq!(std::fs::read_dir(music.path()).unwrap().count(), 0);
    drop(everything);
    let prepared = coven_foundation::clock::within(call).await.unwrap().unwrap().unwrap();
    drop(prepared);
    assert_eq!(watch.holding(), 0);
    f.close().await;
}

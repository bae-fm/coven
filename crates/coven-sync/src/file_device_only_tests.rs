//! Making cloud files device-only (§17.3): what a call whose future the app
//! drops leaves in the person's folder.

use super::*;
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
    *f.files.inner.placing_gate.lock().unwrap() = Some((reached, resumed));
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

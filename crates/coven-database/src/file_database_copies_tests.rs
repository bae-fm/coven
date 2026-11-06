use super::*;
use crate::{
    file_write::tests::{local_count, open},
    tests::TestStore,
    Provenance,
};

/// A crash before a call's switch leaves its copies recorded: opening
/// removes the one still being written and the one placed, and the folder
/// the call made for them, and keeps a folder that was there (§17.3,
/// Appendix C.3).
#[tokio::test]
async fn opening_removes_what_a_crashed_call_made() {
    let store = TestStore::new();
    let db = open(&store, Provenance::UserProvided).await;
    let owner = FileDatabase::new(db.clone());
    let music = tempfile::tempdir().unwrap();
    let album = music.path().join("Album");
    let copy = |name: &str, temporary: &str| FolderCopy {
        path: album.join(name),
        temporary: album.join(temporary),
        folders: vec![album.clone()],
    };
    let reservation = owner
        .record_copies(vec![
            copy("one.flac", ".coven-1.download"),
            copy("two.flac", ".coven-2.download"),
        ])
        .await
        .unwrap();
    assert!(matches!(
        owner
            .record_copies(vec![copy("one.flac", ".coven-3.download")])
            .await,
        Err(DbError::CopyPathTaken { path }) if path == album.join("one.flac")
    ));
    std::fs::create_dir(&album).unwrap();
    owner.made_folder(&album).await.unwrap();
    std::fs::write(album.join(".coven-1.download"), b"half of one").unwrap();
    std::fs::write(album.join("two.flac"), b"all of two").unwrap();
    let folder = coven_foundation::files::CopyFolder::open(
        music.path().to_owned(),
        coven_foundation::task::HeardBugs::shared(),
    )
    .await
    .unwrap();
    let observed = folder.observe(&album.join("two.flac")).await.unwrap();
    owner.place_copy(&observed).await.unwrap();
    // The call ends and the store closes with no write between, as when
    // the process ends with the call: the records are all that is left.
    drop(reservation);
    db.close().await.unwrap();
    let db = open(&store, Provenance::UserProvided).await;
    assert_eq!(local_count(&db, "_coven_copies"), 0);
    assert_eq!(local_count(&db, "_coven_copy_folders"), 0);
    assert!(!album.exists());
    assert!(music.path().exists());
    db.close().await.unwrap();
}

/// A failing call's removal takes only what it made, as it left it: a copy
/// the person changed meanwhile stays, and so does its folder.
#[tokio::test]
async fn abandoning_leaves_a_copy_the_person_changed() {
    let store = TestStore::new();
    let db = open(&store, Provenance::UserProvided).await;
    let owner = FileDatabase::new(db.clone());
    let music = tempfile::tempdir().unwrap();
    let album = music.path().join("Album");
    let copies = ["one.flac", "two.flac"].map(|name| FolderCopy {
        path: album.join(name),
        temporary: album.join(format!(".coven-{name}.download")),
        folders: vec![album.clone()],
    });
    let reservation = owner.record_copies(copies.to_vec()).await.unwrap();
    std::fs::create_dir(&album).unwrap();
    owner.made_folder(&album).await.unwrap();
    let folder = coven_foundation::files::CopyFolder::open(
        music.path().to_owned(),
        coven_foundation::task::HeardBugs::shared(),
    )
    .await
    .unwrap();
    for copy in &copies {
        std::fs::write(&copy.path, b"downloaded").unwrap();
        let observed = folder.observe(&copy.path).await.unwrap();
        owner.place_copy(&observed).await.unwrap();
    }
    std::fs::write(&copies[1].path, b"the person's edit").unwrap();
    owner.abandon_copies(copies.iter().map(|copy| copy.path.clone()).collect()).await.unwrap();
    assert!(!copies[0].path.exists());
    assert_eq!(std::fs::read(&copies[1].path).unwrap(), b"the person's edit");
    assert_eq!(local_count(&db, "_coven_copies"), 0);
    drop(reservation);
    db.close().await.unwrap();
}

/// A call whose future is dropped leaves its copies recorded but no longer
/// reserved: the next write of the store removes them, and the folder it
/// made once empty. While a call holds its reservation, no write does; and
/// a folder the call found already there stays.
#[tokio::test]
async fn the_next_write_removes_what_a_dropped_call_made() {
    let store = TestStore::new();
    let db = open(&store, Provenance::UserProvided).await;
    let owner = FileDatabase::new(db.clone());
    let music = tempfile::tempdir().unwrap();
    let theirs = music.path().join("Theirs");
    let album = theirs.join("Album");
    std::fs::create_dir(&theirs).unwrap();
    let reservation = owner
        .record_copies(vec![FolderCopy {
            path: album.join("one.flac"),
            temporary: album.join(".coven-1.download"),
            folders: vec![theirs.clone(), album.clone()],
        }])
        .await
        .unwrap();
    // The person made `Theirs` before the call reached it; the call made
    // `Album`.
    std::fs::create_dir(&album).unwrap();
    owner.made_folder(&album).await.unwrap();
    std::fs::write(album.join(".coven-1.download"), b"half").unwrap();
    let write = || {
        db.write(|sql| {
            sql.execute("INSERT OR REPLACE INTO files(id) VALUES('8')", [])?;
            Ok(())
        })
    };
    write().await.unwrap();
    assert!(album.join(".coven-1.download").exists(), "the call is running");
    drop(reservation);
    write().await.unwrap();
    assert!(!album.exists());
    assert!(theirs.exists(), "a folder the call didn't make stays");
    assert_eq!(local_count(&db, "_coven_copies"), 0);
    db.close().await.unwrap();
}

/// A call's copies are written while its reservation is held: dropping the
/// store's last handle meanwhile leaves the store closing, and a reopen
/// waits, until the reservation drops. Otherwise the reopen would remove the
/// copies the call is still writing (Appendix C.3).
#[tokio::test]
async fn reopen_waits_for_a_copy_reservation_after_its_database_owner_drops() {
    let store = TestStore::new();
    let db = open(&store, Provenance::UserProvided).await;
    let owner = FileDatabase::new(db.clone());
    let music = tempfile::tempdir().unwrap();
    let temporary = music.path().join(".coven-1.download");
    let reservation = owner
        .record_copies(vec![FolderCopy {
            path: music.path().join("one.flac"),
            temporary: temporary.clone(),
            folders: Vec::new(),
        }])
        .await
        .unwrap();
    std::fs::write(&temporary, b"half").unwrap();
    drop(db);
    drop(owner);
    let waits = store.when_open_waits();
    let mut reopen = Box::pin(open(&store, Provenance::UserProvided));
    crate::tests::until_waiting(&mut reopen, waits).await;
    assert!(temporary.exists(), "the call is still writing");
    drop(reservation);
    let reopened = reopen.await;
    assert!(!temporary.exists());
    assert_eq!(local_count(&reopened, "_coven_copies"), 0);
    reopened.close().await.unwrap();
}

/// Closing waits for a call's reservation, so a call failing as the store
/// closes still removes what it made before its reservation drops.
#[tokio::test]
async fn a_closing_store_lets_a_failing_call_remove_its_copies() {
    use std::{future::Future, task::Poll};
    let store = TestStore::new();
    let db = open(&store, Provenance::UserProvided).await;
    let owner = FileDatabase::new(db.clone());
    let music = tempfile::tempdir().unwrap();
    let (path, temporary) = (music.path().join("one.flac"), music.path().join(".coven-1.download"));
    let reservation = owner
        .record_copies(vec![FolderCopy {
            path: path.clone(),
            temporary: temporary.clone(),
            folders: Vec::new(),
        }])
        .await
        .unwrap();
    std::fs::write(&temporary, b"half").unwrap();
    let mut closing = Box::pin(db.close());
    // Polled once, the close has begun: the store is closing.
    std::future::poll_fn(|cx| {
        assert!(closing.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    owner.abandon_copies(vec![path]).await.unwrap();
    assert!(!temporary.exists());
    drop(reservation);
    drop(owner);
    closing.await.unwrap();
    let reopened = open(&store, Provenance::UserProvided).await;
    assert_eq!(local_count(&reopened, "_coven_copies"), 0);
    reopened.close().await.unwrap();
}

/// Fill this domain's tables from the rows of the plan audit's `fixture`
/// table, inside its transaction (`internal_schema_scan_reasons_tests.rs`).
pub(crate) fn fill_for_plans(db: &rusqlite::Connection) {
    db.execute_batch(
        "INSERT INTO _coven_copies(path,temporary,state,size,modified_at)
           SELECT 'path_' || i,'temporary_' || i,CASE i%4 WHEN 0 THEN 'writing' WHEN 1 THEN 'placed' WHEN 2 THEN 'kept' ELSE 'lost' END,
             CASE WHEN i%4!=0 THEN i END,CASE WHEN i%4!=0 THEN zeroblob(13) END FROM fixture;
         INSERT INTO _coven_copy_folders SELECT 'folder_' || (i%7),'path_' || i,i%2 FROM fixture;",
    )
    .unwrap();
}

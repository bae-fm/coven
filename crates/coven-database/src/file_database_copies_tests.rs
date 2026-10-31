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
    let observed =
        coven_foundation::files::observe_metadata(&album.join("two.flac")).await.unwrap();
    owner.place_copy(&observed).await.unwrap();
    // The process ends with the call: its reservation never drops.
    std::mem::forget(reservation);
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
    for copy in &copies {
        std::fs::write(&copy.path, b"downloaded").unwrap();
        let observed = coven_foundation::files::observe_metadata(&copy.path).await.unwrap();
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

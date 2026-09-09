use super::*;
use crate::sync::test_helpers::{InterceptedStorage, ProtocolRead, StorageInterceptor};
use std::sync::atomic::{AtomicBool, Ordering};

struct PausePackageRead {
    prefix: String,
    exercised: AtomicBool,
    reached: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}

#[async_trait::async_trait]
impl StorageInterceptor for PausePackageRead {
    async fn before_protocol_read(
        &self,
        read: ProtocolRead,
        prefix: &str,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        if read == ProtocolRead::Object
            && prefix == self.prefix
            && !self.exercised.swap(true, Ordering::SeqCst)
        {
            self.reached.notify_one();
            self.resume.notified().await;
        }
        Ok(())
    }
}

#[tokio::test]
async fn a_pull_preserves_a_publication_installed_while_its_package_was_loading() {
    exercise_concurrent_pull(ConcurrentPublication::Same).await;
}

#[tokio::test]
async fn a_pull_preserves_a_newer_publication_installed_while_its_package_was_loading() {
    exercise_concurrent_pull(ConcurrentPublication::Successor).await;
}

#[tokio::test]
async fn a_pull_restarts_after_a_snapshot_retires_its_loaded_publication() {
    exercise_concurrent_pull(ConcurrentPublication::Snapshot).await;
}

enum ConcurrentPublication {
    Same,
    Successor,
    Snapshot,
}

async fn exercise_concurrent_pull(publication: ConcurrentPublication) {
    let owner_dir = test_store_dir();
    let owner_database = open_test_db(owner_dir.clone());
    let signer = UserKeypair::generate();
    let (store, storage) = TestStore::create_with_connection(
        &owner_database,
        owner_dir.clone(),
        "concurrent-publication-install",
        signer.clone(),
        test_cloud_home(),
    )
    .await
    .expect("create Store");
    let receiver_dir = test_store_dir();
    let receiver_database = open_test_db(receiver_dir.clone());
    let receiver = store
        .activate_joined_device(
            &owner_database,
            owner_dir.clone(),
            &receiver_database,
            receiver_dir.clone(),
            &signer,
            "2026-09-08T00:00:00Z",
        )
        .await
        .expect("activate receiver");
    let owner = store
        .bind_device_in(&owner_database, owner_dir, &signer)
        .await
        .expect("bind owner");
    owner_database
        .execute_test_host_write(
            "INSERT INTO notes (id, title, shared, _updated_at, created_at)
             VALUES ('shared', 'Accepted row', 1, '0000000002000-0000-owner', '2026-09-08')",
        )
        .await;
    publish(&owner).await;
    let reference = owner.latest_local_store_position().await.unwrap().unwrap();
    let commit = owner.load_commit_for_test(&reference).await.unwrap();
    let package = commit.store_package().expect("published row package");
    let pause = Arc::new(PausePackageRead {
        prefix: coven_protocol::store_commit::semantic_prefix_from_exact_object(
            &package.object,
            ".pkg",
        )
        .expect("package semantic prefix"),
        exercised: AtomicBool::new(false),
        reached: tokio::sync::Notify::new(),
        resume: tokio::sync::Notify::new(),
    });
    let intercepted = Arc::new(InterceptedStorage::new(storage, pause.clone()));
    let routing = coven_keys::encryption::EncryptionService::from_key([42; 32]);
    let first = store.pull_with_storage_for_test(
        &receiver_database,
        intercepted,
        &receiver_dir,
        Some(&routing),
    );
    tokio::pin!(first);
    tokio::select! {
        result = &mut first => panic!("first pull completed before its package read: {result:?}"),
        () = pause.reached.notified() => {}
    }
    if !matches!(publication, ConcurrentPublication::Same) {
        owner_database
            .execute_test_host_write(
                "UPDATE notes SET title = 'Accepted successor',
                 _updated_at = '0000000003000-0000-owner' WHERE id = 'shared'",
            )
            .await;
        publish(&owner).await;
    }
    if matches!(publication, ConcurrentPublication::Snapshot) {
        let mut writer = owner.authorize_writer().await.unwrap();
        let mut snapshots = writer.snapshots();
        let cut = snapshots.capture_snapshot_cut(None).await.unwrap();
        snapshots
            .push_snapshot_cut(cut, "2026-09-08T00:00:02Z".into())
            .await
            .expect("retire the accepted prefix while the first pull is paused");
    }
    let (_, second) = receiver
        .pull_store()
        .await
        .expect("second pull installs the accepted publication");
    assert!(second.held_positions.is_empty(), "{second:?}");
    let title = match publication {
        ConcurrentPublication::Same => {
            assert_eq!(second.changesets_applied, 1);
            "Accepted row"
        }
        ConcurrentPublication::Successor => {
            assert_eq!(second.changesets_applied, 2);
            "Accepted successor"
        }
        ConcurrentPublication::Snapshot => "Accepted successor",
    };
    let records = StoreDatabase::new(&receiver_database);
    let installed = records.store_current_publication().await.unwrap();
    assert_eq!(
        receiver_database
            .query_test_text("SELECT title FROM notes WHERE id = 'shared'")
            .await,
        title
    );
    pause.resume.notify_one();
    first
        .await
        .expect("first pull settles the publication already installed by the second pull");
    assert!(pause.exercised.load(Ordering::SeqCst));
    assert_eq!(
        records.store_current_publication().await.unwrap(),
        installed
    );
    assert_eq!(
        receiver_database
            .query_test_text("SELECT title FROM notes WHERE id = 'shared'")
            .await,
        title
    );
}

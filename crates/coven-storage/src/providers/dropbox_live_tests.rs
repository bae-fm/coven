use super::*;

#[test]
#[ignore = "requires COVEN_LIVE_DROPBOX_ENV and writes to Dropbox"]
fn dropbox_live_conformance() {
    run(async |live, storage| {
        crate::test_utils::Conformance::new(storage, live.store)
            .run()
            .await
            .unwrap();
        println!("Dropbox Conformance passed: store={}", live.store);
    });
}

#[test]
#[ignore = "requires COVEN_LIVE_DROPBOX_ENV and COVEN_LIVE_DROPBOX_B_ENV and writes to Dropbox"]
fn dropbox_live_account_sharing() {
    run(async |live, storage| {
        let recipient_tokens = tokens("COVEN_LIVE_DROPBOX_B_ENV", live.clock.clone()).await;
        let recipient_session =
            OAuthSession::new(PROVIDER, recipient_tokens, live.clock.clone()).unwrap();
        let recipient = Arc::new(StorageConnection::from_provider(Arc::new(
            DropboxStorage::new(storage.config(), recipient_session).unwrap(),
        )));
        let account = recipient.account().await.unwrap();
        assert!(
            account != storage.account().await.unwrap(),
            "sharing requires distinct accounts"
        );
        let member = account.clone();
        live.members.push((storage.clone(), member.clone()));
        let path = live.path(1);
        storage.create(&path, b"owner").await.unwrap();
        let AccessGrant::Granted { invitation } = storage.grant_access(&account).await.unwrap()
        else {
            panic!("Dropbox did not grant account access")
        };
        storage.grant_access(&account).await.unwrap();
        recipient.join(live.store, &invitation).await.unwrap();
        recipient.join(live.store, &invitation).await.unwrap();
        assert_eq!(recipient.read(&path).await.unwrap(), b"owner");
        let written = live.path(2);
        recipient.create(&written, b"recipient").await.unwrap();
        assert_eq!(storage.read(&written).await.unwrap(), b"recipient");
        crate::test_utils::Conformance::new(recipient.clone(), live.store)
            .owner_only_sharing()
            .await;
        for _ in 0..2 {
            assert!(storage.revoke_access(&member).await.unwrap().is_empty());
        }
        let read = recipient.read(&path).await.unwrap_err();
        println!(
            "Dropbox sharing: recipient_read_write=true revoked_read={:?}",
            read.failure()
        );
        assert_eq!(read.failure(), StorageFailure::PermissionDenied);
        assert_eq!(storage.read(&path).await.unwrap(), b"owner");
    });
}

#[test]
#[ignore = "requires COVEN_LIVE_DROPBOX_ENV and writes to Dropbox"]
fn dropbox_live_complete_publication_time() {
    run(async |live, storage| {
        for (trial, pause) in [3, 10, 20].into_iter().enumerate() {
            let base = trial as u64 * 5 + 1;
            let path = live.path(base);
            let start_before = marker(&storage, &live.path(base + 1)).await;
            let local_start = live.clock.now();
            let mut upload = live.begin(&storage, &path).await;
            let start_after = marker(&storage, &live.path(base + 2)).await;
            let first = vec![0x51; upload.part_size()];
            storage.upload_part(&mut upload, &first).await.unwrap();
            live.clock.sleep(Duration::from_secs(pause)).await;
            let finish_before = marker(&storage, &live.path(base + 3)).await;
            storage.upload_part(&mut upload, b"z").await.unwrap();
            storage.finish_upload(&mut upload).await.unwrap();
            let local_finish = live.clock.now();
            let finish_after = marker(&storage, &live.path(base + 4)).await;
            let object = listed(&storage, &path).await;
            let native = metadata(&storage, &path).await;
            println!("Dropbox publication pause={pause}s start_markers=[{},{}] finish_markers=[{},{}] listed={} server_modified={} local_start={local_start:?} local_finish={local_finish:?}",
                start_before.seconds(), start_after.seconds(), finish_before.seconds(), finish_after.seconds(), object.stored_at.seconds(), http::string(&native, "server_modified").unwrap());
            assert!(
                start_before <= start_after && start_after < finish_before,
                "marker intervals must distinguish initiation from completion"
            );
            assert!(
                finish_before <= object.stored_at && object.stored_at <= finish_after,
                "§4 requires complete-publication time, not session initiation time"
            );
            assert_eq!(
                object.stored_at,
                http::timestamp(&native, "server_modified").unwrap()
            );
            assert_eq!(object.size, first.len() as u64 + 1);
            assert_eq!(storage.status(&path).await.unwrap().unwrap(), object);
            assert_eq!(object.time_resolution, Duration::from_secs(1));
            let mut expected = first;
            expected.push(b'z');
            let read = storage.read_object(&path, None).await.unwrap();
            assert_eq!(read.metadata, object);
            assert_eq!(read.bytes, expected);
        }
    });
}

#[test]
#[ignore = "requires COVEN_LIVE_DROPBOX_ENV and writes to Dropbox"]
fn dropbox_live_unfinished_upload_is_invisible() {
    run(async |live, storage| {
        let path = live.path(1);
        let mut upload = live.begin(&storage, &path).await;
        let first = vec![0x51; upload.part_size()];
        storage.upload_part(&mut upload, &first).await.unwrap();
        live.clock.sleep(Duration::from_secs(3)).await;
        let objects = storage.list(&ObjectPrefix::all(live.store)).await.unwrap();
        let read = storage.read(&path).await;
        let status = storage.status(&path).await.unwrap();
        println!(
            "Dropbox unfinished upload: listed={} status_present={} read={:?}",
            objects.iter().any(|object| object.path == path),
            status.is_some(),
            read.as_ref().err().map(StorageError::failure)
        );
        assert!(!objects.iter().any(|object| object.path == path));
        assert_eq!(status, None);
        assert_eq!(read.unwrap_err().failure(), StorageFailure::NotFound);
    });
}

#[test]
#[ignore = "requires COVEN_LIVE_DROPBOX_ENV and writes to Dropbox"]
// Dropbox keeps rev and server_modified on an identical-byte overwrite, which §4
// permits: coven never replaces an object with identical bytes. The equal-byte
// step is printed for the record and only different bytes must change revision.
fn dropbox_live_replacement_revision() {
    run(async |live, storage| {
        let path = ObjectPath::positions(live.store, DeviceId(31));
        let mut native_revisions = Vec::new();
        let mut adapter_revisions = Vec::new();
        let mut replacements = Vec::new();
        for (step, bytes) in [
            ("initial", b"same"),
            ("equal", b"same"),
            ("different_same_size", b"else"),
        ] {
            let replacement = storage.replace(&path, bytes).await;
            let native = metadata(&storage, &path).await;
            let object = listed(&storage, &path).await;
            let status = storage.status(&path).await.unwrap().unwrap();
            let read = storage.read_object(&path, None).await.unwrap();
            let rev = http::string(&native, "rev").unwrap().to_owned();
            println!("Dropbox replacement {step}: failure={:?} native_rev={rev} server_modified={} listed_revision={} status_revision={} read_revision={}",
                replacement.as_ref().err().map(StorageError::failure), http::string(&native, "server_modified").unwrap(), object.revision, status.revision, read.metadata.revision);
            assert_eq!(object.revision, rev);
            assert_eq!(status, object);
            assert_eq!(read.metadata, object);
            adapter_revisions.push(object.revision);
            native_revisions.push(rev);
            replacements.push(replacement);
            assert_eq!(read.bytes, bytes);
        }
        for replacement in replacements {
            replacement.unwrap();
        }
        assert_ne!(
            native_revisions[1], native_revisions[2],
            "§4: equal-size replacement must change revision"
        );
        assert_ne!(adapter_revisions[1], adapter_revisions[2]);
    });
}

#[test]
#[ignore = "requires COVEN_LIVE_DROPBOX_ENV and writes to Dropbox"]
fn dropbox_live_immutable_time_is_stable() {
    run(async |live, storage| {
        let path = live.path(1);
        storage.create(&path, b"immutable").await.unwrap();
        let before = listed(&storage, &path).await;
        assert_eq!(storage.status(&path).await.unwrap().unwrap(), before);
        let native_before = metadata(&storage, &path).await;
        live.clock.sleep(Duration::from_secs(3)).await;
        marker(&storage, &live.path(2)).await;
        assert_eq!(
            storage
                .create(&path, b"immutable")
                .await
                .unwrap_err()
                .failure(),
            StorageFailure::AlreadyExists
        );
        let read = storage.read_object(&path, None).await.unwrap();
        assert_eq!(read.bytes, b"immutable");
        assert_eq!(read.metadata, before);
        let after = listed(&storage, &path).await;
        let native_after = metadata(&storage, &path).await;
        println!(
            "Dropbox immutable time: before={} after={} native_before={} native_after={}",
            before.stored_at.seconds(),
            after.stored_at.seconds(),
            http::string(&native_before, "server_modified").unwrap(),
            http::string(&native_after, "server_modified").unwrap()
        );
        assert_eq!(before, after);
        assert_eq!(storage.status(&path).await.unwrap().unwrap(), before);
        assert_eq!(
            http::timestamp(&native_before, "server_modified").unwrap(),
            before.stored_at
        );
        assert_eq!(
            http::timestamp(&native_after, "server_modified").unwrap(),
            before.stored_at
        );
    });
}

#[test]
#[ignore = "requires COVEN_LIVE_DROPBOX_ENV and writes to Dropbox"]
fn dropbox_live_create_once() {
    run(async |live, storage| {
        let path = live.path(1);
        storage.create(&path, b"original").await.unwrap();
        for bytes in [b"original", b"replaced"] {
            let error = storage.create(&path, bytes).await.unwrap_err();
            let actual = storage.read(&path).await.unwrap();
            println!(
                "Dropbox create occupied path: failure={:?} original_bytes_retained={}",
                error.failure(),
                actual == b"original"
            );
            assert_eq!(error.failure(), StorageFailure::AlreadyExists);
            assert_eq!(actual, b"original");
        }
    });
}

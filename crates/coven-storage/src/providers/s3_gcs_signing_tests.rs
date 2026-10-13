//! Conformance against a real S3 service. Ignored by default: run with
//! `COVEN_LIVE_S3_ENV=<file> cargo test -p coven-storage --all-features --lib s3_live -- --ignored`.
//! The file holds `COVEN_LIVE_S3_{BUCKET,REGION,ENDPOINT,ACCESS_KEY_ID,SECRET_ACCESS_KEY}`;
//! each run uses a fresh store id under `coven-live/` and cleans it after failures too.
use super::*;
use coven_crypto::SecretText;
use coven_foundation::{
    clock::SystemClock,
    id_source::{StoreId, UuidIds},
};
use std::{collections::BTreeMap, sync::Arc};

fn settings() -> BTreeMap<String, String> {
    let path =
        std::env::var("COVEN_LIVE_S3_ENV").expect("COVEN_LIVE_S3_ENV names the credentials file");
    std::fs::read_to_string(&path)
        .expect("credentials file")
        .lines()
        .filter_map(|line| line.split_once('='))
        .map(|(key, value)| (key.trim().to_owned(), value.trim().to_owned()))
        .collect()
}

fn live() -> StorageConnection<S3Storage> {
    let settings = settings();
    let value = |key: &str| {
        settings
            .get(key)
            .unwrap_or_else(|| panic!("{key} missing"))
            .clone()
    };
    let storage = S3Storage::new(
        StorageConfig::S3 {
            bucket: value("COVEN_LIVE_S3_BUCKET"),
            region: value("COVEN_LIVE_S3_REGION"),
            endpoint: settings
                .get("COVEN_LIVE_S3_ENDPOINT")
                .map(|url| url.parse().unwrap()),
            prefix: "coven-live".into(),
        },
        S3Credentials {
            access_key_id: value("COVEN_LIVE_S3_ACCESS_KEY_ID"),
            secret_access_key: SecretText::new(value("COVEN_LIVE_S3_SECRET_ACCESS_KEY")),
        },
        Arc::new(SystemClock),
        Arc::new(UuidIds),
    )
    .unwrap();
    StorageConnection::from_provider(Arc::new(storage))
}

#[tokio::test]
#[ignore]
async fn s3_live_conformance() {
    let store = StoreId(uuid::Uuid::new_v4());
    let storage = Arc::new(live());
    let connected = storage.clone();
    let result = tokio::spawn(async move {
        crate::test_utils::Conformance::new(connected.clone(), store)
            .run()
            .await
            .unwrap();
        let path = ObjectPath::device_log(
            store,
            coven_foundation::id_source::DeviceId(41),
            std::num::NonZeroU64::MIN,
        );
        check_provider(connected.as_ref(), &path, b"sealed live provider check")
            .await
            .unwrap();
    })
    .await;
    cleanup(&storage.provider, store).await.unwrap();
    result.unwrap();
}

#[tokio::test]
#[ignore]
async fn s3_live_multipart_create_once_and_copy_publication_time() {
    let store = StoreId(uuid::Uuid::new_v4());
    let mut storage = live();
    Arc::get_mut(&mut storage.provider)
        .unwrap()
        .single_request_limit = 1024 * 1024;
    let storage = Arc::new(storage);
    let connected = storage.clone();
    let result = tokio::spawn(async move {
        let path = ObjectPath::device_log(
            store,
            coven_foundation::id_source::DeviceId(41),
            std::num::NonZeroU64::MIN,
        );
        // Cross both the lowered one-request limit and the native multipart
        // minimum part size. The adapter uses two parts: 8 MiB and one byte.
        let bytes = vec![42; 8 * 1024 * 1024 + 1];
        connected.create(&path, &bytes).await.unwrap();
        let listed = connected.list(&ObjectPrefix::all(store)).await.unwrap();
        assert_eq!(listed.len(), 1);
        let first_time = listed[0].stored_at;
        let error = connected
            .create(&path, &vec![7; bytes.len()])
            .await
            .unwrap_err();
        assert_eq!(error.failure(), StorageFailure::AlreadyExists);
        assert_eq!(connected.read(&path).await.unwrap(), bytes);
        assert_eq!(
            connected.list(&ObjectPrefix::all(store)).await.unwrap()[0].stored_at,
            first_time
        );
        connected.delete(&path).await.unwrap();
        check_provider(connected.as_ref(), &path, &bytes)
            .await
            .unwrap();

        let mut session = connected
            .begin_upload(&path, bytes.len() as u64)
            .await
            .unwrap();
        let recording = session.encode().unwrap();
        let part_size = session.part_size();
        connected
            .upload_part(&mut session, &bytes[..part_size])
            .await
            .unwrap();
        assert_eq!(connected.status(&path).await.unwrap(), None, "unfinished multipart upload is absent from exact status");
        assert_eq!(connected.read(&path).await.unwrap_err().failure(), StorageFailure::NotFound);
        let mut resumed = UploadSession::decode(recording.as_bytes()).unwrap();
        connected.resume_upload(&mut resumed).await.unwrap();
        assert_eq!(resumed.confirmed_bytes(), session.part_size() as u64);
        connected
            .upload_part(&mut resumed, &bytes[session.part_size()..])
            .await
            .unwrap();
        if connected.provider.service == S3Service::Gcs {
            // Assemble without publishing, as when the completion reply is lost.
            let SessionState::S3 { id, parts, .. } = &resumed.state else {
                panic!("S3 session");
            };
            let temporary = connected.provider.session_key(&resumed).unwrap();
            connected
                .provider
                .client
                .complete_multipart_upload()
                .bucket(&connected.provider.bucket)
                .key(&temporary)
                .upload_id(id.as_str())
                .multipart_upload(
                    CompletedMultipartUpload::builder()
                        .set_parts(Some(
                            parts
                                .iter()
                                .map(|part| {
                                    CompletedPart::builder()
                                        .part_number(part.number)
                                        .e_tag(&part.etag)
                                        .build()
                                })
                                .collect(),
                        ))
                        .build(),
                )
                .send()
                .await
                .map_err(s3_error)
                .unwrap();
            let before = connected
                .provider
                .client
                .head_object()
                .bucket(&connected.provider.bucket)
                .key(&temporary)
                .send()
                .await
                .map_err(s3_error)
                .unwrap();
            let assembled_at: SystemTime = (*before.last_modified().unwrap()).try_into().unwrap();
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            connected.finish_upload(&mut resumed).await.unwrap();
            let published = connected.list(&ObjectPrefix::all(store)).await.unwrap();
            assert_eq!(published.len(), 1);
            assert!(published[0].stored_at > assembled_at);
            assert_eq!(connected.read(&path).await.unwrap(), bytes);
            assert_eq!(
                connected
                    .provider
                    .client
                    .head_object()
                    .bucket(&connected.provider.bucket)
                    .key(&temporary)
                    .send()
                    .await
                    .map_err(s3_error)
                    .unwrap_err()
                    .failure(),
                StorageFailure::NotFound
            );
            let mut stale = UploadSession::decode(recording.as_bytes()).unwrap();
            connected.resume_upload(&mut stale).await.unwrap();
            assert!(stale.is_complete());
        } else {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            let marker = ObjectPath::clock(store, coven_foundation::id_source::DeviceId(41));
            connected.replace(&marker, b"before completion").await.unwrap();
            let before = connected.status(&marker).await.unwrap().unwrap();
            connected.finish_upload(&mut resumed).await.unwrap();
            let published = connected.status(&path).await.unwrap().unwrap();
            assert!(published.stored_at >= before.stored_at, "AWS multipart Last-Modified reports initiation rather than complete publication: published={:?}, before completion={:?}", published.stored_at, before.stored_at);
        }
        connected.finish_upload(&mut resumed).await.unwrap();
    })
    .await;
    cleanup(&storage.provider, store).await.unwrap();
    result.unwrap();
}

/// Use native listings so completed temporaries and incomplete uploads are both
/// removed, including after an assertion or a provider operation fails.
async fn cleanup(storage: &S3Storage, store: StoreId) -> Result<(), StorageError> {
    let prefix = ObjectPrefix::all(store).under(&storage.prefix);
    let mut key_marker = None;
    let mut upload_marker = None;
    let mut uploads = Vec::new();
    let mut pages = Pagination::new();
    loop {
        let page = storage
            .client
            .list_multipart_uploads()
            .bucket(&storage.bucket)
            .prefix(&prefix)
            .set_key_marker(key_marker)
            .set_upload_id_marker(upload_marker)
            .send()
            .await
            .map_err(s3_error)?;
        for upload in page.uploads() {
            uploads.push((
                upload.key().expect("upload key").to_owned(),
                upload.upload_id().expect("upload id").to_owned(),
            ));
        }
        if !page.is_truncated().expect("multipart listing completion") {
            break;
        }
        let key = page
            .next_key_marker()
            .expect("multipart key marker")
            .to_owned();
        let id = page
            .next_upload_id_marker()
            .expect("multipart id marker")
            .to_owned();
        pages.check(&format!("{key}/{id}"))?;
        key_marker = Some(key);
        upload_marker = Some(id);
    }
    for (key, id) in uploads {
        assert!(key.starts_with(&prefix));
        storage
            .client
            .abort_multipart_upload()
            .bucket(&storage.bucket)
            .key(key)
            .upload_id(id)
            .send()
            .await
            .map_err(s3_error)?;
    }
    let mut marker = None;
    let mut keys = Vec::new();
    let mut pages = Pagination::new();
    loop {
        let page = storage
            .client
            .list_objects_v2()
            .bucket(&storage.bucket)
            .prefix(&prefix)
            .set_continuation_token(marker)
            .send()
            .await
            .map_err(s3_error)?;
        for object in page.contents() {
            keys.push(object.key().expect("object key").to_owned());
        }
        if !page.is_truncated().expect("object listing completion") {
            break;
        }
        let next = page
            .next_continuation_token()
            .expect("object marker")
            .to_owned();
        pages.check(&next)?;
        marker = Some(next);
    }
    for key in keys {
        assert!(key.starts_with(&prefix));
        storage
            .client
            .delete_object()
            .bucket(&storage.bucket)
            .key(key)
            .send()
            .await
            .map_err(s3_error)?;
    }
    let objects = storage
        .client
        .list_objects_v2()
        .bucket(&storage.bucket)
        .prefix(&prefix)
        .send()
        .await
        .map_err(s3_error)?;
    assert!(objects.contents().is_empty());
    let uploads = storage
        .client
        .list_multipart_uploads()
        .bucket(&storage.bucket)
        .prefix(&prefix)
        .send()
        .await
        .map_err(s3_error)?;
    assert!(uploads.uploads().is_empty());
    Ok(())
}

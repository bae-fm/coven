use super::*;
use aws_sdk_s3::operation::{
    abort_multipart_upload::AbortMultipartUploadError,
    complete_multipart_upload::CompleteMultipartUploadError, copy_object::CopyObjectError,
};
use std::collections::BTreeSet;
use std::error::Error;

#[derive(Default)]
struct Conflicts {
    rejected: BTreeSet<String>,
    publications: Vec<String>,
    parts: Vec<(String, Vec<u8>)>,
    refuse_abort: bool,
    conflict_next: bool,
    /// The concurrent-mutation code the next publication is refused with.
    code: &'static str,
}

async fn fixture(
    gcs: bool,
) -> (TestServer, StorageConnection<S3Storage>, Arc<Mutex<Remote>>, Arc<Mutex<Conflicts>>) {
    let remote = Arc::new(Mutex::new(Remote { gcs, ..Remote::default() }));
    let faults = Arc::new(Mutex::new(Conflicts {
        conflict_next: true,
        code: conflict_code(gcs),
        ..Conflicts::default()
    }));
    let state = remote.clone();
    let conflicts = faults.clone();
    let server = TestServer::new(Router::new().fallback(
        move |method: Method, uri: Uri, headers: HeaderMap, body: Bytes| {
            let mut faults = conflicts.lock().unwrap();
            let q = query(&uri);
            let publication = if gcs {
                headers.get("x-goog-copy-source").map(|s| s.to_str().unwrap().to_owned())
            } else {
                q.get("uploadId").filter(|_| method == Method::POST).cloned()
            };
            if let Some(id) = publication {
                faults.publications.push(id.clone());
                if std::mem::take(&mut faults.conflict_next) {
                    faults.rejected.insert(id);
                    return std::future::ready(response(
                        409,
                        format!("<Error><Code>{}</Code></Error>", faults.code),
                    ));
                }
                assert!(!faults.rejected.contains(&id), "reused rejected publication");
            }
            if let Some(id) = q.get("uploadId") {
                if method == Method::PUT {
                    faults.parts.push((id.clone(), body.to_vec()));
                }
                if method == Method::DELETE && faults.refuse_abort {
                    return std::future::ready(response(
                        403,
                        "<Error><Code>AccessDenied</Code></Error>",
                    ));
                }
            }
            std::future::ready(respond(&state, method, uri, headers, body))
        },
    ))
    .await;
    let storage = if gcs { gcs::gcs_provider(&server.url) } else { provider(&server.url) };
    (server, storage, remote, faults)
}

fn path() -> ObjectPath {
    ObjectPath::slot(STORE_ID, std::num::NonZeroU64::MIN)
}

fn conflict_code(gcs: bool) -> &'static str {
    if gcs {
        "OperationAborted"
    } else {
        "ConditionalRequestConflict"
    }
}

/// Every code Appendix E.10 names as a concurrent mutation: AWS's, GCS's and Ceph RGW's.
const CONFLICT_CODES: [&str; 3] =
    ["ConditionalRequestConflict", "OperationAborted", "ConcurrentModification"];

fn assert_conflict_cause(error: &StorageError, gcs: bool, code: &str) {
    let native = error.source().unwrap().downcast_ref::<StorageError>().unwrap();
    assert_eq!(native.failure(), StorageFailure::Conflict);
    if gcs {
        assert_eq!(errors::sdk_error::<CopyObjectError>(native).code(), Some(code));
    } else {
        assert_eq!(errors::sdk_error::<CompleteMultipartUploadError>(native).code(), Some(code));
    }
}

#[tokio::test]
async fn conditional_publication_conflict_restarts_with_identical_bytes() {
    for (gcs, code) in
        [false, true].into_iter().flat_map(|gcs| CONFLICT_CODES.map(|code| (gcs, code)))
    {
        let (_server, storage, remote, faults) = fixture(gcs).await;
        faults.lock().unwrap().code = code;
        let bytes = vec![7; 8 * 1024 * 1024 + 1];
        let mut session = storage.begin_upload(&path(), bytes.len() as u64).await.unwrap();
        for part in bytes.chunks(session.part_size()) {
            storage.upload_part(&mut session, part).await.unwrap();
        }
        let first = storage.provider.upload_id(&session).unwrap().to_owned();
        let error = storage.finish_upload(&mut session).await.unwrap_err();
        assert_eq!(error.failure(), StorageFailure::SessionExpired);
        assert_conflict_cause(&error, gcs, code);
        assert!(remote.lock().unwrap().uploads.is_empty());
        assert!(remote.lock().unwrap().objects.is_empty());
        let mut recorded = UploadSession::decode(session.encode().unwrap().as_bytes()).unwrap();
        assert_eq!(
            storage.finish_upload(&mut recorded).await.unwrap_err().failure(),
            StorageFailure::SessionExpired
        );
        assert_eq!(
            storage.resume_upload(&mut recorded).await.unwrap_err().failure(),
            StorageFailure::SessionExpired
        );
        let mut next = storage.restart_upload(&recorded).await.unwrap();
        assert_eq!(next.path(), session.path());
        assert_eq!(next.total_bytes(), bytes.len() as u64);
        assert_eq!(next.confirmed_bytes(), 0);
        for part in bytes.chunks(next.part_size()) {
            storage.upload_part(&mut next, part).await.unwrap();
        }
        assert_ne!(storage.provider.upload_id(&next).unwrap(), first);
        // The new session must still recover a lost successful publication reply.
        if gcs {
            remote.lock().unwrap().fail_copy_reply = true;
        } else {
            remote.lock().unwrap().fail_completion_reply = true;
        }
        assert_eq!(
            storage.finish_upload(&mut next).await.unwrap_err().failure(),
            StorageFailure::Network
        );
        let mut next = UploadSession::decode(next.encode().unwrap().as_bytes()).unwrap();
        storage.resume_upload(&mut next).await.unwrap();
        assert!(next.is_complete());
        assert_eq!(storage.read(&path()).await.unwrap(), bytes);
        let faults = faults.lock().unwrap();
        assert_eq!(faults.parts.len(), 4);
        for part in 0..2 {
            assert_ne!(faults.parts[part].0, faults.parts[part + 2].0);
            assert_eq!(faults.parts[part].1, faults.parts[part + 2].1);
        }
    }
}

#[tokio::test]
async fn conditional_conflict_abort_failure_preserves_both_causes_and_blocks_reuse() {
    for gcs in [false, true] {
        let (_server, storage, remote, faults) = fixture(gcs).await;
        let mut session = storage.begin_upload(&path(), 4).await.unwrap();
        storage.upload_part(&mut session, b"data").await.unwrap();
        faults.lock().unwrap().refuse_abort = true;
        let error = storage.finish_upload(&mut session).await.unwrap_err();
        let StorageError::Cleanup { operation, cleanup } = error else {
            panic!("both causes required")
        };
        assert_eq!(operation.failure(), StorageFailure::SessionExpired);
        assert_eq!(cleanup.failure(), StorageFailure::PermissionDenied);
        assert_conflict_cause(&operation, gcs, conflict_code(gcs));
        assert_eq!(
            errors::sdk_error::<AbortMultipartUploadError>(&cleanup).code(),
            Some("AccessDenied")
        );
        let mut session = UploadSession::decode(session.encode().unwrap().as_bytes()).unwrap();
        assert!(storage.resume_upload(&mut session).await.is_err());
        assert!(storage.finish_upload(&mut session).await.is_err());
        assert_eq!(
            storage.upload_part(&mut session, b"data").await.unwrap_err().failure(),
            StorageFailure::SessionExpired
        );
        assert!(storage.restart_upload(&session).await.is_err());
        assert_eq!(faults.lock().unwrap().publications.len(), 1);
        assert_eq!(faults.lock().unwrap().parts.len(), 1);
        assert_eq!(remote.lock().unwrap().next_upload, 1);
        faults.lock().unwrap().refuse_abort = false;
        let mut next = storage.restart_upload(&session).await.unwrap();
        storage.upload_part(&mut next, b"data").await.unwrap();
        storage.finish_upload(&mut next).await.unwrap();
        assert_eq!(storage.read(&path()).await.unwrap(), b"data");
        assert!(remote.lock().unwrap().uploads.is_empty());
        assert_eq!(remote.lock().unwrap().objects.len(), 1);
    }
}

#[tokio::test]
async fn restarted_publication_preserves_an_occupant_for_complete_byte_comparison() {
    for gcs in [false, true] {
        for occupant in [b"data", b"else"] {
            let (_server, storage, _remote, _faults) = fixture(gcs).await;
            let mut session = storage.begin_upload(&path(), 4).await.unwrap();
            storage.upload_part(&mut session, b"data").await.unwrap();
            assert_eq!(
                storage.finish_upload(&mut session).await.unwrap_err().failure(),
                StorageFailure::SessionExpired
            );
            storage.create(&path(), occupant).await.unwrap();
            let mut next = storage.restart_upload(&session).await.unwrap();
            storage.upload_part(&mut next, b"data").await.unwrap();
            assert_eq!(
                storage.finish_upload(&mut next).await.unwrap_err().failure(),
                StorageFailure::AlreadyExists
            );
            assert!(!next.is_complete());
            assert_eq!(storage.read(&path()).await.unwrap(), occupant);
            storage.abort_upload(&next).await.unwrap();
            assert_eq!(storage.read(&path()).await.unwrap() == b"data", occupant == b"data");
        }
    }
}

#[tokio::test]
async fn gcs_conflict_cannot_restart_until_temporary_deletion_succeeds() {
    let (_server, storage, remote, faults) = fixture(true).await;
    let mut session = storage.begin_upload(&path(), 4).await.unwrap();
    storage.upload_part(&mut session, b"data").await.unwrap();
    remote.lock().unwrap().refuse_temporary_delete = true;
    let error = storage.finish_upload(&mut session).await.unwrap_err();
    let StorageError::Cleanup { operation, cleanup } = error else {
        panic!("both causes required")
    };
    assert_conflict_cause(&operation, true, conflict_code(true));
    assert_eq!(
        errors::sdk_error::<aws_sdk_s3::operation::delete_object::DeleteObjectError>(&cleanup)
            .code(),
        Some("AccessDenied")
    );
    let session = UploadSession::decode(session.encode().unwrap().as_bytes()).unwrap();
    assert!(storage.restart_upload(&session).await.is_err());
    assert_eq!(remote.lock().unwrap().next_upload, 1);
    assert_eq!(remote.lock().unwrap().objects.len(), 1);
    assert_eq!(faults.lock().unwrap().publications.len(), 1);
    remote.lock().unwrap().refuse_temporary_delete = false;
    let mut next = storage.restart_upload(&session).await.unwrap();
    assert!(remote.lock().unwrap().objects.is_empty());
    storage.upload_part(&mut next, b"data").await.unwrap();
    storage.finish_upload(&mut next).await.unwrap();
    assert_eq!(storage.read(&path()).await.unwrap(), b"data");
}

#[tokio::test]
async fn lost_publication_reply_and_failed_abort_keep_both_native_causes() {
    for gcs in [false, true] {
        let (_server, mut storage, remote, faults) = fixture(gcs).await;
        faults.lock().unwrap().conflict_next = false;
        faults.lock().unwrap().refuse_abort = true;
        Arc::get_mut(&mut storage.provider).unwrap().single_request_limit = 3;
        if gcs {
            remote.lock().unwrap().fail_copy_reply = true;
        } else {
            remote.lock().unwrap().fail_completion_reply = true;
        }
        let error = storage.create(&path(), b"data").await.unwrap_err();
        let StorageError::Cleanup { operation, cleanup } = error else {
            panic!("both causes required")
        };
        assert_eq!(operation.failure(), StorageFailure::Network);
        if gcs {
            assert_eq!(
                errors::sdk_error::<CopyObjectError>(&operation).code(),
                Some("ServiceUnavailable")
            );
        } else {
            assert_eq!(
                errors::sdk_error::<CompleteMultipartUploadError>(&operation).code(),
                Some("ServiceUnavailable")
            );
        }
        assert_eq!(
            errors::sdk_error::<AbortMultipartUploadError>(&cleanup).code(),
            Some("AccessDenied")
        );
        assert_eq!(storage.read(&path()).await.unwrap(), b"data");
    }
}

#[tokio::test]
async fn gcs_copy_conflict_during_recovery_invalidates_the_recorded_session() {
    let (_server, storage, remote, _faults) = fixture(true).await;
    let mut session = storage.begin_upload(&path(), 4).await.unwrap();
    storage.upload_part(&mut session, b"data").await.unwrap();
    remote.lock().unwrap().fail_completion_reply = true;
    assert_eq!(
        storage.finish_upload(&mut session).await.unwrap_err().failure(),
        StorageFailure::Network
    );
    let mut session = UploadSession::decode(session.encode().unwrap().as_bytes()).unwrap();
    let error = storage.resume_upload(&mut session).await.unwrap_err();
    assert_eq!(error.failure(), StorageFailure::SessionExpired);
    assert_conflict_cause(&error, true, conflict_code(true));
    let session = UploadSession::decode(session.encode().unwrap().as_bytes()).unwrap();
    assert!(remote.lock().unwrap().objects.is_empty());
    let mut next = storage.restart_upload(&session).await.unwrap();
    storage.upload_part(&mut next, b"data").await.unwrap();
    storage.finish_upload(&mut next).await.unwrap();
    assert_eq!(storage.read(&path()).await.unwrap(), b"data");
}

#[tokio::test]
async fn single_put_conflict_retains_its_class_for_publication_recovery() {
    for gcs in [false, true] {
        let (_server, storage, remote, _faults) = fixture(gcs).await;
        remote.lock().unwrap().forced_error = Some((conflict_code(gcs), 409));
        let error = storage.create(&path(), b"data").await.unwrap_err();
        assert_eq!(error.failure(), StorageFailure::Conflict);
        assert!(!error.retryable());
        remote.lock().unwrap().forced_error = None;
        storage.create(&path(), b"data").await.unwrap();
        assert_eq!(storage.read(&path()).await.unwrap(), b"data");
        assert_eq!(remote.lock().unwrap().next_upload, 0);
    }
}

mod sequence {
    use super::*;

    #[tokio::test]
    async fn floor_replacement_conflicts_and_lost_reply_preserve_the_accepted_revision() {
        for profile in profile::PROFILES {
            let state = Arc::new(Mutex::new(Remote {
                gcs: profile.settings.signing == S3Signing::GoogleHmac,
                ..Remote::default()
            }));
            let server =
                TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
            let storage = profile_provider(&server.url, profile.settings);
            let path = ObjectPath::floor(crate::test_utils::STORE_ID);
            storage.create(&path, b"floor one").await.unwrap();
            let first = storage.status(&path).await.unwrap().unwrap();
            storage.replace(&first, b"floor two").await.unwrap();
            assert_eq!(
                storage.replace(&first, b"stale floor").await.unwrap_err().failure(),
                StorageFailure::Conflict
            );
            assert_eq!(storage.read(&path).await.unwrap(), b"floor two");
            let second = storage.status(&path).await.unwrap().unwrap();
            if profile.settings.copy_publication.single_request().is_some() {
                state.lock().unwrap().fail_copy_reply = true;
            } else {
                state.lock().unwrap().lose_create_reply = true;
            }
            assert_eq!(
                storage.replace(&second, b"floor three").await.unwrap_err().failure(),
                StorageFailure::Network
            );
            assert_eq!(
                storage.replace(&second, b"stale retry").await.unwrap_err().failure(),
                StorageFailure::Conflict
            );
            let third = storage.read_object(&path, None).await.unwrap();
            assert_eq!(third.bytes, b"floor three");
            assert_ne!(second.revision, third.metadata.revision);
            assert_ne!(second.replace_token, third.metadata.replace_token);
        }
    }

    #[tokio::test]
    async fn slot_create_once_refuses_equal_and_different_bytes_without_replacement() {
        for profile in profile::PROFILES {
            let state = Arc::new(Mutex::new(Remote {
                gcs: profile.settings.signing == S3Signing::GoogleHmac,
                ..Remote::default()
            }));
            let server =
                TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
            let storage = profile_provider(&server.url, profile.settings);
            let path = ObjectPath::slot(crate::test_utils::STORE_ID, std::num::NonZeroU64::MIN);
            storage.create(&path, b"claim one").await.unwrap();
            let first = storage.status(&path).await.unwrap().unwrap();
            for bytes in [b"claim one", b"claim two"] {
                assert_eq!(
                    storage.create(&path, bytes).await.unwrap_err().failure(),
                    StorageFailure::AlreadyExists
                );
                assert_eq!(storage.read(&path).await.unwrap(), b"claim one");
                assert_eq!(storage.status(&path).await.unwrap().unwrap(), first);
            }
        }
    }
}

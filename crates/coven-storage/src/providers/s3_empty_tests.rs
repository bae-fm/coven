use super::*;

/// A fake bucket holding a dev prefix's objects and upload beside a sibling
/// prefix, another prefix and another upload, which must all stay.
fn seeded() -> Arc<Mutex<Remote>> {
    let objects = [
        "dev/0f8e/floor",
        "dev/0f8e/files/1/a",
        "dev/0f8e/.coven-uploads/temporary",
        "dev/stray-note.txt",
        "dev-old/kept",
        "other/kept",
    ];
    let uploads = [("1", "dev/0f8e/files/1/big"), ("2", "other/upload")];
    Arc::new(Mutex::new(Remote {
        objects: objects.iter().map(|key| (key.to_string(), b"bytes".to_vec())).collect(),
        uploads: uploads
            .iter()
            .map(|(id, key)| {
                let upload = PendingUpload {
                    key: key.to_string(),
                    token: "token".into(),
                    parts: BTreeMap::new(),
                };
                (id.to_string(), upload)
            })
            .collect(),
        ..Remote::default()
    }))
}

fn remaining(state: &Mutex<Remote>) -> (Vec<String>, Vec<String>) {
    let state = state.lock().unwrap();
    (
        state.objects.keys().cloned().collect(),
        state.uploads.values().map(|upload| upload.key.clone()).collect(),
    )
}

#[tokio::test]
async fn emptying_removes_everything_under_the_prefix_across_pages_and_nothing_beside_it() {
    let state = seeded();
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = profile_provider(&server.url, profile::PLAIN);

    // The fake lists one key per page, so four objects take four pages.
    let emptied = storage.provider.empty_under("dev/", S3Listing::V2).await.unwrap();

    assert_eq!(emptied, EmptiedS3Prefix { objects: 4, uploads: 1 });
    assert_eq!(
        remaining(&state),
        (vec!["dev-old/kept".to_owned(), "other/kept".to_owned()], vec!["other/upload".to_owned()])
    );
    // Emptying an empty prefix removes nothing and succeeds.
    assert_eq!(
        storage.provider.empty_under("dev/", S3Listing::V2).await.unwrap(),
        EmptiedS3Prefix { objects: 0, uploads: 0 }
    );
}

#[tokio::test]
async fn a_refused_deletion_stops_emptying_with_its_cause() {
    let state = seeded();
    state.lock().unwrap().refuse_delete = true;
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = profile_provider(&server.url, profile::PLAIN);

    let error = storage.provider.empty_under("dev/", S3Listing::V2).await.unwrap_err();

    assert_eq!(error.failure(), StorageFailure::PermissionDenied);
    assert!(matches!(error, StorageError::Provider { .. }), "{error:?}");
    // The upload was aborted before the refusal; one deletion was attempted.
    let (objects, uploads) = remaining(&state);
    assert_eq!(objects.len(), 6);
    assert_eq!(uploads, ["other/upload"]);
    assert_eq!(state.lock().unwrap().deletions, 1);
}

#[tokio::test]
async fn an_object_arriving_while_emptying_is_a_conflict() {
    // Every listing shows the same object, as if a device kept writing it.
    let server = TestServer::new(Router::new().fallback(|method: Method, uri: Uri| async move {
        let q = query(&uri);
        match method {
            Method::DELETE => response(204, ""),
            _ if q.contains_key("uploads") => response(
                200,
                "<ListMultipartUploadsResult><IsTruncated>false</IsTruncated></ListMultipartUploadsResult>",
            ),
            _ => response(
                200,
                "<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>dev/new</Key></Contents></ListBucketResult>",
            ),
        }
    }))
    .await;
    let storage = profile_provider(&server.url, profile::PLAIN);

    let error = storage.provider.empty_under("dev/", S3Listing::V2).await.unwrap_err();

    assert_eq!(error.failure(), StorageFailure::Conflict);
}

#[tokio::test]
async fn a_key_listed_outside_the_prefix_is_refused_before_anything_is_deleted() {
    let deletions = Arc::new(Mutex::new(0));
    let counted = deletions.clone();
    let server = TestServer::new(Router::new().fallback(move |method: Method, uri: Uri| {
        let counted = counted.clone();
        async move {
            let q = query(&uri);
            match method {
                Method::DELETE => {
                    *counted.lock().unwrap() += 1;
                    response(204, "")
                }
                _ if q.contains_key("uploads") => response(
                    200,
                    "<ListMultipartUploadsResult><IsTruncated>false</IsTruncated></ListMultipartUploadsResult>",
                ),
                _ => response(
                    200,
                    "<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>dev/a</Key></Contents><Contents><Key>elsewhere/b</Key></Contents></ListBucketResult>",
                ),
            }
        }
    }))
    .await;
    let storage = profile_provider(&server.url, profile::PLAIN);

    let error = storage.provider.empty_under("dev/", S3Listing::V2).await.unwrap_err();

    assert_eq!(error.failure(), StorageFailure::Protocol);
    assert_eq!(*deletions.lock().unwrap(), 0);
}

#[cfg(feature = "dev-tools")]
mod location {
    use super::*;

    fn location(url: &str, prefix: &str) -> (StorageConfig, S3Credentials) {
        (
            StorageConfig::S3 {
                bucket: "bucket".into(),
                region: "us-east-1".into(),
                endpoint: Some(url.parse().unwrap()),
                // AWS keys can be limited to a prefix, so the location may name one.
                profile: Some(S3Profile::Named(S3ProfileName::Aws)),
                prefix: crate::S3Prefix::parse(prefix).unwrap(),
            },
            S3Credentials {
                access_key_id: "access".into(),
                secret_access_key: SecretText::new("secret".into()),
            },
        )
    }

    async fn empty(url: &str, prefix: &str) -> Result<EmptiedS3Prefix, EmptyS3PrefixError> {
        let (config, credentials) = location(url, prefix);
        empty_prefix(
            config,
            credentials,
            Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH)),
            Arc::new(SequentialIds::new()),
            crate::providers::tests::http_client(),
        )
        .await
    }

    #[tokio::test]
    async fn a_location_is_emptied_under_its_base_prefix_and_not_a_sibling_prefix() {
        // Slashes typed at the prefix's ends name the same prefix.
        for typed in ["dev", "dev/", "/dev/"] {
            let state = seeded();
            let server =
                TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;

            let emptied = empty(&server.url, typed).await.unwrap();

            assert_eq!(emptied, EmptiedS3Prefix { objects: 4, uploads: 1 }, "{typed}");
            assert_eq!(
                remaining(&state).0,
                ["dev-old/kept".to_owned(), "other/kept".to_owned()],
                "{typed}"
            );
        }
    }

    #[tokio::test]
    async fn a_location_without_a_base_prefix_is_refused_before_any_request() {
        // A prefix of only slashes is the bucket's root too.
        for typed in ["", "/", "//"] {
            let state = seeded();
            let server =
                TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;

            let error = empty(&server.url, typed).await.unwrap_err();

            assert!(matches!(error, EmptyS3PrefixError::WholeBucket), "{typed}: {error:?}");
            assert!(state.lock().unwrap().requests.is_empty(), "{typed}");
        }
    }

    #[tokio::test]
    async fn another_provider_is_refused_with_a_storage_error() {
        let error = empty_prefix(
            StorageConfig::OneDrive {
                drive_id: "drive".into(),
                folder_id: "folder".into(),
                account_label: None,
            },
            location("http://127.0.0.1:9", "dev").1,
            Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH)),
            Arc::new(SequentialIds::new()),
            crate::providers::tests::http_client(),
        )
        .await
        .unwrap_err();

        let EmptyS3PrefixError::Storage(error) = error else {
            panic!("expected a storage error: {error:?}");
        };
        assert_eq!(error.failure(), StorageFailure::InvalidConfiguration);
    }
}

use super::*;

#[tokio::test]
async fn gcs_race_prepares_temporaries_then_sends_conditional_publication_copies() {
    let state = Arc::new(Mutex::new(Remote {
        gcs: true,
        ..Remote::default()
    }));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let s = profile_provider(
        &server.url,
        CataloguedProfile::named(S3ProfileName::Gcs).settings,
    );
    let target = path(STORE_ID, 1);
    let first = prepare(&s, &target, b"first").await.unwrap();
    let other = prepare(&s, &target, b"other").await.unwrap();
    assert!(matches!(first, Write::Copy { .. }) && matches!(other, Write::Copy { .. }));
    {
        let state = state.lock().unwrap();
        assert!(state.uploads.is_empty());
        assert_eq!(state.objects.len(), 2);
        assert!(!state.objects.contains_key(&target.under("store")));
        assert_eq!(state.copies, 0);
    }
    fire(&s.provider, &target, b"first".to_vec(), first)
        .await
        .unwrap();
    assert_eq!(
        fire(&s.provider, &target, b"other".to_vec(), other)
            .await
            .unwrap_err()
            .failure(),
        StorageFailure::AlreadyExists
    );
    assert_eq!(state.lock().unwrap().copies, 2);
    assert_eq!(s.read(&target).await.unwrap(), b"first");
}

/// On a profile publishing every object by copy, a single-request writer
/// races as that profile publishes: a staged temporary and its conditional
/// copy, never a conditional PUT, so a mixed race is copy against copy.
#[tokio::test]
async fn every_object_race_stages_single_requests_and_races_only_copies() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let s = profile_provider(
        &server.url,
        CataloguedProfile::named(S3ProfileName::SeaweedFs).settings,
    );
    let target = path(STORE_ID, 1);
    let staged = single(&s.provider, &target, b"single").await.unwrap();
    let assembled = prepare(&s, &target, b"multipart").await.unwrap();
    assert!(matches!(staged, Write::Copy { .. }) && matches!(assembled, Write::Copy { .. }));
    assert_eq!(state.lock().unwrap().copies, 0);
    assert!(s.status(&target).await.unwrap().is_none());
    fire(&s.provider, &target, b"single".to_vec(), staged)
        .await
        .unwrap();
    assert_eq!(
        fire(&s.provider, &target, b"multipart".to_vec(), assembled)
            .await
            .unwrap_err()
            .failure(),
        StorageFailure::AlreadyExists
    );
    assert_eq!(state.lock().unwrap().copies, 2);
    assert_eq!(s.read(&target).await.unwrap(), b"single");
    let plain = profile_provider(&server.url, profile::PLAIN);
    assert!(matches!(
        single(&plain.provider, &path(STORE_ID, 2), b"put")
            .await
            .unwrap(),
        Write::Put {
            header: "if-none-match",
            ..
        }
    ));
}

struct RecordingWaits {
    clock: coven_foundation::clock::SystemClock,
    waits: Mutex<Vec<std::time::Duration>>,
}

impl coven_foundation::clock::Clock for RecordingWaits {
    fn now(&self) -> SystemTime {
        self.clock.now()
    }
    fn elapsed(&self) -> std::time::Duration {
        self.clock.elapsed()
    }
    fn sleep(
        &self,
        duration: std::time::Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        self.waits.lock().unwrap().push(duration);
        Box::pin(std::future::ready(()))
    }
}

#[tokio::test]
async fn native_throttling_restarts_with_fresh_slots_and_current_floor_revisions() {
    use std::sync::atomic::{AtomicBool, Ordering};
    for (race, preparation) in [
        (Race::Multipart, true),
        (Race::Multipart, false),
        (Race::Replace, false),
    ] {
        let state = Arc::new(Mutex::new(Remote {
            gcs: true,
            ..Remote::default()
        }));
        let remote = state.clone();
        let refused = Arc::new(AtomicBool::new(false));
        let first = refused.clone();
        let publications = Arc::new(tokio::sync::Barrier::new(2));
        let server = TestServer::new(Router::new().fallback(
            move |method: Method, uri: Uri, headers: HeaderMap, body: Bytes| {
                let remote = remote.clone();
                let first = first.clone();
                let publications = publications.clone();
                async move {
                    let publication = if matches!(race, Race::Replace) {
                        method == Method::PUT
                            && headers
                                .get("x-goog-if-generation-match")
                                .is_some_and(|value| value != "0")
                    } else {
                        headers.contains_key("x-goog-copy-source")
                    };
                    // Preparation is serial; only the publication requests
                    // race. Neither may answer before both reach this fake.
                    if publication {
                        publications.wait().await;
                    }
                    let selected = if preparation {
                        method == Method::POST && query(&uri).contains_key("uploads")
                    } else {
                        publication
                    };
                    if selected && !first.swap(true, Ordering::SeqCst) {
                        let mut reply = response(429, "<Error><Code>SlowDown</Code></Error>");
                        reply
                            .headers_mut()
                            .insert("retry-after", "7".parse().unwrap());
                        reply
                    } else {
                        endpoint(State(remote), method, uri, headers, body).await
                    }
                }
            },
        ))
        .await;
        let storage = Arc::new(profile_provider(
            &server.url,
            CataloguedProfile::named(S3ProfileName::Gcs).settings,
        ));
        if matches!(race, Race::Replace) {
            storage
                .create(&ObjectPath::floor(STORE_ID), b"initial revision")
                .await
                .unwrap();
        }
        let clock = RecordingWaits {
            clock: coven_foundation::clock::SystemClock,
            waits: Mutex::new(Vec::new()),
        };
        let discarded = crate::providers::tests::sequence_live::rounds(
            &clock,
            "native fixture",
            2,
            async |trial, attempt| {
                run_race_round(&clock, storage.clone(), STORE_ID, race, 2, trial, attempt).await
            },
        )
        .await
        .unwrap();
        assert!(refused.load(Ordering::SeqCst));
        assert_eq!(discarded.throttled, 1);
        assert_eq!(discarded.refused, 0);
        assert!(clock
            .waits
            .lock()
            .unwrap()
            .contains(&std::time::Duration::from_secs(7)));
        let state = state.lock().unwrap();
        if matches!(race, Race::Replace) {
            assert_eq!(state.objects.len(), 1);
            assert!(state.objects[ObjectPath::floor(STORE_ID).as_str()]
                .starts_with(b"trial=2 attempt=3 writer="));
            assert_eq!(state.generation, 4);
        } else {
            let slots: Vec<_> = state
                .objects
                .keys()
                .filter(|key| key.contains("/seq/"))
                .collect();
            assert_eq!(slots.len(), if preparation { 2 } else { 3 });
            assert!(state.objects.contains_key(path(STORE_ID, 3).as_str()));
        }
    }
}

use super::*;
use tokio::time::Instant;

#[tokio::test]
#[ignore = "requires COVEN_LIVE_S3_ENV; creates and deletes live objects"]
async fn s3_live_listing_lag() {
    let mut settings = live_settings("COVEN_LIVE_S3_ENV");
    let mut take = |key| {
        settings
            .remove(key)
            .expect("required live S3 setting missing")
    };
    let config = StorageConfig::S3 {
        bucket: take("COVEN_LIVE_S3_BUCKET").as_str().to_owned(),
        region: take("COVEN_LIVE_S3_REGION").as_str().to_owned(),
        endpoint: Some(
            take("COVEN_LIVE_S3_ENDPOINT")
                .as_str()
                .parse()
                .expect("valid S3 endpoint"),
        ),
        prefix: "coven-live".into(),
    };
    config.validate().unwrap();
    let credentials = S3Credentials {
        access_key_id: take("COVEN_LIVE_S3_ACCESS_KEY_ID").as_str().to_owned(),
        secret_access_key: take("COVEN_LIVE_S3_SECRET_ACCESS_KEY"),
    };
    let clock: ClockRef = Arc::new(SystemClock);
    let provider = S3Storage::new(config, credentials, clock.clone(), Arc::new(UuidIds)).unwrap();
    let storage = Arc::new(StorageConnection::from_provider(Arc::new(provider)));
    let store = StoreId(UuidIds.new_id());
    // Reserve every owned name before any request, so a lost reply cannot leak it.
    let mut paths: Vec<_> = (1..=TRIALS + 1)
        .map(|number| ObjectPath::store_log(store, DeviceId(31), number.try_into().unwrap()))
        .collect();
    paths.push(ObjectPath::store_log(
        store,
        DeviceId(32),
        std::num::NonZeroU64::MIN,
    ));
    let work = storage.clone();
    let owned = paths.clone();
    let outcome = tokio::spawn(async move {
        let prefix = ObjectPrefix::store_log(store, DeviceId(31));
        let mut samples = Vec::new();
        for (trial, path) in owned.iter().enumerate() {
            work.create(path, b"listing visibility").await.unwrap();
            let replied = Instant::now();
            let wall = clock.now();
            let stored_at = stored_time(&work.provider, path).await.unwrap();
            println!("listing_lag S3 create_reply={wall:?} stored_at={stored_at:?}");
            if trial == owned.len() - 1 {
                let all = ObjectPrefix::store_logs(store);
                let (object, discovery) = tokio::join!(
                    observe("S3_new_device_object", 1, replied, stored_at, || async {
                        Ok(work
                            .list(&all)
                            .await?
                            .iter()
                            .any(|object| &object.path == path))
                    }),
                    observe("S3_new_device_folder", 1, replied, stored_at, || async {
                        device_listed(&work.provider, store, DeviceId(32)).await
                    }),
                );
                report("S3_new_device_object", &[object]);
                report("S3_new_device_folder", &[discovery]);
            } else {
                let label = if trial == 0 { "S3_setup" } else { "S3_object" };
                let sample = observe(label, trial as u64, replied, stored_at, || async {
                    Ok(work
                        .list(&prefix)
                        .await?
                        .iter()
                        .any(|object| &object.path == path))
                })
                .await;
                if trial == 0 {
                    report(label, &[sample]);
                } else {
                    samples.push(sample);
                    work.delete(path).await.unwrap();
                }
            }
        }
        report("S3_object", &samples);
    })
    .await;
    let mut failures = Vec::new();
    for path in &paths {
        if let Err(error) = storage.delete(path).await {
            failures.push(error);
        }
        match stored_time(&storage.provider, path).await {
            Err(error) if error.failure() == StorageFailure::NotFound => {}
            Err(error) => failures.push(error),
            Ok(_) => failures
                .push(StorageFailure::Protocol.with_source("live S3 object survived deletion")),
        }
    }
    assert!(failures.is_empty(), "S3 cleanup failures: {failures:?}");
    println!("listing_lag S3 cleanup: all reserved objects deleted and confirmed absent");
    if let Err(error) = outcome {
        if error.is_panic() {
            std::panic::resume_unwind(error.into_panic());
        }
        panic!("S3 live test cancelled: {error}");
    }
}

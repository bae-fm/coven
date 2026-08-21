use super::*;

async fn join_eager_fixture(fixture: &FacadeFixture) -> coven_foundation::config::Config {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind pairing listener");
    let endpoint = listener.local_addr().expect("pairing endpoint");
    let pairing_key = crate::UserKeypair::generate();
    let offer = crate::DevicePairingOffer::new(
        &pairing_key,
        vec![endpoint],
        "Facade Join Store".to_string(),
        crate::CloudProvider::S3,
        1_900_000_000,
    )
    .expect("pairing offer");
    let pairing_journal = tempfile::tempdir().expect("pairing journal directory");
    let host = crate::DevicePairingHost::start(
        listener,
        offer.clone(),
        pairing_key,
        pairing_journal.path().join("pairing.json"),
        Arc::new(crate::SystemClock),
    )
    .await
    .expect("start pairing host");
    let pairing =
        crate::PreparedDevicePairing::open_or_create(&offer.encode(), None, &fixture.layout)
            .expect("prepare joining identity from the scanned code");
    let (_cancel_tx, cancel) = tokio::sync::watch::channel(false);
    let joining = coven_domain::joining::join_with_device_pairing_over_test_home(
        &pairing,
        fixture.layout.clone(),
        fixture.tables.clone(),
        test_migrations(),
        Arc::new(crate::SystemClock),
        fixture.home.clone(),
        timing(),
        Arc::new(|_| {}),
        &cancel,
    );
    let admitting = async {
        let request = host
            .wait_for_request()
            .await
            .expect("receive signed request");
        fixture
            .handle
            .approve_device_pairing(
                &host,
                &request,
                crate::MemberRole::Member,
                crate::DeviceJoinApprovalPolicy::AutoApproveSelfIssued,
                None,
                &|_| {},
                tokio::sync::watch::channel(false).1,
            )
            .await
    };
    let (joined, admitted) = tokio::join!(Box::pin(joining), Box::pin(admitting));
    assert!(matches!(
        admitted.expect("existing device completes pairing"),
        crate::DeviceJoinDriveOutcome::Activated(_)
    ));
    match joined.expect("joining device completes") {
        crate::DeviceJoinTransportOutcome::Joined(config) => config,
        outcome => panic!("joining device did not install the library: {outcome:?}"),
    }
}

/// Joining installs the row snapshot and returns before any CacheEager artwork
/// is fetched. Opening the joined Store starts that cache policy in the
/// background and exposes one retained status stream through completion.
#[test]
fn eager_artwork_fills_only_after_the_joined_library_opens() {
    on_a_deep_stack(run_eager_artwork_fills_only_after_the_joined_library_opens);
}

async fn run_eager_artwork_fills_only_after_the_joined_library_opens() {
    let store_id = "facade-post-open-eager-cache";
    let fixture = FacadeFixture::build_with_eager_images(store_id, 2).await;
    let config = join_eager_fixture(&fixture).await;
    fixture
        .memory_home
        .as_ref()
        .expect("in-memory eager cache fixture")
        .stream_exact_reads_in_chunks(4 * 1024, Duration::from_millis(50));

    let joined_store_dir = fixture.layout.store_dir(store_id);
    let joined_handle = crate::Coven::builder(joined_store_dir.clone(), config)
        .synced_tables(fixture.tables.clone())
        .migrations(test_migrations())
        .open()
        .expect("open joined library");
    let joined_images = joined_handle
        .read(|sql| {
            sql.query_row("SELECT count(*) FROM note_photos", [], |row| {
                row.get::<_, i64>(0)
            })
            .map_err(crate::CovenError::from)
        })
        .await
        .expect("count joined image rows");
    assert_eq!(
        joined_images, 2,
        "snapshot installs image rows before opening"
    );
    let mut expected_bytes_total = 0_u64;
    for index in 0..2 {
        let reference = joined_handle
            .row_blob_ref("note_photos", &format!("image{index:04}"))
            .await
            .expect("read joined image reference before cache fill");
        assert!(
            matches!(reference.authority(), crate::RowBlobAuthority::Remote(_)),
            "snapshot image is remote before cache fill: {reference:?}",
        );
        expected_bytes_total += reference
            .stored()
            .expect("remote image has an exact stored reference")
            .object()
            .stored_size();
    }
    let mut fill = joined_handle.subscribe_eager_cache_fill_status();
    let fill_started = Instant::now();
    let mut downloading_reports = Vec::new();
    joined_handle
        .connect_sync_with_test_home(
            fixture.home.clone(),
            coven_storage::CloudCipher::Encrypted(fixture.encryption.clone()),
        )
        .await
        .expect("connect joined library");

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let current = fill.borrow_and_update().clone();
            match current {
                crate::EagerCacheFillStatus::Downloading(progress) => {
                    if downloading_reports
                        .last()
                        .is_none_or(|(_, previous)| previous != &progress)
                    {
                        downloading_reports.push((fill_started.elapsed(), progress));
                    }
                    fill.changed()
                        .await
                        .expect("cache fill status remains open");
                }
                crate::EagerCacheFillStatus::Complete {
                    files_total,
                    bytes_total,
                } => {
                    assert_eq!(files_total, 2);
                    assert_eq!(bytes_total, expected_bytes_total);
                    break;
                }
                crate::EagerCacheFillStatus::Failed { error, .. } => {
                    panic!("post-open eager cache fill failed: {error}")
                }
                _ => fill
                    .changed()
                    .await
                    .expect("cache fill status remains open"),
            }
        }
    })
    .await
    .expect("post-open eager cache fill completes");
    assert!(
        downloading_reports.len() >= 2,
        "streaming cache fill must expose intermediate progress: {downloading_reports:?}",
    );
    for reports in downloading_reports.windows(2) {
        assert!(
            reports[1].0.saturating_sub(reports[0].0) >= Duration::from_millis(250),
            "non-terminal cache progress exceeded the 300 ms cadence: {downloading_reports:?}",
        );
    }

    for index in 0..2 {
        let reference = joined_handle
            .row_blob_ref("note_photos", &format!("image{index:04}"))
            .await
            .expect("read joined image reference");
        let stored = reference.stored().expect("joined image is remote");
        let path = joined_store_dir
            .cache_blob_path(
                stored.locator().namespace(),
                stored.locator().locator_hash(),
            )
            .expect("joined image cache path");
        assert!(path.is_file(), "eager image is materialized after open");
    }
}

/// Stopping the connected library cancels an in-flight CacheEager fill and
/// retains its exact partial counters so the host can render what stopped.
#[test]
fn stopping_sync_cancels_the_post_open_eager_fill() {
    on_a_deep_stack(run_stopping_sync_cancels_the_post_open_eager_fill);
}

/// Cancelling artwork loading leaves the library's sync connection running and
/// retains the exact partial counters at which only the cache fill stopped.
#[test]
fn eager_artwork_can_be_cancelled_without_stopping_sync() {
    on_a_deep_stack(run_eager_artwork_can_be_cancelled_without_stopping_sync);
}

async fn run_eager_artwork_can_be_cancelled_without_stopping_sync() {
    let store_id = "facade-cancel-only-eager-cache";
    let fixture = FacadeFixture::build_with_eager_images(store_id, 2).await;
    let config = join_eager_fixture(&fixture).await;
    fixture
        .memory_home
        .as_ref()
        .expect("in-memory eager cache fixture")
        .stream_exact_reads_in_chunks(4 * 1024, Duration::from_millis(50));

    let joined_handle = crate::Coven::builder(fixture.layout.store_dir(store_id), config)
        .synced_tables(fixture.tables.clone())
        .migrations(test_migrations())
        .open()
        .expect("open joined library");
    let mut fill = joined_handle.subscribe_eager_cache_fill_status();
    joined_handle
        .connect_sync_with_test_home(
            fixture.home.clone(),
            coven_storage::CloudCipher::Encrypted(fixture.encryption.clone()),
        )
        .await
        .expect("connect joined library");

    let downloading = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let crate::EagerCacheFillStatus::Downloading(progress) = fill.borrow().clone() {
                if progress.bytes_done > 0 {
                    break progress;
                }
            }
            fill.changed()
                .await
                .expect("cache fill status remains open");
        }
    })
    .await
    .expect("eager fill reports streamed bytes");

    joined_handle.cancel_eager_cache_fill();
    fill.changed()
        .await
        .expect("cache fill cancellation remains observable");
    let cancelled = fill.borrow().clone();
    assert!(
        matches!(
            cancelled,
            crate::EagerCacheFillStatus::Cancelled(progress)
                if progress.bytes_done >= downloading.bytes_done
                    && progress.bytes_done < progress.bytes_total
        ),
        "explicit cancellation must retain eager-fill progress, got {cancelled:?}",
    );
    assert!(
        joined_handle.is_syncing(),
        "cache cancellation stopped sync"
    );
}

async fn run_stopping_sync_cancels_the_post_open_eager_fill() {
    let store_id = "facade-cancel-eager-cache";
    let fixture = FacadeFixture::build_with_eager_images(store_id, 2).await;
    let config = join_eager_fixture(&fixture).await;
    fixture
        .memory_home
        .as_ref()
        .expect("in-memory eager cache fixture")
        .stream_exact_reads_in_chunks(4 * 1024, Duration::from_millis(50));

    let joined_handle = crate::Coven::builder(fixture.layout.store_dir(store_id), config)
        .synced_tables(fixture.tables.clone())
        .migrations(test_migrations())
        .open()
        .expect("open joined library");
    let mut fill = joined_handle.subscribe_eager_cache_fill_status();
    joined_handle
        .connect_sync_with_test_home(
            fixture.home.clone(),
            coven_storage::CloudCipher::Encrypted(fixture.encryption.clone()),
        )
        .await
        .expect("connect joined library");

    let downloading = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let crate::EagerCacheFillStatus::Downloading(progress) = fill.borrow().clone() {
                if progress.bytes_done > 0 {
                    break progress;
                }
            }
            fill.changed()
                .await
                .expect("cache fill status remains open");
        }
    })
    .await
    .expect("eager fill reports streamed bytes");
    assert!(downloading.bytes_done < downloading.bytes_total);

    joined_handle.stop_sync();
    let cancelled = fill.borrow().clone();
    assert!(
        matches!(
            cancelled,
            crate::EagerCacheFillStatus::Cancelled(progress)
                if progress.bytes_done >= downloading.bytes_done
                    && progress.bytes_done < progress.bytes_total
        ),
        "stopping sync must retain the cancelled eager-fill progress, got {cancelled:?}",
    );
}

/// One more eager image on the owner, published after a joining device is
/// already open and synchronised.
async fn publish_one_more_eager_image(handle: &crate::CovenHandle, index: usize) {
    const IMAGE_BYTES: usize = 64 * 1024;

    let id = format!("image{index:04}");
    let bytes = vec![u8::try_from(index).expect("image index fits u8"); IMAGE_BYTES];
    let hash = coven_protocol::blob::content_hash(&bytes);
    let blob_id = id.clone();
    let blob_bytes = bytes.clone();
    handle
        .write_with_blobs(
            move |batch| {
                batch.put_blob("photos", blob_id, blob_bytes);
                Ok(())
            },
            move |sql| {
                let stamp = sql.stamp().to_string();
                sql.execute(
                    "INSERT INTO note_photos
                     (id, note_id, kind, size, hash, _updated_at, created_at)
                     VALUES (?1, 'image-root', 'cover', ?2, ?3, ?4, '2026-01-01')",
                    rusqlite::params![id, bytes.len() as i64, hash, stamp],
                )?;
                Ok(())
            },
        )
        .await
        .expect("write one more eager image");
    // The root is already Remote, so the new child row is published with it.
    wait_for_initial_sync(handle).await;
}

/// Artwork that arrives *after* a device is open still becomes local bytes.
///
/// A pull records what its rows bind and downloads none of it, so the arriving
/// row alone would leave the album with no cover. The eager cache fill re-scans
/// whenever a cycle materializes rows, which is what carries the cover across —
/// behind the cycle, which never waits for it.
#[test]
fn eager_artwork_arriving_after_open_still_fills() {
    on_a_deep_stack(run_eager_artwork_arriving_after_open_still_fills);
}

async fn run_eager_artwork_arriving_after_open_still_fills() {
    let store_id = "facade-late-eager-artwork";
    let fixture = FacadeFixture::build_with_eager_images(store_id, 1).await;
    let config = join_eager_fixture(&fixture).await;

    let joined_store_dir = fixture.layout.store_dir(store_id);
    let joined_handle = crate::Coven::builder(joined_store_dir.clone(), config)
        .synced_tables(fixture.tables.clone())
        .migrations(test_migrations())
        .open()
        .expect("open joined library");
    let mut fill = joined_handle.subscribe_eager_cache_fill_status();
    joined_handle
        .connect_sync_with_test_home(
            fixture.home.clone(),
            coven_storage::CloudCipher::Encrypted(fixture.encryption.clone()),
        )
        .await
        .expect("connect joined library");

    await_eager_fill(&mut fill).await;

    // The cover the joined device has never seen.
    publish_one_more_eager_image(&fixture.handle, 1).await;
    joined_handle.sync_now();

    let late_cover = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            if let Ok(reference) = joined_handle.row_blob_ref("note_photos", "image0001").await {
                if let Some(stored) = reference.stored() {
                    let path = joined_store_dir
                        .cache_blob_path(
                            stored.locator().namespace(),
                            stored.locator().locator_hash(),
                        )
                        .expect("late image cache path");
                    if path.is_file() {
                        return path;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("artwork that arrived after open is materialized locally");
    assert!(late_cover.is_file());
}

/// Wait for a cache fill pass to report complete.
async fn await_eager_fill(fill: &mut tokio::sync::watch::Receiver<crate::EagerCacheFillStatus>) {
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let current = fill.borrow_and_update().clone();
            match current {
                crate::EagerCacheFillStatus::Complete { .. } => return,
                crate::EagerCacheFillStatus::Failed { error, .. } => {
                    panic!("eager cache fill failed: {error}")
                }
                _ => fill
                    .changed()
                    .await
                    .expect("cache fill status remains open"),
            }
        }
    })
    .await
    .expect("eager cache fill did not complete");
}

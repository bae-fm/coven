use super::*;

/// What a restore reports, in order.
async fn restore_statuses(owner: &Owner) -> Vec<BootstrapStatus> {
    let install = Installation::new();
    let (_, cancel) = watch::channel(false);
    let reported = std::sync::Mutex::new(Vec::new());
    let handle = install
        .run(
            owner,
            Installation::restore_request(owner),
            owner.storage.clone(),
            &cancel,
            |status| reported.lock().unwrap().push(status),
        )
        .await
        .unwrap()
        .unwrap();
    handle.close().await.unwrap();
    reported.into_inner().unwrap()
}

/// A restore opens its new installation first, running its migration and
/// reporting it, and only then connects (E10).
#[tokio::test]
async fn a_restore_opens_its_new_installation_before_connecting() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let reported = restore_statuses(&owner).await;
    assert_eq!(
        after_opening(&reported).first(),
        Some(&BootstrapStatus::Connecting),
        "{reported:?}"
    );
    owner.close().await;
}

/// Restoring Ana's store from its floor reports loading's steps as
/// `Loading`, in order, from streaming the store's snapshot to applying the
/// last slot after it; keeping keys and credentials follows (E10).
#[tokio::test]
async fn a_restore_reports_loading_from_the_snapshot_to_the_last_slot() {
    let owner = Owner::new(CloudProvider::S3, true).await;
    let reported = restore_statuses(&owner).await;
    let reported = after_opening(&reported);
    assert_eq!(reported.first(), Some(&BootstrapStatus::Connecting));
    let loading: Vec<_> = reported[1..]
        .iter()
        .map(|status| match status {
            BootstrapStatus::Loading(progress) => progress.clone(),
            other => panic!("{other:?} while loading: {reported:?}"),
        })
        .collect();
    assert_honest(reported);
    assert!(
        loading.iter().any(|progress| {
            progress.step == LoadStep::Streaming(Audience::Store) && progress.done == progress.total
        }),
        "{reported:?}"
    );
    let last = loading.last().expect("loading reports");
    assert_eq!(last.step, LoadStep::Applying, "{reported:?}");
    assert_eq!(last.done, last.total, "{reported:?}");
    owner.close().await;
}

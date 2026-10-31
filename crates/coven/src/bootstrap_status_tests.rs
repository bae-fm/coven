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

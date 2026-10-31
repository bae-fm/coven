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

/// Carol's join stops part way through loading: reading slot 2 fails after
/// her phone applied slot 1. Her next call opens her keys again, and that
/// check reports reading and applying the slots left, to the last (E10).
#[tokio::test]
async fn a_resumed_join_reports_the_rest_of_its_loading() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let (_, cancel) = watch::channel(false);
    let slot = ObjectPath::slot(owner.directory.id(), 2.try_into().unwrap());
    owner
        .storage
        .set_faults(Faults {
            read_failures: [(slot, StorageFailure::PermissionDenied)].into(),
            ..Faults::none()
        })
        .await;
    let joining = install.run(
        &owner,
        Installation::join_request(&invite),
        owner.recipient(),
        &cancel,
        |_| {},
    );
    let (result, _) = join_and_approve(&owner.handle, joining, |_| async {}).await;
    assert!(matches!(result, Err(BootstrapError::Sync(SyncError::Storage(_)))), "{result:?}");
    owner.storage.set_faults(Faults::none()).await;
    let reported = std::sync::Mutex::new(Vec::new());
    let handle = install
        .run(&owner, Installation::join_request(&invite), owner.recipient(), &cancel, |status| {
            reported.lock().unwrap().push(status)
        })
        .await
        .unwrap()
        .expect("the approved join loads");
    handle.close().await.unwrap();
    let reported = reported.into_inner().unwrap();
    assert_honest(&reported);
    let loading: Vec<_> = reported
        .iter()
        .filter_map(|status| match status {
            BootstrapStatus::Loading(progress) => Some(progress),
            _ => None,
        })
        .collect();
    // Slot 1 was applied before the join stopped; the rest are read. A
    // host may not see Reading end before Applying replaces it.
    assert!(
        loading.iter().any(|progress| progress.step == LoadStep::Reading && progress.total > 0),
        "{reported:?}"
    );
    let last = loading.last().unwrap();
    assert_eq!(last.step, LoadStep::Applying, "{reported:?}");
    assert_eq!(last.done, last.total, "{reported:?}");
    owner.close().await;
}

/// Ana's store has no floor, so no snapshot holds her copy: restoring reads
/// every slot for it, and reports that as loading's first step, its total
/// found as it begins, before reading and applying the slots (E10).
#[tokio::test]
async fn a_restore_without_a_floor_reports_finding_its_keys_first() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let reported = restore_statuses(&owner).await;
    let reported = after_opening(&reported);
    assert_honest(reported);
    assert_eq!(reported.first(), Some(&BootstrapStatus::Connecting));
    assert!(
        matches!(reported.get(1), Some(BootstrapStatus::Loading(progress))
            if progress.step == LoadStep::FindingKeys
                && progress.done == 0
                && progress.total == owner.applied().await),
        "{reported:?}"
    );
    owner.close().await;
}

/// Carol's phone scans the slots after her invite for her copies in each
/// check for approval. The check after Ana approves finds them, reports the
/// scan as loading's first step and then loads the store (E10).
#[tokio::test]
async fn a_join_reports_finding_its_keys_before_it_loads() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let (_, cancel) = watch::channel(false);
    let reported = std::sync::Mutex::new(Vec::new());
    let joining = install.run(
        &owner,
        Installation::join_request(&invite),
        owner.recipient(),
        &cancel,
        |status| reported.lock().unwrap().push(status),
    );
    let (result, _) = join_and_approve(&owner.handle, joining, |_| async {}).await;
    result.unwrap().unwrap().close().await.unwrap();
    let reported = reported.into_inner().unwrap();
    assert_honest(&reported);
    let loading: Vec<_> = reported
        .iter()
        .skip_while(|status| !matches!(status, BootstrapStatus::Loading(_)))
        .collect();
    // The scan may end before the app hears it begin.
    assert!(
        matches!(loading.first(), Some(BootstrapStatus::Loading(progress))
            if progress.step == LoadStep::FindingKeys && progress.total > 0),
        "{reported:?}"
    );
    assert!(
        loading.iter().all(|status| matches!(status, BootstrapStatus::Loading(_))),
        "{reported:?}"
    );
    owner.close().await;
}

//! The store a join publishes once it finds its admission: it loads as a
//! store that has never loaded, and from then on it is a store like any
//! other, which loses access, stops and is removed as any store does (§13.2).

use super::*;

/// Carol's join is approved, then stops while the store loads, as when the
/// app is closed. It published Household as it found the admission, so no
/// join is listed: the app opens the store, and its next pass loads the rest
/// (§13.2).
#[tokio::test]
async fn an_approved_join_stopped_while_loading_is_a_store_its_next_pass_loads() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let store = owner.directory.id();
    let (_, cancel) = watch::channel(false);
    let (loading, mut loaded) = watch::channel(false);
    let control = JoinControl::new();
    let joining = install.run(
        &owner,
        Installation::join_request(&invite, &control),
        owner.recipient(),
        &cancel,
        |status| {
            if matches!(status, BootstrapStatus::Loading(progress)
                if progress.step == LoadStep::Applying && progress.done == 0)
            {
                loading.send_replace(true);
            }
        },
    );
    let stopped = async {
        tokio::select! {
            result = joining => panic!("the join finished before it stopped: {result:?}"),
            changed = loaded.wait_for(|loading| *loading) => drop(changed.unwrap()),
        }
    };
    let ((), member) = join_with_response(stopped, owner.approve_join(&control)).await;
    assert!(install
        .builder(&owner, owner.recipient())
        .unfinished_joins()
        .await
        .unwrap()
        .is_empty());
    let handle = install.builder(&owner, owner.recipient()).open(store, |_| {}).await.unwrap();
    handle.start_sync().await.unwrap();
    let answer = handle.retry_now().await.unwrap().await;
    assert!(matches!(answer, RetryOutcome::Synced { .. }), "{answer:?}");
    assert!(handle
        .members()
        .await
        .unwrap()
        .iter()
        .any(|found| found.is_self && found.id == member));
    handle.close().await.unwrap();
    owner.close().await;
}

/// Carol's admission lands, and her storage access is taken away before her
/// phone has downloaded anything: it can no longer read slot 1. Household is
/// a store on her phone like any other, not a join: the call returns it
/// with the provider's refusal, nothing lists a join, its sync says it has
/// no access and retries, and the app removes it as any store (§13.2).
#[tokio::test]
async fn an_admitted_join_that_loses_storage_access_before_loading_is_a_store() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let store = owner.directory.id();
    let (_, cancel) = watch::channel(false);
    let control = JoinControl::new();
    let joining = install.run(
        &owner,
        Installation::join_request(&invite, &control),
        owner.recipient(),
        &cancel,
        |_| {},
    );
    let (result, member) = owner
        .join_and_approve(joining, &control, |_| async {
            owner
                .storage
                .set_faults(coven_storage::test_utils::Faults {
                    read_failures: [(
                        ObjectPath::slot(store, 1.try_into().unwrap()),
                        StorageFailure::PermissionDenied,
                    )]
                    .into(),
                    ..coven_storage::test_utils::Faults::none()
                })
                .await;
        })
        .await;
    let name = owner.handle.store_name().await.unwrap();
    let handle = match result {
        Err(BootstrapError::Published { handle, source }) => {
            assert!(
                matches!(&*source, BootstrapError::Sync(SyncError::Storage(error))
                    if error.failure() == StorageFailure::PermissionDenied),
                "{source:?}"
            );
            handle
        }
        result => panic!("the admitted join is a store that couldn't load: {result:?}"),
    };
    assert!(install
        .builder(&owner, owner.recipient())
        .unfinished_joins()
        .await
        .unwrap()
        .is_empty());
    assert!(saved_code(&install, store).is_none());
    handle.start_sync().await.unwrap();
    let answer = handle.retry_now().await.unwrap().await;
    assert!(matches!(answer, RetryOutcome::Failed(_)), "{answer:?}");
    let state = handle.sync_status().await.unwrap().state;
    assert!(matches!(state, SyncState::NoAccess(_)), "{state:?}");
    // Until it loads, the store is listed by the name its invite carried.
    assert_eq!(handle.store_name().await.unwrap(), name);
    handle.close().await.unwrap();
    let listed = install.app.stores().await.unwrap();
    assert!(
        matches!(&listed[..], [ListedStore::Readable(info)]
            if info.id == store && info.name == name && info.unfinished.is_none()),
        "{listed:?}"
    );
    // Access comes back: the store's next pass loads it, its admission
    // among the slots it applies.
    owner.storage.set_faults(coven_storage::test_utils::Faults::none()).await;
    let handle = install.builder(&owner, owner.recipient()).open(store, |_| {}).await.unwrap();
    handle.start_sync().await.unwrap();
    let answer = handle.retry_now().await.unwrap().await;
    assert!(matches!(answer, RetryOutcome::Synced { .. }), "{answer:?}");
    assert!(handle
        .members()
        .await
        .unwrap()
        .iter()
        .any(|found| found.is_self && found.id == member));
    handle.close().await.unwrap();
    install.app.discard_store(&install.app.store_dir(&store)).await.unwrap();
    assert!(install.app.stores().await.unwrap().is_empty());
    owner.close().await;
}

/// A cancel once Carol's join has found its admission changes nothing: the
/// admission registered her phone, so nothing is left for the cancel to take
/// back, and the call publishes Household and loads it (§13.2).
#[tokio::test]
async fn cancelling_an_admitted_join_still_publishes_its_store() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let (stop, cancel) = watch::channel(false);
    let control = JoinControl::new();
    let joining = install.run(
        &owner,
        Installation::join_request(&invite, &control),
        owner.recipient(),
        &cancel,
        cancel_at(Phase::Loading, &stop),
    );
    let (result, member) = owner.join_and_approve(joining, &control, |_| async {}).await;
    let handle = result.unwrap().expect("the admitted join publishes its store");
    assert!(*cancel.borrow(), "the cancel came while the join loaded");
    assert!(handle
        .members()
        .await
        .unwrap()
        .iter()
        .any(|found| found.is_self && found.id == member));
    handle.close().await.unwrap();
    assert!(install
        .builder(&owner, owner.recipient())
        .unfinished_joins()
        .await
        .unwrap()
        .is_empty());
    owner.close().await;
}

/// Carol's phone finds its admission, and Ana removes her and the provider
/// takes back her account's access before the phone has loaded anything.
/// Household is a store on the phone, whose first load the provider refuses:
/// the call returns it with the refusal, and no join is listed (§13.2).
#[tokio::test]
async fn a_member_removed_while_joining_has_a_store_the_provider_refuses() {
    let owner = Owner::new(CloudProvider::OneDrive, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let (_, cancel) = watch::channel(false);
    let (read, reading) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    let (sleeping, mut asleep) = watch::channel(false);
    let (check, checking) = watch::channel(false);
    // The joiner's next check waits until approval has published both slots.
    let clock = Arc::new(JoinClock { clock: owner.clock.clone(), sleeping, resume: checking });
    // Approval claims the store key's copy and then the binding.
    let after = owner.applied().await + 3;
    let joining = async {
        Box::pin(bootstrap_device(
            install.authenticated_builder(&owner, owner.recipient()).await.clock(clock),
            Installation::join_request(&invite, &JoinControl::new()),
            |_| {},
            &cancel,
        ))
        .await
    };
    let remove = async {
        // The joiner sleeps in its check wait once its request is made.
        asleep.wait_for(|sleeping| *sleeping).await.unwrap();
        let request = owner.next_join_request().await;
        crate::tests::approve(&owner.handle, &request).await;
        // The owner's own passes read that slot too: only the joiner's read
        // is held.
        owner.stop_sync().await;
        // Finding the copy and the binding, the check's key scan reads on
        // and ends on this slot's miss. A read ahead of a slot not yet
        // published could be dropped with its pass; this one ends the scan.
        owner
            .storage
            .hold_next_read(
                ObjectPath::slot(owner.directory.id(), after.try_into().unwrap()),
                read,
                resumed,
            )
            .await;
        check.send(true).unwrap();
        reading.await.unwrap();
        // Removal sends through the connection a pass makes.
        owner.pass().await;
        let removal = owner.handle.remove_member(&request.member).await.unwrap();
        crate::tests::landed(&owner.handle, removal).await;
        // Cut off this account in the fake to model provider refusal after removal.
        owner.storage.revoke_access("join@example.com").await.unwrap();
        resume.send(()).unwrap();
    };
    let (result, ()) = join_with_response(joining, remove).await;
    let handle = match result {
        Err(BootstrapError::Published { handle, source })
            if matches!(&*source, BootstrapError::Sync(SyncError::Storage(error))
                if error.failure() == StorageFailure::PermissionDenied) =>
        {
            handle
        }
        result => panic!("the admitted join is a store its provider refuses: {result:?}"),
    };
    handle.close().await.unwrap();
    assert!(install
        .builder(&owner, owner.recipient())
        .unfinished_joins()
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        install.app.stores().await.unwrap().iter().map(ListedStore::id).collect::<Vec<_>>(),
        [owner.directory.id()]
    );
    owner.close().await;
}

/// Carol's join is approved and publishes Household, then stops while the
/// store loads, and Ana removes her before Carol's app opens it again. The
/// store's next pass applies the removal, which stops it for good as any
/// store's removal does, and the app removes it (§13.2, §14.1).
#[tokio::test]
async fn an_admitted_store_whose_member_is_removed_before_it_loads_stops() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let store = owner.directory.id();
    let (_, cancel) = watch::channel(false);
    let (loading, mut loaded) = watch::channel(false);
    let control = JoinControl::new();
    let joining = install.run(
        &owner,
        Installation::join_request(&invite, &control),
        owner.recipient(),
        &cancel,
        |status| {
            if matches!(status, BootstrapStatus::Loading(progress)
                if progress.step == LoadStep::Applying && progress.done == 0)
            {
                loading.send_replace(true);
            }
        },
    );
    let stopped = async {
        tokio::select! {
            result = joining => panic!("the join finished before it stopped: {result:?}"),
            changed = loaded.wait_for(|loading| *loading) => drop(changed.unwrap()),
        }
    };
    let ((), member) = join_with_response(stopped, owner.approve_join(&control)).await;
    crate::tests::landed(&owner.handle, owner.handle.remove_member(&member).await.unwrap()).await;
    assert!(install
        .builder(&owner, owner.recipient())
        .unfinished_joins()
        .await
        .unwrap()
        .is_empty());
    let handle = install.builder(&owner, owner.recipient()).open(store, |_| {}).await.unwrap();
    handle.start_sync().await.unwrap();
    let answer = handle.retry_now().await.unwrap().await;
    assert!(matches!(answer, RetryOutcome::Failed(_)), "{answer:?}");
    let state = handle.sync_status().await.unwrap().state;
    assert!(matches!(state, SyncState::Removed(_)), "{state:?}");
    handle.close().await.unwrap();
    install.app.discard_store(&install.app.store_dir(&store)).await.unwrap();
    assert!(saved_code(&install, store).is_none());
    install.absent(store).await;
    owner.close().await;
}

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

/// Carol's join publishes Household, and the store's first load fails: her
/// storage access is taken away before it streams the floor's snapshot. Her
/// registration hasn't applied, so no member is listed, no profile is written
/// and her app's joining write hasn't run. The load that runs once access is
/// back applies her admission, and her profile is written in that
/// transaction, from the name her request carried, with the circle her
/// admission made (§13.2).
#[tokio::test]
async fn a_joined_stores_profile_is_written_as_its_registration_applies() {
    let owner = Owner::new(CloudProvider::S3, true).await;
    let invite = owner.invite().await;
    let install = Installation::joining();
    let store = owner.directory.id();
    let snapshot = crate::tests::snapshots(&owner.storage, store).await.remove(0).path;
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
                    read_failures: [(snapshot, StorageFailure::PermissionDenied)].into(),
                    ..coven_storage::test_utils::Faults::none()
                })
                .await;
        })
        .await;
    let Err(BootstrapError::Published { handle, source }) = result else {
        panic!("the admitted join is a store that couldn't load: {result:?}");
    };
    assert!(
        matches!(&*source, BootstrapError::Sync(SyncError::Storage(error))
            if error.failure() == StorageFailure::PermissionDenied),
        "{source:?}"
    );
    assert!(saved_code(&install, store).is_none());
    let registered = |handle: CovenHandle| async move {
        let members = handle.members().await.unwrap();
        handle.close().await.unwrap();
        members.iter().any(|found| found.is_self)
    };
    assert!(joining_runs(&handle).await.is_empty());
    assert!(profiles(&handle).await.is_empty());
    assert!(!registered(handle).await);
    owner.storage.set_faults(coven_storage::test_utils::Faults::none()).await;
    let handle = install.builder(&owner, owner.recipient()).open(store, |_| {}).await.unwrap();
    assert!(joining_runs(&handle).await.is_empty());
    // The load installs the floor's snapshot and applies the slots after it.
    handle.start_sync().await.unwrap();
    let answer = handle.retry_now().await.unwrap().await;
    assert!(matches!(answer, RetryOutcome::Synced { .. }), "{answer:?}");
    let circle = handle
        .circles()
        .list()
        .await
        .unwrap()
        .iter()
        .find(|circle| circle.name == crate::tests::CIRCLE)
        .expect("the admission made the circle her request named")
        .id
        .to_string();
    let carol = member.to_string();
    assert_eq!(
        joining_runs(&handle).await,
        [(carol.clone(), Some("Carol".to_owned()), circle.clone())]
    );
    assert_eq!(profiles(&handle).await, [(carol.clone(), "Carol".to_owned(), circle.clone())]);
    // The write was made as the registration applied: Ana accepts it, which
    // she does only for a write whose author's device was registered at the
    // slot it read through.
    owner.pass_now().await;
    handle.retry_now().await.unwrap().await;
    owner.pass_now().await;
    assert_eq!(profiles(&owner.handle).await, [(carol, "Carol".to_owned(), circle)]);
    handle.close().await.unwrap();
    owner.close().await;
}

/// Carol's app's joining write fails, as a bug of the app's would: the load
/// that applies her admission fails with it, and nothing of the admission
/// commits, her registration included. Once the app's write works, the next
/// pass commits the registration and the profile together (§13.2).
#[tokio::test]
async fn a_failing_joining_write_stops_the_load_and_the_next_pass_commits_both() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let install = Installation::joining();
    install.joining_fails.store(true, std::sync::atomic::Ordering::SeqCst);
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
    let (result, _) = owner.join_and_approve(joining, &control, |_| async {}).await;
    let Err(BootstrapError::Published { handle, .. }) = result else {
        panic!("the admitted join is a store whose load failed: {:?}", result.err());
    };
    // The slots before the admission applied; the admission and its write did not.
    assert!(handle.members().await.unwrap().iter().all(|found| !found.is_self));
    assert!(profiles(&handle).await.is_empty());
    assert!(joining_runs(&handle).await.is_empty());
    handle.close().await.unwrap();
    install.joining_fails.store(false, std::sync::atomic::Ordering::SeqCst);
    let handle = install.builder(&owner, owner.recipient()).open(store, |_| {}).await.unwrap();
    handle.start_sync().await.unwrap();
    let answer = handle.retry_now().await.unwrap().await;
    assert!(matches!(answer, RetryOutcome::Synced { .. }), "{answer:?}");
    assert!(handle.members().await.unwrap().iter().any(|found| found.is_self));
    assert_eq!(profiles(&handle).await.len(), 1);
    assert_eq!(joining_runs(&handle).await.len(), 1);
    handle.close().await.unwrap();
    owner.close().await;
}

/// Carol discards Household and joins it again with a recovery invite, which
/// names no one: the recovery's admission carries her new circle and no
/// name, so the joining write runs with the circle and no person (§13.2,
/// §13.3).
#[tokio::test]
async fn a_recovery_runs_the_joining_write_with_its_new_circle_and_no_name() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let install = Installation::joining();
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
    let (result, carol) = owner.join_and_approve(joining, &control, |_| async {}).await;
    let handle = result.unwrap().expect("the admitted join publishes its store");
    let first_circle = joining_runs(&handle).await.remove(0).2;
    handle.close().await.unwrap();
    install.app.discard_store(&install.app.store_dir(&store)).await.unwrap();
    let recovery = crate::tests::device_invite(
        &owner.handle,
        carol,
        InviteAccess::S3AccessKey {
            access_key_id: "recovery-key".into(),
            secret_access_key: SecretText::new("recovery-secret".into()),
        },
        crate::InviteLifetime::SevenDays,
    )
    .await;
    let control = JoinControl::new();
    let joining = install.run(
        &owner,
        Installation::join_request(&recovery, &control),
        owner.recipient(),
        &cancel,
        |_| {},
    );
    let (result, member) = owner.join_and_approve(joining, &control, |_| async {}).await;
    assert_eq!(member, carol);
    let handle = result.unwrap().expect("the recovery publishes its store");
    let runs = joining_runs(&handle).await;
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].0, carol.to_string());
    assert_eq!(runs[0].1, None);
    assert_ne!(runs[0].2, first_circle);
    handle.close().await.unwrap();
    owner.close().await;
}

/// Carol's phone has published Household but not applied its admission, so
/// her device is not registered. A write with a synced change would record
/// the slot she had applied, zero, and Ana's device would refuse it, and the
/// slot that carries it, for want of a device to author it. The write fails
/// at commit instead, leaving the rows as they were, and no write waits to be sent; a write to
/// a table of the device's own commits. Once the first load has applied her
/// registration, the same write commits and Ana's device takes it (§7.1,
/// §13.4).
#[tokio::test]
async fn a_joined_store_refuses_synced_writes_until_its_registration_has_applied() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let install = Installation::new();
    let handle = published_before_loading(&owner, &install).await;
    let early = handle
        .write(|sql| {
            sql.execute(
                "INSERT INTO notes VALUES('early',?1)",
                coven_database::params![vec![9_u8]],
            )?;
            Ok::<_, CovenError>(())
        })
        .await;
    assert!(matches!(early, Err(CovenError::Database(DbError::NotRegistered))), "{early:?}");
    assert_eq!(rows_of(&handle).await, Vec::<String>::new());
    // A table of the device's own is written as before.
    let own = handle
        .write(|sql| {
            sql.execute("INSERT INTO joining_runs VALUES('own',NULL,'none')", [])?;
            Ok::<_, CovenError>(())
        })
        .await;
    assert!(own.is_ok(), "{own:?}");
    owner.storage.set_faults(coven_storage::test_utils::Faults::none()).await;
    handle.start_sync().await.unwrap();
    let answer = handle.retry_now().await.unwrap().await;
    assert!(matches!(answer, RetryOutcome::Synced { .. }), "{answer:?}");
    let late = handle
        .write(|sql| {
            sql.execute(
                "INSERT INTO notes VALUES('late',?1)",
                coven_database::params![vec![9_u8]],
            )?;
            Ok::<_, CovenError>(())
        })
        .await;
    assert!(late.is_ok(), "{late:?}");
    let answer = handle.retry_now().await.unwrap().await;
    assert!(matches!(answer, RetryOutcome::Synced { .. }), "{answer:?}");
    owner.pass_now().await;
    assert!(owner.rows().await.iter().any(|(id, _)| id == "late"));
    handle.close().await.unwrap();
    owner.close().await;
}

async fn rows_of(handle: &CovenHandle) -> Vec<String> {
    handle
        .read(|sql| {
            Ok::<_, CovenError>(sql.query(
                "SELECT id FROM notes WHERE id NOT IN ('before','after') ORDER BY id",
                [],
                |r| r.get(0),
            )?)
        })
        .await
        .unwrap()
}

/// Carol's join has published Household, and the first load, refused by the
/// provider, has applied nothing, so her device is not registered. The app is
/// updated and opens Household again with a breaking migration. Its migration
/// write would record the slot she had applied, zero, and Ana's device would
/// refuse it as it refuses the app's own writes; the open makes none, as the
/// first open of a joining installation makes none (§18.4).
#[tokio::test]
async fn a_breaking_migration_on_a_joined_store_not_yet_loaded_makes_no_migration_write() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let install = Installation::new();
    let store = owner.directory.id();
    let first = published_before_loading(&owner, &install).await;
    first.close().await.unwrap();
    let mut changed = migrations();
    changed.push(Migration::breaking(
        2,
        "extra",
        "ALTER TABLE notes ADD COLUMN extra INTEGER NOT NULL DEFAULT 0",
        crate::OldWrites::Drop,
        tables(),
    ));
    let handle = install
        .builder(&owner, owner.recipient())
        .migrations(changed)
        .open(store, |_| {})
        .await
        .unwrap();
    owner.storage.set_faults(coven_storage::test_utils::Faults::none()).await;
    handle.start_sync().await.unwrap();
    let answer = handle.retry_now().await.unwrap().await;
    assert!(matches!(answer, RetryOutcome::Synced { .. }), "{answer:?}");
    owner.pass_now().await;
    handle.close().await.unwrap();
    owner.close().await;
}

/// Carol's join is approved and published, and her phone cannot read slot 1
/// yet, so it has loaded nothing: the store as the join leaves it before its
/// first load.
async fn published_before_loading(owner: &Owner, install: &Installation) -> CovenHandle {
    let invite = owner.invite().await;
    let store = owner.directory.id();
    let (_, cancel) = watch::channel(false);
    let control = JoinControl::new();
    let joining = install.run(
        owner,
        Installation::join_request(&invite, &control),
        owner.recipient(),
        &cancel,
        |_| {},
    );
    let (result, _member) = owner
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
    let Err(BootstrapError::Published { handle, .. }) = result else {
        panic!("the admitted join is a store that couldn't load: {:?}", result.err())
    };
    handle
}

/// Carol's join has sent its request and waits. `faults` take her access
/// away; she checks at once. Returns what the join returned and the request
/// Ana's phone holds.
async fn join_losing_access(
    owner: &Owner,
    install: &Installation,
    invite: &Invite,
    faults: coven_storage::test_utils::Faults,
) -> (Result<Option<CovenHandle>, BootstrapError>, JoinRequest) {
    let (_, cancel) = watch::channel(false);
    let control = JoinControl::new();
    let joining = install.run(
        owner,
        Installation::join_request(invite, &control),
        owner.recipient(),
        &cancel,
        |_| {},
    );
    join_with_response(joining, async {
        let request = owner.next_join_request().await;
        owner.storage.set_faults(faults).await;
        owner.check_at_once(&control);
        request
    })
    .await
}

/// Every storage request of a waiting join can meet its access gone: the
/// request's read, the marker of the inviting device's departure, the scan
/// of the slots. The join ends as an ended invite, `None`, with its code,
/// sign-in and installation gone; on S3 the invite's key refused is that
/// key's access lost (§13.2).
#[tokio::test]
async fn a_waiting_join_that_loses_its_access_ends_as_an_ended_invite() {
    use coven_storage::test_utils::Faults;
    for provider in [CloudProvider::Dropbox, CloudProvider::S3] {
        let refused = if provider == CloudProvider::S3 {
            StorageFailure::Authentication
        } else {
            StorageFailure::PermissionDenied
        };
        for point in ["request", "marker", "scan"] {
            let owner = Owner::new(provider, false).await;
            let invite = owner.invite().await;
            let install = Installation::new();
            let code = coven_sync::read_invite_code(invite.shown_code()).unwrap();
            let faults = match point {
                "request" => Faults {
                    read_failures: [(crate::tests::request_path(&invite), refused)].into(),
                    ..Faults::none()
                },
                "marker" => Faults {
                    read_failures: [(
                        ObjectPath::device_gone(code.store, code.inviting_writer),
                        refused,
                    )]
                    .into(),
                    ..Faults::none()
                },
                _ => Faults { fail_next: 1_000, failure: refused, ..Faults::none() },
            };
            let (result, _) = join_losing_access(&owner, &install, &invite, faults).await;
            assert!(matches!(result, Ok(None)), "{provider:?} {point}: {:?}", result.err());
            assert!(saved_code(&install, code.store).is_none());
            install.absent(code.store).await;
            owner.close().await;
        }
    }
}

/// Carol's join sent its request and the app closed. While it was closed her
/// access was taken back: resuming ends the join, leaving nothing, instead of
/// failing every call (§13.2).
#[tokio::test]
async fn a_sent_join_whose_share_was_revoked_while_closed_ends_when_resumed() {
    let owner = Owner::new(CloudProvider::Dropbox, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let store = owner.directory.id();
    waiting_join(&owner, &install, &invite).await;
    owner.storage.revoke_access("join@example.com").await.unwrap();
    let control = JoinControl::new();
    let result = install
        .builder(&owner, owner.recipient())
        .resume_join(store, check_when_waiting(&control), &control)
        .await;
    assert!(matches!(result, Ok(None)), "{:?}", result.err());
    assert!(saved_code(&install, store).is_none());
    install.absent(store).await;
    owner.close().await;
}

/// A sign-in the provider rejects is not lost access: the join stays
/// listed for the person to sign in again.
#[tokio::test]
async fn a_rejected_oauth_sign_in_leaves_a_waiting_join_listed() {
    use coven_storage::test_utils::Faults;
    let owner = Owner::new(CloudProvider::Dropbox, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let store = owner.directory.id();
    waiting_join(&owner, &install, &invite).await;
    owner
        .storage
        .set_faults(Faults {
            read_failures: [(crate::tests::request_path(&invite), StorageFailure::Authentication)]
                .into(),
            ..Faults::none()
        })
        .await;
    let control = JoinControl::new();
    let builder = install.builder(&owner, owner.recipient());
    let result = builder.resume_join(store, check_when_waiting(&control), &control).await;
    assert!(
        matches!(
            result,
            Err(BootstrapError::OAuth(coven_storage::providers::OAuthError::Reauthorize(_)))
        ),
        "{:?}",
        result.err()
    );
    let listed = install.builder(&owner, owner.recipient()).unfinished_joins().await.unwrap();
    assert_eq!(listed.len(), 1);
    owner.close().await;
}

/// Ana's invite is still open when Carol's join ended on lost access, and
/// she approves the request. Her admission lands: Carol is a member with a
/// registered device that never syncs, which Ana removes as she removes
/// any device that stopped for good (§13.2).
#[tokio::test]
async fn ana_approving_a_request_whose_join_ended_admits_a_device_she_can_remove() {
    use coven_storage::test_utils::Faults;
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let faults =
        Faults { fail_next: 1_000, failure: StorageFailure::Authentication, ..Faults::none() };
    let (result, request) = join_losing_access(&owner, &install, &invite, faults).await;
    assert!(matches!(result, Ok(None)), "{:?}", result.err());
    owner.storage.set_faults(Faults::none()).await;
    crate::tests::approve(&owner.handle, &request).await;
    owner.pass_now().await;
    let members = owner.handle.members().await.unwrap();
    let carol =
        members.iter().find(|member| member.id == request.member).expect("Carol is a member");
    let phone = carol
        .devices
        .iter()
        .find(|device| device.name == request.device_name)
        .expect("her phone is registered")
        .id;
    let (removal, _) = owner.handle.remove_device(phone).await.unwrap();
    crate::tests::landed(&owner.handle, removal).await;
    owner.close().await;
}

/// Carol joins and the admission makes her circle: Ana's device lists it
/// with Carol alone in it. Her device writes a row into it, and a second
/// device of hers, restored from her code, opens the circle's key from the
/// admission's own box and reads the row (§13.2, §15.5).
#[tokio::test]
async fn a_joined_member_writes_into_the_circle_her_admission_made_and_a_restored_device_reads_it()
{
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let control = JoinControl::new();
    let (_, cancel) = watch::channel(false);
    let joining = install.run(
        &owner,
        Installation::join_request(&invite, &control),
        owner.recipient(),
        &cancel,
        |_| {},
    );
    let (carol, member) = owner.join_and_approve(joining, &control, |_| async {}).await;
    let carol = carol.unwrap().expect("Carol's join found its admission");
    let circles = carol.circles().list().await.unwrap();
    let circle = circles
        .iter()
        .find(|circle| circle.name == crate::tests::CIRCLE)
        .expect("the admission made the circle her request named")
        .id;
    let alone = |members: Vec<coven_database::CircleMemberInfo>| {
        members.into_iter().map(|info| info.member).collect::<Vec<_>>()
    };
    assert_eq!(alone(carol.circles().members(circle).await.unwrap()), [member]);
    owner.pass_now().await;
    assert_eq!(alone(owner.handle.circles().members(circle).await.unwrap()), [member]);

    let audience = circle.to_string();
    carol
        .write(move |sql| {
            sql.execute(
                "INSERT INTO circle_notes VALUES('00000000-0000-4000-8000-000000000001',x'07',?1)",
                [audience],
            )?;
            Ok::<_, CovenError>(())
        })
        .await
        .unwrap();
    carol.start_sync().await.unwrap();
    let answer = carol.retry_now().await.unwrap().await;
    assert!(matches!(answer, RetryOutcome::Synced { .. }), "{answer:?}");

    let second = Installation::new();
    let tablet = restore_from_code(
        second.authenticated_builder(&owner, owner.storage.clone()).await,
        &carol.restore_code().await.unwrap(),
        owner.s3_credentials(),
        "Carol's tablet",
        |_| {},
        &cancel,
    )
    .await
    .unwrap();
    let read = |handle: &CovenHandle| {
        let handle = handle.clone();
        async move {
            handle
                .read(|sql| {
                    Ok::<_, CovenError>(sql.query("SELECT id,body FROM circle_notes", [], |r| {
                        Ok((r.get::<_, String>(0)?, r.get::<_, Vec<u8>>(1)?))
                    })?)
                })
                .await
                .unwrap()
        }
    };
    let note = ("00000000-0000-4000-8000-000000000001".to_owned(), vec![7_u8]);
    assert_eq!(read(&tablet).await, std::slice::from_ref(&note));
    // The circle is Carol's alone: Ana's device reads no row of it.
    owner.pass_now().await;
    assert!(read(&owner.handle).await.is_empty());
    tablet.close().await.unwrap();
    carol.close().await.unwrap();
    owner.close().await;
}

/// A join that stops after its request is made resumes with the same
/// request: its circle's id and copy are the saved request's, and a call
/// that names another circle is a changed join (§13.2).
#[tokio::test]
async fn a_resumed_join_keeps_its_circle_and_another_circle_name_is_a_changed_join() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let code = coven_sync::read_invite_code(invite.shown_code()).unwrap();
    let pending =
        install.app.directories().begin_bootstrap(owner.directory.id(), &UuidIds).await.unwrap();
    let directory = pending.directory();
    let device = directory.settings().unwrap().device_id;
    let first = coven_sync::JoiningIdentity::prepare_for_test(
        &directory,
        &code,
        device,
        "New phone",
        carol_joins(invite.shown_code()),
    )
    .unwrap();
    let again = coven_sync::JoiningIdentity::prepare_for_test(
        &directory,
        &code,
        device,
        "New phone",
        carol_joins(invite.shown_code()),
    )
    .unwrap();
    // Resumed, it is the request it was: the same circle id, key and copy.
    assert_eq!(again.circle(), first.circle());
    assert_eq!(first.circle_name(), crate::tests::CIRCLE);
    // The saved request holds the circle: another name for it is another join.
    let terms = coven_sync::CircleTerms { name: "Another".into(), breaking_version: 0 };
    let changed = coven_sync::JoiningIdentity::prepare(
        &directory,
        &code,
        device,
        "New phone",
        carol_joins(invite.shown_code()),
        &terms,
        &UuidIds,
    );
    assert!(matches!(changed, Err(SyncError::InvitationChanged)));
    drop(directory);
    pending.cancel().unwrap();
    owner.close().await;
}

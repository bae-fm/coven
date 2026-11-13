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

/// Carol's join publishes Household and deletes its invite code, the only
/// other place on her phone holding her name, and the store's first load
/// fails: her storage access is taken away before it streams the floor's
/// snapshot. Household keeps her name, through closing and opening it again
/// and through the load that runs once access is back, for the app to record
/// her profile from (§13.2).
#[tokio::test]
async fn a_joined_store_keeps_its_persons_name_through_a_failed_first_load() {
    let owner = Owner::new(CloudProvider::S3, true).await;
    let invite = owner.invite().await;
    let install = Installation::new();
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
    let (result, _) = owner
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
    assert_eq!(handle.joined_invitee().await.unwrap().as_deref(), Some("Carol"));
    handle.close().await.unwrap();
    owner.storage.set_faults(coven_storage::test_utils::Faults::none()).await;
    let handle = install.builder(&owner, owner.recipient()).open(store, |_| {}).await.unwrap();
    assert_eq!(handle.joined_invitee().await.unwrap().as_deref(), Some("Carol"));
    // The load installs the floor's snapshot and applies the slots after it.
    handle.start_sync().await.unwrap();
    let answer = handle.retry_now().await.unwrap().await;
    assert!(matches!(answer, RetryOutcome::Synced { .. }), "{answer:?}");
    assert_eq!(handle.joined_invitee().await.unwrap().as_deref(), Some("Carol"));
    handle.close().await.unwrap();
    owner.close().await;
}

/// Carol's app records her profile from the name Household keeps and
/// acknowledges the name in the same write. A write that fails after
/// acknowledging keeps it, so the profile and the acknowledgement commit
/// together; once one commits, the store keeps the name no longer, opened
/// again too. Acknowledging again, as an app that can't tell whether its
/// write committed does, succeeds and changes nothing (§13.2).
#[tokio::test]
async fn acknowledging_a_joined_stores_person_ends_the_name_and_may_be_repeated() {
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
    let (result, _) = owner.join_and_approve(joining, &control, |_| async {}).await;
    let handle = result.unwrap().expect("the admitted join publishes its store");
    let name = handle.joined_invitee().await.unwrap().expect("the store keeps Carol's name");
    let failed = handle
        .write(|sql| {
            sql.acknowledge_joined_invitee()?;
            sql.execute("INSERT INTO profiles VALUES('Carol')", [])?;
            Ok::<_, CovenError>(())
        })
        .await;
    assert!(failed.is_err(), "{failed:?}");
    assert_eq!(handle.joined_invitee().await.unwrap().as_deref(), Some("Carol"));
    handle
        .write(move |sql| {
            sql.execute(
                "INSERT INTO notes VALUES('profile',?1)",
                coven_database::params![name.into_bytes()],
            )?;
            sql.acknowledge_joined_invitee()?;
            Ok::<_, CovenError>(())
        })
        .await
        .unwrap();
    assert_eq!(handle.joined_invitee().await.unwrap(), None);
    handle.write(|sql| Ok::<_, CovenError>(sql.acknowledge_joined_invitee()?)).await.unwrap();
    assert_eq!(handle.joined_invitee().await.unwrap(), None);
    handle.close().await.unwrap();
    let handle = install.builder(&owner, owner.recipient()).open(store, |_| {}).await.unwrap();
    assert_eq!(handle.joined_invitee().await.unwrap(), None);
    handle.close().await.unwrap();
    owner.close().await;
}

/// Carol discards Household before her app recorded her profile, and joins
/// it again with a recovery invite, which names no one: the name her first
/// join kept went with the store she discarded, and the store the recovery
/// publishes keeps none (§13.2, §13.3).
#[tokio::test]
async fn discarding_a_joined_store_removes_its_persons_name() {
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
    let (result, carol) = owner.join_and_approve(joining, &control, |_| async {}).await;
    let handle = result.unwrap().expect("the admitted join publishes its store");
    assert_eq!(handle.joined_invitee().await.unwrap().as_deref(), Some("Carol"));
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
    assert_eq!(handle.joined_invitee().await.unwrap(), None);
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
            sql.acknowledge_joined_invitee()?;
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

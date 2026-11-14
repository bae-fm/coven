//! A copy that found its device id in use resets as a new device that
//! replaces nothing (§19.3).

use super::*;
use crate::builder::bootstrap::reset::tests::{builder, custody, notes, reopen, uploaded, Fixture};
use crate::tests::{held, last_pass, synced, wait_for_sent, Device, Network};
use std::path::Path;

/// Back up the device, let it upload one more note, then restore the backup:
/// the store's directory holds a copy that is behind the storage. Returns
/// the copy's device id.
async fn restored_behind(f: &Fixture) -> DeviceId {
    f.setup().await.unwrap();
    synced(&f.handle).await;
    uploaded(f, "kept").await;
    f.handle.close().await.unwrap();
    let store = f.directory.database_path().parent().unwrap().to_owned();
    let backup = tempfile::tempdir().unwrap();
    coven_foundation::files::copy_tree(&store, backup.path());
    let handle = reopen(f).open(f.directory.id(), |_| ()).await.unwrap();
    handle
        .write(|sql| {
            sql.execute("INSERT INTO notes VALUES('newer','uploaded')", [])?;
            Ok::<_, CovenError>(())
        })
        .await
        .unwrap();
    handle.start_sync().await.unwrap();
    wait_for_sent(&handle).await;
    handle.close().await.unwrap();
    std::fs::remove_dir_all(&store).unwrap();
    coven_foundation::files::copy_tree(backup.path(), &store);
    f.directory.settings().unwrap().device_id
}

async fn recorded(f: &Fixture) -> Option<coven_database::Slot> {
    coven_database::read_device_id_in_use(f.directory.database_path(), f.directory.bugs())
        .await
        .unwrap()
}

fn in_use(last: &Option<LastPass>) -> bool {
    matches!(
        last,
        Some(LastPass::Failed {
            failure: StatusFailure::DeviceIdInUse(DeviceIdEvidence::UnattemptedSlot { .. }),
            ..
        })
    )
}

async fn device_states(handle: &CovenHandle) -> Vec<(DeviceId, coven_database::DeviceState)> {
    handle
        .members()
        .await
        .unwrap()
        .into_iter()
        .flat_map(|member| member.devices)
        .map(|device| (device.id, device.state))
        .collect()
}

/// A write of the stop's record that fails fails the handle with its cause,
/// as every state write does, and the copy is not stopped: no record is
/// kept, and the next pass, in a reopened store, finds the same evidence and
/// records it.
#[tokio::test]
async fn a_failed_write_of_the_stop_fails_the_pass_and_the_next_pass_records_it() {
    let f = Fixture::new().await;
    restored_behind(&f).await;
    let handle = reopen(&f).open(f.directory.id(), |_| ()).await.unwrap();
    handle.test_fail_device_id_in_use(true).await.unwrap();
    handle.start_sync().await.unwrap();
    // The failed write fails the handle: every call reports its cause.
    let error = loop {
        match handle.read(|_| Ok::<_, CovenError>(())).await {
            Err(error) => break error,
            Ok(()) => tokio::task::yield_now().await,
        }
    };
    assert!(format!("{error:?}").contains("injected device id record failure"), "{error:?}");
    // Closing reports no failure but that cause.
    let closed = handle.close().await;
    assert!(
        closed.is_ok() || format!("{closed:?}").contains("injected device id record failure"),
        "{closed:?}"
    );
    assert_eq!(recorded(&f).await, None);

    let handle = reopen(&f).open(f.directory.id(), |_| ()).await.unwrap();
    handle.start_sync().await.unwrap();
    last_pass(&handle, in_use).await;
    handle.close().await.unwrap();
    assert!(recorded(&f).await.is_some());
}

/// A reset of a copy that recorded its stop, interrupted past its record,
/// registers the same entry when finished: the fresh id joins as a new device
/// and the old id stays active, whether the old installation is still in place
/// or already moved aside.
#[tokio::test]
async fn an_interrupted_reset_of_a_copy_registers_a_new_device_when_it_finishes() {
    for after_swap in [false, true] {
        let f = Fixture::new().await;
        let old = restored_behind(&f).await;
        let handle = reopen(&f).open(f.directory.id(), |_| ()).await.unwrap();
        handle.start_sync().await.unwrap();
        last_pass(&handle, in_use).await;
        handle.close().await.unwrap();
        let store = f.directory.id();
        let interrupted = if after_swap {
            reopen(&f).interrupt_after_swap().reset_device(store, "Restored phone", |_| ()).await
        } else {
            reopen(&f)
                .interrupt_after_registering()
                .reset_device(store, "Restored phone", |_| ())
                .await
        };
        assert!(matches!(interrupted, Err(BootstrapError::Cancelled)));
        let decided = f.directory.unfinished_replacement().unwrap().unwrap();
        let handle = if after_swap {
            reopen(&f).reset_device(store, "Restored phone", |_| ()).await.unwrap()
        } else {
            let (_, cancel) = watch::channel(false);
            reopen(&f).resume_replacement(store, |_| (), &cancel).await.unwrap()
        };
        let reset = handle.device_reset().await.unwrap().unwrap();
        assert_eq!(reset, decided.reset);
        assert_eq!(reset.old_device, old);
        let states = device_states(&handle).await;
        assert!(states.contains(&(old, coven_database::DeviceState::Active)), "{states:?}");
        assert!(
            states.contains(&(reset.new_device, coven_database::DeviceState::Active)),
            "{states:?}"
        );
        handle.close().await.unwrap();
    }
}

/// Two installations of one device on one store, as a disk clone makes: the
/// clone is a copy of the closed store's directory, opened from an app root of
/// its own holding the same keychain entries.
async fn clone_of(network: &mut Network, index: usize, root: &Path) -> Device {
    let keeper = &mut network.devices[index];
    let store = keeper.store;
    let placeholder = keeper.handle.clone();
    let closed = std::mem::replace(&mut keeper.handle, placeholder);
    closed.close().await.unwrap();
    let keychain = root.join("keychain");
    keeper.app.save_device_keychain(&keychain).unwrap();
    let app_dir = root.join("clone");
    std::fs::create_dir(&app_dir).unwrap();
    let app = Coven::in_memory_with_saved_keychain(app_dir, &keychain).unwrap();
    coven_foundation::files::copy_tree(
        keeper.app.store_dir(&store).database_path().parent().unwrap(),
        app.store_dir(&store).database_path().parent().unwrap(),
    );
    let storage = Arc::new(network.devices[0].storage.for_device());
    let connector = crate::tests::Connector::new(storage.clone());
    let mut clone =
        Device { app, store, handle: network.devices[0].handle.clone(), storage, connector };
    clone.handle = network.reopen(&clone).open(store, |_| ()).await.unwrap();
    let reopened = network.reopen(&network.devices[index]).open(store, |_| ()).await.unwrap();
    reopened.start_sync().await.unwrap();
    network.devices[index].handle = reopened;
    clone
}

async fn insert_parent(handle: &CovenHandle, id: &'static str, value: i64) {
    handle
        .write(move |sql| {
            sql.execute("INSERT INTO parents VALUES(?1,?1,?2)", (id, value))?;
            Ok::<_, CovenError>(())
        })
        .await
        .unwrap();
}

/// Carol's laptop is cloned. The keeper (the laptop that stays) lands a
/// write; the clone queues one, finds the keeper's slot and stops; the app
/// resets the clone. The keeper then writes again and stays synced, and the
/// other devices and the reset clone apply its writes: the reset replaced
/// nothing, so the keeper's id still counts. The clone's unsent write is on no
/// device (§19.3).
#[tokio::test]
async fn a_cloned_laptop_resets_as_a_new_device_and_the_keeper_carries_on() {
    let mut network = Network::new(2).await;
    let root = tempfile::tempdir().unwrap();
    let carol = 1;
    let old = network.devices[carol].handle.device_id();
    let mut clone = clone_of(&mut network, carol, root.path()).await;
    assert_eq!(clone.handle.device_id(), old);
    insert_parent(&network.devices[carol].handle, "keeper-1", 1).await;
    network.sync(carol).await;
    network.sync(0).await;
    insert_parent(&clone.handle, "clone-unsent", 2).await;
    clone.handle.start_sync().await.unwrap();
    last_pass(&clone.handle, in_use).await;
    clone.handle.close().await.unwrap();
    let store = clone.store;
    clone.handle = network.reopen(&clone).reset_device(store, "Clone", |_| ()).await.unwrap();
    let reset = clone.handle.device_reset().await.unwrap().unwrap();
    assert_eq!(reset.old_device, old);
    clone.handle.start_sync().await.unwrap();
    network.devices.push(clone);
    insert_parent(&network.devices[carol].handle, "keeper-2", 3).await;
    network.quiet().await;
    for index in 0..network.devices.len() {
        let (parents, ..) = network.rows(index).await;
        let ids: Vec<_> = parents.iter().map(|parent| parent.0.as_str()).collect();
        assert_eq!(ids, ["keeper-1", "keeper-2"], "device {index}");
    }
    let states = device_states(&network.devices[0].handle).await;
    assert!(states.contains(&(old, coven_database::DeviceState::Active)), "{states:?}");
    assert!(states.contains(&(reset.new_device, coven_database::DeviceState::Active)));
    network.assert_converged().await;
    network.close().await;
}

/// Carol's member is removed after her laptop was cloned. The clone stops on
/// the keeper's slot, which precedes the removal, and its reset loads the
/// removal and fails with `Removed(Member)` before its record, changing
/// nothing: the store, its settings and its keychain are as they were.
#[tokio::test]
async fn a_copy_of_a_removed_member_refuses_its_reset_and_changes_nothing() {
    let mut network = Network::new(2).await;
    let root = tempfile::tempdir().unwrap();
    let carol = 1;
    let member = network.devices[carol]
        .handle
        .members()
        .await
        .unwrap()
        .into_iter()
        .find(|member| member.is_self)
        .unwrap()
        .id;
    let old = network.devices[carol].handle.device_id();
    let clone = clone_of(&mut network, carol, root.path()).await;
    insert_parent(&network.devices[carol].handle, "keeper-1", 1).await;
    network.sync(carol).await;
    network.sync(0).await;
    clone.handle.start_sync().await.unwrap();
    last_pass(&clone.handle, in_use).await;
    clone.handle.close().await.unwrap();
    let removal = network.devices[0].handle.remove_member(&member).await.unwrap();
    crate::tests::landed(&network.devices[0].handle, removal).await;
    let store = clone.store;
    let directory = clone.app.store_dir(&store);
    let settings = directory.settings().unwrap();
    let before = held(&clone.app, store);
    let refused = network.reopen(&clone).reset_device(store, "Clone", |_| ()).await;
    assert!(
        matches!(
            &refused,
            Err(BootstrapError::Sync(SyncError::Removed(coven_database::Removal::Member)))
        ),
        "{:?}",
        refused.as_ref().err()
    );
    assert_eq!(directory.unfinished_replacement().unwrap(), None);
    assert_eq!(directory.settings().unwrap(), settings);
    assert_eq!(settings.device_id, old);
    assert!(!directory.database_path().parent().unwrap().join(".coven-replacement").exists());
    assert_eq!(held(&clone.app, store), before);
    network.close().await;
}

/// A reset of an installation that recorded no stop, and `builder` and
/// `custody` as the fixture's tests use them, still registers in the old id's
/// place: the old id reads replaced.
#[tokio::test]
async fn a_reset_of_a_database_with_no_record_replaces_the_old_id() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    synced(&f.handle).await;
    f.handle.close().await.unwrap();
    let old = f.directory.settings().unwrap().device_id;
    custody(&f).delete_device_id().unwrap();
    assert_eq!(recorded(&f).await, None);
    let handle = builder(&f.app, &f.clock, &f.connector)
        .reset_device(f.directory.id(), "Phone", |_| ())
        .await
        .unwrap();
    let states = device_states(&handle).await;
    assert!(states.contains(&(old, coven_database::DeviceState::Replaced)), "{states:?}");
    assert_eq!(notes(&handle).await, Vec::<String>::new());
    handle.close().await.unwrap();
}

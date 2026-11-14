use super::*;
use crate::tests::{held, Held, KeysReplaced, Network};
use coven_crypto::custody::KeyringCustody;
use std::sync::atomic::{AtomicBool, Ordering};

/// The S3 key the network's invite made for Carol, held apart from her code.
fn carol_key() -> S3Credentials {
    S3Credentials {
        access_key_id: "member-1".into(),
        secret_access_key: SecretText::new("secret".into()),
    }
}

async fn restore(
    builder: CovenBuilder,
    store: StoreId,
    code: &str,
) -> Result<CovenHandle, BootstrapError> {
    let (_, cancel) = watch::channel(false);
    builder
        .restore_device(
            store,
            code,
            Some(carol_key()),
            "Carol's laptop",
            |_| Ok(()),
            |_| {},
            &cancel,
        )
        .await
}

/// The store's own directory, where the replaced installation lived.
fn store_path(replaced: &KeysReplaced) -> std::path::PathBuf {
    replaced.carol.app.store_dir(&replaced.carol.store).database_path().parent().unwrap().to_owned()
}

/// The entries a finished restore leaves no trace of: an archive of the old
/// installation, the hidden loading directory and the swap's record.
fn leftovers(replaced: &KeysReplaced) -> Vec<String> {
    std::fs::read_dir(store_path(replaced))
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .filter(|name| {
            name.starts_with("discarded-device-")
                || name == ".coven-replacement"
                || name == "device-reset.json"
        })
        .collect()
}

/// The laptop's store opens as before, with its old device id.
async fn opens_as_before(network: &Network, replaced: &KeysReplaced, device: DeviceId) {
    let handle = network.reopen(&replaced.carol).open(replaced.carol.store, |_| ()).await.unwrap();
    assert_eq!(handle.device_id(), device);
    assert_eq!(handle.device_reset().await.unwrap(), None);
    handle.close().await.unwrap();
}

/// The restored handle holds Carol's current pair under a fresh device that
/// replaces the old one, and only the new installation remains.
async fn restored(
    network: &Network,
    replaced: &KeysReplaced,
    handle: CovenHandle,
    old: DeviceId,
) -> DeviceId {
    let current = coven_sync::read_restore_code(&replaced.code).unwrap();
    let new = handle.device_id();
    assert_ne!(new, old);
    assert_eq!(
        handle.device_reset().await.unwrap(),
        Some(DeviceReset { old_device: old, new_device: new })
    );
    let members = handle.members().await.unwrap();
    let carol = members.iter().find(|member| member.id == replaced.member).unwrap();
    assert!(carol.is_self);
    assert_eq!(carol.keys, current.member_keys.public_keys());
    let devices: Vec<_> = carol.devices.iter().map(|device| (device.id, device.state)).collect();
    assert!(devices.contains(&(new, crate::DeviceState::Active)));
    assert!(devices.contains(&(old, crate::DeviceState::Replaced)));
    assert_eq!(devices.iter().filter(|(_, state)| *state == crate::DeviceState::Active).count(), 1);
    assert_eq!(handle.restore_code().await.unwrap(), replaced.code);
    handle.close().await.unwrap();
    assert!(leftovers(replaced).is_empty(), "{:?}", leftovers(replaced));
    // The keychain names the new device and holds the current pair; the
    // saved restore code is rewritten with it.
    let after = held(&replaced.carol.app, replaced.carol.store);
    assert_eq!(after.device, Some(new));
    assert_eq!(after.keys, current.member_keys.public_keys());
    let code = after.code.expect("the saved restore code is rewritten, never deleted");
    assert_eq!(
        coven_format::codes::KeychainRestoreCode::from_bytes(&code)
            .unwrap()
            .code
            .member_keys
            .public_keys(),
        current.member_keys.public_keys()
    );
    let handle = network.reopen(&replaced.carol).open(replaced.carol.store, |_| ()).await.unwrap();
    assert_eq!(handle.device_id(), new);
    handle.close().await.unwrap();
    new
}

/// Ana replaced Carol's keys with a pair Carol prepared on her phone. Carol's
/// laptop restores in place from her current code: a fresh device replaces
/// the old one, with the new pair, and no copy of the old installation stays.
/// Calling it again finds the pair installed and just opens the store, and
/// the open's steps reach the app as statuses: here, the migration a newer
/// app adds.
#[tokio::test]
async fn a_restore_installs_the_current_pair_in_place_of_the_replaced_installation() {
    let mut network = Network::new(2).await;
    let replaced = network.replace_keys_elsewhere().await;
    let store = replaced.carol.store;
    let old = held(&replaced.carol.app, store).device.unwrap();
    assert!(held(&replaced.carol.app, store).code.is_some());
    let handle = restore(network.reopen(&replaced.carol), store, &replaced.code).await.unwrap();
    let new = restored(&network, &replaced, handle, old).await;
    let mut migrations = crate::tests::migrations();
    migrations.push(Migration::addition(2, "tags", "CREATE TABLE tags(tag TEXT NOT NULL)"));
    let (_, cancel) = watch::channel(false);
    let statuses = std::cell::RefCell::new(Vec::new());
    let again = network
        .reopen(&replaced.carol)
        .migrations(migrations)
        .restore_device(
            store,
            &replaced.code,
            Some(carol_key()),
            "Carol's laptop",
            |_| Ok(()),
            |status| statuses.borrow_mut().push(status),
            &cancel,
        )
        .await
        .unwrap();
    assert_eq!(again.device_id(), new);
    again.close().await.unwrap();
    assert_eq!(
        statuses.into_inner().last(),
        Some(&BootstrapStatus::Opening(Progress { step: OpenStep::Migrating, done: 1, total: 1 }))
    );
    network.close().await;
}

/// A code history no longer binds, or one for another store, is refused
/// before anything changes.
#[tokio::test]
async fn a_stale_code_or_another_stores_code_changes_nothing() {
    let mut network = Network::new(2).await;
    let replaced = network.replace_keys_elsewhere().await;
    let store = replaced.carol.store;
    let before = held(&replaced.carol.app, store);
    let old = before.device.unwrap();
    let refused = restore(network.reopen(&replaced.carol), store, &replaced.stale).await;
    assert!(
        matches!(&refused, Err(BootstrapError::Sync(SyncError::MemberKeysReplaced { member })) if *member == replaced.member),
        "{:?}",
        refused.as_ref().err()
    );
    let other = StoreId(uuid::Uuid::from_u128(99));
    let foreign = coven_format::codes::RestoreCode {
        store: other,
        ..coven_sync::read_restore_code(&replaced.code).unwrap()
    }
    .to_text()
    .unwrap();
    assert!(matches!(
        restore(network.reopen(&replaced.carol), store, &foreign).await,
        Err(BootstrapError::Code(CodeError::WrongStore { expected, actual }))
            if expected == store && actual == other
    ));
    assert!(leftovers(&replaced).is_empty());
    assert_eq!(held(&replaced.carol.app, store), before);
    opens_as_before(&network, &replaced, old).await;
    network.close().await;
}

/// A load that fails, and a swap that a reader holding the old files
/// refuses, leave the old installation openable and the keychain as it was.
/// The next call continues with the same fresh device.
#[tokio::test]
async fn failures_while_loading_or_swapping_leave_the_old_installation() {
    let mut network = Network::new(2).await;
    let replaced = network.replace_keys_elsewhere().await;
    let store = replaced.carol.store;
    let before = held(&replaced.carol.app, store);
    let old = before.device.unwrap();
    let storage = replaced.carol.storage.clone();
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    storage.hold_next_read(coven_storage::ObjectPath::floor(store), entered, resumed).await;
    let builder = network.reopen(&replaced.carol);
    let code = replaced.code.clone();
    let loading = tokio::spawn(async move { restore(builder, store, &code).await.map(drop) });
    entering.await.unwrap();
    storage.set_online(false);
    resume.send(()).unwrap();
    assert!(loading.await.unwrap().is_err());
    storage.set_online(true);
    assert_eq!(held(&replaced.carol.app, store), before);
    opens_as_before(&network, &replaced, old).await;
    let reader = network.reopen(&replaced.carol).open_read_only(store).await.unwrap();
    assert!(matches!(
        restore(network.reopen(&replaced.carol), store, &replaced.code).await,
        Err(BootstrapError::Directory(BootstrapDirectoryError::Lock(StoreLockError::AlreadyOpen(
            _
        ))))
    ));
    reader.close().await.unwrap();
    assert_eq!(held(&replaced.carol.app, store), before);
    assert_eq!(replaced.carol.app.store_dir(&store).unfinished_replacement().unwrap(), None);
    opens_as_before(&network, &replaced, old).await;
    let handle = restore(network.reopen(&replaced.carol), store, &replaced.code).await.unwrap();
    restored(&network, &replaced, handle, old).await;
    network.close().await;
}

/// Identity custody that refuses its first write, as a locked keychain might.
struct RefusesFirstWrite {
    custody: KeyringCustody<MemberIdentity>,
    refused: AtomicBool,
}

impl Custody<MemberIdentity> for RefusesFirstWrite {
    fn unlock(&self) -> Result<Option<MemberIdentity>, KeyError> {
        self.custody.unlock()
    }
    fn persist(&self, identity: &MemberIdentity) -> Result<(), KeyError> {
        if !self.refused.swap(true, Ordering::SeqCst) {
            return Err(KeyError::Unavailable("refused".into()));
        }
        self.custody.persist(identity)
    }
    fn forget(&self) -> Result<(), KeyError> {
        self.custody.forget()
    }
    fn close(&self) {
        self.custody.close();
    }
}

/// Committing the new pair fails after the swap: every keychain entry gets
/// its value back, and the restore, past its registration, stays for the
/// next call: nothing moves back, and the store refuses opens until
/// resuming finishes it (§19.3).
#[tokio::test]
async fn a_failed_key_commit_leaves_the_swap_for_resuming() {
    let mut network = Network::new(2).await;
    let replaced = network.replace_keys_elsewhere().await;
    let store = replaced.carol.store;
    let before = held(&replaced.carol.app, store);
    let old = before.device.unwrap();
    let custody = RefusesFirstWrite {
        custody: KeyringCustody::new(Arc::new(replaced.carol.app.store_keychain(store))),
        refused: AtomicBool::new(false),
    };
    let failed = restore(
        network.reopen(&replaced.carol).identity_custody(CustodyChoice::Custom(Arc::new(custody))),
        store,
        &replaced.code,
    )
    .await;
    assert!(
        matches!(&failed, Err(BootstrapError::SecureStorage(KeyError::Unavailable(_)))),
        "{:?}",
        failed.as_ref().err()
    );
    assert_eq!(held(&replaced.carol.app, store), before);
    assert!(matches!(
        network.reopen(&replaced.carol).open(store, |_| ()).await,
        Err(CovenError::Lock(StoreLockError::ReplacementUnfinished {
            by: ReplacedBy::Restore,
            ..
        }))
    ));
    let handle = resume(&network, &replaced).await.unwrap();
    restored(&network, &replaced, handle, old).await;
    network.close().await;
}

/// Resume Carol's replacement through a builder given nothing of hers.
async fn resume(network: &Network, replaced: &KeysReplaced) -> Result<CovenHandle, BootstrapError> {
    let (_, cancel) = watch::channel(false);
    network.reopen(&replaced.carol).resume_replacement(replaced.carol.store, |_| {}, &cancel).await
}

/// Opening the store is refused while Carol's restore is unfinished, naming
/// the call that finishes it.
async fn refuses_opens(network: &Network, replaced: &KeysReplaced) {
    let store = replaced.carol.store;
    let unfinished = |error: &StoreLockError| matches!(error, StoreLockError::ReplacementUnfinished { store: named, by: ReplacedBy::Restore } if *named == store);
    assert!(matches!(
        network.reopen(&replaced.carol).open(store, |_| ()).await,
        Err(CovenError::Lock(error)) if unfinished(&error)
    ));
    assert!(matches!(
        network.reopen(&replaced.carol).open_read_only(store).await,
        Err(ReadOnlyOpenError::Local(CovenError::Lock(error))) if unfinished(&error)
    ));
    assert_eq!(
        replaced.carol.app.readable_stores().await[0].unfinished,
        Some(Unfinished::Replacement(ReplacedBy::Restore))
    );
}

/// Carol's laptop restores in place and stops right after its fresh device
/// registered, before the swap. History now names the old installation as
/// replaced, so it never opens again: the store refuses opens, naming
/// `restore_device`, and lists in that state, with custody as it was.
/// Resuming with nothing from the app finishes it (§13.1, §19.3).
#[tokio::test]
async fn a_restore_stopped_after_registering_refuses_opens_and_resumes_from_what_it_kept() {
    let mut network = Network::new(2).await;
    let replaced = network.replace_keys_elsewhere().await;
    let store = replaced.carol.store;
    let before = held(&replaced.carol.app, store);
    let old = before.device.unwrap();
    let interrupted = restore(
        network.reopen(&replaced.carol).interrupt_after_registering(),
        store,
        &replaced.code,
    )
    .await;
    assert!(matches!(interrupted, Err(BootstrapError::Cancelled)), "{interrupted:?}");
    refuses_opens(&network, &replaced).await;
    assert_eq!(held(&replaced.carol.app, store), before);
    restored(&network, &replaced, resume(&network, &replaced).await.unwrap(), old).await;
    assert!(replaced.carol.app.store_keychain(store).replacement_record().unwrap().is_none());
    network.close().await;
}

/// Hold the next object created in Carol's store, the laptop's registration
/// once it has loaded: `started` hears it is on its way, and it is stored
/// once `release` is sent. Ana syncs first, so the slot her pass writes
/// after applying the key replacement is in storage, not the one held.
async fn hold_registration(
    network: &Network,
    replaced: &KeysReplaced,
) -> (tokio::sync::oneshot::Receiver<()>, tokio::sync::oneshot::Sender<()>) {
    network.sync(0).await;
    let (started, starting) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    replaced
        .carol
        .storage
        .hold_next_creation(
            coven_storage::ObjectPrefix::all(replaced.carol.store),
            started,
            released,
        )
        .await;
    (starting, release)
}

/// Carol cancels the restore while its fresh device's registration is on
/// its way. The cancel waits for it to land, then fails with
/// `RestoreRegistered`: the store refuses opens, and resuming finishes
/// (§19.3, §11.6).
#[tokio::test]
async fn cancelling_a_restore_in_place_after_it_registers_refuses_and_keeps_it() {
    let mut network = Network::new(2).await;
    let replaced = network.replace_keys_elsewhere().await;
    let store = replaced.carol.store;
    let old = held(&replaced.carol.app, store).device.unwrap();
    let (started, release) = hold_registration(&network, &replaced).await;
    let (stop, cancel) = watch::channel(false);
    let builder = network.reopen(&replaced.carol);
    let restoring = builder.restore_device(
        store,
        &replaced.code,
        Some(carol_key()),
        "Carol's laptop",
        |_| Ok(()),
        |_| {},
        &cancel,
    );
    let cancelling = async {
        started.await.unwrap();
        stop.send_replace(true);
        release.send(()).unwrap();
    };
    let (result, ()) = tokio::time::timeout(coven_foundation::clock::HANG_GUARD, async {
        tokio::join!(restoring, cancelling)
    })
    .await
    .unwrap();
    assert!(matches!(result, Err(BootstrapError::RestoreRegistered)), "{result:?}");
    refuses_opens(&network, &replaced).await;
    restored(&network, &replaced, resume(&network, &replaced).await.unwrap(), old).await;
    network.close().await;
}

/// Ana removes Carol while Carol's laptop restores in place, and the
/// removal takes the slot the laptop's registration was sent for. Checked
/// again, the registration is refused: nothing in storage names the fresh
/// device, so the restore ends, removing its record and new installation,
/// and the old copy is the store again (§19.3).
#[tokio::test]
async fn a_restore_in_place_whose_registration_is_refused_leaves_the_old_copy() {
    let mut network = Network::new(2).await;
    let replaced = network.replace_keys_elsewhere().await;
    let store = replaced.carol.store;
    let before = held(&replaced.carol.app, store);
    let (started, release) = hold_registration(&network, &replaced).await;
    let restoring = restore(network.reopen(&replaced.carol), store, &replaced.code);
    let ana = &network.devices[0].handle;
    let removing = async {
        started.await.unwrap();
        crate::tests::landed(ana, ana.remove_member(&replaced.member).await.unwrap()).await;
        release.send(()).unwrap();
    };
    let (result, ()) = tokio::time::timeout(coven_foundation::clock::HANG_GUARD, async {
        tokio::join!(restoring, removing)
    })
    .await
    .unwrap();
    assert!(
        matches!(result, Err(BootstrapError::Sync(SyncError::Removed(_) | SyncError::Rejected(_)))),
        "{result:?}"
    );
    assert_eq!(replaced.carol.app.store_dir(&store).unfinished_replacement().unwrap(), None);
    assert!(leftovers(&replaced).is_empty(), "{:?}", leftovers(&replaced));
    assert!(replaced.carol.app.store_keychain(store).replacement_record().unwrap().is_none());
    // The laptop's custody is as it was, but for Carol's restore code: her
    // removal deletes it as the new device applied it (§14.1).
    assert_eq!(held(&replaced.carol.app, store), Held { code: None, ..before });
    let handle = network.reopen(&replaced.carol).open(store, |_| ()).await.unwrap();
    assert_eq!(handle.device_id(), before.device.unwrap());
    handle.close().await.unwrap();
    network.close().await;
}

/// A restore stopped right after its swap leaves the store waiting for it,
/// with the keychain untouched. Opening names the call that finishes it, and
/// the store still lists with its name and that state. A reset is refused
/// and changes nothing; retrying the restore finishes it with the fresh
/// device the swap recorded.
#[tokio::test]
async fn a_restore_interrupted_after_its_swap_is_finished_by_retrying_it() {
    let mut network = Network::new(2).await;
    let replaced = network.replace_keys_elsewhere().await;
    let store = replaced.carol.store;
    let app = &replaced.carol.app;
    let before = held(&replaced.carol.app, store);
    let old = before.device.unwrap();
    let listed = app.readable_stores().await;
    assert_eq!(listed.len(), 1);
    let name = listed[0].name.clone();
    assert!(matches!(
        restore(network.reopen(&replaced.carol).interrupt_after_swap(), store, &replaced.code)
            .await,
        Err(BootstrapError::Cancelled)
    ));
    let directory = app.store_dir(&store);
    let decided = directory.unfinished_replacement().unwrap().unwrap();
    assert_eq!(decided.reset.old_device, old);
    assert_eq!(decided.by, ReplacedBy::Restore);
    let unfinished = |error: &StoreLockError| matches!(error, StoreLockError::ReplacementUnfinished { store: named, by: ReplacedBy::Restore } if *named == store);
    assert!(matches!(
        network.reopen(&replaced.carol).open(store, |_| ()).await,
        Err(CovenError::Lock(error)) if unfinished(&error)
    ));
    assert!(matches!(
        network.reopen(&replaced.carol).open_read_only(store).await,
        Err(ReadOnlyOpenError::Local(CovenError::Lock(error))) if unfinished(&error)
    ));
    assert_eq!(
        app.readable_stores().await,
        [StoreInfo {
            id: store,
            name,
            unfinished: Some(Unfinished::Replacement(ReplacedBy::Restore))
        }]
    );
    assert_eq!(held(&replaced.carol.app, store), before);
    // A reset would load under the replaced pair and keep the old copy.
    let files = files_under(&store_path(&replaced));
    let refused =
        network.reopen(&replaced.carol).reset_device(store, "Carol's laptop", |_| ()).await;
    assert!(
        matches!(&refused, Err(BootstrapError::Directory(BootstrapDirectoryError::Lock(error))) if unfinished(error)),
        "{:?}",
        refused.as_ref().err()
    );
    assert_eq!(files_under(&store_path(&replaced)), files);
    assert_eq!(held(&replaced.carol.app, store), before);
    let handle = restore(network.reopen(&replaced.carol), store, &replaced.code).await.unwrap();
    assert_eq!(restored(&network, &replaced, handle, old).await, decided.reset.new_device);
    assert_eq!(app.readable_stores().await[0].unfinished, None);
    network.close().await;
}

/// Carol's laptop stops past the restore's record, before or after its swap,
/// and she gives up on the laptop's copy. Discarding the store deletes both
/// copies and the laptop's own keychain entries; her restore code and S3 key
/// stay, and she restores from scratch (§13.1).
#[tokio::test]
async fn discarding_a_store_waiting_for_its_restore_keeps_the_persons_entries() {
    for after_swap in [false, true] {
        let mut network = Network::new(2).await;
        let replaced = network.replace_keys_elsewhere().await;
        let store = replaced.carol.store;
        let app = &replaced.carol.app;
        let builder = network.reopen(&replaced.carol);
        let builder = if after_swap {
            builder.interrupt_after_swap()
        } else {
            builder.interrupt_after_registering()
        };
        let interrupted = restore(builder, store, &replaced.code).await;
        assert!(matches!(interrupted, Err(BootstrapError::Cancelled)), "{interrupted:?}");
        refuses_opens(&network, &replaced).await;
        let person = app.person_entries(store);
        let saved = (person.restore_code().unwrap(), person.s3_credentials().unwrap());
        assert!(saved.0.is_some() && saved.1.is_some());

        app.discard_store(&app.store_dir(&store)).await.unwrap();
        assert!(!store_path(&replaced).exists());
        assert!(app.readable_stores().await.is_empty());
        let keychain = app.store_keychain(store);
        assert_eq!(keychain.device_id().unwrap(), None);
        assert!(keychain.storage_credentials().unwrap().is_none());
        assert!(keychain.replacement_record().unwrap().is_none());
        let bytes = |value: Option<SecretBytes>| value.map(|value| value.as_bytes().to_vec());
        assert_eq!(bytes(person.restore_code().unwrap()), bytes(saved.0));
        assert_eq!(bytes(person.s3_credentials().unwrap()), bytes(saved.1));

        let (_, cancel) = watch::channel(false);
        let handle = restore_from_code(
            network.reopen(&replaced.carol),
            &replaced.code,
            Some(carol_key()),
            "Carol's laptop",
            |_| {},
            &cancel,
        )
        .await
        .unwrap();
        assert_eq!(handle.store_id(), store);
        handle.close().await.unwrap();
        network.close().await;
    }
}

/// The network's migrations, with Carol's app keeping her theme in a
/// device-local table.
fn with_preferences() -> Vec<Migration> {
    let mut migrations = crate::tests::migrations();
    migrations.push(Migration::addition(
        2,
        "preferences",
        "CREATE TABLE preferences(name TEXT NOT NULL PRIMARY KEY, value TEXT NOT NULL)",
    ));
    migrations
}

/// Carol's theme, as `handle` holds it.
async fn theme(handle: &CovenHandle) -> Option<String> {
    handle
        .read(|sql| {
            Ok::<_, CovenError>(sql.query(
                "SELECT value FROM preferences WHERE name='theme'",
                [],
                |r| r.get::<_, String>(0),
            )?)
        })
        .await
        .unwrap()
        .pop()
}

/// Carol's laptop keeps her theme in a device-local table the new device
/// would start without. Her app reads it from the old copy, which still
/// opens, and the restore's step writes it into the new device before the
/// swap: a step that writes a synced table fails the restore with the old
/// copy in place and its theme kept, and the restore that follows carries
/// the theme over with the new device (§13.1).
#[tokio::test]
async fn a_restore_carries_device_local_rows_its_step_writes() {
    let mut network = Network::new(2).await;
    let replaced = network.replace_keys_elsewhere().await;
    let store = replaced.carol.store;
    let old = held(&replaced.carol.app, store).device.unwrap();
    let laptop = || network.reopen(&replaced.carol).migrations(with_preferences());
    let handle = laptop().open(store, |_| ()).await.unwrap();
    handle
        .write_bulk(|sql| {
            sql.execute("INSERT INTO preferences VALUES('theme','dark')", [])?;
            Ok::<_, CovenError>(())
        })
        .await
        .unwrap();
    let kept = theme(&handle).await.unwrap();
    handle.close().await.unwrap();

    let (_, cancel) = watch::channel(false);
    let refused = laptop()
        .restore_device(
            store,
            &replaced.code,
            Some(carol_key()),
            "Carol's laptop",
            |sql| {
                sql.execute("INSERT INTO parents VALUES('p','label',1)", [])?;
                Ok(())
            },
            |_| {},
            &cancel,
        )
        .await;
    assert!(
        matches!(&refused, Err(BootstrapError::Store(CovenError::Database(DbError::SyncedTableInBulkWrite { table }))) if table == "parents"),
        "{:?}",
        refused.as_ref().err()
    );
    let handle = laptop().open(store, |_| ()).await.unwrap();
    assert_eq!(handle.device_id(), old);
    assert_eq!(theme(&handle).await.as_deref(), Some("dark"));
    handle.close().await.unwrap();

    let handle = laptop()
        .restore_device(
            store,
            &replaced.code,
            Some(carol_key()),
            "Carol's laptop",
            move |sql| {
                sql.execute("INSERT INTO preferences VALUES('theme',?1)", [kept])?;
                Ok(())
            },
            |_| {},
            &cancel,
        )
        .await
        .unwrap();
    assert_ne!(handle.device_id(), old);
    assert_eq!(theme(&handle).await.as_deref(), Some("dark"));
    handle.close().await.unwrap();
    network.close().await;
}

/// The step's rows are in the new device once it has swapped in: a restore
/// stopped right after the swap is finished by a call whose step isn't run,
/// and the theme the first call's step wrote is there (§13.1).
#[tokio::test]
async fn a_restore_finished_after_its_swap_keeps_the_rows_its_step_wrote() {
    let mut network = Network::new(2).await;
    let replaced = network.replace_keys_elsewhere().await;
    let store = replaced.carol.store;
    let laptop = || network.reopen(&replaced.carol).migrations(with_preferences());
    let (_, cancel) = watch::channel(false);
    let interrupted = laptop()
        .interrupt_after_swap()
        .restore_device(
            store,
            &replaced.code,
            Some(carol_key()),
            "Carol's laptop",
            |sql| {
                sql.execute("INSERT INTO preferences VALUES('theme','dark')", [])?;
                Ok(())
            },
            |_| {},
            &cancel,
        )
        .await;
    assert!(matches!(interrupted, Err(BootstrapError::Cancelled)), "{interrupted:?}");
    let handle = laptop()
        .restore_device(
            store,
            &replaced.code,
            Some(carol_key()),
            "Carol's laptop",
            |_| panic!("the swapped installation's step already ran"),
            |_| {},
            &cancel,
        )
        .await
        .unwrap();
    assert_eq!(theme(&handle).await.as_deref(), Some("dark"));
    handle.close().await.unwrap();
    network.close().await;
}

/// Carol's laptop is dropped mid-restore past its record, as when the app
/// quits, and she gives up on the laptop's copy. The dropped call leaves the
/// loading database to close as its last holders end, so a deletion waits
/// for that close: it begins with no file of the store open, which Windows
/// needs to move the directory.
#[tokio::test]
async fn discarding_a_store_whose_dropped_restore_is_loading_waits_for_its_files_to_close() {
    let mut network = Network::new(2).await;
    let replaced = network.replace_keys_elsewhere().await;
    let store = replaced.carol.store;
    let app = &replaced.carol.app;
    let (registering, release) = hold_registration(&network, &replaced).await;
    tokio::select! {
        result = restore(network.reopen(&replaced.carol), store, &replaced.code) => {
            panic!("the restore finished before it was dropped: {:?}", result.err());
        }
        _ = registering => {}
    }
    // The registration the dropped call had on its way lands, as it does
    // when the app quits and the request is already out.
    release.send(()).unwrap();

    app.store_dir(&store).closed().await.unwrap();
    crate::assert_no_open_files_under(&store_path(&replaced));

    app.discard_store(&app.store_dir(&store)).await.unwrap();
    assert!(!store_path(&replaced).exists());
    network.close().await;
}

/// A restore that stopped right after its fresh device registered leaves the
/// store refusing opens. The app tries to open and list it, finds it
/// unfinished, and discards it. Neither the refused opens nor the listing
/// leave a file of the store open, which Windows needs to move the directory.
#[tokio::test]
async fn discarding_an_unfinished_restore_after_refused_opens_finds_no_file_open() {
    let mut network = Network::new(2).await;
    let replaced = network.replace_keys_elsewhere().await;
    let store = replaced.carol.store;
    let app = &replaced.carol.app;
    let interrupted = restore(
        network.reopen(&replaced.carol).interrupt_after_registering(),
        store,
        &replaced.code,
    )
    .await;
    assert!(matches!(interrupted, Err(BootstrapError::Cancelled)), "{interrupted:?}");
    refuses_opens(&network, &replaced).await;

    app.store_dir(&store).closed().await.unwrap();
    crate::assert_no_open_files_under(&store_path(&replaced));

    app.discard_store(&app.store_dir(&store)).await.unwrap();
    assert!(!store_path(&replaced).exists());
    network.close().await;
}

/// The app quits the moment a restore's record exists: the call is dropped
/// while the replacement is still loading. A removal that follows finds no
/// file of the store open, which Windows needs to move the directory.
#[tokio::test]
async fn discarding_a_store_dropped_at_its_restore_record_finds_no_file_open() {
    let mut network = Network::new(2).await;
    let replaced = network.replace_keys_elsewhere().await;
    let store = replaced.carol.store;
    let app = &replaced.carol.app;
    {
        let restoring = restore(network.reopen(&replaced.carol), store, &replaced.code);
        tokio::pin!(restoring);
        loop {
            tokio::select! {
                result = &mut restoring => panic!("the restore finished: {:?}", result.err()),
                () = tokio::task::yield_now() => {
                    if replaced.carol.app.readable_stores().await[0].unfinished.is_some() {
                        break;
                    }
                }
            }
        }
    }
    app.store_dir(&store).closed().await.unwrap();
    crate::assert_no_open_files_under(&store_path(&replaced));
    app.discard_store(&app.store_dir(&store)).await.unwrap();
    assert!(!store_path(&replaced).exists());
    network.close().await;
}

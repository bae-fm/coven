use super::*;
use crate::tests::{held, KeysReplaced, Network};
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
            |_| {},
            &cancel,
        )
        .await
}

/// The store's own directory, where the replaced installation lived.
fn store_path(replaced: &KeysReplaced) -> std::path::PathBuf {
    replaced
        .carol
        .layout
        .store_dir(&replaced.carol.store)
        .database_path()
        .parent()
        .unwrap()
        .to_owned()
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
    let handle = network
        .reopen(&replaced.carol)
        .open(replaced.carol.store)
        .await
        .unwrap();
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
        Some(DeviceReset {
            old_device: old,
            new_device: new,
        })
    );
    let members = handle.members().await.unwrap();
    let carol = members
        .iter()
        .find(|member| member.id == replaced.member)
        .unwrap();
    assert!(carol.is_self);
    assert_eq!(carol.keys, current.member_keys.public_keys());
    let devices: Vec<_> = carol
        .devices
        .iter()
        .map(|device| (device.id, device.state))
        .collect();
    assert!(devices.contains(&(new, crate::DeviceState::Active)));
    assert!(devices.contains(&(old, crate::DeviceState::Replaced)));
    assert_eq!(
        devices
            .iter()
            .filter(|(_, state)| *state == crate::DeviceState::Active)
            .count(),
        1
    );
    assert_eq!(handle.restore_code().await.unwrap(), replaced.code);
    handle.close().await.unwrap();
    assert!(leftovers(replaced).is_empty(), "{:?}", leftovers(replaced));
    // The keychain names the new device and holds the current pair; the
    // synced restore code is rewritten with it.
    let after = held(&replaced.carol.app, replaced.carol.store);
    assert_eq!(after.device, Some(new));
    assert_eq!(after.keys, current.member_keys.public_keys());
    let code = after
        .code
        .expect("the synced restore code is rewritten, never deleted");
    assert_eq!(
        coven_format::codes::KeychainRestoreCode::from_bytes(&code)
            .unwrap()
            .code
            .member_keys
            .public_keys(),
        current.member_keys.public_keys()
    );
    let handle = network
        .reopen(&replaced.carol)
        .open(replaced.carol.store)
        .await
        .unwrap();
    assert_eq!(handle.device_id(), new);
    handle.close().await.unwrap();
    new
}

/// Ana replaced Carol's keys with a pair Carol prepared on her phone. Carol's
/// laptop restores in place from her current code: a fresh device replaces
/// the old one, with the new pair, and no copy of the old installation stays.
/// Calling it again finds the pair installed and just opens the store.
#[tokio::test]
async fn a_restore_installs_the_current_pair_in_place_of_the_replaced_installation() {
    let mut network = Network::new(2).await;
    let replaced = network.replace_keys_elsewhere().await;
    let store = replaced.carol.store;
    let old = held(&replaced.carol.app, store).device.unwrap();
    assert!(held(&replaced.carol.app, store).code.is_some());
    let handle = restore(network.reopen(&replaced.carol), store, &replaced.code)
        .await
        .unwrap();
    let new = restored(&network, &replaced, handle, old).await;
    let again = restore(network.reopen(&replaced.carol), store, &replaced.code)
        .await
        .unwrap();
    assert_eq!(again.device_id(), new);
    again.close().await.unwrap();
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
    storage
        .hold_next_read(coven_storage::ObjectPath::floor(store), entered, resumed)
        .await;
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
    let reader = network
        .reopen(&replaced.carol)
        .open_read_only(store)
        .await
        .unwrap();
    assert!(matches!(
        restore(network.reopen(&replaced.carol), store, &replaced.code).await,
        Err(BootstrapError::Directory(BootstrapDirectoryError::Lock(
            StoreLockError::AlreadyOpen(_)
        )))
    ));
    reader.close().await.unwrap();
    assert_eq!(held(&replaced.carol.app, store), before);
    assert_eq!(
        replaced
            .carol
            .layout
            .store_dir(&store)
            .unfinished_replacement()
            .unwrap(),
        None
    );
    opens_as_before(&network, &replaced, old).await;
    let handle = restore(network.reopen(&replaced.carol), store, &replaced.code)
        .await
        .unwrap();
    restored(&network, &replaced, handle, old).await;
    network.close().await;
}

/// Identity custody that refuses its first write, as a locked keychain might.
struct RefusesFirstWrite {
    custody: KeyringCustody<MemberIdentity>,
    refused: AtomicBool,
}

impl MemberKeyCustody for RefusesFirstWrite {
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
/// its value back and the old installation moves back into place.
#[tokio::test]
async fn a_failed_key_commit_moves_the_old_installation_back() {
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
        network
            .reopen(&replaced.carol)
            .identity_custody(IdentityCustody::Custom(Arc::new(custody))),
        store,
        &replaced.code,
    )
    .await;
    assert!(
        matches!(
            &failed,
            Err(BootstrapError::SecureStorage(KeyError::Unavailable(_)))
        ),
        "{:?}",
        failed.as_ref().err()
    );
    assert_eq!(held(&replaced.carol.app, store), before);
    assert!(!leftovers(&replaced)
        .iter()
        .any(|name| name.starts_with("discarded-device-") || name == "device-reset.json"));
    opens_as_before(&network, &replaced, old).await;
    let handle = restore(network.reopen(&replaced.carol), store, &replaced.code)
        .await
        .unwrap();
    restored(&network, &replaced, handle, old).await;
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
    let layout = &replaced.carol.layout;
    let before = held(&replaced.carol.app, store);
    let old = before.device.unwrap();
    let listed = layout.stores().await.unwrap();
    assert_eq!(listed.len(), 1);
    let name = listed[0].name.clone();
    assert!(matches!(
        restore(
            network.reopen(&replaced.carol).interrupt_after_swap(),
            store,
            &replaced.code
        )
        .await,
        Err(BootstrapError::Cancelled)
    ));
    let directory = layout.store_dir(&store);
    let decided = directory.unfinished_replacement().unwrap().unwrap();
    assert_eq!(decided.reset.old_device, old);
    assert_eq!(decided.by, ReplacedBy::Restore);
    let unfinished = |error: &StoreLockError| matches!(error, StoreLockError::ReplacementUnfinished { store: named, by: ReplacedBy::Restore } if *named == store);
    assert!(matches!(
        network.reopen(&replaced.carol).open(store).await,
        Err(CovenError::Lock(error)) if unfinished(&error)
    ));
    assert!(matches!(
        network.reopen(&replaced.carol).open_read_only(store).await,
        Err(ReadOnlyOpenError::Local(CovenError::Lock(error))) if unfinished(&error)
    ));
    assert_eq!(
        layout.stores().await.unwrap(),
        [StoreInfo {
            id: store,
            name,
            unfinished_replacement: Some(ReplacedBy::Restore),
        }]
    );
    assert_eq!(held(&replaced.carol.app, store), before);
    // A reset would load under the replaced pair and keep the old copy.
    let files = files_under(&store_path(&replaced));
    let refused = network
        .reopen(&replaced.carol)
        .reset_device(store, "Carol's laptop")
        .await;
    assert!(
        matches!(&refused, Err(BootstrapError::Directory(BootstrapDirectoryError::Lock(error))) if unfinished(error)),
        "{:?}",
        refused.as_ref().err()
    );
    assert_eq!(files_under(&store_path(&replaced)), files);
    assert_eq!(held(&replaced.carol.app, store), before);
    let handle = restore(network.reopen(&replaced.carol), store, &replaced.code)
        .await
        .unwrap();
    assert_eq!(
        restored(&network, &replaced, handle, old).await,
        decided.reset.new_device
    );
    assert_eq!(
        layout.stores().await.unwrap()[0].unfinished_replacement,
        None
    );
    network.close().await;
}

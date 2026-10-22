use super::*;
use coven_crypto::custody::Keychain;
use coven_foundation::id_source::MemberId;
use uuid::Uuid;

fn store() -> StoreId {
    StoreId(Uuid::from_u128(1))
}

fn code(store: StoreId) -> RestoreCode {
    RestoreCode {
        store,
        member: MemberId(Uuid::from_u128(7)),
        member_keys: coven_crypto::MemberKeys::generate().unwrap(),
        storage: StorageConfig::S3 {
            bucket: "household".into(),
            region: "us-east-1".into(),
            endpoint: None,
            profile: None,
            prefix: coven_storage::S3Prefix::parse("libraries").unwrap(),
        }
        .encode()
        .unwrap(),
    }
}

fn name(name: &str, slot: u64) -> SavedName {
    SavedName {
        name: name.into(),
        named_at: Slot(slot),
    }
}

fn synced() -> SyncedKeychain {
    SyncedKeychain::new(Keychain::in_memory("saved-code").unwrap(), store())
}

fn put(synced: &SyncedKeychain, label: &str, slot: u64, code: &RestoreCode) {
    synced
        .set_restore_code(&encode(label, slot, code).unwrap())
        .unwrap();
}

fn held(synced: &SyncedKeychain) -> (String, u64, Vec<u8>) {
    let bytes = synced.restore_code().unwrap().unwrap();
    let (saved, _) = read_saved_code(synced.store(), bytes.as_bytes()).unwrap();
    (
        saved.name,
        saved.named_at,
        saved.code.to_bytes().unwrap().to_vec(),
    )
}

#[test]
fn saving_a_code_keeps_a_newer_saved_name() {
    let synced = synced();
    let ours = code(store());
    let bytes = ours.to_bytes().unwrap().to_vec();
    save(&synced, &ours, &name("Household", 1)).unwrap();
    assert_eq!(held(&synced), ("Household".into(), 1, bytes.clone()));
    // Another device of the person saw Family at slot 41.
    put(&synced, "Family", 41, &code(store()));
    save(&synced, &ours, &name("Household", 1)).unwrap();
    assert_eq!(held(&synced), ("Family".into(), 41, bytes.clone()));
    save(&synced, &ours, &name("Home", 50)).unwrap();
    assert_eq!(held(&synced), ("Home".into(), 50, bytes));
}

#[test]
fn saving_a_code_replaces_an_unreadable_entry_or_one_for_another_store() {
    let synced = synced();
    let ours = code(store());
    synced
        .set_restore_code(&SecretBytes::new(b"damaged".to_vec()))
        .unwrap();
    save(&synced, &ours, &name("Household", 1)).unwrap();
    assert_eq!(held(&synced).0, "Household");
    put(&synced, "Elsewhere", 90, &code(StoreId(Uuid::from_u128(2))));
    save(&synced, &ours, &name("Household", 1)).unwrap();
    assert_eq!(
        held(&synced),
        ("Household".into(), 1, ours.to_bytes().unwrap().to_vec())
    );
}

/// Follow `name` where the entry must be readable or absent: this device's
/// code is never consulted.
fn follow(synced: &SyncedKeychain, name: &SavedName) -> Result<(), KeyError> {
    follow_name(synced, name, || -> Result<_, KeyError> {
        panic!("a readable or absent entry needs no current code")
    })
}

#[test]
fn following_a_name_rewrites_only_an_older_one_and_keeps_the_saved_code() {
    let synced = synced();
    // Absent stays absent: a forgotten entry is never recreated.
    follow(&synced, &name("Family", 41)).unwrap();
    assert!(synced.restore_code().unwrap().is_none());
    // The code another device saved, which this device never replaces.
    let saved = code(store());
    let bytes = saved.to_bytes().unwrap().to_vec();
    put(&synced, "Household", 1, &saved);
    follow(&synced, &name("Family", 41)).unwrap();
    assert_eq!(held(&synced), ("Family".into(), 41, bytes.clone()));
    // A device that is behind leaves the newer name.
    follow(&synced, &name("Household", 1)).unwrap();
    follow(&synced, &name("Other", 41)).unwrap();
    assert_eq!(held(&synced), ("Family".into(), 41, bytes));
}

#[test]
fn following_a_name_replaces_an_unreadable_entry_with_this_devices_code() {
    let keychain = Keychain::in_memory("saved-code").unwrap();
    let synced = SyncedKeychain::new(keychain.clone(), store());
    let ours = code(store());
    let ours_bytes = ours.to_bytes().unwrap().to_vec();
    let current = || Ok::<_, KeyError>(Some(code_copy(&ours)));
    let other_store = SecretBytes::new(
        KeychainRestoreCode::encode("Elsewhere", 90, &code(StoreId(Uuid::from_u128(2))))
            .unwrap()
            .to_vec(),
    );
    for unreadable in [
        SecretBytes::new(b"damaged".to_vec()),
        // A bare restore code, as saved before entries carried the name.
        SecretBytes::new(code(store()).to_bytes().unwrap().to_vec()),
        other_store,
    ] {
        synced.set_restore_code(&unreadable).unwrap();
        follow_name(&synced, &name("Family", 41), current).unwrap();
        assert_eq!(held(&synced), ("Family".into(), 41, ours_bytes.clone()));
    }
    // Copies in two keychain groups become one.
    put(&synced, "Household", 1, &code(store()));
    synced.set_restore_code_in_another_group(&SecretBytes::new(b"old group".to_vec()));
    follow_name(&synced, &name("Family", 41), current).unwrap();
    assert_eq!(held(&synced), ("Family".into(), 41, ours_bytes));
    assert!(matches!(
        &keychain.synced_restore_codes().unwrap()[..],
        [(_, SyncedRestoreCode::Saved(_))]
    ));
}

#[test]
fn an_unreadable_entry_stays_when_this_device_has_no_current_code_or_reading_fails() {
    let synced = synced();
    let damaged = SecretBytes::new(b"damaged".to_vec());
    synced.set_restore_code(&damaged).unwrap();
    follow_name(&synced, &name("Family", 41), || Ok::<_, KeyError>(None)).unwrap();
    assert_eq!(
        synced.restore_code().unwrap().unwrap().as_bytes(),
        b"damaged"
    );
    // Producing the code failed: the call fails and changes nothing.
    assert!(matches!(
        follow_name(&synced, &name("Family", 41), || Err(KeyError::StoreClosed)),
        Err(KeyError::StoreClosed)
    ));
    assert_eq!(
        synced.restore_code().unwrap().unwrap().as_bytes(),
        b"damaged"
    );
}

/// Another value of the same code, since codes erase their keys on drop.
fn code_copy(code: &RestoreCode) -> RestoreCode {
    RestoreCode::from_bytes(&code.to_bytes().unwrap()).unwrap()
}

#[test]
fn following_a_name_after_leaving_writes_nothing() {
    let synced = synced();
    put(&synced, "Household", 1, &code(store()));
    synced.delete_entries().unwrap();
    follow(&synced, &name("Family", 41)).unwrap();
    assert!(synced.restore_code().unwrap().is_none());
}

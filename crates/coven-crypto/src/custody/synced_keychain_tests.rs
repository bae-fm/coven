use super::*;
use crate::custody::StoreKeychain;
use coven_foundation::id_source::DeviceId;
use uuid::Uuid;

fn saved(keychain: &Arc<Keychain>, store: StoreId) -> SyncedKeychain {
    let synced = SyncedKeychain::new(keychain.clone(), store);
    synced
        .set_restore_code(&SecretBytes::new(b"restore".to_vec()))
        .unwrap();
    synced
        .set_s3_credentials(&SecretBytes::new(b"S3 key".to_vec()))
        .unwrap();
    synced
}

#[test]
fn synced_restore_code_failures_preserve_the_previous_code() {
    let fake = Keychain::in_memory("restore-failures").unwrap();
    let store = SyncedKeychain::new(fake.clone(), StoreId(Uuid::from_u128(1)));
    let original = SecretBytes::new(b"original".to_vec());
    store.set_restore_code(&original).unwrap();
    fake.fail_next_operation();
    assert!(matches!(
        store.set_restore_code(&SecretBytes::new(b"replacement".to_vec())),
        Err(KeyError::KeychainTemporarilyUnavailable(_))
    ));
    assert_eq!(
        store.restore_code().unwrap().unwrap().as_bytes(),
        original.as_bytes()
    );
    fake.fail_next_operation();
    assert!(matches!(
        store.delete_restore_code(),
        Err(KeyError::KeychainTemporarilyUnavailable(_))
    ));
    assert_eq!(
        store.restore_code().unwrap().unwrap().as_bytes(),
        original.as_bytes()
    );
    fake.fail_next_operation();
    assert!(matches!(
        store.restore_code(),
        Err(KeyError::KeychainTemporarilyUnavailable(_))
    ));
    fake.fail_next_operation();
    assert!(matches!(
        fake.synced_restore_codes(),
        Err(KeyError::KeychainTemporarilyUnavailable(_))
    ));
    assert_eq!(
        fake.synced_restore_codes().unwrap()[0].1.as_bytes(),
        original.as_bytes()
    );
}

#[test]
fn another_device_shares_only_the_synced_entries() {
    let phone = Keychain::in_memory("person").unwrap();
    let laptop = phone.another_device();
    let id = StoreId(Uuid::from_u128(1));
    saved(&phone, id);
    StoreKeychain::new(phone.clone(), id)
        .set_device_id(DeviceId(1))
        .unwrap();
    let on_laptop = SyncedKeychain::new(laptop.clone(), id);
    assert_eq!(
        on_laptop.restore_code().unwrap().unwrap().as_bytes(),
        b"restore"
    );
    assert_eq!(laptop.synced_restore_codes().unwrap()[0].0, id);
    assert!(StoreKeychain::new(laptop.clone(), id)
        .device_id()
        .unwrap()
        .is_none());
    // A refusal injected on one device refuses only that device's calls.
    phone.fail_next_operation();
    assert!(on_laptop.s3_credentials().unwrap().is_some());
}

#[test]
fn deleting_device_entries_keeps_the_persons_synced_entries() {
    let phone = Keychain::in_memory("discard").unwrap();
    let laptop = phone.another_device();
    let id = StoreId(Uuid::from_u128(1));
    saved(&phone, id);
    let device = StoreKeychain::new(phone.clone(), id);
    device.set_device_id(DeviceId(1)).unwrap();
    device
        .set_storage_credentials(&SecretBytes::new(b"credentials".to_vec()))
        .unwrap();
    device.delete_device_entries().unwrap();
    assert!(device.device_id().unwrap().is_none());
    assert!(device.storage_credentials().unwrap().is_none());
    for keychain in [&phone, &laptop] {
        let synced = SyncedKeychain::new(keychain.clone(), id);
        assert!(synced.restore_code().unwrap().is_some());
        assert!(synced.s3_credentials().unwrap().is_some());
    }
}

#[test]
fn leaving_deletes_the_persons_entries_for_that_store_only() {
    let phone = Keychain::in_memory("leave").unwrap();
    let laptop = phone.another_device();
    let left = StoreId(Uuid::from_u128(1));
    let kept = StoreId(Uuid::from_u128(2));
    let leaving = saved(&phone, left);
    saved(&phone, kept);
    let device = StoreKeychain::new(phone.clone(), left);
    device.set_device_id(DeviceId(1)).unwrap();
    leaving.delete_entries().unwrap();
    leaving.delete_entries().unwrap();
    let on_laptop = SyncedKeychain::new(laptop.clone(), left);
    assert!(on_laptop.restore_code().unwrap().is_none());
    assert!(on_laptop.s3_credentials().unwrap().is_none());
    assert_eq!(
        laptop
            .synced_restore_codes()
            .unwrap()
            .into_iter()
            .map(|(store, _)| store)
            .collect::<Vec<_>>(),
        vec![kept]
    );
    assert!(SyncedKeychain::new(laptop, kept)
        .s3_credentials()
        .unwrap()
        .is_some());
    // This device's own entries are not the person's to delete.
    assert_eq!(device.device_id().unwrap(), Some(DeviceId(1)));
}

#[test]
fn a_failed_leave_keeps_the_code_listed_and_retries() {
    for after in 0..2 {
        let phone = Keychain::in_memory("leave-failures").unwrap();
        let id = StoreId(Uuid::from_u128(1));
        let synced = saved(&phone, id);
        phone.fail_operations_after(&[after]);
        assert!(matches!(
            synced.delete_entries(),
            Err(KeyError::KeychainTemporarilyUnavailable(_))
        ));
        // The S3 key goes first: the code, which lists the store, goes last.
        assert!(synced.restore_code().unwrap().is_some());
        assert_eq!(synced.s3_credentials().unwrap().is_some(), after == 0);
        synced.delete_entries().unwrap();
        assert!(phone.synced_restore_codes().unwrap().is_empty());
        assert!(synced.s3_credentials().unwrap().is_none());
    }
}

#[test]
fn a_synced_refusal_lets_device_only_calls_through() {
    let phone = Keychain::in_memory("synced-refusal").unwrap();
    let id = StoreId(Uuid::from_u128(1));
    let synced = saved(&phone, id);
    let device = StoreKeychain::new(phone.clone(), id);
    phone.refuse_synced_operations(true);
    device.set_device_id(DeviceId(1)).unwrap();
    for _ in 0..2 {
        assert!(matches!(
            synced.delete_entries(),
            Err(KeyError::KeychainTemporarilyUnavailable(_))
        ));
    }
    phone.refuse_synced_operations(false);
    assert!(synced.s3_credentials().unwrap().is_some());
    synced.delete_entries().unwrap();
    assert_eq!(device.device_id().unwrap(), Some(DeviceId(1)));
}

/// Once Ben leaves, nothing sharing his phone's synced keychain can put his
/// S3 key or restore code back, even after a deletion that failed part way
/// (§14.1).
#[test]
fn after_leaving_the_synced_entries_are_never_written_again() {
    let phone = Keychain::in_memory("leaving").unwrap();
    let synced = saved(&phone, StoreId(Uuid::from_u128(1)));
    phone.fail_next_operation();
    assert!(synced.delete_entries().is_err());
    let key = SecretBytes::new(b"new S3 key".to_vec());
    assert!(matches!(
        synced.set_s3_credentials(&key),
        Err(KeyError::Left)
    ));
    assert!(matches!(synced.set_restore_code(&key), Err(KeyError::Left)));
    synced.delete_entries().unwrap();
    assert!(matches!(
        synced.set_s3_credentials(&key),
        Err(KeyError::Left)
    ));
    assert!(synced.s3_credentials().unwrap().is_none());
    assert!(synced.restore_code().unwrap().is_none());
}

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
        storage: coven_storage::RestoreStorage::location_only(StorageConfig::S3 {
            bucket: "household".into(),
            region: "us-east-1".into(),
            endpoint: None,
            profile: None,
            prefix: coven_storage::S3Prefix::parse("libraries").unwrap(),
        })
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

fn person() -> PersonEntries {
    PersonEntries::new(Keychain::in_memory("saved-code").unwrap(), store())
}

fn put(person: &PersonEntries, label: &str, slot: u64, code: &RestoreCode) {
    person
        .set_restore_code(&encode(person, label, slot, code).unwrap())
        .unwrap();
}

fn held(person: &PersonEntries) -> (String, u64, Vec<u8>) {
    let bytes = person.restore_code().unwrap().unwrap();
    let (saved, _) = read_saved_code(person.store(), bytes.as_bytes()).unwrap();
    (
        saved.name,
        saved.named_at,
        saved.code.to_bytes().unwrap().to_vec(),
    )
}

#[test]
fn saving_a_code_keeps_a_newer_saved_name() {
    let person = person();
    let ours = code(store());
    let bytes = ours.to_bytes().unwrap().to_vec();
    save(&person, &ours, &name("Household", 1)).unwrap();
    assert_eq!(held(&person), ("Household".into(), 1, bytes.clone()));
    // Another device of the person saw Family at slot 41.
    put(&person, "Family", 41, &code(store()));
    save(&person, &ours, &name("Household", 1)).unwrap();
    assert_eq!(held(&person), ("Family".into(), 41, bytes.clone()));
    save(&person, &ours, &name("Home", 50)).unwrap();
    assert_eq!(held(&person), ("Home".into(), 50, bytes));
}

#[test]
fn saving_a_code_replaces_an_unreadable_entry_or_one_for_another_store() {
    let person = person();
    let ours = code(store());
    person
        .set_restore_code(&SecretBytes::new(b"damaged".to_vec()))
        .unwrap();
    save(&person, &ours, &name("Household", 1)).unwrap();
    assert_eq!(held(&person).0, "Household");
    put(&person, "Elsewhere", 90, &code(StoreId(Uuid::from_u128(2))));
    save(&person, &ours, &name("Household", 1)).unwrap();
    assert_eq!(
        held(&person),
        ("Household".into(), 1, ours.to_bytes().unwrap().to_vec())
    );
}

/// Follow `name` where the entry must be readable or absent: this device's
/// code is never consulted.
fn follow(person: &PersonEntries, name: &SavedName) -> Result<(), KeyError> {
    follow_name(person, name, || -> Result<_, KeyError> {
        panic!("a readable or absent entry needs no current code")
    })
}

#[test]
fn following_a_name_rewrites_only_an_older_one_and_keeps_the_saved_code() {
    let person = person();
    // Absent stays absent: a forgotten entry is never recreated.
    follow(&person, &name("Family", 41)).unwrap();
    assert!(person.restore_code().unwrap().is_none());
    // The code another device saved, which this device never replaces.
    let saved = code(store());
    let bytes = saved.to_bytes().unwrap().to_vec();
    put(&person, "Household", 1, &saved);
    follow(&person, &name("Family", 41)).unwrap();
    assert_eq!(held(&person), ("Family".into(), 41, bytes.clone()));
    // A device that is behind leaves the newer name.
    follow(&person, &name("Household", 1)).unwrap();
    follow(&person, &name("Other", 41)).unwrap();
    assert_eq!(held(&person), ("Family".into(), 41, bytes));
}

#[test]
fn following_a_name_replaces_an_unreadable_entry_with_this_devices_code() {
    let keychain = Keychain::in_memory("saved-code").unwrap();
    let person = PersonEntries::new(keychain.clone(), store());
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
        person.set_restore_code(&unreadable).unwrap();
        follow_name(&person, &name("Family", 41), current).unwrap();
        assert_eq!(held(&person), ("Family".into(), 41, ours_bytes.clone()));
    }
    // Copies in two keychain groups become one.
    put(&person, "Household", 1, &code(store()));
    person.set_restore_code_in_another_group(&SecretBytes::new(b"old group".to_vec()));
    follow_name(&person, &name("Family", 41), current).unwrap();
    assert_eq!(held(&person), ("Family".into(), 41, ours_bytes));
    assert!(matches!(
        &keychain.person_restore_codes().unwrap()[..],
        [(_, PersonRestoreCode::Saved(_))]
    ));
}

#[test]
fn an_unreadable_entry_stays_when_this_device_has_no_current_code_or_reading_fails() {
    let person = person();
    let damaged = SecretBytes::new(b"damaged".to_vec());
    person.set_restore_code(&damaged).unwrap();
    follow_name(&person, &name("Family", 41), || Ok::<_, KeyError>(None)).unwrap();
    assert_eq!(
        person.restore_code().unwrap().unwrap().as_bytes(),
        b"damaged"
    );
    // Producing the code failed: the call fails and changes nothing.
    assert!(matches!(
        follow_name(&person, &name("Family", 41), || Err(KeyError::StoreClosed)),
        Err(KeyError::StoreClosed)
    ));
    assert_eq!(
        person.restore_code().unwrap().unwrap().as_bytes(),
        b"damaged"
    );
}

/// Another value of the same code, since codes erase their keys on drop.
fn code_copy(code: &RestoreCode) -> RestoreCode {
    RestoreCode::from_bytes(&code.to_bytes().unwrap()).unwrap()
}

#[test]
fn following_a_name_after_leaving_writes_nothing() {
    let person = person();
    put(&person, "Household", 1, &code(store()));
    person.delete_entries().unwrap();
    follow(&person, &name("Family", 41)).unwrap();
    assert!(person.restore_code().unwrap().is_none());
}

#[test]
fn a_saved_code_carrying_an_s3_key_is_unreadable() {
    let mut keyed = code(store());
    keyed.storage = coven_storage::RestoreStorage {
        location: coven_storage::RestoreStorage::decode(keyed.storage.as_bytes())
            .unwrap()
            .location,
        s3_key: Some(coven_storage::S3Credentials {
            access_key_id: "id".into(),
            secret_access_key: coven_crypto::SecretText::new("secret".into()),
        }),
    }
    .encode()
    .unwrap();
    let bytes = encode(&person(), "Household", 1, &keyed).unwrap();
    assert_eq!(
        read_saved_code(store(), bytes.as_bytes()).err(),
        Some(UnreadableSavedCode::Format)
    );
    let bytes = encode(&person(), "Household", 1, &code(store())).unwrap();
    read_saved_code(store(), bytes.as_bytes()).unwrap();
}

fn block_store_person() -> PersonEntries {
    PersonEntries::new(
        Keychain::in_memory_block_store("saved-code").unwrap(),
        store(),
    )
}

/// Block Store holds 1,024 bytes for an entry, its name included. A name too
/// long to fit is saved shortened at a character boundary, to exactly fill it.
#[test]
fn a_name_too_long_for_block_store_is_saved_shortened_to_fit() {
    let person = block_store_person();
    let limit = person.restore_code_limit().unwrap();
    let long = "é".repeat(512);
    let ours = code(store());
    save(&person, &ours, &name(&long, 1)).unwrap();
    let saved = person.restore_code().unwrap().unwrap();
    assert!(saved.as_bytes().len() <= limit);
    assert!(saved.as_bytes().len() + 1 >= limit);
    let (name, slot, bytes) = held(&person);
    assert!(long.starts_with(&name));
    assert!(name.len() < long.len());
    assert_eq!(slot, 1);
    assert_eq!(bytes, ours.to_bytes().unwrap().to_vec());
}

#[test]
fn a_name_that_fits_is_saved_whole_in_block_store() {
    let person = block_store_person();
    save(&person, &code(store()), &name("Household", 1)).unwrap();
    assert_eq!(held(&person).0, "Household");
}

#[test]
fn icloud_keychain_keeps_a_long_name_whole() {
    let person = person();
    let long = "é".repeat(512);
    save(&person, &code(store()), &name(&long, 1)).unwrap();
    assert_eq!(held(&person).0, long);
}

/// A rename follows into Block Store shortened the same way.
#[test]
fn a_followed_rename_is_shortened_to_fit_block_store() {
    let person = block_store_person();
    let ours = code(store());
    save(&person, &ours, &name("Household", 1)).unwrap();
    let long = "a".repeat(1000);
    follow_name::<SyncError>(&person, &name(&long, 41), || unreachable!()).unwrap();
    let (name, slot, bytes) = held(&person);
    assert!(long.starts_with(&name) && name.len() < long.len());
    assert_eq!(slot, 41);
    assert_eq!(bytes, ours.to_bytes().unwrap().to_vec());
}

/// A code whose location alone doesn't fit can't be saved at all.
#[test]
fn a_code_too_long_for_block_store_even_unnamed_fails() {
    let person = block_store_person();
    let mut ours = code(store());
    ours.storage = coven_storage::RestoreStorage::location_only(StorageConfig::S3 {
        bucket: "household".into(),
        region: "us-east-1".into(),
        endpoint: None,
        profile: None,
        prefix: coven_storage::S3Prefix::parse(&vec!["p".repeat(200); 5].join("/")).unwrap(),
    })
    .encode()
    .unwrap();
    assert!(matches!(
        save(&person, &ours, &name("Household", 1)),
        Err(SyncError::SecureStorage(KeyError::SavedEntryTooLarge))
    ));
    assert!(person.restore_code().unwrap().is_none());
}

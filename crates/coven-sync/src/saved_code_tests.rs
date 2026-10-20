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
        storage: SecretBytes::new(b"location".to_vec()),
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
    let saved = read_saved_code(synced.store(), bytes.as_bytes()).unwrap();
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

#[test]
fn following_a_name_rewrites_only_an_older_one_and_keeps_the_saved_code() {
    let synced = synced();
    // Absent stays absent: a forgotten entry is never recreated.
    follow_name(&synced, &name("Family", 41)).unwrap();
    assert!(synced.restore_code().unwrap().is_none());
    // The code another device saved, which this device never replaces.
    let saved = code(store());
    let bytes = saved.to_bytes().unwrap().to_vec();
    put(&synced, "Household", 1, &saved);
    follow_name(&synced, &name("Family", 41)).unwrap();
    assert_eq!(held(&synced), ("Family".into(), 41, bytes.clone()));
    // A device that is behind leaves the newer name.
    follow_name(&synced, &name("Household", 1)).unwrap();
    follow_name(&synced, &name("Other", 41)).unwrap();
    assert_eq!(held(&synced), ("Family".into(), 41, bytes));
}

#[test]
fn following_a_name_refuses_an_entry_without_a_code_to_keep() {
    let synced = synced();
    synced
        .set_restore_code(&SecretBytes::new(b"damaged".to_vec()))
        .unwrap();
    assert!(matches!(
        follow_name(&synced, &name("Family", 41)),
        Err(KeyError::Material(MaterialError::Encoding))
    ));
    put(&synced, "Elsewhere", 1, &code(StoreId(Uuid::from_u128(2))));
    assert!(matches!(
        follow_name(&synced, &name("Family", 41)),
        Err(KeyError::RestoreCodeStoreId)
    ));
    assert_eq!(held_name(&synced), "Elsewhere");
}

fn held_name(synced: &SyncedKeychain) -> String {
    let bytes = synced.restore_code().unwrap().unwrap();
    KeychainRestoreCode::from_bytes(bytes.as_bytes())
        .unwrap()
        .name
}

#[test]
fn following_a_name_after_leaving_writes_nothing() {
    let synced = synced();
    put(&synced, "Household", 1, &code(store()));
    synced.delete_entries().unwrap();
    follow_name(&synced, &name("Family", 41)).unwrap();
    assert!(synced.restore_code().unwrap().is_none());
}

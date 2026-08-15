use coven_keys::keys::{test_keyring, StoreKeys};

#[test]
fn coven_forgets_a_closed_store_master_key_without_exposing_store_keys() {
    test_keyring::install();
    let store_id = "closed-store-master-key";
    let keys = StoreKeys::bind(store_id.to_string());
    keys.set_encryption_key(&"11".repeat(32))
        .expect("seed master key");

    crate::Coven::forget_keyring_master_key(store_id).expect("forget master key");

    assert_eq!(keys.get_encryption_key().expect("read master key"), None);
}

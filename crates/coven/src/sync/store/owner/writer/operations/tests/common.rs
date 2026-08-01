use super::*;

#[test]
fn store_sequence_exhaustion_fails_instead_of_reusing_the_last_sequence() {
    assert!(matches!(
        successor_store_sequence(u64::MAX),
        Err(StoreError::SequenceExhausted { current: u64::MAX })
    ));
}

pub(super) async fn initialize_exact_store(
    db: &Database,
    storage: &Arc<CloudSyncStorage>,
    store_id: &str,
    keypair: &UserKeypair,
) -> (StoreRootRef, String) {
    let root = create_exact_test_store(db, storage, store_id, keypair)
        .await
        .expect("create exact test Store");
    let device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read local device id")
        .expect("exact Store has an activated local device");
    (root, device_id)
}

pub(super) async fn remove_exact_store_root(db: &Database) {
    db.test_sql(|database| database.remove_store_protocol_root())
        .await
        .expect("remove exact Store root authority");
}

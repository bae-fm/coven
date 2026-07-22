use super::*;

#[test]
fn store_sequence_exhaustion_fails_instead_of_reusing_the_last_sequence() {
    assert!(matches!(
        successor_store_sequence(u64::MAX),
        Err(StoreOutboundError::SequenceExhausted { current: u64::MAX })
    ));
}

pub(super) async fn initialize_exact_store(
    db: &Database,
    storage: &CloudSyncStorage,
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

pub(super) async fn local_device_id(db: &Database) -> String {
    db.get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read local device id")
        .expect("local device id exists")
}

pub(super) async fn remove_exact_store_root(db: &Database) {
    db.call(|connection| {
        connection
            .execute("DELETE FROM store_protocol_root_authority", [])
            .map(|_| ())
            .map_err(crate::database::DbError::from)
    })
    .await
    .expect("remove exact Store root authority");
}

pub(super) async fn reinstall_exact_store_root(
    db: &Database,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
) {
    let verified = crate::sync::store_objects::load_store_protocol_root(storage, root)
        .await
        .expect("load exact Store root authority");
    db.install_store_root_authority(root.clone(), verified.bytes)
        .await
        .expect("reinstall exact Store root authority");
}

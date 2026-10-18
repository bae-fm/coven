use super::*;

#[test]
fn settings_check_only_the_store_identity() {
    let store = StoreIdentity {
        id: StoreId(uuid::Uuid::from_u128(1)),
        name: "Renamed".into(),
    };
    let mut settings = coven_foundation::files::StoreSettings {
        id: store.id,
        device_id: DeviceId(2),
    };
    store.check_settings(&settings).unwrap();
    settings.id = StoreId(uuid::Uuid::from_u128(3));
    assert!(
        matches!(store.check_settings(&settings), Err(DbError::StoreSettingsMismatch { store: id }) if id == store.id)
    );
}

use super::*;

#[test]
fn registration_transport_source_is_offline() {
    let error = crate::sync::store::StoreRegistrationError::Object(
        coven_protocol::objects::StoreObjectError::Storage(
            coven_protocol::objects::StorageError::Storage("provider unavailable".to_string()),
        ),
    );

    let object = std::error::Error::source(&error).expect("object source");
    assert!(object
        .downcast_ref::<coven_protocol::objects::StoreObjectError>()
        .is_some());
    let storage = object.source().expect("storage source");
    assert!(storage
        .downcast_ref::<coven_protocol::objects::StorageError>()
        .is_some());

    assert!(SyncCycleFailure::operation("register", error).is_offline());
}

#[test]
fn registration_configuration_source_is_failed() {
    let error = crate::sync::store::StoreRegistrationError::Object(
        coven_protocol::objects::StoreObjectError::Storage(
            coven_protocol::objects::StorageError::Configuration("missing bucket".to_string()),
        ),
    );

    assert!(!SyncCycleFailure::operation("register", error).is_offline());
}

use super::*;

#[test]
fn deserialization_rejects_empty_slot_components() {
    assert!(serde_json::from_str::<ObjectSlot>(
        r#"{"logical_key":"","physical":{"kind":"logical_key"}}"#
    )
    .is_err());
    assert!(serde_json::from_str::<ObjectSlot>(
        r#"{"logical_key":"object","physical":{"kind":"opaque","value":""}}"#
    )
    .is_err());
}

#[test]
fn logical_key_requirement_accepts_logical_slots() {
    let slot = ObjectSlot::logical("protocol/object".to_string()).expect("valid slot");

    slot.require_logical_key_for("S3")
        .expect("logical slot must be accepted");
}

#[test]
fn logical_key_requirement_rejects_opaque_slots() {
    let slot = ObjectSlot::opaque(
        "protocol/object".to_string(),
        "provider-object-id".to_string(),
    )
    .expect("valid slot");

    let error = slot
        .require_logical_key_for("S3")
        .expect_err("opaque slot must be rejected");

    assert_eq!(
        error.to_string(),
        "storage configuration is invalid: S3 slot for protocol/object must use its logical key"
    );
}

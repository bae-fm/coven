use super::*;

#[test]
fn invite_identity_serializes_as_its_uuid() {
    let uuid = Uuid::from_u128(0x00112233_4455_6677_8899_aabbccddeeff);
    let invite = InviteId(uuid);
    let encoded = serde_json::to_string(&invite).unwrap();
    assert_eq!(encoded, serde_json::to_string(&uuid).unwrap());
    assert_eq!(serde_json::from_str::<InviteId>(&encoded).unwrap(), invite);
    assert_eq!(invite.to_string(), "00112233-4455-6677-8899-aabbccddeeff");
    assert!(serde_json::from_str::<InviteId>("\"01\"").is_err());
}

#[test]
fn circle_identity_serializes_as_its_uuid() {
    let uuid = Uuid::from_u128(0x00112233_4455_6677_8899_aabbccddeeff);
    let circle = CircleId(uuid);
    let encoded = serde_json::to_string(&circle).unwrap();
    assert_eq!(encoded, serde_json::to_string(&uuid).unwrap());
    assert_eq!(serde_json::from_str::<CircleId>(&encoded).unwrap(), circle);
    assert_eq!(circle.to_string(), "00112233-4455-6677-8899-aabbccddeeff");
    assert!(serde_json::from_str::<CircleId>("\"01\"").is_err());
}

#[test]
fn production_ids_are_uuidv4_and_distinct() {
    let ids: IdSourceRef = Arc::new(UuidIds);
    let first = ids.new_id();
    let second = ids.new_id();
    assert_eq!(first.get_version_num(), 4);
    assert_eq!(second.get_version_num(), 4);
    assert_ne!(first, second);
    assert_ne!(ids.new_device_id(), ids.new_device_id());
}

#[test]
fn device_ids_use_payload_bits_instead_of_time_version_or_variant() {
    struct SuppliedId(Uuid);
    impl IdSource for SuppliedId {
        fn new_id(&self) -> Uuid {
            self.0
        }
    }
    for prefix in [0, 0xffff_ffff_ffff_7000, 0x1234_5678_9abc_4000] {
        let ids = SuppliedId(Uuid::from_u64_pair(prefix | 3, 0xbfff_ffff_ffff_ffff));
        assert_eq!(ids.new_device_id(), DeviceId(u64::MAX));
    }
    let ids = SuppliedId(Uuid::from_u64_pair(0x7000, 0x8000_0000_0000_0000));
    assert_eq!(ids.new_device_id(), DeviceId(0));
}

#[cfg(feature = "test-utils")]
#[test]
fn sequential_source_shares_one_sequence_between_uuid_and_device_ids() {
    let ids: IdSourceRef = Arc::new(SequentialIds::new());
    assert_eq!(
        ids.new_id(),
        Uuid::from_u64_pair(0x7000, 0x8000_0000_0000_0001)
    );
    assert_eq!(ids.new_device_id(), DeviceId(2));
    assert_eq!(ids.new_device_id(), DeviceId(3));
    assert_eq!(SequentialIds::new().new_device_id(), DeviceId(1));
}

#[cfg(feature = "test-utils")]
#[test]
fn sequential_device_ids_preserve_high_bits() {
    let ids = SequentialIds(std::sync::atomic::AtomicU64::new(1 << 62));
    assert_eq!(ids.new_device_id(), DeviceId(1 << 62));
}

#[cfg(feature = "test-utils")]
#[test]
#[should_panic(expected = "sequential id source exhausted")]
fn sequential_source_never_wraps() {
    SequentialIds(std::sync::atomic::AtomicU64::new(u64::MAX)).new_id();
}

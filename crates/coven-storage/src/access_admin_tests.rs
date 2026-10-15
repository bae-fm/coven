//! Key names that carry the store and the minting device (§4, §13).
use super::*;

fn store(n: u128) -> StoreId {
    StoreId(uuid::Uuid::from_u128(n))
}

#[test]
fn a_key_name_carries_its_store_and_minting_device() {
    let scope = MintScope::new(
        store(0x0123_4567_89ab_cdef_0011_2233_4455_6677),
        "member-id",
        DeviceId(0xfeed_0000_0000_0042),
        uuid::Uuid::from_u128(0xaaaa_bbbb_cccc_dddd_eeee_ffff_0000_1111),
    );
    assert_eq!(scope.name, "coven-aaaabbbbccccddddeeeeffff");
    assert_eq!(
        scope.key_name,
        "coven-0123456789abcdef00112233-feed000000000042-aaaabbbbccccdddd"
    );
    assert_eq!(scope.key_name.len(), 64);
    assert_eq!(
        scope.label(),
        format!("{} member member-id", scope.key_name)
    );
    for text in [scope.key_name.clone(), scope.label()] {
        assert_eq!(
            named_device(&text, scope.store),
            Some(DeviceId(0xfeed_0000_0000_0042))
        );
    }
}

#[test]
fn other_stores_and_other_names_name_no_device() {
    let scope = MintScope::new(store(1 << 100), "m", DeviceId(7), uuid::Uuid::from_u128(9));
    // Another store differing in the digits the name carries.
    assert_eq!(named_device(&scope.key_name, store(2 << 100)), None);
    for text in [
        "",
        "coven",
        "admin",
        scope.name.as_str(),
        &scope.key_name[..63],
        &format!("{}0", scope.key_name),
        &format!("{}-0", scope.key_name),
        &scope.key_name.to_uppercase(),
        &scope.key_name.replace("coven-", "other-"),
    ] {
        assert_eq!(named_device(text, scope.store), None, "{text}");
    }
}

use super::*;

#[test]
fn stored_paths_round_trip_through_serde_and_refuse_aliases() {
    let path = ObjectPath::parse("10101010-1010-1010-1010-101010101010/devices/42/3").unwrap();
    let json = serde_json::to_vec(&path).unwrap();
    assert_eq!(serde_json::from_slice::<ObjectPath>(&json).unwrap(), path);
    assert!(serde_json::from_str::<ObjectPath>(
        r#""10101010-1010-1010-1010-101010101010/devices/042/3""#
    )
    .is_err());
}

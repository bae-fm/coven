use super::*;

#[test]
fn stored_paths_round_trip_through_serde_and_refuse_aliases() {
    let path = ObjectPath::parse("devices/42/3").unwrap();
    let json = serde_json::to_vec(&path).unwrap();
    assert_eq!(serde_json::from_slice::<ObjectPath>(&json).unwrap(), path);
    assert!(serde_json::from_str::<ObjectPath>(r#""devices/042/3""#).is_err());
}

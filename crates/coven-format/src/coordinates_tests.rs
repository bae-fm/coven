use super::*;

const ID: &str = "00112233-4455-6677-8899-aabbccddeeff";
const KEY: &str = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";

#[test]
fn coordinates_name_storage_or_one_device_in_one_spelling() {
    let cloud = format!("file 4242 {ID} {KEY}");
    let Coordinates::Cloud(reference) = Coordinates::decode(&cloud).unwrap() else {
        panic!("cloud coordinates");
    };
    assert_eq!(reference.device, DeviceId(4242));
    assert_eq!(Coordinates::Cloud(reference).encode().as_str(), cloud);
    for device in [0, 1, 4242, u64::MAX] {
        let text = format!("device {device}");
        let decoded = Coordinates::decode(&text).unwrap();
        assert!(matches!(decoded, Coordinates::DeviceOnly(DeviceId(d)) if d == device));
        assert_eq!(decoded.encode().as_str(), text);
    }
}

#[test]
fn coordinates_refuse_other_spellings() {
    for bad in [
        String::new(),
        "device".into(),
        "device ".into(),
        "device 01".into(),
        "device +1".into(),
        "device -1".into(),
        "device 1 ".into(),
        " device 1".into(),
        "Device 1".into(),
        "device  1".into(),
        "device 18446744073709551616".into(),
        format!("device 1 {ID}"),
        format!("file 1 {ID}"),
        format!("on 1 {ID} {KEY}"),
    ] {
        assert!(Coordinates::decode(&bad).is_err(), "{bad:?}");
    }
}

#[test]
fn coordinate_diagnostics_never_disclose_keys() {
    let text = format!("file 1 {ID} {KEY}");
    let decoded = Coordinates::decode(&text).unwrap();
    assert!(!format!("{decoded:?}").contains(KEY));
    assert!(!format!("{:?}", decoded.encode()).contains(KEY));
}

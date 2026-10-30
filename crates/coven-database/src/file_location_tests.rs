use super::*;
use coven_foundation::id_source::SequentialIds;

#[test]
fn coordinates_cells_decode_to_one_place_and_encode_back() {
    let ids = SequentialIds::new();
    let location = FileLocation::new(DeviceId(4242), &ids).unwrap();
    assert_eq!(location.uploader(), DeviceId(4242));
    let cloud = Place::Cloud(location.clone());
    assert_eq!(Place::decode(&cloud.value()).unwrap(), cloud);
    let device = Place::DeviceOnly(DeviceId(7));
    assert_eq!(device.value(), Value::Text("device 7".into()));
    assert_eq!(Place::decode(&device.value()).unwrap(), device);
    for bad in [
        Value::Null,
        Value::Integer(7),
        Value::Text("device 07".into()),
        Value::Text("on 7".into()),
    ] {
        assert!(matches!(Place::decode(&bad), Err(DbError::DamagedDatabase)));
    }
}

#[test]
fn a_new_location_is_a_new_object_and_key() {
    let ids = SequentialIds::new();
    let first = FileLocation::new(DeviceId(1), &ids).unwrap();
    let second = FileLocation::new(DeviceId(1), &ids).unwrap();
    assert_ne!(first.object(), second.object());
    assert_ne!(first, second);
    assert_eq!(FileLocation::decode(first.text()).unwrap(), first);
    assert!(!format!("{first:?}").contains(&first.text()[60..]));
}

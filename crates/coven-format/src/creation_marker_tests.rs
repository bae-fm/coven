use super::*;
use crate::test_utils::STORE_ID;
use crate::tests::{hex, mutations};

#[test]
fn creation_marker_fixture_pins_its_store_and_device() {
    let bytes = hex(include_str!("../fixtures/creation-marker.hex"));
    let marker = CreationMarker {
        store: STORE_ID,
        device: DeviceId(1),
    };
    assert_eq!(marker.encode().unwrap(), bytes);
    assert_eq!(CreationMarker::decode(&bytes).unwrap(), marker);
    assert_ne!(
        CreationMarker {
            device: DeviceId(2),
            ..marker
        }
        .encode()
        .unwrap(),
        bytes
    );
}

#[test]
fn creation_marker_decodes_only_its_exact_layout() {
    let bytes = hex(include_str!("../fixtures/creation-marker.hex"));
    mutations(&bytes, |changed| {
        if let Ok(marker) = CreationMarker::decode(changed) {
            assert_eq!(marker.encode().unwrap(), changed);
        }
    });
    let mut other_kind = bytes.clone();
    other_kind[0] = 40;
    assert!(CreationMarker::decode(&other_kind).is_err());
    let mut newer = bytes.clone();
    newer[2] = 2;
    assert_eq!(
        CreationMarker::decode(&newer),
        Err(Error::UnsupportedVersion(2))
    );
    assert_eq!(
        CreationMarker::decode(&bytes[..bytes.len() - 1]),
        Err(Error::Truncated)
    );
    let mut longer = bytes;
    longer.push(0);
    assert_eq!(CreationMarker::decode(&longer), Err(Error::TrailingBytes));
}

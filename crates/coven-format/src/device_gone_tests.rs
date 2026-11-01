use super::*;

#[test]
fn a_marker_round_trips_and_is_never_a_request_or_an_invites_marker() {
    let bytes = DeviceGone.encode();
    assert_eq!(bytes, [40, 0, 1]);
    assert_eq!(DeviceGone::decode(&bytes).unwrap(), DeviceGone);
    let ended = crate::invite_end::InviteEnd { kept_until: 90_000 }.encode();
    assert!(matches!(DeviceGone::decode(&ended), Err(Error::UnknownTag { tag: 37, .. })));
    // A stored file's header starts with kind 38.
    assert!(matches!(DeviceGone::decode(&[38, 0, 1]), Err(Error::UnknownTag { tag: 38, .. })));
    let request = crate::sealed_single::SingleChunkPrefix::JoinRequest.encode().unwrap();
    assert!(matches!(DeviceGone::decode(&request), Err(Error::UnknownTag { tag: 36, .. })));
    assert!(matches!(
        DeviceGone::decode(&[bytes.clone(), vec![0]].concat()),
        Err(Error::TrailingBytes)
    ));
    assert!(DeviceGone::decode(&bytes[..2]).is_err());
}

#[test]
fn a_marker_matches_its_independently_generated_bytes() {
    let pinned = crate::tests::hex(include_str!("../fixtures/device-gone.hex"));
    assert_eq!(DeviceGone.encode(), pinned);
    assert_eq!(DeviceGone::decode(&pinned).unwrap(), DeviceGone);
}

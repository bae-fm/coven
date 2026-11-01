use super::*;

#[test]
fn a_marker_round_trips_and_is_never_a_join_request() {
    let marker = InviteEnd { kept_until: 90_000 };
    let bytes = marker.encode();
    assert_eq!(bytes, [37, 0, 1, 0, 0, 0, 0, 0, 1, 0x5f, 0x90]);
    assert_eq!(InviteEnd::decode(&bytes).unwrap(), marker);
    let request = crate::sealed_single::SingleChunkPrefix::JoinRequest.encode().unwrap();
    assert!(matches!(InviteEnd::decode(&request), Err(Error::UnknownTag { tag: 36, .. })));
    assert!(matches!(
        InviteEnd::decode(&[bytes.clone(), vec![0]].concat()),
        Err(Error::TrailingBytes)
    ));
    assert!(InviteEnd::decode(&bytes[..10]).is_err());
}

#[test]
fn a_marker_matches_its_independently_generated_bytes() {
    let pinned = crate::tests::hex(include_str!("../fixtures/invite-end.hex"));
    let marker = InviteEnd { kept_until: 1_700_000_000 + 7 * 24 * 3600 };
    assert_eq!(marker.encode(), pinned);
    assert_eq!(InviteEnd::decode(&pinned).unwrap(), marker);
}

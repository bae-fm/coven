use super::*;
use coven_format::objects::Joiner as Wire;

#[test]
fn the_apps_two_joiners_round_trip_through_the_formats() {
    for joiner in [Joiner::NewPerson { name: "Carol".into() }, Joiner::MemberKeys] {
        let wire: Wire = joiner.clone().into();
        assert_eq!(wire.admits(), Some(joiner.admits()));
        assert_eq!(Joiner::try_from(wire).unwrap(), joiner);
    }
}

#[test]
fn a_members_own_device_is_not_an_invites_joiner() {
    assert!(matches!(
        Joiner::try_from(Wire::OwnDevice),
        Err(crate::SyncError::Format(coven_format::Error::UnknownTag { tag: 2, .. }))
    ));
}

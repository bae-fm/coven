use super::*;
use crate::test_utils;
use crate::Object;

fn new_person(person: &str) -> JoinRequest {
    JoinRequest { joiner: Joiner::NewPerson { name: person.into() }, ..test_utils::join_request() }
}

#[test]
fn a_new_persons_request_carries_the_name_they_typed_after_the_device_name() {
    let request = new_person("Dan");
    let bytes = Object::JoinRequest(request.clone()).encode().unwrap();
    // The frame ends with the joiner: tag 1, then the name.
    assert!(bytes.ends_with(b"\x01\0\0\0\x03Dan"));
    assert_eq!(Object::decode(&bytes).unwrap(), Object::JoinRequest(request.clone()));
    assert_eq!(request.joiner.kind(), Some(InviteKind::NewPerson));
}

#[test]
fn a_recoverys_request_says_so_and_carries_no_name() {
    let request = test_utils::join_request();
    let bytes = Object::JoinRequest(request.clone()).encode().unwrap();
    // The frame ends with the joiner: tag 0 and nothing after it.
    assert!(bytes.ends_with(b"\0\0\0\x01D\0"));
    assert_eq!(Object::decode(&bytes).unwrap(), Object::JoinRequest(request.clone()));
    assert_eq!(request.joiner.kind(), Some(InviteKind::Recovery));
}

#[test]
fn a_request_refuses_an_empty_name_and_an_unknown_joiner() {
    assert!(Object::JoinRequest(new_person("")).encode().is_err());
    let mut bytes = Object::JoinRequest(test_utils::join_request()).encode().unwrap();
    *bytes.last_mut().unwrap() = 3;
    assert_eq!(Object::decode(&bytes).err(), Some(Error::UnknownTag { field: "joiner", tag: 3 }));
    // A new person's tag with no name after it is cut short.
    *bytes.last_mut().unwrap() = 1;
    assert!(Object::decode(&bytes).is_err());
}

#[test]
fn a_pairings_request_says_it_is_the_members_own_device_and_carries_no_name() {
    let request = test_utils::pairing_request();
    let bytes = Object::JoinRequest(request.clone()).encode().unwrap();
    // The frame ends with the joiner: tag 2 and nothing after it.
    assert!(bytes.ends_with(b"\0\0\0\x01D\x02"));
    assert_eq!(Object::decode(&bytes).unwrap(), Object::JoinRequest(request.clone()));
    // No invite has this kind, so an invite's kind check refuses it.
    assert_eq!(request.joiner.kind(), None);
}

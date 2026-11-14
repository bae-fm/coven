use super::*;
use crate::test_utils;
use crate::Object;

fn joining_circle() -> JoiningCircle {
    match test_utils::join_request().joiner {
        Joiner::MemberKeys { circle } => circle,
        _ => unreachable!("the fixture request recovers a member"),
    }
}

fn new_person(person: &str) -> JoinRequest {
    JoinRequest {
        joiner: Joiner::NewPerson { name: person.into(), circle: joining_circle() },
        ..test_utils::join_request()
    }
}

/// The request's circle: what the admission repeats, then the key's copy.
fn circle_bytes() -> Vec<u8> {
    let circle = joining_circle();
    let routing = &circle.copy.routing;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(circle.circle.circle.0.as_bytes());
    bytes.extend_from_slice(b"\0\0\0\x01R");
    bytes.extend_from_slice(&2u32.to_be_bytes());
    bytes.extend_from_slice(circle.circle.key.0.as_bytes());
    bytes.extend_from_slice(&circle.circle.key_hash);
    // The box: its routing, the ephemeral key and nonce, the ciphertext's
    // length, the ciphertext and the tag.
    bytes.push(1);
    bytes.extend_from_slice(circle.circle.circle.0.as_bytes());
    bytes.extend_from_slice(routing.key.0.as_bytes());
    bytes.extend_from_slice(routing.member.0.as_bytes());
    bytes.extend_from_slice(routing.sealing.as_bytes());
    bytes.extend_from_slice(&circle.copy.sealed[..coven_crypto::KEY_BOX_HEAD_LEN]);
    bytes.extend_from_slice(&64u32.to_be_bytes());
    bytes.extend_from_slice(&circle.copy.sealed[coven_crypto::KEY_BOX_HEAD_LEN..]);
    bytes
}

#[test]
fn a_new_persons_request_carries_the_name_they_typed_then_their_circle() {
    let request = new_person("Dan");
    let bytes = Object::JoinRequest(request.clone()).encode().unwrap();
    // The frame ends with the joiner: tag 1, the name, then the circle.
    assert!(bytes.ends_with(&[b"\x01\0\0\0\x03Dan".as_slice(), &circle_bytes()].concat()));
    assert_eq!(Object::decode(&bytes).unwrap(), Object::JoinRequest(request.clone()));
    assert_eq!(request.joiner.admits(), Some(InviteAdmits::NewPerson));
}

#[test]
fn a_recoverys_request_says_so_and_carries_no_name_but_a_circle() {
    let request = test_utils::join_request();
    let bytes = Object::JoinRequest(request.clone()).encode().unwrap();
    // The frame ends with the joiner: tag 0, then the circle.
    assert!(bytes.ends_with(&[b"\0\0\0\x01D\0".as_slice(), &circle_bytes()].concat()));
    assert_eq!(Object::decode(&bytes).unwrap(), Object::JoinRequest(request.clone()));
    assert_eq!(request.joiner.admits(), Some(InviteAdmits::MemberKeys));
    assert_eq!(request.joiner.circle(), Some(&joining_circle()));
}

#[test]
fn a_request_refuses_an_empty_name_and_an_unknown_joiner() {
    assert!(Object::JoinRequest(new_person("")).encode().is_err());
    let bytes = Object::JoinRequest(test_utils::join_request()).encode().unwrap();
    // The joiner's tag sits before its circle, whose length is fixed.
    let tag = bytes.len() - circle_bytes().len() - 1;
    let mut unknown = bytes.clone();
    unknown[tag] = 3;
    assert_eq!(Object::decode(&unknown).err(), Some(Error::UnknownTag { field: "joiner", tag: 3 }));
    // A new person's tag with no name after it is cut short.
    let mut cut = bytes;
    cut[tag] = 1;
    assert!(Object::decode(&cut).is_err());
}

/// The copy is the key its request's circle commits to, for that circle:
/// a copy routed to another circle or key is no request's (§13.2).
#[test]
fn a_request_whose_copy_is_another_circles_or_keys_is_refused() {
    let mut other_circle = joining_circle();
    other_circle.copy.routing.audience = coven_merge::Audience::Circle(
        coven_foundation::id_source::CircleId(uuid::Uuid::from_bytes([1; 16])),
    );
    let mut other_key = joining_circle();
    other_key.copy.routing.key =
        coven_foundation::id_source::KeyId(uuid::Uuid::from_bytes([1; 16]));
    let mut store_copy = joining_circle();
    store_copy.copy.routing.audience = coven_merge::Audience::Store;
    for circle in [other_circle, other_key, store_copy] {
        let request =
            JoinRequest { joiner: Joiner::MemberKeys { circle }, ..test_utils::join_request() };
        assert!(Object::JoinRequest(request).encode().is_err());
    }
}

/// Every truncation and bit flip of a request fails without a panic.
#[test]
#[expect(
    clippy::let_underscore_must_use,
    reason = "the mutations check that decoding never panics; any result is fine"
)]
fn a_damaged_request_fails_to_decode_without_panicking() {
    let bytes = Object::JoinRequest(test_utils::join_request()).encode().unwrap();
    assert_eq!(Object::decode(&bytes).unwrap(), Object::JoinRequest(test_utils::join_request()));
    for length in 0..bytes.len() {
        assert!(Object::decode(&bytes[..length]).is_err());
    }
    for index in 0..bytes.len() {
        let mut changed = bytes.clone();
        changed[index] ^= 1;
        let _ = Object::decode(&changed);
    }
}

#[test]
fn a_pairings_request_says_it_is_the_members_own_device_and_carries_no_name() {
    let request = test_utils::pairing_request();
    let bytes = Object::JoinRequest(request.clone()).encode().unwrap();
    // The frame ends with the joiner: tag 2 and nothing after it.
    assert!(bytes.ends_with(b"\0\0\0\x01D\x02"));
    assert_eq!(Object::decode(&bytes).unwrap(), Object::JoinRequest(request.clone()));
    // No invite has this kind, so an invite's kind check refuses it.
    assert_eq!(request.joiner.admits(), None);
}

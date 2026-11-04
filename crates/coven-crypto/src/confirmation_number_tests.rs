use super::*;
use crate::{SealingKey, SigningKey};

fn keys(hex: &str) -> MemberPublicKeys {
    let bytes = hex::decode(hex).unwrap();
    MemberPublicKeys {
        signing: SigningKey::from_bytes(bytes[..32].try_into().unwrap()).unwrap(),
        sealing: SealingKey::from_bytes(bytes[32..].try_into().unwrap()),
    }
}

#[test]
fn numbers_are_pinned_for_the_fixture_pairs() {
    // fixtures/generate-deterministic.py calculates each case from format.md D11:
    // an invite id, the request's `MemberKeys` and the number they give.
    let mut cases = Vec::new();
    for line in include_str!("../../coven-format/fixtures/confirmation-numbers.txt").lines() {
        let [invite, member_keys, number] = line.split(' ').collect::<Vec<_>>()[..] else {
            panic!("a case is `<invite> <keys> <number>`: {line}");
        };
        let invite = InviteId(uuid::Uuid::from_slice(&hex::decode(invite).unwrap()).unwrap());
        let number = ConfirmationNumber(number.parse().unwrap());
        assert_eq!(ConfirmationNumber::of(invite, &keys(member_keys)), number);
        cases.push((invite, member_keys, number));
    }
    assert_eq!(cases.len(), 3);
    // Another invite answered by the same keys has its own number.
    let [(first, first_keys, first_number), _, (other, other_keys, other_number)] = cases[..]
    else {
        unreachable!()
    };
    assert_ne!(first, other);
    assert_eq!(first_keys, other_keys);
    assert_ne!(first_number, other_number);
}

#[test]
fn numbers_display_as_two_groups_of_three_digits() {
    assert_eq!(ConfirmationNumber(41_778).to_string(), "041 778");
    assert_eq!(ConfirmationNumber(0).to_string(), "000 000");
    assert_eq!(ConfirmationNumber(999_999).to_string(), "999 999");
}

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
    // Calculated independently with Python's hashlib and libsodium: the
    // public pairs of signing and sealing seeds 0x33 and 0x34, answering the
    // invite 0x55 repeated (format.md D11).
    let invite = InviteId(uuid::Uuid::from_bytes([0x55; 16]));
    let original = keys(
        "17cb79fb2b4120f2b1ec65e4198d6e08b28e813feb01e4a400839b85e18080ce\
         7b0d47d93427f8311160781c7c733fd89f88970aef490d8aa0ee19a4cb8a1b14",
    );
    let replacement = keys(
        "eb2cf13bf7ae3a5f614168a1fb09272d39771ec4b8523727815e466caf8ee163\
         ffc951aa6f2fa03096d1d1b579735b2f6f84019fe2f617aa65ff3d68705f2527",
    );
    assert_eq!(ConfirmationNumber::of(invite, &original), ConfirmationNumber(181_205));
    assert_eq!(ConfirmationNumber::of(invite, &replacement), ConfirmationNumber(246_716));
    // Another invite answered by the same keys has its own number.
    let other = InviteId(uuid::Uuid::from_bytes([0x56; 16]));
    assert_ne!(ConfirmationNumber::of(other, &original), ConfirmationNumber(181_205));
}

#[test]
fn numbers_display_as_two_groups_of_three_digits() {
    assert_eq!(ConfirmationNumber(41_778).to_string(), "041 778");
    assert_eq!(ConfirmationNumber(0).to_string(), "000 000");
    assert_eq!(ConfirmationNumber(999_999).to_string(), "999 999");
}

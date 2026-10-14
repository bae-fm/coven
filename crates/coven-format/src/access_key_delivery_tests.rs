use super::*;

fn key() -> DeliveredKey {
    DeliveredKey {
        access_key_id: "AKIAMEMBER".into(),
        secret_access_key: SecretText::new("member-secret".into()),
        principal: S3Principal::IamUser {
            user: "coven-0123".into(),
            policy: crate::IamPolicy::Inline {
                name: "coven-member".into(),
            },
        },
    }
}

#[test]
fn a_delivery_opens_only_for_its_member_and_names_its_signer() {
    let admin = MemberKeys::generate().unwrap();
    let ana = MemberKeys::generate().unwrap();
    let ben = MemberKeys::generate().unwrap();
    let path = "s/access-keys/a/1";
    let bytes = seal(
        &admin,
        &ana.member_id(),
        &ana.sealing_public_key(),
        path,
        &key(),
    )
    .unwrap();
    let routed = routing(&bytes).unwrap();
    assert_eq!(routed.member, ana.member_id());
    assert_eq!(routed.author, admin.member_id());
    let opened = open(&ana, path, &bytes).unwrap();
    assert_eq!(opened.access_key_id, "AKIAMEMBER");
    assert_eq!(opened.secret_access_key.as_str(), "member-secret");
    assert_eq!(opened.principal, key().principal);
    assert!(!format!("{opened:?}").contains("member-secret"));
    assert!(open(&ben, path, &bytes).is_err());
    assert!(open(&ana, "s/access-keys/a/2", &bytes).is_err());
    for index in 0..bytes.len() {
        let mut changed = bytes.clone();
        changed[index] ^= 1;
        assert!(open(&ana, path, &changed).is_err(), "byte {index}");
    }
    for end in 0..bytes.len() {
        assert!(open(&ana, path, &bytes[..end]).is_err());
    }
    // Another member cannot pass off a key as the admin's: the signature
    // names its real signer.
    let forged = seal(
        &ben,
        &ana.member_id(),
        &ana.sealing_public_key(),
        path,
        &key(),
    )
    .unwrap();
    assert_eq!(routing(&forged).unwrap().author, ben.member_id());
}

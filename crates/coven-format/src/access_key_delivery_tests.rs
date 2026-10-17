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

fn identity(id: u128) -> MemberIdentity {
    MemberIdentity::generate(MemberId(uuid::Uuid::from_u128(id))).unwrap()
}

#[test]
fn independently_sealed_delivery_opens_for_its_recorded_member() {
    let directory = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures");
    let bytes = crate::tests::hex(
        &std::fs::read_to_string(directory.join("sealed-access-key.hex"))
            .expect("D14 includes an independently generated delivered S3 key"),
    );
    let ana = MemberIdentity::new(
        MemberId(uuid::Uuid::from_bytes([0x4d; 16])),
        crate::test_utils::keys_from_seed(0x33),
    );
    let ben = MemberIdentity::new(
        MemberId(uuid::Uuid::from_bytes([0x42; 16])),
        crate::test_utils::keys_from_seed(0x35),
    );
    let path = crate::path::ObjectPath::access_key(
        coven_foundation::id_source::StoreId(uuid::Uuid::from_bytes([0x10; 16])),
        &ben.member(),
        uuid::Uuid::from_bytes([0x77; 16]),
    );
    let signer = ana.keys().public_keys().signing;
    let opened = open(&ben, &signer, path.as_str(), &bytes).unwrap();
    assert_eq!(opened.access_key_id, key().access_key_id);
    assert_eq!(
        opened.secret_access_key.as_str(),
        key().secret_access_key.as_str()
    );
    assert_eq!(opened.principal, key().principal);
    let prefix = routing(&bytes).unwrap().encode().unwrap();
    assert_eq!(prefix, bytes[..prefix.len()]);
    let length =
        u32::from_be_bytes(bytes[prefix.len()..prefix.len() + 4].try_into().unwrap()) as usize;
    let sealed = &bytes[prefix.len() + 4..bytes.len() - 64];
    assert_eq!(length, sealed.len());
    let plaintext = ben
        .keys()
        .open_access_key(path.as_str(), &prefix, sealed)
        .unwrap();
    assert_eq!(
        encode_key(&opened).unwrap().as_slice(),
        plaintext.as_slice()
    );
    crate::tests::mutations(&bytes, |changed| {
        assert!(open(&ben, &signer, path.as_str(), changed).is_err());
    });
}

#[test]
fn a_delivery_opens_only_for_its_member_and_names_its_signer() {
    let admin = identity(1);
    let ana = identity(2);
    let ben = identity(3);
    let signer = admin.keys().public_keys().signing;
    let path = "s/access-keys/a/1";
    let bytes = seal(
        &admin,
        &ana.member(),
        &ana.keys().public_keys().sealing,
        path,
        &key(),
    )
    .unwrap();
    assert_eq!(&bytes[3..19], ana.member().0.as_bytes());
    assert_eq!(&bytes[19..51], ana.keys().public_keys().sealing.as_bytes());
    assert_eq!(&bytes[51..67], admin.member().0.as_bytes());
    let routed = routing(&bytes).unwrap();
    assert_eq!(routed.member, ana.member());
    assert_eq!(routed.sealing, ana.keys().public_keys().sealing);
    assert_eq!(routed.author, admin.member());
    let opened = open(&ana, &signer, path, &bytes).unwrap();
    assert_eq!(opened.access_key_id, "AKIAMEMBER");
    assert_eq!(opened.secret_access_key.as_str(), "member-secret");
    assert_eq!(opened.principal, key().principal);
    assert!(!format!("{opened:?}").contains("member-secret"));
    assert!(open(&ben, &signer, path, &bytes).is_err());
    assert!(open(&ana, &signer, "s/access-keys/a/2", &bytes).is_err());
    // The author's id is resolved by the caller; only its current key verifies.
    assert!(open(&ana, &ana.keys().public_keys().signing, path, &bytes).is_err());
    // Ana's id with a different credential is a different recipient.
    let rebound = MemberIdentity::new(ana.member(), coven_crypto::MemberKeys::generate().unwrap());
    assert!(open(&rebound, &signer, path, &bytes).is_err());
    for index in 0..bytes.len() {
        let mut changed = bytes.clone();
        changed[index] ^= 1;
        assert!(open(&ana, &signer, path, &changed).is_err(), "byte {index}");
    }
    for end in 0..bytes.len() {
        assert!(open(&ana, &signer, path, &bytes[..end]).is_err());
    }
    // Another member cannot pass off a key as the admin's: the routing names
    // the real signer, whose key the caller resolves.
    let forged = seal(
        &ben,
        &ana.member(),
        &ana.keys().public_keys().sealing,
        path,
        &key(),
    )
    .unwrap();
    assert_eq!(routing(&forged).unwrap().author, ben.member());
    assert!(open(&ana, &signer, path, &forged).is_err());
}

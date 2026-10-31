use super::*;

fn encode(access: &MemberAccess) -> Result<Vec<u8>, Error> {
    let mut out = Encoder::new();
    access.put(&mut out)?;
    Ok(out.bytes)
}

fn decode(bytes: &[u8]) -> Result<MemberAccess, Error> {
    let mut input = Decoder::new(bytes)?;
    let access = MemberAccess::get(&mut input)?;
    input.finish()?;
    Ok(access)
}

#[test]
fn both_kinds_of_access_round_trip() {
    let key = MemberAccess::S3AccessKey { access_key_id: "key".into() };
    let bytes = encode(&key).unwrap();
    assert_eq!(decode(&bytes).unwrap(), key);
    for end in 0..bytes.len() {
        assert!(decode(&bytes[..end]).is_err());
    }
    let account = MemberAccess::ProviderAccount("ana@example.test".into());
    assert_eq!(decode(&encode(&account).unwrap()).unwrap(), account);
}

#[test]
fn a_key_id_is_bounded_and_the_kind_tagged() {
    let access = MemberAccess::S3AccessKey { access_key_id: "a".repeat(1025) };
    assert!(matches!(
        encode(&access),
        Err(Error::Limit { field: "member access", actual: 1025, maximum: 1024 })
    ));
    let mut bytes = encode(&MemberAccess::S3AccessKey { access_key_id: "key".into() }).unwrap();
    bytes[0] = 2;
    assert!(matches!(decode(&bytes), Err(Error::UnknownTag { field: "member access", tag: 2 })));
}

#[test]
fn credentials_compare_by_account_or_key_id() {
    let key = |id: &str| MemberAccess::S3AccessKey { access_key_id: id.into() };
    assert!(key("a").same_credential(&key("a")));
    assert!(!key("a").same_credential(&key("b")));
    assert!(MemberAccess::ProviderAccount(" Ana@Example.test".into())
        .same_credential(&MemberAccess::ProviderAccount("ana@example.test ".into())));
    assert!(!MemberAccess::ProviderAccount("a".into()).same_credential(&key("a")));
}

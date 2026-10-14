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

pub(crate) fn principals() -> Vec<S3Principal> {
    vec![
        S3Principal::Console,
        S3Principal::IamUser {
            user: "coven-store-member".into(),
            policy: IamPolicy::Inline {
                name: "coven-store".into(),
            },
        },
        S3Principal::IamUser {
            user: "coven-store-member".into(),
            policy: IamPolicy::Managed {
                arn: "arn:aws:iam:::policy/coven-store-member".into(),
            },
        },
        S3Principal::GcsServiceAccount {
            project: "household-storage".into(),
            email: "coven-member@household-storage.iam.gserviceaccount.com".into(),
            managed_folder: "base/store/".into(),
        },
        S3Principal::R2Token {
            account: "0123456789abcdef".into(),
        },
        S3Principal::RustFsServiceAccount {
            parent: "coven".into(),
        },
    ]
}

#[test]
fn every_recorded_principal_round_trips() {
    for principal in principals() {
        let access = MemberAccess::S3AccessKey {
            access_key_id: "key".into(),
            principal,
        };
        let bytes = encode(&access).unwrap();
        assert_eq!(decode(&bytes).unwrap(), access);
        for end in 0..bytes.len() {
            assert!(decode(&bytes[..end]).is_err());
        }
    }
    let account = MemberAccess::ProviderAccount("ana@example.test".into());
    assert_eq!(decode(&encode(&account).unwrap()).unwrap(), account);
}

#[test]
fn principal_handles_are_bounded_and_tagged() {
    let access = MemberAccess::S3AccessKey {
        access_key_id: "key".into(),
        principal: S3Principal::R2Token {
            account: "a".repeat(1025),
        },
    };
    assert!(matches!(
        encode(&access),
        Err(Error::Limit {
            field: "member access",
            actual: 1025,
            maximum: 1024
        })
    ));
    let mut bytes = encode(&MemberAccess::S3AccessKey {
        access_key_id: "key".into(),
        principal: S3Principal::Console,
    })
    .unwrap();
    *bytes.last_mut().unwrap() = 5;
    assert!(matches!(
        decode(&bytes),
        Err(Error::UnknownTag {
            field: "S3 principal",
            tag: 5
        })
    ));
    let mut bytes = encode(&MemberAccess::S3AccessKey {
        access_key_id: "key".into(),
        principal: principals()[1].clone(),
    })
    .unwrap();
    let policy = bytes.len() - 4 - "coven-store".len() - 1;
    bytes[policy] = 2;
    assert!(matches!(
        decode(&bytes),
        Err(Error::UnknownTag {
            field: "IAM policy",
            tag: 2
        })
    ));
}

#[test]
fn credentials_compare_by_account_or_key_id() {
    let key = |id: &str, principal| MemberAccess::S3AccessKey {
        access_key_id: id.into(),
        principal,
    };
    assert!(key("a", S3Principal::Console).same_credential(&key("a", principals()[1].clone())));
    assert!(!key("a", S3Principal::Console).same_credential(&key("b", S3Principal::Console)));
    assert!(MemberAccess::ProviderAccount(" Ana@Example.test".into())
        .same_credential(&MemberAccess::ProviderAccount("ana@example.test ".into())));
    assert!(
        !MemberAccess::ProviderAccount("a".into()).same_credential(&key("a", S3Principal::Console))
    );
}

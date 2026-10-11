use super::*;
use coven_foundation::id_source::{DeviceId, KeyId};
use coven_merge::WriteId;

#[test]
fn causes_survive_cloning_but_do_not_change_equality_or_recorded_tags() {
    let write = WriteId {
        device: DeviceId(1),
        number: 2,
    };
    let failures = [
        Refusal::Decryption {
            cause: Some(Arc::new(CryptoError::Authentication)),
        },
        Refusal::Signature {
            cause: Some(Arc::new(CryptoError::Signature)),
        },
        Refusal::Parse {
            cause: Some(Arc::new(coven_format::Error::Truncated)),
        },
        Refusal::InvalidWrite {
            cause: Some(Arc::new(DbError::InvalidWrite {
                write,
                error: MergeError::GenerationParity(1),
            })),
        },
        Refusal::NotAuthorized,
        Refusal::InvalidCausality {
            cause: Some(Arc::new(MergeError::CausalTimestamp(write))),
        },
        Refusal::WrongIdentity {
            cause: Some(Arc::new(coven_crypto::MaterialError::StoreKeyConflict(
                KeyId(uuid::Uuid::from_u128(1)),
            ))),
        },
        Refusal::ContentHash,
    ];
    for (tag, failure) in failures.iter().enumerate() {
        let cloned = failure.clone();
        assert_eq!(&cloned, failure);
        if let Some(source) = failure.source() {
            assert!(std::ptr::eq(source, cloned.source().unwrap()));
            assert!(failure.to_string().contains(&source.to_string()));
        } else {
            assert!(matches!(
                failure,
                Refusal::NotAuthorized | Refusal::ContentHash
            ));
        }
        let wire = StuckFailure::from(failure);
        assert_eq!(u8::from(wire), tag as u8);
        let restored = Refusal::from(StuckFailure::try_from(tag as u8).unwrap());
        assert_eq!(failure, &restored);
        assert!(restored.source().is_none());
        for (other_tag, other) in failures.iter().enumerate() {
            assert_eq!(failure == other, tag == other_tag);
        }
    }
    assert_eq!(
        Refusal::Parse {
            cause: Some(Arc::new(coven_format::Error::Truncated))
        },
        Refusal::Parse {
            cause: Some(Arc::new(coven_format::Error::TrailingBytes))
        },
    );
    assert_eq!(
        Refusal::InvalidCausality {
            cause: Some(Arc::new(MergeError::CausalTimestamp(write)))
        },
        Refusal::InvalidCausality {
            cause: Some(Arc::new(MergeError::CausalClosure(write)))
        },
    );
}

#[test]
fn format_identity_and_merge_checks_keep_their_native_causes() {
    let error = coven_format::Error::Invalid {
        field: "write timestamp",
        rule: coven_format::error::Rule::TimestampDevice,
    };
    let failure = Refusal::from(error);
    assert_eq!(failure, Refusal::WrongIdentity { cause: None });
    assert_eq!(
        failure
            .source()
            .unwrap()
            .downcast_ref::<coven_format::Error>(),
        Some(&coven_format::Error::Invalid {
            field: "write timestamp",
            rule: coven_format::error::Rule::TimestampDevice,
        }),
    );
    let write = WriteId {
        device: DeviceId(3),
        number: 1,
    };
    let failure = Refusal::from(coven_format::Error::Merge(MergeError::CausalTimestamp(
        write,
    )));
    assert_eq!(failure, Refusal::InvalidCausality { cause: None });
    assert_eq!(
        failure.source().unwrap().downcast_ref::<MergeError>(),
        Some(&MergeError::CausalTimestamp(write)),
    );
}

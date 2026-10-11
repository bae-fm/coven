use super::*;
use coven_foundation::id_source::DeviceId;
use coven_merge::{MergeError, WriteId};
use coven_storage::ObjectPath;
use std::error::Error;

#[test]
fn newer_key_envelopes_require_an_update_at_the_sync_boundary() {
    for version in [coven_format::FORMAT_VERSION + 1, 0] {
        let mut bytes = vec![37];
        bytes.extend_from_slice(&version.to_be_bytes());
        let error = coven_crypto::SealedKey::decode(&bytes).unwrap_err();
        let error = SyncError::from(error);
        if version == 0 {
            assert!(matches!(
                error,
                SyncError::Crypto(CryptoError::UnsupportedVersion(0))
            ));
        } else {
            assert!(matches!(
                error,
                SyncError::Stopped(SyncFailure::UpdateRequired)
            ));
        }
    }
}

#[test]
fn merge_causality_refusal_preserves_its_native_cause() {
    let write = WriteId {
        device: DeviceId(1),
        number: 2,
    };
    let path = ObjectPath::device_log(write.device, write.number.try_into().unwrap());
    let error = crate::write_object::database_failure(
        &path,
        DbError::InvalidWrite {
            write,
            error: MergeError::CausalTimestamp(write),
        },
    );
    let SyncError::Damaged(object) = error else {
        panic!("{error:?}")
    };
    assert_eq!(
        u8::from(coven_format::stuck::StuckFailure::from(&object.failure)),
        5
    );
    assert_eq!(object.path, path.as_str());
    assert_eq!(
        object
            .failure
            .source()
            .unwrap()
            .downcast_ref::<MergeError>(),
        Some(&MergeError::CausalTimestamp(write))
    );
}

#[test]
fn object_validation_keeps_causes_and_local_failures_stay_local() {
    let write = WriteId {
        device: DeviceId(1),
        number: 1,
    };
    let path = ObjectPath::device_log(write.device, write.number.try_into().unwrap());
    for error in [
        DbError::InvalidWrite {
            write,
            error: MergeError::GenerationParity(1),
        },
        DbError::Snapshot(coven_database::SnapshotError::Inconsistent(
            "row outside snapshot",
        )),
    ] {
        let SyncError::Damaged(object) = crate::write_object::database_failure(&path, error) else {
            panic!("expected an invalid object");
        };
        assert_eq!(object.failure, crate::Refusal::InvalidWrite { cause: None });
        assert!(object.failure.source().unwrap().is::<DbError>());
    }
    let SyncError::Damaged(object) = crate::write_object::database_failure(
        &path,
        DbError::WriteFormat(coven_format::Error::Truncated),
    ) else {
        panic!("expected malformed bytes")
    };
    assert_eq!(object.failure, crate::Refusal::Parse { cause: None });
    assert_eq!(
        object
            .failure
            .source()
            .unwrap()
            .downcast_ref::<coven_format::Error>(),
        Some(&coven_format::Error::Truncated)
    );
    assert!(matches!(
        crate::write_object::database_failure(&path, DbError::DamagedDatabase),
        SyncError::Database(DbError::DamagedDatabase)
    ));
}

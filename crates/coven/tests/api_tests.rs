//! Applications use Appendix E's declarations through the public crate root.

use coven::{
    Clock, CryptoError, DbError, DeleteAccessKeyError, ObjectPath, PathError, S3Detection, S3Probe,
    S3ProbeResult, SnapshotError, StorageTime, StoredObject, WriteWait,
};
use std::time::{Duration, SystemTime};

struct AppClock;
impl Clock for AppClock {
    fn now(&self) -> SystemTime {
        SystemTime::UNIX_EPOCH
    }
    fn elapsed(&self) -> Duration {
        Duration::ZERO
    }
}

#[test]
fn appendix_e_types_and_variants_are_available_at_the_crate_root() {
    let _: Result<ObjectPath, PathError> = ObjectPath::parse("invalid");
    let clock: &dyn Clock = &AppClock;
    assert_eq!(clock.elapsed(), Duration::ZERO);
    let stored_at: StorageTime = StorageTime::new(1, 2).unwrap();
    assert_eq!((stored_at.seconds(), stored_at.nanos()), (1, 2));
    let path = ObjectPath::floor(coven::StoreId(uuid::Uuid::from_u128(1)));
    let object = StoredObject {
        object_id: path.as_str().into(),
        path,
        size: 0,
        stored_at,
        time_resolution: Duration::from_nanos(1),
        revision: "revision".into(),
        replace_token: "condition".into(),
        checksum: None,
    };
    assert_eq!(object.replace_token, "condition");
    let _deletions = [
        DeleteAccessKeyError::DeleteFailed(coven::StorageFailure::PermissionDenied.into()),
        DeleteAccessKeyError::ReadFailed(coven::StorageFailure::Network.into()),
    ];
    let _waits = [
        WriteWait::Slot(1),
        WriteWait::Sources(Vec::new()),
        WriteWait::SchemaPublication(2),
    ];
    let _exhausted: DbError = DbError::WriteNumberExhausted;
    let _identity: CryptoError = CryptoError::KeyIdentity;
    let _snapshot: SnapshotError = SnapshotError::KeyUnavailable {
        audience: coven::Audience::Store,
        key: coven::KeyId(uuid::Uuid::from_u128(2)),
    };
    let _results: fn(&S3Detection) -> &[S3ProbeResult] = |detection| &detection.results;
    let _probe: fn(&S3ProbeResult) -> &S3Probe = |result| &result.probe;
}

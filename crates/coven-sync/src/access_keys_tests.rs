use super::*;
use coven_storage::StorageFailure;

fn read(key: &str, reply: Result<KeyPresence, StorageError>) -> Option<KeyObservation> {
    Some(KeyObservation {
        access_key_id: key.into(),
        reply,
    })
}

#[test]
fn deletion_clears_only_with_a_read_of_that_key_absent() {
    assert!(verify_deletion("old", Ok(()), read("old", Ok(KeyPresence::Absent))).is_ok());
    assert!(matches!(
        verify_deletion(
            "old",
            Err(StorageFailure::PermissionDenied.into()),
            read("old", Ok(KeyPresence::Absent))
        ),
        Err(DeleteAccessKeyError::DeleteFailed(_))
    ));
    assert!(matches!(
        verify_deletion("old", Ok(()), None),
        Err(DeleteAccessKeyError::ObservationMissing)
    ));
    assert!(matches!(
        verify_deletion("old", Ok(()), read("other", Ok(KeyPresence::Absent))),
        Err(DeleteAccessKeyError::WrongKey)
    ));
    assert!(matches!(
        verify_deletion("old", Ok(()), read("old", Ok(KeyPresence::Present))),
        Err(DeleteAccessKeyError::StillPresent)
    ));
    assert!(matches!(
        verify_deletion(
            "old",
            Ok(()),
            read("old", Err(StorageFailure::Network.into()))
        ),
        Err(DeleteAccessKeyError::ObservationFailed(_))
    ));
}

//! What each upload in the queue is doing (§17.9): the pause holds a trailing
//! upload and never a bound one, and a failure that clears by itself is a wait.
//! `Facts::upload_phase` against the Lean model's `Upload.phase` is
//! `sync_status::tests::io_differential_upload_phase`.
use super::*;
use crate::{FileSourceFailure, PendingReason, PendingRecord, PendingSubject};
use coven_database::{RecordedLocation, RetrySync};
use coven_foundation::id_source::{DeviceId, ObjectId};
use coven_storage::{CloudProvider, StorageFailure};
use std::time::{Duration, UNIX_EPOCH};

/// A connected device with nothing queued, sync running and nothing held.
fn connected() -> Facts {
    Facts {
        location: RecordedLocation::Connected(CloudProvider::S3),
        last: None,
        moving: false,
        uploads: Vec::new(),
        queued: false,
        unsent: false,
        records: Vec::new(),
        sync: RetrySync::Started { cooling_until: None },
        paused: false,
        unmetered_only: false,
        metered: false,
        keys_changing: false,
    }
}

/// A bound upload.
fn upload(progress: Progress) -> Upload {
    Upload { progress, bound: true, retry_at: None, record: None }
}

/// The record of the upload of the `n`th file.
fn upload_record(n: u128, reason: PendingReason) -> PendingRecord {
    PendingRecord::new(
        PendingSubject::Upload { device: DeviceId(1), object: ObjectId(uuid::Uuid::from_u128(n)) },
        reason,
    )
}

/// A trailing upload under the pause waits for the pause, one whose original
/// went missing is failed with that record, and a bound upload meets neither
/// the pause, a metered network nor a self-replacement: it waits for its
/// backoff or its turn (§17.4, §17.9).
#[test]
fn the_pause_holds_a_trailing_upload_and_never_a_bound_one() {
    let missing = upload_record(
        1,
        PendingReason::Original {
            path: "/Volumes/Drive/track.flac".into(),
            failure: FileSourceFailure::Missing,
        },
    );
    let at = UNIX_EPOCH + Duration::from_secs(9);
    let trailing = Upload { bound: false, ..upload(Progress::Queued) };
    let bound = upload(Progress::Queued);
    let held = Facts {
        paused: true,
        unmetered_only: true,
        metered: true,
        keys_changing: true,
        ..connected()
    };
    assert_eq!(held.upload_phase(&trailing), UploadPhase::Waiting(UploadWait::Paused));
    assert_eq!(held.upload_phase(&bound), UploadPhase::Waiting(UploadWait::Queued));
    let retrying = Upload { retry_at: Some(at), ..bound.clone() };
    assert_eq!(held.upload_phase(&retrying), UploadPhase::Waiting(UploadWait::Retrying { at }));
    let unpaused = Facts { paused: false, ..held.clone() };
    assert_eq!(unpaused.upload_phase(&trailing), UploadPhase::Waiting(UploadWait::Metered));
    let unmetered = Facts { metered: false, ..unpaused.clone() };
    assert_eq!(unmetered.upload_phase(&trailing), UploadPhase::Waiting(UploadWait::KeysChanging));
    let changed = Facts { keys_changing: false, ..unmetered };
    let backoff = Upload { retry_at: Some(at), ..trailing.clone() };
    assert_eq!(changed.upload_phase(&backoff), UploadPhase::Waiting(UploadWait::Retrying { at }));
    assert_eq!(changed.upload_phase(&trailing), UploadPhase::Waiting(UploadWait::Queued));
    let lost = Upload { record: Some(missing.clone()), ..trailing };
    assert_eq!(held.upload_phase(&lost), UploadPhase::Failed(missing));
    let stopped = Facts { sync: RetrySync::Stopped, ..held };
    assert_eq!(stopped.upload_phase(&bound), UploadPhase::Waiting(UploadWait::NotSyncing));
    assert_eq!(
        Facts { sync: RetrySync::Stopped, ..connected() }.upload_phase(&upload(Progress::Moving)),
        UploadPhase::Uploading
    );
}

/// A server error on a trailing upload waits for its backoff, as on a bound
/// one, and never names a state; a stored upload is stored (§17.9, §19.1).
#[test]
fn a_failure_that_clears_by_itself_is_a_wait_not_a_failure() {
    let at = UNIX_EPOCH + Duration::from_secs(9);
    let network = upload_record(
        1,
        PendingReason::Storage {
            provider: CloudProvider::S3,
            failure: StorageFailure::Network,
            answered: true,
        },
    );
    let trailing = Upload {
        bound: false,
        retry_at: Some(at),
        record: Some(network),
        ..upload(Progress::Queued)
    };
    assert_eq!(
        connected().upload_phase(&trailing),
        UploadPhase::Waiting(UploadWait::Retrying { at })
    );
    let stored = Upload { progress: Progress::Stored, ..trailing };
    assert_eq!(connected().upload_phase(&stored), UploadPhase::Stored);
}

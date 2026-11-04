use super::*;
use crate::{
    DeviceIdEvidence, FileMissingReason, FileSourceFailure, Prerequisite, ProviderAccessAction,
    Refusal, RequiredUpdate,
};
use coven_database::{EntryTarget, OperationRejection, WriteCutoff};
use coven_foundation::id_source::{DeviceId, FileId, KeyId, MemberId, StoreId};
use coven_merge::{Audience, Slot};
use coven_storage::{OAuthAccess, ObjectPath, OwnBucketReason};
use serde_json::{json, Value};
use std::time::{Duration, UNIX_EPOCH};

/// Every provider failure.
fn failures() -> Vec<StorageFailure> {
    use StorageFailure::*;
    vec![
        ProviderUnavailable,
        AccessNotRegistered { required: OAuthAccess::OneDrive },
        AccessNotGranted { required: OAuthAccess::OneDrive },
        DropboxAppFolderNotShareable,
        CatchUpRequired,
        Network,
        Authentication,
        PermissionDenied,
        NotFound,
        AlreadyExists,
        Conflict,
        ContainerNotFound,
        RegionMismatch,
        StoreExists,
        ClockSkew,
        OwnBucketRequired { reason: OwnBucketReason::Prefix },
        QuotaExceeded,
        RateLimited,
        InvalidConfiguration,
        Refused,
        Protocol,
        InvalidPath,
        InvalidRange,
        NotStoreOwner,
        InviteeIsStoreOwner,
        AccountIdUnavailable,
        InvitationMismatch,
        SessionMismatch,
        SessionExpired,
        SessionIdentityUnknown,
        InvalidPart,
        SingleRequestTooLarge { size: 2, limit: 1 },
        CopyTooLarge { size: 2, limit: 1 },
        Encoding,
        File,
        MemberKeysMissing,
    ]
}

/// The model's name for a provider failure.
#[deny(clippy::wildcard_enum_match_arm)]
fn failure_name(failure: StorageFailure) -> &'static str {
    use StorageFailure::*;
    match failure {
        ProviderUnavailable => "providerUnavailable",
        AccessNotRegistered { .. } => "accessNotRegistered",
        AccessNotGranted { .. } => "accessNotGranted",
        DropboxAppFolderNotShareable => "dropboxAppFolderNotShareable",
        CatchUpRequired => "catchUpRequired",
        Network => "network",
        Authentication => "authentication",
        PermissionDenied => "permissionDenied",
        NotFound => "notFound",
        AlreadyExists => "alreadyExists",
        Conflict => "conflict",
        ContainerNotFound => "containerNotFound",
        RegionMismatch => "regionMismatch",
        StoreExists => "storeExists",
        ClockSkew => "clockSkew",
        OwnBucketRequired { .. } => "ownBucketRequired",
        QuotaExceeded => "quotaExceeded",
        RateLimited => "rateLimited",
        InvalidConfiguration => "invalidConfiguration",
        Refused => "refused",
        Protocol => "protocol",
        InvalidPath => "invalidPath",
        InvalidRange => "invalidRange",
        NotStoreOwner => "notStoreOwner",
        InviteeIsStoreOwner => "inviteeIsStoreOwner",
        AccountIdUnavailable => "accountIdUnavailable",
        InvitationMismatch => "invitationMismatch",
        SessionMismatch => "sessionMismatch",
        SessionExpired => "sessionExpired",
        SessionIdentityUnknown => "sessionIdentityUnknown",
        InvalidPart => "invalidPart",
        SingleRequestTooLarge { .. } => "singleRequestTooLarge",
        CopyTooLarge { .. } => "copyTooLarge",
        Encoding => "encoding",
        File => "file",
        MemberKeysMissing => "memberKeysMissing",
    }
}

#[deny(clippy::wildcard_enum_match_arm)]
fn local_name(failure: LocalFailure) -> &'static str {
    match failure {
        LocalFailure::Database => "database",
        LocalFailure::Disk => "disk",
        LocalFailure::KeyCustody => "keyCustody",
        LocalFailure::Crypto => "crypto",
        LocalFailure::Operation => "operation",
        LocalFailure::Shutdown => "shutdown",
    }
}

/// The model's encoding of a pending reason.
#[deny(clippy::wildcard_enum_match_arm)]
fn reason_json(reason: &PendingReason) -> Value {
    match reason {
        PendingReason::Missing { .. } => json!("missing"),
        PendingReason::Waits(_) => json!("waits"),
        PendingReason::KeyUnavailable { .. } => json!("keyUnavailable"),
        PendingReason::UpdateRequired(_) => json!("updateRequired"),
        PendingReason::NoStorage => json!("noStorage"),
        PendingReason::Storage { failure, answered, .. } => {
            json!({"storage": failure_name(*failure), "answered": answered})
        }
        PendingReason::Refused(_) => json!("refused"),
        PendingReason::WritesRefused { .. } => json!("writesRefused"),
        PendingReason::ProviderPending { .. } => json!("providerPending"),
        PendingReason::AccountInUse { .. } => json!("accountInUse"),
        PendingReason::AccessRemains { .. } => json!("accessRemains"),
        PendingReason::FileUnavailable(FileMissingReason::Lost) => json!("fileLost"),
        PendingReason::FileUnavailable(FileMissingReason::Source(_)) => json!("fileSource"),
        PendingReason::Original { failure: FileSourceFailure::Missing, .. } => {
            json!("originalMissing")
        }
        PendingReason::Original {
            failure: FileSourceFailure::Changed | FileSourceFailure::Integrity,
            ..
        } => json!("originalChanged"),
        PendingReason::Failed(failure) => json!({"failed": local_name(*failure)}),
        PendingReason::Rejected(_) => json!("rejected"),
        PendingReason::Cleanup { operation, cleanup } => {
            json!({"cleanup": [reason_json(operation), reason_json(cleanup)]})
        }
        PendingReason::Paused => json!("paused"),
        PendingReason::Removed => json!("removed"),
        PendingReason::MemberKeysReplaced { .. } => json!("memberKeysReplaced"),
    }
}

fn member() -> MemberId {
    MemberId(uuid::Uuid::from_u128(1))
}

fn key_unavailable() -> PendingReason {
    PendingReason::KeyUnavailable {
        audience: Audience::Store,
        key: KeyId(uuid::Uuid::from_u128(2)),
    }
}

/// A reason of every kind the model names, every provider failure, answered
/// or not, and every local failure included, and cleanups over a spread of
/// them.
fn reasons() -> Vec<PendingReason> {
    let store = StoreId(uuid::Uuid::from_u128(1));
    let original =
        |failure| PendingReason::Original { path: "/Volumes/Drive/track.flac".into(), failure };
    let mut reasons = vec![
        PendingReason::Missing { path: ObjectPath::floor(store) },
        PendingReason::Waits(Prerequisite::SequenceCatchUp),
        key_unavailable(),
        PendingReason::UpdateRequired(RequiredUpdate::AppSchema { version: 2 }),
        PendingReason::NoStorage,
        PendingReason::Refused(Refusal::Parse { cause: None }),
        PendingReason::WritesRefused { cutoff: Slot(3), cause: WriteCutoff::DeviceRemoved },
        PendingReason::ProviderPending {
            action: ProviderAccessAction::Grant,
            account: "carol@example.org".into(),
        },
        PendingReason::AccountInUse { account: "carol@example.org".into() },
        PendingReason::AccessRemains { account: "carol@example.org".into(), shares: Vec::new() },
        PendingReason::FileUnavailable(FileMissingReason::Lost),
        PendingReason::FileUnavailable(FileMissingReason::Source(FileSourceFailure::Integrity)),
        original(FileSourceFailure::Missing),
        original(FileSourceFailure::Changed),
        PendingReason::Rejected(OperationRejection::TargetGone(EntryTarget::Member(member()))),
        PendingReason::Paused,
        PendingReason::Removed,
        PendingReason::MemberKeysReplaced { member: member() },
    ];
    for answered in [false, true] {
        reasons.extend(failures().into_iter().map(|failure| PendingReason::Storage {
            provider: CloudProvider::S3,
            failure,
            answered,
        }));
    }
    reasons.extend(
        [
            LocalFailure::Database,
            LocalFailure::Disk,
            LocalFailure::KeyCustody,
            LocalFailure::Crypto,
            LocalFailure::Operation,
            LocalFailure::Shutdown,
        ]
        .map(PendingReason::Failed),
    );
    let cleanups: Vec<_> = reasons
        .iter()
        .step_by(5)
        .map(|cleanup| PendingReason::Cleanup {
            operation: Box::new(PendingReason::Failed(LocalFailure::Disk)),
            cleanup: Box::new(cleanup.clone()),
        })
        .collect();
    reasons.extend(cleanups);
    reasons
}

/// The record of the upload of the `n`th file.
fn upload_record(n: u128, reason: PendingReason) -> PendingRecord {
    PendingRecord::new(
        PendingSubject::Upload { device: DeviceId(1), file: FileId(uuid::Uuid::from_u128(n)) },
        reason,
    )
}

fn record(reason: PendingReason) -> PendingRecord {
    PendingRecord::new(PendingSubject::Slot(Slot(7)), reason)
}

/// How a pass ended, of every kind, with every provider failure and every
/// reason a blocked slot or a failed operation of coven's own can carry.
fn passes() -> Vec<Option<LastPass>> {
    let failed = |failure| Some(LastPass::Failed { failure, error: "error".into() });
    let mut passes = vec![
        None,
        Some(LastPass::Synced),
        Some(LastPass::Offline { failure: StorageFailure::Network, error: "offline".into() }),
        failed(StatusFailure::UpdateRequired(RequiredUpdate::AppSchema { version: 2 })),
        failed(StatusFailure::Removed),
        failed(StatusFailure::DeviceIdInUse(DeviceIdEvidence::Replaced)),
        failed(StatusFailure::Other),
    ];
    passes.extend(failures().into_iter().map(|failure| failed(StatusFailure::Storage(failure))));
    for reason in reasons() {
        passes.push(Some(LastPass::Blocked { slot: Slot(7), reason: reason.clone(), error: None }));
        passes.push(failed(StatusFailure::Operation(record(reason))));
    }
    passes
}

/// The model's encoding of how the last pass ended; none counts as synced.
#[deny(clippy::wildcard_enum_match_arm)]
fn pass_json(last: &Option<LastPass>) -> Value {
    let failure = match last {
        None | Some(LastPass::Synced) => return json!("synced"),
        Some(LastPass::Offline { .. }) => json!("offline"),
        Some(LastPass::Blocked { reason, .. }) => json!({"blocked": reason_json(reason)}),
        Some(LastPass::Failed { failure, .. }) => match failure {
            StatusFailure::UpdateRequired(_) => json!("updateRequired"),
            StatusFailure::Removed => json!("removed"),
            StatusFailure::DeviceIdInUse(_) => json!("deviceIdInUse"),
            StatusFailure::Storage(failure) => json!({"storage": failure_name(*failure)}),
            StatusFailure::Operation(record) => json!({"operation": reason_json(&record.reason)}),
            StatusFailure::Other => json!("other"),
        },
    };
    json!({"failed": failure})
}

#[deny(clippy::wildcard_enum_match_arm)]
fn state_name(state: &SyncState) -> &'static str {
    match state {
        SyncState::NotSynced => "notSynced",
        SyncState::Syncing => "syncing",
        SyncState::WaitingToSync(_) => "waitingToSync",
        SyncState::Synced => "synced",
        SyncState::Offline(_) => "offline",
        SyncState::OfflineWithUnsentChanges(_) => "offlineUnsent",
        SyncState::SignedOut { .. } => "signedOut",
        SyncState::NoAccess(_) => "noAccess",
        SyncState::StorageFull(_) => "storageFull",
        SyncState::FolderMissing(_) => "folderMissing",
        SyncState::ClockWrong(_) => "clockWrong",
        SyncState::UpdateNeeded(_) => "updateNeeded",
        SyncState::Removed(_) => "removed",
        SyncState::NeedsReset(_) => "needsReset",
        SyncState::Failed(_) => "failed",
    }
}

#[deny(clippy::wildcard_enum_match_arm)]
fn wait_name(wait: UploadWait) -> &'static str {
    match wait {
        UploadWait::NotSyncing => "notSyncing",
        UploadWait::CoolingDown { .. } => "coolingDown",
        UploadWait::Blocked => "blocked",
        UploadWait::Paused => "paused",
        UploadWait::Metered => "metered",
        UploadWait::Retrying { .. } => "retrying",
        UploadWait::Queued => "queued",
    }
}

fn connected() -> Facts {
    Facts {
        location: RecordedLocation::Connected(CloudProvider::S3),
        last: None,
        moving: false,
        downloading: false,
        uploads: Vec::new(),
        unsent: false,
        records: Vec::new(),
        sync: RetrySync::Started { cooling_until: None },
        paused: false,
        unmetered_only: false,
        metered: false,
    }
}

fn upload(progress: Progress) -> Upload {
    Upload { progress, bound: false, retry_at: None, record: None }
}

fn offline() -> Option<LastPass> {
    Some(LastPass::Offline { failure: StorageFailure::Network, error: "offline".into() })
}

/// The model's encoding of `facts`.
fn facts_json(facts: &Facts) -> Value {
    let storage = match facts.location {
        RecordedLocation::None => "none",
        RecordedLocation::SignedOut(_) => "signedOut",
        RecordedLocation::Connected(_) => "connected",
    };
    let (running, cooling) = match facts.sync {
        RetrySync::Stopped => (false, false),
        RetrySync::Started { cooling_until } => (true, cooling_until.is_some()),
    };
    let uploads: Vec<Value> = facts
        .uploads
        .iter()
        .map(|upload| {
            let progress = match upload.progress {
                Progress::Moving => "moving",
                Progress::Stored => "stored",
                Progress::Queued => "queued",
            };
            let record =
                upload.record.as_ref().map_or(Value::Null, |record| reason_json(&record.reason));
            json!({
                "progress": progress,
                "bound": upload.bound,
                "retrying": upload.retry_at.is_some(),
                "record": record,
            })
        })
        .collect();
    let records: Vec<Value> =
        facts.records.iter().map(|record| reason_json(&record.reason)).collect();
    json!({
        "check": "sync_state",
        "storage": storage,
        "last": pass_json(&facts.last),
        "moving": facts.moving,
        "downloading": facts.downloading,
        "uploads": uploads,
        "unsent": facts.unsent,
        "running": running,
        "cooling": cooling,
        "paused": facts.paused,
        "unmeteredOnly": facts.unmetered_only,
        "metered": facts.metered,
        "records": records,
    })
}

/// The state `facts` computes, as the model's runner reports one.
fn shown(facts: &Facts) -> Value {
    let state = facts.state();
    let wait = match &state {
        SyncState::WaitingToSync(wait) => json!(wait_name(*wait)),
        _ => Value::Null,
    };
    json!({"state": state_name(&state), "wait": wait})
}

/// Every set of facts the differential test compares: each pass of every
/// kind alone, with a write or a stored file queued or not; each reason as
/// a record, as a queued upload's record with and without the record in the
/// list, and beside a location with no credentials; every location, moving,
/// download and upload progress combination, offline; and queued uploads
/// under every wait, one alone or two after a stored one, so the first
/// waiting one names the wait, with the setting to move files only on
/// unmetered networks on or off and the network metered or not.
fn cases() -> Vec<Facts> {
    let mut cases = Vec::new();
    for last in passes() {
        for (unsent, stored) in [(false, 0), (true, 0), (false, 1)] {
            cases.push(Facts {
                last: last.clone(),
                unsent,
                uploads: vec![upload(Progress::Stored); stored],
                ..connected()
            });
        }
    }
    for (n, reason) in (0_u128..).zip(reasons()) {
        cases.push(Facts { records: vec![record(reason.clone())], ..connected() });
        let mut queued = upload(Progress::Queued);
        let own = upload_record(n, reason.clone());
        queued.record = Some(own.clone());
        queued.retry_at = Some(UNIX_EPOCH + Duration::from_secs(9));
        cases.push(Facts { uploads: vec![queued.clone()], ..connected() });
        cases.push(Facts { uploads: vec![queued], records: vec![own], ..connected() });
        cases.push(Facts {
            location: RecordedLocation::SignedOut(CloudProvider::Dropbox),
            records: vec![record(reason)],
            ..connected()
        });
    }
    for location in [
        RecordedLocation::None,
        RecordedLocation::SignedOut(CloudProvider::OneDrive),
        RecordedLocation::Connected(CloudProvider::CloudKit),
    ] {
        for moving in [false, true] {
            for downloading in [false, true] {
                for progress in
                    [None, Some(Progress::Moving), Some(Progress::Stored), Some(Progress::Queued)]
                {
                    cases.push(Facts {
                        location,
                        moving,
                        downloading,
                        uploads: progress.map(upload).into_iter().collect(),
                        last: offline(),
                        ..connected()
                    });
                }
            }
        }
    }
    let cooling = RetrySync::Started { cooling_until: Some(UNIX_EPOCH + Duration::from_secs(60)) };
    let blocked = Some(LastPass::Blocked { slot: Slot(7), reason: key_unavailable(), error: None });
    for sync in [RetrySync::Stopped, RetrySync::Started { cooling_until: None }, cooling] {
        for last in [None, blocked.clone()] {
            for (paused, unmetered_only, metered) in [
                (false, false, false),
                (true, false, false),
                (false, true, false),
                (false, false, true),
                (false, true, true),
                (true, true, true),
            ] {
                for bound in [false, true] {
                    for retrying in [false, true] {
                        let mut first = upload(Progress::Queued);
                        first.bound = bound;
                        first.retry_at = retrying.then_some(UNIX_EPOCH + Duration::from_secs(9));
                        let mut second = upload(Progress::Queued);
                        second.bound = true;
                        for uploads in
                            [vec![first.clone()], vec![upload(Progress::Stored), first, second]]
                        {
                            cases.push(Facts {
                                sync,
                                last: last.clone(),
                                paused,
                                unmetered_only,
                                metered,
                                uploads,
                                ..connected()
                            });
                        }
                    }
                }
            }
        }
    }
    cases
}

/// The computed state agrees with the Lean model's `state`, and waiting to
/// sync names the wait its `waits` lists first, for every pending reason,
/// every provider failure, every way a pass ends, and upload and wait
/// combinations (§19.1, proofs/io.md).
#[test]
#[ignore = "requires COVEN_IO_LEAN"]
fn io_differential_sync_state() {
    let cases = cases();
    let inputs: Vec<Value> = cases.iter().map(facts_json).collect();
    let actual: Vec<Value> = cases.iter().map(shown).collect();
    let expected = crate::tests::io_runner(&inputs);
    assert_eq!(actual.len(), expected.len());
    for ((input, actual), expected) in inputs.iter().zip(&actual).zip(&expected) {
        assert_eq!(actual, expected, "{input}");
    }
}

/// The differential's cases hold a reason of each of the model's kinds.
#[test]
fn the_differential_covers_every_reason() {
    let mut kinds: Vec<String> = reasons()
        .iter()
        .map(|reason| match reason_json(reason) {
            Value::String(name) => name,
            Value::Object(object) => object.keys().next().expect("a kind").clone(),
            other => panic!("{other}"),
        })
        .collect();
    kinds.sort();
    kinds.dedup();
    assert_eq!(kinds.len(), 21, "{kinds:?}");
}

/// Among candidates of one state, the last pass is the cause, then the first
/// record in the order the records arose; a location with no credentials is
/// signed out with no cause, ahead of a pass storage refused (§19.1).
#[test]
fn the_cause_is_the_pass_then_the_first_record() {
    let full = |n| {
        upload_record(
            n,
            PendingReason::Storage {
                provider: CloudProvider::S3,
                failure: StorageFailure::QuotaExceeded,
                answered: true,
            },
        )
    };
    let pass = LastPass::Failed {
        failure: StatusFailure::Storage(StorageFailure::QuotaExceeded),
        error: "full".into(),
    };
    let facts = Facts { records: vec![full(1), full(2)], ..connected() };
    assert_eq!(facts.state(), SyncState::StorageFull(SyncCause::Pending(full(1))));
    let facts = Facts { last: Some(pass.clone()), ..facts };
    assert_eq!(facts.state(), SyncState::StorageFull(SyncCause::Pass(pass)));
    let refused = LastPass::Failed {
        failure: StatusFailure::Storage(StorageFailure::Authentication),
        error: "refused".into(),
    };
    let facts = Facts {
        location: RecordedLocation::SignedOut(CloudProvider::Dropbox),
        last: Some(refused.clone()),
        ..connected()
    };
    assert_eq!(
        facts.state(),
        SyncState::SignedOut { provider: CloudProvider::Dropbox, cause: None }
    );
    let facts = Facts { location: RecordedLocation::Connected(CloudProvider::Dropbox), ..facts };
    assert_eq!(
        facts.state(),
        SyncState::SignedOut {
            provider: CloudProvider::Dropbox,
            cause: Some(SyncCause::Pass(refused))
        }
    );
}

/// Waiting to sync names what holds the first waiting upload with its time:
/// a cooldown's end, or the upload's own retry (§19.1).
#[test]
fn a_wait_carries_its_time() {
    let until = UNIX_EPOCH + Duration::from_secs(60);
    let at = UNIX_EPOCH + Duration::from_secs(9);
    let mut retrying = upload(Progress::Queued);
    retrying.retry_at = Some(at);
    let facts = Facts { uploads: vec![retrying], ..connected() };
    assert_eq!(facts.state(), SyncState::WaitingToSync(UploadWait::Retrying { at }));
    let facts = Facts { sync: RetrySync::Started { cooling_until: Some(until) }, ..facts };
    assert_eq!(facts.state(), SyncState::WaitingToSync(UploadWait::CoolingDown { until }));
}

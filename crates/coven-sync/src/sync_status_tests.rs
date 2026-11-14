use super::*;
use crate::{
    DeviceIdEvidence, FileMissingReason, FileSourceFailure, Prerequisite, ProviderAccessAction,
    Refusal, RequiredUpdate,
};
use crate::{PendingSubject, UploadPhase, UploadWait};
use coven_database::{EntryTarget, OperationRejection, WriteCutoff};
use coven_foundation::id_source::{DeviceId, KeyId, MemberId, ObjectId, StoreId};
use coven_merge::slot;
use coven_merge::Audience;
use coven_storage::{OAuthAccess, ObjectPath};
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
        Corrupted,
        Authentication,
        PermissionDenied,
        NotFound,
        AlreadyExists,
        Conflict,
        ContainerNotFound,
        RegionMismatch,
        StoreExists,
        ClockSkew,
        OwnLocationRequired,
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
        InvalidPart,
        SingleRequestTooLarge { size: 2, limit: 1 },
        CopyTooLarge { size: 2, limit: 1 },
        Encoding,
        File,
        MemberKeysMissing,
        Bug,
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
        Corrupted => "corrupted",
        Authentication => "authentication",
        PermissionDenied => "permissionDenied",
        NotFound => "notFound",
        AlreadyExists => "alreadyExists",
        Conflict => "conflict",
        ContainerNotFound => "containerNotFound",
        RegionMismatch => "regionMismatch",
        StoreExists => "storeExists",
        ClockSkew => "clockSkew",
        OwnLocationRequired => "ownLocationRequired",
        QuotaExceeded => "quotaExceeded",
        RateLimited => "rateLimited",
        InvalidConfiguration => "invalidConfiguration",
        Refused => "refused",
        Protocol => "protocol",
        InvalidPath => "invalidPath",
        InvalidRange => "invalidRange",
        NotStoreOwner => "notStoreOwner",
        InviteeIsStoreOwner => "inviteeIsStoreOwner",
        SharingRefused(_) => "sharingRefused",
        AccountIdUnavailable => "accountIdUnavailable",
        InvitationMismatch => "invitationMismatch",
        SessionMismatch => "sessionMismatch",
        SessionExpired => "sessionExpired",
        InvalidPart => "invalidPart",
        SingleRequestTooLarge { .. } => "singleRequestTooLarge",
        CopyTooLarge { .. } => "copyTooLarge",
        Encoding => "encoding",
        File => "file",
        MemberKeysMissing => "memberKeysMissing",
        Bug => "bug",
    }
}

#[deny(clippy::wildcard_enum_match_arm)]
fn local_name(failure: LocalFailure) -> &'static str {
    match failure {
        LocalFailure::Database => "database",
        LocalFailure::DiskFull => "diskFull",
        LocalFailure::DiskDenied => "diskDenied",
        LocalFailure::DiskOther => "diskOther",
        LocalFailure::KeyCustody => "keyCustody",
        LocalFailure::Crypto => "crypto",
        LocalFailure::Operation => "operation",
        LocalFailure::Shutdown => "shutdown",
        LocalFailure::Bug => "bug",
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
        // A refused admission names the removed state, as a removal does:
        // the model's fact is the same (§13.2).
        PendingReason::Removed | PendingReason::NotAdmitted => json!("removed"),
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
        PendingReason::WritesRefused { cutoff: slot(3), cause: WriteCutoff::DeviceRemoved },
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
        PendingReason::NotAdmitted,
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
            LocalFailure::DiskOther,
            LocalFailure::KeyCustody,
            LocalFailure::Crypto,
            LocalFailure::Operation,
            LocalFailure::Shutdown,
            LocalFailure::Bug,
        ]
        .map(PendingReason::Failed),
    );
    let cleanups: Vec<_> = reasons
        .iter()
        .step_by(5)
        .map(|cleanup| PendingReason::Cleanup {
            operation: Box::new(PendingReason::Failed(LocalFailure::DiskOther)),
            cleanup: Box::new(cleanup.clone()),
        })
        .collect();
    reasons.extend(cleanups);
    reasons
}

/// The record of the upload of the `n`th file.
fn upload_record(n: u128, reason: PendingReason) -> PendingRecord {
    PendingRecord::new(
        PendingSubject::Upload { device: DeviceId(1), object: ObjectId(uuid::Uuid::from_u128(n)) },
        reason,
    )
}

fn record(reason: PendingReason) -> PendingRecord {
    PendingRecord::new(PendingSubject::Slot(slot(7)), reason)
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
        failed(StatusFailure::NotAdmitted),
        failed(StatusFailure::DeviceIdInUse(DeviceIdEvidence::Replaced)),
        failed(StatusFailure::Other),
    ];
    passes.extend(failures().into_iter().map(|failure| failed(StatusFailure::Storage(failure))));
    for reason in reasons() {
        passes.push(Some(LastPass::Blocked { slot: slot(7), reason: reason.clone(), error: None }));
        passes.push(failed(StatusFailure::Operation(record(reason))));
    }
    passes
}

/// Trailing uploads of every progress, with and without a backoff, and with
/// no record, one that names a state and one that names none.
fn trailing_uploads() -> Vec<Upload> {
    let missing = upload_record(
        90,
        PendingReason::Original {
            path: "/Volumes/Drive/track.flac".into(),
            failure: FileSourceFailure::Missing,
        },
    );
    let network = upload_record(
        91,
        PendingReason::Storage {
            provider: CloudProvider::S3,
            failure: StorageFailure::Network,
            answered: true,
        },
    );
    let mut uploads = Vec::new();
    for progress in [Progress::Moving, Progress::Stored, Progress::Queued] {
        for retrying in [false, true] {
            for record in [None, Some(missing.clone()), Some(network.clone())] {
                uploads.push(Upload {
                    progress,
                    bound: false,
                    retry_at: retrying.then_some(UNIX_EPOCH + Duration::from_secs(9)),
                    record,
                });
            }
        }
    }
    uploads
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
            StatusFailure::Removed | StatusFailure::NotAdmitted => json!("removed"),
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
fn wait_name(wait: SyncWait) -> &'static str {
    match wait {
        SyncWait::NotSyncing => "notSyncing",
        SyncWait::CoolingDown { .. } => "coolingDown",
        SyncWait::Blocked => "blocked",
        SyncWait::Retrying { .. } => "retrying",
        SyncWait::Queued => "queued",
    }
}

#[deny(clippy::wildcard_enum_match_arm)]
fn upload_wait_name(wait: UploadWait) -> &'static str {
    match wait {
        UploadWait::NotSyncing => "notSyncing",
        UploadWait::CoolingDown { .. } => "coolingDown",
        UploadWait::Sync => "sync",
        UploadWait::Paused => "paused",
        UploadWait::Metered => "metered",
        UploadWait::KeysChanging => "keysChanging",
        UploadWait::Retrying { .. } => "retrying",
        UploadWait::Queued => "queued",
    }
}

/// The model's encoding of an upload's phase.
#[deny(clippy::wildcard_enum_match_arm)]
fn phase_json(phase: &UploadPhase) -> Value {
    match phase {
        UploadPhase::Uploading => json!({"phase": "uploading", "wait": null}),
        UploadPhase::Stored => json!({"phase": "stored", "wait": null}),
        UploadPhase::Failed(_) => json!({"phase": "failed", "wait": null}),
        UploadPhase::Waiting(wait) => json!({"phase": "waiting", "wait": upload_wait_name(*wait)}),
    }
}

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
        pass_retry: None,
    }
}

/// A bound upload: the state reads these.
fn upload(progress: Progress) -> Upload {
    Upload { progress, bound: true, retry_at: None, record: None }
}

fn offline() -> Option<LastPass> {
    Some(LastPass::Offline { failure: StorageFailure::Network, error: "offline".into() })
}

fn upload_json(upload: &Upload) -> Value {
    let progress = match upload.progress {
        Progress::Moving => "moving",
        Progress::Stored => "stored",
        Progress::Queued => "queued",
    };
    let record = upload.record.as_ref().map_or(Value::Null, |record| reason_json(&record.reason));
    json!({
        "progress": progress,
        "bound": upload.bound,
        "retrying": upload.retry_at.is_some(),
        "record": record,
    })
}

/// The model's encoding of `facts`, with `trailing` uploads beside the bound
/// ones the facts hold: the database reads none for the state, and the model
/// says they change nothing it shows (`trailing_keeps_state`).
fn facts_json(facts: &Facts, trailing: &[Upload]) -> Value {
    let storage = match facts.location {
        RecordedLocation::None => "none",
        RecordedLocation::SignedOut(_) => "signedOut",
        RecordedLocation::Connected(_) => "connected",
    };
    let (running, cooling) = match facts.sync {
        RetrySync::Stopped => (false, false),
        RetrySync::Started { cooling_until } => (true, cooling_until.is_some()),
    };
    let uploads: Vec<Value> = trailing.iter().chain(&facts.uploads).map(upload_json).collect();
    let records: Vec<Value> =
        facts.records.iter().map(|record| reason_json(&record.reason)).collect();
    json!({
        "check": "sync_state",
        "storage": storage,
        "last": pass_json(&facts.last),
        "moving": facts.moving,
        "uploads": uploads,
        "unsent": facts.unsent,
        "running": running,
        "cooling": cooling,
        "paused": facts.paused,
        "unmeteredOnly": facts.unmetered_only,
        "metered": facts.metered,
        "keysChanging": facts.keys_changing,
        "passRetrying": facts.pass_retry.is_some(),
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
/// kind with a write queued, a stored file, both or neither; each reason as
/// a record, as a queued upload's record with and without the record in the
/// list, and beside a location with no credentials; every location, moving
/// and upload progress combination, offline; and queued bound uploads under
/// every wait, one alone or two after a stored one, so the first waiting one
/// names the wait, with the pause, the setting to move files only on
/// unmetered networks and the network set any way, with a write queued
/// beside them or not; and a pass held at a slot or by a cooldown, with
/// nothing queued, and a write queued alone, under each way sync can be
/// held.
fn cases() -> Vec<Facts> {
    let mut cases = Vec::new();
    let backoff = Some(UNIX_EPOCH + Duration::from_secs(5));
    for last in passes() {
        for (unsent, stored) in [(false, 0), (true, 0), (false, 1), (true, 1)] {
            for pass_retry in [None, backoff] {
                cases.push(Facts {
                    last: last.clone(),
                    unsent,
                    uploads: vec![upload(Progress::Stored); stored],
                    pass_retry,
                    ..connected()
                });
            }
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
            for progress in
                [None, Some(Progress::Moving), Some(Progress::Stored), Some(Progress::Queued)]
            {
                cases.push(Facts {
                    location,
                    moving,
                    uploads: progress.map(upload).into_iter().collect(),
                    last: offline(),
                    ..connected()
                });
            }
        }
    }
    let cooling = RetrySync::Started { cooling_until: Some(UNIX_EPOCH + Duration::from_secs(60)) };
    let blocked = Some(LastPass::Blocked { slot: slot(7), reason: key_unavailable(), error: None });
    let limited = Some(LastPass::Failed {
        failure: StatusFailure::Storage(StorageFailure::RateLimited),
        error: "slow down".into(),
    });
    for sync in [RetrySync::Stopped, RetrySync::Started { cooling_until: None }, cooling] {
        for last in [blocked.clone(), limited.clone()] {
            cases.push(Facts { sync, last, ..connected() });
        }
        cases.push(Facts { sync, unsent: true, ..connected() });
        for (last, unsent) in [None, blocked.clone()]
            .into_iter()
            .flat_map(|last| [(last.clone(), false), (last, true)])
        {
            for (paused, unmetered_only, metered) in [
                (false, false, false),
                (true, false, false),
                (false, true, false),
                (false, false, true),
                (false, true, true),
                (true, true, true),
            ] {
                for retrying in [false, true] {
                    let mut first = upload(Progress::Queued);
                    first.retry_at = retrying.then_some(UNIX_EPOCH + Duration::from_secs(9));
                    let second = upload(Progress::Queued);
                    for uploads in
                        [vec![first.clone()], vec![upload(Progress::Stored), first, second]]
                    {
                        cases.push(Facts {
                            sync,
                            last: last.clone(),
                            unsent,
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
    for case in &mut cases {
        case.queued = case.uploads.iter().any(|upload| upload.progress != Progress::Stored);
    }
    cases
}

/// The computed state agrees with the Lean model's `state`, and waiting to
/// sync names the wait its `waits` lists first, for every pending reason,
/// every provider failure, every way a pass ends, and upload and wait
/// combinations (§19.1, proofs/io.md). The model is also given trailing
/// uploads of every progress, retry and record beside each case's bound
/// ones, which coven never reads for the state: they change nothing it
/// shows (`trailing_keeps_state`).
#[test]
#[ignore = "requires COVEN_IO_LEAN"]
fn io_differential_sync_state() {
    let cases = cases();
    let trailing = trailing_uploads();
    let inputs: Vec<Value> = cases.iter().map(|facts| facts_json(facts, &trailing)).collect();
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

/// Waiting to sync names what holds the first waiting bound upload with its
/// time: a cooldown's end, or the upload's own retry (§19.1).
#[test]
fn a_wait_carries_its_time() {
    let until = UNIX_EPOCH + Duration::from_secs(60);
    let at = UNIX_EPOCH + Duration::from_secs(9);
    let mut retrying = upload(Progress::Queued);
    retrying.retry_at = Some(at);
    let facts = Facts { uploads: vec![retrying], ..connected() };
    assert_eq!(facts.state(), SyncState::WaitingToSync(SyncWait::Retrying { at }));
    let facts = Facts { sync: RetrySync::Started { cooling_until: Some(until) }, ..facts };
    assert_eq!(facts.state(), SyncState::WaitingToSync(SyncWait::CoolingDown { until }));
}

/// Synced means nothing waits. A pass blocked at a slot for a wait, as for
/// a key another device hasn't delivered, and a pass a provider's cooldown
/// cut short, wait to sync though no file is queued, naming what holds
/// them: sync stopped, the cooldown, the slot, or the pass's turn once the
/// cooldown is over (§19.1).
#[test]
fn a_waiting_pass_is_never_synced() {
    let until = UNIX_EPOCH + Duration::from_secs(60);
    let stopped = RetrySync::Stopped;
    let started = RetrySync::Started { cooling_until: None };
    let cooling = RetrySync::Started { cooling_until: Some(until) };
    let blocked = LastPass::Blocked { slot: slot(7), reason: key_unavailable(), error: None };
    let limited = LastPass::Failed {
        failure: StatusFailure::Storage(StorageFailure::RateLimited),
        error: "slow down".into(),
    };
    let shown =
        |last: &LastPass, sync| Facts { last: Some(last.clone()), sync, ..connected() }.state();
    assert_eq!(shown(&blocked, started), SyncState::WaitingToSync(SyncWait::Blocked));
    assert_eq!(shown(&blocked, stopped), SyncState::WaitingToSync(SyncWait::NotSyncing));
    assert_eq!(shown(&blocked, cooling), SyncState::WaitingToSync(SyncWait::CoolingDown { until }));
    assert_eq!(shown(&limited, cooling), SyncState::WaitingToSync(SyncWait::CoolingDown { until }));
    assert_eq!(shown(&limited, started), SyncState::WaitingToSync(SyncWait::Queued));
    assert_eq!(shown(&limited, stopped), SyncState::WaitingToSync(SyncWait::NotSyncing));
}

/// Synced means nothing waits, writes included. A write in the send queue,
/// with nothing moving and the last pass synced or none ended here yet,
/// waits to sync: for what holds every transfer, else its turn. A queued
/// bound upload's wait comes before the write's (§19.1).
#[test]
fn a_queued_write_is_never_synced() {
    let until = UNIX_EPOCH + Duration::from_secs(60);
    for last in [None, Some(LastPass::Synced)] {
        let shown = |sync| Facts { last: last.clone(), unsent: true, sync, ..connected() }.state();
        assert_eq!(
            shown(RetrySync::Started { cooling_until: None }),
            SyncState::WaitingToSync(SyncWait::Queued)
        );
        assert_eq!(shown(RetrySync::Stopped), SyncState::WaitingToSync(SyncWait::NotSyncing));
        assert_eq!(
            shown(RetrySync::Started { cooling_until: Some(until) }),
            SyncState::WaitingToSync(SyncWait::CoolingDown { until })
        );
        // The pause holds trailing uploads only, which the state never
        // reads: a write beside a paused bound upload waits for its turn.
        let paused = Facts {
            last: last.clone(),
            unsent: true,
            paused: true,
            uploads: vec![upload(Progress::Queued)],
            queued: true,
            ..connected()
        };
        assert_eq!(paused.state(), SyncState::WaitingToSync(SyncWait::Queued));
        let moving = Facts { last: last.clone(), unsent: true, moving: true, ..connected() };
        assert_eq!(moving.state(), SyncState::Syncing);
    }
}

/// A bug stays failed while its work retries: a record of storage's bug
/// names failed, as one of a local bug does, never nothing (§19.1, E15).
#[test]
fn a_bug_names_failed_while_its_work_retries() {
    for reason in [
        PendingReason::Storage {
            provider: CloudProvider::S3,
            failure: StorageFailure::Bug,
            answered: false,
        },
        PendingReason::Failed(LocalFailure::Bug),
    ] {
        let bugged = upload_record(1, reason);
        let facts = Facts { records: vec![bugged.clone()], ..connected() };
        assert_eq!(facts.state(), SyncState::Failed(SyncCause::Pending(bugged)));
    }
}

/// The uploads the database reads: the first one sending, the first queued
/// one with no record, and every one with a record, in queue order
/// (`sync_facts.rs`).
fn reduced(queue: &[Upload]) -> Vec<Upload> {
    let first = |wanted: &dyn Fn(&Upload) -> bool| queue.iter().position(wanted);
    let sending = first(&|upload| upload.progress == Progress::Moving);
    let waiting = first(&|upload| upload.progress == Progress::Queued && upload.record.is_none());
    queue
        .iter()
        .enumerate()
        .filter(|(at, upload)| {
            upload.record.is_some() || Some(*at) == sending || Some(*at) == waiting
        })
        .map(|(_, upload)| upload.clone())
        .collect()
}

/// The state from the reduced uploads equals the state from the whole queue,
/// for every queue of up to three bound uploads over every progress, retry
/// and record (none, one that names a state, one that names none), under
/// each way the last pass ended.
#[test]
fn the_reduced_uploads_show_the_state_of_the_whole_queue() {
    let at = |n: u64| UNIX_EPOCH + Duration::from_secs(n);
    let kinds = [
        PendingReason::FileUnavailable(FileMissingReason::Lost),
        PendingReason::Storage {
            provider: CloudProvider::S3,
            failure: StorageFailure::QuotaExceeded,
            answered: false,
        },
        PendingReason::Paused,
        PendingReason::Waits(Prerequisite::SequenceCatchUp),
    ];
    let mut alphabet = Vec::new();
    for progress in [Progress::Moving, Progress::Stored, Progress::Queued] {
        for retrying in [false, true] {
            for record in [None, Some(0), Some(1), Some(2)] {
                alphabet.push((progress, retrying, record));
            }
        }
    }
    let build = |picks: &[usize]| -> Vec<Upload> {
        (0_u64..)
            .zip(picks)
            .map(|(n, pick)| {
                let (progress, retrying, record) = alphabet[*pick];
                Upload {
                    progress,
                    bound: true,
                    retry_at: retrying.then(|| at(10 + n)),
                    record: record.map(|kind| upload_record(n.into(), kinds[kind].clone())),
                }
            })
            .collect()
    };
    let mut queues = vec![Vec::new()];
    for a in 0..alphabet.len() {
        queues.push(vec![a]);
        for b in 0..alphabet.len() {
            queues.push(vec![a, b]);
            for c in 0..alphabet.len() {
                queues.push(vec![a, b, c]);
            }
        }
    }
    let lasts = [None, offline()];
    for picks in &queues {
        let queue = build(picks);
        for last in &lasts {
            let whole = Facts {
                last: last.clone(),
                uploads: queue.clone(),
                queued: queue.iter().any(|upload| upload.progress != Progress::Stored),
                ..connected()
            };
            let kept = Facts { uploads: reduced(&queue), ..whole.clone() };
            assert_eq!(kept.state(), whole.state(), "{queue:?}");
        }
    }
}

/// A closed store's state is the one opening shows before sync starts: the
/// move and the cooldown a process that ended mid-pass left in its rows
/// show nothing, so its queued cover waits with sync not running, as a
/// queued write alone does, and the credentials the keychain holds now
/// decide signed out (api.md E1, E5).
#[test]
fn a_closed_store_shows_what_opening_shows() {
    let facts = coven_database::SyncFacts::<PendingRecord> {
        row: coven_database::SyncStatusRow {
            location: RecordedLocation::Connected("Dropbox".into()),
            last_pass: Some(serde_json::to_vec(&LastPass::Synced).unwrap()),
            moving: true,
            last_synced: Some(UNIX_EPOCH + Duration::from_secs(30)),
        },
        retry: coven_database::RetryState {
            sync: RetrySync::Started { cooling_until: Some(UNIX_EPOCH + Duration::from_secs(90)) },
            held: false,
        },
        unsent: true,
        uploads: vec![coven_database::UploadFacts {
            sending: false,
            stored: false,
            retry_at: None,
            record: None,
        }],
        queued: true,
        records: Vec::new(),
        paused: false,
        unmetered_only: false,
        metered: false,
        keys_changing: false,
        pass_retry: None,
        operation_retries: Vec::new(),
    };
    assert_eq!(SyncStatus::computed(facts.clone()).unwrap().state, SyncState::Syncing);
    let closed = |location| SyncStatus::closed(facts.clone(), location).unwrap();
    assert_eq!(
        closed(RecordedLocation::Connected(CloudProvider::Dropbox)),
        SyncStatus {
            state: SyncState::WaitingToSync(SyncWait::NotSyncing),
            last_synced: Some(UNIX_EPOCH + Duration::from_secs(30)),
        }
    );
    assert_eq!(
        closed(RecordedLocation::SignedOut(CloudProvider::Dropbox)).state,
        SyncState::SignedOut { provider: CloudProvider::Dropbox, cause: None }
    );
    let write_only = coven_database::SyncFacts { uploads: Vec::new(), queued: false, ..facts };
    assert_eq!(
        SyncStatus::closed(write_only, RecordedLocation::Connected(CloudProvider::Dropbox))
            .unwrap()
            .state,
        SyncState::WaitingToSync(SyncWait::NotSyncing)
    );
}

/// An upload of each kind: bound or trailing, sending, stored or queued, with
/// a backoff or not, and a record naming a state, one naming none or none.
fn phase_uploads() -> Vec<Upload> {
    let mut uploads = trailing_uploads();
    let bound: Vec<Upload> =
        uploads.iter().map(|upload| Upload { bound: true, ..upload.clone() }).collect();
    uploads.extend(bound);
    uploads
}

/// Every set of facts the phase differential compares: sync stopped, running
/// and cooling; the last pass none, blocked at a slot or offline; and the
/// pause, the setting to move files only on unmetered networks, the network
/// and a self-replacement's write pause each on or off, over every upload of
/// `phase_uploads`.
fn phase_cases() -> Vec<Facts> {
    let cooling = RetrySync::Started { cooling_until: Some(UNIX_EPOCH + Duration::from_secs(60)) };
    let blocked = Some(LastPass::Blocked { slot: slot(7), reason: key_unavailable(), error: None });
    let mut cases = Vec::new();
    for sync in [RetrySync::Stopped, RetrySync::Started { cooling_until: None }, cooling] {
        for last in [None, blocked.clone(), offline()] {
            for bits in 0..16_u8 {
                cases.push(Facts {
                    sync,
                    last: last.clone(),
                    paused: bits & 1 != 0,
                    unmetered_only: bits & 2 != 0,
                    metered: bits & 4 != 0,
                    keys_changing: bits & 8 != 0,
                    uploads: phase_uploads(),
                    queued: true,
                    ..connected()
                });
            }
        }
    }
    cases
}

/// Each upload's phase agrees with the Lean model's `Upload.phase`, bound and
/// trailing, under every wait, setting and network (§17.9, proofs/io.md).
#[test]
#[ignore = "requires COVEN_IO_LEAN"]
fn io_differential_upload_phase() {
    let cases = phase_cases();
    let inputs: Vec<Value> = cases
        .iter()
        .map(|facts| {
            let mut input = facts_json(facts, &[]);
            input["check"] = json!("upload_phase");
            input
        })
        .collect();
    let expected = crate::tests::io_runner(&inputs);
    assert_eq!(cases.len(), expected.len());
    for ((facts, input), expected) in cases.iter().zip(&inputs).zip(&expected) {
        let actual: Vec<Value> =
            facts.uploads.iter().map(|upload| phase_json(&facts.upload_phase(upload))).collect();
        assert_eq!(&Value::Array(actual), expected, "{input}");
    }
}

/// A failure that clears by itself and names no state of its own is one wait
/// with the time of the next try, whichever way it was met: the pass's own
/// failure with the provider answering, a slot's application, coven's own
/// operation, a cooldown over. A pass that got no answer is offline, and a
/// failure only a person clears keeps its own state (§19.1).
#[test]
fn a_failure_that_clears_by_itself_waits_for_its_backoff() {
    let at = UNIX_EPOCH + Duration::from_secs(5);
    let network = |answered| PendingReason::Storage {
        provider: CloudProvider::S3,
        failure: StorageFailure::Network,
        answered,
    };
    let failed = |failure| LastPass::Failed { failure, error: "503".into() };
    let blocked = |reason| LastPass::Blocked { slot: slot(7), reason, error: None };
    let met = [
        failed(StatusFailure::Storage(StorageFailure::Network)),
        failed(StatusFailure::Storage(StorageFailure::Conflict)),
        failed(StatusFailure::Storage(StorageFailure::SessionExpired)),
        failed(StatusFailure::Storage(StorageFailure::RateLimited)),
        failed(StatusFailure::Operation(record(network(true)))),
        failed(StatusFailure::Operation(record(PendingReason::Failed(LocalFailure::Shutdown)))),
        blocked(network(true)),
    ];
    for last in met {
        let armed = Facts { last: Some(last.clone()), pass_retry: Some(at), ..connected() };
        assert_eq!(armed.state(), SyncState::WaitingToSync(SyncWait::Retrying { at }), "{last:?}");
        let turn = Facts { pass_retry: None, ..armed.clone() };
        assert_eq!(turn.state(), SyncState::WaitingToSync(SyncWait::Queued), "{last:?}");
        let written = Facts { unsent: true, ..armed };
        assert_eq!(written.state(), SyncState::WaitingToSync(SyncWait::Retrying { at }));
    }
    let blocked_on_a_key =
        Facts { last: Some(blocked(key_unavailable())), pass_retry: Some(at), ..connected() };
    assert_eq!(blocked_on_a_key.state(), SyncState::WaitingToSync(SyncWait::Blocked));
    let no_answer = Facts { last: offline(), pass_retry: Some(at), ..connected() };
    assert!(matches!(no_answer.state(), SyncState::Offline(_)), "{:?}", no_answer.state());
    let refused = failed(StatusFailure::Storage(StorageFailure::Refused));
    let never = Facts { last: Some(refused), pass_retry: Some(at), ..connected() };
    assert!(matches!(never.state(), SyncState::Failed(_)), "{:?}", never.state());
    let operation =
        failed(StatusFailure::Operation(record(PendingReason::Failed(LocalFailure::Operation))));
    let failed_for_good = Facts { last: Some(operation), pass_retry: Some(at), ..connected() };
    assert!(matches!(failed_for_good.state(), SyncState::Failed(_)));
}

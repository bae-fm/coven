use super::*;
use crate::{
    DeviceIdEvidence, FileMissingReason, FileSourceFailure, Prerequisite, ProviderAccessAction,
    Refusal, RequiredUpdate,
};
use crate::{HolderDevice, KeyHolder, PendingSubject, UploadPhase, UploadWait};
use coven_database::{EntryTarget, OperationRejection, PassMove, Removal, UnsentWork, WriteCutoff};
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
        PendingReason::Waits(Prerequisite::SequenceCatchUp) => json!("waits"),
        PendingReason::Waits(Prerequisite::Reload(_)) => json!("waitsReload"),
        PendingReason::Waits(Prerequisite::Floor) => json!("waitsFloor"),
        PendingReason::KeyUnavailable { holders: Some(holders), .. } if holders.is_empty() => {
            json!("keyUnavailableNoHolder")
        }
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
        PendingReason::Removed(Removal::Device) => json!("removedDevice"),
        PendingReason::Removed(Removal::Member) => json!("removedMember"),
        PendingReason::Removed(Removal::NeverAdmitted) => json!("neverAdmitted"),
        PendingReason::MemberKeysReplaced { .. } => json!("memberKeysReplaced"),
    }
}

fn member() -> MemberId {
    MemberId(uuid::Uuid::from_u128(1))
}

/// A member that holds the key and reads its audience, with a phone.
pub(super) fn holder() -> KeyHolder {
    KeyHolder {
        member: member(),
        reads_audience: true,
        devices: vec![HolderDevice { id: DeviceId(1), name: "Carol's phone".into() }],
    }
}

pub(super) fn key_unavailable() -> PendingReason {
    PendingReason::KeyUnavailable {
        audience: Audience::Store,
        key: KeyId(uuid::Uuid::from_u128(2)),
        holders: Some(vec![holder()]),
    }
}

/// The same wait with no member holding the key.
pub(super) fn key_unavailable_without_holder() -> PendingReason {
    PendingReason::KeyUnavailable {
        audience: Audience::Store,
        key: KeyId(uuid::Uuid::from_u128(2)),
        holders: Some(Vec::new()),
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
        PendingReason::Waits(Prerequisite::Reload(Audience::Store)),
        PendingReason::Waits(Prerequisite::Floor),
        key_unavailable(),
        key_unavailable_without_holder(),
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
        PendingReason::Removed(Removal::Device),
        PendingReason::Removed(Removal::Member),
        PendingReason::Removed(Removal::NeverAdmitted),
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
pub(super) fn upload_record(n: u128, reason: PendingReason) -> PendingRecord {
    PendingRecord::new(
        PendingSubject::Upload { device: DeviceId(1), object: ObjectId(uuid::Uuid::from_u128(n)) },
        reason,
    )
}

/// The record of the first write in the send queue waiting for a key.
pub(super) fn write_key_record() -> PendingRecord {
    PendingRecord::new(
        PendingSubject::Write(coven_merge::WriteId { device: DeviceId(1), number: 1 }),
        key_unavailable(),
    )
}

pub(super) fn record(reason: PendingReason) -> PendingRecord {
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
        failed(StatusFailure::Removed(Removal::Device)),
        failed(StatusFailure::Removed(Removal::Member)),
        failed(StatusFailure::Removed(Removal::NeverAdmitted)),
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
            StatusFailure::Removed(Removal::Device) => json!("removedDevice"),
            StatusFailure::Removed(Removal::Member) => json!("removedMember"),
            StatusFailure::Removed(Removal::NeverAdmitted) => json!("neverAdmitted"),
            StatusFailure::DeviceIdInUse(_) => json!("replaced"),
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
        SyncState::Syncing(_) => "syncing",
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
        SyncState::Removed { .. } => "removed",
        SyncState::NeedsReset { .. } => "needsReset",
        SyncState::Failed(_) => "failed",
    }
}

#[deny(clippy::wildcard_enum_match_arm)]
fn wait_name(wait: &SyncWait) -> &'static str {
    match wait {
        SyncWait::NotSyncing => "notSyncing",
        SyncWait::CoolingDown { .. } => "coolingDown",
        SyncWait::KeyCopy { .. } => "keyCopy",
        SyncWait::Snapshot { .. } => "snapshot",
        SyncWait::Retrying { .. } => "retrying",
    }
}

#[deny(clippy::wildcard_enum_match_arm)]
fn activity_name(activity: SyncActivity) -> &'static str {
    match activity {
        SyncActivity::Receiving => "receiving",
        SyncActivity::Sending => "sending",
        SyncActivity::UploadingBoundFile => "uploadingBound",
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

/// What the queues hold with a write queued or not, and no file.
pub(super) fn writes(queued: bool) -> UnsentWork {
    UnsentWork { writes: u64::from(queued), files: 0 }
}

pub(super) fn connected() -> Facts {
    Facts {
        location: RecordedLocation::Connected(CloudProvider::S3),
        last: None,
        moved: None,
        uploads: Vec::new(),
        queued: false,
        unsent: writes(false),
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
pub(super) fn upload(progress: Progress) -> Upload {
    Upload { progress, bound: true, retry_at: None, record: None }
}

pub(super) fn offline() -> Option<LastPass> {
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
        "moved": match facts.moved {
            None => Value::Null,
            Some(PassMove::Read) => json!("read"),
            Some(PassMove::Claimed) => json!("claimed"),
        },
        "uploads": uploads,
        "unsent": facts.unsent.writes > 0,
        "unsentFiles": facts.unsent.files > 0,
        "running": running,
        "cooling": cooling,
        "paused": facts.paused,
        "unmeteredOnly": facts.unmetered_only,
        "metered": facts.metered,
        "keysChanging": facts.keys_changing,
        "writeKey": facts.write_key().is_some(),
        "passRetrying": facts.pass_retry.is_some(),
        "records": records,
    })
}

/// The state `facts` computes, as the model's runner reports one.
fn shown(facts: &Facts) -> Value {
    let state = facts.state();
    let wait = match &state {
        SyncState::WaitingToSync(wait) => json!(wait_name(wait)),
        _ => Value::Null,
    };
    let activity = match &state {
        SyncState::Syncing(activity) => json!(activity_name(*activity)),
        _ => Value::Null,
    };
    let (removal, reset, unsent) = match &state {
        SyncState::Removed { removal, unsent } => (
            json!(match removal {
                Removal::Device => "device",
                Removal::Member => "member",
                Removal::NeverAdmitted => "neverAdmitted",
            }),
            Value::Null,
            json!({"writes": unsent.writes > 0, "files": unsent.files > 0}),
        ),
        SyncState::NeedsReset { reason, unsent } => (
            Value::Null,
            json!(match reason {
                ResetReason::Replaced => "replaced",
                ResetReason::MemberKeysReplaced { .. } => "memberKeysReplaced",
            }),
            json!({"writes": unsent.writes > 0, "files": unsent.files > 0}),
        ),
        _ => (Value::Null, Value::Null, Value::Null),
    };
    json!({
        "state": state_name(&state),
        "wait": wait,
        "activity": activity,
        "removal": removal,
        "reset": reset,
        "unsent": unsent,
    })
}

/// Every set of facts the differential test compares: each pass of every
/// kind with a write queued, a stored file, both or neither; each reason as
/// a record, as a queued upload's record with and without the record in the
/// list, and beside a location with no credentials; every location, move
/// and upload progress combination, offline; every activity source alone and
/// in pairs (what the pass moved, a bound upload moving or next in line, a
/// write, the next pass's turn), under each way sync can be held; a write
/// recorded as waiting for a key beside a held or retrying pass and file; and
/// queued bound uploads under
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
                    unsent: writes(unsent),
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
        for moved in [None, Some(PassMove::Read), Some(PassMove::Claimed)] {
            for progress in
                [None, Some(Progress::Moving), Some(Progress::Stored), Some(Progress::Queued)]
            {
                cases.push(Facts {
                    location,
                    moved,
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
        cases.push(Facts { sync, unsent: writes(true), ..connected() });
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
                            unsent: writes(unsent),
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
    let limited = Some(LastPass::Failed {
        failure: StatusFailure::Storage(StorageFailure::RateLimited),
        error: "slow down".into(),
    });
    for sync in [RetrySync::Stopped, RetrySync::Started { cooling_until: None }, cooling] {
        for last in [None, Some(LastPass::Synced), limited.clone(), offline()] {
            for moved in [None, Some(PassMove::Read), Some(PassMove::Claimed)] {
                for unsent in [false, true] {
                    let mut retrying = upload(Progress::Queued);
                    retrying.retry_at = Some(UNIX_EPOCH + Duration::from_secs(9));
                    for uploads in [
                        Vec::new(),
                        vec![upload(Progress::Moving)],
                        vec![upload(Progress::Queued)],
                        vec![retrying.clone()],
                        vec![retrying, upload(Progress::Queued)],
                    ] {
                        cases.push(Facts {
                            sync,
                            last: last.clone(),
                            moved,
                            unsent: writes(unsent),
                            uploads,
                            ..connected()
                        });
                    }
                }
            }
        }
    }
    let keyed = Some(LastPass::Blocked { slot: slot(7), reason: key_unavailable(), error: None });
    for sync in [RetrySync::Stopped, RetrySync::Started { cooling_until: None }, cooling] {
        for last in [None, keyed.clone(), limited.clone()] {
            for unsent in [false, true] {
                let mut retrying = upload(Progress::Queued);
                retrying.retry_at = Some(UNIX_EPOCH + Duration::from_secs(9));
                for uploads in [Vec::new(), vec![retrying], vec![upload(Progress::Queued)]] {
                    cases.push(Facts {
                        sync,
                        last: last.clone(),
                        unsent: writes(unsent),
                        uploads,
                        records: vec![write_key_record()],
                        ..connected()
                    });
                }
            }
        }
    }
    // Two failures that name a standing state, one from the pass and one from a
    // record or a queued upload's record, in every pairing: the first the
    // state's candidates list names it, and the pass only while nothing moves.
    let standing: Vec<(StatusFailure, PendingReason)> = vec![
        (StatusFailure::Removed(Removal::Device), PendingReason::Removed(Removal::Device)),
        (StatusFailure::Removed(Removal::Member), PendingReason::Removed(Removal::Member)),
        (
            StatusFailure::Removed(Removal::NeverAdmitted),
            PendingReason::Removed(Removal::NeverAdmitted),
        ),
        (
            StatusFailure::DeviceIdInUse(DeviceIdEvidence::Replaced),
            PendingReason::MemberKeysReplaced { member: member() },
        ),
    ];
    for (failure, _) in &standing {
        for (_, reason) in &standing {
            for moved in [None, Some(PassMove::Read)] {
                let last = Some(LastPass::Failed { failure: failure.clone(), error: "e".into() });
                let mut queued = upload(Progress::Queued);
                queued.record = Some(upload_record(1, reason.clone()));
                cases.push(Facts {
                    last: last.clone(),
                    moved,
                    records: vec![record(reason.clone())],
                    unsent: writes(true),
                    ..connected()
                });
                cases.push(Facts { last, moved, uploads: vec![queued], ..connected() });
            }
        }
        for (_, first) in &standing {
            for (_, second) in &standing {
                cases.push(Facts {
                    records: vec![record(first.clone()), record(second.clone())],
                    ..connected()
                });
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
    // The upload queue holds the bound uploads the facts list and the
    // trailing ones; its count is what a stopped state says is unsent, so each
    // run's cases carry it, with and without trailing uploads.
    for trailing in [trailing_uploads(), Vec::new()] {
        let cases: Vec<Facts> = cases()
            .into_iter()
            .map(|mut facts| {
                facts.unsent.files = (facts.uploads.len() + trailing.len()) as u64;
                facts
            })
            .collect();
        let inputs: Vec<Value> = cases.iter().map(|facts| facts_json(facts, &trailing)).collect();
        let actual: Vec<Value> = cases.iter().map(shown).collect();
        let expected = crate::tests::io_runner(&inputs);
        assert_eq!(actual.len(), expected.len());
        for ((input, actual), expected) in inputs.iter().zip(&actual).zip(&expected) {
            assert_eq!(actual, expected, "{input}");
        }
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
    assert_eq!(kinds.len(), 26, "{kinds:?}");
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

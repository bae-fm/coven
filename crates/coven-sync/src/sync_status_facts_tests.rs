use super::*;
use crate::sync_status::tests::{
    connected, key_unavailable, offline, record, upload, upload_record, write_key_record,
};
use crate::{
    FileMissingReason, LocalFailure, PendingReason, Prerequisite, SyncStatus, UploadPhase,
    UploadWait,
};
use coven_foundation::id_source::KeyId;
use coven_merge::{slot, Audience};
use coven_storage::StorageFailure;
use std::time::{Duration, UNIX_EPOCH};

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
/// them: sync stopped, the cooldown or the slot; once the cooldown is over
/// the pass's turn has come, and that is syncing (§19.1).
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
    let key = SyncWait::KeyCopy { audience: Audience::Store, key: KeyId(uuid::Uuid::from_u128(2)) };
    assert_eq!(shown(&blocked, started), SyncState::WaitingToSync(key.clone()));
    assert_eq!(shown(&blocked, stopped), SyncState::WaitingToSync(SyncWait::NotSyncing));
    assert_eq!(shown(&blocked, cooling), SyncState::WaitingToSync(SyncWait::CoolingDown { until }));
    assert_eq!(shown(&limited, cooling), SyncState::WaitingToSync(SyncWait::CoolingDown { until }));
    assert_eq!(shown(&limited, started), SyncState::Syncing(SyncActivity::Receiving));
    assert_eq!(shown(&limited, stopped), SyncState::WaitingToSync(SyncWait::NotSyncing));
}

/// Synced means nothing waits, writes included. A write in the send queue,
/// with nothing moving and the last pass synced or none ended here yet,
/// waits to sync for what holds every transfer, else it is next in line and
/// the state is syncing, sending. A queued bound upload comes before the
/// write (§19.1).
#[test]
fn a_queued_write_is_never_synced() {
    let until = UNIX_EPOCH + Duration::from_secs(60);
    for last in [None, Some(LastPass::Synced)] {
        let shown = |sync| Facts { last: last.clone(), unsent: true, sync, ..connected() }.state();
        assert_eq!(
            shown(RetrySync::Started { cooling_until: None }),
            SyncState::Syncing(SyncActivity::Sending)
        );
        assert_eq!(shown(RetrySync::Stopped), SyncState::WaitingToSync(SyncWait::NotSyncing));
        assert_eq!(
            shown(RetrySync::Started { cooling_until: Some(until) }),
            SyncState::WaitingToSync(SyncWait::CoolingDown { until })
        );
        // The pause holds trailing uploads only, which the state never
        // reads: a write beside a paused bound upload is next in line behind
        // the upload.
        let paused = Facts {
            last: last.clone(),
            unsent: true,
            paused: true,
            uploads: vec![upload(Progress::Queued)],
            queued: true,
            ..connected()
        };
        assert_eq!(paused.state(), SyncState::Syncing(SyncActivity::UploadingBoundFile));
        let claimed = Facts {
            last: last.clone(),
            unsent: true,
            moved: Some(PassMove::Claimed),
            ..connected()
        };
        assert_eq!(claimed.state(), SyncState::Syncing(SyncActivity::Sending));
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
            moved: Some(PassMove::Read),
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
    assert_eq!(
        SyncStatus::computed(facts.clone()).unwrap().state,
        SyncState::Syncing(SyncActivity::Receiving)
    );
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
        assert_eq!(turn.state(), SyncState::Syncing(SyncActivity::Receiving), "{last:?}");
        let written = Facts { unsent: true, ..armed };
        assert_eq!(written.state(), SyncState::WaitingToSync(SyncWait::Retrying { at }));
    }
    let key = SyncWait::KeyCopy { audience: Audience::Store, key: KeyId(uuid::Uuid::from_u128(2)) };
    let blocked_on_a_key =
        Facts { last: Some(blocked(key_unavailable())), pass_retry: Some(at), ..connected() };
    assert_eq!(blocked_on_a_key.state(), SyncState::WaitingToSync(key));
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

/// The uploads the database reads: the first one sending, the first queued
/// one with no record and no retry, the first with no record that waits out a
/// retry, and every one with a record, in queue order (`sync_facts.rs`).
fn reduced(queue: &[Upload]) -> Vec<Upload> {
    let first = |wanted: &dyn Fn(&Upload) -> bool| queue.iter().position(wanted);
    let queued_free =
        |upload: &Upload| upload.progress == Progress::Queued && upload.record.is_none();
    let sending = first(&|upload| upload.progress == Progress::Moving);
    let turn = first(&|upload| queued_free(upload) && upload.retry_at.is_none());
    let retrying = first(&|upload| queued_free(upload) && upload.retry_at.is_some());
    queue
        .iter()
        .enumerate()
        .filter(|(at, upload)| {
            upload.record.is_some()
                || Some(*at) == sending
                || Some(*at) == turn
                || Some(*at) == retrying
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
    let limited = Some(LastPass::Failed {
        failure: StatusFailure::Storage(StorageFailure::RateLimited),
        error: "slow down".into(),
    });
    let lasts = [None, offline(), limited];
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

fn rate_limited() -> Option<LastPass> {
    Some(LastPass::Failed {
        failure: StatusFailure::Storage(StorageFailure::RateLimited),
        error: "slow down".into(),
    })
}

/// A write in the send queue with nothing holding the pass is next in line:
/// the state is syncing, sending, from the step that commits it. Sync stopped
/// or a cooldown holds it, and it waits to sync (§19.1).
#[test]
fn a_write_with_nothing_holding_it_is_sending() {
    let until = UNIX_EPOCH + Duration::from_secs(60);
    for last in [None, Some(LastPass::Synced)] {
        let write = Facts { last, unsent: true, ..connected() };
        assert_eq!(write.state(), SyncState::Syncing(SyncActivity::Sending));
        let stopped = Facts { sync: RetrySync::Stopped, ..write.clone() };
        assert_eq!(stopped.state(), SyncState::WaitingToSync(SyncWait::NotSyncing));
        let cooling = Facts { sync: RetrySync::Started { cooling_until: Some(until) }, ..write };
        assert_eq!(cooling.state(), SyncState::WaitingToSync(SyncWait::CoolingDown { until }));
    }
}

/// A bound cover queued behind another one is next in line in the bound lane:
/// syncing, uploading a bound file. Its own backoff, or sync stopped, holds
/// it, and it waits to sync (§19.1).
#[test]
fn a_bound_file_waiting_its_turn_is_uploading() {
    let at = UNIX_EPOCH + Duration::from_secs(9);
    let queue = vec![upload(Progress::Queued), upload(Progress::Queued)];
    let covers = Facts { uploads: queue.clone(), queued: true, ..connected() };
    assert_eq!(covers.state(), SyncState::Syncing(SyncActivity::UploadingBoundFile));
    let behind_a_sending_one = Facts {
        uploads: vec![upload(Progress::Moving), upload(Progress::Queued)],
        ..covers.clone()
    };
    assert_eq!(behind_a_sending_one.state(), SyncState::Syncing(SyncActivity::UploadingBoundFile));
    let mut retrying = upload(Progress::Queued);
    retrying.retry_at = Some(at);
    let waiting = Facts { uploads: vec![retrying.clone()], ..covers.clone() };
    assert_eq!(waiting.state(), SyncState::WaitingToSync(SyncWait::Retrying { at }));
    let free_behind = Facts { uploads: vec![retrying, upload(Progress::Queued)], ..covers.clone() };
    assert_eq!(free_behind.state(), SyncState::Syncing(SyncActivity::UploadingBoundFile));
    let stopped = Facts { sync: RetrySync::Stopped, ..covers };
    assert_eq!(stopped.state(), SyncState::WaitingToSync(SyncWait::NotSyncing));
}

/// A pass a provider's cooldown cut short waits on the cooldown. Once the
/// cooldown is over the next pass's turn has come: syncing, receiving (§19.1).
#[test]
fn the_pass_after_a_cooldown_is_receiving() {
    let until = UNIX_EPOCH + Duration::from_secs(60);
    let over = Facts { last: rate_limited(), ..connected() };
    assert_eq!(over.state(), SyncState::Syncing(SyncActivity::Receiving));
    let cooling = Facts { sync: RetrySync::Started { cooling_until: Some(until) }, ..over.clone() };
    assert_eq!(cooling.state(), SyncState::WaitingToSync(SyncWait::CoolingDown { until }));
    let stopped = Facts { sync: RetrySync::Stopped, ..over };
    assert_eq!(stopped.state(), SyncState::WaitingToSync(SyncWait::NotSyncing));
}

/// A store whose last pass synced, with nothing queued and nothing moved,
/// stays synced: an idle pass shows no activity (§19.1).
#[test]
fn an_idle_pass_on_a_synced_store_stays_synced() {
    let idle = Facts { last: Some(LastPass::Synced), ..connected() };
    assert_eq!(idle.state(), SyncState::Synced);
    let stored = Facts { uploads: vec![upload(Progress::Stored)], ..idle };
    assert_eq!(stored.state(), SyncState::Synced);
}

/// What the pass moved comes first, then a bound file, then a write, then the
/// pass's own turn, as the model's `Facts.activities` orders them.
#[test]
fn activities_show_in_order() {
    let at = |facts: Facts| facts.state();
    let write = Facts { unsent: true, ..connected() };
    let covers = Facts { uploads: vec![upload(Progress::Queued)], queued: true, ..write.clone() };
    assert_eq!(at(covers.clone()), SyncState::Syncing(SyncActivity::UploadingBoundFile));
    let read = Facts { moved: Some(PassMove::Read), ..covers.clone() };
    assert_eq!(at(read), SyncState::Syncing(SyncActivity::Receiving));
    let claimed = Facts { moved: Some(PassMove::Claimed), ..covers };
    assert_eq!(at(claimed), SyncState::Syncing(SyncActivity::Sending));
    let turn_and_write = Facts { last: rate_limited(), ..write };
    assert_eq!(at(turn_and_write), SyncState::Syncing(SyncActivity::Sending));
}

/// A write behind a bound cover waits with the cover: the writes behind a file
/// go out once it is stored, so a cover that waits out a backoff holds them,
/// and the cover's own wait shows (§17.4, §19.1).
#[test]
fn a_write_behind_a_waiting_cover_waits_with_it() {
    let at = UNIX_EPOCH + Duration::from_secs(9);
    let mut retrying = upload(Progress::Queued);
    retrying.retry_at = Some(at);
    let facts = Facts { unsent: true, uploads: vec![retrying], queued: true, ..connected() };
    assert_eq!(facts.state(), SyncState::WaitingToSync(SyncWait::Retrying { at }));
    let sending = Facts { uploads: vec![upload(Progress::Moving)], ..facts.clone() };
    assert_eq!(sending.state(), SyncState::Syncing(SyncActivity::UploadingBoundFile));
    let stored = Facts { uploads: vec![upload(Progress::Stored)], queued: false, ..facts };
    assert_eq!(stored.state(), SyncState::Syncing(SyncActivity::Sending));
}

/// A slot waiting on a Gifts key reads as the key copy wait with the audience
/// and the key; a slot holding a raise whose snapshot isn't named yet reads as
/// the snapshot wait; a slot whose reload met a newer floor holds nothing, and
/// the next pass's turn is syncing, receiving; a slot blocked with a reason it
/// never blocks with is failed (§19.1).
#[test]
fn a_blocked_slot_names_its_wait() {
    let gifts = Audience::Circle(coven_foundation::id_source::CircleId(uuid::Uuid::from_u128(5)));
    let key = KeyId(uuid::Uuid::from_u128(6));
    let blocked = |reason| Facts {
        last: Some(LastPass::Blocked { slot: slot(7), reason, error: None }),
        ..connected()
    };
    assert_eq!(
        blocked(PendingReason::KeyUnavailable { audience: gifts.clone(), key }).state(),
        SyncState::WaitingToSync(SyncWait::KeyCopy { audience: gifts.clone(), key })
    );
    assert_eq!(
        blocked(PendingReason::Waits(Prerequisite::Reload(gifts.clone()))).state(),
        SyncState::WaitingToSync(SyncWait::Snapshot { audience: gifts })
    );
    assert_eq!(
        blocked(PendingReason::Waits(Prerequisite::Floor)).state(),
        SyncState::Syncing(SyncActivity::Receiving)
    );
    assert!(matches!(
        blocked(PendingReason::Waits(Prerequisite::SequenceCatchUp)).state(),
        SyncState::Failed(_)
    ));
    // Every transfer waits behind the slot, a bound file too.
    let held = Facts {
        unsent: true,
        uploads: vec![upload(Progress::Queued)],
        queued: true,
        ..blocked(key_unavailable())
    };
    assert_eq!(held.upload_phase(&held.uploads[0]), UploadPhase::Waiting(UploadWait::Sync));
}

/// A queued write whose circle key isn't held waits for that key, behind what
/// holds the pass and a bound file; once the record is gone it is next in
/// line (§12.1, §19.1).
#[test]
fn a_queued_write_waits_for_its_key() {
    let key = SyncWait::KeyCopy { audience: Audience::Store, key: KeyId(uuid::Uuid::from_u128(2)) };
    let write = Facts { unsent: true, records: vec![write_key_record()], ..connected() };
    assert_eq!(write.state(), SyncState::WaitingToSync(key));
    let stopped = Facts { sync: RetrySync::Stopped, ..write.clone() };
    assert_eq!(stopped.state(), SyncState::WaitingToSync(SyncWait::NotSyncing));
    let sent = Facts { unsent: false, last: Some(LastPass::Synced), ..write.clone() };
    assert_eq!(sent.state(), SyncState::Synced);
    let cleared = Facts { records: Vec::new(), ..write };
    assert_eq!(cleared.state(), SyncState::Syncing(SyncActivity::Sending));
}

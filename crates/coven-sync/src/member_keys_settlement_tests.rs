//! Settling member-key replacement through the operation calls (§9, §10,
//! §18): a losing self-replacement is reported and never attempted again,
//! every failure resumes the same attempt, and discarding releases the pause
//! only when nothing remains behind it.

use super::member_keys_replacement::{fresh, notes, own_entries, schema, settle, write_note};
use super::*;
use coven_crypto::custody::{KeyError, MemberKeyCustody};
use coven_crypto::MemberIdentity;
use coven_database::{DbError, DropReason, EntryOutcome};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

/// Ben, another admin, replaces Carol's keys while her own replacement is
/// reserved but unpublished. His wins although hers is earlier: her call
/// reports the rejection, publishes nothing more, releases the pause and
/// keeps her losing candidate held. Her installation now holds no current
/// pair, so it cannot send.
#[tokio::test]
async fn an_admin_replacement_beats_self_replacement_without_a_new_attempt() {
    let storage = google();
    let [_a, mut b, mut c] = accounts_with(storage.clone(), notes, schema).await;
    let carol = c.member.member();
    // Carol's clock runs behind, so her entry is earlier.
    b.clock.set(UNIX_EPOCH + Duration::from_secs(1_000_000));
    let operation = begin(&mut c, Command::ReplaceOwnKeys).await;
    let next = own_entries(&c).await + 1;
    c.interrupt_entry(next).await;
    loop {
        match step(&mut c, operation).await {
            Ok(Progress::Advanced) => (),
            Err(SyncError::Storage(_)) => break,
            other => panic!("{:?}", other.map(|_| ())),
        }
    }
    c.restore_storage();
    let prepared = fresh(carol);
    let theirs = begin(&mut b, Command::ReplaceKeysFor(carol, public(&prepared))).await;
    assert!(matches!(finish(&mut b, theirs).await, Output::Unit));
    let winner = b.log().await.entries.last().unwrap().entry.position;
    let result = settle(&mut c, operation).await;
    assert!(
        matches!(&result, Err(SyncError::Rejected(DropReason::BeatenBy(id))) if *id == winner),
        "{:?}",
        result.err()
    );
    assert_eq!(own_entries(&c).await, next);
    assert!(c.db.operations().await.unwrap().is_empty());
    write_note(&c, "unpaused").await.unwrap();
    let held = c.identity.read().unwrap().unwrap();
    assert!(held.candidate().is_none());
    assert_eq!(held.keys().public_keys(), public(&c.member));
    assert_eq!(
        c.log().await.replay.state.members[&carol].keys,
        public(&prepared)
    );
    assert!(matches!(
        c.sync.sync_store_log().await,
        Err(SyncError::MemberKeysReplaced { member }) if member == carol
    ));
}

/// Identity custody whose saves can be refused, as a keychain failure would.
struct Flaky {
    held: Mutex<Option<MemberIdentity>>,
    refuse: AtomicBool,
}

impl MemberKeyCustody for Flaky {
    fn unlock(&self) -> Result<Option<MemberIdentity>, KeyError> {
        Ok(self.held.lock().unwrap().clone())
    }
    fn persist(&self, identity: &MemberIdentity) -> Result<(), KeyError> {
        if self.refuse.load(Ordering::SeqCst) {
            return Err(KeyError::Unavailable("keychain refused".into()));
        }
        *self.held.lock().unwrap() = Some(identity.clone());
        Ok(())
    }
    fn forget(&self) -> Result<(), KeyError> {
        self.held.lock().unwrap().take();
        Ok(())
    }
    fn close(&self) {}
}

/// Reopen `d` with identity custody `custody`, as a restart would.
async fn reopen_with(d: &mut Device, storage: Arc<MemoryStorage>, custody: Arc<Flaky>) {
    d.identity = Arc::new(coven_crypto::custody::KeySession::member(custody).unwrap());
    d.reopen(storage, notes(), schema()).await;
}

/// Carol's self-replacement survives a restart after keeping its candidate,
/// an interrupted publication and a refused custody save. Each retry
/// resumes the same attempt: one entry, binding the candidate kept before
/// the restart. Until custody installs the kept pair, writes stay paused and
/// discarding returns the pending record; then it finishes.
#[tokio::test]
async fn self_replacement_resumes_the_same_attempt_after_each_failure() {
    let storage = google();
    let [mut a, _b, mut c] = accounts_with(storage.clone(), notes, schema).await;
    let carol = c.member.member();
    let custody = Arc::new(Flaky {
        held: Mutex::new(Some(c.member.clone())),
        refuse: AtomicBool::new(false),
    });
    reopen_with(&mut c, storage.clone(), custody.clone()).await;
    let operation = begin(&mut c, Command::ReplaceOwnKeys).await;
    // A refused save keeps no candidate and leaves the step to retry.
    custody.refuse.store(true, Ordering::SeqCst);
    assert!(matches!(
        step(&mut c, operation).await,
        Err(SyncError::SecureStorage(_))
    ));
    custody.refuse.store(false, Ordering::SeqCst);
    assert!(matches!(
        step(&mut c, operation).await.unwrap(),
        Progress::Advanced
    ));
    let candidate = custody
        .held
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .candidate()
        .unwrap()
        .public_keys();
    // A crash after keeping the candidate reuses it.
    reopen_with(&mut c, storage.clone(), custody.clone()).await;
    let next = own_entries(&c).await + 1;
    c.interrupt_entry(next).await;
    loop {
        match step(&mut c, operation).await {
            Ok(Progress::Advanced) => (),
            Err(SyncError::Storage(_)) => break,
            other => panic!("{:?}", other.map(|_| ())),
        }
    }
    let reserved = c.db.local_store_log().await.unwrap().upload.unwrap();
    assert!(matches!(
        &reserved.entry.change,
        StoreChange::ReplaceMemberKeys { member, keys } if *member == carol && *keys == candidate
    ));
    // Discarding cannot release the pause behind a reserved entry.
    c.sync
        .record_operation_error(
            &c.operation(operation).await,
            &SyncError::from(StorageFailure::Network),
        )
        .await
        .unwrap();
    assert!(matches!(
        c.sync
            .begin_operation_call(Command::Discard(operation))
            .await,
        Err(SyncError::Pending(_))
    ));
    assert!(matches!(
        write_note(&c, "paused").await,
        Err(DbError::MemberKeysChanging)
    ));
    c.restore_storage();
    // The kept binding cannot be installed while custody refuses.
    custody.refuse.store(true, Ordering::SeqCst);
    let failed = loop {
        match step(&mut c, operation).await {
            Ok(Progress::Advanced) => (),
            Err(error) => break error,
            Ok(_) => panic!("finished without installing custody"),
        }
    };
    assert!(matches!(failed, SyncError::SecureStorage(_)), "{failed:?}");
    let log = c.log().await;
    assert_eq!(
        log.replay.entries[&reserved.entry.position],
        EntryOutcome::Kept
    );
    assert_eq!(log.replay.state.members[&carol].keys, candidate);
    assert!(matches!(
        write_note(&c, "paused").await,
        Err(DbError::MemberKeysChanging)
    ));
    // The candidate the kept binding names is what this installation signs
    // with once installed; until then it is no replaced installation.
    c.sync.sync_store_log().await.unwrap();
    custody.refuse.store(false, Ordering::SeqCst);
    assert!(matches!(settle(&mut c, operation).await, Ok(Output::Unit)));
    assert_eq!(own_entries(&c).await, next);
    assert_eq!(
        custody
            .held
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .keys()
            .public_keys(),
        candidate
    );
    write_note(&c, "after").await.unwrap();
    a.sync().await;
    assert_eq!(a.log().await.replay.state.members[&carol].keys, candidate);
}

/// Before anything is reserved, discarding a self-replacement drops its
/// unpublished candidate and releases the pause, but only while the held
/// pair is still current. After Ben replaced Carol's keys, it cannot.
#[tokio::test]
async fn discarding_before_reservation_needs_the_held_pair_current() {
    let storage = google();
    let [_a, mut b, mut c] = accounts_with(storage.clone(), notes, schema).await;
    let carol = c.member.member();
    let operation = begin(&mut c, Command::ReplaceOwnKeys).await;
    step(&mut c, operation).await.unwrap();
    assert!(c.identity.read().unwrap().unwrap().candidate().is_some());
    c.sync
        .record_operation_condition(&c.operation(operation).await, PendingReason::Paused)
        .await
        .unwrap();
    assert!(matches!(
        c.sync
            .begin_operation_call(Command::Discard(operation))
            .await,
        Ok(Begun::Value(Output::Unit))
    ));
    assert!(c.identity.read().unwrap().unwrap().candidate().is_none());
    write_note(&c, "released").await.unwrap();
    c.catch_up().await;
    assert_eq!(c.writes.upload_writes().await.unwrap().len(), 1);
    // Carol keeps a candidate; then Ben's replacement lands before hers is
    // reserved, and reserving catches up first: her held pair is not current.
    let operation = begin(&mut c, Command::ReplaceOwnKeys).await;
    assert!(matches!(
        step(&mut c, operation).await.unwrap(),
        Progress::Advanced
    ));
    let theirs = begin(
        &mut b,
        Command::ReplaceKeysFor(carol, public(&fresh(carol))),
    )
    .await;
    finish(&mut b, theirs).await;
    let refused = step(&mut c, operation).await;
    assert!(
        matches!(refused, Err(SyncError::MemberKeysReplaced { member }) if member == carol),
        "{:?}",
        refused.map(|_| ())
    );
    c.sync
        .record_operation_error(
            &c.operation(operation).await,
            &SyncError::MemberKeysReplaced { member: carol },
        )
        .await
        .unwrap();
    let pending = c
        .sync
        .begin_operation_call(Command::Discard(operation))
        .await;
    assert!(
        matches!(&pending, Err(SyncError::Pending(record))
            if record.reason == PendingReason::MemberKeysReplaced { member: carol }),
        "{:?}",
        pending.err()
    );
    assert!(matches!(
        write_note(&c, "paused").await,
        Err(DbError::MemberKeysChanging)
    ));
}

/// Carol's snapshot of the store, as a maintenance operation.
async fn snapshot(d: &Device) -> OperationId {
    let data = crate::operation_data::Data::Snapshots(crate::snapshot_data::SnapshotTask {
        job: crate::snapshot_data::SnapshotJob::Write {
            audience: Audience::Store,
            device: d.device().await,
            trigger: crate::snapshot_data::SnapshotTrigger::Requested,
            attempt: None,
        },
        temporary: Vec::new(),
    });
    d.db.start_operation(data.new_operation("coven").unwrap())
        .await
        .unwrap()
}

/// The path snapshot operation `id`'s prepared attempt reserved.
async fn attempt_path(d: &Device, id: OperationId) -> ObjectPath {
    let record = d.operation(id).await;
    let crate::operation_data::Data::Snapshots(crate::snapshot_data::SnapshotTask {
        job:
            crate::snapshot_data::SnapshotJob::Write {
                audience,
                device,
                attempt: Some(attempt),
                ..
            },
        ..
    }) = crate::operation_data::Data::read(&record).unwrap()
    else {
        panic!("a prepared snapshot attempt")
    };
    let id = coven_format::store_log::SnapshotId {
        audience,
        device,
        number: attempt.number.get(),
    };
    crate::store_log_sync::snapshots::snapshot_path(store(1), &id).unwrap()
}

/// A snapshot's file is signed when it is sealed, and nothing sends a file
/// signed by a pair a kept replacement retired (§10, D9). So Carol's
/// self-replacement reserves its entry only once her sealed snapshot is
/// stored, and while her reserved replacement is unsettled no snapshot is
/// sealed. A snapshot started meanwhile seals under the installed pair and
/// is stored once the exposed store key is rotated.
#[tokio::test]
async fn self_replacement_and_snapshots_never_strand_a_file_signed_by_a_retired_pair() {
    let storage = google();
    let [_a, _b, mut c] = accounts_with(storage.clone(), notes, schema).await;
    write_note(&c, "row").await.unwrap();
    c.catch_up().await;
    c.writes.upload_writes().await.unwrap();
    let sealed = snapshot(&c).await;
    // Preparation follows a completed catch-up of every writer's log (§15).
    let pass = c.caught_up_pass().await;
    assert!(matches!(
        step(&mut c, sealed).await.unwrap(),
        Progress::Advanced
    ));
    drop(pass);
    let path = attempt_path(&c, sealed).await;
    let operation = begin(&mut c, Command::ReplaceOwnKeys).await;
    assert!(matches!(
        step(&mut c, operation).await.unwrap(),
        Progress::Waiting(PendingReason::Waits(Prerequisite::Object(waiting))) if waiting == path
    ));
    while !matches!(step(&mut c, sealed).await.unwrap(), Progress::Finished(_)) {}
    assert!(storage.status(&path).await.unwrap().is_some());
    // Keep the candidate and reserve the entry, then start another snapshot.
    while c.db.local_store_log().await.unwrap().upload.is_none() {
        assert!(matches!(
            step(&mut c, operation).await.unwrap(),
            Progress::Advanced
        ));
    }
    let entry =
        c.db.local_store_log()
            .await
            .unwrap()
            .upload
            .unwrap()
            .entry
            .position;
    let later = snapshot(&c).await;
    assert!(matches!(
        step(&mut c, later).await.unwrap(),
        Progress::Waiting(PendingReason::Waits(Prerequisite::Object(waiting)))
            if waiting == crate::store_log_object::path(store(1), entry)
    ));
    assert!(matches!(settle(&mut c, operation).await, Ok(Output::Unit)));
    // The kept replacement exposed the store key to Carol's old credential;
    // she rotates it, then the snapshot seals under her installed pair.
    c.catch_up().await;
    assert!(c.sync.rotate_exposed_keys().await.unwrap());
    for record in c.db.operations().await.unwrap() {
        if record.id != later {
            finish(&mut c, record.id).await;
        }
    }
    let _pass = c.caught_up_pass().await;
    loop {
        match step(&mut c, later).await {
            Ok(Progress::Finished(_)) => break,
            Ok(Progress::Advanced) => (),
            other => panic!("{:?}", other.map(|_| ())),
        }
    }
    assert!(c.db.operations().await.unwrap().is_empty());
}

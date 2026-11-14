use super::*;
use coven_foundation::progress::RecordingClock;

/// What a restore reports, in order, and every value its loading reports.
async fn restore_statuses(owner: &Owner) -> (Vec<BootstrapStatus>, Vec<Progress<LoadStep>>) {
    let install = Installation::new();
    let (_, cancel) = watch::channel(false);
    let reported = std::sync::Mutex::new(Vec::new());
    let recorder = loading_recorder(owner);
    let handle = install
        .run_recorded(
            owner,
            Installation::restore_request(owner),
            owner.storage.clone(),
            &cancel,
            |status| reported.lock().unwrap().push(status),
            &recorder,
        )
        .await
        .unwrap()
        .unwrap();
    handle.close().await.unwrap();
    (reported.into_inner().unwrap(), recorder.seen())
}

/// Whether `loading` reports `step` from its start to its end.
fn reported_whole(loading: &[Progress<LoadStep>], step: LoadStep) -> bool {
    let of_step = || loading.iter().filter(|progress| progress.step == step);
    of_step().any(|progress| progress.done == 0 && progress.total > 0)
        && of_step().any(|progress| progress.done == progress.total)
}

/// A restore opens its new installation first, running its migration and
/// reporting it, and only then connects (E10).
#[tokio::test]
async fn a_restore_opens_its_new_installation_before_connecting() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let (reported, _) = restore_statuses(&owner).await;
    assert_eq!(
        after_opening(&reported).first(),
        Some(&BootstrapStatus::Connecting),
        "{reported:?}"
    );
    owner.close().await;
}

/// Restoring Ana's store from its floor reports loading's steps as
/// `Loading`, in order, from streaming the store's snapshot to applying the
/// last slot after it. Registering the device follows, then keeping keys and
/// credentials and publishing the store, each a phase of its own with no
/// count (E10).
#[tokio::test]
async fn a_restore_reports_loading_from_the_snapshot_to_the_last_slot() {
    let owner = Owner::new(CloudProvider::S3, true).await;
    let (reported, loading) = restore_statuses(&owner).await;
    let reported = after_opening(&reported);
    assert_eq!(reported.first(), Some(&BootstrapStatus::Connecting));
    let loaded = before_finishing(&reported[1..]);
    for status in loaded {
        assert!(matches!(status, BootstrapStatus::Loading(_)), "{status:?} while loading");
    }
    assert_honest(reported);
    assert_load_order(&loading);
    assert!(reported_whole(&loading, LoadStep::Streaming(Audience::Store)), "{loading:?}");
    let last = loading.last().expect("loading reports");
    assert_eq!(last.step, LoadStep::Applying, "{loading:?}");
    assert_eq!(last.done, last.total, "{loading:?}");
    assert_eq!(loaded.last(), Some(&BootstrapStatus::Loading(last.clone())));
    owner.close().await;
}

/// The store Carol's join publishes stops part way through its first load:
/// reading slot 2 fails after her phone applied slot 1. The call returns the
/// store with that failure, and the store's next pass loads the rest, as any
/// store's pass does (§13.2).
#[tokio::test]
async fn a_joined_store_whose_first_load_stopped_loads_the_rest_in_its_next_pass() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let (_, cancel) = watch::channel(false);
    let control = JoinControl::new();
    let slot = ObjectPath::slot(owner.directory.id(), 2.try_into().unwrap());
    owner
        .storage
        .set_faults(Faults {
            read_failures: [(slot, StorageFailure::PermissionDenied)].into(),
            ..Faults::none()
        })
        .await;
    let joining = install.run(
        &owner,
        Installation::join_request(&invite, &control),
        owner.recipient(),
        &cancel,
        |_| {},
    );
    let (result, member) = owner.join_and_approve(joining, &control, |_| async {}).await;
    let Err(BootstrapError::Published { handle, source }) = result else {
        panic!("the join publishes its store before it loads: {result:?}");
    };
    assert!(matches!(*source, BootstrapError::Sync(SyncError::Storage(_))), "{source:?}");
    owner.storage.set_faults(Faults::none()).await;
    handle.start_sync().await.unwrap();
    let answer = handle.retry_now().await.unwrap().await;
    assert!(matches!(answer, RetryOutcome::Synced { .. }), "{answer:?}");
    assert!(handle
        .members()
        .await
        .unwrap()
        .iter()
        .any(|found| found.is_self && found.id == member));
    handle.close().await.unwrap();
    owner.close().await;
}

/// Ana's store has no floor, so no snapshot holds her copy: restoring reads
/// every slot for it, and reports that as loading's first step, its total
/// found as it begins, before reading and applying the slots (E10).
#[tokio::test]
async fn a_restore_without_a_floor_reports_finding_its_keys_first() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let (reported, loading) = restore_statuses(&owner).await;
    let reported = after_opening(&reported);
    assert_honest(reported);
    assert_eq!(reported.first(), Some(&BootstrapStatus::Connecting));
    assert!(matches!(reported.get(1), Some(BootstrapStatus::Loading(_))), "{reported:?}");
    assert_load_order(&loading);
    assert_eq!(
        loading.first(),
        Some(&Progress { step: LoadStep::FindingKeys, done: 0, total: owner.applied().await }),
        "{loading:?}"
    );
    owner.close().await;
}

/// Carol's phone scans the slots after her invite for her copies in each
/// check for approval. The check after Ana approves finds them and reports
/// the scan as loading's first step; the join publishes the store and
/// reports its loading after (E10, §13.2). Carol's
/// waits end only when the test moves the clock, so no check runs while
/// Ana's approval is half written; a held check runs the next one.
#[tokio::test]
async fn a_join_reports_finding_its_keys_before_it_loads() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let recorder = Arc::new(RecordingClock::new(owner.clock.clone()));
    let builder = install.builder(&owner, owner.recipient()).record_loading(recorder.clone());
    let control = JoinControl::new();
    let (waits, mut waiting) = watch::channel(false);
    let reported = std::sync::Mutex::new(Vec::new());
    let joining = join_with_invite(
        builder,
        invite.shown_code(),
        carol_joins(invite.shown_code()),
        crate::tests::CIRCLE,
        "New phone",
        |status| {
            if matches!(status, BootstrapStatus::WaitingForApproval { .. }) {
                waits.send_replace(true);
            }
            reported.lock().unwrap().push(status);
        },
        &control,
    );
    let approve = async {
        waiting.wait_for(|waiting| *waiting).await.unwrap();
        owner.approve_join(&control).await;
    };
    let (result, ()) = join_with_response(joining, approve).await;
    result.unwrap().unwrap().close().await.unwrap();
    let reported = reported.into_inner().unwrap();
    assert_honest(&reported);
    let after_loading_began: Vec<_> = around_committing(&reported)
        .0
        .iter()
        .skip_while(|status| !matches!(status, BootstrapStatus::Loading(_)))
        .collect();
    assert!(
        after_loading_began.iter().all(|status| matches!(status, BootstrapStatus::Loading(_))),
        "{reported:?}"
    );
    let loading = recorder.seen();
    assert_load_order(&loading);
    assert_eq!(
        loading.first().map(|progress| &progress.step),
        Some(&LoadStep::FindingKeys),
        "{loading:?}"
    );
    assert!(reported_whole(&loading, LoadStep::FindingKeys), "{loading:?}");
    let last = loading.last().unwrap();
    assert_eq!(last.step, LoadStep::Applying, "{loading:?}");
    assert_eq!(last.done, last.total, "{loading:?}");
    owner.close().await;
}

/// A join reports opening its new installation, then connecting, then
/// waiting for approval, then the scan that found its admission, then
/// publishing the store and loading it (E10). While it waits, its status
/// carries the number Ana's phone shows with the request, and when the next
/// check runs. Carol's waits end only when the test moves the clock, so
/// Ana approves while the join waits, and its held check finds the approval.
#[tokio::test]
async fn a_join_reports_each_bootstrap_phase_in_order() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let started = owner.clock.now();
    let invite = owner.invite().await;
    let install = Installation::new();
    let builder = install.builder(&owner, owner.recipient());
    let control = JoinControl::new();
    let reported = std::sync::Mutex::new(Vec::new());
    let (waited, mut waiting) = watch::channel(false);
    let joining = join_with_invite(
        builder,
        invite.shown_code(),
        carol_joins(invite.shown_code()),
        crate::tests::CIRCLE,
        "New phone",
        |status| {
            if matches!(status, BootstrapStatus::WaitingForApproval { .. }) {
                waited.send_replace(true);
            }
            let mut reported = reported.lock().unwrap();
            if reported.last() != Some(&status) {
                reported.push(status);
            }
        },
        &control,
    );
    let mut _often = None;
    let approve = async {
        waiting.wait_for(|waited| *waited).await.unwrap();
        // Ana's next pass reads the request.
        owner.pass().await;
        let request = owner.next_join_request().await;
        crate::tests::approve(&owner.handle, &request).await;
        _often = Some(control.check_often(Duration::ZERO));
        request
    };
    let (result, request) = join_with_response(joining, approve).await;
    result.unwrap().unwrap().close().await.unwrap();
    let reported = reported.into_inner().unwrap();
    let (reported, _) = around_committing(after_opening(&reported));
    assert_eq!(reported.first(), Some(&BootstrapStatus::Connecting));
    assert_honest(reported);
    let loading = reported
        .iter()
        .rev()
        .take_while(|status| matches!(status, BootstrapStatus::Loading(_)))
        .count();
    assert!(loading > 0, "{reported:?}");
    let waits = &reported[1..reported.len() - loading];
    // The wait is one status, however many checks it holds.
    let [BootstrapStatus::WaitingForApproval { confirmation }] = waits else {
        panic!("{reported:?} while waiting");
    };
    assert_eq!(*confirmation, request.confirmation);
    // The clock moved only as the test moved it, by Ana's one pass: no wait
    // of hers or Carol's passed on its own.
    assert_eq!(owner.clock.now(), started + Duration::from_secs(1));
    owner.close().await;
}

/// Ana's approval lands Carol's admission. Carol's next check opens her keys
/// in it, and the join publishes the store, whose loading then fails reading
/// its first slot: the call returns the store with that failure, its status
/// at loading from the moment her keys opened but for committing, and never
/// waiting again. No join is left to resume: the store's passes go on
/// (E10, §13.2).
#[tokio::test]
async fn a_join_whose_keys_opened_never_reports_waiting_again() {
    use coven_storage::test_utils::{Faults, MemoryRequest};
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let builder = install.builder(&owner, owner.recipient());
    let control = JoinControl::new();
    let reported = std::sync::Mutex::new(Vec::new());
    let (statuses, mut seen) = watch::channel(Vec::<BootstrapStatus>::new());
    let joining = join_with_invite(
        builder,
        invite.shown_code(),
        carol_joins(invite.shown_code()),
        crate::tests::CIRCLE,
        "New phone",
        |status| {
            let mut reported = reported.lock().unwrap();
            if reported.last() != Some(&status) {
                reported.push(status);
                statuses.send_replace(reported.clone());
            }
        },
        &control,
    );
    let first = ObjectPath::slot(owner.directory.id(), 1.try_into().unwrap());
    let unreadable = Faults {
        read_failures: [(first.clone(), StorageFailure::Network)].into(),
        ..Faults::none()
    };
    let mut _often = None;
    let approve = async {
        seen.wait_for(|reported| {
            matches!(reported.last(), Some(BootstrapStatus::WaitingForApproval { .. }))
        })
        .await
        .unwrap();
        let request = owner.next_join_request().await;
        let before = owner.applied().await;
        owner.handle.approve_join_request(&request).await.unwrap();
        owner.pass().await;
        assert!(owner.applied().await > before, "the admission landed");
        owner.storage.set_faults(unreadable.clone()).await;
        let asked = owner.storage.request_trace().len();
        _often = Some(control.check_often(Duration::ZERO));
        // The check opens Carol's keys in her admission, then loads from
        // the first slot.
        requested(&owner, asked, &MemoryRequest::Read(first.clone(), None)).await;
    };
    let (result, ()) = join_with_response(joining, approve).await;
    let Err(BootstrapError::Published { handle, source }) = result else {
        panic!("the join publishes its store before it loads: {result:?}");
    };
    assert!(
        matches!(&*source, BootstrapError::Sync(SyncError::Storage(error))
            if error.failure() == StorageFailure::Network),
        "{source:?}"
    );
    handle.close().await.unwrap();
    owner.storage.set_faults(Faults::none()).await;
    let reported = reported.into_inner().unwrap();
    let loading = reported
        .iter()
        .position(|status| matches!(status, BootstrapStatus::Loading(_)))
        .expect("the join loaded");
    assert!(
        reported[loading..].iter().all(|status| matches!(
            status,
            BootstrapStatus::Loading(_) | BootstrapStatus::Committing
        )),
        "{reported:?}"
    );
    let control = JoinControl::new();
    let resumed = install.builder(&owner, owner.recipient()).resume_join(
        owner.directory.id(),
        |_| {},
        &control,
    );
    let result = tokio::time::timeout(HANG_GUARD, resumed).await.unwrap();
    assert!(matches!(result, Err(BootstrapError::NoUnfinishedJoin(_))), "{result:?}");
    owner.close().await;
}

/// Ana writes a row while Carol's phone waits for approval. Carol's next
/// check reads Ana's slot as it looks for her copies and finds none: that
/// scan is part of her wait, so her status stays waiting for approval and
/// never shows loading before her keys open (E10).
#[tokio::test]
async fn a_waiting_joins_scan_reports_its_wait() {
    use coven_storage::test_utils::MemoryRequest;
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let builder = install.builder(&owner, owner.recipient());
    let control = JoinControl::new();
    let reported = std::sync::Mutex::new(Vec::new());
    let (statuses, mut seen) = watch::channel(Vec::<BootstrapStatus>::new());
    let joining = join_with_invite(
        builder,
        invite.shown_code(),
        carol_joins(invite.shown_code()),
        crate::tests::CIRCLE,
        "New phone",
        |status| {
            let mut reported = reported.lock().unwrap();
            if reported.last() != Some(&status) {
                reported.push(status);
                statuses.send_replace(reported.clone());
            }
        },
        &control,
    );
    let request_path = crate::tests::request_path(&invite);
    let mut _often = None;
    let write_and_check = async {
        seen.wait_for(|reported| {
            matches!(reported.last(), Some(BootstrapStatus::WaitingForApproval { .. }))
        })
        .await
        .unwrap();
        let before = owner.applied().await;
        owner
            .handle
            .write(|sql| {
                sql.execute(
                    "INSERT INTO notes VALUES('later',?1)",
                    coven_database::params![vec![1_u8; 9]],
                )?;
                Ok::<_, CovenError>(())
            })
            .await
            .unwrap();
        owner.pass().await;
        assert!(owner.applied().await > before);
        let asked = owner.storage.request_trace().len();
        _often = Some(control.check_often(Duration::ZERO));
        // The check reads Ana's slot, finds no copy, and asks whether the
        // request is still there.
        requested(&owner, asked, &MemoryRequest::Read(request_path.clone(), None)).await;
        control.cancel();
    };
    let (result, ()) = join_with_response(joining, write_and_check).await;
    assert!(matches!(result, Err(BootstrapError::JoinRequestSent)), "{result:?}");
    let reported = reported.into_inner().unwrap();
    assert!(
        !reported.iter().any(|status| matches!(status, BootstrapStatus::Loading(_))),
        "{reported:?}"
    );
    owner.close().await;
}

/// Wait until the owner's provider has taken `request`, at or after the
/// `from`th request it took.
async fn requested(owner: &Owner, from: usize, request: &MemoryRequest) {
    let mut requests = owner.storage.subscribe_requests();
    tokio::time::timeout(HANG_GUARD, async {
        while !owner.storage.request_trace()[from..].contains(request) {
            requests.changed().await.expect("the provider is kept");
        }
    })
    .await
    .expect("the provider takes the request");
}

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

/// Carol's join stops part way through loading: reading slot 2 fails after
/// her phone applied slot 1. Her next call opens her keys again, and that
/// check reports reading and applying the slots left, to the last (E10).
#[tokio::test]
async fn a_resumed_join_reports_the_rest_of_its_loading() {
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
    let (result, _) = owner.join_and_approve(joining, &control, |_| async {}).await;
    assert!(matches!(result, Err(BootstrapError::Sync(SyncError::Storage(_)))), "{result:?}");
    owner.storage.set_faults(Faults::none()).await;
    let reported = std::sync::Mutex::new(Vec::new());
    let recorder = loading_recorder(&owner);
    let handle = install
        .run_recorded(
            &owner,
            Installation::join_request(&invite, &control),
            owner.recipient(),
            &cancel,
            |status| reported.lock().unwrap().push(status),
            &recorder,
        )
        .await
        .unwrap()
        .expect("the approved join loads");
    handle.close().await.unwrap();
    let reported = reported.into_inner().unwrap();
    assert_honest(&reported);
    let loading = recorder.seen();
    assert_load_order(&loading);
    // Slot 1 was applied before the join stopped; the rest are read.
    assert!(reported_whole(&loading, LoadStep::Reading), "{loading:?}");
    let last = loading.last().unwrap();
    assert_eq!(last.step, LoadStep::Applying, "{loading:?}");
    assert_eq!(last.done, last.total, "{loading:?}");
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
/// check for approval. The check after Ana approves finds them, reports the
/// scan as loading's first step and then loads the store (E10). Carol's
/// waits end only when the test moves the clock, so no check runs while
/// Ana's approval is half written; Check now runs the next one.
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
        "New phone",
        |status| {
            if matches!(
                status,
                BootstrapStatus::WaitingForApproval { next_check: NextCheck::At(_), .. }
            ) {
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
    let after_loading_began: Vec<_> = before_finishing(&reported)
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
/// waiting for approval, then loading (E10). While it waits, its status
/// carries the number Ana's phone shows with the request, and when the next
/// check runs. Carol's waits end only when the test moves the clock, so
/// Ana approves while the join waits, and Check now runs the next check.
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
        "New phone",
        |status| {
            if matches!(
                status,
                BootstrapStatus::WaitingForApproval { next_check: NextCheck::At(_), .. }
            ) {
                waited.send_replace(true);
            }
            let mut reported = reported.lock().unwrap();
            if reported.last() != Some(&status) {
                reported.push(status);
            }
        },
        &control,
    );
    let approve = async {
        waiting.wait_for(|waited| *waited).await.unwrap();
        // Ana's next pass reads the request.
        owner.pass().await;
        let request = owner.next_join_request().await;
        crate::tests::approve(&owner.handle, &request).await;
        control.check_now();
        request
    };
    let (result, request) = join_with_response(joining, approve).await;
    result.unwrap().unwrap().close().await.unwrap();
    let reported = reported.into_inner().unwrap();
    let reported = before_finishing(after_opening(&reported));
    assert_eq!(reported.first(), Some(&BootstrapStatus::Connecting));
    assert_honest(reported);
    let loading = reported
        .iter()
        .rev()
        .take_while(|status| matches!(status, BootstrapStatus::Loading(_)))
        .count();
    assert!(loading > 0, "{reported:?}");
    let waits = &reported[1..reported.len() - loading];
    assert!(waits.len() >= 2, "{reported:?}");
    for (index, status) in waits.iter().enumerate() {
        let BootstrapStatus::WaitingForApproval { confirmation, next_check } = status else {
            panic!("{status:?} while waiting");
        };
        assert_eq!(*confirmation, request.confirmation);
        // The first check runs at once; then checks and waits alternate.
        assert_eq!(matches!(next_check, NextCheck::Checking), index % 2 == 0, "{reported:?}");
    }
    // The clock moved only as the test moved it, by Ana's one pass: no wait
    // of hers or Carol's passed on its own.
    assert_eq!(owner.clock.now(), started + Duration::from_secs(1));
    owner.close().await;
}

/// Ana's approval claims Carol's copies, then the entry adding her, and the
/// entry fails to land. Carol's next check opens her keys and loads the
/// store, and finds no entry; from then on her join reports loading as it
/// waits for the entry, never waiting for approval again (E10). Nor does it
/// once she closes the app and resumes the join: its keys opened in the call
/// before, so the resumed call reports no wait even before its first check.
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
    // Each check that loads or finds no entry ends by asking whether the
    // request is still there.
    let request_path = crate::tests::request_path(&invite);
    async fn checked(owner: &Owner, request_path: &ObjectPath, control: &JoinControl) {
        let asked = owner.storage.request_trace().len();
        control.check_now();
        requested(owner, asked, &MemoryRequest::Read(request_path.clone(), None)).await;
    }
    let approve = async {
        seen.wait_for(|reported| {
            matches!(
                reported.last(),
                Some(BootstrapStatus::WaitingForApproval { next_check: NextCheck::At(_), .. })
            )
        })
        .await
        .unwrap();
        let request = owner.next_join_request().await;
        // The copy lands in the next slot; the entry after it never does.
        let entry =
            ObjectPath::slot(owner.directory.id(), (owner.applied().await + 2).try_into().unwrap());
        owner
            .storage
            .set_faults(Faults {
                create_failures: [(entry.clone(), (StorageFailure::Network, 500))].into(),
                ..Faults::none()
            })
            .await;
        owner.handle.approve_join_request(&request).await.unwrap();
        // The pass the approval asks for claims the copy, then fails at the
        // entry.
        requested(&owner, 0, &MemoryRequest::Create(entry.clone())).await;
        // The check that loads, then one more after its wait.
        checked(&owner, &request_path, &control).await;
        checked(&owner, &request_path, &control).await;
        control.cancel();
    };
    let (result, ()) = join_with_response(joining, approve).await;
    assert!(result.is_err());
    let reported = reported.into_inner().unwrap();
    let loading = reported
        .iter()
        .position(|status| matches!(status, BootstrapStatus::Loading(_)))
        .expect("the join loaded");
    assert!(
        reported[loading..].iter().all(|status| matches!(status, BootstrapStatus::Loading(_))),
        "{reported:?}"
    );
    // The entry still hasn't landed when Carol's app resumes the join.
    let control = JoinControl::new();
    let reported = std::sync::Mutex::new(Vec::new());
    let resumed = install.builder(&owner, owner.recipient()).resume_join(
        owner.directory.id(),
        |status| {
            let mut reported = reported.lock().unwrap();
            if reported.last() != Some(&status) {
                reported.push(status);
            }
        },
        &control,
    );
    let check_twice = async {
        checked(&owner, &request_path, &control).await;
        checked(&owner, &request_path, &control).await;
        control.cancel();
    };
    let (result, ()) = join_with_response(resumed, check_twice).await;
    assert!(result.is_err());
    let reported = reported.into_inner().unwrap();
    assert!(
        reported.iter().any(|status| matches!(status, BootstrapStatus::Loading(_))),
        "{reported:?}"
    );
    assert!(
        !reported.iter().any(|status| matches!(
            status,
            BootstrapStatus::WaitingForApproval { .. } | BootstrapStatus::Pending(_)
        )),
        "{reported:?}"
    );
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
    let write_and_check = async {
        seen.wait_for(|reported| {
            matches!(
                reported.last(),
                Some(BootstrapStatus::WaitingForApproval { next_check: NextCheck::At(_), .. })
            )
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
        control.check_now();
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

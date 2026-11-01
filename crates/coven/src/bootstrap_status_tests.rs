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
/// last slot after it; keeping keys and credentials follows (E10).
#[tokio::test]
async fn a_restore_reports_loading_from_the_snapshot_to_the_last_slot() {
    let owner = Owner::new(CloudProvider::S3, true).await;
    let (reported, loading) = restore_statuses(&owner).await;
    let reported = after_opening(&reported);
    assert_eq!(reported.first(), Some(&BootstrapStatus::Connecting));
    for status in &reported[1..] {
        assert!(matches!(status, BootstrapStatus::Loading(_)), "{status:?} while loading");
    }
    assert_honest(reported);
    assert_load_order(&loading);
    assert!(reported_whole(&loading, LoadStep::Streaming(Audience::Store)), "{loading:?}");
    let last = loading.last().expect("loading reports");
    assert_eq!(last.step, LoadStep::Applying, "{loading:?}");
    assert_eq!(last.done, last.total, "{loading:?}");
    assert_eq!(reported.last(), Some(&BootstrapStatus::Loading(last.clone())));
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
        Installation::join_request(&invite),
        owner.recipient(),
        &cancel,
        |_| {},
    );
    let (result, _) = join_and_approve(&owner.handle, joining, |_| async {}).await;
    assert!(matches!(result, Err(BootstrapError::Sync(SyncError::Storage(_)))), "{result:?}");
    owner.storage.set_faults(Faults::none()).await;
    let reported = std::sync::Mutex::new(Vec::new());
    let recorder = loading_recorder(&owner);
    let handle = install
        .run_recorded(
            &owner,
            Installation::join_request(&invite),
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
        &invite.code,
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
        let request = next_join_request(&owner.handle).await;
        crate::tests::approve(&owner.handle, &request).await;
        control.check_now();
    };
    let (result, ()) = join_with_response(joining, approve).await;
    result.unwrap().unwrap().close().await.unwrap();
    let reported = reported.into_inner().unwrap();
    assert_honest(&reported);
    let after_loading_began: Vec<_> = reported
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

//! How a join that ends without a store, declined, cancelled or expired,
//! stops part way and finishes (§13.4), and how one that recorded its send
//! before the create sends it again (§13.2).

use super::*;

/// Ana declines Carol's request and her phone's next call ends the join. The
/// join records that it ends before deleting anything, so a call that stops
/// after the kept sign-in went finishes on the next try without signing in.
#[tokio::test]
async fn a_join_stopped_after_its_sign_in_was_deleted_finishes_without_signing_in() {
    let owner = Owner::new(CloudProvider::Dropbox, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let store = owner.directory.id();
    waiting_join(&owner, &install, &invite).await;
    let request = owner.next_join_request().await;
    owner.handle.decline_join_request(&request).await.unwrap();
    owner.pass_now().await;
    // Carol's phone recorded the ending, deleted the sign-in and stopped.
    let code = coven_sync::read_invite_code(invite.shown_code()).unwrap();
    coven_sync::record_ending(&install.app.store_dir(&store), &code).unwrap();
    StoreKeychain::new(install.keychain.clone(), store).delete_storage_credentials().unwrap();
    // No sign-in is left, and the app signs in no more.
    let control = JoinControl::new();
    let resumed =
        install.builder(&owner, owner.recipient()).resume_join(store, |_| {}, &control).await;
    assert!(matches!(resumed, Ok(None)), "{resumed:?}");
    install.absent(store).await;
    assert!(!install.root.path().join("stores").join(store.to_string()).exists());
    owner.close().await;
}

/// Whichever keychain operation of the ending fails, the next call finishes
/// it without the sign-in the failed one may have deleted. Ana's pass after
/// the decline puts the marker in the request's place and then takes back the
/// invite's share; the test holds that at the provider until the join is done,
/// so the resumes' reads of storage see the marker, not the provider's refusal.
#[tokio::test]
async fn a_declined_join_finishes_after_a_failure_at_any_step_of_its_ending() {
    // The whole test is bounded: an await that never finishes fails with the
    // step it stopped at.
    let progress = std::sync::Mutex::new(String::from("before the first round"));
    let rounds = async {
        for failing in 0..40 {
            *progress.lock().unwrap() = format!("round {failing}: setting up");
            let owner = Owner::new(CloudProvider::Dropbox, false).await;
            let invite = owner.invite().await;
            let install = Installation::new();
            let store = owner.directory.id();
            waiting_join(&owner, &install, &invite).await;
            let request = owner.next_join_request().await;
            let (revoking, revoked) = tokio::sync::oneshot::channel();
            let (release, released) = tokio::sync::oneshot::channel();
            owner.storage.hold_next_access("join@example.com", false, revoking, released).await;
            *progress.lock().unwrap() = format!("round {failing}: declining");
            owner.handle.decline_join_request(&request).await.unwrap();
            // Ana's pass, or the background one the decline woke, puts the
            // marker in place and then waits at the provider to take the
            // access back: its end or that wait shows the marker is there.
            let answer = owner.handle.retry_now().await.unwrap();
            tokio::select! {
                outcome = answer => assert!(matches!(outcome, RetryOutcome::Synced { .. })),
                revoking = revoked => revoking.unwrap(),
            }
            let control = JoinControl::new();
            // The stopped join saved the wait before its next check: a
            // resume that reaches it checks now and finds the marker.
            let check_when_waiting = check_when_waiting(&control);
            install.keychain.fail_operations_after(&[failing]);
            *progress.lock().unwrap() = format!("round {failing}: first resume");
            let ended = install
                .builder(&owner, owner.recipient())
                .resume_join(store, check_when_waiting, &control)
                .await;
            install.keychain.fail_operations_after(&[]);
            let failed = ended.is_err();
            if failed {
                *progress.lock().unwrap() = format!("round {failing}: second resume");
                let again = install
                    .builder(&owner, owner.recipient())
                    .resume_join(store, check_when_waiting, &control)
                    .await;
                // Once the code is gone nothing lists the join: the leftover
                // installation is for the next invite to clear.
                assert!(
                    matches!(again, Ok(None) | Err(BootstrapError::NoUnfinishedJoin(_))),
                    "failing after {failing}: {again:?}"
                );
            }
            install.absent(store).await;
            release.send(()).unwrap();
            *progress.lock().unwrap() = format!("round {failing}: closing");
            owner.close().await;
            if !failed {
                return;
            }
        }
        panic!("the ending never completed without a failure");
    };
    if tokio::time::timeout(HANG_GUARD, rounds).await.is_err() {
        panic!("hung past {HANG_GUARD:?} at {}", progress.lock().unwrap());
    }
}

/// Carol's first join ended and stopped after its code was deleted, before
/// the installation went. A later invite of the store sends its own request:
/// the leftover saved request opens only with the first invite's code.
#[tokio::test]
async fn an_ended_join_left_without_its_code_does_not_fail_the_next_invite() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let first = owner.invite().await;
    let install = Installation::new();
    let store = owner.directory.id();
    waiting_join(&owner, &install, &first).await;
    let code = coven_sync::read_invite_code(first.shown_code()).unwrap();
    coven_sync::record_ending(&install.app.store_dir(&store), &code).unwrap();
    StoreKeychain::new(install.keychain.clone(), store).delete_invite_code().unwrap();
    assert!(install.root.path().join("stores").join(store.to_string()).exists());
    // Ana declined the first invite's request, so the next request is the second's.
    let request = owner.next_join_request().await;
    owner.handle.decline_join_request(&request).await.unwrap();
    joins_and_is_approved(&owner, &install, &owner.invite().await).await;
    owner.close().await;
}

/// A join that stopped after it saved its request and before it saved its
/// code leaves the same.
#[tokio::test]
async fn an_unsent_join_left_without_its_code_does_not_fail_the_next_invite() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let first = owner.invite().await;
    let install = Installation::new();
    let store = owner.directory.id();
    let builder = install.builder(&owner, owner.recipient());
    let pending = builder.directories.begin_bootstrap(store, builder.ids.as_ref()).await.unwrap();
    let code = coven_sync::read_invite_code(first.shown_code()).unwrap();
    let directory = pending.directory();
    let device = directory.settings().unwrap().device_id;
    coven_sync::JoiningIdentity::prepare(
        &directory,
        &code,
        device,
        "Carol's phone",
        carol_joins(first.shown_code()),
    )
    .unwrap();
    drop(pending);
    assert!(saved_code(&install, store).is_none());
    joins_and_is_approved(&owner, &install, &owner.invite().await).await;
    owner.close().await;
}

async fn joins_and_is_approved(owner: &Owner, install: &Installation, invite: &Invite) {
    let control = JoinControl::new();
    let joining = join_with_invite(
        install.builder(owner, owner.recipient()),
        invite.shown_code(),
        carol_joins(invite.shown_code()),
        "Carol's phone",
        |_| {},
        &control,
    );
    let (result, _) = owner.join_and_approve(joining, &control, |_| async {}).await;
    result.unwrap().unwrap().close().await.unwrap();
}

/// Carol's phone recorded the send and stopped before the create reached
/// storage. Her next call finds nothing at the request's path and the invite
/// open, so it sends the request, and Ana approves it.
#[tokio::test]
async fn a_join_that_stopped_before_its_create_sends_the_request_on_resume() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let store = owner.directory.id();
    waiting_join(&owner, &install, &invite).await;
    // As if the create had never reached storage.
    let path = crate::tests::request_path(&invite);
    owner.storage.delete_if_present(&path).await.unwrap();
    let control = JoinControl::new();
    let resumed = install.builder(&owner, owner.recipient()).resume_join(store, |_| {}, &control);
    let (result, _) = owner.join_and_approve(resumed, &control, |_| async {}).await;
    // The resume gave no name: the saved request returns the one Carol typed.
    let handle = result.unwrap().unwrap();
    assert_eq!(handle.joined_invitee().await.unwrap().as_deref(), Some("Carol"));
    handle.close().await.unwrap();
    owner.close().await;
}

/// Past the invite's end, nothing is sent again: no first request is sent
/// once the invite has expired by the device's clock.
#[tokio::test]
async fn a_join_that_stopped_before_its_create_sends_nothing_once_the_invite_ended() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let store = owner.directory.id();
    waiting_join(&owner, &install, &invite).await;
    let path = crate::tests::request_path(&invite);
    owner.storage.delete_if_present(&path).await.unwrap();
    owner.clock.set(invite.expires_at);
    let control = JoinControl::new();
    let resumed =
        install.builder(&owner, owner.recipient()).resume_join(store, |_| {}, &control).await;
    assert!(matches!(resumed, Ok(None)), "{resumed:?}");
    assert!(crate::tests::no_request(&owner.storage, &path).await, "no request was sent");
    owner.close().await;
}

/// A join published its store and stopped before it deleted its joining
/// keys and saved code. The next open of the store deletes them.
#[tokio::test]
async fn opening_a_published_store_deletes_the_joining_keys_and_code_a_stopped_join_left() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let store = owner.directory.id();
    joins_and_is_approved(&owner, &install, &invite).await;
    let directory = install.app.store_dir(&store);
    directory.owned_file(StoreFile::Bootstrap).replace(b"joining keys").unwrap();
    let kept = StoreKeychain::new(install.keychain.clone(), store);
    kept.set_invite_code(&SecretText::new(invite.shown_code().to_string())).unwrap();
    let opened = install.builder(&owner, owner.recipient()).open(store, |_| ()).await.unwrap();
    opened.close().await.unwrap();
    assert!(directory.owned_file(StoreFile::Bootstrap).read_optional().unwrap().is_none());
    assert!(kept.invite_code().unwrap().is_none());
    owner.close().await;
}

/// Ending without a store also removes a Dropbox join's kept sign-in, with its
/// code (§13.2). Carol's join ends as declined only while it can still read
/// its request's path: once Ana takes back the access the invite granted, the
/// join's check fails with the provider's refusal instead. Ana puts the
/// marker in the request's place before she takes the access back, so the
/// test holds that at the provider while Carol's join checks.
#[tokio::test]
async fn declined_cancelled_and_expired_requests_finish_without_committing_custody() {
    // A cancel deletes the request as a decline does; the joiner sees only that.
    for (provider, ending) in [CloudProvider::S3, CloudProvider::Dropbox]
        .into_iter()
        .flat_map(|provider| ["decline", "cancel", "expiry"].map(|ending| (provider, ending)))
    {
        let owner = Owner::new(provider, false).await;
        let invite = owner.invite().await;
        let install = Installation::new();
        let (_, cancel) = watch::channel(false);
        let control = JoinControl::new();
        let (waiting, mut waits) = waiting_for_approval();
        let joining = install.run(
            &owner,
            Installation::join_request(&invite, &control),
            owner.recipient(),
            &cancel,
            waiting,
        );
        let takes_back = provider.signs_in_with_oauth();
        let (revoking, revoked) = tokio::sync::oneshot::channel();
        let (release, released) = tokio::sync::oneshot::channel();
        if takes_back {
            owner.storage.hold_next_access("join@example.com", false, revoking, released).await;
        }
        let settle = async {
            waits.wait_for(|waiting| *waiting).await.unwrap();
            let request = owner.next_join_request().await;
            match ending {
                "decline" => owner.handle.decline_join_request(&request).await.unwrap(),
                "cancel" => owner.handle.cancel_invite(&invite.id).await.unwrap(),
                _ => owner.clock.set(invite.expires_at),
            }
            // The ending is a write that takes the invite off Ana's list: a
            // decline's or a cancel's own, or the expiry's, which her handle
            // makes once its clock reaches the invite's end.
            owner
                .handle
                .test_invites()
                .wait_for(|invites| invites.iter().all(|listed| listed.id != invite.id))
                .await;
            // Ana's pass after the ending puts the marker in the request's
            // place, then may take back the access, which waits at the
            // provider: its end or that wait shows the marker is there. On
            // S3 there is no access to take back.
            let answer = owner.handle.retry_now().await.unwrap();
            let passed = |outcome| assert!(matches!(outcome, RetryOutcome::Synced { .. }));
            if takes_back {
                tokio::select! {
                    outcome = answer => passed(outcome),
                    revoking = revoked => revoking.unwrap(),
                }
            } else {
                passed(answer.await);
            }
            assert!(
                crate::tests::no_request(&owner.storage, &crate::tests::request_path(&invite))
                    .await,
                "{provider:?} {ending}"
            );
            control.check_now();
        };
        let (result, ()) = join_with_response(joining, settle).await;
        assert!(matches!(result, Ok(None)), "{provider:?} {ending}: {result:?}");
        if takes_back {
            release.send(()).unwrap();
        }
        install.absent(owner.directory.id()).await;
        assert!(StoreKeychain::new(install.keychain.clone(), owner.directory.id())
            .invite_code()
            .unwrap()
            .is_none());
        assert!(!install
            .root
            .path()
            .join("stores")
            .join(owner.directory.id().to_string())
            .exists());
        owner.close().await;
    }
}

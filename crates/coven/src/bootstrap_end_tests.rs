//! How a join that ends without a store, declined, cancelled or expired,
//! stops part way and finishes (§13.4), how one admitted and then removed
//! ends, and how one that recorded its send before the create sends it again
//! (§13.2).

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

/// Carol's join is approved, then stops while it loads, and Ana removes her
/// before the next call. That call applies the removal, which stops Carol's
/// device for good: the join ends with `Removed` and deletes what it kept,
/// instead of waiting on a store it can never open (§13.2).
#[tokio::test]
async fn an_approved_join_whose_member_is_removed_before_it_loads_ends() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let store = owner.directory.id();
    let (_, cancel) = watch::channel(false);
    let (loading, mut loaded) = watch::channel(false);
    let control = JoinControl::new();
    let joining = install.run(
        &owner,
        Installation::join_request(&invite, &control),
        owner.recipient(),
        &cancel,
        |status| {
            if matches!(status, BootstrapStatus::Loading(progress)
                if progress.step == LoadStep::Applying && progress.done == progress.total)
            {
                loading.send_replace(true);
            }
        },
    );
    let stopped = async {
        tokio::select! {
            result = joining => panic!("the join finished before it stopped: {result:?}"),
            changed = loaded.wait_for(|loading| *loading) => drop(changed.unwrap()),
        }
    };
    let ((), member) = join_with_response(stopped, owner.approve_join(&control)).await;
    crate::tests::landed(&owner.handle, owner.handle.remove_member(&member).await.unwrap()).await;
    let result =
        install.builder(&owner, owner.recipient()).resume_join(store, |_| {}, &control).await;
    assert!(matches!(result, Err(BootstrapError::Sync(SyncError::Removed))), "{result:?}");
    assert!(saved_code(&install, store).is_none());
    install.absent(store).await;
    assert!(install
        .builder(&owner, owner.recipient())
        .unfinished_joins()
        .await
        .unwrap()
        .is_empty());
    owner.close().await;
}

/// Whichever keychain operation of the ending fails, the next call finishes
/// it without the sign-in the failed one may have deleted.
#[tokio::test]
async fn a_declined_join_finishes_after_a_failure_at_any_step_of_its_ending() {
    for failing in 0..40 {
        let owner = Owner::new(CloudProvider::Dropbox, false).await;
        let invite = owner.invite().await;
        let install = Installation::new();
        let store = owner.directory.id();
        waiting_join(&owner, &install, &invite).await;
        let request = owner.next_join_request().await;
        owner.handle.decline_join_request(&request).await.unwrap();
        owner.pass_now().await;
        let control = JoinControl::new();
        install.keychain.fail_operations_after(&[failing]);
        let ended =
            install.builder(&owner, owner.recipient()).resume_join(store, |_| {}, &control).await;
        install.keychain.fail_operations_after(&[]);
        let failed = ended.is_err();
        if failed {
            let again = install
                .builder(&owner, owner.recipient())
                .resume_join(store, |_| {}, &control)
                .await;
            // Once the code is gone nothing lists the join: the leftover
            // installation is for the next invite to clear.
            assert!(
                matches!(again, Ok(None) | Err(BootstrapError::NoUnfinishedJoin(_))),
                "failing after {failing}: {again:?}"
            );
        }
        install.absent(store).await;
        owner.close().await;
        if !failed {
            return;
        }
    }
    panic!("the ending never completed without a failure");
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
    coven_sync::JoiningIdentity::prepare(&directory, &code, device, "Carol's phone").unwrap();
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
    result.unwrap().unwrap().close().await.unwrap();
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

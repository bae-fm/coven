//! How a join that ends without a store stops part way and finishes (§13.4),
//! and how one that recorded its send before the create sends it again
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
    coven_sync::JoiningIdentity::prepare(&pending.directory(), &code, "Carol's phone").unwrap();
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

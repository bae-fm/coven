//! How a join that ends without a store stops part way and finishes (§13.4).

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

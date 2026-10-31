use super::*;
use crate::authentication::SignIn;

#[tokio::test]
async fn restore_and_keychain_restore_use_builder_sign_in_without_returning_tokens() {
    for from_keychain in [false, true] {
        let owner = Owner::new(CloudProvider::Dropbox, false).await;
        let install = Installation::new();
        let sign_in = SignIn::new(owner.clock.clone()).await;
        let mut builder = sign_in.configure(install.builder(&owner, owner.storage.clone()));
        builder
            .authenticate(coven_storage::OAuthAccess::Dropbox(
                coven_storage::DropboxAccess::FullDropbox,
            ))
            .await
            .unwrap();
        install.absent(owner.directory.id()).await;
        let (_, cancel) = watch::channel(false);
        let handle = if from_keychain {
            PersonEntries::new(install.keychain.clone(), owner.directory.id())
                .set_restore_code(&crate::tests::keychain_entry(&owner.code))
                .unwrap();
            restore_from_saved_code(
                builder,
                &crate::tests::readable(
                    crate::saved_restore::list(install.keychain.clone()).unwrap(),
                )[0],
                None,
                "Laptop",
                |_| {},
                &cancel,
            )
            .await
            .unwrap()
            .unwrap()
        } else {
            restore_from_code(
                builder,
                &owner.code.to_text().unwrap(),
                None,
                "Laptop",
                |_| {},
                &cancel,
            )
            .await
            .unwrap()
        };
        let scoped = StoreKeychain::new(install.keychain.clone(), owner.directory.id());
        let StorageCredentials::OAuth(tokens) =
            StorageCredentials::decode(scoped.storage_credentials().unwrap().unwrap().as_bytes())
                .unwrap()
        else {
            panic!("OAuth custody")
        };
        assert_eq!(tokens.access_token.as_str(), "access-0");
        assert_eq!(handle.restore_code().await.unwrap(), owner.code.to_text().unwrap().as_str());
        assert_eq!(sign_in.presentation_count(), 1);
        handle.close().await.unwrap();
        owner.close().await;
    }
}

#[tokio::test]
async fn bootstrap_requires_explicit_sign_in_and_never_presents_ui() {
    let owner = Owner::new(CloudProvider::OneDrive, false).await;
    let install = Installation::new();
    let sign_in = SignIn::new(owner.clock.clone()).await;
    let builder = sign_in.configure(install.builder(&owner, owner.storage.clone()));
    let (_, cancel) = watch::channel(false);
    assert!(matches!(
        restore_from_code(builder, &owner.code.to_text().unwrap(), None, "Laptop", |_| {}, &cancel)
            .await,
        Err(BootstrapError::OAuth(OAuthError::Reauthorize(CloudProvider::OneDrive)))
    ));
    assert_eq!(sign_in.presentation_count(), 0);
    install.absent(owner.directory.id()).await;
    owner.close().await;
}

#[tokio::test]
async fn join_requires_the_shared_folder_access_before_contacting_storage() {
    let owner = Owner::new(CloudProvider::Dropbox, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let sign_in = SignIn::new(owner.clock.clone()).await;
    let recipient = Arc::new(owner.recipient().for_device());
    let mut builder = sign_in.configure(install.builder(&owner, recipient.clone()));
    builder.authenticate(OAuthAccess::Dropbox(DropboxAccess::AppFolder)).await.unwrap();
    let before = recipient.request_count();
    let control = JoinControl::new();
    let required = OAuthAccess::Dropbox(DropboxAccess::FullDropbox);
    assert!(matches!(
        join_with_invite(builder, &invite.code, "New phone", |_| {}, &control).await,
        Err(BootstrapError::Sync(SyncError::Storage(error)))
            if error.failure() == StorageFailure::AccessNotGranted { required }
    ));
    assert_eq!(recipient.request_count(), before);
    assert_eq!(sign_in.presentation_count(), 1);
    install.absent(owner.directory.id()).await;
    owner.close().await;
}

#[tokio::test]
async fn a_join_waiting_past_its_token_refreshes_on_rejection_and_publishes_the_replacement() {
    let owner = Owner::new(CloudProvider::Dropbox, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let sign_in = SignIn::new(owner.clock.clone()).await;
    let (sleeping, mut asleep) = watch::channel(false);
    let (resume, resumed) = watch::channel(false);
    let clock = Arc::new(JoinClock {
        clock: crate::tests::PollingClock::new(owner.clock.clone()),
        sleeping,
        resume: resumed,
    });
    let mut builder = sign_in.configure(install.builder(&owner, owner.recipient()).clock(clock));
    builder
        .authenticate(coven_storage::OAuthAccess::Dropbox(
            coven_storage::DropboxAccess::FullDropbox,
        ))
        .await
        .unwrap();
    let control = JoinControl::new();
    let join = join_with_invite(builder, &invite.code, "New phone", |_| {}, &control);
    let (joined, _) = join_and_approve(&owner.handle, join, |_| async {
        // The waiting join keeps only its sign-in (§13.2).
        let kept = install.only_kept_sign_in(owner.directory.id()).await;
        assert_eq!(kept.unwrap().access_token.as_str(), "access-0");
        asleep.wait_for(|sleeping| *sleeping).await.unwrap();
        owner.storage.reject_access_token("access-0").await;
        owner.handle.unlock_store_key().await.unwrap();
        resume.send(true).unwrap();
    })
    .await;
    let handle = joined.unwrap().unwrap();
    let scoped = StoreKeychain::new(install.keychain.clone(), owner.directory.id());
    let StorageCredentials::OAuth(tokens) =
        StorageCredentials::decode(scoped.storage_credentials().unwrap().unwrap().as_bytes())
            .unwrap()
    else {
        panic!("OAuth custody")
    };
    assert_eq!(tokens.access_token.as_str(), "access-1");
    assert_eq!(sign_in.request_count(), 2);
    assert_eq!(sign_in.presentation_count(), 1);
    handle.close().await.unwrap();
    owner.close().await;
}

#[tokio::test]
async fn restore_replaces_a_codes_label_with_the_members_own_sign_in() {
    for provider in [CloudProvider::Dropbox, CloudProvider::OneDrive] {
        for from_keychain in [false, true] {
            let owner = Owner::new(provider, false).await;
            let install = Installation::new();
            let mut code = coven_sync::read_restore_code(&owner.code.to_text().unwrap()).unwrap();
            let mut location =
                coven_storage::RestoreStorage::decode(code.storage.as_bytes()).unwrap().location;
            match &mut location {
                StorageConfig::Dropbox { account_label, .. }
                | StorageConfig::OneDrive { account_label, .. } => {
                    *account_label = Some("old@example.com".into());
                }
                _ => unreachable!(),
            }
            code.storage =
                coven_storage::RestoreStorage::location_only(location.clone()).encode().unwrap();
            let builder = install.authenticated_builder(&owner, owner.storage.clone()).await;
            let (_, cancel) = watch::channel(false);
            let handle = if from_keychain {
                PersonEntries::new(install.keychain.clone(), owner.directory.id())
                    .set_restore_code(&crate::tests::keychain_entry(&code))
                    .unwrap();
                let saved = crate::tests::readable(
                    crate::saved_restore::list(install.keychain.clone()).unwrap(),
                );
                assert_eq!(saved[0].location, location);
                restore_from_saved_code(builder, &saved[0], None, "Laptop", |_| {}, &cancel)
                    .await
                    .unwrap()
                    .unwrap()
            } else {
                restore_from_code(
                    builder,
                    &code.to_text().unwrap(),
                    None,
                    "Laptop",
                    |_| {},
                    &cancel,
                )
                .await
                .unwrap()
            };
            let expected = owner.handle.storage_config().await.unwrap().unwrap();
            assert_eq!(handle.storage_config().await.unwrap(), Some(expected.clone()));
            assert_eq!(
                crate::tests::readable(
                    crate::saved_restore::list(install.keychain.clone()).unwrap()
                )[0]
                .location,
                expected
            );
            assert_eq!(
                handle.restore_code().await.unwrap(),
                owner.code.to_text().unwrap().as_str()
            );
            handle.close().await.unwrap();
            owner.close().await;
        }
    }
}

/// On Dropbox the join connects through Carol's own sign-in, which her phone
/// keeps beside the code while she waits. After a restart her join resumes
/// with no new sign-in, and the joined store keeps that sign-in (§13.2).
#[tokio::test]
async fn a_closed_join_on_dropbox_resumes_with_the_sign_in_it_kept() {
    let owner = Owner::new(CloudProvider::Dropbox, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let store = owner.directory.id();
    waiting_join(&owner, &install, &invite).await;
    let kept = install.only_kept_sign_in(store).await.expect("a kept sign-in");
    let [listed] =
        &install.builder(&owner, owner.recipient()).unfinished_joins().await.unwrap()[..]
    else {
        panic!("one join under way");
    };
    // It still names the access a new sign-in needs, should Dropbox refuse
    // the kept one.
    assert_eq!(listed.metadata.oauth_access, owner.storage.config().oauth_access());
    let sign_in = SignIn::new(owner.clock.clone()).await;
    let builder = sign_in.configure(install.builder(&owner, owner.recipient()));
    let control = JoinControl::new();
    let resumed = builder.resume_join(store, |_| {}, &control);
    let (result, _) = join_and_approve(&owner.handle, resumed, |_| async {}).await;
    let handle = result.unwrap().unwrap();
    assert_eq!(sign_in.presentation_count(), 0);
    assert_eq!(
        install.device_tokens(store).unwrap().access_token.as_str(),
        kept.access_token.as_str()
    );
    assert!(saved_code(&install, store).is_none());
    handle.close().await.unwrap();
    owner.close().await;
}

/// Dropbox rejects Carol's token while she waits. Its one refresh is
/// committed to her phone's keychain before the retried check uses it. Her
/// phone then restarts, with no sign-in and no way to refresh one: the
/// resumed join starts from the refreshed token, and the joined store keeps
/// it (§13.2, §20.2).
#[tokio::test]
async fn a_refresh_while_waiting_is_kept_for_the_resumed_join() {
    let owner = Owner::new(CloudProvider::Dropbox, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let store = owner.directory.id();
    let sign_in = SignIn::new(owner.clock.clone()).await;
    let (sleeping, mut asleep) = watch::channel(false);
    let (resume, resumed) = watch::channel(false);
    let clock = Arc::new(JoinClock {
        clock: crate::tests::PollingClock::new(owner.clock.clone()),
        sleeping,
        resume: resumed,
    });
    let mut builder = sign_in.configure(install.builder(&owner, owner.recipient()).clock(clock));
    builder.authenticate(OAuthAccess::Dropbox(DropboxAccess::FullDropbox)).await.unwrap();
    let control = JoinControl::new();
    let (waited, mut waits) = watch::channel(0_usize);
    let joining = join_with_invite(
        builder,
        &invite.code,
        "Carol's phone",
        |status| {
            if matches!(
                status,
                BootstrapStatus::WaitingForApproval { next_check: NextCheck::At(_), .. }
            ) {
                waited.send_modify(|waits| *waits += 1);
            }
        },
        &control,
    );
    let refreshed = async {
        asleep.wait_for(|sleeping| *sleeping).await.unwrap();
        owner.storage.reject_access_token("access-0").await;
        resume.send(true).unwrap();
        // The second wait follows the check that refreshed the token.
        waits.wait_for(|waits| *waits >= 2).await.unwrap();
    };
    tokio::time::timeout(Duration::from_secs(20), async {
        tokio::select! {
            result = joining => panic!("the join ended while waiting: {result:?}"),
            () = refreshed => (),
        }
    })
    .await
    .unwrap();
    assert_eq!(sign_in.request_count(), 2);
    let kept = install.only_kept_sign_in(store).await.unwrap();
    assert_eq!(kept.access_token.as_str(), "access-1");
    let resumed = install.builder(&owner, owner.recipient()).resume_join(store, |_| {}, &control);
    let (result, _) = join_and_approve(&owner.handle, resumed, |_| async {}).await;
    let handle = result.unwrap().unwrap();
    assert_eq!(install.device_tokens(store).unwrap().access_token.as_str(), "access-1");
    assert_eq!(sign_in.request_count(), 2);
    handle.close().await.unwrap();
    owner.close().await;
}

/// A sign-in the builder holds when Carol's join resumes replaces the one it
/// kept, but only once it is shown to grant the shared folder's access: an
/// App-folder sign-in is refused and leaves the kept one (§13.2).
#[tokio::test]
async fn a_new_sign_in_replaces_the_kept_one_only_with_the_locations_access() {
    let owner = Owner::new(CloudProvider::Dropbox, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let store = owner.directory.id();
    let sign_in = SignIn::new(owner.clock.clone()).await;
    let signed_in = |access| {
        let mut builder = sign_in.configure(install.builder(&owner, owner.recipient()));
        async move {
            builder.authenticate(OAuthAccess::Dropbox(access)).await.unwrap();
            builder
        }
    };
    waiting_join_with(signed_in(DropboxAccess::FullDropbox).await, &invite).await;
    let kept = || async { install.only_kept_sign_in(store).await.unwrap().access_token };
    assert_eq!(kept().await.as_str(), "access-0");
    let control = JoinControl::new();
    let required = OAuthAccess::Dropbox(DropboxAccess::FullDropbox);
    let refused =
        signed_in(DropboxAccess::AppFolder).await.resume_join(store, |_| {}, &control).await;
    assert!(
        matches!(&refused, Err(BootstrapError::Sync(SyncError::Storage(error)))
            if error.failure() == StorageFailure::AccessNotGranted { required }),
        "{refused:?}"
    );
    assert_eq!(kept().await.as_str(), "access-0");
    let resumed = signed_in(DropboxAccess::FullDropbox).await.resume_join(store, |_| {}, &control);
    let (result, _) = join_and_approve(&owner.handle, resumed, |_| async {}).await;
    let handle = result.unwrap().unwrap();
    assert_eq!(install.device_tokens(store).unwrap().access_token.as_str(), "access-2");
    handle.close().await.unwrap();
    owner.close().await;
}

/// Dropbox refuses the sign-in Carol's join kept, and it can't be refreshed:
/// resuming asks for a new sign-in with `OAuthError::Reauthorize` and keeps
/// the join, which resumes once she signs in (§13.2).
#[tokio::test]
async fn a_refused_kept_sign_in_asks_for_a_new_one_and_keeps_the_join() {
    let owner = Owner::new(CloudProvider::Dropbox, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let store = owner.directory.id();
    waiting_join(&owner, &install, &invite).await;
    // A kept sign-in without a refresh token: its rejection can't be refreshed.
    let mut refused = install.only_kept_sign_in(store).await.unwrap();
    refused.access_token = SecretText::new("refused-access".into());
    refused.refresh_token = None;
    StoreKeychain::new(install.keychain.clone(), store)
        .set_storage_credentials(&StorageCredentials::OAuth(refused).encode().unwrap())
        .unwrap();
    owner.storage.reject_access_token("refused-access").await;
    let sign_in = SignIn::new(owner.clock.clone()).await;
    let control = JoinControl::new();
    let result = sign_in
        .configure(install.builder(&owner, owner.recipient()))
        .resume_join(store, |_| {}, &control)
        .await;
    assert!(
        matches!(
            result,
            Err(BootstrapError::OAuth(OAuthError::Reauthorize(CloudProvider::Dropbox)))
        ),
        "{result:?}"
    );
    let builder = install.builder(&owner, owner.recipient());
    assert_eq!(builder.unfinished_joins().await.unwrap().len(), 1);
    assert_eq!(
        install.only_kept_sign_in(store).await.unwrap().access_token.as_str(),
        "refused-access"
    );
    let mut builder = sign_in.configure(builder);
    builder.authenticate(OAuthAccess::Dropbox(DropboxAccess::FullDropbox)).await.unwrap();
    let resumed = builder.resume_join(store, |_| {}, &control);
    let (result, _) = join_and_approve(&owner.handle, resumed, |_| async {}).await;
    let handle = result.unwrap().unwrap();
    assert_eq!(install.device_tokens(store).unwrap().access_token.as_str(), "access-0");
    handle.close().await.unwrap();
    owner.close().await;
}

/// Ending a declined join removes its kept sign-in, then its code, then its
/// installation. A keychain failure part way leaves the code, so the join
/// still lists, and the next call ends it: from the kept sign-in when
/// removing that failed, or after a new sign-in when it was already gone
/// (§13.2).
#[tokio::test]
async fn a_join_that_fails_to_end_still_lists_and_the_next_call_ends_it() {
    for sign_in_removed in [false, true] {
        let owner = Owner::new(CloudProvider::Dropbox, false).await;
        let invite = owner.invite().await;
        let install = Installation::new();
        let store = owner.directory.id();
        waiting_join(&owner, &install, &invite).await;
        let request = next_join_request(&owner.handle).await;
        owner.handle.decline_join_request(&request).await.unwrap();
        let pending = install.layout.begin_bootstrap(store, &UuidIds).await.unwrap();
        // The first deletion that fails: the sign-in's, or the code's after it.
        install.keychain.fail_operations_after(&[usize::from(sign_in_removed)]);
        let ended = remove_unfinished(
            &StoreKeychain::new(install.keychain.clone(), store),
            &pending,
            Ok(None::<CovenHandle>),
        );
        assert!(matches!(ended, Err(BootstrapError::SecureStorage(_))), "{ended:?}");
        drop(pending);
        let builder = install.builder(&owner, owner.recipient());
        assert_eq!(builder.unfinished_joins().await.unwrap().len(), 1);
        assert_eq!(install.only_kept_sign_in(store).await.is_none(), sign_in_removed);
        let sign_in = SignIn::new(owner.clock.clone()).await;
        let control = JoinControl::new();
        let mut builder = sign_in.configure(builder);
        if sign_in_removed {
            let asked = sign_in
                .configure(install.builder(&owner, owner.recipient()))
                .resume_join(store, |_| {}, &control)
                .await;
            assert!(
                matches!(
                    asked,
                    Err(BootstrapError::OAuth(OAuthError::Reauthorize(CloudProvider::Dropbox)))
                ),
                "{asked:?}"
            );
            builder.authenticate(OAuthAccess::Dropbox(DropboxAccess::FullDropbox)).await.unwrap();
        }
        let result = builder.resume_join(store, |_| {}, &control).await;
        assert!(matches!(result, Ok(None)), "{result:?}");
        install.absent(store).await;
        assert!(saved_code(&install, store).is_none());
        assert!(install
            .builder(&owner, owner.recipient())
            .unfinished_joins()
            .await
            .unwrap()
            .is_empty());
        owner.close().await;
    }
}

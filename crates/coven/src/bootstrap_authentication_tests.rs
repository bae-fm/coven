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
            SyncedKeychain::new(install.keychain.clone(), owner.directory.id())
                .set_restore_code(&SecretBytes::new(owner.code.to_bytes().unwrap().to_vec()))
                .unwrap();
            restore_from_keychain(
                builder,
                &crate::saved_restore::list(install.keychain.clone()).unwrap()[0],
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
        assert_eq!(
            handle.restore_code().await.unwrap(),
            owner.code.to_text().unwrap().as_str()
        );
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
        restore_from_code(
            builder,
            &owner.code.to_text().unwrap(),
            None,
            "Laptop",
            |_| {},
            &cancel
        )
        .await,
        Err(BootstrapError::OAuth(OAuthError::Reauthorize(
            CloudProvider::OneDrive
        )))
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
    builder
        .authenticate(OAuthAccess::Dropbox(DropboxAccess::AppFolder))
        .await
        .unwrap();
    let before = recipient.request_count();
    let (_, cancel) = watch::channel(false);
    let required = OAuthAccess::Dropbox(DropboxAccess::FullDropbox);
    assert!(matches!(
        join_with_invite(builder, &invite.code, "New phone", |_| {}, &cancel).await,
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
        clock: crate::tests::PollingClock(owner.clock.clone()),
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
    let (_, cancel) = watch::channel(false);
    let join = join_with_invite(builder, &invite.code, "New phone", |_| {}, &cancel);
    let (joined, _) = join_and_approve(&owner.handle, join, |_| async {
        install.absent(owner.directory.id()).await;
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
            let mut location = StorageConfig::decode(code.storage.as_bytes()).unwrap();
            match &mut location {
                StorageConfig::Dropbox { account_label, .. }
                | StorageConfig::OneDrive { account_label, .. } => {
                    *account_label = Some("old@example.com".into());
                }
                _ => unreachable!(),
            }
            code.storage = location.encode().unwrap();
            let builder = install
                .authenticated_builder(&owner, owner.storage.clone())
                .await;
            let (_, cancel) = watch::channel(false);
            let handle = if from_keychain {
                SyncedKeychain::new(install.keychain.clone(), owner.directory.id())
                    .set_restore_code(&SecretBytes::new(code.to_bytes().unwrap().to_vec()))
                    .unwrap();
                let saved = crate::saved_restore::list(install.keychain.clone()).unwrap();
                assert_eq!(saved[0].location, location);
                restore_from_keychain(builder, &saved[0], None, "Laptop", |_| {}, &cancel)
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
            assert_eq!(
                handle.storage_config().await.unwrap(),
                Some(expected.clone())
            );
            assert_eq!(
                crate::saved_restore::list(install.keychain.clone()).unwrap()[0].location,
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

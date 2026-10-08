use super::*;
use crate::authentication::SignIn;

#[tokio::test]
async fn restore_and_keychain_restore_use_builder_sign_in_without_returning_tokens() {
    for from_keychain in [false, true] {
        let owner = Owner::new(CloudProvider::Dropbox, false).await;
        let install = Installation::new();
        let sign_in = SignIn::new(owner.clock.clone()).await;
        let mut builder = sign_in.configure(install.builder(&owner, owner.storage.clone()));
        builder.authenticate(CloudProvider::Dropbox).await.unwrap();
        install.absent(owner.directory.id()).await;
        let (_, cancel) = watch::channel(false);
        let handle = if from_keychain {
            StoreKeychain::new(install.keychain.clone(), owner.directory.id())
                .set_synced_restore_code(&SecretBytes::new(owner.code.to_bytes().unwrap().to_vec()))
                .unwrap();
            restore_from_keychain(builder, "Laptop", |_| {}, &cancel)
                .await
                .unwrap()
                .unwrap()
        } else {
            restore_from_code(
                builder,
                &owner.code.to_text().unwrap(),
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
    let owner = Owner::new(CloudProvider::GoogleDrive, false).await;
    let install = Installation::new();
    let sign_in = SignIn::new(owner.clock.clone()).await;
    let builder = sign_in.configure(install.builder(&owner, owner.storage.clone()));
    let (_, cancel) = watch::channel(false);
    assert!(matches!(
        restore_from_code(
            builder,
            &owner.code.to_text().unwrap(),
            "Laptop",
            |_| {},
            &cancel
        )
        .await,
        Err(BootstrapError::OAuth(OAuthError::Reauthorize(
            CloudProvider::GoogleDrive
        )))
    ));
    assert_eq!(sign_in.presentation_count(), 0);
    install.absent(owner.directory.id()).await;
    owner.close().await;
}

#[tokio::test]
async fn join_refreshes_while_waiting_and_publishes_the_refreshed_credentials() {
    let owner = Owner::new(CloudProvider::Dropbox, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let sign_in = SignIn::new(owner.clock.clone()).await;
    let mut builder = sign_in.configure(install.builder(&owner, owner.recipient()));
    builder.authenticate(CloudProvider::Dropbox).await.unwrap();
    let (_, cancel) = watch::channel(false);
    let mut requests = owner.operations.subscribe_join_requests();
    let join = join_with_invite(builder, &invite.code, "New phone", |_| {}, &cancel);
    let approve = async {
        let request = next_request(&mut requests).await;
        install.absent(owner.directory.id()).await;
        owner.clock.set(UNIX_EPOCH + Duration::from_secs(3600));
        owner
            .operations
            .approve_join_request(&request)
            .await
            .unwrap();
    };
    let (joined, ()) = tokio::time::timeout(Duration::from_secs(20), async {
        tokio::join!(join, approve)
    })
    .await
    .unwrap();
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

use super::*;
use crate::authentication::SignIn;

#[cfg(test)]
struct JoinClock {
    clock: crate::tests::PollingClock,
    sleeping: watch::Sender<bool>,
    resume: watch::Receiver<bool>,
}

impl Clock for JoinClock {
    fn now(&self) -> std::time::SystemTime {
        self.clock.now()
    }

    fn sleep(
        &self,
        duration: Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        if duration == Duration::from_secs(1) && !*self.resume.borrow() {
            self.sleeping.send(true).unwrap();
            let mut resume = self.resume.clone();
            Box::pin(async move {
                resume.wait_for(|resumed| *resumed).await.unwrap();
            })
        } else {
            self.clock.sleep(duration)
        }
    }
}

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
    let (sleeping, mut asleep) = watch::channel(false);
    let (resume, resumed) = watch::channel(false);
    let clock = Arc::new(JoinClock {
        clock: crate::tests::PollingClock(owner.clock.clone()),
        sleeping,
        resume: resumed,
    });
    let mut builder = sign_in.configure(install.builder(&owner, owner.recipient()).clock(clock));
    builder.authenticate(CloudProvider::Dropbox).await.unwrap();
    let (_, cancel) = watch::channel(false);
    let join = join_with_invite(builder, &invite.code, "New phone", |_| {}, &cancel);
    let (joined, _) = join_and_approve(&owner.handle, join, |_| async {
        install.absent(owner.directory.id()).await;
        asleep.wait_for(|sleeping| *sleeping).await.unwrap();
        owner
            .clock
            .set(owner.clock.now() + Duration::from_secs(3600));
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

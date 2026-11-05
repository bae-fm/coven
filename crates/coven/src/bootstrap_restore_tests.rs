use super::*;
use coven_database::DeviceInfo;
use coven_foundation::clock::HANG_GUARD;

/// Ana's devices as the owner's applied slots have them, once it has read
/// what storage holds.
async fn ana_devices(owner: &Owner) -> Vec<DeviceInfo> {
    owner.sync().await;
    let members = owner.handle.members().await.unwrap();
    members.into_iter().find(|member| member.is_self).expect("Ana is a member").devices
}

/// The new installation's device id, which its settings hold from the start.
fn laptop(install: &Installation, owner: &Owner) -> DeviceId {
    install.app.store_dir(&owner.directory.id()).settings().unwrap().device_id
}

/// Hold the next object created in the store, the laptop's registration
/// once loading is done: `started` hears it is on its way, and it is stored
/// once `release` is sent.
async fn hold_registration(
    owner: &Owner,
) -> (tokio::sync::oneshot::Receiver<()>, tokio::sync::oneshot::Sender<()>) {
    let (started, starting) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    owner
        .storage
        .hold_next_creation(ObjectPrefix::all(owner.directory.id()), started, released)
        .await;
    (starting, release)
}

/// Ana restores Household on her laptop from her code.
fn restoring<'a>(
    owner: &'a Owner,
    builder: CovenBuilder,
    cancel: &'a watch::Receiver<bool>,
) -> impl std::future::Future<Output = Result<CovenHandle, BootstrapError>> + 'a {
    let code = owner.code.to_text().unwrap();
    async move {
        restore_from_code(builder, &code, owner.s3_credentials(), "Ana’s laptop", |_| {}, cancel)
            .await
    }
}

/// The restore Ana's laptop lists as under way: Household's, from her code.
async fn listed(install: &Installation, owner: &Owner) -> Vec<StoreId> {
    install
        .builder(owner, owner.storage.clone())
        .unfinished_restores()
        .await
        .unwrap()
        .into_iter()
        .map(|metadata| {
            assert_eq!(metadata.member, owner.member.member());
            metadata.store_id
        })
        .collect()
}

/// Resume the listed restore through a builder holding no sign-in, so a
/// Dropbox restore resumes from the sign-in it kept.
async fn resume(install: &Installation, owner: &Owner) -> Result<CovenHandle, BootstrapError> {
    let (_, cancel) = watch::channel(false);
    install
        .builder(owner, owner.storage.clone())
        .resume_restore(owner.directory.id(), |_| {}, &cancel)
        .await
}

/// Ana's laptop is registered once, as the device of the store it published.
async fn finished(install: &Installation, owner: &Owner, handle: CovenHandle) {
    assert_eq!(handle.restore_code().await.unwrap(), owner.code.to_text().unwrap().as_str());
    let device = laptop(install, owner);
    handle.close().await.unwrap();
    let devices = ana_devices(owner).await;
    assert_eq!(devices.iter().filter(|known| known.id == device).count(), 1, "{devices:?}");
    assert_eq!(devices.len(), 2, "{devices:?}");
    assert!(listed(install, owner).await.is_empty());
    assert_eq!(install.app.stores().await.unwrap().len(), 1);
}

/// Ana's laptop registers while restoring Household, and its call fails
/// before it learns so: the provider stores the registration and answers
/// with an error. The laptop holds a device that history names as Ana's.
/// The restore is listed, and resuming it finishes without the code,
/// registering nothing more (§13.1, §11.6).
#[tokio::test]
async fn a_restore_that_ends_after_its_registration_landed_is_listed_and_resumes() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let install = Installation::new();
    let (_, cancel) = watch::channel(false);
    owner.storage.set_faults(Faults { refuse_after_storing: 1, ..Faults::none() }).await;
    let builder = install.authenticated_builder(&owner, owner.storage.clone()).await;
    let ended = restoring(&owner, builder, &cancel).await;
    assert!(ended.is_err(), "{ended:?}");
    owner.storage.set_faults(Faults::none()).await;
    let device = laptop(&install, &owner);
    assert!(ana_devices(&owner).await.iter().any(|known| known.id == device));
    assert_eq!(listed(&install, &owner).await, [owner.directory.id()]);
    finished(&install, &owner, resume(&install, &owner).await.unwrap()).await;
    owner.close().await;
}

/// The app drops Ana's restore while its registration is on its way. The
/// registration goes on and lands; the restore is listed from before it was
/// sent, and resumes from the S3 key or Dropbox sign-in it kept, or the
/// Apple account (§13.1).
#[tokio::test]
async fn a_dropped_restore_is_listed_and_resumes_from_what_it_kept() {
    for provider in [CloudProvider::S3, CloudProvider::Dropbox, CloudProvider::CloudKit] {
        let owner = Owner::new(provider, false).await;
        let install = Installation::new();
        let (_, cancel) = watch::channel(false);
        let (started, release) = hold_registration(&owner).await;
        let builder = install.authenticated_builder(&owner, owner.storage.clone()).await;
        tokio::time::timeout(HANG_GUARD, async {
            tokio::select! {
                result = restoring(&owner, builder, &cancel) => {
                    panic!("{provider:?}: the restore ended before registering: {result:?}")
                }
                started = started => started.unwrap(),
            }
        })
        .await
        .unwrap();
        assert_eq!(listed(&install, &owner).await, [owner.directory.id()], "{provider:?}");
        release.send(()).unwrap();
        let resumed = tokio::time::timeout(HANG_GUARD, resume(&install, &owner)).await.unwrap();
        finished(&install, &owner, resumed.unwrap()).await;
        owner.close().await;
    }
}

/// Ana cancels the restore while its registration is on its way. The cancel
/// waits for the registration to land, then refuses: the laptop is a device
/// of hers now, so the restore stays listed, and resuming it finishes
/// (§13.1, §11.6).
#[tokio::test]
async fn cancelling_a_restore_after_it_registers_refuses_and_keeps_it() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let install = Installation::new();
    let (stop, cancel) = watch::channel(false);
    let (started, release) = hold_registration(&owner).await;
    let builder = install.authenticated_builder(&owner, owner.storage.clone()).await;
    let cancelling = async {
        started.await.unwrap();
        stop.send_replace(true);
        release.send(()).unwrap();
    };
    let (result, ()) = tokio::time::timeout(HANG_GUARD, async {
        tokio::join!(restoring(&owner, builder, &cancel), cancelling)
    })
    .await
    .unwrap();
    assert!(matches!(result, Err(BootstrapError::RestoreRegistered)), "{result:?}");
    let device = laptop(&install, &owner);
    assert!(ana_devices(&owner).await.iter().any(|known| known.id == device));
    assert_eq!(listed(&install, &owner).await, [owner.directory.id()]);
    finished(&install, &owner, resume(&install, &owner).await.unwrap()).await;
    owner.close().await;
}

/// Carol restores on a new phone while Ana removes her. Ana's removal takes
/// the slot Carol's registration was sent for, and checked again, the
/// registration is refused: nothing in storage names the phone, so the
/// restore ends without its store and leaves nothing behind (§11.6).
#[tokio::test]
async fn a_restore_whose_registration_is_refused_ends_and_removes_its_installation() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let joined = Installation::new();
    let (_, cancel) = watch::channel(false);
    let joining =
        joined.run(&owner, Installation::join_request(&invite), owner.recipient(), &cancel, |_| {});
    let (result, carol) = join_and_approve(&owner.handle, joining, |_| async {}).await;
    let carol_phone = result.unwrap().unwrap();
    let code = carol_phone.restore_code().await.unwrap();
    carol_phone.close().await.unwrap();
    let install = Installation::new();
    let (started, release) = hold_registration(&owner).await;
    let builder = install.authenticated_builder(&owner, owner.storage.clone()).await;
    let restoring = restore_from_code(
        builder,
        &code,
        Some(S3Credentials {
            access_key_id: "invited-key".into(),
            secret_access_key: SecretText::new("invited-secret".into()),
        }),
        "Carol’s new phone",
        |_| {},
        &cancel,
    );
    let removing = async {
        started.await.unwrap();
        crate::tests::landed(&owner.handle, owner.handle.remove_member(&carol).await.unwrap())
            .await;
        release.send(()).unwrap();
    };
    let (result, ()) =
        tokio::time::timeout(HANG_GUARD, async { tokio::join!(restoring, removing) })
            .await
            .unwrap();
    assert!(
        matches!(result, Err(BootstrapError::Sync(SyncError::Removed | SyncError::Rejected(_)))),
        "{result:?}"
    );
    assert!(listed(&install, &owner).await.is_empty());
    install.absent(owner.directory.id()).await;
    assert!(!install.root.path().join("stores").join(owner.directory.id().to_string()).exists());
    owner.close().await;
}

/// Ana's laptop sends its registration, and the provider refuses it, so
/// nothing is stored and the call fails. The registration stays journaled
/// with its claim, and the restore listed. Resuming settles that claim,
/// which sends the same registration again, and finishes without
/// registering a second time (§13.1, §5.1).
#[tokio::test]
async fn a_resumed_restore_finishes_the_registration_it_journaled() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let install = Installation::new();
    let (_, cancel) = watch::channel(false);
    let slot = std::num::NonZeroU64::new(owner.applied().await + 1).unwrap();
    let path = ObjectPath::slot(owner.directory.id(), slot);
    owner
        .storage
        .set_faults(Faults {
            create_failures: [(path, (StorageFailure::Network, 503))].into(),
            ..Faults::none()
        })
        .await;
    let builder = install.authenticated_builder(&owner, owner.storage.clone()).await;
    let ended = restoring(&owner, builder, &cancel).await;
    assert!(matches!(ended, Err(BootstrapError::Sync(SyncError::Storage(_)))), "{ended:?}");
    owner.storage.set_faults(Faults::none()).await;
    let device = laptop(&install, &owner);
    assert!(!ana_devices(&owner).await.iter().any(|known| known.id == device));
    assert_eq!(listed(&install, &owner).await, [owner.directory.id()]);
    finished(&install, &owner, resume(&install, &owner).await.unwrap()).await;
    owner.close().await;
}

/// Ana's laptop registers and its call fails before it learns so. Before
/// she resumes, her phone removes the laptop. Nothing in storage names the
/// laptop as active any more, so the resumed restore ends without its store
/// and leaves nothing behind (§13.1, §11.6).
#[tokio::test]
async fn a_restore_whose_device_was_removed_ends_and_removes_its_installation() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let install = Installation::new();
    let (_, cancel) = watch::channel(false);
    owner.storage.set_faults(Faults { refuse_after_storing: 1, ..Faults::none() }).await;
    let builder = install.authenticated_builder(&owner, owner.storage.clone()).await;
    assert!(restoring(&owner, builder, &cancel).await.is_err());
    owner.storage.set_faults(Faults::none()).await;
    let device = laptop(&install, &owner);
    assert!(ana_devices(&owner).await.iter().any(|known| known.id == device));
    let (removal, _) = owner.handle.remove_device(device).await.unwrap();
    crate::tests::landed(&owner.handle, removal).await;
    let resumed = resume(&install, &owner).await;
    assert!(matches!(resumed, Err(BootstrapError::Sync(SyncError::Removed))), "{resumed:?}");
    assert!(listed(&install, &owner).await.is_empty());
    install.absent(owner.directory.id()).await;
    assert!(!install.root.path().join("stores").join(owner.directory.id().to_string()).exists());
    owner.close().await;
}

/// Carol's phone waits for approval of its join to Household, and the app
/// is closed. Someone then restores Household on the phone from Ana's code
/// and cancels it as it connects. The phone's one installation of Household
/// is the join's, so the restore is refused before it changes anything:
/// cancelled, it would remove the installation with the join's saved code
/// and sign-in, while Ana may still approve Carol's request (§13.4).
#[tokio::test]
async fn a_restore_refuses_a_waiting_joins_installation() {
    for provider in [CloudProvider::S3, CloudProvider::Dropbox] {
        let owner = Owner::new(provider, false).await;
        let invite = owner.invite().await;
        let install = Installation::new();
        let store = owner.directory.id();
        waiting_join(&owner, &install, &invite).await;
        let kept = install
            .only_kept_sign_in(store)
            .await
            .map(|tokens| tokens.access_token.as_str().to_owned());
        let (stop, cancel) = watch::channel(false);
        let ended = restore_from_code(
            install.authenticated_builder(&owner, owner.storage.clone()).await,
            &owner.code.to_text().unwrap(),
            owner.s3_credentials(),
            "Ana’s laptop",
            cancel_at(Phase::Connecting, &stop),
            &cancel,
        )
        .await;
        assert!(
            matches!(
                ended,
                Err(BootstrapError::Directory(BootstrapDirectoryError::Lock(
                    StoreLockError::BootstrapPending(refused)
                ))) if refused == store
            ),
            "{provider:?}: {ended:?}"
        );
        assert_eq!(saved_code(&install, store).unwrap().as_str(), invite.shown_code());
        assert_eq!(
            install
                .only_kept_sign_in(store)
                .await
                .map(|tokens| tokens.access_token.as_str().to_owned()),
            kept,
            "{provider:?}"
        );
        let control = JoinControl::new();
        let resumed =
            install.builder(&owner, owner.recipient()).resume_join(store, |_| {}, &control);
        let (result, _) = join_and_approve(&owner.handle, resumed, |_| async {}).await;
        result.unwrap().unwrap().close().await.unwrap();
        owner.close().await;
    }
}

/// Ana's laptop registers while restoring Household, and its call fails
/// before it learns so: the restore is recorded and listed. A join with an
/// invite to Household then begins on the laptop and is cancelled at once.
/// The laptop's one installation of Household is the restore's, so the join
/// is refused before it changes anything: cancelled, it would remove the
/// installation with the restore's record, and the laptop would stay
/// registered with nothing listed to finish it (§13.4).
#[tokio::test]
async fn a_join_refuses_a_recorded_restores_installation() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let store = owner.directory.id();
    let (_, cancel) = watch::channel(false);
    owner.storage.set_faults(Faults { refuse_after_storing: 1, ..Faults::none() }).await;
    let builder = install.authenticated_builder(&owner, owner.storage.clone()).await;
    assert!(restoring(&owner, builder, &cancel).await.is_err());
    owner.storage.set_faults(Faults::none()).await;
    assert_eq!(listed(&install, &owner).await, [store]);
    let control = JoinControl::new();
    control.cancel();
    let joined = join_with_invite(
        install.authenticated_builder(&owner, owner.recipient()).await,
        invite.shown_code(),
        "Carol's phone",
        |_| {},
        &control,
    )
    .await;
    assert!(
        matches!(
            joined,
            Err(BootstrapError::Directory(BootstrapDirectoryError::Lock(
                StoreLockError::BootstrapPending(refused)
            ))) if refused == store
        ),
        "{joined:?}"
    );
    assert!(saved_code(&install, store).is_none());
    assert_eq!(listed(&install, &owner).await, [store]);
    // Carol's invite waits for a request, so the owner's passes stay
    // pending: the resumed restore is checked on the laptop alone.
    let handle = resume(&install, &owner).await.unwrap();
    assert_eq!(handle.restore_code().await.unwrap(), owner.code.to_text().unwrap().as_str());
    handle.close().await.unwrap();
    assert!(listed(&install, &owner).await.is_empty());
    owner.close().await;
}

/// Ana's laptop stops while it loads Household, before the restore records
/// itself: nothing in storage names the laptop, and nothing lists the
/// installation it was loading. The app's next listing of unfinished
/// restores removes it. While the restore was still loading, the same
/// listing left it alone (§13.4, §11.6).
#[tokio::test]
async fn listing_reclaims_an_installation_left_before_the_record() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    owner.stop_sync().await;
    let install = Installation::new();
    let store = owner.directory.id();
    let installation = install.root.path().join("stores").join(store.to_string());
    let (read, reading) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    owner
        .storage
        .hold_next_read(ObjectPath::slot(store, std::num::NonZeroU64::MIN), read, resumed)
        .await;
    let (_, cancel) = watch::channel(false);
    let builder = install.authenticated_builder(&owner, owner.storage.clone()).await;
    {
        let restoring = restoring(&owner, builder, &cancel);
        tokio::pin!(restoring);
        tokio::time::timeout(HANG_GUARD, async {
            tokio::select! {
                result = &mut restoring => panic!("the restore ended while loading: {result:?}"),
                held = reading => held.unwrap(),
            }
        })
        .await
        .unwrap();
        assert!(listed(&install, &owner).await.is_empty());
        assert!(installation.exists(), "a listing removed an installation a call holds");
    }
    // The app dropped the restore, and the read it was waiting for with it.
    drop(resume);
    assert!(installation.exists());
    assert!(listed(&install, &owner).await.is_empty());
    assert!(!installation.exists(), "the listing left the installation nothing refers to");
    install.absent(store).await;
    finished(
        &install,
        &owner,
        restoring(
            &owner,
            install.authenticated_builder(&owner, owner.storage.clone()).await,
            &cancel,
        )
        .await
        .unwrap(),
    )
    .await;
    owner.close().await;
}

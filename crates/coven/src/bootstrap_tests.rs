use super::*;
use coven_crypto::custody::{Keychain, KeyringCustody};
use coven_database::DatabaseBuilder;
use coven_storage::{
    test_utils::{Faults, MemoryStorage},
    ObjectPath, ObjectPrefix, S3Credentials, Storage, StorageFailure,
};
use std::num::NonZeroUsize;
use std::time::Duration;

#[path = "../tests/fixtures/bootstrap.rs"]
mod fixtures;
use coven_foundation::clock::HANG_GUARD;
use fixtures::*;

#[tokio::test]
async fn restore_code_api_never_discloses_storage_credentials() {
    for provider in [
        CloudProvider::S3,
        CloudProvider::Dropbox,
        CloudProvider::OneDrive,
        CloudProvider::CloudKit,
    ] {
        let owner = Owner::new(provider, false).await;
        let text = owner.handle.restore_code().await.unwrap();
        let code = RestoreCode::from_text(&text).unwrap();
        let bytes = code.to_bytes().unwrap();
        if let Some(key) = owner.s3_credentials() {
            let secret = key.secret_access_key.as_str().as_bytes();
            assert!(!bytes.windows(secret.len()).any(|window| window == secret));
        }
        assert_eq!(code.store, owner.directory.id());
        assert!(!bytes.windows(b"Household".len()).any(|part| part == b"Household"));
        assert_eq!(code.member, owner.member.member());
        // The storage part is the location alone.
        let location = owner.handle.storage_config().await.unwrap().unwrap();
        assert_eq!(
            code.storage.as_bytes(),
            coven_storage::RestoreStorage::location_only(location).encode().unwrap().as_bytes()
        );
        owner.close().await;
    }
}

struct JoinClock {
    clock: Arc<FixedClock>,
    sleeping: watch::Sender<bool>,
    resume: watch::Receiver<bool>,
}

impl JoinClock {
    /// The joiner's one-second check wait, held until the test resumes it.
    fn held(
        &self,
        duration: Duration,
    ) -> Option<std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>> {
        if duration != Duration::from_secs(1) || *self.resume.borrow() {
            return None;
        }
        self.sleeping.send(true).unwrap();
        let mut resume = self.resume.clone();
        Some(Box::pin(async move {
            resume.wait_for(|resumed| *resumed).await.unwrap();
        }))
    }
}

impl Clock for JoinClock {
    fn now(&self) -> std::time::SystemTime {
        self.clock.now()
    }

    fn elapsed(&self) -> Duration {
        self.clock.elapsed()
    }

    fn deadline(
        &self,
        duration: Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        self.held(duration).unwrap_or_else(|| self.clock.deadline(duration))
    }
    fn wait_out(
        &self,
        wait: Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        self.held(wait).unwrap_or_else(|| self.clock.wait_out(wait))
    }
    fn sleep_until(
        &self,
        at: std::time::SystemTime,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<(), coven_foundation::clock::WallTimerError>>
                + Send,
        >,
    > {
        self.clock.sleep_until(at)
    }
}

#[tokio::test]
async fn restore_loads_snapshots_and_later_writes_and_registers_the_device() {
    let owner = Owner::new(CloudProvider::S3, true).await;
    let install = Installation::new();
    let handle = install.restore(&owner).await;
    let directory = install.app.store_dir(&owner.directory.id());
    assert_eq!(handle.restore_code().await.unwrap(), owner.code.to_text().unwrap().as_str());
    let (person, _) = keychain_code(
        &PersonEntries::new(install.keychain.clone(), owner.directory.id()),
        owner.directory.id(),
    )
    .unwrap()
    .unwrap();
    assert_eq!(person.to_text().unwrap().as_str(), handle.restore_code().await.unwrap());
    handle.close().await.unwrap();
    assert_ne!(
        directory.settings().unwrap().device_id,
        owner.directory.settings().unwrap().device_id
    );
    let db = DatabaseBuilder::new(directory.clone())
        .synced_tables(tables())
        .migrations(migrations())
        .open()
        .await
        .unwrap();
    assert_eq!(rows(&db).await, owner.rows().await);
    // Registration claimed the slot after everything the owner applied.
    assert_eq!(db.applied_slot().await.unwrap(), owner.applied().await + 1);
    let state = db.store_state().await.unwrap();
    let device = directory.settings().unwrap().device_id;
    assert_eq!(state.devices[&device].member, owner.member.member());
    assert_eq!(state.devices[&device].name, "Ana’s laptop");
    assert!(directory.owned_file(StoreFile::Bootstrap).read_optional().unwrap().is_none());
    db.close().await.unwrap();
    owner.close().await;
}

#[tokio::test]
async fn join_accepts_provider_access_and_approval_then_loads_the_store() {
    for provider in [
        CloudProvider::S3,
        CloudProvider::Dropbox,
        CloudProvider::OneDrive,
        CloudProvider::CloudKit,
    ] {
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
        let oauth = provider.signs_in_with_oauth();
        let kept = std::sync::Mutex::new(None);
        let (result, member) = join_with_response(joining, async {
            waits.wait_for(|waiting| *waiting).await.unwrap();
            let request = owner.next_join_request().await;
            assert_eq!(request.device_name, "New phone");
            // While it waits, a join keeps nothing in custody but a Dropbox
            // or OneDrive sign-in, beside its code (§13.2).
            let held = install.only_kept_sign_in(owner.directory.id()).await;
            assert_eq!(held.is_some(), oauth, "{provider:?}");
            *kept.lock().unwrap() = held;
            crate::tests::approve(&owner.handle, &request).await;
            control.check_now();
            request.member
        })
        .await;
        let handle = result.unwrap().unwrap();
        if let Some(kept) = kept.into_inner().unwrap() {
            // The joined store's credentials are the sign-in the join kept.
            assert_eq!(
                install.device_tokens(owner.directory.id()).unwrap().access_token.as_str(),
                kept.access_token.as_str()
            );
        }
        if provider == CloudProvider::S3 {
            restore_credentials::assert_key(
                &install,
                owner.directory.id(),
                &S3Credentials {
                    access_key_id: "invited-key".into(),
                    secret_access_key: SecretText::new("invited-secret".into()),
                },
            );
        }
        let restored =
            coven_sync::read_restore_code(&handle.restore_code().await.unwrap()).unwrap();
        let location =
            coven_storage::RestoreStorage::decode(restored.storage.as_bytes()).unwrap().location;
        match &location {
            StorageConfig::Dropbox { account_label, .. }
            | StorageConfig::OneDrive { account_label, .. } => {
                assert_eq!(account_label.as_deref(), Some("join@example.com"));
            }
            _ => assert_eq!(location, owner.storage.config()),
        }
        // The device keeps the folder's label beside its location; no code holds it (§20.2).
        let kept = crate::storage::with_folder_label(&location);
        assert_eq!(handle.storage_config().await.unwrap(), Some(kept));
        assert_eq!(
            crate::tests::readable(crate::saved_restore::list(install.keychain.clone()).unwrap())
                [0]
            .location,
            location
        );
        assert_eq!(restored.member, member);
        assert_ne!(member, owner.member.member());
        assert_eq!(
            handle
                .read(|sql| Ok::<_, CovenError>(sql.query(
                    "SELECT id,body FROM notes ORDER BY id",
                    [],
                    |r| Ok((r.get::<_, String>(0)?, r.get::<_, Vec<u8>>(1)?))
                )?))
                .await
                .unwrap(),
            owner.rows().await
        );
        assert_eq!(
            handle.members().await.unwrap().iter().find(|m| m.is_self).unwrap().devices.len(),
            1
        );
        handle.close().await.unwrap();
        owner.close().await;
    }
}

#[tokio::test]
async fn restart_while_waiting_reuses_the_member_device_and_request_bytes() {
    use std::{
        future::{poll_fn, Future},
        task::Poll,
    };
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let (_, cancel) = watch::channel(false);
    let control = JoinControl::new();
    // Interrupt successive await points, including database opening and reads.
    // Each next attempt starts immediately, with no approval-timer barrier.
    for stop_at in 1..=12 {
        let mut joining = Box::pin(join_with_invite(
            install.builder(&owner, owner.recipient()),
            invite.shown_code(),
            "New phone",
            |_| {},
            &control,
        ));
        let mut polls = 0;
        poll_fn(|cx| {
            assert!(joining.as_mut().poll(cx).is_pending());
            polls += 1;
            if polls == stop_at {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
        drop(joining);
    }
    let mut joining = Box::pin(join_with_invite(
        install.builder(&owner, owner.recipient()),
        invite.shown_code(),
        "New phone",
        |_| {},
        &control,
    ));
    let request = tokio::time::timeout(HANG_GUARD, async {
        tokio::select! {
            result = &mut joining => panic!("join returned before approval: {result:?}"),
            request = owner.next_join_request() => request,
        }
    })
    .await
    .unwrap();
    let bytes = owner.storage.read(&crate::tests::request_path(&invite)).await.unwrap();
    drop(joining);
    let pending =
        install.app.directories().begin_bootstrap(owner.directory.id(), &UuidIds).await.unwrap();
    drop(pending.directory().lock_exclusive().unwrap());
    let device = pending.directory().settings().unwrap().device_id;
    drop(pending);
    install.absent(owner.directory.id()).await;
    let resumed = install.run(
        &owner,
        Installation::join_request(&invite, &control),
        owner.recipient(),
        &cancel,
        |_| {},
    );
    let approve = async {
        assert_eq!(owner.storage.read(&crate::tests::request_path(&invite)).await.unwrap(), bytes);
        crate::tests::approve(&owner.handle, &request).await;
        control.check_now();
    };
    let (result, ()) = join_with_response(resumed, approve).await;
    let handle = result.unwrap().unwrap();
    let directory = install.app.store_dir(&owner.directory.id());
    assert_eq!(directory.settings().unwrap().device_id, device);
    assert_eq!(
        keychain_code(
            &PersonEntries::new(install.keychain.clone(), owner.directory.id()),
            owner.directory.id(),
        )
        .unwrap()
        .unwrap()
        .0
        .member,
        request.member
    );
    handle.close().await.unwrap();
    owner.close().await;
}

#[tokio::test]
async fn permission_revocation_while_joining_keeps_the_provider_failure() {
    let owner = Owner::new(CloudProvider::OneDrive, false).await;
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
    let revoke = async {
        waits.wait_for(|waiting| *waiting).await.unwrap();
        owner.next_join_request().await;
        owner.storage.revoke_access("join@example.com").await.unwrap();
        control.check_now();
    };
    let (result, ()) = join_with_response(joining, revoke).await;
    assert!(
        matches!(result, Err(BootstrapError::Sync(SyncError::Storage(error))) if error.failure() == StorageFailure::PermissionDenied)
    );
    // The join stays unfinished with its code and kept sign-in (§13.2).
    assert!(install.only_kept_sign_in(owner.directory.id()).await.is_some());
    assert!(StoreKeychain::new(install.keychain.clone(), owner.directory.id())
        .invite_code()
        .unwrap()
        .is_some());
    owner.close().await;
}

#[tokio::test]
async fn cancellation_during_restore_and_before_publication_keeps_final_custody_empty() {
    // Before connecting, while loading, and after loading before publication.
    for phase in [Phase::Connecting, Phase::Loading, Phase::Loaded] {
        let owner = Owner::new(CloudProvider::S3, false).await;
        let install = Installation::new();
        let (stop, cancel) = watch::channel(false);
        let result = install
            .run(
                &owner,
                Installation::restore_request(&owner),
                owner.storage.clone(),
                &cancel,
                cancel_at(phase, &stop),
            )
            .await;
        assert!(matches!(result, Err(BootstrapError::Cancelled)), "{phase:?}: {result:?}");
        install.absent(owner.directory.id()).await;
        assert!(!install
            .root
            .path()
            .join("stores")
            .join(owner.directory.id().to_string())
            .exists());
        owner.close().await;
    }
}

/// A restore that fails before it records itself has only read storage: it
/// removes the installation it loaded into, and the next one starts over
/// (§13.1, §11.6).
#[tokio::test]
async fn a_restore_failing_before_its_record_removes_its_installation() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let install = Installation::new();
    let (_, cancel) = watch::channel(false);
    owner.storage.set_faults(Faults { fail_next: 1, ..Faults::none() }).await;
    assert!(matches!(
        install
            .run(
                &owner,
                Installation::restore_request(&owner),
                owner.storage.clone(),
                &cancel,
                |_| {}
            )
            .await,
        Err(BootstrapError::Sync(SyncError::Storage(_)))
    ));
    install.absent(owner.directory.id()).await;
    assert!(!install.root.path().join("stores").join(owner.directory.id().to_string()).exists());
    assert!(install
        .run(&owner, Installation::restore_request(&owner), owner.storage.clone(), &cancel, |_| {})
        .await
        .unwrap()
        .is_some());
    owner.close().await;
}

#[tokio::test]
async fn s3_credential_changes_preserve_the_code_location_and_member_across_reopen() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let install = Installation::new();
    let handle = install.restore(&owner).await;
    let directory = install.app.store_dir(&owner.directory.id());
    let before = handle.restore_code().await.unwrap();
    let code = coven_sync::read_restore_code(&before).unwrap();
    let person = PersonEntries::new(install.keychain.clone(), directory.id());
    let replacement = S3Credentials {
        access_key_id: "replacement".into(),
        secret_access_key: SecretText::new("new-secret".into()),
    };
    handle
        .replace_access_key(
            replacement.access_key_id.clone(),
            replacement.secret_access_key.clone(),
        )
        .await
        .unwrap();
    assert_eq!(handle.restore_code().await.unwrap(), before);
    assert_eq!(
        coven_sync::read_saved_code(
            directory.id(),
            person.restore_code().unwrap().unwrap().as_bytes()
        )
        .unwrap()
        .0
        .code
        .to_bytes()
        .unwrap(),
        code.to_bytes().unwrap()
    );
    restore_credentials::assert_key(&install, directory.id(), &replacement);
    handle
        .update_credentials("retained-device".into(), SecretText::new("retained-secret".into()))
        .await
        .unwrap();
    assert_eq!(handle.restore_code().await.unwrap(), before);
    restore_credentials::assert_key(
        &install,
        directory.id(),
        &S3Credentials {
            access_key_id: "retained-device".into(),
            secret_access_key: SecretText::new("retained-secret".into()),
        },
    );
    handle.close().await.unwrap();
    let handle = install
        .app
        .builder()
        .synced_tables(tables())
        .migrations(migrations())
        .clock(owner.clock.clone())
        .open(directory.id(), |_| ())
        .await
        .unwrap();
    assert_eq!(handle.restore_code().await.unwrap(), before);
    let original = owner.s3_credentials().unwrap();
    handle
        .update_credentials(original.access_key_id.clone(), original.secret_access_key.clone())
        .await
        .unwrap();
    restore_credentials::assert_key(&install, directory.id(), &original);
    assert_eq!(handle.restore_code().await.unwrap(), before);
    assert_eq!(coven_storage::recorded_config(&directory).unwrap(), Some(owner.storage.config()));
    assert_eq!(
        coven_sync::read_restore_code(&before).unwrap().member_keys.to_secret_bytes().as_bytes(),
        owner.member.keys().to_secret_bytes().as_bytes()
    );
    handle.close().await.unwrap();
    assert!(matches!(handle.restore_code().await, Err(SyncError::Database(DbError::StoreClosed))));
    owner.close().await;
}

#[tokio::test]
async fn approval_after_a_copy_miss_is_not_mistaken_for_decline() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let (_, cancel) = watch::channel(false);
    let (listed, listing) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    let code = coven_sync::read_invite_code(invite.shown_code()).unwrap();
    // The owner's own passes read that slot too: only the joiner's read is held.
    owner.stop_sync().await;
    // The first slot after the invite, where approval claims the copy.
    owner
        .storage
        .hold_next_read(
            ObjectPath::slot(owner.directory.id(), (code.read_through + 1).try_into().unwrap()),
            listed,
            resumed,
        )
        .await;
    let control = JoinControl::new();
    let joining = install.run(
        &owner,
        Installation::join_request(&invite, &control),
        owner.recipient(),
        &cancel,
        |_| {},
    );
    let approve = async {
        listing.await.unwrap();
        owner.handle.start_sync().await.unwrap();
        let request = owner.next_join_request().await;
        crate::tests::approve(&owner.handle, &request).await;
        resume.send(()).unwrap();
        control.check_now();
    };
    let (result, ()) = join_with_response(joining, approve).await;
    assert!(
        result.unwrap().is_some(),
        "approval deleted the request after this device observed a key-copy miss"
    );
    owner.close().await;
}

#[tokio::test]
async fn removal_lists_every_recorded_key_including_a_concurrent_replacement() {
    for concurrent in [true, false] {
        let owner = Owner::new(CloudProvider::S3, false).await;
        let invite = owner.invite().await;
        let install = Installation::new();
        let (_, cancel) = watch::channel(false);
        let control = JoinControl::new();
        let joining = install.run(
            &owner,
            Installation::join_request(&invite, &control),
            owner.recipient(),
            &cancel,
            |_| {},
        );
        let (result, member) = owner.join_and_approve(joining, &control, |_| async {}).await;
        let handle = result.unwrap().unwrap();
        let replace = || {
            handle.replace_access_key("new-member-key".into(), SecretText::new("new-secret".into()))
        };
        if concurrent {
            let (listed, listing) = tokio::sync::oneshot::channel();
            let (resume, resumed) = tokio::sync::oneshot::channel();
            owner
                .storage
                .hold_next_creation(ObjectPrefix::all(owner.directory.id()), listed, resumed)
                .await;
            let replacement = async {
                listing.await.unwrap();
                replace().await.unwrap();
                let code = handle.restore_code().await.unwrap();
                assert_eq!(coven_sync::read_restore_code(&code).unwrap().member, member);
                resume.send(()).unwrap();
            };
            let removal = async {
                let journaled = owner.handle.remove_member(&member).await.unwrap();
                crate::tests::landed(&owner.handle, journaled).await
            };
            tokio::time::timeout(HANG_GUARD, async { tokio::join!(removal, replacement) })
                .await
                .unwrap();
        } else {
            replace().await.unwrap();
            let code = handle.restore_code().await.unwrap();
            assert_eq!(coven_sync::read_restore_code(&code).unwrap().member, member);
            owner.sync().await;
            crate::tests::landed(&owner.handle, owner.handle.remove_member(&member).await.unwrap())
                .await;
        };
        // The person deletes the member's keys in the console (§14.3).
        assert!(owner.handle.pending().await.unwrap().is_empty());
        handle.close().await.unwrap();
        owner.close().await;
    }
}

/// A replacement whose record fails to publish keeps the new key apart:
/// custody connects with the old one until history records the new, and
/// retrying finishes it once (§14.3).
#[tokio::test]
async fn failed_access_publication_keeps_the_key_apart_and_retry_finishes_once() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let install = Installation::new();
    let handle = install.restore(&owner).await;
    let directory = install.app.store_dir(&owner.directory.id());
    let before = handle.restore_code().await.unwrap();
    let slots = || async {
        crate::tests::stored(&owner.storage, owner.directory.id(), |path| {
            path.slot_number().is_some()
        })
        .await
        .len()
    };
    let entries_before = slots().await;
    owner.storage.set_faults(Faults { fail_next: 1, ..Faults::none() }).await;
    assert!(matches!(
        handle.replace_access_key("replacement".into(), SecretText::new("secret".into())).await,
        Err(SyncError::Storage(_))
    ));
    let retained = handle.restore_code().await.unwrap();
    assert_eq!(retained, before);
    let custody = StoreKeychain::new(install.keychain.clone(), directory.id());
    let connected = || {
        let StorageCredentials::S3(key) =
            StorageCredentials::decode(custody.storage_credentials().unwrap().unwrap().as_bytes())
                .unwrap()
        else {
            panic!("S3 device key")
        };
        key.access_key_id
    };
    let original = connected();
    assert_ne!(original, "replacement");
    let kept = custody.replacing_access_key().unwrap().unwrap();
    assert_eq!(S3Credentials::decode(kept.as_bytes()).unwrap().access_key_id, "replacement");

    assert_eq!(
        keychain_code(
            &PersonEntries::new(install.keychain.clone(), owner.directory.id()),
            owner.directory.id(),
        )
        .unwrap()
        .unwrap()
        .0
        .to_text()
        .unwrap()
        .as_str(),
        retained
    );
    handle.close().await.unwrap();
    let handle = install.handle(directory.clone(), &owner).await;
    for _ in 0..2 {
        handle
            .replace_access_key("replacement".into(), SecretText::new("secret".into()))
            .await
            .unwrap();
        assert_eq!(handle.restore_code().await.unwrap(), retained);
    }
    restore_credentials::assert_key(
        &install,
        directory.id(),
        &S3Credentials {
            access_key_id: "replacement".into(),
            secret_access_key: SecretText::new("secret".into()),
        },
    );
    assert!(custody.replacing_access_key().unwrap().is_none());
    owner.sync().await;
    assert_eq!(slots().await, entries_before + 1);
    handle.close().await.unwrap();
    let disconnected = install
        .app
        .builder()
        .synced_tables(tables())
        .migrations(migrations())
        .open(directory.id(), |_| ())
        .await
        .unwrap();
    assert!(matches!(
        disconnected.replace_access_key("offline".into(), SecretText::new("secret".into())).await,
        Err(SyncError::NoStorage)
    ));
    assert_eq!(disconnected.restore_code().await.unwrap(), retained);
    disconnected.close().await.unwrap();
    owner.close().await;
}

#[tokio::test]
async fn restores_return_session_keys_and_builder_choices_without_reopening() {
    for from_keychain in [false, true] {
        for passphrase in [false, true] {
            let owner = Owner::new(CloudProvider::S3, false).await;
            let install = Installation::new();
            let (_, cancel) = watch::channel(false);
            let builder = install
                .builder(&owner, owner.storage.clone())
                .max_concurrent_uploads(NonZeroUsize::new(3).unwrap())
                .max_concurrent_downloads(NonZeroUsize::new(4).unwrap())
                .key_custody(if passphrase {
                    CustodyChoice::Passphrase(Passphrase::new("store password".into()))
                } else {
                    CustodyChoice::InMemory
                })
                .identity_custody(if passphrase {
                    CustodyChoice::Passphrase(Passphrase::new("identity password".into()))
                } else {
                    CustodyChoice::InMemory
                });
            let handle = if from_keychain {
                PersonEntries::new(install.keychain.clone(), owner.directory.id())
                    .set_restore_code(&crate::tests::keychain_entry(&owner.code))
                    .unwrap();
                restore_from_saved_code(
                    builder,
                    &crate::tests::readable(
                        crate::saved_restore::list(install.keychain.clone()).unwrap(),
                    )[0],
                    owner.s3_credentials(),
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
                    owner.s3_credentials(),
                    "Laptop",
                    |_| {},
                    &cancel,
                )
                .await
                .unwrap()
            };
            assert_eq!(handle.transfer_limits().uploads.get(), 3);
            assert_eq!(handle.transfer_limits().downloads.get(), 4);
            assert_eq!(
                handle.restore_code().await.unwrap(),
                owner.code.to_text().unwrap().as_str()
            );
            assert_eq!(handle.store_key_state().unwrap(), StoreKeyState::Available);
            handle
                .write(|sql| {
                    sql.execute("INSERT INTO notes VALUES('new',?1)", params![b"new".to_vec()])?;
                    Ok::<_, CovenError>(())
                })
                .await
                .unwrap();
            assert_eq!(install.app.stores().await.unwrap().len(), 1);
            let directory = install.app.store_dir(&owner.directory.id());
            assert!(matches!(directory.lock_exclusive(), Err(StoreLockError::AlreadyOpen(_))));
            let member = coven_sync::read_restore_code(&handle.restore_code().await.unwrap())
                .unwrap()
                .member;
            assert_eq!(member, owner.member.member());
            handle.close().await.unwrap();
            if passphrase {
                // The same files remain usable at their permanent paths after publication.
                let reopened = install
                    .builder(&owner, owner.storage.clone())
                    .key_custody(CustodyChoice::Passphrase(Passphrase::new(
                        "store password".into(),
                    )))
                    .identity_custody(CustodyChoice::Passphrase(Passphrase::new(
                        "identity password".into(),
                    )))
                    .open(directory.id(), |_| ())
                    .await
                    .unwrap();
                assert_eq!(reopened.store_key_state().unwrap(), StoreKeyState::Available);
                reopened.close().await.unwrap();
            }
            owner.close().await;
        }
    }
}

#[tokio::test]
async fn joining_retains_the_approved_member_in_session_custody() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let control = JoinControl::new();
    let builder = install
        .builder(&owner, owner.recipient())
        .key_custody(CustodyChoice::InMemory)
        .identity_custody(CustodyChoice::InMemory);
    let joining = join_with_invite(builder, invite.shown_code(), "New phone", |_| {}, &control);
    let (handle, member) = owner
        .join_and_approve(joining, &control, |_| async {
            install.absent(owner.directory.id()).await;
        })
        .await;
    let handle = handle.unwrap().unwrap();
    assert_eq!(
        coven_sync::read_restore_code(&handle.restore_code().await.unwrap()).unwrap().member,
        member
    );
    assert_eq!(handle.store_key_state().unwrap(), StoreKeyState::Available);
    handle.close().await.unwrap();
    owner.close().await;
}

#[tokio::test]
async fn missing_keychain_code_returns_none_without_creating_a_store() {
    let owner = Owner::new(CloudProvider::CloudKit, false).await;
    let install = Installation::new();
    let person = PersonEntries::new(install.keychain.clone(), owner.directory.id());
    person.set_restore_code(&crate::tests::keychain_entry(&owner.code)).unwrap();
    let saved =
        crate::tests::readable(crate::saved_restore::list(install.keychain.clone()).unwrap())
            .remove(0);
    person.delete_restore_code().unwrap();
    let (_, cancel) = watch::channel(false);
    let result =
        restore_from_saved_code(install.app.builder(), &saved, None, "Laptop", |_| {}, &cancel)
            .await
            .unwrap();
    assert!(result.is_none());
    assert!(install.app.stores().await.unwrap().is_empty());
    owner.close().await;
}

#[path = "bootstrap_authentication_tests.rs"]
// Keep the authentication tests under the same private installation fixtures.
mod authentication;

#[path = "restore_credentials_tests.rs"]
mod restore_credentials;

#[path = "bootstrap_join_tests.rs"]
mod join;

#[path = "bootstrap_admitted_tests.rs"]
mod admitted;

#[path = "bootstrap_restore_tests.rs"]
mod restore;

#[path = "bootstrap_status_tests.rs"]
mod status;

#[path = "member_keys_tests.rs"]
mod member_keys;

/// Ana's phone sets up the Household store again after her laptop, restored
/// from her code, registered in a slot the phone hasn't read: setup's last
/// step loads that slot, counted as loading counts it (§13.4).
#[tokio::test]
async fn setting_up_again_loads_what_other_devices_published() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    owner.handle.stop_sync().await.unwrap();
    let install = Installation::new();
    install.restore(&owner).await.close().await.unwrap();
    let key = owner.s3_credentials().unwrap();
    let seen = std::sync::Mutex::new(Vec::new());
    owner
        .handle
        .setup_s3_storage(
            owner.storage.config(),
            "Ana’s phone",
            key.access_key_id,
            key.secret_access_key,
            |progress| seen.lock().unwrap().push(progress),
        )
        .await
        .unwrap();
    let seen = seen.into_inner().unwrap();
    let last = seen.last().unwrap();
    assert!(
        matches!(last.step, StorageSetupStep::Loading(LoadStep::Applying))
            && last.total > 0
            && last.done == last.total,
        "{seen:?}"
    );
    owner.close().await;
}

/// Ana's account holds Household on iCloud, and Household was shared with
/// her work account too. Restoring on her laptop signed in with the work
/// account is refused before the laptop registers: no member would reach
/// storage through the account that holds the store, and the laptop's
/// registration would stand for a restore that can't finish (§10.1, §20.9).
#[tokio::test]
async fn restoring_the_owner_with_another_account_is_refused_before_registering() {
    let owner = Owner::new(CloudProvider::CloudKit, false).await;
    let coven_storage::AccessGrant::Granted { invitation } =
        owner.storage.grant_access("ana@work.example").await.unwrap()
    else {
        panic!("an account grant")
    };
    let work = Arc::new(MemoryStorage::for_recipient(&owner.storage, "ana@work.example").unwrap());
    work.join(owner.directory.id(), &invitation).await.unwrap();
    let devices = owner.handle.members().await.unwrap()[0].devices.len();
    let install = Installation::new();
    let (_, cancel) = watch::channel(false);
    let result = restore_from_code(
        install.authenticated_builder(&owner, work).await,
        &owner.code.to_text().unwrap(),
        None,
        "Ana’s laptop",
        |_| {},
        &cancel,
    )
    .await;
    assert!(matches!(result, Err(BootstrapError::Sync(SyncError::StoreOwner))), "{result:?}");
    install.absent(owner.directory.id()).await;
    owner.sync().await;
    assert_eq!(owner.handle.members().await.unwrap()[0].devices.len(), devices);
    owner.close().await;
}

/// Ana restores Household on S3 with a key she made for her laptop. As
/// setup does, the restore records the key she now reaches storage with, so
/// her access names the key in use (§14.3, §20.2).
#[tokio::test]
async fn restoring_with_another_key_records_it() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let install = Installation::new();
    let (_, cancel) = watch::channel(false);
    let handle = restore_from_code(
        install.authenticated_builder(&owner, owner.storage.clone()).await,
        &owner.code.to_text().unwrap(),
        Some(S3Credentials {
            access_key_id: "laptop-key".into(),
            secret_access_key: SecretText::new("laptop secret".into()),
        }),
        "Ana’s laptop",
        |_| {},
        &cancel,
    )
    .await
    .unwrap();
    let members = handle.members().await.unwrap();
    assert_eq!(
        members.into_iter().find(|member| member.is_self).unwrap().access,
        MemberAccess::S3AccessKey { access_key_id: "laptop-key".into() }
    );
    handle.close().await.unwrap();
    owner.close().await;
}

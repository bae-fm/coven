use super::*;
use coven_sync::JoiningIdentity;

#[tokio::test]
async fn joining_refuses_an_invite_for_another_store_before_keeping_an_identity() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let pending = install
        .layout
        .begin_bootstrap(owner.directory.id(), &UuidIds)
        .await
        .unwrap();
    let directory = pending.directory();
    let mut code = coven_sync::read_invite_code(&invite.code).unwrap();
    code.store = StoreId(UuidIds.new_id());
    assert!(matches!(
        JoiningIdentity::prepare(&directory, &code, "New phone"),
        Err(SyncError::WrongStore { expected, actual })
            if expected == directory.id() && actual == code.store
    ));
    assert!(directory
        .owned_file(StoreFile::Bootstrap)
        .read_optional()
        .unwrap()
        .is_none());
    install.absent(owner.directory.id()).await;
    pending.cancel().unwrap();
    owner.close().await;
}

#[tokio::test]
async fn resuming_join_authenticates_the_initial_key_and_writer_cursor() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let pending = install
        .layout
        .begin_bootstrap(owner.directory.id(), &UuidIds)
        .await
        .unwrap();
    let directory = pending.directory();
    let code = coven_sync::read_invite_code(&invite.code).unwrap();
    let mut identity = JoiningIdentity::prepare(&directory, &code, "New phone").unwrap();
    let file = directory.owned_file(StoreFile::Bootstrap);
    identity.record_attempt(&file, &code).unwrap();
    let saved = file.read_optional().unwrap().unwrap();
    // A live directory clone keeps the bootstrap lease, and the join below
    // would wait for it. The file handle names only the path.
    drop(directory);
    drop(pending);
    let control = JoinControl::new();
    for field in ["initial_key", "inviting_writer", "read_through"] {
        let mut changed = coven_sync::read_invite_code(&invite.code).unwrap();
        match field {
            "initial_key" => changed.initial_key = KeyId(UuidIds.new_id()),
            "inviting_writer" => changed.inviting_writer.0 ^= 1,
            "read_through" => changed.read_through += 1,
            _ => unreachable!(),
        }
        let result = join_with_invite(
            install.builder(&owner, owner.recipient()),
            &changed.to_text().unwrap(),
            "New phone",
            |_| {},
            &control,
        )
        .await;
        assert!(
            matches!(result, Err(BootstrapError::Sync(SyncError::Crypto(_)))),
            "{field}: {result:?}"
        );
        assert_eq!(file.read_optional().unwrap().unwrap(), saved);
        install.absent(owner.directory.id()).await;
    }
    let pending = install
        .layout
        .begin_bootstrap(owner.directory.id(), &UuidIds)
        .await
        .unwrap();
    let directory = pending.directory();
    let resumed = JoiningIdentity::prepare(&directory, &code, "New phone").unwrap();
    assert!(resumed.attempted());
    assert_eq!(
        resumed.identity().keys().public_keys(),
        identity.identity().keys().public_keys()
    );
    // The resumed identity keeps the id the invite fixed.
    assert_eq!(resumed.identity().member(), code.member);
    assert_eq!(file.read_optional().unwrap().unwrap(), saved);
    drop(directory);
    pending.cancel().unwrap();
    owner.close().await;
}

/// Carol joins, then loses her only installation. Ana's recovery invite
/// fixes Carol's id; Carol's new installation joins with fresh keys and
/// keeps her id, role and old device id. The binding records no access: the
/// invite's new S3 key is recorded for Carol by its own entry, which leaves
/// her old key pending deletion. Carol's old installation and her old
/// restore code report the replacement.
#[tokio::test]
async fn a_recovery_invite_rebinds_a_lost_member_through_the_public_calls() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let first = Installation::new();
    let (_, cancel) = watch::channel(false);
    let joining = first.run(
        &owner,
        Installation::join_request(&invite),
        owner.recipient(),
        &cancel,
        |_| {},
    );
    let (result, carol) = join_and_approve(&owner.handle, joining, |_| async {}).await;
    let lost = result.unwrap().unwrap();
    let old_code = lost.restore_code().await.unwrap();
    let recovery = owner
        .handle
        .create_invite(
            InviteTarget::ReplaceMemberKeys { member: carol },
            InviteAccess::S3AccessKey {
                access_key_id: "recovery-key".into(),
                secret_access_key: SecretText::new("recovery-secret".into()),
            },
            crate::InviteDelivery::Sent {
                lifetime: crate::InviteLifetime::Day,
            },
        )
        .await
        .unwrap();
    assert_eq!(recovery.member, carol);
    let second = Installation::new();
    let joining = second.run(
        &owner,
        Installation::join_request(&recovery),
        owner.recipient(),
        &cancel,
        |_| {},
    );
    let (result, member) = join_and_approve(&owner.handle, joining, |_| async {}).await;
    assert_eq!(member, carol);
    let handle = result.unwrap().unwrap();
    let code = coven_sync::read_restore_code(&handle.restore_code().await.unwrap()).unwrap();
    let old = coven_sync::read_restore_code(&old_code).unwrap();
    assert_eq!((code.member, old.member), (carol, carol));
    assert_ne!(
        code.member_keys.public_keys(),
        old.member_keys.public_keys()
    );
    owner.sync().await;
    let members = owner.handle.members().await.unwrap();
    let info = members.iter().find(|m| m.id == carol).unwrap();
    assert_eq!(info.keys, code.member_keys.public_keys());
    assert_eq!(info.role, MemberRole::Member);
    assert_eq!(info.devices.len(), 2);
    assert!(info.devices.iter().all(|d| d.state == DeviceState::Active));
    assert!(owner.handle.pending().await.unwrap().is_empty());
    let stopped = lost.circles().create("Garden").await;
    assert!(
        matches!(&stopped, Err(SyncError::MemberKeysReplaced { member }) if *member == carol),
        "{stopped:?}"
    );
    lost.close().await.unwrap();
    // The old code cannot bind its pair back to Carol's id.
    let stale = Installation::new();
    let restored = restore_from_code(
        stale.builder(&owner, owner.storage.clone()),
        &old_code,
        Some(S3Credentials {
            access_key_id: "recovery-key".into(),
            secret_access_key: SecretText::new("recovery-secret".into()),
        }),
        "Carol's old code",
        |_| {},
        &cancel,
    )
    .await;
    assert!(
        matches!(&restored, Err(BootstrapError::Sync(SyncError::MemberKeysReplaced { member })) if *member == carol),
        "{:?}",
        restored.as_ref().err()
    );
    stale.absent(owner.directory.id()).await;
    handle.close().await.unwrap();
    owner.close().await;
}

/// The provider refuses the first join request outright, so it stored
/// nothing: the retry sends it again and is approved, instead of reading the
/// absent request as a declined invite (§13.2).
#[tokio::test]
async fn a_refused_join_request_is_sent_again() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let path = coven_storage::ObjectPath::join_request(owner.directory.id(), invite.id);
    owner
        .storage
        .set_faults(coven_storage::test_utils::Faults {
            create_failures: [(path, (StorageFailure::PermissionDenied, 403))].into(),
            ..coven_storage::test_utils::Faults::none()
        })
        .await;
    let install = Installation::new();
    let (_, cancel) = watch::channel(false);
    let refused = install
        .run(
            &owner,
            Installation::join_request(&invite),
            owner.recipient(),
            &cancel,
            |_| {},
        )
        .await;
    assert!(
        matches!(&refused, Err(BootstrapError::Sync(SyncError::Storage(error)))
            if error.failure() == StorageFailure::PermissionDenied),
        "{:?}",
        refused.as_ref().err()
    );
    owner
        .storage
        .set_faults(coven_storage::test_utils::Faults::none())
        .await;
    let joining = install.run(
        &owner,
        Installation::join_request(&invite),
        owner.recipient(),
        &cancel,
        |_| {},
    );
    let (result, _) = join_and_approve(&owner.handle, joining, |_| async {}).await;
    result.unwrap().unwrap().close().await.unwrap();
    owner.close().await;
}

/// A join reports opening its new installation, then connecting, then
/// waiting for approval, then loading (E10). While it waits, a sent invite's
/// status carries the number Ana's phone shows with the request, and when the
/// next check runs.
#[tokio::test]
async fn a_join_reports_each_bootstrap_phase_in_order() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let (_, cancel) = watch::channel(false);
    let reported = std::sync::Mutex::new(Vec::new());
    let joining = install.run(
        &owner,
        Installation::join_request(&invite),
        owner.recipient(),
        &cancel,
        |status| {
            let mut reported = reported.lock().unwrap();
            if reported.last() != Some(&status) {
                reported.push(status);
            }
        },
    );
    let approved = std::sync::Mutex::new(None);
    let (result, _) = join_and_approve(&owner.handle, joining, |request| {
        *approved.lock().unwrap() = Some(request);
        async {}
    })
    .await;
    result.unwrap().unwrap().close().await.unwrap();
    let reported = reported.into_inner().unwrap();
    let request = approved.into_inner().unwrap().unwrap();
    let reported = after_opening(&reported);
    assert_eq!(reported.first(), Some(&BootstrapStatus::Connecting));
    assert_eq!(reported.last(), Some(&BootstrapStatus::Loading));
    let waits = &reported[1..reported.len() - 1];
    assert!(waits.len() >= 2, "{reported:?}");
    for (index, status) in waits.iter().enumerate() {
        let BootstrapStatus::WaitingForApproval {
            confirmation,
            next_check,
        } = status
        else {
            panic!("{status:?} while waiting");
        };
        assert_eq!(*confirmation, Some(request.confirmation));
        // The first check runs at once; then checks and waits alternate.
        assert_eq!(
            matches!(next_check, NextCheck::Checking),
            index % 2 == 0,
            "{reported:?}"
        );
    }
    owner.close().await;
}

#[tokio::test]
async fn snapshots_restore_the_name_and_invites_keep_the_name_at_creation() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let old_code = owner.code.to_text().unwrap();
    owner.handle.rename_store("Snapshot name").await.unwrap();
    let invite = owner.invite().await;
    let code = coven_sync::read_invite_code(&invite.code).unwrap();
    assert_eq!(code.name, "Snapshot name");
    assert_eq!(
        invite.expires_at.duration_since(owner.clock.now()).unwrap(),
        Duration::from_secs(24 * 3600)
    );
    owner.handle.reset_store().await.unwrap();
    owner.handle.reload_from_snapshot().await.unwrap();
    assert_eq!(owner.handle.store_name().await.unwrap(), "Snapshot name");
    owner.handle.rename_store("After snapshot").await.unwrap();
    owner.handle.reload_from_snapshot().await.unwrap();
    assert_eq!(owner.handle.store_name().await.unwrap(), "After snapshot");
    assert_eq!(
        owner.handle.restore_code().await.unwrap(),
        old_code.as_str()
    );

    let install = Installation::new();
    let restored = install.restore(&owner).await;
    assert_eq!(restored.store_name().await.unwrap(), "After snapshot");
    let saved = install.keychain.person_restore_codes().unwrap();
    let [(_, coven_crypto::custody::PersonRestoreCode::Saved(saved))] = &saved[..] else {
        panic!("expected one saved code, got {saved:?}");
    };
    let saved = coven_format::codes::KeychainRestoreCode::from_bytes(saved.as_bytes()).unwrap();
    // The saved code takes the name loading recovered from the snapshot.
    assert_eq!(saved.name, "After snapshot");
    assert_eq!(saved.code.to_text().unwrap().as_str(), old_code.as_str());
    restored.close().await.unwrap();
    let reopened = install
        .builder(&owner, owner.storage.clone())
        .open(owner.directory.id(), |_| ())
        .await
        .unwrap();
    assert_eq!(reopened.store_name().await.unwrap(), "After snapshot");
    reopened.close().await.unwrap();
    assert_eq!(
        install.layout.stores().await.unwrap()[0].name,
        "After snapshot"
    );

    // An unexpired invite's captured label cannot become the joined store's name.
    let joined = Installation::new();
    let control = JoinControl::new();
    let joining = join_with_invite(
        joined.builder(&owner, owner.recipient()),
        &invite.code,
        "Guest phone",
        |_| {},
        &control,
    );
    let (handle, _) = join_and_approve(&owner.handle, joining, |_| async {}).await;
    let handle = handle.unwrap().unwrap();
    assert_eq!(handle.store_name().await.unwrap(), "After snapshot");
    handle.close().await.unwrap();
    let current = owner.invite().await;
    assert_eq!(
        coven_sync::read_invite_code(&current.code).unwrap().name,
        "After snapshot"
    );
    owner.handle.cancel_invite(&current.id).await.unwrap();
    owner.close().await;
}

#[tokio::test]
async fn a_restore_learns_the_renamed_name_from_the_floor_snapshot() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    owner.handle.rename_store("Retained name").await.unwrap();
    owner
        .handle
        .write(|sql| {
            sql.execute(
                "INSERT INTO notes VALUES('snapshot',?1)",
                coven_database::params![vec![9_u8; 1_100_000]],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    owner.sync().await;
    owner
        .clock
        .set(owner.clock.now() + Duration::from_secs(31 * 24 * 3600));
    owner.sync().await;
    owner.sync().await;
    assert!(owner
        .storage
        .status(&ObjectPath::slot(
            owner.directory.id(),
            1.try_into().unwrap()
        ))
        .await
        .unwrap()
        .is_none());
    let install = Installation::new();
    let handle = install.restore(&owner).await;
    assert_eq!(handle.store_name().await.unwrap(), "Retained name");
    handle.reload_from_snapshot().await.unwrap();
    assert_eq!(handle.store_name().await.unwrap(), "Retained name");
    handle.close().await.unwrap();
    owner.close().await;
}

/// Ana's own sync pass approves an in-person invite's request: Carol's join
/// loads the store with no call from Ana's app (§13.2).
#[tokio::test]
async fn an_in_person_join_loads_without_an_approval_call() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner
        .handle
        .create_invite(
            InviteTarget::NewMember {
                role: MemberRole::Member,
                name: "Carol".into(),
            },
            InviteAccess::S3AccessKey {
                access_key_id: "invited-key".into(),
                secret_access_key: SecretText::new("invited-secret".into()),
            },
            InviteDelivery::InPerson,
        )
        .await
        .unwrap();
    let install = Installation::new();
    let (_, cancel) = watch::channel(false);
    let joining = install.run(
        &owner,
        Installation::join_request(&invite),
        owner.recipient(),
        &cancel,
        |_| {},
    );
    let handle = tokio::time::timeout(Duration::from_secs(20), joining)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(handle
        .members()
        .await
        .unwrap()
        .iter()
        .any(|member| member.is_self && member.id == invite.member));
    assert!(owner.handle.subscribe_invites().borrow().is_empty());
    // The finished join no longer keeps its code.
    assert!(saved_code(&install, owner.directory.id()).is_none());
    handle.close().await.unwrap();
    owner.close().await;
}

/// Carol is on a call with Ana as Ana approves: Carol's Check now runs the
/// next check at once, instead of after the join's wait (§13.2).
#[tokio::test]
async fn check_now_finds_an_approval_before_the_joins_wait_ends() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    // The joining device's waits end only when the test moves the clock.
    let builder = install
        .builder(&owner, owner.recipient())
        .clock(owner.clock.clone());
    let control = JoinControl::new();
    let (waits, mut waiting) = watch::channel(false);
    let joining = join_with_invite(
        builder,
        &invite.code,
        "New phone",
        |status| {
            if matches!(
                status,
                BootstrapStatus::WaitingForApproval {
                    next_check: NextCheck::At(_),
                    ..
                }
            ) {
                waits.send_replace(true);
            }
        },
        &control,
    );
    let approve = async {
        waiting.wait_for(|waiting| *waiting).await.unwrap();
        let request = next_join_request(&owner.handle).await;
        owner.handle.approve_join_request(&request).await.unwrap();
        control.check_now();
    };
    let (result, ()) = join_with_response(joining, approve).await;
    result.unwrap().unwrap().close().await.unwrap();
    owner.close().await;
}

/// Carol's join is approved, then stops while it loads, as when the app is
/// closed. Her installation had applied the entry adding her; the next call
/// finds that approval and loads, instead of taking the deleted request for a
/// decline (§13.2).
#[tokio::test]
async fn an_approved_join_stopped_while_loading_loads_on_the_next_call() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let (_, cancel) = watch::channel(false);
    let (loading, mut loaded) = watch::channel(false);
    let joining = install.run(
        &owner,
        Installation::join_request(&invite),
        owner.recipient(),
        &cancel,
        |status| {
            if status == BootstrapStatus::Loading {
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
    let approve = async {
        let request = next_join_request(&owner.handle).await;
        owner.handle.approve_join_request(&request).await.unwrap();
        request.member
    };
    let ((), member) = join_with_response(stopped, approve).await;
    let handle = install
        .run(
            &owner,
            Installation::join_request(&invite),
            owner.recipient(),
            &cancel,
            |_| {},
        )
        .await
        .unwrap()
        .expect("the approved join loads");
    assert!(handle
        .members()
        .await
        .unwrap()
        .iter()
        .any(|found| found.is_self && found.id == member));
    handle.close().await.unwrap();
    owner.close().await;
}

/// Once the request may have been sent, cancelling can't take it back: the
/// join ends with `JoinRequestSent`, keeps its installation and request, and
/// a later call with the same code continues it to approval (§13.2).
#[tokio::test]
async fn cancelling_a_sent_join_keeps_it_for_a_later_call() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let (stop, cancel) = watch::channel(false);
    let joining = install.run(
        &owner,
        Installation::join_request(&invite),
        owner.recipient(),
        &cancel,
        |_| {},
    );
    let cancel_join = async {
        let request = next_join_request(&owner.handle).await;
        stop.send(true).unwrap();
        request
    };
    let (result, request) = join_with_response(joining, cancel_join).await;
    assert!(
        matches!(result, Err(BootstrapError::JoinRequestSent)),
        "{result:?}"
    );
    let directory = install
        .root
        .path()
        .join("stores")
        .join(owner.directory.id().to_string());
    assert!(directory.exists());
    assert!(owner
        .storage
        .status(&ObjectPath::join_request(owner.directory.id(), invite.id))
        .await
        .unwrap()
        .is_some());
    let builder = install.builder(&owner, owner.recipient());
    assert_eq!(
        builder
            .unfinished_joins()
            .await
            .unwrap()
            .iter()
            .map(|join| join.metadata.store_id)
            .collect::<Vec<_>>(),
        [owner.directory.id()]
    );
    let control = JoinControl::new();
    let resumed = builder.resume_join(owner.directory.id(), |_| {}, &control);
    let approve = owner.handle.approve_join_request(&request);
    let (result, approved) = join_with_response(resumed, approve).await;
    approved.unwrap();
    result.unwrap().unwrap().close().await.unwrap();
    owner.close().await;
}

/// Before the request may have been sent, cancelling removes everything, as
/// for a restore, a Dropbox join's kept sign-in included (§13.2).
#[tokio::test]
async fn cancelling_a_join_before_its_request_is_sent_removes_every_local_value() {
    for provider in [CloudProvider::S3, CloudProvider::Dropbox] {
        cancel_before_sending(provider).await;
    }
}

async fn cancel_before_sending(provider: CloudProvider) {
    let owner = Owner::new(provider, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let (stop, cancel) = watch::channel(false);
    let result = install
        .run(
            &owner,
            Installation::join_request(&invite),
            owner.recipient(),
            &cancel,
            cancel_at(&(BootstrapStatus::Connecting, 1), &stop),
        )
        .await;
    assert!(
        matches!(result, Err(BootstrapError::Cancelled)),
        "{result:?}"
    );
    install.absent(owner.directory.id()).await;
    assert!(saved_code(&install, owner.directory.id()).is_none());
    assert!(!install
        .root
        .path()
        .join("stores")
        .join(owner.directory.id().to_string())
        .exists());
    assert!(owner
        .storage
        .status(&ObjectPath::join_request(owner.directory.id(), invite.id))
        .await
        .unwrap()
        .is_none());
    owner.close().await;
}

/// An approved join cancelled while it loads keeps its installation too: the
/// entry adding Carol has landed, and only this installation holds her keys.
#[tokio::test]
async fn cancelling_an_approved_join_while_it_loads_keeps_it() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let (stop, cancel) = watch::channel(false);
    let loading = (BootstrapStatus::Loading, 1);
    let joining = install.run(
        &owner,
        Installation::join_request(&invite),
        owner.recipient(),
        &cancel,
        cancel_at(&loading, &stop),
    );
    let (result, member) = join_and_approve(&owner.handle, joining, |_| async {}).await;
    assert!(
        matches!(result, Err(BootstrapError::JoinRequestSent)),
        "{result:?}"
    );
    assert!(install.layout.stores().await.unwrap().is_empty());
    let (_, cancel) = watch::channel(false);
    let handle = install
        .run(
            &owner,
            Installation::join_request(&invite),
            owner.recipient(),
            &cancel,
            |_| {},
        )
        .await
        .unwrap()
        .unwrap();
    assert!(handle
        .members()
        .await
        .unwrap()
        .iter()
        .any(|found| found.is_self && found.id == member));
    handle.close().await.unwrap();
    owner.close().await;
}

/// Carol closes the app while she waits. Her phone lists the join with the
/// names the invite carried, and continues it without the code; once it
/// loads, the saved code is gone (§13.2).
#[tokio::test]
async fn a_closed_join_is_listed_and_resumes_without_its_code() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    owner.handle.rename_store("Household").await.unwrap();
    let invite = owner.invite().await;
    let install = Installation::new();
    let store = owner.directory.id();
    waiting_join(&owner, &install, &invite).await;
    let builder = install.builder(&owner, owner.recipient());
    let CodeInfo::Invite(info) = decode_code_info(&invite.code).unwrap() else {
        panic!("an invite code");
    };
    assert_eq!(
        (info.store_name.as_str(), info.invitee.as_deref()),
        ("Household", Some("Carol"))
    );
    assert_eq!(builder.unfinished_joins().await.unwrap(), [info]);
    assert_eq!(saved_code(&install, store).unwrap().as_str(), invite.code);
    let control = JoinControl::new();
    let resumed = builder.resume_join(store, |_| {}, &control);
    let (result, _) = join_and_approve(&owner.handle, resumed, |request| async move {
        // The resumed request is the one Carol's phone made, with its name.
        assert_eq!(request.device_name, "Carol's phone");
    })
    .await;
    let handle = result.unwrap().unwrap();
    assert!(saved_code(&install, store).is_none());
    let builder = install.builder(&owner, owner.recipient());
    assert!(builder.unfinished_joins().await.unwrap().is_empty());
    assert!(matches!(
        builder.resume_join(store, |_| {}, &control).await,
        Err(BootstrapError::NoUnfinishedJoin(missing)) if missing == store
    ));
    handle.close().await.unwrap();
    owner.close().await;
}

/// A closed join that Ana declines resumes to `None`, and its saved code and
/// installation are gone (§13.2).
#[tokio::test]
async fn a_closed_join_declined_meanwhile_resumes_to_none_and_forgets_its_code() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let store = owner.directory.id();
    waiting_join(&owner, &install, &invite).await;
    let request = next_join_request(&owner.handle).await;
    owner.handle.decline_join_request(&request).await.unwrap();
    let control = JoinControl::new();
    let result = install
        .builder(&owner, owner.recipient())
        .resume_join(store, |_| {}, &control)
        .await;
    assert!(matches!(result, Ok(None)), "{result:?}");
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

/// A saved code coven can't read fails the listing: a join that may have
/// sent its request is never silently left out (§13.2).
#[tokio::test]
async fn an_unreadable_saved_code_fails_the_listing() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let store = owner.directory.id();
    waiting_join(&owner, &install, &invite).await;
    StoreKeychain::new(install.keychain.clone(), store)
        .set_invite_code(&SecretText::new("CVI1-NOTACODE".into()))
        .unwrap();
    assert!(matches!(
        install
            .builder(&owner, owner.recipient())
            .unfinished_joins()
            .await,
        Err(BootstrapError::Code(CodeError::Invalid))
    ));
    owner.handle.cancel_invite(&invite.id).await.unwrap();
    owner.close().await;
}

/// Ending without a store also removes a Dropbox join's kept sign-in, with its
/// code (§13.2).
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
        let joining = install.run(
            &owner,
            Installation::join_request(&invite),
            owner.recipient(),
            &cancel,
            |_| {},
        );
        let settle = async {
            let request = next_join_request(&owner.handle).await;
            match ending {
                "decline" => owner.handle.decline_join_request(&request).await.unwrap(),
                "cancel" => owner.handle.cancel_invite(&invite.id).await.unwrap(),
                _ => {
                    owner.clock.set(invite.expires_at);
                    if provider == CloudProvider::S3 {
                        owner.sync().await;
                    } else {
                        // Dropbox takes back the access afterwards, so the
                        // pass ends with that provider change still pending.
                        owner.handle.sync_now();
                    }
                }
            }
        };
        let (result, ()) = join_with_response(joining, settle).await;
        assert!(result.unwrap().is_none(), "{provider:?} {ending}");
        install.absent(owner.directory.id()).await;
        assert!(
            StoreKeychain::new(install.keychain.clone(), owner.directory.id())
                .invite_code()
                .unwrap()
                .is_none()
        );
        assert!(!install
            .root
            .path()
            .join("stores")
            .join(owner.directory.id().to_string())
            .exists());
        owner.close().await;
    }
}

/// The revocation of a removed member's account that left a grant in place:
/// its operation, the account and the grants it left.
fn retained_grant(
    records: &[PendingRecord],
) -> Option<(OperationId, String, Vec<coven_storage::RetainedAccess>)> {
    records
        .iter()
        .find_map(|record| match (&record.subject, &record.reason) {
            (
                PendingSubject::Operation { id, .. },
                PendingReason::AccessRemains { account, shares },
            ) => Some((*id, account.clone(), shares.clone())),
            _ => None,
        })
}

/// Removing a member whose account keeps a grant the owner's device can't
/// take back, here one inherited from a parent folder, leaves the revocation
/// pending as `AccessRemains` with the account and the grant. Later passes
/// keep it listed, and once the owner has removed the grant at the provider,
/// a retry finishes the revocation (§14.2, §20.9).
#[tokio::test]
async fn a_removed_members_retained_grant_stays_pending_until_a_retry_revokes_it() {
    let owner = Owner::new(CloudProvider::OneDrive, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let (_, cancel) = watch::channel(false);
    let joining = install.run(
        &owner,
        Installation::join_request(&invite),
        owner.recipient(),
        &cancel,
        |_| {},
    );
    let (result, member) = join_and_approve(&owner.handle, joining, |_| async {}).await;
    result.unwrap().unwrap().close().await.unwrap();
    let shares = vec![coven_storage::RetainedAccess {
        provider_id: "parent-folder".into(),
        reason: coven_storage::RetainedAccessReason::Inherited,
    }];
    owner
        .storage
        .set_retained_access("join@example.com", shares.clone())
        .await;

    owner.handle.remove_member(&member).await.unwrap();
    owner.pass().await;
    let (operation, account, retained) =
        retained_grant(&owner.handle.pending().await.unwrap()).expect("the grant is reported");
    assert_eq!(account, "join@example.com");
    assert_eq!(retained, shares);
    owner.pass().await;
    assert_eq!(
        retained_grant(&owner.handle.pending().await.unwrap()),
        Some((operation, account, retained))
    );

    owner
        .storage
        .set_retained_access("join@example.com", Vec::new())
        .await;
    owner
        .handle
        .retry_pending_operation(operation)
        .await
        .unwrap();
    // `sync` checks the pass leaves no operation pending.
    owner.sync().await;
    owner.close().await;
}

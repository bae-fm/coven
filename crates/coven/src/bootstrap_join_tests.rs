use super::*;
use coven_sync::JoiningIdentity;

#[tokio::test]
async fn joining_refuses_an_invite_for_another_store_before_keeping_an_identity() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let pending =
        install.app.directories().begin_bootstrap(owner.directory.id(), &UuidIds).await.unwrap();
    let directory = pending.directory();
    let mut code = coven_sync::read_invite_code(invite.shown_code()).unwrap();
    code.store = StoreId(UuidIds.new_id());
    let device = directory.settings().unwrap().device_id;
    assert!(matches!(
        JoiningIdentity::prepare(&directory, &code, device, "New phone", carol_joins(invite.shown_code())),
        Err(SyncError::Code(CodeError::WrongStore { expected, actual }))
            if expected == directory.id() && actual == code.store
    ));
    assert!(directory.owned_file(StoreFile::Bootstrap).read_optional().unwrap().is_none());
    // Through bootstrap, it is refused as the join's code, as a restore's is.
    let refused = JoiningIdentity::prepare(
        &directory,
        &code,
        device,
        "New phone",
        carol_joins(invite.shown_code()),
    )
    .map_err(BootstrapError::from);
    assert!(matches!(refused, Err(BootstrapError::Code(CodeError::WrongStore { .. }))));
    install.absent(owner.directory.id()).await;
    pending.cancel().unwrap();
    owner.close().await;
}

#[tokio::test]
async fn resuming_join_authenticates_the_initial_key_and_writer_cursor() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let pending =
        install.app.directories().begin_bootstrap(owner.directory.id(), &UuidIds).await.unwrap();
    let directory = pending.directory();
    let code = coven_sync::read_invite_code(invite.shown_code()).unwrap();
    let mut identity = JoiningIdentity::prepare(
        &directory,
        &code,
        directory.settings().unwrap().device_id,
        "New phone",
        carol_joins(invite.shown_code()),
    )
    .unwrap();
    let file = directory.owned_file(StoreFile::Bootstrap);
    identity.record_attempt().unwrap();
    // As the join saves it before it sends: the code lists the installation.
    StoreKeychain::new(install.keychain.clone(), owner.directory.id())
        .set_invite_code(&SecretText::new(invite.shown_code().to_string()))
        .unwrap();
    let saved = file.read_optional().unwrap().unwrap();
    // A live directory clone keeps the bootstrap lease, and the join below
    // would wait for it. The file handle names only the path.
    drop(directory);
    drop(pending);
    let control = JoinControl::new();
    for field in ["initial_key", "inviting_writer", "read_through"] {
        let mut changed = coven_sync::read_invite_code(invite.shown_code()).unwrap();
        match field {
            "initial_key" => changed.initial_key = KeyId(UuidIds.new_id()),
            "inviting_writer" => changed.inviting_writer.0 ^= 1,
            "read_through" => changed.read_through += 1,
            _ => unreachable!(),
        }
        let result = join_with_invite(
            install.builder(&owner, owner.recipient()),
            &changed.to_text().unwrap(),
            carol_joins(invite.shown_code()),
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
    let pending =
        install.app.directories().begin_bootstrap(owner.directory.id(), &UuidIds).await.unwrap();
    let directory = pending.directory();
    let resumed = JoiningIdentity::prepare(
        &directory,
        &code,
        directory.settings().unwrap().device_id,
        "New phone",
        carol_joins(invite.shown_code()),
    )
    .unwrap();
    assert!(resumed.attempted());
    // The saved request gives the person's name back, and another name or a
    // recovery in its place is a changed join.
    assert_eq!(resumed.joiner(), &Joiner::NewPerson { name: "Carol".into() });
    for other in [Joiner::NewPerson { name: "Caroline".into() }, Joiner::Recovery] {
        let device = directory.settings().unwrap().device_id;
        let changed = JoiningIdentity::prepare(&directory, &code, device, "New phone", other);
        assert!(matches!(
            changed,
            Err(SyncError::InvitationChanged | SyncError::Code(CodeError::WrongJoiner { .. }))
        ));
    }
    assert_eq!(resumed.identity().keys().public_keys(), identity.identity().keys().public_keys());
    // The resumed identity keeps the id the invite fixed.
    assert_eq!(resumed.identity().member(), code.member);
    assert_eq!(file.read_optional().unwrap().unwrap(), saved);
    drop(directory);
    pending.cancel().unwrap();
    owner.close().await;
}

/// Carol joins, then loses her only installation. Ana's recovery invite
/// fixes Carol's id; Carol's new installation joins with fresh keys and
/// keeps her id, role and old device id. The admission that recovers her
/// also records the invite's new S3 key as her access, which leaves her old
/// key pending deletion. Carol's old installation and her old restore code
/// report the replacement.
#[tokio::test]
async fn a_recovery_invite_rebinds_a_lost_member_through_the_public_calls() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let first = Installation::new();
    let (_, cancel) = watch::channel(false);
    let control = JoinControl::new();
    let joining = first.run(
        &owner,
        Installation::join_request(&invite, &control),
        owner.recipient(),
        &cancel,
        |_| {},
    );
    let (result, carol) = owner.join_and_approve(joining, &control, |_| async {}).await;
    let lost = result.unwrap().unwrap();
    let old_code = lost.restore_code().await.unwrap();
    let recovery = crate::tests::invite(
        &owner.handle,
        InviteTarget::ReplaceMemberKeys { member: carol },
        InviteAccess::S3AccessKey {
            access_key_id: "recovery-key".into(),
            secret_access_key: SecretText::new("recovery-secret".into()),
        },
        crate::InviteLifetime::SevenDays,
    )
    .await;
    assert_eq!(recovery.member, carol);
    let second = Installation::new();
    let control = JoinControl::new();
    let joining = second.run(
        &owner,
        Installation::join_request(&recovery, &control),
        owner.recipient(),
        &cancel,
        |_| {},
    );
    let (result, member) = owner.join_and_approve(joining, &control, |_| async {}).await;
    assert_eq!(member, carol);
    let handle = result.unwrap().unwrap();
    let code = coven_sync::read_restore_code(&handle.restore_code().await.unwrap()).unwrap();
    let old = coven_sync::read_restore_code(&old_code).unwrap();
    assert_eq!((code.member, old.member), (carol, carol));
    assert_ne!(code.member_keys.public_keys(), old.member_keys.public_keys());
    owner.sync().await;
    let members = owner.handle.members().await.unwrap();
    let info = members.iter().find(|m| m.id == carol).unwrap();
    assert_eq!(info.keys, code.member_keys.public_keys());
    assert_eq!(info.role, MemberRole::Member);
    assert_eq!(
        info.access,
        Some(coven_format::MemberAccess::S3AccessKey { access_key_id: "recovery-key".into() })
    );
    assert_eq!(info.devices.len(), 2);
    assert!(info.devices.iter().all(|d| d.state == DeviceState::Active));
    assert!(owner.handle.pending().await.unwrap().is_empty());
    // Its pass applies the replacement and stops; its admin writes refuse
    // from then on.
    lost.start_sync().await.unwrap();
    lost.test_last_pass().wait_for(|last| matches!(last, Some(LastPass::Blocked { .. }))).await;
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
    let path = crate::tests::request_path(&invite);
    owner
        .storage
        .set_faults(coven_storage::test_utils::Faults {
            create_failures: [(path, (StorageFailure::PermissionDenied, 403))].into(),
            ..coven_storage::test_utils::Faults::none()
        })
        .await;
    let install = Installation::new();
    let (_, cancel) = watch::channel(false);
    let control = JoinControl::new();
    let refused = install
        .run(
            &owner,
            Installation::join_request(&invite, &control),
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
    owner.storage.set_faults(coven_storage::test_utils::Faults::none()).await;
    let joining = install.run(
        &owner,
        Installation::join_request(&invite, &control),
        owner.recipient(),
        &cancel,
        |_| {},
    );
    let (result, _) = owner.join_and_approve(joining, &control, |_| async {}).await;
    result.unwrap().unwrap().close().await.unwrap();
    owner.close().await;
}

#[tokio::test]
async fn snapshots_restore_the_name_and_invites_keep_the_name_at_creation() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let old_code = owner.code.to_text().unwrap();
    crate::tests::landed(&owner.handle, owner.handle.rename_store("Snapshot name").await.unwrap())
        .await;
    let invite = owner.invite().await;
    let code = coven_sync::read_invite_code(invite.shown_code()).unwrap();
    assert_eq!(code.name, "Snapshot name");
    assert_eq!(
        invite.expires_at.duration_since(owner.clock.now()).unwrap(),
        Duration::from_secs(7 * 86_400)
    );
    owner.handle.reset_store().await.unwrap();
    owner.handle.reload_from_snapshot().await.unwrap();
    assert_eq!(owner.handle.store_name().await.unwrap(), "Snapshot name");
    crate::tests::landed(&owner.handle, owner.handle.rename_store("After snapshot").await.unwrap())
        .await;
    owner.handle.reload_from_snapshot().await.unwrap();
    assert_eq!(owner.handle.store_name().await.unwrap(), "After snapshot");
    assert_eq!(owner.handle.restore_code().await.unwrap(), old_code.as_str());

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
    assert_eq!(install.app.readable_stores().await[0].name, "After snapshot");

    // An unexpired invite's captured label cannot become the joined store's name.
    let joined = Installation::new();
    let control = JoinControl::new();
    let joining = join_with_invite(
        joined.builder(&owner, owner.recipient()),
        invite.shown_code(),
        carol_joins(invite.shown_code()),
        "Guest phone",
        |_| {},
        &control,
    );
    let (handle, _) = owner.join_and_approve(joining, &control, |_| async {}).await;
    let handle = handle.unwrap().unwrap();
    assert_eq!(handle.store_name().await.unwrap(), "After snapshot");
    handle.close().await.unwrap();
    let current = owner.invite().await;
    assert_eq!(coven_sync::read_invite_code(current.shown_code()).unwrap().name, "After snapshot");
    owner.handle.cancel_invite(&current.id).await.unwrap();
    owner.close().await;
}

#[tokio::test]
async fn a_restore_learns_the_renamed_name_from_the_floor_snapshot() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    crate::tests::landed(&owner.handle, owner.handle.rename_store("Retained name").await.unwrap())
        .await;
    owner
        .handle
        .write(|sql| {
            sql.execute(
                "INSERT INTO notes VALUES('snapshot',?1)",
                coven_database::params![vec![9_u8; 1_100_000]],
            )?;
            Ok::<_, CovenError>(())
        })
        .await
        .unwrap();
    owner.sync().await;
    owner.clock.set(owner.clock.now() + Duration::from_secs(31 * 24 * 3600));
    owner.sync().await;
    owner.sync().await;
    assert!(owner
        .storage
        .status(&ObjectPath::slot(owner.directory.id(), 1.try_into().unwrap()))
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

/// Carol watches her waiting screen as Ana approves: the guard her app
/// holds runs the next check at once, instead of after the join's wait
/// (§13.2).
#[tokio::test]
async fn a_held_check_finds_an_approval_before_the_joins_wait_ends() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let builder = install.builder(&owner, owner.recipient());
    let control = JoinControl::new();
    let (waits, mut waiting) = watch::channel(false);
    let joining = join_with_invite(
        builder,
        invite.shown_code(),
        carol_joins(invite.shown_code()),
        "New phone",
        |status| {
            if matches!(status, BootstrapStatus::WaitingForApproval { .. }) {
                waits.send_replace(true);
            }
        },
        &control,
    );
    let approve = async {
        waiting.wait_for(|waiting| *waiting).await.unwrap();
        owner.approve_join(&control).await;
    };
    let (result, ()) = join_with_response(joining, approve).await;
    result.unwrap().unwrap().close().await.unwrap();
    owner.close().await;
}

/// Ana approves Carol's request with sync stopped, beside a row of her
/// app's. Her write journals the binding, which `coven_operations` lists
/// running, and the invite shows its approval under way with that binding,
/// so her app offers no second decision. Carol's phone keeps waiting; once
/// sync runs, the binding lands and Carol's join loads (§13.2, §11.4).
#[tokio::test]
async fn an_approval_journals_beside_the_apps_row_and_lands_once_sync_runs() {
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
    let mut _often = None;
    let approve = async {
        let request = owner.next_join_request().await;
        owner.stop_sync().await;
        let binding = owner
            .handle
            .write(move |sql| {
                let binding = sql.approve_join_request(&request)?;
                sql.execute(
                    "INSERT INTO notes VALUES('approval',?1)",
                    coven_database::params![vec![1_u8]],
                )?;
                Ok::<_, CovenError>(binding)
            })
            .await
            .unwrap();
        let listed = owner
            .handle
            .read(move |sql| {
                Ok::<_, CovenError>(sql.query_row(
                    "SELECT kind,outcome FROM coven_operations WHERE operation=?1",
                    [binding.0],
                    |r| {
                        Ok((
                            r.get::<_, OperationKind>(0)?,
                            r.get::<_, Option<OperationOutcome>>(1)?,
                        ))
                    },
                )?)
            })
            .await
            .unwrap();
        assert_eq!(listed, (OperationKind::Invite, None));
        // The invite shows its approval under way, with the binding the
        // app's screen follows, and a second decision is refused.
        let open = owner.handle.read(Invite::list).await.unwrap();
        assert!(
            matches!(&open[..], [Invite { stage: InviteStage::Approving { binding: found, .. }, .. }] if *found == binding),
            "{open:?}"
        );
        let InviteStage::Approving { request, .. } = &open[0].stage else { unreachable!() };
        let again = owner.handle.decline_join_request(request).await;
        assert!(matches!(again, Err(SyncError::InvitationChanged)), "{again:?}");
        assert_eq!(owner.rows().await.iter().filter(|(id, _)| id == "approval").count(), 1);
        owner.handle.start_sync().await.unwrap();
        crate::tests::landed(&owner.handle, Some(binding)).await;
        _often = Some(control.check_often(Duration::ZERO));
    };
    let (result, ()) = join_with_response(joining, approve).await;
    result.unwrap().unwrap().close().await.unwrap();
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
        Installation::join_request(&invite, &JoinControl::new()),
        owner.recipient(),
        &cancel,
        |_| {},
    );
    let cancel_join = async {
        let request = owner.next_join_request().await;
        stop.send(true).unwrap();
        request
    };
    let (result, request) = join_with_response(joining, cancel_join).await;
    assert!(matches!(result, Err(BootstrapError::JoinRequestSent)), "{result:?}");
    let directory = install.root.path().join("stores").join(owner.directory.id().to_string());
    assert!(directory.exists());
    assert!(owner.storage.status(&crate::tests::request_path(&invite)).await.unwrap().is_some());
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
    let mut _often = None;
    let approve = async {
        crate::tests::approve(&owner.handle, &request).await;
        _often = Some(control.check_often(Duration::ZERO));
    };
    let (result, ()) = join_with_response(resumed, approve).await;
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
            Installation::join_request(&invite, &JoinControl::new()),
            owner.recipient(),
            &cancel,
            cancel_at(Phase::Connecting, &stop),
        )
        .await;
    assert!(matches!(result, Err(BootstrapError::Cancelled)), "{result:?}");
    install.absent(owner.directory.id()).await;
    assert!(saved_code(&install, owner.directory.id()).is_none());
    assert!(!install.root.path().join("stores").join(owner.directory.id().to_string()).exists());
    assert!(owner.storage.status(&crate::tests::request_path(&invite)).await.unwrap().is_none());
    owner.close().await;
}

/// Dan's phone mounts Household in his Dropbox, and Dropbox refuses his
/// request, so it was never sent. Cancelled from a later call, before it
/// sends again, the join unmounts Household before it removes itself: the
/// cancel knows of the mount only from what the join recorded (§13.2).
#[tokio::test]
async fn cancelling_an_unsent_join_unmounts_its_dropbox_folder() {
    let owner = Owner::new(CloudProvider::Dropbox, false).await;
    let invite = owner.invite().await;
    owner
        .storage
        .set_faults(coven_storage::test_utils::Faults {
            create_failures: [(
                crate::tests::request_path(&invite),
                (StorageFailure::PermissionDenied, 403),
            )]
            .into(),
            ..coven_storage::test_utils::Faults::none()
        })
        .await;
    let install = Installation::new();
    let control = JoinControl::new();
    let (_, running) = watch::channel(false);
    let refused = install
        .run(
            &owner,
            Installation::join_request(&invite, &control),
            owner.recipient(),
            &running,
            |_| {},
        )
        .await;
    assert!(
        matches!(&refused, Err(BootstrapError::Sync(SyncError::Storage(error)))
            if error.failure() == StorageFailure::PermissionDenied),
        "{:?}",
        refused.as_ref().err()
    );
    assert!(owner.storage.joined("join@example.com").await);
    owner.storage.set_faults(coven_storage::test_utils::Faults::none()).await;
    let (_, cancelled) = watch::channel(true);
    let result = install
        .run(
            &owner,
            Installation::join_request(&invite, &control),
            owner.recipient(),
            &cancelled,
            |_| {},
        )
        .await;
    assert!(matches!(result, Err(BootstrapError::Cancelled)), "{result:?}");
    assert!(!owner.storage.joined("join@example.com").await);
    install.absent(owner.directory.id()).await;
    assert!(saved_code(&install, owner.directory.id()).is_none());
    assert!(!install.root.path().join("stores").join(owner.directory.id().to_string()).exists());
    owner.close().await;
}

/// Carol closes the app while she waits. Her phone lists the join with the
/// store's name and the inviter's the invite carried, and continues it without the code; once it
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
    let CodeInfo::Invite(info) = decode_code_info(invite.shown_code()).unwrap() else {
        panic!("an invite code");
    };
    assert_eq!(
        (info.store_name.as_str(), info.inviter.as_str(), info.kind),
        ("Household", "Ana", InviteKind::NewPerson)
    );
    assert_eq!(builder.unfinished_joins().await.unwrap(), [info]);
    assert_eq!(saved_code(&install, store).unwrap().as_str(), invite.shown_code());
    let control = JoinControl::new();
    let resumed = builder.resume_join(store, |_| {}, &control);
    let (result, _) = owner
        .join_and_approve(resumed, &control, |request| async move {
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
/// installation are gone (§13.2). Ana's pass after the decline puts the
/// marker in the request's place, which the resumed join finds once it is
/// asked to check, its saved wait held.
#[tokio::test]
async fn a_closed_join_declined_meanwhile_resumes_to_none_and_forgets_its_code() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let store = owner.directory.id();
    waiting_join(&owner, &install, &invite).await;
    let request = owner.next_join_request().await;
    owner.handle.decline_join_request(&request).await.unwrap();
    owner.pass_now().await;
    assert!(crate::tests::no_request(&owner.storage, &crate::tests::request_path(&invite)).await);
    let control = JoinControl::new();
    let result = install
        .builder(&owner, owner.recipient())
        .resume_join(store, check_when_waiting(&control), &control)
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
        install.builder(&owner, owner.recipient()).unfinished_joins().await,
        Err(BootstrapError::Code(CodeError::Invalid))
    ));
    owner.handle.cancel_invite(&invite.id).await.unwrap();
    owner.close().await;
}

/// The revocation of a removed member's account that left a grant in place:
/// its operation, the account and the grants it left.
fn retained_grant(
    records: &[PendingRecord],
) -> Option<(OperationId, String, Vec<coven_storage::RetainedAccess>)> {
    records.iter().find_map(|record| match (&record.subject, &record.reason) {
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
/// a retry finishes the revocation (§14.2, §20.9). The retry is a step of
/// a write that changes no synced row, so it queues no upload to wake the
/// loop: its commit wakes the loop, which revokes the grant with no pass
/// asked for.
#[tokio::test]
async fn a_removed_members_retained_grant_stays_pending_until_a_retry_revokes_it() {
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
    let (result, member) = join_with_response(joining, async {
        waits.wait_for(|waiting| *waiting).await.unwrap();
        owner.approve_join(&control).await
    })
    .await;
    result.unwrap().unwrap().close().await.unwrap();
    let shares = vec![coven_storage::RetainedAccess {
        provider_id: "parent-folder".into(),
        reason: coven_storage::RetainedAccessReason::Inherited,
    }];
    owner.storage.set_retained_access("join@example.com", shares.clone()).await;

    crate::tests::landed(&owner.handle, owner.handle.remove_member(&member).await.unwrap()).await;
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

    owner.storage.set_retained_access("join@example.com", Vec::new()).await;
    // The pass the commit wakes finishes at the clock's new time.
    let mut status = owner.handle.test_sync_status();
    let finished = owner.clock.now() + std::time::Duration::from_secs(1);
    owner.clock.set(finished);
    owner
        .handle
        .write(move |sql| Ok::<_, CovenError>(sql.retry_pending_operation(operation)?))
        .await
        .unwrap();
    status.wait_for(|s| s.last_synced == Some(finished)).await;
    assert_eq!(retained_grant(&owner.handle.pending().await.unwrap()), None);
    // `sync` checks the pass leaves no operation pending.
    owner.sync().await;
    owner.close().await;
}

/// Carol scans an invite but opens it only once it has ended. Her phone
/// sends no request, which no device would ever answer, and the join ends as
/// an expired one does, leaving nothing behind (§13.2).
#[tokio::test]
async fn a_join_after_the_invite_ended_sends_no_request() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    owner.clock.set(invite.expires_at);
    let install = Installation::new();
    let (_, cancel) = watch::channel(false);
    let result = install
        .run(
            &owner,
            Installation::join_request(&invite, &JoinControl::new()),
            owner.recipient(),
            &cancel,
            |_| {},
        )
        .await;
    assert!(matches!(result, Ok(None)), "{result:?}");
    install.absent(owner.directory.id()).await;
    // Ana's expiry ended the invite: its marker, if her pass has written it,
    // is all there is at the request's path.
    let path = crate::tests::request_path(&invite);
    assert!(crate::tests::no_request(&owner.storage, &path).await);
    owner.close().await;
}

/// Carol's phone records its request before sending it, and the provider
/// stores it but the reply is lost, so the call fails. Ana declines the
/// request, which deletes it. Carol's retry finds neither the request nor
/// sealed keys for it and treats the invite as finished: the join ends
/// declined, and no second request is sent (§13.2).
#[tokio::test]
async fn a_join_request_whose_create_reply_was_lost_ends_as_settled() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let path = crate::tests::request_path(&invite);
    let install = Installation::new();
    let (_, cancel) = watch::channel(false);
    owner
        .storage
        .set_faults(coven_storage::test_utils::Faults {
            lose_completion_reply: true,
            ..coven_storage::test_utils::Faults::none()
        })
        .await;
    let control = JoinControl::new();
    let lost = install
        .run(
            &owner,
            Installation::join_request(&invite, &control),
            owner.recipient(),
            &cancel,
            |_| {},
        )
        .await;
    assert!(matches!(&lost, Err(BootstrapError::Sync(SyncError::Storage(_)))), "{:?}", lost.err());
    owner.storage.set_faults(coven_storage::test_utils::Faults::none()).await;
    let request = owner.next_join_request().await;
    owner.handle.decline_join_request(&request).await.unwrap();
    // Ana's pass deletes the request and puts the ended invite's marker in its place.
    owner.sync().await;
    let creates = || {
        owner
            .storage
            .request_trace()
            .into_iter()
            .filter(|request| {
                *request == coven_storage::test_utils::MemoryRequest::Create(path.clone())
            })
            .count()
    };
    let before = creates();
    let retried = install
        .run(
            &owner,
            Installation::join_request(&invite, &control),
            owner.recipient(),
            &cancel,
            |_| {},
        )
        .await;
    assert!(matches!(retried, Ok(None)), "{:?}", retried.err());
    assert_eq!(creates(), before, "the retry sent no second request");
    owner.close().await;
}

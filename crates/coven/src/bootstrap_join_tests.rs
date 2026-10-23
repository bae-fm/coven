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
    let (_, cancel) = watch::channel(false);
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
            &cancel,
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

/// A join reports connecting, then waiting for approval, then loading (E10).
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
    let (result, _) = join_and_approve(&owner.handle, joining, |_| async {}).await;
    result.unwrap().unwrap().close().await.unwrap();
    assert_eq!(
        reported.into_inner().unwrap(),
        [
            BootstrapStatus::Connecting,
            BootstrapStatus::WaitingForApproval,
            BootstrapStatus::Loading,
        ]
    );
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
    let saved = install.keychain.synced_restore_codes().unwrap();
    let [(_, coven_crypto::custody::SyncedRestoreCode::Saved(saved))] = &saved[..] else {
        panic!("expected one saved code, got {saved:?}");
    };
    let saved = coven_format::codes::KeychainRestoreCode::from_bytes(saved.as_bytes()).unwrap();
    // The saved code takes the name loading recovered from the snapshot.
    assert_eq!(saved.name, "After snapshot");
    assert_eq!(saved.code.to_text().unwrap().as_str(), old_code.as_str());
    restored.close().await.unwrap();
    let reopened = install
        .builder(&owner, owner.storage.clone())
        .open(owner.directory.id())
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
    let (_, cancel) = watch::channel(false);
    let joining = join_with_invite(
        joined.builder(&owner, owner.recipient()),
        &invite.code,
        "Guest phone",
        |_| {},
        &cancel,
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
    handle.close().await.unwrap();
    owner.close().await;
}

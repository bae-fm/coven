use super::*;
use coven_sync::JoiningIdentity;

#[tokio::test]
async fn joining_refuses_an_invite_for_another_store_before_keeping_an_identity() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let pending = install
        .layout
        .begin_bootstrap(owner.directory.id(), "Household", &UuidIds)
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
        .begin_bootstrap(owner.directory.id(), "Household", &UuidIds)
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
    for field in ["initial_key", "inviting_writer", "key_copy_number"] {
        let mut changed = coven_sync::read_invite_code(&invite.code).unwrap();
        match field {
            "initial_key" => changed.initial_key = KeyId(UuidIds.new_id()),
            "inviting_writer" => changed.inviting_writer.0 ^= 1,
            "key_copy_number" => changed.key_copy_number += 1,
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
        .begin_bootstrap(owner.directory.id(), "Household", &UuidIds)
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
    assert_eq!(
        crate::tests::access_keys(&owner.handle).await,
        [AccessKeyToDelete {
            access_key_id: "invited-key".into(),
            principal: coven_format::S3Principal::Console,
            member: Some(carol),
        }]
    );
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
/// absent request as a declined invite (§12.2).
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

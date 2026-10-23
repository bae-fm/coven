use super::*;
use crate::sequence_sync::tests::{Device, Group};
use coven_foundation::id_source::IdSource;
use coven_storage::test_utils::{Faults, MemoryRequest};

async fn replacement(group: &Group, old: &Device) -> Device {
    let new = group
        .newcomer(old.identity.clone(), old.storage.for_device())
        .await;
    new.sync.unlock().await.unwrap();
    new.sync
        .register_device("Replacement".into(), Some(old.sync.device))
        .await
        .unwrap();
    new
}

async fn carry(old: &Device, new: &Device) {
    let records = old.database().operations().await.unwrap();
    new.sync
        .record_device_reset(
            DeviceReset {
                old_device: old.sync.device,
                new_device: new.sync.device,
            },
            &records,
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn carried_account_revocation_reports_failure_and_survives_reopening() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group
        .create_with_access(
            &ana,
            MemberAccess::ProviderAccount("ana@example.org".into()),
        )
        .await;
    ana.sync
        .create_invite(
            InviteTarget::NewMember {
                role: MemberRole::Member,
                name: "Carol".into(),
            },
            InviteAccess::ProviderAccount {
                email: "carol@example.org".into(),
            },
            crate::InviteDelivery::Sent {
                lifetime: crate::InviteLifetime::Day,
            },
        )
        .await
        .unwrap();
    let new = replacement(&group, &ana).await;
    carry(&ana, &new).await;
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    new.storage
        .hold_next_access("carol@example.org", false, entered, released)
        .await;
    let (pass, ()) = tokio::join!(new.sync.pass(), async {
        entering.await.unwrap();
        new.storage
            .set_faults(Faults {
                fail_next: 1,
                failure: StorageFailure::PermissionDenied,
                ..Faults::none()
            })
            .await;
        release.send(()).unwrap();
    });
    pass.unwrap();
    let pending = new.database().pending::<PendingRecord>().await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(
        pending[0].reason,
        PendingReason::Storage {
            provider: CloudProvider::Dropbox,
            failure: StorageFailure::PermissionDenied,
            answered: false,
        }
    );
    let PendingSubject::Operation {
        id,
        kind: OperationKind::RevokeAccess,
    } = pending[0].subject
    else {
        panic!("owed account revocation: {pending:?}")
    };
    let new = group.reopen(new).await;
    assert_eq!(
        new.database().pending::<PendingRecord>().await.unwrap(),
        pending
    );
    new.sync.retry_pending_operation(id).await.unwrap();
    assert!(new
        .database()
        .pending::<PendingRecord>()
        .await
        .unwrap()
        .is_empty());
    let access = new
        .database()
        .provider_access("carol@example.org".into())
        .await
        .unwrap()
        .unwrap();
    assert_eq!((access.intended, access.granted), (false, Some(false)));
    ana.database().close().await.unwrap();
    new.database().close().await.unwrap();
}

#[tokio::test]
async fn reset_carries_account_work_after_the_invite_journal_has_finished() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group
        .create_with_access(
            &ana,
            MemberAccess::ProviderAccount("ana@example.org".into()),
        )
        .await;
    let invite = ana
        .sync
        .create_invite(
            InviteTarget::NewMember {
                role: MemberRole::Member,
                name: "Carol".into(),
            },
            InviteAccess::ProviderAccount {
                email: "carol@example.org".into(),
            },
            crate::InviteDelivery::Sent {
                lifetime: crate::InviteLifetime::Day,
            },
        )
        .await
        .unwrap();
    ana.sync.cancel_invite(invite.id).await.unwrap();
    assert!(ana
        .database()
        .first_operation(INVITE)
        .await
        .unwrap()
        .is_none());
    let new = replacement(&group, &ana).await;
    carry(&ana, &new).await;
    new.sync.pass().await.unwrap();
    let access = new
        .database()
        .provider_access("carol@example.org".into())
        .await
        .unwrap()
        .unwrap();
    assert_eq!((access.intended, access.granted), (false, Some(false)));
    assert!(new.database().operations().await.unwrap().is_empty());
    ana.database().close().await.unwrap();
    new.database().close().await.unwrap();
}

#[tokio::test]
async fn reset_preserves_an_active_members_account() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group
        .create_with_access(
            &ana,
            MemberAccess::ProviderAccount("ana@example.org".into()),
        )
        .await;
    let target = MemberId(group.ids.new_id());
    ana.sync
        .administer(StoreChange::AddMember {
            member: target,
            keys: coven_format::test_utils::keys_from_seed(3).public_keys(),
            role: MemberRole::Member,
            access: MemberAccess::ProviderAccount("carol@example.org".into()),
        })
        .await
        .unwrap();
    ana.sync
        .create_invite(
            InviteTarget::ReplaceMemberKeys { member: target },
            InviteAccess::ProviderAccount {
                email: "carol@example.org".into(),
            },
            crate::InviteDelivery::Sent {
                lifetime: crate::InviteLifetime::Day,
            },
        )
        .await
        .unwrap();
    let new = replacement(&group, &ana).await;
    carry(&ana, &new).await;
    new.sync.pass().await.unwrap();
    assert!(!new
        .storage
        .request_trace()
        .contains(&MemoryRequest::Other("revoke_access")));
    assert!(
        new.database()
            .provider_access("carol@example.org".into())
            .await
            .unwrap()
            .unwrap()
            .intended
    );
    assert!(new.database().operations().await.unwrap().is_empty());
    ana.database().close().await.unwrap();
    new.database().close().await.unwrap();
}

#[tokio::test]
async fn a_reset_ends_a_replacement_invite_and_records_nothing_about_its_s3_key() {
    for key in ["ben", "new-key"] {
        let group = Group::new();
        let ana = group.device(1).await;
        let ben = group.device(2).await;
        group.create(&ana).await;
        group.admit(&ana, &ben).await;
        ana.sync
            .create_invite(
                InviteTarget::ReplaceMemberKeys {
                    member: ben.identity.member(),
                },
                InviteAccess::S3AccessKey {
                    access_key_id: key.into(),
                    secret_access_key: SecretText::new("secret".into()),
                },
                crate::InviteDelivery::Sent {
                    lifetime: crate::InviteLifetime::Day,
                },
            )
            .await
            .unwrap();
        let new = replacement(&group, &ana).await;
        carry(&ana, &new).await;
        new.sync.pass().await.unwrap();
        assert!(new.database().operations().await.unwrap().is_empty());
        assert!(new
            .database()
            .pending::<PendingRecord>()
            .await
            .unwrap()
            .is_empty());
        for device in [&ana, &ben, &new] {
            device.database().close().await.unwrap();
        }
    }
}

#[tokio::test]
async fn reset_keeps_an_invite_key_whose_approval_landed_before_journal_completion() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let invite = ana
        .sync
        .create_invite(
            InviteTarget::NewMember {
                role: MemberRole::Member,
                name: "Carol".into(),
            },
            InviteAccess::S3AccessKey {
                access_key_id: "carol-key".into(),
                secret_access_key: SecretText::new("secret".into()),
            },
            crate::InviteDelivery::Sent {
                lifetime: crate::InviteLifetime::Day,
            },
        )
        .await
        .unwrap();
    let record = ana
        .database()
        .first_operation(INVITE)
        .await
        .unwrap()
        .unwrap();
    let mut work = InviteWork::read(&record).unwrap();
    let keys = coven_format::test_utils::keys_from_seed(3).public_keys();
    work.state = InviteState::Approving {
        code: SecretText::new(invite.code.clone()),
        request: Object::JoinRequest(coven_format::objects::JoinRequest {
            invite: invite.id,
            member: invite.member,
            keys,
            device_name: "Carol".into(),
        })
        .encode()
        .unwrap(),
    };
    ana.database()
        .advance_operation(work.update(&record).unwrap())
        .await
        .unwrap();
    // The binding has applied, but the parent invite still records Approving,
    // as after interruption before its next local commit.
    ana.sync
        .administer(StoreChange::AddMember {
            member: invite.member,
            keys,
            role: MemberRole::Member,
            access: work.access.member_access(),
        })
        .await
        .unwrap();
    let new = replacement(&group, &ana).await;
    carry(&ana, &new).await;
    new.sync.pass().await.unwrap();
    assert!(new.database().operations().await.unwrap().is_empty());
    assert!(new
        .database()
        .pending::<PendingRecord>()
        .await
        .unwrap()
        .is_empty());
    ana.database().close().await.unwrap();
    new.database().close().await.unwrap();
}

#[tokio::test]
async fn an_undecodable_invite_fails_reset_without_a_completion_notice() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    ana.sync
        .create_invite(
            InviteTarget::NewMember {
                role: MemberRole::Member,
                name: "Carol".into(),
            },
            InviteAccess::S3AccessKey {
                access_key_id: "carol-key".into(),
                secret_access_key: SecretText::new("secret".into()),
            },
            crate::InviteDelivery::Sent {
                lifetime: crate::InviteLifetime::Day,
            },
        )
        .await
        .unwrap();
    let mut records = ana.database().operations().await.unwrap();
    records
        .iter_mut()
        .find(|record| record.kind == INVITE)
        .unwrap()
        .data = b"{".to_vec();
    let new = replacement(&group, &ana).await;
    assert!(new
        .sync
        .record_device_reset(
            DeviceReset {
                old_device: ana.sync.device,
                new_device: new.sync.device,
            },
            &records
        )
        .await
        .is_err());
    assert_eq!(new.database().device_reset().await.unwrap(), None);
    assert!(new.database().operations().await.unwrap().is_empty());
    carry(&ana, &new).await;
    new.sync.pass().await.unwrap();
    assert!(new.database().operations().await.unwrap().is_empty());
    ana.database().close().await.unwrap();
    new.database().close().await.unwrap();
}

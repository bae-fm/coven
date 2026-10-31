use crate::sequence_sync::sequence_invites::tests::requests;
use crate::sequence_sync::tests::*;
use crate::*;
use coven_format::{admin::MemberRole, MemberAccess};
use coven_storage::{ObjectPath, Storage};

async fn in_person_invite(ana: &Device) -> Invite {
    ana.sync
        .create_invite(
            InviteTarget::NewMember { role: MemberRole::Member, name: "Carol".into() },
            InviteAccess::ProviderAccount { email: "carol@example.org".into() },
            InviteDelivery::InPerson,
        )
        .await
        .unwrap()
}

/// Ana watched Carol's phone scan an in-person invite: the pass that reads
/// and checks the request approves it, with no call from the app (§13.2).
#[tokio::test]
async fn an_in_person_request_is_approved_in_the_pass_that_reads_it() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group.create_with_access(&ana, MemberAccess::ProviderAccount("ana@example.org".into())).await;
    let invite = in_person_invite(&ana).await;
    let carol = join(&group, &invite.code, "Carol's phone").await;
    ana.sync.pass().await.unwrap();
    assert!(ana.sync.invites().await.unwrap().is_empty());
    assert!(group
        .storage
        .status(&ObjectPath::join_request(group.store, invite.id))
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        carol.device.sync.join_outcome(&carol.request).await.unwrap(),
        JoinOutcome::Admitted
    );
    for device in [&ana, &carol.device] {
        device.sync.database.close().await.unwrap();
    }
}

/// Once an in-person invite's approval has begun, copies for Carol may
/// already be claimed: approving, declining and cancelling all fail with
/// `InvitationChanged`, and the approval finishes on retry (§13.2).
#[tokio::test]
async fn an_in_person_approval_under_way_refuses_the_apps_decisions() {
    use coven_storage::test_utils::Faults;
    let group = Group::shared();
    let ana = group.device(1).await;
    group.create_with_access(&ana, MemberAccess::ProviderAccount("ana@example.org".into())).await;
    let invite = in_person_invite(&ana).await;
    let carol = join(&group, &invite.code, "Carol's phone").await;
    let next = ana.sync.database.applied_slot().await.unwrap() + 1;
    ana.storage
        .set_faults(Faults {
            create_failures: [(
                ObjectPath::slot(group.store, next.try_into().unwrap()),
                (coven_storage::StorageFailure::Network, 500),
            )]
            .into(),
            ..Faults::none()
        })
        .await;
    ana.sync.pass().await.unwrap();
    let listed = ana.sync.invites().await.unwrap();
    assert_eq!(listed.len(), 1);
    let request = listed[0].request.clone().expect("the request being approved");
    assert_eq!(request.device_name, "Carol's phone");
    for result in [
        ana.sync.approve_join_request(&request).await,
        ana.sync.decline_join_request(&request).await,
        ana.sync.cancel_invite(invite.id).await,
    ] {
        assert!(matches!(result, Err(SyncError::InvitationChanged)), "{result:?}");
    }
    ana.storage.set_faults(Faults::none()).await;
    let operation = ana
        .sync
        .database
        .operations()
        .await
        .unwrap()
        .into_iter()
        .find(|record| record.kind == "invite")
        .unwrap()
        .id;
    ana.sync.retry_pending_operation(operation).await.unwrap();
    assert!(ana.sync.invites().await.unwrap().is_empty());
    assert_eq!(
        carol.device.sync.join_outcome(&carol.request).await.unwrap(),
        JoinOutcome::Admitted
    );
    for device in [&ana, &carol.device] {
        device.sync.database.close().await.unwrap();
    }
}

#[tokio::test]
async fn approval_after_the_approver_lost_its_role_undoes_and_fails() {
    let group = Group::shared();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create_with_access(&ana, MemberAccess::ProviderAccount("ana@example.org".into())).await;
    group.admit(&ana, &ben).await;
    let invite = invite(&ana).await;
    let carol = join(&group, &invite.code, "Carol's phone").await;
    ana.sync.pass().await.unwrap();
    let checked = requests(&ana).await;
    ben.sync.pass().await.unwrap();
    ben.sync
        .administer(coven_format::admin::StoreChange::ChangeRole {
            member: ana.identity.member(),
            role: MemberRole::Member,
        })
        .await
        .unwrap();
    let result = ana.sync.approve_join_request(&checked[0]).await;
    assert!(
        matches!(result, Err(SyncError::Rejected(OperationRejection::NotAllowed))),
        "{result:?}"
    );
    assert!(ana
        .sync
        .database
        .operations()
        .await
        .unwrap()
        .iter()
        .all(|record| record.kind != "invite"));
    assert!(!ana.sync.database.store_state().await.unwrap().members.contains_key(&invite.member));
    assert_eq!(
        carol.device.sync.join_outcome(&carol.request).await.unwrap(),
        JoinOutcome::Declined
    );
    for device in [&ana, &ben, &carol.device] {
        device.sync.database.close().await.unwrap();
    }
}

// §13.2, §11 and E9: an invite cannot replace a competing binding on retry.
#[tokio::test]
async fn a_replacement_invite_keeps_its_expected_pair_across_approval_and_retry() {
    for (interrupted, failed_cleanup) in
        [(true, false), (false, false), (false, true), (true, true)]
    {
        let group = Group::shared();
        let ana = group.device(1).await;
        let ben = group.device(2).await;
        group
            .create_with_access(&ana, MemberAccess::ProviderAccount("ana@example.org".into()))
            .await;
        group.admit(&ana, &ben).await;
        let target = coven_foundation::id_source::MemberId(uuid::Uuid::from_u128(91));
        ana.sync
            .administer(coven_format::admin::StoreChange::AddMember {
                member: target,
                keys: coven_format::test_utils::keys_from_seed(3).public_keys(),
                role: MemberRole::Member,
                access: MemberAccess::ProviderAccount("carol@example.org".into()),
            })
            .await
            .unwrap();
        let invite = ana
            .sync
            .create_invite(
                InviteTarget::ReplaceMemberKeys { member: target },
                InviteAccess::ProviderAccount { email: "carol@example.org".into() },
                crate::InviteDelivery::Sent { lifetime: crate::InviteLifetime::Day },
            )
            .await
            .unwrap();
        let carol = join(&group, &invite.code, "Carol's replacement").await;
        ana.sync.pass().await.unwrap();
        let request = requests(&ana).await.remove(0);
        let operation = ana
            .sync
            .database
            .operations()
            .await
            .unwrap()
            .into_iter()
            .find(|record| record.kind == "invite")
            .unwrap()
            .id;
        if interrupted {
            let next = ana.sync.database.applied_slot().await.unwrap() + 1;
            ana.storage
                .set_faults(coven_storage::test_utils::Faults {
                    create_failures: [(
                        ObjectPath::slot(group.store, next.try_into().unwrap()),
                        (coven_storage::StorageFailure::Network, 500),
                    )]
                    .into(),
                    ..coven_storage::test_utils::Faults::none()
                })
                .await;
            assert!(matches!(
                ana.sync.approve_join_request(&request).await,
                Err(SyncError::Storage(_))
            ));
            ana.storage.set_faults(coven_storage::test_utils::Faults::none()).await;
        }
        let competing = coven_format::test_utils::keys_from_seed(4).public_keys();
        ben.sync.replace_member_keys_for(target, competing).await.unwrap();
        if failed_cleanup {
            ana.storage
                .set_faults(coven_storage::test_utils::Faults {
                    delete_failures: [(
                        ObjectPath::join_request(group.store, invite.id),
                        coven_storage::StorageFailure::PermissionDenied,
                    )]
                    .into(),
                    ..coven_storage::test_utils::Faults::none()
                })
                .await;
        }
        let result = if interrupted {
            ana.sync.retry_pending_operation(operation).await.map(drop)
        } else {
            ana.sync.approve_join_request(&request).await
        };
        if failed_cleanup {
            // E6: failed undo retains both causes and its operation journal.
            assert!(
                matches!(
                    &result,
                    Err(SyncError::Cleanup { operation, cleanup })
                        if matches!(operation.as_ref(), SyncError::Rejected(_))
                        && matches!(cleanup.as_ref(), SyncError::Storage(error)
                            if error.failure() == coven_storage::StorageFailure::PermissionDenied)
                ),
                "{result:?}"
            );
            assert!(ana.database().operation(operation).await.unwrap().is_some());
            ana.storage.set_faults(coven_storage::test_utils::Faults::none()).await;
            ana.sync.retry_pending_operation(operation).await.unwrap();
        } else {
            assert!(matches!(result, Err(SyncError::Rejected(_))), "{result:?}");
        }
        assert!(
            ana.sync.database.operation(operation).await.unwrap().is_none(),
            "rejection must finish the parent undo"
        );
        assert!(ana
            .sync
            .database
            .operations()
            .await
            .unwrap()
            .iter()
            .all(|record| record.started_by != "invite"));
        assert!(group
            .storage
            .status(&ObjectPath::join_request(group.store, invite.id))
            .await
            .unwrap()
            .is_none());
        ana.sync.pass().await.unwrap();
        assert_eq!(ana.sync.database.store_state().await.unwrap().members[&target].keys, competing);
        for device in [&ana, &ben, &carol.device] {
            device.sync.database.close().await.unwrap();
        }
    }
}

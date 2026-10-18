use crate::sequence_sync::tests::*;
use crate::*;
use coven_format::{admin::MemberRole, MemberAccess};
use coven_storage::{ObjectPath, Storage};

#[tokio::test]
async fn an_approved_request_binds_its_pair_after_copies_and_the_joiner_registers() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group
        .create_with_access(
            &ana,
            MemberAccess::ProviderAccount("ana@example.org".into()),
        )
        .await;
    insert(&ana.sync.database, "a", "before the invite").await;
    ana.sync.pass().await.unwrap();
    let invite = invite(&ana).await;
    let carol = join(&group, &invite.code, "Carol's phone").await;
    assert_eq!(
        carol
            .device
            .sync
            .join_outcome(&carol.request)
            .await
            .unwrap(),
        JoinOutcome::Waiting
    );
    ana.sync.pass().await.unwrap();
    let requests = ana.sync.join_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].member, invite.member);
    assert_eq!(requests[0].device_name, "Carol's phone");
    assert_eq!(
        requests[0].provider_account_email.as_deref(),
        Some("carol@example.org")
    );
    let count = ana.storage.request_count();
    ana.sync.pass().await.unwrap();
    assert_eq!(
        ana.storage.request_count() - count,
        2,
        "a checked request waits without reads"
    );
    ana.sync.approve_join_request(&requests[0]).await.unwrap();
    assert!(group
        .storage
        .status(&ObjectPath::join_request(group.store, invite.id))
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        carol
            .device
            .sync
            .join_outcome(&carol.request)
            .await
            .unwrap(),
        JoinOutcome::Admitted
    );
    carol
        .device
        .sync
        .register_device("Carol's phone".into(), None)
        .await
        .unwrap();
    assert_eq!(
        rows(&carol.device.sync.database).await,
        rows(&ana.sync.database).await
    );
    insert(&carol.device.sync.database, "c", "from Carol").await;
    carol.device.sync.pass().await.unwrap();
    ana.sync.pass().await.unwrap();
    assert_eq!(rows(&ana.sync.database).await.len(), 2);
    assert!(ana.sync.join_requests().await.unwrap().is_empty());
    assert!(
        ana.sync
            .database
            .provider_access("carol@example.org".into())
            .await
            .unwrap()
            .unwrap()
            .intended
    );
    for device in [&ana, &carol.device] {
        device.sync.database.close().await.unwrap();
    }
}

#[tokio::test]
async fn a_declined_request_takes_back_the_share_and_the_joiner_sees_the_decline() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group
        .create_with_access(
            &ana,
            MemberAccess::ProviderAccount("ana@example.org".into()),
        )
        .await;
    let invite = invite(&ana).await;
    let carol = join(&group, &invite.code, "Carol's phone").await;
    ana.sync.pass().await.unwrap();
    let requests = ana.sync.join_requests().await.unwrap();
    ana.sync.decline_join_request(&requests[0]).await.unwrap();
    ana.sync.pass().await.unwrap();
    let access = ana
        .sync
        .database
        .provider_access("carol@example.org".into())
        .await
        .unwrap()
        .unwrap();
    assert!(!access.intended);
    assert_eq!(access.granted, Some(false));
    assert!(ana.sync.database.operations().await.unwrap().is_empty());
    // The revoked share makes the request unreadable, which is not absence:
    // the provider's permission failure reaches the app (§13.2).
    let outcome = carol.device.sync.join_outcome(&carol.request).await;
    assert!(
        matches!(&outcome, Err(SyncError::Storage(error)) if error.failure() == coven_storage::StorageFailure::PermissionDenied),
        "{outcome:?}"
    );
    for device in [&ana, &carol.device] {
        device.sync.database.close().await.unwrap();
    }
}

#[tokio::test]
async fn an_invite_expires_a_day_later_on_the_inviting_device() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group
        .create_with_access(
            &ana,
            MemberAccess::ProviderAccount("ana@example.org".into()),
        )
        .await;
    invite(&ana).await;
    group
        .clock
        .set(std::time::UNIX_EPOCH + std::time::Duration::from_secs(86_400));
    ana.sync.pass().await.unwrap();
    ana.sync.pass().await.unwrap();
    assert!(ana.sync.database.operations().await.unwrap().is_empty());
    let access = ana
        .sync
        .database
        .provider_access("carol@example.org".into())
        .await
        .unwrap()
        .unwrap();
    assert_eq!((access.intended, access.granted), (false, Some(false)));
    ana.sync.database.close().await.unwrap();
}

#[tokio::test]
async fn approval_after_the_approver_lost_its_role_undoes_and_fails() {
    let group = Group::shared();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group
        .create_with_access(
            &ana,
            MemberAccess::ProviderAccount("ana@example.org".into()),
        )
        .await;
    group.admit(&ana, &ben).await;
    let invite = invite(&ana).await;
    let carol = join(&group, &invite.code, "Carol's phone").await;
    ana.sync.pass().await.unwrap();
    let requests = ana.sync.join_requests().await.unwrap();
    ben.sync.pass().await.unwrap();
    ben.sync
        .administer(coven_format::admin::StoreChange::ChangeRole {
            member: ana.identity.member(),
            role: MemberRole::Member,
        })
        .await
        .unwrap();
    let result = ana.sync.approve_join_request(&requests[0]).await;
    assert!(
        matches!(
            result,
            Err(SyncError::Rejected(OperationRejection::NotAllowed))
        ),
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
    assert!(!ana
        .sync
        .database
        .store_state()
        .await
        .unwrap()
        .members
        .contains_key(&invite.member));
    assert_eq!(
        carol
            .device
            .sync
            .join_outcome(&carol.request)
            .await
            .unwrap(),
        JoinOutcome::Declined
    );
    for device in [&ana, &ben, &carol.device] {
        device.sync.database.close().await.unwrap();
    }
}

#[tokio::test]
async fn a_replacement_invite_rebinds_an_existing_member_on_a_new_device() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group
        .create_with_access(
            &ana,
            MemberAccess::ProviderAccount("ana@example.org".into()),
        )
        .await;
    let carol_id = coven_crypto::MemberIdentity::new(
        coven_foundation::id_source::MemberId(uuid::Uuid::from_u128(91)),
        coven_format::test_utils::keys_from_seed(3),
    );
    ana.sync
        .administer(coven_format::admin::StoreChange::AddMember {
            member: carol_id.member(),
            keys: carol_id.keys().public_keys(),
            role: MemberRole::Member,
            access: MemberAccess::ProviderAccount("carol@example.org".into()),
        })
        .await
        .unwrap();
    let invite = ana
        .sync
        .create_invite(
            InviteTarget::ReplaceMemberKeys {
                member: carol_id.member(),
            },
            InviteAccess::ProviderAccount {
                email: "carol@example.org".into(),
            },
        )
        .await
        .unwrap();
    assert_eq!(invite.member, carol_id.member());
    let carol = join(&group, &invite.code, "Carol's new phone").await;
    ana.sync.pass().await.unwrap();
    let requests = ana.sync.join_requests().await.unwrap();
    ana.sync.approve_join_request(&requests[0]).await.unwrap();
    let state = ana.sync.database.store_state().await.unwrap();
    assert_eq!(
        state.members[&carol_id.member()].keys,
        carol.request.identity().keys().public_keys()
    );
    assert_eq!(
        carol
            .device
            .sync
            .join_outcome(&carol.request)
            .await
            .unwrap(),
        JoinOutcome::Admitted
    );
    carol
        .device
        .sync
        .register_device("Carol's new phone".into(), None)
        .await
        .unwrap();
    assert!(
        ana.sync
            .database
            .provider_access("carol@example.org".into())
            .await
            .unwrap()
            .unwrap()
            .intended
    );
    for device in [&ana, &carol.device] {
        device.sync.database.close().await.unwrap();
    }
}

#[tokio::test]
async fn an_explicit_retry_reads_a_request_before_its_backoff_is_due() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group
        .create_with_access(
            &ana,
            MemberAccess::ProviderAccount("ana@example.org".into()),
        )
        .await;
    let invite = invite(&ana).await;
    ana.sync.pass().await.unwrap();
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
    let pending = ana
        .sync
        .database
        .operation_pending::<PendingRecord>(operation)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(pending.reason, PendingReason::Missing { .. }));
    let carol = join(&group, &invite.code, "Carol's phone").await;
    let count = ana.storage.request_count();
    ana.sync.pass().await.unwrap();
    assert_eq!(
        ana.storage.request_count() - count,
        2,
        "the request poll backs off"
    );
    assert!(ana.sync.join_requests().await.unwrap().is_empty());
    ana.sync.retry_pending_operation(operation).await.unwrap();
    assert_eq!(ana.sync.join_requests().await.unwrap().len(), 1);
    for device in [&ana, &carol.device] {
        device.sync.database.close().await.unwrap();
    }
}

/// A grant the provider refuses answers the call at once with that refusal,
/// and the invite takes back what it intended (T296).
#[tokio::test]
async fn a_refused_grant_answers_the_invite_call_and_settles_the_invite() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group
        .create_with_access(
            &ana,
            MemberAccess::ProviderAccount("ana@example.org".into()),
        )
        .await;
    let result = ana
        .sync
        .create_invite(
            InviteTarget::NewMember {
                role: MemberRole::Member,
            },
            InviteAccess::ProviderAccount {
                email: "owner@example.test".into(),
            },
        )
        .await;
    assert!(
        matches!(&result, Err(SyncError::Storage(error)) if error.failure() == coven_storage::StorageFailure::InviteeIsStoreOwner),
        "{:?}",
        result.err()
    );
    assert!(ana
        .sync
        .database
        .operations()
        .await
        .unwrap()
        .iter()
        .all(|record| record.kind != "invite"));
    let access = ana
        .sync
        .database
        .provider_access("owner@example.test".into())
        .await
        .unwrap()
        .unwrap();
    assert!(!access.intended);
    ana.sync.database.close().await.unwrap();
}

/// The owner's checked requests reach the app's subscription without a call.
#[tokio::test]
async fn a_pass_publishes_checked_join_requests_to_the_subscription() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group
        .create_with_access(
            &ana,
            MemberAccess::ProviderAccount("ana@example.org".into()),
        )
        .await;
    let requests = ana.sync.state.subscribe_join_requests();
    let invite = invite(&ana).await;
    assert!(requests.borrow().is_empty());
    let carol = join(&group, &invite.code, "Carol's phone").await;
    ana.sync.pass().await.unwrap();
    let shown = requests.borrow().clone();
    assert_eq!(shown.len(), 1);
    assert_eq!(shown, ana.sync.join_requests().await.unwrap());
    ana.sync.decline_join_request(&shown[0]).await.unwrap();
    assert!(requests.borrow().is_empty());
    for device in [&ana, &carol.device] {
        device.sync.database.close().await.unwrap();
    }
}

/// A request waiting for the app's decision does not keep an expired invite
/// open: the next pass takes its grant back (§13.2).
#[tokio::test]
async fn an_invite_waiting_for_a_decision_still_expires() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group
        .create_with_access(
            &ana,
            MemberAccess::ProviderAccount("ana@example.org".into()),
        )
        .await;
    let invite = invite(&ana).await;
    let carol = join(&group, &invite.code, "Carol's phone").await;
    ana.sync.pass().await.unwrap();
    assert_eq!(ana.sync.join_requests().await.unwrap().len(), 1);
    group.clock.set(invite.expires_at);
    ana.sync.pass().await.unwrap();
    assert!(ana.sync.join_requests().await.unwrap().is_empty());
    assert!(ana
        .sync
        .database
        .operations()
        .await
        .unwrap()
        .iter()
        .all(|record| record.kind != "invite"));
    for device in [&ana, &carol.device] {
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
            .create_with_access(
                &ana,
                MemberAccess::ProviderAccount("ana@example.org".into()),
            )
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
                InviteAccess::ProviderAccount {
                    email: "carol@example.org".into(),
                },
            )
            .await
            .unwrap();
        let carol = join(&group, &invite.code, "Carol's replacement").await;
        ana.sync.pass().await.unwrap();
        let request = ana.sync.join_requests().await.unwrap().remove(0);
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
            ana.storage
                .set_faults(coven_storage::test_utils::Faults::none())
                .await;
        }
        let competing = coven_format::test_utils::keys_from_seed(4).public_keys();
        ben.sync
            .replace_member_keys_for(target, competing)
            .await
            .unwrap();
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
            ana.storage
                .set_faults(coven_storage::test_utils::Faults::none())
                .await;
            ana.sync.retry_pending_operation(operation).await.unwrap();
        } else {
            assert!(matches!(result, Err(SyncError::Rejected(_))), "{result:?}");
        }
        assert!(
            ana.sync
                .database
                .operation(operation)
                .await
                .unwrap()
                .is_none(),
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
        assert_eq!(
            ana.sync.database.store_state().await.unwrap().members[&target].keys,
            competing
        );
        for device in [&ana, &ben, &carol.device] {
            device.sync.database.close().await.unwrap();
        }
    }
}

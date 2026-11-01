use crate::sequence_sync::sequence_invites::tests::requests;
use crate::sequence_sync::sequence_invites::{InviteState, InviteWork, INVITE};
use crate::sequence_sync::tests::*;
use crate::sequence_sync::OperationState;
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
    let carol = join(&group, invite.shown_code(), "Carol's phone").await;
    ana.sync.pass().await.unwrap();
    assert!(listed(&ana).await.is_empty());
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
    let carol = join(&group, invite.shown_code(), "Carol's phone").await;
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
    let open = listed(&ana).await;
    assert_eq!(open.len(), 1);
    let request = open[0].shown_request().cloned().clone().expect("the request being approved");
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
    assert!(listed(&ana).await.is_empty());
    assert_eq!(
        carol.device.sync.join_outcome(&carol.request).await.unwrap(),
        JoinOutcome::Admitted
    );
    for device in [&ana, &carol.device] {
        device.sync.database.close().await.unwrap();
    }
}

/// Ana's approval claims Carol's copies, then the entry adding her. A check
/// between the two opens Carol's keys and loads the store, so the join is
/// past waiting for her copies: it is `Approving`, never `Waiting` again,
/// until the entry admits her (§13.2).
#[tokio::test]
async fn a_check_between_the_copies_and_the_entry_finds_the_approval_begun() {
    use coven_storage::test_utils::Faults;
    let group = Group::shared();
    let ana = group.device(1).await;
    group.create_with_access(&ana, MemberAccess::ProviderAccount("ana@example.org".into())).await;
    let invite = in_person_invite(&ana).await;
    let carol = join(&group, invite.shown_code(), "Carol's phone").await;
    assert_eq!(carol.device.sync.join_outcome(&carol.request).await.unwrap(), JoinOutcome::Waiting);
    // The copy lands in the next slot; the entry after it fails.
    let entry = ana.sync.database.applied_slot().await.unwrap() + 2;
    ana.storage
        .set_faults(Faults {
            create_failures: [(
                ObjectPath::slot(group.store, entry.try_into().unwrap()),
                (coven_storage::StorageFailure::Network, 500),
            )]
            .into(),
            ..Faults::none()
        })
        .await;
    ana.sync.pass().await.unwrap();
    for _ in 0..2 {
        assert_eq!(
            carol.device.sync.join_outcome(&carol.request).await.unwrap(),
            JoinOutcome::Approving
        );
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
    let carol = join(&group, invite.shown_code(), "Carol's phone").await;
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

/// An approval whose binding's operation ended in an attempt that stopped
/// before the invite moved on resumes from the applied history: a binding
/// that landed finishes the approval, and one that didn't ends the invite as
/// a decline does, without claiming the entry again (§13.2, §11).
#[tokio::test]
async fn an_approval_whose_binding_ended_resumes_from_the_applied_history() {
    for landed in [true, false] {
        let group = Group::shared();
        let ana = group.device(1).await;
        group
            .create_with_access(&ana, MemberAccess::ProviderAccount("ana@example.org".into()))
            .await;
        let invite = invite(&ana).await;
        let carol = join(&group, invite.shown_code(), "Carol's phone").await;
        ana.sync.pass().await.unwrap();
        let record = ana.database().first_operation(INVITE).await.unwrap().unwrap();
        let mut work = InviteWork::read(&record).unwrap();
        let InviteState::Waiting { code, request: Some(request) } = &work.state else {
            panic!("the request waits for Ana");
        };
        let change = work.binding_change(request).unwrap();
        work.state = InviteState::Approving {
            code: code.clone(),
            request: request.clone(),
            // No journal row has this id: the binding's operation has ended.
            binding: coven_database::OperationId(i64::MAX),
        };
        ana.database().advance_operation(work.update(&record).unwrap()).await.unwrap();
        if landed {
            ana.sync.administer(change).await.unwrap();
        }
        ana.sync.pass().await.unwrap();
        // The invite has ended, and started no claim; taking back an unused
        // share is the account's own work.
        assert!(ana
            .database()
            .operations()
            .await
            .unwrap()
            .iter()
            .all(|record| record.kind != INVITE && record.started_by != INVITE));
        assert_eq!(
            ana.sync.database.store_state().await.unwrap().members.contains_key(&invite.member),
            landed
        );
        let expected = if landed { JoinOutcome::Admitted } else { JoinOutcome::Declined };
        assert_eq!(carol.device.sync.join_outcome(&carol.request).await.unwrap(), expected);
        for device in [&ana, &carol.device] {
            device.sync.database.close().await.unwrap();
        }
    }
}

/// Approve the request as the app's write step does, without a pass, and
/// return the operation that binds its pair.
async fn approve(device: &Device, request: &JoinRequest) -> coven_database::OperationId {
    let (id, held, state) =
        (device.sync.device, device.identity.clone(), device.sync.state.clone());
    let (now, waits, request) =
        (device.sync.clock.now(), device.sync.waits.clone(), request.clone());
    device
        .database()
        .write(move |sql| {
            crate::approve_join_request(&sql, id, &held, now, waits, state, &request)
                .map_err(|error| coven_database::DbError::SyncStream(Box::new(error)))
        })
        .await
        .unwrap()
}

/// The operation's outcome, as `coven_operations` lists it.
async fn outcome(
    device: &Device,
    operation: coven_database::OperationId,
) -> Option<OperationOutcome> {
    device
        .database()
        .read(move |sql| {
            Ok(sql.query_row(
                "SELECT outcome FROM coven_operations WHERE operation=?1",
                [operation.0],
                |row| row.get::<_, Option<OperationOutcome>>(0),
            )?)
        })
        .await
        .unwrap()
}

// §13.2, §11 and E9: an invite cannot replace a competing binding on retry.
// The approval's rejection is its binding's outcome, and the invite ends as
// a decline does; a failed deletion of the request stays the invite's work.
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
        let carol = join(&group, invite.shown_code(), "Carol's replacement").await;
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
        let interrupted_binding = if interrupted {
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
            let binding = approve(&ana, &request).await;
            ana.sync.pass().await.unwrap();
            // The claim that failed is the invite's to retry.
            assert_eq!(outcome(&ana, binding).await, None);
            assert!(ana
                .database()
                .operation_pending::<PendingRecord>(operation)
                .await
                .unwrap()
                .is_some());
            ana.storage.set_faults(coven_storage::test_utils::Faults::none()).await;
            Some(binding)
        } else {
            None
        };
        let competing = coven_format::test_utils::keys_from_seed(4).public_keys();
        // Ben's write checks the replacement against the slots he has applied.
        ben.sync.pass().await.unwrap();
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
        let binding = match interrupted_binding {
            Some(binding) => {
                ana.sync.retry_pending_operation(operation).await.unwrap();
                binding
            }
            None => {
                let binding = approve(&ana, &request).await;
                ana.sync.pass().await.unwrap();
                binding
            }
        };
        assert_eq!(
            outcome(&ana, binding).await,
            Some(OperationOutcome::Rejected(OperationRejection::MemberKeysChanged(target)))
        );
        if failed_cleanup {
            let pending =
                ana.database().operation_pending::<PendingRecord>(operation).await.unwrap();
            assert!(pending.is_some(), "the ended invite keeps its request's deletion");
            ana.storage.set_faults(coven_storage::test_utils::Faults::none()).await;
            ana.sync.retry_pending_operation(operation).await.unwrap();
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

/// Ana's kept replacement of Carol's pair on S3 has bound the new pair, and
/// the invite goes on to record Carol's new key. Ana removed Carol meanwhile,
/// so the applied history refuses the record. The invite ends, as a refused
/// binding ends it, instead of keeping a pending record that never retries,
/// can't be cancelled and is never cleared (§13.2, §14.3).
#[tokio::test]
async fn a_refused_access_record_ends_its_invite() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let carol = coven_crypto::MemberIdentity::new(
        coven_foundation::id_source::MemberId(uuid::Uuid::from_u128(91)),
        coven_format::test_utils::keys_from_seed(3),
    );
    ana.sync
        .administer(coven_format::admin::StoreChange::AddMember {
            member: carol.member(),
            keys: carol.keys().public_keys(),
            role: MemberRole::Member,
            access: access("carol-old"),
        })
        .await
        .unwrap();
    ana.sync
        .create_invite(
            InviteTarget::ReplaceMemberKeys { member: carol.member() },
            InviteAccess::S3AccessKey {
                access_key_id: "carol-new".into(),
                secret_access_key: coven_crypto::SecretText::new("secret".into()),
            },
            InviteDelivery::Sent { lifetime: InviteLifetime::Day },
        )
        .await
        .unwrap();
    // The binding landed; recording Carol's new key comes next.
    let record = ana.database().first_operation(INVITE).await.unwrap().unwrap();
    let mut work = InviteWork::read(&record).unwrap();
    work.state = InviteState::Recording;
    ana.database().advance_operation(work.update(&record).unwrap()).await.unwrap();
    ana.sync
        .administer(coven_format::admin::StoreChange::RemoveMember { member: carol.member() })
        .await
        .unwrap();
    ana.sync.pass().await.unwrap();
    assert!(ana.database().operations().await.unwrap().is_empty());
    assert!(ana.database().operation_pending::<PendingRecord>(record.id).await.unwrap().is_none());
    ana.database().close().await.unwrap();
}

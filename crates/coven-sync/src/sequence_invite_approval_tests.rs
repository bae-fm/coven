use crate::sequence_sync::sequence_invites::tests::{
    invite, join, no_request, request_path, requests,
};
use crate::sequence_sync::sequence_invites::{InviteState, InviteWork};
use crate::sequence_sync::tests::*;
use crate::sequence_sync::OperationState;
use crate::*;
use coven_database::operation_kind;
use coven_database::DbError;
use coven_format::{admin::MemberRole, MemberAccess};
use coven_merge::Audience;
use coven_storage::{ObjectPath, Storage};

/// Ana approves Carol's checked request. One admission admits Carol with the
/// request's pair, carries her copies and registers her phone, which writes
/// without registering itself (§13.2).
#[tokio::test]
async fn an_approved_request_admits_its_pair_and_registers_its_device() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group.create(&ana).await;
    insert(&ana.sync.database, "a", "before the invite").await;
    ana.sync.pass().await.unwrap();
    let invite = invite(&ana).await;
    let mut carol = join(&group, invite.shown_code(), "Carol's phone").await;
    assert_eq!(
        carol.device.sync.join_outcome(&mut carol.request).await.unwrap(),
        JoinOutcome::Waiting
    );
    ana.sync.pass().await.unwrap();
    let checked = requests(&ana).await;
    assert_eq!(checked.len(), 1);
    assert_eq!(checked[0].member, invite.member);
    assert_eq!(checked[0].device_name, "Carol's phone");
    assert_eq!(checked[0].provider_account_email.as_deref(), Some("carol@example.org"));
    // Ana's number names the keys Carol's phone made for its request.
    assert_eq!(
        checked[0].confirmation,
        coven_crypto::ConfirmationNumber::of(
            invite.id,
            &carol.request.identity().keys().public_keys()
        )
    );
    let count = ana.storage.request_count();
    ana.sync.pass().await.unwrap();
    assert_eq!(ana.storage.request_count() - count, 2, "a checked request waits without reads");
    ana.sync.approve_join_request(&checked[0]).await.unwrap();
    assert!(no_request(&group.storage, &request_path(&invite)).await);
    assert_eq!(
        carol.device.sync.join_outcome(&mut carol.request).await.unwrap(),
        JoinOutcome::Admitted
    );
    // The admission registered Carol's phone: it applies it and writes.
    carol.device.sync.pass().await.unwrap();
    let state = carol.device.sync.database.store_state().await.unwrap();
    assert_eq!(state.devices[&carol.device.sync.device].member, invite.member);
    assert_eq!(rows(&carol.device.sync.database).await, rows(&ana.sync.database).await);
    insert(&carol.device.sync.database, "c", "from Carol").await;
    carol.device.sync.pass().await.unwrap();
    ana.sync.pass().await.unwrap();
    assert_eq!(rows(&ana.sync.database).await.len(), 2);
    assert!(requests(&ana).await.is_empty());
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

/// Once Ana's approval has begun, its admission may already be on its way:
/// approving, declining and cancelling all fail with `InvitationChanged`,
/// and the approval finishes on retry (§13.2).
#[tokio::test]
async fn an_approval_under_way_refuses_the_apps_decisions() {
    use coven_storage::test_utils::Faults;
    let group = Group::shared();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let invite = invite(&ana).await;
    let mut carol = join(&group, invite.shown_code(), "Carol's phone").await;
    ana.sync.pass().await.unwrap();
    let request = requests(&ana).await.remove(0);
    approve(&ana, &request).await;
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
    assert!(matches!(open[0].stage, InviteStage::Approving { .. }), "{:?}", open[0].stage);
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
        carol.device.sync.join_outcome(&mut carol.request).await.unwrap(),
        JoinOutcome::Admitted
    );
    for device in [&ana, &carol.device] {
        device.sync.database.close().await.unwrap();
    }
}

/// Ana's approval posts one slot, the admission carrying Carol's copies.
/// While that post fails, a check finds no copy and Carol's join keeps
/// waiting; once it lands, the next check finds her copies and her admission
/// together and she is admitted. No check finds one without the other
/// (§13.2).
#[tokio::test]
async fn a_join_waits_until_its_admission_lands_whole() {
    use coven_storage::test_utils::Faults;
    let group = Group::shared();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let invite = invite(&ana).await;
    let mut carol = join(&group, invite.shown_code(), "Carol's phone").await;
    ana.sync.pass().await.unwrap();
    approve(&ana, &requests(&ana).await.remove(0)).await;
    let admission = ana.sync.database.applied_slot().await.unwrap() + 1;
    ana.storage
        .set_faults(Faults {
            create_failures: [(
                ObjectPath::slot(group.store, admission.try_into().unwrap()),
                (coven_storage::StorageFailure::Network, 500),
            )]
            .into(),
            ..Faults::none()
        })
        .await;
    ana.sync.pass().await.unwrap();
    for _ in 0..2 {
        assert_eq!(
            carol.device.sync.join_outcome(&mut carol.request).await.unwrap(),
            JoinOutcome::Waiting
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
        carol.device.sync.join_outcome(&mut carol.request).await.unwrap(),
        JoinOutcome::Admitted
    );
    for device in [&ana, &carol.device] {
        device.sync.database.close().await.unwrap();
    }
}

/// Ana's approval hands Carol every store key, and Ben's phone syncs while
/// it is under way. Carol's keys reach her only with the entry admitting
/// her, so at no point does a key's copy name someone who isn't a member:
/// Ben's pass finds no store key exposed and rotates none (§12.2, §13.2).
#[tokio::test]
async fn no_device_rotates_the_store_key_during_an_approval() {
    use coven_storage::test_utils::Faults;
    let group = Group::shared();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    ben.sync.pass().await.unwrap();
    let store_keys = |device: &Device| {
        let device = device.sync.clone();
        async move {
            let session = device.session.lock().await;
            let introductions = &session.history.introductions;
            introductions.iter().filter(|intro| intro.audience == Audience::Store).count()
        }
    };
    let before = store_keys(&ben).await;
    let invite = invite(&ana).await;
    let carol = join(&group, invite.shown_code(), "Carol's phone").await;
    ana.sync.pass().await.unwrap();
    approve(&ana, &requests(&ana).await.remove(0)).await;
    // The approval's first slot lands; whatever it posts after fails.
    let second = ana.sync.database.applied_slot().await.unwrap() + 2;
    ana.storage
        .set_faults(Faults {
            create_failures: [(
                ObjectPath::slot(group.store, second.try_into().unwrap()),
                (coven_storage::StorageFailure::Network, 500),
            )]
            .into(),
            ..Faults::none()
        })
        .await;
    ana.sync.pass().await.unwrap();
    // Ben's phone reaches storage while Ana's waits to retry.
    ana.storage.set_faults(Faults::none()).await;
    ben.sync.pass().await.unwrap();
    assert_eq!(store_keys(&ben).await, before, "Ben's pass rotated the store key");
    for device in [&ana, &ben, &carol.device] {
        device.sync.database.close().await.unwrap();
    }
}

/// Ben demotes Ana, and her phone has applied it, when her app approves
/// Carol's request. The write doesn't check the approval against the
/// applied state: it journals the binding, and the pass that catches up
/// refuses it. The refusal is the binding's outcome, and the invite ends as
/// a decline does. So an approval the app is offered is never one its write
/// refuses (§11.1, §13.2).
#[tokio::test]
async fn a_demoted_approvers_approval_is_refused_by_its_pass() {
    let group = Group::shared();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let invite = invite(&ana).await;
    let mut carol = join(&group, invite.shown_code(), "Carol's phone").await;
    ana.sync.pass().await.unwrap();
    ben.sync.pass().await.unwrap();
    ben.sync
        .administer(coven_format::admin::StoreChange::ChangeRole {
            member: ana.identity.member(),
            role: MemberRole::Member,
        })
        .await
        .unwrap();
    ana.sync.pass().await.unwrap();
    let request = requests(&ana).await.remove(0);
    let binding = approve(&ana, &request).await;
    assert!(matches!(listed(&ana).await[0].stage, InviteStage::Approving { .. }));
    ana.sync.pass().await.unwrap();
    assert_eq!(
        outcome(&ana, binding).await,
        Some(OperationOutcome::Rejected(OperationRejection::NotAllowed))
    );
    assert!(listed(&ana).await.is_empty());
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
        carol.device.sync.join_outcome(&mut carol.request).await.unwrap(),
        JoinOutcome::Declined
    );
    for device in [&ana, &ben, &carol.device] {
        device.sync.database.close().await.unwrap();
    }
}

/// Approve the request as the app's write step does, without a pass, and
/// return the operation that binds its pair.
async fn approve(device: &Device, request: &JoinRequest) -> coven_database::OperationId {
    let (steps, request) = (device.sync.operation_steps(), request.clone());
    device.database().write_as(move |sql| steps.approve_join_request(&sql, &request)).await.unwrap()
}

/// The operation's outcome, as `coven_operations` lists it.
async fn outcome(
    device: &Device,
    operation: coven_database::OperationId,
) -> Option<OperationOutcome> {
    device
        .database()
        .read(move |sql| {
            Ok::<_, DbError>(sql.query_row(
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
        group.create(&ana).await;
        group.admit(&ana, &ben).await;
        let target = coven_foundation::id_source::MemberId(uuid::Uuid::from_u128(91));
        ana.sync
            .administer(coven_format::admin::StoreChange::AdmitMember {
                member: target,
                keys: coven_format::test_utils::keys_from_seed(3).public_keys(),
                role: MemberRole::Member,
                access: MemberAccess::ProviderAccount("carol@example.org".into()),
                device: coven_foundation::id_source::DeviceId(81),
                device_name: "Carol's old phone".into(),
            })
            .await
            .unwrap();
        let invite = key_replacement_invite(&ana, target).await;
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
                        request_path(&invite),
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
        assert!(no_request(&group.storage, &request_path(&invite)).await);
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
        .administer(coven_format::admin::StoreChange::AdmitMember {
            member: carol.member(),
            keys: carol.keys().public_keys(),
            role: MemberRole::Member,
            access: access("carol-old"),
            device: coven_foundation::id_source::DeviceId(81),
            device_name: "Carol's old phone".into(),
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
            InviteLifetime::Day,
        )
        .await
        .unwrap();
    // The binding landed; recording Carol's new key comes next.
    let record = ana.database().first_operation(operation_kind::INVITE).await.unwrap().unwrap();
    let mut work = InviteWork::read(&record).unwrap();
    work.state = InviteState::Recording { set_access: None };
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

/// Ana approves Carol's request, and the pass that runs the approval commits
/// a transaction at a time. A crash after any of them leaves the database as
/// it was at that commit; at each, the approval under way is listed exactly
/// while its binding's operation runs. Ending the binding apart from the
/// invite left, after a crash between the two, the binding's outcome listed
/// beside an approval still under way (§11.4, §13.2).
#[tokio::test]
async fn a_crash_between_any_two_commits_leaves_the_approval_and_its_binding_agreeing() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let invite = invite(&ana).await;
    let carol = join(&group, invite.shown_code(), "Carol's phone").await;
    ana.sync.pass().await.unwrap();
    let binding = approve(&ana, &requests(&ana).await.remove(0)).await;
    // Copy the installation as each commit leaves it.
    let commits = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let (source, copies) = (ana._temp.path().to_owned(), commits.clone());
    ana.database()
        .test_on_write_checkpoint(move |point| {
            if point == coven_database::test_utils::WriteCheckpoint::Committed {
                let copy = tempfile::tempdir().unwrap();
                copy_committed(&source, copy.path());
                copies.lock().unwrap().push(copy);
            }
        })
        .await
        .unwrap();
    ana.sync.pass().await.unwrap();
    let commits = std::mem::take(&mut *commits.lock().unwrap());
    assert!(commits.len() > 1, "the approval commits more than once");
    for (at, copy) in commits.into_iter().enumerate() {
        let crashed = group.open(copy, ana.identity.clone()).await;
        let approving = listed(&crashed)
            .await
            .into_iter()
            .any(|listed| matches!(listed.stage, InviteStage::Approving { .. }));
        let running = crashed.database().operation(binding).await.unwrap().is_some();
        let ended = outcome(&crashed, binding).await.is_some();
        assert_eq!((approving, running, ended), (approving, approving, !approving), "commit {at}");
        crashed.database().close().await.unwrap();
    }
    for device in [&ana, &carol.device] {
        device.sync.database.close().await.unwrap();
    }
}

/// Copy an open installation as its last commit left it, as a crash then
/// would: its database and write-ahead log, without the shared-memory index
/// SQLite rebuilds from the log on opening, or the lock files, which the
/// running installation holds locked and opening makes again. Windows
/// refuses to read a range another process has locked.
fn copy_committed(from: &std::path::Path, to: &std::path::Path) {
    coven_foundation::files::copy_tree_keeping(from, to, &|name| {
        !name.ends_with(".lock") && !name.ends_with("-shm")
    });
}

/// Carol's invite and the request path its code names, with the member id
/// the invite fixed for her.
async fn carol_request_slot(
    group: &Group,
    ana: &Device,
) -> (coven_format::codes::InviteCode, coven_foundation::id_source::MemberId) {
    let invite = invite(ana).await;
    let code = read_invite_code(invite.shown_code()).unwrap();
    let record = ana
        .sync
        .database
        .operations()
        .await
        .unwrap()
        .into_iter()
        .find(|record| record.kind == operation_kind::INVITE)
        .unwrap();
    let work: InviteWork = serde_json::from_slice(&record.data).unwrap();
    assert!(no_request(&group.storage, &code.request_path()).await);
    (code, work.member)
}

/// Publish `request` at the invite's request path, signed by `signer`.
async fn publish_request(
    group: &Group,
    code: &coven_format::codes::InviteCode,
    request: &coven_format::objects::JoinRequest,
    signer: &coven_crypto::MemberKeys,
) {
    let path = code.request_path();
    let bytes = sealed_join_request(&code.secret, &path, request, signer).unwrap();
    group.storage.create(&path, &bytes).await.unwrap();
}

/// A request that fails one of Ana's checks, among them one naming a device
/// id history registered, published at her invite's one path, ends the
/// invite in the pass that reads it, as a decline does: no
/// attempt could pass it, and no other request can reach that path. The
/// invite leaves her list with no pending record and no binding journaled,
/// its marker takes the request's place, and the next pass takes back
/// Carol's account (§13.2).
#[tokio::test]
async fn a_request_failing_a_check_ends_its_invite() {
    for check in ["signature", "member", "keys", "device"] {
        let group = Group::shared();
        let ana = group.device(1).await;
        let ben = group.device(2).await;
        group.create(&ana).await;
        group.admit(&ana, &ben).await;
        let (code, member) = carol_request_slot(&group, &ana).await;
        let operation = listed(&ana).await[0].operation;
        let carol = coven_crypto::MemberKeys::generate().unwrap();
        let (keys, member, signer) = match check {
            // Signed by a pair other than the one it names.
            "signature" => (carol.public_keys(), member, ben.identity.keys()),
            // Naming another member than the invite fixed.
            "member" => (
                carol.public_keys(),
                coven_foundation::id_source::MemberId(uuid::Uuid::from_u128(99)),
                &carol,
            ),
            // Ben's pair, which history bound when he joined.
            "keys" => (ben.identity.keys().public_keys(), member, ben.identity.keys()),
            // A fresh pair naming Ben's phone, which history registered.
            _ => (carol.public_keys(), member, &carol),
        };
        let device = match check {
            "device" => ben.sync.device,
            _ => coven_foundation::id_source::DeviceId(70),
        };
        let request = coven_format::objects::JoinRequest {
            invite: code.invite,
            member,
            keys,
            device,
            device_name: "Carol's phone".into(),
        };
        publish_request(&group, &code, &request, signer).await;
        let pass = ana.sync.pass().await;
        assert!(pass.is_ok(), "{check}: {:?}", pass.err());
        assert!(listed(&ana).await.is_empty(), "{check}");
        assert_eq!(
            ana.sync.database.operation_pending::<PendingRecord>(operation).await.unwrap(),
            None,
            "{check}"
        );
        let journaled = ana.sync.database.operations().await.unwrap();
        assert!(
            journaled.iter().all(|record| {
                record.kind != operation_kind::INVITE && record.kind != operation_kind::SEQUENCE
            }),
            "{check}: the invite ended, and no binding was journaled"
        );
        let marker = group.storage.read(&code.request_path()).await.unwrap();
        assert!(
            coven_format::invite_end::InviteEnd::decode(&marker).is_ok(),
            "{check}: the marker takes the request's place"
        );
        ana.sync.pass().await.unwrap();
        assert!(
            !group.storage.shared_with("carol@example.org").await,
            "{check}: the account goes back"
        );
        for device in [&ana, &ben] {
            device.sync.database.close().await.unwrap();
        }
    }
}

/// Ana's phone made Carol's invite and holds its secret; her laptop, a
/// second device of hers, lists no invite and its approve, decline and
/// cancel all fail, leaving the phone's invite as it was (§13.2).
#[tokio::test]
async fn only_the_inviting_device_lists_and_decides_its_invite() {
    let group = Group::shared();
    let phone = group.device(1).await;
    group.create(&phone).await;
    let laptop = group.laptop(&phone).await;
    let invite = invite(&phone).await;
    let carol = join(&group, invite.shown_code(), "Carol's phone").await;
    phone.sync.pass().await.unwrap();
    laptop.sync.pass().await.unwrap();
    let [request] = requests(&phone).await.try_into().expect("the phone shows Carol's request");
    assert!(listed(&laptop).await.is_empty());
    for result in [
        laptop.sync.approve_join_request(&request).await,
        laptop.sync.decline_join_request(&request).await,
        laptop.sync.cancel_invite(invite.id).await,
    ] {
        assert!(matches!(result, Err(SyncError::InvitationChanged)), "{result:?}");
    }
    assert!(laptop.sync.database.operations().await.unwrap().is_empty());
    assert_eq!(requests(&phone).await, [request]);
    assert!(!no_request(&group.storage, &request_path(&invite)).await);
    for device in [&phone, &laptop, &carol.device] {
        device.sync.database.close().await.unwrap();
    }
}

/// Ana's phone rotated the store key, then lost the older one from custody,
/// as when its copy has not reached the phone yet. Carol's approval needs a
/// copy of every store key in history, so it claims no copy and no entry,
/// and stays journaled with its pending record, even when retried; once the
/// phone holds the key the same approval lands (§13.2, §12.1).
#[tokio::test]
async fn an_approval_missing_a_key_it_must_copy_waits_for_it() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group.create(&ana).await;
    ana.sync.rotate_key(Audience::Store).await.unwrap();
    let invite = invite(&ana).await;
    let carol = join(&group, invite.shown_code(), "Carol's phone").await;
    ana.sync.pass().await.unwrap();
    let [request] = requests(&ana).await.try_into().expect("the phone shows Carol's request");
    let full = ana.sync.keys.read().unwrap().unwrap();
    let newest = ana.sync.session.lock().await.history.metadata_key(&full).unwrap();
    assert_eq!(full.store_key_ids().count(), 2, "the rotation leaves an older key");
    ana.sync
        .keys
        .persist(&coven_crypto::StoreKeyring::new(full.store_key(newest).unwrap().clone()))
        .unwrap();
    let applied = ana.sync.database.applied_slot().await.unwrap();
    let binding = approve(&ana, &request).await;
    let invite_operation = ana
        .sync
        .database
        .operations()
        .await
        .unwrap()
        .into_iter()
        .find(|record| record.kind == operation_kind::INVITE && record.id != binding)
        .unwrap()
        .id;
    let member = request.member;
    let enrolled =
        || async { ana.sync.database.store_state().await.unwrap().members.contains_key(&member) };
    ana.sync.pass().await.unwrap();
    ana.sync.retry_pending_operation(invite_operation).await.unwrap();
    assert_eq!(ana.sync.database.applied_slot().await.unwrap(), applied, "no copy, no entry");
    assert_eq!(outcome(&ana, binding).await, None);
    assert!(!enrolled().await);
    assert!(ana
        .sync
        .database
        .operation_pending::<PendingRecord>(invite_operation)
        .await
        .unwrap()
        .is_some());
    ana.sync.keys.persist(&full).unwrap();
    ana.sync.retry_pending_operation(invite_operation).await.unwrap();
    assert!(enrolled().await);
    assert!(matches!(outcome(&ana, binding).await, Some(OperationOutcome::Landed(_))));
    for device in [&ana, &carol.device] {
        device.sync.database.close().await.unwrap();
    }
}

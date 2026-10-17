use crate::sequence_sync::tests::*;
use crate::*;
use coven_format::{admin::MemberRole, MemberAccess};
use coven_storage::{test_utils::MemoryStorage, InviteStorage, ObjectPath, Storage};

struct Joiner {
    device: Device,
    request: JoiningIdentity,
}

async fn join(group: &Group, code: &str, name: &str) -> Joiner {
    let code = read_invite_code(code).unwrap();
    let InviteStorage::Account(invitation) =
        InviteStorage::decode(code.storage.as_bytes()).unwrap()
    else {
        panic!("an account invite");
    };
    let (temp, directory) = group.directory();
    let request = JoiningIdentity::prepare(&directory, &code, name).unwrap();
    let storage = MemoryStorage::for_recipient(&group.storage, "carol@example.org").unwrap();
    let device = group
        .open_with(temp, request.identity(), None, storage)
        .await;
    device.sync.accept_join(&invitation).await.unwrap();
    device.sync.submit_join(&request).await.unwrap();
    Joiner { device, request }
}

async fn invite(ana: &Device) -> Invite {
    ana.sync
        .create_invite(
            InviteTarget::NewMember {
                role: MemberRole::Member,
            },
            InviteAccess::ProviderAccount {
                email: "Carol@example.org".into(),
            },
        )
        .await
        .unwrap()
}

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
    // the provider's permission failure reaches the app (§12.2).
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

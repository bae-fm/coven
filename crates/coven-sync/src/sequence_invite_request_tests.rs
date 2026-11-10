use super::*;
use crate::sequence_sync::sequence_invites::tests::{
    accepted_invitation, carol_for, invite, request_path, requests,
};
use crate::sequence_sync::tests::*;
use crate::*;
use coven_database::operation_kind;
use coven_foundation::files::StoreDirAccess;
use coven_storage::Storage;

/// A join request signed by its own pair, naming a member other than the one
/// its invite fixed, is refused as the wrong identity and binds nothing; the
/// same request naming that member passes (§13.2, format.md D13).
#[tokio::test]
async fn a_request_naming_another_member_than_its_invite_is_refused() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let invite = invite(&ana).await;
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
    let state = coven_database::StoreState::default();
    let keys = coven_crypto::MemberKeys::generate().unwrap();
    let request = |member| coven_format::objects::JoinRequest {
        invite: code.invite,
        member,
        keys: keys.public_keys(),
        device: coven_foundation::id_source::DeviceId(70),
        device_name: "Carol's phone".into(),
        joiner: carol_for(&code),
    };
    let sealed = |request: &coven_format::objects::JoinRequest| {
        sealed_join_request(&code.secret, &code.request_path(), request, &keys).unwrap()
    };

    let other = coven_foundation::id_source::MemberId(uuid::Uuid::from_u128(99));
    assert_ne!(other, work.member);
    let refused = ana.sync.open_request(&work, &state, &sealed(&request(other))).unwrap();
    assert!(
        matches!(
            refused,
            Opened::Refused(SyncError::Damaged(ref damaged))
                if matches!(damaged.failure, Refusal::WrongIdentity { .. })
        ),
        "a request naming another member"
    );
    let accepted = ana.sync.open_request(&work, &state, &sealed(&request(work.member))).unwrap();
    assert!(matches!(accepted, Opened::Checked(_)));
    ana.sync.database.close().await.unwrap();
}

/// A join request is checked as signed by the pair it names before what it
/// says is compared, as a saved request is: one naming another member and
/// not signed by its own pair is refused as unsigned (§13.2).
#[tokio::test]
async fn a_request_not_signed_by_its_pair_is_refused_before_its_fields() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let invite = invite(&ana).await;
    let code = read_invite_code(invite.shown_code()).unwrap();
    let named = coven_crypto::MemberKeys::generate().unwrap();
    let signer = coven_crypto::MemberKeys::generate().unwrap();
    let forged = coven_format::objects::JoinRequest {
        invite: code.invite,
        member: coven_foundation::id_source::MemberId(uuid::Uuid::from_u128(99)),
        keys: named.public_keys(),
        device: coven_foundation::id_source::DeviceId(70),
        device_name: "forged".into(),
        joiner: carol_for(&code),
    };
    let bytes = sealed_join_request(&code.secret, &code.request_path(), &forged, &signer).unwrap();
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
    let state = coven_database::StoreState::default();
    let result = ana.sync.open_request(&work, &state, &bytes).unwrap();
    assert!(matches!(result, Opened::Refused(SyncError::Crypto(_))));
    ana.sync.database.close().await.unwrap();
}

/// A request takes only its invite's kind: one that names a person for a
/// recovery invite, or a recovery for a new person's, is refused as the
/// wrong identity and binds nothing (§13.2).
#[tokio::test]
async fn a_request_of_the_other_kind_than_its_invite_is_refused() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let invite = invite(&ana).await;
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
    let new_person_invite: InviteWork = serde_json::from_slice(&record.data).unwrap();
    assert_eq!(new_person_invite.kind(), InviteKind::NewPerson);
    let mut recovery_invite: InviteWork = serde_json::from_slice(&record.data).unwrap();
    let keys = coven_crypto::MemberKeys::generate().unwrap();
    recovery_invite.binding = InviteBinding::Replace { expected: keys.public_keys() };
    assert_eq!(recovery_invite.kind(), InviteKind::Recovery);
    let state = coven_database::StoreState::default();
    let sealed = |joiner| {
        let request = coven_format::objects::JoinRequest {
            invite: code.invite,
            member: code.member,
            keys: keys.public_keys(),
            device: coven_foundation::id_source::DeviceId(70),
            device_name: "Carol's phone".into(),
            joiner,
        };
        sealed_join_request(&code.secret, &code.request_path(), &request, &keys).unwrap()
    };
    let person = || Joiner::NewPerson { name: "Carol".into() };
    let refused = |opened: Opened| {
        matches!(
            opened,
            Opened::Refused(SyncError::Damaged(ref damaged))
                if matches!(damaged.failure, Refusal::WrongIdentity { .. })
        )
    };
    let recovery_for_person =
        ana.sync.open_request(&new_person_invite, &state, &sealed(Joiner::Recovery)).unwrap();
    assert!(refused(recovery_for_person), "a recovery's request for a new person's invite");
    let person_for_recovery =
        ana.sync.open_request(&recovery_invite, &state, &sealed(person())).unwrap();
    assert!(refused(person_for_recovery), "a new person's request for a recovery's invite");
    let matching = ana.sync.open_request(&new_person_invite, &state, &sealed(person())).unwrap();
    assert!(matches!(matching, Opened::Checked(_)));
    let matching =
        ana.sync.open_request(&recovery_invite, &state, &sealed(Joiner::Recovery)).unwrap();
    assert!(matches!(matching, Opened::Checked(_)));
    ana.sync.database.close().await.unwrap();
}

/// A new person's request carries the name they typed, sealed as the rest of
/// the request is, and the admin's approval sees it with the request; the
/// invite itself carries no name (§13.2).
#[tokio::test]
async fn a_new_persons_request_carries_the_name_they_typed_to_the_approval() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let invite = invite(&ana).await;
    assert_eq!(invite.kind, InviteKind::NewPerson);
    let code = read_invite_code(invite.shown_code()).unwrap();
    let (temp, directory) = group.directory();
    let request = JoiningIdentity::prepare(
        &directory,
        &code,
        directory.settings().unwrap().device_id,
        "Carol's phone",
        Joiner::NewPerson { name: "Carol Danvers".into() },
    )
    .unwrap();
    let carol = group.open_with(temp, request.identity(), None, group.storage.for_device()).await;
    carol.sync.accept_join(&accepted_invitation(&code)).await.unwrap();
    assert!(carol.sync.submit_join(&request).await.unwrap());
    // The stored request is sealed: the name is nowhere in its bytes.
    let stored = group.storage.read(&request_path(&invite)).await.unwrap();
    assert!(!stored.windows(5).any(|window| window == b"Carol"));
    ana.sync.pass().await.unwrap();
    let shown = requests(&ana).await;
    assert_eq!(shown.len(), 1);
    assert_eq!(shown[0].joiner, Joiner::NewPerson { name: "Carol Danvers".into() });
    ana.database().close().await.unwrap();
    carol.database().close().await.unwrap();
}

/// A join gives its person to the kind its invite is for, or fails before it
/// changes anything (§13.2).
#[tokio::test]
async fn a_join_of_the_other_kind_than_its_invite_fails_before_it_begins() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let invite = invite(&ana).await;
    let code = read_invite_code(invite.shown_code()).unwrap();
    let (_temp, directory) = group.directory();
    let result = JoiningIdentity::prepare(
        &directory,
        &code,
        directory.settings().unwrap().device_id,
        "Carol's phone",
        Joiner::Recovery,
    );
    assert!(matches!(
        result,
        Err(SyncError::Code(CodeError::WrongJoiner {
            expected: InviteKind::NewPerson,
            actual: InviteKind::Recovery
        }))
    ));
    ana.database().close().await.unwrap();
}

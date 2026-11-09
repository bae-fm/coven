use super::*;
use crate::sequence_sync::sequence_invites::tests::invite;
use crate::sequence_sync::tests::*;
use crate::*;
use coven_database::operation_kind;

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

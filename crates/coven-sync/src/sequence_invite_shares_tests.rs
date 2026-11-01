use crate::sequence_sync::tests::*;
use crate::*;
use coven_format::{
    admin::{MemberRole, StoreChange},
    MemberAccess,
};
use coven_foundation::clock::Clock;
use coven_storage::test_utils::MemoryRequest;

const CAROL: &str = "carol@example.org";

/// Ana's phone makes Household on Dropbox and admits Ben, an admin with his
/// own account; then
/// Ana's laptop, holding the same account that holds the store, registers.
async fn household(group: &Group) -> (Device, Device, Device) {
    let phone = group.device(1).await;
    let ben = group.device(2).await;
    group.create_with_access(&phone, MemberAccess::ProviderAccount("ana@example.org".into())).await;
    phone
        .sync
        .administer(StoreChange::AddMember {
            member: ben.identity.member(),
            keys: ben.identity.keys().public_keys(),
            role: MemberRole::Admin,
            access: MemberAccess::ProviderAccount("ben@example.org".into()),
        })
        .await
        .unwrap();
    ben.sync
        .administer(StoreChange::AddDevice { name: "Ben's phone".into(), replaces: None })
        .await
        .unwrap();
    let (temp, _) = group.directory();
    let laptop = group.open(temp, phone.identity.clone()).await;
    laptop
        .sync
        .administer(StoreChange::AddDevice { name: "Ana's laptop".into(), replaces: None })
        .await
        .unwrap();
    phone.sync.pass().await.unwrap();
    ben.sync.pass().await.unwrap();
    (phone, ben, laptop)
}

/// The account's row on `device`: whether it is meant to be shared, and
/// whether a grant is confirmed.
async fn row(device: &Device, account: &str) -> Option<(bool, Option<bool>)> {
    device
        .database()
        .provider_access(account.into())
        .await
        .unwrap()
        .map(|access| (access.intended, access.granted))
}

fn grants(device: &Device) -> usize {
    device
        .storage
        .request_trace()
        .into_iter()
        .filter(|request| *request == MemoryRequest::Other("grant_access"))
        .count()
}

/// Ben removes Ana's phone while its invite to Carol is open and shared. The
/// phone never reads storage again, so it can't take the share back. Ana's
/// laptop reads the share from history: applying the removal, it revokes
/// Carol's account. It never granted it: only the phone, which made the
/// invite, did. Ben's device holds no account, so it records nothing for an
/// invite's share (§13.2, §14.2).
#[tokio::test]
async fn the_owners_other_device_revokes_the_share_of_a_device_gone_for_good() {
    let group = Group::shared();
    let (phone, ben, laptop) = household(&group).await;
    invite(&phone).await;
    assert!(group.storage.shared_with(CAROL).await);
    laptop.sync.pass().await.unwrap();
    assert_eq!(grants(&laptop), 0, "only the inviting device grants");
    ben.sync.administer(StoreChange::RemoveDevice { device: phone.sync.device }).await.unwrap();
    laptop.sync.pass().await.unwrap();
    assert!(!group.storage.shared_with(CAROL).await);
    assert_eq!(row(&laptop, CAROL).await, Some((false, Some(false))));
    assert_eq!(row(&ben, CAROL).await, None);
    for device in [&phone, &ben, &laptop] {
        device.database().close().await.unwrap();
    }
}

/// The laptop has its own open invite to Carol when Ben removes the phone.
/// Sharing is per account, so the laptop keeps Carol's account shared for
/// its invite, and takes it back only when that invite ends too (§13.2).
#[tokio::test]
async fn a_gone_devices_share_stays_while_another_devices_invite_is_open() {
    let group = Group::shared();
    let (phone, ben, laptop) = household(&group).await;
    invite(&phone).await;
    let own = invite(&laptop).await;
    ben.sync.pass().await.unwrap();
    ben.sync.administer(StoreChange::RemoveDevice { device: phone.sync.device }).await.unwrap();
    laptop.sync.pass().await.unwrap();
    assert!(group.storage.shared_with(CAROL).await);
    assert_eq!(row(&laptop, CAROL).await.map(|(intended, _)| intended), Some(true));
    laptop.sync.cancel_invite(own.id).await.unwrap();
    // The pass that ends the invite applies its share's end; the next sends
    // the revoke that end recorded.
    laptop.sync.pass().await.unwrap();
    assert!(!group.storage.shared_with(CAROL).await);
    for device in [&phone, &ben, &laptop] {
        device.database().close().await.unwrap();
    }
}

/// The phone shows Carol a second invite in person, and she joins with it:
/// her account is now an active member's. When Ben removes the phone, its
/// first invite's share ends with it, and the laptop keeps the account
/// shared for Carol (§14.2).
#[tokio::test]
async fn a_gone_devices_share_stays_for_an_active_member() {
    let group = Group::shared();
    let (phone, ben, laptop) = household(&group).await;
    invite(&phone).await;
    let in_person = phone
        .sync
        .create_invite(
            InviteTarget::NewMember { role: MemberRole::Member, name: "Carol".into() },
            InviteAccess::ProviderAccount { email: CAROL.into() },
            InviteDelivery::InPerson,
        )
        .await
        .unwrap();
    let mut carol = join(&group, in_person.shown_code(), "Carol's phone").await;
    group.clock.set(group.clock.now() + std::time::Duration::from_secs(1));
    phone.sync.pass().await.unwrap();
    assert_eq!(
        carol.device.sync.join_outcome(&mut carol.request).await.unwrap(),
        JoinOutcome::Admitted
    );
    ben.sync.pass().await.unwrap();
    ben.sync.administer(StoreChange::RemoveDevice { device: phone.sync.device }).await.unwrap();
    laptop.sync.pass().await.unwrap();
    assert!(group.storage.shared_with(CAROL).await);
    assert_eq!(row(&laptop, CAROL).await.map(|(intended, _)| intended), Some(true));
    for device in [&phone, &ben, &laptop, &carol.device] {
        device.database().close().await.unwrap();
    }
}

/// Ana cancels the phone's invite. The phone records the share's end in
/// history and revokes the account when it applies that entry; the laptop
/// applies the same entry and takes the account back too, without ever
/// having granted it. Neither leaves provider work behind (§13.2, §14.2).
#[tokio::test]
async fn an_ended_invites_share_is_revoked_by_every_owner_device() {
    let group = Group::shared();
    let (phone, ben, laptop) = household(&group).await;
    let invite = invite(&phone).await;
    laptop.sync.pass().await.unwrap();
    phone.sync.cancel_invite(invite.id).await.unwrap();
    phone.sync.pass().await.unwrap();
    assert!(!group.storage.shared_with(CAROL).await);
    assert_eq!(row(&phone, CAROL).await, Some((false, Some(false))));
    laptop.sync.pass().await.unwrap();
    assert_eq!(row(&laptop, CAROL).await, Some((false, Some(false))));
    assert_eq!(grants(&laptop), 0);
    for device in [&phone, &laptop] {
        let operations = device.database().operations().await.unwrap();
        assert!(operations.is_empty(), "{operations:?}");
    }
    for device in [&phone, &ben, &laptop] {
        device.database().close().await.unwrap();
    }
}

use crate::sequence_sync::tests::*;
use crate::*;
use coven_format::{
    admin::{MemberRole, StoreChange},
    MemberAccess,
};
use coven_foundation::clock::Clock;
use coven_storage::test_utils::{Faults, MemoryRequest};

const CAROL: &str = "carol@example.org";

/// Ana's phone makes Household on Dropbox and admits Ben, an admin with his
/// own account; then
/// Ana's laptop, holding the same account that holds the store, registers.
async fn household(group: &Group) -> (Device, Device, Device) {
    let phone = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&phone).await;
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
    let laptop = group.laptop(&phone).await;
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
    let in_person = carol_invite(&phone, crate::InviteDelivery::InPerson).await;
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

/// Run `during` while the next slot `device` creates is stored with its
/// reply lost: the claim lands, and the device hears nothing back, as when
/// it stops before the reply.
async fn losing_the_next_claims_reply<T>(
    group: &Group,
    device: &Device,
    during: impl std::future::Future<Output = T>,
) -> T {
    let (started, observed) = tokio::sync::oneshot::channel();
    let (resume, held) = tokio::sync::oneshot::channel();
    device
        .storage
        .hold_next_creation(coven_storage::ObjectPrefix::slots(group.store), started, held)
        .await;
    let lose = async {
        observed.await.unwrap();
        device.storage.set_faults(Faults { lose_completion_reply: true, ..Faults::none() }).await;
        resume.send(()).unwrap();
    };
    let (result, ()) = tokio::join!(during, lose);
    result
}

/// Reopen `device`, whose claim landed unfinished, as restarting after a
/// crash does, past the invite's wait for the failure.
async fn after_the_crash(group: &Group, device: Device) -> Device {
    let claim = device.database().current_claim().await.unwrap().unwrap();
    assert_eq!(claim.settlement, coven_database::ClaimSettlement::Unknown);
    let device = group.reopen(device).await;
    group.clock.set(group.clock.now() + std::time::Duration::from_secs(600));
    device
}

/// Whether `device`'s invites and their claims have all ended, and a write
/// it makes now goes out in its next pass.
async fn assert_ended_and_sending(device: &Device) {
    let operations = device.database().operations().await.unwrap();
    assert!(operations.is_empty(), "{operations:?}");
    assert_eq!(device.database().current_claim().await.unwrap(), None);
    insert(device.database(), "after", "the invite").await;
    device.sync.pass().await.unwrap();
    assert_eq!(device.database().unsent().await.unwrap().writes, 0, "the write went out");
}

/// Ana cancels the phone's invite, and its pass claims the end of the
/// invite's share. The provider stores the entry, and the phone stops before
/// the reply. Reopened, it reads its own entry: the share has ended. The
/// invite still ends the claim with it, so the phone's writes go on. Ending
/// the invite apart from its claim left that claim open for good, and every
/// later claim of the phone's refused (§11.4, §13.2).
#[tokio::test]
async fn a_crash_after_a_share_end_lands_ends_its_claim_with_the_invite() {
    let group = Group::shared();
    let phone = group.device(1).await;
    group.create(&phone).await;
    let invite = invite(&phone).await;
    losing_the_next_claims_reply(&group, &phone, phone.sync.cancel_invite(invite.id))
        .await
        .unwrap();
    let phone = after_the_crash(&group, phone).await;
    phone.sync.pass().await.unwrap();
    phone.sync.pass().await.unwrap();
    assert!(!group.storage.shared_with(CAROL).await);
    assert_ended_and_sending(&phone).await;
    phone.database().close().await.unwrap();
}

/// The phone's pass claims the share of Ana's new invite before granting
/// it, and the phone stops before the provider's reply, which stored the
/// entry. Reopened, before any pass, Ana cancels the invite, which hasn't
/// granted anything. The cancelled invite settles the share's claim it
/// started: the entry landed, so it ends the share, then ends the claims
/// with the invite, and the phone's writes go on. Ignoring the share's
/// claim left it open for good, and the share's end, refused by it, kept
/// the invite forever (§11.4, §13.2).
#[tokio::test]
async fn an_invite_cancelled_while_its_share_lands_settles_the_share() {
    let group = Group::shared();
    let phone = group.device(1).await;
    group.create(&phone).await;
    let made = losing_the_next_claims_reply(
        &group,
        &phone,
        phone.sync.create_invite(
            InviteTarget::NewMember { role: MemberRole::Member, name: "Carol".into() },
            InviteAccess::ProviderAccount { email: CAROL.into() },
            InviteDelivery::Sent { lifetime: InviteLifetime::Day },
        ),
    )
    .await;
    assert!(matches!(made, Err(SyncError::Pending(_))), "{made:?}");
    let invite = listed(&phone).await.remove(0);
    let phone = after_the_crash(&group, phone).await;
    phone.sync.cancel_invite(invite.id).await.unwrap();
    phone.sync.pass().await.unwrap();
    assert!(phone.sync.session.lock().await.history.invite_shares[&invite.id].ended);
    assert!(!group.storage.shared_with(CAROL).await);
    assert_ended_and_sending(&phone).await;
    phone.database().close().await.unwrap();
}

/// Ana's phone is taking Carol's account back, its invite ended, when
/// her laptop shares the account again for an invite of its own. The
/// revoke finishes; the account's work is not complete until the opposite
/// request reverses it, one request at a time (§20.9).
#[tokio::test]
async fn a_request_made_obsolete_while_it_runs_is_reversed_before_the_accounts_work_ends() {
    let group = Group::shared();
    let (phone, ben, laptop) = household(&group).await;
    let first = invite(&phone).await;
    laptop.sync.pass().await.unwrap();

    phone.sync.cancel_invite(first.id).await.unwrap();
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    phone.storage.hold_next_access(CAROL, false, entered, released).await;
    let (revoking, ()) = tokio::join!(phone.sync.pass(), async {
        entering.await.unwrap();
        invite(&laptop).await;
        release.send(()).unwrap();
    });
    revoking.unwrap();

    let account_requests: Vec<_> = phone
        .storage
        .request_trace()
        .into_iter()
        .filter(|request| {
            matches!(
                request,
                MemoryRequest::Other("grant_access") | MemoryRequest::Other("revoke_access")
            )
        })
        .collect();
    assert_eq!(
        account_requests,
        [
            MemoryRequest::Other("grant_access"),
            MemoryRequest::Other("revoke_access"),
            MemoryRequest::Other("grant_access"),
        ]
    );
    assert_eq!(row(&phone, CAROL).await, Some((true, Some(true))));
    assert!(group.storage.shared_with(CAROL).await);
    assert!(phone.database().pending::<PendingRecord>().await.unwrap().is_empty());
    for device in [&phone, &ben, &laptop] {
        device.database().close().await.unwrap();
    }
}

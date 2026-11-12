use crate::sequence_sync::sequence_invites::shares::tests::{grants, household, row, CAROL};
use crate::sequence_sync::tests::*;
use crate::*;
use coven_format::admin::{MemberRole, StoreChange};

/// §20.9: Ana's phone has shared Carol's account with a grant still in
/// flight when Ben removes the phone. Her laptop reads the removal, which
/// ends the phone's share, and takes Carol's account back; then the phone's
/// grant lands, and the phone stops. Nothing of Ana's is listed or running,
/// yet Carol's account is shared with nobody to take it back. Once Ana signs
/// the phone out at the provider and tells the laptop, the laptop takes it
/// back.
#[tokio::test]
async fn confirming_a_lost_device_cut_off_takes_back_a_share_that_landed_late() {
    let group = Group::shared();
    let (phone, ben, laptop) = household(&group).await;
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    phone.storage.hold_next_access(CAROL, true, entered, released).await;
    let making = phone.sync.create_invite(
        InviteTarget::NewMember { role: MemberRole::Member },
        InviteAccess::ProviderAccount { email: CAROL.into() },
        InviteLifetime::SevenDays,
    );
    let meanwhile = async {
        entering.await.unwrap();
        laptop.sync.pass().await.unwrap();
        ben.sync.pass().await.unwrap();
        ben.sync.administer(StoreChange::RemoveDevice { device: phone.sync.device }).await.unwrap();
        laptop.sync.pass().await.unwrap();
        assert_eq!(row(&laptop, CAROL).await, Some((false, Some(false))));
        release.send(()).unwrap();
    };
    let (made, ()) = tokio::time::timeout(coven_foundation::clock::HANG_GUARD, async {
        tokio::join!(making, meanwhile)
    })
    .await
    .expect("the held grant and the removal finish");
    assert!(made.is_err(), "the phone stops reading its removal");
    assert_eq!(grants(&laptop), 0);
    assert!(group.storage.shared_with(CAROL).await, "the late grant landed");

    laptop.sync.pass().await.unwrap();
    assert!(group.storage.shared_with(CAROL).await, "no pass of the laptop takes it back");

    laptop.sync.confirm_device_cut_off(phone.sync.device).await.unwrap();
    assert_eq!(row(&laptop, CAROL).await.map(|(intended, _)| intended), Some(false));
    laptop.sync.pass().await.unwrap();
    assert!(!group.storage.shared_with(CAROL).await);
    assert_eq!(row(&laptop, CAROL).await, Some((false, Some(false))));
    assert!(laptop.database().operations().await.unwrap().is_empty());
    assert!(laptop.database().pending::<PendingRecord>().await.unwrap().is_empty());
    for device in [&phone, &ben, &laptop] {
        device.database().close().await.unwrap();
    }
}

/// §20.9: an account another open invite of the laptop's names stays shared.
#[tokio::test]
async fn confirming_a_device_cut_off_leaves_an_account_something_still_uses() {
    let group = Group::shared();
    let (phone, ben, laptop) = household(&group).await;
    invite(&phone).await;
    invite(&laptop).await;
    ben.sync.pass().await.unwrap();
    ben.sync.administer(StoreChange::RemoveDevice { device: phone.sync.device }).await.unwrap();
    laptop.sync.pass().await.unwrap();
    laptop.sync.confirm_device_cut_off(phone.sync.device).await.unwrap();
    laptop.sync.pass().await.unwrap();
    assert!(group.storage.shared_with(CAROL).await);
    assert_eq!(row(&laptop, CAROL).await.map(|(intended, _)| intended), Some(true));
    for device in [&phone, &ben, &laptop] {
        device.database().close().await.unwrap();
    }
}

/// §20.9: a device whose removal this device hasn't read can be confirmed
/// after the next pass; one of a member who doesn't hold the store's account
/// never can, and the answers say which.
#[tokio::test]
async fn confirming_names_why_a_device_is_refused() {
    let group = Group::shared();
    let (phone, ben, laptop) = household(&group).await;
    invite(&phone).await;
    assert!(matches!(
        laptop.sync.confirm_device_cut_off(phone.sync.device).await,
        Err(SyncError::CutOffRefused(CutOffRefusal::DeviceNotGone))
    ));
    ben.sync.pass().await.unwrap();
    ben.sync.administer(StoreChange::RemoveDevice { device: phone.sync.device }).await.unwrap();
    assert!(matches!(
        ben.sync.confirm_device_cut_off(phone.sync.device).await,
        Err(SyncError::CutOffRefused(CutOffRefusal::NotStoreOwner))
    ));
    for device in [&phone, &ben, &laptop] {
        device.database().close().await.unwrap();
    }
}

/// §20.9: on S3 nothing is shared with an account, so there is nothing to
/// take back.
#[tokio::test]
async fn confirming_is_refused_on_s3() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    ana.sync.administer(StoreChange::RemoveDevice { device: ben.sync.device }).await.unwrap();
    assert!(matches!(
        ana.sync.confirm_device_cut_off(ben.sync.device).await,
        Err(SyncError::CutOffRefused(CutOffRefusal::NoAccountShared))
    ));
    for device in [&ana, &ben] {
        device.database().close().await.unwrap();
    }
}

/// §20.9: the member whose account holds the store is the only one whose
/// devices can confirm a cut-off; a device says whether its member is that
/// member, and on S3 none is.
#[tokio::test]
async fn a_device_says_whether_its_member_holds_the_store() {
    let group = Group::shared();
    let (phone, ben, laptop) = household(&group).await;
    assert!(phone.sync.holds_store().await.unwrap());
    assert!(laptop.sync.holds_store().await.unwrap());
    assert!(!ben.sync.holds_store().await.unwrap());
    for device in [&phone, &ben, &laptop] {
        device.database().close().await.unwrap();
    }
    let s3 = Group::new();
    let ana = s3.device(1).await;
    s3.create(&ana).await;
    assert!(!ana.sync.holds_store().await.unwrap());
    ana.database().close().await.unwrap();
}

use crate::sequence_sync::tests::*;
use coven_format::{
    admin::{MemberRole, StoreChange},
    MemberAccess,
};

/// Ana's account holds Household on Dropbox; Ben, an admin with his own
/// account, removes Dan. Only Ana's device changes sharing, and Ben's can't
/// see it happen, so Ben's device records no provider work and shows no
/// wait it could never see end. Ana's device takes Dan's access back (§14.2).
#[tokio::test]
async fn only_the_owners_device_records_the_work_a_removal_needs() {
    let group = Group::shared();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    let dan = group.device(3).await;
    group.create_with_access(&ana, MemberAccess::ProviderAccount("ana@example.org".into())).await;
    for (device, account) in [(&ben, "ben@example.org"), (&dan, "dan@example.org")] {
        ana.sync
            .administer(StoreChange::AddMember {
                member: device.identity.member(),
                keys: device.identity.keys().public_keys(),
                role: MemberRole::Admin,
                access: MemberAccess::ProviderAccount(account.into()),
            })
            .await
            .unwrap();
    }
    ben.sync
        .administer(StoreChange::AddDevice { name: "Ben's phone".into(), replaces: None })
        .await
        .unwrap();
    ben.sync.administer(StoreChange::RemoveMember { member: dan.identity.member() }).await.unwrap();
    ben.sync.pass().await.unwrap();
    assert!(ben.database().operations().await.unwrap().is_empty());
    assert!(ben.database().pending::<crate::PendingRecord>().await.unwrap().is_empty());
    ana.sync.pass().await.unwrap();
    ana.sync.pass().await.unwrap();
    let access = ana.database().provider_access("dan@example.org".into()).await.unwrap().unwrap();
    assert_eq!((access.intended, access.granted), (false, Some(false)));
    for device in [&ana, &ben, &dan] {
        device.database().close().await.unwrap();
    }
}

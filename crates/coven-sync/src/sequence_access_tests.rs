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
    group.create(&ana).await;
    for (device, account) in [(&ben, "ben@example.org"), (&dan, "dan@example.org")] {
        ana.sync
            .administer(admission(
                device,
                MemberRole::Admin,
                MemberAccess::ProviderAccount(account.into()),
                "phone",
            ))
            .await
            .unwrap();
    }
    ben.sync.pass().await.unwrap();
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

/// Dan's access was recorded with two accounts, and each keeps a grant the
/// owner's device can't take back. Ben, another admin, removes Dan; Ana's
/// device, applying that with no app call waiting on it, lists one revocation
/// per account, each with the grants it left, and later passes keep both
/// (§14.2, E9).
#[tokio::test]
async fn retained_grants_stay_one_record_per_account_whoever_removed_the_member() {
    let group = Group::shared();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    let dan = group.device(3).await;
    group.create(&ana).await;
    for (device, role, account) in [
        (&ben, MemberRole::Admin, "ben@example.org"),
        (&dan, MemberRole::Member, "dan@example.org"),
    ] {
        ana.sync
            .administer(admission(
                device,
                role,
                MemberAccess::ProviderAccount(account.into()),
                "phone",
            ))
            .await
            .unwrap();
    }
    for device in [&ben, &dan] {
        device.sync.pass().await.unwrap();
    }
    dan.sync
        .administer(StoreChange::SetAccess {
            member: dan.identity.member(),
            access: MemberAccess::ProviderAccount("dan-work@example.org".into()),
        })
        .await
        .unwrap();
    ben.sync.administer(StoreChange::RemoveMember { member: dan.identity.member() }).await.unwrap();

    let inherited = vec![coven_storage::RetainedAccess {
        provider_id: "parent-folder".into(),
        reason: coven_storage::RetainedAccessReason::Inherited,
    }];
    for account in ["dan@example.org", "dan-work@example.org"] {
        group.storage.set_retained_access(account, inherited.clone()).await;
    }
    let remaining = |records: Vec<crate::PendingRecord>| {
        let mut remaining: Vec<_> = records
            .into_iter()
            .filter_map(|record| match (record.subject, record.reason) {
                (
                    crate::PendingSubject::Operation {
                        kind: crate::OperationKind::RevokeAccess,
                        ..
                    },
                    crate::PendingReason::AccessRemains { account, shares },
                ) => Some((account, shares)),
                _ => None,
            })
            .collect();
        remaining.sort_by(|a, b| a.0.cmp(&b.0));
        remaining
    };
    let expected = vec![
        ("dan-work@example.org".to_owned(), inherited.clone()),
        ("dan@example.org".to_owned(), inherited),
    ];
    for _ in 0..3 {
        // A retained grant keeps its revocation failing, so each pass ends in
        // that pending reason.
        drop(ana.sync.pass().await);
        assert_eq!(remaining(ana.database().pending().await.unwrap()), expected);
    }
    assert!(remaining(ben.database().pending().await.unwrap()).is_empty());
    for device in [&ana, &ben, &dan] {
        device.database().close().await.unwrap();
    }
}

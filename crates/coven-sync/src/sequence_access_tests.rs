use crate::history::tests::{account, member, share, Household};
use crate::sequence_sync::tests::*;
use crate::OperationRejection;
use coven_format::{
    admin::{MemberRole, StoreChange},
    MemberAccess,
};
use coven_foundation::id_source::DeviceId;

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
            .administer_admission(admission(
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
            .administer_admission(admission(
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

fn admit_ben(access: coven_format::MemberAccess) -> StoreChange {
    StoreChange::AdmitMember {
        member: member(2),
        keys: coven_format::test_utils::keys_from_seed(2).public_keys(),
        role: MemberRole::Admin,
        access,
        device: DeviceId(2),
        device_name: "Ben's phone".into(),
        person: "Person".into(),
        circle: crate::effects::tests::circle(2),
    }
}

/// The store's creator is a member with no storage access until Set up
/// storage records it; before it, nobody is admitted, given access or
/// invited, since the store has no kind of access to keep to (§10.3).
#[test]
fn no_access_entry_admission_or_invite_share_applies_before_set_up_storage() {
    let household = Household::created();
    let ana = household.ana;
    assert_eq!(household.history.state.members[&ana].access, None);
    assert_eq!(household.history.store_access(), None);
    for change in [
        admit_ben(account("ben@example.org")),
        admit_ben(crate::sequence_sync::tests::access("ben")),
        StoreChange::SetAccess { member: ana, access: account("ana@example.org") },
        StoreChange::SetAccess { member: ana, access: crate::sequence_sync::tests::access("ana") },
        share(1, "dan@example.org"),
    ] {
        assert_eq!(
            household.check(1, ana, change.clone()).err(),
            Some(OperationRejection::NotAllowed),
            "{change:?}"
        );
    }
    let set_up = StoreChange::SetUpStorage { access: account("ana@example.org") };
    assert!(household.check(1, ana, set_up).unwrap().is_some());
}

/// Set up storage records its author's access, once. The same author
/// recording the same credential again is satisfied; any other second entry
/// is refused (§10.3).
#[test]
fn set_up_storage_applies_once() {
    let mut household = Household::created();
    let ana = household.ana;
    let set_up = |email: &str| StoreChange::SetUpStorage { access: account(email) };
    household.entry(1, ana, set_up("ana@example.org"));
    assert_eq!(household.history.state.members[&ana].access, Some(account("ana@example.org")));
    assert_eq!(household.history.store_access(), Some(&account("ana@example.org")));
    // The same credential spelled another way is the same one.
    assert!(household.check(1, ana, set_up(" ANA@example.org")).unwrap().is_none());
    assert_eq!(
        household.check(1, ana, set_up("ana@work.example")).err(),
        Some(OperationRejection::NotAllowed)
    );
    assert_eq!(
        household
            .check(
                1,
                ana,
                StoreChange::SetUpStorage { access: crate::sequence_sync::tests::access("ana") }
            )
            .err(),
        Some(OperationRejection::NotAllowed)
    );
    // Ben, admitted after it, is no author of a second one.
    household.entry(1, ana, admit_ben(account("ben@example.org")));
    let ben = household.ben;
    assert_eq!(
        household.check(2, ben, set_up("ben@example.org")).err(),
        Some(OperationRejection::NotAllowed)
    );
    assert_eq!(
        household.check(2, ben, set_up("ana@example.org")).err(),
        Some(OperationRejection::NotAllowed)
    );
}

/// The member whose account Set up storage recorded holds the store: they
/// can't be removed (§10.1, §20.9).
#[test]
fn the_member_whose_account_set_up_storage_recorded_holds_the_store() {
    let household = Household::shared();
    let ana = household.ana;
    assert!(household.history.holds_store(ana));
    assert!(!household.history.holds_store(household.ben));
    assert_eq!(
        household.check(2, household.ben, StoreChange::RemoveMember { member: ana }).err(),
        Some(OperationRejection::StoreOwner)
    );
}

use crate::sequence_sync::tests::*;
use crate::*;
use coven_format::admin::{MemberRole, StoreChange};
use coven_foundation::id_source::{IdSource, KeyId};
use coven_storage::{test_utils::MemoryStorage, ObjectPath, Storage};

#[tokio::test]
async fn the_provider_owner_cannot_be_removed_or_claim_a_removal() {
    let group = Group::shared();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    let owner = coven_format::MemberAccess::ProviderAccount("ana@example.org".into());
    group.create_with_access(&ana, owner).await;
    ana.sync
        .administer(StoreChange::AddMember {
            member: ben.identity.member(),
            keys: ben.identity.keys().public_keys(),
            role: MemberRole::Admin,
            access: coven_format::MemberAccess::ProviderAccount("ben@example.org".into()),
        })
        .await
        .unwrap();
    ben.sync
        .administer(StoreChange::AddDevice {
            name: "Ben".into(),
            replaces: None,
        })
        .await
        .unwrap();
    ana.sync.pass().await.unwrap();
    let before = ana.sync.database.applied_slot().await.unwrap();
    for initiator in [&ana, &ben] {
        let counter = initiator.sync.database.claim_counter().await.unwrap();
        let result = initiator
            .sync
            .administer(StoreChange::RemoveMember {
                member: ana.identity.member(),
            })
            .await;
        assert!(matches!(result, Err(SyncError::StoreOwner)), "{result:?}");
        assert_eq!(
            initiator.sync.database.applied_slot().await.unwrap(),
            before
        );
        assert_eq!(
            initiator.sync.database.claim_counter().await.unwrap(),
            counter
        );
        assert!(initiator
            .sync
            .database
            .current_claim()
            .await
            .unwrap()
            .is_none());
        assert!(initiator
            .sync
            .database
            .operations()
            .await
            .unwrap()
            .is_empty());
    }
    assert!(group
        .storage
        .status(&ObjectPath::slot(
            group.store,
            (before + 1).try_into().unwrap()
        ))
        .await
        .unwrap()
        .is_none());
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

#[tokio::test]
async fn abandoned_preparation_deletion_preserves_reader_progress() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    let carol = group.device(3).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    group.admit(&ana, &carol).await;
    ben.sync.pass().await.unwrap();
    let before = carol.sync.database.applied_slot().await.unwrap();
    let (upload_started, upload_observed) = tokio::sync::oneshot::channel();
    let (upload_resume, upload_held) = tokio::sync::oneshot::channel();
    group
        .storage
        .hold_next_creation(
            coven_storage::ObjectPrefix::all(group.store),
            upload_started,
            upload_held,
        )
        .await;
    let prepare = ana.sync.reset(coven_merge::Audience::Store);
    let remove = async {
        upload_observed.await.unwrap();
        let (announce_started, announce_observed) = tokio::sync::oneshot::channel();
        let (announce_resume, announce_held) = tokio::sync::oneshot::channel();
        group
            .storage
            .hold_next_creation(
                coven_storage::ObjectPrefix::all(group.store),
                announce_started,
                announce_held,
            )
            .await;
        upload_resume.send(()).unwrap();
        announce_observed.await.unwrap();
        let (activate_started, activate_observed) = tokio::sync::oneshot::channel();
        let (activate_resume, activate_held) = tokio::sync::oneshot::channel();
        group
            .storage
            .hold_next_creation(
                coven_storage::ObjectPrefix::all(group.store),
                activate_started,
                activate_held,
            )
            .await;
        announce_resume.send(()).unwrap();
        activate_observed.await.unwrap();
        ben.sync
            .administer(StoreChange::RemoveMember {
                member: ana.identity.member(),
            })
            .await
            .unwrap();
        activate_resume.send(()).unwrap();
    };
    let (result, ()) = tokio::time::timeout(std::time::Duration::from_secs(15), async {
        tokio::join!(prepare, remove)
    })
    .await
    .expect("publication controls must finish");
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
        .all(|op| op.kind == "delete-access-key"));
    let session = ben.sync.session.lock().await;
    let (announced, announcement) = session.history.announcements.last_key_value().unwrap();
    assert!(announced.0 > before);
    let announced = *announced;
    let path = crate::snapshot_object::path(group.store, &announcement.snapshot).unwrap();
    assert!(
        group.storage.status(&path).await.unwrap().is_none(),
        "undo must delete its artifact"
    );
    assert!(group
        .storage
        .status(&ObjectPath::floor(group.store))
        .await
        .unwrap()
        .is_none());
    drop(session);
    let result = carol.sync.pass().await;
    assert!(
        result.is_ok(),
        "honest undo must preserve Carol's ability to reach the head: {result:?}"
    );
    assert!(carol.sync.database.applied_slot().await.unwrap() > announced.0);
    assert!(
        matches!(&*carol.sync.state.subscribe().borrow(),SequenceStatus::Synced {through} if *through > announced.0)
    );
    ana.sync.database.close().await.unwrap();
    ben.sync.database.close().await.unwrap();
    carol.sync.database.close().await.unwrap();
}

#[tokio::test]
async fn s3_deletion_obligations_follow_applied_membership_and_survive_confirmation() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    ana.sync
        .administer(StoreChange::SetAccess {
            member: ben.identity.member(),
            access: access("ben-new"),
        })
        .await
        .unwrap();
    let records = ana.sync.database.operations().await.unwrap();
    let deletion = records
        .iter()
        .find(|record| record.kind == "delete-access-key")
        .unwrap();
    let key: crate::sequence_sync::access_keys::Deletion =
        serde_json::from_slice(&deletion.data).unwrap();
    assert_eq!(key.key.access_key_id, "ben");
    assert!(ana
        .sync
        .database
        .operation_pending::<PendingRecord>(deletion.id)
        .await
        .unwrap()
        .is_some());
    ana.sync
        .confirm_access_key_deleted("ben".into())
        .await
        .unwrap();
    ana.sync
        .administer(StoreChange::RemoveMember {
            member: ben.identity.member(),
        })
        .await
        .unwrap();
    let records = ana.sync.database.operations().await.unwrap();
    let keys: Vec<_> = records
        .iter()
        .filter(|r| r.kind == "delete-access-key")
        .map(|r| {
            serde_json::from_slice::<crate::sequence_sync::access_keys::Deletion>(&r.data)
                .unwrap()
                .key
                .access_key_id
        })
        .collect();
    assert_eq!(keys, ["ben-new"]);
    ana.sync.database.close().await.unwrap();
    ben.sync.database.close().await.unwrap();
}

#[tokio::test]
async fn provider_access_stays_until_the_last_member_using_the_account_is_removed() {
    let mut group = Group::new();
    group.storage = MemoryStorage::builder()
        .provider(coven_storage::CloudProvider::Dropbox)
        .clock(group.clock.clone())
        .build()
        .unwrap();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    let carol = group.device(3).await;
    group
        .create_with_access(
            &ana,
            coven_format::MemberAccess::ProviderAccount("ana@example.org".into()),
        )
        .await;
    group
        .storage
        .grant_access("shared@example.org")
        .await
        .unwrap();
    for member in [&ben, &carol] {
        ana.sync
            .administer(StoreChange::AddMember {
                member: member.identity.member(),
                keys: member.identity.keys().public_keys(),
                role: MemberRole::Member,
                access: coven_format::MemberAccess::ProviderAccount("shared@example.org".into()),
            })
            .await
            .unwrap();
    }
    ana.sync
        .administer(StoreChange::RemoveMember {
            member: ben.identity.member(),
        })
        .await
        .unwrap();
    assert!(ana
        .sync
        .database
        .provider_access("shared@example.org".into())
        .await
        .unwrap()
        .unwrap()
        .request
        .is_none());
    ana.sync
        .administer(StoreChange::RemoveMember {
            member: carol.identity.member(),
        })
        .await
        .unwrap();
    let before = ana
        .sync
        .database
        .provider_access("shared@example.org".into())
        .await
        .unwrap()
        .unwrap();
    assert!(!before.intended);
    assert!(before.request.is_some());
    ana.sync.pass().await.unwrap();
    let after = ana
        .sync
        .database
        .provider_access("shared@example.org".into())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.granted, Some(false));
    assert!(after.request.is_none());
    assert_eq!(
        ana.storage
            .request_trace()
            .iter()
            .filter(|request| matches!(
                request,
                coven_storage::test_utils::MemoryRequest::Other("revoke_access")
            ))
            .count(),
        1
    );
    for device in [&ana, &ben, &carol] {
        device.sync.database.close().await.unwrap();
    }
}

#[tokio::test]
async fn creating_a_circle_does_not_require_a_snapshot_of_nonexistent_history() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let circle = coven_foundation::id_source::CircleId(group.ids.new_id());
    let key = coven_crypto::CircleKey::from_bytes(circle, KeyId(group.ids.new_id()), [9; 32]);
    let mut ring = ana.sync.keys.read().unwrap().unwrap();
    let key_id = key.id();
    let hash = key.hash();
    ring.insert_circle_key(key).unwrap();
    ana.sync.keys.persist(&ring).unwrap();
    ana.sync
        .administer(StoreChange::CreateCircle {
            circle,
            name: "Gifts".into(),
            key: key_id,
            key_hash: hash,
        })
        .await
        .unwrap();
    assert!(ana
        .sync
        .database
        .store_state()
        .await
        .unwrap()
        .reads_circle(circle, &ana.identity.member()));
    assert!(ana
        .sync
        .database
        .reloading_audiences()
        .await
        .unwrap()
        .is_empty());
    ana.sync.database.close().await.unwrap();
}

async fn offline_circle_write(compare: bool) {
    use serde_json::json;
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let circle = ana.sync.create_circle("Gifts".into()).await.unwrap();
    ana.sync
        .administer(StoreChange::AddCircleMember {
            circle,
            member: ben.identity.member(),
        })
        .await
        .unwrap();
    ben.sync.pass().await.unwrap();
    pin(
        ben.database(),
        "00000000-0000-4000-8000-000000000001",
        circle,
        "before removal",
    )
    .await;
    let originals = ben.database().waiting_snapshot_headers().await.unwrap();
    let original = originals
        .iter()
        .find(|header| {
            header
                .parts
                .iter()
                .any(|part| part.audience == coven_merge::Audience::Circle(circle))
        })
        .unwrap()
        .clone();
    ana.sync
        .administer(StoreChange::RemoveCircleMember {
            circle,
            member: ben.identity.member(),
        })
        .await
        .unwrap();
    let removal = ana.database().applied_slot().await.unwrap();
    assert!(original.header.read_through < removal);
    let mut after_read = original.clone();
    after_read.header.read_through = removal;
    let (before_allowed, after_allowed) = {
        let session = ana.sync.session.lock().await;
        (
            session.history.write_signer(&original).is_ok(),
            session.history.write_signer(&after_read).is_ok(),
        )
    };
    assert!(before_allowed);
    assert!(!after_allowed);
    ben.sync.pass().await.unwrap();
    ana.sync.pass().await.unwrap();
    assert_eq!(
        pins(ana.database()).await,
        vec![(
            "00000000-0000-4000-8000-000000000001".into(),
            "before removal".into()
        )]
    );
    if compare {
        // Prefix zero projects the shared state immediately before removal.
        let part = |read_through| {
            json!({"audience":1,"version":1,"read_through":read_through,
            "unique_claims":[],"owner_creations":[],"writes":[1],"rows":[]})
        };
        let input = json!({"kind":"apply","member_count":3,
            "initial":{"name":"sequence tests","members":[{"member":0,"role":"admin","key_gen":0},
                {"member":2,"role":"member","key_gen":0}],
                "devices":[{"device":0,"member":0},{"device":2,"member":2}]},
            "audiences":[{"audience":1,"version":1,"writers":[0,2]}],
            "slots":[{"kind":"admin","device":0,"claim":1,"digest":"removal",
                "entry":{"kind":"circleMember","audience":1,"member":2,"allowed":false}},
                {"kind":"writes","device":2,"claim":1,"digest":"before","key_gen":0,"parts":[part(0)]},
                {"kind":"writes","device":2,"claim":2,"digest":"after","key_gen":0,"parts":[part(1)]}]});
        let model = crate::tests::run_lean("COVEN_SEQUENCE_LEAN", &input);
        let refused = model["audiences"][0]["refused"].as_array().unwrap();
        assert_eq!(
            (!refused.contains(&json!(2)), !refused.contains(&json!(3))),
            (before_allowed, after_allowed)
        );
    }
    ana.database().close().await.unwrap();
    ben.database().close().await.unwrap();
}

#[tokio::test]
async fn circle_write_uses_authored_permission_after_removal() {
    offline_circle_write(false).await;
}

#[tokio::test]
#[ignore = "requires COVEN_SEQUENCE_LEAN; scripts/check.sh builds sequenceRunner"]
async fn sequence_authored_circle_permission_matches_lean() {
    offline_circle_write(true).await;
}

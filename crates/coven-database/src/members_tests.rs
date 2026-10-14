use super::*;
use crate::pending::tests::Record;
use crate::store_log::tests::{keys, member};
use crate::tests::TestStore;
use crate::{EntryOutcome, StoreCircle, StoreDevice, StoreMember};
use std::time::Duration;

fn access() -> coven_format::MemberAccess {
    coven_format::MemberAccess::S3AccessKey {
        access_key_id: "fixture-access-key".into(),
        principal: coven_format::S3Principal::Console,
    }
}

fn store_member(seed: u8, role: MemberRole, removed: bool) -> (MemberId, StoreMember) {
    (
        member(seed),
        StoreMember {
            keys: keys(seed),
            access: access(),
            role,
            removed,
        },
    )
}

/// Replay state with this device's member 1, member 2 with an active, a
/// removed and a replaced device, and removed member 3; circle 7 holds 1 and
/// 2, and deleted circle 8 held 1.
async fn fixture(db: &crate::Database) {
    let own = db.inspect_writer(|sql| sql.pending_device()).unwrap();
    let log = db.store_log().await.unwrap();
    let mut replay = log.replay;
    replay.state.members = [
        store_member(1, MemberRole::Admin, false),
        store_member(2, MemberRole::Member, false),
        store_member(3, MemberRole::Member, true),
    ]
    .into();
    let device = |owner: u8, name: &str, state| StoreDevice {
        member: member(owner),
        name: name.into(),
        state,
    };
    replay.state.devices = [
        (own, device(1, "mine", DeviceState::Active)),
        (DeviceId(20), device(2, "phone", DeviceState::Active)),
        (DeviceId(21), device(2, "lost", DeviceState::Removed)),
        (DeviceId(22), device(2, "restored", DeviceState::Replaced)),
        (DeviceId(30), device(3, "gone", DeviceState::Removed)),
    ]
    .into();
    replay.state.circles = [
        (
            CircleId(uuid::Uuid::from_u128(7)),
            StoreCircle {
                name: "Gifts".into(),
                members: [member(1), member(2), member(3)].into(),
            },
        ),
        (
            CircleId(uuid::Uuid::from_u128(8)),
            StoreCircle {
                name: "Old".into(),
                members: [].into(),
            },
        ),
    ]
    .into();
    let deletion = crate::EntryId {
        device: own,
        number: 1,
    };
    let entry = coven_format::store_log::StoreLogEntry {
        position: deletion,
        timestamp: coven_merge::Timestamp::new(1, 0, own).unwrap(),
        author: member(1),
        had_read: coven_format::value::EntryPositions(Vec::new()),
        change: coven_format::store_log::StoreChange::DeleteCircle {
            circle: CircleId(uuid::Uuid::from_u128(8)),
        },
    };
    replay
        .state
        .circle_deletions
        .insert(CircleId(uuid::Uuid::from_u128(8)), deletion);
    replay.entries.insert(deletion, EntryOutcome::Kept);
    db.apply_store_log::<Record>(crate::store_log::tests::checked(entry), replay, Vec::new())
        .await
        .unwrap();
}

#[tokio::test]
async fn members_list_every_device_with_its_state_and_the_local_member() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    // Before this device's registration replays, nothing is listed as self.
    assert!(db.members().await.unwrap().is_empty());
    fixture(&db).await;
    let own = db.inspect_writer(|sql| sql.pending_device()).unwrap();
    let members = db.members().await.unwrap();
    let mut expected = vec![
        MemberInfo {
            id: member(1),
            keys: keys(1),
            role: MemberRole::Admin,
            devices: vec![DeviceInfo {
                id: own,
                name: "mine".into(),
                state: DeviceState::Active,
            }],
            is_self: true,
        },
        MemberInfo {
            id: member(2),
            keys: keys(2),
            role: MemberRole::Member,
            devices: [
                (20, "phone", DeviceState::Active),
                (21, "lost", DeviceState::Removed),
                (22, "restored", DeviceState::Replaced),
            ]
            .into_iter()
            .map(|(id, name, state)| DeviceInfo {
                id: DeviceId(id),
                name: name.into(),
                state,
            })
            .collect(),
            is_self: false,
        },
    ];
    expected.sort_by(|a, b| a.id.cmp(&b.id));
    assert_eq!(members, expected);
    assert_eq!(
        db.circles().await.unwrap(),
        [Circle {
            id: CircleId(uuid::Uuid::from_u128(7)),
            name: "Gifts".into(),
        }]
    );
    let mut circle = db
        .circle_members(CircleId(uuid::Uuid::from_u128(7)))
        .await
        .unwrap();
    circle.sort_by(|a, b| a.member.cmp(&b.member));
    let mut wanted = vec![
        CircleMemberInfo {
            member: member(1),
            is_self: true,
        },
        CircleMemberInfo {
            member: member(2),
            is_self: false,
        },
    ];
    wanted.sort_by(|a, b| a.member.cmp(&b.member));
    assert_eq!(circle, wanted);
    for absent in [8, 9] {
        assert!(db
            .circle_members(CircleId(uuid::Uuid::from_u128(absent)))
            .await
            .unwrap()
            .is_empty());
    }
}

/// A subscription's first value is the current state, and the replay's
/// transaction delivers the new value whole (E4).
#[tokio::test]
async fn subscriptions_start_with_the_committed_value_and_follow_replay() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let mut members = db.subscribe_members();
    let mut circles = db.subscribe_circles();
    let mut circle = db.subscribe_circle_members(CircleId(uuid::Uuid::from_u128(7)));
    assert!(members.next().await.unwrap().is_empty());
    assert!(circles.next().await.unwrap().is_empty());
    assert!(circle.next().await.unwrap().is_empty());
    fixture(&db).await;
    let next = tokio::time::timeout(Duration::from_secs(5), members.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(next, db.members().await.unwrap());
    assert_eq!(next.len(), 2);
    let next = tokio::time::timeout(Duration::from_secs(5), circles.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(next.len(), 1);
    let next = tokio::time::timeout(Duration::from_secs(5), circle.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(next.len(), 2);
}

/// A failed read reaches the caller and the subscriber; it is never an empty
/// list. Damaged replay state fails the query instead of being skipped.
#[tokio::test]
async fn read_failures_reach_callers_and_subscribers() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    fixture(&db).await;
    let mut members = db.subscribe_members();
    assert_eq!(members.next().await.unwrap().len(), 2);
    db.inspect_writer(|sql| {
        sql.batch(
            "PRAGMA ignore_check_constraints=ON;
             UPDATE _coven_members SET role='owner' WHERE role='member';
             PRAGMA ignore_check_constraints=OFF;",
        )
        .unwrap()
    });
    assert!(db.members().await.is_err());
    assert!(db.subscribe_members().next().await.is_err());
    db.close().await.unwrap();
    assert!(matches!(
        db.members().await,
        Err(crate::CovenError::Database(DbError::StoreClosed))
    ));
}

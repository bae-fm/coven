use super::*;
use crate::tests::TestStore;
use crate::tests::{keys, member};
use crate::{StoreCircle, StoreDevice, StoreMember};
use std::time::Duration;

/// Member 2 reaches storage through a provider account, the others through
/// S3 keys.
fn access(seed: u8) -> coven_format::MemberAccess {
    match seed {
        2 => coven_format::MemberAccess::ProviderAccount("ben@example.com".into()),
        _ => coven_format::MemberAccess::S3AccessKey {
            access_key_id: format!("fixture-access-key-{seed}"),
        },
    }
}

fn store_member(seed: u8, role: MemberRole, removed: bool) -> (MemberId, StoreMember) {
    (member(seed), StoreMember { keys: keys(seed), access: access(seed), role, removed })
}

/// Replay state with this device's member 1, member 2 with an active, a
/// removed and a replaced device, and removed member 3; circle 7 holds 1 and
/// 2, and deleted circle 8 held 1.
async fn fixture(db: &crate::Database) {
    let own = db.inspect_writer(|sql| sql.local_device()).unwrap();
    let mut state = db.store_state().await.unwrap();
    state.members = [
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
    state.devices = [
        (own, device(1, "mine", DeviceState::Active)),
        (DeviceId(11), device(1, "my other device", DeviceState::Removed)),
        (DeviceId(20), device(2, "phone", DeviceState::Active)),
        (DeviceId(21), device(2, "lost", DeviceState::Removed)),
        (DeviceId(22), device(2, "restored", DeviceState::Replaced)),
        (DeviceId(30), device(3, "gone", DeviceState::Removed)),
    ]
    .into();
    state.circles = [
        (
            CircleId(uuid::Uuid::from_u128(7)),
            StoreCircle { name: "Gifts".into(), members: [member(1), member(2), member(3)].into() },
        ),
        (
            CircleId(uuid::Uuid::from_u128(8)),
            StoreCircle { name: "Old".into(), members: [].into() },
        ),
    ]
    .into();
    let deletion = coven_merge::Slot(1);
    state.circle_deletions.insert(CircleId(uuid::Uuid::from_u128(8)), deletion);
    db.admin_fixture(
        coven_format::admin::StoreChange::DeleteCircle {
            circle: CircleId(uuid::Uuid::from_u128(8)),
        },
        state,
        None,
    )
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
    let own = db.inspect_writer(|sql| sql.local_device()).unwrap();
    let members = db.members().await.unwrap();
    let mut expected = vec![
        MemberInfo {
            id: member(1),
            keys: keys(1),
            role: MemberRole::Admin,
            access: access(1),
            devices: vec![
                DeviceInfo {
                    id: own,
                    name: "mine".into(),
                    state: DeviceState::Active,
                    is_self: true,
                },
                DeviceInfo {
                    id: DeviceId(11),
                    name: "my other device".into(),
                    state: DeviceState::Removed,
                    is_self: false,
                },
            ],
            is_self: true,
        },
        MemberInfo {
            id: member(2),
            keys: keys(2),
            role: MemberRole::Member,
            access: access(2),
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
                is_self: false,
            })
            .collect(),
            is_self: false,
        },
    ];
    // Members come in member-id order, each one's devices in device-id order.
    expected.sort_by_key(|a| a.id);
    for member in &mut expected {
        member.devices.sort_by_key(|device| device.id);
    }
    assert_eq!(members, expected);
    assert_eq!(
        db.circles().await.unwrap(),
        [Circle { id: CircleId(uuid::Uuid::from_u128(7)), name: "Gifts".into() }]
    );
    let circle = db.circle_members(CircleId(uuid::Uuid::from_u128(7))).await.unwrap();
    let mut wanted = vec![
        CircleMemberInfo { member: member(1), is_self: true },
        CircleMemberInfo { member: member(2), is_self: false },
    ];
    wanted.sort_by_key(|a| a.member);
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
    assert!(members.next().await.expect("the live query answers").unwrap().is_empty());
    assert!(circles.next().await.expect("the live query answers").unwrap().is_empty());
    assert!(circle.next().await.expect("the live query answers").unwrap().is_empty());
    fixture(&db).await;
    let next = tokio::time::timeout(Duration::from_secs(5), members.next())
        .await
        .unwrap()
        .expect("the live query answers")
        .unwrap();
    assert_eq!(next, db.members().await.unwrap());
    assert_eq!(next.len(), 2);
    let next = tokio::time::timeout(Duration::from_secs(5), circles.next())
        .await
        .unwrap()
        .expect("the live query answers")
        .unwrap();
    assert_eq!(next.len(), 1);
    let next = tokio::time::timeout(Duration::from_secs(5), circle.next())
        .await
        .unwrap()
        .expect("the live query answers")
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
    assert_eq!(members.next().await.expect("the live query answers").unwrap().len(), 2);
    db.inspect_writer(|sql| {
        sql.batch(
            "PRAGMA ignore_check_constraints=ON;
             UPDATE _coven_members SET role='owner' WHERE role='member';
             PRAGMA ignore_check_constraints=OFF;",
        )
        .unwrap()
    });
    assert!(db.members().await.is_err());
    assert!(db.subscribe_members().next().await.expect("the live query answers").is_err());
    db.close().await.unwrap();
    assert!(matches!(db.members().await, Err(crate::CovenError::Database(DbError::StoreClosed))));
}

#[tokio::test]
async fn retired_local_registration_still_identifies_this_installation() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    fixture(&db).await;
    let own = db.inspect_writer(|sql| sql.local_device()).unwrap();
    for state in [DeviceState::Removed, DeviceState::Replaced] {
        let mut prefix = db.store_state().await.unwrap();
        prefix.devices.get_mut(&own).unwrap().state = state;
        db.admin_fixture(
            coven_format::admin::StoreChange::DeleteCircle {
                circle: CircleId(uuid::Uuid::from_u128(8)),
            },
            prefix,
            None,
        )
        .await
        .unwrap();
        let devices: Vec<_> = db
            .members()
            .await
            .unwrap()
            .into_iter()
            .flat_map(|member| member.devices)
            .filter(|device| device.is_self)
            .collect();
        assert_eq!(devices.len(), 1);
        assert_eq!(devices[0].id, own);
        assert_eq!(devices[0].state, state);
    }
    db.close().await.unwrap();
}

const MEMBERS: &str = "SELECT member,role,removed,is_self FROM coven_members ORDER BY member";
const DEVICES: &str = "SELECT device,member,name,state,is_self FROM coven_devices ORDER BY name";

type MemberRow = (MemberId, String, bool, bool);
type DeviceRow = (DeviceId, MemberId, String, String, bool);

fn member_rows(sql: crate::SqlReadContext<'_>) -> crate::CovenResult<Vec<MemberRow>> {
    Ok(sql.query(MEMBERS, [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))?)
}

fn device_rows(sql: crate::SqlReadContext<'_>) -> crate::CovenResult<Vec<DeviceRow>> {
    Ok(sql.query(DEVICES, [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)))?)
}

/// The app's SQL reads every member, former ones included, and every device,
/// marked as this installation's where they are, and joins them by id
/// (Appendix C.1).
#[tokio::test]
async fn the_apps_sql_reads_members_and_devices_former_ones_included() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let mut members = db.subscribe(member_rows);
    assert!(members.next().await.expect("the live query answers").unwrap().is_empty());
    fixture(&db).await;
    let mut expected = vec![
        (member(1), "admin".to_owned(), false, true),
        (member(2), "member".to_owned(), false, false),
        (member(3), "member".to_owned(), true, false),
    ];
    expected.sort_by_key(|row| row.0.to_string());
    // Applying the membership reruns the app's query.
    let next = tokio::time::timeout(Duration::from_secs(5), members.next())
        .await
        .unwrap()
        .expect("the live query answers")
        .unwrap();
    assert_eq!(next, expected);
    let own = db.inspect_writer(|sql| sql.local_device()).unwrap();
    let device = |id, owner, name: &str, state: &str| {
        (id, member(owner), name.to_owned(), state.to_owned(), id == own)
    };
    let devices = vec![
        device(DeviceId(30), 3, "gone", "removed"),
        device(DeviceId(21), 2, "lost", "removed"),
        device(own, 1, "mine", "active"),
        device(DeviceId(11), 1, "my other device", "removed"),
        device(DeviceId(20), 2, "phone", "active"),
        device(DeviceId(22), 2, "restored", "replaced"),
    ];
    assert_eq!(db.read(device_rows).await.unwrap(), devices);
    // The member id is the text an app keeps in its own tables.
    let joined: Vec<String> = db
        .read(move |sql| {
            Ok(sql.query(
                "SELECT d.name FROM coven_devices AS d JOIN coven_members AS m ON m.member=d.member
                 WHERE m.member=?1 ORDER BY d.name",
                [member(2)],
                |r| r.get(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(joined, ["lost", "phone", "restored"]);

    // A read-only handle names the same installation.
    let reading = store.builder(vec![], vec![]).open_read_only().await.unwrap();
    assert_eq!(reading.read(member_rows).await.unwrap(), expected);
    assert_eq!(reading.read(device_rows).await.unwrap(), devices);
    reading.close().await.unwrap();
    db.close().await.unwrap();
}

/// The app's SQL reads each member's public keys and how they reach storage:
/// the account or the S3 key id, whichever their access names, as
/// `members()` gives them (Appendix C.1).
#[tokio::test]
async fn the_apps_sql_reads_each_members_keys_and_access() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    fixture(&db).await;
    type KeysRow = (MemberId, [u8; 32], [u8; 32], Option<String>, Option<String>);
    let rows: Vec<KeysRow> = db
        .read(|sql| {
            Ok(sql.query(
                "SELECT member,signing,sealing,account,access_key_id FROM coven_members
                 ORDER BY member",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
            )?)
        })
        .await
        .unwrap();
    let mut expected: Vec<KeysRow> = [1, 2, 3]
        .into_iter()
        .map(|seed| {
            let (account, access_key_id) = match access(seed) {
                coven_format::MemberAccess::ProviderAccount(account) => (Some(account), None),
                coven_format::MemberAccess::S3AccessKey { access_key_id } => {
                    (None, Some(access_key_id))
                }
            };
            (
                member(seed),
                *keys(seed).signing.as_bytes(),
                *keys(seed).sealing.as_bytes(),
                account,
                access_key_id,
            )
        })
        .collect();
    // The view's member is UUID text, ordered as that text sorts.
    expected.sort_by_key(|row| row.0.to_string());
    assert_eq!(rows, expected);
    db.close().await.unwrap();
}

/// The app's SQL finds a circle this device's member is in by its name, and
/// reads its members who remain in the store, as `circles()` and
/// `circle_members()` list them: a deleted circle is in neither view
/// (Appendix C.1).
#[tokio::test]
async fn the_apps_sql_finds_a_circle_by_name_and_reads_its_members() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    fixture(&db).await;
    let gifts = CircleId(uuid::Uuid::from_u128(7));
    let named = |name: &'static str| {
        db.read(move |sql| {
            Ok(sql.query("SELECT circle FROM coven_circles WHERE name=?1", [name], |r| {
                r.get::<_, CircleId>(0)
            })?)
        })
    };
    assert_eq!(named("Gifts").await.unwrap(), [gifts]);
    assert!(named("Old").await.unwrap().is_empty(), "a deleted circle");
    let members: Vec<(CircleId, MemberId, bool)> = db
        .read(|sql| {
            Ok(sql.query(
                "SELECT circle,member,is_self FROM coven_circle_members ORDER BY circle,member",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )?)
        })
        .await
        .unwrap();
    let mut expected: Vec<_> = db
        .circle_members(gifts)
        .await
        .unwrap()
        .into_iter()
        .map(|info| (gifts, info.member, info.is_self))
        .collect();
    // The view's member is UUID text, ordered as that text sorts.
    expected.sort_by_key(|row| row.1.to_string());
    assert_eq!(members, expected);
    assert_eq!(
        members.iter().map(|row| (row.1, row.2)).collect::<Vec<_>>(),
        [(member(1), true), (member(2), false)]
    );
    db.close().await.unwrap();
}

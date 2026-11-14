//! The app's joining write runs in the transaction that first applies this
//! device's registration, as a local write whose past is that slot (§13.2).

use crate::tests::{member, TestStore};
use crate::write::tests::{count, notes, records, NOTES};
use crate::{
    Database, DbError, DeviceState, Joined, Joining, JoiningWrite, Migration, StoreDevice,
    StoreMember, StoreState,
};
use coven_format::admin::{MemberRole, StoreChange};
use coven_foundation::id_source::CircleId;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

const CIRCLE: CircleId = CircleId(uuid::Uuid::from_u128(77));

/// A joining write that counts its runs in a table of the device's own and
/// records the person in a synced one, once.
fn recording(
    runs: &Arc<AtomicUsize>,
) -> impl Fn(&JoiningWrite<'_>, &Joining) -> Result<(), DbError> + Send + Sync + 'static {
    let runs = runs.clone();
    move |sql, joining| {
        let run = runs.fetch_add(1, Ordering::SeqCst);
        sql.execute("INSERT INTO local_rows VALUES(?1)", [format!("run-{run}")])?;
        if let Some(person) = &joining.person {
            sql.execute(
                "INSERT INTO notes(id,title) VALUES(?1,?2) ON CONFLICT(id) DO NOTHING",
                [joining.member.to_string(), person.clone()],
            )?;
        }
        Ok(())
    }
}

async fn open(
    store: &TestStore,
    joining: impl Fn(&JoiningWrite<'_>, &Joining) -> Result<(), DbError> + Send + Sync + 'static,
) -> Database {
    store
        .builder(notes(), vec![Migration::addition(1, "notes", NOTES)])
        .joining_write(joining)
        .open()
        .await
        .unwrap()
}

/// The state after an admission that registered `store`'s device for a
/// member whose admission recorded `joined`.
fn registered(store: &TestStore, joined: Option<Joined>) -> StoreState {
    let mut state = StoreState::default();
    state.members.insert(
        member(7),
        StoreMember {
            keys: crate::tests::keys(7),
            access: None,
            role: MemberRole::Member,
            removed: false,
            joined,
        },
    );
    state.devices.insert(
        store.device(),
        StoreDevice { member: member(7), name: "Phone".into(), state: DeviceState::Active },
    );
    state
}

fn dan() -> Option<Joined> {
    Some(Joined { person: Some("Dan".into()), circle: CIRCLE })
}

fn registration() -> StoreChange {
    StoreChange::AddDevice { name: "Phone".into(), replaces: None }
}

#[tokio::test]
async fn the_slot_that_registers_this_device_commits_the_joining_write_with_it() {
    let store = TestStore::new();
    let runs = Arc::new(AtomicUsize::new(0));
    let db = open(&store, recording(&runs)).await;
    db.admin_fixture(registration(), registered(&store, dan()), None).await.unwrap();
    assert_eq!(db.applied_slot().await.unwrap(), 1);
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    assert_eq!(count(&db, "notes"), 1);
    assert_eq!(count(&db, "local_rows"), 1);
    // The write is queued as any local write is, and its past is the slot
    // whose application it ran in.
    let written = records(&db);
    assert_eq!(written.len(), 1);
    assert_eq!(written[0].header.read_through, 1);
    // A later slot that leaves the registration as it is runs nothing.
    db.admin_fixture(registration(), registered(&store, dan()), None).await.unwrap();
    assert_eq!(db.applied_slot().await.unwrap(), 2);
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    assert_eq!(records(&db).len(), 1);
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_failing_joining_write_leaves_the_registration_unapplied_until_it_succeeds() {
    let store = TestStore::new();
    let failing = Arc::new(AtomicBool::new(true));
    let runs = Arc::new(AtomicUsize::new(0));
    let first = recording(&runs);
    let db = open(&store, {
        let failing = failing.clone();
        move |sql, joining| {
            first(sql, joining)?;
            if failing.load(Ordering::SeqCst) {
                return Err(DbError::Invariant { what: "the app's joining write failed" });
            }
            Ok(())
        }
    })
    .await;
    let state = registered(&store, dan());
    let refused = db.admin_fixture(registration(), state.clone(), None).await;
    assert!(matches!(refused, Err(DbError::Invariant { .. })), "{refused:?}");
    // Nothing of the slot or the write committed: not even the registration.
    assert_eq!(db.applied_slot().await.unwrap(), 0);
    assert!(db.store_state().await.unwrap().devices.is_empty());
    assert_eq!(count(&db, "notes"), 0);
    assert_eq!(count(&db, "local_rows"), 0);
    assert!(records(&db).is_empty());
    failing.store(false, Ordering::SeqCst);
    db.admin_fixture(registration(), state, None).await.unwrap();
    assert_eq!(db.applied_slot().await.unwrap(), 1);
    assert_eq!(count(&db, "notes"), 1);
    assert_eq!(count(&db, "local_rows"), 1);
    assert_eq!(records(&db).len(), 1);
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_member_no_admission_made_has_no_joining_write() {
    let store = TestStore::new();
    let runs = Arc::new(AtomicUsize::new(0));
    let db = open(&store, recording(&runs)).await;
    db.admin_fixture(registration(), registered(&store, None), None).await.unwrap();
    assert_eq!(db.applied_slot().await.unwrap(), 1);
    assert_eq!(runs.load(Ordering::SeqCst), 0);
    assert!(records(&db).is_empty());
    db.close().await.unwrap();
}

#[tokio::test]
async fn another_devices_registration_runs_no_joining_write_here() {
    let store = TestStore::new();
    let runs = Arc::new(AtomicUsize::new(0));
    let db = open(&store, recording(&runs)).await;
    let mut state = registered(&store, dan());
    let device = state.devices.remove(&store.device()).unwrap();
    state.devices.insert(coven_foundation::id_source::DeviceId(u64::MAX - 1), device);
    db.admin_fixture(registration(), state, None).await.unwrap();
    assert_eq!(runs.load(Ordering::SeqCst), 0);
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_recovery_runs_the_write_with_no_person_and_its_new_circle() {
    let store = TestStore::new();
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let db = open(&store, {
        let seen = seen.clone();
        move |_, joining| {
            seen.lock().unwrap().push(joining.clone());
            Ok(())
        }
    })
    .await;
    let recovered = Joined { person: None, circle: CIRCLE };
    db.admin_fixture(registration(), registered(&store, Some(recovered)), None).await.unwrap();
    assert_eq!(
        *seen.lock().unwrap(),
        [Joining { member: member(7), person: None, circle: CIRCLE }]
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_load_that_brings_the_registration_commits_the_joining_write_with_it() {
    let store = TestStore::new();
    let runs = Arc::new(AtomicUsize::new(0));
    let db = open(&store, recording(&runs)).await;
    let mut reload = crate::tests::reload::<std::io::Empty, std::io::Empty>(
        &db,
        Vec::new(),
        Vec::new(),
        Arc::new(crate::test_utils::TestAuthorship),
    )
    .await;
    reload.state = registered(&store, dan());
    reload.snapshots = vec![crate::SnapshotSource::Empty(coven_merge::Audience::Store, 0)];
    db.load_snapshots::<crate::pending::tests::Record, std::io::Empty>(reload, None).await.unwrap();
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    assert_eq!(count(&db, "notes"), 1);
    assert_eq!(count(&db, "local_rows"), 1);
    assert_eq!(records(&db).len(), 1);
    db.close().await.unwrap();
}

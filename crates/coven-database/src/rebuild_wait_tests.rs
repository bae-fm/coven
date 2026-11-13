use super::*;
use crate::pending::tests::Record;
use crate::raise_capture::tests::connect;
use crate::tests::TestStore;
use crate::write::tests::{notes, records, sql, NOTES};
use crate::{Migration, OldWrites};
use coven_format::write::WriteDisposition;
use coven_foundation::id_source::CircleId;

fn circle(number: u128) -> CircleId {
    CircleId(uuid::Uuid::from_u128(number))
}

/// A state holding the circles with these bases, one of them deleted.
fn state(bases: &[(u128, u32)], deleted: &[u128]) -> StoreState {
    let mut state = StoreState::default();
    for (number, base) in bases {
        state.circles.insert(
            circle(*number),
            crate::store_state::StoreCircle {
                name: "Circle".into(),
                members: Default::default(),
                base: *base,
            },
        );
    }
    for number in deleted {
        state.circle_deletions.insert(circle(*number), coven_merge::slot(1));
    }
    state
}

fn garden() -> Audience {
    Audience::Circle(circle(1))
}

#[test]
fn only_circles_below_the_breaking_version_are_rebuilt() {
    let state = state(&[(1, 0), (2, 2), (3, 0)], &[3]);
    let orchard = Audience::Circle(circle(2));
    let gone = Audience::Circle(circle(3));
    let rebuilds = |audience: &Audience, breaking| state.rebuilt_by(audience, breaking);
    assert!(rebuilds(&garden(), 2));
    assert!(rebuilds(&garden(), 1));
    assert!(!rebuilds(&orchard, 2), "a circle at the version reloads");
    assert!(!rebuilds(&gone, 2), "a deleted circle reloads");
    assert!(!rebuilds(&garden(), 0), "no breaking migration ran");
    assert!(!rebuilds(&Audience::Store, 2), "the store reloads");
}

fn index() -> Migration {
    Migration::breaking(2, "index", "CREATE INDEX titles ON notes(title)", OldWrites::Drop, notes())
}

/// This device after its app migrated: a store raise captured through its
/// migration marker, which is the third write.
async fn migrated() -> (TestStore, crate::Database) {
    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    connect(&db).await;
    sql(&db, "INSERT INTO notes VALUES('n','title','body')").await.unwrap();
    db.close().await.unwrap();
    let db = store
        .builder(notes(), vec![Migration::addition(1, "notes", NOTES), index()])
        .open()
        .await
        .unwrap();
    (store, db)
}

#[tokio::test]
async fn beginning_a_wait_queues_a_marker_read_through_the_applied_slot_and_gates_the_circle() {
    let (_store, db) = migrated().await;
    let before = records(&db);
    let applied = db.applied_slot().await.unwrap();
    let newest = before.iter().map(|r| r.header.timestamp).max().unwrap();
    db.inspect_writer_schema(|connection, schema| {
        begin::<Record>(connection, schema, std::time::SystemTime::now(), &[garden()]).unwrap();
    });
    let queued = records(&db);
    assert_eq!(queued.len(), before.len() + 1);
    let marker = queued.last().unwrap();
    assert_eq!(marker.header.disposition, WriteDisposition::Migration);
    assert!(marker.parts.is_empty());
    assert_eq!(marker.header.read_through, applied);
    assert_eq!(marker.header.schema_version, 2);
    assert!(marker.header.timestamp > newest, "newer than every write the device holds");
    assert_eq!(marker.header.position.number, before.last().unwrap().header.position.number + 1);
    let waiting = db.rebuilds_waiting().await.unwrap();
    assert_eq!(waiting.len(), 1);
    assert_eq!(waiting[&garden()], marker.header.position);
    assert_eq!(db.reloading_audiences().await.unwrap(), [garden()].into());
    assert_eq!(
        db.pending::<Record>().await.unwrap().len(),
        1,
        "the circle's wait is recorded as a pending record"
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_waiting_raise_captures_the_marker_too() {
    let (_store, db) = migrated().await;
    let before = db.raise_captures().await.unwrap();
    assert!(!before.is_empty(), "the migration captured the store");
    db.inspect_writer_schema(|connection, schema| {
        begin::<Record>(connection, schema, std::time::SystemTime::now(), &[garden()]).unwrap();
    });
    let marker = records(&db).last().unwrap().header.position;
    for capture in db.raise_captures().await.unwrap() {
        assert_eq!(capture.through, marker, "no raise goes out ahead of the rebuild's marker");
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_reload_replaces_a_rebuild_wait() {
    let (_store, db) = migrated().await;
    db.inspect_writer_schema(|connection, schema| {
        begin::<Record>(connection, schema, std::time::SystemTime::now(), &[garden()]).unwrap();
        crate::reload_gate::require_reload::<Record>(connection, &garden()).unwrap();
    });
    assert!(db.rebuilds_waiting().await.unwrap().is_empty());
    assert_eq!(db.reloading_audiences().await.unwrap(), [garden()].into());
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_breaking_migration_points_a_waiting_rebuild_at_its_own_marker() {
    let (store, db) = migrated().await;
    db.inspect_writer_schema(|connection, schema| {
        begin::<Record>(connection, schema, std::time::SystemTime::now(), &[garden()]).unwrap();
    });
    let first = db.rebuilds_waiting().await.unwrap()[&garden()];
    db.close().await.unwrap();
    let third = Migration::breaking(
        3,
        "bodies",
        "CREATE INDEX bodies ON notes(body)",
        OldWrites::Drop,
        notes(),
    );
    let db = store
        .builder(notes(), vec![Migration::addition(1, "notes", NOTES), index(), third])
        .open()
        .await
        .unwrap();
    let marker = records(&db).last().unwrap().clone();
    assert_eq!(marker.header.schema_version, 3);
    let waiting = db.rebuilds_waiting().await.unwrap();
    assert_eq!(waiting[&garden()], marker.header.position);
    assert_ne!(waiting[&garden()], first);
    db.close().await.unwrap();
}

#[tokio::test]
async fn restarting_points_a_waiting_rebuild_at_a_new_marker_read_through_the_applied_slot() {
    let (_store, db) = migrated().await;
    db.inspect_writer_schema(|connection, schema| {
        begin::<Record>(connection, schema, std::time::SystemTime::now(), &[garden()]).unwrap();
    });
    let first = db.rebuilds_waiting().await.unwrap()[&garden()];
    db.restart_rebuilds().await.unwrap();
    let marker = records(&db).last().unwrap().clone();
    assert_eq!(marker.header.disposition, WriteDisposition::Migration);
    assert_eq!(marker.header.position.number, first.number + 1);
    assert_eq!(marker.header.read_through, db.applied_slot().await.unwrap());
    assert_eq!(db.rebuilds_waiting().await.unwrap()[&garden()], marker.header.position);
    assert_eq!(db.reloading_audiences().await.unwrap(), [garden()].into());
    db.close().await.unwrap();
}

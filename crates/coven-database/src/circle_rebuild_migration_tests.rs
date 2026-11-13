use super::*;
use crate::database::circle_rebuild::tests::{garden, rebuild_folder, reload_into};
use crate::raise_capture::tests::connect;
use crate::snapshot_write::tests::SnapshotFrames;
use crate::tests::TestStore;
use crate::{Migration, OldWrites, RowIdentity, SyncedTable};
use coven_foundation::files::StoreDirAccess;
use coven_foundation::id_source::SequentialIds;
use coven_merge::Audience;

use coven_format::snapshot::SnapshotRecord;
use coven_format::value::Value;
use coven_format::write::{RowChange, WriteDisposition, WriteHeader, WritePart, WriteRecord};
use coven_foundation::id_source::{DeviceId, KeyId};

const FILLED_V1: &str = "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL); CREATE TABLE pins(id TEXT NOT NULL PRIMARY KEY, audience TEXT NOT NULL, title TEXT NOT NULL, note TEXT NOT NULL)";

/// Renames a pin's column and fills a new one from the store's note it names.
const FILL: &str = "ALTER TABLE pins RENAME COLUMN title TO name; ALTER TABLE pins ADD COLUMN note_title TEXT; UPDATE pins SET note_title=(SELECT title FROM notes WHERE notes.id=pins.note)";

fn filled_tables() -> Vec<SyncedTable> {
    vec![
        SyncedTable::new("notes", RowIdentity::SharedKey),
        SyncedTable::new("pins", RowIdentity::IndependentUuid).audience_column("audience"),
    ]
}

fn filled_migrations(statement: &'static str, old_writes: OldWrites) -> Vec<Migration> {
    vec![
        Migration::addition(1, "initial", FILLED_V1),
        Migration::breaking(2, "fill", statement, old_writes, filled_tables()),
    ]
}

fn convert() -> OldWrites {
    OldWrites::Convert(std::sync::Arc::new(|row| {
        if row.table == "pins" {
            row.rename_column("title", "name");
        }
        Ok(())
    }))
}

async fn run(db: &Database, statement: String) {
    db.write(move |context| {
        context.execute_batch(&statement)?;
        Ok(())
    })
    .await
    .unwrap();
}

/// Count every write `db` has queued, as the next slots.
async fn count_queue(db: &Database) {
    for record in records_of(db) {
        db.receive(record.into()).await.unwrap();
    }
}

fn records_of(db: &Database) -> Vec<WriteRecord> {
    crate::write::tests::records(db)
}

fn garden_pin(position: u32, title: &str, note: &str) -> String {
    format!(
        "INSERT INTO pins VALUES('00000000-0000-4000-8000-00000000000{position}','{}','{title}','{note}')",
        Audience::Circle(garden())
    )
}

/// The row change of `table`'s row whose `id` column is `id`, as `edits`
/// would change it, read from what `records` wrote of it.
fn edit_of(records: &[WriteRecord], table: &str, id: &str, edits: &[(&str, Value)]) -> RowChange {
    use coven_merge::Operation;
    let rows: Vec<_> = records
        .iter()
        .flat_map(|r| &r.parts)
        .flat_map(|p| &p.rows)
        .filter(|r| r.row.table == table)
        .collect();
    let key = rows
        .iter()
        .find_map(|r| match &r.change.operation {
            Operation::Insert(columns) if columns["id"].value == Value::Text(id.into()) => {
                Some(r.row.key.clone())
            }
            _ => None,
        })
        .expect("the row was inserted");
    let rows: Vec<_> = rows.into_iter().filter(|r| r.row.key == key).collect();
    let mut row = (*rows.last().unwrap()).clone();
    row.change.generation += u64::from(matches!(row.change.operation, Operation::Insert(_)));
    row.old.clear();
    let mut columns = std::collections::BTreeMap::new();
    for (name, value) in edits {
        let previous = rows
            .iter()
            .rev()
            .find_map(|r| match &r.change.operation {
                Operation::Insert(columns) | Operation::Update(columns) => columns.get(*name),
                Operation::Delete => None,
            })
            .expect("an authored column");
        let mut column = previous.clone();
        column.value = value.clone();
        columns.insert((*name).into(), column);
    }
    row.change.operation = Operation::Update(columns);
    row
}

/// A new pin as `insert`, the insert of an existing pin, made it, with its
/// own id.
fn another_pin(insert: &RowChange, id: &str, title: &str) -> RowChange {
    use coven_merge::Operation;
    let mut row = insert.clone();
    let Operation::Insert(columns) = &mut row.change.operation else {
        panic!("an insert");
    };
    columns.get_mut("id").unwrap().value = Value::Text(id.into());
    columns.get_mut("title").unwrap().value = Value::Text(title.into());
    row.row.key = coven_format::key::encode_key(&[Value::Text(id.into())]).unwrap();
    row
}

/// `change` as a write of another device made after `after`, having read
/// through `read_through`, which `db` hasn't applied: its sources are read
/// from `db`'s state.
fn written_by_another(
    db: &Database,
    another: u64,
    change: RowChange,
    read_through: u64,
    after: &WriteHeader,
) -> WriteRecord {
    let row = db.inspect_writer_schema(|connection, schema| {
        let app = crate::write_rows::AppView::after(connection, schema);
        let store = crate::merge_store::MergeStore::new(connection, &app);
        crate::write_sources::authored(connection, &store, change.row, change.change).unwrap()
    });
    let device = DeviceId(another);
    // Newer than the migration write that comes after `after`.
    let timestamp =
        coven_merge::Timestamp::new(after.timestamp.milliseconds() + 60_000 + another, 0, device)
            .unwrap();
    WriteRecord {
        header: WriteHeader {
            position: coven_merge::WriteId { device, number: 1 },
            timestamp,
            read_through,
            schema_version: 1,
            disposition: WriteDisposition::Apply,
        },
        parts: vec![WritePart {
            audience: row.row.audience.clone(),
            rows: vec![row],
            executions: vec![],
            dismissals: vec![],
        }],
    }
}

/// This device holding Garden and the store at version 1, through slot
/// `applied`, and what it saw: the snapshots it took early, the slots after
/// them, and the writes of another device made after `applied` that land
/// next.
struct Holder {
    store: TestStore,
    db: Database,
    store_snapshot: SnapshotFrames,
    circle_snapshot: SnapshotFrames,
    /// The slots after the snapshots, through `applied`.
    counted: Vec<WriteRecord>,
    /// Version 1 writes of another device that land after `applied`: an edit
    /// of a pin's old column, an edit of a column the migration leaves, and
    /// a new pin.
    late: [WriteRecord; 3],
    applied: u64,
}

async fn holder() -> Holder {
    let store = TestStore::with_ids(&SequentialIds::new());
    let db = store
        .builder(filled_tables(), vec![Migration::addition(1, "initial", FILLED_V1)])
        .open()
        .await
        .unwrap();
    connect(&db).await;
    store.join_circle(&db, garden());
    run(&db, "INSERT INTO notes VALUES('n1','alpha')".into()).await;
    run(&db, garden_pin(1, "song", "n1")).await;
    // Early snapshots, taken after the first two slots.
    let store_snapshot = crate::snapshot_write::tests::frames(&db, Audience::Store).await;
    let circle_snapshot =
        crate::snapshot_write::tests::frames(&db, Audience::Circle(garden())).await;
    let before = records_of(&db).len();
    run(&db, garden_pin(2, "second", "n1")).await;
    run(
        &db,
        "UPDATE pins SET title='edited' WHERE id='00000000-0000-4000-8000-000000000001'".into(),
    )
    .await;
    run(&db, "UPDATE notes SET title='alpha two' WHERE id='n1'".into()).await;
    count_queue(&db).await;
    let applied = db.applied_slot().await.unwrap();
    let all = records_of(&db);
    let counted = all[before..].to_vec();
    let last = all.last().unwrap().header.clone();
    let pin = |position: u32| format!("00000000-0000-4000-8000-00000000000{position}");
    let title = edit_of(&all, "pins", &pin(1), &[("title", Value::Text("late".into()))]);
    let note = edit_of(&all, "pins", &pin(2), &[("note", Value::Text("n2".into()))]);
    let insert = all
        .iter()
        .flat_map(|r| &r.parts)
        .flat_map(|p| &p.rows)
        .find(|r| matches!(&r.change.operation, coven_merge::Operation::Insert(c) if c["id"].value == Value::Text(pin(2))))
        .unwrap();
    let new = another_pin(insert, &pin(3), "ana's");
    let late = [
        written_by_another(&db, 10_001, title, applied, &last),
        written_by_another(&db, 10_002, note, applied, &last),
        written_by_another(&db, 10_003, new, applied, &last),
    ];
    Holder { store, db, store_snapshot, circle_snapshot, counted, late, applied }
}

impl Holder {
    /// The holder opened at the newest app, whose migration write is the
    /// last of its queue.
    async fn migrated(self, migrations: Vec<Migration>) -> (Self, WriteRecord) {
        self.db.close().await.unwrap();
        let db = self.store.builder(filled_tables(), migrations).open().await.unwrap();
        let marker = records_of(&db).pop().unwrap();
        assert_eq!(marker.header.disposition, WriteDisposition::Migration);
        assert_eq!(marker.header.read_through, self.applied);
        (Self { db, ..self }, marker)
    }
}

/// A reload of the slots `writes` after slot `after`, every audience as it
/// stands, with the store state of `holder`.
async fn read_forward(
    holder: &Holder,
    after: u64,
    writes: &[WriteRecord],
) -> crate::SnapshotReload<
    crate::SnapshotStaging,
    crate::DownloadedWriteStream<std::io::Cursor<Vec<u8>>>,
> {
    let state = holder.db.store_state().await.unwrap();
    let slots = writes
        .iter()
        .enumerate()
        .map(|(index, write)| {
            let slot = coven_merge::slot(after + index as u64 + 1);
            let device = write.header.position.device;
            crate::IncomingSlot {
                digest: [0x31; 32],
                slot,
                header: coven_format::sequence::SlotHeader {
                    device,
                    claim: write.header.position.number,
                    author: state
                        .devices
                        .get(&device)
                        .map_or(crate::tests::member(1), |d| d.member),
                    after: slot.get() - 1,
                },
                content: crate::SlotContent::Writes {
                    size: 1,
                    writes: vec![crate::snapshot_write::tests::stream(write)],
                },
            }
        })
        .collect();
    crate::SnapshotReload {
        snapshots: vec![
            crate::SnapshotSource::Current(Audience::Store),
            crate::SnapshotSource::Current(Audience::Circle(garden())),
        ],
        slots,
        target: after + writes.len() as u64,
        state,
        cutoffs: vec![],
        provider_access: Vec::new(),
        boundaries: vec![],
        operation: None,
        authorship: std::sync::Arc::new(crate::test_utils::TestAuthorship),
    }
}

/// A rebuild loaded with what `holder` held through its applied slot.
async fn rebuild_through_applied(app: &Database, holder: &Holder) -> RebuildDatabase {
    let rebuild = app.open_rebuild(Audience::Circle(garden()), 1).await.unwrap();
    let reload = reload_into(
        &rebuild,
        &holder.db,
        [&holder.store_snapshot, &holder.circle_snapshot],
        &holder.counted,
    )
    .await;
    assert_eq!(reload.target, holder.applied);
    rebuild.load::<crate::pending::tests::Record, _>(reload, holder.applied).await.unwrap();
    rebuild
}

/// The audience's counted state as a snapshot, whether or not a raise waits.
async fn counted_frames(db: &Database, audience: Audience) -> SnapshotFrames {
    let frames = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let metadata = std::sync::Arc::new(std::sync::Mutex::new(None));
    let output = frames.clone();
    let start = metadata.clone();
    let id =
        coven_format::admin::SnapshotId { audience, device: db.local_device().unwrap(), number: 1 };
    db.write_counted_snapshot(
        id,
        move |header| {
            *start.lock().unwrap() = Some(crate::snapshot_write::tests::prefix(header));
            Ok(())
        },
        move |frame| {
            output.lock().unwrap().push(frame);
            Ok::<_, std::convert::Infallible>(())
        },
    )
    .await
    .unwrap();
    SnapshotFrames {
        prefix: std::sync::Arc::try_unwrap(metadata).unwrap().into_inner().unwrap().unwrap(),
        frames: std::sync::Arc::try_unwrap(frames).unwrap().into_inner().unwrap(),
    }
}

/// What a body holds of an audience's rows: each record but the signatures
/// and the store's slot copies, which depend on who wrote the body.
fn body_of(snapshot: &SnapshotFrames) -> (u64, Vec<String>) {
    let (header, records) = crate::snapshot_write::tests::decode(snapshot);
    let mut kept: Vec<String> = records
        .iter()
        .filter(|record| {
            matches!(
                record,
                SnapshotRecord::Write(_)
                    | SnapshotRecord::Column(_)
                    | SnapshotRecord::Merge(_)
                    | SnapshotRecord::Loss(_)
                    | SnapshotRecord::Authored(_)
                    | SnapshotRecord::Creation(_)
            )
        })
        .map(|record| format!("{record:?}"))
        .collect();
    kept.sort();
    (header.covers, kept)
}

#[tokio::test]
async fn a_rebuilt_circle_is_the_circle_of_a_device_that_held_it_and_migrated() {
    for (old_writes, drops) in [(OldWrites::Drop, true), (convert(), false)] {
        let holder = holder().await;
        let (holder, marker) = holder.migrated(filled_migrations(FILL, old_writes.clone())).await;
        // The slots after the holder's: late writes of version 1 around its
        // marker.
        let [first, second, third] = holder.late.clone();
        let next = [first, second, marker.clone(), third];
        for write in &next {
            holder.db.receive(write.clone().into()).await.unwrap();
        }
        // The rebuilder: this device's id, at the newest app, with no Garden.
        let store = TestStore::with_ids(&SequentialIds::new());
        assert_eq!(store.device(), holder.store.device());
        let app = store
            .builder(filled_tables(), filled_migrations(FILL, old_writes))
            .open()
            .await
            .unwrap();
        let mut rebuild = rebuild_through_applied(&app, &holder).await;
        rebuild.migrate(marker.header.clone()).await.unwrap();
        let forward = read_forward(&holder, holder.applied, &next).await;
        let target = forward.target;
        rebuild.load::<crate::pending::tests::Record, _>(forward, target).await.unwrap();
        let scratch = rebuild.database.as_ref().unwrap();
        for audience in [Audience::Circle(garden()), Audience::Store] {
            let rebuilt = body_of(&counted_frames(scratch, audience.clone()).await);
            let held = body_of(&counted_frames(&holder.db, audience.clone()).await);
            assert_eq!(rebuilt.0, held.0, "{audience}: covered slot");
            assert_eq!(rebuilt.1, held.1, "{audience}: records");
        }
        let pins = crate::tests::contents(scratch)["pins"].clone();
        assert_eq!(pins, crate::tests::contents(&holder.db)["pins"]);
        assert!(
            format!("{pins:?}").contains("alpha two"),
            "the fill read the store's note: {pins:?}"
        );
        let held = body_of(&counted_frames(&holder.db, Audience::Circle(garden())).await).1;
        assert_eq!(
            held.iter().any(|record| record.starts_with("Loss")),
            drops,
            "late writes are kept as lost values when the migration drops them"
        );
        assert!(
            !rebuild.scratch.audience_database_path().exists(),
            "the per-audience check's database is gone"
        );
        rebuild.discard().await.unwrap();
        holder.db.close().await.unwrap();
        app.close().await.unwrap();
    }
}

fn open_error(error: RebuildError) -> crate::DbError {
    match error {
        RebuildError::Open(error) => crate::tests::database_error(error),
        other => panic!("expected an open error, found {other:?}"),
    }
}

/// The app of the rebuilding device: its newest version, with no Garden.
async fn rebuilding_app(
    holder: &Holder,
    statement: &'static str,
    old_writes: OldWrites,
) -> (TestStore, Database) {
    rebuilding_app_with(holder, filled_migrations(statement, old_writes)).await
}

async fn rebuilding_app_with(holder: &Holder, migrations: Vec<Migration>) -> (TestStore, Database) {
    let store = TestStore::with_ids(&SequentialIds::new());
    assert_eq!(store.device(), holder.store.device());
    // Its store loads from storage next, so it queues no migration write of
    // its own beside the holder's.
    let app = store.builder(filled_tables(), migrations).loading_store().open().await.unwrap();
    (store, app)
}

#[tokio::test]
async fn a_header_the_rebuilds_store_cannot_make_is_refused_before_any_statement() {
    let holder = holder().await;
    let (holder, marker) = holder.migrated(filled_migrations(FILL, OldWrites::Drop)).await;
    let (store, app) = rebuilding_app(&holder, FILL, OldWrites::Drop).await;
    let good = marker.header.clone();
    let device = good.position.device;
    let other = DeviceId(good.position.device.0 + 1);
    let cases = [
        (
            WriteHeader { disposition: WriteDisposition::Apply, ..good.clone() },
            "it is not a migration write",
        ),
        (
            WriteHeader { schema_version: 1, ..good.clone() },
            "it is not at a version the run reaches after its newest breaking migration",
        ),
        (
            WriteHeader { schema_version: 3, ..good.clone() },
            "it is not at a version the run reaches after its newest breaking migration",
        ),
        (
            WriteHeader {
                position: coven_merge::WriteId { device: other, number: good.position.number },
                timestamp: coven_merge::Timestamp::new(good.timestamp.milliseconds(), 0, other)
                    .unwrap(),
                ..good.clone()
            },
            "it is another device's write",
        ),
        (
            WriteHeader { position: coven_merge::WriteId { device, number: 1 }, ..good.clone() },
            "it is not newer than this device's writes in the store",
        ),
        (
            WriteHeader {
                timestamp: coven_merge::Timestamp::new(1, 0, device).unwrap(),
                ..good.clone()
            },
            "it is not newer than every write the store holds",
        ),
        (
            WriteHeader { read_through: good.read_through - 1, ..good.clone() },
            "it did not read through the slot the store has applied",
        ),
    ];
    for (header, reason) in cases {
        let mut rebuild = rebuild_through_applied(&app, &holder).await;
        let error = rebuild.migrate(header).await.unwrap_err();
        match open_error(error) {
            crate::DbError::RebuildHeader { reason: found } => assert_eq!(found, reason),
            other => panic!("{reason}: found {other:?}"),
        }
        assert!(!rebuild_folder(&store).exists(), "{reason}: the failed rebuild left its folder");
    }
    // A store that read on before migrating has applied more than the
    // header's write read.
    let mut rebuild = rebuild_through_applied(&app, &holder).await;
    let early = read_forward(&holder, holder.applied, &holder.late[..2]).await;
    let target = early.target;
    rebuild.load::<crate::pending::tests::Record, _>(early, target).await.unwrap();
    match open_error(rebuild.migrate(good).await.unwrap_err()) {
        crate::DbError::RebuildHeader { reason } => {
            assert_eq!(reason, "it did not read through the slot the store has applied");
        }
        other => panic!("found {other:?}"),
    }
    holder.db.close().await.unwrap();
    app.close().await.unwrap();
}

#[tokio::test]
async fn a_migration_that_answers_differently_per_audience_fails_the_rebuild_and_leaves_no_folder()
{
    let holder = holder().await;
    let (holder, marker) = holder.migrated(filled_migrations(FILL, OldWrites::Drop)).await;
    let random = "ALTER TABLE pins RENAME COLUMN title TO name; ALTER TABLE pins ADD COLUMN note_title TEXT; UPDATE pins SET note_title=CAST(random() AS TEXT)";
    let (store, app) = rebuilding_app(&holder, random, OldWrites::Drop).await;
    let mut rebuild = rebuild_through_applied(&app, &holder).await;
    match open_error(rebuild.migrate(marker.header).await.unwrap_err()) {
        crate::DbError::MigrationReadsAcross { table, .. } => assert_eq!(table, "pins"),
        other => panic!("found {other:?}"),
    }
    assert!(!rebuild_folder(&store).exists());
    rebuild.discard().await.unwrap();
    holder.db.close().await.unwrap();
    app.close().await.unwrap();
}

/// A rebuild of Garden for `holder`, as far as the circle at the slots the
/// holder applied: migrated under `marker`'s header and read on through `next`.
async fn rebuilt_to_the_end(
    app: &Database,
    holder: &Holder,
    marker: &WriteRecord,
    next: &[WriteRecord],
) -> RebuildDatabase {
    let mut rebuild = rebuild_through_applied(app, holder).await;
    rebuild.migrate(marker.header.clone()).await.unwrap();
    let forward = read_forward(holder, holder.applied, next).await;
    let target = forward.target;
    rebuild.load::<crate::pending::tests::Record, _>(forward, target).await.unwrap();
    rebuild
}

fn circle_snapshot_id(holder: &Holder) -> coven_format::admin::SnapshotId {
    coven_format::admin::SnapshotId {
        audience: Audience::Circle(garden()),
        device: holder.store.device(),
        number: 1,
    }
}

#[tokio::test]
async fn an_exported_circle_loads_as_the_circle_of_the_holder_and_the_rebuild_is_gone() {
    let holder = holder().await;
    let (holder, marker) = holder.migrated(filled_migrations(FILL, OldWrites::Drop)).await;
    let [first, second, third] = holder.late.clone();
    let next = [first, second, marker.clone(), third];
    for write in &next {
        holder.db.receive(write.clone().into()).await.unwrap();
    }
    let (store, app) = rebuilding_app(&holder, FILL, OldWrites::Drop).await;
    let rebuild = rebuilt_to_the_end(&app, &holder, &marker, &next).await;
    let staging = rebuild
        .export(circle_snapshot_id(&holder), KeyId(uuid::Uuid::from_u128(7)), &app)
        .await
        .unwrap();
    assert!(!rebuild_folder(&store).exists(), "exporting removes the rebuild");
    // The store's rows beside it, from the holder's own snapshot.
    let store_frames = counted_frames(&holder.db, Audience::Store).await;
    let (store_id, store_prefix, store_input, _) = store_frames.input();
    let store_staging =
        crate::test_utils::stage_plaintext(&app, store_id.clone(), store_prefix, store_input)
            .await
            .unwrap();
    let state = holder.db.store_state().await.unwrap();
    app.load_snapshots::<crate::pending::tests::Record, std::io::Empty>(
        crate::SnapshotReload {
            snapshots: vec![
                crate::SnapshotSource::Stored {
                    id: store_id,
                    input: store_staging,
                    author: crate::tests::member(1),
                },
                crate::SnapshotSource::Stored {
                    id: circle_snapshot_id(&holder),
                    input: staging,
                    author: crate::tests::member(1),
                },
            ],
            slots: vec![],
            target: holder.applied + next.len() as u64,
            state,
            cutoffs: vec![],
            provider_access: Vec::new(),
            boundaries: vec![],
            operation: None,
            authorship: std::sync::Arc::new(crate::test_utils::TestAuthorship),
        },
        None,
    )
    .await
    .unwrap();
    let garden_audience = Audience::Circle(garden());
    assert_eq!(
        body_of(&counted_frames(&app, garden_audience.clone()).await),
        body_of(&counted_frames(&holder.db, garden_audience).await),
    );
    assert_eq!(app.staged_snapshot_records().await.unwrap(), 0);
    holder.db.close().await.unwrap();
    app.close().await.unwrap();
}

#[tokio::test]
async fn a_failed_export_leaves_no_folder_and_no_staged_records() {
    let holder = holder().await;
    let (holder, marker) = holder.migrated(filled_migrations(FILL, OldWrites::Drop)).await;
    let [first, second, third] = holder.late.clone();
    let next = [first, second, marker.clone(), third];
    for write in &next {
        holder.db.receive(write.clone().into()).await.unwrap();
    }
    let (store, app) = rebuilding_app(&holder, FILL, OldWrites::Drop).await;
    let rebuild = rebuilt_to_the_end(&app, &holder, &marker, &next).await;
    app.damage_staged_finishes(crate::StagedDamage::Writes, 1).await.unwrap();
    let error = rebuild
        .export(circle_snapshot_id(&holder), KeyId(uuid::Uuid::from_u128(7)), &app)
        .await
        .err()
        .expect("the staged body is refused");
    assert!(matches!(error, RebuildError::Database(_)), "{error:?}");
    assert!(!rebuild_folder(&store).exists());
    assert_eq!(app.staged_snapshot_records().await.unwrap(), 0);
    holder.db.close().await.unwrap();
    app.close().await.unwrap();
}

#[tokio::test]
async fn a_marker_queued_before_a_later_addition_serves_the_rebuild() {
    let holder = holder().await;
    let (holder, marker) = holder.migrated(filled_migrations(FILL, OldWrites::Drop)).await;
    assert_eq!(marker.header.schema_version, 2);
    let mut migrations = filled_migrations(FILL, OldWrites::Drop);
    migrations.push(Migration::addition(3, "extra", "CREATE TABLE extra(id TEXT)"));
    let (_store, app) = rebuilding_app_with(&holder, migrations).await;
    let mut rebuild = rebuild_through_applied(&app, &holder).await;
    rebuild.migrate(marker.header).await.unwrap();
    let scratch = rebuild.database.as_ref().unwrap();
    assert_eq!(scratch.schema_version().await.unwrap(), 3, "the run reaches the app's version");
    rebuild.discard().await.unwrap();
    holder.db.close().await.unwrap();
    app.close().await.unwrap();
}

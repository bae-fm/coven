//! Receipt of foreign-key effects and direct changes of owned rows (§8.8).
//! Ana's, Ben's and Carol's devices declare every table `owner_only()`.
//! Mallory is another device of Ben's member whose app declares no owner
//! tables, so its writes carry whatever Ben's member did: what a receiver
//! accepts is decided by its own check alone.
use super::*;
use crate::owner_permission::tests::{deliver, member, tables, ANA, BEN, SCHEMA};
use crate::snapshot_write::tests::stream;
use crate::tests::TestStore;
use crate::write::tests::{count, records, sql};
use crate::{Database, LostValue, Replacement, RowAuthorizationFailure, RowIdentity, SyncedTable};
use coven_format::snapshot_rows::ChangeVerdict;
use coven_format::value::EntryPositions;
use coven_format::write::{RowChange, WriteRecord};
use coven_foundation::id_source::SequentialIds;
use coven_merge::{Audience, ColumnValue};

pub(crate) const CAROL: u8 = 3;

pub(crate) struct Devices {
    ana: Database,
    ben: Database,
    mallory: Database,
    carol: Database,
    /// A device of Carol's member that has received nothing.
    dan: Database,
    _stores: Vec<TestStore>,
}

/// Every table as Mallory's app declares it: the same, but none owner-only.
fn unchecked() -> Vec<SyncedTable> {
    tables()
        .into_iter()
        .map(|declared| {
            let plain = SyncedTable::new(&declared.name, RowIdentity::SharedKey);
            match declared.name.as_str() {
                "albums" => plain.shared_trigger("album_gone"),
                "reviews" => plain.shared_trigger("review_gone"),
                _ => plain,
            }
        })
        .collect()
}

impl Devices {
    pub(crate) async fn new() -> Self {
        Self::with(tables(), unchecked(), SCHEMA).await
    }

    /// The devices with Mallory's app declaring `unchecked` tables.
    pub(crate) async fn with(
        tables: Vec<SyncedTable>,
        unchecked: Vec<SyncedTable>,
        schema: &'static str,
    ) -> Self {
        let ids = SequentialIds::new();
        let stores: Vec<_> = (0..5).map(|_| TestStore::with_ids(&ids)).collect();
        let ana = stores[0].schema(tables.clone(), schema).await.unwrap();
        let ben = stores[1].schema(tables.clone(), schema).await.unwrap();
        let mallory = stores[2].schema(unchecked, schema).await.unwrap();
        let carol = stores[3].schema(tables.clone(), schema).await.unwrap();
        let dan = stores[4].schema(tables, schema).await.unwrap();
        for db in [&ana, &ben, &mallory, &carol, &dan] {
            TestStore::register(db, stores[0].device(), ANA);
            TestStore::register(db, stores[1].device(), BEN);
            TestStore::register(db, stores[2].device(), BEN);
            TestStore::register(db, stores[3].device(), CAROL);
            TestStore::register(db, stores[4].device(), CAROL);
        }
        Self {
            ana,
            ben,
            mallory,
            carol,
            dan,
            _stores: stores,
        }
    }

    /// Ben's tracks, keys and album; Ana's reviews, comments, pin, labels and
    /// log; every device holds all of them.
    pub(crate) async fn seeded() -> Self {
        let devices = Self::new().await;
        sql(
            &devices.ben,
            "INSERT INTO tracks VALUES('t','Song'),('s','Other'),('w','Watched'),('p','Pinned');
             INSERT INTO keys VALUES('7'),('9');
             INSERT INTO albums VALUES('a');",
        )
        .await
        .unwrap();
        deliver(&devices.ben, &devices.ana).await;
        sql(
            &devices.ana,
            "INSERT INTO reviews VALUES('r','t','good'),('q','s','fine'),('watched','w','seen');
             INSERT INTO comments VALUES('c','r');
             INSERT INTO pins VALUES('pin','p');
             INSERT INTO labels VALUES('l7','7','free'),('l9','9','nine'),('m9',NULL,'other');
             INSERT INTO logs VALUES('log',0);",
        )
        .await
        .unwrap();
        for to in [&devices.ben, &devices.mallory, &devices.carol] {
            deliver(&devices.ben, to).await;
            deliver(&devices.ana, to).await;
        }
        devices
    }

    pub(crate) async fn close(self) {
        for db in [self.ana, self.ben, self.mallory, self.carol, self.dan] {
            db.close().await.unwrap();
        }
    }
}

/// Deliver one write, as its author's device published it.
pub(crate) async fn receive(to: &Database, record: &WriteRecord) -> crate::ApplyOutcome {
    to.receive_stream(stream(record), EntryPositions(Vec::new()), || Ok(()))
        .await
        .unwrap()
}

pub(crate) fn key(table: &str, key: &str) -> RowId {
    RowId {
        table: table.into(),
        key: coven_format::key::encode_key(&[Value::Text(key.into())]).unwrap(),
        audience: Audience::Store,
    }
}

/// The verdict a device recorded for one original change.
pub(crate) fn verdict(db: &Database, record: &WriteRecord, row: &RowId) -> ChangeVerdict {
    db.inspect_writer(|c| stored(c, &record.header, row).unwrap())
}

pub(crate) fn text(db: &Database, sql: &'static str) -> Option<String> {
    db.inspect_writer(|c| {
        c.query_row(sql, [], |r| r.get::<_, Option<String>>(0))
            .unwrap()
    })
}

pub(crate) fn causes(record: &WriteRecord, rows: &[(&str, &str)]) -> Vec<ChangeId> {
    rows.iter()
        .map(|(table, k)| ChangeId {
            write: record.header.position,
            row: key(table, k),
        })
        .collect()
}

pub(crate) fn not_owner(owner: u8) -> ChangeVerdict {
    ChangeVerdict::NotAuthorized(AuthorizationFailure::NotOwner {
        owner: member(owner),
    })
}

/// The lost values of one table's row on a device.
async fn lost(db: &Database, table: &str, row: &str) -> Vec<LostValue> {
    db.lost_values()
        .await
        .unwrap()
        .into_iter()
        .filter(|value| {
            value.table == table && value.key.0 == [rusqlite::types::Value::Text(row.into())]
        })
        .collect()
}

/// Ben's member deletes his track: the cascade reaches Ana's review and on
/// to her comment, each recorded as derived from its causes, with no loss.
#[tokio::test]
async fn authorized_cascades_apply_and_record_their_causes() {
    let devices = Devices::seeded().await;
    sql(&devices.mallory, "DELETE FROM tracks WHERE id='t'")
        .await
        .unwrap();
    let record = records(&devices.mallory).pop().unwrap();
    receive(&devices.ana, &record).await;
    assert_eq!(count(&devices.ana, "comments"), 0);
    assert_eq!(
        text(&devices.ana, "SELECT (SELECT id FROM reviews WHERE id='r')"),
        None
    );
    assert_eq!(
        verdict(&devices.ana, &record, &key("tracks", "t")),
        ChangeVerdict::Applied
    );
    assert_eq!(
        verdict(&devices.ana, &record, &key("reviews", "r")),
        ChangeVerdict::Derived(causes(&record, &[("tracks", "t")]))
    );
    assert_eq!(
        verdict(&devices.ana, &record, &key("comments", "c")),
        ChangeVerdict::Derived(causes(&record, &[("tracks", "t"), ("reviews", "r")]))
    );
    assert!(devices.ana.lost_values().await.unwrap().is_empty());
    // The author's own device recorded the same verdicts when it committed.
    assert_eq!(
        verdict(&devices.mallory, &record, &key("comments", "c")),
        verdict(&devices.ana, &record, &key("comments", "c"))
    );
    devices.close().await;
}

/// A write changing Ben's track and Ana's review directly: the track change
/// applies; the review change is Ana's to make, so it is a frozen loss
/// naming her, and her review keeps its body (§8.8).
#[tokio::test]
async fn a_direct_change_of_another_members_row_is_refused_alone() {
    let devices = Devices::seeded().await;
    sql(
        &devices.mallory,
        "UPDATE tracks SET title='kept' WHERE id='s'; UPDATE reviews SET body='mine' WHERE id='r'",
    )
    .await
    .unwrap();
    let record = records(&devices.mallory).pop().unwrap();
    receive(&devices.ana, &record).await;
    assert_eq!(
        text(&devices.ana, "SELECT title FROM tracks WHERE id='s'"),
        Some("kept".into())
    );
    assert_eq!(
        text(&devices.ana, "SELECT body FROM reviews WHERE id='r'"),
        Some("good".into())
    );
    assert_eq!(
        verdict(&devices.ana, &record, &key("reviews", "r")),
        not_owner(ANA)
    );
    let lost = lost(&devices.ana, "reviews", "r").await;
    assert_eq!(lost.len(), 1);
    assert_eq!(
        lost[0].replaced_by,
        Replacement::NotAuthorized {
            write: record.header.position,
            reason: RowAuthorizationFailure::NotOwner { owner: member(ANA) },
        }
    );
    // Dismissing it changes no row.
    devices.ana.dismiss_lost_values(&lost).await.unwrap();
    assert!(lost_is_empty(&devices.ana, "reviews", "r").await);
    assert_eq!(
        text(&devices.ana, "SELECT body FROM reviews WHERE id='r'"),
        Some("good".into())
    );
    devices.close().await;
}

async fn lost_is_empty(db: &Database, table: &str, row: &str) -> bool {
    lost(db, table, row).await.is_empty()
}

/// A shared trigger's write is direct even when a cascade fires it; the
/// unauthorized root of a cascade authorizes none of its effects.
#[tokio::test]
async fn trigger_writes_and_unauthorized_roots_authorize_nothing() {
    let devices = Devices::seeded().await;
    // The trigger on Ben's album writes Ana's review.
    sql(&devices.mallory, "DELETE FROM albums WHERE id='a'")
        .await
        .unwrap();
    let trigger = records(&devices.mallory).pop().unwrap();
    // Deleting Ana's review directly cascades to her comment.
    sql(&devices.mallory, "DELETE FROM reviews WHERE id='r'")
        .await
        .unwrap();
    let root = records(&devices.mallory).pop().unwrap();
    receive(&devices.ana, &trigger).await;
    receive(&devices.ana, &root).await;
    assert_eq!(
        verdict(&devices.ana, &trigger, &key("albums", "a")),
        ChangeVerdict::Applied
    );
    assert_eq!(
        verdict(&devices.ana, &trigger, &key("reviews", "r")),
        not_owner(ANA)
    );
    assert_eq!(
        verdict(&devices.ana, &root, &key("reviews", "r")),
        not_owner(ANA)
    );
    assert_eq!(
        verdict(&devices.ana, &root, &key("comments", "c")),
        not_owner(ANA)
    );
    assert_eq!(count(&devices.ana, "albums"), 0);
    assert_eq!(count(&devices.ana, "comments"), 1);
    assert_eq!(
        text(&devices.ana, "SELECT body FROM reviews WHERE id='r'"),
        Some("good".into())
    );
    devices.close().await;
}

/// Set one column in a row change's net change and in each of its steps.
fn set(row: &mut RowChange, column: &str, value: Value) {
    for operation in std::iter::once(&mut row.change.operation).chain(
        row.execution
            .steps
            .iter_mut()
            .map(|step| &mut step.change.operation),
    ) {
        if let coven_merge::Operation::Update(columns) = operation {
            columns.insert(
                column.into(),
                ColumnValue {
                    value: value.clone(),
                    parents: Default::default(),
                },
            );
        }
    }
}

/// Ben's key changing 7 to 8 sets Ana's label's reference NULL. Any other
/// payload makes it a direct change of her label.
#[tokio::test]
async fn forged_action_payloads_are_direct_changes() {
    let label = key("labels", "l7");
    let forge = |genuine: &WriteRecord, change: &dyn Fn(&mut RowChange)| {
        let mut forged = genuine.clone();
        change(
            forged.parts[0]
                .rows
                .iter_mut()
                .find(|row| row.row == label)
                .unwrap(),
        );
        forged
    };
    let cases: [&dyn Fn(&mut RowChange); 3] = [
        // A changed scalar where the action writes NULL.
        &|row| set(row, "key", Value::Text("x".into())),
        // An additional column beside the action's.
        &|row| {
            set(row, "slot", Value::Text("taken".into()));
            row.old.insert("slot".into(), Value::Text("free".into()));
        },
        // The net change differs from the action that produced it.
        &|row| {
            let coven_merge::Operation::Update(columns) = &mut row.change.operation else {
                unreachable!("SET NULL is an update")
            };
            columns.get_mut("key").unwrap().value = Value::Text("y".into());
        },
    ];
    for case in cases {
        let devices = Devices::seeded().await;
        sql(&devices.mallory, "UPDATE keys SET id='8' WHERE id='7'")
            .await
            .unwrap();
        let genuine = records(&devices.mallory).pop().unwrap();
        let forged = forge(&genuine, case);
        receive(&devices.ana, &forged).await;
        assert_eq!(verdict(&devices.ana, &forged, &label), not_owner(ANA));
        assert_eq!(
            verdict(&devices.ana, &forged, &key("keys", "7")),
            ChangeVerdict::Applied
        );
        // Without the action, Ana's label still points at Ben's old key, whose
        // deletion takes it out under its NO ACTION delete rule (§8.4).
        assert_eq!(count(&devices.ana, "labels"), 2);
        let lost = lost(&devices.ana, "labels", "l7").await;
        assert!(lost.iter().any(|value| matches!(
            &value.replaced_by,
            Replacement::NotAuthorized { reason: RowAuthorizationFailure::NotOwner { owner }, .. }
                if *owner == member(ANA)
        )));
        assert!(lost
            .iter()
            .any(|value| matches!(value.replaced_by, Replacement::Rules(_))));
        // The genuine write's action applies on another device.
        receive(&devices.carol, &genuine).await;
        assert_eq!(
            verdict(&devices.carol, &genuine, &label),
            ChangeVerdict::Derived(causes(&genuine, &[("keys", "7")]))
        );
        assert_eq!(
            text(
                &devices.carol,
                "SELECT (SELECT key FROM labels WHERE id='l7')"
            ),
            None
        );
        assert_eq!(count(&devices.carol, "labels"), 3);
        devices.close().await;
    }
}

/// Every device judges the same writes the same way, whichever arrives
/// first.
#[tokio::test]
async fn verdicts_agree_in_both_arrival_orders() {
    let devices = Devices::seeded().await;
    sql(
        &devices.mallory,
        "UPDATE reviews SET body='mine' WHERE id='q'; DELETE FROM tracks WHERE id='t'",
    )
    .await
    .unwrap();
    let forged = records(&devices.mallory).pop().unwrap();
    sql(&devices.ana, "UPDATE labels SET slot='ana' WHERE id='m9'")
        .await
        .unwrap();
    let concurrent = records(&devices.ana).pop().unwrap();
    receive(&devices.carol, &forged).await;
    receive(&devices.carol, &concurrent).await;
    receive(&devices.ben, &concurrent).await;
    receive(&devices.ben, &forged).await;
    for db in [&devices.ben, &devices.carol] {
        assert_eq!(verdict(db, &forged, &key("reviews", "q")), not_owner(ANA));
        assert_eq!(
            verdict(db, &forged, &key("reviews", "r")),
            ChangeVerdict::Derived(causes(&forged, &[("tracks", "t")]))
        );
    }
    let state = |db: &Database| {
        db.inspect_writer(|c| {
            c.query("SELECT id,body FROM reviews ORDER BY id", [], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
            })
            .unwrap()
        })
    };
    assert_eq!(state(&devices.ben), state(&devices.carol));
    assert_eq!(
        devices.ben.lost_values().await.unwrap(),
        devices.carol.lost_values().await.unwrap()
    );
    devices.close().await;
}

/// Notes point at two tracks, each reference set NULL when its track goes.
const TWO_PARENTS: &str = "CREATE TABLE tracks(id TEXT NOT NULL PRIMARY KEY, title TEXT);
CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,
  a TEXT REFERENCES tracks(id) ON DELETE SET NULL,
  b TEXT REFERENCES tracks(id) ON DELETE SET NULL, body TEXT);";

fn two_parents(owned: bool) -> Vec<SyncedTable> {
    ["tracks", "notes"]
        .into_iter()
        .map(|table| {
            let declared = SyncedTable::new(table, RowIdentity::SharedKey);
            if owned {
                declared.owner_only()
            } else {
                declared
            }
        })
        .collect()
}

/// Ben's tracks x and y, and Ana's note pointing at both, on every device.
async fn two_parent_devices() -> Devices {
    let devices = Devices::with(two_parents(true), two_parents(false), TWO_PARENTS).await;
    sql(&devices.ben, "INSERT INTO tracks VALUES('x','X'),('y','Y')")
        .await
        .unwrap();
    deliver(&devices.ben, &devices.ana).await;
    sql(&devices.ana, "INSERT INTO notes VALUES('n','x','y','both')")
        .await
        .unwrap();
    for to in [&devices.ben, &devices.mallory, &devices.carol] {
        deliver(&devices.ben, to).await;
        deliver(&devices.ana, to).await;
    }
    devices
}

/// Deleting both tracks in one write sets each of the note's references
/// NULL: the note's change is both actions combined, caused by both deletes.
/// When one delete is refused, the combined change goes with it, while the
/// other delete still applies (`dropped_contributing_root_no_effect`).
#[tokio::test]
async fn coalesced_actions_keep_every_cause_and_lose_with_any() {
    let devices = two_parent_devices().await;
    sql(&devices.mallory, "DELETE FROM tracks").await.unwrap();
    let genuine = records(&devices.mallory).pop().unwrap();
    let note = key("notes", "n");
    let change = genuine.parts[0]
        .rows
        .iter()
        .find(|row| row.row == note)
        .unwrap();
    assert_eq!(change.execution.steps.len(), 2);
    receive(&devices.carol, &genuine).await;
    assert_eq!(
        verdict(&devices.carol, &genuine, &note),
        ChangeVerdict::Derived(causes(&genuine, &[("tracks", "x"), ("tracks", "y")]))
    );
    assert_eq!(
        text(
            &devices.carol,
            "SELECT coalesce(a,'') || coalesce(b,'') FROM notes"
        ),
        Some(String::new())
    );
    // Track y's net change no longer is where its delete ends: its delete
    // causes nothing, and the note's combined change loses with it.
    let forged = inconsistent_root(&genuine);
    receive(&devices.ana, &forged).await;
    assert_eq!(
        verdict(&devices.ana, &forged, &key("tracks", "y")),
        ChangeVerdict::NotAuthorized(AuthorizationFailure::InvalidForeignKeyEffect)
    );
    assert_eq!(
        verdict(&devices.ana, &forged, &key("tracks", "x")),
        ChangeVerdict::Applied
    );
    assert_eq!(
        verdict(&devices.ana, &forged, &note),
        ChangeVerdict::Dependent(causes(&forged, &[("tracks", "y")]))
    );
    // The note keeps both written references; x's applied delete shows its
    // reference as NULL, as for any child whose parent went (§8.4).
    assert_eq!(
        text(
            &devices.ana,
            "SELECT coalesce(a,'-') || b FROM notes WHERE id='n'"
        ),
        Some("-y".into())
    );
    let lost = lost(&devices.ana, "notes", "n").await;
    assert!(lost.iter().any(|value| matches!(
        &value.replaced_by,
        Replacement::DataDependency { sources, .. }
            if sources.len() == 1 && sources[0].table == "tracks"
    )));
    devices.close().await;
}

/// The write with track y's net change an update its steps never made.
fn inconsistent_root(genuine: &WriteRecord) -> WriteRecord {
    let mut forged = genuine.clone();
    let y = forged.parts[0]
        .rows
        .iter_mut()
        .find(|row| row.row == key("tracks", "y"))
        .unwrap();
    let title = y.sources.setters["title"].clone();
    y.change.operation = coven_merge::Operation::Update(
        [(
            "title".to_owned(),
            ColumnValue {
                value: Value::Text("still here".into()),
                parents: Default::default(),
            },
        )]
        .into(),
    );
    y.old = [("title".to_owned(), Value::Text("Y".into()))].into();
    y.sources.setters = [("title".to_owned(), title)].into();
    forged
}

/// A snapshot carries every verdict with its data: the device loading it
/// shows the same refusals, and later changes built on a refused change
/// are judged against the loaded verdict.
#[tokio::test]
async fn snapshots_carry_verdicts_and_refusals() {
    let devices = Devices::seeded().await;
    sql(
        &devices.mallory,
        "UPDATE reviews SET body='mine' WHERE id='r'; DELETE FROM tracks WHERE id='s'",
    )
    .await
    .unwrap();
    let forged = records(&devices.mallory).pop().unwrap();
    receive(&devices.carol, &forged).await;
    let snapshot = crate::snapshot_write::tests::frames(&devices.carol, Audience::Store).await;
    let (id, prefix, input, _) = snapshot.input();
    crate::snapshot_write::tests::load_one(&devices.dan, id, prefix, input)
        .await
        .unwrap();
    for row in [key("reviews", "r"), key("reviews", "q"), key("tracks", "s")] {
        assert_eq!(
            verdict(&devices.dan, &forged, &row),
            verdict(&devices.carol, &forged, &row),
            "{row:?}"
        );
    }
    assert_eq!(
        verdict(&devices.dan, &forged, &key("reviews", "r")),
        not_owner(ANA)
    );
    assert_eq!(
        devices.dan.lost_values().await.unwrap(),
        devices.carol.lost_values().await.unwrap()
    );
    devices.close().await;
}

/// Tracks and the reviews that cascade from them, with no owner tables: the
/// restoration below is the data rule's, whoever owns the rows.
const CASCADE: &str = "CREATE TABLE tracks(id TEXT NOT NULL PRIMARY KEY, title TEXT);
CREATE TABLE reviews(id TEXT NOT NULL PRIMARY KEY, track TEXT REFERENCES tracks(id) ON DELETE CASCADE, body TEXT);";

/// The cascade restoration witness (B18 `cascade_reversed`). Ben's track
/// delete replaced the title another of his devices set, in a write that
/// later lands outside its device's retirement window. Replay drops that
/// write, so the delete used dropped data; its cascade to Ana's review is
/// dropped with it as a dependent change, and both rows return with their
/// original values, the review with Ana's setter (§8, §8.8, §10).
#[tokio::test]
async fn a_dropped_cause_restores_the_rows_its_actions_changed() {
    use crate::pending::tests::Record;
    use crate::write_landing::tests::{at, retire, DAY};
    let declared = || {
        ["tracks", "reviews"]
            .map(|table| SyncedTable::new(table, RowIdentity::SharedKey))
            .to_vec()
    };
    let ids = SequentialIds::new();
    let stores: Vec<_> = (0..4).map(|_| TestStore::with_ids(&ids)).collect();
    let mut dbs = Vec::new();
    for store in &stores {
        dbs.push(store.schema(declared(), CASCADE).await.unwrap());
    }
    let [ben, other, ana, target] = [&dbs[0], &dbs[1], &dbs[2], &dbs[3]];
    sql(ben, "INSERT INTO tracks VALUES('t','Song')")
        .await
        .unwrap();
    deliver(ben, other).await;
    deliver(ben, ana).await;
    sql(other, "UPDATE tracks SET title='Renamed' WHERE id='t'")
        .await
        .unwrap();
    deliver(other, ben).await;
    sql(ana, "INSERT INTO reviews VALUES('r','t','good')")
        .await
        .unwrap();
    deliver(ana, ben).await;
    sql(ben, "DELETE FROM tracks WHERE id='t'").await.unwrap();
    let ben_writes = records(ben);
    let (create, delete) = (&ben_writes[0], ben_writes.last().unwrap());
    let rename = &records(other)[0];
    let review = &records(ana)[0];
    for (write, day) in [(create, 0), (rename, 31), (review, 0), (delete, 0)] {
        target
            .apply_downloaded::<Record>(
                write.clone().into(),
                crate::test_utils::published(at(day * DAY)),
            )
            .await
            .unwrap();
    }
    assert_eq!(count(target, "tracks"), 0);
    assert_eq!(count(target, "reviews"), 0);
    assert_eq!(
        verdict(target, delete, &key("reviews", "r")),
        ChangeVerdict::Derived(causes(delete, &[("tracks", "t")]))
    );
    retire(target, stores[1].device(), 1, "device", 0).await;
    assert_eq!(
        text(target, "SELECT title FROM tracks WHERE id='t'"),
        Some("Song".into())
    );
    assert_eq!(
        text(target, "SELECT body FROM reviews WHERE id='r'"),
        Some("good".into())
    );
    assert_eq!(
        verdict(target, rename, &key("tracks", "t")),
        ChangeVerdict::Dropped
    );
    assert_eq!(
        verdict(target, delete, &key("tracks", "t")),
        ChangeVerdict::Dependent(vec![ChangeId {
            write: rename.header.position,
            row: key("tracks", "t"),
        }])
    );
    assert_eq!(
        verdict(target, delete, &key("reviews", "r")),
        ChangeVerdict::Dependent(causes(delete, &[("tracks", "t")]))
    );
    let lost = lost(target, "reviews", "r").await;
    assert!(
        lost.iter().any(|value| matches!(
            &value.replaced_by,
            Replacement::DataDependency { write, sources }
                if *write == delete.header.position && sources.len() == 1
                    && sources[0].table == "tracks"
        )),
        "{lost:?}"
    );
    for db in dbs {
        db.close().await.unwrap();
    }
}

/// Codes have INTEGER keys that tags hold as text. Codes are unowned, so Ana
/// can insert one after the tag that references it, as a deferred key allows.
const AFFINITY: &str = "CREATE TABLE codes(id INT NOT NULL PRIMARY KEY) WITHOUT ROWID;
CREATE TABLE tags(id TEXT NOT NULL PRIMARY KEY, code TEXT REFERENCES codes(id)
  ON DELETE CASCADE ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED);";

fn coded(owned: bool) -> Vec<SyncedTable> {
    let tags = SyncedTable::new("tags", RowIdentity::SharedKey);
    vec![
        SyncedTable::new("codes", RowIdentity::SharedKey),
        if owned { tags.owner_only() } else { tags },
    ]
}

/// SQLite compares Ana's text '7' with code 7 in the code's affinity, and a
/// cascade stores the new key 8 as the text '8'. Ben's delete and key update
/// reach her tag whether she inserted the code before or after the tag: his
/// own device commits them, and a receiver records the same derived change.
#[tokio::test]
async fn actions_across_affinities_are_actions() {
    for seed in [
        "INSERT INTO tags VALUES('k','7'); INSERT INTO codes VALUES(7);",
        "INSERT INTO codes VALUES(7); INSERT INTO tags VALUES('k','7');",
    ] {
        for (statement, left) in [
            ("DELETE FROM codes", None),
            ("UPDATE codes SET id=8", Some("8".to_owned())),
        ] {
            let devices = Devices::with(coded(true), coded(false), AFFINITY).await;
            sql(&devices.ana, seed).await.unwrap();
            deliver(&devices.ana, &devices.ben).await;
            deliver(&devices.ana, &devices.carol).await;
            sql(&devices.ben, statement).await.unwrap();
            let record = records(&devices.ben).pop().unwrap();
            let tag = key("tags", "k");
            let code = RowId {
                table: "codes".into(),
                key: coven_format::key::encode_key(&[Value::Integer(7)]).unwrap(),
                audience: Audience::Store,
            };
            let derived = ChangeVerdict::Derived(vec![ChangeId {
                write: record.header.position,
                row: code,
            }]);
            assert_eq!(
                verdict(&devices.ben, &record, &tag),
                derived,
                "{seed} {statement}"
            );
            receive(&devices.carol, &record).await;
            assert_eq!(
                verdict(&devices.carol, &record, &tag),
                derived,
                "{seed} {statement}"
            );
            assert_eq!(
                text(
                    &devices.carol,
                    "SELECT (SELECT code FROM tags WHERE id='k')"
                ),
                left
            );
            devices.close().await;
        }
    }
}

/// The receipts the model comparison checks (`judge_view::tests`), each
/// passed with the device that received it: direct, derived, transitive and
/// coalesced changes, triggers, unauthorized roots, a forged action payload,
/// actions across affinities and by value alone, and an action whose cause is
/// refused.
pub(crate) async fn receipts(mut compare: impl FnMut(&Database, &WriteRecord)) {
    for statement in [
        "DELETE FROM tracks WHERE id='t'",
        "UPDATE keys SET id='8' WHERE id='7'; UPDATE keys SET id='7' WHERE id='8'",
        "UPDATE reviews SET body='mine' WHERE id='q'; DELETE FROM tracks WHERE id='t'",
        "DELETE FROM albums WHERE id='a'",
        "DELETE FROM reviews WHERE id='r'",
        "DELETE FROM tracks WHERE id='w'",
        "UPDATE keys SET id='10' WHERE id='9'; INSERT INTO tracks VALUES('new','New')",
    ] {
        let devices = Devices::seeded().await;
        sql(&devices.mallory, statement).await.unwrap();
        let record = records(&devices.mallory).pop().unwrap();
        receive(&devices.ana, &record).await;
        compare(&devices.ana, &record);
        devices.close().await;
    }
    let devices = Devices::seeded().await;
    sql(&devices.mallory, "UPDATE keys SET id='8' WHERE id='7'")
        .await
        .unwrap();
    let mut forged = records(&devices.mallory).pop().unwrap();
    let label = forged.parts[0]
        .rows
        .iter_mut()
        .find(|row| row.row == key("labels", "l7"))
        .unwrap();
    set(label, "slot", Value::Text("taken".into()));
    label.old.insert("slot".into(), Value::Text("free".into()));
    receive(&devices.ana, &forged).await;
    compare(&devices.ana, &forged);
    devices.close().await;
    // Ana's tag holds code 7 as text; Mallory's last write inserts his own
    // tag before its code, so it names that code by value alone.
    for statement in [
        "DELETE FROM codes",
        "UPDATE codes SET id=8",
        "INSERT INTO tags VALUES('b','9'); INSERT INTO codes VALUES(9);
         UPDATE codes SET id=10 WHERE id=9; DELETE FROM codes",
    ] {
        let devices = Devices::with(coded(true), coded(false), AFFINITY).await;
        sql(
            &devices.ana,
            "INSERT INTO tags VALUES('k','7'); INSERT INTO codes VALUES(7);",
        )
        .await
        .unwrap();
        deliver(&devices.ana, &devices.mallory).await;
        deliver(&devices.ana, &devices.carol).await;
        sql(&devices.mallory, statement).await.unwrap();
        let record = records(&devices.mallory).pop().unwrap();
        receive(&devices.carol, &record).await;
        compare(&devices.carol, &record);
        devices.close().await;
    }
    let devices = two_parent_devices().await;
    sql(&devices.mallory, "DELETE FROM tracks").await.unwrap();
    let genuine = records(&devices.mallory).pop().unwrap();
    receive(&devices.carol, &genuine).await;
    compare(&devices.carol, &genuine);
    let forged = inconsistent_root(&genuine);
    receive(&devices.ana, &forged).await;
    assert!(matches!(
        verdict(&devices.ana, &forged, &key("notes", "n")),
        ChangeVerdict::Dependent(_)
    ));
    compare(&devices.ana, &forged);
    devices.close().await;
}

//! A local write evaluates the rows it inserts only where something the app
//! can't see could take them out (§9.3). Each test compares the device that
//! made the write with a device that receives it and judges every row in full.

use crate::author_judgement::tests::{deliver, Pair};
use crate::tests::TestStore;
use crate::write::tests::{count, records};
use crate::{ApplyOutcome, Database, Migration, OldWrites, RowIdentity, SyncedTable};
use coven_foundation::id_source::{CircleId, SequentialIds};
use coven_merge::{Audience, Rule};
use std::collections::BTreeSet;

const GIFTS: CircleId = CircleId(uuid::Uuid::from_u128(10));
const K: &str = "00000000-0000-4000-8000-00000000000a";
const M: &str = "00000000-0000-4000-8000-00000000000b";

/// Run `statements` as one write.
async fn run(database: &Database, statements: String) {
    database
        .write(move |context| {
            context.execute_batch(&statements)?;
            Ok(())
        })
        .await
        .unwrap();
}

/// A removed row: its table, key and audience, with the rules that took it out.
type Removed = (String, Vec<u8>, String, BTreeSet<Rule>);

/// What a device shows of `tables`, and which rows its rules took out.
fn state(database: &Database, tables: &[&str]) -> (Vec<String>, Vec<Removed>) {
    database.inspect_writer(|db| {
        let mut shown = Vec::new();
        for table in tables {
            for id in db
                .query(&format!("SELECT id FROM {table} ORDER BY id"), [], |r| r.get::<_, String>(0))
                .unwrap()
            {
                shown.push(format!("{table} {id}"));
            }
        }
        let removed = db
            .query(
                "SELECT table_name,key,audience,replaced_by FROM _coven_lost
                 WHERE column_id IS NULL AND replacement_kind='rules' ORDER BY table_name,key,audience",
                [],
                |r| {
                    Ok((
                        r.get(0)?,
                        r.get(1)?,
                        r.get(2)?,
                        coven_format::merge_fields::decode_rules(&r.get::<_, Vec<u8>>(3)?).unwrap(),
                    ))
                },
            )
            .unwrap();
        (shown, removed)
    })
}

/// Notes with a unique title in each audience.
const NOTES: &str = "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, audience TEXT NOT NULL,
    title TEXT NOT NULL, UNIQUE(audience, title))";

async fn notes_pair() -> Pair {
    let pair = Pair::with(
        vec![SyncedTable::new("notes", RowIdentity::IndependentUuid).audience_column("audience")],
        NOTES,
    )
    .await;
    pair.circle(GIFTS);
    pair
}

/// Ana's write of `statements`, as she shows it before her own write lands,
/// and as Ben shows it once he has judged every row of it.
async fn written_by_ana(pair: &Pair, statements: String) -> [(Vec<String>, Vec<Removed>); 2] {
    run(&pair.ana, statements).await;
    let author = state(&pair.ana, &["notes"]);
    deliver(&pair.ana, &pair.ben).await;
    [author, state(&pair.ben, &["notes"])]
}

/// Ana's note k in Gifts titled "Plan" is taken out by the store's note k,
/// titled "Other": the key is in two audiences, and the store's wins.
/// Her claim on "Plan" in Gifts stays, held by a row that is out.
#[tokio::test]
async fn a_new_note_taking_a_value_a_row_that_is_out_claims_is_taken_out() {
    let pair = notes_pair().await;
    run(&pair.ana, format!("INSERT INTO notes VALUES('{K}','{GIFTS}','Plan')")).await;
    run(&pair.ben, format!("INSERT INTO notes VALUES('{K}','store','Other')")).await;
    deliver(&pair.ana, &pair.ben).await;
    deliver(&pair.ben, &pair.ana).await;
    let [author, receiver] =
        written_by_ana(&pair, format!("INSERT INTO notes VALUES('{M}','{GIFTS}','Plan')")).await;
    assert_eq!(author, receiver);
    assert_eq!(author.0, [format!("notes {K}")]);
    let rules: Vec<_> = author.1.iter().map(|(_, _, _, rules)| rules.clone()).collect();
    assert_eq!(
        rules,
        [BTreeSet::from([Rule::OtherAudience]), BTreeSet::from([Rule::Unique(plan_title())])]
    );
    pair.close().await;
}

/// The store's note k is taken out after it loses its title to note m. Ana
/// inserts note k into Gifts: the store's note k, though out, still holds the
/// key, so hers is taken out under the two-audience rule.
#[tokio::test]
async fn a_new_note_under_a_key_a_row_that_is_out_holds_is_taken_out() {
    let pair = notes_pair().await;
    run(&pair.ana, format!("INSERT INTO notes VALUES('{M}','store','Plan')")).await;
    run(&pair.ben, format!("INSERT INTO notes VALUES('{K}','store','Plan')")).await;
    deliver(&pair.ana, &pair.ben).await;
    deliver(&pair.ben, &pair.ana).await;
    assert_eq!(state(&pair.ana, &["notes"]).0, [format!("notes {M}")]);
    let [author, receiver] =
        written_by_ana(&pair, format!("INSERT INTO notes VALUES('{K}','{GIFTS}','Gift')")).await;
    assert_eq!(author, receiver);
    assert_eq!(author.0, [format!("notes {M}")]);
    let rules: Vec<_> = author.1.iter().map(|(_, _, audience, rules)| (audience, rules)).collect();
    assert_eq!(
        rules,
        [
            (&GIFTS.to_string(), &BTreeSet::from([Rule::OtherAudience])),
            (&"store".to_string(), &BTreeSet::from([Rule::Unique(plan_title())])),
        ]
    );
    pair.close().await;
}

fn plan_title() -> coven_merge::UniqueConstraint {
    coven_merge::UniqueConstraint { terms: vec!["audience".into(), "title".into()], partial: None }
}

/// Notes and the attachments that cascade from them.
const ATTACHMENTS: &str = "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL);
    CREATE TABLE attachments(id TEXT NOT NULL PRIMARY KEY,
        note TEXT NOT NULL REFERENCES notes(id) ON DELETE CASCADE)";

fn attachment_tables() -> Vec<SyncedTable> {
    ["notes", "attachments"]
        .into_iter()
        .map(|table| SyncedTable::new(table, RowIdentity::SharedKey))
        .collect()
}

fn attachment_migrations(version: u32) -> Vec<Migration> {
    let mut steps = vec![Migration::addition(1, "schema", ATTACHMENTS)];
    if version > 1 {
        steps.push(Migration::breaking(
            2,
            "index",
            "CREATE INDEX notes_title ON notes(title)",
            OldWrites::Drop,
            Vec::new(),
        ));
    }
    steps
}

/// Ben's write 1 creates note 50 under schema version 1, and his write 2,
/// under version 2, adds attachment 9 to it. The devices that count version
/// 2 on drop write 1 and keep write 2: attachment 9 is out, pointing at a
/// note no row holds. Ana adds note 50 herself, and attachment 9 comes back.
#[tokio::test]
async fn a_new_note_brings_back_the_row_stored_pointing_at_it() {
    let ids = SequentialIds::new();
    let (ben_store, ana_store, carol_store) =
        (TestStore::with_ids(&ids), TestStore::with_ids(&ids), TestStore::with_ids(&ids));
    let ben =
        ben_store.builder(attachment_tables(), attachment_migrations(1)).open().await.unwrap();
    run(&ben, "INSERT INTO notes VALUES('50','Fifty')".into()).await;
    ben.close().await.unwrap();
    let ben =
        ben_store.builder(attachment_tables(), attachment_migrations(2)).open().await.unwrap();
    run(&ben, "INSERT INTO attachments VALUES('9','50')".into()).await;
    let writes = records(&ben);
    // Write 1, then the migration's own write, then write 2.
    assert_eq!(writes.iter().map(|w| w.header.schema_version).collect::<Vec<_>>(), [1, 2, 2]);

    let mut receivers = Vec::new();
    for store in [&ana_store, &carol_store] {
        let db = store.builder(attachment_tables(), attachment_migrations(2)).open().await.unwrap();
        for (device, seed) in [(ana_store.device(), 1), (ben_store.device(), 2)] {
            TestStore::register(&db, device, seed);
        }
        db.install_schema_fixture(Audience::Store, 2, 0).await.unwrap();
        for write in &writes {
            assert_eq!(db.receive(write.clone().into()).await.unwrap(), ApplyOutcome::Applied);
        }
        receivers.push(db);
    }
    let [ana, carol] = <[Database; 2]>::try_from(receivers).ok().unwrap();
    let tables = ["notes", "attachments"];
    assert_eq!(state(&ana, &tables).0, Vec::<String>::new());
    assert_eq!(state(&ana, &tables).1.len(), 1, "attachment 9 is out");

    run(&ana, "INSERT INTO notes VALUES('50','Fifty')".into()).await;
    let author = state(&ana, &tables);
    deliver(&ana, &carol).await;
    assert_eq!(author, state(&carol, &tables));
    assert_eq!(author.0, ["notes 50", "attachments 9"].map(String::from));
    assert!(author.1.is_empty());
    for db in [ben, ana, carol] {
        db.close().await.unwrap();
    }
}

/// Albums, releases under them and tracks on those, each with the CHECK, UNIQUE
/// and foreign key an import's rows have.
const LIBRARY: &str = "CREATE TABLE albums(id TEXT NOT NULL PRIMARY KEY,
        title TEXT NOT NULL CHECK(length(title) > 0));
    CREATE TABLE releases(id TEXT NOT NULL PRIMARY KEY,
        album TEXT NOT NULL REFERENCES albums(id) ON DELETE CASCADE,
        year INTEGER NOT NULL, UNIQUE(album, year));
    CREATE TABLE tracks(id TEXT NOT NULL PRIMARY KEY,
        release TEXT NOT NULL REFERENCES releases(id) ON DELETE CASCADE,
        position INTEGER NOT NULL CHECK(position > 0), UNIQUE(release, position))";

/// A library holding album `a`, its release `r` and tracks `t1` and `t2`,
/// counting what its removal views evaluate.
async fn library() -> (Database, TestStore, crate::test_utils::WorkCounter) {
    let store = TestStore::new();
    let tables = ["albums", "releases", "tracks"]
        .into_iter()
        .map(|table| SyncedTable::new(table, RowIdentity::SharedKey))
        .collect();
    let db = store.schema(tables, LIBRARY).await.unwrap();
    run(
        &db,
        "INSERT INTO albums VALUES('a','Album');
         INSERT INTO releases VALUES('r','a',1999);
         INSERT INTO tracks VALUES('t1','r',1),('t2','r',2)"
            .into(),
    )
    .await;
    let counter = crate::test_utils::WorkCounter::new();
    db.test_track_work(counter.clone()).await.unwrap();
    (db, store, counter)
}

/// The rows the removal views evaluate while `statements` write.
async fn evaluating(statements: &str) -> usize {
    let (db, _store, counter) = library().await;
    run(&db, statements.into()).await;
    let evaluated = counter.take().evaluated_rows;
    db.close().await.unwrap();
    evaluated
}

/// An import's rows have new keys, valid values and parents the app shows:
/// SQLite judged all of it, and no removal rule has a reason to look at them.
#[tokio::test]
async fn a_write_of_new_rows_evaluates_none_of_them() {
    let evaluated = evaluating(
        "INSERT INTO albums VALUES('b','Another');
         INSERT INTO releases VALUES('s','b',2005);
         INSERT INTO tracks VALUES('u1','s',1),('u2','s',2),('u3','s',3)",
    )
    .await;
    assert_eq!(evaluated, 0);
}

/// A row the write inserts under a parent the same write edits is judged with
/// that parent's change, which can take the parent out and the new row with it.
#[tokio::test]
async fn a_new_track_under_an_edited_release_is_evaluated() {
    let edit = "UPDATE releases SET year=2001 WHERE id='r'";
    let alone = evaluating(edit).await;
    let with_track = evaluating(&format!("{edit}; INSERT INTO tracks VALUES('t3','r',3)")).await;
    assert_eq!(with_track, alone + 1);
}

#[tokio::test]
async fn new_rows_are_shown_and_stored() {
    let (db, _store, _counter) = library().await;
    run(
        &db,
        "INSERT INTO albums VALUES('b','Another');
        INSERT INTO releases VALUES('s','b',2005);
        INSERT INTO tracks VALUES('u1','s',1)"
            .into(),
    )
    .await;
    assert_eq!(count(&db, "tracks"), 3);
    assert_eq!(count(&db, "_coven_lost"), 0);
    db.close().await.unwrap();
}

use super::*;
use crate::snapshot_write::tests::stream;
use crate::tests::TestStore;
use crate::write::tests::{count, records, sql};
use crate::{Database, RowIdentity, SyncedTable};
use coven_foundation::id_source::{MemberId, SequentialIds};
use coven_merge::Audience;

/// Ben's tracks and Ana's reviews of them, all owned by their creators.
/// Reviews cascade from a track, comments from a review; a pin restricts a
/// track's deletion; a label's key reference is set NULL when its key
/// changes. Two shared triggers write Ana's rows from Ben's changes.
pub(crate) const SCHEMA: &str = "CREATE TABLE tracks(id TEXT NOT NULL PRIMARY KEY, title TEXT);
CREATE TABLE reviews(id TEXT NOT NULL PRIMARY KEY, track TEXT REFERENCES tracks(id) ON DELETE CASCADE, body TEXT);
CREATE TABLE comments(id TEXT NOT NULL PRIMARY KEY, review TEXT REFERENCES reviews(id) ON DELETE CASCADE);
CREATE TABLE pins(id TEXT NOT NULL PRIMARY KEY, track TEXT REFERENCES tracks(id) ON DELETE RESTRICT);
CREATE TABLE keys(id TEXT NOT NULL PRIMARY KEY);
CREATE TABLE labels(id TEXT NOT NULL PRIMARY KEY, key TEXT REFERENCES keys(id) ON UPDATE SET NULL, slot TEXT);
CREATE UNIQUE INDEX label_slots ON labels(coalesce(key, slot));
CREATE TABLE albums(id TEXT NOT NULL PRIMARY KEY);
CREATE TABLE logs(id TEXT NOT NULL PRIMARY KEY, n INTEGER NOT NULL);
CREATE TRIGGER album_gone AFTER DELETE ON albums WHEN NOT coven_applying()
  BEGIN UPDATE reviews SET body='album gone' WHERE id='r'; END;
CREATE TRIGGER review_gone AFTER DELETE ON reviews WHEN NOT coven_applying() AND old.id='watched'
  BEGIN UPDATE logs SET n=n+1 WHERE id='log'; END;";

pub(crate) fn tables() -> Vec<SyncedTable> {
    [
        "tracks", "reviews", "comments", "pins", "keys", "labels", "albums", "logs",
    ]
    .into_iter()
    .map(|table| {
        let declared = SyncedTable::new(table, RowIdentity::SharedKey).owner_only();
        match table {
            "albums" => declared.shared_trigger("album_gone"),
            "reviews" => declared.shared_trigger("review_gone"),
            _ => declared,
        }
    })
    .collect()
}

pub(crate) struct Pair {
    ana: Database,
    ben: Database,
    _stores: (TestStore, TestStore),
}

pub(crate) const ANA: u8 = 1;
pub(crate) const BEN: u8 = 2;

pub(crate) fn member(seed: u8) -> MemberId {
    crate::tests::member(seed)
}

/// Deliver every write `from` has queued that `to` has not applied.
pub(crate) async fn deliver(from: &Database, to: &Database) {
    for record in records(from) {
        from.receive_stream(stream(&record), || Ok(()))
            .await
            .unwrap();
        to.receive_stream(stream(&record), || Ok(())).await.unwrap();
    }
}

impl Pair {
    /// Ana's and Ben's devices, each knowing both members.
    pub(crate) async fn new() -> Self {
        Self::with(tables(), SCHEMA).await
    }

    pub(crate) async fn with(tables: Vec<SyncedTable>, schema: &'static str) -> Self {
        let ids = SequentialIds::new();
        let ana_store = TestStore::with_ids(&ids);
        let ben_store = TestStore::with_ids(&ids);
        let ana = ana_store.schema(tables.clone(), schema).await.unwrap();
        let ben = ben_store.schema(tables, schema).await.unwrap();
        for db in [&ana, &ben] {
            TestStore::register(db, ana_store.device(), ANA);
            TestStore::register(db, ben_store.device(), BEN);
        }
        Self {
            ana,
            ben,
            _stores: (ana_store, ben_store),
        }
    }

    fn circle(&self, circle: coven_foundation::id_source::CircleId) {
        self._stores.0.join_circle(&self.ana, circle);
        self._stores.1.join_circle(&self.ben, circle);
    }

    /// Ben's tracks, keys and album; Ana's reviews, comments, pin, labels
    /// and log; both devices hold all of them.
    pub(crate) async fn seeded() -> Self {
        let pair = Self::new().await;
        sql(
            &pair.ben,
            "INSERT INTO tracks VALUES('t','Song'),('s','Other'),('w','Watched'),('p','Pinned');
             INSERT INTO keys VALUES('7'),('9');
             INSERT INTO albums VALUES('a');",
        )
        .await
        .unwrap();
        deliver(&pair.ben, &pair.ana).await;
        sql(
            &pair.ana,
            "INSERT INTO reviews VALUES('r','t','good'),('q','s','fine'),('watched','w','seen');
             INSERT INTO comments VALUES('c','r');
             INSERT INTO pins VALUES('pin','p');
             INSERT INTO labels VALUES('l7','7','free'),('l9','9','nine'),('m9',NULL,'other');
             INSERT INTO logs VALUES('log',0);",
        )
        .await
        .unwrap();
        deliver(&pair.ana, &pair.ben).await;
        pair
    }

    pub(crate) async fn close(self) {
        self.ana.close().await.unwrap();
        self.ben.close().await.unwrap();
    }
}

fn refused_for(error: DbError, table: &str, owner: u8) {
    match error {
        DbError::NotOwner {
            table: refused,
            owner: member_id,
            ..
        } => {
            assert_eq!(refused, table);
            assert_eq!(member_id, member(owner));
        }
        error => panic!("expected NotOwner on {table}, got {error:?}"),
    }
}

fn value(db: &Database, sql: &'static str) -> Option<String> {
    db.inspect_writer(|c| {
        c.query_row(sql, [], |r| r.get::<_, Option<String>>(0))
            .unwrap()
    })
}

/// Ben deleting his track cascades to Ana's review and on to her comment:
/// exact actions of an authorized root need no permission of their own.
#[tokio::test]
async fn cascades_from_an_authorized_delete_change_another_members_rows() {
    let pair = Pair::seeded().await;
    sql(&pair.ben, "DELETE FROM tracks WHERE id='t'")
        .await
        .unwrap();
    assert_eq!(count(&pair.ben, "comments"), 0);
    assert_eq!(
        value(&pair.ben, "SELECT (SELECT id FROM reviews WHERE id='r')"),
        None
    );
    // A RESTRICT reference refuses the delete as SQLite always does.
    let error = sql(&pair.ben, "DELETE FROM tracks WHERE id='p'")
        .await
        .unwrap_err();
    assert!(
        matches!(&error, DbError::Sqlite(e) if e.to_string().contains("FOREIGN KEY")),
        "{error:?}"
    );
    pair.close().await;
}

/// Ben can never directly update, delete or insert over Ana's review. The
/// whole transaction rolls back, and the refused write's upload number is
/// used by his next one.
#[tokio::test]
async fn direct_targets_of_another_members_rows_refuse_the_whole_transaction() {
    let pair = Pair::seeded().await;
    let queued = records(&pair.ben).len();
    for attempt in [
        "UPDATE tracks SET title='kept?' WHERE id='s'; UPDATE reviews SET body='mine' WHERE id='r'",
        "DELETE FROM reviews WHERE id='q'",
        "INSERT OR REPLACE INTO reviews VALUES('r','t','replaced')",
        // A direct edit coalesced with an authorized action on the same row.
        "UPDATE reviews SET body='before' WHERE id='r'; DELETE FROM tracks WHERE id='t'",
    ] {
        refused_for(sql(&pair.ben, attempt).await.unwrap_err(), "reviews", ANA);
        // The refusal is the verdict every receiver gives that row.
        let (_, judgement) = pair.ben.inspect_writer(|c| c.take_judged().unwrap());
        let review = judgement
            .rows
            .iter()
            .find(|(row, checked)| row.table == "reviews" && !checked.derived)
            .unwrap();
        assert_eq!(
            review.1.check,
            Err(AuthorizationFailure::NotOwner { owner: member(ANA) })
        );
    }
    assert_eq!(records(&pair.ben).len(), queued);
    assert_eq!(
        value(&pair.ben, "SELECT title FROM tracks WHERE id='s'"),
        Some("Other".into())
    );
    assert_eq!(
        value(&pair.ben, "SELECT body FROM reviews WHERE id='r'"),
        Some("good".into())
    );
    sql(&pair.ben, "UPDATE tracks SET title='mine' WHERE id='s'")
        .await
        .unwrap();
    let all = records(&pair.ben);
    assert_eq!(all.len(), queued + 1);
    assert_eq!(
        all[queued].header.position.number,
        all[queued - 1].header.position.number + 1
    );
    pair.close().await;
}

/// A shared trigger cannot borrow permission from the row that fired it,
/// whether a statement or a foreign-key action fired it.
#[tokio::test]
async fn trigger_writes_are_direct_even_when_an_action_fires_them() {
    let pair = Pair::seeded().await;
    refused_for(
        sql(&pair.ben, "DELETE FROM albums WHERE id='a'")
            .await
            .unwrap_err(),
        "reviews",
        ANA,
    );
    assert_eq!(count(&pair.ben, "albums"), 1);
    // Deleting Ben's track cascades to Ana's watched review, which is
    // authorized; the trigger it fires on Ana's log is not.
    refused_for(
        sql(&pair.ben, "DELETE FROM tracks WHERE id='w'")
            .await
            .unwrap_err(),
        "logs",
        ANA,
    );
    assert_eq!(count(&pair.ben, "reviews"), 3);
    // Ana's own trigger targets are hers.
    sql(&pair.ana, "DELETE FROM reviews WHERE id='watched'")
        .await
        .unwrap();
    assert_eq!(
        value(&pair.ana, "SELECT CAST(n AS TEXT) FROM logs"),
        Some("1".into())
    );
    pair.close().await;
}

/// A referenced key changing 7 to 8 and back sets Ana's reference NULL,
/// where it stays. Unique expressions over the NULLs apply as always.
#[tokio::test]
async fn set_null_actions_follow_referenced_key_updates() {
    let pair = Pair::seeded().await;
    sql(
        &pair.ben,
        "UPDATE keys SET id='8' WHERE id='7'; UPDATE keys SET id='7' WHERE id='8'",
    )
    .await
    .unwrap();
    assert_eq!(
        value(&pair.ben, "SELECT key FROM labels WHERE id='l7'"),
        None
    );
    // l9's expression becomes its slot, 'nine', still unique.
    sql(&pair.ben, "UPDATE keys SET id='10' WHERE id='9'")
        .await
        .unwrap();
    assert_eq!(
        value(&pair.ben, "SELECT key FROM labels WHERE id='l9'"),
        None
    );
    // Ana's own edit colliding with another expression fails as always.
    deliver(&pair.ben, &pair.ana).await;
    let error = sql(&pair.ana, "UPDATE labels SET slot='other' WHERE id='l7'")
        .await
        .unwrap_err();
    assert!(
        matches!(&error, DbError::Sqlite(e) if e.to_string().contains("UNIQUE")),
        "{error:?}"
    );
    pair.close().await;
}

/// Dismissing a removed row deletes it, which is its owner's to do.
#[tokio::test]
async fn only_the_owner_dismisses_a_removed_row() {
    let pair = Pair::seeded().await;
    // Both devices give a label the same slot concurrently: one is removed.
    sql(&pair.ana, "INSERT INTO labels VALUES('ana',NULL,'same')")
        .await
        .unwrap();
    sql(&pair.ben, "INSERT INTO labels VALUES('ben',NULL,'same')")
        .await
        .unwrap();
    deliver(&pair.ana, &pair.ben).await;
    deliver(&pair.ben, &pair.ana).await;
    for (db, seed) in [(&pair.ana, ANA), (&pair.ben, BEN)] {
        let lost = db.lost_values().await.unwrap();
        let removed: Vec<_> = lost
            .into_iter()
            .filter(|value| matches!(value.target, crate::lost::LossTarget::Removed { .. }))
            .collect();
        assert_eq!(removed.len(), 1);
        let owner = if removed[0].key.0[0] == rusqlite::types::Value::Text("ana".into()) {
            ANA
        } else {
            BEN
        };
        let result = db.dismiss_lost_values(&removed).await;
        if owner == seed {
            result.unwrap();
        } else {
            refused_for(
                crate::tests::database_error(result.unwrap_err()),
                "labels",
                owner,
            );
        }
    }
    pair.close().await;
}

/// Each of Ben's local writes against `Admission.check`: the write commits
/// exactly when every row it changed passes, whether a direct step or an
/// exact foreign-key action changed it (`statement_authorized`,
/// `derived_roots_authorized`, and the accepted cascade).
#[tokio::test]
#[ignore = "requires COVEN_MERGE_LEAN; scripts/check.sh supplies the merge runner"]
async fn local_admission_matches_the_model() {
    use crate::execution_capture::tests::{lean::effects, Classified};
    use crate::test_utils::tests::run_lean;
    let runner = std::env::var_os("COVEN_MERGE_LEAN").expect("built mergeRunner");
    let cases = [
        ("DELETE FROM tracks WHERE id='t'", true),
        ("UPDATE keys SET id='8' WHERE id='7'; UPDATE keys SET id='7' WHERE id='8'", true),
        ("UPDATE reviews SET body='mine' WHERE id='r'", false),
        ("DELETE FROM reviews WHERE id='q'", false),
        ("UPDATE reviews SET body='x' WHERE id='r'; DELETE FROM tracks WHERE id='t'", false),
        ("DELETE FROM albums WHERE id='a'", false),
        ("DELETE FROM tracks WHERE id='w'", false),
        ("INSERT INTO tracks VALUES('new','New'); UPDATE tracks SET title='Renamed' WHERE id='s'", true),
    ];
    let tables = [
        "tracks", "reviews", "comments", "pins", "keys", "labels", "albums", "logs",
    ];
    for (statement, expected) in cases {
        let pair = Pair::seeded().await;
        let committed = sql(&pair.ben, statement).await;
        assert_eq!(committed.is_ok(), expected, "{statement}: {committed:?}");
        let operations = Classified::last(&pair.ben);
        let (mut input, ids) = pair
            .ben
            .inspect_writer_schema(|_, schema| effects(schema, &operations, true));
        // Owners as the rolled-back database holds them; a new row is Ben's.
        let rows: Vec<_> = pair.ben.inspect_writer(|c| {
            ids.rows
                .iter()
                .map(|(row, id)| {
                    let owner = match crate::ownership::creation(c, row).unwrap() {
                        Some(creation) => {
                            crate::ownership::member(c, creation.creator.write.device)
                                .unwrap()
                                .unwrap()
                        }
                        None => member(BEN),
                    };
                    let owner = if owner == member(ANA) { ANA } else { BEN };
                    serde_json::json!({"row": id, "owner": owner,
                        "table": tables.iter().position(|t| *t == row.table).unwrap()})
                })
                .collect()
        });
        input["mode"] = "admission".into();
        input["rows"] = rows.into();
        input["owned"] = (0..tables.len()).collect::<Vec<_>>().into();
        input["author"] = BEN.into();
        let verdicts = run_lean(&runner, &input);
        let passed = verdicts
            .as_array()
            .unwrap()
            .iter()
            .all(|verdict| verdict["check"] == true);
        assert_eq!(passed, expected, "{statement}: {input} -> {verdicts}");
        pair.close().await;
    }
}

/// Moving a row into another audience copies it, keeping its creation, so
/// only its owner may move it (§9.6, §15.4).
#[tokio::test]
async fn moving_another_members_row_is_a_direct_change_of_it() {
    let circle = coven_foundation::id_source::CircleId(uuid::Uuid::from_u128(10));
    let pair = Pair::with(
        vec![SyncedTable::new("notes", RowIdentity::IndependentUuid)
            .owner_only()
            .audience_column("audience")],
        "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, audience TEXT NOT NULL)",
    )
    .await;
    pair.circle(circle);
    sql(
        &pair.ana,
        "INSERT INTO notes VALUES('f47ac10b-58cc-4372-a567-0e02b2c3d479','store')",
    )
    .await
    .unwrap();
    deliver(&pair.ana, &pair.ben).await;
    let audience = circle.to_string();
    let moved = audience.clone();
    let error = pair
        .ben
        .write(move |sql| {
            sql.execute(
                "UPDATE notes SET audience=?1 WHERE id='f47ac10b-58cc-4372-a567-0e02b2c3d479'",
                [&moved],
            )?;
            Ok(())
        })
        .await
        .unwrap_err();
    refused_for(error, "notes", ANA);
    // His own row moves, and its copy names his creation.
    sql(
        &pair.ben,
        "INSERT INTO notes VALUES('f47ac10b-58cc-4372-a567-0e02b2c3d47a','store')",
    )
    .await
    .unwrap();
    pair.ben
        .write(move |sql| {
            sql.execute(
                "UPDATE notes SET audience=?1 WHERE id='f47ac10b-58cc-4372-a567-0e02b2c3d47a'",
                [&audience],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let copy = records(&pair.ben)
        .pop()
        .unwrap()
        .parts
        .into_iter()
        .flat_map(|part| part.rows)
        .find(|change| change.creator.is_some())
        .unwrap();
    assert_eq!(copy.creator.unwrap().row.audience, Audience::Store);
    pair.close().await;
}

/// Ben's album carries Ana's track through its audience foreign key. Moving
/// the album copies the track too, a direct change of her row, so Ben cannot
/// move it while her track is in it; his own album and track move together,
/// each copy naming its original creation (§9.6, §15.4; Lean
/// `Moves.carried_foreign_refuses`).
#[tokio::test]
async fn moving_a_row_that_carries_another_members_row_is_refused() {
    const BENS: &str = "00000000-0000-4000-8000-0000000000b1";
    const OTHER: &str = "00000000-0000-4000-8000-0000000000b2";
    let circle = coven_foundation::id_source::CircleId(uuid::Uuid::from_u128(10));
    let pair = Pair::with(
        vec![
            SyncedTable::new("albums", RowIdentity::IndependentUuid)
                .owner_only()
                .audience_column("audience"),
            SyncedTable::new("tracks", RowIdentity::IndependentUuid)
                .owner_only()
                .audience_from("album"),
        ],
        "CREATE TABLE albums(id TEXT NOT NULL PRIMARY KEY, audience TEXT NOT NULL);
         CREATE TABLE tracks(id TEXT NOT NULL PRIMARY KEY, album TEXT NOT NULL REFERENCES albums(id) ON DELETE CASCADE)",
    )
    .await;
    pair.circle(circle);
    sql(
        &pair.ben,
        "INSERT INTO albums VALUES('00000000-0000-4000-8000-0000000000b1','store'),('00000000-0000-4000-8000-0000000000b2','store');
         INSERT INTO tracks VALUES('00000000-0000-4000-8000-0000000000c2','00000000-0000-4000-8000-0000000000b2')",
    )
    .await
    .unwrap();
    deliver(&pair.ben, &pair.ana).await;
    sql(
        &pair.ana,
        "INSERT INTO tracks VALUES('00000000-0000-4000-8000-0000000000a1','00000000-0000-4000-8000-0000000000b1')",
    )
    .await
    .unwrap();
    deliver(&pair.ana, &pair.ben).await;
    let queued = records(&pair.ben).len();
    let audience = circle.to_string();
    let error = pair
        .ben
        .write(move |sql| {
            sql.execute(
                "UPDATE albums SET audience=?1 WHERE id=?2",
                [audience.as_str(), BENS],
            )?;
            Ok(())
        })
        .await
        .unwrap_err();
    refused_for(error, "tracks", ANA);
    assert_eq!(records(&pair.ben).len(), queued);
    assert_eq!(
        value(
            &pair.ben,
            "SELECT audience FROM albums WHERE id='00000000-0000-4000-8000-0000000000b1'"
        ),
        Some("store".into())
    );
    let audience = circle.to_string();
    pair.ben
        .write(move |sql| {
            sql.execute(
                "UPDATE albums SET audience=?1 WHERE id=?2",
                [audience.as_str(), OTHER],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let copies: Vec<_> = records(&pair.ben)
        .pop()
        .unwrap()
        .parts
        .into_iter()
        .flat_map(|part| part.rows)
        .filter_map(|change| change.creator.map(|creator| (change.row, creator)))
        .collect();
    assert_eq!(
        copies
            .iter()
            .map(|(row, _)| row.table.as_str())
            .collect::<Vec<_>>(),
        ["albums", "tracks"]
    );
    for (row, creator) in copies {
        assert_eq!(row.audience, Audience::Circle(circle));
        assert_eq!(creator.row.audience, Audience::Store);
        assert_eq!(creator.row.key, row.key);
    }
    pair.close().await;
}

/// Ben's tracks are in the store; Ana's reviews of them are in their circle.
/// Deleting one track and changing the other's key reach her reviews
/// through CASCADE and SET NULL: authorized actions, carried in the
/// circle's part of Ben's write, which Ana's device applies (§9.6, §15).
#[tokio::test]
async fn foreign_key_actions_reach_rows_in_another_audience() {
    const DELETED: &str = "00000000-0000-4000-8000-0000000000d1";
    const NULLED: &str = "00000000-0000-4000-8000-0000000000d2";
    let circle = coven_foundation::id_source::CircleId(uuid::Uuid::from_u128(10));
    let pair = Pair::with(
        vec![
            SyncedTable::new("tracks", RowIdentity::SharedKey).owner_only(),
            SyncedTable::new("reviews", RowIdentity::IndependentUuid)
                .owner_only()
                .audience_column("audience"),
        ],
        "CREATE TABLE tracks(id TEXT NOT NULL PRIMARY KEY);
         CREATE TABLE reviews(id TEXT NOT NULL PRIMARY KEY, audience TEXT NOT NULL,
           track TEXT REFERENCES tracks(id) ON DELETE CASCADE ON UPDATE SET NULL)",
    )
    .await;
    pair.circle(circle);
    sql(&pair.ben, "INSERT INTO tracks VALUES('t'),('u')")
        .await
        .unwrap();
    deliver(&pair.ben, &pair.ana).await;
    let audience = circle.to_string();
    pair.ana
        .write(move |sql| {
            sql.execute(
                "INSERT INTO reviews VALUES(?1,?3,'t'),(?2,?3,'u')",
                [DELETED, NULLED, audience.as_str()],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    deliver(&pair.ana, &pair.ben).await;
    // A direct edit of her review is still refused across audiences.
    refused_for(
        sql(&pair.ben, "UPDATE reviews SET track=NULL")
            .await
            .unwrap_err(),
        "reviews",
        ANA,
    );
    sql(
        &pair.ben,
        "DELETE FROM tracks WHERE id='t'; UPDATE tracks SET id='v' WHERE id='u'",
    )
    .await
    .unwrap();
    let record = records(&pair.ben).pop().unwrap();
    let tables = |audience: Audience| -> Vec<String> {
        record
            .parts
            .iter()
            .filter(|part| part.audience == audience)
            .flat_map(|part| &part.rows)
            .map(|change| change.row.table.clone())
            .collect()
    };
    assert_eq!(tables(Audience::Store), ["tracks", "tracks", "tracks"]);
    assert_eq!(tables(Audience::Circle(circle)), ["reviews", "reviews"]);
    deliver(&pair.ben, &pair.ana).await;
    for db in [&pair.ben, &pair.ana] {
        assert_eq!(count(db, "reviews"), 1);
        assert_eq!(
            value(
                db,
                "SELECT track FROM reviews WHERE id='00000000-0000-4000-8000-0000000000d2'"
            ),
            None
        );
        assert_eq!(
            value(db, "SELECT audience FROM reviews"),
            Some(circle.to_string())
        );
    }
    assert!(pair.ana.lost_values().await.unwrap().is_empty());
    pair.close().await;
}

/// Labels contest one slot, and one member's label is removed; only its
/// owner may delete it by dismissing that loss. A migration that drops a
/// column then freezes the removed row's values. The other member may dismiss
/// the frozen loss: that changes no row and leaves the row's creation as it
/// was (§9.6, §18.7, E4).
#[tokio::test]
async fn dismissing_a_frozen_loss_rewrites_no_row() {
    const LABELS: &str =
        "CREATE TABLE labels(id TEXT NOT NULL PRIMARY KEY, slot TEXT NOT NULL UNIQUE, note TEXT)";
    let tables = || vec![SyncedTable::new("labels", RowIdentity::SharedKey).owner_only()];
    let ids = SequentialIds::new();
    let stores = [TestStore::with_ids(&ids), TestStore::with_ids(&ids)];
    let ana = stores[0].schema(tables(), LABELS).await.unwrap();
    let ben = stores[1].schema(tables(), LABELS).await.unwrap();
    for db in [&ana, &ben] {
        TestStore::register(db, stores[0].device(), ANA);
        TestStore::register(db, stores[1].device(), BEN);
    }
    sql(&ana, "INSERT INTO labels VALUES('ana','same','hers')")
        .await
        .unwrap();
    sql(&ben, "INSERT INTO labels VALUES('ben','same','his')")
        .await
        .unwrap();
    deliver(&ana, &ben).await;
    deliver(&ben, &ana).await;
    let removed = ben.lost_values().await.unwrap();
    assert_eq!(removed.len(), 1, "{removed:?}");
    let crate::lost::LossTarget::Removed { row, .. } = removed[0].target.clone() else {
        panic!("{removed:?}")
    };
    let (owner, other) = if removed[0].key.0[0] == rusqlite::types::Value::Text("ana".into()) {
        (ANA, 1)
    } else {
        (BEN, 0)
    };
    let (db, owners) = if other == 1 { (ben, ana) } else { (ana, ben) };
    owners.close().await.unwrap();
    refused_for(
        crate::tests::database_error(db.dismiss_lost_values(&removed).await.unwrap_err()),
        "labels",
        owner,
    );
    let creation =
        |db: &Database| db.inspect_writer(|c| crate::ownership::creation(c, &row).unwrap());
    let created = creation(&db);
    assert!(created.is_some());
    db.close().await.unwrap();
    let db = stores[other]
        .builder(
            tables(),
            vec![
                crate::Migration::addition(1, "schema", LABELS),
                crate::Migration::breaking(
                    2,
                    "drop note",
                    "ALTER TABLE labels DROP COLUMN note",
                    crate::OldWrites::Drop,
                ),
            ],
        )
        .open()
        .await
        .unwrap();
    let frozen = db.lost_values().await.unwrap();
    assert_eq!(frozen.len(), 1, "{frozen:#?}");
    assert!(matches!(
        frozen[0].target,
        crate::lost::LossTarget::Acknowledgements(_)
    ));
    assert_eq!(creation(&db), created);
    let rows = count(&db, "labels");
    let queued = records(&db).len();
    db.dismiss_lost_values(&frozen).await.unwrap();
    let all = records(&db);
    assert_eq!(all.len(), queued + 1);
    let dismissal = all.last().unwrap();
    assert!(dismissal
        .parts
        .iter()
        .all(|part| part.rows.is_empty() && part.executions.is_empty()));
    assert!(dismissal
        .parts
        .iter()
        .any(|part| !part.dismissals.is_empty()));
    assert!(db.lost_values().await.unwrap().is_empty());
    assert_eq!(creation(&db), created);
    assert_eq!(count(&db, "labels"), rows);
    db.close().await.unwrap();
}

/// Ben deletes his track. Its cascade deletes Ana's play, and the shared
/// trigger that fires deletes her review before the cascade reaches it: the
/// cascade's exact effect, which every device classifies as the action (§9.6,
/// B18 `TriggerFirst`). Ben's device commits the write, and Ana's admits it
/// with the same verdicts.
#[tokio::test]
async fn a_trigger_making_a_cascades_exact_effect_commits_as_receivers_admit_it() {
    const TRIGGER_FIRST: &str = "CREATE TABLE tracks(id TEXT NOT NULL PRIMARY KEY, title TEXT);
    CREATE TABLE plays(id TEXT NOT NULL PRIMARY KEY, track TEXT REFERENCES tracks(id) ON DELETE CASCADE);
    CREATE TABLE reviews(id TEXT NOT NULL PRIMARY KEY, track TEXT REFERENCES tracks(id) ON DELETE CASCADE);
    CREATE TRIGGER play_gone AFTER DELETE ON plays WHEN NOT coven_applying()
      BEGIN DELETE FROM reviews WHERE track=old.track; END;
    CREATE TRIGGER review_gone AFTER DELETE ON reviews WHEN NOT coven_applying()
      BEGIN DELETE FROM plays WHERE track=old.track; END;";
    let owned = |table| SyncedTable::new(table, RowIdentity::SharedKey).owner_only();
    let pair = Pair::with(
        vec![
            owned("tracks"),
            owned("plays").shared_trigger("play_gone"),
            owned("reviews").shared_trigger("review_gone"),
        ],
        TRIGGER_FIRST,
    )
    .await;
    sql(&pair.ben, "INSERT INTO tracks VALUES('t','Song')")
        .await
        .unwrap();
    deliver(&pair.ben, &pair.ana).await;
    sql(
        &pair.ana,
        "INSERT INTO plays VALUES('p','t'); INSERT INTO reviews VALUES('r','t')",
    )
    .await
    .unwrap();
    deliver(&pair.ana, &pair.ben).await;
    sql(&pair.ben, "DELETE FROM tracks WHERE id='t'")
        .await
        .unwrap();
    let record = records(&pair.ben).pop().unwrap();
    deliver(&pair.ben, &pair.ana).await;
    let verdicts = |db: &Database| {
        db.inspect_writer(|c| {
            record
                .parts
                .iter()
                .flat_map(|part| part.records())
                .map(|r| {
                    let verdict = crate::judge::stored(c, &record.header, r.row()).unwrap();
                    (r.row().table.clone(), verdict)
                })
                .collect::<std::collections::BTreeMap<_, _>>()
        })
    };
    let track = coven_format::write_sources::ChangeId {
        write: record.header.position,
        row: record.parts[0]
            .rows
            .iter()
            .find(|change| change.row.table == "tracks")
            .unwrap()
            .row
            .clone(),
    };
    let expected = [
        (
            "plays".to_owned(),
            ChangeVerdict::Derived(vec![track.clone()]),
        ),
        ("reviews".to_owned(), ChangeVerdict::Derived(vec![track])),
        ("tracks".to_owned(), ChangeVerdict::Applied),
    ]
    .into();
    assert_eq!(verdicts(&pair.ben), expected);
    assert_eq!(verdicts(&pair.ana), expected);
    for db in [&pair.ben, &pair.ana] {
        assert_eq!(count(db, "plays") + count(db, "reviews"), 0);
    }
    assert!(pair.ana.lost_values().await.unwrap().is_empty());
    pair.close().await;
}

/// On tables nobody owns, deleting or replacing Ben's album commits: the
/// note's set-NULL and the track's cascade match their actions exactly, since
/// no net change or step carries a generated column (D5). The check still
/// holds for every row: a receiver refuses a note whose net change differs
/// from the action, as an invalid foreign-key effect (§9.6).
#[tokio::test]
async fn actions_on_rows_nobody_owns_match_with_generated_columns() {
    use crate::judge::tests::{album_kept, generated_tables, key, verdict, ALBUM_GONE, GENERATED};
    for statement in ALBUM_GONE {
        let pair = Pair::with(generated_tables(false), GENERATED).await;
        sql(
            &pair.ben,
            "INSERT INTO albums(id,title) VALUES('a','Old');
             INSERT INTO notes(id,album) VALUES('n','a'); INSERT INTO tracks VALUES('t','a')",
        )
        .await
        .unwrap();
        deliver(&pair.ben, &pair.ana).await;
        sql(&pair.ben, statement).await.unwrap();
        let genuine = records(&pair.ben).pop().unwrap();
        for row in [key("notes", "n"), key("tracks", "t")] {
            let derived = verdict(&pair.ben, &genuine, &row);
            assert!(
                matches!(derived, ChangeVerdict::Derived(_)),
                "{statement}: {derived:?}"
            );
        }
        let forged = album_kept(&genuine);
        pair.ana
            .receive_stream(stream(&forged), || Ok(()))
            .await
            .unwrap();
        assert_eq!(
            verdict(&pair.ana, &forged, &key("notes", "n")),
            ChangeVerdict::NotAuthorized(AuthorizationFailure::InvalidForeignKeyEffect),
            "{statement}"
        );
        pair.close().await;
    }
}

/// The author judges its own write from the record it queues, not by
/// reading back the records it retained from that record a moment before.
#[tokio::test]
async fn the_author_judges_its_write_without_reading_it_back() {
    let store = crate::tests::TestStore::new();
    let db = store
        .schema(crate::write::tests::notes(), crate::write::tests::NOTES)
        .await
        .unwrap();
    crate::write::tests::sql(
        &db,
        "INSERT INTO notes VALUES('00000000-0000-4000-8000-000000000001','title','body')",
    )
    .await
    .unwrap();
    let statements = db.inspect_writer(|writer| writer.profiled_statements());
    let reads = statements
        .iter()
        .filter(|s| {
            s.sql.starts_with(
                "SELECT record FROM _coven_authored_changes WHERE write_id=?1 ORDER BY",
            )
        })
        .count();
    assert_eq!(reads, 0);
    db.close().await.unwrap();
}

/// No record carries a generated column (D5), so a receiver refuses one that
/// does, before judging it: none of its values merges or is kept as lost.
/// Ben's album delete sets Ana's note's `album` NULL. A forged copy of his
/// write also sets the note's generated `shown`, or carries a generated cell
/// in an insert, a delete's old values or a step. A replaced setter can't
/// name one either: no write sets a generated cell to be its source.
#[tokio::test]
async fn a_record_naming_a_generated_column_is_refused() {
    use crate::judge::tests::{generated_tables, key, GENERATED};
    use coven_format::value::Value;
    use coven_format::write::WriteRecord;
    use coven_merge::{ColumnValue, Operation};
    type Forge = fn(&mut WriteRecord);
    fn row<'a>(record: &'a mut WriteRecord, table: &str) -> &'a mut coven_format::write::RowChange {
        record.parts[0]
            .rows
            .iter_mut()
            .find(|row| row.row == key(table, if table == "albums" { "a" } else { "n" }))
            .unwrap()
    }
    fn cell(value: &str) -> ColumnValue<Value> {
        ColumnValue {
            value: Value::Text(value.into()),
            parents: Default::default(),
        }
    }
    let cases: [(&str, &str, Forge); 4] = [
        (
            "INSERT INTO albums(id,title) VALUES('b','New')",
            "insert",
            |record| {
                let Operation::Insert(columns) = &mut record.parts[0].rows[0].change.operation
                else {
                    panic!("an insert")
                };
                columns.insert("sort".into(), cell("forged"));
            },
        ),
        ("DELETE FROM albums", "derived update", |record| {
            let note = row(record, "notes");
            let Operation::Update(columns) = &mut note.change.operation else {
                panic!("the note's set-NULL")
            };
            columns.insert("shown".into(), cell("forged"));
            note.old.insert("shown".into(), Value::Text("a".into()));
        }),
        ("DELETE FROM albums", "delete's old values", |record| {
            let album = row(record, "albums");
            album.old.insert("shout".into(), Value::Text("OLD".into()));
        }),
        ("DELETE FROM albums", "step", |record| {
            let note = row(record, "notes");
            let Operation::Update(columns) = &mut note.execution.steps[0].change.operation else {
                panic!("the note's set-NULL step")
            };
            columns.insert("shown".into(), cell("forged"));
        }),
    ];
    for (statement, case, forge) in cases {
        let pair = Pair::with(generated_tables(true), GENERATED).await;
        sql(&pair.ben, "INSERT INTO albums(id,title) VALUES('a','Old')")
            .await
            .unwrap();
        deliver(&pair.ben, &pair.ana).await;
        sql(&pair.ana, "INSERT INTO notes(id,album) VALUES('n','a')")
            .await
            .unwrap();
        deliver(&pair.ana, &pair.ben).await;
        sql(&pair.ben, statement).await.unwrap();
        let mut forged = records(&pair.ben).pop().unwrap();
        forge(&mut forged);
        let refused = pair.ana.receive_stream(stream(&forged), || Ok(())).await;
        assert!(
            matches!(
                &refused,
                Err(crate::DbError::Snapshot(
                    crate::SnapshotError::Inconsistent("row names a generated column")
                ))
            ),
            "{case}: {refused:?}"
        );
        let shown: String = pair.ana.inspect_writer(|db| {
            db.query_row("SELECT shown FROM notes", [], |r| r.get(0))
                .unwrap()
        });
        assert_eq!(shown, "a", "{case}");
        assert_eq!(count(&pair.ana, "albums"), 1, "{case}");
        assert_eq!(count(&pair.ana, "_coven_written_values"), 0, "{case}");
        assert_eq!(count(&pair.ana, "_coven_lost"), 0, "{case}");
        pair.close().await;
    }
}

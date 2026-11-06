//! Ben's write moves or replaces a parent of Ana's circle row before SQLite
//! acts on her row through another reference. Every receiver, his own device
//! first, places that parent where the write's earlier steps leave it, and
//! accepts SQLite's action (§9.6, D5).
use crate::author_judgement::tests::{deliver, value, Pair, ANA};
use crate::judge::tests::{not_owner, receive, verdict};
use crate::write::tests::{records, sql};
use crate::{RowIdentity, SyncedTable};
use coven_format::snapshot_rows::ChangeVerdict;
use coven_foundation::id_source::CircleId;
use coven_merge::Operation;

const TRACK: &str = "00000000-0000-4000-8000-0000000000e1";
const REVIEW: &str = "00000000-0000-4000-8000-0000000000e2";
const ALBUM: &str = "00000000-0000-4000-8000-0000000000e3";

fn circle() -> CircleId {
    CircleId(uuid::Uuid::from_u128(10))
}

/// Ana's review, in their circle, of rows Ben added to the store.
async fn pair(tables: Vec<SyncedTable>, schema: &'static str, ben: &'static str, a: &str) -> Pair {
    let pair = Pair::with(tables, schema).await;
    pair.circle(circle());
    sql(&pair.ben, ben).await.unwrap();
    deliver(&pair.ben, &pair.ana).await;
    let (audience, a, b) = (circle().to_string(), a.to_owned(), TRACK);
    pair.ana
        .write(move |sql| {
            sql.execute("INSERT INTO reviews VALUES(?1,?2,?3,?4)", [REVIEW, &audience, &a, b])?;
            Ok(())
        })
        .await
        .unwrap();
    deliver(&pair.ana, &pair.ben).await;
    pair
}

/// Ben's write, after which both devices hold Ana's review as `expected`.
async fn accepted(pair: Pair, write: Vec<String>, expected: &'static str, holds: Option<&str>) {
    pair.ben
        .write(move |sql| {
            for statement in &write {
                sql.execute(statement, [])?;
            }
            Ok(())
        })
        .await
        .unwrap();
    deliver(&pair.ben, &pair.ana).await;
    for db in [&pair.ben, &pair.ana] {
        assert_eq!(value(db, expected).as_deref(), holds);
        assert!(db.lost_values().await.unwrap().is_empty());
    }
    pair.close().await;
}

/// The review's pair is set NULL while its track keeps its value: the
/// action records the track's parent, which Ben moved into the circle
/// earlier in the same write.
#[tokio::test]
async fn an_unchanged_reference_names_its_parent_where_a_move_took_it() {
    let pair = pair(
        vec![
            SyncedTable::new("tracks", RowIdentity::IndependentUuid)
                .owner_only()
                .audience_column("audience"),
            SyncedTable::new("pairs", RowIdentity::SharedKey).owner_only(),
            SyncedTable::new("combos", RowIdentity::SharedKey).owner_only().key_columns(["p", "t"]),
            SyncedTable::new("reviews", RowIdentity::IndependentUuid)
                .owner_only()
                .audience_column("audience"),
        ],
        "CREATE TABLE tracks(id TEXT NOT NULL PRIMARY KEY, audience TEXT NOT NULL);
         CREATE TABLE pairs(id TEXT NOT NULL PRIMARY KEY);
         CREATE TABLE combos(p TEXT NOT NULL, t TEXT NOT NULL, PRIMARY KEY(p, t));
         CREATE TABLE reviews(id TEXT NOT NULL PRIMARY KEY, audience TEXT NOT NULL,
           a TEXT REFERENCES pairs(id) ON DELETE SET NULL, b TEXT REFERENCES tracks(id),
           FOREIGN KEY(a, b) REFERENCES combos(p, t))",
        "INSERT INTO tracks VALUES('00000000-0000-4000-8000-0000000000e1','store');
         INSERT INTO pairs VALUES('p');
         INSERT INTO combos VALUES('p','00000000-0000-4000-8000-0000000000e1')",
        "p",
    )
    .await;
    let write = vec![
        format!("UPDATE tracks SET audience='{}' WHERE id='{TRACK}'", circle()),
        "DELETE FROM pairs WHERE id='p'".to_owned(),
    ];
    accepted(
        pair,
        write,
        "SELECT coalesce(a, 'null') || b FROM reviews",
        Some(&format!("null{TRACK}")),
    )
    .await;
}

/// Ana's review of a track Ben moves into their circle and deletes in one
/// write cascades from the delete of the track's circle row.
#[tokio::test]
async fn a_cascade_follows_its_parent_through_a_move() {
    let pair = pair(
        vec![
            SyncedTable::new("tracks", RowIdentity::IndependentUuid)
                .owner_only()
                .audience_column("audience"),
            SyncedTable::new("reviews", RowIdentity::IndependentUuid)
                .owner_only()
                .audience_column("audience"),
        ],
        "CREATE TABLE tracks(id TEXT NOT NULL PRIMARY KEY, audience TEXT NOT NULL);
         CREATE TABLE reviews(id TEXT NOT NULL PRIMARY KEY, audience TEXT NOT NULL,
           a TEXT, b TEXT REFERENCES tracks(id) ON DELETE CASCADE)",
        "INSERT INTO tracks VALUES('00000000-0000-4000-8000-0000000000e1','store')",
        "a",
    )
    .await;
    let write = vec![
        format!("UPDATE tracks SET audience='{}' WHERE id='{TRACK}'", circle()),
        format!("DELETE FROM tracks WHERE id='{TRACK}'"),
    ];
    accepted(pair, write, "SELECT count(*) || '' FROM reviews", Some("0")).await;
}

/// Combos keyed by pair and track; Ben's pair key change cascades into the
/// review, pointing its combo reference at another combo.
fn combo_tables(audience: bool) -> Vec<SyncedTable> {
    let combos =
        |identity| SyncedTable::new("combos", identity).owner_only().key_columns(["p", "t"]);
    vec![
        SyncedTable::new("pairs", RowIdentity::SharedKey).owner_only(),
        // A row whose audience can change keeps its key: it is keyed by UUID.
        if audience {
            combos(RowIdentity::IndependentUuid).audience_column("audience")
        } else {
            combos(RowIdentity::SharedKey)
        },
        SyncedTable::new("reviews", RowIdentity::IndependentUuid)
            .owner_only()
            .audience_column("audience"),
    ]
}

/// Ben adds the combo the cascade points the review at in their circle and
/// moves it to the store before changing the pair's key: the review's combo
/// is the store's row the move began, not the circle's it ended.
#[tokio::test]
async fn a_retargeted_reference_names_its_parent_after_a_move() {
    let pair = pair(
        combo_tables(true),
        "CREATE TABLE pairs(id TEXT NOT NULL PRIMARY KEY);
         CREATE TABLE combos(p TEXT NOT NULL, t TEXT NOT NULL, audience TEXT NOT NULL,
           PRIMARY KEY(p, t));
         CREATE TABLE reviews(id TEXT NOT NULL PRIMARY KEY, audience TEXT NOT NULL,
           a TEXT REFERENCES pairs(id) ON UPDATE CASCADE, b TEXT,
           FOREIGN KEY(a, b) REFERENCES combos(p, t))",
        "INSERT INTO pairs VALUES('p');
         INSERT INTO combos VALUES('p','00000000-0000-4000-8000-0000000000e1','store')",
        "p",
    )
    .await;
    let write = vec![
        format!("INSERT INTO combos VALUES('q','{TRACK}','{}')", circle()),
        "UPDATE combos SET audience='store' WHERE p='q'".to_owned(),
        "UPDATE pairs SET id='q' WHERE id='p'".to_owned(),
    ];
    accepted(pair, write, "SELECT a FROM reviews", Some("q")).await;
}

/// Ben deletes the store's combo the first cascade points the review at,
/// adds another and changes the pair's key again: at the first cascade no
/// combo holds that key, which the write's delete shows, so it names no
/// combo; the second names the one he added.
#[tokio::test]
async fn a_retargeted_reference_to_a_deleted_row_names_no_parent() {
    let pair = pair(combo_tables(false), combos(true), TWO_COMBOS, "p").await;
    let write = vec![
        "DELETE FROM combos WHERE p='q'".to_owned(),
        "UPDATE pairs SET id='q' WHERE id='p'".to_owned(),
        format!("INSERT INTO combos VALUES('r','{TRACK}')"),
        "UPDATE pairs SET id='r' WHERE id='q'".to_owned(),
    ];
    accepted(pair, write, "SELECT a FROM reviews", Some("r")).await;
}

/// Albums whose audience their tracks follow, and Ana's review of Ben's
/// track in their circle, pointing at a pair and combo of his.
async fn album_pair() -> Pair {
    pair(
        vec![
            SyncedTable::new("albums", RowIdentity::IndependentUuid)
                .owner_only()
                .audience_column("audience"),
            SyncedTable::new("tracks", RowIdentity::IndependentUuid)
                .owner_only()
                .audience_from("album"),
            SyncedTable::new("pairs", RowIdentity::SharedKey).owner_only(),
            SyncedTable::new("combos", RowIdentity::SharedKey).owner_only().key_columns(["p", "t"]),
            SyncedTable::new("reviews", RowIdentity::IndependentUuid)
                .owner_only()
                .audience_column("audience"),
        ],
        "CREATE TABLE albums(id TEXT NOT NULL PRIMARY KEY, audience TEXT NOT NULL);
         CREATE TABLE tracks(id TEXT NOT NULL PRIMARY KEY,
           album TEXT NOT NULL REFERENCES albums(id) ON DELETE CASCADE);
         CREATE TABLE pairs(id TEXT NOT NULL PRIMARY KEY);
         CREATE TABLE combos(p TEXT NOT NULL, t TEXT NOT NULL, PRIMARY KEY(p, t));
         CREATE TABLE reviews(id TEXT NOT NULL PRIMARY KEY, audience TEXT NOT NULL,
           a TEXT REFERENCES pairs(id) ON DELETE SET NULL, b TEXT REFERENCES tracks(id),
           FOREIGN KEY(a, b) REFERENCES combos(p, t))",
        "INSERT INTO albums VALUES('00000000-0000-4000-8000-0000000000e3','store');
         INSERT INTO tracks VALUES('00000000-0000-4000-8000-0000000000e1',
           '00000000-0000-4000-8000-0000000000e3');
         INSERT INTO pairs VALUES('p');
         INSERT INTO combos VALUES('p','00000000-0000-4000-8000-0000000000e1')",
        "p",
    )
    .await
}

/// Ben's album carries his track into their circle, and his pair's delete
/// then sets the review's pair NULL: the move of the album records the
/// track's move too, so the action names the circle's track, the row the
/// carry began.
#[tokio::test]
async fn a_reference_to_a_carried_row_is_an_action() {
    let write = vec![
        format!("UPDATE albums SET audience='{}' WHERE id='{ALBUM}'", circle()),
        "DELETE FROM pairs WHERE id='p'".to_owned(),
    ];
    accepted(
        album_pair().await,
        write,
        "SELECT coalesce(a, 'null') || b FROM reviews",
        Some(&format!("null{TRACK}")),
    )
    .await;
}

/// The same write with the track's moves taken out and the review naming
/// the store's track, whose incarnation the carry ended: the write now
/// changes that track, the row the review names, at no step of its own,
/// which may have moved it anywhere, so the action is a direct change of
/// Ana's review, and her device refuses it (§9.6, §15.4).
#[tokio::test]
async fn a_reference_to_a_row_changed_at_no_step_is_a_direct_change() {
    let pair = album_pair().await;
    let audience = circle().to_string();
    pair.ben
        .write(move |sql| {
            sql.execute("UPDATE albums SET audience=?1 WHERE id=?2", [audience.as_str(), ALBUM])?;
            sql.execute("DELETE FROM pairs WHERE id='p'", [])?;
            Ok(())
        })
        .await
        .unwrap();
    let (mut forged, _) = pair.ben.inspect_writer(|c| c.take_judged().unwrap());
    let insert = records(&pair.ana).pop().unwrap();
    let setter = insert.parts.iter().flat_map(|part| &part.rows).find(|r| r.row.table == "reviews");
    let Operation::Insert(columns) = &setter.unwrap().change.operation else {
        panic!("Ana inserted her review")
    };
    let named: std::collections::BTreeMap<_, _> = columns["b"]
        .parents
        .iter()
        .filter(|(_, parent)| parent.row.table == "tracks")
        .map(|(name, parent)| (name.clone(), parent.clone()))
        .collect();
    let rows = forged.parts.iter_mut().flat_map(|part| &mut part.rows);
    for track in rows.filter(|r| r.row.table == "tracks") {
        track.execution = Default::default();
    }
    let review = forged
        .parts
        .iter_mut()
        .flat_map(|part| &mut part.rows)
        .find(|r| r.row.table == "reviews")
        .unwrap();
    let row = review.row.clone();
    for operation in std::iter::once(&mut review.change.operation)
        .chain(review.execution.steps.iter_mut().map(|step| &mut step.change.operation))
    {
        if let Operation::Update(columns) = operation {
            columns.get_mut("b").unwrap().parents = named.clone();
        }
    }
    receive(&pair.ana, &forged).await;
    assert_eq!(verdict(&pair.ana, &forged, &row), not_owner(ANA));
    pair.close().await;
}

/// Ben and Ana each move his track into another circle at once, so the
/// track is in both and each device shows the one whose move came first,
/// taking the other out. Ana reviews the shown one. Ben's write then moves
/// the album of the taken-out track into a third circle, which carries the
/// taken-out track along, and deletes his pair, which sets the review's
/// pair NULL. The carried track was never in SQLite's tables, so it doesn't
/// change where the track Ana's review names is (§8.4, §15.4).
#[tokio::test]
async fn a_reference_beside_a_carried_removed_row_is_an_action() {
    const STORE_ALBUM: &str = "00000000-0000-4000-8000-0000000000f0";
    let circles = [11u128, 12, 13].map(|n| CircleId(uuid::Uuid::from_u128(n)));
    let albums: [String; 3] =
        std::array::from_fn(|i| format!("00000000-0000-4000-8000-0000000000f{}", i + 1));
    let pair = Pair::with(
        vec![
            SyncedTable::new("albums", RowIdentity::IndependentUuid).audience_column("audience"),
            SyncedTable::new("tracks", RowIdentity::IndependentUuid).audience_from("album"),
            SyncedTable::new("pairs", RowIdentity::SharedKey).owner_only(),
            SyncedTable::new("combos", RowIdentity::SharedKey).key_columns(["p", "t"]),
            SyncedTable::new("reviews", RowIdentity::IndependentUuid)
                .owner_only()
                .audience_column("audience"),
        ],
        "CREATE TABLE albums(id TEXT NOT NULL PRIMARY KEY, audience TEXT NOT NULL);
         CREATE TABLE tracks(id TEXT NOT NULL PRIMARY KEY,
           album TEXT NOT NULL REFERENCES albums(id) ON DELETE CASCADE);
         CREATE TABLE pairs(id TEXT NOT NULL PRIMARY KEY);
         CREATE TABLE combos(p TEXT NOT NULL, t TEXT NOT NULL, PRIMARY KEY(p, t));
         CREATE TABLE reviews(id TEXT NOT NULL PRIMARY KEY, audience TEXT NOT NULL,
           a TEXT REFERENCES pairs(id) ON DELETE SET NULL, b TEXT REFERENCES tracks(id),
           FOREIGN KEY(a, b) REFERENCES combos(p, t))",
    )
    .await;
    for circle in circles {
        pair.circle(circle);
    }
    let setup = format!(
        "INSERT INTO albums VALUES('{STORE_ALBUM}','store'),('{}','{}'),('{}','{}'),('{}','{}');
         INSERT INTO tracks VALUES('{TRACK}','{STORE_ALBUM}');
         INSERT INTO pairs VALUES('p');
         INSERT INTO combos VALUES('p','{TRACK}')",
        albums[0], circles[0], albums[1], circles[1], albums[2], circles[2]
    );
    let ben = pair.ben.clone();
    ben.write(move |sql| Ok(sql.execute_batch(&setup)?)).await.unwrap();
    deliver(&pair.ben, &pair.ana).await;
    // The concurrent moves: Ben's into the first circle, Ana's into the second.
    for (db, album) in [(&pair.ben, &albums[0]), (&pair.ana, &albums[1])] {
        let album = album.clone();
        db.write(move |sql| {
            sql.execute("UPDATE tracks SET album=?1 WHERE id=?2", [album.as_str(), TRACK])?;
            Ok(())
        })
        .await
        .unwrap();
    }
    deliver(&pair.ben, &pair.ana).await;
    deliver(&pair.ana, &pair.ben).await;
    let shown = value(&pair.ana, "SELECT album FROM tracks").unwrap();
    assert_eq!(value(&pair.ben, "SELECT album FROM tracks").as_ref(), Some(&shown));
    let (shown, removed) = if shown == albums[0] { (0, 1) } else { (1, 0) };
    let audience = circles[shown].to_string();
    pair.ana
        .write(move |sql| {
            sql.execute(
                "INSERT INTO reviews VALUES(?1,?2,'p',?3)",
                [REVIEW, audience.as_str(), TRACK],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    deliver(&pair.ana, &pair.ben).await;
    let removed = albums[removed].clone();
    let carry = circles[2].to_string();
    pair.ben
        .write(move |sql| {
            sql.execute("UPDATE albums SET audience=?1 WHERE id=?2", [&carry, &removed])?;
            sql.execute("DELETE FROM pairs WHERE id='p'", [])?;
            Ok(())
        })
        .await
        .unwrap();
    deliver(&pair.ben, &pair.ana).await;
    let write = records(&pair.ben).pop().unwrap();
    let review = write.parts.iter().flat_map(|part| &part.rows).find(|r| r.row.table == "reviews");
    assert!(matches!(verdict(&pair.ana, &write, &review.unwrap().row), ChangeVerdict::Derived(_)));
    // The track taken out keeps its own loss; the review loses nothing.
    for db in [&pair.ben, &pair.ana] {
        let review = value(db, "SELECT coalesce(a, 'null') || b FROM reviews");
        assert_eq!(review, Some(format!("null{TRACK}")));
        assert!(db.lost_values().await.unwrap().iter().all(|lost| lost.table == "tracks"));
    }
    pair.close().await;
}

/// Combos keyed by pair and track, the review's combo key checked at each
/// statement or, `deferred`, at commit.
fn combos(deferred: bool) -> &'static str {
    if deferred {
        "CREATE TABLE pairs(id TEXT NOT NULL PRIMARY KEY);
         CREATE TABLE combos(p TEXT NOT NULL, t TEXT NOT NULL, PRIMARY KEY(p, t));
         CREATE TABLE reviews(id TEXT NOT NULL PRIMARY KEY, audience TEXT NOT NULL,
           a TEXT REFERENCES pairs(id) ON UPDATE CASCADE, b TEXT,
           FOREIGN KEY(a, b) REFERENCES combos(p, t) DEFERRABLE INITIALLY DEFERRED)"
    } else {
        "CREATE TABLE pairs(id TEXT NOT NULL PRIMARY KEY);
         CREATE TABLE combos(p TEXT NOT NULL, t TEXT NOT NULL, PRIMARY KEY(p, t));
         CREATE TABLE reviews(id TEXT NOT NULL PRIMARY KEY, audience TEXT NOT NULL,
           a TEXT REFERENCES pairs(id) ON UPDATE CASCADE, b TEXT,
           FOREIGN KEY(a, b) REFERENCES combos(p, t))"
    }
}

const TWO_COMBOS: &str = "INSERT INTO pairs VALUES('p');
     INSERT INTO combos VALUES('p','00000000-0000-4000-8000-0000000000e1'),
       ('q','00000000-0000-4000-8000-0000000000e1')";

/// Ben deletes the combo the cascade points the review at and adds it again
/// before changing the pair's key: the action names the combo the re-add
/// began, and the review's net change the incarnation the combo's net change
/// continues.
#[tokio::test]
async fn a_reference_to_a_row_deleted_and_added_again_is_an_action() {
    let pair = pair(combo_tables(false), combos(false), TWO_COMBOS, "p").await;
    let write = vec![
        "DELETE FROM combos WHERE p='q'".to_owned(),
        format!("INSERT INTO combos VALUES('q','{TRACK}')"),
        "UPDATE pairs SET id='q' WHERE id='p'".to_owned(),
    ];
    accepted(pair, write, "SELECT a FROM reviews", Some("q")).await;
}

/// Ben deletes the combo the cascade points the review at, and adds it
/// again only after: the deferred reference names no combo at the cascade,
/// and the one he added once the write's SQL has run.
#[tokio::test]
async fn a_deferred_reference_to_a_row_added_later_is_an_action() {
    let pair = pair(combo_tables(false), combos(true), TWO_COMBOS, "p").await;
    let write = vec![
        "DELETE FROM combos WHERE p='q'".to_owned(),
        "UPDATE pairs SET id='q' WHERE id='p'".to_owned(),
        format!("INSERT INTO combos VALUES('q','{TRACK}')"),
    ];
    accepted(pair, write, "SELECT a FROM reviews", Some("q")).await;
}

/// Ben moves the combo the cascade points the review at into their circle
/// and back before changing the pair's key: the action names the store's
/// row the second move began, and the review's net change the incarnation
/// the store's combo had.
#[tokio::test]
async fn a_reference_to_a_row_moved_out_and_back_is_an_action() {
    let pair = pair(
        combo_tables(true),
        "CREATE TABLE pairs(id TEXT NOT NULL PRIMARY KEY);
         CREATE TABLE combos(p TEXT NOT NULL, t TEXT NOT NULL, audience TEXT NOT NULL,
           PRIMARY KEY(p, t));
         CREATE TABLE reviews(id TEXT NOT NULL PRIMARY KEY, audience TEXT NOT NULL,
           a TEXT REFERENCES pairs(id) ON UPDATE CASCADE, b TEXT,
           FOREIGN KEY(a, b) REFERENCES combos(p, t))",
        "INSERT INTO pairs VALUES('p');
         INSERT INTO combos VALUES('p','00000000-0000-4000-8000-0000000000e1','store'),
           ('q','00000000-0000-4000-8000-0000000000e1','store')",
        "p",
    )
    .await;
    let write = vec![
        format!("UPDATE combos SET audience='{}' WHERE p='q'", circle()),
        "UPDATE combos SET audience='store' WHERE p='q'".to_owned(),
        "UPDATE pairs SET id='q' WHERE id='p'".to_owned(),
    ];
    accepted(pair, write, "SELECT a FROM reviews", Some("q")).await;
}

/// `StepIndex::place` over the steps the merge model's operations on one
/// parent key record finds what the model's `place` does, and
/// `StepIndex::resolve` and `StepIndex::net_parent`, given the rows those
/// operations changed at no step, what its `named` and `namedNet` do
/// (`Place.place_settles`, `Place.recorded_sound`, `Place.net_sound`).
mod model {
    use crate::judge::{Indexed, IndexedRow, Place, StepIndex};
    use crate::test_utils::tests::run_lean;
    use coven_format::write::Half;
    use coven_foundation::id_source::CircleId;
    use coven_merge::{Audience, Parent, RowId};
    use serde_json::{json, Value as Json};

    /// A fixed sequence of numbers below a bound.
    struct Numbers(u64);

    impl Numbers {
        fn below(&mut self, bound: u64) -> u64 {
            self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            (self.0 >> 33) % bound
        }
    }

    fn row(audience: u64) -> RowId {
        RowId {
            table: "tracks".into(),
            key: vec![1],
            audience: match audience {
                0 => Audience::Store,
                circle => Audience::Circle(CircleId(uuid::Uuid::from_u128(circle.into()))),
            },
        }
    }

    /// Writes of up to five operations on the key, from each kind of view.
    fn cases() -> Vec<Json> {
        let mut numbers = Numbers(7);
        (0..4000)
            .map(|_| {
                let start = match numbers.below(4) {
                    0 => Json::Null,
                    audience => json!([audience - 1, 1]),
                };
                let length = numbers.below(6);
                let ops: Vec<Json> = (0..length)
                    .map(|_| {
                        let (audience, inc) = (numbers.below(3), 2 * numbers.below(3) + 1);
                        match numbers.below(8) {
                            0 => json!({"op": "insert", "aud": audience, "inc": inc}),
                            1 => json!({"op": "update"}),
                            2 => json!({"op": "delete"}),
                            3 => json!({"op": "move", "aud": audience, "inc": inc}),
                            4 => json!({"op": "unstepped", "to": [audience, inc]}),
                            5 => json!({"op": "unstepped", "to": null}),
                            6 => json!({"op": "aside", "aud": audience}),
                            _ => json!({"op": "other"}),
                        }
                    })
                    .collect();
                // The generation the write's record of each audience's row
                // leaves it at.
                let ends: Vec<Json> =
                    (0..3).map(|audience| json!([audience, 2 * numbers.below(3) + 1])).collect();
                json!({"start": start, "ops": ops, "at": numbers.below(length + 1), "ends": ends})
            })
            .collect()
    }

    /// The index of a write whose steps on the key are `steps`, whose
    /// records leave each audience's row where the case's `ends` says, and
    /// that changed the rows of the `marked` audiences at no step.
    fn index(case: &Json, steps: &Json, marked: &Json) -> StepIndex {
        fn entry<'a>(index: &'a mut StepIndex, case: &Json, audience: u64) -> &'a mut IndexedRow {
            let ends = &case["ends"].as_array().unwrap().iter().find(|end| end[0] == audience);
            index.rows.entry(row(audience)).or_insert(IndexedRow {
                steps: Vec::new(),
                ends: ends.unwrap()[1].as_u64().unwrap(),
            })
        }
        let mut index = StepIndex::default();
        for step in steps.as_array().unwrap() {
            let inc = step["inc"].as_u64().unwrap();
            let delete = step["delete"].as_bool().unwrap();
            entry(&mut index, case, step["aud"].as_u64().unwrap()).steps.push(Indexed {
                sequence: step["seq"].as_u64().unwrap().try_into().unwrap(),
                delete,
                half: if step["move"].as_bool().unwrap() { Half::Move } else { Half::Whole },
                generation: inc,
                after: if delete { inc + 1 } else { inc },
            });
        }
        for audience in marked.as_array().unwrap() {
            let audience = audience.as_u64().unwrap();
            entry(&mut index, case, audience);
            index.unstepped.insert(row(audience));
        }
        index
    }

    fn audience(row: &RowId) -> u64 {
        match &row.audience {
            Audience::Store => 0,
            Audience::Circle(circle) => circle.0.as_u128() as u64,
        }
    }

    fn found(place: Place<'_>) -> Json {
        match place {
            Place::Untouched => json!({"kind": "untouched"}),
            Place::Unplaced => json!({"kind": "unplaced"}),
            Place::Last(row, step) => json!({
                "kind": "last",
                "seq": step.sequence,
                "aud": audience(row),
                "delete": step.delete,
                "move": step.half == Half::Move,
                "inc": if step.delete { step.generation } else { step.after },
            }),
        }
    }

    fn parent(parent: &Option<Parent>) -> Json {
        parent
            .as_ref()
            .map_or(Json::Null, |parent| json!([audience(&parent.row), parent.generation]))
    }

    fn resolved(resolved: Option<Option<Parent>>) -> Json {
        match resolved {
            None => json!({"kind": "refused"}),
            Some(named) => json!({"kind": "parent", "parent": parent(&named)}),
        }
    }

    /// The parent a reference of a row in audience 1 records, left as it
    /// was, before `at`, and the one its row change names, given the parent
    /// its setter recorded.
    fn named(index: &StepIndex, at: u32, setter: Option<&Parent>) -> (Json, Json) {
        let child = row(1).audience;
        let place = index.place("tracks", &[1], at);
        let named = index.resolve("tracks", &[1], place, &child, setter, |_, last| last.after);
        let net = index.net_parent("tracks", &[1], &child, setter);
        (resolved(named), resolved(net))
    }

    #[test]
    #[ignore = "requires COVEN_MERGE_LEAN; scripts/check.sh supplies the merge runner"]
    fn places_match_the_model() {
        let runner = std::env::var_os("COVEN_MERGE_LEAN").expect("built mergeRunner");
        let cases = cases();
        let lean = run_lean(&runner, &json!({"mode": "place", "cases": cases}));
        let mut kinds = std::collections::BTreeMap::<String, usize>::new();
        let mut nets = std::collections::BTreeMap::<String, usize>::new();
        let (mut moves, mut readded) = (0, 0);
        for (case, lean) in cases.iter().zip(lean.as_array().unwrap()) {
            if lean.is_null() {
                continue;
            }
            let at: u32 = case["at"].as_u64().unwrap().try_into().unwrap();
            let index = index(case, &lean["steps"], &lean["marked"]);
            let rust = found(index.place("tracks", &[1], at));
            assert_eq!(rust, lean["place"], "{case}");
            *kinds.entry(rust["kind"].as_str().unwrap().to_owned()).or_default() += 1;
            moves += usize::from(rust["move"] == true);
            let setter = match &lean["setter"] {
                Json::Null => None,
                setter => Some(Parent {
                    row: row(setter[0].as_u64().unwrap()),
                    generation: setter[1].as_u64().unwrap(),
                }),
            };
            let (named, net) = named(&index, at, setter.as_ref());
            assert_eq!(named, lean["named"], "{case}");
            assert_eq!(net, lean["net"], "{case}");
            let marks = if lean["marked"].as_array().unwrap().is_empty() { "" } else { ":marked" };
            for resolved in [&named, &net] {
                let some = if resolved["parent"].is_null() { "none" } else { "parent" };
                let kind = match resolved["kind"].as_str().unwrap() {
                    "parent" => format!("{some}{marks}"),
                    refused => format!("{refused}{marks}"),
                };
                *nets.entry(kind).or_default() += 1;
            }
            // The row change names another incarnation than the key's last step made.
            let last = lean["steps"].as_array().unwrap().last();
            readded +=
                usize::from(last.is_some_and(|step| {
                    !net["parent"].is_null() && net["parent"][1] != step["inc"]
                }));
        }
        assert!(kinds.values().all(|count| *count >= 100) && kinds.len() == 2, "{kinds:?}");
        assert!(moves >= 50, "{moves} places on a move's half");
        // Parents and none, with and without rows changed at no step, and
        // refusals, which only such rows make for a reference left as it was.
        assert!(nets.values().all(|count| *count >= 100) && nets.len() == 5, "{nets:?}");
        assert!(readded >= 50, "{readded} row changes naming another incarnation");
    }
}

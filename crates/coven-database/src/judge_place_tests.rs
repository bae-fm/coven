//! Ben's write moves or replaces a parent of Ana's circle row before SQLite
//! acts on her row through another reference. Every receiver, his own device
//! first, places that parent where the write's earlier steps leave it, and
//! accepts SQLite's action (§9.6, D5).
use crate::author_judgement::tests::{deliver, value, Pair, ANA};
use crate::judge::tests::{not_owner, receive, verdict};
use crate::write::tests::{records, sql};
use crate::{RowIdentity, SyncedTable};
use coven_foundation::id_source::CircleId;
use coven_merge::Operation;

const TRACK: &str = "00000000-0000-4000-8000-0000000000e1";
const REVIEW: &str = "00000000-0000-4000-8000-0000000000e2";

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
    let pair = pair(
        combo_tables(false),
        "CREATE TABLE pairs(id TEXT NOT NULL PRIMARY KEY);
         CREATE TABLE combos(p TEXT NOT NULL, t TEXT NOT NULL, PRIMARY KEY(p, t));
         CREATE TABLE reviews(id TEXT NOT NULL PRIMARY KEY, audience TEXT NOT NULL,
           a TEXT REFERENCES pairs(id) ON UPDATE CASCADE, b TEXT,
           FOREIGN KEY(a, b) REFERENCES combos(p, t) DEFERRABLE INITIALLY DEFERRED)",
        "INSERT INTO pairs VALUES('p');
         INSERT INTO combos VALUES('p','00000000-0000-4000-8000-0000000000e1'),
           ('q','00000000-0000-4000-8000-0000000000e1')",
        "p",
    )
    .await;
    let write = vec![
        "DELETE FROM combos WHERE p='q'".to_owned(),
        "UPDATE pairs SET id='q' WHERE id='p'".to_owned(),
        format!("INSERT INTO combos VALUES('r','{TRACK}')"),
        "UPDATE pairs SET id='r' WHERE id='q'".to_owned(),
    ];
    accepted(pair, write, "SELECT a FROM reviews", Some("r")).await;
}

/// Ben's album carries his track into their circle at no step of the track's
/// own, so no step says where the track is when the pair's delete sets the
/// review's pair NULL: the action records a parent no step carries, and is a
/// direct change of Ana's review, which Ben may not make. A write naming the
/// store's track, whose incarnation the move ended, is refused on Ana's
/// device too (§9.6, §15.4).
#[tokio::test]
async fn a_reference_to_a_carried_row_is_a_direct_change() {
    const ALBUM: &str = "00000000-0000-4000-8000-0000000000e3";
    let pair = pair(
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
    .await;
    let audience = circle().to_string();
    let error = pair
        .ben
        .write(move |sql| {
            sql.execute("UPDATE albums SET audience=?1 WHERE id=?2", [audience.as_str(), ALBUM])?;
            sql.execute("DELETE FROM pairs WHERE id='p'", [])?;
            Ok(())
        })
        .await
        .unwrap_err();
    assert!(
        matches!(&error, crate::DbError::NotOwner { table, .. } if table == "reviews"),
        "{error:?}"
    );
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

/// `StepIndex::place` over the steps the merge model's operations on one
/// parent key record finds what the model's `place` does
/// (`Place.place_sound`, `Place.recorded_sound`).
mod model {
    use crate::judge::{Indexed, Place, StepIndex};
    use crate::test_utils::tests::run_lean;
    use coven_format::write::Half;
    use coven_foundation::id_source::CircleId;
    use coven_merge::{Audience, RowId};
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
                        match numbers.below(6) {
                            0 => json!({"op": "insert", "aud": audience, "inc": inc}),
                            1 => json!({"op": "update"}),
                            2 => json!({"op": "delete"}),
                            3 => json!({"op": "move", "aud": audience, "inc": inc}),
                            4 => json!({"op": "carry", "aud": audience, "inc": inc}),
                            _ => json!({"op": "other"}),
                        }
                    })
                    .collect();
                json!({"start": start, "ops": ops, "at": numbers.below(length + 1)})
            })
            .collect()
    }

    /// The index of a write whose steps on the key are `steps`, and whose
    /// carries changed the rows they moved the key into at no step.
    fn index(case: &Json, steps: &Json) -> StepIndex {
        let mut index = StepIndex::default();
        for step in steps.as_array().unwrap() {
            let inc = step["inc"].as_u64().unwrap();
            let delete = step["delete"].as_bool().unwrap();
            index.rows.entry(row(step["aud"].as_u64().unwrap())).or_default().push(Indexed {
                sequence: step["seq"].as_u64().unwrap().try_into().unwrap(),
                delete,
                half: if step["move"].as_bool().unwrap() { Half::Move } else { Half::Whole },
                generation: inc,
                after: if delete { inc + 1 } else { inc },
            });
        }
        for op in case["ops"].as_array().unwrap().iter().filter(|op| op["op"] == "carry") {
            let carried = row(op["aud"].as_u64().unwrap());
            index.rows.entry(carried.clone()).or_default();
            index.unstepped.insert(carried);
        }
        index
    }

    fn found(place: Place<'_>) -> Json {
        match place {
            Place::Untouched => json!({"kind": "untouched"}),
            Place::Unplaced => json!({"kind": "unplaced"}),
            Place::Last(row, step) => json!({
                "kind": "last",
                "seq": step.sequence,
                "aud": match &row.audience {
                    Audience::Store => 0,
                    Audience::Circle(circle) => circle.0.as_u128() as u64,
                },
                "delete": step.delete,
                "move": step.half == Half::Move,
                "inc": if step.delete { step.generation } else { step.after },
            }),
        }
    }

    #[test]
    #[ignore = "requires COVEN_MERGE_LEAN; scripts/check.sh supplies the merge runner"]
    fn places_match_the_model() {
        let runner = std::env::var_os("COVEN_MERGE_LEAN").expect("built mergeRunner");
        let cases = cases();
        let lean = run_lean(&runner, &json!({"mode": "place", "cases": cases}));
        let mut kinds = std::collections::BTreeMap::<String, usize>::new();
        let mut moves = 0;
        for (case, lean) in cases.iter().zip(lean.as_array().unwrap()) {
            if lean.is_null() {
                continue;
            }
            let at: u32 = case["at"].as_u64().unwrap().try_into().unwrap();
            let rust = found(index(case, &lean["steps"]).place("tracks", &[1], at));
            assert_eq!(rust, lean["place"], "{case}");
            *kinds.entry(rust["kind"].as_str().unwrap().to_owned()).or_default() += 1;
            moves += usize::from(rust["move"] == true);
        }
        assert!(kinds.values().all(|count| *count >= 100) && kinds.len() == 3, "{kinds:?}");
        assert!(moves >= 50, "{moves} places on a move's half");
    }
}

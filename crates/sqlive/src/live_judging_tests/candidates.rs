//! A write is judged only against the live queries whose scopes its rows
//! could pass: those whose equality or IN list holds a value its rows hold
//! before or after it, and those whose scope has no such key, however many
//! other live queries wait (§4).

use crate::tests::{next, QueryError, TestDatabase};
use crate::{ConnectionWorkCounter, LiveQuery};

const SCHEMA: &str = "CREATE TABLE tracks(id INTEGER PRIMARY KEY, release INTEGER, title TEXT)";

/// How many album pages wait, one for each release.
const PAGES: i64 = 200;

fn page(db: &TestDatabase, release: i64) -> LiveQuery<Vec<String>, QueryError> {
    db.subscribe(move |sql| {
        Ok(sql.query(
            "SELECT title FROM tracks WHERE release = ?1 ORDER BY id",
            [release],
            |r| r.get::<_, String>(0),
        )?)
    })
}

#[tokio::test]
async fn a_write_is_judged_only_against_its_candidates() {
    let db = TestDatabase::open(SCHEMA);
    db.write("INSERT INTO tracks VALUES (1, 1, 'one')").unwrap();
    let mut pages = Vec::new();
    for release in 0..PAGES {
        let mut query = page(&db, release);
        next(&mut query).await;
        pages.push(query);
    }
    let mut pair = db.subscribe(|sql| {
        Ok(sql.query(
            "SELECT title FROM tracks WHERE release IN (300, 301) ORDER BY id",
            [],
            |r| r.get::<_, String>(0),
        )?)
    });
    assert!(next(&mut pair).await.is_empty());
    // A comparison other than an equality gives the scope no key, so every
    // write to `tracks` judges this query.
    let mut late = db.subscribe(|sql| {
        Ok(sql.query("SELECT title FROM tracks WHERE title > 'x' ORDER BY id", [], |r| {
            r.get::<_, String>(0)
        })?)
    });
    assert!(next(&mut late).await.is_empty());
    let counter = ConnectionWorkCounter::new();
    db.track_work(&counter);
    let marked = |pages: &[LiveQuery<Vec<String>, QueryError>]| -> Vec<i64> {
        (0..PAGES).filter(|release| pages[*release as usize].is_marked_for_rerun()).collect()
    };

    // A track added to release 7 judges its page and the query with no key.
    db.write("INSERT INTO tracks VALUES (2, 7, 'two')").unwrap();
    assert_eq!(counter.take().judged_queries, 2);
    assert_eq!(marked(&pages), [7]);
    assert!(!late.is_marked_for_rerun());
    assert_eq!(next(&mut pages[7]).await, ["two"]);

    // A track moved from release 7 to release 9 judges both pages, by the
    // row before and the row after.
    db.write("UPDATE tracks SET release = 9 WHERE id = 2").unwrap();
    assert_eq!(counter.take().judged_queries, 3);
    assert_eq!(marked(&pages), [7, 9]);
    assert!(next(&mut pages[7]).await.is_empty());
    assert_eq!(next(&mut pages[9]).await, ["two"]);

    // One write to two releases judges each page once.
    db.write("INSERT INTO tracks VALUES (3, 4, 'three'), (4, 5, 'four'), (5, 4, 'five')").unwrap();
    assert_eq!(counter.take().judged_queries, 3);
    assert_eq!(marked(&pages), [4, 5]);
    assert_eq!(next(&mut pages[4]).await, ["three", "five"]);
    assert_eq!(next(&mut pages[5]).await, ["four"]);

    // A value of the IN list judges its query; a release no page shows
    // judges only the query with no key, which a late title reruns.
    db.write("INSERT INTO tracks VALUES (6, 301, 'six')").unwrap();
    assert_eq!(counter.take().judged_queries, 2);
    assert_eq!(next(&mut pair).await, ["six"]);
    db.write("INSERT INTO tracks VALUES (7, 500, 'zero')").unwrap();
    assert_eq!(counter.take().judged_queries, 1);
    assert!(marked(&pages).is_empty());
    assert_eq!(next(&mut late).await, ["zero"]);

    // A dropped page is judged no more.
    drop(pages.remove(7));
    db.write("INSERT INTO tracks VALUES (8, 7, 'eight')").unwrap();
    assert_eq!(counter.take().judged_queries, 1);
    db.close();
}

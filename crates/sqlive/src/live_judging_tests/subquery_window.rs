//! A subquery that takes `ORDER BY ... LIMIT` for each row around it depends
//! only on the rows of each window up to its LIMIT-th row, as a full list
//! window does on the rows up to its last (§4).

use super::lookup::follows;
use crate::tests::TestDatabase;

const ALBUMS: &str = "CREATE TABLE albums(id INTEGER PRIMARY KEY, title TEXT);
CREATE TABLE releases(id INTEGER PRIMARY KEY, album_id INTEGER, album_order INTEGER, title TEXT);
INSERT INTO albums VALUES (1,'one'),(2,'two'),(3,'three');
INSERT INTO releases VALUES (11,1,1,'a'),(12,1,2,'b'),(13,1,3,'c'),(21,2,1,'d'),(22,2,2,'e'),(31,3,1,'f')";

/// Each album shown with its first release's title.
const FIRST_RELEASE: &str = "SELECT (SELECT r.title FROM releases AS r WHERE r.album_id = a.id \
     ORDER BY r.album_order, r.id LIMIT 1), a.id FROM albums AS a WHERE a.id < 3";

/// A release after its album's first changes nothing the album shows; one
/// added before the first does, and the release it displaced then sits
/// after the window.
#[tokio::test]
async fn a_window_reruns_only_for_releases_up_to_its_limit() {
    let db = TestDatabase::open(ALBUMS);
    follows(
        &db,
        FIRST_RELEASE,
        vec![],
        &[
            ("UPDATE releases SET title = 'x' WHERE id = 13", false),
            ("UPDATE releases SET title = 'x' WHERE id = 22", false),
            ("UPDATE releases SET title = 'y' WHERE id = 11", true),
            ("INSERT INTO releases VALUES (14,1,9,'g')", false),
            // Album 1's window now begins with release 15.
            ("INSERT INTO releases VALUES (15,1,0,'h')", true),
            ("UPDATE releases SET title = 'z' WHERE id = 11", false),
            ("UPDATE releases SET title = 'z' WHERE id = 15", true),
            // A release equal on every term but `id` sorts after 15.
            ("INSERT INTO releases VALUES (16,1,0,'i')", false),
            // An album the screen doesn't show reruns nothing.
            ("UPDATE releases SET title = 'z' WHERE id = 31", false),
        ],
    )
    .await;
    db.close();
}

/// A release that moves to another album reruns the screen when it sorts
/// within the LIMIT of the window it leaves or the one it joins.
#[tokio::test]
async fn a_release_moving_between_albums_reruns_where_it_sorts_first() {
    let db = TestDatabase::open(ALBUMS);
    follows(
        &db,
        FIRST_RELEASE,
        vec![],
        &[
            // Release 13 becomes album 2's first.
            ("UPDATE releases SET album_id = 2, album_order = 0 WHERE id = 13", true),
            // Release 22 leaves album 2 from second place and joins album 1
            // last.
            ("UPDATE releases SET album_id = 1, album_order = 9 WHERE id = 22", false),
            // Album 1's first release moves to album 2 after 13 and 21.
            ("UPDATE releases SET album_id = 2, album_order = 5 WHERE id = 11", true),
            ("UPDATE releases SET title = 'x' WHERE id = 11", false),
            ("UPDATE releases SET title = 'x' WHERE id = 12", true),
        ],
    )
    .await;
    db.close();
}

/// Deleting an album's first release puts its second in the window, and an
/// album the screen stops showing narrows its releases away.
#[tokio::test]
async fn deleting_a_window_s_first_release_or_its_album_moves_the_window() {
    let db = TestDatabase::open(ALBUMS);
    follows(
        &db,
        FIRST_RELEASE,
        vec![],
        &[
            ("DELETE FROM releases WHERE id = 12", false),
            ("DELETE FROM releases WHERE id = 11", true),
            ("UPDATE releases SET title = 'x' WHERE id = 13", true),
            ("DELETE FROM albums WHERE id = 1", true),
            ("UPDATE releases SET title = 'y' WHERE id = 13", false),
            ("UPDATE releases SET title = 'y' WHERE id = 21", true),
        ],
    )
    .await;
    db.close();
}

/// A window of two releases holds each album's first two, among the
/// releases its own condition keeps.
#[tokio::test]
async fn a_window_of_two_holds_each_album_s_first_two_releases() {
    let db = TestDatabase::open(ALBUMS);
    follows(
        &db,
        "SELECT a.id FROM albums AS a WHERE a.id = 1 AND EXISTS (SELECT 1 FROM releases AS r \
         WHERE r.album_id = a.id AND r.title <> 'q' ORDER BY r.album_order DESC, r.id LIMIT 2)",
        vec![],
        &[
            ("UPDATE releases SET title = 'x' WHERE id = 11", false),
            ("UPDATE releases SET title = 'x' WHERE id = 12", true),
            // A release the own condition drops sits in no window.
            ("INSERT INTO releases VALUES (17,1,8,'q')", false),
            // The window now holds releases 18 and 13.
            ("INSERT INTO releases VALUES (18,1,8,'r')", true),
            ("UPDATE releases SET title = 'y' WHERE id = 12", false),
            ("UPDATE releases SET title = 'y' WHERE id = 13", true),
        ],
    )
    .await;
    db.close();
}

/// A comparison that takes a subquery's value narrows by every release the
/// subquery's conditions keep, so the subquery's window narrows nothing.
#[tokio::test]
async fn a_subquery_whose_value_a_comparison_takes_keeps_every_release() {
    let db = TestDatabase::open(ALBUMS);
    follows(
        &db,
        "SELECT a.id FROM albums AS a WHERE a.id = 1 AND a.title = (SELECT r.title \
         FROM releases AS r WHERE r.album_id = a.id ORDER BY r.album_order, r.id LIMIT 1)",
        vec![],
        &[("UPDATE releases SET title = 'x' WHERE id = 13", true)],
    )
    .await;
    db.close();
}

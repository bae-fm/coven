//! A device withdrawing an ancestor's last shared child while another device
//! adds a new shared child to it.
//!
//! The ancestor is `gated_by_descendants`: it is shared while some kept child
//! references it. One device removes the last such child, so its write retracts
//! the ancestor; another device, apart, shares a new child of the same
//! ancestor, whose write carries only the child because the ancestor was
//! already shared there. Whichever write is accepted first, both devices must
//! converge on the ancestor kept alive by the new child.

use crate::sync::local_replay_causality_tests::drain;
use crate::sync::test_helpers::*;
use coven_database::{DbError, Migration};
use coven_protocol::synced_schema::{RowIdentity, SyncedTable};

fn album_schema() -> (Vec<SyncedTable>, Vec<Migration>) {
    (
        vec![
            SyncedTable::new("albums", RowIdentity::SharedKey).gated_by_descendants(),
            SyncedTable::new("releases", RowIdentity::SharedKey).gated_by("shared"),
        ],
        vec![Migration::sql(
            1,
            "concurrent-ancestor-keep",
            "CREATE TABLE albums (
                 id TEXT PRIMARY KEY,
                 title TEXT NOT NULL,
                 _updated_at TEXT NOT NULL,
                 created_at TEXT NOT NULL
             ) STRICT;
             CREATE TABLE releases (
                 id TEXT PRIMARY KEY,
                 album_id TEXT NOT NULL REFERENCES albums(id),
                 shared INTEGER NOT NULL,
                 _updated_at TEXT NOT NULL,
                 created_at TEXT NOT NULL
             ) STRICT;",
        )],
    )
}

async fn write(db: &coven_database::Database, sql: &'static str) {
    store_database(db)
        .run_host_store_write_for_test(None, None, move |tx| {
            tx.execute_batch(sql)?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("capture write");
}

/// Which write publishes first.
#[derive(Clone, Copy, Debug)]
enum FirstPublished {
    Withdrawal,
    NewChild,
}

/// Which write replays first. Concurrent commits replay in the order of
/// their author streams, not of their publication, so the test picks the
/// withdrawing device by its stream to fix this order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FirstReplayed {
    Withdrawal,
    NewChild,
}

/// The author stream a device's next commit lands in: the one stream whose
/// position `publish` advances.
async fn stream_of(
    device: &TestDevice,
    publish: impl std::future::Future<Output = ()>,
) -> coven_protocol::membership::AuthorStreamId {
    let before = device
        .materialized_frontier()
        .await
        .expect("frontier before");
    publish.await;
    let after = device
        .materialized_frontier()
        .await
        .expect("frontier after");
    after
        .into_iter()
        .find(|(stream, reference)| {
            before
                .get(stream)
                .is_none_or(|prior| prior.coord.sequence() < reference.coord.sequence())
        })
        .map(|(_, reference)| reference.coord.stream_id)
        .expect("the device's own stream advanced")
}

#[derive(Clone, Copy, Debug)]
enum Withdrawer {
    Author,
    Peer,
}

async fn exercise(order: FirstPublished, replayed_first: FirstReplayed) {
    let seed = 81;
    let author_dir = test_store_dir();
    let (tables, migrations) = album_schema();
    let author_db = open_test_db_schema(author_dir.clone(), tables, migrations);
    let signer = user_keypair_from_seed([seed; 32]);
    let (store, _) = TestStore::create_with_connection(
        &author_db,
        author_dir.clone(),
        "concurrent-ancestor-keep",
        signer.clone(),
        test_cloud_home(),
    )
    .await
    .expect("create Store");
    let author = store
        .bind_device_in(&author_db, author_dir.clone(), &signer)
        .await
        .expect("bind author");
    let peer_dir = test_store_dir();
    let (tables, migrations) = album_schema();
    let peer_db = open_test_db_schema(peer_dir.clone(), tables, migrations);
    let peer = store
        .activate_joined_device(
            &author_db,
            author_dir,
            &peer_db,
            peer_dir,
            &signer,
            "2026-07-16T00:00:00Z",
        )
        .await
        .expect("activate peer");
    author.pull_store().await.expect("pull activation");

    let author_stream = stream_of(&author, async {
        write(
            &author_db,
            "INSERT INTO albums VALUES ('album', 'Album', '0000000001000-0000-author', '2026-01-01');
             INSERT INTO releases VALUES ('first', 'album', 1, '0000000001000-0000-author', '2026-01-01');",
        )
        .await;
        drain(&author).await;
    })
    .await;
    peer.pull_store()
        .await
        .expect("peer pulls the shared album");
    let peer_stream = stream_of(&peer, async {
        write(
            &peer_db,
            "INSERT INTO albums VALUES ('probe', 'Probe', '0000000001100-0000-peer', '2026-01-01');
             INSERT INTO releases VALUES ('probe', 'probe', 1, '0000000001100-0000-peer', '2026-01-01');",
        )
        .await;
        drain(&peer).await;
    })
    .await;
    author.pull_store().await.expect("author pulls the probe");
    let author_replays_first = author_stream < peer_stream;
    let withdrawer = if author_replays_first == (replayed_first == FirstReplayed::Withdrawal) {
        Withdrawer::Author
    } else {
        Withdrawer::Peer
    };

    // Apart: one device deletes the album's only release, which retracts the
    // album; the other adds a second release to it.
    let (withdrawing, withdrawing_db, adding, adding_db) = match withdrawer {
        Withdrawer::Author => (&author, &author_db, &peer, &peer_db),
        Withdrawer::Peer => (&peer, &peer_db, &author, &author_db),
    };
    write(withdrawing_db, "DELETE FROM releases WHERE id = 'first';").await;
    write(
        adding_db,
        "INSERT INTO releases VALUES ('second', 'album', 1, '0000000002000-0000-adder', '2026-01-01');",
    )
    .await;
    match order {
        FirstPublished::Withdrawal => {
            drain(withdrawing).await;
            drain(adding).await;
        }
        FirstPublished::NewChild => {
            drain(adding).await;
            drain(withdrawing).await;
        }
    }
    for device in [&author, &peer, &author, &peer] {
        let (_, pull) = device.pull_store().await.expect("pull");
        assert!(
            pull.held_positions.is_empty(),
            "{order:?}/{replayed_first:?}: a pull held: {:?}",
            pull.held_positions
        );
    }

    for (name, device) in [("author", &author), ("peer", &peer)] {
        assert_eq!(
            device
                .query_test_text(
                    "SELECT group_concat(id, ',') FROM (SELECT id FROM releases ORDER BY id)"
                )
                .await,
            "probe,second",
            "{order:?}/{replayed_first:?}: {name}'s releases",
        );
        assert_eq!(
            device
                .query_test_text("SELECT title FROM albums WHERE id = 'album'")
                .await,
            "Album",
            "{order:?}/{replayed_first:?}: {name} keeps the album the new release keeps alive",
        );
    }
}

#[tokio::test]
async fn withdrawal_replayed_and_published_first_keeps_the_ancestor() {
    exercise(FirstPublished::Withdrawal, FirstReplayed::Withdrawal).await;
}

#[tokio::test]
async fn withdrawal_replayed_first_but_published_second_keeps_the_ancestor() {
    exercise(FirstPublished::NewChild, FirstReplayed::Withdrawal).await;
}

#[tokio::test]
async fn new_child_replayed_first_but_published_second_keeps_the_ancestor() {
    exercise(FirstPublished::Withdrawal, FirstReplayed::NewChild).await;
}

#[tokio::test]
async fn new_child_replayed_and_published_first_keeps_the_ancestor() {
    exercise(FirstPublished::NewChild, FirstReplayed::NewChild).await;
}

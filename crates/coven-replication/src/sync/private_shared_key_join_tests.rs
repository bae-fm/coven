//! A row one device created privately meeting the shared row with the same
//! application key.
//!
//! `RowIdentity::SharedKey` names one logical row on every device. Two devices
//! that create that row while apart — each inside a subtree it keeps private
//! for now — have made two concurrent edits of one row, and the row merges
//! like any concurrent edit once either copy is shared, whichever side meets
//! the other first.

use crate::sync::local_replay_causality_tests::drain;
use crate::sync::store::pull::HeldStorePositionReason;
use crate::sync::test_helpers::*;
use coven_database::{DbError, Migration};
use coven_protocol::synced_schema::{RowIdentity, SyncedTable};

fn catalog_schema() -> (Vec<SyncedTable>, Vec<Migration>) {
    (
        vec![
            SyncedTable::new("artists", RowIdentity::SharedKey).gated_by_descendants(),
            SyncedTable::new("releases", RowIdentity::SharedKey).gated_by("shared"),
        ],
        vec![Migration::sql(
            1,
            "shared-key-catalog",
            "CREATE TABLE artists (
                 id TEXT PRIMARY KEY,
                 name TEXT NOT NULL,
                 sort_name TEXT NOT NULL,
                 _updated_at TEXT NOT NULL,
                 created_at TEXT NOT NULL
             ) STRICT;
             CREATE TABLE releases (
                 id TEXT PRIMARY KEY,
                 artist_id TEXT NOT NULL REFERENCES artists(id),
                 shared INTEGER NOT NULL,
                 _updated_at TEXT NOT NULL,
                 created_at TEXT NOT NULL
             ) STRICT;",
        )],
    )
}

struct TwoDevices {
    author: TestDevice,
    author_db: coven_database::Database,
    peer: TestDevice,
    peer_db: coven_database::Database,
}

async fn two_devices(name: &str, seed: u8) -> TwoDevices {
    let author_dir = test_store_dir();
    let (tables, migrations) = catalog_schema();
    let author_db = open_test_db_schema(author_dir.clone(), tables, migrations);
    let signer = user_keypair_from_seed([seed; 32]);
    let (store, _) = TestStore::create_with_connection(
        &author_db,
        author_dir.clone(),
        name,
        signer.clone(),
        test_cloud_home(),
    )
    .await
    .expect("create Store");
    let author = store
        .bind_device_in(&author_db, author_dir.clone(), &signer)
        .await
        .expect("bind author device");
    let peer_dir = test_store_dir();
    let (tables, migrations) = catalog_schema();
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
    TwoDevices {
        author,
        author_db,
        peer,
        peer_db,
    }
}

/// Import one artist and a release of it: the release gated by `shared`, the
/// artist shared only while a shared release keeps it.
async fn import(
    db: &coven_database::Database,
    release: &str,
    artist_name: &str,
    stamp: &str,
    shared: bool,
) {
    let release = release.to_string();
    let artist_name = artist_name.to_string();
    let stamp = stamp.to_string();
    store_database(db)
        .run_host_store_write_for_test(None, None, move |tx| {
            tx.execute(
                "INSERT INTO artists VALUES ('mb-artist', ?1, ?1, ?2, '2026-01-01')",
                rusqlite::params![artist_name, stamp],
            )?;
            tx.execute(
                "INSERT INTO releases VALUES (?1, 'mb-artist', ?2, ?3, '2026-01-01')",
                rusqlite::params![release, shared as i64, stamp],
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("capture import");
}

async fn share(db: &coven_database::Database, release: &str, stamp: &str) {
    let release = release.to_string();
    let stamp = stamp.to_string();
    store_database(db)
        .run_host_store_write_for_test(None, None, move |tx| {
            tx.execute(
                "UPDATE releases SET shared = 1, _updated_at = ?2 WHERE id = ?1",
                rusqlite::params![release, stamp],
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("capture share");
}

async fn pull_without_holds(device: &TestDevice, who: &str) {
    let (_, pull) = device.pull_store().await.expect("pull");
    let holds = pull
        .held_positions
        .iter()
        .map(|held| &held.reason)
        .collect::<Vec<_>>();
    assert!(
        !holds.iter().any(|reason| matches!(
            reason,
            HeldStorePositionReason::PrivateSharedConflict { .. }
        )),
        "{who}'s pull held on a private/shared key collision: {holds:?}"
    );
    assert!(holds.is_empty(), "{who}'s pull held: {holds:?}");
}

async fn artist(device: &TestDevice) -> String {
    device
        .query_test_text(
            "SELECT name || '/' || sort_name || '@' || _updated_at FROM artists WHERE id = 'mb-artist'",
        )
        .await
}

/// The bae import shape: each device imports the same artist into a Local
/// release, then makes its release Remote, all while apart. Both artist rows
/// were created privately; each device's pull meets the other's shared copy.
#[tokio::test]
async fn two_private_imports_of_one_key_merge_once_both_are_shared() {
    let devices = two_devices("private-imports-merge", 71).await;
    import(
        &devices.author_db,
        "author-release",
        "Author name",
        "0000000001000-0000-author",
        false,
    )
    .await;
    share(
        &devices.author_db,
        "author-release",
        "0000000001500-0000-author",
    )
    .await;
    import(
        &devices.peer_db,
        "peer-release",
        "Peer name",
        "0000000002000-0000-peer",
        false,
    )
    .await;
    share(&devices.peer_db, "peer-release", "0000000002500-0000-peer").await;
    drain(&devices.author).await;
    drain(&devices.peer).await;

    pull_without_holds(&devices.author, "author").await;
    pull_without_holds(&devices.peer, "peer").await;
    drain(&devices.author).await;
    drain(&devices.peer).await;
    pull_without_holds(&devices.author, "author").await;
    pull_without_holds(&devices.peer, "peer").await;

    let expected = "Peer name/Peer name@0000000002000-0000-peer";
    assert_eq!(artist(&devices.author).await, expected);
    assert_eq!(artist(&devices.peer).await, expected);
    for device in [&devices.author, &devices.peer] {
        assert_eq!(
            device
                .query_test_text(
                    "SELECT group_concat(id, ',') FROM (SELECT id FROM releases ORDER BY id)"
                )
                .await,
            "author-release,peer-release",
        );
    }
}

/// A device still holding its copy privately meets the other device's shared
/// copy. The older private copy gives way to the shared one.
#[tokio::test]
async fn an_older_private_row_takes_the_shared_row_with_its_key() {
    let devices = two_devices("older-private-joins", 72).await;
    import(
        &devices.author_db,
        "author-release",
        "Author name",
        "0000000001000-0000-author",
        false,
    )
    .await;
    import(
        &devices.peer_db,
        "peer-release",
        "Peer name",
        "0000000002000-0000-peer",
        true,
    )
    .await;
    drain(&devices.author).await;
    drain(&devices.peer).await;

    pull_without_holds(&devices.author, "author").await;
    assert_eq!(
        artist(&devices.author).await,
        "Peer name/Peer name@0000000002000-0000-peer"
    );
    assert_eq!(
        devices
            .author
            .query_test_text(
                "SELECT CAST(shared AS TEXT) FROM releases WHERE id = 'author-release'"
            )
            .await,
        "0",
        "the private release stays private",
    );
}

/// Even a newer private copy takes the shared row: its values were never
/// published, and the other device sharing the key is no reason to publish
/// them. Nothing of the private copy reaches the peer, and when this device
/// later shares its release, the rows it re-emits are the shared ones.
#[tokio::test]
async fn a_newer_private_row_takes_the_shared_row_and_publishes_nothing() {
    let devices = two_devices("newer-private-joins", 73).await;
    import(
        &devices.author_db,
        "author-release",
        "Author name",
        "0000000003000-0000-author",
        false,
    )
    .await;
    import(
        &devices.peer_db,
        "peer-release",
        "Peer name",
        "0000000002000-0000-peer",
        true,
    )
    .await;
    drain(&devices.author).await;
    drain(&devices.peer).await;

    pull_without_holds(&devices.author, "author").await;
    drain(&devices.author).await;
    pull_without_holds(&devices.peer, "peer").await;

    let expected = "Peer name/Peer name@0000000002000-0000-peer";
    assert_eq!(artist(&devices.author).await, expected);
    assert_eq!(artist(&devices.peer).await, expected);
    assert_eq!(
        devices
            .peer
            .query_test_text(
                "SELECT CAST(COUNT(*) AS TEXT) FROM releases WHERE id = 'author-release'"
            )
            .await,
        "0",
        "the private release stays on its device",
    );

    share(
        &devices.author_db,
        "author-release",
        "0000000004000-0000-author",
    )
    .await;
    drain(&devices.author).await;
    pull_without_holds(&devices.peer, "peer").await;
    assert_eq!(artist(&devices.peer).await, expected);
    assert_eq!(
        devices
            .peer
            .query_test_text(
                "SELECT CAST(COUNT(*) AS TEXT) FROM releases WHERE id = 'author-release'"
            )
            .await,
        "1",
    );
}

/// The other direction: the device's own retained private write is replayed
/// onto a snapshot that already holds the shared row with its key, which is
/// how publication rebases unpublished work. The private change to that row
/// gives way to the accepted row instead of blocking the write.
#[tokio::test]
async fn a_rebased_private_write_takes_the_shared_row_in_the_snapshot() {
    let devices = two_devices("rebased-private-joins", 75).await;
    store_database(&devices.author_db)
        .run_host_store_write_for_test(None, None, |tx| {
            tx.execute_batch(
                "INSERT INTO artists VALUES ('other-artist', 'Other', 'Other', \
                 '0000000001000-0000-author', '2026-01-01');
                 INSERT INTO releases VALUES ('other-release', 'other-artist', 1, \
                 '0000000001000-0000-author', '2026-01-01');",
            )?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("capture a shared write to reserve");
    let mut writer = devices
        .author
        .authorize_writer()
        .await
        .expect("authorize author writer");
    assert!(writer
        .prepare_pending_store_write()
        .await
        .expect("reserve the shared write"));
    drop(writer);
    import(
        &devices.author_db,
        "author-release",
        "Author name",
        "0000000003000-0000-author",
        false,
    )
    .await;

    import(
        &devices.peer_db,
        "peer-release",
        "Peer name",
        "0000000002000-0000-peer",
        true,
    )
    .await;
    drain(&devices.peer).await;
    devices
        .peer
        .publish_snapshot_generation_for_test()
        .await
        .expect("peer snapshots the shared artist");

    let mut writer = devices
        .author
        .authorize_writer()
        .await
        .expect("resume author publication");
    writer
        .drain_store_writes()
        .await
        .expect("the rebase joins the private artist instead of blocking its write");
    drop(writer);
    assert_eq!(
        artist(&devices.author).await,
        "Peer name/Peer name@0000000002000-0000-peer"
    );
    assert_eq!(
        devices
            .author
            .query_test_text(
                "SELECT CAST(shared AS TEXT) FROM releases WHERE id = 'author-release'"
            )
            .await,
        "0",
    );
}

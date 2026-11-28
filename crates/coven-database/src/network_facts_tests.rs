use crate::file_write::tests::{namespaces, open, stage, tables, SCHEMA};
use crate::tests::next;
use crate::tests::TestStore;
use crate::DbError;
use crate::{Migration, Provenance};
use coven_foundation::network_watch::NetworkState;

/// The network and the setting, as `coven_network` shows them, and whether
/// the one queued file is held for the network, as `coven_uploads` does.
const SHOWN: &str = "SELECT n.usable,n.metered,n.unmetered_only,
        (SELECT metered FROM coven_uploads)
    FROM coven_network AS n";

type Shown = (Option<bool>, bool, bool, Option<bool>);

fn shown(row: &sqlive::rusqlite::Row<'_>) -> sqlive::rusqlite::Result<Shown> {
    Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
}

/// `coven_network` shows the network as the store's watch last said and the
/// stored setting, and a live query that reads either runs again as the
/// network changes, with no write: nothing about the network is stored.
#[tokio::test]
async fn a_live_query_of_the_network_runs_again_as_it_changes() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    stage(&db, b"photo".to_vec(), true).await.unwrap();
    let mut network: crate::LiveQuery<_, DbError> =
        db.subscribe(crate::test_utils::LiveSql::of(SHOWN, shown).one());
    assert_eq!(next(&mut network).await, (None, false, false, Some(false)));
    db.record_network(NetworkState { usable: Some(true), metered: true, joined: 1 });
    assert_eq!(next(&mut network).await, (Some(true), true, false, Some(false)));
    db.write(|sql| sql.set_unmetered_only(true)).await.unwrap();
    assert_eq!(next(&mut network).await, (Some(true), true, true, Some(true)));
    db.record_network(NetworkState { usable: Some(true), metered: false, joined: 1 });
    assert_eq!(next(&mut network).await, (Some(true), false, true, Some(false)));
    db.close().await.unwrap();
}

/// A live query over `coven_uploads` that doesn't read `metered` doesn't run
/// again as the network becomes metered or stops being, and one that reads it
/// does.
#[tokio::test]
async fn a_metered_change_reruns_only_the_upload_reads_that_read_metered() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    stage(&db, b"photo".to_vec(), true).await.unwrap();
    db.write(|sql| sql.set_unmetered_only(true)).await.unwrap();
    let mut files: crate::LiveQuery<_, DbError> = db.subscribe(
        crate::test_utils::LiveSql::of(
            "SELECT count(file) FROM coven_uploads WHERE NOT sending",
            |r| r.get::<_, i64>(0),
        )
        .one(),
    );
    let mut held: crate::LiveQuery<_, DbError> = db.subscribe(
        crate::test_utils::LiveSql::of("SELECT metered FROM coven_uploads", |r| {
            r.get::<_, bool>(0)
        })
        .one(),
    );
    assert_eq!(next(&mut files).await, 1);
    assert!(!next(&mut held).await);
    for metered in [true, false] {
        db.record_network(NetworkState { usable: Some(true), metered, joined: 1 });
        assert!(!files.is_marked_for_rerun(), "metered {metered}");
        assert_eq!(next(&mut held).await, metered);
    }
    db.close().await.unwrap();
}

/// The setting persists across reopening; the network doesn't, until the
/// store's watch says it again.
#[tokio::test]
async fn the_setting_persists_and_the_network_does_not() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    db.write(|sql| sql.set_unmetered_only(true)).await.unwrap();
    db.record_network(NetworkState { usable: Some(true), metered: true, joined: 1 });
    db.close().await.unwrap();
    let db = open(&store, Provenance::AppProvided).await;
    let row = db
        .read(|sql| {
            Ok::<_, DbError>(sql.query_row(
                "SELECT usable,metered,unmetered_only FROM coven_network",
                [],
                |r| Ok((r.get::<_, Option<bool>>(0)?, r.get::<_, bool>(1)?, r.get::<_, bool>(2)?)),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(row, (None, false, true));
    db.write(|sql| sql.set_unmetered_only(false)).await.unwrap();
    let on = db
        .read(|sql| {
            Ok::<_, DbError>(sql.query_row(
                "SELECT unmetered_only FROM coven_network",
                [],
                |r| r.get::<_, bool>(0),
            )?)
        })
        .await
        .unwrap();
    assert!(!on);
    db.close().await.unwrap();
}

/// App SQL reads the network only through coven's views: the table that
/// holds it is coven's own.
#[tokio::test]
async fn app_sql_reads_the_network_only_through_the_views() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    for statement in ["SELECT usable FROM _coven_network", "SELECT count(*) FROM _coven_network"] {
        let error =
            db.read(move |sql| Ok(sql.query(statement, [], |_| Ok(()))?)).await.unwrap_err();
        assert!(matches!(error, DbError::InternalTable { .. }), "{statement}: {error:?}");
    }
    db.close().await.unwrap();
}

/// A read-only handle watches no network: it shows none known, and the
/// setting as stored.
#[tokio::test]
async fn a_read_only_handle_knows_no_network() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    db.write(|sql| sql.set_unmetered_only(true)).await.unwrap();
    db.record_network(NetworkState { usable: Some(true), metered: true, joined: 1 });
    let reader = store
        .builder(tables(), vec![Migration::addition(1, "files", SCHEMA)])
        .namespaces(namespaces(Provenance::AppProvided))
        .open_read_only()
        .await
        .unwrap();
    let row = reader
        .read(|sql| {
            Ok::<_, DbError>(sql.query_row(
                "SELECT usable,metered,unmetered_only FROM coven_network",
                [],
                |r| Ok((r.get::<_, Option<bool>>(0)?, r.get::<_, bool>(1)?, r.get::<_, bool>(2)?)),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(row, (None, false, true));
    reader.close().await.unwrap();
    db.close().await.unwrap();
}

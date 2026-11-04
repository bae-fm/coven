use crate::file_write::tests::{namespaces, open, stage, tables, SCHEMA};
use crate::tests::TestStore;
use crate::{Migration, Provenance};
use coven_foundation::network_watch::NetworkState;
use std::time::Duration;

/// The network and the setting, as `coven_network` shows them, and whether
/// the one queued file is held for the network, as `coven_uploads` does.
const SHOWN: &str = "SELECT n.usable,n.metered,n.unmetered_only,
        (SELECT metered FROM coven_uploads)
    FROM coven_network AS n";

type Shown = (Option<bool>, bool, bool, Option<bool>);

fn shown(row: &rusqlite::Row<'_>) -> rusqlite::Result<Shown> {
    Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
}

async fn next<T: Clone + PartialEq + Send + 'static>(query: &mut crate::LiveQuery<T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), query.next())
        .await
        .expect("the live query answers")
        .expect("the live query is open")
        .unwrap()
}

/// `coven_network` shows the network as the store's watch last said and the
/// stored setting, and a live query that reads either runs again as the
/// network changes, with no write: nothing about the network is stored.
#[tokio::test]
async fn a_live_query_of_the_network_runs_again_as_it_changes() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    stage(&db, b"photo".to_vec(), true).await.unwrap();
    let mut network = db.subscribe(|sql| Ok(sql.query_row(SHOWN, [], shown)?));
    assert_eq!(next(&mut network).await, (None, false, false, Some(false)));
    db.record_network(NetworkState { usable: Some(true), metered: true, joined: 1 });
    assert_eq!(next(&mut network).await, (Some(true), true, false, Some(false)));
    db.write(|sql| sql.set_unmetered_only(true)).await.unwrap();
    assert_eq!(next(&mut network).await, (Some(true), true, true, Some(true)));
    db.record_network(NetworkState { usable: Some(true), metered: false, joined: 1 });
    assert_eq!(next(&mut network).await, (Some(true), false, true, Some(false)));
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
            Ok(sql.query_row(
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
            Ok(sql.query_row("SELECT unmetered_only FROM coven_network", [], |r| {
                r.get::<_, bool>(0)
            })?)
        })
        .await
        .unwrap();
    assert!(!on);
    db.close().await.unwrap();
}

/// App SQL reads the network only through coven's views, whose columns tell
/// a live query to run again as it changes.
#[tokio::test]
async fn app_sql_reads_the_network_only_through_the_views() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    for statement in ["SELECT _coven_network_usable()", "SELECT _coven_network_metered()"] {
        let error =
            db.read(move |sql| Ok(sql.query(statement, [], |_| Ok(()))?)).await.unwrap_err();
        assert!(
            matches!(error, crate::CovenError::Database(crate::DbError::StatementForbidden { .. })),
            "{statement}: {error:?}"
        );
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
            Ok(sql.query_row(
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

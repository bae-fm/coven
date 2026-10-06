use crate::tests::TestStore;
use crate::write::tests::{notes, sql, NOTES};

#[tokio::test]
async fn changing_one_row_changes_only_one_leaf_and_its_audience_sum() {
    let store = TestStore::new();
    let db = store
        .schema(
            vec![
                crate::SyncedTable::new("notes", crate::RowIdentity::IndependentUuid)
                    .audience_column("audience"),
            ],
            "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT,audience TEXT NOT NULL)",
        )
        .await
        .unwrap();
    sql(
        &db,
        "INSERT INTO notes VALUES('00000000-0000-4000-8000-000000000001','first','store')",
    )
    .await
    .unwrap();
    let changed_key = db.inspect_writer(|db| {
        db.query_row("SELECT key FROM coven_fingerprint_leaves", [], |r| {
            r.get::<_, Vec<u8>>(0)
        })
        .unwrap()
    });
    sql(
        &db,
        "INSERT INTO notes VALUES('00000000-0000-4000-8000-000000000002','second','store'),('00000000-0000-4000-8000-000000000003','third','00000000-0000-4000-8000-000000000004')",
    )
    .await
    .unwrap();
    db.inspect_writer(|db| {
        db.batch("CREATE TABLE fingerprint_changes(table_name TEXT,audience TEXT,key BLOB)").unwrap();
        let tables = db.query("SELECT name FROM sqlite_schema WHERE type='table' AND name GLOB 'coven_fingerprint_*'", [], |r| r.get::<_, String>(0)).unwrap();
        for table in tables {
            let key = if table == "coven_fingerprint_leaves" { "new.key" } else { "NULL" };
            for action in ["INSERT", "UPDATE"] {
                db.batch(&format!("CREATE TRIGGER audit_{table}_{action} AFTER {action} ON {table} BEGIN INSERT INTO fingerprint_changes VALUES('{table}',new.audience,{key}); END")).unwrap();
            }
        }
    });
    sql(
        &db,
        "UPDATE notes SET title='changed' WHERE id='00000000-0000-4000-8000-000000000001'",
    )
    .await
    .unwrap();
    let changes = db.inspect_writer(|db| {
        db.query(
            "SELECT table_name,audience FROM fingerprint_changes ORDER BY table_name",
            [],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
        )
        .unwrap()
    });
    assert_eq!(
        changes.len(),
        2,
        "a row changes its leaf and audience sum once each"
    );
    assert_eq!(
        changes,
        [
            ("coven_fingerprint_leaves".into(), "store".into()),
            ("coven_fingerprint_sums".into(), "store".into())
        ]
    );
    db.inspect_writer(|db| {
        let key: Vec<u8> = db
            .query_row(
                "SELECT key FROM fingerprint_changes WHERE table_name='coven_fingerprint_leaves'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(key, changed_key);
        let statements: Vec<_> = db
            .fullscan_statements()
            .into_iter()
            .filter(|(sql, _)| sql.starts_with("INSERT INTO coven_fingerprint_"))
            .collect();
        assert_eq!(statements.len(), 2, "{statements:?}");
        assert!(
            statements.iter().all(|(_, fullscan)| *fullscan == 0),
            "{statements:?}"
        );
    });
    db.close().await.unwrap();
}

#[test]
fn sum_arithmetic_wraps_and_carries_across_all_256_bits() {
    let mut one = [0; 32];
    one[31] = 1;
    assert_eq!(super::replace_sum([255; 32], [0; 32], one), [0; 32]);
    assert_eq!(super::replace_sum([0; 32], one, [0; 32]), [255; 32]);
    let mut high = [0; 32];
    high[0] = 1;
    let mut borrowed = [255; 32];
    borrowed[0] = 0;
    assert_eq!(super::replace_sum(high, one, [0; 32]), borrowed);
    assert_eq!(super::replace_sum(borrowed, [0; 32], one), high);
    assert_eq!(super::replace_sum([17; 32], [255; 32], [255; 32]), [17; 32]);
}

#[tokio::test]
async fn sums_agree_in_every_order_of_concurrent_and_excluded_writes() {
    use crate::write::tests::records;
    use coven_format::write::WriteDisposition;
    use coven_foundation::{clock::FixedClock, id_source::SequentialIds};
    use coven_merge::Audience;
    use std::{
        sync::Arc,
        time::{Duration, UNIX_EPOCH},
    };
    let ids = SequentialIds::new();
    let clock = Arc::new(FixedClock::new(UNIX_EPOCH + Duration::from_secs(1)));
    let stores = [
        TestStore::with_ids(&ids),
        TestStore::with_ids(&ids),
        TestStore::with_ids(&ids),
    ];
    let mut sources = Vec::new();
    for store in &stores {
        sources.push(
            store
                .builder(notes(), vec![crate::Migration::sql(1, "notes", NOTES)])
                .clock(clock.clone())
                .open()
                .await
                .unwrap(),
        );
    }
    sql(
        &sources[0],
        "INSERT INTO notes VALUES('shared','initial','')",
    )
    .await
    .unwrap();
    let initial = records(&sources[0]).remove(0);
    sources[1]
        .apply_downloaded(initial.clone().into())
        .await
        .unwrap();
    sql(&sources[0], "UPDATE notes SET title='a'")
        .await
        .unwrap();
    sql(&sources[1], "UPDATE notes SET title='b'")
        .await
        .unwrap();
    sql(
        &sources[2],
        "INSERT INTO notes VALUES('excluded','lost','')",
    )
    .await
    .unwrap();
    let mut excluded = records(&sources[2]).remove(0);
    excluded.header.disposition = WriteDisposition::Lost(1);
    let changes = [
        records(&sources[0]).remove(1),
        records(&sources[1]).remove(0),
        excluded,
    ];
    let key =
        coven_crypto::StoreKey::from_bytes(std::num::NonZeroU64::new(1).unwrap(), [7; 32]).derive();
    let mut expected = None;
    for order in [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ] {
        let store = TestStore::with_ids(&ids);
        let db = store.schema(notes(), NOTES).await.unwrap();
        db.apply_downloaded(initial.clone().into()).await.unwrap();
        for index in order {
            db.apply_downloaded(changes[index].clone().into())
                .await
                .unwrap();
        }
        assert_eq!(db.lost_values().await.unwrap().len(), 2);
        // Read the persisted sum itself, as well as the keyed value sync posts.
        let sum = db.inspect_writer(|db| {
            db.query_row(
                "SELECT sum FROM coven_fingerprint_sums WHERE audience='store'",
                [],
                |r| r.get::<_, [u8; 32]>(0),
            )
            .unwrap()
        });
        db.close().await.unwrap();
        let db = store.schema(notes(), NOTES).await.unwrap();
        let state = db
            .sync_state(vec![(Audience::Store, key.fingerprint_hasher())])
            .await
            .unwrap();
        let actual = (sum, state.fingerprints[0].1, state.positions);
        if let Some(expected) = &expected {
            assert_eq!(&actual, expected, "{order:?}");
        } else {
            expected = Some(actual);
        }
        db.close().await.unwrap();
    }
    for db in sources {
        db.close().await.unwrap();
    }
}

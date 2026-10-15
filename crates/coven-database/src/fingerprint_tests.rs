use crate::tests::TestStore;
use crate::write::tests::{notes, sql, NOTES};
use coven_foundation::id_source::KeyId;

#[tokio::test]
async fn fingerprint_leaves_pin_context_and_bind_both_hashes() {
    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    // Independently calculated with Python hashlib and D11's length-prefixed
    // context: leaf label, identity hash, value hash, in that order.
    for (identity, value, expected) in [
        (
            1,
            2,
            [
                0xb9, 0x40, 0xf0, 0x9f, 0xd7, 0xf8, 0xa6, 0x94, 0x8f, 0x86, 0xf1, 0x72, 0xca, 0x89,
                0x14, 0x41, 0x56, 0x4a, 0x5c, 0x57, 0x41, 0x38, 0xce, 0xc1, 0xd5, 0x7d, 0x5c, 0xd4,
                0xa5, 0x4b, 0xf7, 0xbf,
            ],
        ),
        (
            3,
            2,
            [
                0x51, 0x0f, 0x59, 0x17, 0x83, 0x62, 0x1b, 0xaa, 0xca, 0xbb, 0xc0, 0xad, 0x35, 0x5e,
                0x90, 0x1c, 0x97, 0x6f, 0xfc, 0x4e, 0x80, 0x76, 0x37, 0x72, 0x3c, 0x69, 0xcc, 0x43,
                0xb0, 0x70, 0x93, 0x7d,
            ],
        ),
        (
            1,
            4,
            [
                0xe1, 0x65, 0xb5, 0x81, 0x07, 0xbe, 0xc4, 0x70, 0xd4, 0xe0, 0x46, 0x12, 0xb6, 0x66,
                0x75, 0x01, 0x4d, 0x98, 0x13, 0xb5, 0xc3, 0x39, 0x76, 0xc3, 0x3c, 0x17, 0xd5, 0x37,
                0xac, 0xe9, 0x9b, 0xd2,
            ],
        ),
    ] {
        let identity = [identity; 32];
        let stored: [u8; 32] = db.inspect_writer(|connection| {
            super::put(
                connection,
                &coven_merge::Audience::Store,
                identity,
                [value; 32],
                None,
            )
            .unwrap();
            connection
                .query_row(
                    "SELECT hash FROM _coven_fingerprint_leaves WHERE audience='store' AND key=?1",
                    [identity.as_slice()],
                    |row| row.get(0),
                )
                .unwrap()
        });
        assert_eq!(stored, expected);
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn fingerprint_roots_pin_raw_audience_and_sum_bytes() {
    use coven_foundation::id_source::CircleId;
    use coven_merge::Audience;

    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    let key = coven_crypto::StoreKey::from_bytes(KeyId(uuid::Uuid::from_bytes([1; 16])), [7; 32])
        .derive();
    // Independently calculated with Python hashlib/hmac: HKDF-SHA256 with
    // D11's fingerprint label, then the raw root label, audience and sum.
    for (audience, text, empty, populated) in [
        (
            Audience::Store,
            "store",
            [
                0xcd, 0x72, 0x28, 0x38, 0x3f, 0xf4, 0xe6, 0x43, 0xbc, 0xd1, 0x75, 0x86, 0x5a, 0x44,
                0x98, 0xd9, 0x63, 0x06, 0xb0, 0x9f, 0xe0, 0x86, 0x13, 0xbb, 0x1f, 0x55, 0x0b, 0x3d,
                0x12, 0x84, 0x0e, 0x9d,
            ],
            [
                0xf8, 0x27, 0xad, 0xcb, 0xd7, 0xde, 0xcc, 0x82, 0x89, 0x1b, 0x24, 0xe3, 0x52, 0x4a,
                0x1a, 0x38, 0x70, 0x57, 0x2c, 0x42, 0x88, 0x81, 0x24, 0x7e, 0x3d, 0x25, 0xd6, 0x1b,
                0x37, 0xc6, 0x90, 0x4e,
            ],
        ),
        (
            Audience::Circle(CircleId(uuid::Uuid::from_u128(
                0x00112233445566778899aabbccddeeff,
            ))),
            "00112233-4455-6677-8899-aabbccddeeff",
            [
                0xb2, 0x8b, 0x8d, 0x16, 0x6b, 0x49, 0xf9, 0x6e, 0x06, 0xfa, 0x18, 0x5d, 0xd5, 0xb6,
                0xd1, 0x24, 0x5d, 0x78, 0x2d, 0x0c, 0xde, 0x47, 0x39, 0xb2, 0x99, 0xd9, 0xb6, 0x0c,
                0x82, 0x9e, 0x98, 0x35,
            ],
            [
                0x1b, 0x08, 0xa5, 0xc9, 0x5c, 0x3d, 0xe7, 0x3c, 0x42, 0x33, 0x5a, 0xc1, 0x6f, 0x58,
                0xcf, 0x00, 0xe6, 0x3d, 0x59, 0x6e, 0x5f, 0x79, 0xd2, 0x23, 0x5e, 0x4e, 0xf6, 0x8a,
                0x7c, 0xf5, 0x28, 0xe6,
            ],
        ),
    ] {
        let read = || {
            db.inspect_writer(|connection| {
                super::read(connection, &audience, key.fingerprint_hasher()).unwrap()
            })
        };
        assert_eq!(read().as_bytes(), &empty);
        db.inspect_writer(|connection| {
            connection
                .internal_execute(
                    "INSERT INTO _coven_fingerprint_sums(audience,sum) VALUES(?1,?2)",
                    rusqlite::params![text, [0u8; 32].as_slice()],
                )
                .unwrap();
        });
        assert_eq!(read().as_bytes(), &empty);
        let sum: [u8; 32] = std::array::from_fn(|i| i as u8);
        db.inspect_writer(|connection| {
            connection
                .internal_execute(
                    "UPDATE _coven_fingerprint_sums SET sum=?2 WHERE audience=?1",
                    rusqlite::params![text, sum.as_slice()],
                )
                .unwrap();
        });
        assert_eq!(read().as_bytes(), &populated);
    }
    db.close().await.unwrap();
}

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
    store.join_circle(
        &db,
        coven_foundation::id_source::CircleId(
            uuid::Uuid::parse_str("00000000-0000-4000-8000-000000000004").unwrap(),
        ),
    );
    sql(
        &db,
        "INSERT INTO notes VALUES('00000000-0000-4000-8000-000000000001','first','store')",
    )
    .await
    .unwrap();
    let changed_key = db.inspect_writer(|db| {
        db.query_row("SELECT key FROM _coven_fingerprint_leaves", [], |r| {
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
        let tables = db.query("SELECT name FROM sqlite_schema WHERE type='table' AND name GLOB '_coven_fingerprint_*'", [], |r| r.get::<_, String>(0)).unwrap();
        for table in tables {
            let key = if table == "_coven_fingerprint_leaves" { "new.key" } else { "NULL" };
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
            ("_coven_fingerprint_leaves".into(), "store".into()),
            ("_coven_fingerprint_sums".into(), "store".into())
        ]
    );
    db.inspect_writer(|db| {
        let key: Vec<u8> = db
            .query_row(
                "SELECT key FROM fingerprint_changes WHERE table_name='_coven_fingerprint_leaves'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(key, changed_key);
        let statements: Vec<_> = db
            .fullscan_statements()
            .into_iter()
            .filter(|(sql, _)| sql.starts_with("INSERT INTO _coven_fingerprint_"))
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
    sources[1].receive(initial.clone().into()).await.unwrap();
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
    excluded.header.schema_version = 0;
    let changes = [
        records(&sources[0]).remove(1),
        records(&sources[1]).remove(0),
        excluded,
    ];
    let key = coven_crypto::StoreKey::from_bytes(KeyId(uuid::Uuid::from_bytes([1; 16])), [7; 32])
        .derive();
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
        db.exclude_version_zero().await;
        db.receive(initial.clone().into()).await.unwrap();
        for index in order {
            db.receive(changes[index].clone().into()).await.unwrap();
        }
        assert_eq!(db.lost_values().await.unwrap().len(), 2);
        // Read the persisted sum itself, as well as the keyed value sync posts.
        let sum = db.inspect_writer(|db| {
            db.query_row(
                "SELECT sum FROM _coven_fingerprint_sums WHERE audience='store'",
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

#[tokio::test]
async fn root_bytes_match_independent_hmac_vectors() {
    use coven_foundation::id_source::CircleId;
    use coven_merge::Audience;

    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    let key = coven_crypto::StoreKey::from_bytes(KeyId(uuid::Uuid::from_bytes([1; 16])), [7; 32])
        .derive();
    // Python hashlib/hmac: no-salt HKDF with context("coven/fingerprints/v1"),
    // then HMAC of the raw root label, audience text and 32-byte sum.
    for (audience, empty, populated) in [
        (
            Audience::Store,
            "cd7228383ff4e643bcd175865a4498d96306b09fe08613bb1f550b3d12840e9d",
            "f827adcbd7decc82891b24e3524a1a3870572c428881247e3d25d61b37c6904e",
        ),
        (
            Audience::Circle(CircleId(uuid::Uuid::from_bytes([1; 16]))),
            "730b973b198ba16495ebf71ac3d7c50c3816579d38746124bf1cc8bf207860f0",
            "902f2fda1a025e49e220e779bef1c671cf9dc297804ba49fc3fd53c448d01cb4",
        ),
    ] {
        for (sum, expected) in [
            (None, empty),
            (
                Some(std::array::from_fn::<_, 32, _>(|i| i as u8)),
                populated,
            ),
        ] {
            db.commit_writer(|connection| {
                if let Some(sum) = sum {
                    connection
                        .internal_execute(
                            "INSERT INTO _coven_fingerprint_sums(audience,sum) VALUES(?1,?2)",
                            rusqlite::params![
                                crate::write_encoding::audience_text(&audience),
                                sum.as_slice()
                            ],
                        )
                        .unwrap();
                }
                let actual = super::read(connection, &audience, key.fingerprint_hasher()).unwrap();
                let expected: Vec<_> = expected
                    .as_bytes()
                    .chunks_exact(2)
                    .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
                    .collect();
                assert_eq!(actual.as_bytes().as_slice(), expected);
            });
        }
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn row_fingerprint_pins_generations_setters_written_parents_and_visible_values() {
    use crate::{RowIdentity, SyncedTable};
    let store = TestStore::new();
    let db = store.schema(
        vec![SyncedTable::new("parents", RowIdentity::SharedKey), SyncedTable::new("children", RowIdentity::SharedKey)],
        "CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE children(id TEXT NOT NULL PRIMARY KEY,parent TEXT REFERENCES parents(id) ON DELETE SET NULL,value TEXT)",
    ).await.unwrap();
    assert_eq!(db.local_store_log().await.unwrap().device.0, 2);
    // Independently calculated from D2/D3/D11 with Python hashlib. The
    // written-parent map includes only parent; visible maps have no links.
    // The deleted row keeps both generations and encodes empty cell maps.
    let identity: [u8; 32] = [
        253, 146, 165, 112, 142, 243, 210, 89, 231, 109, 229, 181, 166, 91, 65, 5, 236, 57, 82,
        111, 83, 18, 242, 139, 182, 100, 236, 175, 70, 48, 77, 248,
    ];
    for (statement, expected) in [
        (
            "INSERT INTO parents VALUES('p'); INSERT INTO children VALUES('c','p','v')",
            [
                144, 125, 246, 166, 30, 172, 22, 114, 194, 139, 164, 8, 225, 70, 225, 58, 87, 131,
                228, 174, 37, 27, 161, 50, 159, 219, 192, 37, 198, 148, 129, 194,
            ],
        ),
        (
            "UPDATE children SET value='w' WHERE id='c'",
            [
                218, 251, 107, 72, 14, 182, 218, 10, 98, 172, 227, 84, 198, 250, 196, 152, 36, 208,
                2, 4, 54, 209, 100, 216, 214, 150, 192, 226, 28, 165, 225, 175,
            ],
        ),
        (
            "DELETE FROM children WHERE id='c'",
            [
                8, 67, 13, 43, 206, 195, 4, 138, 15, 252, 72, 197, 15, 30, 184, 231, 157, 168, 190,
                216, 58, 252, 192, 89, 217, 95, 137, 18, 185, 41, 139, 68,
            ],
        ),
    ] {
        sql(&db, statement).await.unwrap();
        let hash = db.inspect_writer(|db| {
            db.query_row(
                "SELECT hash FROM _coven_fingerprint_leaves WHERE audience='store' AND key=?1",
                [identity.as_slice()],
                |row| row.get::<_, [u8; 32]>(0),
            )
            .unwrap()
        });
        assert_eq!(hash, expected, "{statement}");
    }
    db.close().await.unwrap();
}

/// A snapshot's frames with the agreement state they were written from.
async fn captured(
    database: &crate::Database,
) -> (
    crate::snapshot_write::tests::SnapshotFrames,
    crate::CapturedState,
) {
    use std::sync::{Arc, Mutex};
    let frames = Arc::new(Mutex::new(Vec::new()));
    let metadata = Arc::new(Mutex::new(None));
    let (output, start) = (frames.clone(), metadata.clone());
    let state = database
        .write_snapshot(
            crate::snapshot_write::tests::id(coven_merge::Audience::Store),
            move |header| {
                *start.lock().unwrap() = Some(crate::snapshot_write::tests::prefix(header));
                Ok(())
            },
            move |frame| {
                output.lock().unwrap().push(frame);
                Ok::<_, std::convert::Infallible>(())
            },
        )
        .await
        .unwrap();
    let frames = crate::snapshot_write::tests::SnapshotFrames {
        prefix: metadata.lock().unwrap().clone().unwrap(),
        frames: frames.lock().unwrap().clone(),
    };
    (frames, state)
}

/// Before publication the writer loads its sealed plaintext in a discarded
/// transaction (§15): the snapshot it wrote reproduces the captured state,
/// also after later writes; damaged plaintext or another state's capture is
/// refused and nothing changes.
#[tokio::test]
async fn a_written_snapshot_reproduces_its_captured_state_before_publication() {
    use crate::write::tests::{count, notes, sql, NOTES};
    let store = crate::tests::TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('a','one',''),('b','two','')")
        .await
        .unwrap();
    let (snapshot, state) = captured(&db).await;
    let id = crate::snapshot_write::tests::id(coven_merge::Audience::Store);
    let verify = |bytes: Vec<u8>, state| {
        let db = db.clone();
        let (id, prefix) = (id.clone(), snapshot.prefix.clone());
        async move {
            db.verify_snapshot(
                id,
                prefix,
                std::io::Cursor::new(bytes),
                Some(state),
                std::sync::Arc::new(crate::test_utils::TestAuthorship),
            )
            .await
        }
    };
    verify(snapshot.concat(), state).await.unwrap();
    // Later writes do not take part in the comparison.
    sql(&db, "UPDATE notes SET title='later' WHERE id='a'")
        .await
        .unwrap();
    let (later, later_state) = captured(&db).await;
    verify(snapshot.concat(), state).await.unwrap();
    assert!(matches!(
        verify(snapshot.concat(), later_state).await,
        Err(crate::DbError::Snapshot(
            crate::SnapshotError::CapturedState
        ))
    ));
    let mut damaged = later.concat();
    let middle = damaged.len() / 2;
    damaged[middle] ^= 0x40;
    assert!(verify(damaged, later_state).await.is_err());
    assert_eq!(count(&db, "notes"), 2);
    let title: String = db
        .read(|sql| Ok(sql.query_row("SELECT title FROM notes WHERE id='a'", [], |r| r.get(0))?))
        .await
        .unwrap();
    assert_eq!(title, "later");
}

/// The agreement state is a function of the rows, not of when a column was
/// added: a device that received rows before an addition and one that
/// received them after agree (§19.1).
#[tokio::test]
async fn an_addition_leaves_fingerprints_independent_of_arrival() {
    use crate::write::tests::{notes, records, sql, NOTES};
    let migrations = || {
        vec![
            crate::Migration::sql(1, "notes", NOTES),
            crate::Migration::sql(2, "color", "ALTER TABLE notes ADD COLUMN color TEXT"),
        ]
    };
    let ids = coven_foundation::id_source::SequentialIds::new();
    let stores: Vec<_> = (0..3)
        .map(|_| crate::tests::TestStore::with_ids(&ids))
        .collect();
    let author = stores[0].schema(notes(), NOTES).await.unwrap();
    sql(&author, "INSERT INTO notes VALUES('a','one','')")
        .await
        .unwrap();
    let write = records(&author).remove(0);
    let early = stores[1].schema(notes(), NOTES).await.unwrap();
    early.receive(write.clone().into()).await.unwrap();
    early.close().await.unwrap();
    let early = stores[1]
        .builder(notes(), migrations())
        .open()
        .await
        .unwrap();
    let late = stores[2]
        .builder(notes(), migrations())
        .open()
        .await
        .unwrap();
    late.receive(write.into()).await.unwrap();
    let key = coven_crypto::StoreKey::from_bytes(
        coven_foundation::id_source::KeyId(uuid::Uuid::from_bytes([1; 16])),
        [7; 32],
    )
    .derive();
    let fingerprint = |db: crate::Database| {
        let hasher = key.fingerprint_hasher();
        async move {
            db.sync_state(vec![(coven_merge::Audience::Store, hasher)])
                .await
                .unwrap()
                .fingerprints
        }
    };
    assert_eq!(
        fingerprint(early.clone()).await,
        fingerprint(late.clone()).await
    );
    for db in [author, early, late] {
        db.close().await.unwrap();
    }
}

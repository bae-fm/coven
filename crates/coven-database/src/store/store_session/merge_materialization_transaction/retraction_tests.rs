use super::retraction::*;
use super::*;
use rusqlite::Connection;

fn test_object(path: &str) -> coven_protocol::objects::ExactObjectRef {
    coven_protocol::objects::ExactObjectRef::new(
        coven_protocol::objects::ObjectSlot::logical(path.to_string())
            .expect("valid test object slot"),
        0,
        ObjectHash::digest(path.as_bytes()),
    )
}

#[test]
fn merge_retraction_requires_the_exact_transitive_dependent_closure() {
    let stream = coven_protocol::causal_grants::AuthorStreamId::from_bytes([19; 32]);
    let commit = |sequence: u64, label: &str| StoreBatchCommitRef {
        coord: StoreCommitCoord {
            stream_id: stream,
            sequence,
        },
        commit_hash: ObjectHash::digest(format!("{label} commit").as_bytes()),
        object: test_object(&format!("store-v1/test/{label}/commit.json")),
    };
    let root = commit(1, "retraction-root");
    let child = commit(2, "retraction-child");
    let grandchild = commit(3, "retraction-grandchild");
    let independent = commit(4, "retraction-independent");
    let graph = BTreeMap::from([
        (root.clone(), BTreeSet::new()),
        (child.clone(), BTreeSet::from([root.clone()])),
        (grandchild.clone(), BTreeSet::from([child.clone()])),
        (independent.clone(), BTreeSet::new()),
    ]);

    let required = complete_merge_retraction_closure(&graph, BTreeSet::from([root.clone()]));

    assert_eq!(
        required,
        BTreeSet::from([root.clone(), child.clone(), grandchild]),
    );
    assert_ne!(required, BTreeSet::from([root.clone(), child.clone()]));
    assert!(!required.contains(&independent));
    assert!(require_exact_merge_retraction_closure(
        &graph,
        BTreeSet::from([root.clone()]),
        &BTreeSet::from([root, child]),
    )
    .is_err());
}

#[test]
fn merge_retraction_retires_its_circle_bootstrap_coverage_atomically() {
    let (_store_temp, store_dir) = coven_foundation::store_dir::temp_store_dir();
    let connection = Connection::open_in_memory().expect("open retraction database");
    crate::apply_coven_schema(&connection).expect("apply Coven schema");
    let connection = crate::DatabaseTestSql::for_store(&connection, &store_dir);
    let activation = StoreBatchCommitRef {
        coord: StoreCommitCoord {
            stream_id: coven_protocol::causal_grants::AuthorStreamId::from_bytes([23; 32]),
            sequence: 7,
        },
        commit_hash: ObjectHash::digest(b"Circle bootstrap retraction activation"),
        object: test_object("store-v1/test/circle-bootstrap-retraction/commit.json"),
    };
    let encoded_activation =
        serde_json::to_string(&activation).expect("serialize bootstrap activation");
    let image = b"Circle bootstrap retraction image";
    let image_hash = connection
        .install_payload(image)
        .expect("install bootstrap image payload");
    let circle_id = coven_protocol::circle::CircleId::from_bytes([1; 16]);
    connection
        .execute(
            "INSERT INTO circle_bootstrap_coverage
                         (circle_id, control_coord, activation_commit, exact_cut, image_hash,
                          bootstrap_ref)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![
                circle_id.to_string(),
                "{}",
                encoded_activation,
                "{}",
                image_hash.to_string(),
                b"{}".as_slice(),
            ],
        )
        .expect("insert Circle bootstrap coverage");
    connection
        .set_payload_owner_claims(
            &crate::payload_store::circle_bootstrap_coverage_owner_key(circle_id),
            &BTreeSet::from([image_hash]),
        )
        .expect("claim bootstrap image payload");
    connection
        .rolled_back_transaction(|transaction| {
            assert_eq!(
                transaction.retire_circle_bootstrap_coverage(&store_dir, &activation)?,
                1
            );
            let retained: i64 = transaction
                .query_row(
                    "SELECT COUNT(*) FROM circle_bootstrap_coverage",
                    [],
                    |row| row.get(0),
                )
                .map_err(DbError::from)?;
            assert_eq!(retained, 0);
            Ok(())
        })
        .expect("roll back bootstrap coverage retirement");
    let retained: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM circle_bootstrap_coverage",
            [],
            |row| row.get(0),
        )
        .expect("count retained bootstrap coverage");
    assert_eq!(retained, 1);
    let claims: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM payload_owners WHERE owner_key = ?1",
            [crate::payload_store::circle_bootstrap_coverage_owner_key(
                circle_id,
            )],
            |row| row.get(0),
        )
        .expect("count retained payload claims");
    assert_eq!(claims, 1);
    connection
        .transaction(|transaction| {
            assert_eq!(
                transaction.retire_circle_bootstrap_coverage(&store_dir, &activation)?,
                1
            );
            Ok(())
        })
        .expect("commit bootstrap coverage retirement");
    let retained: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM circle_bootstrap_coverage",
            [],
            |row| row.get(0),
        )
        .expect("count committed bootstrap coverage");
    assert_eq!(retained, 0);
    let (claims, cleanup): (i64, i64) = connection
        .query_row(
            "SELECT
                         (SELECT COUNT(*) FROM payload_owners WHERE owner_key = ?1),
                         (SELECT COUNT(*) FROM payload_cleanup WHERE payload_hash = ?2)",
            rusqlite::params![
                crate::payload_store::circle_bootstrap_coverage_owner_key(circle_id),
                image_hash.to_string(),
            ],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("count committed payload cleanup");
    assert_eq!((claims, cleanup), (0, 1));
}

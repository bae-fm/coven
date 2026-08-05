use super::retraction::*;
use super::*;

fn test_object(path: &str) -> crate::protocol::objects::ExactObjectRef {
    crate::protocol::objects::ExactObjectRef::new(
        crate::protocol::objects::ObjectSlot::logical(path.to_string())
            .expect("valid test object slot"),
        0,
        ObjectHash::digest(path.as_bytes()),
    )
}

#[test]
fn merge_retraction_requires_the_exact_transitive_dependent_closure() {
    let stream = crate::protocol::causal_grants::AuthorStreamId::from_bytes([19; 32]);
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

#[tokio::test]
async fn merge_retraction_retires_its_circle_bootstrap_coverage_atomically() {
    let database = crate::sync::test_helpers::open_test_db();
    let activation = StoreBatchCommitRef {
        coord: StoreCommitCoord {
            stream_id: crate::protocol::causal_grants::AuthorStreamId::from_bytes([23; 32]),
            sequence: 7,
        },
        commit_hash: ObjectHash::digest(b"Circle bootstrap retraction activation"),
        object: test_object("store-v1/test/circle-bootstrap-retraction/commit.json"),
    };
    let encoded_activation =
        serde_json::to_string(&activation).expect("serialize bootstrap activation");
    database
        .call(move |connection| {
            connection
                .execute(
                    "INSERT INTO circle_bootstrap_coverage
                         (circle_id, control_coord, activation_commit, exact_cut, image_hash,
                          image_bytes, bootstrap_ref)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    rusqlite::params![
                        "00000000-0000-4000-8000-000000000001",
                        "{}",
                        encoded_activation,
                        "{}",
                        ObjectHash::digest(b"Circle bootstrap retraction image").to_string(),
                        b"Circle bootstrap retraction image".as_slice(),
                        b"{}".as_slice(),
                    ],
                )
                .map_err(DbError::from)?;
            let transaction = connection.unchecked_transaction().map_err(DbError::from)?;
            assert_eq!(
                MergeMaterializationTransaction::new(&transaction)
                    .retire_circle_bootstrap_coverage(&activation)?,
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
            transaction.rollback().map_err(DbError::from)?;
            let retained: i64 = connection
                .query_row(
                    "SELECT COUNT(*) FROM circle_bootstrap_coverage",
                    [],
                    |row| row.get(0),
                )
                .map_err(DbError::from)?;
            assert_eq!(retained, 1);
            let transaction = connection.unchecked_transaction().map_err(DbError::from)?;
            assert_eq!(
                MergeMaterializationTransaction::new(&transaction)
                    .retire_circle_bootstrap_coverage(&activation)?,
                1
            );
            transaction.commit().map_err(DbError::from)?;
            let retained: i64 = connection
                .query_row(
                    "SELECT COUNT(*) FROM circle_bootstrap_coverage",
                    [],
                    |row| row.get(0),
                )
                .map_err(DbError::from)?;
            assert_eq!(retained, 0);
            Ok(())
        })
        .await
        .expect("retire retracted Circle bootstrap coverage");
}

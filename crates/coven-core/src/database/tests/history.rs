use super::super::*;

use super::fixtures::*;

async fn assert_store_creation_installs_generation_zero_replay_baseline() {
    let db = crate::sync::test_helpers::open_test_db();
    let store = crate::sync::test_helpers::TestStore::create(
        &db,
        "retained-replay-genesis",
        crate::keys::UserKeypair::generate(),
    )
    .await
    .expect("create Store");
    let baseline = db
        .call(load_generation_zero_replay_baseline_on)
        .await
        .expect("load retained replay baseline")
        .expect("Store creation installs retained replay baseline");

    assert_eq!(baseline.schema_version, db.schema_version());
    assert_eq!(baseline.routing_hash, db.sync_routing_hash());
    match &baseline.authority {
        RetainedReplayAuthority::Genesis(authority) => {
            assert_eq!(authority.store_root, store.root)
        }
        RetainedReplayAuthority::StableSnapshot(_) => {
            panic!("Store creation installed a snapshot replay baseline")
        }
    }
    baseline.validate_image().expect("validate replay image");
}

#[tokio::test]
async fn store_creation_installs_generation_zero_replay_baseline() {
    assert_store_creation_installs_generation_zero_replay_baseline().await;
}

#[test]
fn author_exclusion_locator_skips_a_terminal_whose_own_cut_accepts_the_candidate() {
    let stream = crate::sync::causal_grants::AuthorStreamId::from_bytes([7; 32]);
    let registration = StoreDeviceRegistrationRef {
        device_id: "07".repeat(32).parse().expect("test device id"),
        registration_hash: ObjectHash::digest(b"test registration"),
        object: reclaim_test_object("store-v1/test/registration.json"),
    };
    let exclusion = |label: &str| crate::sync::store_commit::StoreDeviceExclusionRef {
        proposal: crate::sync::store_commit::StoreDeviceExclusionProposalRef {
            proposal_id: crate::sync::store_commit::StoreDeviceExclusionProposalId::from_hash(
                ObjectHash::digest(format!("{label} proposal id").as_bytes()),
            ),
            target: registration.clone(),
            proposal_hash: ObjectHash::digest(format!("{label} proposal").as_bytes()),
            object: reclaim_test_object(&format!("store-v1/test/{label}/proposal.json")),
        },
        outcome_hash: ObjectHash::digest(format!("{label} outcome").as_bytes()),
        object: reclaim_test_object(&format!("store-v1/test/{label}/outcome.json")),
    };
    let high = exclusion("high");
    let low = exclusion("low");
    let commit = |sequence: u64, label: &str| StoreBatchCommitRef {
        coord: StoreCommitCoord {
            stream_id: stream,
            sequence,
        },
        commit_hash: ObjectHash::digest(format!("{label} commit").as_bytes()),
        object: reclaim_test_object(&format!("store-v1/test/{label}/commit.json")),
    };
    let locator = |exclusion, sequence, label: &str| {
        AuthorExclusionActivationLocator::verified(
            exclusion,
            BTreeMap::from([(stream, commit(sequence, label))]),
            commit(sequence + 1, &format!("{label}-activation")),
            crate::sync::store_commit::StoreDeviceHeadRef {
                head_hash: ObjectHash::digest(format!("{label} head").as_bytes()),
                object: reclaim_test_object(&format!("store-v1/test/{label}/head.json")),
            },
        )
    };
    let high_locator = locator(high.clone(), 5, "high");
    let low_locator = locator(low.clone(), 2, "low");
    let terminals = vec![high.clone(), low.clone()];

    let selected =
        select_author_exclusion_activation_locator(&terminals, &stream, 4, |candidate| {
            if candidate == &high {
                Ok(high_locator.clone())
            } else if candidate == &low {
                Ok(low_locator.clone())
            } else {
                Err(DbError::Message("unexpected exclusion".to_string()))
            }
        })
        .expect("select exclusion locator")
        .expect("one terminal excludes the candidate");

    assert_eq!(selected, low_locator);
}

#[test]
fn merge_retraction_requires_the_exact_transitive_dependent_closure() {
    let stream = crate::sync::causal_grants::AuthorStreamId::from_bytes([19; 32]);
    let commit = |sequence: u64, label: &str| StoreBatchCommitRef {
        coord: StoreCommitCoord {
            stream_id: stream,
            sequence,
        },
        commit_hash: ObjectHash::digest(format!("{label} commit").as_bytes()),
        object: reclaim_test_object(&format!("store-v1/test/{label}/commit.json")),
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

    let required =
        Database::complete_merge_retraction_closure(&graph, BTreeSet::from([root.clone()]));

    assert_eq!(
        required,
        BTreeSet::from([root.clone(), child.clone(), grandchild]),
    );
    assert_ne!(required, BTreeSet::from([root.clone(), child.clone()]));
    assert!(!required.contains(&independent));
    assert!(Database::require_exact_merge_retraction_closure(
        &graph,
        BTreeSet::from([root.clone()]),
        &BTreeSet::from([root, child]),
    )
    .is_err());
}

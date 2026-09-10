use super::*;

fn successor(fixture: &Fixture, order: StoreCommitOrder) -> StoreBatchCommit {
    let write_id = WriteId::from_generated("successor".into());
    let coord = StoreCommitCoord {
        stream_id: fixture.commit_ref.coord.stream_id,
        sequence: order.seq(),
    };
    let family = CandidateFamilyId::derive(
        fixture.root_ref.store_root_hash,
        &fixture.registration_ref,
        &write_id,
        &order,
    );
    let package = exact(
        format!(
            "{}.pkg",
            package_semantic_prefix(
                family,
                &coord.stream_id.to_string(),
                coord.sequence(),
                ObjectHash::digest(&fixture.package),
            )
        ),
        &fixture.package,
    );
    let devices = fixture
        .commit
        .device_state
        .with_frontier(CommitFrontier(BTreeMap::from([(
            fixture.commit_ref.coord.stream_id,
            fixture.commit_ref.clone(),
        )])))
        .unwrap();
    StoreBatchCommit::signed_operations(
        fixture.root_ref.store_root_hash,
        write_id,
        coord,
        fixture.registration_ref.clone(),
        &fixture.registration,
        order,
        StorePublicationBase::Genesis,
        fixture.commit.membership_state.clone(),
        devices,
        fixture.commit.operations_membership_authority().unwrap(),
        StoreCommitOperationsInput {
            store_package: Some(StorePackageInput {
                candidate_family: family,
                schema_version: 3,
                bytes: &fixture.package,
                object: package,
            }),
            ..StoreCommitOperationsInput::empty()
        },
        &fixture.registration.device_signer(&fixture.signer).unwrap(),
    )
    .unwrap()
}

fn verify(
    fixture: &Fixture,
    commit: &StoreBatchCommit,
) -> Result<VerifiedStoreBatchCommit, StoreProtocolError> {
    let coord = StoreCommitCoord {
        stream_id: fixture.commit_ref.coord.stream_id,
        sequence: commit.seq(),
    };
    let bytes = commit.to_bytes();
    let object = exact(
        format!(
            "{}.json",
            commit_semantic_prefix(
                commit.candidate_family(),
                &coord.stream_id.to_string(),
                coord.sequence(),
                commit.commit_hash(),
            )
        ),
        &bytes,
    );
    VerifiedStoreBatchCommit::parse_prepared(
        &bytes,
        fixture.root_ref.store_root_hash,
        coord,
        object,
        &fixture.registration,
    )
}

fn successor_order(fixture: &Fixture) -> StoreCommitOrder {
    StoreCommitOrder {
        seq: fixture.commit.seq() + 1,
        predecessor: Some(fixture.commit_ref.clone()),
        dependencies: BTreeMap::new(),
    }
}

#[test]
fn predecessor_cut_accepts_an_identical_same_stream_dependency() {
    let fixture = fixture();
    assert!(fixture
        .commit
        .order
        .predecessor_cut()
        .unwrap()
        .commits()
        .is_empty());
    for duplicate in [false, true] {
        let mut order = successor_order(&fixture);
        if duplicate {
            order.dependencies.insert(
                fixture.commit_ref.coord.stream_id,
                fixture.commit_ref.clone(),
            );
        }
        assert_eq!(
            order.predecessor_cut().unwrap().commits(),
            &BTreeMap::from([(
                fixture.commit_ref.coord.stream_id,
                fixture.commit_ref.clone()
            ),])
        );
        let commit = successor(&fixture, order);
        verify(&fixture, &commit).expect("accept the exact predecessor once");
    }
}

#[test]
fn predecessor_cut_and_commit_parser_report_the_same_conflict() {
    let fixture = fixture();
    let mut commit = successor(&fixture, successor_order(&fixture));
    let mut conflicting = fixture.commit_ref.clone();
    conflicting.commit_hash = ObjectHash::digest(b"another exact predecessor");
    commit
        .body_mut()
        .order
        .dependencies
        .insert(conflicting.coord.stream_id, conflicting);
    fixture.resign(&mut commit);
    for result in [
        commit.order.predecessor_cut().map(|_| ()),
        verify(&fixture, &commit).map(|_| ()),
    ] {
        assert!(
            matches!(result, Err(StoreProtocolError::Malformed(ref reason))
            if reason == "Merge predecessor disagrees with the same-stream dependency"),
            "{result:?}"
        );
    }
}

#[test]
fn commit_parser_requires_the_device_frontier_to_match_its_predecessors() {
    let fixture = fixture();
    let mut commit = successor(&fixture, successor_order(&fixture));
    commit.body_mut().device_state = fixture.commit.device_state.clone();
    fixture.resign(&mut commit);
    let result = verify(&fixture, &commit);
    assert!(
        matches!(result, Err(StoreProtocolError::Malformed(ref reason))
        if reason == "Store device state names a different Merge predecessor cut"),
        "{result:?}"
    );
}

#[test]
fn commit_parser_requires_matching_canonical_recovery_cursors() {
    let fixture = fixture();
    verify(&fixture, &fixture.commit).unwrap();
    let cursor = fixture.commit.membership_state.recovery[0].clone();
    let repeated = vec![cursor.clone(), cursor.clone()];
    for (membership, devices) in [(Vec::new(), vec![cursor]), (repeated.clone(), repeated)] {
        let mut commit = fixture.commit.clone();
        commit.body_mut().membership_state.recovery = membership;
        let mut serialized = serde_json::to_value(&commit.device_state).unwrap();
        serialized["recovery"] = serde_json::to_value(devices).unwrap();
        commit.body_mut().device_state = serde_json::from_value(serialized).unwrap();
        fixture.resign(&mut commit);
        let result = verify(&fixture, &commit);
        assert!(
            matches!(result, Err(StoreProtocolError::OwnerRecoveryMismatch)),
            "{result:?}"
        );
    }
}

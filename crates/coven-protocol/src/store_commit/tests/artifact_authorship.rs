use super::*;

fn assert_author_store<T: std::fmt::Debug>(
    result: Result<T, StoreProtocolError>,
    expected_root: &StoreRootRef,
    author: &Fixture,
) {
    if expected_root == &author.root_ref {
        result.expect("accept an artifact from the expected Store's author");
    } else {
        assert!(
            matches!(
                result,
                Err(StoreProtocolError::StoreRootMismatch { expected, actual })
                    if expected == expected_root.store_root_hash
                        && actual == author.root_ref.store_root_hash
            ),
            "reject another Store's author: {result:?}"
        );
    }
}

#[test]
fn membership_rollup_requires_an_author_from_the_expected_store() {
    let author = fixture();
    let foreign = fixture();
    assert_ne!(
        author.root_ref.store_root_hash,
        foreign.root_ref.store_root_hash
    );
    let signer = author.registration.device_signer(&author.signer).unwrap();
    for expected_root in [&author.root_ref, &foreign.root_ref] {
        let rollup = MembershipRollup::signed(
            expected_root.store_root_hash,
            author.registration_ref.clone(),
            Vec::new(),
            &signer,
        )
        .unwrap();
        let bytes = rollup.to_bytes();
        let reference = MembershipRollupRef {
            rollup_hash: ObjectHash::digest(&bytes),
            object: exact("store-v1/tests/membership-rollup.json".into(), &bytes),
        };
        assert_author_store(
            MembershipRollup::parse_at(
                &bytes,
                expected_root.store_root_hash,
                &reference,
                &author.registration,
            ),
            expected_root,
            &author,
        );
    }
}

#[test]
fn circle_ack_requires_an_author_from_the_expected_store() {
    let author = fixture();
    let foreign = fixture();
    assert_ne!(
        author.root_ref.store_root_hash,
        foreign.root_ref.store_root_hash
    );
    let signer = author.registration.device_signer(&author.signer).unwrap();
    let circle_id = CircleId::from_bytes([1; 16]);
    let control = author.circle_control_coord(ObjectHash::digest(b"Circle control"));
    let ids = coven_foundation::id_provider::SequentialIdProvider::new("Circle ack author");
    let epoch_id = CircleEpochId::generate(&ids);
    for expected_root in [&author.root_ref, &foreign.root_ref] {
        let activation = StreamActivation::device_authorized(
            expected_root.store_root_hash,
            author.registration_ref.clone(),
            DeviceStreamAnchor::CircleAcknowledgements {
                circle_id,
                first_slot: slot(format!(
                    "{}.json",
                    circle_ack_slot_prefix(
                        circle_id,
                        &author.registration.device_id.to_string(),
                        1
                    )
                )),
            },
        );
        let ack = CircleAck::signed(
            expected_root.store_root_hash,
            circle_id,
            author.registration_ref.clone(),
            1,
            CommitFrontier(BTreeMap::new()),
            control.clone(),
            epoch_id,
            KeyFingerprint::from_bytes([7; 32]),
            None,
            "2026-09-10T00:00:00Z".into(),
            SuccessorLink {
                activation: activation.activation_id(),
                predecessor: None,
                next_slot: slot(format!(
                    "{}.json",
                    circle_ack_slot_prefix(
                        circle_id,
                        &author.registration.device_id.to_string(),
                        2
                    )
                )),
            },
            &signer,
        )
        .unwrap();
        let bytes = ack.to_bytes();
        let reference = CircleAckRef {
            registration: author.registration_ref.clone(),
            circle_id,
            control: control.clone(),
            sequence: 1,
            ack_hash: ack.ack_hash(),
            object: ExactObjectRef::new(
                activation.first_slot().clone(),
                bytes.len() as u64,
                ObjectHash::digest(&bytes),
            ),
        };
        assert_author_store(
            CircleAck::parse_at(&bytes, expected_root, &reference, &author.registration),
            expected_root,
            &author,
        );
    }
}

#[test]
fn circle_snapshot_requires_an_author_from_the_expected_store() {
    let author = fixture();
    let foreign = fixture();
    assert_ne!(
        author.root_ref.store_root_hash,
        foreign.root_ref.store_root_hash
    );
    let signer = author.registration.device_signer(&author.signer).unwrap();
    let circle_id = CircleId::from_bytes([1; 16]);
    let ids = coven_foundation::id_provider::SequentialIdProvider::new("Circle snapshot author");
    let epoch_id = CircleEpochId::generate(&ids);
    for expected_root in [&author.root_ref, &foreign.root_ref] {
        let snapshot = CircleSnapshotMeta::signed(
            expected_root.store_root_hash,
            circle_id,
            author.registration_ref.clone(),
            author.circle_control_coord(ObjectHash::digest(b"Circle control")),
            epoch_id,
            KeyFingerprint::from_bytes([7; 32]),
            0,
            CircleBootstrapRef {
                coverage: CommitFrontier(BTreeMap::new()),
                schema_version: author.root.descriptor.schema_version,
                sync_routing_hash: author.root.descriptor.sync_routing_hash,
                image: SnapshotImageRef {
                    image_hash: ObjectHash::digest(b"Circle image"),
                    object: exact("store-v1/tests/circle-image.db".into(), b"Circle image"),
                },
                blobs: Vec::new(),
            },
            "2026-09-10T00:00:00Z".into(),
            CircleSnapshotSuccessorLink {
                predecessor: None,
                next_slot: slot(format!(
                    "{}.json",
                    circle_snapshot_slot_prefix(
                        circle_id,
                        &author.registration.device_id.to_string(),
                        1
                    )
                )),
            },
            &signer,
        )
        .unwrap();
        let bytes = snapshot.to_bytes();
        let reference = CircleSnapshotRef {
            generation: 0,
            snapshot_hash: snapshot.snapshot_hash(),
            object: exact(
                format!(
                    "{}.json",
                    circle_snapshot_slot_prefix(
                        circle_id,
                        &author.registration.device_id.to_string(),
                        0
                    )
                ),
                &bytes,
            ),
        };
        assert_author_store(
            CircleSnapshotMeta::parse_at(
                &bytes,
                expected_root.store_root_hash,
                &reference,
                &author.registration,
            ),
            expected_root,
            &author,
        );
    }
}

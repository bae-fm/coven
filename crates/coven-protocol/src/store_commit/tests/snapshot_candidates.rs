use super::*;

fn metadata(
    fixture: &Fixture,
    previous: StoreCurrentPublicationRecord,
    metadata_slot: &ObjectSlot,
) -> SnapshotMeta {
    let signer = fixture.registration.device_signer(&fixture.signer).unwrap();
    let membership = fixture.commit.membership_state.clone();
    let rollup = MembershipRollup::signed(
        fixture.root_ref.store_root_hash,
        fixture.registration_ref.clone(),
        Vec::new(),
        &signer,
    )
    .expect("sign membership rollup");
    let rollup_bytes = rollup.to_bytes();
    let image_bytes = b"snapshot image contents bound by metadata";
    SnapshotMeta::signed(
        fixture.root_ref.store_root_hash,
        fixture.registration_ref.clone(),
        previous,
        SnapshotImageRef {
            image_hash: ObjectHash::digest(image_bytes),
            object: exact(
                format!(
                    "{}.db",
                    snapshot_image_semantic_prefix(metadata_slot, ObjectHash::digest(image_bytes),)
                ),
                image_bytes,
            ),
        },
        MembershipRollupRef {
            rollup_hash: ObjectHash::digest(&rollup_bytes),
            object: exact(
                format!(
                    "{}.json",
                    membership_rollup_semantic_prefix(
                        metadata_slot,
                        ObjectHash::digest(&rollup_bytes),
                    )
                ),
                &rollup_bytes,
            ),
        },
        fixture.commit.device_state.frontier().clone(),
        StoreSnapshotState {
            membership: membership.clone(),
            devices: ResolvedStoreDeviceState::founder(
                &fixture.root_ref,
                fixture.registration_ref.clone(),
                &fixture.root.descriptor.founder_pubkey,
                fixture.root.descriptor.founder_grant.clone(),
                &fixture.root.descriptor.founder_recovery,
            )
            .expect("resolve the snapshot founder state"),
        },
        RetainedVerifiedMergeHistorySummary {
            reclaim: RetainedReclaimState::genesis(),
            version: STORE_PROTOCOL_VERSION,
            store_root_hash: fixture.root_ref.store_root_hash,
            causal_cut: BTreeMap::new(),
            last_non_acknowledgement_commits: BTreeMap::new(),
            post_state: fixture.commit.device_state.clone(),
            membership_floor: MembershipCausalFloor {
                effective_coordinates: membership
                    .heads
                    .iter()
                    .map(|head| head.coord.clone())
                    .collect(),
            },
            registrations: BTreeMap::from([(
                fixture.registration.device_id,
                ReferencedStoreDeviceRegistration::verified(
                    fixture.registration_ref.clone(),
                    fixture.registration.clone(),
                )
                .expect("reference founder registration"),
            )]),
            acknowledgements: BTreeMap::new(),
            membership_proofs: BTreeMap::new(),
            pending_owner_promotions: BTreeMap::new(),
            pending_device_joins: BTreeMap::new(),
        },
        fixture.root.descriptor.schema_version,
        "2026-09-08T00:00:00Z".to_string(),
        &signer,
    )
    .expect("sign snapshot metadata")
}

fn reference(metadata: &SnapshotMeta, slot: ObjectSlot) -> StoreSnapshotRef {
    let bytes = metadata.to_bytes();
    StoreSnapshotRef {
        snapshot_hash: metadata.snapshot_hash(),
        object: ExactObjectRef::new(slot, bytes.len() as u64, ObjectHash::digest(&bytes)),
    }
}

fn candidate_slot(fixture: &Fixture, candidate: &str) -> ObjectSlot {
    ObjectSlot::opaque(
        format!(
            "{}.json",
            snapshot_candidate_semantic_prefix(
                &fixture.registration.device_id.to_string(),
                candidate
            )
        ),
        format!("provider-{candidate}"),
    )
    .expect("allocate metadata candidate slot")
}

#[test]
fn store_snapshot_candidates_follow_shared_publication_without_a_device_chain() {
    let fixture = fixture();
    let signer = fixture.registration.device_signer(&fixture.signer).unwrap();
    let mut current =
        StoreCurrentPublicationRecord::genesis(fixture.root_ref.store_root_hash, &fixture.signer);
    let mut slots = BTreeSet::new();
    for candidate in ["checkpoint-alpha", "checkpoint-omega"] {
        let slot = candidate_slot(&fixture, candidate);
        let metadata = metadata(&fixture, current.clone(), &slot);
        let reference = reference(&metadata, slot);
        let parsed = SnapshotMeta::parse_at(
            &metadata.to_bytes(),
            fixture.root_ref.store_root_hash,
            &reference,
            &fixture.registration,
        )
        .expect("verify independently allocated candidate");
        assert_eq!(parsed.publication_predecessor, current);
        assert!(slots.insert(reference.object.slot().clone()));
        let entry = StorePublicationEntry::signed_snapshot(
            &current,
            fixture.registration_ref.clone(),
            reference.clone(),
            &signer,
        )
        .expect("sign shared snapshot publication");
        let entry_ref = StorePublicationRef::from_entry(
            &entry,
            exact(
                format!("{}.json", store_publication_entry_semantic_prefix(&entry)),
                &entry.to_bytes(),
            ),
        )
        .expect("reference shared publication");
        current =
            StoreCurrentPublicationRecord::advance_snapshot(&current, &entry, entry_ref, &signer)
                .expect("accept exact snapshot candidate");
        assert_eq!(&current.latest_snapshot().unwrap().snapshot, &reference);
        let encoded = serde_json::to_value(&metadata).unwrap();
        for obsolete in ["generation", "predecessor", "successor"] {
            assert!(encoded["body"].get(obsolete).is_none());
        }
        assert!(serde_json::to_value(&reference)
            .unwrap()
            .get("generation")
            .is_none());
    }
    assert!(serde_json::to_value(&fixture.registration).unwrap()["body"]
        .get("snapshots")
        .is_none());
}

#[test]
fn snapshot_artifacts_are_bound_to_the_full_metadata_slot() {
    let fixture = fixture();
    let slot = candidate_slot(&fixture, "exact-owner");
    let metadata = metadata(
        &fixture,
        StoreCurrentPublicationRecord::genesis(fixture.root_ref.store_root_hash, &fixture.signer),
        &slot,
    );
    let original = reference(&metadata, slot.clone());
    SnapshotMeta::parse_at(
        &metadata.to_bytes(),
        fixture.root_ref.store_root_hash,
        &original,
        &fixture.registration,
    )
    .expect("original artifact owner");
    for other in [
        candidate_slot(&fixture, "other-owner"),
        ObjectSlot::opaque(
            slot.logical_key().into(),
            "another-physical-metadata-object".into(),
        )
        .expect("same logical key, distinct provider identity"),
    ] {
        let relocated = reference(&metadata, other);
        assert!(
            matches!(
                SnapshotMeta::parse_at(
                    &metadata.to_bytes(),
                    fixture.root_ref.store_root_hash,
                    &relocated,
                    &fixture.registration
                ),
                Err(StoreProtocolError::RelocatedSlot { .. })
            ),
            "candidate cannot acquire another exact snapshot's artifacts"
        );
    }
}

#[test]
fn store_snapshot_candidate_rejects_other_author_and_unsafe_candidate_paths() {
    let fixture = fixture();
    let metadata = metadata(
        &fixture,
        StoreCurrentPublicationRecord::genesis(fixture.root_ref.store_root_hash, &fixture.signer),
        &candidate_slot(&fixture, "exact"),
    );
    for candidate in [
        "",
        ".",
        "..",
        "nested/candidate",
        "back\\slash",
        "drive:relative",
        "nul\0byte",
    ] {
        let reference = reference(&metadata, candidate_slot(&fixture, candidate));
        assert!(
            matches!(
                SnapshotMeta::parse_at(&metadata.to_bytes(), fixture.root_ref.store_root_hash, &reference, &fixture.registration),
                Err(StoreProtocolError::Malformed(message)) if message.contains("invalid Store snapshot candidate")
            ),
            "unsafe candidate {candidate:?}"
        );
    }
    let reference = reference(
        &metadata,
        slot("store-v1/snapshots/another-author/candidate.json".to_string()),
    );
    assert!(matches!(
        SnapshotMeta::parse_at(&metadata.to_bytes(), fixture.root_ref.store_root_hash, &reference, &fixture.registration),
        Err(StoreProtocolError::Malformed(message)) if message.contains("outside its author's metadata path")
    ));
}

#[test]
fn store_snapshot_candidate_rejects_inexact_bytes_hash_and_signature() {
    let fixture = fixture();
    let metadata = metadata(
        &fixture,
        StoreCurrentPublicationRecord::genesis(fixture.root_ref.store_root_hash, &fixture.signer),
        &candidate_slot(&fixture, "exact"),
    );
    let exact_reference = reference(&metadata, candidate_slot(&fixture, "exact"));
    let mut wrong_bytes = exact_reference.clone();
    wrong_bytes.object = exact(
        wrong_bytes.object.slot().logical_key().to_string(),
        b"substituted metadata",
    );
    assert!(matches!(
        SnapshotMeta::parse_at(
            &metadata.to_bytes(),
            fixture.root_ref.store_root_hash,
            &wrong_bytes,
            &fixture.registration
        ),
        Err(StoreProtocolError::Storage(_))
    ));
    let mut wrong_hash = exact_reference;
    wrong_hash.snapshot_hash = ObjectHash::digest(b"another signed metadata hash");
    assert!(matches!(
        SnapshotMeta::parse_at(
            &metadata.to_bytes(),
            fixture.root_ref.store_root_hash,
            &wrong_hash,
            &fixture.registration
        ),
        Err(StoreProtocolError::ObjectHashMismatch { .. })
    ));
    let mut unsigned = metadata;
    unsigned.corrupt_signature_for_test();
    let unsigned_reference = reference(&unsigned, candidate_slot(&fixture, "unsigned"));
    assert!(matches!(
        SnapshotMeta::parse_at(
            &unsigned.to_bytes(),
            fixture.root_ref.store_root_hash,
            &unsigned_reference,
            &fixture.registration
        ),
        Err(StoreProtocolError::InvalidSignature)
    ));
}

#[test]
fn store_snapshot_candidate_rejects_a_signed_foreign_publication_boundary() {
    let fixture = fixture();
    let mut metadata = metadata(
        &fixture,
        StoreCurrentPublicationRecord::genesis(fixture.root_ref.store_root_hash, &fixture.signer),
        &candidate_slot(&fixture, "exact"),
    );
    metadata.body_mut().publication_predecessor = StoreCurrentPublicationRecord::genesis(
        ObjectHash::digest(b"foreign Store"),
        &fixture.signer,
    );
    metadata.resign(&fixture.registration.device_signer(&fixture.signer).unwrap());
    let reference = reference(&metadata, candidate_slot(&fixture, "foreign-boundary"));
    assert!(matches!(
        SnapshotMeta::parse_at(
            &metadata.to_bytes(),
            fixture.root_ref.store_root_hash,
            &reference,
            &fixture.registration
        ),
        Err(StoreProtocolError::StoreRootMismatch { .. })
    ));
}

#[test]
fn snapshot_retirement_cannot_name_another_protocol_domain_as_metadata() {
    let fixture = fixture();
    let signer = fixture.registration.device_signer(&fixture.signer).unwrap();
    let genesis =
        StoreCurrentPublicationRecord::genesis(fixture.root_ref.store_root_hash, &fixture.signer);
    let unsafe_slot = ObjectSlot::logical(store_current_publication_logical_key().into()).unwrap();
    let old = metadata(&fixture, genesis.clone(), &unsafe_slot);
    let old_ref = reference(&old, unsafe_slot);
    let entry = StorePublicationEntry::signed_snapshot(
        &genesis,
        fixture.registration_ref.clone(),
        old_ref.clone(),
        &signer,
    )
    .unwrap();
    let publication = StorePublicationRef::from_entry(
        &entry,
        exact(
            format!("{}.json", store_publication_entry_semantic_prefix(&entry)),
            &entry.to_bytes(),
        ),
    )
    .unwrap();
    let predecessor = StoreCurrentPublicationRecord::advance_snapshot(
        &genesis,
        &entry,
        publication.clone(),
        &signer,
    )
    .unwrap();
    let slot = candidate_slot(&fixture, "retirement-boundary");
    let mut candidate = metadata(&fixture, predecessor, &slot);
    candidate
        .body_mut()
        .history_summary
        .reclaim
        .snapshots
        .insert(
            old_ref.snapshot_hash,
            RetainedStoreSnapshotOwnership {
                accepted: AcceptedStoreSnapshotRef {
                    snapshot: old_ref,
                    publication,
                },
                image: old.image.clone(),
                rollup: old.membership_rollup.clone(),
            },
        );
    candidate.resign(&signer);
    let candidate_ref = reference(&candidate, slot);
    assert!(
        SnapshotMeta::parse_at(
            &candidate.to_bytes(),
            fixture.root_ref.store_root_hash,
            &candidate_ref,
            &fixture.registration,
        )
        .is_err(),
        "an accepted artifact inventory cannot turn the current publication pointer into snapshot metadata"
    );
}

#[test]
fn snapshot_acknowledgement_summary_rejects_mismatched_exact_commit_bounds() {
    let fixture = fixture();
    let mut summary = metadata(
        &fixture,
        StoreCurrentPublicationRecord::genesis(fixture.root_ref.store_root_hash, &fixture.signer),
        &candidate_slot(&fixture, "acknowledgement-summary"),
    )
    .history_summary
    .clone();
    let reference = &fixture.commit_ref;
    summary
        .causal_cut
        .insert(reference.coord.clone(), reference.clone());
    summary.post_state = summary
        .post_state
        .with_frontier(CommitFrontier(BTreeMap::from([(
            reference.coord.stream_id,
            reference.clone(),
        )])))
        .unwrap();
    summary
        .last_non_acknowledgement_commits
        .insert(reference.coord.stream_id, reference.clone());
    summary.validate_snapshot_baseline().unwrap();

    let mut different_hash = reference.clone();
    different_hash.commit_hash = ObjectHash::digest(b"another publication at the same coordinate");
    let mut future = reference.clone();
    future.coord.sequence += 1;
    let foreign_stream = AuthorStreamId::from_digest(ObjectHash::digest(b"foreign author stream"));
    let mut foreign = reference.clone();
    foreign.coord.stream_id = foreign_stream;
    for (stream, invalid) in [
        (reference.coord.stream_id, different_hash),
        (reference.coord.stream_id, future),
        (foreign_stream, reference.clone()),
        (foreign_stream, foreign),
    ] {
        let mut invalid_summary = summary.clone();
        invalid_summary.last_non_acknowledgement_commits = BTreeMap::from([(stream, invalid)]);
        assert!(invalid_summary.validate_snapshot_baseline().is_err());
    }
}

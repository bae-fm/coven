use super::*;
use crate::objects::ObjectSlot;
use crate::store_commit::StoreCommitCoord;

fn proof_object(path: &str) -> ExactObjectRef {
    let bytes = path.as_bytes();
    ExactObjectRef::new(
        ObjectSlot::logical(path.to_string()).expect("valid proof slot"),
        u64::try_from(bytes.len()).expect("proof length fits u64"),
        ObjectHash::digest(bytes),
    )
}

fn frontier_at(stream: &str, sequence: u64) -> crate::store_commit::CommitFrontier {
    let stream_id =
        crate::causal_grants::AuthorStreamId::from_digest(ObjectHash::digest(stream.as_bytes()));
    let commit = crate::store_commit::StoreBatchCommitRef {
        coord: StoreCommitCoord {
            stream_id,
            sequence,
        },
        commit_hash: ObjectHash::digest(format!("{stream}:{sequence}").as_bytes()),
        object: proof_object(&format!(
            "store-v1/candidates/f/commits/{stream}/{sequence}/hash"
        )),
    };
    crate::store_commit::CommitFrontier(std::collections::BTreeMap::from([(stream_id, commit)]))
}

/// A deterministic device registration reference for claim-shape tests.
fn claim_registration(label: &str) -> crate::store_commit::StoreDeviceRegistrationRef {
    crate::store_commit::StoreDeviceRegistrationRef {
        device_id: ObjectHash::digest(label.as_bytes())
            .to_string()
            .parse()
            .expect("a digest is a valid device id"),
        registration_hash: ObjectHash::digest(format!("{label}:registration").as_bytes()),
        object: proof_object(&format!("store-v1/devices/{label}/registration.json")),
    }
}

/// A deterministic Circle control coordinate for claim-shape tests.
fn claim_control(label: &str, seq: u64) -> CircleControlCoord {
    CircleControlCoord {
        device_id: label.to_string(),
        stream_id: crate::causal_grants::AuthorStreamId::from_digest(ObjectHash::digest(
            label.as_bytes(),
        )),
        author_pubkey: format!("{label}-pubkey"),
        author_owner_grant: crate::causal_grants::MembershipGrantId::from_test_label(label),
        seq,
        control_hash: ObjectHash::digest(format!("{label}:{seq}").as_bytes()),
    }
}

fn claim_image(label: &str) -> crate::store_commit::SnapshotImageRef {
    crate::store_commit::SnapshotImageRef {
        image_hash: ObjectHash::digest(format!("{label}:image").as_bytes()),
        object: proof_object(&format!("circles/{label}/snapshot-images/{label}/image.db")),
    }
}

fn claim_ack(circle_id: CircleId, label: &str) -> CircleAckRef {
    CircleAckRef {
        registration: claim_registration(label),
        circle_id,
        control: claim_control(label, 1),
        sequence: 1,
        ack_hash: ObjectHash::digest(format!("{label}:ack").as_bytes()),
        object: proof_object(&format!("circles/{circle_id}/acks/{label}/1.json")),
    }
}

fn claim_coverage(circle_id: CircleId, label: &str) -> CircleBootstrapCoverageRef {
    CircleBootstrapCoverageRef {
        circle_id,
        control: claim_control(label, 1),
        activation_commit: frontier_at(label, 4)
            .0
            .values()
            .next()
            .expect("single-stream frontier names a commit")
            .clone(),
        bootstrap: crate::circle::CircleBootstrapRef {
            coverage: frontier_at(label, 4),
            schema_version: 1,
            sync_routing_hash: ObjectHash::digest(b"routing"),
            image: claim_image(label),
            blobs: Vec::new(),
        },
    }
}

/// A Circle bootstrap reclaim claim must be refused when its proof acknowledgement
/// belongs to a different Circle than the coverage it authorizes deleting. Reclaim
/// evidence is Store-visible, so this shape has to be caught on the claim itself
/// rather than only where a Circle member can read the acknowledgement: without
/// the check, an acknowledgement from a Circle the Owner does hold access to would
/// travel as evidence over another Circle's image.
#[test]
fn circle_bootstrap_reclaim_refuses_an_acknowledgement_from_another_circle() {
    let target = CircleId::from_bytes([1; 16]);
    let other = CircleId::from_bytes([2; 16]);
    let claim = |acknowledged: CircleId| {
        ReclaimClaim::CircleBootstrapImage(CircleBootstrapImageReclaimClaim {
            target: CircleBootstrapImageReclaimTarget {
                coverage: claim_coverage(target, "recipient"),
            },
            proof: CircleBootstrapReclaimProof::RecipientCoverage {
                acknowledgement: claim_ack(acknowledged, "recipient"),
            },
        })
    };
    claim(target)
        .validate()
        .expect("an acknowledgement from the target Circle is well-formed");
    let error = claim(other)
        .validate()
        .expect_err("an acknowledgement from another Circle is refused");
    assert!(
        error
            .to_string()
            .contains("acknowledgement names another Circle"),
        "{error}"
    );
}

/// Every reclaim target names the object it deletes and, separately, the signed
/// statement that authorizes deleting it. A claim whose target IS its own
/// authority would have the Owner delete the evidence its verification rests on,
/// so each kind refuses that aliasing on the claim itself.
#[test]
fn reclaim_claims_refuse_a_target_that_aliases_its_own_authority() {
    let circle_id = CircleId::from_bytes([3; 16]);

    let mut coverage = claim_coverage(circle_id, "aliased");
    coverage.bootstrap.image.object = coverage.activation_commit.object.clone();
    let error = ReclaimClaim::CircleBootstrapImage(CircleBootstrapImageReclaimClaim {
        target: CircleBootstrapImageReclaimTarget { coverage },
        proof: CircleBootstrapReclaimProof::RecipientCoverage {
            acknowledgement: claim_ack(circle_id, "aliased"),
        },
    })
    .validate()
    .expect_err("a bootstrap image that is its own activation commit is refused");
    assert!(
        error.to_string().contains("aliases proof authority"),
        "{error}"
    );

    let snapshot = CircleSnapshotRef {
        generation: 0,
        snapshot_hash: ObjectHash::digest(b"generation zero"),
        object: proof_object("circles/aliased/snapshots/device/0.json"),
    };
    let error = ReclaimClaim::CircleSnapshotImage(CircleSnapshotImageReclaimClaim {
        target: CircleSnapshotImageReclaimTarget {
            circle_id,
            snapshot_author: claim_registration("aliased"),
            control: claim_control("aliased", 1),
            snapshot: snapshot.clone(),
            image: crate::store_commit::SnapshotImageRef {
                image_hash: ObjectHash::digest(b"aliased image"),
                object: snapshot.object.clone(),
            },
        },
        superseding: CircleSnapshotRef {
            generation: 1,
            snapshot_hash: ObjectHash::digest(b"generation one"),
            object: proof_object("circles/aliased/snapshots/device/1.json"),
        },
    })
    .validate()
    .expect_err("a snapshot image that is its own metadata object is refused");
    assert!(
        error.to_string().contains("aliases proof authority"),
        "{error}"
    );
}

/// A superseded snapshot generation is only superseded by a LATER one. A claim
/// naming an equal or earlier generation as its superseding evidence is refused on
/// the claim, before any stream walk: the generation ordering is part of what the
/// claim asserts, and nothing later in verification re-establishes it if the
/// claim's own numbering is inverted.
#[test]
fn circle_snapshot_reclaim_refuses_a_superseding_generation_that_is_not_later() {
    let circle_id = CircleId::from_bytes([4; 16]);
    let claim = |target_generation: u64, superseding_generation: u64| {
        ReclaimClaim::CircleSnapshotImage(CircleSnapshotImageReclaimClaim {
            target: CircleSnapshotImageReclaimTarget {
                circle_id,
                snapshot_author: claim_registration("ordering"),
                control: claim_control("ordering", 1),
                snapshot: CircleSnapshotRef {
                    generation: target_generation,
                    snapshot_hash: ObjectHash::digest(b"target generation"),
                    object: proof_object("circles/ordering/snapshots/device/target.json"),
                },
                image: claim_image("ordering"),
            },
            superseding: CircleSnapshotRef {
                generation: superseding_generation,
                snapshot_hash: ObjectHash::digest(b"superseding generation"),
                object: proof_object("circles/ordering/snapshots/device/superseding.json"),
            },
        })
    };
    claim(0, 1)
        .validate()
        .expect("a strictly later generation is well-formed");
    for (target_generation, superseding_generation) in [(1, 1), (2, 1)] {
        let error = claim(target_generation, superseding_generation)
            .validate()
            .expect_err("a superseding generation that is not later is refused");
        assert!(
            error
                .to_string()
                .contains("superseding generation that is not later"),
            "{error}"
        );
    }
}

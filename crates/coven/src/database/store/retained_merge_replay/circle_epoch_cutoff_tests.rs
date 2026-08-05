use super::*;
use crate::id_provider::SequentialIdProvider;
use crate::protocol::causal_grants::AuthorStreamId;
use crate::protocol::circle::{CircleControlCoord, CircleEpochId, CircleId};
use crate::protocol::membership::MembershipGrantId;
use crate::protocol::objects::ObjectSlot;

fn commit_reference(stream_id: AuthorStreamId, sequence: u64, label: &str) -> StoreBatchCommitRef {
    let bytes = format!("{label}-stored");
    StoreBatchCommitRef {
        coord: StoreCommitCoord {
            stream_id,
            sequence,
        },
        commit_hash: ObjectHash::digest(format!("{label}-semantic").as_bytes()),
        object: crate::protocol::objects::ExactObjectRef::new(
            ObjectSlot::logical(format!("store-v1/commits/{label}.json"))
                .expect("valid commit slot"),
            bytes.len() as u64,
            ObjectHash::digest(bytes.as_bytes()),
        ),
    }
}

#[test]
fn circle_epoch_cutoff_accepts_exact_history_and_omits_later_packages() {
    let stream_id = AuthorStreamId::from_digest(ObjectHash::digest(b"cutoff stream"));
    let accepted = commit_reference(stream_id, 2, "accepted");
    let later = commit_reference(stream_id, 3, "later");
    let circle_id = CircleId::from_bytes([7; 16]);
    let control = CircleControlCoord {
        device_id: "cutoff-device".to_string(),
        stream_id: AuthorStreamId::from_digest(ObjectHash::digest(b"control stream")),
        author_pubkey: "cutoff-author".to_string(),
        author_owner_grant: MembershipGrantId(ObjectHash::digest(b"owner grant")),
        seq: 1,
        control_hash: ObjectHash::digest(b"control"),
    };
    let epoch_id = CircleEpochId::generate(&SequentialIdProvider::new("cutoff-epoch"));
    let index = CircleReplayEpochIndex {
        control_epochs: BTreeMap::from([((circle_id, control.clone()), epoch_id)]),
        cutoffs: BTreeMap::from([(
            (circle_id, epoch_id),
            CommitFrontier(BTreeMap::from([(stream_id, accepted.clone())])),
        )]),
    };

    assert!(index
        .permits(&accepted, circle_id, &control)
        .expect("accepted commit is valid"));
    assert!(!index
        .permits(&later, circle_id, &control)
        .expect("later commit is excluded"));
}

#[test]
fn circle_epoch_cutoff_rejects_another_commit_at_the_accepted_coordinate() {
    let stream_id = AuthorStreamId::from_digest(ObjectHash::digest(b"collision stream"));
    let accepted = commit_reference(stream_id, 2, "accepted-coordinate");
    let collision = commit_reference(stream_id, 2, "conflicting-coordinate");
    let circle_id = CircleId::from_bytes([8; 16]);
    let control = CircleControlCoord {
        device_id: "collision-device".to_string(),
        stream_id: AuthorStreamId::from_digest(ObjectHash::digest(b"collision control")),
        author_pubkey: "collision-author".to_string(),
        author_owner_grant: MembershipGrantId(ObjectHash::digest(b"collision owner grant")),
        seq: 1,
        control_hash: ObjectHash::digest(b"collision control hash"),
    };
    let epoch_id = CircleEpochId::generate(&SequentialIdProvider::new("collision-epoch"));
    let index = CircleReplayEpochIndex {
        control_epochs: BTreeMap::from([((circle_id, control.clone()), epoch_id)]),
        cutoffs: BTreeMap::from([(
            (circle_id, epoch_id),
            CommitFrontier(BTreeMap::from([(stream_id, accepted)])),
        )]),
    };

    let error = index
        .permits(&collision, circle_id, &control)
        .expect_err("same coordinate with different exact commit must fail");
    assert!(
        error
            .to_string()
            .contains("conflicts with its accepted epoch cutoff"),
        "{error}"
    );
}

use super::*;
use crate::protocol::store_commit::StoreBatchCommitDeletionTarget;

#[tokio::test]
async fn merge_nonactivation_requires_exact_candidate_and_winner_bindings() {
    let fixture = PreparedWriteFixture::prepare().await;
    let batch = fixture.prepared_write().await;
    assert!(!fixture.contains_exact_object(&batch.commit.object));
    fixture.publish_competing_merge_head().await;
    let observation = match fixture
        .observe_excluded_candidate_head(&batch)
        .await
        .expect("observe occupied Merge winner")
    {
        ExcludedCandidateHeadObservation::MergeWinner(observation) => observation,
        ExcludedCandidateHeadObservation::AuthorExclusion => {
            panic!("competing Merge head was classified as author exclusion")
        }
    };
    let author = fixture.candidate_author(&batch).await;
    let candidate = StoreBatchCommitDeletionTarget {
        coord: batch.head.value.commit.coord.clone(),
        object: batch.head.value.commit.object.clone(),
        canonical_signed_bytes: batch.commit.value.to_bytes(),
    };
    observation
        .verified_nonactivation(candidate.clone(), &author)
        .expect("exact losing candidate is verified");

    let mut wrong_head_commit = observation.clone();
    wrong_head_commit.winner_mut_for_test().commit = batch.head.value.commit.clone();
    assert!(wrong_head_commit
        .verified_nonactivation(candidate.clone(), &author,)
        .is_err());

    let winner_target = StoreBatchCommitDeletionTarget {
        coord: observation.winner().commit.coord.clone(),
        object: observation.winner().commit.object.clone(),
        canonical_signed_bytes: observation.winner_commit().to_bytes(),
    };
    assert!(observation
        .verified_nonactivation(winner_target, &author,)
        .is_err());

    let mut wrong_slot = observation.clone();
    wrong_slot.set_expected_slot_for_test(
        crate::storage::cloud::ObjectSlot::logical("store-v1/heads/wrong-slot.json".to_string())
            .expect("valid wrong slot"),
    );
    assert!(wrong_slot
        .verified_nonactivation(candidate.clone(), &author,)
        .is_err());
    let mut wrong_competition_point = candidate.clone();
    let StoreCommitCoord {
        stream_id,
        sequence,
    } = wrong_competition_point.coord;
    wrong_competition_point.coord = StoreCommitCoord {
        stream_id,
        sequence: sequence
            .checked_add(1)
            .expect("test sequence has a successor"),
    };
    assert!(observation
        .verified_nonactivation(wrong_competition_point, &author,)
        .is_err());

    let exact_target = |commit: StoreBatchCommit| {
        let bytes = commit.to_bytes();
        StoreBatchCommitDeletionTarget {
            coord: candidate.coord.clone(),
            object: crate::storage::ExactObjectRef::new(
                candidate.object.slot().clone(),
                bytes.len() as u64,
                ObjectHash::digest(&bytes),
            ),
            canonical_signed_bytes: bytes,
        }
    };
    let mut wrong_root = batch.commit.value.value().clone();
    wrong_root.store_root_hash = ObjectHash::digest(b"wrong Store root");
    assert!(observation
        .verified_nonactivation(exact_target(wrong_root), &author,)
        .is_err());
    let mut wrong_predecessor = batch.commit.value.value().clone();
    let predecessor = &mut wrong_predecessor.order.predecessor;
    *predecessor = match predecessor.take() {
        Some(_) => None,
        None => Some(observation.winner().commit.clone()),
    };
    assert!(observation
        .verified_nonactivation(exact_target(wrong_predecessor), &author,)
        .is_err());
    let mut wrong_author = batch.commit.value.value().clone();
    wrong_author.author_registration = observation.winner().author_registration.clone();
    wrong_author.author_registration.registration_hash =
        ObjectHash::digest(b"wrong author registration");
    assert!(observation
        .verified_nonactivation(exact_target(wrong_author), &author,)
        .is_err());
    let mut unsigned = batch.commit.value.value().clone();
    unsigned.signature = "00".to_string();
    assert!(observation
        .verified_nonactivation(exact_target(unsigned), &author,)
        .is_err());
    let mut noncanonical = candidate;
    noncanonical.canonical_signed_bytes.push(b' ');
    noncanonical.object = crate::storage::ExactObjectRef::new(
        noncanonical.object.slot().clone(),
        noncanonical.canonical_signed_bytes.len() as u64,
        ObjectHash::digest(&noncanonical.canonical_signed_bytes),
    );
    assert!(observation
        .verified_nonactivation(noncanonical, &author,)
        .is_err());
}

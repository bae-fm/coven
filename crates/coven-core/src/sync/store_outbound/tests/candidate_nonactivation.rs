use super::*;
use crate::sync::store_commit::StoreBatchCommitDeletionTarget;

#[tokio::test]
async fn merge_nonactivation_requires_exact_candidate_and_winner_bindings() {
    let fixture = prepared_write_fixture().await;
    let batch = fixture
        .db
        .oldest_prepared_store_write()
        .await
        .expect("load prepared Merge write")
        .expect("prepared Merge write exists");
    assert!(!exact_object_exists(&fixture.home, &batch.commit.object));
    publish_competing_merge_head(&fixture).await;
    let observation = match observe_excluded_candidate_head(
        &fixture.db,
        &fixture.storage,
        fixture.root.store_root_hash,
        &batch.head.value,
        &batch.commit.value,
        &batch.head.object,
    )
    .await
    .expect("observe occupied Merge winner")
    {
        ExcludedCandidateHeadObservation::MergeWinner(observation) => observation,
        ExcludedCandidateHeadObservation::AuthorExclusion => {
            panic!("competing Merge head was classified as author exclusion")
        }
    };
    let author = fixture
        .db
        .activated_store_device_registration(batch.commit.value.author_registration.clone())
        .await
        .expect("load candidate author");
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
    let StoreCommitCoord::MergeConcurrent {
        stream_id,
        sequence,
    } = wrong_competition_point.coord
    else {
        unreachable!("Merge fixture candidate")
    };
    wrong_competition_point.coord = StoreCommitCoord::MergeConcurrent {
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
            object: crate::sync::storage::ExactObjectRef::new(
                candidate.object.slot().clone(),
                bytes.len() as u64,
                ObjectHash::digest(&bytes),
            ),
            canonical_signed_bytes: bytes,
        }
    };
    let mut wrong_root = batch.commit.value.clone();
    wrong_root.store_root_hash = ObjectHash::digest(b"wrong Store root");
    assert!(observation
        .verified_nonactivation(exact_target(wrong_root), &author,)
        .is_err());
    let mut wrong_predecessor = batch.commit.value.clone();
    let StoreCommitOrder::MergeConcurrent { predecessor, .. } = &mut wrong_predecessor.order else {
        unreachable!("Merge fixture commit")
    };
    *predecessor = match predecessor.take() {
        Some(_) => None,
        None => Some(observation.winner().commit.clone()),
    };
    assert!(observation
        .verified_nonactivation(exact_target(wrong_predecessor), &author,)
        .is_err());
    let mut wrong_author = batch.commit.value.clone();
    wrong_author.author_registration = observation.winner().author_registration.clone();
    wrong_author.author_registration.registration_hash =
        ObjectHash::digest(b"wrong author registration");
    assert!(observation
        .verified_nonactivation(exact_target(wrong_author), &author,)
        .is_err());
    let mut unsigned = batch.commit.value.clone();
    unsigned.signature = "00".to_string();
    assert!(observation
        .verified_nonactivation(exact_target(unsigned), &author,)
        .is_err());
    let mut noncanonical = candidate;
    noncanonical.canonical_signed_bytes.push(b' ');
    noncanonical.object = crate::sync::storage::ExactObjectRef::new(
        noncanonical.object.slot().clone(),
        noncanonical.canonical_signed_bytes.len() as u64,
        ObjectHash::digest(&noncanonical.canonical_signed_bytes),
    );
    assert!(observation
        .verified_nonactivation(noncanonical, &author,)
        .is_err());
}

#[tokio::test]
async fn serial_nonactivation_requires_a_different_verified_immediate_successor() {
    let (_home, storage, db, keypair, root, _pending) =
        serial_fixture("serial-verified-nonactivation").await;
    let (_temp, store_dir) = temp_store_dir();
    assert!(prepare_serial_store_write(
        &db,
        &storage,
        storage.serial_coordination().expect("Serial coordination"),
        &local_device_id(&db).await,
        &keypair,
        &store_dir
    )
    .await
    .expect("prepare losing Serial branch"));
    let branch = db
        .prepared_serial_store_branch()
        .await
        .expect("load losing Serial branch")
        .expect("losing Serial branch exists");
    let losing = branch
        .writes
        .first()
        .expect("losing branch has a candidate");
    let StoreCommitOrder::Serial { predecessor, .. } = &losing.commit.value.order else {
        unreachable!("Serial fixture candidate")
    };
    let winner = competing_head(&db, &storage, &keypair, "verified-nonactivation").await;
    let coordination = storage.serial_coordination().expect("Serial coordination");
    let current = coordination
        .read_head(serial_head_key())
        .await
        .expect("read Serial base head");
    let accepted_head = coordination
        .replace_head(serial_head_key(), &current.version, &winner.to_bytes())
        .await
        .expect("activate competing Serial successor");
    let crate::sync::store_engine::serial::pull::SerialSuccessorObservation::Advanced(suffix) =
        crate::sync::store_engine::serial::pull::observe_serial_successors_after(
            &storage,
            coordination,
            &root,
            predecessor,
        )
        .await
        .expect("observe verified Serial successor")
    else {
        panic!("competing Serial successor did not advance the head")
    };
    assert_eq!(
        suffix.durable().observed_version_hash,
        ObjectHash::digest(accepted_head.version.cloud().as_provider().as_bytes()),
    );
    let author = db
        .activated_store_device_registration(losing.commit.value.author_registration.clone())
        .await
        .expect("load losing Serial author");
    let target = StoreBatchCommitDeletionTarget {
        coord: StoreCommitCoord::Serial {
            sequence: losing.commit.value.seq(),
        },
        object: losing.commit.object.clone(),
        canonical_signed_bytes: losing.commit.bytes.clone(),
    };
    super::super::super::remote_object::VerifiedCandidateNonactivation::serial(
        &suffix,
        vec![(target.clone(), author.clone())],
    )
    .expect("verified competing successor discards the losing candidate");

    assert!(
        super::super::super::remote_object::VerifiedCandidateNonactivation::serial(
            &suffix,
            vec![(target.clone(), author.clone()), (target.clone(), author)],
        )
        .is_err()
    );

    let accepted_ref = suffix
        .commits()
        .first()
        .expect("accepted suffix has an immediate successor");
    let StoreSerialHeadState::Commit {
        author_registration: accepted_author_ref,
        ..
    } = &winner.state
    else {
        unreachable!("competing Serial head activates a commit")
    };
    let accepted_author = db
        .activated_store_device_registration(accepted_author_ref.clone())
        .await
        .expect("load accepted Serial author");
    let accepted_commit = super::super::super::store_objects::load_commit_ref(
        &storage,
        root.store_root_hash,
        accepted_ref,
        &accepted_author,
    )
    .await
    .expect("load accepted Serial commit")
    .value;
    let accepted_target = StoreBatchCommitDeletionTarget {
        coord: accepted_ref.coord.clone(),
        object: accepted_ref.object.clone(),
        canonical_signed_bytes: accepted_commit.to_bytes(),
    };
    assert!(
        super::super::super::remote_object::VerifiedCandidateNonactivation::serial(
            &suffix,
            vec![(accepted_target, accepted_author.clone())],
        )
        .is_err()
    );

    let mut noncanonical = target;
    noncanonical.canonical_signed_bytes.push(b' ');
    noncanonical.object = crate::sync::storage::ExactObjectRef::new(
        noncanonical.object.slot().clone(),
        noncanonical.canonical_signed_bytes.len() as u64,
        ObjectHash::digest(&noncanonical.canonical_signed_bytes),
    );
    let losing_author = db
        .activated_store_device_registration(losing.commit.value.author_registration.clone())
        .await
        .expect("reload losing Serial author");
    assert!(
        super::super::super::remote_object::VerifiedCandidateNonactivation::serial(
            &suffix,
            vec![(noncanonical, losing_author)],
        )
        .is_err()
    );

    let current = coordination
        .read_head(serial_head_key())
        .await
        .expect("read accepted Serial head");
    let mut invalid_head = winner.clone();
    invalid_head.signature = "00".to_string();
    coordination
        .replace_head(
            serial_head_key(),
            &current.version,
            &invalid_head.to_bytes(),
        )
        .await
        .expect("install invalid signed Serial head bytes");
    assert!(
        crate::sync::store_engine::serial::pull::observe_serial_successors_after(
            &storage,
            coordination,
            &root,
            predecessor,
        )
        .await
        .is_err()
    );

    let current = coordination
        .read_head(serial_head_key())
        .await
        .expect("read invalid Serial head receipt");
    let mut missing_commit = accepted_ref.clone();
    missing_commit.commit_hash = ObjectHash::digest(b"missing accepted Serial commit");
    missing_commit.object = crate::sync::storage::ExactObjectRef::new(
        crate::storage::cloud::ObjectSlot::logical(
            "store-v1/commits/missing-accepted-serial.json".to_string(),
        )
        .expect("valid missing Serial commit slot"),
        1,
        ObjectHash::digest(b"missing"),
    );
    let device_signer = accepted_author
        .device_signer(&keypair)
        .expect("derive accepted Serial head signer");
    let broken_chain_head = StoreSerialHead::signed(
        root.store_root_hash,
        StoreSerialHeadState::Commit {
            author_registration: accepted_author_ref.clone(),
            commit: missing_commit,
        },
        &device_signer,
    )
    .expect("sign Serial head with an absent exact chain tip");
    coordination
        .replace_head(
            serial_head_key(),
            &current.version,
            &broken_chain_head.to_bytes(),
        )
        .await
        .expect("install Serial head with an absent chain tip");
    assert!(
        crate::sync::store_engine::serial::pull::observe_serial_successors_after(
            &storage,
            coordination,
            &root,
            predecessor,
        )
        .await
        .is_err()
    );
}

use super::*;
use coven_protocol::objects::{ProtocolObjectContext, ProtocolObjectDomain};
use coven_protocol::store_commit::{self, *};
use coven_storage::CloudSyncObjectStorage;

#[tokio::test]
async fn an_unaccepted_owner_promotion_cannot_authorize_its_own_snapshot() {
    let (fixture, storage) =
        PromotionCandidate::build_with_connection("snapshot-unaccepted-owner-promotion").await;
    let owner = fixture
        .store
        .bind_device_in(
            &fixture.owner_db,
            fixture.owner_db_store_dir.clone(),
            &fixture.owner,
        )
        .await
        .expect("bind promoter");
    let member = fixture
        .store
        .bind_device_in(
            &fixture.member_db,
            fixture.member_db_store_dir.clone(),
            &fixture.member,
        )
        .await
        .expect("bind promotion target");
    let request = owner
        .begin_owner_promotion(fixture.member_registration.clone())
        .await
        .expect("publish real promotion request");
    let acceptance = member
        .accept_owner_promotion(request)
        .await
        .expect("accept real promotion request");
    let database = StoreDatabase::new(&fixture.owner_db);
    let directory = tempfile::tempdir().expect("snapshot directory");
    let image_bytes = database
        .capture_snapshot_image_for_test(
            fixture.store.root(),
            directory.path().to_path_buf(),
            Some(fixture.encryption.clone()),
        )
        .await
        .expect("capture accepted predecessor image");
    let coverage = CommitFrontier::from_refs(
        database
            .materialized_frontier()
            .await
            .expect("accepted predecessor frontier"),
    )
    .expect("exact predecessor cut");
    owner
        .publish_snapshot(image_bytes.clone(), coverage)
        .await
        .expect("publish predecessor snapshot");
    let template = database
        .latest_local_store_snapshot()
        .await
        .expect("read predecessor snapshot")
        .expect("snapshot exists");
    let (_, pulled) = member
        .pull_store()
        .await
        .expect("install accepted predecessor");
    assert!(pulled.held_positions.is_empty(), "{pulled:?}");
    let mut membership = owner
        .membership_for_test()
        .await
        .expect("read accepted membership");
    assert!(!membership.is_owner_now(&keys::public_key_hex(&fixture.member)));
    let before = database
        .store_current_publication()
        .await
        .expect("accepted boundary");
    let member_database = StoreDatabase::new(&fixture.member_db);
    let member_before = member_database
        .store_current_publication()
        .await
        .expect("member boundary");

    // Retain the real signed candidate before any upload. The adversary can
    // expose its prepared authority objects without accepting the Store commit.
    fixture.home.fail_exact_create_before_call(1);
    owner
        .finalize_owner_promotion(&fixture.encryption, acceptance.clone())
        .await
        .expect_err("retain the signed promotion before its Store publication");
    let journal = database
        .load_owner_promotion_journal(acceptance.request.promotion_id)
        .await
        .expect("read pending promotion")
        .expect("promotion journal exists");
    let OwnerPromotionJournalState::MergeHeadPrepared {
        candidate,
        wrapped_key,
        ..
    } = &journal.state
    else {
        panic!("promotion must be fully prepared, got {:?}", journal.state);
    };
    let publication = candidate
        .prepared_membership_publication()
        .expect("prepared exact publication");
    candidate
        .validate_closed_shape()
        .expect("genuine signed promotion graph");
    assert_eq!(candidate.publication.previous, *before.record());
    assert_eq!(
        database
            .store_current_publication()
            .await
            .expect("unchanged publication"),
        before
    );
    assert!(!database
        .store_publication_entries()
        .await
        .expect("accepted entries")
        .iter()
        .any(|entry| entry.value.payload
            == StorePublicationPayload::Commit(candidate.reference.clone())));
    for prepared in [
        wrapped_key.object.clone(),
        publication.prepared_entry().expect("exact prepared entry"),
        publication.prepared_head().expect("exact prepared head"),
    ] {
        storage
            .create_protocol_object(&prepared)
            .await
            .expect("expose signed but unaccepted promotion authority");
    }
    let root = fixture.store.root();
    let current_context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreCurrentPublication,
    );
    let (current_bytes, current_version) = storage
        .read_versioned_protocol_record(
            &current_context,
            &owner
                .protocol_root_for_test()
                .descriptor
                .current_publication_slot,
            store_commit::store_current_publication_semantic_prefix(),
        )
        .await
        .expect("read actual unmodified provider acceptance");
    let current: StoreCurrentPublicationRecord =
        coven_protocol::objects::decode_protocol_object(&current_bytes)
            .expect("decode provider acceptance");
    assert_eq!(
        &current,
        before.record(),
        "promotion must remain unaccepted at the provider"
    );
    assert_eq!(
        &current_version,
        before
            .require_observed()
            .expect("fixture retained an observed provider boundary")
            .version()
    );
    let head_context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreMembershipHead,
    );
    let head_prefix =
        store_commit::semantic_prefix_from_exact_object(&publication.head_ref.object, ".json")
            .expect("exact pending head prefix");
    assert_eq!(
        storage
            .read_protocol_object(&head_context, &publication.head_ref.object, &head_prefix)
            .await
            .expect("prepared head is actually uploaded"),
        serde_json::to_vec(&publication.head).expect("head bytes")
    );

    // Construct adversarial data from the actual signed pending transition.
    // No receiver is told to accept its Store commit or given local authority.
    membership
        .add_entry(publication.entry.clone())
        .expect("apply signed proposed grant");
    membership
        .activate_head_ref(publication.head_ref.clone())
        .expect("select exact proposed head");
    assert!(membership.is_owner_now(&keys::public_key_hex(&fixture.member)));
    let (_, predecessor_state) = database
        .store_device_state_for_history_cut(
            &candidate
                .commit
                .order
                .predecessor_cut()
                .expect("candidate predecessor cut"),
        )
        .await
        .expect("actual predecessor device state");
    let activation = OwnerRecoveryActivationId::derive(
        &root,
        &acceptance.request.member_pubkey,
        &acceptance.request.intended_owner_grant,
        acceptance.anchors.recovery(),
    )
    .expect("proposed recovery activation");
    let state = predecessor_state
        .activate_owner_recovery(acceptance.request.intended_owner_grant.clone(), activation)
        .expect("apply the proposed device-state change");
    let mut coverage = template.meta.coverage.clone();
    coverage.0.insert(
        candidate.reference.coord.stream_id,
        candidate.reference.clone(),
    );
    let state_ref = StoreDeviceStateRef::from_resolved(coverage.clone(), &state)
        .expect("claimed post-promotion cut");
    let image = coven_database::DatabaseImageTest::from_bytes(&image_bytes)
        .expect("open real captured image");
    image
        .replace_store_device_snapshot(&candidate.reference, &state)
        .expect("bind image state to the exact unaccepted commit");
    let image_bytes = image.into_bytes().expect("serialize adversarial image");
    let mut summary = template.meta.history_summary.clone();
    summary.causal_cut.insert(
        candidate.reference.coord.clone(),
        candidate.reference.clone(),
    );
    summary.membership_proofs.insert(
        candidate.reference.clone(),
        *candidate
            .history_evidence
            .membership_proof
            .clone()
            .expect("real retained promotion proof"),
    );
    summary.post_state = state_ref.clone();
    summary.membership_floor = MembershipCausalFloor::from_membership(&membership);
    summary
        .reclaim
        .include_previous_snapshot(
            before
                .record()
                .latest_snapshot()
                .expect("accepted predecessor snapshot"),
            &template.meta,
        )
        .expect("retain exact predecessor snapshot ownership");
    let mut history = crate::sync::store::HistoryConstructionAuthority::for_snapshot()
        .open_pinned(storage.as_ref(), &root)
        .await
        .expect("open cold verification owner");
    let registration = history
        .load_registration(&fixture.member_registration)
        .await
        .expect("actual member registration");
    let device_signer = registration
        .value
        .device_signer(&fixture.member)
        .expect("member device signing key");
    summary.registrations.insert(
        fixture.member_registration.device_id,
        ReferencedStoreDeviceRegistration::verified(
            fixture.member_registration.clone(),
            registration.value.clone(),
        )
        .expect("exact member registration"),
    );
    summary
        .validate_snapshot_baseline()
        .expect("signed retained proof is internally valid");

    let rollup_context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreMembershipRollup,
    );
    let rollup_prefix = store_commit::semantic_prefix_from_exact_object(
        &template.meta.membership_rollup.object,
        ".json",
    )
    .expect("exact predecessor rollup prefix");
    let rollup_bytes = storage
        .read_protocol_object(
            &rollup_context,
            &template.meta.membership_rollup.object,
            &rollup_prefix,
        )
        .await
        .expect("read genuine predecessor membership rollup");
    let original: MembershipRollup = coven_protocol::objects::decode_protocol_object(&rollup_bytes)
        .expect("decode predecessor rollup");
    let predecessor_acceptance = match publication
        .head
        .body
        .predecessor
        .as_ref()
        .and_then(|predecessor| predecessor.acceptance())
    {
        Some(reference) => {
            let context = ProtocolObjectContext::signed_plaintext(
                root.store_root_hash,
                ProtocolObjectDomain::StoreMembershipHeadAcceptance,
            );
            let prefix = store_commit::semantic_prefix_from_exact_object(reference, ".json")
                .expect("exact predecessor acceptance prefix");
            let bytes = storage
                .read_protocol_object(&context, reference, &prefix)
                .await
                .expect("read actual accepted predecessor result");
            Some(
                coven_protocol::objects::decode_protocol_object(&bytes)
                    .expect("decode actual accepted predecessor result"),
            )
        }
        None => None,
    };
    let mut streams = original.streams.clone();
    streams
        .iter_mut()
        .find(|stream| {
            stream.stream_id == publication.head_ref.coord.stream_id
                && stream.author_owner_grant == publication.head_ref.coord.author_owner_grant
        })
        .expect("promoter membership stream")
        .heads
        .push(MembershipRollupHead {
            head: publication.head_ref.clone(),
            head_value: publication.head.clone(),
            entry: publication.entry_ref.clone(),
            entry_value: publication.entry.clone(),
            predecessor_acceptance,
        });
    let metadata_context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreSnapshotMeta,
    );
    let metadata_prefix = store_commit::snapshot_candidate_semantic_prefix(
        &fixture.member_registration.device_id.to_string(),
        "unaccepted-promotion",
    );
    let metadata_slot = storage
        .allocate_protocol_slot(&metadata_context, &metadata_prefix, ".json")
        .await
        .expect("reserve exact candidate metadata slot");
    let rollup = MembershipRollup::signed(
        root.store_root_hash,
        fixture.member_registration.clone(),
        streams,
        original.resolutions.clone(),
        &device_signer,
    )
    .expect("sign exact proposed membership rollup");
    let rollup_bytes = rollup.to_bytes();
    let rollup_hash = ObjectHash::digest(&rollup_bytes);
    let rollup_prefix =
        store_commit::membership_rollup_semantic_prefix(&metadata_slot, rollup_hash);
    let rollup_object = fixture
        .store
        .create_exact_protocol_object(&rollup_context, &rollup_prefix, ".json", &rollup_bytes)
        .await
        .expect("upload signed proposed rollup");
    let image_hash = ObjectHash::digest(&image_bytes);
    let image_context = ProtocolObjectContext::store_encrypted(
        root.store_root_hash,
        ProtocolObjectDomain::StoreSnapshotImage,
    );
    let image_prefix = store_commit::snapshot_image_semantic_prefix(&metadata_slot, image_hash);
    let image_object = fixture
        .store
        .create_exact_protocol_object(&image_context, &image_prefix, ".db", &image_bytes)
        .await
        .expect("upload exact proposed image");
    let meta = SnapshotMeta::signed(
        root.store_root_hash,
        fixture.member_registration.clone(),
        before.record().clone(),
        SnapshotImageRef {
            image_hash,
            object: image_object,
        },
        MembershipRollupRef {
            rollup_hash,
            object: rollup_object,
        },
        coverage,
        StoreSnapshotState {
            devices: state.clone(),
            membership: coven_protocol::circle_control::StoreMembershipStateRef::from_membership(
                &membership,
                state.recovery.clone(),
            )
            .expect("claimed promoted membership"),
        },
        summary,
        template.meta.schema_version,
        template.meta.created_at.clone(),
        &device_signer,
    )
    .expect("sign snapshot with the unaccepted Owner's device key");
    let prepared_metadata = storage
        .prepare_protocol_object(
            &metadata_context,
            metadata_slot,
            &metadata_prefix,
            meta.to_bytes(),
        )
        .expect("prepare candidate metadata at its reserved slot");
    storage
        .create_protocol_object(&prepared_metadata)
        .await
        .expect("upload candidate metadata");
    let object = prepared_metadata.reference().clone();
    let reference = StoreSnapshotRef {
        snapshot_hash: meta.snapshot_hash(),
        object,
    };
    SnapshotMeta::parse_at(
        &meta.to_bytes(),
        root.store_root_hash,
        &reference,
        &registration.value,
    )
    .expect("snapshot signature and exact shape are valid");
    let entry = StorePublicationEntry::signed_snapshot(
        before.record(),
        fixture.member_registration.clone(),
        reference,
        &device_signer,
    )
    .expect("sign snapshot publication");
    let context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StorePublicationEntry,
    );
    let prefix = store_commit::store_publication_entry_semantic_prefix(&entry);
    let object = fixture
        .store
        .create_exact_protocol_object(&context, &prefix, ".json", &entry.to_bytes())
        .await
        .expect("upload snapshot publication");
    let publication_ref =
        StorePublicationRef::from_entry(&entry, object).expect("exact snapshot publication");
    let replacement = StoreCurrentPublicationRecord::advance_snapshot(
        before.record(),
        &entry,
        publication_ref,
        &device_signer,
    )
    .expect("sign shared current record");
    let context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreCurrentPublication,
    );
    let replaced = storage
        .replace_protocol_record_if_version(
            &context,
            &owner
                .protocol_root_for_test()
                .descriptor
                .current_publication_slot,
            store_commit::store_current_publication_semantic_prefix(),
            before
                .require_observed()
                .expect("fixture replaces an observed provider boundary")
                .version(),
            replacement.to_bytes(),
        )
        .await
        .expect("provider conditionally accepts the candidate snapshot");
    assert!(matches!(
        replaced,
        coven_storage::cloud::ConditionalWriteOutcome::Replaced(_)
    ));

    let result = history.load_current_accepted_snapshot().await;
    assert_eq!(
        database
            .store_current_publication()
            .await
            .expect("promoter boundary after verification"),
        before
    );
    assert_eq!(
        member_database
            .store_current_publication()
            .await
            .expect("member boundary after verification"),
        member_before
    );
    assert_eq!(
        serde_json::to_vec(
            &database
                .load_owner_promotion_journal(journal.promotion_id)
                .await
                .expect("pending journal after verification")
                .expect("pending journal remains owned")
        )
        .expect("encode retained journal"),
        serde_json::to_vec(&journal).expect("encode original journal")
    );
    let error = result
        .expect_err("an unaccepted promotion cannot supply Owner authority to its own snapshot");
    assert!(
        !error.to_string().contains("NotFound"),
        "rejection must establish authority failure, not missing fixture bytes: {error}"
    );
    history
        .verify_refs([candidate.reference.clone()])
        .await
        .expect_err("rejected snapshot cannot seed the pending promotion's acceptance");
}

use super::*;
use coven_protocol::prepared_commit::PreparedStoreOperationCommit;
use coven_protocol::wrapped_store_key::PreparedWrappedStoreKey;

#[tokio::test]
async fn staging_a_removal_derives_its_rotation_from_the_prepared_request() {
    stage_removal(false).await;
}

#[tokio::test]
async fn staging_a_removal_rejects_a_wrapped_key_owned_by_another_candidate() {
    stage_removal(true).await;
}

async fn stage_removal(substitute_owner: bool) {
    let fixture = PromotionCandidate::build("removal-staging-generation").await;
    let owner = fixture
        .store
        .bind_device_in(
            &fixture.owner_db,
            fixture.owner_db_store_dir.clone(),
            &fixture.owner,
        )
        .await
        .expect("bind removal owner");
    let (_, pulled) = owner
        .pull_store()
        .await
        .expect("install accepted predecessor");
    assert!(pulled.held_positions.is_empty());
    let database = StoreDatabase::new(&fixture.owner_db);
    assert!(database.active_store_publication().await.unwrap().is_none());
    assert!(database.load_rotation_gate().await.unwrap().is_none());
    let before = fixture.owner_db.database_image_for_test().await.unwrap();
    fixture.home.fail_exact_create_before_call(1);
    fixture
        .store
        .remove_member(
            &fixture.owner_db,
            fixture.owner_db_store_dir.clone(),
            &fixture.owner,
            &keys::public_key_hex(&fixture.member),
            &fixture.encryption,
            &crate::sync::test_helpers::TestCustody::default(),
        )
        .await
        .expect_err("retain real removal before its first upload");
    let row = database
        .outbound_membership_mutation()
        .await
        .unwrap()
        .unwrap();
    let gate = database
        .load_rotation_gate()
        .await
        .unwrap()
        .expect("production removal staged its rotation");
    let encoded: serde_json::Value = serde_json::from_slice(&row.plan_bytes).unwrap();
    let candidate: PreparedStoreOperationCommit =
        serde_json::from_value(encoded["plan"]["publication"]["candidate"].clone()).unwrap();
    let publication = candidate.prepared_membership_publication().unwrap();
    let wraps = encoded["plan"]["wraps"]
        .as_array()
        .unwrap()
        .iter()
        .map(|wrap| {
            serde_json::from_value::<PreparedWrappedStoreKey>(wrap["prepared"].clone()).unwrap()
        })
        .collect::<Vec<_>>();
    let mut remotes = candidate
        .merge_membership_activation_remote_objects(&publication.transition(), &publication, &wraps)
        .unwrap();
    if substitute_owner {
        let other_candidate = database
            .store_publication_entries()
            .await
            .unwrap()
            .into_iter()
            .find_map(|entry| match &entry.value.payload {
                coven_protocol::store_commit::StorePublicationPayload::Commit(reference) => {
                    Some(reference.clone())
                }
                _ => None,
            })
            .expect("fixture has an accepted candidate");
        assert_ne!(other_candidate, candidate.reference);
        let index = remotes
            .iter()
            .position(|remote| remote.object() == &wraps[0].reference.object)
            .expect("removal owns a wrapped key");
        let substituted = remotes
            .remove(index)
            .map_record(|mut remote| {
                let coven_protocol::remote_object::RemoteObjectRecord::CandidateExclusive(record) =
                    &mut remote
                else {
                    panic!("wrapped key is candidate exclusive");
                };
                let coven_protocol::remote_object::CandidateObjectState::Prepared { ownership } =
                    &mut record.state
                else {
                    panic!("wrapped key is prepared");
                };
                ownership.pending = std::collections::BTreeSet::from([other_candidate]);
                remote.validate()?;
                Ok(remote)
            })
            .expect("substituted record remains internally valid with its original bytes");
        remotes.insert(index, substituted);
    }
    drop(owner);
    fixture
        .owner_db
        .replace_with_database_image_for_test(before)
        .await
        .unwrap();
    assert!(database
        .outbound_membership_mutation()
        .await
        .unwrap()
        .is_none());
    assert!(database.load_rotation_gate().await.unwrap().is_none());
    let staged = database
        .stage_membership_candidate_mutation(row.plan_bytes, row.progress_bytes, remotes, candidate)
        .await;
    if substitute_owner {
        staged.expect_err("staging must bind every remote owner to the prepared candidate");
        assert!(database
            .outbound_membership_mutation()
            .await
            .unwrap()
            .is_none());
        assert!(database.active_store_publication().await.unwrap().is_none());
        assert!(database.load_rotation_gate().await.unwrap().is_none());
    } else {
        assert_eq!(
            staged.expect("stage the exact removal request"),
            row.intent_hash
        );
        assert_eq!(
            database.load_rotation_gate().await.unwrap(),
            Some(gate),
            "the database must derive the rotation from its prepared removal"
        );
    }
}

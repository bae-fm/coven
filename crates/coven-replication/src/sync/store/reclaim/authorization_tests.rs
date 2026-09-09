use super::*;
use crate::sync::test_helpers::{open_test_db, test_cloud_home, test_store_dir, TestStore};
use coven_keys::keys::UserKeypair;

#[tokio::test]
async fn retired_snapshot_rejects_reclaim_authority_for_a_forged_lower_commit() {
    check_retired_reclaim_authority(ForgeTarget::Activation).await;
}

#[tokio::test]
async fn retired_snapshot_preserves_reclaim_authority_for_the_exact_covered_commit() {
    check_retired_reclaim_authority(ForgeTarget::Neither).await;
}

#[tokio::test]
async fn retired_snapshot_rejects_another_package_at_the_exact_covered_commit() {
    check_retired_reclaim_authority(ForgeTarget::Package).await;
}

enum ForgeTarget {
    Neither,
    Activation,
    Package,
}

async fn check_retired_reclaim_authority(forge: ForgeTarget) {
    let directory = test_store_dir();
    let database = open_test_db(directory.clone());
    let signer = UserKeypair::generate();
    let home = test_cloud_home();
    let (store, storage) = TestStore::create_with_connection(
        &database,
        directory.clone(),
        "retired-reclaim-authority",
        signer.clone(),
        home.clone(),
    )
    .await
    .expect("create the reclaim Store");
    let device = store
        .bind_device_in(&database, directory, &signer)
        .await
        .expect("bind the owner device");
    let source = open_test_db(test_store_dir());
    let first = source
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('covered-target', 'target', NULL, \
             '0000000001000-0000-reclaim-proof', '2026-01-01')",
        ])
        .await;
    let first = store
        .publish_changeset("founder", 1, &first, database.schema_version())
        .await
        .expect("accept the target package");
    let second = source
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('covering-successor', 'successor', NULL, \
             '0000000002000-0000-reclaim-proof', '2026-01-01')",
        ])
        .await;
    store
        .publish_changeset("founder", 2, &second, database.schema_version())
        .await
        .expect("accept a strictly later author coordinate");
    let target_commit = device
        .load_commit_for_test(&first)
        .await
        .expect("read the target's signed commit");
    let package = target_commit
        .value()
        .store_package()
        .expect("the target has a Store package")
        .clone();
    device
        .ensure_device_join_snapshot_for_test()
        .await
        .expect("publish and adopt the covering snapshot");
    let database = StoreDatabase::new(&database);
    let baseline = database
        .installed_replay_baseline()
        .await
        .expect("read the installed snapshot baseline");
    assert!(baseline.snapshot().is_some());
    assert!(
        baseline.coverage().commits()[&first.coord.stream_id]
            .coord
            .sequence()
            > first.coord.sequence()
    );
    assert!(!database
        .retained_merge_materialization_refs()
        .await
        .expect("read the retained replay interval")
        .contains(&first));

    home.remove_exact_object(first.object.slot());
    assert!(
        home.stored_exact_bytes(first.object.slot()).is_none(),
        "retired activation is absent at the provider"
    );

    let mut activation = first.clone();
    if matches!(forge, ForgeTarget::Activation) {
        activation.commit_hash = ObjectHash::digest(b"another commit at the retired coordinate");
        assert_ne!(activation, first);
        assert!(baseline.covers(&activation));
    }
    let mut claimed_package = package.clone();
    if matches!(forge, ForgeTarget::Package) {
        claimed_package.content_hash =
            ObjectHash::digest(b"another package at the accepted activation");
    }
    let mut writer = device
        .authorize_writer()
        .await
        .expect("authorize the owner");
    let plan = writer
        .prepare_plan()
        .await
        .expect("reserve the next author operation");
    let evidence = plan
        .sign_reclaim_evidence(ReclaimClaim::StorePackage(StorePackageReclaimClaim {
            target: StorePackageReclaimTarget {
                package: claimed_package,
                activation,
            },
        }))
        .expect("sign the target claim with the active owner");
    let evidence_context = ProtocolObjectContext::store_encrypted(
        store.root().store_root_hash,
        ProtocolObjectDomain::StoreReclaimEvidence,
    );
    let evidence_prefix = reclaim_evidence_semantic_prefix(evidence.evidence_hash());
    let evidence_slot = storage
        .allocate_protocol_slot(&evidence_context, &evidence_prefix, ".json")
        .await
        .expect("allocate evidence");
    let evidence_prepared = storage
        .prepare_protocol_object(
            &evidence_context,
            evidence_slot,
            &evidence_prefix,
            evidence.to_bytes(),
        )
        .expect("prepare exact evidence");
    let evidence_ref =
        ReclaimEvidenceRef::from_evidence(&evidence, evidence_prepared.reference().clone());
    let authorization = plan.sign_reclaim_authorization(
        evidence.claim.target(),
        evidence_ref.clone(),
        StoreReclaimAuthority {
            membership: plan.membership_state().clone(),
            owner_grant: plan.owner_grant().expect("the signer is an owner").clone(),
        },
    );
    let authorization_context = ProtocolObjectContext::signed_plaintext(
        store.root().store_root_hash,
        ProtocolObjectDomain::StoreReclaimAuthorization,
    );
    let authorization_prefix =
        reclaim_authorization_semantic_prefix(authorization.authorization_hash());
    let authorization_slot = storage
        .allocate_protocol_slot(&authorization_context, &authorization_prefix, ".json")
        .await
        .expect("allocate the public authorization");
    let authorization_prepared = storage
        .prepare_protocol_object(
            &authorization_context,
            authorization_slot,
            &authorization_prefix,
            authorization.to_bytes(),
        )
        .expect("prepare the public authorization");
    let authorization_ref = ReclaimAuthorizationRef::from_authorization(
        &authorization,
        authorization_prepared.reference().clone(),
    );
    let candidate = writer
        .prepare_candidate(
            &plan,
            crate::sync::store::commit_publication::operation::commit_plan::StoreOperationBatch::ReclaimAuthorization(Box::new(authorization_ref.clone())),
        )
        .await
        .expect("prepare the signed authorization candidate");
    let candidate_ref = candidate.reference.clone();
    let operation = DurableStoreReclaimOperation::AuthorizationCandidate {
        object: Box::new(DurableStoreReclaimObject::Authorization {
            evidence_ref,
            evidence,
            evidence_prepared,
            authorization_ref,
            authorization,
            authorization_prepared,
        }),
        candidate: Box::new(candidate),
    };
    database
        .begin_store_reclaim_operation(operation.clone())
        .await
        .expect("journal the exact candidate");
    drop(plan);
    let before = database
        .store_current_publication()
        .await
        .expect("read the local accepted boundary");
    let current_context = ProtocolObjectContext::signed_plaintext(
        store.root().store_root_hash,
        ProtocolObjectDomain::StoreCurrentPublication,
    );
    let current_slot = &device.protocol_root().descriptor.current_publication_slot;
    let current_prefix = coven_protocol::store_commit::store_current_publication_semantic_prefix();
    let remote_before = storage
        .read_versioned_protocol_record(&current_context, current_slot, current_prefix)
        .await
        .expect("read the provider's accepted boundary");
    let deletes_before = home.deletes_seen();
    let result = writer.reclaim().drive_candidate(operation.clone()).await;
    if !matches!(forge, ForgeTarget::Neither) {
        assert!(result.is_err(), "retired numeric coverage must not admit a reclaim authorization for another exact commit: {result:?}");
        assert_eq!(
            database
                .store_current_publication()
                .await
                .expect("read the unchanged local boundary"),
            before
        );
        assert_eq!(
            storage
                .read_versioned_protocol_record(&current_context, current_slot, current_prefix)
                .await
                .expect("read the unchanged provider boundary"),
            remote_before
        );
        assert_eq!(
            database
                .store_reclaim_operations()
                .await
                .expect("read the unresolved authorization"),
            vec![operation]
        );
    } else {
        result.expect("the exact covered target remains publishable");
        assert!(database
            .exact_materialized_ref(
                &candidate_ref.coord.stream_id.to_string(),
                candidate_ref.coord.sequence()
            )
            .await
            .expect("read the accepted authorization")
            .is_some_and(|installed| installed == candidate_ref));
        assert!(
            matches!(database.store_reclaim_operations().await.expect("read accepted authorization").as_slice(), [DurableStoreReclaimOperation::Authorized { activation, .. }] if activation == &candidate_ref)
        );
    }
    assert_eq!(home.deletes_seen(), deletes_before);
    assert!(
        home.stored_exact_bytes(package.object.slot()).is_some(),
        "authorization admission must not delete its target"
    );
}

use super::*;

/// A Store package reclaim over a target no accepted commit ever published,
/// authorized by a reclaim no accepted commit ever activated. Every identity is
/// well-shaped and none of them is in this Store's history.
fn unactivated_reclaim(
    fixture: &ReclaimJourneyFixture,
) -> (ReclaimAuthorizationRef, ReclaimTarget, StoreBatchCommitRef) {
    let mut target = fixture.packages[0].clone();
    target.package.content_hash = ObjectHash::digest(b"a Store package no commit published");
    target.package.object = proof_object("store-v1/candidates/unactivated/package.pkg");
    target.activation.coord = StoreCommitCoord {
        stream_id: target.activation.coord.stream_id,
        sequence: target.activation.coord.sequence() + 1,
    };
    target.activation.commit_hash = ObjectHash::digest(b"a commit that published nothing");
    target.activation.object = proof_object("store-v1/commits/unactivated-package.json");
    let authorization = ReclaimAuthorizationRef {
        authorization_hash: ObjectHash::digest(b"a reclaim authorization no commit activated"),
        evidence: ReclaimEvidenceRef {
            evidence_hash: ObjectHash::digest(b"a reclaim evidence no commit activated"),
            target: Box::new(ReclaimTarget::StorePackage(target.clone())),
            object: proof_object("store-v1/reclaim/evidence/unactivated.json"),
        },
        object: proof_object("store-v1/reclaim/authorizations/unactivated.json"),
    };
    let activation = StoreBatchCommitRef {
        coord: StoreCommitCoord {
            stream_id: target.activation.coord.stream_id,
            sequence: target.activation.coord.sequence() + 1,
        },
        commit_hash: ObjectHash::digest(b"a commit that activated nothing"),
        object: proof_object("store-v1/commits/unactivated-authorization.json"),
    };
    (
        authorization,
        ReclaimTarget::StorePackage(target),
        activation,
    )
}

/// Journal a reclaim standing at verified absence over that unactivated
/// authorization, which is the state a completion is authored from.
async fn journal_unactivated_absence(
    fixture: &ReclaimJourneyFixture,
) -> DurableStoreReclaimOperation {
    let (authorization, target, authorization_activation) = unactivated_reclaim(fixture);
    let operation = DurableStoreReclaimOperation::AbsentVerified {
        authorization: authorization.clone(),
        authorization_activation: authorization_activation.clone(),
        target,
    };
    let reclaimed = coven_database::ReclaimedStorePackage::absent_verified(
        authorization,
        authorization_activation,
    )
    .expect("shape the verified absence");
    fixture
        .db
        .install_reclaimed_store_package_for_test(operation.clone(), reclaimed)
        .await
        .expect("journal the verified absence");
    operation
}

/// Author, journal and publish the completion that closes `absence` under
/// `provider_admin_grant`, returning what the publication made of it.
async fn publish_completion(
    fixture: &ReclaimJourneyFixture,
    absence: DurableStoreReclaimOperation,
    provider_admin_grant: coven_protocol::provider::ProviderAdminGrantId,
) -> Result<(), StoreReclaimError> {
    let database = coven_database::StoreDatabase::new(&fixture.db);
    let mut writer = fixture
        .device
        .authorize_writer()
        .await
        .expect("authorize the completion author");
    let plan = writer
        .prepare_plan()
        .await
        .expect("reserve the next author position");
    let candidate = writer
        .prepare_candidate(
            &plan,
            crate::sync::store::commit_publication::operation::commit_plan::StoreOperationBatch::ReclaimCompletion(
                ReclaimCompletion {
                    authorization: absence.authorization().clone(),
                    provider_admin_grant,
                },
            ),
        )
        .await
        .expect("prepare the completion candidate");
    drop(plan);
    let journalled = database
        .begin_store_reclaim_completion(absence, candidate)
        .await
        .expect("journal the completion candidate");
    writer.reclaim().drive_candidate(journalled).await
}

/// The completion asserts which provider-administrator grant its executor
/// deleted under, and the predecessor has to agree the commit's author holds
/// that grant. A completion naming a grant its author does not hold is refused.
#[tokio::test]
async fn a_completion_commit_from_a_non_administrator_is_refused() {
    let fixture = ReclaimJourneyFixture::build("reclaim-completion-unheld-grant").await;
    let absence = journal_unactivated_absence(&fixture).await;
    let error = publish_completion(
        &fixture,
        absence,
        coven_protocol::provider::ProviderAdminGrantId(ObjectHash::digest(
            b"a provider-admin grant no member holds",
        )),
    )
    .await
    .expect_err("a completion under an unheld grant is refused");
    assert!(
        format!("{error:?}").contains(
            "reclaim completion author is not the effective provider administrator at its exact predecessor"
        ),
        "{error:?}",
    );
}

/// A completion closes an authorization the predecessor history accepted. One
/// naming an authorization that history never carried is refused, so a
/// completion cannot retire an obligation that was never taken on.
#[tokio::test]
async fn a_completion_commit_for_an_unknown_authorization_is_refused() {
    let fixture = ReclaimJourneyFixture::build("reclaim-completion-unknown-authorization").await;
    let absence = journal_unactivated_absence(&fixture).await;
    let error = publish_completion(
        &fixture,
        absence,
        fixture
            .device
            .protocol_root()
            .descriptor
            .founder_provider_admin
            .grant_id
            .clone(),
    )
    .await
    .expect_err("a completion for an unknown authorization is refused");
    assert!(
        format!("{error:?}")
            .contains("reclaim completion authorization is absent from predecessor history"),
        "{error:?}",
    );
}

/// The journalled completion candidate is the operation's exact commit: an
/// upload that fails republishes that same commit rather than authoring a
/// second one at a new position, so the reclaim closes at one identity whatever
/// the provider did to the first attempt.
#[tokio::test]
async fn retrying_a_completion_reuses_the_journalled_candidate() {
    let fixture = ReclaimJourneyFixture::build("reclaim-completion-retry").await;
    let database = coven_database::StoreDatabase::new(&fixture.db);
    let target = fixture.packages[0].clone();
    let mut writer = fixture
        .device
        .authorize_writer()
        .await
        .expect("authorize the reclaim writer");

    writer
        .reclaim()
        .prepare_authorization(ReclaimClaim::StorePackage(StorePackageReclaimClaim {
            target,
        }))
        .await
        .expect("journal the authorization candidate");
    let authorization_candidate = sole_operation(&database).await;
    writer
        .reclaim()
        .drive_candidate(authorization_candidate)
        .await
        .expect("activate the authorization");
    let authorized = sole_operation(&database).await;
    writer
        .reclaim()
        .execute_delete(authorized)
        .await
        .expect("delete the authorized target");
    let absent = sole_operation(&database).await;
    writer
        .reclaim()
        .prepare_completion(absent)
        .await
        .expect("journal the completion candidate");

    let operation = sole_operation(&database).await;
    let DurableStoreReclaimOperation::CompletionCandidate { candidate, .. } = &operation else {
        panic!("the journal holds a completion candidate: {operation:?}");
    };
    let candidate_ref = candidate.reference.clone();

    fixture.home.arm_write_failures();
    let interrupted = writer.reclaim().drive_candidate(operation.clone()).await;
    assert!(
        interrupted.is_err(),
        "a failed upload fails the completion to its initiator: {interrupted:?}",
    );
    assert_eq!(
        sole_operation(&database).await,
        operation,
        "the journal still holds the same completion candidate",
    );

    fixture.home.clear_write_failures();
    writer
        .reclaim()
        .drive_candidate(operation)
        .await
        .expect("the retry publishes the journalled candidate");
    let completed = sole_operation(&database).await;
    assert!(
        matches!(
            &completed,
            DurableStoreReclaimOperation::Completed {
                completion_activation,
                ..
            } if completion_activation == &candidate_ref
        ),
        "the reclaim closes at the journalled candidate: {completed:?}",
    );
    assert_eq!(
        database
            .exact_materialized_ref(
                &candidate_ref.coord.stream_id.to_string(),
                candidate_ref.coord.sequence(),
            )
            .await
            .expect("read the accepted completion"),
        Some(candidate_ref),
    );
}

/// The one operation the reclaim journal holds.
async fn sole_operation(database: &coven_database::StoreDatabase) -> DurableStoreReclaimOperation {
    let operations = database
        .store_reclaim_operations()
        .await
        .expect("read the reclaim journal");
    let [operation] = operations.as_slice() else {
        panic!("the journal holds exactly one operation: {operations:?}");
    };
    operation.clone()
}

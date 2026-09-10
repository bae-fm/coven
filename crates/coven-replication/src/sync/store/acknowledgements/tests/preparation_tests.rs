use super::*;
use crate::sync::store::{StoreError, StoreOperationBatch, StorePullError};
use coven_protocol::objects::{ExactObjectRef, StorageError};
use coven_protocol::store_commit::{ObjectHash, StoreProtocolError};

#[tokio::test]
async fn acknowledgement_preparation_requires_exact_signed_evidence() {
    let home = InMemoryCloudHome::new();
    let signer = UserKeypair::generate();
    let storage = storage(&home, &signer);
    let (db, directory) = open(Path::new(":memory:"), "ack-preparation-device");
    let device = initialize(&db, directory, &storage, &signer).await;
    device
        .stage_current_acknowledgement("2026-07-16T00:00:00Z")
        .await
        .unwrap();
    let database = store_database(&db);
    let outbound = database.oldest_outbound_store_ack().await.unwrap().unwrap();
    let published = database.latest_local_store_ack().await.unwrap().unwrap();
    let creates = home.exact_create_count();
    let mut writer = device.authorize_writer().await.unwrap();
    let plan = writer.prepare_plan().await.unwrap();
    plan.validate_acknowledgement(&outbound.ack.value).unwrap();

    let mut invalid_signature = outbound.ack.value.clone();
    invalid_signature.corrupt_signature_for_test();
    let bytes = invalid_signature.to_bytes();
    let mut signed_reference = outbound.reference.clone();
    signed_reference.ack_hash = invalid_signature.ack_hash();
    signed_reference.object = ExactObjectRef::new(
        signed_reference.object.slot().clone(),
        bytes.len() as u64,
        ObjectHash::digest(&bytes),
    );
    let result = writer
        .prepare_candidate(
            &plan,
            StoreOperationBatch::Acknowledgement {
                reference: signed_reference,
                value: invalid_signature,
                circle_acknowledgements: outbound.circle_acknowledgements.clone(),
            },
        )
        .await;
    assert!(
        matches!(
            result,
            Err(StoreError::Pull(StorePullError::Protocol(
                StoreProtocolError::InvalidSignature
            )))
        ),
        "{result:?}"
    );

    let mut inexact_reference = outbound.reference.clone();
    inexact_reference.object = ExactObjectRef::new(
        inexact_reference.object.slot().clone(),
        outbound.ack.bytes.len() as u64,
        ObjectHash::digest(b"different acknowledgement bytes"),
    );
    let result = writer
        .prepare_candidate(
            &plan,
            StoreOperationBatch::Acknowledgement {
                reference: inexact_reference,
                value: outbound.ack.value.clone(),
                circle_acknowledgements: outbound.circle_acknowledgements.clone(),
            },
        )
        .await;
    assert!(
        matches!(result, Err(StoreError::Pull(StorePullError::Context { source, .. }))
        if matches!(*source, StorePullError::Storage(StorageError::InvalidContent(_))))
    );

    let candidate = writer
        .prepare_candidate(
            &plan,
            StoreOperationBatch::Acknowledgement {
                reference: outbound.reference.clone(),
                value: outbound.ack.value.clone(),
                circle_acknowledgements: outbound.circle_acknowledgements.clone(),
            },
        )
        .await
        .unwrap();
    let retained = candidate.history_evidence.acknowledgement.as_ref().unwrap();
    assert_eq!(
        retained.acknowledgement,
        (outbound.reference.clone(), outbound.ack.value.clone())
    );
    assert_eq!(retained.activating_commit, candidate.reference);
    assert_eq!(
        database
            .oldest_outbound_store_ack()
            .await
            .unwrap()
            .unwrap()
            .reference,
        outbound.reference
    );
    assert_eq!(
        database
            .latest_local_store_ack()
            .await
            .unwrap()
            .unwrap()
            .reference,
        published.reference
    );
    assert!(database.active_store_publication().await.unwrap().is_none());
    assert_eq!(home.exact_create_count(), creates);
}

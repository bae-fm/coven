use super::super::history::abandonment::verify_merge_candidate_nonactivations;
use crate::sync::storage::{ProtocolObjectContext, ProtocolObjectDomain, StorageError};
use crate::sync::store::StoreError;
use crate::sync::store_commit::{
    commit_semantic_prefix, head_slot_prefix, StoreBatchCommitDeletionTarget, StoreDeviceHeadRef,
};
use crate::sync::store_objects::StoreObjectError;

use crate::sync::store::operations::{blocked_status, publish_prepared_remote_objects};

use super::AuthorizedWriterOperation;

pub(super) async fn drain_store_writes(
    operation: &mut AuthorizedWriterOperation<'_>,
) -> Result<u64, StoreError> {
    let database = &operation.store.database();
    // Each candidate here takes a position on this device's own stream by
    // publishing its head, so this waits its turn behind any operation composing
    // against that same position. Queued writes are the one composer that can
    // lose a position safely — they re-prepare against the winner — but nothing
    // else can, so they must not be the ones to take it out from under an
    // operation that is mid-activation.
    let _authorship = database.author_own_stream().await;
    database.retire_uploaded_blob_spools().await?;
    let Some(first) = database.oldest_prepared_store_write().await? else {
        return Ok(0);
    };
    drain_prepared_store_writes(operation, first).await
}

async fn drain_prepared_store_writes(
    operation: &mut AuthorizedWriterOperation<'_>,
    first: crate::database::PreparedStoreWriteCommit,
) -> Result<u64, StoreError> {
    let database = operation.store.database().clone();
    let storage = operation.store.storage();
    #[cfg(any(test, feature = "test-utils"))]
    let db = database.sqlite();
    let mut published = 0_u64;
    let mut next = Some(first);
    while let Some(batch) = next {
        let root = operation.store.store_root().clone();
        let write_id = batch.commit.value.write_id.clone();
        database
            .set_write_status(&write_id, crate::WriteStatus::Publishing)
            .await?;
        let attempt = async {
            Box::pin(operation.reject_excluded_merge_candidate(
                &batch.head.value.commit,
                &batch.commit.value.author_registration,
            ))
            .await?;
            let store_root_hash = root.store_root_hash;
            let commit = &batch.commit.value;
            if !matches!(
                commit.body,
                crate::sync::store_commit::StoreCommitBody::AbandonCandidates { .. }
            ) {
                publish_prepared_remote_objects(&database, storage, &write_id, store_root_hash)
                    .await?;
                database.retire_uploaded_blob_spools().await?;
            }
            let head = &batch.head.value;
            let stream_id = head.commit.coord.stream_id.to_string();
            let commit_context = ProtocolObjectContext::signed_plaintext(
                store_root_hash,
                ProtocolObjectDomain::StoreCommit,
            );
            let commit_prefix = commit_semantic_prefix(
                commit.candidate_family(),
                &stream_id,
                commit.seq(),
                commit.commit_hash(),
            );
            storage
                .create_protocol_object(&batch.commit.prepared)
                .await
                .map_err(StoreObjectError::from)?;
            let opened_commit = storage
                .read_protocol_object(&commit_context, &batch.commit.object, &commit_prefix)
                .await
                .map_err(StoreObjectError::from)?;
            if opened_commit != batch.commit.bytes {
                return Err(StoreError::InvalidOutbound(
                    "prepared commit exact readback differs from its signed bytes".to_string(),
                ));
            }
            database
                .mark_candidate_commit_uploaded(head.commit.clone())
                .await?;
            #[cfg(any(test, feature = "test-utils"))]
            db.reach_test_point(
                crate::database::DatabaseTestPoint::StoreWriteCommitUploaded {
                    write_id: write_id.clone(),
                },
            )
            .await;
            Box::pin(
                operation
                    .reject_excluded_merge_candidate(&head.commit, &commit.author_registration),
            )
            .await?;
            let head_prefix = head_slot_prefix(
                &head.author_registration.device_id.to_string(),
                commit.seq(),
            );
            if let Err(error) = storage.create_protocol_object(&batch.head.prepared).await {
                if !matches!(error, StorageError::SlotCollision(_)) {
                    return Err(StoreObjectError::from(error).into());
                }
                let observation = operation
                    .history()
                    .observe_occupied_merge_head(
                        head,
                        commit,
                        batch.head.object.slot(),
                        &head_prefix,
                    )
                    .await?;
                if observation.winner().commit == head.commit {
                    let registration = database
                        .activated_store_device_registration(head.author_registration.clone())
                        .await?;
                    let nonactivations = verify_merge_candidate_nonactivations(
                        &observation,
                        commit
                            .abandoned_candidates()
                            .iter()
                            .map(|manifest| manifest.candidate.clone()),
                        &registration,
                    )?;
                    let (winner, winner_prepared) = observation.into_head();
                    database
                        .adopt_alternate_merge_head(write_id.clone(), winner, winner_prepared)
                        .await?;
                    #[cfg(any(test, feature = "test-utils"))]
                    db.reach_test_point(
                        crate::database::DatabaseTestPoint::StoreWriteHeadReadBack {
                            write_id: write_id.clone(),
                        },
                    )
                    .await;
                    match database
                        .complete_prepared_store_write(
                            root.clone(),
                            head.commit.clone(),
                            nonactivations,
                        )
                        .await?
                    {
                        crate::database::CompletePreparedStoreWriteOutcome::Published => {}
                        crate::database::CompletePreparedStoreWriteOutcome::AuthorExcluded {
                            device_id,
                        } => return Err(StoreError::AuthorExcluded { device_id }),
                    }
                    return Ok::<bool, StoreError>(true);
                }
                let registration = database
                    .activated_store_device_registration(head.author_registration.clone())
                    .await?;
                let nonactivations = verify_merge_candidate_nonactivations(
                    &observation,
                    std::iter::once(StoreBatchCommitDeletionTarget {
                        coord: head.commit.coord.clone(),
                        object: head.commit.object.clone(),
                        canonical_signed_bytes: commit.to_bytes(),
                    })
                    .chain(
                        commit
                            .abandoned_candidates()
                            .iter()
                            .map(|manifest| manifest.candidate.clone()),
                    ),
                    &registration,
                )?;
                database
                    .mark_merge_candidate_conflict(write_id.clone(), nonactivations)
                    .await?;
                return Ok::<bool, StoreError>(false);
            }
            let observation = operation
                .history()
                .observe_occupied_merge_head(head, commit, batch.head.object.slot(), &head_prefix)
                .await?;
            if observation.winner() != head
                || observation.winner_prepared().reference() != &batch.head.object
            {
                return Err(StoreError::InvalidOutbound(
                    "prepared head exact readback differs from its signed bytes".to_string(),
                ));
            }
            let registration = database
                .activated_store_device_registration(head.author_registration.clone())
                .await?;
            let nonactivations = verify_merge_candidate_nonactivations(
                &observation,
                commit
                    .abandoned_candidates()
                    .iter()
                    .map(|manifest| manifest.candidate.clone()),
                &registration,
            )?;
            database
                .mark_store_head_uploaded(StoreDeviceHeadRef {
                    head_hash: head.head_hash(),
                    object: batch.head.object.clone(),
                })
                .await?;
            #[cfg(any(test, feature = "test-utils"))]
            db.reach_test_point(crate::database::DatabaseTestPoint::StoreWriteHeadReadBack {
                write_id: write_id.clone(),
            })
            .await;
            match database
                .complete_prepared_store_write(root, head.commit.clone(), nonactivations)
                .await?
            {
                crate::database::CompletePreparedStoreWriteOutcome::Published => {}
                crate::database::CompletePreparedStoreWriteOutcome::AuthorExcluded {
                    device_id,
                } => return Err(StoreError::AuthorExcluded { device_id }),
            }
            Ok::<bool, StoreError>(true)
        }
        .await;
        match attempt {
            Ok(false) => return Ok(published),
            Ok(true) => {}
            Err(error) => {
                if let Some(block) = blocked_status(&error) {
                    database.block_write_if_unresolved(&write_id, block).await?;
                }
                return Err(error);
            }
        }
        published = published
            .checked_add(1)
            .ok_or_else(|| StoreError::Database("publish count exceeded u64".into()))?;
        next = database.oldest_prepared_store_write().await?;
    }
    Ok(published)
}

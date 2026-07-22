use super::abandonment::{read_occupied_merge_head, verify_merge_candidate_nonactivations};
use super::*;
use crate::sync::storage::{ProtocolObjectContext, ProtocolObjectDomain, StorageError};
use crate::sync::store_commit::{
    commit_semantic_prefix, head_slot_prefix, StoreBatchCommitDeletionTarget, StoreDeviceHeadRef,
};
use crate::sync::store_objects::StoreObjectError;
use crate::sync::store_outbound::StoreOutboundError;

use super::operations::{
    blocked_status, publish_prepared_remote_objects, reject_excluded_merge_candidate,
    required_store_root,
};

pub(crate) async fn drain_store_writes(
    db: &Database,
    storage: &dyn SyncStorage,
) -> Result<u64, StoreOutboundError> {
    let mut published = 0_u64;
    while let Some(batch) = db.oldest_prepared_store_write().await? {
        let write_id = batch.commit.value.write_id.clone();
        db.set_write_status(&write_id, crate::WriteStatus::Publishing)
            .await?;
        let attempt = async {
            Box::pin(reject_excluded_merge_candidate(
                db,
                &batch.head.value.commit,
                &batch.commit.value.author_registration,
            ))
            .await?;
            let store_root_hash = required_store_root(db).await?.store_root_hash;
            let commit = &batch.commit.value;
            if !matches!(
                commit.body,
                crate::sync::store_commit::StoreCommitBody::AbandonCandidates { .. }
            ) {
                publish_prepared_remote_objects(db, storage, &write_id, store_root_hash).await?;
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
                return Err(StoreOutboundError::InvalidOutbound(
                    "prepared commit exact readback differs from its signed bytes".to_string(),
                ));
            }
            db.mark_candidate_commit_uploaded(head.commit.clone())
                .await?;
            #[cfg(any(test, feature = "test-utils"))]
            db.reach_test_point(
                crate::database::DatabaseTestPoint::StoreWriteCommitUploaded {
                    write_id: write_id.clone(),
                },
            )
            .await;
            Box::pin(reject_excluded_merge_candidate(
                db,
                &head.commit,
                &commit.author_registration,
            ))
            .await?;
            let head_prefix = head_slot_prefix(
                &head.author_registration.device_id.to_string(),
                commit.seq(),
            );
            if let Err(error) = storage.create_protocol_object(&batch.head.prepared).await {
                if !matches!(error, StorageError::SlotCollision(_)) {
                    return Err(StoreObjectError::from(error).into());
                }
                let observation = read_occupied_merge_head(
                    db,
                    storage,
                    store_root_hash,
                    head,
                    commit,
                    batch.head.object.slot(),
                    &head_prefix,
                )
                .await?;
                if observation.winner().commit == head.commit {
                    let registration = db
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
                    db.adopt_alternate_merge_head(write_id.clone(), winner, winner_prepared)
                        .await?;
                    #[cfg(any(test, feature = "test-utils"))]
                    db.reach_test_point(
                        crate::database::DatabaseTestPoint::StoreWriteHeadReadBack {
                            write_id: write_id.clone(),
                        },
                    )
                    .await;
                    match db
                        .complete_prepared_store_write(head.commit.clone(), nonactivations)
                        .await?
                    {
                        crate::database::CompletePreparedStoreWriteOutcome::Published => {}
                        crate::database::CompletePreparedStoreWriteOutcome::AuthorExcluded {
                            device_id,
                        } => return Err(StoreOutboundError::AuthorExcluded { device_id }),
                    }
                    return Ok::<bool, StoreOutboundError>(true);
                }
                let registration = db
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
                db.mark_merge_candidate_conflict(write_id.clone(), nonactivations)
                    .await?;
                return Ok::<bool, StoreOutboundError>(false);
            }
            let observation = read_occupied_merge_head(
                db,
                storage,
                store_root_hash,
                head,
                commit,
                batch.head.object.slot(),
                &head_prefix,
            )
            .await?;
            if observation.winner() != head
                || observation.winner_prepared().reference() != &batch.head.object
            {
                return Err(StoreOutboundError::InvalidOutbound(
                    "prepared head exact readback differs from its signed bytes".to_string(),
                ));
            }
            let registration = db
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
            db.mark_store_head_uploaded(StoreDeviceHeadRef {
                head_hash: head.head_hash(),
                object: batch.head.object.clone(),
            })
            .await?;
            #[cfg(any(test, feature = "test-utils"))]
            db.reach_test_point(crate::database::DatabaseTestPoint::StoreWriteHeadReadBack {
                write_id: write_id.clone(),
            })
            .await;
            match db
                .complete_prepared_store_write(head.commit.clone(), nonactivations)
                .await?
            {
                crate::database::CompletePreparedStoreWriteOutcome::Published => {}
                crate::database::CompletePreparedStoreWriteOutcome::AuthorExcluded {
                    device_id,
                } => return Err(StoreOutboundError::AuthorExcluded { device_id }),
            }
            Ok::<bool, StoreOutboundError>(true)
        }
        .await;
        match attempt {
            Ok(false) => return Ok(published),
            Ok(true) => {}
            Err(error) => {
                if let Some(block) = blocked_status(&error) {
                    db.block_write_if_unresolved(&write_id, block).await?;
                }
                return Err(error);
            }
        }
        published = published
            .checked_add(1)
            .ok_or_else(|| StoreOutboundError::Database("publish count exceeded u64".into()))?;
    }
    Ok(published)
}

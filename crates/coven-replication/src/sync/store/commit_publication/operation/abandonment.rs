use super::*;
use crate::sync::store::commit_verification::merge_history::prepare_merge_abandonment_history_summary;
use crate::sync::store::merge_conflict::MergeCandidateAbandonment;
use coven_database::{MergeCandidateAbandonmentPreparation, PreparedProtocolObject};
use coven_protocol::objects::{ProtocolObjectContext, ProtocolObjectDomain, StoreObjectError};
use coven_protocol::store_commit::{
    commit_semantic_prefix, head_slot_prefix, CandidateCleanupManifest,
    StoreBatchCommitDeletionTarget,
};
use std::sync::Arc;

impl AuthorizedWriterOperation<'_> {
    pub(crate) async fn prepare_merge_candidate_abandonment(
        &mut self,
        write_id: coven_protocol::write::WriteId,
    ) -> Result<bool, StoreError> {
        let database = self.database.clone();
        let Some(candidate) = database.blocked_merge_candidate(write_id.clone()).await? else {
            return Ok(false);
        };
        let candidate_summary = database
            .blocked_merge_history_summary(write_id.clone())
            .await?;
        let device_id = self.local_device_id().to_string();
        let root = self.store_root().clone();
        let storage = Arc::clone(self.storage);
        if !self
            .writer
            .is_authored_by_registration(&candidate.commit.author_registration)
        {
            return Err(StoreError::InvalidOutbound(
                "blocked Merge candidate belongs to another local registration".to_string(),
            ));
        }
        let coord = candidate.head.commit.coord.clone();
        let commit = self
            .writer
            .sign_candidate_abandonment(
                root.store_root_hash,
                write_id.clone(),
                coord.clone(),
                candidate.commit.order.clone(),
                candidate.commit.membership_state.clone(),
                candidate.commit.device_state.clone(),
                vec![CandidateCleanupManifest {
                    candidate: StoreBatchCommitDeletionTarget {
                        coord: coord.clone(),
                        object: candidate.commit_object.clone(),
                        canonical_signed_bytes: candidate.commit_bytes.clone(),
                    },
                }],
            )
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        let stream_id = coord.stream_id;
        let sequence = coord.sequence;
        let commit_context = ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::StoreCommit,
        );
        let commit_prefix = commit_semantic_prefix(
            commit.candidate_family(),
            &stream_id.to_string(),
            sequence,
            commit.commit_hash(),
        );
        let commit_slot = storage
            .allocate_protocol_slot(&commit_context, &commit_prefix, ".json")
            .await
            .map_err(StoreObjectError::from)?;
        let commit_prepared = storage
            .prepare_protocol_object(
                &commit_context,
                commit_slot,
                &commit_prefix,
                commit.to_bytes(),
            )
            .map_err(StoreObjectError::from)?;
        let commit = self
            .writer
            .verify_prepared_commit(
                &commit.to_bytes(),
                root.store_root_hash,
                coord,
                commit_prepared.reference().clone(),
            )
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        let commit_ref = commit.reference().clone();
        let history_summary = prepare_merge_abandonment_history_summary(
            &candidate_summary,
            &candidate.commit,
            &commit,
        )
        .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        let head = self.writer.sign_device_head(
            root.store_root_hash,
            commit_ref,
            history_summary.digest(),
            candidate.head.successor.clone(),
        )?;
        let head_context = ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::StoreHead,
        );
        let head_prefix = head_slot_prefix(&device_id, sequence);
        let head_prepared = storage
            .prepare_protocol_object(
                &head_context,
                candidate.head_object.slot().clone(),
                &head_prefix,
                head.to_bytes(),
            )
            .map_err(StoreObjectError::from)?;
        database
            .prepare_merge_candidate_abandonment(MergeCandidateAbandonmentPreparation {
                write_id,
                commit: PreparedProtocolObject {
                    value: commit,
                    prepared: commit_prepared,
                },
                head: PreparedProtocolObject {
                    value: head,
                    prepared: head_prepared,
                },
                history_summary,
            })
            .await?;
        Ok(true)
    }

    pub(crate) async fn abandon_merge_candidate(
        &mut self,
        write_id: coven_protocol::write::WriteId,
    ) -> Result<MergeCandidateAbandonment, StoreError> {
        let root = self.store_root().clone();
        let database = self.database.clone();
        match database.merge_abandonment_state(&write_id).await? {
            coven_database::MergeAbandonmentState::None => {
                if database.merge_candidate_cleanup_pending(&write_id).await? {
                    self.cleanup_merge_candidate(write_id.clone()).await?;
                    database
                        .finish_retracted_merge_candidate_cleanup(write_id.clone())
                        .await?;
                    return Ok(MergeCandidateAbandonment::Abandoned);
                }
                if matches!(
                    database.write_status(&write_id).await?,
                    coven_protocol::write::WriteStatus::Resolved(_)
                ) {
                    return Ok(MergeCandidateAbandonment::NotRequired);
                }
                if let Some(candidate) = database.blocked_merge_candidate(write_id.clone()).await? {
                    if let Some(nonactivation) =
                        self.blocked_candidate_nonactivation(&candidate).await?
                    {
                        database
                            .begin_blocked_merge_candidate_nonactivation(
                                root.clone(),
                                write_id.clone(),
                                nonactivation,
                            )
                            .await?;
                        self.cleanup_merge_candidate(write_id.clone()).await?;
                        return Ok(MergeCandidateAbandonment::Abandoned);
                    }
                }
                if !self
                    .prepare_merge_candidate_abandonment(write_id.clone())
                    .await?
                {
                    return Ok(MergeCandidateAbandonment::NotRequired);
                }
            }
            coven_database::MergeAbandonmentState::Prepared => {
                let candidates = database
                    .prepared_merge_abandonment_candidates(write_id.clone())
                    .await?
                    .ok_or_else(|| {
                        StoreError::InvalidOutbound(
                            "prepared Merge abandonment has no exact candidates".to_string(),
                        )
                    })?;
                let candidate = self
                    .blocked_candidate_nonactivation(&candidates.candidate)
                    .await?;
                let authority = self
                    .blocked_candidate_nonactivation(&candidates.authority)
                    .await?;
                match (candidate, authority) {
                    (Some(candidate), Some(authority)) => {
                        database
                            .begin_prepared_merge_abandonment_nonactivation(
                                root.clone(),
                                write_id.clone(),
                                candidate,
                                authority,
                            )
                            .await?;
                        self.cleanup_merge_candidate(write_id.clone()).await?;
                        database
                            .finish_author_excluded_merge_abandonment(write_id)
                            .await?;
                        return Ok(MergeCandidateAbandonment::Abandoned);
                    }
                    (None, None) => {}
                    _ => {
                        return Err(StoreError::InvalidOutbound(
                            "prepared Merge abandonment candidates disagree on author exclusion"
                                .to_string(),
                        ));
                    }
                }
            }
            coven_database::MergeAbandonmentState::Accepted
            | coven_database::MergeAbandonmentState::CandidateWon
            | coven_database::MergeAbandonmentState::OtherWon => {
                if database.merge_candidate_cleanup_pending(&write_id).await? {
                    self.cleanup_merge_candidate(write_id.clone()).await?;
                }
                return self.finish_merge_abandonment(write_id).await;
            }
            coven_database::MergeAbandonmentState::AuthorExcluded => {
                if database.merge_candidate_cleanup_pending(&write_id).await? {
                    self.cleanup_merge_candidate(write_id.clone()).await?;
                }
                database
                    .finish_author_excluded_merge_abandonment(write_id)
                    .await?;
                return Ok(MergeCandidateAbandonment::Abandoned);
            }
        }
        self.drain_prepared_store_writes().await?;
        if !database.merge_candidate_cleanup_pending(&write_id).await? {
            return Err(StoreError::InvalidOutbound(
                "accepted Merge abandonment has no exact cleanup transition".to_string(),
            ));
        }
        self.cleanup_merge_candidate(write_id.clone()).await?;
        self.finish_merge_abandonment(write_id).await
    }

    async fn finish_merge_abandonment(
        &mut self,
        write_id: coven_protocol::write::WriteId,
    ) -> Result<MergeCandidateAbandonment, StoreError> {
        let database = self.database.clone();
        match database.merge_abandonment_state(&write_id).await? {
            coven_database::MergeAbandonmentState::None
            | coven_database::MergeAbandonmentState::Accepted => {
                Ok(MergeCandidateAbandonment::Abandoned)
            }
            coven_database::MergeAbandonmentState::OtherWon => {
                database.finish_lost_merge_abandonment(write_id).await?;
                Ok(MergeCandidateAbandonment::Abandoned)
            }
            coven_database::MergeAbandonmentState::CandidateWon => {
                database.resume_winning_merge_candidate(write_id).await?;
                self.drain_prepared_store_writes().await?;
                Ok(MergeCandidateAbandonment::CandidateActivated)
            }
            coven_database::MergeAbandonmentState::Prepared => Err(StoreError::InvalidOutbound(
                "Merge abandonment has no accepted head outcome".to_string(),
            )),
            coven_database::MergeAbandonmentState::AuthorExcluded => {
                if database.merge_candidate_cleanup_pending(&write_id).await? {
                    self.cleanup_merge_candidate(write_id.clone()).await?;
                }
                database
                    .finish_author_excluded_merge_abandonment(write_id)
                    .await?;
                Ok(MergeCandidateAbandonment::Abandoned)
            }
        }
    }

    pub(super) async fn cleanup_merge_candidate(
        &mut self,
        write_id: coven_protocol::write::WriteId,
    ) -> Result<(), crate::sync::store::pull::StorePullError> {
        self.cleanup_merge_candidate_history(write_id).await
    }
}

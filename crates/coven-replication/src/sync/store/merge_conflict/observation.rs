use super::abandonment;
use super::MergeConflictHistory;
use crate::sync::store::StoreError;

impl MergeConflictHistory<'_, '_> {
    pub(crate) async fn observe_occupied_merge_head(
        &mut self,
        expected: &coven_protocol::store_commit::StoreDeviceHead,
        expected_commit: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
        slot: &coven_protocol::objects::ObjectSlot,
        semantic_prefix: &str,
    ) -> Result<abandonment::VerifiedMergeWinner, StoreError> {
        let store_root_hash = self.history.verified_root().reference().store_root_hash;
        let context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
            store_root_hash,
            coven_protocol::objects::ProtocolObjectDomain::StoreHead,
        );
        let (winner_bytes, winner_prepared) = self
            .storage
            .read_prepared_protocol_slot(&context, slot, semantic_prefix)
            .await
            .map_err(coven_protocol::objects::StoreObjectError::from)?;
        let unverified: coven_protocol::store_commit::StoreDeviceHead =
            serde_json::from_slice(&winner_bytes).map_err(|error| {
                StoreError::InvalidOutbound(format!("parse competing Merge head: {error}"))
            })?;
        if unverified.author_registration != expected.author_registration
            || unverified.commit.coord != expected.commit.coord
            || unverified.successor.activation != expected.successor.activation
            || unverified.successor.predecessor != expected.successor.predecessor
        {
            return Err(StoreError::InvalidOutbound(
                "competing Merge head does not occupy the prepared successor point".to_string(),
            ));
        }
        let registration = self
            .database
            .activated_store_device_registration(expected.author_registration.clone())
            .await?;
        if expected_commit.store_root_hash() != store_root_hash
            || expected_commit.reference() != &expected.commit
            || expected_commit.author() != registration.value()
        {
            return Err(StoreError::InvalidOutbound(
                "expected Merge head differs from its authenticated commit".to_string(),
            ));
        }
        coven_protocol::store_commit::StoreDeviceHead::parse_at(
            &expected.to_bytes(),
            store_root_hash,
            registration.value(),
            &expected.commit,
        )
        .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        let winner_commit = self.history.load_ref(&unverified.commit).await?;
        if winner_commit.author() != registration.value() {
            return Err(StoreError::InvalidOutbound(
                "occupied Merge head commit has a different authenticated author".to_string(),
            ));
        }
        let winner = coven_protocol::store_commit::StoreDeviceHead::parse_at(
            &winner_bytes,
            store_root_hash,
            registration.value(),
            &unverified.commit,
        )
        .map_err(|error| {
            StoreError::InvalidOutbound(format!("verify occupied Merge head: {error}"))
        })?;
        Ok(abandonment::VerifiedMergeWinner::from_verified_parts(
            store_root_hash,
            slot.clone(),
            expected.clone(),
            expected_commit.clone(),
            winner,
            winner_prepared,
            winner_commit,
        ))
    }

    pub(crate) async fn observe_excluded_candidate_head(
        &mut self,
        candidate: &coven_protocol::store_commit::StoreDeviceHead,
        candidate_commit: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
        candidate_object: &coven_protocol::objects::ExactObjectRef,
    ) -> Result<abandonment::ExcludedCandidateHeadObservation, StoreError> {
        let store_root_hash = self.history.verified_root().reference().store_root_hash;
        let context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
            store_root_hash,
            coven_protocol::objects::ProtocolObjectDomain::StoreHead,
        );
        let prefix = coven_protocol::store_commit::head_slot_prefix(
            &candidate.author_registration.device_id.to_string(),
            candidate.commit.coord.sequence(),
        );
        match self
            .storage
            .read_protocol_slot(&context, candidate_object.slot(), &prefix)
            .await
        {
            Err(coven_protocol::objects::StorageError::NotFound(_)) => {
                Ok(abandonment::ExcludedCandidateHeadObservation::AuthorExclusion)
            }
            Ok((bytes, object)) if bytes == candidate.to_bytes() && object == *candidate_object => {
                Ok(abandonment::ExcludedCandidateHeadObservation::AuthorExclusion)
            }
            Ok(_) => self
                .observe_occupied_merge_head(
                    candidate,
                    candidate_commit,
                    candidate_object.slot(),
                    &prefix,
                )
                .await
                .map(abandonment::ExcludedCandidateHeadObservation::MergeWinner),
            Err(error) => Err(coven_protocol::objects::StoreObjectError::Storage(error).into()),
        }
    }
}

use crate::protocol::objects::PreparedExactObject;
use crate::protocol::store_commit::{
    ObjectHash, StoreBatchCommitDeletionTarget, StoreDeviceHead, StoreDeviceRegistration,
    VerifiedStoreBatchCommit,
};
use crate::sync::store::StoreError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MergeCandidateAbandonment {
    NotRequired,
    Abandoned,
    CandidateActivated,
}

/// The nonactivation proof for discarding a candidate whose slot is already
/// resolved. Unlike Merge abandonment, discard never publishes an abandonment
/// commit to race for the slot — it is invoked after the slot is lost, so it
/// observes the outcome directly. A different verified winner occupying the
/// successor slot is a standalone proof (the candidate is bound to that
/// create-once slot and can never take it), independent of the author's status.
/// Author exclusion covers a slot the author was excluded from before anyone
/// claimed it. An accepted Store commit whose membership state tombstones the
/// candidate's exact grant and whose predecessor cut excludes the candidate is
/// the membership-revocation proof.
/// Publish the exact prepared object graph in sequence order. Every remote object
/// is verified at its reserved slot before the exact head activates the commit.
#[derive(Debug, Clone)]
pub(crate) struct VerifiedMergeWinner {
    store_root_hash: ObjectHash,
    expected_slot: crate::protocol::objects::ObjectSlot,
    expected: StoreDeviceHead,
    expected_commit: Box<VerifiedStoreBatchCommit>,
    winner: StoreDeviceHead,
    winner_prepared: PreparedExactObject,
    winner_commit: Box<VerifiedStoreBatchCommit>,
}

impl VerifiedMergeWinner {
    pub(crate) fn from_verified_parts(
        store_root_hash: ObjectHash,
        expected_slot: crate::protocol::objects::ObjectSlot,
        expected: StoreDeviceHead,
        expected_commit: VerifiedStoreBatchCommit,
        winner: StoreDeviceHead,
        winner_prepared: PreparedExactObject,
        winner_commit: VerifiedStoreBatchCommit,
    ) -> Self {
        Self {
            store_root_hash,
            expected_slot,
            expected,
            expected_commit: Box::new(expected_commit),
            winner,
            winner_prepared,
            winner_commit: Box::new(winner_commit),
        }
    }

    pub(crate) fn verified_nonactivation(
        &self,
        candidate: StoreBatchCommitDeletionTarget,
        author: &StoreDeviceRegistration,
    ) -> Result<
        crate::protocol::remote_object::VerifiedCandidateNonactivation,
        crate::protocol::remote_object::RemoteObjectRecordError,
    > {
        let commit = candidate
            .verify_nonactivation_candidate(self.store_root_hash, author)
            .map_err(|error| {
                crate::protocol::remote_object::RemoteObjectRecordError::InvalidProof(
                    error.to_string(),
                )
            })?;
        let reference = commit.reference().clone();
        if self.expected.store_root_hash != self.store_root_hash
            || commit.store_root_hash != self.store_root_hash
            || self.expected.author_registration != commit.author_registration
            || self.expected.commit.coord != reference.coord
            || self.expected_commit.author_registration != commit.author_registration
            || self.expected_commit.order.predecessor() != commit.order.predecessor()
            || self.winner.store_root_hash != self.store_root_hash
            || self.winner.author_registration != self.expected.author_registration
            || self.winner.commit.coord != self.expected.commit.coord
            || self.winner.successor.activation != self.expected.successor.activation
            || self.winner.successor.predecessor != self.expected.successor.predecessor
            || self.winner_prepared.reference().slot() != &self.expected_slot
            || self.winner.commit == reference
            || self.winner.commit != *self.winner_commit.reference()
        {
            return Err(
                crate::protocol::remote_object::RemoteObjectRecordError::InvalidProof(
                    "Merge winner observation is not bound to the losing candidate's exact activation point"
                        .to_string(),
                ),
            );
        }
        crate::protocol::remote_object::VerifiedCandidateNonactivation::from_verified_merge_winner(
            candidate,
            crate::protocol::store_commit::StoreDeviceHeadRef {
                head_hash: self.winner.head_hash(),
                object: self.winner_prepared.reference().clone(),
            },
            self.winner.commit.clone(),
        )
    }

    pub(crate) fn verified_nonactivations(
        &self,
        targets: impl IntoIterator<Item = StoreBatchCommitDeletionTarget>,
        author: &StoreDeviceRegistration,
    ) -> Result<Vec<crate::protocol::remote_object::VerifiedCandidateNonactivation>, StoreError>
    {
        let mut nonactivations = Vec::new();
        for target in targets {
            if target.coord == self.winner.commit.coord
                && target.object == self.winner.commit.object
                && target.canonical_signed_bytes == self.winner_commit.to_bytes()
            {
                continue;
            }
            nonactivations.push(
                self.verified_nonactivation(target, author)
                    .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?,
            );
        }
        Ok(nonactivations)
    }

    pub(crate) fn winner(&self) -> &StoreDeviceHead {
        &self.winner
    }

    pub(crate) fn winner_prepared(&self) -> &PreparedExactObject {
        &self.winner_prepared
    }

    pub(crate) fn into_head(self) -> (StoreDeviceHead, PreparedExactObject) {
        (self.winner, self.winner_prepared)
    }

    #[cfg(test)]
    pub(crate) fn winner_commit(&self) -> &VerifiedStoreBatchCommit {
        &self.winner_commit
    }

    #[cfg(test)]
    pub(crate) fn winner_mut_for_test(&mut self) -> &mut StoreDeviceHead {
        &mut self.winner
    }

    #[cfg(test)]
    pub(crate) fn set_expected_slot_for_test(
        &mut self,
        expected_slot: crate::protocol::objects::ObjectSlot,
    ) {
        self.expected_slot = expected_slot;
    }
}

pub(crate) enum ExcludedCandidateHeadObservation {
    AuthorExclusion,
    MergeWinner(VerifiedMergeWinner),
}

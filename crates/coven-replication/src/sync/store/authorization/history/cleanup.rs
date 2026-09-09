use super::*;
use crate::sync::store::device_join::DeviceJoinError;

impl AuthorizedStoreHistory<'_> {
    pub(crate) async fn materialize_device_join_activation(
        &mut self,
        reference: &StoreBatchCommitRef,
        attempt_id: coven_protocol::store_commit::DeviceJoinAttemptId,
        membership: &mut MembershipChain,
        identity: &UserKeypair,
    ) -> Result<(), DeviceJoinError> {
        let verified_commit = self.history_verifier.load_ref(reference).await?;
        pull::verify_device_join_activation_commit(verified_commit.value(), attempt_id)?;
        let pulled = self
            .install_current_publication(membership, identity)
            .await?;
        if self
            .database
            .installed_store_commit_evidence(verified_commit)
            .await?
            .is_none()
        {
            if !pulled.held_positions.is_empty() {
                return Err(StoreError::PublicationHeld(pulled.held_positions).into());
            }
            return Err(DeviceJoinError::ActivationNotMaterialized);
        }
        Ok(())
    }
}

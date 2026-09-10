use crate::sync::store::membership::AnchoredChainError;
use coven_protocol::membership::{MembershipChain, MembershipHeadRef};
use coven_protocol::store_commit::{
    OwnerPromotionAcceptance, OwnerPromotionRequest, StoreDeviceRegistration,
    StoreDeviceRegistrationRef,
};

use crate::sync::store::commit_verification::merge_history::{
    MergeHistoryVerifier, VerifiedOwnerPromotionRequestActivation,
};
use crate::sync::store::pull::StorePullError;

pub(crate) struct OwnerPromotionHistory<'operation, 'storage> {
    database: coven_database::StoreDatabase,
    history: &'operation mut MergeHistoryVerifier<'storage>,
}

impl<'operation, 'storage> OwnerPromotionHistory<'operation, 'storage> {
    pub(crate) async fn candidate_grant_retirement(
        &mut self,
        candidate: &coven_protocol::prepared_commit::PreparedStoreOperationCommit,
    ) -> Result<
        Option<(
            MembershipChain,
            coven_protocol::store_commit::StorePublicationRef,
        )>,
        StorePullError,
    > {
        let verified = self
            .history
            .authenticate_bytes(&candidate.reference, &candidate.commit.to_bytes())
            .await?;
        self.history
            .candidate_grant_retirement(&self.database, &verified)
            .await
    }

    pub(crate) async fn load_request_publication(
        &self,
        commit: &coven_protocol::store_commit::StoreBatchCommit,
    ) -> Result<
        coven_protocol::store_commit::RetainedOwnerPromotionRequestPublication,
        StorePullError,
    > {
        self.history
            .load_owner_promotion_request_publication(commit)
            .await
    }

    pub(crate) fn new(
        database: coven_database::StoreDatabase,
        history: &'operation mut MergeHistoryVerifier<'storage>,
    ) -> Self {
        Self { database, history }
    }

    pub(crate) async fn find_request_activation(
        &mut self,
        request: &OwnerPromotionRequest,
    ) -> Result<VerifiedOwnerPromotionRequestActivation, StorePullError> {
        let observed = self.database.store_current_publication().await?;
        let publication = self
            .history
            .load_store_publication_interval(&observed)
            .await?;
        self.history
            .verify_refs(publication.commits.into_keys())
            .await?;
        self.history
            .find_owner_promotion_request_activation(request)
            .await
    }

    pub(crate) async fn verify_acceptance(
        &mut self,
        acceptance: &OwnerPromotionAcceptance,
    ) -> Result<(), StorePullError> {
        self.history
            .verify_owner_promotion_acceptance_with_history(acceptance)
            .await
    }

    pub(crate) async fn verify_acceptance_from_request(
        &mut self,
        acceptance: &OwnerPromotionAcceptance,
        verified: VerifiedOwnerPromotionRequestActivation,
    ) -> Result<(), StorePullError> {
        self.history
            .verify_owner_promotion_acceptance_from_request_activation(acceptance, verified)
            .await
    }

    pub(crate) async fn load_registration(
        &mut self,
        reference: &StoreDeviceRegistrationRef,
    ) -> Result<
        coven_protocol::objects::VerifiedObject<StoreDeviceRegistration>,
        coven_protocol::objects::StoreObjectError,
    > {
        self.history.load_registration(reference).await
    }

    pub(crate) async fn load_membership(
        &mut self,
        heads: &[MembershipHeadRef],
    ) -> Result<MembershipChain, AnchoredChainError> {
        self.history.load_membership_at_exact_heads(heads).await
    }
}

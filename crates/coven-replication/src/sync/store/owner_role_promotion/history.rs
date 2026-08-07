use crate::sync::store::membership::AnchoredChainError;
use coven_protocol::membership::{
    MembershipChain, MembershipHeadRef, StoreMembershipConflictResolutionRef,
};
use coven_protocol::store_commit::{
    OwnerPromotionAcceptance, OwnerPromotionRequest, StoreDeviceRegistration,
    StoreDeviceRegistrationRef,
};

use crate::sync::store::owner::pull::StorePullError;
use crate::sync::store::owner::verified_history::{
    MergeHistoryVerifier, VerifiedOwnerPromotionRequestActivation,
};

pub(crate) struct OwnerPromotionHistory<'operation, 'storage> {
    history: &'operation mut MergeHistoryVerifier<'storage>,
}

impl<'operation, 'storage> OwnerPromotionHistory<'operation, 'storage> {
    pub(crate) fn new(history: &'operation mut MergeHistoryVerifier<'storage>) -> Self {
        Self { history }
    }

    pub(crate) async fn find_request_activation(
        &mut self,
        request: &OwnerPromotionRequest,
    ) -> Result<VerifiedOwnerPromotionRequestActivation, StorePullError> {
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
        resolutions: &[StoreMembershipConflictResolutionRef],
    ) -> Result<MembershipChain, AnchoredChainError> {
        self.history
            .load_membership_at_exact_heads(heads, resolutions)
            .await
    }
}

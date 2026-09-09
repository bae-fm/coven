use super::*;
use crate::sync::store::commit_verification::merge_history::MergeHistoryVerifier;
use coven_protocol::store_commit::{StoreDeviceRegistrationActivationRef, StorePublicationPayload};

/// A rooted membership walk and the acceptance results whose issuers it verified.
/// Retaining this capability lets admission consume its exact Join acceptance
/// without rediscovering authority or borrowing a later provider revision.
pub struct AcceptedMembershipAuthority {
    root: StoreRootRef,
    membership: MembershipChain,
    device_authority: AcceptedDeviceAuthority,
}

impl AcceptedMembershipAuthority {
    pub(super) fn new(
        root: StoreRootRef,
        membership: MembershipChain,
        device_authority: AcceptedDeviceAuthority,
    ) -> Self {
        Self {
            root,
            membership,
            device_authority,
        }
    }

    pub fn chain(&self) -> &MembershipChain {
        &self.membership
    }

    pub(crate) fn verify_join_bootstrap(
        &self,
        root: &StoreRootRef,
        plan: &coven_database::DeviceJoinBootstrapPlan,
    ) -> Result<(), AnchoredChainError> {
        let invalid = || {
            AnchoredChainError::LoadFailed(
                "carried Join prefix differs from its rooted registration acceptance".into(),
            )
        };
        if &self.root != root {
            return Err(invalid());
        }
        let terminal = plan
            .publication
            .interval()
            .entries()
            .last()
            .ok_or_else(invalid)?;
        let StorePublicationPayload::Commit(reference) = &terminal.entry().payload else {
            return Err(invalid());
        };
        let commit = plan
            .commits
            .iter()
            .find(|commit| &commit.reference == reference)
            .ok_or_else(invalid)?;
        commit
            .history_evidence
            .validate_for(reference, commit.commit.value())
            .map_err(|error| AnchoredChainError::LoadFailed(error.to_string()))?;
        let proof = commit
            .history_evidence
            .membership_proof
            .as_ref()
            .ok_or_else(invalid)?;
        let [registration] = commit.commit.device_registrations() else {
            return Err(invalid());
        };
        if !matches!(
            registration.authority,
            StoreDeviceRegistrationActivationRef::Join { .. }
        ) || !self
            .membership
            .device_registration_activations()
            .any(|(coord, activated)| coord == &proof.head.coord && activated == registration)
        {
            return Err(invalid());
        }
        let receipt = self.device_authority.receipt(&proof.head)?;
        if receipt.publication()? != terminal.reference()
            || plan.publication.interval().current() != &receipt.accepted_current
        {
            return Err(invalid());
        }
        Ok(())
    }
}

impl MergeHistoryVerifier<'_> {
    pub async fn load_accepted_membership_authority(
        &self,
        heads: &[MembershipHeadRef],
        owner: Option<&str>,
    ) -> Result<AcceptedMembershipAuthority, AnchoredChainError> {
        AcceptedMembershipActivation::new(&self.root, &self.commit_verifier)
            .load_anchored_authority(heads, owner)
            .await
    }
}

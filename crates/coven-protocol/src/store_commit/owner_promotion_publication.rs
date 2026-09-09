use super::*;

impl SignedBody for OwnerPromotionRequestActivation {
    const DOMAIN: &'static [u8] = b"coven.owner-promotion-request-publication.v1\0";
}

/// The original promoter's statement of the request's winning publication.
/// Its publisher signs this after observing the exact accepted provider position.
pub type OwnerPromotionRequestPublication = Signed<OwnerPromotionRequestActivation>;

impl OwnerPromotionRequestActivation {
    pub fn validate_for_request_commit(
        &self,
        commit: &StoreBatchCommit,
    ) -> Result<(), StoreProtocolError> {
        let request = commit
            .owner_promotion_request()
            .ok_or(StoreProtocolError::OwnerPromotionMismatch)?;
        self.commit.verify_commit(commit)?;
        self.commit.object.verify(&commit.to_bytes())?;
        self.publication.validate_slot()?;
        StorePublicationPosition::new(self.publication.position.get())?;
        if self.publication.store_root_hash != request.store_root_hash
            || commit.store_root_hash != request.store_root_hash
            || commit.author_registration != request.promoter_registration
            || commit.membership_state != request.predecessor_membership
            || commit.device_state != request.predecessor_devices
        {
            return Err(StoreProtocolError::OwnerPromotionMismatch);
        }
        Ok(())
    }
}

impl OwnerPromotionRequestPublication {
    pub fn signed(
        commit: &StoreBatchCommit,
        accepted_entry: &StorePublicationEntry,
        accepted_reference: &StorePublicationRef,
        author: &StoreDeviceRegistration,
        device_signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        let StorePublicationPayload::Commit(reference) = &accepted_entry.payload else {
            return Err(StoreProtocolError::OwnerPromotionMismatch);
        };
        StorePublicationEntry::parse_at(
            &accepted_entry.to_bytes(),
            commit.store_root_hash,
            accepted_reference,
            &author.device_signing_pubkey,
        )?;
        if accepted_entry.author_registration != commit.author_registration {
            return Err(StoreProtocolError::OwnerPromotionMismatch);
        }
        let value = Signed::sign(
            OwnerPromotionRequestActivation {
                commit: reference.clone(),
                publication: accepted_reference.clone(),
            },
            device_signer,
        );
        value.verify_for(commit, author)?;
        Ok(value)
    }

    pub fn verify_for(
        &self,
        commit: &StoreBatchCommit,
        author: &StoreDeviceRegistration,
    ) -> Result<(), StoreProtocolError> {
        self.body().validate_for_request_commit(commit)?;
        let request = commit
            .owner_promotion_request()
            .ok_or(StoreProtocolError::OwnerPromotionMismatch)?;
        request.verify(&author.store_root, author)?;
        commit.verify_by(&author.device_signing_pubkey)?;
        self.verify_by(&author.device_signing_pubkey)
    }
}

/// Exact publication result retained by a request's continuing operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedOwnerPromotionRequestPublication {
    pub value: OwnerPromotionRequestPublication,
    pub object: ExactObjectRef,
}

impl RetainedOwnerPromotionRequestPublication {
    pub fn validate_for(&self, commit: &StoreBatchCommit) -> Result<(), StoreProtocolError> {
        self.value.require_version()?;
        self.value.body().validate_for_request_commit(commit)?;
        self.object.verify(&self.value.to_bytes())?;
        let request = commit
            .owner_promotion_request()
            .ok_or(StoreProtocolError::OwnerPromotionMismatch)?;
        let expected = format!(
            "{}.json",
            owner_promotion_request_publication_semantic_prefix(request.promotion_id),
        );
        if self.object.slot() != &request.publication_slot
            || self.object.slot().logical_key() != expected
        {
            return Err(StoreProtocolError::OwnerPromotionMismatch);
        }
        Ok(())
    }
}

/// Continuing authority for one request whose target may still accept it.
/// The request's result supplies exact acceptance; its original predecessor
/// state remains owned only while that request can consume it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedOwnerPromotionRequest {
    pub commit: StoreBatchCommit,
    pub publication: RetainedOwnerPromotionRequestPublication,
    pub predecessor_state: ResolvedStoreDeviceState,
}

impl RetainedOwnerPromotionRequest {
    pub fn request(&self) -> Result<&OwnerPromotionRequest, StoreProtocolError> {
        self.commit
            .owner_promotion_request()
            .ok_or(StoreProtocolError::OwnerPromotionMismatch)
    }

    pub fn validate_shape(&self) -> Result<(), StoreProtocolError> {
        self.publication.validate_for(&self.commit)?;
        self.predecessor_state.validate_canonical()?;
        let predecessor = StoreDeviceStateRef::from_resolved(
            self.commit.order.predecessor_cut()?.frontier(),
            &self.predecessor_state,
        )?;
        if predecessor != self.request()?.predecessor_devices {
            return Err(StoreProtocolError::OwnerPromotionMismatch);
        }
        Ok(())
    }
}

pub fn owner_promotion_request_publication_semantic_prefix(id: OwnerPromotionId) -> String {
    format!("store-v1/owner-promotion-publications/{id}")
}

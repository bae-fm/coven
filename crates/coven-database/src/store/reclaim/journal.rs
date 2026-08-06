//! Durable publication and execution state for exact Store reclaim operations.

use serde::{Deserialize, Serialize};

use coven_protocol::objects::PreparedExactObject;
use coven_protocol::prepared_commit::PreparedStoreOperationCommit;
use coven_protocol::reclaim::{
    ReclaimAuthorization, ReclaimAuthorizationRef, ReclaimEvidence, ReclaimEvidenceRef,
    ReclaimReceipt, ReclaimReceiptRef, ReclaimTarget,
};
use coven_protocol::remote_object::{
    CandidateNonactivationProof, RemoteObjectRecord, RemoteObjectRecordError,
};
use coven_protocol::store_commit::{ObjectHash, StoreBatchCommitRef, StoreDeviceHeadRef};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum DurableStoreReclaimObject {
    Authorization {
        evidence_ref: ReclaimEvidenceRef,
        evidence: ReclaimEvidence,
        evidence_prepared: PreparedExactObject,
        authorization_ref: ReclaimAuthorizationRef,
        authorization: ReclaimAuthorization,
        authorization_prepared: PreparedExactObject,
    },
    Receipt {
        receipt_ref: ReclaimReceiptRef,
        receipt: ReclaimReceipt,
        receipt_prepared: PreparedExactObject,
    },
}

impl DurableStoreReclaimObject {
    pub fn authorization_ref(&self) -> &ReclaimAuthorizationRef {
        match self {
            Self::Authorization {
                authorization_ref, ..
            } => authorization_ref,
            Self::Receipt { receipt_ref, .. } => &receipt_ref.authorization,
        }
    }

    pub fn validate(&self) -> Result<(), StoreReclaimJournalError> {
        match self {
            Self::Authorization {
                evidence_ref,
                evidence,
                evidence_prepared,
                authorization_ref,
                authorization,
                authorization_prepared,
            } => {
                evidence_ref
                    .verify(evidence)
                    .map_err(|error| StoreReclaimJournalError::Invalid(error.to_string()))?;
                authorization_ref
                    .verify_identity(authorization)
                    .map_err(|error| StoreReclaimJournalError::Invalid(error.to_string()))?;
                if evidence_prepared.reference() != &evidence_ref.object
                    || authorization_prepared.reference() != &authorization_ref.object
                    || authorization_ref.evidence != *evidence_ref
                    || authorization.evidence != *evidence_ref
                    || authorization.target != evidence.claim.target()
                    || authorization.store_root_hash != evidence.store_root_hash
                {
                    return Err(StoreReclaimJournalError::Invalid(
                        "reclaim authorization graph has inconsistent exact identities".to_string(),
                    ));
                }
            }
            Self::Receipt {
                receipt_ref,
                receipt,
                receipt_prepared,
            } => {
                receipt_ref
                    .verify_identity(receipt)
                    .map_err(|error| StoreReclaimJournalError::Invalid(error.to_string()))?;
                if receipt_prepared.reference() != &receipt_ref.object {
                    return Err(StoreReclaimJournalError::Invalid(
                        "reclaim receipt differs from its prepared exact object".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }

    pub fn commit_names_object(&self, candidate: &PreparedStoreOperationCommit) -> bool {
        match self {
            Self::Authorization {
                authorization_ref, ..
            } => candidate.commit.reclaim_authorization() == Some(authorization_ref),
            Self::Receipt { receipt_ref, .. } => {
                candidate.commit.reclaim_receipt() == Some(receipt_ref)
            }
        }
    }

    pub fn remote_objects(
        &self,
        candidate: &PreparedStoreOperationCommit,
    ) -> Result<Vec<RemoteObjectRecord>, StoreReclaimJournalError> {
        self.validate()?;
        if !self.commit_names_object(candidate) {
            return Err(StoreReclaimJournalError::Invalid(
                "reclaim candidate does not activate its exact durable object".to_string(),
            ));
        }
        let owner = candidate.reference.clone();
        let authorities = match self {
            Self::Authorization {
                evidence_ref,
                evidence,
                evidence_prepared,
                authorization_ref,
                authorization,
                authorization_prepared,
            } => vec![
                RemoteObjectRecord::candidate_activated_reclaim_evidence(
                    evidence_ref.clone(),
                    evidence.to_bytes(),
                    evidence_prepared.stored_bytes().to_vec(),
                    owner.clone(),
                )?,
                RemoteObjectRecord::candidate_activated_reclaim_authorization(
                    authorization_ref.clone(),
                    authorization.to_bytes(),
                    authorization_prepared.stored_bytes().to_vec(),
                    owner,
                )?,
            ],
            Self::Receipt {
                receipt_ref,
                receipt,
                receipt_prepared,
            } => vec![RemoteObjectRecord::candidate_activated_reclaim_receipt(
                receipt_ref.clone(),
                receipt.to_bytes(),
                receipt_prepared.stored_bytes().to_vec(),
                owner,
            )?],
        };
        candidate
            .retained_authority_remote_objects(authorities)
            .map_err(StoreReclaimJournalError::Outbound)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReclaimCommitActivation {
    pub commit: StoreBatchCommitRef,
    pub head: StoreDeviceHeadRef,
}

impl ReclaimCommitActivation {
    pub fn new(
        commit: StoreBatchCommitRef,
        head: StoreDeviceHeadRef,
    ) -> Result<Self, StoreReclaimJournalError> {
        let activation = Self { commit, head };
        activation.validate()?;
        Ok(activation)
    }

    pub fn commit(&self) -> &StoreBatchCommitRef {
        &self.commit
    }

    pub fn validate(&self) -> Result<(), StoreReclaimJournalError> {
        if self.commit.object == self.head.object {
            return Err(StoreReclaimJournalError::Invalid(
                "reclaim activation has aliased commit and head identities".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ReclaimedStorePackage {
    AbsentVerified {
        authorization: ReclaimAuthorizationRef,
        authorization_activation: ReclaimCommitActivation,
    },
    Receipted {
        authorization: ReclaimAuthorizationRef,
        authorization_activation: ReclaimCommitActivation,
        receipt: ReclaimReceiptRef,
        receipt_activation: ReclaimCommitActivation,
    },
}

impl ReclaimedStorePackage {
    pub fn absent_verified(
        authorization: ReclaimAuthorizationRef,
        authorization_activation: ReclaimCommitActivation,
    ) -> Result<Self, StoreReclaimJournalError> {
        let value = Self::AbsentVerified {
            authorization,
            authorization_activation,
        };
        value.validate()?;
        Ok(value)
    }

    pub fn receipted(
        authorization: ReclaimAuthorizationRef,
        authorization_activation: ReclaimCommitActivation,
        receipt: ReclaimReceiptRef,
        receipt_activation: ReclaimCommitActivation,
    ) -> Result<Self, StoreReclaimJournalError> {
        let value = Self::Receipted {
            authorization,
            authorization_activation,
            receipt,
            receipt_activation,
        };
        value.validate()?;
        Ok(value)
    }

    pub fn authorization(&self) -> &ReclaimAuthorizationRef {
        match self {
            Self::AbsentVerified { authorization, .. } | Self::Receipted { authorization, .. } => {
                authorization
            }
        }
    }

    pub fn authorization_activation(&self) -> &ReclaimCommitActivation {
        match self {
            Self::AbsentVerified {
                authorization_activation,
                ..
            }
            | Self::Receipted {
                authorization_activation,
                ..
            } => authorization_activation,
        }
    }

    pub fn object_id(&self) -> ObjectHash {
        coven_protocol::remote_object::remote_object_id(self.authorization().target().object())
    }

    pub fn validate(&self) -> Result<(), StoreReclaimJournalError> {
        let authorization = self.authorization();
        let authorization_activation = self.authorization_activation();
        validate_reclaim_identity(authorization, authorization_activation)?;
        let target = authorization.target();
        let target_activation = authorization.target_activation();
        if *target.object() == authorization.object
            || *target.object() == authorization.evidence.object
            || target.object() == target_activation.object()
        {
            return Err(StoreReclaimJournalError::Invalid(
                "reclaimed package aliases authority or crosses Store histories".to_string(),
            ));
        }
        if let Self::Receipted {
            receipt,
            receipt_activation,
            ..
        } = self
        {
            receipt_activation.validate()?;
            if &receipt.authorization != authorization
                || receipt.object == *authorization.target().object()
                || receipt_activation.commit() == authorization_activation.commit()
            {
                return Err(StoreReclaimJournalError::Invalid(
                    "reclaim receipt does not close its exact authorization history".to_string(),
                ));
            }
        }
        Ok(())
    }
}

fn validate_reclaim_identity(
    authorization: &ReclaimAuthorizationRef,
    authorization_activation: &ReclaimCommitActivation,
) -> Result<(), StoreReclaimJournalError> {
    authorization_activation.validate()?;
    // The commit carrying the authorization must follow whatever activated the
    // target, never be it — an Owner cannot authorize a reclaim in the same signed
    // statement that published the object.
    if authorization.target_activation().object() == &authorization_activation.commit().object {
        return Err(StoreReclaimJournalError::Invalid(
            "reclaim authorization does not follow its target in one Store history".to_string(),
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum DurableStoreReclaimOperation {
    AuthorizationCandidate {
        object: Box<DurableStoreReclaimObject>,
        candidate: Box<PreparedStoreOperationCommit>,
    },
    Authorized {
        authorization: ReclaimAuthorizationRef,
        activation: ReclaimCommitActivation,
    },
    AbsentVerified {
        authorization: ReclaimAuthorizationRef,
        authorization_activation: ReclaimCommitActivation,
        target: ReclaimTarget,
    },
    ReceiptCandidate {
        authorization: ReclaimAuthorizationRef,
        authorization_activation: ReclaimCommitActivation,
        object: Box<DurableStoreReclaimObject>,
        candidate: Box<PreparedStoreOperationCommit>,
    },
    AuthorizationReplacing {
        object: Box<DurableStoreReclaimObject>,
        candidate: Box<PreparedStoreOperationCommit>,
        losing: Box<StoreReclaimCandidateLoss>,
    },
    ReceiptReplacing {
        authorization: ReclaimAuthorizationRef,
        authorization_activation: ReclaimCommitActivation,
        object: Box<DurableStoreReclaimObject>,
        candidate: Box<PreparedStoreOperationCommit>,
        losing: Box<StoreReclaimCandidateLoss>,
    },
    Completed {
        authorization: ReclaimAuthorizationRef,
        authorization_activation: ReclaimCommitActivation,
        receipt: ReclaimReceiptRef,
        receipt_activation: ReclaimCommitActivation,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreReclaimCandidateLoss {
    pub candidate: Box<PreparedStoreOperationCommit>,
    pub proof: CandidateNonactivationProof,
}

impl DurableStoreReclaimOperation {
    pub fn operation_id(&self) -> ObjectHash {
        self.authorization().authorization_hash
    }

    pub fn authorization(&self) -> &ReclaimAuthorizationRef {
        match self {
            Self::AuthorizationCandidate { object, .. } => object.authorization_ref(),
            Self::AuthorizationReplacing { object, .. } => object.authorization_ref(),
            Self::Authorized { authorization, .. }
            | Self::AbsentVerified { authorization, .. }
            | Self::ReceiptCandidate { authorization, .. }
            | Self::ReceiptReplacing { authorization, .. }
            | Self::Completed { authorization, .. } => authorization,
        }
    }

    pub fn candidate(&self) -> Option<&PreparedStoreOperationCommit> {
        match self {
            Self::AuthorizationCandidate { candidate, .. }
            | Self::ReceiptCandidate { candidate, .. }
            | Self::AuthorizationReplacing { candidate, .. }
            | Self::ReceiptReplacing { candidate, .. } => Some(candidate),
            Self::Authorized { .. } | Self::AbsentVerified { .. } | Self::Completed { .. } => None,
        }
    }

    pub fn object(&self) -> Option<&DurableStoreReclaimObject> {
        match self {
            Self::AuthorizationCandidate { object, .. }
            | Self::ReceiptCandidate { object, .. }
            | Self::AuthorizationReplacing { object, .. }
            | Self::ReceiptReplacing { object, .. } => Some(object),
            Self::Authorized { .. } | Self::AbsentVerified { .. } | Self::Completed { .. } => None,
        }
    }

    pub fn losing_candidate(&self) -> Option<&StoreReclaimCandidateLoss> {
        match self {
            Self::AuthorizationReplacing { losing, .. } | Self::ReceiptReplacing { losing, .. } => {
                Some(losing)
            }
            _ => None,
        }
    }

    pub fn validate(&self) -> Result<(), StoreReclaimJournalError> {
        match self {
            Self::AuthorizationCandidate { object, candidate } => {
                object.validate()?;
                candidate
                    .reference
                    .verify_commit(&candidate.commit)
                    .map_err(|error| StoreReclaimJournalError::Invalid(error.to_string()))?;
                if !object.commit_names_object(candidate) {
                    return Err(StoreReclaimJournalError::Invalid(
                        "reclaim journal candidate names another operation".to_string(),
                    ));
                }
            }
            Self::Authorized {
                authorization,
                activation,
            } => validate_reclaim_identity(authorization, activation)?,
            Self::AbsentVerified {
                authorization,
                authorization_activation,
                target,
            } => {
                if target != authorization.target() {
                    return Err(StoreReclaimJournalError::Invalid(
                        "reclaim target differs from its exact authorization".to_string(),
                    ));
                }
                ReclaimedStorePackage::absent_verified(
                    authorization.clone(),
                    authorization_activation.clone(),
                )?;
            }
            Self::ReceiptCandidate {
                authorization,
                authorization_activation,
                object,
                candidate,
                ..
            } => {
                validate_reclaim_identity(authorization, authorization_activation)?;
                object.validate()?;
                candidate
                    .reference
                    .verify_commit(&candidate.commit)
                    .map_err(|error| StoreReclaimJournalError::Invalid(error.to_string()))?;
                if object.authorization_ref() != authorization
                    || !matches!(&**object, DurableStoreReclaimObject::Receipt { .. })
                    || !object.commit_names_object(candidate)
                {
                    return Err(StoreReclaimJournalError::Invalid(
                        "reclaim receipt candidate changes its authorization".to_string(),
                    ));
                }
            }
            Self::AuthorizationReplacing {
                object,
                candidate,
                losing,
            } => validate_replacement(object, candidate, losing)?,
            Self::ReceiptReplacing {
                authorization,
                authorization_activation,
                object,
                candidate,
                losing,
                ..
            } => {
                validate_reclaim_identity(authorization, authorization_activation)?;
                validate_replacement(object, candidate, losing)?;
                if object.authorization_ref() != authorization
                    || !matches!(&**object, DurableStoreReclaimObject::Receipt { .. })
                {
                    return Err(StoreReclaimJournalError::Invalid(
                        "replacement reclaim receipt changes its authorization".to_string(),
                    ));
                }
            }
            Self::Completed {
                authorization,
                authorization_activation,
                receipt,
                receipt_activation,
            } => {
                ReclaimedStorePackage::receipted(
                    authorization.clone(),
                    authorization_activation.clone(),
                    receipt.clone(),
                    receipt_activation.clone(),
                )?;
            }
        }
        Ok(())
    }
}

fn validate_replacement(
    object: &DurableStoreReclaimObject,
    candidate: &PreparedStoreOperationCommit,
    losing: &StoreReclaimCandidateLoss,
) -> Result<(), StoreReclaimJournalError> {
    object.validate()?;
    candidate
        .reference
        .verify_commit(&candidate.commit)
        .map_err(|error| StoreReclaimJournalError::Invalid(error.to_string()))?;
    losing
        .candidate
        .reference
        .verify_commit(&losing.candidate.commit)
        .map_err(|error| StoreReclaimJournalError::Invalid(error.to_string()))?;
    coven_protocol::remote_object::CandidateNonactivation::validate_durable_shape(
        &losing.candidate.reference,
        &losing.candidate.commit,
        losing.proof.clone(),
    )?;
    if candidate.reference == losing.candidate.reference
        || !object.commit_names_object(candidate)
        || !object.commit_names_object(&losing.candidate)
    {
        return Err(StoreReclaimJournalError::Invalid(
            "replacement reclaim candidate changes its signed object".to_string(),
        ));
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum StoreReclaimJournalError {
    #[error("invalid durable Store reclaim state: {0}")]
    Invalid(String),
    #[error(transparent)]
    RemoteObject(#[from] RemoteObjectRecordError),
    #[error(transparent)]
    Outbound(#[from] coven_protocol::prepared_commit::PreparedCommitError),
    #[error(transparent)]
    Storage(#[from] coven_protocol::objects::StorageError),
}

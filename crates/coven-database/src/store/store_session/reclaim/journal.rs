//! Durable publication and execution state for exact Store reclaim operations.

use serde::{Deserialize, Serialize};

use coven_protocol::objects::PreparedExactObject;
use coven_protocol::prepared_commit::PreparedStoreOperationCommit;
use coven_protocol::reclaim::{
    ReclaimAuthorization, ReclaimAuthorizationRef, ReclaimEvidence, ReclaimEvidenceRef,
    ReclaimTarget,
};
use coven_protocol::remote_object::{RemoteObjectRecord, RemoteObjectRecordError};
use coven_protocol::store_commit::{ObjectHash, StoreBatchCommitRef};

/// The exact objects one reclaim authorization publishes: the Owner's sealed
/// evidence and the public authorization it signs, each with the prepared
/// object its candidate commit activates.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurableStoreReclaimAuthorization {
    pub evidence_ref: ReclaimEvidenceRef,
    pub evidence: ReclaimEvidence,
    pub evidence_prepared: PreparedExactObject,
    pub authorization_ref: ReclaimAuthorizationRef,
    pub authorization: ReclaimAuthorization,
    pub authorization_prepared: PreparedExactObject,
}

impl DurableStoreReclaimAuthorization {
    pub fn validate(&self) -> Result<(), StoreReclaimJournalError> {
        self.evidence_ref
            .verify(&self.evidence)
            .map_err(StoreReclaimJournalError::from)?;
        self.authorization_ref
            .verify_identity(&self.authorization)
            .map_err(StoreReclaimJournalError::from)?;
        if self.evidence_prepared.reference() != &self.evidence_ref.object
            || self.authorization_prepared.reference() != &self.authorization_ref.object
            || self.authorization_ref.evidence != self.evidence_ref
            || self.authorization.evidence != self.evidence_ref
            || self.authorization.target != self.evidence.claim.target()
            || self.authorization.store_root_hash != self.evidence.store_root_hash
        {
            return Err(StoreReclaimJournalError::Invalid(
                "reclaim authorization graph has inconsistent exact identities".to_string(),
            ));
        }
        Ok(())
    }

    pub fn commit_names_object(&self, candidate: &PreparedStoreOperationCommit) -> bool {
        candidate.commit.reclaim_authorization() == Some(&self.authorization_ref)
    }

    pub fn remote_objects(
        &self,
        candidate: &PreparedStoreOperationCommit,
    ) -> Result<Vec<coven_protocol::remote_object::ClosedRemoteObject>, StoreReclaimJournalError>
    {
        self.validate()?;
        if !self.commit_names_object(candidate) {
            return Err(StoreReclaimJournalError::Invalid(
                "reclaim candidate does not activate its exact durable object".to_string(),
            ));
        }
        let owner = candidate.reference.clone();
        let authorities = vec![
            RemoteObjectRecord::candidate_activated_reclaim_evidence(
                self.evidence_ref.clone(),
                &self.evidence.to_bytes(),
                self.evidence_prepared.stored_bytes(),
                owner.clone(),
            )?,
            RemoteObjectRecord::candidate_activated_reclaim_authorization(
                self.authorization_ref.clone(),
                &self.authorization.to_bytes(),
                self.authorization_prepared.stored_bytes(),
                owner,
            )?,
        ];
        candidate
            .retained_authority_remote_objects(authorities)
            .map_err(StoreReclaimJournalError::Outbound)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ReclaimedStorePackage {
    AbsentVerified {
        authorization: ReclaimAuthorizationRef,
        authorization_activation: StoreBatchCommitRef,
    },
    Completed {
        authorization: ReclaimAuthorizationRef,
        authorization_activation: StoreBatchCommitRef,
        completion_activation: StoreBatchCommitRef,
    },
}

impl ReclaimedStorePackage {
    pub fn absent_verified(
        authorization: ReclaimAuthorizationRef,
        authorization_activation: StoreBatchCommitRef,
    ) -> Result<Self, StoreReclaimJournalError> {
        let value = Self::AbsentVerified {
            authorization,
            authorization_activation,
        };
        value.validate()?;
        Ok(value)
    }

    pub fn completed(
        authorization: ReclaimAuthorizationRef,
        authorization_activation: StoreBatchCommitRef,
        completion_activation: StoreBatchCommitRef,
    ) -> Result<Self, StoreReclaimJournalError> {
        let value = Self::Completed {
            authorization,
            authorization_activation,
            completion_activation,
        };
        value.validate()?;
        Ok(value)
    }

    pub fn authorization(&self) -> &ReclaimAuthorizationRef {
        match self {
            Self::AbsentVerified { authorization, .. } | Self::Completed { authorization, .. } => {
                authorization
            }
        }
    }

    pub fn authorization_activation(&self) -> &StoreBatchCommitRef {
        match self {
            Self::AbsentVerified {
                authorization_activation,
                ..
            }
            | Self::Completed {
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
            || target_activation.names_authority_object(target.object())
        {
            return Err(StoreReclaimJournalError::Invalid(
                "reclaimed package aliases authority or crosses Store histories".to_string(),
            ));
        }
        if let Self::Completed {
            completion_activation,
            ..
        } = self
        {
            if completion_activation == authorization_activation {
                return Err(StoreReclaimJournalError::Invalid(
                    "reclaim completion does not close its exact authorization history".to_string(),
                ));
            }
        }
        Ok(())
    }
}

fn validate_reclaim_identity(
    authorization: &ReclaimAuthorizationRef,
    authorization_activation: &StoreBatchCommitRef,
) -> Result<(), StoreReclaimJournalError> {
    // The commit carrying the authorization must follow whatever activated the
    // target, never be it — an Owner cannot authorize a reclaim in the same signed
    // statement that published the object.
    if authorization
        .target_activation()
        .names_authority_object(&authorization_activation.object)
    {
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
        object: Box<DurableStoreReclaimAuthorization>,
        candidate: Box<PreparedStoreOperationCommit>,
    },
    Authorized {
        authorization: ReclaimAuthorizationRef,
        activation: StoreBatchCommitRef,
    },
    AbsentVerified {
        authorization: ReclaimAuthorizationRef,
        authorization_activation: StoreBatchCommitRef,
        target: ReclaimTarget,
    },
    CompletionCandidate {
        authorization: ReclaimAuthorizationRef,
        authorization_activation: StoreBatchCommitRef,
        candidate: Box<PreparedStoreOperationCommit>,
    },
    Completed {
        authorization: ReclaimAuthorizationRef,
        authorization_activation: StoreBatchCommitRef,
        completion_activation: StoreBatchCommitRef,
    },
}

/// A journalled reclaim operation whose last run failed with an error that
/// running it again cannot change.
///
/// Every later cycle skips it, so it spends no provider requests and holds
/// nothing else up; it runs again only when the host asks. The target and the
/// error are what the host shows the person who has to decide.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StuckReclaimOperation {
    pub operation_id: ObjectHash,
    pub target: ReclaimTarget,
    pub error: String,
}

impl DurableStoreReclaimOperation {
    pub fn operation_id(&self) -> ObjectHash {
        self.authorization().authorization_hash
    }

    pub fn authorization(&self) -> &ReclaimAuthorizationRef {
        match self {
            Self::AuthorizationCandidate { object, .. } => &object.authorization_ref,
            Self::Authorized { authorization, .. }
            | Self::AbsentVerified { authorization, .. }
            | Self::CompletionCandidate { authorization, .. }
            | Self::Completed { authorization, .. } => authorization,
        }
    }

    pub fn candidate(&self) -> Option<&PreparedStoreOperationCommit> {
        match self {
            Self::AuthorizationCandidate { candidate, .. }
            | Self::CompletionCandidate { candidate, .. } => Some(candidate),
            Self::Authorized { .. } | Self::AbsentVerified { .. } | Self::Completed { .. } => None,
        }
    }

    pub fn object(&self) -> Option<&DurableStoreReclaimAuthorization> {
        match self {
            Self::AuthorizationCandidate { object, .. } => Some(object),
            Self::Authorized { .. }
            | Self::AbsentVerified { .. }
            | Self::CompletionCandidate { .. }
            | Self::Completed { .. } => None,
        }
    }

    pub fn validate(&self) -> Result<(), StoreReclaimJournalError> {
        match self {
            Self::AuthorizationCandidate { object, candidate } => {
                object.validate()?;
                candidate
                    .reference
                    .verify_commit(&candidate.commit)
                    .map_err(StoreReclaimJournalError::from)?;
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
            Self::CompletionCandidate {
                authorization,
                authorization_activation,
                candidate,
            } => {
                validate_reclaim_identity(authorization, authorization_activation)?;
                candidate
                    .reference
                    .verify_commit(&candidate.commit)
                    .map_err(StoreReclaimJournalError::from)?;
                if candidate
                    .commit
                    .reclaim_completion()
                    .map(|completion| &completion.authorization)
                    != Some(authorization)
                {
                    return Err(StoreReclaimJournalError::Invalid(
                        "reclaim completion candidate changes its authorization".to_string(),
                    ));
                }
            }
            Self::Completed {
                authorization,
                authorization_activation,
                completion_activation,
            } => {
                ReclaimedStorePackage::completed(
                    authorization.clone(),
                    authorization_activation.clone(),
                    completion_activation.clone(),
                )?;
            }
        }
        Ok(())
    }
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
    #[error(transparent)]
    Protocol(#[from] coven_protocol::store_commit::StoreProtocolError),
}

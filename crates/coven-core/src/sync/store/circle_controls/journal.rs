use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::CircleOperationError;
use crate::sync::circle::{
    CircleId, CircleOperationId, CircleOperationKind, CircleOperationState,
    PreparedCircleTransition,
};
use crate::sync::storage::{ExactObjectRef, PreparedExactObject};
use crate::sync::store_commit::{StoreBatchCommit, StoreBatchCommitRef, StoreDeviceHead};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CircleOperationPolicy {
    pub head: StoreDeviceHead,
    pub history_summary: crate::sync::store_commit::RetainedVerifiedMergeHistorySummary,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PreparedCircleOperation {
    pub creation: PreparedCircleTransition,
    pub history: CircleTransitionHistory,
    pub commit_bytes: Vec<u8>,
    pub commit_ref: StoreBatchCommitRef,
    pub prepared_objects: BTreeMap<String, PreparedExactObject>,
    pub policy: CircleOperationPolicy,
    pub uploaded: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum CircleTransitionHistory {
    Founder,
    Successor(Box<crate::sync::store_commit::CircleControlRef>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum CircleOperationIntent {
    Create { name: String },
    Rename { name: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum CircleOperationProgress {
    Ready(Box<PreparedCircleOperation>),
    Blocked {
        reason: String,
        operation: Box<PreparedCircleOperation>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CircleOperationJournal {
    pub operation_id: CircleOperationId,
    pub circle_id: CircleId,
    pub intent: CircleOperationIntent,
    pub progress: CircleOperationProgress,
}

impl CircleOperationJournal {
    pub(crate) fn circle_id(&self) -> CircleId {
        self.circle_id
    }

    pub(crate) fn operation(&self) -> &PreparedCircleOperation {
        match &self.progress {
            CircleOperationProgress::Ready(operation)
            | CircleOperationProgress::Blocked { operation, .. } => operation,
        }
    }

    pub(crate) fn operation_mut(&mut self) -> &mut PreparedCircleOperation {
        match &mut self.progress {
            CircleOperationProgress::Ready(operation)
            | CircleOperationProgress::Blocked { operation, .. } => operation,
        }
    }

    pub(crate) fn closed_remote_objects(
        &self,
    ) -> Result<Vec<crate::sync::remote_object::RemoteObjectRecord>, CircleOperationError> {
        self.uploaded_object_ids()?;
        let operation = self.operation();
        let commit: StoreBatchCommit = serde_json::from_slice(&operation.commit_bytes)
            .map_err(|error| CircleOperationError::Journal(format!("Circle commit: {error}")))?;
        operation
            .commit_ref
            .verify_commit(&commit)
            .map_err(|error| CircleOperationError::Journal(error.to_string()))?;
        let access_refs = commit
            .circle_controls()
            .iter()
            .flat_map(|control| control.objects().access.iter())
            .collect::<Vec<_>>();
        if access_refs.len() != operation.creation.access.len() {
            return Err(CircleOperationError::Journal(
                "Circle access material does not cover the signed candidate graph".to_string(),
            ));
        }
        let prepared_for = |object: &ExactObjectRef| {
            operation
                .prepared_objects
                .values()
                .find(|prepared| prepared.reference() == object)
                .ok_or_else(|| {
                    CircleOperationError::Journal(format!(
                        "Circle candidate object {} has no prepared bytes",
                        crate::sync::remote_object::remote_object_id(object)
                    ))
                })
        };
        let mut materials = Vec::with_capacity(access_refs.len() * 2);
        for (access, reference) in operation.creation.access.iter().zip(access_refs) {
            let leaf = prepared_for(&reference.leaf.object)?;
            materials.push(crate::sync::remote_object::CandidateObjectMaterial {
                object: reference.leaf.object.clone(),
                canonical_semantic_bytes: serde_json::to_vec(&access.leaf.value).map_err(
                    |error| CircleOperationError::Journal(format!("Circle access leaf: {error}")),
                )?,
                stored_bytes: leaf.stored_bytes().to_vec(),
            });
            let envelope = prepared_for(&reference.envelope.object)?;
            materials.push(crate::sync::remote_object::CandidateObjectMaterial {
                object: reference.envelope.object.clone(),
                canonical_semantic_bytes: serde_json::to_vec(&access.envelope).map_err(
                    |error| {
                        CircleOperationError::Journal(format!("Circle access envelope: {error}"))
                    },
                )?,
                stored_bytes: envelope.stored_bytes().to_vec(),
            });
        }
        let mut remotes = crate::sync::remote_object::CandidateObjectGraph::from_commit(&commit)
            .and_then(|graph| graph.close(&commit, &operation.commit_ref, materials))
            .map_err(|error| CircleOperationError::Journal(error.to_string()))?;
        let commit_prepared = operation
            .prepared_objects
            .get("store-commit")
            .ok_or_else(|| {
                CircleOperationError::Journal(
                    "Circle operation lacks its prepared Store commit".to_string(),
                )
            })?;
        remotes.push(
            crate::sync::remote_object::RemoteObjectRecord::candidate_commit(
                operation.commit_ref.clone(),
                operation.commit_bytes.clone(),
                commit_prepared.stored_bytes().to_vec(),
            )
            .map_err(|error| CircleOperationError::Journal(error.to_string()))?,
        );
        let prepared = operation
            .prepared_objects
            .get("store-head")
            .ok_or_else(|| {
                CircleOperationError::Journal(
                    "Circle operation lacks its prepared Store head".to_string(),
                )
            })?;
        remotes.push(
            crate::sync::remote_object::RemoteObjectRecord::candidate_activated_store_head(
                crate::sync::store_commit::StoreDeviceHeadRef {
                    head_hash: operation.policy.head.head_hash(),
                    object: prepared.reference().clone(),
                },
                operation.policy.head.to_bytes(),
                prepared.stored_bytes().to_vec(),
                operation.commit_ref.clone(),
            )
            .map_err(|error| CircleOperationError::Journal(error.to_string()))?,
        );
        Ok(remotes)
    }

    pub(crate) fn uploaded_object_ids(
        &self,
    ) -> Result<BTreeSet<crate::sync::store_commit::ObjectHash>, CircleOperationError> {
        self.operation()
            .uploaded
            .iter()
            .map(|step| {
                let prepared = self.operation().prepared_objects.get(step).ok_or_else(|| {
                    CircleOperationError::Journal(format!(
                        "Circle upload marker {step} names no prepared object"
                    ))
                })?;
                Ok(crate::sync::remote_object::remote_object_id(
                    prepared.reference(),
                ))
            })
            .collect()
    }

    pub(crate) fn state(&self) -> CircleOperationState {
        match &self.progress {
            CircleOperationProgress::Ready(_) => CircleOperationState::Pending,
            CircleOperationProgress::Blocked { reason, .. } => CircleOperationState::Blocked {
                reason: reason.clone(),
            },
        }
    }

    pub(crate) fn block(&mut self, reason: String) -> Result<(), CircleOperationError> {
        let CircleOperationProgress::Ready(operation) = &mut self.progress else {
            return Err(CircleOperationError::Journal(format!(
                "Circle operation {} is already blocked",
                self.operation_id
            )));
        };
        let operation = operation.clone();
        self.progress = CircleOperationProgress::Blocked { reason, operation };
        Ok(())
    }

    pub(crate) fn commit(&self) -> Result<StoreBatchCommit, CircleOperationError> {
        serde_json::from_slice(&self.operation().commit_bytes)
            .map_err(|error| CircleOperationError::Journal(format!("parse Store commit: {error}")))
    }

    pub(crate) fn validate_identity(&self) -> Result<(), CircleOperationError> {
        if self.operation().creation.circle_id != self.circle_id {
            return Err(CircleOperationError::Journal(format!(
                "circle operation {} payload names circle {} but its operation names circle {}",
                self.operation_id,
                self.circle_id,
                self.operation().creation.circle_id
            )));
        }
        let commit = self.commit()?;
        if commit.write_id.as_str() != self.operation_id.as_str() {
            return Err(CircleOperationError::Journal(format!(
                "circle operation id {} differs from payload commit operation id {}",
                self.operation_id, commit.write_id
            )));
        }
        Ok(())
    }

    pub(crate) fn kind(&self) -> CircleOperationKind {
        match self.intent {
            CircleOperationIntent::Create { .. } => CircleOperationKind::Create,
            CircleOperationIntent::Rename { .. } => CircleOperationKind::Rename,
        }
    }
}

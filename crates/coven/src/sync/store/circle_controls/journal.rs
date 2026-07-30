use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::CircleOperationError;
use crate::protocol::circle::{
    CircleId, CircleOperationId, CircleOperationKind, CircleOperationState,
    PreparedCircleTransition,
};
use crate::protocol::store_commit::{StoreBatchCommit, StoreBatchCommitRef, StoreDeviceHead};
use crate::storage::{ExactObjectRef, PreparedExactObject};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CircleOperationPolicy {
    pub head: StoreDeviceHead,
    pub history_summary: crate::protocol::store_commit::RetainedVerifiedMergeHistorySummary,
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
    Successor(Box<crate::protocol::store_commit::CircleControlRef>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum CircleOperationIntent {
    Create {
        name: String,
    },
    Rename {
        name: String,
    },
    AddMember {
        member_pubkey: String,
        role: crate::protocol::circle::CircleRole,
    },
    RemoveMember {
        member_pubkey: String,
    },
    ResolveControl {
        chosen: crate::protocol::circle::CircleControlCoord,
    },
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum CircleOperationProgress {
    Ready(Box<PreparedCircleOperation>),
    WaitingForCloseResponses(Box<PreparedCircleOperation>),
    Finalizing(Box<PreparedCircleOperation>),
    Blocked {
        block: crate::protocol::circle::CircleOperationBlock,
        phase: CircleOperationPhase,
        operation: Box<PreparedCircleOperation>,
    },
    /// A verified nonactivation proof was accepted. The candidate's exclusive
    /// objects are being exact-deleted and the durable row cleared in the
    /// completing transaction. The retained payload identifies the candidate
    /// graph so a restart resumes the exact same cleanup.
    Discarding(Box<PreparedCircleOperation>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CircleOperationPhase {
    Initial,
    Finalization,
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
            | CircleOperationProgress::WaitingForCloseResponses(operation)
            | CircleOperationProgress::Finalizing(operation)
            | CircleOperationProgress::Blocked { operation, .. }
            | CircleOperationProgress::Discarding(operation) => operation,
        }
    }

    pub(crate) fn operation_mut(&mut self) -> &mut PreparedCircleOperation {
        match &mut self.progress {
            CircleOperationProgress::Ready(operation)
            | CircleOperationProgress::WaitingForCloseResponses(operation)
            | CircleOperationProgress::Finalizing(operation)
            | CircleOperationProgress::Blocked { operation, .. }
            | CircleOperationProgress::Discarding(operation) => operation,
        }
    }

    pub(crate) fn closed_remote_objects(
        &self,
    ) -> Result<Vec<crate::protocol::remote_object::RemoteObjectRecord>, CircleOperationError> {
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
                        crate::protocol::remote_object::remote_object_id(object)
                    ))
                })
        };
        let mut materials = Vec::with_capacity(access_refs.len() * 3 + 1);
        let [circle_reference] = commit.circle_controls() else {
            return Err(CircleOperationError::Journal(
                "Circle operation commit must activate exactly one Circle control".to_string(),
            ));
        };
        match (
            &circle_reference.objects().close_intent,
            &operation.creation.close_intent,
        ) {
            (Some(reference), Some(intent))
                if reference.close_id == intent.close_id
                    && reference.intent_hash == intent.intent_hash() =>
            {
                let prepared = prepared_for(&reference.object)?;
                materials.push(crate::protocol::remote_object::CandidateObjectMaterial {
                    object: reference.object.clone(),
                    canonical_semantic_bytes: serde_json::to_vec(intent).map_err(|error| {
                        CircleOperationError::Journal(format!("Circle epoch-close intent: {error}"))
                    })?,
                    stored_bytes: prepared.stored_bytes().to_vec(),
                });
            }
            (None, None) => {}
            _ => {
                return Err(CircleOperationError::Journal(
                    "Circle epoch-close intent does not match its signed candidate graph"
                        .to_string(),
                ));
            }
        }
        match (
            &circle_reference.objects().close_outcome,
            &operation.creation.close_outcome,
        ) {
            (Some(reference), Some(outcome))
                if reference.close_id == outcome.close_id
                    && reference.outcome_hash == outcome.outcome_hash() =>
            {
                let prepared = prepared_for(&reference.object)?;
                materials.push(crate::protocol::remote_object::CandidateObjectMaterial {
                    object: reference.object.clone(),
                    canonical_semantic_bytes:
                        crate::protocol::circle::CircleEpochCloseSlotValue::Outcome(outcome.clone())
                            .to_bytes(),
                    stored_bytes: prepared.stored_bytes().to_vec(),
                });
            }
            (None, None) => {}
            _ => {
                return Err(CircleOperationError::Journal(
                    "Circle epoch-close outcome does not match its signed candidate graph"
                        .to_string(),
                ));
            }
        }
        match (
            &circle_reference.objects().close_cancellation,
            &operation.creation.close_cancellation,
        ) {
            (Some(reference), Some(cancellation))
                if reference.close_id == cancellation.close_id
                    && reference.cancellation_hash == cancellation.cancellation_hash() =>
            {
                let prepared = prepared_for(&reference.object)?;
                materials.push(crate::protocol::remote_object::CandidateObjectMaterial {
                    object: reference.object.clone(),
                    canonical_semantic_bytes:
                        crate::protocol::circle::CircleEpochCloseSlotValue::Cancellation(
                            cancellation.clone(),
                        )
                        .to_bytes(),
                    stored_bytes: prepared.stored_bytes().to_vec(),
                });
            }
            (None, None) => {}
            _ => {
                return Err(CircleOperationError::Journal(
                    "Circle epoch-close cancellation does not match its signed candidate graph"
                        .to_string(),
                ));
            }
        }
        let mut bootstrap_blobs = BTreeMap::new();
        for (access, reference) in operation.creation.access.iter().zip(access_refs) {
            let leaf = prepared_for(&reference.leaf.object)?;
            materials.push(crate::protocol::remote_object::CandidateObjectMaterial {
                object: reference.leaf.object.clone(),
                canonical_semantic_bytes: serde_json::to_vec(&access.leaf.value).map_err(
                    |error| CircleOperationError::Journal(format!("Circle access leaf: {error}")),
                )?,
                stored_bytes: leaf.stored_bytes().to_vec(),
            });
            let envelope = prepared_for(&reference.envelope.object)?;
            materials.push(crate::protocol::remote_object::CandidateObjectMaterial {
                object: reference.envelope.object.clone(),
                canonical_semantic_bytes: serde_json::to_vec(&access.envelope).map_err(
                    |error| {
                        CircleOperationError::Journal(format!("Circle access envelope: {error}"))
                    },
                )?,
                stored_bytes: envelope.stored_bytes().to_vec(),
            });
            if let Some(bootstrap) = &reference.bootstrap {
                let image = prepared_for(&bootstrap.object)?;
                materials.push(crate::protocol::remote_object::CandidateObjectMaterial {
                    object: bootstrap.object.clone(),
                    canonical_semantic_bytes: Vec::new(),
                    stored_bytes: image.stored_bytes().to_vec(),
                });
            }
            if let crate::protocol::circle::CircleAccessDisposition::Active {
                bootstrap: Some(bootstrap),
                ..
            } = &access.leaf.value.disposition
            {
                for blob in &bootstrap.blobs {
                    let stored = blob.stored().ok_or_else(|| {
                        CircleOperationError::Journal(
                            "Circle bootstrap row blob has no exact stored locator".to_string(),
                        )
                    })?;
                    let object_id =
                        crate::protocol::remote_object::remote_object_id(stored.object());
                    if bootstrap_blobs
                        .insert(object_id, stored.clone())
                        .is_some_and(|existing| existing != *stored)
                    {
                        return Err(CircleOperationError::Journal(format!(
                            "Circle bootstrap blob {object_id} has conflicting exact references"
                        )));
                    }
                }
            }
        }
        let mut remotes =
            crate::protocol::remote_object::CandidateObjectGraph::from_commit(&commit)
                .and_then(|graph| graph.close(&commit, &operation.commit_ref, materials))
                .map_err(|error| CircleOperationError::Journal(error.to_string()))?;
        for blob in bootstrap_blobs.into_values() {
            remotes.push(
                crate::protocol::remote_object::RemoteObjectRecord::candidate_owned_blob(
                    &blob,
                    operation.commit_ref.clone(),
                    true,
                )
                .map_err(|error| CircleOperationError::Journal(error.to_string()))?,
            );
        }
        let commit_prepared = operation
            .prepared_objects
            .get("store-commit")
            .ok_or_else(|| {
                CircleOperationError::Journal(
                    "Circle operation lacks its prepared Store commit".to_string(),
                )
            })?;
        remotes.push(
            crate::protocol::remote_object::RemoteObjectRecord::candidate_commit(
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
            crate::protocol::remote_object::RemoteObjectRecord::candidate_activated_store_head(
                crate::protocol::store_commit::StoreDeviceHeadRef {
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
    ) -> Result<BTreeSet<crate::protocol::store_commit::ObjectHash>, CircleOperationError> {
        self.operation()
            .uploaded
            .iter()
            .map(|step| {
                let prepared = self.operation().prepared_objects.get(step).ok_or_else(|| {
                    CircleOperationError::Journal(format!(
                        "Circle upload marker {step} names no prepared object"
                    ))
                })?;
                Ok(crate::protocol::remote_object::remote_object_id(
                    prepared.reference(),
                ))
            })
            .collect()
    }

    pub(crate) fn state(&self) -> CircleOperationState {
        match &self.progress {
            CircleOperationProgress::Ready(_) => CircleOperationState::Pending,
            CircleOperationProgress::WaitingForCloseResponses(_) => {
                CircleOperationState::WaitingForCloseResponses
            }
            CircleOperationProgress::Finalizing(_) => CircleOperationState::Finalizing,
            CircleOperationProgress::Blocked { block, .. } => CircleOperationState::Blocked {
                block: block.clone(),
            },
            CircleOperationProgress::Discarding(_) => CircleOperationState::Discarding,
        }
    }

    /// Enter cleanup after a verified nonactivation proof was accepted. Legal
    /// from any state whose candidate has not activated — a ready or blocked
    /// initial candidate, or a finalization candidate. A candidate that already
    /// won its slot has no journal row in these states, so no path reaches here.
    pub(crate) fn begin_discard(&mut self) -> Result<(), CircleOperationError> {
        let operation = match &self.progress {
            CircleOperationProgress::Ready(operation)
            | CircleOperationProgress::Finalizing(operation)
            | CircleOperationProgress::Blocked { operation, .. } => operation.clone(),
            CircleOperationProgress::WaitingForCloseResponses(_)
            | CircleOperationProgress::Discarding(_) => {
                return Err(CircleOperationError::Journal(format!(
                    "Circle operation {} cannot enter discard from its current state",
                    self.operation_id
                )));
            }
        };
        self.progress = CircleOperationProgress::Discarding(operation);
        Ok(())
    }

    pub(crate) fn is_discarding(&self) -> bool {
        matches!(&self.progress, CircleOperationProgress::Discarding(_))
    }

    pub(crate) fn block(
        &mut self,
        block: crate::protocol::circle::CircleOperationBlock,
    ) -> Result<(), CircleOperationError> {
        let (phase, operation) = match &self.progress {
            CircleOperationProgress::Ready(operation) => {
                (CircleOperationPhase::Initial, operation.clone())
            }
            CircleOperationProgress::Finalizing(operation) => {
                (CircleOperationPhase::Finalization, operation.clone())
            }
            CircleOperationProgress::WaitingForCloseResponses(_)
            | CircleOperationProgress::Blocked { .. }
            | CircleOperationProgress::Discarding(_) => {
                return Err(CircleOperationError::Journal(format!(
                    "Circle operation {} is not publishable",
                    self.operation_id
                )));
            }
        };
        self.progress = CircleOperationProgress::Blocked {
            block,
            phase,
            operation,
        };
        Ok(())
    }

    /// Return a blocked operation to the phase captured when it blocked, so it
    /// re-enters the idempotent publish pipeline with its exact retained payload.
    pub(crate) fn unblock(&mut self) -> Result<(), CircleOperationError> {
        let CircleOperationProgress::Blocked {
            phase, operation, ..
        } = &self.progress
        else {
            return Err(CircleOperationError::Journal(format!(
                "Circle operation {} is not blocked",
                self.operation_id
            )));
        };
        let operation = operation.clone();
        self.progress = match phase {
            CircleOperationPhase::Initial => CircleOperationProgress::Ready(operation),
            CircleOperationPhase::Finalization => CircleOperationProgress::Finalizing(operation),
        };
        Ok(())
    }

    pub(crate) fn wait_for_close_responses(&mut self) -> Result<(), CircleOperationError> {
        let CircleOperationProgress::Ready(operation) = &mut self.progress else {
            return Err(CircleOperationError::Journal(format!(
                "Circle operation {} is not ready to enter close-response waiting",
                self.operation_id
            )));
        };
        let operation = operation.clone();
        self.progress = CircleOperationProgress::WaitingForCloseResponses(operation);
        Ok(())
    }

    pub(crate) fn begin_finalization(
        &mut self,
        operation: PreparedCircleOperation,
    ) -> Result<(), CircleOperationError> {
        if !matches!(
            &self.progress,
            CircleOperationProgress::WaitingForCloseResponses(_)
        ) {
            return Err(CircleOperationError::Journal(format!(
                "Circle operation {} is not waiting for close responses",
                self.operation_id
            )));
        }
        self.progress = CircleOperationProgress::Finalizing(Box::new(operation));
        Ok(())
    }

    pub(crate) fn is_finalizing(&self) -> bool {
        matches!(
            &self.progress,
            CircleOperationProgress::Finalizing(_)
                | CircleOperationProgress::Blocked {
                    phase: CircleOperationPhase::Finalization,
                    ..
                }
        )
    }

    pub(crate) fn is_publishable(&self) -> bool {
        matches!(
            &self.progress,
            CircleOperationProgress::Ready(_) | CircleOperationProgress::Finalizing(_)
        )
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
        let expected_write_id = if self.is_finalizing() {
            if self.operation().creation.close_cancellation.is_some() {
                self.operation_id.cancellation_write_id()
            } else {
                self.operation_id.finalization_write_id()
            }
        } else {
            crate::WriteId::from_generated(self.operation_id.as_str().to_string())
        };
        if commit.write_id != expected_write_id {
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
            CircleOperationIntent::AddMember { .. } => CircleOperationKind::AddMember,
            CircleOperationIntent::RemoveMember { .. } => CircleOperationKind::RemoveMember,
            CircleOperationIntent::ResolveControl { .. } => CircleOperationKind::ResolveControl,
            CircleOperationIntent::Delete => CircleOperationKind::Delete,
        }
    }
}

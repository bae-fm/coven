use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::circle::{
    CircleId, CircleOperationId, CircleOperationKind, CircleOperationState,
    PreparedCircleTransition,
};
use crate::objects::{ExactObjectRef, PreparedExactObject};
use crate::store_commit::{StoreBatchCommit, StoreBatchCommitRef, StoreDeviceHead};

/// A journal whose recorded state contradicts itself or the commit it
/// describes. Produced by the journal's own validation; workflow errors wrap
/// it at the operation boundary.
#[derive(Debug, thiserror::Error)]
pub enum CircleJournalError {
    #[error("Circle operation journal: {0}")]
    Invariant(String),
    #[error("Circle operation journal protocol: {0}")]
    Protocol(#[from] crate::store_commit::StoreProtocolError),
    #[error("Circle operation journal remote object: {0}")]
    RemoteObject(#[from] crate::remote_object::RemoteObjectRecordError),
    #[error("{operation}: {source}")]
    Json {
        operation: &'static str,
        #[source]
        source: serde_json::Error,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleOperationPolicy {
    pub head: StoreDeviceHead,
    pub history_evidence: crate::store_commit::RetainedMergeCommitEvidence,
}

/// One Circle operation as prepared: everything the publication pipeline needs
/// to upload its object graph, and nothing that changes while it does.
///
/// The objects themselves are named, not carried. Their stored bytes live in
/// the payload spool under each reference's stored hash, written before the row
/// that names them, so this value stays KB-scale however large the graph is —
/// and the upload progress that does change per step lives in
/// `circle_operation_uploads`, not here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedCircleOperation {
    pub creation: PreparedCircleTransition,
    pub history: CircleTransitionHistory,
    pub commit_bytes: Vec<u8>,
    pub commit_ref: StoreBatchCommitRef,
    pub prepared_objects: BTreeMap<String, ExactObjectRef>,
    pub policy: CircleOperationPolicy,
}

impl PreparedCircleOperation {
    /// Refuse a byte-carrying object map that is not this operation's own.
    ///
    /// The spool holds the bytes and this value holds the references; a caller
    /// that supplies both is asserting they belong together, and the assertion
    /// is checked rather than trusted.
    pub fn require_prepared_objects(
        &self,
        prepared: &BTreeMap<String, PreparedExactObject>,
    ) -> Result<(), CircleJournalError> {
        if prepared.len() != self.prepared_objects.len()
            || !prepared
                .iter()
                .all(|(step, object)| self.prepared_objects.get(step) == Some(object.reference()))
        {
            return Err(CircleJournalError::Invariant(
                "Circle prepared object bytes name a different object graph than the operation"
                    .to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum CircleTransitionHistory {
    Founder,
    Successor(Box<crate::store_commit::CircleControlRef>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum CircleOperationIntent {
    Create {
        name: String,
    },
    Rename {
        name: String,
    },
    AddMember {
        member_pubkey: String,
        role: crate::circle::CircleRole,
    },
    RemoveMember {
        member_pubkey: String,
    },
    ResolveControl {
        chosen: crate::circle::CircleControlCoord,
    },
    Delete,
}

/// Where one Circle operation stands. Persisted on its own, apart from the
/// operation it describes: this is what a transition rewrites, and the prepared
/// operation is what a transition leaves alone.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum CircleOperationProgress {
    Ready,
    WaitingForCloseResponses,
    Finalizing,
    Blocked {
        block: crate::circle::CircleOperationBlock,
        phase: CircleOperationPhase,
    },
    /// A verified nonactivation proof was accepted. The candidate's exclusive
    /// objects are being exact-deleted and the durable row cleared in the
    /// completing transaction. The retained operation identifies the candidate
    /// graph so a restart resumes the exact same cleanup.
    Discarding,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CircleOperationPhase {
    Initial,
    Finalization,
}

/// One Circle operation as it stands right now: the identity and prepared
/// operation held in `circle_operations`, the phase held beside them, and the
/// upload steps already completed, joined from `circle_operation_uploads`.
///
/// The three parts have different lifetimes on disk, which is why they are
/// stored apart: the operation is written once, the phase changes on
/// transitions, and the upload steps accumulate one row at a time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleOperationJournal {
    pub operation_id: CircleOperationId,
    pub circle_id: CircleId,
    pub intent: CircleOperationIntent,
    pub operation: PreparedCircleOperation,
    pub progress: CircleOperationProgress,
    pub uploaded: BTreeSet<String>,
}

impl CircleOperationJournal {
    /// A freshly prepared operation, with nothing uploaded yet.
    pub fn ready(
        operation_id: CircleOperationId,
        circle_id: CircleId,
        intent: CircleOperationIntent,
        operation: PreparedCircleOperation,
    ) -> Self {
        Self {
            operation_id,
            circle_id,
            intent,
            operation,
            progress: CircleOperationProgress::Ready,
            uploaded: BTreeSet::new(),
        }
    }

    pub fn circle_id(&self) -> CircleId {
        self.circle_id
    }

    pub fn operation(&self) -> &PreparedCircleOperation {
        &self.operation
    }

    pub fn operation_mut(&mut self) -> &mut PreparedCircleOperation {
        &mut self.operation
    }

    /// Refuse an upload step that names no object in this operation. Every
    /// completed step must name one, so a joined upload row that does not is a
    /// journal that contradicts itself.
    pub fn validate_uploaded(&self) -> Result<(), CircleJournalError> {
        for step in &self.uploaded {
            if !self.operation.prepared_objects.contains_key(step) {
                return Err(CircleJournalError::Invariant(format!(
                    "Circle upload marker {step} names no prepared object"
                )));
            }
        }
        Ok(())
    }

    /// The objects of this operation that `remote_objects` holds a record for:
    /// its commit's candidate-exclusive graph, plus the commit and the Store
    /// head published with it.
    ///
    /// The rest of an operation's objects — its control head, roster and
    /// metadata — are shared Circle objects the candidate does not own
    /// exclusively, so completing their upload step has no candidate record to
    /// mark. This names that set so a caller dispatches on it rather than
    /// discovering it by a lookup that comes back empty.
    pub fn candidate_owned_objects(&self) -> Result<BTreeSet<ExactObjectRef>, CircleJournalError> {
        let operation = self.operation();
        let commit = self.commit()?;
        operation.commit_ref.verify_commit(&commit)?;
        let mut objects = crate::remote_object::CandidateObjectGraph::from_commit(&commit)?
            .exact_objects()
            .cloned()
            .collect::<BTreeSet<_>>();
        objects.insert(operation.commit_ref.object.clone());
        objects.insert(
            operation
                .prepared_objects
                .get("store-head")
                .ok_or_else(|| {
                    CircleJournalError::Invariant(
                        "Circle operation lacks its prepared Store head".to_string(),
                    )
                })?
                .clone(),
        );
        Ok(objects)
    }

    /// The candidate graph this operation would activate, closed over the
    /// stored bytes of its objects.
    ///
    /// The bytes come from the caller because this value holds only references
    /// to them: the durable copy is in the payload spool, and the caller that
    /// has just written or read it supplies what it read.
    pub fn closed_remote_objects(
        &self,
        prepared_objects: &BTreeMap<String, PreparedExactObject>,
    ) -> Result<Vec<crate::remote_object::ClosedRemoteObject>, CircleJournalError> {
        let operation = self.operation();
        operation.require_prepared_objects(prepared_objects)?;
        let commit: StoreBatchCommit =
            serde_json::from_slice(&operation.commit_bytes).map_err(|source| {
                CircleJournalError::Json {
                    operation: "parse Circle commit",
                    source,
                }
            })?;
        operation.commit_ref.verify_commit(&commit)?;
        let access_refs = commit
            .circle_controls()
            .iter()
            .flat_map(|control| control.objects().access.iter())
            .collect::<Vec<_>>();
        if access_refs.len() != operation.creation.access.len() {
            return Err(CircleJournalError::Invariant(
                "Circle access material does not cover the signed candidate graph".to_string(),
            ));
        }
        let prepared_for = |object: &ExactObjectRef| {
            prepared_objects
                .values()
                .find(|prepared| prepared.reference() == object)
                .ok_or_else(|| {
                    CircleJournalError::Invariant(format!(
                        "Circle candidate object {} has no prepared bytes",
                        crate::remote_object::remote_object_id(object)
                    ))
                })
        };
        let mut materials = Vec::with_capacity(access_refs.len() * 3 + 1);
        let [circle_reference] = commit.circle_controls() else {
            return Err(CircleJournalError::Invariant(
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
                materials.push(crate::remote_object::CandidateObjectMaterial {
                    object: reference.object.clone(),
                    canonical_semantic_bytes: serde_json::to_vec(intent).map_err(|source| {
                        CircleJournalError::Json {
                            operation: "serialize Circle epoch-close intent",
                            source,
                        }
                    })?,
                    stored_bytes: prepared.stored_bytes().to_vec(),
                });
            }
            (None, None) => {}
            _ => {
                return Err(CircleJournalError::Invariant(
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
                materials.push(crate::remote_object::CandidateObjectMaterial {
                    object: reference.object.clone(),
                    canonical_semantic_bytes: crate::circle::CircleEpochCloseSlotValue::Outcome(
                        outcome.clone(),
                    )
                    .to_bytes(),
                    stored_bytes: prepared.stored_bytes().to_vec(),
                });
            }
            (None, None) => {}
            _ => {
                return Err(CircleJournalError::Invariant(
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
                materials.push(crate::remote_object::CandidateObjectMaterial {
                    object: reference.object.clone(),
                    canonical_semantic_bytes:
                        crate::circle::CircleEpochCloseSlotValue::Cancellation(cancellation.clone())
                            .to_bytes(),
                    stored_bytes: prepared.stored_bytes().to_vec(),
                });
            }
            (None, None) => {}
            _ => {
                return Err(CircleJournalError::Invariant(
                    "Circle epoch-close cancellation does not match its signed candidate graph"
                        .to_string(),
                ));
            }
        }
        let mut bootstrap_blobs = BTreeMap::new();
        for (access, reference) in operation.creation.access.iter().zip(access_refs) {
            let leaf = prepared_for(&reference.leaf.object)?;
            materials.push(crate::remote_object::CandidateObjectMaterial {
                object: reference.leaf.object.clone(),
                canonical_semantic_bytes: serde_json::to_vec(&access.leaf.value).map_err(
                    |source| CircleJournalError::Json {
                        operation: "serialize Circle access leaf",
                        source,
                    },
                )?,
                stored_bytes: leaf.stored_bytes().to_vec(),
            });
            let envelope = prepared_for(&reference.envelope.object)?;
            materials.push(crate::remote_object::CandidateObjectMaterial {
                object: reference.envelope.object.clone(),
                canonical_semantic_bytes: serde_json::to_vec(&access.envelope).map_err(
                    |source| CircleJournalError::Json {
                        operation: "serialize Circle access envelope",
                        source,
                    },
                )?,
                stored_bytes: envelope.stored_bytes().to_vec(),
            });
            if let Some(bootstrap) = &reference.bootstrap {
                let image = prepared_for(&bootstrap.object)?;
                materials.push(crate::remote_object::CandidateObjectMaterial {
                    object: bootstrap.object.clone(),
                    canonical_semantic_bytes: Vec::new(),
                    stored_bytes: image.stored_bytes().to_vec(),
                });
            }
            if let crate::circle::CircleAccessDisposition::Active {
                bootstrap: Some(bootstrap),
                ..
            } = &access.leaf.value.disposition
            {
                for blob in &bootstrap.blobs {
                    let stored = blob.stored().ok_or_else(|| {
                        CircleJournalError::Invariant(
                            "Circle bootstrap row blob has no exact stored locator".to_string(),
                        )
                    })?;
                    let object_id = crate::remote_object::remote_object_id(stored.object());
                    if bootstrap_blobs
                        .insert(object_id, stored.clone())
                        .is_some_and(|existing| existing != *stored)
                    {
                        return Err(CircleJournalError::Invariant(format!(
                            "Circle bootstrap blob {object_id} has conflicting exact references"
                        )));
                    }
                }
            }
        }
        let mut remotes = crate::remote_object::CandidateObjectGraph::from_commit(&commit)
            .and_then(|graph| graph.close(&commit, &operation.commit_ref, materials))?;
        for blob in bootstrap_blobs.into_values() {
            remotes.push(
                crate::remote_object::RemoteObjectRecord::candidate_owned_blob(
                    &blob,
                    operation.commit_ref.clone(),
                    true,
                )?,
            );
        }
        let commit_prepared = prepared_objects.get("store-commit").ok_or_else(|| {
            CircleJournalError::Invariant(
                "Circle operation lacks its prepared Store commit".to_string(),
            )
        })?;
        remotes.push(crate::remote_object::RemoteObjectRecord::candidate_commit(
            operation.commit_ref.clone(),
            &operation.commit_bytes,
            commit_prepared.stored_bytes(),
        )?);
        let prepared = prepared_objects.get("store-head").ok_or_else(|| {
            CircleJournalError::Invariant(
                "Circle operation lacks its prepared Store head".to_string(),
            )
        })?;
        remotes.push(
            crate::remote_object::RemoteObjectRecord::candidate_activated_store_head(
                crate::store_commit::StoreDeviceHeadRef {
                    head_hash: operation.policy.head.head_hash(),
                    object: prepared.reference().clone(),
                },
                &operation.policy.head.to_bytes(),
                prepared.stored_bytes(),
                operation.commit_ref.clone(),
            )?,
        );
        Ok(remotes)
    }

    pub fn state(&self) -> CircleOperationState {
        match &self.progress {
            CircleOperationProgress::Ready => CircleOperationState::Pending,
            CircleOperationProgress::WaitingForCloseResponses => {
                CircleOperationState::WaitingForCloseResponses
            }
            CircleOperationProgress::Finalizing => CircleOperationState::Finalizing,
            CircleOperationProgress::Blocked { block, .. } => CircleOperationState::Blocked {
                block: block.clone(),
            },
            CircleOperationProgress::Discarding => CircleOperationState::Discarding,
        }
    }

    /// Enter cleanup after a verified nonactivation proof was accepted. Legal
    /// from any state whose candidate has not activated — a ready or blocked
    /// initial candidate, or a finalization candidate. A candidate that already
    /// won its slot has no journal row in these states, so no path reaches here.
    pub fn begin_discard(&mut self) -> Result<(), CircleJournalError> {
        match &self.progress {
            CircleOperationProgress::Ready
            | CircleOperationProgress::Finalizing
            | CircleOperationProgress::Blocked { .. } => {}
            CircleOperationProgress::WaitingForCloseResponses
            | CircleOperationProgress::Discarding => {
                return Err(CircleJournalError::Invariant(format!(
                    "Circle operation {} cannot enter discard from its current state",
                    self.operation_id
                )));
            }
        }
        self.progress = CircleOperationProgress::Discarding;
        Ok(())
    }

    pub fn is_discarding(&self) -> bool {
        matches!(&self.progress, CircleOperationProgress::Discarding)
    }

    pub fn block(
        &mut self,
        block: crate::circle::CircleOperationBlock,
    ) -> Result<(), CircleJournalError> {
        let phase = match &self.progress {
            CircleOperationProgress::Ready => CircleOperationPhase::Initial,
            CircleOperationProgress::Finalizing => CircleOperationPhase::Finalization,
            CircleOperationProgress::WaitingForCloseResponses
            | CircleOperationProgress::Blocked { .. }
            | CircleOperationProgress::Discarding => {
                return Err(CircleJournalError::Invariant(format!(
                    "Circle operation {} is not publishable",
                    self.operation_id
                )));
            }
        };
        self.progress = CircleOperationProgress::Blocked { block, phase };
        Ok(())
    }

    /// Return a blocked operation to the phase captured when it blocked, so it
    /// re-enters the idempotent publish pipeline against its exact retained
    /// operation.
    pub fn unblock(&mut self) -> Result<(), CircleJournalError> {
        let CircleOperationProgress::Blocked { phase, .. } = &self.progress else {
            return Err(CircleJournalError::Invariant(format!(
                "Circle operation {} is not blocked",
                self.operation_id
            )));
        };
        self.progress = match phase {
            CircleOperationPhase::Initial => CircleOperationProgress::Ready,
            CircleOperationPhase::Finalization => CircleOperationProgress::Finalizing,
        };
        Ok(())
    }

    pub fn wait_for_close_responses(&mut self) -> Result<(), CircleJournalError> {
        if !matches!(&self.progress, CircleOperationProgress::Ready) {
            return Err(CircleJournalError::Invariant(format!(
                "Circle operation {} is not ready to enter close-response waiting",
                self.operation_id
            )));
        }
        self.progress = CircleOperationProgress::WaitingForCloseResponses;
        Ok(())
    }

    /// Install the freshly prepared finalization operation, replacing the one
    /// that reached its close.
    ///
    /// This is the one point in an operation's life where the prepared
    /// operation changes: the finalization commit is a new candidate graph.
    /// Its steps are named for the object kinds they carry, so they repeat the
    /// names the superseded operation used — which is why the completed uploads
    /// go with the operation they belonged to.
    pub fn begin_finalization(
        &mut self,
        operation: PreparedCircleOperation,
    ) -> Result<(), CircleJournalError> {
        if !matches!(
            &self.progress,
            CircleOperationProgress::WaitingForCloseResponses
        ) {
            return Err(CircleJournalError::Invariant(format!(
                "Circle operation {} is not waiting for close responses",
                self.operation_id
            )));
        }
        self.operation = operation;
        self.uploaded.clear();
        self.progress = CircleOperationProgress::Finalizing;
        Ok(())
    }

    pub fn is_finalizing(&self) -> bool {
        matches!(
            &self.progress,
            CircleOperationProgress::Finalizing
                | CircleOperationProgress::Blocked {
                    phase: CircleOperationPhase::Finalization,
                    ..
                }
        )
    }

    pub fn is_publishable(&self) -> bool {
        matches!(
            &self.progress,
            CircleOperationProgress::Ready | CircleOperationProgress::Finalizing
        )
    }

    pub fn commit(&self) -> Result<StoreBatchCommit, CircleJournalError> {
        serde_json::from_slice(&self.operation().commit_bytes).map_err(|source| {
            CircleJournalError::Json {
                operation: "parse Store commit",
                source,
            }
        })
    }

    pub fn validate_identity(&self) -> Result<(), CircleJournalError> {
        if self.operation().creation.circle_id != self.circle_id {
            return Err(CircleJournalError::Invariant(format!(
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
            crate::write::WriteId::from_generated(self.operation_id.as_str().to_string())
        };
        if commit.write_id != expected_write_id {
            return Err(CircleJournalError::Invariant(format!(
                "circle operation id {} differs from payload commit operation id {}",
                self.operation_id, commit.write_id
            )));
        }
        Ok(())
    }

    pub fn kind(&self) -> CircleOperationKind {
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

//! Durable identity and publication status for one host transaction.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::sync::store_commit::{StoreBatchCommitRef, StoreSerialPredecessor};

/// Store-wide ordering policy selected by the application and signed into the
/// Store protocol root.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WritePolicy {
    MergeConcurrent,
    Serial,
}

/// Stable identity of one successfully committed host transaction.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WriteId(String);

impl WriteId {
    pub(crate) fn from_generated(value: String) -> Self {
        Self(value)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for WriteId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Exact policy-specific position that made a write visible to peers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum PublishedPosition {
    MergeConcurrent {
        device_id: String,
        commit: StoreBatchCommitRef,
    },
    Serial {
        commit: StoreBatchCommitRef,
    },
}

impl PublishedPosition {
    pub fn commit(&self) -> &StoreBatchCommitRef {
        match self {
            Self::MergeConcurrent { commit, .. } | Self::Serial { commit } => commit,
        }
    }
}

/// Stable identity of the one ordered provisional Serial branch.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PendingBranchId(WriteId);

impl PendingBranchId {
    pub(crate) fn from_first_write(write_id: WriteId) -> Self {
        Self(write_id)
    }

    pub fn first_write_id(&self) -> &WriteId {
        &self.0
    }
}

/// A Serial branch ran against `base`, but storage now commits `current`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SerializationConflict {
    pub branch_id: PendingBranchId,
    pub base: StoreSerialPredecessor,
    pub current: StoreSerialPredecessor,
}

/// A semantic write fault. Retrying transport cannot change this result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum WriteBlock {
    InvalidPackage { reason: String },
    InvalidProtocolState { reason: String },
    MissingBlob { namespace: String, id: String },
    LocalUserBlob { namespace: String, id: String },
}

/// Current durable state of one host transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum WriteStatus {
    LocalOnly,
    Pending,
    Publishing,
    Published(Box<PublishedPosition>),
    Conflict(Box<SerializationConflict>),
    Blocked(WriteBlock),
    Resolved(WriteResolution),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum WriteResolution {
    Discarded,
    Replaced { replacement: WriteId },
    Retracted { witness: WriteRetractionWitness },
}

/// Durable proof that a previously published write cannot activate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WriteRetractionWitness {
    original: PublishedPosition,
    nonactivation: crate::sync::remote_object::CandidateNonactivation,
}

impl WriteRetractionWitness {
    pub(crate) fn new(
        original: PublishedPosition,
        nonactivation: crate::sync::remote_object::CandidateNonactivation,
    ) -> Result<Self, String> {
        let candidate = nonactivation
            .reference()
            .map_err(|error| error.to_string())?;
        if original.commit() != &candidate {
            return Err("write retraction proof names another published commit".to_string());
        }
        let witness = Self {
            original,
            nonactivation,
        };
        witness.validate()?;
        Ok(witness)
    }

    pub fn original_position(&self) -> &PublishedPosition {
        &self.original
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        let candidate = self
            .nonactivation
            .reference()
            .map_err(|error| error.to_string())?;
        if self.original.commit() != &candidate {
            return Err("write retraction proof names another published commit".to_string());
        }
        Ok(())
    }
}

/// One table/primary-key identity affected by the shared part of a write.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AffectedRow {
    pub table: String,
    pub primary_key: String,
}

/// Durable write information returned by `CovenHandle::pending_writes`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingWrite {
    pub write_id: WriteId,
    pub status: WriteStatus,
    pub affected_rows: Vec<AffectedRow>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingBranch {
    pub branch_id: PendingBranchId,
    pub base: StoreSerialPredecessor,
    pub current: StoreSerialPredecessor,
    pub writes: Vec<PendingWrite>,
}

/// Result of one successful host transaction and its durable publication identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteReceipt<R> {
    pub value: R,
    pub write_id: WriteId,
    pub status: WriteStatus,
    /// The ordered provisional branch containing this write. MergeConcurrent
    /// writes and transactions with no synchronized changes have no branch.
    pub pending_branch_id: Option<PendingBranchId>,
}

//! Durable identity and publication status for one host transaction.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::protocol::store_commit::StoreBatchCommitRef;

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

/// Exact position that made a write visible to peers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishedPosition {
    pub device_id: String,
    pub commit: StoreBatchCommitRef,
}

impl PublishedPosition {
    pub fn commit(&self) -> &StoreBatchCommitRef {
        &self.commit
    }
}

/// A semantic write fault. Retrying transport cannot change this result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum WriteBlock {
    InvalidPackage {
        reason: String,
    },
    InvalidProtocolState {
        reason: String,
    },
    MissingBlob {
        namespace: String,
        id: String,
    },
    LocalUserBlob {
        namespace: String,
        id: String,
    },
    RotationRequired {
        circle_id: crate::protocol::circle::CircleId,
        removed_members: Vec<String>,
    },
}

/// Current durable state of one host transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum WriteStatus {
    LocalOnly,
    Pending,
    Publishing,
    Published(Box<PublishedPosition>),
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
    nonactivation: crate::protocol::remote_object::CandidateNonactivation,
}

impl WriteRetractionWitness {
    pub(crate) fn new(
        original: PublishedPosition,
        nonactivation: crate::protocol::remote_object::CandidateNonactivation,
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

/// Result of one successful host transaction and its durable publication identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteReceipt<R> {
    pub value: R,
    pub write_id: WriteId,
    pub status: WriteStatus,
}

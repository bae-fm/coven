//! Durable identity and publication status for one host transaction.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::store_commit::{
    AcceptedStoreSnapshotRef, StoreBatchCommitRef, StoreCommitCoord, StoreDeviceRegistrationRef,
};

/// Stable identity of one successfully committed host transaction.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WriteId(String);

impl WriteId {
    pub fn from_generated(value: String) -> Self {
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

/// A reserved author position whose accepted edit is included in a snapshot.
/// The snapshot does not identify which exact candidate completed the edit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotCoveredPosition {
    pub author_registration: StoreDeviceRegistrationRef,
    pub coord: StoreCommitCoord,
    pub snapshot: AcceptedStoreSnapshotRef,
}

/// Durable evidence that a host write was published, with or without its exact commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum PublishedWrite {
    Commit(PublishedPosition),
    Snapshot(SnapshotCoveredPosition),
}

impl PublishedWrite {
    pub fn coord(&self) -> &StoreCommitCoord {
        match self {
            Self::Commit(position) => &position.commit.coord,
            Self::Snapshot(position) => &position.coord,
        }
    }

    pub fn exact_commit(&self) -> Option<&StoreBatchCommitRef> {
        match self {
            Self::Commit(position) => Some(position.commit()),
            Self::Snapshot(_) => None,
        }
    }
}

/// A semantic write fault. Retrying transport cannot change this result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum WriteBlock {
    RebaseConflict(WriteRebaseConflict),
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
        circle_id: crate::circle::CircleId,
        removed_members: Vec<String>,
    },
}

/// Current durable state of one host transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum WriteStatus {
    LocalOnly,
    /// Private intent retained for explicit resolution; it never owes publication.
    LocalOnlyBlocked(WriteBlock),
    Pending,
    Publishing,
    Published(Box<PublishedWrite>),
    Blocked(WriteBlock),
    Resolved(WriteResolution),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum WriteResolution {
    Discarded,
}

/// One table/primary-key identity affected by a write.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AffectedRow {
    pub table: String,
    pub primary_key: String,
}

/// A recorded edit violates a constraint or its retained audience authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(deny_unknown_fields)]
#[error("write {write_id} cannot rebase rows {affected_rows:?}: {reason}")]
pub struct WriteRebaseConflict {
    pub write_id: WriteId,
    /// The exact row for an attributed conflict, or the captured write's rows
    /// when SQLite rejects the transaction without attributing a single row.
    pub affected_rows: Vec<AffectedRow>,
    pub reason: WriteRebaseConflictReason,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum WriteRebaseConflictReason {
    #[error("the private edit conflicts with an accepted shared row")]
    PrivateShared,
    #[error("Circle {circle_id} no longer authorizes the captured edit")]
    InvalidCircleContext { circle_id: crate::circle::CircleId },
    #[error("the edit violates a constraint: {message}")]
    Constraint { message: String },
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

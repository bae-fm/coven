//! Durable identity and publication status for one host transaction.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::sync::store_commit::CommitPosition;

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

/// Exact MergeConcurrent position that made a write visible to peers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishedPosition {
    pub device_id: String,
    pub position: CommitPosition,
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
    Published(PublishedPosition),
    Blocked(WriteBlock),
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

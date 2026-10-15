//! The rows whose commits can change what a waiting operation needs (§3.1,
//! SP waiting; IO `WaitingEvidence`). A commit changing one of a wait's rows
//! is evidence for that wait; no other commit is. The rows are exactly what
//! the waiting work reads, except that a write's writer position counts even
//! when it stops short of the write (IO `PrerequisiteRows`).

use crate::key_scope::KeyScope;
use crate::observation::{CommitSubscription, TableRead};
use crate::sql_value::SqlValue;
use crate::{EntryId, WriteId};
use coven_foundation::id_source::DeviceId;
use coven_merge::Audience;

/// Committed state a waiting operation names.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PrerequisiteRows {
    /// A store-log entry: received, applied, or published by this device.
    Entry(EntryId),
    /// A key-copy object: received, or published by this device.
    KeyCopy {
        /// The publishing device.
        writer: DeviceId,
        /// Its number.
        number: u64,
    },
    /// A write: its writer's applied position, this device's queue, its landing.
    Write(WriteId),
    /// A device's registration.
    Device(DeviceId),
    /// The positions a peer posted.
    PeerPositions(DeviceId),
    /// An audience's pending reload.
    Reload(Audience),
    /// The finality horizon.
    Finality,
    /// The reset and schema boundaries.
    Boundaries,
    /// A provider account's access record.
    ProviderAccess(String),
    /// Another operation's journal row: it advances or finishes.
    Operation(crate::OperationId),
}

/// Every commit made outside one attempt since it began, kept until the
/// attempt names the rows its wait depends on.
pub struct AttemptCommits {
    pub(crate) commits: CommitSubscription,
}

impl AttemptCommits {
    /// Observe `rows` from here on. The result first reports whether a commit
    /// since the attempt began changed one of them: such a commit is evidence
    /// against the wait the attempt ends in (IO `racing_commit_wakes`).
    pub fn watch(self, rows: &[PrerequisiteRows]) -> crate::DatabaseChanges {
        let reads: Vec<_> = rows.iter().flat_map(PrerequisiteRows::reads).collect();
        self.commits.finish(reads.clone());
        crate::DatabaseChanges {
            commits: self.commits,
            reads,
            first: false,
        }
    }
}

fn blob(value: impl Into<Vec<u8>>) -> SqlValue {
    SqlValue::Blob(value.into())
}

fn text(value: &str) -> SqlValue {
    SqlValue::Text(value.as_bytes().to_vec())
}

fn device(device: DeviceId) -> SqlValue {
    blob(device.0.to_be_bytes())
}

fn number(number: u64) -> SqlValue {
    blob(number.to_be_bytes())
}

/// Rows of `table` whose primary key holds `key`, through every listed
/// column: its non-key columns, so an update to any of them counts.
fn rows(table: &str, key: &[(&str, SqlValue)], columns: &[&str]) -> TableRead {
    let keys = key
        .iter()
        .map(|(column, value)| KeyScope::In {
            column: (*column).into(),
            values: vec![value.clone()],
        })
        .reduce(|left, right| KeyScope::And(Box::new(left), Box::new(right)))
        .unwrap_or(KeyScope::All);
    TableRead {
        table: table.into(),
        columns: columns.iter().map(|column| (*column).into()).collect(),
        keys,
        inserted_only: false,
    }
}

const STORE_LOG: &[&str] = &[
    "record",
    "timestamp",
    "author_view",
    "stored_at",
    "outcome",
    "beaten_device",
    "beaten_number",
    "target",
    "retires_device",
    "retires_member",
    "keys_member",
    "keys_signing",
    "registers_device",
];
const LOG_INPUTS: &[&str] = &["observation", "bytes"];
const STORE_LOG_UPLOADS: &[&str] = &["record", "timestamp", "sealing_key"];
const KEY_UPLOADS: &[&str] = &["bytes", "before_device", "before_number"];
const POSITIONS: &[&str] = &["number"];
const UPLOADS: &[&str] = &["record", "sealing_keys"];
const WRITE_LANDINGS: &[&str] = &["stored_at", "header", "signer", "late"];
const DEVICES: &[&str] = &["member", "name", "state"];
const PEER_POSITIONS: &[&str] = &["observation"];
const DATA_RELOADS: &[&str] = &["subject", "update_required"];
const FINALITY: &[&str] = &["horizon"];
const BOUNDARIES: &[&str] = &[
    "kind",
    "version",
    "audience",
    "snapshot_device",
    "snapshot_number",
    "included",
    "input",
    "applied",
];
const PROVIDER_ACCESS: &[&str] = &["intended", "granted", "invitation", "operation", "target"];
const OPERATIONS: &[&str] = &["kind", "last_step", "data", "started_by"];

impl PrerequisiteRows {
    pub(crate) fn reads(&self) -> Vec<TableRead> {
        match self {
            Self::Entry(entry) => {
                let at = [
                    ("device", device(entry.device)),
                    ("number", number(entry.number)),
                ];
                vec![
                    rows("_coven_store_log", &at, STORE_LOG),
                    rows(
                        "_coven_log_inputs",
                        &[
                            ("kind", text("entry")),
                            ("writer", device(entry.device)),
                            ("number", number(entry.number)),
                        ],
                        LOG_INPUTS,
                    ),
                    rows("_coven_store_log_uploads", &at, STORE_LOG_UPLOADS),
                ]
            }
            Self::KeyCopy { writer, number: n } => vec![
                rows(
                    "_coven_log_inputs",
                    &[
                        ("kind", text("key")),
                        ("writer", device(*writer)),
                        ("number", number(*n)),
                    ],
                    LOG_INPUTS,
                ),
                rows(
                    "_coven_key_uploads",
                    &[("writer", device(*writer)), ("number", number(*n))],
                    KEY_UPLOADS,
                ),
            ],
            Self::Write(write) => {
                let at = [
                    ("device", device(write.device)),
                    ("number", number(write.number)),
                ];
                vec![
                    rows(
                        "_coven_positions",
                        &[("device", device(write.device))],
                        POSITIONS,
                    ),
                    rows("_coven_uploads", &at, UPLOADS),
                    rows("_coven_write_landings", &at, WRITE_LANDINGS),
                ]
            }
            Self::Device(id) => vec![rows("_coven_devices", &[("device", device(*id))], DEVICES)],
            Self::PeerPositions(id) => vec![rows(
                "_coven_peer_positions",
                &[("device", device(*id))],
                PEER_POSITIONS,
            )],
            Self::Reload(audience) => vec![rows(
                "_coven_data_reloads",
                &[(
                    "audience",
                    text(&crate::write_encoding::audience_text(audience)),
                )],
                DATA_RELOADS,
            )],
            Self::Finality => vec![rows("_coven_finality", &[], FINALITY)],
            Self::Boundaries => vec![rows("_coven_boundaries", &[], BOUNDARIES)],
            Self::ProviderAccess(account) => vec![rows(
                "_coven_provider_access",
                &[("account", text(account))],
                PROVIDER_ACCESS,
            )],
            Self::Operation(id) => vec![rows(
                "_coven_operations",
                &[("id", SqlValue::Integer(id.0))],
                OPERATIONS,
            )],
        }
    }
}

#[cfg(test)]
#[path = "prerequisite_rows_tests.rs"]
mod tests;

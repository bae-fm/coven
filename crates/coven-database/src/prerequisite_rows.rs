//! The rows whose commits can change what a waiting operation needs (§6.7,
//! §21.2; IO `WaitingEvidence`). A commit changing one of a wait's rows
//! is evidence for that wait; no other commit is. The rows are exactly what
//! the waiting work reads, including advancement of the shared prefix.

use crate::observation::{CommitSubscription, TableRead};
use crate::row_scope::RowScope;
use crate::sql_value::SqlValue;
use crate::WriteId;
use coven_foundation::id_source::DeviceId;
use coven_merge::Audience;

/// Committed state a waiting operation names.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PrerequisiteRows {
    /// The applied store prefix.
    Slot,
    /// This installation's active claim and settlement.
    Claim,
    /// An original write's queue and counted prefix.
    Write(WriteId),
    /// A device's registration.
    Device(DeviceId),
    /// An audience's pending reload.
    Reload(Audience),
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
        crate::DatabaseChanges { commits: self.commits, reads, first: false }
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
    let scope = key
        .iter()
        .map(|(column, value)| RowScope::equal((*column).into(), vec![value.clone()]))
        .fold(RowScope::All, RowScope::and);
    TableRead {
        table: table.into(),
        columns: columns.iter().map(|column| (*column).into()).collect(),
        scope,
        inserted_only: false,
    }
}

const UPLOADS: &[&str] = &["record", "sealing_keys", "claim"];
const DEVICES: &[&str] = &["member", "name", "state"];
const DATA_RELOADS: &[&str] = &["subject"];
const BOUNDARIES: &[&str] =
    &["kind", "version", "audience", "snapshot_device", "snapshot_number", "included", "applied"];
const PROVIDER_ACCESS: &[&str] = &["intended", "granted", "invitation", "operation", "target"];
const OPERATIONS: &[&str] = &["kind", "last_step", "data", "started_by"];

impl PrerequisiteRows {
    pub(crate) fn reads(&self) -> Vec<TableRead> {
        match self {
            Self::Slot => vec![rows("_coven_sequence", &[], &["applied"])],
            Self::Claim => vec![rows(
                "_coven_claim_journal",
                &[],
                &["through", "operation", "settlement", "singleton"],
            )],
            Self::Write(write) => {
                let at = [("device", device(write.device)), ("number", number(write.number))];
                vec![
                    rows("_coven_sequence", &[], &["applied"]),
                    rows("_coven_uploads", &at, UPLOADS),
                ]
            }
            Self::Device(id) => vec![rows("_coven_devices", &[("device", device(*id))], DEVICES)],
            Self::Reload(audience) => vec![rows(
                "_coven_data_reloads",
                &[("audience", text(&crate::write_encoding::audience_text(audience)))],
                DATA_RELOADS,
            )],
            Self::Boundaries => vec![rows("_coven_boundaries", &[], BOUNDARIES)],
            Self::ProviderAccess(account) => {
                vec![rows("_coven_provider_access", &[("account", text(account))], PROVIDER_ACCESS)]
            }
            Self::Operation(id) => {
                vec![rows("_coven_operations", &[("id", SqlValue::Integer(id.0))], OPERATIONS)]
            }
        }
    }
}

#[cfg(test)]
#[path = "prerequisite_rows_tests.rs"]
mod tests;

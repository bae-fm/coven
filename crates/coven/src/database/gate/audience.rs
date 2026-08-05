//! Audience resolution and atomic partitioning of one captured host changeset.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};

use rusqlite::ffi;
use rusqlite::Connection;

use super::ffi::{collect_deletes, for_each_change, ChangeRow, Changegroup};
use super::model::{fk_column_ref, foreign_keys, rows_referencing, truthy, Gates, TableGate};
use super::outbound::{
    deleted_or_live_parent, deleted_row_audience, fk_parent_row, full_state_diff,
    gate_store_outbound, query_column_present, query_column_text, row_id_for_column_value,
    DeletedAudiences, DeletedParent, FkParentRow, FullStateDirection, UnresolvedAudience,
};
use super::{query_mapped_rows, query_row_optional, GateError};
use crate::database::quote_ident;
use crate::protocol::circle::{
    row_routing_id, Audience, CircleControlCoord, CircleId, RowRoutingKey,
};
use crate::protocol::circle_activation::CircleCurrentState;

mod inbound;
mod partitioning;
mod routing;
mod snapshot_pruning;

pub(crate) use inbound::{
    align_inbound_scoped_root_audiences, filter_inbound_circle_changeset,
    filter_inbound_store_rows, normalize_inbound_store_changeset, store_audience_transitions,
};
pub(crate) use partitioning::{
    audience_moves, partition_outbound, validate_scoped_foreign_key_audiences,
};
pub(crate) use routing::{active_circle_control, capture_routing_changes, live_row_audience};
pub(crate) use snapshot_pruning::{
    prune_ineligible_scoped_rows, prune_private_routes_without_rows, retain_snapshot_audience_rows,
    validate_snapshot_routing_state,
};

pub(crate) fn is_routing_table(table: &str) -> bool {
    matches!(table, "_coven_audience" | "_coven_row_routes")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AudiencePartition {
    pub(crate) audience: Audience,
    pub(crate) control: Option<CirclePartitionControl>,
    pub(crate) changeset: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AudienceMove {
    pub(crate) source: Audience,
    pub(crate) destination: Audience,
    pub(crate) rows: BTreeSet<(String, String)>,
    /// The moved row's `_updated_at` after the change that moved it — the version
    /// at which its whole component now lives in `destination`, and the stamp the
    /// routing transitions for that component carry.
    pub(crate) stamp: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PartitionedAudienceWrite {
    pub(crate) partitions: Vec<AudiencePartition>,
    pub(crate) moves: Vec<AudienceMove>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CirclePartitionControl {
    coordinate: CircleControlCoord,
    stored_json: String,
}

impl CirclePartitionControl {
    pub(crate) fn from_stored_json(stored_json: String) -> Result<Self, String> {
        let coordinate: CircleControlCoord =
            serde_json::from_str(&stored_json).map_err(|error| error.to_string())?;
        coordinate.validate().map_err(|error| error.to_string())?;
        Ok(Self {
            coordinate,
            stored_json,
        })
    }

    pub(crate) fn coordinate(&self) -> &CircleControlCoord {
        &self.coordinate
    }

    pub(crate) fn stored_json(&self) -> &str {
        &self.stored_json
    }
}

pub(crate) struct RoutingChanges {
    store_mirror: Vec<u8>,
    private_routes: BTreeMap<Audience, Vec<u8>>,
    deleted_rows: BTreeMap<(String, String), Audience>,
}

#[derive(Default)]
pub(crate) struct StoreAudienceTransitions {
    by_routing_id: HashMap<String, (Audience, String)>,
}

#[derive(Debug)]
pub(crate) struct InboundStoreChangesets {
    pub(crate) mirror: Vec<u8>,
    pub(crate) rows: Vec<u8>,
}

impl RoutingChanges {
    pub(crate) fn empty() -> Self {
        Self {
            store_mirror: Vec::new(),
            private_routes: BTreeMap::new(),
            deleted_rows: BTreeMap::new(),
        }
    }
}

struct PartitionGroup {
    control: Option<CirclePartitionControl>,
    group: Changegroup,
}

#[cfg(test)]
mod tests;

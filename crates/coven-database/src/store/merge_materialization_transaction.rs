mod changeset_application;
mod conflict;

mod activation_records;
mod application;
mod commit_records;
mod retraction;

use std::collections::{BTreeMap, BTreeSet};

use rusqlite::OptionalExtension;
use tracing::{debug, warn};

#[cfg(any(test, feature = "test-utils"))]
pub use changeset_application::{resolve_and_apply_changeset, ApplyResult};
pub use changeset_application::{ValidatedChangeset, WinningRow};
pub use conflict::{IncomingTimestampPolicy, TableSchema};

use super::candidate_records::{
    author_exclusion_activation_for_candidate_on, load_author_exclusion_activation_locator_on,
    validate_terminal_nonactivation_authority_on,
};
use super::owner_promotion::advance_owner_promotion_journal_on;
use super::store_device_state::{
    load_store_device_exclusion_freezes_on, replace_store_device_exclusion_freezes_on,
    store_device_state_for_history_cut_on,
};
use super::{
    apply_store_device_exclusion_freezes_on, load_declared_store_device_state_on,
    verified_store_authority::{
        RetainedReplayTransaction, VerifiedRegistrationLookup, VerifiedStoreLookup,
    },
    StoreDatabase,
};
use crate::blob_records::{
    live_blob_row, validate_live_blob_row, validate_stored_locator_on,
    validate_stored_row_binding_on,
};
use crate::local_blob_cleanup_intents::intents_from_changes as local_blob_cleanup_intents;
use coven_foundation::store_dir::StoreDir;

use crate::remote_object_records::validate_remote_object_on;
use crate::ReclaimCommitActivation;
use crate::{
    candidate_graph_exact_objects, finish_remote_candidate_nonactivation_on,
    insert_store_reclaim_operation_on, load_remote_object_on, load_store_reclaim_operation_on,
    record_reclaimed_store_package_on, store_reclaim_journal_error, update_remote_object_on,
    update_store_reclaim_operation_on, BlobActivation, BlobDecls, Database, DbError,
    DurableStoreReclaimOperation, OwnedVerifiedMergeMaterialization, ReclaimedStorePackage,
    RetainedMergeMaterializationKey, RetainedPackageApplication, VerifiedMergeMaterialization,
};
use crate::{PreparedMergeMaterialization, PreparedMergeMaterializationPackage};
use coven_foundation::changeset::RowChange;
use coven_protocol::audience_package::{AudiencePackage, PackageAudience};
use coven_protocol::blob::locator::RemoteAudience;
use coven_protocol::circle_activation::{VerifiedCircleActivations, VerifiedStreamActivations};
use coven_protocol::membership::{ApplyOutcome, HeldStorePositionReason, LocalStoreMembership};
use coven_protocol::objects::ExactObjectRef;
use coven_protocol::remote_object::{remote_object_id, RemoteObjectRecord, RetainedReplayOwner};
use coven_protocol::store_commit::{
    ActivatedStoreDeviceRegistration, CircleAckRef, ObjectHash, StoreAckRef, StoreBatchCommit,
    StoreBatchCommitRef, StoreCommitCoord, StoreDeviceHead, StoreDeviceProposalState,
    StoreDeviceRegistrationRef, StoreHistoryCut, VerifiedStoreBatchCommit,
    VerifiedStoreDeviceOperations,
};
use coven_protocol::synced_schema::SyncedTable;
use coven_protocol::write::{PublishedPosition, WriteId, WriteResolution, WriteStatus};

pub(crate) use commit_records::derive_materialized_store_device_state_on;

pub(crate) struct AppliedMergeMaterialization {
    pub outcome: ApplyOutcome,
    pub max_updated_at: Option<coven_protocol::hlc::Timestamp>,
    pub write_status_notifications: Vec<(
        coven_protocol::write::WriteId,
        coven_protocol::write::WriteStatus,
    )>,
    pub retained: Option<crate::OwnedVerifiedMergeMaterialization>,
}

pub(super) fn retract_verified_merge_materializations(
    transaction: &MergeMaterializationTransaction<'_, '_>,
    root: &coven_protocol::store_commit::StoreRootRef,
    retained_replay: &mut RetainedReplayTransaction,
    retractions: Vec<coven_protocol::remote_object::VerifiedCandidateNonactivation>,
) -> Result<Vec<(WriteId, WriteStatus)>, DbError> {
    transaction.retract_verified_merge_materializations(root, retained_replay, retractions)
}

pub enum MergeSubsetOutcome {
    Applied(Vec<crate::WinningRow>),
    ConstraintConflict(Vec<String>),
}

impl MergeSubsetOutcome {
    fn extend_winning_rows(
        self,
        winning_rows: &mut Vec<crate::WinningRow>,
    ) -> Result<(), Vec<String>> {
        match self {
            Self::Applied(rows) => {
                winning_rows.extend(rows);
                Ok(())
            }
            Self::ConstraintConflict(tables) => Err(tables),
        }
    }
}

/// Advance `max` past the greatest `_updated_at` among `changes`, parsing each
/// as an HLC [`coven_protocol::hlc::Timestamp`]. A row whose `_updated_at` fails to
/// parse is logged and skipped — it must not panic the pull or silently default
/// the clock.
///
/// `max` becomes the value the caller advances the local HLC past, and that
/// advance is deliberately uncapped (it trusts a value already written to disk).
/// So the bound lives here, at the point a stamp is *collected*: a grossly-future
/// stamp — beyond `receiver_wall_ms` +
/// [`coven_protocol::hlc::MAX_FUTURE_SKEW_MS`] — is logged and skipped, so it can
/// never ratchet the clock. A conflicting row with such a stamp was already
/// refused by the apply, but a *non-conflicting* INSERT (no local row to conflict
/// with) reaches here as an applied row, so this is the gate that stops it from
/// dragging the clock forward.
fn advance_max_updated_at(
    max: &mut Option<coven_protocol::hlc::Timestamp>,
    changes: &[RowChange],
    schema: &TableSchema,
    receiver_wall_ms: u64,
) {
    for change in changes {
        let Some(idx) = schema.updated_at(&change.table) else {
            // Incoming apply rejects the entire changeset before mutation when any
            // operation names an undeclared table. Reaching this after a successful
            // apply means its walked rows and the apply schema disagree.
            debug!(
                table = %change.table,
                "applied changeset references a table absent from the synced set, not advancing HLC"
            );
            continue;
        };
        let Some(raw) = change.col(idx) else {
            // A DELETE carries no new-state columns, and an absent value at the
            // schema's `_updated_at` index means this row change has no stamp to
            // advance past — expected for deletes, but a genuinely wrong index
            // or a schema mismatch surfaces here as the same absence, so log it.
            debug!(
                table = %change.table,
                updated_at_idx = idx,
                "applied row change has no _updated_at value (DELETE or absent new-state column), not advancing HLC past it"
            );
            continue;
        };
        match coven_protocol::hlc::Timestamp::parse(raw) {
            Some(ts) if !ts.is_within_future_bound(receiver_wall_ms) => warn!(
                table = %change.table,
                value = raw,
                receiver_wall_ms,
                "applied row's _updated_at is grossly beyond the offline-skew allowance, not advancing HLC past it"
            ),
            Some(ts) => {
                if max.as_ref().is_none_or(|cur| ts > *cur) {
                    *max = Some(ts);
                }
            }
            None => warn!(
                table = %change.table,
                value = raw,
                "applied row has an unparseable _updated_at, not advancing HLC past it"
            ),
        }
    }
}

pub struct MergeMaterializationTransaction<'transaction, 'connection> {
    transaction: &'transaction rusqlite::Transaction<'connection>,
    /// Where this store's payload files go. Materializing a pulled commit
    /// writes remote object rows, and a row that names a payload installs it in
    /// the same transaction.
    store_dir: &'transaction StoreDir,
}

impl<'transaction, 'connection> MergeMaterializationTransaction<'transaction, 'connection> {
    pub fn new(
        transaction: &'transaction rusqlite::Transaction<'connection>,
        store_dir: &'transaction StoreDir,
    ) -> Self {
        Self {
            transaction,
            store_dir,
        }
    }

    pub fn circle_current_state_is_deleted(
        &self,
        circle_id: coven_protocol::circle::CircleId,
    ) -> Result<bool, DbError> {
        Ok(
            StoreDatabase::circle_current_state_on(self.transaction, circle_id)?
                .is_some_and(|state| state.is_deleted()),
        )
    }

    /// Install only blob bindings whose exact row stamp won the enclosing
    /// changeset. App rows, locator facts, and the materialized commit position
    /// share this transaction and therefore commit or roll back together.
    pub fn install_winning_blob_bindings(
        &self,
        gates: &crate::Gates,
        synced_tables: &[SyncedTable],
        package: &AudiencePackage,
        activation: &BlobActivation,
        winning_rows: &[WinningRow],
    ) -> Result<usize, DbError> {
        let conn = self.transaction;
        if package.commit_coord() != &activation.coord {
            return Err(DbError::Message(format!(
                "blob activation {:?} does not match audience package {:?}",
                activation.coord,
                package.commit_coord()
            )));
        }

        let package_audience = package.audience().remote_audience();
        for winner in winning_rows {
            if crate::is_routing_table(&winner.table) {
                continue;
            }
            let Some(table) = synced_tables
                .iter()
                .find(|table| table.name() == winner.table)
            else {
                return Err(DbError::Message(format!(
                    "winning changeset row names undeclared table {:?}",
                    winner.table
                )));
            };
            let Some(declaration) = table.blob() else {
                continue;
            };
            match winner.row_stamp.as_deref() {
                Some(row_stamp) => conn.execute(
                    "DELETE FROM row_blob_locators
                     WHERE table_name = ?1 AND row_id = ?2 AND column_name = ?3
                       AND row_stamp <> ?4",
                    rusqlite::params![
                        winner.table,
                        winner.row_id,
                        declaration.id_column,
                        row_stamp,
                    ],
                ),
                None => conn.execute(
                    "DELETE FROM row_blob_locators
                     WHERE table_name = ?1 AND row_id = ?2 AND column_name = ?3",
                    rusqlite::params![winner.table, winner.row_id, declaration.id_column],
                ),
            }
            .map_err(DbError::from)?;
        }
        let mut installed = 0;
        for binding in package.blob_bindings() {
            let Some(table) = synced_tables
                .iter()
                .find(|table| table.name() == binding.table())
            else {
                return Err(DbError::Message(format!(
                    "blob binding names undeclared table {:?}",
                    binding.table()
                )));
            };
            let declaration = table.blob().ok_or_else(|| {
                DbError::Message(format!(
                    "blob binding names table {:?}, which has no blob declaration",
                    binding.table()
                ))
            })?;
            if binding.column() != declaration.id_column {
                return Err(DbError::Message(format!(
                    "blob binding column {:?} does not match declared blob-id column {:?} on table {:?}",
                    binding.column(), declaration.id_column, binding.table()
                )));
            }

            let Some(row) = live_blob_row(conn, binding.table(), binding.row_id(), declaration)?
            else {
                continue;
            };
            if row.stamp != binding.row_stamp() {
                continue;
            }

            let live_audience =
                crate::live_row_audience(conn, gates, binding.table(), binding.row_id()).map_err(
                    |error| {
                        DbError::context(
                            format!(
                                "resolve winning blob row audience for {:?}/{:?}",
                                binding.table(),
                                binding.row_id()
                            ),
                            error,
                        )
                    },
                )?;
            let live_audience = RemoteAudience::try_from(live_audience).map_err(|error| {
                DbError::context(
                    format!(
                        "winning blob row {:?}/{:?} is not remote",
                        binding.table(),
                        binding.row_id()
                    ),
                    error,
                )
            })?;
            if live_audience != package_audience {
                return Err(DbError::Message(format!(
                    "winning blob row {:?}/{:?} belongs to {:?}, but its package belongs to {:?}",
                    binding.table(),
                    binding.row_id(),
                    live_audience,
                    package_audience
                )));
            }
            validate_live_blob_row(binding, declaration, &row, &live_audience)?;

            let locator = binding.blob().locator();
            let locator_hash = locator.locator_hash();
            let object_id =
                coven_protocol::remote_object::remote_object_id(binding.blob().object());
            let remote = load_remote_object_on(conn, object_id)?;
            if !remote.is_activated_stored_blob() {
                return Err(DbError::Message(format!(
                    "blob locator {locator_hash} does not reference an activated uploaded blob"
                )));
            }
            validate_remote_object_on(
                conn,
                object_id,
                binding.blob().object(),
                &locator.to_bytes(),
            )?;
            conn.execute(
                "INSERT INTO blob_locators
                 (remote_object_id, locator_hash)
                 VALUES (?1, ?2)
                 ON CONFLICT(remote_object_id) DO NOTHING",
                rusqlite::params![object_id.to_string(), locator_hash.to_string()],
            )
            .map_err(DbError::from)?;
            validate_stored_locator_on(conn, binding.blob())?;

            let audience_authority =
                serde_json::to_string(package.audience()).map_err(|error| {
                    DbError::context("serialize row blob audience authority", error)
                })?;
            conn.execute(
                "INSERT INTO row_blob_locators
                 (table_name, row_id, column_name, row_stamp, audience_authority, remote_object_id)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(table_name, row_id, column_name, row_stamp) DO NOTHING",
                rusqlite::params![
                    binding.table(),
                    binding.row_id(),
                    binding.column(),
                    binding.row_stamp(),
                    audience_authority,
                    object_id.to_string(),
                ],
            )
            .map_err(DbError::from)?;
            validate_stored_row_binding_on(conn, binding, package.audience(), object_id)?;
            installed += 1;
        }
        Ok(installed)
    }
}

#[cfg(test)]
mod retraction_tests;

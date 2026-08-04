mod changeset_application;
mod conflict;

use std::collections::{BTreeMap, BTreeSet};

use rusqlite::OptionalExtension;
use tracing::{debug, warn};

#[cfg(test)]
pub(crate) use changeset_application::{resolve_and_apply_changeset, ApplyResult};
pub(crate) use changeset_application::{ValidatedChangeset, WinningRow};
pub(crate) use conflict::{IncomingTimestampPolicy, TableSchema};

use super::candidate_records::{
    author_exclusion_activation_for_candidate_on, load_author_exclusion_activation_locator_on,
    validate_terminal_nonactivation_authority_on,
};
use super::store_device_state::{
    load_store_device_exclusion_freezes_on, replace_store_device_exclusion_freezes_on,
    store_device_state_for_history_cut_on,
};
use super::{
    apply_store_device_exclusion_freezes_on, load_declared_store_device_state_on,
    RetainedMergeMaterializationCache, StoreDatabase,
};
use crate::changeset::RowChange;
use crate::database::blob_records::{
    live_blob_row, validate_live_blob_row, validate_stored_locator_on,
    validate_stored_row_binding_on,
};
use crate::database::local_blob_cleanup_intents::intents_from_changes as local_blob_cleanup_intents;
use crate::database::remote_object_records::{
    validate_remote_object_on, RemoteStoredRepresentationRef,
};
use crate::database::ReclaimCommitActivation;
use crate::database::{
    candidate_graph_exact_objects, finish_remote_candidate_nonactivation_on,
    insert_store_reclaim_operation_on, load_activated_registration_on, load_remote_object_on,
    load_store_reclaim_operation_on, record_reclaimed_store_package_on,
    required_store_root_authority_on, store_reclaim_journal_error, update_remote_object_on,
    update_store_reclaim_operation_on, BlobActivation, BlobDecls, Database, DbError,
    DurableStoreReclaimOperation, OwnedVerifiedMergeMaterialization, ReclaimedStorePackage,
    RetainedMergeMaterializationKey, RetainedPackageApplication, VerifiedMergeMaterialization,
};
use crate::database::{PreparedMergeMaterialization, PreparedMergeMaterializationPackage};
use crate::protocol::audience_package::{AudiencePackage, PackageAudience};
use crate::protocol::blob::locator::RemoteAudience;
use crate::protocol::circle_activation::{VerifiedCircleActivations, VerifiedStreamActivations};
use crate::protocol::membership::{ApplyOutcome, HeldStorePositionReason, LocalStoreMembership};
use crate::protocol::objects::ExactObjectRef;
use crate::protocol::remote_object::{remote_object_id, RemoteObjectRecord, RetainedReplayOwner};
use crate::protocol::store_commit::{
    ActivatedStoreDeviceRegistration, CircleAckRef, CommitFrontier, ObjectHash, StoreAckRef,
    StoreBatchCommit, StoreBatchCommitRef, StoreCommitCoord, StoreDeviceHead,
    StoreDeviceProposalState, StoreDeviceRegistrationRef, StoreHistoryCut,
    VerifiedStoreBatchCommit, VerifiedStoreDeviceOperations,
};
use crate::protocol::synced_schema::SyncedTable;
use crate::write::{PublishedPosition, WriteId, WriteResolution, WriteStatus};

pub(crate) struct AppliedMergeMaterialization {
    pub(crate) outcome: ApplyOutcome,
    pub(crate) max_updated_at: Option<crate::protocol::hlc::Timestamp>,
    pub(crate) write_status_notifications: Vec<(crate::WriteId, crate::WriteStatus)>,
    pub(crate) retained: Option<crate::database::OwnedVerifiedMergeMaterialization>,
}

enum MergeSubsetOutcome {
    Applied(Vec<crate::database::WinningRow>),
    ConstraintConflict(Vec<String>),
}

impl MergeSubsetOutcome {
    fn extend_winning_rows(
        self,
        winning_rows: &mut Vec<crate::database::WinningRow>,
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
/// as an HLC [`crate::protocol::hlc::Timestamp`]. A row whose `_updated_at` fails to
/// parse is logged and skipped — it must not panic the pull or silently default
/// the clock.
///
/// `max` becomes the value the caller advances the local HLC past, and that
/// advance is deliberately uncapped (it trusts a value already written to disk).
/// So the bound lives here, at the point a stamp is *collected*: a grossly-future
/// stamp — beyond `receiver_wall_ms` +
/// [`crate::protocol::hlc::MAX_FUTURE_SKEW_MS`] — is logged and skipped, so it can
/// never ratchet the clock. A conflicting row with such a stamp was already
/// refused by the apply, but a *non-conflicting* INSERT (no local row to conflict
/// with) reaches here as an applied row, so this is the gate that stops it from
/// dragging the clock forward.
fn advance_max_updated_at(
    max: &mut Option<crate::protocol::hlc::Timestamp>,
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
        match crate::protocol::hlc::Timestamp::parse(raw) {
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

fn complete_merge_retraction_closure(
    direct_dependencies: &BTreeMap<StoreBatchCommitRef, BTreeSet<StoreBatchCommitRef>>,
    mut closure: BTreeSet<StoreBatchCommitRef>,
) -> BTreeSet<StoreBatchCommitRef> {
    loop {
        let additions = direct_dependencies
            .iter()
            .filter(|(reference, _)| !closure.contains(*reference))
            .filter(|(_, dependencies)| {
                dependencies
                    .iter()
                    .any(|dependency| closure.contains(dependency))
            })
            .map(|(reference, _)| reference.clone())
            .collect::<Vec<_>>();
        if additions.is_empty() {
            return closure;
        }
        closure.extend(additions);
    }
}

fn require_exact_merge_retraction_closure(
    direct_dependencies: &BTreeMap<StoreBatchCommitRef, BTreeSet<StoreBatchCommitRef>>,
    roots: BTreeSet<StoreBatchCommitRef>,
    provided: &BTreeSet<StoreBatchCommitRef>,
) -> Result<(), DbError> {
    let required = complete_merge_retraction_closure(direct_dependencies, roots);
    if provided != &required {
        return Err(DbError::Message(
            "verified terminal retractions do not exactly cover excluded materializations"
                .to_string(),
        ));
    }
    Ok(())
}

pub(crate) struct MergeMaterializationTransaction<'transaction, 'connection> {
    transaction: &'transaction rusqlite::Transaction<'connection>,
}

impl<'transaction, 'connection> MergeMaterializationTransaction<'transaction, 'connection> {
    pub(crate) fn new(transaction: &'transaction rusqlite::Transaction<'connection>) -> Self {
        Self { transaction }
    }
}

impl MergeMaterializationTransaction<'_, '_> {
    pub(super) fn circle_current_state_is_deleted(
        &self,
        circle_id: crate::protocol::circle::CircleId,
    ) -> Result<bool, DbError> {
        Ok(
            StoreDatabase::circle_current_state_on(self.transaction, circle_id)?
                .is_some_and(|state| state.is_deleted()),
        )
    }

    /// Install only blob bindings whose exact row stamp won the enclosing
    /// changeset. App rows, locator facts, and the materialized commit position
    /// share this transaction and therefore commit or roll back together.
    pub(crate) fn install_winning_blob_bindings(
        &self,
        gates: &crate::database::Gates,
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
            if crate::database::is_routing_table(&winner.table) {
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
                crate::database::live_row_audience(conn, gates, binding.table(), binding.row_id())
                    .map_err(|error| {
                        DbError::Message(format!(
                            "resolve winning blob row audience for {:?}/{:?}: {error}",
                            binding.table(),
                            binding.row_id()
                        ))
                    })?;
            let live_audience = RemoteAudience::try_from(live_audience).map_err(|error| {
                DbError::Message(format!(
                    "winning blob row {:?}/{:?} is not remote: {error}",
                    binding.table(),
                    binding.row_id()
                ))
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
                crate::protocol::remote_object::remote_object_id(binding.blob().object());
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
                RemoteStoredRepresentationRef::Blob,
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
                    DbError::Message(format!("serialize row blob audience authority: {error}"))
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

    fn record_store_reclaim_activation(
        &self,
        commit: &StoreBatchCommit,
        commit_ref: &StoreBatchCommitRef,
        activation: &ReclaimCommitActivation,
    ) -> Result<(), DbError> {
        activation.validate().map_err(store_reclaim_journal_error)?;
        if activation.commit() != commit_ref {
            return Err(DbError::Message(
                "Store reclaim activation evidence names another commit".to_string(),
            ));
        }
        if let Some(authorization) = commit.reclaim_authorization() {
            let operation_id = authorization.authorization_hash;
            let next = DurableStoreReclaimOperation::Authorized {
                authorization: authorization.clone(),
                activation: activation.clone(),
            };
            next.validate().map_err(store_reclaim_journal_error)?;
            match load_store_reclaim_operation_on(self.transaction, operation_id)? {
                Some(expected)
                    if matches!(
                        &expected,
                        DurableStoreReclaimOperation::AuthorizationCandidate { object, .. }
                            | DurableStoreReclaimOperation::AuthorizationReplacing { object, .. }
                            if object.authorization_ref() == authorization
                    ) =>
                {
                    update_store_reclaim_operation_on(self.transaction, &expected, &next)?;
                }
                Some(existing) if existing == next => {}
                Some(_) => {
                    return Err(DbError::Message(
                        "reclaim authorization conflicts with its durable operation".to_string(),
                    ));
                }
                None => insert_store_reclaim_operation_on(self.transaction, &next)?,
            }
        }
        if let Some(receipt) = commit.reclaim_receipt() {
            let operation_id = receipt.authorization.authorization_hash;
            let expected = load_store_reclaim_operation_on(self.transaction, operation_id)?
                .ok_or_else(|| {
                    DbError::Message("reclaim receipt has no durable authorization".to_string())
                })?;
            let (authorization, authorization_activation) = match &expected {
                DurableStoreReclaimOperation::AuthorizationCandidate { .. } => {
                    return Err(DbError::Message(
                        "reclaim receipt precedes authorization activation".to_string(),
                    ));
                }
                DurableStoreReclaimOperation::AuthorizationReplacing { .. } => {
                    return Err(DbError::Message(
                        "reclaim receipt precedes replacement authorization activation".to_string(),
                    ));
                }
                DurableStoreReclaimOperation::Authorized {
                    authorization,
                    activation,
                } => (authorization.clone(), activation.clone()),
                DurableStoreReclaimOperation::AbsentVerified {
                    authorization,
                    authorization_activation,
                    ..
                } => (authorization.clone(), authorization_activation.clone()),
                DurableStoreReclaimOperation::ReceiptCandidate {
                    authorization,
                    authorization_activation,
                    object,
                    ..
                } if matches!(
                    &**object,
                    crate::sync::store::DurableStoreReclaimObject::Receipt {
                        receipt_ref,
                        ..
                    } if receipt_ref == receipt
                ) =>
                {
                    (authorization.clone(), authorization_activation.clone())
                }
                DurableStoreReclaimOperation::ReceiptReplacing {
                    authorization,
                    authorization_activation,
                    object,
                    ..
                } if matches!(
                    &**object,
                    crate::sync::store::DurableStoreReclaimObject::Receipt {
                        receipt_ref,
                        ..
                    } if receipt_ref == receipt
                ) =>
                {
                    (authorization.clone(), authorization_activation.clone())
                }
                DurableStoreReclaimOperation::ReceiptCandidate { .. } => {
                    return Err(DbError::Message(
                        "reclaim receipt differs from its durable candidate".to_string(),
                    ));
                }
                DurableStoreReclaimOperation::ReceiptReplacing { .. } => {
                    return Err(DbError::Message(
                        "reclaim receipt differs from its replacement candidate".to_string(),
                    ));
                }
                DurableStoreReclaimOperation::Completed { .. } => {
                    return Err(DbError::Message(
                        "reclaim authorization already has a receipt".to_string(),
                    ));
                }
            };
            let next = DurableStoreReclaimOperation::Completed {
                authorization: authorization.clone(),
                authorization_activation: authorization_activation.clone(),
                receipt: receipt.clone(),
                receipt_activation: activation.clone(),
            };
            let reclaimed = ReclaimedStorePackage::receipted(
                authorization,
                authorization_activation,
                receipt.clone(),
                activation.clone(),
            )
            .map_err(store_reclaim_journal_error)?;
            record_reclaimed_store_package_on(self.transaction, &reclaimed)?;
            update_store_reclaim_operation_on(self.transaction, &expected, &next)?;
        }
        Ok(())
    }

    pub(crate) fn replace_store_device_exclusion_freezes_from_replay(&self) -> Result<(), DbError> {
        let root = required_store_root_authority_on(self.transaction)?;
        let existing = load_store_device_exclusion_freezes_on(self.transaction, &root)?;
        let frontier = StoreDatabase::materialized_frontier_on(self.transaction, None)?
            .into_values()
            .map(|reference| (reference.coord.stream_id, reference))
            .collect::<BTreeMap<_, _>>();
        let (_, state) =
            store_device_state_for_history_cut_on(self.transaction, &StoreHistoryCut(frontier))?;
        let mut retained = Vec::new();
        for freeze in existing.into_values() {
            let proposal_state = state
                .devices
                .get(&freeze.proposal.target.device_id)
                .and_then(|record| record.proposals.get(&freeze.proposal.proposal_id));
            match proposal_state {
                Some(StoreDeviceProposalState::Pending { proposal })
                    if proposal == &freeze.proposal =>
                {
                    retained.push(freeze);
                }
                Some(StoreDeviceProposalState::Cancelled { outcome })
                    if outcome.proposal == freeze.proposal => {}
                Some(StoreDeviceProposalState::Superseded { proposal, .. })
                    if proposal == &freeze.proposal => {}
                None => {}
                Some(_) => {
                    return Err(DbError::Message(
                        "stored device exclusion freeze differs from replayed device state"
                            .to_string(),
                    ));
                }
            }
        }
        retained.sort_by_key(|freeze| freeze.proposal.proposal_id);
        replace_store_device_exclusion_freezes_on(self.transaction, &retained)
    }

    pub(crate) fn complete_membership_journal(
        &self,
        completion: crate::protocol::membership_mutation::StoreMembershipJournalCompletion,
        candidate: &StoreBatchCommitRef,
    ) -> Result<(), DbError> {
        match completion {
            crate::protocol::membership_mutation::StoreMembershipJournalCompletion::Mutation {
                intent_hash,
                progress_bytes,
                remote_objects,
            } => StoreDatabase::record_activated_membership_candidate_mutation_on(
                self.transaction,
                intent_hash,
                candidate,
                &remote_objects
                    .iter()
                    .map(|remote| remote.object().clone())
                    .collect::<Vec<_>>(),
                progress_bytes,
                crate::database::MembershipMutationActivation::WithoutRotation,
            ),
            crate::protocol::membership_mutation::StoreMembershipJournalCompletion::RotationMutation {
                intent_hash,
                progress_bytes,
                generation,
                remote_objects,
            } => StoreDatabase::record_activated_membership_candidate_mutation_on(
                self.transaction,
                intent_hash,
                candidate,
                &remote_objects
                    .iter()
                    .map(|remote| remote.object().clone())
                    .collect::<Vec<_>>(),
                progress_bytes,
                crate::database::MembershipMutationActivation::Rotation { generation },
            ),
            crate::protocol::membership_mutation::StoreMembershipJournalCompletion::OwnerPromotion {
                transition,
                remote_objects,
            } => {
                let mut unique = std::collections::BTreeSet::new();
                let object_ids = remote_objects
                    .iter()
                    .map(|remote| remote.object_id())
                    .map(|object_id| {
                        if unique.insert(object_id) {
                            Ok(object_id)
                        } else {
                            Err(DbError::Message(
                                "activated Owner-promotion graph repeats an exact object"
                                    .to_string(),
                            ))
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                if object_ids.is_empty() {
                    return Err(DbError::Message(
                        "activated Owner-promotion graph is empty".to_string(),
                    ));
                }
                self.activate_store_operation_remote_objects(candidate, &object_ids)?;
                let (journal_key, target_key, previous_value, next_value, remote_objects) =
                    transition.into_values();
                StoreDatabase::advance_owner_promotion_journal_on(
                    self.transaction,
                    journal_key,
                    target_key,
                    previous_value,
                    next_value,
                    remote_objects,
                )
            }
        }
    }

    pub(crate) fn record_obsolete_blob_cleanup_intent(
        &self,
        declarations: &crate::database::BlobDecls,
        intent: &crate::database::local_blob_cleanup_intents::LocalBlobCleanupIntent,
    ) -> Result<(), DbError> {
        super::local_blob_cleanup::record_obsolete_copy_intents_on(
            self.transaction,
            declarations,
            intent,
        )
    }

    pub(super) fn record_materialized_merge_commit(
        &self,
        root: &crate::protocol::store_commit::StoreRootRef,
        verified_commit: &VerifiedStoreBatchCommit,
        registrations: &[ActivatedStoreDeviceRegistration],
        activation_head: &StoreDeviceHead,
        activation_head_object: &ExactObjectRef,
        history_summary: &crate::protocol::store_commit::RetainedVerifiedMergeHistorySummary,
        packages: &[AudiencePackage],
        package_application: Option<RetainedPackageApplication>,
    ) -> Result<(), DbError> {
        let commit = verified_commit.value();
        let commit_ref = verified_commit.reference();
        let device_operations = VerifiedStoreDeviceOperations::without_exclusions(commit)
            .map_err(|error| DbError::Message(error.to_string()))?;
        let circle_activations = VerifiedCircleActivations::none(commit, commit_ref)
            .map_err(|error| DbError::Message(error.to_string()))?;
        let materialization = VerifiedMergeMaterialization::verify(
            root,
            verified_commit,
            registrations,
            &device_operations,
            &circle_activations,
            activation_head,
            activation_head_object,
            history_summary,
            None,
            packages,
            package_application,
        )?;
        self.record_verified_merge_materialization(materialization)?;
        Ok(())
    }

    pub(crate) fn record_verified_merge_materialization(
        &self,
        materialization: VerifiedMergeMaterialization<'_>,
    ) -> Result<OwnedVerifiedMergeMaterialization, DbError> {
        let conn = self.transaction;
        self.record_author_exclusion_activations(
            materialization.verified_commit(),
            materialization.device_operations(),
            materialization.activation_head(),
            materialization.activation_head_object(),
        )?;
        let root = required_store_root_authority_on(conn)?;
        let state_after = self.derive_materialized_store_device_state(
            &root,
            materialization.commit(),
            materialization.device_operations(),
        )?;
        let expected_post_state =
            crate::protocol::store_commit::StoreDeviceStateRef::from_resolved(
                CommitFrontier(
                    materialization
                        .history_summary()
                        .frontier()
                        .map_err(|error| DbError::Message(error.to_string()))?,
                ),
                &state_after,
            )
            .map_err(|error| DbError::Message(error.to_string()))?;
        if materialization.history_summary().post_state != expected_post_state {
            return Err(DbError::Message(
                "retained Merge history summary differs from the derived post-commit device state"
                    .to_string(),
            ));
        }
        let (retained_commit_ref, retained) =
            crate::database::StoreDatabase::retain_merge_materialization_on(
                conn,
                &root,
                &materialization,
            )?;
        StoreDatabase::record_circle_bootstrap_coverage_on(
            conn,
            &root,
            materialization.commit_ref(),
            materialization.circle_activations(),
        )?;
        let activation = ReclaimCommitActivation::new(
            materialization.commit_ref().clone(),
            crate::protocol::store_commit::StoreDeviceHeadRef {
                head_hash: materialization.activation_head().head_hash(),
                object: materialization.activation_head_object().clone(),
            },
        )
        .map_err(store_reclaim_journal_error)?;
        self.record_materialized_commit_with_device_operations(
            materialization.verified_commit(),
            materialization.device_operations(),
            materialization.circle_activations().stream_activations(),
            &retained_commit_ref,
            &activation,
        )?;
        Ok(retained)
    }

    fn derive_materialized_store_device_state(
        &self,
        root: &crate::protocol::store_commit::StoreRootRef,
        commit: &StoreBatchCommit,
        device_operations: &VerifiedStoreDeviceOperations,
    ) -> Result<crate::protocol::store_commit::ResolvedStoreDeviceState, DbError> {
        let conn = self.transaction;
        let mut device_state = load_declared_store_device_state_on(conn, &commit.device_state)?;
        let recovery_author = commit
            .device_registrations()
            .iter()
            .find_map(|activation| {
                if activation.registration != commit.author_registration {
                    return None;
                }
                let crate::protocol::store_commit::StoreDeviceRegistrationActivationRef::Recovery {
                    node,
                    ..
                } = &activation.authority
                else {
                    return None;
                };
                Some((&activation.registration, node))
            })
            .map(|(registration_ref, node)| {
                let registration = load_activated_registration_on(conn, root, registration_ref)?;
                let crate::protocol::store_commit::StoreDeviceRegistrationOrigin::Recovery {
                    owner_grant,
                    ..
                } = registration.origin
                else {
                    return Err(DbError::Message(
                        "recovery activation author has a non-recovery registration origin"
                            .to_string(),
                    ));
                };
                Ok((
                    registration_ref.clone(),
                    crate::protocol::store_commit::OwnerRecoveryCursor {
                        owner_grant,
                        position: crate::protocol::store_commit::OwnerRecoveryPosition::At {
                            node: node.clone(),
                        },
                    },
                ))
            })
            .transpose()?;
        if let Some((registration, recovery)) = &recovery_author {
            device_state = device_state
                .activate_registration(registration.clone(), Some(recovery.clone()))
                .map_err(|error| DbError::Message(error.to_string()))?;
        }
        let active_author = device_state
            .devices
            .get(&commit.author_registration.device_id)
            .is_some_and(|record| {
                record.registration == commit.author_registration
                    && matches!(
                        record.status,
                        crate::protocol::store_commit::StoreDeviceStatus::Active
                    )
            });
        if !active_author {
            return Err(DbError::Message(
                "materialized commit author is not active at its exact predecessor state".into(),
            ));
        }
        device_state = device_operations
            .apply_to(device_state, &commit.device_state)
            .map_err(|error| DbError::Message(error.to_string()))?;
        for activation in commit.device_registrations() {
            if recovery_author
                .as_ref()
                .is_some_and(|(registration, _)| registration == &activation.registration)
            {
                continue;
            }
            device_state = device_state
                .activate_registration(activation.registration.clone(), None)
                .map_err(|error| DbError::Message(error.to_string()))?;
        }
        let mut owner_recoveries = commit.stream_activations().iter().filter_map(|activation| {
            let crate::protocol::store_commit::StreamActivation::GrantAuthorized {
                author_registration,
                grant_id,
                anchor:
                    anchor @ crate::protocol::store_commit::GrantStreamAnchor::OwnerRecovery { .. },
                ..
            } = activation
            else {
                return None;
            };
            Some((author_registration, grant_id, anchor))
        });
        let owner_recovery = owner_recoveries.next();
        if owner_recoveries.next().is_some() {
            return Err(DbError::Message(
                "materialized commit activates more than one Owner recovery stream".to_string(),
            ));
        }
        let owner_recovery = match owner_recovery {
            Some((registration, grant_id, anchor)) => {
                let registration = load_activated_registration_on(conn, root, registration)?;
                Some((
                    grant_id.clone(),
                    crate::protocol::store_commit::OwnerRecoveryActivationId::derive(
                        root,
                        &registration.author_pubkey,
                        grant_id,
                        anchor,
                    )
                    .map_err(|error| DbError::Message(error.to_string()))?,
                ))
            }
            None => None,
        };
        if let Some((grant_id, activation)) = owner_recovery {
            device_state = device_state
                .activate_owner_recovery(grant_id, activation)
                .map_err(|error| DbError::Message(error.to_string()))?;
        }
        Ok(device_state)
    }

    fn record_materialized_commit_with_device_operations(
        &self,
        verified_commit: &VerifiedStoreBatchCommit,
        device_operations: &VerifiedStoreDeviceOperations,
        stream_activations: &VerifiedStreamActivations,
        retention: &RetainedMergeMaterializationKey,
        activation: &ReclaimCommitActivation,
    ) -> Result<(), DbError> {
        let conn = self.transaction;
        let commit = verified_commit.value();
        let commit_ref = verified_commit.reference();
        let stored_registration: String = conn
            .query_row(
                "SELECT registration_object FROM store_device_registration_activations \
                 WHERE device_id = ?1 AND registration_hash = ?2",
                (
                    commit.author_registration.device_id.to_string(),
                    commit.author_registration.registration_hash.to_string(),
                ),
                |row| row.get(0),
            )
            .map_err(DbError::from)?;
        let stored_registration: StoreDeviceRegistrationRef =
            serde_json::from_str(&stored_registration).map_err(|error| {
                DbError::Message(format!("materialized author registration ref: {error}"))
            })?;
        if stored_registration != commit.author_registration {
            return Err(DbError::Message(
                "materialized commit author registration differs from its activation".to_string(),
            ));
        }
        let root = required_store_root_authority_on(conn)?;
        if root.store_root_hash != commit.store_root_hash {
            return Err(DbError::Message(
                "materialized commit belongs to a different Store root".to_string(),
            ));
        }
        let expected_stream =
            crate::protocol::store_commit::StreamActivation::device_authorized_stream_id(
                root.store_root_hash,
                &commit.author_registration,
                crate::protocol::store_commit::StreamAnchorDomain::StoreAnnouncements,
            );
        if commit_ref.coord.stream_id != expected_stream {
            return Err(DbError::Message(
                "materialization stream differs from its exact author registration".to_string(),
            ));
        }
        let stream_id = commit_ref.coord.stream_id.to_string();
        let sequence = commit_ref.coord.sequence;
        if sequence != commit.seq() {
            return Err(DbError::Message(
                "materialization coordinate differs from its signed commit".to_string(),
            ));
        }
        let predecessor = if commit.seq() == 1 {
            None
        } else if let Some(reference) = crate::database::StoreDatabase::materialized_commit_ref_on(
            conn,
            &stream_id,
            commit.seq() - 1,
        )? {
            Some(reference)
        } else {
            conn.query_row(
                "SELECT commit_ref FROM snapshot_coverage \
                 WHERE device_id = ?1 AND seq = ?2",
                (
                    &stream_id,
                    Database::sequence_to_sqlite(&stream_id, commit.seq() - 1)?,
                ),
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(DbError::from)?
            .map(|reference| {
                serde_json::from_str(&reference).map_err(|error| {
                    DbError::Message(format!("snapshot coverage exact commit ref: {error}"))
                })
            })
            .transpose()?
        };
        if predecessor.as_ref() != commit.order.predecessor() {
            return Err(DbError::Message(format!(
                "Store commit {}/{} names predecessor {:?}, durable predecessor is {:?}",
                stream_id,
                commit.seq(),
                commit.order.predecessor(),
                predecessor
            )));
        }
        let device_state =
            self.derive_materialized_store_device_state(&root, commit, device_operations)?;
        self.record_activated_store_ack(commit, commit_ref)?;
        self.record_activated_circle_acks(commit, commit_ref)?;
        let seq = Database::sequence_to_sqlite(&stream_id, commit.seq())?;
        let commit_ref_json = serde_json::to_string(commit_ref).map_err(|error| {
            DbError::Message(format!("serialize exact Store commit ref: {error}"))
        })?;
        if retention.commit_ref != commit_ref_json {
            return Err(DbError::Message(
                "retained input names another exact commit".to_string(),
            ));
        }
        let retained_commit_ref = retention.commit_ref.as_str();
        let retained_input_hash = retention.input_hash.to_string();
        conn.execute(
            "INSERT INTO store_device_state_snapshots (commit_ref, state) VALUES (?1, ?2)",
            rusqlite::params![
                &commit_ref_json,
                serde_json::to_string(&device_state).map_err(|error| {
                    DbError::Message(format!(
                        "serialize materialized Store device state: {error}"
                    ))
                })?,
            ],
        )
        .map_err(DbError::from)?;
        conn.execute(
            "INSERT INTO materialized_commits
             (device_id, seq, commit_ref, retained_commit_ref, retained_input_hash)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![
                &stream_id,
                seq,
                &commit_ref_json,
                retained_commit_ref,
                retained_input_hash
            ],
        )
        .map_err(DbError::from)?;
        if stream_activations.as_slice() != commit.stream_activations() {
            return Err(DbError::Message(
                "verified stream activations differ from the materialized Store commit".to_string(),
            ));
        }
        if stream_activations.activating_commit() != commit_ref {
            return Err(DbError::Message(
                "verified stream activation commit differs from the materialized Store commit"
                    .to_string(),
            ));
        }
        StoreDatabase::record_verified_stream_activations_on(
            conn,
            stream_activations,
            &commit_ref_json,
        )?;
        apply_store_device_exclusion_freezes_on(conn, &root, &device_state, device_operations)?;
        self.record_store_reclaim_activation(commit, commit_ref, activation)
    }

    fn record_activated_store_ack(
        &self,
        commit: &StoreBatchCommit,
        commit_ref: &StoreBatchCommitRef,
    ) -> Result<(), DbError> {
        let Some(reference) = commit.acknowledgement() else {
            return Ok(());
        };
        if reference.registration != commit.author_registration {
            return Err(DbError::Message(
                "activated Store acknowledgement names another registration".to_string(),
            ));
        }
        let conn = self.transaction;
        let device_id = reference.registration.device_id.to_string();
        let current = conn
            .query_row(
                "SELECT ack_ref FROM activated_store_acks WHERE device_id = ?1",
                [&device_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(DbError::from)?
            .map(|raw| {
                serde_json::from_str::<StoreAckRef>(&raw).map_err(|error| {
                    DbError::Message(format!("activated Store acknowledgement ref: {error}"))
                })
            })
            .transpose()?;
        if current.as_ref().is_some_and(|current| {
            current.registration != reference.registration || current.sequence >= reference.sequence
        }) {
            return Err(DbError::Message(
                "Store acknowledgement activation does not advance the exact registration stream"
                    .to_string(),
            ));
        }
        conn.execute(
            "INSERT INTO activated_store_acks (device_id, ack_ref, activating_commit) \
             VALUES (?1, ?2, ?3) \
             ON CONFLICT(device_id) DO UPDATE SET \
               ack_ref = excluded.ack_ref, activating_commit = excluded.activating_commit",
            rusqlite::params![
                device_id,
                serde_json::to_string(reference).map_err(|error| DbError::Message(format!(
                    "serialize activated Store acknowledgement ref: {error}"
                )))?,
                serde_json::to_string(commit_ref).map_err(|error| DbError::Message(format!(
                    "serialize acknowledgement activating commit ref: {error}"
                )))?,
            ],
        )
        .map(|_| ())
        .map_err(DbError::from)
    }

    fn record_activated_circle_acks(
        &self,
        commit: &StoreBatchCommit,
        commit_ref: &StoreBatchCommitRef,
    ) -> Result<(), DbError> {
        let conn = self.transaction;
        for reference in commit.circle_acknowledgements() {
            if reference.registration != commit.author_registration {
                return Err(DbError::Message(
                    "activated Circle acknowledgement names another registration".to_string(),
                ));
            }
            let circle_id = reference.circle_id.to_string();
            let device_id = reference.registration.device_id.to_string();
            let current: Option<CircleAckRef> = conn
                .query_row(
                    "SELECT ack_ref FROM activated_circle_acks
                     WHERE circle_id = ?1 AND device_id = ?2",
                    rusqlite::params![circle_id, device_id],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .map_err(DbError::from)?
                .map(|raw| {
                    serde_json::from_str::<CircleAckRef>(&raw).map_err(|error| {
                        DbError::Message(format!("activated Circle acknowledgement ref: {error}"))
                    })
                })
                .transpose()?;
            if current.as_ref().is_some_and(|current| {
                current.registration != reference.registration
                    || current.circle_id != reference.circle_id
                    || current.sequence >= reference.sequence
            }) {
                return Err(DbError::Message(
                    "Circle acknowledgement activation does not advance the exact stream"
                        .to_string(),
                ));
            }
            conn.execute(
                "INSERT INTO activated_circle_acks
                   (circle_id, device_id, ack_ref, activating_commit)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(circle_id, device_id) DO UPDATE SET
                   ack_ref = excluded.ack_ref, activating_commit = excluded.activating_commit",
                rusqlite::params![
                    circle_id,
                    device_id,
                    serde_json::to_string(reference).map_err(|error| DbError::Message(format!(
                        "serialize activated Circle acknowledgement ref: {error}"
                    )))?,
                    serde_json::to_string(commit_ref).map_err(|error| DbError::Message(
                        format!("serialize Circle acknowledgement activating commit: {error}")
                    ))?,
                ],
            )
            .map(|_| ())
            .map_err(DbError::from)?;
        }
        Ok(())
    }

    fn record_author_exclusion_activations(
        &self,
        verified_commit: &VerifiedStoreBatchCommit,
        device_operations: &VerifiedStoreDeviceOperations,
        activation_head: &StoreDeviceHead,
        activation_head_object: &ExactObjectRef,
    ) -> Result<(), DbError> {
        let conn = self.transaction;
        let root = required_store_root_authority_on(conn)?;
        let commit = verified_commit.value();
        let commit_ref = verified_commit.reference();
        if commit.store_root_hash != root.store_root_hash
            || activation_head.author_registration != commit.author_registration
            || activation_head.commit != *commit_ref
        {
            return Err(DbError::Message(
                "author exclusion activation head differs from its exact commit authority"
                    .to_string(),
            ));
        }
        let author =
            load_activated_registration_on(conn, &root, &activation_head.author_registration)?;
        StoreDeviceHead::parse_at(
            &activation_head.to_bytes(),
            root.store_root_hash,
            &author,
            commit_ref,
        )
        .map_err(|error| {
            DbError::Message(format!("verify author exclusion activation head: {error}"))
        })?;
        activation_head_object
            .verify(&activation_head.to_bytes())
            .map_err(|error| {
                DbError::Message(format!(
                    "verify author exclusion activation head object: {error}"
                ))
            })?;
        let expected_head_key = format!(
            "{}.json",
            crate::protocol::store_commit::head_slot_prefix(
                &activation_head.author_registration.device_id.to_string(),
                commit_ref.coord.sequence(),
            )
        );
        if activation_head_object.slot().logical_key() != expected_head_key {
            return Err(DbError::Message(
                "author exclusion activation head object occupies another protocol slot"
                    .to_string(),
            ));
        }
        let activation_head = crate::protocol::store_commit::StoreDeviceHeadRef {
            head_hash: activation_head.head_hash(),
            object: activation_head_object.clone(),
        };
        let activation_commit = serde_json::to_string(commit_ref).map_err(|error| {
            DbError::Message(format!(
                "serialize author exclusion activation commit: {error}"
            ))
        })?;
        for (exclusion, accepted_cut) in device_operations.exclusions() {
            let StoreHistoryCut(accepted_cut) = accepted_cut;
            let exclusion_json = serde_json::to_string(exclusion).map_err(|error| {
                DbError::Message(format!("serialize author exclusion reference: {error}"))
            })?;
            let accepted_cut_json = serde_json::to_string(accepted_cut).map_err(|error| {
                DbError::Message(format!("serialize author exclusion accepted cut: {error}"))
            })?;
            let activation_head_json =
                serde_json::to_string(&activation_head).map_err(|error| {
                    DbError::Message(format!(
                        "serialize author exclusion activation head: {error}"
                    ))
                })?;
            let inserted = conn
                .execute(
                    "INSERT INTO store_author_exclusion_activations (
                         exclusion_ref, accepted_cut, activation_commit, activation_head
                     ) VALUES (?1, ?2, ?3, ?4)
                     ON CONFLICT(exclusion_ref) DO NOTHING",
                    (
                        &exclusion_json,
                        &accepted_cut_json,
                        &activation_commit,
                        &activation_head_json,
                    ),
                )
                .map_err(DbError::from)?;
            if inserted == 0 {
                let stored: (String, String, String) = conn
                    .query_row(
                        "SELECT accepted_cut, activation_commit, activation_head
                         FROM store_author_exclusion_activations
                         WHERE exclusion_ref = ?1",
                        [&exclusion_json],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                    )
                    .map_err(DbError::from)?;
                if stored
                    != (
                        accepted_cut_json,
                        activation_commit.clone(),
                        activation_head_json,
                    )
                {
                    return Err(DbError::Message(
                        "author exclusion already names different activation evidence".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn record_verified_circle_activations(
        &self,
        verified_commit: &VerifiedStoreBatchCommit,
        activations: &[crate::protocol::circle_activation::VerifiedCircleReference],
    ) -> Result<(), DbError> {
        let conn = self.transaction;
        let commit = verified_commit.value();
        let commit_ref = verified_commit.reference();
        if activations.len() != commit.circle_controls().len() {
            return Err(DbError::Message(
                "verified circle activations do not cover every control reference".to_string(),
            ));
        }
        let stream_id = commit_ref.coord.stream_id.to_string();
        let seq = Database::sequence_to_sqlite(&stream_id, commit_ref.coord.sequence())?;
        for activation in activations {
            if !commit.circle_controls().contains(&activation.reference)
                || activation.reference.circle_id() != activation.circle_id
                || activation.reference.control() != &activation.control.coord
                || !activation.control.verify()
            {
                return Err(DbError::Message(
                    "verified circle activation differs from Store control reference".to_string(),
                ));
            }
            let circle_id = activation.circle_id.to_string();
            if let Some(access) = &activation.local_access {
                let leaf = &access.leaf.value;
                if activation.control.value.author_pubkey != leaf.owner_pubkey {
                    return Err(DbError::Message(format!(
                        "circle {circle_id} local access signer differs from its control author"
                    )));
                }
                match (&leaf.disposition, &access.active) {
                    (crate::protocol::circle::CircleAccessDisposition::Active { .. }, Some(_))
                    | (crate::protocol::circle::CircleAccessDisposition::Inactive, None) => {}
                    _ => {
                        return Err(DbError::Message(format!(
                            "circle {circle_id} access state differs from its disposition"
                        )));
                    }
                }
            }
            let mut statement = conn
                .prepare(
                    "SELECT control_bytes FROM circle_control_activations
                     WHERE circle_id = ?1",
                )
                .map_err(DbError::from)?;
            let rows = statement
                .query_map([&circle_id], |row| row.get::<_, Vec<u8>>(0))
                .map_err(DbError::from)?;
            let mut existing_controls = Vec::new();
            for bytes in rows {
                let bytes = bytes.map_err(DbError::from)?;
                let control: crate::protocol::circle::CircleControl =
                    serde_json::from_slice(&bytes).map_err(|error| {
                        DbError::Message(format!("parse activated circle control: {error}"))
                    })?;
                existing_controls.push(control);
            }
            drop(statement);
            if activation.control.value.is_founder() {
                if !existing_controls.is_empty() {
                    return Err(DbError::Message(format!(
                        "circle {circle_id} already has a founder control"
                    )));
                }
            } else {
                let covered = existing_controls
                    .iter()
                    .filter(|control| activation.control.value.causally_covers(control))
                    .collect::<Vec<_>>();
                let order = &activation.control.value.value.order;
                let expected_covered =
                    order.dependencies.len() + usize::from(order.previous_control_hash.is_some());
                if covered.len() != expected_covered
                    || covered.iter().any(|control| {
                        control
                            .owners()
                            .binary_search(&activation.control.value.author_pubkey)
                            .is_err()
                    })
                {
                    return Err(DbError::Message(format!(
                        "circle {circle_id} control does not cover every authorized predecessor"
                    )));
                }
            }
            let next_state = crate::protocol::circle_activation::CircleCurrentState::from_verified(
                commit.candidate_family(),
                activation,
            )
            .map_err(DbError::Message)?;
            let current_state = StoreDatabase::circle_current_state_on(conn, activation.circle_id)?;
            let current_state = match current_state {
                Some(current) => current.advance(next_state).map_err(DbError::Message)?,
                None if activation.control.value.is_founder() => next_state,
                None => {
                    return Err(DbError::Message(format!(
                        "circle {} current state is absent for a successor control",
                        activation.circle_id
                    )));
                }
            };
            let current_state_payload = serde_json::to_vec(&current_state).map_err(|error| {
                DbError::Message(format!("serialize Circle current state: {error}"))
            })?;
            let control_coord =
                serde_json::to_string(&activation.control.coord).map_err(|error| {
                    DbError::Message(format!("serialize circle control coordinate: {error}"))
                })?;
            conn.execute(
                "INSERT INTO circle_control_activations
                 (circle_id, control_coord, stream_id, seq, commit_hash, control_bytes)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                rusqlite::params![
                    &circle_id,
                    &control_coord,
                    stream_id,
                    seq,
                    commit.commit_hash().to_string(),
                    &activation.control.bytes,
                ],
            )
            .map_err(DbError::from)?;
            if let Some(access) = &activation.local_access {
                let disposition = match access.leaf.value.disposition {
                    crate::protocol::circle::CircleAccessDisposition::Active { .. } => "active",
                    crate::protocol::circle::CircleAccessDisposition::Inactive => "inactive",
                };
                conn.execute(
                    "INSERT INTO circle_access_cache
                     (circle_id, control_coord, owner_pubkey, disposition)
                     VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![
                        &circle_id,
                        &control_coord,
                        &access.leaf.value.owner_pubkey,
                        disposition,
                    ],
                )
                .map_err(DbError::from)?;
            }
            conn.execute(
                "INSERT INTO circle_current_state (circle_id, state) VALUES (?1, ?2)
                 ON CONFLICT(circle_id) DO UPDATE SET state = excluded.state",
                rusqlite::params![&circle_id, current_state_payload],
            )
            .map_err(DbError::from)?;
            if self.circle_current_state_is_deleted(activation.circle_id)? {
                conn.execute(
                    "DELETE FROM circle_access_cache WHERE circle_id = ?1",
                    [&circle_id],
                )
                .map_err(DbError::from)?;
            }
        }
        Ok(())
    }

    pub(crate) fn activate_store_operation_remote_objects(
        &self,
        commit_ref: &StoreBatchCommitRef,
        object_ids: &[ObjectHash],
    ) -> Result<(), DbError> {
        let conn = self.transaction;
        let mut unique = std::collections::BTreeSet::new();
        for object_id in object_ids {
            if !unique.insert(*object_id) {
                return Err(DbError::Message(
                    "Store operation names a duplicate remote object".to_string(),
                ));
            }
            let remote = load_remote_object_on(conn, *object_id).map_err(|error| {
                DbError::Message(format!(
                    "load Store operation remote object {object_id} for activation: {error}"
                ))
            })?;
            let kind = match &remote {
                RemoteObjectRecord::CandidateCommit(_) => "candidate commit",
                RemoteObjectRecord::CandidateExclusive(_) => "candidate-exclusive object",
                RemoteObjectRecord::RetainedAuthority(_) => "retained authority",
                RemoteObjectRecord::SharedLiveSet(_) => "shared live-set object",
            };
            let remote = remote.into_activated(commit_ref).map_err(|error| {
                DbError::Message(format!(
                    "activate Store operation {kind} {object_id}: {error}"
                ))
            })?;
            update_remote_object_on(conn, *object_id, &remote)?;
        }
        Ok(())
    }
    #[allow(clippy::too_many_arguments)]
    fn apply_merge_subset(
        &self,
        blob_decls: &BlobDecls,
        gates: &crate::database::Gates,
        routing_key: Option<&crate::protocol::circle::RowRoutingKey>,
        source: &ValidatedChangeset<Vec<u8>>,
        bytes: Vec<u8>,
        package_audience: Option<&crate::protocol::circle::Audience>,
        timestamp_policy: IncomingTimestampPolicy,
        changeset_max: &mut Option<crate::protocol::hlc::Timestamp>,
        returned_changes: &mut Vec<RowChange>,
        package_reported_fk_violation: &mut bool,
    ) -> Result<MergeSubsetOutcome, DbError> {
        let applied_changeset = source
            .validate_subset(bytes.clone())
            .map_err(|error| DbError::Message(error.to_string()))?;
        let actual_changes = crate::database::walk_changeset(&bytes).map_err(DbError::Message)?;
        if let Some(receiver_wall_ms) = timestamp_policy.received_wall_ms() {
            advance_max_updated_at(
                changeset_max,
                &actual_changes,
                source.schema(),
                receiver_wall_ms,
            );
        }
        returned_changes.extend(
            actual_changes
                .iter()
                .filter(|change| !crate::database::is_routing_table(&change.table))
                .cloned(),
        );
        let apply = self.apply_changeset(applied_changeset, timestamp_policy)?;
        if !apply.constraint_conflict_tables.is_empty() {
            return Ok(MergeSubsetOutcome::ConstraintConflict(
                apply.constraint_conflict_tables,
            ));
        }
        *package_reported_fk_violation |= apply.had_fk_violations;
        if let Some(package_audience) = package_audience {
            crate::database::align_inbound_scoped_root_audiences(
                self.transaction,
                &bytes,
                package_audience,
                gates,
                routing_key.ok_or_else(|| {
                    DbError::Message(
                        "scoped audience application requires a row-routing key".to_string(),
                    )
                })?,
            )
            .map_err(|error| DbError::Message(error.to_string()))?;
        }
        let winning_rows = self.current_winning_rows(source.schema(), &bytes)?;
        let old_changes = crate::database::walk_old_changeset(&bytes).map_err(DbError::Message)?;
        let cleanup = local_blob_cleanup_intents(blob_decls, &old_changes, &actual_changes)
            .map_err(|error| DbError::Message(error.to_string()))?;
        for intent in cleanup {
            self.record_obsolete_blob_cleanup_intent(blob_decls, &intent)?;
        }
        Ok(MergeSubsetOutcome::Applied(winning_rows))
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_merge_package(
        &self,
        blob_decls: &BlobDecls,
        gates: &crate::database::Gates,
        routing_key: Option<&crate::protocol::circle::RowRoutingKey>,
        package: &AudiencePackage,
        changeset: &ValidatedChangeset<Vec<u8>>,
        store_audience_transitions: &crate::database::StoreAudienceTransitions,
        timestamp_policy: IncomingTimestampPolicy,
        changeset_max: &mut Option<crate::protocol::hlc::Timestamp>,
        returned_changes: &mut Vec<RowChange>,
        package_reported_fk_violation: &mut bool,
    ) -> Result<MergeSubsetOutcome, DbError> {
        let conn = self.transaction;
        let mut winning_rows = Vec::new();
        match package.audience() {
            PackageAudience::Store if gates.has_scoped_graph() => {
                let routing_key = routing_key.ok_or_else(|| {
                    DbError::Message(
                        "scoped Store package application requires a row-routing key".to_string(),
                    )
                })?;
                let inbound = crate::database::normalize_inbound_store_changeset(
                    conn,
                    package.changeset(),
                    gates,
                    routing_key,
                )
                .map_err(|error| DbError::Message(error.to_string()))?;
                if let Err(tables) = self
                    .apply_merge_subset(
                        blob_decls,
                        gates,
                        Some(routing_key),
                        changeset,
                        inbound.mirror,
                        None,
                        timestamp_policy,
                        changeset_max,
                        returned_changes,
                        package_reported_fk_violation,
                    )?
                    .extend_winning_rows(&mut winning_rows)
                {
                    return Ok(MergeSubsetOutcome::ConstraintConflict(tables));
                }
                let rows = crate::database::filter_inbound_store_rows(
                    conn,
                    &inbound.rows,
                    gates,
                    routing_key,
                )
                .map_err(|error| DbError::Message(error.to_string()))?;
                if let Err(tables) = self
                    .apply_merge_subset(
                        blob_decls,
                        gates,
                        Some(routing_key),
                        changeset,
                        rows,
                        Some(&crate::protocol::circle::Audience::Store),
                        timestamp_policy,
                        changeset_max,
                        returned_changes,
                        package_reported_fk_violation,
                    )?
                    .extend_winning_rows(&mut winning_rows)
                {
                    return Ok(MergeSubsetOutcome::ConstraintConflict(tables));
                }
            }
            PackageAudience::Store => {
                return self.apply_merge_subset(
                    blob_decls,
                    gates,
                    None,
                    changeset,
                    package.changeset().to_vec(),
                    None,
                    timestamp_policy,
                    changeset_max,
                    returned_changes,
                    package_reported_fk_violation,
                );
            }
            PackageAudience::Circle { circle_id, .. } => {
                let routing_key = routing_key.ok_or_else(|| {
                    DbError::Message(
                        "Circle package application requires a row-routing key".to_string(),
                    )
                })?;
                let rows = crate::database::filter_inbound_circle_changeset(
                    conn,
                    package.changeset(),
                    *circle_id,
                    store_audience_transitions,
                    gates,
                    routing_key,
                )
                .map_err(|error| DbError::Message(error.to_string()))?;
                return self.apply_merge_subset(
                    blob_decls,
                    gates,
                    Some(routing_key),
                    changeset,
                    rows,
                    Some(&crate::protocol::circle::Audience::Circle(*circle_id)),
                    timestamp_policy,
                    changeset_max,
                    returned_changes,
                    package_reported_fk_violation,
                );
            }
        }
        Ok(MergeSubsetOutcome::Applied(winning_rows))
    }

    pub(crate) fn apply_prepared_merge_materialization(
        &self,
        blob_decls: &BlobDecls,
        gates: &crate::database::Gates,
        synced_tables: &[SyncedTable],
        routing_key: Option<&crate::protocol::circle::RowRoutingKey>,
        local_store_membership: LocalStoreMembership,
        timestamp_policy: IncomingTimestampPolicy,
        baseline_circle_cuts: Option<
            &BTreeMap<
                crate::protocol::circle::CircleId,
                crate::protocol::store_commit::CommitFrontier,
            >,
        >,
        materialization: PreparedMergeMaterialization,
    ) -> Result<AppliedMergeMaterialization, DbError> {
        let conn = self.transaction;
        let PreparedMergeMaterialization {
            root,
            verified_commit,
            activation_head,
            activation_head_object,
            history_summary,
            membership_objects,
            membership_remote_objects,
            registrations,
            packages,
            device_operations,
            circle_activations,
            package_application,
        } = materialization;
        let commit = verified_commit.value();
        let commit_ref = verified_commit.reference();
        let mut inactive_circles = circle_activations
            .circles()
            .iter()
            .filter_map(|activation| {
                activation
                    .local_access
                    .as_ref()
                    .filter(|access| access.active.is_none())
                    .filter(|_| {
                        baseline_circle_cuts
                            .and_then(|cuts| cuts.get(&activation.circle_id))
                            .is_none_or(|cut| !cut.covers_commit(commit_ref))
                    })
                    .map(|_| activation.circle_id)
            })
            .collect::<BTreeSet<_>>();
        let mut changeset_max = None;
        let mut returned_changes = Vec::new();
        let mut package_reported_fk_violation = false;
        super::record_activated_store_device_registrations_on(conn, commit, &registrations)?;
        for bootstrap in circle_activations.bootstraps() {
            crate::database::install_circle_bootstrap_remote_objects_on(
                conn, commit_ref, bootstrap,
            )?;
        }
        self.record_verified_circle_activations(&verified_commit, circle_activations.circles())?;
        // A Circle whose winning control chain is now Deleted prunes its rows,
        // routes, and blob bindings like an inactive recipient. Recording the
        // verified activation above already removed its live access cache while
        // retaining the authority spine.
        for activation in circle_activations.circles() {
            if self.circle_current_state_is_deleted(activation.circle_id)? {
                inactive_circles.insert(activation.circle_id);
            }
        }
        let retained_packages = packages
            .iter()
            .map(|prepared| prepared.package.clone())
            .collect::<Vec<_>>();
        let store_audience_transitions = packages
            .iter()
            .find(|prepared| matches!(prepared.package.audience(), PackageAudience::Store))
            .map(|prepared| {
                crate::database::store_audience_transitions(prepared.package.changeset())
            })
            .transpose()
            .map_err(|error| DbError::Message(error.to_string()))?
            .unwrap_or_default();
        for prepared in packages {
            let PreparedMergeMaterializationPackage { package, changeset } = prepared;
            let winning_rows = match self.apply_merge_package(
                blob_decls,
                gates,
                routing_key,
                &package,
                &changeset,
                &store_audience_transitions,
                timestamp_policy,
                &mut changeset_max,
                &mut returned_changes,
                &mut package_reported_fk_violation,
            )? {
                MergeSubsetOutcome::Applied(rows) => rows,
                MergeSubsetOutcome::ConstraintConflict(tables) => {
                    return Ok(AppliedMergeMaterialization {
                        outcome: ApplyOutcome::Held(HeldStorePositionReason::ConstraintConflict(
                            tables,
                        )),
                        max_updated_at: None,
                        write_status_notifications: Vec::new(),
                        retained: None,
                    });
                }
            };
            let retained = crate::database::RetainedAudiencePackage::verify(
                commit,
                commit_ref,
                package.clone(),
            )?;
            Database::install_pulled_package_activation_on(
                conn,
                commit_ref,
                retained.domain(),
                retained.object(),
                retained.package(),
            )?;
            Database::install_pulled_blob_activations_on(conn, &package, commit_ref)?;
            self.install_winning_blob_bindings(
                gates,
                synced_tables,
                &package,
                &BlobActivation {
                    coord: commit_ref.coord.clone(),
                },
                &winning_rows,
            )?;
        }
        if gates.has_scoped_graph() && !local_store_membership.retains_circle_rows() {
            let mut statement = conn
                .prepare(
                    "SELECT DISTINCT circle_id
                     FROM _coven_audience
                     WHERE circle_id IS NOT NULL
                     ORDER BY circle_id",
                )
                .map_err(DbError::from)?;
            let circles = statement
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(DbError::from)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(DbError::from)?;
            drop(statement);
            for encoded in circles {
                inactive_circles.insert(encoded.parse().map_err(|error| {
                    DbError::Message(format!(
                        "parse materialized Circle audience {encoded}: {error}"
                    ))
                })?);
            }
            crate::database::StoreDatabase::remove_local_circle_access_on(conn)?;
        }
        let mut removal_session = rusqlite::session::Session::new(conn).map_err(DbError::from)?;
        for table in synced_tables {
            removal_session
                .attach(Some(table.name()))
                .map_err(DbError::from)?;
        }
        crate::database::prune_ineligible_scoped_rows(conn, gates, &inactive_circles)
            .map_err(|error| DbError::Message(error.to_string()))?;
        crate::database::validate_scoped_foreign_key_audiences(conn, gates)
            .map_err(|error| DbError::Message(error.to_string()))?;
        let mut removal_changeset = Vec::new();
        removal_session
            .changeset_strm(&mut removal_changeset)
            .map_err(DbError::from)?;
        drop(removal_session);
        let removed =
            crate::database::walk_old_changeset(&removal_changeset).map_err(DbError::Message)?;
        let removal_changes =
            crate::database::walk_changeset(&removal_changeset).map_err(DbError::Message)?;
        let removal_cleanup = local_blob_cleanup_intents(blob_decls, &removed, &removal_changes)
            .map_err(|error| DbError::Message(error.to_string()))?;
        returned_changes.extend(removal_changes);
        for intent in removal_cleanup {
            self.record_obsolete_blob_cleanup_intent(blob_decls, &intent)?;
        }
        if package_reported_fk_violation {
            let violations: bool = conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM pragma_foreign_key_check)",
                    [],
                    |row| row.get(0),
                )
                .map_err(DbError::from)?;
            if violations {
                return Ok(AppliedMergeMaterialization {
                    outcome: ApplyOutcome::Held(HeldStorePositionReason::ForeignKeyDependency),
                    max_updated_at: None,
                    write_status_notifications: Vec::new(),
                    retained: None,
                });
            }
        }
        let verified = VerifiedMergeMaterialization::verify(
            &root,
            &verified_commit,
            &registrations,
            &device_operations,
            &circle_activations,
            &activation_head,
            &activation_head_object,
            &history_summary,
            membership_objects.as_ref(),
            &retained_packages,
            package_application,
        )?;
        Database::install_pulled_merge_membership_activations_on(
            conn,
            commit_ref,
            &membership_remote_objects,
        )?;
        let retained = self.record_verified_merge_materialization(verified)?;
        Ok(AppliedMergeMaterialization {
            outcome: ApplyOutcome::Applied(returned_changes),
            max_updated_at: changeset_max,
            write_status_notifications: Vec::new(),
            retained: Some(retained),
        })
    }

    fn insert_merge_retraction_cleanup(
        &self,
        retained: &OwnedVerifiedMergeMaterialization,
    ) -> Result<(), DbError> {
        let StoreCommitCoord {
            stream_id,
            sequence,
        } = &retained.commit_ref().coord;
        let input = super::materialization_models::MergeRetractionCleanupInput {
            commit: crate::protocol::objects::PreparedExactObject::new(
                retained.commit_ref().object.clone(),
                retained.commit().to_bytes(),
            )
            .map_err(|error| DbError::Message(error.to_string()))?,
            activation_head: crate::protocol::objects::PreparedExactObject::new(
                retained.activation_head_object().clone(),
                retained.activation_head().to_bytes(),
            )
            .map_err(|error| DbError::Message(error.to_string()))?,
        };
        let canonical_cleanup = serde_json::to_vec(&input).map_err(|error| {
            DbError::Message(format!("serialize Merge retraction cleanup: {error}"))
        })?;
        let cleanup_hash = ObjectHash::digest(&canonical_cleanup);
        let stream_id = stream_id.to_string();
        let sequence_sql = Database::sequence_to_sqlite(&stream_id, *sequence)?;
        let encoded_ref = serde_json::to_string(&retained.commit_ref()).map_err(|error| {
            DbError::Message(format!("serialize Merge retraction cleanup ref: {error}"))
        })?;
        self.transaction
            .execute(
                "INSERT INTO merge_retraction_cleanups
                 (device_id, seq, commit_ref, cleanup_hash, canonical_cleanup)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![
                    &stream_id,
                    sequence_sql,
                    &encoded_ref,
                    cleanup_hash.to_string(),
                    &canonical_cleanup,
                ],
            )
            .map_err(DbError::from)?;
        StoreDatabase::load_merge_retraction_cleanup_on(self.transaction, retained.commit_ref())?;
        Ok(())
    }

    fn retire_circle_bootstrap_coverage(
        &self,
        activation_commit: &StoreBatchCommitRef,
    ) -> Result<usize, DbError> {
        let encoded = serde_json::to_string(activation_commit).map_err(|error| {
            DbError::Message(format!(
                "serialize retracted Circle bootstrap activation: {error}"
            ))
        })?;
        self.transaction
            .execute(
                "DELETE FROM circle_bootstrap_coverage WHERE activation_commit = ?1",
                [encoded],
            )
            .map_err(DbError::from)
    }

    pub(crate) fn retract_verified_merge_materializations(
        &self,
        root: &crate::protocol::store_commit::StoreRootRef,
        retained_merge_materializations: &mut RetainedMergeMaterializationCache,
        retractions: Vec<crate::protocol::remote_object::VerifiedCandidateNonactivation>,
    ) -> Result<Vec<(WriteId, WriteStatus)>, DbError> {
        let conn = self.transaction;
        let provided = retractions
            .iter()
            .map(|retraction| {
                retraction
                    .candidate_reference()
                    .map_err(|error| DbError::Message(error.to_string()))
            })
            .collect::<Result<BTreeSet<_>, _>>()?;
        let retained = retained_merge_materializations.replay_inputs_on(conn, root)?;
        let mut required = BTreeSet::new();
        for retained in &retained {
            if author_exclusion_activation_for_candidate_on(
                conn,
                root,
                retained.commit_ref(),
                &retained.commit().author_registration,
            )?
            .is_some()
            {
                required.insert(retained.commit_ref().clone());
            }
        }
        for retraction in &retractions {
            if matches!(
                retraction.proof(),
                crate::protocol::remote_object::CandidateNonactivationProof::MergeMembershipGrantRevocation { .. }
            ) {
                required.insert(
                    retraction
                        .candidate_reference()
                        .map_err(|error| DbError::Message(error.to_string()))?,
                );
            }
        }
        let direct_dependencies = retained
            .iter()
            .map(|retained| {
                let mut direct = retained
                    .commit()
                    .order
                    .dependencies()
                    .values()
                    .cloned()
                    .collect::<BTreeSet<_>>();
                if let Some(predecessor) = retained.commit().order.predecessor() {
                    direct.insert(predecessor.clone());
                }
                (retained.commit_ref().clone(), direct)
            })
            .collect::<BTreeMap<_, _>>();
        require_exact_merge_retraction_closure(&direct_dependencies, required, &provided)?;
        let mut notifications = Vec::new();
        for verified in retractions {
            let (nonactivation, head_nonactivation) =
                verified
                    .into_terminal_head_nonactivation()
                    .map_err(|error| DbError::Message(error.to_string()))?;
            let candidate = nonactivation
                .reference()
                .map_err(|error| DbError::Message(error.to_string()))?;
            validate_terminal_nonactivation_authority_on(conn, root, &nonactivation)?;
            match nonactivation.proof() {
                crate::protocol::remote_object::CandidateNonactivationProof::AuthorExclusion {
                    exclusion,
                    accepted_cut,
                    activation_head,
                } => {
                    let locator =
                        load_author_exclusion_activation_locator_on(conn, root, exclusion)?;
                    if locator.accepted_cut() != accepted_cut
                        || locator.activation_head() != activation_head
                    {
                        return Err(DbError::Message(
                            "terminal Merge retraction differs from its activated exclusion"
                                .to_string(),
                        ));
                    }
                }
                crate::protocol::remote_object::CandidateNonactivationProof::MergeMembershipGrantRevocation { .. } => {}
                crate::protocol::remote_object::CandidateNonactivationProof::MergeDependencyRetraction { .. } => {}
                crate::protocol::remote_object::CandidateNonactivationProof::MergeWinner { .. } => {
                    return Err(DbError::Message(
                        "terminal Merge retraction carries nonterminal evidence".to_string(),
                    ));
                }
            }
            let StoreCommitCoord {
                stream_id,
                sequence,
            } = &candidate.coord;
            let stream_id = stream_id.to_string();
            let sequence_sql = Database::sequence_to_sqlite(&stream_id, *sequence)?;
            let encoded_ref = serde_json::to_string(&candidate).map_err(|error| {
                DbError::Message(format!("serialize retracted Merge commit: {error}"))
            })?;
            let input_hash: String = conn
                .query_row(
                    "SELECT retained_input_hash FROM materialized_commits
                     WHERE device_id = ?1 AND seq = ?2 AND commit_ref = ?3",
                    rusqlite::params![&stream_id, sequence_sql, &encoded_ref],
                    |row| row.get(0),
                )
                .map_err(DbError::from)?;
            let retained = StoreDatabase::load_retained_merge_materialization_on(
                conn,
                root,
                &stream_id,
                *sequence,
                &candidate,
                &input_hash,
            )?;
            if retained.commit().to_bytes() != nonactivation.candidate().canonical_signed_bytes
                || retained.activation_head_object() != head_nonactivation.head().object()
            {
                return Err(DbError::Message(
                    "terminal retraction differs from its retained materialization".to_string(),
                ));
            }
            self.insert_merge_retraction_cleanup(&retained)?;
            let replay_owner = RetainedReplayOwner::Commit {
                commit: candidate.clone(),
                input_hash: retained.input_hash(),
            };
            let mut replay_statement = conn
                .prepare(
                    "SELECT object_id FROM retained_replay_objects
                     WHERE device_id = ?1 AND seq = ?2
                     ORDER BY object_id",
                )
                .map_err(DbError::from)?;
            let replay_object_ids = replay_statement
                .query_map(rusqlite::params![&stream_id, sequence_sql], |row| {
                    row.get::<_, String>(0)
                })
                .map_err(DbError::from)?
                .map(|row| {
                    let encoded = row.map_err(DbError::from)?;
                    encoded.parse().map_err(|error| {
                        DbError::Message(format!(
                            "retracted Merge replay object id {encoded}: {error}"
                        ))
                    })
                })
                .collect::<Result<BTreeSet<ObjectHash>, DbError>>()?;
            drop(replay_statement);
            let head_object_id = remote_object_id(retained.activation_head_object());
            let mut activated_object_ids = candidate_graph_exact_objects(retained.commit())?
                .iter()
                .map(remote_object_id)
                .collect::<BTreeSet<_>>();
            activated_object_ids.extend(replay_object_ids.iter().copied());
            if let Some(membership_objects) = retained.membership_objects() {
                activated_object_ids.extend(membership_objects.object_ids());
            }
            activated_object_ids.insert(remote_object_id(&candidate.object));
            activated_object_ids.insert(head_object_id);
            for object_id in &replay_object_ids {
                let mut remote = load_remote_object_on(conn, *object_id)?;
                remote
                    .remove_retained_replay_owner(&replay_owner)
                    .map_err(|error| {
                        DbError::Message(format!(
                            "remove retracted replay owner from {object_id}: {error}"
                        ))
                    })?;
                update_remote_object_on(conn, *object_id, &remote)?;
            }
            conn.execute(
                "DELETE FROM retained_replay_objects WHERE device_id = ?1 AND seq = ?2",
                rusqlite::params![&stream_id, sequence_sql],
            )
            .map_err(DbError::from)?;
            for object_id in activated_object_ids {
                let mut remote = load_remote_object_on(conn, object_id)?
                    .into_observed_activated(&candidate)
                    .map_err(|error| {
                        DbError::Message(format!(
                            "record observed Merge activation for {object_id}: {error}"
                        ))
                    })?;
                let inert = remote
                    .retract_activated_candidate(
                        nonactivation.clone(),
                        (object_id == head_object_id).then_some(&head_nonactivation),
                    )
                    .map_err(|error| {
                        DbError::Message(format!(
                            "retract activated Merge object {object_id}: {error}"
                        ))
                    })?;
                finish_remote_candidate_nonactivation_on(conn, object_id, remote, inert)?;
            }
            let deleted = conn
                .execute(
                    "DELETE FROM materialized_commits
                     WHERE device_id = ?1 AND seq = ?2 AND commit_ref = ?3",
                    rusqlite::params![&stream_id, sequence_sql, &encoded_ref],
                )
                .map_err(DbError::from)?;
            if deleted != 1 {
                return Err(DbError::Message(
                    "retracted Merge materialization disappeared".to_string(),
                ));
            }
            let deleted = conn
                .execute(
                    "DELETE FROM store_device_state_snapshots WHERE commit_ref = ?1",
                    [&encoded_ref],
                )
                .map_err(DbError::from)?;
            if deleted != 1 {
                return Err(DbError::Message(
                    "retracted Merge device state disappeared".to_string(),
                ));
            }
            self.retire_circle_bootstrap_coverage(&candidate)?;
            let deleted = conn
                .execute(
                    "DELETE FROM retained_merge_materializations
                     WHERE device_id = ?1 AND seq = ?2 AND commit_ref = ?3 AND input_hash = ?4",
                    rusqlite::params![
                        &stream_id,
                        sequence_sql,
                        &encoded_ref,
                        retained.input_hash().to_string()
                    ],
                )
                .map_err(DbError::from)?;
            if deleted != 1 {
                return Err(DbError::Message(
                    "retracted Merge retained input disappeared".to_string(),
                ));
            }
            let raw_status: Option<String> = conn
                .query_row(
                    "SELECT status FROM store_writes WHERE write_id = ?1",
                    [retained.commit().write_id.as_str()],
                    |row| row.get(0),
                )
                .optional()
                .map_err(DbError::from)?;
            if let Some(raw_status) = raw_status {
                let stored_status: WriteStatus =
                    serde_json::from_str(&raw_status).map_err(|error| {
                        DbError::Message(format!("retracted Merge write status: {error}"))
                    })?;
                let original = match stored_status {
                    WriteStatus::Published(original) if original.commit() == &candidate => {
                        *original
                    }
                    WriteStatus::Publishing | WriteStatus::Blocked(_) => PublishedPosition {
                        device_id: retained.commit().author_registration.device_id.to_string(),
                        commit: candidate.clone(),
                    },
                    WriteStatus::Resolved(WriteResolution::Retracted { witness })
                        if witness.original_position().commit() == &candidate =>
                    {
                        return Err(DbError::Message(
                            "retracted Merge write still owns an active materialization"
                                .to_string(),
                        ));
                    }
                    other => {
                        return Err(DbError::Message(format!(
                            "retracted Merge write has incompatible status {other:?}"
                        )));
                    }
                };
                let witness = crate::WriteRetractionWitness::new(original, nonactivation.clone())
                    .map_err(DbError::Message)?;
                let status = WriteStatus::Resolved(WriteResolution::Retracted { witness });
                conn.execute(
                    "DELETE FROM store_write_blob_leases WHERE write_id = ?1",
                    [retained.commit().write_id.as_str()],
                )
                .map_err(DbError::from)?;
                conn.execute(
                    "DELETE FROM store_write_packages WHERE write_id = ?1",
                    [retained.commit().write_id.as_str()],
                )
                .map_err(DbError::from)?;
                conn.execute(
                    "DELETE FROM store_write_blobs WHERE write_id = ?1",
                    [retained.commit().write_id.as_str()],
                )
                .map_err(DbError::from)?;
                Database::set_write_status_on(conn, &retained.commit().write_id, &status)?;
                notifications.push((retained.commit().write_id.clone(), status));
            }
        }
        Ok(notifications)
    }
}

#[cfg(test)]
mod retraction_tests {
    use super::*;

    fn test_object(path: &str) -> crate::protocol::objects::ExactObjectRef {
        crate::protocol::objects::ExactObjectRef::new(
            crate::protocol::objects::ObjectSlot::logical(path.to_string())
                .expect("valid test object slot"),
            0,
            ObjectHash::digest(path.as_bytes()),
        )
    }

    #[test]
    fn merge_retraction_requires_the_exact_transitive_dependent_closure() {
        let stream = crate::protocol::causal_grants::AuthorStreamId::from_bytes([19; 32]);
        let commit = |sequence: u64, label: &str| StoreBatchCommitRef {
            coord: StoreCommitCoord {
                stream_id: stream,
                sequence,
            },
            commit_hash: ObjectHash::digest(format!("{label} commit").as_bytes()),
            object: test_object(&format!("store-v1/test/{label}/commit.json")),
        };
        let root = commit(1, "retraction-root");
        let child = commit(2, "retraction-child");
        let grandchild = commit(3, "retraction-grandchild");
        let independent = commit(4, "retraction-independent");
        let graph = BTreeMap::from([
            (root.clone(), BTreeSet::new()),
            (child.clone(), BTreeSet::from([root.clone()])),
            (grandchild.clone(), BTreeSet::from([child.clone()])),
            (independent.clone(), BTreeSet::new()),
        ]);

        let required = complete_merge_retraction_closure(&graph, BTreeSet::from([root.clone()]));

        assert_eq!(
            required,
            BTreeSet::from([root.clone(), child.clone(), grandchild]),
        );
        assert_ne!(required, BTreeSet::from([root.clone(), child.clone()]));
        assert!(!required.contains(&independent));
        assert!(require_exact_merge_retraction_closure(
            &graph,
            BTreeSet::from([root.clone()]),
            &BTreeSet::from([root, child]),
        )
        .is_err());
    }

    #[tokio::test]
    async fn merge_retraction_retires_its_circle_bootstrap_coverage_atomically() {
        let database = crate::sync::test_helpers::open_test_db();
        let activation = StoreBatchCommitRef {
            coord: StoreCommitCoord {
                stream_id: crate::protocol::causal_grants::AuthorStreamId::from_bytes([23; 32]),
                sequence: 7,
            },
            commit_hash: ObjectHash::digest(b"Circle bootstrap retraction activation"),
            object: test_object("store-v1/test/circle-bootstrap-retraction/commit.json"),
        };
        let encoded_activation =
            serde_json::to_string(&activation).expect("serialize bootstrap activation");
        database
            .call(move |connection| {
                connection
                    .execute(
                        "INSERT INTO circle_bootstrap_coverage
                         (circle_id, control_coord, activation_commit, exact_cut, image_hash,
                          image_bytes, bootstrap_ref)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                        rusqlite::params![
                            "00000000-0000-4000-8000-000000000001",
                            "{}",
                            encoded_activation,
                            "{}",
                            ObjectHash::digest(b"Circle bootstrap retraction image").to_string(),
                            b"Circle bootstrap retraction image".as_slice(),
                            b"{}".as_slice(),
                        ],
                    )
                    .map_err(DbError::from)?;
                let transaction = connection.unchecked_transaction().map_err(DbError::from)?;
                assert_eq!(
                    MergeMaterializationTransaction::new(&transaction)
                        .retire_circle_bootstrap_coverage(&activation)?,
                    1
                );
                let retained: i64 = transaction
                    .query_row(
                        "SELECT COUNT(*) FROM circle_bootstrap_coverage",
                        [],
                        |row| row.get(0),
                    )
                    .map_err(DbError::from)?;
                assert_eq!(retained, 0);
                transaction.rollback().map_err(DbError::from)?;
                let retained: i64 = connection
                    .query_row(
                        "SELECT COUNT(*) FROM circle_bootstrap_coverage",
                        [],
                        |row| row.get(0),
                    )
                    .map_err(DbError::from)?;
                assert_eq!(retained, 1);
                let transaction = connection.unchecked_transaction().map_err(DbError::from)?;
                assert_eq!(
                    MergeMaterializationTransaction::new(&transaction)
                        .retire_circle_bootstrap_coverage(&activation)?,
                    1
                );
                transaction.commit().map_err(DbError::from)?;
                let retained: i64 = connection
                    .query_row(
                        "SELECT COUNT(*) FROM circle_bootstrap_coverage",
                        [],
                        |row| row.get(0),
                    )
                    .map_err(DbError::from)?;
                assert_eq!(retained, 0);
                Ok(())
            })
            .await
            .expect("retire retracted Circle bootstrap coverage");
    }
}

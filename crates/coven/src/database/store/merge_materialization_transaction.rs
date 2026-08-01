mod changeset_application;
mod conflict;

use std::collections::{BTreeMap, BTreeSet};

use rusqlite::OptionalExtension;
use tracing::{debug, warn};

#[cfg(test)]
pub(crate) use changeset_application::{
    resolve_and_apply_changeset, resolve_and_apply_changeset_with_schema_on, ApplyResult,
};
pub(crate) use changeset_application::{ValidatedChangeset, WinningRow};
pub(crate) use conflict::{IncomingTimestampPolicy, TableSchema};

use super::store_device_state::{
    load_store_device_exclusion_freezes_on, replace_store_device_exclusion_freezes_on,
    store_device_state_for_history_cut_on,
};
use super::{
    apply_store_device_exclusion_freezes_on, load_declared_store_device_state_on, StoreDatabase,
};
use crate::blob::local_cleanup::intents_from_changes as local_blob_cleanup_intents;
use crate::changeset::RowChange;
use crate::database::ReclaimCommitActivation;
use crate::database::{
    insert_store_reclaim_operation_on, load_activated_registration_on, load_remote_object_on,
    load_store_reclaim_operation_on, record_reclaimed_store_package_on,
    required_store_root_authority_on, store_reclaim_journal_error, update_remote_object_on,
    update_store_reclaim_operation_on, BlobActivation, BlobDecls, Database, DbError,
    DurableStoreReclaimOperation, OwnedVerifiedMergeMaterialization, ReclaimedStorePackage,
    RetainedMergeMaterializationKey, RetainedPackageApplication, VerifiedMergeMaterialization,
};
use crate::protocol::audience_package::{AudiencePackage, PackageAudience};
use crate::protocol::remote_object::RemoteObjectRecord;
use crate::protocol::store_commit::{
    CircleAckRef, CommitFrontier, ObjectHash, StoreAckRef, StoreBatchCommit, StoreBatchCommitRef,
    StoreDeviceHead, StoreDeviceProposalState, StoreDeviceRegistration, StoreDeviceRegistrationRef,
    StoreHistoryCut, VerifiedStoreBatchCommit, VerifiedStoreDeviceOperations,
};
use crate::storage::ExactObjectRef;
use crate::sync::{
    ApplyOutcome, HeldStorePositionReason, LocalStoreMembership, PreparedMergeMaterialization,
    PreparedMergeMaterializationPackage, SyncedTable, VerifiedCircleActivations,
    VerifiedStreamActivations,
};

pub(crate) struct AppliedMergeMaterialization {
    pub(crate) outcome: ApplyOutcome,
    pub(crate) max_updated_at: Option<crate::sync::hlc::Timestamp>,
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
/// as an HLC [`crate::sync::hlc::Timestamp`]. A row whose `_updated_at` fails to
/// parse is logged and skipped — it must not panic the pull or silently default
/// the clock.
///
/// `max` becomes the value the caller advances the local HLC past, and that
/// advance is deliberately uncapped (it trusts a value already written to disk).
/// So the bound lives here, at the point a stamp is *collected*: a grossly-future
/// stamp — beyond `receiver_wall_ms` +
/// [`crate::sync::hlc::MAX_FUTURE_SKEW_MS`] — is logged and skipped, so it can
/// never ratchet the clock. A conflicting row with such a stamp was already
/// refused by the apply, but a *non-conflicting* INSERT (no local row to conflict
/// with) reaches here as an applied row, so this is the gate that stops it from
/// dragging the clock forward.
fn advance_max_updated_at(
    max: &mut Option<crate::sync::hlc::Timestamp>,
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
        match crate::sync::hlc::Timestamp::parse(raw) {
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

pub(crate) struct MergeMaterializationTransaction<'transaction, 'connection> {
    transaction: &'transaction rusqlite::Transaction<'connection>,
}

impl<'transaction, 'connection> MergeMaterializationTransaction<'transaction, 'connection> {
    pub(crate) fn new(transaction: &'transaction rusqlite::Transaction<'connection>) -> Self {
        Self { transaction }
    }
}

impl MergeMaterializationTransaction<'_, '_> {
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
        completion: crate::sync::StoreMembershipJournalCompletion,
        candidate: &StoreBatchCommitRef,
    ) -> Result<(), DbError> {
        match completion {
            crate::sync::StoreMembershipJournalCompletion::Mutation {
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
            crate::sync::StoreMembershipJournalCompletion::RotationMutation {
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
            crate::sync::StoreMembershipJournalCompletion::OwnerPromotion {
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
        intent: &crate::blob::local_cleanup::LocalBlobCleanupIntent,
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
        registrations: &[(
            StoreDeviceRegistration,
            crate::protocol::store_commit::StoreDeviceRegistrationActivation,
        )],
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
        activations: &[crate::sync::VerifiedCircleReference],
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
            let current_state_payload = StoreDatabase::reduce_circle_current_state_on(
                conn,
                commit.candidate_family(),
                activation,
            )?;
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
            if StoreDatabase::circle_current_state_is_deleted_on(conn, activation.circle_id)? {
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
        changeset_max: &mut Option<crate::sync::hlc::Timestamp>,
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
        changeset_max: &mut Option<crate::sync::hlc::Timestamp>,
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
        crate::database::StoreDatabase::record_activated_store_device_registrations_on(
            conn,
            commit,
            &registrations,
        )?;
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
            if crate::database::StoreDatabase::circle_current_state_is_deleted_on(
                conn,
                activation.circle_id,
            )? {
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
            Database::install_winning_blob_bindings_on(
                conn,
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
}

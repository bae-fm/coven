use std::collections::BTreeSet;

use rusqlite::OptionalExtension;

use super::*;
use crate::{
    begin_remote_candidate_nonactivation_on, insert_store_reclaim_operation_on,
    load_remote_object_on, load_store_reclaim_operation_on, parse_store_reclaim_operation,
    persist_exact_remote_object_on, record_reclaimed_store_package_on,
    replace_prepared_merge_head_remote_on, store_reclaim_journal_error, update_remote_object_on,
    update_store_reclaim_operation_on,
};
use coven_protocol::remote_object::{remote_object_id, RemoteObjectRecord, RetainedReplayOwner};
use coven_protocol::store_commit::{StoreBatchCommitRef, StorePackageRef};

pub mod journal;

impl StoreSession<'_> {
    fn begin_store_reclaim_operation(
        &mut self,
        operation: DurableStoreReclaimOperation,
        remotes: Vec<coven_protocol::remote_object::ClosedRemoteObject>,
    ) -> Result<DurableStoreReclaimOperation, DbError> {
        let conn = self.records.conn;
        let tx = conn.unchecked_transaction().map_err(DbError::from)?;
        let operation_id = operation.operation_id();
        if let Some(existing) = load_store_reclaim_operation_on(&tx, operation_id)? {
            if existing != operation {
                return Err(DbError::Message(format!(
                    "Store reclaim operation {operation_id} already has different durable state"
                )));
            }
            return Ok(existing);
        }
        for remote in &remotes {
            persist_exact_remote_object_on(
                &tx,
                self.records.store_dir,
                remote,
                "Store reclaim candidate object",
            )?;
        }
        insert_store_reclaim_operation_on(&tx, &operation)?;
        tx.commit().map_err(DbError::from)?;
        Ok(operation)
    }

    fn store_package_is_retained_for_replay(
        &mut self,
        root: &coven_protocol::store_commit::StoreRootRef,
        target: &StorePackageRef,
        activation: &StoreBatchCommitRef,
    ) -> Result<bool, DbError> {
        let object_id = remote_object_id(&target.object);
        let exists: bool = self
            .records
            .conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM remote_objects WHERE object_id = ?1)",
                [object_id.to_string()],
                |row| row.get(0),
            )
            .map_err(DbError::from)?;
        if !exists {
            return Ok(false);
        }
        let remote = load_remote_object_on(self.records.conn, object_id)?;
        let retained = remote
            .store_package_is_retained_for_replay(target, activation)
            .map_err(|error| {
                DbError::context(
                    format!("validate Store package {object_id} replay ownership"),
                    error,
                )
            })?;
        if !retained {
            return Ok(false);
        }
        for owner in remote.retained_replay_owners() {
            let RetainedReplayOwner::Commit { commit, input_hash } = owner;
            let retained = self
                .verified_store_authority
                .validate_retained_materialization_by_ref_on(self.records, commit)?;
            if retained.root() != root || retained.input_hash() != *input_hash {
                return Err(DbError::Message(
                    "Store package replay owner differs from retained materialization".to_string(),
                ));
            }
        }
        Ok(true)
    }

    fn circle_package_is_retained_for_replay(
        &mut self,
        root: &coven_protocol::store_commit::StoreRootRef,
        target: &coven_protocol::store_commit::CirclePackageRef,
        activation: &StoreBatchCommitRef,
    ) -> Result<bool, DbError> {
        let object_id = remote_object_id(&target.package.object);
        let exists: bool = self
            .records
            .conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM remote_objects WHERE object_id = ?1)",
                [object_id.to_string()],
                |row| row.get(0),
            )
            .map_err(DbError::from)?;
        if !exists {
            return Ok(false);
        }
        let remote = load_remote_object_on(self.records.conn, object_id)?;
        let retained = remote
            .circle_package_is_retained_for_replay(target, activation)
            .map_err(|error| {
                DbError::context(
                    format!("validate Circle package {object_id} replay ownership"),
                    error,
                )
            })?;
        if !retained {
            return Ok(false);
        }
        for owner in remote.retained_replay_owners() {
            let RetainedReplayOwner::Commit { commit, input_hash } = owner;
            let retained = self
                .verified_store_authority
                .validate_retained_materialization_by_ref_on(self.records, commit)?;
            if retained.root() != root || retained.input_hash() != *input_hash {
                return Err(DbError::Message(
                    "Circle package replay owner differs from retained materialization".to_string(),
                ));
            }
        }
        Ok(true)
    }

    fn circle_image_is_retained_for_replay(
        &self,
        circle_id: coven_protocol::circle::CircleId,
        image: &coven_protocol::store_commit::SnapshotImageRef,
    ) -> Result<bool, DbError> {
        let row: Option<Vec<u8>> = self
            .records
            .conn
            .query_row(
                "SELECT bootstrap_ref FROM circle_bootstrap_coverage WHERE circle_id = ?1",
                [circle_id.to_string()],
                |row| row.get(0),
            )
            .optional()
            .map_err(DbError::from)?;
        let Some(bootstrap_ref) = row else {
            return Ok(false);
        };
        let bootstrap: coven_protocol::circle::CircleBootstrapRef =
            serde_json::from_slice(&bootstrap_ref).map_err(|error| {
                DbError::context("parse retained Circle bootstrap reference", error)
            })?;
        Ok(bootstrap.image == *image)
    }

    fn stored_blob_reclaim_candidates(
        &self,
    ) -> Result<
        Vec<(
            coven_protocol::blob::locator::StoredBlobRef,
            Vec<StoreBatchCommitRef>,
        )>,
        DbError,
    > {
        let conn = self.records.conn;
        let mut statement = conn
            .prepare("SELECT remote_object_id FROM blob_locators ORDER BY remote_object_id")
            .map_err(DbError::from)?;
        let object_ids = statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(DbError::from)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(DbError::from)?;
        drop(statement);
        let mut candidates = Vec::new();
        for object_id in object_ids {
            let parsed = object_id.parse().map_err(|error| {
                DbError::context(format!("stored blob object id {object_id:?}"), error)
            })?;
            let remote = load_remote_object_on(conn, parsed)?;
            if !remote.is_activated_stored_blob() {
                continue;
            }
            let Some(locator_bytes) = remote.payloads().carried_locator_bytes() else {
                return Err(DbError::Message(format!(
                    "stored blob {object_id} carries no locator"
                )));
            };
            let locator = coven_protocol::blob::locator::BlobLocator::parse(locator_bytes)
                .map_err(|error| {
                    DbError::context(format!("stored blob {object_id} locator"), error)
                })?;
            let stored =
                coven_protocol::blob::locator::StoredBlobRef::new(locator, remote.object().clone())
                    .map_err(|error| {
                        DbError::context(format!("stored blob {object_id} reference"), error)
                    })?;
            candidates.push((stored, remote.stored_blob_commit_owners()));
        }
        Ok(candidates)
    }

    fn stored_blob_is_row_orphaned(
        &self,
        stored: &coven_protocol::blob::locator::StoredBlobRef,
    ) -> Result<bool, DbError> {
        match crate::Database::stored_blob_reference_state_on(
            self.records.conn,
            self.gates,
            self.synced_tables,
            stored,
        )? {
            crate::StoredBlobReferenceState::NotLiveRemote => Ok(true),
            crate::StoredBlobReferenceState::LiveRemote => Ok(false),
            crate::StoredBlobReferenceState::Unresolved => Err(DbError::Message(format!(
                "stored blob {} has a live reference whose locality is unresolved",
                remote_object_id(stored.object())
            ))),
        }
    }

    fn audience_blob_is_retained_for_replay(
        &self,
        stored: &coven_protocol::blob::locator::StoredBlobRef,
    ) -> Result<bool, DbError> {
        let conn = self.records.conn;
        let object_id = remote_object_id(stored.object());
        let exists: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM remote_objects WHERE object_id = ?1)",
                [object_id.to_string()],
                |row| row.get(0),
            )
            .map_err(DbError::from)?;
        if exists {
            let remote = load_remote_object_on(conn, object_id)?;
            if remote.snapshot_owners().next().is_some()
                || remote.retained_replay_owners().next().is_some()
            {
                return Ok(true);
            }
        }
        let mut statement = conn
            .prepare("SELECT bootstrap_ref FROM circle_bootstrap_coverage")
            .map_err(DbError::from)?;
        let coverages = statement
            .query_map([], |row| row.get::<_, Vec<u8>>(0))
            .map_err(DbError::from)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(DbError::from)?;
        drop(statement);
        for bytes in coverages {
            let bootstrap: coven_protocol::circle::CircleBootstrapRef =
                serde_json::from_slice(&bytes).map_err(|error| {
                    DbError::context("parse retained Circle bootstrap reference", error)
                })?;
            if bootstrap
                .blobs
                .iter()
                .any(|blob| blob.stored() == Some(stored))
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn store_reclaim_operations(&self) -> Result<Vec<DurableStoreReclaimOperation>, DbError> {
        let mut statement = self
            .records
            .conn
            .prepare(
                "SELECT authorization_hash, state FROM store_reclaim_operations
                 ORDER BY authorization_hash",
            )
            .map_err(DbError::from)?;
        let rows = statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(DbError::from)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(DbError::from)?;
        rows.into_iter()
            .map(|(raw_id, raw)| {
                let id = raw_id
                    .parse()
                    .map_err(|error| DbError::context("Store reclaim operation id", error))?;
                parse_store_reclaim_operation(id, &raw)
            })
            .collect()
    }

    fn begin_store_reclaim_receipt(
        &mut self,
        expected: DurableStoreReclaimOperation,
        next: DurableStoreReclaimOperation,
        remotes: Vec<coven_protocol::remote_object::ClosedRemoteObject>,
    ) -> Result<DurableStoreReclaimOperation, DbError> {
        let tx = self
            .records
            .conn
            .unchecked_transaction()
            .map_err(DbError::from)?;
        let current = load_store_reclaim_operation_on(&tx, expected.operation_id())?
            .ok_or_else(|| DbError::Message("Store reclaim operation disappeared".to_string()))?;
        if current != expected {
            return Err(DbError::Message(
                "Store reclaim operation changed before receipt preparation".to_string(),
            ));
        }
        for remote in &remotes {
            persist_exact_remote_object_on(
                &tx,
                self.records.store_dir,
                remote,
                "Store reclaim receipt candidate",
            )?;
        }
        update_store_reclaim_operation_on(&tx, &expected, &next)?;
        tx.commit().map_err(DbError::from)?;
        Ok(next)
    }

    fn mark_store_reclaim_target_absent(
        &mut self,
        expected: DurableStoreReclaimOperation,
        next: DurableStoreReclaimOperation,
        reclaimed: ReclaimedStorePackage,
    ) -> Result<DurableStoreReclaimOperation, DbError> {
        let root = self.required_root_authority()?;
        let tx = self
            .records
            .conn
            .unchecked_transaction()
            .map_err(DbError::from)?;
        let current = load_store_reclaim_operation_on(&tx, expected.operation_id())?
            .ok_or_else(|| DbError::Message("Store reclaim operation disappeared".to_string()))?;
        if current != expected {
            return Err(DbError::Message(
                "Store reclaim operation changed before absence recording".to_string(),
            ));
        }
        record_reclaimed_store_package_on(&tx, Some(root.store_root_hash), &reclaimed)?;
        update_store_reclaim_operation_on(&tx, &expected, &next)?;
        tx.commit().map_err(DbError::from)?;
        Ok(next)
    }

    fn replace_store_reclaim_candidate(
        &mut self,
        expected: DurableStoreReclaimOperation,
        current_candidate: coven_protocol::prepared_commit::PreparedStoreOperationCommit,
        next: DurableStoreReclaimOperation,
    ) -> Result<DurableStoreReclaimOperation, DbError> {
        let tx = self
            .records
            .conn
            .unchecked_transaction()
            .map_err(DbError::from)?;
        let current = load_store_reclaim_operation_on(&tx, expected.operation_id())?
            .ok_or_else(|| DbError::Message("Store reclaim operation disappeared".to_string()))?;
        if current != expected {
            return Err(DbError::Message(
                "Store reclaim operation changed before candidate replacement".to_string(),
            ));
        }
        let next_candidate = next.candidate().expect("constructed candidate state");
        match (current_candidate.head_ref(), next_candidate.head_ref()) {
            (current, replacement) if current != replacement => {
                let (winner, prepared) = next_candidate.publication();
                replace_prepared_merge_head_remote_on(
                    &tx,
                    self.records.store_dir,
                    &current.object,
                    winner,
                    prepared,
                    &current_candidate.reference,
                )?;
            }
            _ => {}
        }
        update_store_reclaim_operation_on(&tx, &expected, &next)?;
        tx.commit().map_err(DbError::from)?;
        Ok(next)
    }

    fn begin_store_reclaim_candidate_replacement(
        &mut self,
        expected: DurableStoreReclaimOperation,
        next: DurableStoreReclaimOperation,
        replacement_remotes: Vec<coven_protocol::remote_object::ClosedRemoteObject>,
        nonactivation: coven_protocol::remote_object::CandidateNonactivation,
        losing_candidate: coven_protocol::prepared_commit::PreparedStoreOperationCommit,
    ) -> Result<DurableStoreReclaimOperation, DbError> {
        let tx = self
            .records
            .conn
            .unchecked_transaction()
            .map_err(DbError::from)?;
        let current = load_store_reclaim_operation_on(&tx, expected.operation_id())?
            .ok_or_else(|| DbError::Message("Store reclaim operation disappeared".to_string()))?;
        if current != expected {
            return Err(DbError::Message(
                "Store reclaim operation changed before candidate replacement".to_string(),
            ));
        }
        let authority_ids = replacement_remotes
            .iter()
            .filter(|remote| matches!(
                remote.record(),
                RemoteObjectRecord::RetainedAuthority(record)
                    if matches!(
                        record.identity.domain,
                        coven_protocol::remote_object::RetainedAuthorityObjectDomain::ReclaimEvidence { .. }
                            | coven_protocol::remote_object::RetainedAuthorityObjectDomain::ReclaimAuthorization { .. }
                            | coven_protocol::remote_object::RetainedAuthorityObjectDomain::ReclaimReceipt { .. }
                    )
            ))
            .map(|remote| remote.object_id())
            .collect::<BTreeSet<_>>();
        for remote in replacement_remotes
            .iter()
            .filter(|remote| !authority_ids.contains(&remote.object_id()))
        {
            persist_exact_remote_object_on(
                &tx,
                self.records.store_dir,
                remote,
                "replacement Store reclaim candidate object",
            )?;
        }
        for authority_id in authority_ids {
            let mut authority = load_remote_object_on(&tx, authority_id)?;
            authority
                .add_retained_authority_candidate(
                    next.candidate()
                        .expect("constructed candidate state")
                        .reference
                        .clone(),
                )
                .map_err(|error| {
                    DbError::context("attach replacement reclaim authority candidate", error)
                })?;
            update_remote_object_on(&tx, authority_id, &authority)?;
            if begin_remote_candidate_nonactivation_on(&tx, authority_id, nonactivation.clone())?
                .is_some()
            {
                return Err(DbError::Message(
                    "reusable reclaim authority became a deletion target".to_string(),
                ));
            }
        }
        let head = losing_candidate.head_ref();
        if begin_remote_candidate_nonactivation_on(
            &tx,
            remote_object_id(&head.object),
            nonactivation.clone(),
        )?
        .is_some()
        {
            return Err(DbError::Message(
                "losing reclaim activation head became a deletion target".to_string(),
            ));
        }
        if begin_remote_candidate_nonactivation_on(
            &tx,
            remote_object_id(&losing_candidate.reference.object),
            nonactivation,
        )?
        .is_none()
        {
            return Err(DbError::Message(
                "losing reclaim commit has no exact deletion target".to_string(),
            ));
        }
        update_store_reclaim_operation_on(&tx, &expected, &next)?;
        tx.commit().map_err(DbError::from)?;
        Ok(next)
    }

    fn store_reclaim_replacement_cleanup_targets(
        &self,
        expected: &DurableStoreReclaimOperation,
    ) -> Result<Vec<CandidateCleanupObject>, DbError> {
        let current = load_store_reclaim_operation_on(self.records.conn, expected.operation_id())?
            .ok_or_else(|| DbError::Message("Store reclaim operation disappeared".to_string()))?;
        if &current != expected {
            return Err(DbError::Message(
                "Store reclaim operation changed before cleanup".to_string(),
            ));
        }
        let losing = current.losing_candidate().ok_or_else(|| {
            DbError::Message("Store reclaim operation has no losing candidate".to_string())
        })?;
        super::candidate_records::candidate_cleanup_targets_on(
            self.records.conn,
            &losing.candidate.reference,
            std::slice::from_ref(&losing.candidate.reference.object),
        )
    }

    fn complete_store_reclaim_candidate_replacement(
        &mut self,
        expected: DurableStoreReclaimOperation,
        losing: StoreReclaimCandidateLoss,
        next: DurableStoreReclaimOperation,
    ) -> Result<DurableStoreReclaimOperation, DbError> {
        let tx = self
            .records
            .conn
            .unchecked_transaction()
            .map_err(DbError::from)?;
        let current = load_store_reclaim_operation_on(&tx, expected.operation_id())?
            .ok_or_else(|| DbError::Message("Store reclaim operation disappeared".to_string()))?;
        if current != expected {
            return Err(DbError::Message(
                "Store reclaim operation changed before replacement completion".to_string(),
            ));
        }
        let object_id = remote_object_id(&losing.candidate.reference.object);
        if !super::candidate_records::candidate_cleanup_targets_on(
            &tx,
            &losing.candidate.reference,
            std::slice::from_ref(&losing.candidate.reference.object),
        )?
        .is_empty()
        {
            return Err(DbError::Message(
                "losing reclaim commit cleanup is incomplete".to_string(),
            ));
        }
        super::candidate_records::delete_remote_objects_on(&tx, [object_id], "losing reclaim")?;
        update_store_reclaim_operation_on(&tx, &expected, &next)?;
        tx.commit().map_err(DbError::from)?;
        Ok(next)
    }

    #[cfg(any(test, feature = "test-utils"))]
    fn stored_blob_has_snapshot_owner_for_test(
        &self,
        stored: &coven_protocol::blob::locator::StoredBlobRef,
    ) -> Result<bool, DbError> {
        let remote = load_remote_object_on(self.records.conn, remote_object_id(stored.object()))?;
        let pinned = remote.snapshot_owners().next().is_some();
        Ok(pinned)
    }
}

impl StoreDatabase {
    pub async fn begin_store_reclaim_operation(
        &self,
        operation: DurableStoreReclaimOperation,
    ) -> Result<DurableStoreReclaimOperation, DbError> {
        operation.validate().map_err(store_reclaim_journal_error)?;
        let DurableStoreReclaimOperation::AuthorizationCandidate { .. } = &operation else {
            return Err(DbError::Message(
                "a new Store reclaim operation must own an activation candidate".to_string(),
            ));
        };
        let remotes = match &operation {
            DurableStoreReclaimOperation::AuthorizationCandidate { object, candidate } => object
                .remote_objects(candidate)
                .map_err(store_reclaim_journal_error)?,
            _ => unreachable!("matched reclaim candidate"),
        };
        self.call_store(move |session| session.begin_store_reclaim_operation(operation, remotes))
            .await
    }

    pub async fn store_package_is_retained_for_replay(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        target: StorePackageRef,
        activation: StoreBatchCommitRef,
    ) -> Result<bool, DbError> {
        self.call_store(move |session| {
            session.store_package_is_retained_for_replay(&root, &target, &activation)
        })
        .await
    }

    pub async fn circle_package_is_retained_for_replay(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        target: coven_protocol::store_commit::CirclePackageRef,
        activation: StoreBatchCommitRef,
    ) -> Result<bool, DbError> {
        self.call_store(move |session| {
            session.circle_package_is_retained_for_replay(&root, &target, &activation)
        })
        .await
    }

    /// Whether a Circle bootstrap image is still the local device's live seed for
    /// its Circle: the `circle_bootstrap_coverage` row names the same image. Such a
    /// bootstrap is a retained replay input and is never eligible for reclamation —
    /// the per-Circle analogue of the package retained-replay guard, re-checked
    /// before deletion so a seed installed since authoring fails the delete loud.
    pub async fn circle_bootstrap_image_is_retained_for_replay(
        &self,
        coverage: coven_protocol::circle::CircleBootstrapCoverageRef,
    ) -> Result<bool, DbError> {
        self.circle_image_is_retained_for_replay(
            coverage.circle_id,
            coverage.bootstrap.image.clone(),
        )
        .await
    }

    /// Whether the local device's live Circle projection was seeded from this exact
    /// image. One `circle_bootstrap_coverage` row per Circle names whichever image
    /// the projection came from — a recipient bootstrap installed on pull or a
    /// standalone snapshot installed on restore — so both kinds of image answer the
    /// same question against the same row.
    pub async fn circle_image_is_retained_for_replay(
        &self,
        circle_id: coven_protocol::circle::CircleId,
        image: coven_protocol::store_commit::SnapshotImageRef,
    ) -> Result<bool, DbError> {
        self.call_store(move |session| {
            session.circle_image_is_retained_for_replay(circle_id, &image)
        })
        .await
    }

    /// Every stored row blob this device has an ownership record for, paired with
    /// the activated Store commits whose package bindings published it.
    /// `blob_locators` is the stored-blob subset of `remote_objects`, so it is the
    /// exact candidate set without scanning every remote object.
    pub async fn stored_blob_reclaim_candidates(
        &self,
    ) -> Result<
        Vec<(
            coven_protocol::blob::locator::StoredBlobRef,
            Vec<StoreBatchCommitRef>,
        )>,
        DbError,
    > {
        self.call_store(|session| session.stored_blob_reclaim_candidates())
            .await
    }

    /// Whether no live row in this device's materialized state binds the blob as a
    /// remote reference — the same predicate the member-signed tombstone path
    /// applies before deleting a blob body. An unresolved reference is not an
    /// answer: it means a row's locality cannot be decided yet, so it fails rather
    /// than counting as an orphan.
    pub async fn stored_blob_is_row_orphaned(
        &self,
        stored: coven_protocol::blob::locator::StoredBlobRef,
    ) -> Result<bool, DbError> {
        self.call_store(move |session| session.stored_blob_is_row_orphaned(&stored))
            .await
    }

    /// Whether an installable image still pins this row blob.
    ///
    /// A snapshot or bootstrap image lists the exact blobs a device installing
    /// from it must be able to read. Those blobs outlive the rows that published
    /// them: a device restoring from an image reads its listed blobs before it has
    /// any rows at all, so "no live row binds this blob" does not mean the blob is
    /// free. A blob a retained image lists is never eligible, whatever its rows
    /// say. Re-checked before deletion, so an image published since the
    /// authorization was signed fails the delete loud rather than removing a blob
    /// a restore now needs.
    pub async fn audience_blob_is_retained_for_replay(
        &self,
        stored: coven_protocol::blob::locator::StoredBlobRef,
    ) -> Result<bool, DbError> {
        self.call_store(move |session| session.audience_blob_is_retained_for_replay(&stored))
            .await
    }

    pub async fn store_reclaim_operations(
        &self,
    ) -> Result<Vec<DurableStoreReclaimOperation>, DbError> {
        self.call_store(|session| session.store_reclaim_operations())
            .await
    }

    pub async fn begin_store_reclaim_receipt(
        &self,
        expected: DurableStoreReclaimOperation,
        object: DurableStoreReclaimObject,
        candidate: coven_protocol::prepared_commit::PreparedStoreOperationCommit,
    ) -> Result<DurableStoreReclaimOperation, DbError> {
        let DurableStoreReclaimOperation::AbsentVerified {
            authorization,
            authorization_activation,
            ..
        } = &expected
        else {
            return Err(DbError::Message(
                "only an authorized reclaim can prepare a receipt".to_string(),
            ));
        };
        let next = DurableStoreReclaimOperation::ReceiptCandidate {
            authorization: authorization.clone(),
            authorization_activation: authorization_activation.clone(),
            object: Box::new(object),
            candidate: Box::new(candidate),
        };
        next.validate().map_err(store_reclaim_journal_error)?;
        let remotes = match &next {
            DurableStoreReclaimOperation::ReceiptCandidate {
                object, candidate, ..
            } => object
                .remote_objects(candidate)
                .map_err(store_reclaim_journal_error)?,
            _ => unreachable!("constructed receipt candidate"),
        };
        self.call_store(move |session| session.begin_store_reclaim_receipt(expected, next, remotes))
            .await
    }

    pub async fn mark_store_reclaim_target_absent(
        &self,
        expected: DurableStoreReclaimOperation,
        target: coven_protocol::reclaim::ReclaimTarget,
    ) -> Result<DurableStoreReclaimOperation, DbError> {
        let DurableStoreReclaimOperation::Authorized {
            authorization,
            activation,
        } = &expected
        else {
            return Err(DbError::Message(
                "only an authorized reclaim can record target absence".to_string(),
            ));
        };
        if &target != authorization.target() {
            return Err(DbError::Message(
                "verified reclaim target differs from its signed exact reference".to_string(),
            ));
        }
        let next = DurableStoreReclaimOperation::AbsentVerified {
            authorization: authorization.clone(),
            authorization_activation: activation.clone(),
            target,
        };
        let reclaimed =
            ReclaimedStorePackage::absent_verified(authorization.clone(), activation.clone())
                .map_err(store_reclaim_journal_error)?;
        next.validate().map_err(store_reclaim_journal_error)?;
        self.call_store(move |session| {
            session.mark_store_reclaim_target_absent(expected, next, reclaimed)
        })
        .await
    }

    pub async fn replace_store_reclaim_candidate(
        &self,
        expected: DurableStoreReclaimOperation,
        replacement: coven_protocol::prepared_commit::PreparedStoreOperationCommit,
    ) -> Result<DurableStoreReclaimOperation, DbError> {
        let current_candidate = expected.candidate().cloned().ok_or_else(|| {
            DbError::Message("Store reclaim state has no replaceable candidate".to_string())
        })?;
        if current_candidate.reference != replacement.reference
            || current_candidate.commit != replacement.commit
        {
            return Err(DbError::Message(
                "Store reclaim candidate replacement changes its signed commit".to_string(),
            ));
        }
        let next = match &expected {
            DurableStoreReclaimOperation::AuthorizationCandidate { object, .. } => {
                DurableStoreReclaimOperation::AuthorizationCandidate {
                    object: object.clone(),
                    candidate: Box::new(replacement),
                }
            }
            DurableStoreReclaimOperation::ReceiptCandidate {
                authorization,
                authorization_activation,
                object,
                ..
            } => DurableStoreReclaimOperation::ReceiptCandidate {
                authorization: authorization.clone(),
                authorization_activation: authorization_activation.clone(),
                object: object.clone(),
                candidate: Box::new(replacement),
            },
            _ => {
                return Err(DbError::Message(
                    "Store reclaim state has no replaceable candidate".to_string(),
                ));
            }
        };
        next.validate().map_err(store_reclaim_journal_error)?;
        self.call_store(move |session| {
            session.replace_store_reclaim_candidate(expected, current_candidate, next)
        })
        .await
    }

    pub async fn begin_store_reclaim_candidate_replacement(
        &self,
        expected: DurableStoreReclaimOperation,
        replacement: coven_protocol::prepared_commit::PreparedStoreOperationCommit,
        nonactivation: coven_protocol::remote_object::VerifiedCandidateNonactivation,
    ) -> Result<DurableStoreReclaimOperation, DbError> {
        let object = expected.object().cloned().ok_or_else(|| {
            DbError::Message("Store reclaim operation has no replaceable object".to_string())
        })?;
        let losing_candidate = expected.candidate().cloned().ok_or_else(|| {
            DbError::Message("Store reclaim operation has no losing candidate".to_string())
        })?;
        if nonactivation
            .candidate_reference()
            .map_err(|error| DbError::Message(error.to_string()))?
            != losing_candidate.reference
        {
            return Err(DbError::Message(
                "verified nonactivation names another Store reclaim candidate".to_string(),
            ));
        }
        let nonactivation = nonactivation.into_durable();
        let proof = nonactivation.proof().clone();
        let loss = StoreReclaimCandidateLoss {
            candidate: Box::new(losing_candidate.clone()),
            proof: proof.clone(),
        };
        let next = match &expected {
            DurableStoreReclaimOperation::AuthorizationCandidate { .. } => {
                DurableStoreReclaimOperation::AuthorizationReplacing {
                    object: Box::new(object.clone()),
                    candidate: Box::new(replacement.clone()),
                    losing: Box::new(loss),
                }
            }
            DurableStoreReclaimOperation::ReceiptCandidate {
                authorization,
                authorization_activation,
                ..
            } => DurableStoreReclaimOperation::ReceiptReplacing {
                authorization: authorization.clone(),
                authorization_activation: authorization_activation.clone(),
                object: Box::new(object.clone()),
                candidate: Box::new(replacement.clone()),
                losing: Box::new(loss),
            },
            _ => {
                return Err(DbError::Message(
                    "Store reclaim operation is not awaiting candidate publication".to_string(),
                ));
            }
        };
        next.validate().map_err(store_reclaim_journal_error)?;
        if nonactivation.candidate().canonical_signed_bytes != losing_candidate.commit.to_bytes() {
            return Err(DbError::Message(
                "verified nonactivation bytes differ from the Store reclaim candidate".to_string(),
            ));
        }
        let replacement_remotes = object
            .remote_objects(&replacement)
            .map_err(store_reclaim_journal_error)?;
        self.call_store(move |session| {
            session.begin_store_reclaim_candidate_replacement(
                expected,
                next,
                replacement_remotes,
                nonactivation,
                losing_candidate,
            )
        })
        .await
    }

    pub async fn store_reclaim_replacement_cleanup_targets(
        &self,
        expected: DurableStoreReclaimOperation,
    ) -> Result<Vec<CandidateCleanupObject>, DbError> {
        self.call_store(move |session| session.store_reclaim_replacement_cleanup_targets(&expected))
            .await
    }

    pub async fn complete_store_reclaim_candidate_replacement(
        &self,
        expected: DurableStoreReclaimOperation,
    ) -> Result<DurableStoreReclaimOperation, DbError> {
        let losing = expected.losing_candidate().cloned().ok_or_else(|| {
            DbError::Message("Store reclaim operation has no replacement cleanup".to_string())
        })?;
        let next = match &expected {
            DurableStoreReclaimOperation::AuthorizationReplacing {
                object, candidate, ..
            } => DurableStoreReclaimOperation::AuthorizationCandidate {
                object: object.clone(),
                candidate: candidate.clone(),
            },
            DurableStoreReclaimOperation::ReceiptReplacing {
                authorization,
                authorization_activation,
                object,
                candidate,
                ..
            } => DurableStoreReclaimOperation::ReceiptCandidate {
                authorization: authorization.clone(),
                authorization_activation: authorization_activation.clone(),
                object: object.clone(),
                candidate: candidate.clone(),
            },
            _ => {
                return Err(DbError::Message(
                    "Store reclaim operation has no replacement cleanup".to_string(),
                ));
            }
        };
        next.validate().map_err(store_reclaim_journal_error)?;
        self.call_store(move |session| {
            session.complete_store_reclaim_candidate_replacement(expected, losing, next)
        })
        .await
    }

    /// Whether a published snapshot generation lists this blob in its image, read
    /// straight off the ownership record.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn stored_blob_has_snapshot_owner_for_test(
        &self,
        stored: coven_protocol::blob::locator::StoredBlobRef,
    ) -> Result<bool, DbError> {
        self.call_store(move |session| session.stored_blob_has_snapshot_owner_for_test(&stored))
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn stored_blob_reclaim_candidates_for_test(
        &self,
    ) -> Result<
        Vec<(
            coven_protocol::blob::locator::StoredBlobRef,
            Vec<StoreBatchCommitRef>,
        )>,
        DbError,
    > {
        self.stored_blob_reclaim_candidates().await
    }
}

use rusqlite::OptionalExtension;

use super::*;
use crate::{
    insert_store_reclaim_operation_on, load_remote_object_on, load_store_reclaim_operation_on,
    parse_store_reclaim_operation, persist_exact_remote_object_on,
    record_reclaimed_store_package_on, store_reclaim_journal_error,
    update_store_reclaim_operation_on, ActiveStorePublication, ActiveStorePublicationOwner,
};
use coven_protocol::remote_object::{remote_object_id, RetainedReplayOwner};
use coven_protocol::store_commit::{ObjectHash, StoreBatchCommitRef, StorePackageRef};

pub mod journal;

impl VerifiedStoreTransaction<'_, '_, '_, '_> {
    pub(super) fn prepare_snapshot_replay_baseline_advance(
        &mut self,
        root: &coven_protocol::store_commit::StoreRootRef,
        snapshot_authority: &coven_protocol::store_commit::RetainedReplaySnapshotAuthority,
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
    ) -> Result<
        Option<crate::store::materialization_models::PreparedSnapshotReplayBaselineAdvance>,
        DbError,
    > {
        let cut = &snapshot_authority.metadata.coverage;
        if !crate::store::store_session::replay_baseline_advances_on(
            crate::store::store_session::StoreRecords::new(
                self.store.transaction,
                self.store.store_dir,
            ),
            &snapshot_authority.snapshot,
            cut,
        )? {
            return Ok(None);
        }
        let current_cut = coven_protocol::store_commit::CommitFrontier::from_refs(
            crate::store::materialized_commit_index::materialized_frontier_on(
                self.store.transaction,
                None,
            )?,
        )
        .map_err(DbError::from)?;
        if !current_cut.covers(cut) {
            return Err(DbError::Message(
                "accepted Store snapshot is ahead of the locally reconstructable frontier"
                    .to_string(),
            ));
        }
        let installed = super::retained_replay::load_replay_baseline_metadata_on(
            StoreRecords::new(self.store.transaction, self.store.store_dir),
        )?
        .ok_or_else(|| {
            DbError::Message("snapshot advance has no installed baseline".to_string())
        })?;
        let changes_publication_base = !matches!(&installed.authority,
            crate::RetainedReplayAuthority::InstalledSnapshot(previous) if previous.snapshot == snapshot_authority.snapshot);
        let (image, folded) = self.capture_replay_baseline_at_cut(
            root,
            cut,
            &current_cut,
            snapshot_authority.snapshot.snapshot_hash,
            routing_encryption,
        )?;
        Ok(Some(
            crate::store::materialization_models::PreparedSnapshotReplayBaselineAdvance {
                changes_publication_base,
                expected_current_cut: current_cut,
                image,
                folded,
            },
        ))
    }
}

impl StoreSession<'_> {
    fn begin_store_reclaim_operation(
        &mut self,
        operation: DurableStoreReclaimOperation,
        remotes: Vec<coven_protocol::remote_object::ClosedRemoteObject>,
    ) -> Result<DurableStoreReclaimOperation, DbError> {
        let conn = self.conn;
        let tx = conn.unchecked_transaction().map_err(DbError::from)?;
        let operation_id = operation.operation_id();
        let candidate = operation.candidate().ok_or_else(|| {
            DbError::Message("Store reclaim operation has no publication candidate".to_string())
        })?;
        let active_publication = ActiveStorePublication::for_commit(
            ActiveStorePublicationOwner::Reclaim(operation_id),
            candidate,
        )?;
        if let Some(existing) = load_store_reclaim_operation_on(&tx, operation_id)? {
            if existing != operation {
                return Err(DbError::Message(format!(
                    "Store reclaim operation {operation_id} already has different durable state"
                )));
            }
            if !super::active_store_publication::load_active_store_publication_on(&tx)?
                .is_some_and(|existing| existing.same_commit_reservation(&active_publication))
            {
                return Err(DbError::Message(format!(
                    "Store reclaim operation {operation_id} differs from its active publication"
                )));
            }
            return Ok(existing);
        }
        match super::active_store_publication::claim_active_store_publication_on(
            &tx,
            &active_publication,
        )? {
            super::active_store_publication::ActiveStorePublicationClaim::Acquired => {}
            super::active_store_publication::ActiveStorePublicationClaim::AlreadyOwned => {
                return Err(DbError::Message(
                    "Store reclaim owns publication before its journal".to_string(),
                ));
            }
            super::active_store_publication::ActiveStorePublicationClaim::Occupied(owner) => {
                return Err(DbError::Message(format!(
                    "another local Store operation owns publication: {owner:?}"
                )));
            }
        }
        for remote in &remotes {
            persist_exact_remote_object_on(
                &tx,
                self.store_dir,
                remote,
                "Store reclaim candidate object",
            )?;
        }
        insert_store_reclaim_operation_on(&tx, &operation)?;
        tx.commit().map_err(DbError::from)?;
        Ok(operation)
    }

    /// Adopt an accepted snapshot as this device's replay baseline.
    fn advance_snapshot_replay_baseline(
        &mut self,
        root: &coven_protocol::store_commit::StoreRootRef,
        snapshot_authority: coven_protocol::store_commit::RetainedReplaySnapshotAuthority,
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
        membership: coven_protocol::membership::LocalStoreMembership,
    ) -> Result<Option<crate::AdvancedReplayBaseline>, DbError> {
        let schema_version = self.schema_version;
        let routing_hash = self.sync_routing_hash;
        self.verified_store_transaction(|transaction| {
            let current = super::observed_store_publication::load_store_current_publication_on(transaction.store.transaction)?;
            let accepted = current.record().latest_snapshot().filter(|accepted| accepted.snapshot == snapshot_authority.snapshot).ok_or_else(|| DbError::Message("replay baseline must adopt the current accepted snapshot".to_string()))?;
            let Some(prepared) = transaction.prepare_snapshot_replay_baseline_advance(root, &snapshot_authority, routing_encryption)? else {
                return Ok(StoreTransactionOutcome::Rollback(None));
            };
            let changes_publication_base = prepared.changes_publication_base;
            let advanced = transaction.store.advance_snapshot_replay_baseline(
                transaction.authority, root, schema_version, routing_hash,
                snapshot_authority, prepared, transaction.blob_decls, transaction.synced_tables,
            )?;
            if advanced.is_some() {
                transaction.authority.forget_superseded_replay_baseline();
                let routing_key = if transaction.gates.has_scoped_graph() {
                    let encryption = routing_encryption.ok_or_else(|| DbError::Message("scoped write rebase requires Store routing encryption".to_string()))?;
                    Some(coven_protocol::circle::derive_row_routing_key(encryption, root.store_root_hash)?)
                } else { None };
                if changes_publication_base {
                    transaction.rebase_unpublished_store_writes(accepted, routing_key.as_ref(), membership)?;
                }
                super::observed_store_publication::retire_store_publication_prefix_before_snapshot_on(transaction.store, transaction.authority, &accepted.publication)?;
            }
            Ok(StoreTransactionOutcome::Commit(advanced))
        })
    }

    fn store_package_is_retained_for_replay(
        &mut self,
        root: &coven_protocol::store_commit::StoreRootRef,
        target: &StorePackageRef,
        activation: &StoreBatchCommitRef,
    ) -> Result<bool, DbError> {
        let object_id = remote_object_id(&target.object);
        let exists: bool = self
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
        let remote = load_remote_object_on(self.conn, object_id)?;
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
                .validate_retained_materialization_by_ref_on(
                    crate::store::store_session::StoreRecords::new(self.conn, self.store_dir),
                    commit,
                )?;
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
        let remote = load_remote_object_on(self.conn, object_id)?;
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
                .validate_retained_materialization_by_ref_on(
                    crate::store::store_session::StoreRecords::new(self.conn, self.store_dir),
                    commit,
                )?;
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
        let conn = self.conn;
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
            self.conn,
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
        let conn = self.conn;
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

    /// Whether a pending audience-blob reclaim still names this package as the
    /// one that published its blob.
    ///
    /// Executing a blob reclaim re-reads that package from the provider to
    /// confirm the binding, so the package has to outlive the blob operation:
    /// a package reclaim that deleted it first would strand the blob operation
    /// at a read that can never succeed. Completed operations hold nothing; a
    /// stuck one holds its package like any other unfinished operation,
    /// because stuck means waiting on a person, not gone.
    fn package_is_retained_by_pending_blob_reclaim(
        &self,
        package: &coven_protocol::objects::ExactObjectRef,
    ) -> Result<bool, DbError> {
        let package_id = remote_object_id(package);
        Ok(self.store_reclaim_operations()?.iter().any(|operation| {
            if matches!(operation, DurableStoreReclaimOperation::Completed { .. }) {
                return false;
            }
            match operation.authorization().target() {
                coven_protocol::reclaim::ReclaimTarget::AudienceBlob(
                    coven_protocol::reclaim::AudienceBlobReclaimTarget::Circle { source, .. },
                ) => remote_object_id(&source.package.package.object) == package_id,
                _ => false,
            }
        }))
    }

    /// Every journalled reclaim operation paired with the error that made it
    /// stuck, if any. The three questions the journal answers — what exists,
    /// what a cycle may still run, and what is waiting on a person — all come
    /// off this one read.
    fn store_reclaim_journal(
        &self,
    ) -> Result<Vec<(DurableStoreReclaimOperation, Option<String>)>, DbError> {
        let mut statement = self
            .conn
            .prepare(
                "SELECT authorization_hash, state, stuck_error FROM store_reclaim_operations
                 ORDER BY authorization_hash",
            )
            .map_err(DbError::from)?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            })
            .map_err(DbError::from)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(DbError::from)?;
        rows.into_iter()
            .map(|(raw_id, raw, stuck_error)| {
                let id = raw_id
                    .parse()
                    .map_err(|error| DbError::context("Store reclaim operation id", error))?;
                Ok((parse_store_reclaim_operation(id, &raw)?, stuck_error))
            })
            .collect()
    }

    fn store_reclaim_operations(&self) -> Result<Vec<DurableStoreReclaimOperation>, DbError> {
        Ok(self
            .store_reclaim_journal()?
            .into_iter()
            .map(|(operation, _)| operation)
            .collect())
    }

    fn runnable_store_reclaim_operations(
        &self,
    ) -> Result<Vec<DurableStoreReclaimOperation>, DbError> {
        Ok(self
            .store_reclaim_journal()?
            .into_iter()
            .filter_map(|(operation, stuck_error)| stuck_error.is_none().then_some(operation))
            .collect())
    }

    fn stuck_reclaim_operations(&self) -> Result<Vec<StuckReclaimOperation>, DbError> {
        Ok(self
            .store_reclaim_journal()?
            .into_iter()
            .filter_map(|(operation, stuck_error)| {
                stuck_error.map(|error| StuckReclaimOperation {
                    operation_id: operation.operation_id(),
                    target: operation.authorization().target().clone(),
                    error,
                })
            })
            .collect())
    }

    fn mark_store_reclaim_operation_stuck(
        &mut self,
        operation_id: ObjectHash,
        error: String,
    ) -> Result<(), DbError> {
        crate::mark_store_reclaim_operation_stuck_on(self.conn, operation_id, &error)
    }

    fn retry_stuck_reclaim_operation(&mut self, operation_id: ObjectHash) -> Result<(), DbError> {
        crate::clear_store_reclaim_operation_stuck_on(self.conn, operation_id)
    }

    fn begin_store_reclaim_receipt(
        &mut self,
        expected: DurableStoreReclaimOperation,
        next: DurableStoreReclaimOperation,
        remotes: Vec<coven_protocol::remote_object::ClosedRemoteObject>,
    ) -> Result<DurableStoreReclaimOperation, DbError> {
        let tx = self.conn.unchecked_transaction().map_err(DbError::from)?;
        let current = load_store_reclaim_operation_on(&tx, expected.operation_id())?
            .ok_or_else(|| DbError::Message("Store reclaim operation disappeared".to_string()))?;
        if current != expected {
            return Err(DbError::Message(
                "Store reclaim operation changed before receipt preparation".to_string(),
            ));
        }
        let candidate = next.candidate().ok_or_else(|| {
            DbError::Message("Store reclaim receipt has no publication candidate".to_string())
        })?;
        let active_publication = ActiveStorePublication::for_commit(
            ActiveStorePublicationOwner::Reclaim(next.operation_id()),
            candidate,
        )?;
        match super::active_store_publication::claim_active_store_publication_on(
            &tx,
            &active_publication,
        )? {
            super::active_store_publication::ActiveStorePublicationClaim::Acquired => {}
            super::active_store_publication::ActiveStorePublicationClaim::AlreadyOwned => {
                return Err(DbError::Message(
                    "Store reclaim receipt owns publication before its journal".to_string(),
                ));
            }
            super::active_store_publication::ActiveStorePublicationClaim::Occupied(owner) => {
                return Err(DbError::Message(format!(
                    "another local Store operation owns publication: {owner:?}"
                )));
            }
        }
        for remote in &remotes {
            persist_exact_remote_object_on(
                &tx,
                self.store_dir,
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
        let tx = self.conn.unchecked_transaction().map_err(DbError::from)?;
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

    #[cfg(any(test, feature = "test-utils"))]
    fn stored_blob_has_snapshot_owner_for_test(
        &self,
        stored: &coven_protocol::blob::locator::StoredBlobRef,
    ) -> Result<bool, DbError> {
        let remote = load_remote_object_on(self.conn, remote_object_id(stored.object()))?;
        let pinned = remote.snapshot_owners().next().is_some();
        Ok(pinned)
    }
}

impl StoreDatabase {
    /// Whether adopting the snapshot would move the retained replay baseline or fold
    /// a settled write-journal prefix into it.
    pub async fn replay_baseline_would_advance(
        &self,
        snapshot: coven_protocol::store_commit::StoreSnapshotRef,
        cut: coven_protocol::store_commit::CommitFrontier,
    ) -> Result<bool, DbError> {
        self.call_store(move |session| {
            crate::store::store_session::replay_baseline_advances_on(
                crate::store::store_session::StoreRecords::new(session.conn, session.store_dir),
                &snapshot,
                &cut,
            )
        })
        .await
    }

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

    /// Adopt a snapshot admitted for replay retirement and retire the retained
    /// history it supersedes.
    ///
    /// `Ok(None)` means the snapshot does not advance this device's cut, which
    /// is the ordinary result once a device has caught up to the newest
    /// acknowledged snapshot.
    pub async fn advance_snapshot_replay_baseline(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        authority: crate::VerifiedStoreSnapshotAuthority,
        routing_encryption: Option<coven_keys::encryption::EncryptionService>,
        membership: coven_protocol::membership::LocalStoreMembership,
    ) -> Result<Option<crate::AdvancedReplayBaseline>, DbError> {
        let authority = authority.into_authority();
        self.call_store(move |session| {
            session.advance_snapshot_replay_baseline(
                &root,
                authority,
                routing_encryption.as_ref(),
                membership,
            )
        })
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

    /// Whether a pending audience-blob reclaim still names this package as the
    /// one that published its blob. See the session method.
    pub async fn package_is_retained_by_pending_blob_reclaim(
        &self,
        package: coven_protocol::objects::ExactObjectRef,
    ) -> Result<bool, DbError> {
        self.call_store(move |session| {
            session.package_is_retained_by_pending_blob_reclaim(&package)
        })
        .await
    }

    /// Every operation the reclaim journal holds, stuck ones included. An
    /// existing operation for a target is what blocks re-authorizing it, and a
    /// stuck operation blocks it exactly as a running one does.
    pub async fn store_reclaim_operations(
        &self,
    ) -> Result<Vec<DurableStoreReclaimOperation>, DbError> {
        self.call_store(|session| session.store_reclaim_operations())
            .await
    }

    /// The operations a cycle may still run: everything the journal holds
    /// except the ones waiting on a person.
    pub async fn runnable_store_reclaim_operations(
        &self,
    ) -> Result<Vec<DurableStoreReclaimOperation>, DbError> {
        self.call_store(|session| session.runnable_store_reclaim_operations())
            .await
    }

    /// The operations that failed with an error retrying cannot change, with
    /// the target and message the host shows.
    pub async fn stuck_reclaim_operations(&self) -> Result<Vec<StuckReclaimOperation>, DbError> {
        self.call_store(|session| session.stuck_reclaim_operations())
            .await
    }

    /// Mark one operation stuck, so every later cycle skips it until the host
    /// asks for it again.
    pub async fn mark_store_reclaim_operation_stuck(
        &self,
        operation_id: ObjectHash,
        error: String,
    ) -> Result<(), DbError> {
        self.call_store(move |session| {
            session.mark_store_reclaim_operation_stuck(operation_id, error)
        })
        .await
    }

    /// Clear one operation's stuck mark so the next cycle runs it again.
    /// Refused when the operation is not stuck.
    pub async fn retry_stuck_reclaim_operation(
        &self,
        operation_id: ObjectHash,
    ) -> Result<(), DbError> {
        self.call_store(move |session| session.retry_stuck_reclaim_operation(operation_id))
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

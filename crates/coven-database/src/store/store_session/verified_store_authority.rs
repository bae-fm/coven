use super::retained_merge_replay::CircleReplayEpochIndex;
use super::ReplayProjection;
use super::*;
use crate::BlobDecls;
use coven_protocol::store_commit::{
    ReferencedStoreDeviceRegistration, StoreDeviceRegistration, StoreDeviceRegistrationRef,
    StoreProtocolRoot, StoreRootRef,
};
use std::collections::BTreeMap;

#[path = "retained_merge_replay/cache.rs"]
mod cache;
use cache::RetainedReplayCache;

/// Immutable Store facts verified during this database connection's lifetime.
///
/// The retained replay entries are verified under the root and registrations
/// held here, so they share the authority's lifetime and cannot be paired with
/// authority from another open connection.
#[derive(Default)]
pub(crate) struct VerifiedStoreAuthority {
    root_authority: Option<(StoreRootRef, StoreProtocolRoot)>,
    registrations: BTreeMap<StoreDeviceRegistrationRef, StoreDeviceRegistration>,
    retained_replay: RetainedReplayCache,
    owner_anchor: Option<RetainedReplayGenesisAuthority>,
}

/// Verified authority staged beside one SQL transaction.
///
/// Root, registration, and retained replay reads start from the connection's
/// verified cache. Publishing every newly verified value back is infallible and
/// happens only after the SQL commit; dropping this value leaves the connection
/// cache unchanged.
pub(super) struct VerifiedStoreAuthorityTransaction {
    root: StoreRootRef,
    registrations: BTreeMap<StoreDeviceRegistrationRef, StoreDeviceRegistration>,
    cache: RetainedReplayCache,
}

pub(crate) trait VerifiedRegistrationLookup {
    fn activated_registration_on(
        &mut self,
        records: crate::store::store_session::StoreRecords<'_>,
        root: &StoreRootRef,
        reference: &StoreDeviceRegistrationRef,
    ) -> Result<StoreDeviceRegistration, DbError>;

    fn local_registration_on(
        &mut self,
        records: crate::store::store_session::StoreRecords<'_>,
        root: &StoreRootRef,
    ) -> Result<ReferencedStoreDeviceRegistration, DbError> {
        let reference = records.local_activated_registration_ref()?.ok_or_else(|| {
            DbError::Message("local Store device has no activated registration".to_string())
        })?;
        let registration = self.activated_registration_on(records, root, &reference)?;
        ReferencedStoreDeviceRegistration::verified(reference, registration).map_err(DbError::from)
    }
}

pub(super) fn verify_prepared_store_commit_on(
    lookup: &mut dyn VerifiedRegistrationLookup,
    records: crate::store::store_session::StoreRecords<'_>,
    root: &StoreRootRef,
    prepared: &super::publication_state::PreparedStoreWriteState,
) -> Result<coven_protocol::store_commit::VerifiedStoreBatchCommit, DbError> {
    let unverified: coven_protocol::store_commit::StoreBatchCommit =
        serde_json::from_slice(prepared.commit.semantic_bytes())
            .map_err(|error| DbError::context("prepared Store commit", error))?;
    let registration =
        lookup.activated_registration_on(records, root, &unverified.author_registration)?;
    let coord = coven_protocol::store_commit::StoreCommitCoord {
        stream_id: coven_protocol::store_commit::StreamActivation::device_authorized_stream_id(
            root.store_root_hash,
            &unverified.author_registration,
            coven_protocol::store_commit::StreamAnchorDomain::StoreAnnouncements,
        ),
        sequence: unverified.seq(),
    };
    coven_protocol::store_commit::VerifiedStoreBatchCommit::parse_prepared(
        prepared.commit.semantic_bytes(),
        root.store_root_hash,
        coord,
        prepared.commit.prepared().reference().clone(),
        &registration,
    )
    .map_err(|error| DbError::context("prepared Store commit", error))
}

pub(crate) trait VerifiedStoreLookup: VerifiedRegistrationLookup {
    fn retained_replay_object_coverage_on(
        &mut self,
        records: crate::store::store_session::StoreRecords<'_>,
    ) -> Result<super::retained_merge_replay::RetainedReplayObjectCoverage<'_>, DbError>;

    fn pending_device_join_retention_on(
        &mut self,
        records: crate::store::store_session::StoreRecords<'_>,
        root: &StoreRootRef,
        coverage: &coven_protocol::store_commit::CommitFrontier,
    ) -> Result<
        BTreeMap<
            coven_protocol::store_commit::AcceptedStoreSnapshotRef,
            std::collections::BTreeSet<coven_protocol::store_commit::StoreBatchCommitRef>,
        >,
        DbError,
    >;

    fn open_retained_materialization_on(
        &mut self,
        records: crate::store::store_session::StoreRecords<'_>,
        root: &StoreRootRef,
        input: &crate::store::materialization_models::RetainedMergeMaterializationInput,
        input_hash: coven_protocol::store_commit::ObjectHash,
        materialization: &crate::VerifiedMergeMaterialization<'_>,
    ) -> Result<OwnedVerifiedMergeMaterialization, DbError>;

    fn retained_materialization_by_ref_on(
        &mut self,
        records: crate::store::store_session::StoreRecords<'_>,
        reference: &coven_protocol::store_commit::StoreBatchCommitRef,
    ) -> Result<OwnedVerifiedMergeMaterialization, DbError>;
}

pub(super) struct CachedVerifiedRegistrations<'cache> {
    registrations: &'cache mut BTreeMap<StoreDeviceRegistrationRef, StoreDeviceRegistration>,
}

impl<'cache> CachedVerifiedRegistrations<'cache> {
    pub(super) fn new(
        registrations: &'cache mut BTreeMap<StoreDeviceRegistrationRef, StoreDeviceRegistration>,
    ) -> Self {
        Self { registrations }
    }
}

impl VerifiedRegistrationLookup for CachedVerifiedRegistrations<'_> {
    fn activated_registration_on(
        &mut self,
        records: crate::store::store_session::StoreRecords<'_>,
        root: &StoreRootRef,
        reference: &StoreDeviceRegistrationRef,
    ) -> Result<StoreDeviceRegistration, DbError> {
        VerifiedStoreAuthority::activated_registration_with_cache_on(
            self.registrations,
            records,
            root,
            reference,
        )
    }
}

impl VerifiedStoreAuthorityTransaction {
    pub(super) fn root(&self) -> &StoreRootRef {
        &self.root
    }

    pub(super) fn insert_verified(
        &mut self,
        materialization: OwnedVerifiedMergeMaterialization,
    ) -> Result<(), DbError> {
        self.cache.insert_verified(materialization)
    }

    pub(super) fn forget_superseded_replay_baseline(&mut self) {
        self.cache.forget_superseded_baseline();
    }

    pub(super) fn replace_installed_replay_baseline(
        &mut self,
        baseline: RetainedReplayBaseline,
    ) -> Result<(), DbError> {
        let baseline_root = match &baseline.authority {
            RetainedReplayAuthority::Genesis(authority) => &authority.store_root,
            RetainedReplayAuthority::InstalledSnapshot(authority) => &authority.store_root,
        };
        if baseline_root != &self.root {
            return Err(DbError::Message(
                "received replay baseline belongs to another Store root".into(),
            ));
        }
        self.cache.forget_superseded_baseline();
        self.cache.commit_installed_baseline(baseline);
        Ok(())
    }

    fn retained_materialization_by_ref_on(
        &mut self,
        records: crate::store::store_session::StoreRecords<'_>,
        reference: &coven_protocol::store_commit::StoreBatchCommitRef,
    ) -> Result<OwnedVerifiedMergeMaterialization, DbError> {
        let mut registrations = CachedVerifiedRegistrations::new(&mut self.registrations);
        self.cache
            .materialization_by_ref_on(records, &self.root, &mut registrations, reference)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn replay_projection_watching_on(
        &mut self,
        records: crate::store::store_session::StoreTransaction<'_, '_>,
        blob_decls: &BlobDecls,
        gates: &crate::Gates,
        synced_tables: &[coven_protocol::synced_schema::SyncedTable],
        routing_key: Option<&coven_protocol::circle::RowRoutingKey>,
        retracted: &std::collections::BTreeSet<coven_protocol::store_commit::StoreBatchCommitRef>,
        journal: crate::ReplayJournal<'_>,
        local_store_membership: coven_protocol::membership::LocalStoreMembership,
        watched: &coven_protocol::store_commit::StoreBatchCommitRef,
    ) -> Result<super::ReplayProjectionResult, DbError> {
        let mut registrations = CachedVerifiedRegistrations::new(&mut self.registrations);
        self.cache.replay_projection_watching_on(
            records,
            &self.root,
            &mut registrations,
            blob_decls,
            gates,
            synced_tables,
            routing_key,
            retracted,
            None,
            journal,
            local_store_membership,
            Some(watched),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn replay_projection_result_on(
        &mut self,
        records: crate::store::store_session::StoreTransaction<'_, '_>,
        blob_decls: &BlobDecls,
        gates: &crate::Gates,
        synced_tables: &[coven_protocol::synced_schema::SyncedTable],
        routing_key: Option<&coven_protocol::circle::RowRoutingKey>,
        history_cut: Option<&coven_protocol::store_commit::CommitFrontier>,
        journal: crate::ReplayJournal<'_>,
        local_store_membership: coven_protocol::membership::LocalStoreMembership,
    ) -> Result<super::ReplayProjectionResult, DbError> {
        let mut registrations = CachedVerifiedRegistrations::new(&mut self.registrations);
        self.cache.replay_projection_watching_on(
            records,
            &self.root,
            &mut registrations,
            blob_decls,
            gates,
            synced_tables,
            routing_key,
            &std::collections::BTreeSet::new(),
            history_cut,
            journal,
            local_store_membership,
            None,
        )
    }
}

impl VerifiedStoreAuthority {
    pub(super) fn for_replay_baseline(baseline: RetainedReplayBaseline) -> Self {
        let mut authority = Self::default();
        authority
            .retained_replay
            .commit_installed_baseline(baseline);
        authority
    }

    fn commit_installed_root(&mut self, reference: StoreRootRef, value: StoreProtocolRoot) {
        match &self.root_authority {
            Some(existing) => assert_eq!(
                existing,
                &(reference, value),
                "committed Store root conflicts with connection authority"
            ),
            None => self.root_authority = Some((reference, value)),
        }
    }

    fn commit_installed_registration(
        &mut self,
        reference: StoreDeviceRegistrationRef,
        value: StoreDeviceRegistration,
    ) {
        match self.registrations.entry(reference) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(value);
            }
            std::collections::btree_map::Entry::Occupied(entry) => assert_eq!(
                entry.get(),
                &value,
                "committed Store registration conflicts with connection authority"
            ),
        }
    }

    fn commit_installed_retained_replay_baseline(&mut self, baseline: RetainedReplayBaseline) {
        let baseline_root = match &baseline.authority {
            RetainedReplayAuthority::Genesis(authority) => &authority.store_root,
            RetainedReplayAuthority::InstalledSnapshot(authority) => &authority.store_root,
        };
        assert_eq!(
            self.root_authority.as_ref().map(|(reference, _)| reference),
            Some(baseline_root),
            "committed retained replay baseline belongs to another Store root"
        );
        self.retained_replay.commit_installed_baseline(baseline);
    }

    fn validate_owner_anchor_cache(
        &self,
        authority: &RetainedReplayGenesisAuthority,
    ) -> Result<(), DbError> {
        let (root, _) = self.root_authority.as_ref().ok_or_else(|| {
            DbError::Message("verified Store owner anchor has no Store root".to_string())
        })?;
        if root != &authority.store_root {
            return Err(DbError::Message(
                "verified Store owner anchor belongs to another Store root".to_string(),
            ));
        }
        if !self
            .registrations
            .contains_key(&authority.founder_registration)
        {
            return Err(DbError::Message(
                "verified Store owner anchor has no founder registration".to_string(),
            ));
        }
        self.retained_replay.validate_owner_anchor(authority)
    }

    pub(super) fn remember_verified_owner_anchor(
        &mut self,
        authority: RetainedReplayGenesisAuthority,
    ) -> Result<(), DbError> {
        self.validate_owner_anchor_cache(&authority)?;
        match &self.owner_anchor {
            Some(existing) if existing != &authority => {
                return Err(DbError::Message(
                    "verified Store owner anchor conflicts with connection authority".to_string(),
                ));
            }
            Some(_) => {}
            None => self.owner_anchor = Some(authority),
        }
        Ok(())
    }

    pub(super) fn commit_installed_owner_anchor(
        &mut self,
        authority: RetainedReplayGenesisAuthority,
        root: StoreProtocolRoot,
        founder: StoreDeviceRegistration,
        baseline: RetainedReplayBaseline,
    ) {
        self.commit_installed_root(authority.store_root.clone(), root);
        self.commit_installed_registration(authority.founder_registration.clone(), founder);
        self.commit_installed_retained_replay_baseline(baseline);
        self.remember_verified_owner_anchor(authority)
            .expect("committed Store owner anchor must match its connection authority");
    }

    pub(super) fn verified_prepared_store_commit_on(
        &mut self,
        records: crate::store::store_session::StoreRecords<'_>,
        prepared: &super::publication_state::PreparedStoreWriteState,
    ) -> Result<coven_protocol::store_commit::VerifiedStoreBatchCommit, DbError> {
        let root = self.required_root_authority_on(records)?;
        verify_prepared_store_commit_on(self, records, &root, prepared)
    }

    pub(super) fn reuses_owner_anchor(
        &self,
        anchor: &crate::StoreOwnerAnchor,
    ) -> Result<bool, DbError> {
        let Some(installed) = &self.owner_anchor else {
            return Ok(false);
        };
        if installed != anchor.authority() {
            return Err(DbError::Message(
                "Store owner anchor differs from connection authority".to_string(),
            ));
        }
        self.validate_owner_anchor_cache(installed)?;
        let (_, root) = self
            .root_authority
            .as_ref()
            .expect("validated Store owner anchor has a root");
        let founder = self
            .registrations
            .get(&installed.founder_registration)
            .expect("validated Store owner anchor has a founder registration");
        if root != &anchor.root().value || founder != &anchor.founder().value {
            return Err(DbError::Message(
                "Store owner anchor values differ from connection authority".to_string(),
            ));
        }
        Ok(true)
    }

    pub(super) fn begin_transaction_on(
        &mut self,
        records: crate::store::store_session::StoreRecords<'_>,
    ) -> Result<VerifiedStoreAuthorityTransaction, DbError> {
        let root = self.required_root_authority_on(records)?;
        Ok(VerifiedStoreAuthorityTransaction {
            root,
            registrations: self.registrations.clone(),
            cache: self.retained_replay.clone(),
        })
    }

    pub(super) fn commit_transaction(&mut self, transaction: VerifiedStoreAuthorityTransaction) {
        assert_eq!(
            self.root_authority.as_ref().map(|(reference, _)| reference),
            Some(&transaction.root),
            "verified authority transaction belongs to another Store root"
        );
        for (reference, registration) in transaction.registrations {
            match self.registrations.entry(reference) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(registration);
                }
                std::collections::btree_map::Entry::Occupied(entry) => assert_eq!(
                    entry.get(),
                    &registration,
                    "verified authority transaction found conflicting registration bytes"
                ),
            }
        }
        self.retained_replay = transaction.cache;
    }

    pub(super) fn root_authority_on(
        &mut self,
        records: crate::store::store_session::StoreRecords<'_>,
    ) -> Result<Option<(StoreRootRef, StoreProtocolRoot)>, DbError> {
        if self.root_authority.is_none() {
            self.root_authority = records.store_root_authority()?;
        }
        Ok(self.root_authority.clone())
    }

    pub(super) fn required_root_authority_on(
        &mut self,
        records: crate::store::store_session::StoreRecords<'_>,
    ) -> Result<StoreRootRef, DbError> {
        self.root_authority_on(records)?
            .map(|(reference, _)| reference)
            .ok_or(DbError::StoreRootHashMissing)
    }

    pub(super) fn activated_registration_on(
        &mut self,
        records: crate::store::store_session::StoreRecords<'_>,
        root: &StoreRootRef,
        reference: &StoreDeviceRegistrationRef,
    ) -> Result<StoreDeviceRegistration, DbError> {
        Self::activated_registration_with_cache_on(
            &mut self.registrations,
            records,
            root,
            reference,
        )
    }

    pub(super) fn activated_registration_with_cache_on(
        registrations: &mut BTreeMap<StoreDeviceRegistrationRef, StoreDeviceRegistration>,
        records: crate::store::store_session::StoreRecords<'_>,
        root: &StoreRootRef,
        reference: &StoreDeviceRegistrationRef,
    ) -> Result<StoreDeviceRegistration, DbError> {
        if let Some(registration) = registrations.get(reference) {
            return Ok(registration.clone());
        }
        let registration = records.activated_registration(root, reference)?;
        registrations.insert(reference.clone(), registration.clone());
        Ok(registration)
    }

    pub(super) fn local_store_authority_on(
        &mut self,
        records: crate::store::store_session::StoreRecords<'_>,
    ) -> Result<ReferencedStoreDeviceRegistration, DbError> {
        let root = self.required_root_authority_on(records)?;
        self.local_registration_on(records, &root)
    }

    /// Drop replay state derived from a baseline this session just advanced.
    ///
    /// Both halves of the cache — the baseline and the verified
    /// materializations read against it — describe history the advance retired.
    pub(crate) fn forget_superseded_replay_baseline(&mut self) {
        self.retained_replay.forget_superseded_baseline();
    }

    pub(super) fn retained_replay_baseline_on(
        &mut self,
        records: crate::store::store_session::StoreRecords<'_>,
    ) -> Result<&RetainedReplayBaseline, DbError> {
        let root = self.required_root_authority_on(records)?;
        let baseline = self.retained_replay.baseline_on(records)?;
        let baseline_root = match &baseline.authority {
            RetainedReplayAuthority::Genesis(authority) => &authority.store_root,
            RetainedReplayAuthority::InstalledSnapshot(authority) => &authority.store_root,
        };
        if baseline_root != &root {
            return Err(DbError::Message(
                "retained replay baseline belongs to another Store root".to_string(),
            ));
        }
        Ok(baseline)
    }

    pub(super) fn retained_replay_inputs_on(
        &mut self,
        records: crate::store::store_session::StoreRecords<'_>,
        expected_root: &StoreRootRef,
    ) -> Result<Vec<OwnedVerifiedMergeMaterialization>, DbError> {
        let root = self.required_root_authority_on(records)?;
        if &root != expected_root {
            return Err(DbError::Message(
                "retained replay request belongs to another Store root".to_string(),
            ));
        }
        let mut registrations = CachedVerifiedRegistrations::new(&mut self.registrations);
        self.retained_replay
            .replay_inputs_on(records, &root, &mut registrations)
    }

    pub(super) fn retained_history_checkpoint_on(
        &mut self,
        records: crate::store::store_session::StoreRecords<'_>,
        reference: &coven_protocol::store_commit::StoreBatchCommitRef,
    ) -> Result<RetainedMergeHistoryCheckpoint, DbError> {
        self.retained_materialization_by_ref_on(records, reference)?;
        let mut registrations = CachedVerifiedRegistrations::new(&mut self.registrations);
        self.retained_replay
            .retained_history_checkpoint_on(records, &mut registrations, reference)
    }

    pub(super) fn retained_materialization_by_ref_on(
        &mut self,
        records: crate::store::store_session::StoreRecords<'_>,
        reference: &coven_protocol::store_commit::StoreBatchCommitRef,
    ) -> Result<OwnedVerifiedMergeMaterialization, DbError> {
        let root = self.required_root_authority_on(records)?;
        let mut registrations = CachedVerifiedRegistrations::new(&mut self.registrations);
        self.retained_replay.materialization_by_ref_on(
            records,
            &root,
            &mut registrations,
            reference,
        )
    }

    pub(super) fn validate_retained_materialization_by_ref_on(
        &mut self,
        records: crate::store::store_session::StoreRecords<'_>,
        reference: &coven_protocol::store_commit::StoreBatchCommitRef,
    ) -> Result<OwnedVerifiedMergeMaterialization, DbError> {
        let root = self.required_root_authority_on(records)?;
        let mut registrations = CachedVerifiedRegistrations::new(&mut self.registrations);
        StoreDatabase::load_retained_merge_materialization_by_ref_on(
            records,
            &root,
            &mut registrations,
            reference,
        )
    }

    pub(super) fn verified_circle_activation_on(
        &self,
        records: crate::store::store_session::StoreRecords<'_>,
        circle_id: coven_protocol::circle::CircleId,
        control: &coven_protocol::circle::CircleControlCoord,
    ) -> Result<Option<coven_protocol::circle_activation::VerifiedCircleReference>, DbError> {
        self.retained_replay
            .verified_circle_activation_on(records, circle_id, control)
    }

    pub(super) fn circle_replay_epoch_index_on(
        &self,
        records: crate::store::store_session::StoreRecords<'_>,
    ) -> Result<CircleReplayEpochIndex, DbError> {
        self.retained_replay.circle_replay_epoch_index_on(records)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn replay_projection_for_root_on(
        &mut self,
        records: crate::store::store_session::StoreTransaction<'_, '_>,
        root: &StoreRootRef,
        blob_decls: &BlobDecls,
        gates: &crate::Gates,
        synced_tables: &[coven_protocol::synced_schema::SyncedTable],
        routing_key: Option<&coven_protocol::circle::RowRoutingKey>,
        retracted: &std::collections::BTreeSet<coven_protocol::store_commit::StoreBatchCommitRef>,
        history_cut: Option<&coven_protocol::store_commit::CommitFrontier>,
        journal: crate::ReplayJournal<'_>,
        local_store_membership: coven_protocol::membership::LocalStoreMembership,
    ) -> Result<super::ReplayProjectionResult, DbError> {
        let mut registrations = CachedVerifiedRegistrations::new(&mut self.registrations);
        self.retained_replay.replay_projection_on(
            records,
            root,
            &mut registrations,
            blob_decls,
            gates,
            synced_tables,
            routing_key,
            retracted,
            history_cut,
            journal,
            local_store_membership,
        )
    }
}

impl VerifiedRegistrationLookup for VerifiedStoreAuthority {
    fn activated_registration_on(
        &mut self,
        records: crate::store::store_session::StoreRecords<'_>,
        root: &StoreRootRef,
        reference: &StoreDeviceRegistrationRef,
    ) -> Result<StoreDeviceRegistration, DbError> {
        VerifiedStoreAuthority::activated_registration_on(self, records, root, reference)
    }
}

impl VerifiedStoreLookup for VerifiedStoreAuthority {
    fn retained_replay_object_coverage_on(
        &mut self,
        records: crate::store::store_session::StoreRecords<'_>,
    ) -> Result<super::retained_merge_replay::RetainedReplayObjectCoverage<'_>, DbError> {
        Ok(
            super::retained_merge_replay::RetainedReplayObjectCoverage::from_baseline(Some(
                self.retained_replay_baseline_on(records)?,
            )),
        )
    }

    fn pending_device_join_retention_on(
        &mut self,
        records: crate::store::store_session::StoreRecords<'_>,
        root: &StoreRootRef,
        coverage: &coven_protocol::store_commit::CommitFrontier,
    ) -> Result<
        BTreeMap<
            coven_protocol::store_commit::AcceptedStoreSnapshotRef,
            std::collections::BTreeSet<coven_protocol::store_commit::StoreBatchCommitRef>,
        >,
        DbError,
    > {
        let baseline = self.retained_replay_baseline_on(records)?.clone();
        StoreDatabase::pending_device_join_retention_on(records, self, root, coverage, &baseline)
    }

    fn open_retained_materialization_on(
        &mut self,
        records: crate::store::store_session::StoreRecords<'_>,
        root: &StoreRootRef,
        input: &crate::store::materialization_models::RetainedMergeMaterializationInput,
        input_hash: coven_protocol::store_commit::ObjectHash,
        materialization: &crate::VerifiedMergeMaterialization<'_>,
    ) -> Result<OwnedVerifiedMergeMaterialization, DbError> {
        if self.required_root_authority_on(records)? != *root {
            return Err(DbError::Message(
                "retained materialization belongs to another Store root".to_string(),
            ));
        }
        let mut registrations = CachedVerifiedRegistrations::new(&mut self.registrations);
        StoreDatabase::open_retained_merge_materialization_input_with_verified_materialization_on(
            records,
            root,
            &mut registrations,
            materialization.commit_ref(),
            input,
            input_hash,
            materialization,
        )
    }

    fn retained_materialization_by_ref_on(
        &mut self,
        records: crate::store::store_session::StoreRecords<'_>,
        reference: &coven_protocol::store_commit::StoreBatchCommitRef,
    ) -> Result<OwnedVerifiedMergeMaterialization, DbError> {
        VerifiedStoreAuthority::retained_materialization_by_ref_on(self, records, reference)
    }
}

impl crate::store::store_session::StoreTransaction<'_, '_> {
    pub(super) fn begin_verified_authority_transaction(
        self,
        authority: &mut VerifiedStoreAuthority,
    ) -> Result<VerifiedStoreAuthorityTransaction, DbError> {
        authority.begin_transaction_on(crate::store::store_session::StoreRecords::new(
            self.transaction,
            self.store_dir,
        ))
    }

    pub(super) fn required_root_authority(
        self,
        authority: &mut VerifiedStoreAuthority,
    ) -> Result<StoreRootRef, DbError> {
        authority.required_root_authority_on(crate::store::store_session::StoreRecords::new(
            self.transaction,
            self.store_dir,
        ))
    }

    pub(super) fn root_authority(
        self,
        authority: &mut VerifiedStoreAuthority,
    ) -> Result<Option<(StoreRootRef, StoreProtocolRoot)>, DbError> {
        authority.root_authority_on(crate::store::store_session::StoreRecords::new(
            self.transaction,
            self.store_dir,
        ))
    }

    pub(super) fn activated_registration(
        self,
        authority: &mut VerifiedStoreAuthority,
        root: &StoreRootRef,
        reference: &StoreDeviceRegistrationRef,
    ) -> Result<StoreDeviceRegistration, DbError> {
        authority.activated_registration_on(
            crate::store::store_session::StoreRecords::new(self.transaction, self.store_dir),
            root,
            reference,
        )
    }

    pub(super) fn retained_replay_baseline(
        self,
        authority: &mut VerifiedStoreAuthority,
    ) -> Result<&RetainedReplayBaseline, DbError> {
        authority.retained_replay_baseline_on(crate::store::store_session::StoreRecords::new(
            self.transaction,
            self.store_dir,
        ))
    }
}

impl VerifiedRegistrationLookup for VerifiedStoreAuthorityTransaction {
    fn activated_registration_on(
        &mut self,
        records: crate::store::store_session::StoreRecords<'_>,
        root: &StoreRootRef,
        reference: &StoreDeviceRegistrationRef,
    ) -> Result<StoreDeviceRegistration, DbError> {
        if root != &self.root {
            return Err(DbError::Message(
                "retained replay lookup belongs to another Store root".to_string(),
            ));
        }
        VerifiedStoreAuthority::activated_registration_with_cache_on(
            &mut self.registrations,
            records,
            root,
            reference,
        )
    }
}

impl VerifiedStoreLookup for VerifiedStoreAuthorityTransaction {
    fn retained_replay_object_coverage_on(
        &mut self,
        records: crate::store::store_session::StoreRecords<'_>,
    ) -> Result<super::retained_merge_replay::RetainedReplayObjectCoverage<'_>, DbError> {
        Ok(
            super::retained_merge_replay::RetainedReplayObjectCoverage::from_baseline(Some(
                self.cache.baseline_on(records)?,
            )),
        )
    }

    fn pending_device_join_retention_on(
        &mut self,
        records: crate::store::store_session::StoreRecords<'_>,
        root: &StoreRootRef,
        coverage: &coven_protocol::store_commit::CommitFrontier,
    ) -> Result<
        BTreeMap<
            coven_protocol::store_commit::AcceptedStoreSnapshotRef,
            std::collections::BTreeSet<coven_protocol::store_commit::StoreBatchCommitRef>,
        >,
        DbError,
    > {
        let baseline = self.cache.baseline_on(records)?.clone();
        StoreDatabase::pending_device_join_retention_on(records, self, root, coverage, &baseline)
    }

    fn open_retained_materialization_on(
        &mut self,
        records: crate::store::store_session::StoreRecords<'_>,
        root: &StoreRootRef,
        input: &crate::store::materialization_models::RetainedMergeMaterializationInput,
        input_hash: coven_protocol::store_commit::ObjectHash,
        materialization: &crate::VerifiedMergeMaterialization<'_>,
    ) -> Result<OwnedVerifiedMergeMaterialization, DbError> {
        if &self.root != root {
            return Err(DbError::Message(
                "retained materialization belongs to another Store root".to_string(),
            ));
        }
        let mut registrations = CachedVerifiedRegistrations::new(&mut self.registrations);
        StoreDatabase::open_retained_merge_materialization_input_with_verified_materialization_on(
            records,
            root,
            &mut registrations,
            materialization.commit_ref(),
            input,
            input_hash,
            materialization,
        )
    }

    fn retained_materialization_by_ref_on(
        &mut self,
        records: crate::store::store_session::StoreRecords<'_>,
        reference: &coven_protocol::store_commit::StoreBatchCommitRef,
    ) -> Result<OwnedVerifiedMergeMaterialization, DbError> {
        VerifiedStoreAuthorityTransaction::retained_materialization_by_ref_on(
            self, records, reference,
        )
    }
}

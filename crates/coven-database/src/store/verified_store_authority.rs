use super::candidate_records::PreparedMergeCandidate;
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
        records: crate::store::StoreRecords<'_>,
        root: &StoreRootRef,
        reference: &StoreDeviceRegistrationRef,
    ) -> Result<StoreDeviceRegistration, DbError>;
}

pub(crate) trait VerifiedStoreLookup: VerifiedRegistrationLookup {
    fn retained_materialization_by_ref_on(
        &mut self,
        records: crate::store::StoreRecords<'_>,
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
        records: crate::store::StoreRecords<'_>,
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

    pub(super) fn replay_inputs_on(
        &mut self,
        records: crate::store::StoreRecords<'_>,
    ) -> Result<Vec<OwnedVerifiedMergeMaterialization>, DbError> {
        let mut registrations = CachedVerifiedRegistrations::new(&mut self.registrations);
        self.cache
            .replay_inputs_on(records, &self.root, &mut registrations)
    }

    fn retained_materialization_by_ref_on(
        &mut self,
        records: crate::store::StoreRecords<'_>,
        reference: &coven_protocol::store_commit::StoreBatchCommitRef,
    ) -> Result<OwnedVerifiedMergeMaterialization, DbError> {
        if let Some(materialization) = self.cache.cached_by_ref(reference)? {
            return Ok(materialization.clone());
        }
        let mut registrations = CachedVerifiedRegistrations::new(&mut self.registrations);
        let materialization = StoreDatabase::load_retained_merge_materialization_by_ref_on(
            records,
            &self.root,
            &mut registrations,
            reference,
        )?;
        self.cache.insert_verified(materialization.clone())?;
        Ok(materialization)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn replay_projection_on(
        &mut self,
        records: crate::store::StoreRecordTransaction<'_, '_>,
        blob_decls: &BlobDecls,
        gates: &crate::Gates,
        synced_tables: &[coven_protocol::synced_schema::SyncedTable],
        routing_key: Option<&coven_protocol::circle::RowRoutingKey>,
        retracted: &std::collections::BTreeSet<coven_protocol::store_commit::StoreBatchCommitRef>,
        history_cut: Option<&coven_protocol::store_commit::CommitFrontier>,
        include_local_write_overlays: bool,
        local_store_membership: coven_protocol::membership::LocalStoreMembership,
    ) -> Result<ReplayProjection, DbError> {
        let mut registrations = CachedVerifiedRegistrations::new(&mut self.registrations);
        self.cache.replay_projection_on(
            records,
            &self.root,
            &mut registrations,
            blob_decls,
            gates,
            synced_tables,
            routing_key,
            retracted,
            history_cut,
            include_local_write_overlays,
            local_store_membership,
        )
    }
}

impl VerifiedStoreAuthority {
    pub(super) fn commit_installed_root(
        &mut self,
        reference: StoreRootRef,
        value: StoreProtocolRoot,
    ) {
        match &self.root_authority {
            Some(existing) => assert_eq!(
                existing,
                &(reference, value),
                "committed Store root conflicts with connection authority"
            ),
            None => self.root_authority = Some((reference, value)),
        }
    }

    pub(super) fn commit_installed_registration(
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

    pub(super) fn prepared_merge_candidate_on(
        &mut self,
        records: crate::store::StoreRecords<'_>,
        prepared: &super::publication_state::PreparedStoreWriteState,
    ) -> Result<PreparedMergeCandidate, DbError> {
        let (commit, head) = super::candidate_records::prepared_merge_candidate_objects(prepared);
        self.prepared_merge_candidate_parts_on(
            records,
            commit.semantic_bytes(),
            commit.prepared().reference(),
            head.semantic_bytes(),
            head.prepared().reference(),
        )
    }

    pub(super) fn prepared_merge_candidate_parts_on(
        &mut self,
        records: crate::store::StoreRecords<'_>,
        commit_bytes: &[u8],
        commit_object: &coven_protocol::objects::ExactObjectRef,
        head_bytes: &[u8],
        head_object: &coven_protocol::objects::ExactObjectRef,
    ) -> Result<PreparedMergeCandidate, DbError> {
        let root = self.required_root_authority_on(records)?;
        let unverified: coven_protocol::store_commit::StoreBatchCommit =
            serde_json::from_slice(commit_bytes)
                .map_err(|error| DbError::context("signed Merge candidate", error))?;
        let registration =
            self.activated_registration_on(records, &root, &unverified.author_registration)?;
        super::candidate_records::verify_prepared_merge_candidate_parts(
            &root,
            unverified,
            &registration,
            commit_bytes,
            commit_object,
            head_bytes,
            head_object,
        )
    }

    pub(super) fn begin_transaction_on(
        &mut self,
        records: crate::store::StoreRecords<'_>,
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
        records: crate::store::StoreRecords<'_>,
    ) -> Result<Option<(StoreRootRef, StoreProtocolRoot)>, DbError> {
        if self.root_authority.is_none() {
            self.root_authority = records.store_root_authority()?;
        }
        Ok(self.root_authority.clone())
    }

    pub(super) fn required_root_authority_on(
        &mut self,
        records: crate::store::StoreRecords<'_>,
    ) -> Result<StoreRootRef, DbError> {
        self.root_authority_on(records)?
            .map(|(reference, _)| reference)
            .ok_or(DbError::StoreRootHashMissing)
    }

    pub(super) fn activated_registration_on(
        &mut self,
        records: crate::store::StoreRecords<'_>,
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
        records: crate::store::StoreRecords<'_>,
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
        records: crate::store::StoreRecords<'_>,
    ) -> Result<ReferencedStoreDeviceRegistration, DbError> {
        let root = self.required_root_authority_on(records)?;
        let reference = records.local_activated_registration_ref()?.ok_or_else(|| {
            DbError::Message("local Store device has no activated registration".to_string())
        })?;
        let registration = self.activated_registration_on(records, &root, &reference)?;
        ReferencedStoreDeviceRegistration::verified(reference, registration)
            .map_err(|error| DbError::Message(error.to_string()))
    }

    pub(super) fn local_merge_stream_id_on(
        &mut self,
        records: crate::store::StoreRecords<'_>,
    ) -> Result<Option<String>, DbError> {
        if !records.has_local_device()? {
            return Ok(None);
        }
        let registration = self.local_store_authority_on(records)?;
        Ok(Some(
            coven_protocol::store_commit::StreamActivation::device_authorized_stream_id(
                registration.value().store_root.store_root_hash,
                registration.reference(),
                coven_protocol::store_commit::StreamAnchorDomain::StoreAnnouncements,
            )
            .to_string(),
        ))
    }

    pub(super) fn retained_replay_baseline_on(
        &mut self,
        records: crate::store::StoreRecords<'_>,
    ) -> Result<&RetainedReplayBaseline, DbError> {
        let root = self.required_root_authority_on(records)?;
        let baseline = self.retained_replay.baseline_on(records)?;
        let baseline_root = match &baseline.authority {
            RetainedReplayAuthority::Genesis(authority) => &authority.store_root,
            RetainedReplayAuthority::StableSnapshot(authority) => &authority.store_root,
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
        records: crate::store::StoreRecords<'_>,
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

    pub(super) fn retained_replay_inputs_with_verified_commits_on(
        &mut self,
        records: crate::store::StoreRecords<'_>,
        expected_root: &StoreRootRef,
        verified: &BTreeMap<
            coven_protocol::store_commit::StoreBatchCommitRef,
            coven_protocol::store_commit::VerifiedStoreBatchCommit,
        >,
    ) -> Result<Vec<OwnedVerifiedMergeMaterialization>, DbError> {
        let root = self.required_root_authority_on(records)?;
        if &root != expected_root {
            return Err(DbError::Message(
                "retained replay request belongs to another Store root".to_string(),
            ));
        }
        let mut registrations = CachedVerifiedRegistrations::new(&mut self.registrations);
        self.retained_replay.replay_inputs_with_verified_commits_on(
            records,
            &root,
            &mut registrations,
            verified,
        )
    }

    pub(super) fn retained_history_checkpoint_on(
        &mut self,
        records: crate::store::StoreRecords<'_>,
        reference: &coven_protocol::store_commit::StoreBatchCommitRef,
    ) -> Result<RetainedMergeHistoryCheckpoint, DbError> {
        self.retained_materialization_by_ref_on(records, reference)?;
        let mut registrations = CachedVerifiedRegistrations::new(&mut self.registrations);
        self.retained_replay
            .retained_history_checkpoint_on(records, &mut registrations, reference)
    }

    pub(super) fn retained_materialization_by_ref_on(
        &mut self,
        records: crate::store::StoreRecords<'_>,
        reference: &coven_protocol::store_commit::StoreBatchCommitRef,
    ) -> Result<OwnedVerifiedMergeMaterialization, DbError> {
        if let Some(materialization) = self.retained_replay.cached_by_ref(reference)? {
            return Ok(materialization.clone());
        }
        let root = self.required_root_authority_on(records)?;
        let mut registrations = CachedVerifiedRegistrations::new(&mut self.registrations);
        let materialization = StoreDatabase::load_retained_merge_materialization_by_ref_on(
            records,
            &root,
            &mut registrations,
            reference,
        )?;
        self.retained_replay
            .insert_verified(materialization.clone())?;
        Ok(materialization)
    }

    pub(super) fn validate_retained_materialization_by_ref_on(
        &mut self,
        records: crate::store::StoreRecords<'_>,
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

    pub(super) fn validate_retained_materialization_insert(
        &self,
        materialization: &OwnedVerifiedMergeMaterialization,
    ) -> Result<(), DbError> {
        self.retained_replay
            .validate_insert_verified(materialization)
    }

    pub(super) fn insert_retained_materialization(
        &mut self,
        materialization: OwnedVerifiedMergeMaterialization,
    ) -> Result<(), DbError> {
        self.retained_replay.insert_verified(materialization)
    }

    pub(super) fn verified_circle_activation_on(
        &self,
        records: crate::store::StoreRecords<'_>,
        circle_id: coven_protocol::circle::CircleId,
        control: &coven_protocol::circle::CircleControlCoord,
    ) -> Result<Option<coven_protocol::circle_activation::VerifiedCircleReference>, DbError> {
        self.retained_replay
            .verified_circle_activation_on(records, circle_id, control)
    }

    pub(super) fn circle_replay_epoch_index_on(
        &self,
        records: crate::store::StoreRecords<'_>,
    ) -> Result<CircleReplayEpochIndex, DbError> {
        self.retained_replay.circle_replay_epoch_index_on(records)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn replay_projection_for_root_on(
        &mut self,
        records: crate::store::StoreRecordTransaction<'_, '_>,
        root: &StoreRootRef,
        blob_decls: &BlobDecls,
        gates: &crate::Gates,
        synced_tables: &[coven_protocol::synced_schema::SyncedTable],
        routing_key: Option<&coven_protocol::circle::RowRoutingKey>,
        retracted: &std::collections::BTreeSet<coven_protocol::store_commit::StoreBatchCommitRef>,
        history_cut: Option<&coven_protocol::store_commit::CommitFrontier>,
        include_local_write_overlays: bool,
        local_store_membership: coven_protocol::membership::LocalStoreMembership,
    ) -> Result<ReplayProjection, DbError> {
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
            include_local_write_overlays,
            local_store_membership,
        )
    }
}

impl VerifiedRegistrationLookup for VerifiedStoreAuthority {
    fn activated_registration_on(
        &mut self,
        records: crate::store::StoreRecords<'_>,
        root: &StoreRootRef,
        reference: &StoreDeviceRegistrationRef,
    ) -> Result<StoreDeviceRegistration, DbError> {
        VerifiedStoreAuthority::activated_registration_on(self, records, root, reference)
    }
}

impl VerifiedStoreLookup for VerifiedStoreAuthority {
    fn retained_materialization_by_ref_on(
        &mut self,
        records: crate::store::StoreRecords<'_>,
        reference: &coven_protocol::store_commit::StoreBatchCommitRef,
    ) -> Result<OwnedVerifiedMergeMaterialization, DbError> {
        VerifiedStoreAuthority::retained_materialization_by_ref_on(self, records, reference)
    }
}

impl VerifiedRegistrationLookup for VerifiedStoreAuthorityTransaction {
    fn activated_registration_on(
        &mut self,
        records: crate::store::StoreRecords<'_>,
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
    fn retained_materialization_by_ref_on(
        &mut self,
        records: crate::store::StoreRecords<'_>,
        reference: &coven_protocol::store_commit::StoreBatchCommitRef,
    ) -> Result<OwnedVerifiedMergeMaterialization, DbError> {
        VerifiedStoreAuthorityTransaction::retained_materialization_by_ref_on(
            self, records, reference,
        )
    }
}

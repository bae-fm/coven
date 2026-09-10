use super::{AcceptedStoreCommitEvidence, StoreCommitAcceptance};
use crate::store::materialization_models::{
    RetainedAudiencePackage, RetainedMergeMaterializationInput,
};
use crate::store::store_session::retained_merge_replay::RetainedCommitAuthority;
use crate::store::store_session::verified_store_authority::VerifiedRegistrationLookup;
use crate::store::store_session::StoreRecords;
use crate::{
    Database, DbError, OwnedVerifiedMergeMaterialization, RetainedReplayAuthority,
    RetainedReplayBaseline, StoreDatabase,
};
use coven_protocol::circle_activation::VerifiedCircleActivations;
use coven_protocol::remote_object::RetainedReplayOwner;
use coven_protocol::store_commit::{
    ObjectHash, StoreBatchCommit, StoreBatchCommitRef, StoreDeviceRegistrationRef,
};

impl StoreRecords<'_> {
    pub(crate) fn open_retained_merge_materialization(
        self,
        root: &coven_protocol::store_commit::StoreRootRef,
        registrations: &mut dyn VerifiedRegistrationLookup,
        stream_id: &str,
        sequence: u64,
        commit_ref: &StoreBatchCommitRef,
        expected_input_hash: &str,
        baseline: Option<&RetainedReplayBaseline>,
    ) -> Result<OwnedVerifiedMergeMaterialization, DbError> {
        let sequence_sql = Database::sequence_to_sqlite(stream_id, sequence)?;
        let (stored_ref, stored_hash, canonical_input) =
            self.retained_materialization_row(stream_id, sequence_sql)?;
        let expected_ref = serde_json::to_string(commit_ref)
            .map_err(|error| DbError::context("serialize materialized Merge commit ref", error))?;
        if stored_ref != expected_ref {
            return Err(DbError::Message(format!(
                "retained Merge coordinate {stream_id}/{sequence} names another commit"
            )));
        }
        if stored_hash != expected_input_hash
            || stored_hash != ObjectHash::digest(&canonical_input).to_string()
        {
            return Err(DbError::Message(format!(
                "retained Merge coordinate {stream_id}/{sequence} input hash differs from its bytes"
            )));
        }
        let input: RetainedMergeMaterializationInput = serde_json::from_slice(&canonical_input)
            .map_err(|error| DbError::context("retained Merge materialization input", error))?;
        if serde_json::to_vec(&input)
            .map_err(|error| DbError::context("serialize retained Merge materialization", error))?
            != canonical_input
        {
            return Err(DbError::Message(
                "retained Merge materialization input is not canonical".to_string(),
            ));
        }
        let input_hash = stored_hash.parse().map_err(|error| {
            DbError::context(
                format!("retained Merge coordinate {stream_id}/{sequence} input hash is invalid"),
                error,
            )
        })?;
        let acceptance = match baseline {
            Some(RetainedReplayBaseline {
                authority: RetainedReplayAuthority::InstalledSnapshot(snapshot),
                ..
            }) if snapshot.metadata.coverage.covers_commit(commit_ref) => {
                snapshot.validate()?;
                if snapshot.store_root != *root {
                    return Err(DbError::Message(
                        "retained Store commit differs from its installed snapshot coverage".into(),
                    ));
                }
                // The exact input above came from the installed retained row. Its
                // acceptance survives compaction even when the bounded causal cut
                // no longer needs this historical coordinate.
                match snapshot
                    .metadata
                    .history_summary
                    .pending_device_join_accepted_commit(commit_ref)?
                {
                    Some(exact) => {
                        Some(super::AcceptedStoreCommitPublication::from_verified(exact).into())
                    }
                    None => Some(AcceptedStoreCommitEvidence {
                        acceptance: StoreCommitAcceptance::SnapshotCovered {
                            commit: commit_ref.clone(),
                            snapshot: snapshot.snapshot.clone(),
                        },
                    }),
                }
            }
            _ => None,
        };
        let verified = StoreDatabase::open_retained_merge_materialization_input_with_authority_on(
            self,
            root,
            registrations,
            commit_ref,
            &input,
            input_hash,
            RetainedCommitAuthority::StoredBytes(acceptance),
        )?;
        self.validate_retained_merge_pin_closure(
            &input,
            &RetainedReplayOwner::Commit {
                commit: commit_ref.clone(),
                input_hash,
            },
            &crate::store::retained_merge_replay::RetainedReplayObjectCoverage::from_baseline(
                baseline,
            ),
        )?;
        Ok(verified)
    }
}

impl StoreDatabase {
    pub(crate) fn open_retained_merge_materialization_input_with_verified_materialization_on(
        records: StoreRecords<'_>,
        root: &coven_protocol::store_commit::StoreRootRef,
        registration_lookup: &mut dyn VerifiedRegistrationLookup,
        commit_ref: &StoreBatchCommitRef,
        input: &RetainedMergeMaterializationInput,
        input_hash: ObjectHash,
        materialization: &crate::VerifiedMergeMaterialization<'_>,
    ) -> Result<OwnedVerifiedMergeMaterialization, DbError> {
        Self::open_retained_merge_materialization_input_with_authority_on(
            records,
            root,
            registration_lookup,
            commit_ref,
            input,
            input_hash,
            RetainedCommitAuthority::Operation(materialization),
        )
    }

    fn open_retained_merge_materialization_input_with_authority_on(
        records: StoreRecords<'_>,
        root: &coven_protocol::store_commit::StoreRootRef,
        registration_lookup: &mut dyn VerifiedRegistrationLookup,
        commit_ref: &StoreBatchCommitRef,
        input: &RetainedMergeMaterializationInput,
        input_hash: ObjectHash,
        authority: RetainedCommitAuthority<'_, '_>,
    ) -> Result<OwnedVerifiedMergeMaterialization, DbError> {
        let sequence = &commit_ref.coord.sequence;
        if sequence == &0 {
            return Err(DbError::Message(
                "retained Merge input names sequence zero".to_string(),
            ));
        }
        let unverified: StoreBatchCommit = serde_json::from_slice(input.commit.stored_bytes())
            .map_err(|error| DbError::context("retained Merge commit", error))?;
        let registrations = input
            .activation
            .registrations
            .verify_for(root, &unverified)
            .map_err(DbError::from)?;
        let introduced_registration = |reference: &StoreDeviceRegistrationRef| {
            let mut matches = unverified
                .device_registrations()
                .iter()
                .zip(&registrations)
                .filter(|(activated, _)| &activated.registration == reference)
                .map(|(_, registration)| registration.value());
            let registration = matches.next();
            if matches.next().is_some() {
                return Err(DbError::Message(
                    "retained Merge input introduces one registration more than once".to_string(),
                ));
            }
            Ok(registration)
        };
        let introduced_author = introduced_registration(&unverified.author_registration)?;
        let operation_author = match &authority {
            RetainedCommitAuthority::Operation(verified)
                if verified.root() == root
                    && verified.commit_ref() == commit_ref
                    && verified.commit().author_registration == unverified.author_registration
                    && verified.commit().to_bytes() == input.commit.stored_bytes() =>
            {
                Some(verified.verified_commit().author())
            }
            RetainedCommitAuthority::Operation(_) => {
                return Err(DbError::Message(
                    "retained Merge commit differs from its operation-verified exact commit"
                        .to_string(),
                ));
            }
            RetainedCommitAuthority::StoredBytes(_) => None,
        };
        let stored_author;
        let author = match (introduced_author, operation_author) {
            (Some(author), _) => author,
            (None, Some(author)) => author,
            (None, None) => {
                stored_author = registration_lookup.activated_registration_on(
                    records,
                    root,
                    &unverified.author_registration,
                )?;
                &stored_author
            }
        };
        let verified_commit = match &authority {
            RetainedCommitAuthority::StoredBytes(_) => {
                coven_protocol::store_commit::VerifiedStoreBatchCommit::parse(
                    input.commit.stored_bytes(),
                    root.store_root_hash,
                    commit_ref,
                    author,
                )
                .map_err(|error| DbError::context("retained Merge commit", error))?
            }
            RetainedCommitAuthority::Operation(verified)
                if verified.verified_commit().author() == author =>
            {
                verified.verified_commit().clone()
            }
            RetainedCommitAuthority::Operation(_) => {
                return Err(DbError::Message(
                    "retained Merge commit differs from its operation-verified exact commit"
                        .to_string(),
                ));
            }
        };
        let commit = verified_commit.value().clone();
        if commit.to_bytes() != input.commit.stored_bytes() {
            return Err(DbError::Message(
                "retained Merge commit bytes are not canonical".to_string(),
            ));
        }
        let exact_ref = StoreBatchCommitRef::from_commit(
            &commit,
            commit_ref.coord.clone(),
            input.commit.reference().clone(),
        )
        .map_err(DbError::from)?;
        if &exact_ref != commit_ref {
            return Err(DbError::Message(
                "retained Merge commit differs from its materialized coordinate".to_string(),
            ));
        }
        let acceptance = match &authority {
            RetainedCommitAuthority::Operation(materialization) => {
                materialization.acceptance().clone()
            }
            RetainedCommitAuthority::StoredBytes(Some(acceptance)) => acceptance.clone(),
            RetainedCommitAuthority::StoredBytes(None) => records
                .accepted_store_commit(&verified_commit, &author.device_signing_pubkey)?
                .into(),
        };
        let package_values = input
            .packages
            .iter()
            .map(RetainedAudiencePackage::package)
            .cloned()
            .collect::<Vec<_>>();
        let packages =
            crate::store::store_session::retained_merge_replay::canonical_retained_merge_packages(
                &commit,
                commit_ref,
                &package_values,
            )?;
        if packages != input.packages {
            return Err(DbError::Message(
                "retained Merge packages are not in commit order".to_string(),
            ));
        }
        if packages.is_empty() != input.activation.package_application.is_none() {
            return Err(DbError::Message(
                "retained Merge package application does not match its applied packages"
                    .to_string(),
            ));
        }
        let device_operations = input
            .activation
            .device_operations
            .verify_for(root, &commit)
            .map_err(DbError::from)?;
        let local_identity = match records.local_activated_registration_ref()? {
            Some(reference) => Some(match introduced_registration(&reference)? {
                Some(registration) => registration.author_pubkey.clone(),
                None if reference == unverified.author_registration => author.author_pubkey.clone(),
                None => registration_lookup
                    .activated_registration_on(records, root, &reference)?
                    .author_pubkey
                    .clone(),
            }),
            None => None,
        };
        let circle_activations = VerifiedCircleActivations::parse_retained_for_verified_commit(
            &input.activation.circle_activations,
            &verified_commit,
            local_identity.as_deref(),
        )
        .map_err(DbError::from)?;
        if commit.control().is_some() != input.membership_objects.is_some() {
            return Err(DbError::Message(
                "retained Merge membership closure differs from its exact Store control"
                    .to_string(),
            ));
        }
        OwnedVerifiedMergeMaterialization::verify(
            root.clone(),
            verified_commit,
            registrations,
            device_operations,
            circle_activations,
            acceptance,
            input.history_evidence.clone(),
            input.membership_objects.clone(),
            package_values,
            input.activation.package_application,
            input_hash,
        )
    }
}

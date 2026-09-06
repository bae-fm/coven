use super::*;
use crate::store::retained_replay::load_generation_zero_replay_baseline_on;
use crate::store::store_session::StoreRecords;

/// The plaintext one record names in the payload store.
fn stored_semantic_payload(
    records: StoreRecords<'_>,
    remote: &coven_protocol::remote_object::RemoteObjectRecord,
) -> Result<Vec<u8>, DbError> {
    let coven_protocol::remote_object::SemanticPayload::Spooled(hash) = remote.semantic_payload()
    else {
        return Err(DbError::Message(format!(
            "remote object {} names no stored plaintext",
            remote.object_id()
        )));
    };
    records.payload(hash).map_err(DbError::from)
}

fn prepared_state_contains_commit(
    encoded: &str,
    reference: &StoreBatchCommitRef,
) -> Result<bool, DbError> {
    let prepared: crate::store::publication_state::PreparedStoreWriteState =
        serde_json::from_str(encoded)
            .map_err(|error| DbError::context("retained replay prepared Store write", error))?;
    let candidates = match &prepared {
        crate::store::publication_state::PreparedStoreWriteState::Publication {
            commit, ..
        } => {
            vec![commit]
        }
        crate::store::publication_state::PreparedStoreWriteState::MergeAbandonment {
            candidate_commit,
            authority_commit,
            ..
        } => vec![candidate_commit, authority_commit],
    };
    for candidate in candidates {
        let value: StoreBatchCommit = serde_json::from_slice(candidate.semantic_bytes())
            .map_err(|error| DbError::context("retained replay prepared Store commit", error))?;
        if reference.coord.sequence() == value.seq()
            && reference.commit_hash == value.commit_hash()
            && &reference.object == candidate.prepared().reference()
        {
            return Ok(true);
        }
    }
    Ok(false)
}

impl crate::store::store_session::StoreTransaction<'_, '_> {
    pub(crate) fn load_merge_retraction_cleanup(
        self,
        authority: &mut VerifiedStoreAuthority,
        candidate: &StoreBatchCommitRef,
    ) -> Result<PreparedMergeCandidate, DbError> {
        StoreDatabase::load_merge_retraction_cleanup_on(
            crate::store::store_session::StoreRecords::new(self.transaction, self.store_dir),
            authority,
            candidate,
        )
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn load_retained_merge_materialization_by_ref(
        self,
        root: &coven_protocol::store_commit::StoreRootRef,
        registrations: &mut dyn VerifiedRegistrationLookup,
        reference: &StoreBatchCommitRef,
    ) -> Result<OwnedVerifiedMergeMaterialization, DbError> {
        StoreDatabase::load_retained_merge_materialization_by_ref_on(
            crate::store::store_session::StoreRecords::new(self.transaction, self.store_dir),
            root,
            registrations,
            reference,
        )
    }
}

impl StoreDatabase {
    pub(crate) fn open_retained_merge_materialization_input_with_verified_commit_on(
        records: StoreRecords<'_>,
        root: &coven_protocol::store_commit::StoreRootRef,
        registration_lookup: &mut dyn VerifiedRegistrationLookup,
        commit_ref: &StoreBatchCommitRef,
        input: &RetainedMergeMaterializationInput,
        input_hash: ObjectHash,
        verified: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
    ) -> Result<OwnedVerifiedMergeMaterialization, DbError> {
        Self::open_retained_merge_materialization_input_with_authority_on(
            records,
            root,
            registration_lookup,
            commit_ref,
            input,
            input_hash,
            RetainedCommitAuthority::Operation(verified),
        )
    }

    fn open_retained_merge_materialization_input_with_authority_on(
        records: StoreRecords<'_>,
        root: &coven_protocol::store_commit::StoreRootRef,
        registration_lookup: &mut dyn VerifiedRegistrationLookup,
        commit_ref: &StoreBatchCommitRef,
        input: &RetainedMergeMaterializationInput,
        input_hash: ObjectHash,
        authority: RetainedCommitAuthority<'_>,
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
                if verified.reference() == commit_ref
                    && verified.value().author_registration == unverified.author_registration
                    && verified.value().to_bytes() == input.commit.stored_bytes() =>
            {
                Some(verified.author())
            }
            RetainedCommitAuthority::Operation(_) => {
                return Err(DbError::Message(
                    "retained Merge commit differs from its operation-verified exact commit"
                        .to_string(),
                ));
            }
            RetainedCommitAuthority::StoredBytes => None,
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
        let verified_commit = match authority {
            RetainedCommitAuthority::StoredBytes => {
                coven_protocol::store_commit::VerifiedStoreBatchCommit::parse(
                    input.commit.stored_bytes(),
                    root.store_root_hash,
                    commit_ref,
                    author,
                )
                .map_err(|error| DbError::context("retained Merge commit", error))?
            }
            RetainedCommitAuthority::Operation(verified) if verified.author() == author => {
                verified.clone()
            }
            RetainedCommitAuthority::Operation(_) => {
                return Err(DbError::Message(
                    "retained Merge commit differs from its operation-verified exact commit"
                        .to_string(),
                ))
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
        let head = StoreDeviceHead::parse_at(
            input.activation_head.stored_bytes(),
            root.store_root_hash,
            author,
            commit_ref,
        )
        .map_err(|error| DbError::context("retained Merge activation head", error))?;
        if head.to_bytes() != input.activation_head.stored_bytes() {
            return Err(DbError::Message(
                "retained Merge activation head bytes are not canonical".to_string(),
            ));
        }
        let package_values = input
            .packages
            .iter()
            .map(RetainedAudiencePackage::package)
            .cloned()
            .collect::<Vec<_>>();
        let packages =
            super::canonical_retained_merge_packages(&commit, commit_ref, &package_values)?;
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
        if let Some(objects) = &input.membership_objects {
            let entry_remote = records.remote_object(remote_object_id(&objects.entry().object))?;
            let entry: MembershipEntry =
                serde_json::from_slice(&stored_semantic_payload(records, &entry_remote)?)
                    .map_err(|error| DbError::context("retained membership entry", error))?;
            let head_remote = records.remote_object(remote_object_id(&objects.head().object))?;
            let head_value: AuthorHead =
                serde_json::from_slice(&stored_semantic_payload(records, &head_remote)?)
                    .map_err(|error| DbError::context("retained membership head", error))?;
            let verified_objects = VerifiedMergeMembershipObjects::verify(
                &commit,
                commit_ref,
                &entry,
                &head_value,
                objects.head().clone(),
            )?;
            if &verified_objects != objects || entry_remote.object() != &objects.entry().object {
                return Err(DbError::Message(
                    "retained membership objects differ from their exact authority".to_string(),
                ));
            }
            if let Some(reference) = objects.resolution() {
                let remote = records.remote_object(remote_object_id(&reference.object))?;
                let resolution: coven_protocol::membership::StoreMembershipConflictResolution =
                    serde_json::from_slice(&stored_semantic_payload(records, &remote)?).map_err(
                        |error| DbError::context("retained membership resolution", error),
                    )?;
                if !resolution.verify_signature()
                    || resolution.resolution_ref(reference.object.clone()) != *reference
                {
                    return Err(DbError::Message(
                        "retained membership resolution differs from its exact authority"
                            .to_string(),
                    ));
                }
            }
        }
        OwnedVerifiedMergeMaterialization::verify(
            root.clone(),
            verified_commit,
            registrations,
            device_operations,
            circle_activations,
            head,
            input.activation_head.reference().clone(),
            input.history_evidence.clone(),
            input.membership_objects.clone(),
            package_values,
            input.activation.package_application,
            input_hash,
        )
    }

    pub(crate) fn load_retained_merge_materialization_on(
        records: StoreRecords<'_>,
        root: &coven_protocol::store_commit::StoreRootRef,
        registrations: &mut dyn VerifiedRegistrationLookup,
        stream_id: &str,
        sequence: u64,
        commit_ref: &StoreBatchCommitRef,
        expected_input_hash: &str,
    ) -> Result<OwnedVerifiedMergeMaterialization, DbError> {
        Self::load_retained_merge_materialization_with_authority_on(
            records,
            root,
            registrations,
            stream_id,
            sequence,
            commit_ref,
            expected_input_hash,
            RetainedCommitAuthority::StoredBytes,
        )
    }

    pub(crate) fn load_retained_merge_materialization_with_verified_commit_on(
        records: StoreRecords<'_>,
        root: &coven_protocol::store_commit::StoreRootRef,
        registrations: &mut dyn VerifiedRegistrationLookup,
        stream_id: &str,
        sequence: u64,
        commit_ref: &StoreBatchCommitRef,
        expected_input_hash: &str,
        verified: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
    ) -> Result<OwnedVerifiedMergeMaterialization, DbError> {
        Self::load_retained_merge_materialization_with_authority_on(
            records,
            root,
            registrations,
            stream_id,
            sequence,
            commit_ref,
            expected_input_hash,
            RetainedCommitAuthority::Operation(verified),
        )
    }

    fn load_retained_merge_materialization_with_authority_on(
        records: StoreRecords<'_>,
        root: &coven_protocol::store_commit::StoreRootRef,
        registrations: &mut dyn VerifiedRegistrationLookup,
        stream_id: &str,
        sequence: u64,
        commit_ref: &StoreBatchCommitRef,
        expected_input_hash: &str,
        authority: RetainedCommitAuthority<'_>,
    ) -> Result<OwnedVerifiedMergeMaterialization, DbError> {
        let sequence_sql = Database::sequence_to_sqlite(stream_id, sequence)?;
        let (stored_ref, stored_hash, canonical_input) =
            records.retained_materialization_row(stream_id, sequence_sql)?;
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
        let verified = Self::open_retained_merge_materialization_input_with_authority_on(
            records,
            root,
            registrations,
            commit_ref,
            &input,
            input_hash,
            authority,
        )?;
        records.validate_retained_merge_pin_closure(
            &input,
            &RetainedReplayOwner::Commit {
                commit: commit_ref.clone(),
                input_hash,
            },
        )?;
        Ok(verified)
    }

    pub(crate) fn load_retained_merge_materialization_by_ref_on(
        records: StoreRecords<'_>,
        root: &coven_protocol::store_commit::StoreRootRef,
        registrations: &mut dyn VerifiedRegistrationLookup,
        reference: &StoreBatchCommitRef,
    ) -> Result<OwnedVerifiedMergeMaterialization, DbError> {
        let StoreCommitCoord {
            stream_id,
            sequence,
        } = &reference.coord;
        let stream_id = stream_id.to_string();
        let sequence_sql = Database::sequence_to_sqlite(&stream_id, *sequence)?;
        let (stored_ref, input_hash) =
            records.retained_materialization_identity(&stream_id, sequence_sql)?;
        let stored_ref = crate::store::materialized_commit_index::parse_stored_commit_ref(
            &stream_id,
            *sequence,
            &stored_ref,
        )?;
        if &stored_ref != reference {
            return Err(DbError::Message(
                "retained Merge materialization coordinate contains another commit".to_string(),
            ));
        }
        Self::load_retained_merge_materialization_on(
            records,
            root,
            registrations,
            &stream_id,
            *sequence,
            reference,
            &input_hash,
        )
    }

    /// The checkpoint a reference resolves to.
    ///
    /// A position at or under the installed snapshot coverage resolves to the
    /// coverage itself. The snapshot's owner signed one state for its whole
    /// covered prefix and the image restates that prefix in one object, so the
    /// per-position states below the coverage are not merely unavailable — they
    /// have stopped existing, and the coverage answers in their place. Above the
    /// coverage a retained row holds the position and answers for itself.
    ///
    /// This is why a predecessor cut may name a position no retained row holds:
    /// a commit's cut can reach back to any earlier coordinate on another
    /// stream, and a device that has advanced its replay baseline retired
    /// everything the new cut covers.
    ///
    /// `baseline` is supplied rather than loaded because loading one deserializes
    /// the whole baseline database image into a fresh in-memory connection and
    /// revalidates it, about fifteen milliseconds a call. The connection already
    /// holds a verified baseline; this reads that.
    pub(crate) fn load_retained_merge_history_checkpoint_on(
        records: StoreRecords<'_>,
        root: &coven_protocol::store_commit::StoreRootRef,
        registrations: &mut dyn VerifiedRegistrationLookup,
        baseline: &RetainedReplayBaseline,
        reference: &StoreBatchCommitRef,
    ) -> Result<crate::RetainedMergeHistoryCheckpoint, DbError> {
        let StoreCommitCoord {
            stream_id,
            sequence,
        } = &reference.coord;
        let stream = stream_id.to_string();
        let sequence_sql = Database::sequence_to_sqlite(&stream, *sequence)?;
        let coverage = records
            .snapshot_coverage_position(&stream)?
            .filter(|(covered, _)| covered >= sequence);
        if let Some((covered, encoded_coverage)) = coverage {
            let snapshot_reference: StoreBatchCommitRef = serde_json::from_str(&encoded_coverage)
                .map_err(|error| {
                DbError::context("snapshot Merge checkpoint commit ref", error)
            })?;
            // At the tip the coverage names this exact commit, and disagreeing
            // there is a corrupted position rather than a superseded one. Below
            // the tip there is nothing to compare against: the coverage state is
            // the answer for every position it covers.
            if covered == *sequence && &snapshot_reference != reference {
                return Err(DbError::Message(
                    "snapshot Merge checkpoint coordinate contains another commit".to_string(),
                ));
            }
            let opened = Self::open_installed_baseline_history_summary(records, baseline)?;
            if opened
                .summary
                .frontier()
                .map_err(|error| DbError::context("snapshot Merge checkpoint frontier", error))?
                .get(&snapshot_reference.coord.stream_id)
                != Some(&snapshot_reference)
            {
                return Err(DbError::Message(
                    "snapshot Merge checkpoint is absent from its signed frontier".to_string(),
                ));
            }
            return Ok(crate::RetainedMergeHistoryCheckpoint::Snapshot(opened));
        }
        let (stored_ref, input_hash) =
            records.retained_materialization_identity(&stream, sequence_sql)?;
        let stored_ref: StoreBatchCommitRef = serde_json::from_str(&stored_ref)
            .map_err(|error| DbError::context("retained Merge checkpoint commit ref", error))?;
        if &stored_ref != reference {
            return Err(DbError::Message(
                "retained Merge checkpoint coordinate contains another commit".to_string(),
            ));
        }
        let retained = Self::load_retained_merge_materialization_on(
            records,
            root,
            registrations,
            &stream_id.to_string(),
            *sequence,
            reference,
            &input_hash,
        )?;
        Self::open_retained_merge_history_checkpoint_on(
            records,
            registrations,
            reference,
            &retained,
        )
    }

    /// The signed history summary the installed baseline rests on, opened
    /// against the device state this database holds at its coverage.
    ///
    /// This is the authority that stands in for everything under the coverage:
    /// a walk that stops at the baseline resumes its composition from here
    /// rather than from the retired commits behind it.
    pub(crate) fn open_installed_baseline_history_summary(
        records: StoreRecords<'_>,
        baseline: &RetainedReplayBaseline,
    ) -> Result<coven_protocol::store_commit::OpenedRetainedMergeHistorySummary, DbError> {
        let RetainedReplayAuthority::InstalledSnapshot(authority) = &baseline.authority else {
            return Err(DbError::Message(
                "snapshot Merge checkpoint has genesis replay authority".to_string(),
            ));
        };
        let summary = authority.metadata.history_summary.clone();
        summary
            .validate_snapshot_baseline()
            .map_err(|error| DbError::context("snapshot Merge checkpoint", error))?;
        let frontier = summary.frontier().map_err(DbError::from)?;
        // Every stream the coverage names, not just the one a caller asked
        // about: `post_state` is the merge across the whole frontier, so
        // comparing one stream's state against it only ever agreed because the
        // stores that reached here had one stream.
        let (expected_state, state) = records.store_device_state_for_history_cut(
            &coven_protocol::store_commit::StoreHistoryCut(frontier),
        )?;
        if summary.post_state != expected_state {
            return Err(DbError::Message(
                "snapshot Merge checkpoint state differs from its signed reference".to_string(),
            ));
        }
        Ok(
            coven_protocol::store_commit::OpenedRetainedMergeHistorySummary {
                announcement_frontier: summary.announcement_frontier.clone(),
                post_state: state,
                summary,
            },
        )
    }

    pub(crate) fn open_retained_merge_history_checkpoint_on(
        records: StoreRecords<'_>,
        registrations: &mut dyn VerifiedRegistrationLookup,
        reference: &StoreBatchCommitRef,
        retained: &OwnedVerifiedMergeMaterialization,
    ) -> Result<crate::RetainedMergeHistoryCheckpoint, DbError> {
        if retained.commit_ref() != reference {
            return Err(DbError::Message(
                "retained Merge checkpoint materialization names another commit".to_string(),
            ));
        }
        let state = records.store_device_snapshot(reference)?;
        state
            .validate_canonical()
            .map_err(|error| DbError::context("retained Merge checkpoint state", error))?;
        let derived = crate::store::merge_materialization_transaction::derive_materialized_store_device_state_on(
            records,
            registrations,
            retained.root(),
            retained.commit(),
            retained.device_operations(),
        )?;
        if state != derived {
            return Err(DbError::Message(
                "retained Merge checkpoint state differs from its verified commit application"
                    .to_string(),
            ));
        }
        Ok(crate::RetainedMergeHistoryCheckpoint::Commit(Box::new(
            retained.clone(),
        )))
    }

    /// The prefix of the write journal a baseline at `cut` absorbs.
    ///
    /// A write is settled at `cut` when nothing it did is still owed: a
    /// local-only write the moment it commits, a resolved write once its
    /// candidate is cleaned up, a published write once `cut` covers the commit
    /// that carries it. Everything a settled write said is therefore restated
    /// by an image captured at `cut`: the baseline replay schedules each write
    /// at its observed Store frontier, with its shared commit when one exists.
    /// What the journal still holds for the settled prefix is therefore dead
    /// weight, and the advance strips each row to its receipt or drops it.
    ///
    /// It is a *prefix* rather than a set because the journal is ordered and
    /// its local partitions are the only record of the local rows. Folding a
    /// later write while retaining an earlier one would reverse their journal
    /// order on the next replay, so the walk stops at the first write that is
    /// not settled and everything after it stays.
    pub(crate) fn settled_store_write_prefix_on(
        records: StoreRecords<'_>,
        cut: &CommitFrontier,
    ) -> Result<Vec<crate::SettledStoreWrite>, DbError> {
        let mut settled = Vec::new();
        for row in records.store_write_replay_rows()? {
            let status: WriteStatus = serde_json::from_str(&row.status).map_err(|error| {
                DbError::context(format!("settled write {} status", row.write_id), error)
            })?;
            let fold = match status.clone() {
                WriteStatus::LocalOnly => crate::SettledWriteFold::LocalOnly,
                WriteStatus::Published(position) => {
                    if !cut.covers_commit(position.commit()) {
                        break;
                    }
                    crate::SettledWriteFold::Published
                }
                WriteStatus::Resolved(_) => crate::SettledWriteFold::Reversed,
                WriteStatus::Pending | WriteStatus::Publishing | WriteStatus::Blocked(_) => break,
            };
            let base: StoreWriteBase = serde_json::from_str(&row.base).map_err(|error| {
                DbError::context(
                    format!("settled write {} observed frontier", row.write_id),
                    error,
                )
            })?;
            let observed = CommitFrontier::from_refs(base.dependencies.clone())
                .map_err(|error| DbError::context("settled write observed frontier", error))?;
            if fold.states_local_rows() && !cut.covers(&observed) {
                break;
            }
            let changeset_hash = row.changeset_hash.parse::<ObjectHash>().map_err(|error| {
                DbError::context(
                    format!("settled write {} changeset hash", row.write_id),
                    error,
                )
            })?;
            records.payload(changeset_hash)?;
            settled.push(crate::SettledStoreWrite {
                ordinal: row.ordinal,
                write_id: WriteId::from_generated(row.write_id),
                fold,
                status,
                observed: base,
                changeset_hash,
                input_hash: row.input_hash,
            });
        }
        Ok(settled)
    }

    fn retained_write_effect_on(
        records: StoreRecords<'_>,
        write_id: WriteId,
        raw_base: Option<String>,
        raw_changeset_hash: Option<String>,
        local_only: bool,
    ) -> Result<(MergeReplayWriteEffect, CommitFrontier), DbError> {
        let base = raw_base.ok_or_else(|| {
            DbError::Message(format!(
                "retained write {write_id} has no observed frontier"
            ))
        })?;
        let changeset_hash = raw_changeset_hash.ok_or_else(|| {
            DbError::Message(format!(
                "retained write {write_id} has no original changeset"
            ))
        })?;
        let changeset_hash = changeset_hash.parse::<ObjectHash>()?;
        records.payload(changeset_hash)?;
        let base: StoreWriteBase = serde_json::from_str(&base).map_err(|error| {
            DbError::context(
                format!("retained write {write_id} observed frontier"),
                error,
            )
        })?;
        let observed = CommitFrontier::from_refs(base.dependencies)
            .map_err(|error| DbError::context("retained write observed frontier", error))?;
        let mut partitions = records.store_write_partitions(write_id.as_str())?;
        if local_only {
            partitions.store = None;
            partitions.circles.clear();
        }
        Ok((
            MergeReplayWriteEffect {
                write_id,
                partitions,
            },
            observed,
        ))
    }

    pub(crate) fn load_folded_replay_journal_on(
        records: StoreRecords<'_>,
        folded: &[crate::SettledStoreWrite],
    ) -> Result<Vec<MergeReplayWrite>, DbError> {
        let rows = records.store_write_replay_rows()?;
        if rows.len() < folded.len() {
            return Err(DbError::Message(
                "folded write prefix extends past the retained journal".to_string(),
            ));
        }
        let mut journal = Vec::with_capacity(folded.len());
        for (settled, row) in folded.iter().zip(rows) {
            if row.ordinal != settled.ordinal
                || row.write_id != settled.write_id.as_str()
                || row.input_hash != settled.input_hash
                || row.changeset_hash != settled.changeset_hash.to_string()
            {
                return Err(DbError::Message(
                    "folded write prefix differs from retained journal input".to_string(),
                ));
            }
            let status: WriteStatus = serde_json::from_str(&row.status).map_err(|error| {
                DbError::context(format!("folded write {} status", row.write_id), error)
            })?;
            let base: StoreWriteBase = serde_json::from_str(&row.base).map_err(|error| {
                DbError::context(
                    format!("folded write {} observed frontier", row.write_id),
                    error,
                )
            })?;
            if status != settled.status || base != settled.observed {
                return Err(DbError::Message(format!(
                    "folded write {} changed during baseline capture",
                    row.write_id
                )));
            }
            let write = match (settled.fold, status) {
                (crate::SettledWriteFold::LocalOnly, WriteStatus::LocalOnly) => {
                    let (effect, observed) = Self::retained_write_effect_on(
                        records,
                        settled.write_id.clone(),
                        Some(row.base),
                        Some(row.changeset_hash),
                        true,
                    )?;
                    MergeReplayWrite::LocalOnly { effect, observed }
                }
                (crate::SettledWriteFold::Published, WriteStatus::Published(position)) => {
                    let commit = position.commit().clone();
                    let (effect, observed) = Self::retained_write_effect_on(
                        records,
                        settled.write_id.clone(),
                        Some(row.base),
                        Some(row.changeset_hash),
                        false,
                    )?;
                    MergeReplayWrite::Accepted {
                        effect,
                        observed,
                        commit,
                    }
                }
                (crate::SettledWriteFold::Reversed, WriteStatus::Resolved(_)) => {
                    MergeReplayWrite::Consumed {
                        write_id: settled.write_id.clone(),
                    }
                }
                _ => {
                    return Err(DbError::Message(format!(
                        "folded write {} changed status during baseline capture",
                        row.write_id
                    )))
                }
            };
            journal.push(write);
        }
        Ok(journal)
    }

    pub(crate) fn load_merge_replay_journal_on(
        records: StoreRecords<'_>,
        baseline_cut: &CommitFrontier,
        active_accepted_writes: &std::collections::BTreeMap<WriteId, StoreBatchCommitRef>,
        retracted_writes: &BTreeSet<WriteId>,
    ) -> Result<Vec<MergeReplayWrite>, DbError> {
        if active_accepted_writes
            .keys()
            .any(|write_id| retracted_writes.contains(write_id))
        {
            return Err(DbError::Message(
                "retained replay classifies one write as active and retracted".to_string(),
            ));
        }
        let mut journal = Vec::new();
        for row in records.store_write_replay_rows()? {
            let encoded_id = row.write_id;
            let write_id = WriteId::from_generated(encoded_id.clone());
            let status: WriteStatus = serde_json::from_str(&row.status).map_err(|error| {
                DbError::context(format!("retained replay write {encoded_id} status"), error)
            })?;
            let active = active_accepted_writes.get(&write_id);
            let retracted = retracted_writes.contains(&write_id);
            let write = match status {
                WriteStatus::LocalOnly => {
                    let (effect, observed) = Self::retained_write_effect_on(
                        records,
                        write_id,
                        Some(row.base),
                        Some(row.changeset_hash),
                        true,
                    )?;
                    if effect.partitions.store.is_some() || !effect.partitions.circles.is_empty() {
                        return Err(DbError::Message(format!(
                            "Local-only write {encoded_id} carries a shared partition"
                        )));
                    }
                    MergeReplayWrite::LocalOnly { effect, observed }
                }
                WriteStatus::Pending => {
                    let (effect, observed) = Self::retained_write_effect_on(
                        records,
                        write_id,
                        Some(row.base),
                        Some(row.changeset_hash),
                        false,
                    )?;
                    MergeReplayWrite::Unaccepted { effect, observed }
                }
                WriteStatus::Publishing | WriteStatus::Blocked(_) => {
                    if retracted {
                        return Err(DbError::Message(format!(
                            "unresolved write {encoded_id} is already terminally retracted"
                        )));
                    }
                    let accepted = match (active, row.prepared.as_deref()) {
                        (Some(reference), Some(prepared))
                            if prepared_state_contains_commit(prepared, reference)? =>
                        {
                            Some(reference.clone())
                        }
                        (Some(_), None) => {
                            return Err(DbError::Message(format!(
                                "unresolved write {encoded_id} has no prepared candidate"
                            )))
                        }
                        _ => None,
                    };
                    let (effect, observed) = Self::retained_write_effect_on(
                        records,
                        write_id,
                        Some(row.base),
                        Some(row.changeset_hash),
                        false,
                    )?;
                    match accepted {
                        Some(commit) => MergeReplayWrite::Accepted {
                            effect,
                            observed,
                            commit,
                        },
                        None => MergeReplayWrite::Unaccepted { effect, observed },
                    }
                }
                WriteStatus::Published(position) => {
                    if retracted {
                        MergeReplayWrite::Consumed { write_id }
                    } else if baseline_cut.covers_commit(position.commit()) {
                        let (effect, observed) = Self::retained_write_effect_on(
                            records,
                            write_id,
                            Some(row.base),
                            Some(row.changeset_hash),
                            false,
                        )?;
                        MergeReplayWrite::LocalOnly { effect, observed }
                    } else {
                        let active = active.ok_or_else(|| {
                            DbError::Message(format!(
                                "published write {encoded_id} has no retained replay input"
                            ))
                        })?;
                        if active != position.commit() {
                            return Err(DbError::Message(format!(
                                "published write {encoded_id} is associated with another accepted commit"
                            )));
                        }
                        let (effect, observed) = Self::retained_write_effect_on(
                            records,
                            write_id,
                            Some(row.base),
                            Some(row.changeset_hash),
                            false,
                        )?;
                        MergeReplayWrite::Accepted {
                            effect,
                            observed,
                            commit: active.clone(),
                        }
                    }
                }
                WriteStatus::Resolved(_) => MergeReplayWrite::Consumed { write_id },
            };
            journal.push(write);
        }
        Ok(journal)
    }

    pub(crate) fn load_merge_replay_associations_on(
        records: StoreRecords<'_>,
        baseline_cut: &CommitFrontier,
        active_accepted_writes: &std::collections::BTreeMap<WriteId, StoreBatchCommitRef>,
        retracted_writes: &BTreeSet<WriteId>,
    ) -> Result<Vec<MergeReplayWrite>, DbError> {
        let associations = Self::load_merge_replay_journal_on(
            records,
            baseline_cut,
            active_accepted_writes,
            retracted_writes,
        )?
        .into_iter()
        .filter_map(|write| match write {
            MergeReplayWrite::Accepted {
                mut effect,
                observed,
                commit,
            } => {
                effect.partitions.local = None;
                Some(MergeReplayWrite::Accepted {
                    effect,
                    observed,
                    commit,
                })
            }
            MergeReplayWrite::LocalOnly { .. }
            | MergeReplayWrite::Unaccepted { .. }
            | MergeReplayWrite::Consumed { .. } => None,
        })
        .collect::<Vec<_>>();
        Ok(associations)
    }

    pub(crate) fn generation_zero_replay_baseline_on(
        records: StoreRecords<'_>,
    ) -> Result<RetainedReplayBaseline, DbError> {
        load_generation_zero_replay_baseline_on(records)?.ok_or_else(|| {
            DbError::Message("generation-zero retained replay baseline is absent".to_string())
        })
    }

    pub(crate) fn load_merge_retraction_cleanup_on(
        records: StoreRecords<'_>,
        authority: &mut VerifiedStoreAuthority,
        candidate: &StoreBatchCommitRef,
    ) -> Result<PreparedMergeCandidate, DbError> {
        let (commit, head) = records.merge_retraction_cleanup_objects(candidate)?;
        let prepared = parse_prepared_merge_candidate_parts_on(
            records,
            authority,
            commit.semantic_bytes(),
            commit.prepared().reference(),
            head.semantic_bytes(),
            head.prepared().reference(),
        )?;
        if &prepared.reference != candidate {
            return Err(DbError::Message(
                "Merge retraction cleanup opens another candidate".to_string(),
            ));
        }
        Ok(prepared)
    }

    pub(crate) fn load_merge_retraction_cleanup_for_verified_materialization_on(
        conn: &Connection,
        retained: &OwnedVerifiedMergeMaterialization,
    ) -> Result<PreparedMergeCandidate, DbError> {
        let (commit, head) = load_merge_retraction_cleanup_objects_on(conn, retained.commit_ref())?;
        let unverified = serde_json::from_slice(commit.semantic_bytes())
            .map_err(|error| DbError::context("signed Merge retraction cleanup", error))?;
        let prepared = verify_prepared_merge_candidate_parts(
            &retained.verified_commit().author().store_root,
            unverified,
            retained.verified_commit().author(),
            commit.semantic_bytes(),
            commit.prepared().reference(),
            head.semantic_bytes(),
            head.prepared().reference(),
        )?;
        if prepared.reference != *retained.commit_ref()
            || prepared.commit.to_bytes() != retained.commit().to_bytes()
            || prepared.head != *retained.activation_head()
            || prepared.head_object != *retained.activation_head_object()
        {
            return Err(DbError::Message(
                "Merge retraction cleanup differs from its verified materialization".to_string(),
            ));
        }
        Ok(prepared)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn load_retained_merge_replay_inputs_on(
        records: StoreRecords<'_>,
        root: &coven_protocol::store_commit::StoreRootRef,
        registrations: &mut dyn VerifiedRegistrationLookup,
    ) -> Result<Vec<OwnedVerifiedMergeMaterialization>, DbError> {
        let rows = records.retained_materialization_rows()?;
        rows.into_iter()
            .map(|(stream_id, sequence, encoded_ref, input_hash)| {
                let sequence = Database::sequence_from_sqlite(&stream_id, sequence)?;
                let commit_ref = crate::store::materialized_commit_index::parse_stored_commit_ref(
                    &stream_id,
                    sequence,
                    &encoded_ref,
                )?;
                Self::load_retained_merge_materialization_on(
                    records,
                    root,
                    registrations,
                    &stream_id,
                    sequence,
                    &commit_ref,
                    &input_hash,
                )
            })
            .collect()
    }
}

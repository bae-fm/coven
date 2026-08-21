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

    /// The local write partitions replay overlays on top of the baseline image.
    ///
    /// `baseline_cut` is what the image already states. A published write whose
    /// commit the cut covers has had its shared partition folded into the image
    /// and its commit retired, so it is settled in exactly the way a write
    /// replayed from a retained row above the cut is — only its local partition
    /// is still owed. Without that, a device that advanced its baseline would
    /// find every write it had ever published stranded: not replayed from a
    /// retained row, not retracted, and so reported as a write with no input.
    pub(crate) fn load_merge_replay_write_overlays_on(
        records: StoreRecords<'_>,
        baseline_cut: &CommitFrontier,
        active_accepted_writes: &BTreeSet<WriteId>,
        retracted_writes: &BTreeSet<WriteId>,
    ) -> Result<Vec<MergeReplayWriteOverlay>, DbError> {
        if !active_accepted_writes.is_disjoint(retracted_writes) {
            return Err(DbError::Message(
                "retained replay classifies one write as active and retracted".to_string(),
            ));
        }
        let rows = records.store_write_status_rows()?;
        let mut overlays = Vec::new();
        for (encoded_write_id, raw_status) in rows {
            let write_id = WriteId::from_generated(encoded_write_id.clone());
            let status: WriteStatus = serde_json::from_str(&raw_status).map_err(|error| {
                DbError::context(
                    format!("retained replay write {encoded_write_id} status"),
                    error,
                )
            })?;
            let partitions = records.store_write_partitions(&encoded_write_id)?;
            let active = active_accepted_writes.contains(&write_id);
            let retracted = retracted_writes.contains(&write_id);
            let partitions = match status {
                WriteStatus::LocalOnly => {
                    if partitions.store.is_some() || !partitions.circles.is_empty() {
                        return Err(DbError::Message(format!(
                            "Local-only write {encoded_write_id} carries a shared partition"
                        )));
                    }
                    PreparedStoreWritePartitions {
                        store: None,
                        circles: Vec::new(),
                        local: partitions.local,
                    }
                }
                WriteStatus::Pending => partitions,
                WriteStatus::Publishing | WriteStatus::Blocked(_) => {
                    if retracted {
                        return Err(DbError::Message(format!(
                            "unresolved write {encoded_write_id} is already terminally retracted"
                        )));
                    }
                    if active {
                        PreparedStoreWritePartitions {
                            store: None,
                            circles: Vec::new(),
                            local: partitions.local,
                        }
                    } else {
                        partitions
                    }
                }
                WriteStatus::Published(position) => {
                    let active = active || baseline_cut.covers_commit(position.commit());
                    if retracted {
                        PreparedStoreWritePartitions {
                            store: None,
                            circles: Vec::new(),
                            local: None,
                        }
                    } else if active {
                        PreparedStoreWritePartitions {
                            store: None,
                            circles: Vec::new(),
                            local: partitions.local,
                        }
                    } else {
                        return Err(DbError::Message(format!(
                            "published write {encoded_write_id} has no retained replay input"
                        )));
                    }
                }
                WriteStatus::Resolved(_) => PreparedStoreWritePartitions {
                    store: None,
                    circles: Vec::new(),
                    local: None,
                },
            };
            if partitions.store.is_some()
                || !partitions.circles.is_empty()
                || partitions.local.is_some()
            {
                overlays.push(MergeReplayWriteOverlay {
                    write_id,
                    partitions,
                });
            }
        }
        Ok(overlays)
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

use super::cache::*;
use super::*;

impl StoreDatabase {
    pub(crate) fn open_retained_merge_materialization_input_with_authority_on(
        conn: &Connection,
        root: &crate::protocol::store_commit::StoreRootRef,
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
            .map_err(|error| DbError::Message(format!("retained Merge commit: {error}")))?;
        let registrations = input
            .activation
            .registrations
            .verify_for(root, &unverified)
            .map_err(|error| DbError::Message(error.to_string()))?;
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
        let stored_author;
        let author = match introduced_author {
            Some(author) => author,
            None => {
                stored_author =
                    load_activated_registration_on(conn, root, &unverified.author_registration)?;
                &stored_author
            }
        };
        let verified_commit = match authority {
            RetainedCommitAuthority::StoredBytes => {
                crate::protocol::store_commit::VerifiedStoreBatchCommit::parse(
                    input.commit.stored_bytes(),
                    root.store_root_hash,
                    commit_ref,
                    author,
                )
                .map_err(|error| DbError::Message(format!("retained Merge commit: {error}")))?
            }
            RetainedCommitAuthority::Operation(verified)
                if verified.reference() == commit_ref
                    && verified.author() == author
                    && verified.value().to_bytes() == input.commit.stored_bytes() =>
            {
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
        .map_err(|error| DbError::Message(error.to_string()))?;
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
        .map_err(|error| DbError::Message(format!("retained Merge activation head: {error}")))?;
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
            Self::canonical_retained_merge_packages(&commit, commit_ref, &package_values)?;
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
            .map_err(|error| DbError::Message(error.to_string()))?;
        let local_identity = match local_activated_registration_ref_on(conn)? {
            Some(reference) => Some(match introduced_registration(&reference)? {
                Some(registration) => registration.author_pubkey.clone(),
                None => load_activated_registration_on(conn, root, &reference)?.author_pubkey,
            }),
            None => None,
        };
        let circle_activations = VerifiedCircleActivations::parse_retained_for_verified_commit(
            &input.activation.circle_activations,
            &verified_commit,
            local_identity.as_deref(),
        )
        .map_err(|error| DbError::Message(error.to_string()))?;
        if commit.control().is_some() != input.membership_objects.is_some() {
            return Err(DbError::Message(
                "retained Merge membership closure differs from its exact Store control"
                    .to_string(),
            ));
        }
        if let Some(objects) = &input.membership_objects {
            let entry_remote =
                load_remote_object_on(conn, remote_object_id(&objects.entry().object))?;
            let entry: MembershipEntry = serde_json::from_slice(
                entry_remote.bytes().canonical_semantic_bytes(),
            )
            .map_err(|error| DbError::Message(format!("retained membership entry: {error}")))?;
            let head_remote =
                load_remote_object_on(conn, remote_object_id(&objects.head().object))?;
            let head_value: AuthorHead = serde_json::from_slice(
                head_remote.bytes().canonical_semantic_bytes(),
            )
            .map_err(|error| DbError::Message(format!("retained membership head: {error}")))?;
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
                let remote = load_remote_object_on(conn, remote_object_id(&reference.object))?;
                let resolution: crate::protocol::membership::StoreMembershipConflictResolution =
                    serde_json::from_slice(remote.bytes().canonical_semantic_bytes()).map_err(
                        |error| {
                            DbError::Message(format!("retained membership resolution: {error}"))
                        },
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
            input.history_summary.clone(),
            input.membership_objects.clone(),
            package_values,
            input.activation.package_application,
            input_hash,
        )
    }

    pub(crate) fn retain_merge_materialization_on(
        conn: &rusqlite::Transaction<'_>,
        root: &crate::protocol::store_commit::StoreRootRef,
        materialization: &VerifiedMergeMaterialization<'_>,
    ) -> Result<
        (
            RetainedMergeMaterializationKey,
            OwnedVerifiedMergeMaterialization,
        ),
        DbError,
    > {
        Self::retain_merge_materialization_with_authority_on(
            conn,
            root,
            materialization,
            RetainedCommitAuthority::Operation(materialization.verified_commit()),
        )
    }

    pub(crate) fn retain_merge_materialization_with_authority_on(
        conn: &rusqlite::Transaction<'_>,
        root: &crate::protocol::store_commit::StoreRootRef,
        materialization: &VerifiedMergeMaterialization<'_>,
        authority: RetainedCommitAuthority<'_>,
    ) -> Result<
        (
            RetainedMergeMaterializationKey,
            OwnedVerifiedMergeMaterialization,
        ),
        DbError,
    > {
        let packages = Self::canonical_retained_merge_packages(
            materialization.commit(),
            materialization.commit_ref(),
            materialization.packages(),
        )?;
        let input = RetainedMergeMaterializationInput {
            commit: PreparedExactObject::new(
                materialization.commit_ref().object.clone(),
                materialization.commit().to_bytes(),
            )
            .map_err(|error| DbError::Message(error.to_string()))?,
            activation_head: PreparedExactObject::new(
                materialization.activation_head_object().clone(),
                materialization.activation_head().to_bytes(),
            )
            .map_err(|error| DbError::Message(error.to_string()))?,
            history_summary: materialization.history_summary().clone(),
            membership_objects: materialization.membership_objects().cloned(),
            packages,
            activation: RetainedCommitActivationInput {
                registrations: RetainedStoreDeviceRegistrationActivations::from_verified(
                    root,
                    materialization.commit(),
                    materialization.registrations(),
                )
                .map_err(|error| DbError::Message(error.to_string()))?,
                device_operations: materialization.device_operations().to_retained(),
                circle_activations: materialization
                    .circle_activations()
                    .to_retained()
                    .map_err(|error| DbError::Message(error.to_string()))?,
                package_application: materialization.package_application(),
            },
        };
        let canonical_input = serde_json::to_vec(&input).map_err(|error| {
            DbError::Message(format!("serialize retained Merge materialization: {error}"))
        })?;
        let input_hash = ObjectHash::digest(&canonical_input);
        let verified = Self::open_retained_merge_materialization_input_with_authority_on(
            conn,
            root,
            materialization.commit_ref(),
            &input,
            input_hash,
            authority,
        )?;
        let StoreCommitCoord {
            stream_id,
            sequence,
        } = &materialization.commit_ref().coord;
        let stream_id = stream_id.to_string();
        let sequence = Database::sequence_to_sqlite(&stream_id, *sequence)?;
        let commit_ref_json =
            serde_json::to_string(materialization.commit_ref()).map_err(|error| {
                DbError::Message(format!("serialize retained Merge commit ref: {error}"))
            })?;
        let inserted = conn
            .execute(
                "INSERT INTO retained_merge_materializations
                 (device_id, seq, commit_ref, input_hash, canonical_input)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(device_id, seq) DO NOTHING",
                rusqlite::params![
                    &stream_id,
                    sequence,
                    &commit_ref_json,
                    input_hash.to_string(),
                    &canonical_input
                ],
            )
            .map_err(DbError::from)?;
        if inserted == 0 {
            let stored: (String, String, Vec<u8>) = conn
                .query_row(
                    "SELECT commit_ref, input_hash, canonical_input
                     FROM retained_merge_materializations
                     WHERE device_id = ?1 AND seq = ?2",
                    rusqlite::params![&stream_id, sequence],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .map_err(DbError::from)?;
            if stored
                != (
                    commit_ref_json.clone(),
                    input_hash.to_string(),
                    canonical_input,
                )
            {
                return Err(DbError::Message(format!(
                    "retained Merge coordinate {stream_id}/{} already contains different exact input",
                    materialization.commit_ref().coord.sequence()
                )));
            }
        }
        let replay_owner = RetainedReplayOwner::Commit {
            commit: materialization.commit_ref().clone(),
            input_hash,
        };
        Self::pin_retained_merge_objects_on(conn, &input, &replay_owner)?;
        Self::validate_retained_merge_pin_closure_on(conn, &input, &replay_owner)?;
        Ok((
            RetainedMergeMaterializationKey {
                commit_ref: commit_ref_json,
                input_hash,
            },
            verified,
        ))
    }

    pub(crate) fn load_retained_merge_materialization_on(
        conn: &Connection,
        root: &crate::protocol::store_commit::StoreRootRef,
        stream_id: &str,
        sequence: u64,
        commit_ref: &StoreBatchCommitRef,
        expected_input_hash: &str,
    ) -> Result<OwnedVerifiedMergeMaterialization, DbError> {
        Self::load_retained_merge_materialization_with_authority_on(
            conn,
            root,
            stream_id,
            sequence,
            commit_ref,
            expected_input_hash,
            RetainedCommitAuthority::StoredBytes,
        )
    }

    pub(crate) fn load_retained_merge_materialization_with_verified_commit_on(
        conn: &Connection,
        root: &crate::protocol::store_commit::StoreRootRef,
        stream_id: &str,
        sequence: u64,
        commit_ref: &StoreBatchCommitRef,
        expected_input_hash: &str,
        verified: &crate::protocol::store_commit::VerifiedStoreBatchCommit,
    ) -> Result<OwnedVerifiedMergeMaterialization, DbError> {
        Self::load_retained_merge_materialization_with_authority_on(
            conn,
            root,
            stream_id,
            sequence,
            commit_ref,
            expected_input_hash,
            RetainedCommitAuthority::Operation(verified),
        )
    }

    pub(crate) fn load_retained_merge_materialization_with_authority_on(
        conn: &Connection,
        root: &crate::protocol::store_commit::StoreRootRef,
        stream_id: &str,
        sequence: u64,
        commit_ref: &StoreBatchCommitRef,
        expected_input_hash: &str,
        authority: RetainedCommitAuthority<'_>,
    ) -> Result<OwnedVerifiedMergeMaterialization, DbError> {
        let sequence_sql = Database::sequence_to_sqlite(stream_id, sequence)?;
        let (stored_ref, stored_hash, canonical_input): (String, String, Vec<u8>) = conn
            .query_row(
                "SELECT commit_ref, input_hash, canonical_input
                 FROM retained_merge_materializations
                 WHERE device_id = ?1 AND seq = ?2",
                rusqlite::params![stream_id, sequence_sql],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(DbError::from)?;
        let expected_ref = serde_json::to_string(commit_ref).map_err(|error| {
            DbError::Message(format!("serialize materialized Merge commit ref: {error}"))
        })?;
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
            .map_err(|error| {
                DbError::Message(format!("retained Merge materialization input: {error}"))
            })?;
        if serde_json::to_vec(&input).map_err(|error| {
            DbError::Message(format!("serialize retained Merge materialization: {error}"))
        })? != canonical_input
        {
            return Err(DbError::Message(
                "retained Merge materialization input is not canonical".to_string(),
            ));
        }
        let input_hash = stored_hash.parse().map_err(|error| {
            DbError::Message(format!(
                "retained Merge coordinate {stream_id}/{sequence} input hash is invalid: {error}"
            ))
        })?;
        let verified = Self::open_retained_merge_materialization_input_with_authority_on(
            conn, root, commit_ref, &input, input_hash, authority,
        )?;
        Self::validate_retained_merge_pin_closure_on(
            conn,
            &input,
            &RetainedReplayOwner::Commit {
                commit: commit_ref.clone(),
                input_hash,
            },
        )?;
        Ok(verified)
    }

    pub(crate) fn load_retained_merge_materialization_by_ref_on(
        conn: &Connection,
        root: &crate::protocol::store_commit::StoreRootRef,
        reference: &StoreBatchCommitRef,
    ) -> Result<OwnedVerifiedMergeMaterialization, DbError> {
        let StoreCommitCoord {
            stream_id,
            sequence,
        } = &reference.coord;
        let stream_id = stream_id.to_string();
        let sequence_sql = Database::sequence_to_sqlite(&stream_id, *sequence)?;
        let (stored_ref, input_hash): (String, String) = conn
            .query_row(
                "SELECT commit_ref, input_hash FROM retained_merge_materializations
                 WHERE device_id = ?1 AND seq = ?2",
                rusqlite::params![&stream_id, sequence_sql],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(DbError::from)?;
        let stored_ref = Self::parse_stored_commit_ref(&stream_id, *sequence, &stored_ref)?;
        if &stored_ref != reference {
            return Err(DbError::Message(
                "retained Merge materialization coordinate contains another commit".to_string(),
            ));
        }
        Self::load_retained_merge_materialization_on(
            conn,
            root,
            &stream_id,
            *sequence,
            reference,
            &input_hash,
        )
    }

    pub(crate) fn load_retained_merge_history_checkpoint_on(
        conn: &Connection,
        root: &crate::protocol::store_commit::StoreRootRef,
        reference: &StoreBatchCommitRef,
    ) -> Result<crate::protocol::store_commit::OpenedRetainedMergeHistorySummary, DbError> {
        let StoreCommitCoord {
            stream_id,
            sequence,
        } = &reference.coord;
        let stream = stream_id.to_string();
        let sequence_sql = Database::sequence_to_sqlite(&stream, *sequence)?;
        let snapshot_reference: Option<String> = conn
            .query_row(
                "SELECT commit_ref FROM snapshot_coverage WHERE device_id = ?1 AND seq = ?2",
                rusqlite::params![&stream, sequence_sql],
                |row| row.get(0),
            )
            .optional()
            .map_err(DbError::from)?;
        if let Some(snapshot_reference) = snapshot_reference {
            let snapshot_reference: StoreBatchCommitRef = serde_json::from_str(&snapshot_reference)
                .map_err(|error| {
                    DbError::Message(format!("snapshot Merge checkpoint commit ref: {error}"))
                })?;
            if &snapshot_reference != reference {
                return Err(DbError::Message(
                    "snapshot Merge checkpoint coordinate contains another commit".to_string(),
                ));
            }
            let baseline = load_generation_zero_replay_baseline_on(conn)?.ok_or_else(|| {
                DbError::Message(
                    "snapshot Merge checkpoint has no retained replay baseline".to_string(),
                )
            })?;
            let RetainedReplayAuthority::StableSnapshot(authority) = baseline.authority else {
                return Err(DbError::Message(
                    "snapshot Merge checkpoint has genesis replay authority".to_string(),
                ));
            };
            let summary = authority.metadata.history_summary;
            summary
                .validate_snapshot_baseline()
                .map_err(|error| DbError::Message(format!("snapshot Merge checkpoint: {error}")))?;
            if summary
                .frontier()
                .map_err(|error| {
                    DbError::Message(format!("snapshot Merge checkpoint frontier: {error}"))
                })?
                .get(stream_id)
                != Some(reference)
            {
                return Err(DbError::Message(
                    "snapshot Merge checkpoint is absent from its signed frontier".to_string(),
                ));
            }
            let state = load_store_device_snapshot_on(conn, reference)?;
            let expected_state = crate::protocol::store_commit::StoreDeviceStateRef::from_resolved(
                CommitFrontier(
                    summary
                        .frontier()
                        .map_err(|error| DbError::Message(error.to_string()))?,
                ),
                &state,
            )
            .map_err(|error| DbError::Message(error.to_string()))?;
            if summary.post_state != expected_state {
                return Err(DbError::Message(
                    "snapshot Merge checkpoint state differs from its signed reference".to_string(),
                ));
            }
            return Ok(
                crate::protocol::store_commit::OpenedRetainedMergeHistorySummary {
                    announcement_frontier: summary.announcement_frontier.clone(),
                    post_state: state,
                    summary,
                },
            );
        }
        let (stored_ref, input_hash): (String, String) = conn
            .query_row(
                "SELECT commit_ref, input_hash FROM retained_merge_materializations \
                 WHERE device_id = ?1 AND seq = ?2",
                rusqlite::params![stream, sequence_sql],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(DbError::from)?;
        let stored_ref: StoreBatchCommitRef =
            serde_json::from_str(&stored_ref).map_err(|error| {
                DbError::Message(format!("retained Merge checkpoint commit ref: {error}"))
            })?;
        if &stored_ref != reference {
            return Err(DbError::Message(
                "retained Merge checkpoint coordinate contains another commit".to_string(),
            ));
        }
        let retained = Self::load_retained_merge_materialization_on(
            conn,
            root,
            &stream_id.to_string(),
            *sequence,
            reference,
            &input_hash,
        )?;
        Self::open_retained_merge_history_checkpoint_on(conn, reference, &retained)
    }

    pub(crate) fn open_retained_merge_history_checkpoint_on(
        conn: &Connection,
        reference: &StoreBatchCommitRef,
        retained: &OwnedVerifiedMergeMaterialization,
    ) -> Result<crate::protocol::store_commit::OpenedRetainedMergeHistorySummary, DbError> {
        if retained.commit_ref() != reference {
            return Err(DbError::Message(
                "retained Merge checkpoint materialization names another commit".to_string(),
            ));
        }
        let head_ref = crate::protocol::store_commit::StoreDeviceHeadRef {
            head_hash: retained.activation_head().head_hash(),
            object: retained.activation_head_object().clone(),
        };
        let state = load_store_device_snapshot_on(conn, reference)?;
        retained
            .history_summary()
            .open(
                retained.commit(),
                reference,
                retained.activation_head(),
                &head_ref,
                &state,
            )
            .map_err(|error| DbError::Message(format!("retained Merge checkpoint: {error}")))
    }

    pub(crate) fn load_merge_replay_write_overlays_on(
        conn: &Connection,
        active_accepted_writes: &BTreeSet<WriteId>,
        retracted_writes: &BTreeSet<WriteId>,
    ) -> Result<Vec<MergeReplayWriteOverlay>, DbError> {
        if !active_accepted_writes.is_disjoint(retracted_writes) {
            return Err(DbError::Message(
                "retained replay classifies one write as active and retracted".to_string(),
            ));
        }
        let mut statement = conn
            .prepare(
                "SELECT write_id, status, changeset
                 FROM store_writes
                 ORDER BY ordinal",
            )
            .map_err(DbError::from)?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                ))
            })
            .map_err(DbError::from)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(DbError::from)?;
        drop(statement);
        let mut overlays = Vec::new();
        for (encoded_write_id, raw_status, stored_store_changeset) in rows {
            let write_id = WriteId::from_generated(encoded_write_id.clone());
            let status: WriteStatus = serde_json::from_str(&raw_status).map_err(|error| {
                DbError::Message(format!(
                    "retained replay write {encoded_write_id} status: {error}"
                ))
            })?;
            let partitions = StoreDatabase::store_write_partitions_on(
                conn,
                &encoded_write_id,
                &stored_store_changeset,
            )?;
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
                WriteStatus::Published(_) => {
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
        conn: &Connection,
    ) -> Result<RetainedReplayBaseline, DbError> {
        load_generation_zero_replay_baseline_on(conn)?.ok_or_else(|| {
            DbError::Message("generation-zero retained replay baseline is absent".to_string())
        })
    }

    pub(crate) fn load_merge_retraction_cleanup_on(
        conn: &Connection,
        candidate: &StoreBatchCommitRef,
    ) -> Result<PreparedMergeCandidate, DbError> {
        let StoreCommitCoord {
            stream_id,
            sequence,
        } = &candidate.coord;
        let stream_id = stream_id.to_string();
        let sequence_sql = Database::sequence_to_sqlite(&stream_id, *sequence)?;
        let encoded_ref = serde_json::to_string(candidate).map_err(|error| {
            DbError::Message(format!("serialize Merge retraction cleanup ref: {error}"))
        })?;
        let (stored_hash, canonical_cleanup): (String, Vec<u8>) = conn
            .query_row(
                "SELECT cleanup_hash, canonical_cleanup
                 FROM merge_retraction_cleanups
                 WHERE device_id = ?1 AND seq = ?2 AND commit_ref = ?3",
                rusqlite::params![&stream_id, sequence_sql, &encoded_ref],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(DbError::from)?;
        if stored_hash != ObjectHash::digest(&canonical_cleanup).to_string() {
            return Err(DbError::Message(
                "Merge retraction cleanup hash differs from its bytes".to_string(),
            ));
        }
        let input: MergeRetractionCleanupInput = serde_json::from_slice(&canonical_cleanup)
            .map_err(|error| {
                DbError::Message(format!("parse Merge retraction cleanup: {error}"))
            })?;
        if serde_json::to_vec(&input).map_err(|error| {
            DbError::Message(format!("serialize Merge retraction cleanup: {error}"))
        })? != canonical_cleanup
        {
            return Err(DbError::Message(
                "Merge retraction cleanup is not canonical".to_string(),
            ));
        }
        let commit =
            DurablePreparedProtocolObject::new(input.commit.stored_bytes().to_vec(), input.commit);
        let head = DurablePreparedProtocolObject::new(
            input.activation_head.stored_bytes().to_vec(),
            input.activation_head,
        );
        let prepared = parse_prepared_merge_candidate_parts_on(conn, &commit, &head)?;
        if &prepared.reference != candidate {
            return Err(DbError::Message(
                "Merge retraction cleanup opens another candidate".to_string(),
            ));
        }
        Ok(prepared)
    }

    #[cfg(test)]
    pub(crate) fn load_retained_merge_replay_inputs_on(
        conn: &Connection,
        root: &crate::protocol::store_commit::StoreRootRef,
    ) -> Result<Vec<OwnedVerifiedMergeMaterialization>, DbError> {
        let mut statement = conn
            .prepare(
                "SELECT device_id, seq, commit_ref, input_hash
                 FROM retained_merge_materializations
                 ORDER BY device_id, seq",
            )
            .map_err(DbError::from)?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })
            .map_err(DbError::from)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(DbError::from)?;
        drop(statement);
        rows.into_iter()
            .map(|(stream_id, sequence, encoded_ref, input_hash)| {
                let sequence = Database::sequence_from_sqlite(&stream_id, sequence)?;
                let commit_ref = Self::parse_stored_commit_ref(&stream_id, sequence, &encoded_ref)?;
                Self::load_retained_merge_materialization_on(
                    conn,
                    root,
                    &stream_id,
                    sequence,
                    &commit_ref,
                    &input_hash,
                )
            })
            .collect()
    }
}

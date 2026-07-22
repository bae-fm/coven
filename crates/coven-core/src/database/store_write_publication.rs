use crate::database::store_device_state::store_serial_predecessor_on;

use crate::database::blob_records::load_activated_registration_on;
use crate::database::blob_records::load_prepared_audience_objects_on;
use crate::database::local_store_identity::local_store_authority_on;
use crate::database::remote_object_records::load_remote_object_on;
use crate::database::remote_object_records::update_remote_object_on;

use super::*;

impl Database {
    pub(crate) async fn oldest_prepared_store_write(
        &self,
    ) -> Result<Option<PreparedStoreWriteCommit>, DbError> {
        let loaded = self
            .call(|conn| {
                let row = conn
                    .query_row(
                        "SELECT write_id, changeset, base, prepared FROM store_writes
                 WHERE prepared IS NOT NULL
                   AND status = '\"publishing\"'
                 ORDER BY ordinal LIMIT 1",
                        [],
                        |row| {
                            Ok((
                                row.get::<_, String>(0)?,
                                row.get::<_, Vec<u8>>(1)?,
                                row.get::<_, String>(2)?,
                                row.get::<_, String>(3)?,
                            ))
                        },
                    )
                    .optional()
                    .map_err(DbError::from)?;
                row.map(|(write_id, stored_changeset, base, prepared)| {
                    let prepared: PreparedStoreWriteState = serde_json::from_str(&prepared)
                        .map_err(|error| {
                            DbError::Message(format!("prepared Store write: {error}"))
                        })?;
                    let (commit, head, graph_commit) = match &prepared {
                        PreparedStoreWriteState::MergeConcurrent { commit, head, .. } => {
                            (commit, head, None)
                        }
                        PreparedStoreWriteState::MergeAbandonment {
                            candidate_commit,
                            authority_commit,
                            authority_head,
                            ..
                        } => (authority_commit, authority_head, Some(candidate_commit)),
                        PreparedStoreWriteState::SerialPreparing
                        | PreparedStoreWriteState::Serial { .. } => {
                            return Err(DbError::Message(
                                "serial branch reached MergeConcurrent publication".to_string(),
                            ));
                        }
                    };
                    let write_id = WriteId::from_generated(write_id);
                    let unverified_commit: StoreBatchCommit =
                        serde_json::from_slice(&commit.semantic_bytes).map_err(|error| {
                            DbError::Message(format!("prepared Store commit: {error}"))
                        })?;
                    if unverified_commit.write_id != write_id {
                        return Err(DbError::Message(
                            "prepared write id differs from signed commit".to_string(),
                        ));
                    }
                    let root = required_store_root_authority_on(conn)?;
                    let registration_ref = &unverified_commit.author_registration;
                    let (registration_bytes, stored_registration_ref): (Vec<u8>, String) = conn
                        .query_row(
                            "SELECT registration_bytes, registration_object \
                         FROM store_device_registration_activations \
                         WHERE device_id = ?1 AND registration_hash = ?2",
                            (
                                registration_ref.device_id.to_string(),
                                registration_ref.registration_hash.to_string(),
                            ),
                            |row| Ok((row.get(0)?, row.get(1)?)),
                        )
                        .map_err(DbError::from)?;
                    let stored_registration_ref: StoreDeviceRegistrationRef =
                        serde_json::from_str(&stored_registration_ref).map_err(|error| {
                            DbError::Message(format!("prepared write registration ref: {error}"))
                        })?;
                    if stored_registration_ref != *registration_ref {
                        return Err(DbError::Message(
                            "prepared commit registration differs from its activation".to_string(),
                        ));
                    }
                    let registration = StoreDeviceRegistration::parse_at(
                        &registration_bytes,
                        &root,
                        registration_ref.device_id,
                    )
                    .map_err(|error| {
                        DbError::Message(format!("prepared write registration: {error}"))
                    })?;
                    let stream_id =
                        crate::sync::store_commit::StreamActivation::device_authorized_stream_id(
                            root.store_root_hash,
                            registration_ref,
                            crate::sync::store_commit::StreamAnchorDomain::StoreAnnouncements,
                        );
                    let coord = StoreCommitCoord::MergeConcurrent {
                        stream_id,
                        sequence: unverified_commit.seq(),
                    };
                    let commit_value = StoreBatchCommit::parse_at(
                        &commit.semantic_bytes,
                        root.store_root_hash,
                        &coord,
                        &registration,
                    )
                    .map_err(|error| {
                        DbError::Message(format!("verify prepared Store commit: {error}"))
                    })?;
                    let commit_ref = StoreBatchCommitRef::from_commit(
                        &commit_value,
                        coord,
                        commit.prepared.reference().clone(),
                    )
                    .map_err(|error| DbError::Message(error.to_string()))?;
                    let head_value = StoreDeviceHead::parse_at(
                        &head.semantic_bytes,
                        root.store_root_hash,
                        &registration,
                        &commit_ref,
                    )
                    .map_err(|error| {
                        DbError::Message(format!("verify prepared Store head: {error}"))
                    })?;
                    let base: StoreWriteBase = serde_json::from_str(&base).map_err(|error| {
                        DbError::Message(format!("prepared write base: {error}"))
                    })?;
                    let StoreWriteBase::MergeConcurrent { dependencies } = base else {
                        return Err(DbError::Message(
                            "serial base reached MergeConcurrent publication".to_string(),
                        ));
                    };
                    let dependencies =
                        CommitFrontier::from_refs(WritePolicy::MergeConcurrent, dependencies)
                            .map_err(|error| {
                                DbError::Message(format!("prepared dependency frontier: {error}"))
                            })?;
                    if dependencies
                        .merge_commits()
                        .map_err(|error| DbError::Message(error.to_string()))?
                        != commit_value.merge_dependencies().map_err(|error| {
                            DbError::Message(format!("prepared Store commit policy: {error}"))
                        })?
                    {
                        return Err(DbError::Message(
                            "prepared commit differs from its write dependency frontier"
                                .to_string(),
                        ));
                    }
                    let partitions = Self::store_write_partitions_on(
                        conn,
                        write_id.as_str(),
                        &stored_changeset,
                        WritePolicy::MergeConcurrent,
                    )?;
                    let audiences = load_prepared_audience_objects_on(conn, &write_id)?;
                    let graph_commit = match graph_commit {
                        Some(graph_commit) => {
                            let candidate = parse_prepared_merge_candidate_parts_on(
                                conn,
                                graph_commit,
                                match &prepared {
                                    PreparedStoreWriteState::MergeAbandonment {
                                        candidate_head,
                                        ..
                                    } => candidate_head,
                                    _ => unreachable!("matched Merge abandonment"),
                                },
                            )?;
                            candidate.commit
                        }
                        None => commit_value.clone(),
                    };
                    let expected_package_count =
                        usize::from(graph_commit.store_package().is_some())
                            .checked_add(graph_commit.circle_packages().len())
                            .ok_or_else(|| {
                                DbError::Message("package count overflow".to_string())
                            })?;
                    if audiences.packages.len() != expected_package_count
                        || audiences.packages.len()
                            != usize::from(partitions.store.is_some()) + partitions.circles.len()
                    {
                        return Err(DbError::Message(
                            "prepared package indexes do not exactly cover commit audiences"
                                .to_string(),
                        ));
                    }
                    for package in &audiences.packages {
                        let value = package.package();
                        if value.write_id() != &write_id
                            || value.commit_coord() != &commit_ref.coord
                            || value.candidate_family() != commit_value.candidate_family()
                        {
                            return Err(DbError::Message(
                                "indexed audience package differs from its exact commit"
                                    .to_string(),
                            ));
                        }
                        let expected_object = match value.audience() {
                            crate::sync::audience_package::PackageAudience::Store => {
                                graph_commit
                                    .verify_store_package(package.semantic_bytes())
                                    .map_err(|error| DbError::Message(error.to_string()))?;
                                &graph_commit
                                    .store_package()
                                    .as_ref()
                                    .expect("verified present")
                                    .object
                            }
                            crate::sync::audience_package::PackageAudience::Circle {
                                circle_id,
                                ..
                            } => {
                                graph_commit
                                    .verify_circle_package(*circle_id, package.semantic_bytes())
                                    .map_err(|error| DbError::Message(error.to_string()))?;
                                &graph_commit
                                    .circle_packages()
                                    .iter()
                                    .find(|entry| entry.circle_id == *circle_id)
                                    .expect("verified present")
                                    .package
                                    .object
                            }
                        };
                        if package.object() != expected_object {
                            return Err(DbError::Message(
                                "indexed audience package exact object differs from its commit"
                                    .to_string(),
                            ));
                        }
                    }
                    for package in &audiences.packages {
                        let audience = package.package().audience().remote_audience();
                        for binding in package.package().blob_bindings() {
                            if !audiences.blobs.iter().any(|blob| {
                                blob.audience() == &audience && blob.blob() == binding.blob()
                            }) {
                                return Err(DbError::Message(
                                    "prepared package blob binding has no exact blob index"
                                        .to_string(),
                                ));
                            }
                        }
                    }
                    for blob in &audiences.blobs {
                        if !audiences.packages.iter().any(|package| {
                            package.package().audience().remote_audience() == *blob.audience()
                                && package
                                    .package()
                                    .blob_bindings()
                                    .iter()
                                    .any(|binding| binding.blob() == blob.blob())
                        }) {
                            return Err(DbError::Message(
                                "prepared blob index has no exact package binding".to_string(),
                            ));
                        }
                    }
                    Ok(PreparedStoreWriteCommit {
                        audiences,
                        commit: ExactProtocolObject {
                            value: commit_value,
                            bytes: commit.semantic_bytes.clone(),
                            object: commit.prepared.reference().clone(),
                            prepared: commit.prepared.clone(),
                        },
                        head: ExactProtocolObject {
                            value: head_value,
                            bytes: head.semantic_bytes.clone(),
                            object: head.prepared.reference().clone(),
                            prepared: head.prepared.clone(),
                        },
                    })
                })
                .transpose()
            })
            .await?;
        if let Some(batch) = &loaded {
            for blob in &batch.audiences.blobs {
                if let Some(spool_path) = blob.spool_path() {
                    crate::local_blob::verify_exact_file(blob.blob().object(), spool_path)
                        .await
                        .map_err(|error| {
                            DbError::Message(format!("prepared blob spool: {error}"))
                        })?;
                }
            }
        }
        Ok(loaded)
    }

    pub(crate) async fn prepared_serial_store_branch(
        &self,
    ) -> Result<Option<PreparedSerialStoreBranch>, DbError> {
        let loaded = self
            .call(|conn| {
                let root = required_store_root_authority_on(conn)?;
                let mut statement = conn
                    .prepare(
                        "SELECT write_id, changeset, base, prepared FROM store_writes
                     WHERE prepared IS NOT NULL AND status = '\"publishing\"'
                     ORDER BY ordinal",
                    )
                    .map_err(DbError::from)?;
                let rows = statement
                    .query_map([], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, Vec<u8>>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                        ))
                    })
                    .map_err(DbError::from)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(DbError::from)?;
                drop(statement);
                let mut branch_id = None;
                let mut base = None;
                let mut base_head: Option<VersionedObject> = None;
                let mut writes = Vec::new();
                let mut head = None;
                let mut predecessor: Option<StoreBatchCommitRef> = None;
                for row in rows {
                    let (write_id, stored_changeset, raw_base, prepared) = row;
                    let prepared: PreparedStoreWriteState = serde_json::from_str(&prepared)
                        .map_err(|error| {
                            DbError::Message(format!("prepared Serial write: {error}"))
                        })?;
                    if matches!(prepared, PreparedStoreWriteState::SerialPreparing) {
                        if writes.is_empty() {
                            return Ok(None);
                        }
                        return Err(DbError::Message(
                            "Serial branch mixes reserved and exact prepared writes".to_string(),
                        ));
                    }
                    let PreparedStoreWriteState::Serial {
                        base_head: row_base_head,
                        commit,
                        tip_head_bytes,
                        ..
                    } = prepared
                    else {
                        return Err(DbError::Message(
                            "MergeConcurrent write reached Serial publication".to_string(),
                        ));
                    };
                    let StoreWriteBase::Serial {
                        branch_id: row_branch_id,
                        base: row_base,
                    } = serde_json::from_str(&raw_base).map_err(|error| {
                        DbError::Message(format!("prepared Serial base: {error}"))
                    })?
                    else {
                        return Err(DbError::Message(
                            "MergeConcurrent base reached Serial publication".to_string(),
                        ));
                    };
                    if branch_id
                        .as_ref()
                        .is_some_and(|value| value != &row_branch_id)
                        || base.as_ref().is_some_and(|value| value != &row_base)
                        || base_head
                            .as_ref()
                            .is_some_and(|value| value != &row_base_head)
                    {
                        return Err(DbError::Message(
                            "prepared Serial writes do not share one branch base".to_string(),
                        ));
                    }
                    branch_id.get_or_insert(row_branch_id);
                    base.get_or_insert(row_base);
                    base_head.get_or_insert(row_base_head);
                    let unverified: StoreBatchCommit =
                        serde_json::from_slice(&commit.semantic_bytes).map_err(|error| {
                            DbError::Message(format!("prepared Serial commit: {error}"))
                        })?;
                    if unverified.write_id.as_str() != write_id {
                        return Err(DbError::Message(
                            "prepared Serial write id differs from signed commit".to_string(),
                        ));
                    }
                    if writes.is_empty() {
                        predecessor = base.as_ref().expect("first row stored base").clone();
                    }
                    let expected_sequence = predecessor
                        .as_ref()
                        .map_or(1, |reference| reference.coord.sequence().saturating_add(1));
                    let coord = StoreCommitCoord::Serial {
                        sequence: expected_sequence,
                    };
                    let registration = load_activated_registration_on(
                        conn,
                        &root,
                        &unverified.author_registration,
                    )?;
                    let commit_value = StoreBatchCommit::parse_at(
                        &commit.semantic_bytes,
                        root.store_root_hash,
                        &coord,
                        &registration,
                    )
                    .map_err(|error| {
                        DbError::Message(format!("verify prepared Serial commit: {error}"))
                    })?;
                    let order_matches = match (&predecessor, &commit_value.order) {
                        (
                            Some(expected),
                            crate::sync::store_commit::StoreCommitOrder::Serial {
                                predecessor: StoreSerialPredecessor::Commit(actual),
                                ..
                            },
                        ) => actual == expected,
                        (
                            None,
                            crate::sync::store_commit::StoreCommitOrder::Serial {
                                predecessor:
                                    StoreSerialPredecessor::Genesis {
                                        root: commit_root,
                                        founder_registration,
                                    },
                                ..
                            },
                        ) => {
                            commit_root == &root
                                && founder_registration == &commit_value.author_registration
                        }
                        _ => false,
                    };
                    if !order_matches {
                        return Err(DbError::Message(
                            "prepared Serial commit chain has a different exact predecessor"
                                .to_string(),
                        ));
                    }
                    let commit_ref = StoreBatchCommitRef::from_commit(
                        &commit_value,
                        coord,
                        commit.prepared.reference().clone(),
                    )
                    .map_err(|error| DbError::Message(error.to_string()))?;
                    let write_id = WriteId::from_generated(write_id);
                    let partitions = Self::store_write_partitions_on(
                        conn,
                        write_id.as_str(),
                        &stored_changeset,
                        WritePolicy::Serial,
                    )?;
                    let audiences = load_prepared_audience_objects_on(conn, &write_id)?;
                    Self::validate_loaded_write_objects(
                        &write_id,
                        &commit_ref,
                        &commit_value,
                        &partitions,
                        &audiences,
                    )?;
                    if let Some(head_bytes) = tip_head_bytes {
                        if head.is_some() {
                            return Err(DbError::Message(
                                "prepared Serial branch has more than one tip head".to_string(),
                            ));
                        }
                        let value = StoreSerialHead::parse(
                            &head_bytes,
                            root.store_root_hash,
                            &registration,
                        )
                        .map_err(|error| {
                            DbError::Message(format!("verify prepared Serial head: {error}"))
                        })?;
                        head = Some(CanonicalProtocolObject {
                            value,
                            bytes: head_bytes,
                        });
                    }
                    writes.push(PreparedSerialStoreWriteCommit {
                        audiences,
                        commit: ExactProtocolObject {
                            value: commit_value,
                            bytes: commit.semantic_bytes,
                            object: commit.prepared.reference().clone(),
                            prepared: commit.prepared,
                        },
                    });
                    predecessor = Some(commit_ref);
                }
                if writes.is_empty() {
                    return Ok(None);
                }
                let head = head.ok_or_else(|| {
                    DbError::Message(
                        "prepared Serial branch has no activating tip head".to_string(),
                    )
                })?;
                let tip_ref = predecessor.expect("nonempty branch has exact tip");
                let tip_author = &writes
                    .last()
                    .expect("nonempty branch")
                    .commit
                    .value
                    .author_registration;
                if !matches!(
                    &head.value.state,
                    StoreSerialHeadState::Commit {
                        author_registration,
                        commit,
                    } if author_registration == tip_author && commit == &tip_ref
                ) {
                    return Err(DbError::Message(
                        "prepared Serial head does not activate the exact final commit".to_string(),
                    ));
                }
                let base_value = base.expect("nonempty branch");
                let base_head = base_head.expect("nonempty branch");
                {
                    let bytes = base_head.bytes.as_slice();
                    let unverified: StoreSerialHead =
                        serde_json::from_slice(bytes).map_err(|error| {
                            DbError::Message(format!("stored Serial base head: {error}"))
                        })?;
                    let executor_ref = match &unverified.state {
                        StoreSerialHeadState::Genesis {
                            founder_registration,
                            ..
                        } => founder_registration,
                        StoreSerialHeadState::Commit {
                            author_registration,
                            ..
                        } => author_registration,
                    };
                    let executor = load_activated_registration_on(conn, &root, executor_ref)?;
                    let verified = StoreSerialHead::parse(bytes, root.store_root_hash, &executor)
                        .map_err(|error| {
                        DbError::Message(format!("verify stored Serial base head: {error}"))
                    })?;
                    let observed = match verified.state {
                        StoreSerialHeadState::Genesis {
                            root: observed_root,
                            ..
                        } => {
                            if observed_root != root {
                                return Err(DbError::Message(
                                    "stored Serial genesis head has a different exact root"
                                        .to_string(),
                                ));
                            }
                            None
                        }
                        StoreSerialHeadState::Commit { commit, .. } => Some(commit),
                    };
                    if observed != base_value {
                        return Err(DbError::Message(
                            "stored Serial base evidence differs from branch base".to_string(),
                        ));
                    }
                }
                Ok(Some(PreparedSerialStoreBranch {
                    branch_id: branch_id.expect("nonempty branch"),
                    base: base_value,
                    base_head,
                    writes,
                    head,
                }))
            })
            .await?;
        if let Some(branch) = &loaded {
            for write in &branch.writes {
                for blob in &write.audiences.blobs {
                    if let Some(spool_path) = blob.spool_path() {
                        crate::local_blob::verify_exact_file(blob.blob().object(), spool_path)
                            .await
                            .map_err(|error| {
                                DbError::Message(format!("prepared Serial blob spool: {error}"))
                            })?;
                    }
                }
            }
        }
        Ok(loaded)
    }

    pub(crate) async fn prepared_serial_candidate_abandonment(
        &self,
    ) -> Result<Option<PreparedSerialCandidateAbandonment>, DbError> {
        self.call(|conn| {
            let raw: Option<String> = conn
                .query_row(
                    "SELECT value FROM protocol_state WHERE key = ?1",
                    [SERIAL_CANDIDATE_ABANDONMENT_STATE_KEY],
                    |row| row.get(0),
                )
                .optional()
                .map_err(DbError::from)?;
            let Some(raw) = raw else {
                return Ok(None);
            };
            let durable: DurableSerialCandidateAbandonment =
                serde_json::from_str(&raw).map_err(|error| {
                    DbError::Message(format!("prepared Serial candidate abandonment: {error}"))
                })?;
            let expected_base = serde_json::to_string(&StoreWriteBase::Serial {
                branch_id: durable.branch_id.clone(),
                base: durable.base.clone(),
            })
            .map_err(|error| {
                DbError::Message(format!("Serial abandonment branch base: {error}"))
            })?;
            let raw_prepared: String = conn
                .query_row(
                    "SELECT prepared FROM store_writes
                     WHERE base = ?1 AND prepared IS NOT NULL
                     ORDER BY ordinal LIMIT 1",
                    [expected_base.as_str()],
                    |row| row.get(0),
                )
                .map_err(DbError::from)?;
            let parsed = parse_prepared_serial_candidate(&raw_prepared)?.ok_or_else(|| {
                DbError::Message("Serial abandonment target is not an exact candidate".to_string())
            })?;
            if parsed.reference != durable.candidate {
                return Err(DbError::Message(
                    "Serial abandonment target differs from its durable candidate".to_string(),
                ));
            }
            let prepared: PreparedStoreWriteState =
                serde_json::from_str(&raw_prepared).map_err(|error| {
                    DbError::Message(format!("Serial abandonment candidate state: {error}"))
                })?;
            let PreparedStoreWriteState::Serial {
                base_head, commit, ..
            } = prepared
            else {
                return Err(DbError::Message(
                    "Serial abandonment target is not an exact candidate".to_string(),
                ));
            };
            if base_head != durable.base_head {
                return Err(DbError::Message(
                    "Serial abandonment differs from its durable branch base".to_string(),
                ));
            }
            let candidate = ExactProtocolObject {
                value: parsed.commit,
                bytes: parsed.canonical_signed_bytes,
                object: parsed.reference.object,
                prepared: commit.prepared,
            };
            let raw_tip_prepared: String = conn
                .query_row(
                    "SELECT prepared FROM store_writes
                     WHERE base = ?1 AND prepared IS NOT NULL
                     ORDER BY ordinal DESC LIMIT 1",
                    [expected_base],
                    |row| row.get(0),
                )
                .map_err(DbError::from)?;
            let tip_prepared: PreparedStoreWriteState = serde_json::from_str(&raw_tip_prepared)
                .map_err(|error| {
                    DbError::Message(format!("Serial abandonment tip state: {error}"))
                })?;
            if !matches!(
                tip_prepared,
                PreparedStoreWriteState::Serial {
                    tip_head_bytes: Some(ref tip_head_bytes),
                    ..
                } if tip_head_bytes == &durable.original_head_bytes
            ) {
                return Err(DbError::Message(
                    "Serial abandonment original head differs from its durable branch tip"
                        .to_string(),
                ));
            }
            let root = required_store_root_authority_on(conn)?;
            let unverified: StoreBatchCommit =
                serde_json::from_slice(&durable.commit.semantic_bytes).map_err(|error| {
                    DbError::Message(format!("Serial abandonment commit: {error}"))
                })?;
            let registration =
                load_activated_registration_on(conn, &root, &unverified.author_registration)?;
            let value = StoreBatchCommit::parse_at(
                &durable.commit.semantic_bytes,
                root.store_root_hash,
                &durable.candidate.coord,
                &registration,
            )
            .map_err(|error| {
                DbError::Message(format!("verify Serial abandonment commit: {error}"))
            })?;
            let reference = StoreBatchCommitRef::from_commit(
                &value,
                durable.candidate.coord.clone(),
                durable.commit.prepared.reference().clone(),
            )
            .map_err(|error| DbError::Message(error.to_string()))?;
            if value.abandoned_candidates()
                != [crate::sync::store_commit::CandidateCleanupManifest {
                    candidate: crate::sync::store_commit::StoreBatchCommitDeletionTarget {
                        coord: durable.candidate.coord.clone(),
                        object: durable.candidate.object.clone(),
                        canonical_signed_bytes: candidate.bytes.clone(),
                    },
                }]
            {
                return Err(DbError::Message(
                    "durable Serial abandonment names another candidate".to_string(),
                ));
            }
            let head =
                StoreSerialHead::parse(&durable.head_bytes, root.store_root_hash, &registration)
                    .map_err(|error| {
                        DbError::Message(format!("verify Serial abandonment head: {error}"))
                    })?;
            if !matches!(
                &head.state,
                StoreSerialHeadState::Commit { commit, .. } if commit == &reference
            ) {
                return Err(DbError::Message(
                    "durable Serial abandonment head names another commit".to_string(),
                ));
            }
            let unverified_original: StoreSerialHead =
                serde_json::from_slice(&durable.original_head_bytes).map_err(|error| {
                    DbError::Message(format!("Serial abandonment original head: {error}"))
                })?;
            let original_author = match &unverified_original.state {
                StoreSerialHeadState::Commit {
                    author_registration,
                    ..
                } => author_registration,
                StoreSerialHeadState::Genesis { .. } => {
                    return Err(DbError::Message(
                        "Serial abandonment original head is not a commit".to_string(),
                    ));
                }
            };
            let original_registration =
                load_activated_registration_on(conn, &root, original_author)?;
            StoreSerialHead::parse(
                &durable.original_head_bytes,
                root.store_root_hash,
                &original_registration,
            )
            .map_err(|error| {
                DbError::Message(format!("verify Serial abandonment original head: {error}"))
            })?;
            Ok(Some(PreparedSerialCandidateAbandonment {
                branch_id: durable.branch_id,
                base: durable.base,
                base_head: durable.base_head,
                authority: ExactProtocolObject {
                    value,
                    bytes: durable.commit.semantic_bytes,
                    object: reference.object,
                    prepared: durable.commit.prepared,
                },
                head: CanonicalProtocolObject {
                    value: head,
                    bytes: durable.head_bytes,
                },
                original_head_bytes: durable.original_head_bytes,
                durable_state: raw,
            }))
        })
        .await
    }

    pub(crate) async fn latest_local_store_position(
        &self,
    ) -> Result<Option<StoreBatchCommitRef>, DbError> {
        let write_policy = self.write_policy();
        self.call(move |conn| {
            let stream_id = match write_policy {
                WritePolicy::MergeConcurrent => {
                    let (root, registration, _) = local_store_authority_on(conn)?;
                    crate::sync::store_commit::StreamActivation::device_authorized_stream_id(
                        root.store_root_hash,
                        &registration,
                        crate::sync::store_commit::StreamAnchorDomain::StoreAnnouncements,
                    )
                    .to_string()
                }
                WritePolicy::Serial => SERIAL_STREAM_ID.to_string(),
            };
            Self::latest_position_for_device_on(conn, &stream_id)
        })
        .await
    }

    pub(crate) async fn complete_prepared_store_write(
        &self,
        accepted: StoreBatchCommitRef,
        nonactivations: Vec<crate::sync::remote_object::VerifiedCandidateNonactivation>,
    ) -> Result<CompletePreparedStoreWriteOutcome, DbError> {
        let nonactivations = nonactivations
            .into_iter()
            .map(|verified| {
                verified
                    .candidate_reference()
                    .map(|reference| (reference, verified.into_durable()))
                    .map_err(|error| DbError::Message(error.to_string()))
            })
            .collect::<Result<std::collections::BTreeMap<_, _>, _>>()?;
        let statuses = self.state.write_statuses.clone();
        let gates = self.state.gates.clone();
        let synced_tables = self.state.synced_tables.clone();
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let local_device_id: String = tx
                .query_row(
                    "SELECT value FROM protocol_state WHERE key = ?1",
                    [LOCAL_DEVICE_ID_STATE_KEY],
                    |row| row.get(0),
                )
                .map_err(DbError::from)?;
            let prepared_count: i64 = tx
                .query_row(
                    "SELECT COUNT(*) FROM store_writes WHERE prepared IS NOT NULL",
                    [],
                    |row| row.get(0),
                )
                .map_err(DbError::from)?;
            if prepared_count != 1 {
                return Err(DbError::Message(format!(
                    "Store publication expected one prepared write, found {prepared_count}"
                )));
            }
            let (stored_write_id, raw_status, raw_prepared): (String, String, String) = tx
                .query_row(
                    "SELECT write_id, status, prepared FROM store_writes
                     WHERE prepared IS NOT NULL ORDER BY ordinal LIMIT 1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .map_err(DbError::from)?;
            let current_status: WriteStatus =
                serde_json::from_str(&raw_status).map_err(|error| {
                    DbError::Message(format!("prepared Store write status: {error}"))
                })?;
            let prepared: PreparedStoreWriteState = serde_json::from_str(&raw_prepared)
                .map_err(|error| DbError::Message(format!("prepared Store write: {error}")))?;
            let exclusion_candidate = parse_prepared_merge_candidate_on(&tx, &prepared)?
                .ok_or_else(|| {
                    DbError::Message("Serial branch reached MergeConcurrent completion".to_string())
                })?;
            if author_exclusion_activation_for_candidate_on(
                &tx,
                &exclusion_candidate.reference,
                &exclusion_candidate.commit.author_registration,
            )?
            .is_some()
            {
                let device_id = exclusion_candidate.commit.author_registration.device_id;
                let write_id = WriteId::from_generated(stored_write_id.clone());
                if let WriteStatus::Resolved(WriteResolution::Retracted { witness }) =
                    &current_status
                {
                    witness.validate().map_err(DbError::Message)?;
                    if witness.original_position().commit() != &exclusion_candidate.reference {
                        return Err(DbError::Message(
                            "terminal write retraction names another prepared candidate"
                                .to_string(),
                        ));
                    }
                    tx.execute(
                        "DELETE FROM store_write_blob_leases WHERE write_id = ?1",
                        [write_id.as_str()],
                    )
                    .map_err(DbError::from)?;
                    tx.execute(
                        "DELETE FROM store_write_packages WHERE write_id = ?1",
                        [write_id.as_str()],
                    )
                    .map_err(DbError::from)?;
                    tx.execute(
                        "DELETE FROM store_write_blobs WHERE write_id = ?1",
                        [write_id.as_str()],
                    )
                    .map_err(DbError::from)?;
                    let updated = tx
                        .execute(
                            "UPDATE store_writes SET prepared = NULL
                             WHERE write_id = ?1 AND status = ?2 AND prepared = ?3",
                            rusqlite::params![write_id.as_str(), &raw_status, &raw_prepared],
                        )
                        .map_err(DbError::from)?;
                    if updated != 1 {
                        return Err(DbError::Message(
                            "terminally retracted Store write changed during completion"
                                .to_string(),
                        ));
                    }
                    tx.commit().map_err(DbError::from)?;
                    return Ok(CompletePreparedStoreWriteOutcome::AuthorExcluded { device_id });
                }
                let status = WriteStatus::Blocked(crate::WriteBlock::InvalidProtocolState {
                    reason: format!(
                        "Store author {device_id} was excluded before candidate activation"
                    ),
                });
                Self::set_write_status_on(&tx, &write_id, &status)?;
                tx.commit().map_err(DbError::from)?;
                Self::notify_write_status_in(&statuses, &write_id, status);
                return Ok(CompletePreparedStoreWriteOutcome::AuthorExcluded { device_id });
            }
            if let PreparedStoreWriteState::MergeAbandonment {
                candidate_commit,
                candidate_head,
                authority_commit,
                authority_head,
                authority_history_summary,
                ..
            } = &prepared
            {
                let root = required_store_root_authority_on(&tx)?;
                let candidate =
                    parse_prepared_merge_candidate_parts_on(&tx, candidate_commit, candidate_head)?;
                let authority =
                    parse_prepared_merge_candidate_parts_on(&tx, authority_commit, authority_head)?;
                if authority.commit.write_id.as_str() != stored_write_id
                    || accepted != authority.reference
                    || !matches!(
                        &authority.commit.body,
                        crate::sync::store_commit::StoreCommitBody::AbandonCandidates { .. }
                    )
                {
                    return Err(DbError::Message(
                        "accepted Merge abandonment differs from its durable authority".to_string(),
                    ));
                }
                let registration = load_activated_registration_on(
                    &tx,
                    &root,
                    &authority.commit.author_registration,
                )?;
                StoreDeviceHead::parse_at(
                    &authority.head.to_bytes(),
                    root.store_root_hash,
                    &registration,
                    &accepted,
                )
                .map_err(|error| {
                    DbError::Message(format!("verify accepted Merge abandonment head: {error}"))
                })?;
                for object in [
                    authority_commit.prepared.reference(),
                    authority_head.prepared.reference(),
                ] {
                    let object_id = remote_object_id(object);
                    let remote = load_remote_object_on(&tx, object_id)?
                        .into_activated(&accepted)
                        .map_err(|error| {
                            DbError::Message(format!(
                                "activate Merge abandonment object {object_id}: {error}"
                            ))
                        })?;
                    update_remote_object_on(&tx, object_id, &remote)?;
                }
                let nonactivation = nonactivations.get(&candidate.reference).ok_or_else(|| {
                    DbError::Message(
                        "accepted Merge abandonment has no verified candidate nonactivation"
                            .to_string(),
                    )
                })?;
                begin_merge_candidate_nonactivation_on(
                    &tx,
                    &WriteId::from_generated(stored_write_id.clone()),
                    &candidate,
                    nonactivation,
                    true,
                )?;
                Self::record_materialized_merge_commit_on(
                    &tx,
                    &root,
                    &authority.commit,
                    &accepted,
                    &[],
                    &authority.head,
                    authority.head_prepared.reference(),
                    authority_history_summary,
                    &[],
                    None,
                )?;
                let mut completed_preparation = prepared.clone();
                let PreparedStoreWriteState::MergeAbandonment { outcome, .. } =
                    &mut completed_preparation
                else {
                    unreachable!("matched Merge abandonment")
                };
                *outcome = MergeAbandonmentOutcome::Accepted {
                    authority: accepted.clone(),
                };
                let completed_preparation =
                    serde_json::to_string(&completed_preparation).map_err(|error| {
                        DbError::Message(format!("serialize accepted Merge abandonment: {error}"))
                    })?;
                let updated = tx
                    .execute(
                        "UPDATE store_writes SET prepared = ?2
                         WHERE write_id = ?1 AND prepared = ?3",
                        rusqlite::params![
                            stored_write_id.as_str(),
                            completed_preparation,
                            raw_prepared
                        ],
                    )
                    .map_err(DbError::from)?;
                if updated != 1 {
                    return Err(DbError::Message(
                        "Merge abandonment changed during activation".to_string(),
                    ));
                }
                let blocked = WriteStatus::Blocked(crate::WriteBlock::InvalidProtocolState {
                    reason: format!(
                        "candidate abandonment {} is accepted; exact cleanup is pending",
                        authority.head.head_hash()
                    ),
                });
                let write_id = authority.commit.write_id.clone();
                Self::set_write_status_on(&tx, &write_id, &blocked)?;
                tx.commit().map_err(DbError::from)?;
                Self::notify_write_status_in(&statuses, &write_id, blocked);
                return Ok(CompletePreparedStoreWriteOutcome::Published);
            }
            let PreparedStoreWriteState::MergeConcurrent {
                commit,
                head,
                history_summary,
                local_cleanup,
                ..
            } = prepared
            else {
                return Err(DbError::Message(
                    "serial branch reached MergeConcurrent completion".to_string(),
                ));
            };
            let root = required_store_root_authority_on(&tx)?;
            let unverified: StoreBatchCommit = serde_json::from_slice(&commit.semantic_bytes)
                .map_err(|error| DbError::Message(format!("prepared Store commit: {error}")))?;
            let registration =
                load_activated_registration_on(&tx, &root, &unverified.author_registration)?;
            let expected_stream =
                crate::sync::store_commit::StreamActivation::device_authorized_stream_id(
                    root.store_root_hash,
                    &unverified.author_registration,
                    crate::sync::store_commit::StreamAnchorDomain::StoreAnnouncements,
                );
            if !matches!(
                accepted.coord,
                StoreCommitCoord::MergeConcurrent { stream_id, .. }
                    if stream_id == expected_stream
            ) || accepted.object != *commit.prepared.reference()
            {
                return Err(DbError::Message(
                    "accepted Merge head differs from the exact prepared commit".to_string(),
                ));
            }
            let commit_value = StoreBatchCommit::parse_at(
                &commit.semantic_bytes,
                root.store_root_hash,
                &accepted.coord,
                &registration,
            )
            .map_err(|error| DbError::Message(format!("outbound commit: {error}")))?;
            accepted
                .verify_commit(&commit_value)
                .map_err(|error| DbError::Message(error.to_string()))?;
            let head_value = StoreDeviceHead::parse_at(
                &head.semantic_bytes,
                root.store_root_hash,
                &registration,
                &accepted,
            )
            .map_err(|error| DbError::Message(format!("outbound Store head: {error}")))?;
            if commit_value.write_id.as_str() != stored_write_id {
                return Err(DbError::Message(
                    "prepared write id differs from signed commit".to_string(),
                ));
            }
            let write_id = commit_value.write_id.clone();
            let head_object_id = remote_object_id(head.prepared.reference());
            Self::activate_prepared_write_on(
                &tx,
                &root,
                &gates,
                &synced_tables,
                &write_id,
                &commit_value,
                &accepted,
                PreparedWriteMaterialization::MergeConcurrent {
                    head: &head_value,
                    head_object: head.prepared.reference(),
                    history_summary: &history_summary,
                },
                local_cleanup,
                &[head_object_id],
            )?;
            let cleared = tx
                .execute(
                    "UPDATE store_writes SET prepared = NULL
                     WHERE write_id = ?1 AND prepared IS NOT NULL",
                    [stored_write_id.as_str()],
                )
                .map_err(DbError::from)?;
            if cleared != 1 {
                return Err(DbError::Message(
                    "prepared Store write disappeared".to_string(),
                ));
            }
            let status = WriteStatus::Published(Box::new(PublishedPosition::MergeConcurrent {
                device_id: local_device_id,
                commit: accepted.clone(),
            }));
            Self::set_write_status_on(&tx, &write_id, &status)?;
            tx.commit().map_err(DbError::from)?;
            Self::notify_write_status_in(&statuses, &write_id, status);
            Ok(CompletePreparedStoreWriteOutcome::Published)
        })
        .await
    }

    pub(crate) async fn mark_serial_branch_conflict(
        &self,
        branch_id: PendingBranchId,
        base: Option<StoreBatchCommitRef>,
        current: StoreSerialPredecessor,
    ) -> Result<(), DbError> {
        let statuses = self.state.write_statuses.clone();
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let expected_base = StoreWriteBase::Serial {
                branch_id: branch_id.clone(),
                base: base.clone(),
            };
            let conflict = crate::SerializationConflict {
                branch_id: branch_id.clone(),
                base: store_serial_predecessor_on(&tx, base.as_ref())?,
                current,
            };
            let status = WriteStatus::Conflict(Box::new(conflict));
            let status_json = serde_json::to_string(&status)
                .map_err(|error| DbError::Message(format!("serialize Serial conflict: {error}")))?;
            let mut statement = tx
                .prepare(
                    "SELECT write_id, base FROM store_writes
                     WHERE status != '\"local_only\"'
                       AND json_extract(status, '$.published') IS NULL
                       AND json_extract(status, '$.resolved') IS NULL
                     ORDER BY ordinal",
                )
                .map_err(DbError::from)?;
            let rows = statement
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .map_err(DbError::from)?;
            let write_ids = Self::write_ids_matching_serial_base(rows, &expected_base)?;
            drop(statement);
            if write_ids.is_empty() {
                return Err(DbError::Message(format!(
                    "Serial branch {:?} has no pending writes",
                    branch_id.first_write_id()
                )));
            }
            for write_id in &write_ids {
                let updated = tx
                    .execute(
                        "UPDATE store_writes SET status = ?2 WHERE write_id = ?1",
                        rusqlite::params![write_id.as_str(), &status_json],
                    )
                    .map_err(DbError::from)?;
                if updated != 1 {
                    return Err(DbError::Message(format!(
                        "Serial conflict write {write_id} disappeared"
                    )));
                }
            }
            tx.commit().map_err(DbError::from)?;
            for write_id in write_ids {
                Self::notify_write_status_in(&statuses, &write_id, status.clone());
            }
            Ok(())
        })
        .await
    }

    pub(crate) async fn mark_merge_candidate_conflict(
        &self,
        write_id: WriteId,
        nonactivations: Vec<crate::sync::remote_object::VerifiedCandidateNonactivation>,
    ) -> Result<(), DbError> {
        let first = nonactivations.first().ok_or_else(|| {
            DbError::Message("Merge candidate conflict has no verified candidates".to_string())
        })?;
        let winner_commit = first
            .merge_winner_commit()
            .cloned()
            .map_err(|error| DbError::Message(error.to_string()))?;
        let winner_head = match first.proof() {
            crate::sync::remote_object::CandidateNonactivationProof::MergeWinner {
                winner_head,
            } => winner_head.clone(),
            crate::sync::remote_object::CandidateNonactivationProof::SerialImmediateSuccessor {
                ..
            } => {
                return Err(DbError::Message(
                    "Merge candidate conflict carries Serial evidence".to_string(),
                ));
            }
            crate::sync::remote_object::CandidateNonactivationProof::AuthorExclusion { .. } => {
                return Err(DbError::Message(
                    "Merge slot conflict cannot carry author-exclusion evidence".to_string(),
                ));
            }
            crate::sync::remote_object::CandidateNonactivationProof::MergeMembershipGrantRevocation { .. } => {
                return Err(DbError::Message(
                    "Merge slot conflict cannot carry membership-grant revocation evidence"
                        .to_string(),
                ));
            }
            crate::sync::remote_object::CandidateNonactivationProof::MergeDependencyRetraction { .. } => {
                return Err(DbError::Message(
                    "Merge slot conflict cannot carry dependent-retraction evidence".to_string(),
                ));
            }
        };
        let winner_proof = first.proof().clone();
        let nonactivations = nonactivations
            .into_iter()
            .map(|verified| {
                if verified
                    .merge_winner_commit()
                    .map_err(|error| DbError::Message(error.to_string()))?
                    != &winner_commit
                    || verified.proof() != &winner_proof
                {
                    return Err(DbError::Message(
                        "Merge candidate conflict observations name different winners".to_string(),
                    ));
                }
                verified
                    .candidate_reference()
                    .map(|reference| (reference, verified.into_durable()))
                    .map_err(|error| DbError::Message(error.to_string()))
            })
            .collect::<Result<std::collections::BTreeMap<_, _>, _>>()?;
        let statuses = self.state.write_statuses.clone();
        let notified_write_id = write_id.clone();
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let (raw_status, raw_prepared): (String, String) = tx
                .query_row(
                    "SELECT status, prepared FROM store_writes WHERE write_id = ?1",
                    [write_id.as_str()],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(DbError::from)?;
            let status: WriteStatus = serde_json::from_str(&raw_status)
                .map_err(|error| DbError::Message(format!("Merge candidate status: {error}")))?;
            if !matches!(status, WriteStatus::Publishing) {
                return Err(DbError::Message(format!(
                    "Merge candidate {write_id} is not publishing"
                )));
            }
            let prepared: PreparedStoreWriteState = serde_json::from_str(&raw_prepared)
                .map_err(|error| DbError::Message(format!("prepared Merge candidate: {error}")))?;
            let prepared_candidate = parse_prepared_merge_candidate_on(&tx, &prepared)?
                .ok_or_else(|| {
                    DbError::Message("Serial branch reached Merge candidate conflict".to_string())
                })?;
            let publication = parse_prepared_merge_publication_on(&tx, &prepared)?
                .expect("parsed Merge publication");
            if winner_head.object.slot() != publication.head_prepared.reference().slot()
                || winner_head.object == *publication.head_prepared.reference()
            {
                return Err(DbError::Message(
                    "Merge winner does not replace the prepared exact head slot".to_string(),
                ));
            }
            if prepared_candidate.commit.write_id != write_id {
                return Err(DbError::Message(
                    "prepared Merge graph differs from its write identity".to_string(),
                ));
            }
            if matches!(&prepared, PreparedStoreWriteState::MergeAbandonment { .. }) {
                let publication_nonactivation =
                    nonactivations.get(&publication.reference).ok_or_else(|| {
                        DbError::Message(
                            "Merge abandonment authority has no verified nonactivation".to_string(),
                        )
                    })?;
                begin_merge_candidate_nonactivation_on(
                    &tx,
                    &write_id,
                    &publication,
                    publication_nonactivation,
                    false,
                )?;
                if winner_commit != prepared_candidate.reference {
                    let candidate_nonactivation = nonactivations
                        .get(&prepared_candidate.reference)
                        .ok_or_else(|| {
                            DbError::Message(
                                "Merge abandonment candidate has no verified nonactivation"
                                    .to_string(),
                            )
                        })?;
                    begin_merge_candidate_nonactivation_on(
                        &tx,
                        &write_id,
                        &prepared_candidate,
                        candidate_nonactivation,
                        true,
                    )?;
                }
                let mut lost_preparation = prepared.clone();
                let PreparedStoreWriteState::MergeAbandonment { outcome, .. } =
                    &mut lost_preparation
                else {
                    unreachable!("matched Merge abandonment")
                };
                *outcome = MergeAbandonmentOutcome::Lost {
                    winner_commit: winner_commit.clone(),
                    winner_head: winner_head.clone(),
                };
                let lost_preparation =
                    serde_json::to_string(&lost_preparation).map_err(|error| {
                        DbError::Message(format!("serialize lost Merge abandonment: {error}"))
                    })?;
                let updated = tx
                    .execute(
                        "UPDATE store_writes SET prepared = ?2
                         WHERE write_id = ?1 AND prepared = ?3",
                        rusqlite::params![write_id.as_str(), lost_preparation, raw_prepared],
                    )
                    .map_err(DbError::from)?;
                if updated != 1 {
                    return Err(DbError::Message(
                        "Merge abandonment changed while recording its winner".to_string(),
                    ));
                }
            } else {
                let candidate_nonactivation = nonactivations
                    .get(&prepared_candidate.reference)
                    .ok_or_else(|| {
                        DbError::Message(
                            "Merge candidate has no verified nonactivation".to_string(),
                        )
                    })?;
                begin_merge_candidate_nonactivation_on(
                    &tx,
                    &write_id,
                    &prepared_candidate,
                    candidate_nonactivation,
                    true,
                )?;
            }
            let blocked = WriteStatus::Blocked(crate::WriteBlock::InvalidProtocolState {
                reason: format!(
                    "Merge successor slot is occupied by signed head {}",
                    winner_head.head_hash
                ),
            });
            Self::set_write_status_on(&tx, &write_id, &blocked)?;
            tx.commit().map_err(DbError::from)?;
            Self::notify_write_status_in(&statuses, &notified_write_id, blocked);
            Ok(())
        })
        .await
    }
}

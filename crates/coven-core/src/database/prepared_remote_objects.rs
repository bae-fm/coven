use crate::database::blob_records::load_prepared_audience_objects_on;
use crate::database::blob_records::remote_audience_to_db;
use crate::database::cloud_outbox_records::consume_created_upload_handoff_on;
use crate::database::remote_object_records::candidate_graph_exact_objects;
use crate::database::remote_object_records::load_remote_object_on;
use crate::database::remote_object_records::mark_remote_object_uploaded_on;
use crate::database::remote_object_records::mark_reusable_retained_authority_uploaded_on;
use crate::database::remote_object_records::merge_prepared_remote_object;
use crate::database::remote_object_records::persist_exact_remote_object_on;
use crate::database::remote_object_records::validate_prepared_blob_on;
use crate::database::remote_object_records::validate_prepared_package_on;
use crate::database::remote_object_records::validate_remote_object_on;
use crate::database::remote_object_records::RemoteStoredRepresentationRef;
use crate::database::store_device_exclusion_records::store_device_exclusion_journal_error;
use crate::database::store_reclaim_records::store_reclaim_journal_error;

use super::*;

impl Database {
    pub(crate) fn persist_prepared_audience_objects_on(
        conn: &Connection,
        write_id: &WriteId,
        packages: &[PreparedAudiencePackage],
        blobs: &[PreparedAudienceBlob],
    ) -> Result<(), DbError> {
        let package_audiences = packages
            .iter()
            .map(|prepared| {
                if prepared.package().write_id() != write_id {
                    return Err(DbError::Message(format!(
                        "prepared audience package write {} differs from journal write {write_id}",
                        prepared.package().write_id()
                    )));
                }
                Ok(prepared.package().audience().remote_audience())
            })
            .collect::<Result<std::collections::BTreeSet<_>, DbError>>()?;
        if package_audiences.len() != packages.len() {
            return Err(DbError::Message(format!(
                "write {write_id} has duplicate prepared package audiences"
            )));
        }
        for prepared in packages {
            let audience = prepared.package().audience().remote_audience();
            validate_remote_object_on(
                conn,
                prepared.remote_object_id(),
                prepared.object(),
                prepared.semantic_bytes(),
                RemoteStoredRepresentationRef::Inline(prepared.stored_bytes()),
            )?;
            conn.execute(
                "INSERT INTO store_write_packages
                 (write_id, audience, remote_object_id)
                 VALUES (?1, ?2, ?3)
                 ON CONFLICT(write_id, audience) DO NOTHING",
                rusqlite::params![
                    write_id.as_str(),
                    remote_audience_to_db(&audience),
                    prepared.remote_object_id().to_string(),
                ],
            )
            .map_err(DbError::from)?;
            validate_prepared_package_on(conn, write_id, prepared)?;
        }
        for prepared in blobs {
            if !package_audiences.contains(prepared.audience()) {
                return Err(DbError::Message(format!(
                    "write {write_id} has a prepared blob for {:?} without that audience's package",
                    prepared.audience()
                )));
            }
            let locator = prepared.blob().locator();
            validate_remote_object_on(
                conn,
                prepared.remote_object_id(),
                prepared.blob().object(),
                &locator.to_bytes(),
                RemoteStoredRepresentationRef::Blob,
            )?;
            let locator_hash = locator.locator_hash();
            let spool_path = prepared
                .spool_path()
                .map(|path| {
                    path.to_str().map(str::to_string).ok_or_else(|| {
                        DbError::Message("prepared blob spool path is not UTF-8".to_string())
                    })
                })
                .transpose()?;
            conn.execute(
                "INSERT INTO store_write_blobs
                 (write_id, audience, locator_hash, remote_object_id, spool_path)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(write_id, audience, remote_object_id) DO NOTHING",
                rusqlite::params![
                    write_id.as_str(),
                    remote_audience_to_db(prepared.audience()),
                    locator_hash.to_string(),
                    prepared.remote_object_id().to_string(),
                    spool_path,
                ],
            )
            .map_err(DbError::from)?;
            validate_prepared_blob_on(conn, write_id, prepared)?;
        }
        Ok(())
    }

    pub(super) fn persist_closed_write_objects_on(
        conn: &Connection,
        write_id: &WriteId,
        store_root_hash: ObjectHash,
        commit_ref: &StoreBatchCommitRef,
        commit: &StoreBatchCommit,
        commit_stored_bytes: &[u8],
        partitions: &PreparedStoreWritePartitions,
        remote_objects: &[RemoteObjectRecord],
        audiences: &PreparedAudienceObjects,
    ) -> Result<(), DbError> {
        let mut object_ids = std::collections::BTreeSet::new();
        for remote in remote_objects {
            remote
                .validate()
                .map_err(|error| DbError::Message(format!("prepared remote object: {error}")))?;
            if !object_ids.insert(remote.object_id()) {
                return Err(DbError::Message(
                    "prepared write contains a duplicate remote object".to_string(),
                ));
            }
        }
        validate_prepared_audience_blob_graph(&object_ids, audiences)?;
        for remote in remote_objects {
            let object_id = remote.object_id();
            let existing = conn
                .query_row(
                    "SELECT state FROM remote_objects WHERE object_id = ?1",
                    [object_id.to_string()],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .map_err(DbError::from)?;
            let persisted = match existing {
                Some(state) => {
                    let existing: RemoteObjectRecord = serde_json::from_str(&state).map_err(
                        |error| {
                            DbError::Message(format!(
                                "prepared remote object {object_id} has invalid closed state: {error}"
                            ))
                        },
                    )?;
                    merge_prepared_remote_object(existing, remote, commit_ref)?
                }
                None => remote.clone(),
            };
            let state = serde_json::to_string(&persisted).map_err(|error| {
                DbError::Message(format!("serialize closed remote object: {error}"))
            })?;
            conn.execute(
                "INSERT INTO remote_objects (object_id, state) VALUES (?1, ?2)
                 ON CONFLICT(object_id) DO UPDATE SET state = excluded.state",
                (object_id.to_string(), state),
            )
            .map_err(DbError::from)?;
        }
        let commit_remote = RemoteObjectRecord::candidate_commit(
            commit_ref.clone(),
            commit.to_bytes(),
            commit_stored_bytes.to_vec(),
        )
        .map_err(|error| DbError::Message(format!("prepared candidate commit: {error}")))?;
        persist_exact_remote_object_on(conn, &commit_remote, "candidate commit")?;
        let expected_partition_count = usize::from(partitions.store.is_some())
            .checked_add(partitions.circles.len())
            .ok_or_else(|| DbError::Message("audience partition count overflow".to_string()))?;
        if audiences.packages.len() != expected_partition_count {
            return Err(DbError::Message(
                "prepared audience packages do not cover every write partition".to_string(),
            ));
        }
        let mut indexed = std::collections::BTreeSet::new();
        for package in &audiences.packages {
            let value = package.package();
            if value.store_root_hash() != store_root_hash
                || value.write_id() != write_id
                || value.commit_coord() != &commit_ref.coord
                || value.candidate_family() != commit.candidate_family()
            {
                return Err(DbError::Message(
                    "prepared audience package differs from its exact Store commit".to_string(),
                ));
            }
            match value.audience() {
                crate::sync::audience_package::PackageAudience::Store => {
                    let partition = partitions.store.as_ref().ok_or_else(|| {
                        DbError::Message(
                            "prepared Store package has no Store partition".to_string(),
                        )
                    })?;
                    if value.changeset() != partition.changeset {
                        return Err(DbError::Message(
                            "prepared Store package changeset differs from its partition"
                                .to_string(),
                        ));
                    }
                    commit
                        .verify_store_package(package.semantic_bytes())
                        .map_err(|error| DbError::Message(error.to_string()))?;
                }
                crate::sync::audience_package::PackageAudience::Circle { circle_id, .. } => {
                    let partition = partitions
                        .circles
                        .iter()
                        .find(|partition| partition.audience == Audience::Circle(*circle_id))
                        .ok_or_else(|| {
                            DbError::Message(format!(
                                "prepared Circle package {circle_id} has no partition"
                            ))
                        })?;
                    if value.changeset() != partition.changeset {
                        return Err(DbError::Message(format!(
                            "prepared Circle package {circle_id} changeset differs from its partition"
                        )));
                    }
                    commit
                        .verify_circle_package(*circle_id, package.semantic_bytes())
                        .map_err(|error| DbError::Message(error.to_string()))?;
                }
            }
            indexed.insert(package.remote_object_id());
        }
        indexed.extend(
            audiences
                .blobs
                .iter()
                .map(PreparedAudienceBlob::remote_object_id),
        );
        debug_assert_eq!(indexed, object_ids);
        Self::persist_prepared_audience_objects_on(
            conn,
            write_id,
            &audiences.packages,
            &audiences.blobs,
        )
    }

    pub(super) fn validate_loaded_write_objects(
        write_id: &WriteId,
        commit_ref: &StoreBatchCommitRef,
        commit: &StoreBatchCommit,
        partitions: &PreparedStoreWritePartitions,
        audiences: &PreparedAudienceObjects,
    ) -> Result<(), DbError> {
        let expected_package_count = usize::from(commit.store_package().is_some())
            .checked_add(commit.circle_packages().len())
            .ok_or_else(|| DbError::Message("package count overflow".to_string()))?;
        let partition_count = usize::from(partitions.store.is_some())
            .checked_add(partitions.circles.len())
            .ok_or_else(|| DbError::Message("audience partition count overflow".to_string()))?;
        if audiences.packages.len() != expected_package_count
            || audiences.packages.len() != partition_count
        {
            return Err(DbError::Message(
                "prepared package indexes do not exactly cover commit audiences".to_string(),
            ));
        }
        for package in &audiences.packages {
            let value = package.package();
            if value.write_id() != write_id
                || value.commit_coord() != &commit_ref.coord
                || value.candidate_family() != commit.candidate_family()
            {
                return Err(DbError::Message(
                    "indexed audience package differs from its exact commit".to_string(),
                ));
            }
            let expected_object = match value.audience() {
                crate::sync::audience_package::PackageAudience::Store => {
                    commit
                        .verify_store_package(package.semantic_bytes())
                        .map_err(|error| DbError::Message(error.to_string()))?;
                    &commit
                        .store_package()
                        .as_ref()
                        .expect("verified present")
                        .object
                }
                crate::sync::audience_package::PackageAudience::Circle { circle_id, .. } => {
                    commit
                        .verify_circle_package(*circle_id, package.semantic_bytes())
                        .map_err(|error| DbError::Message(error.to_string()))?;
                    &commit
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
                    "indexed audience package exact object differs from its commit".to_string(),
                ));
            }
            value
                .validate_blob_uploader(&commit.author_registration)
                .map_err(|error| DbError::Message(error.to_string()))?;
        }
        validate_prepared_audience_blob_bindings(audiences)
    }

    pub(crate) fn activate_prepared_write_on(
        conn: &rusqlite::Transaction<'_>,
        root: &crate::sync::store_commit::StoreRootRef,
        gates: &Gates,
        synced_tables: &[SyncedTable],
        write_id: &WriteId,
        commit: &StoreBatchCommit,
        commit_ref: &StoreBatchCommitRef,
        materialization: PreparedWriteMaterialization<'_>,
        local_cleanup: StoreBatchLocalCleanup,
        additional_object_ids: &[ObjectHash],
    ) -> Result<(), DbError> {
        commit_ref
            .verify_commit(commit)
            .map_err(|error| DbError::Message(error.to_string()))?;
        let audiences = load_prepared_audience_objects_on(conn, write_id)?;
        let retained_packages = audiences
            .packages
            .iter()
            .map(|package| package.package().clone())
            .collect::<Vec<_>>();
        for package in &audiences.packages {
            package
                .package()
                .validate_blob_uploader(&commit.author_registration)
                .map_err(|error| DbError::Message(error.to_string()))?;
        }
        let mut object_ids = std::collections::BTreeSet::new();
        object_ids.insert(remote_object_id(&commit_ref.object));
        object_ids.extend(
            candidate_graph_exact_objects(commit)?
                .iter()
                .map(remote_object_id),
        );
        object_ids.extend(
            audiences
                .blobs
                .iter()
                .map(PreparedAudienceBlob::remote_object_id),
        );
        object_ids.extend(additional_object_ids.iter().copied());
        for object_id in object_ids {
            let remote = load_remote_object_on(conn, object_id)?
                .into_activated(commit_ref)
                .map_err(|error| {
                    DbError::Message(format!("activate remote object {object_id}: {error}"))
                })?;
            let state = serde_json::to_string(&remote).map_err(|error| {
                DbError::Message(format!("serialize activated remote object: {error}"))
            })?;
            let updated = conn
                .execute(
                    "UPDATE remote_objects SET state = ?2 WHERE object_id = ?1",
                    (object_id.to_string(), state),
                )
                .map_err(DbError::from)?;
            if updated != 1 {
                return Err(DbError::Message(format!(
                    "remote object {object_id} disappeared during activation"
                )));
            }
        }
        let activation = BlobActivation {
            coord: commit_ref.coord.clone(),
        };
        let apply_schema = crate::sync::conflict::TableSchema::for_apply(
            conn,
            synced_tables,
            gates,
            commit.order.policy(),
        )?;
        let mut consumed_uploads = 0;
        for package in &audiences.packages {
            let winning_rows = crate::sync::apply::current_winning_rows_with_schema(
                conn,
                &apply_schema,
                package.package().changeset(),
            )?;
            Self::install_winning_blob_bindings_on(
                conn,
                gates,
                synced_tables,
                package.package(),
                &activation,
                &winning_rows,
            )?;
            for binding in package.package().blob_bindings() {
                if consume_created_upload_handoff_on(conn, package.package(), binding)? {
                    consumed_uploads += 1;
                }
            }
        }
        match Self::make_remote_publication_root_on(conn, write_id)? {
            Some((root_table, root_id)) => {
                if consumed_uploads == 0 {
                    return Err(DbError::Message(format!(
                        "make_remote publication {write_id} for {root_table:?}/{root_id:?} contains no Created upload handoff"
                    )));
                }
                let remaining: i64 = conn
                    .query_row(
                        "SELECT COUNT(*) FROM cloud_outbox
                         WHERE operation = 'upload' AND root_table = ?1 AND root_id = ?2",
                        (&root_table, &root_id),
                        |row| row.get(0),
                    )
                    .map_err(DbError::from)?;
                if remaining != 0 {
                    return Err(DbError::Message(format!(
                        "make_remote publication {write_id} left {remaining} upload handoff(s) for {root_table:?}/{root_id:?}"
                    )));
                }
                Self::complete_make_remote_publication_on(conn, write_id)?;
            }
            None if consumed_uploads != 0 => {
                return Err(DbError::Message(format!(
                    "Store write {write_id} consumed Created upload handoffs without a make_remote publication intent"
                )));
            }
            None => {}
        }
        match materialization {
            PreparedWriteMaterialization::MergeConcurrent {
                head,
                head_object,
                history_summary,
            } => {
                Self::record_materialized_merge_commit_on(
                    conn,
                    root,
                    commit,
                    commit_ref,
                    &[],
                    head,
                    head_object,
                    history_summary,
                    &retained_packages,
                    (!retained_packages.is_empty())
                        .then_some(RetainedPackageApplication::LocallyAuthored),
                )?;
            }
            PreparedWriteMaterialization::Serial => {
                let device_operations = VerifiedStoreDeviceOperations::without_exclusions(commit)
                    .map_err(|error| DbError::Message(error.to_string()))?;
                let stream_activations = VerifiedStreamActivations::none(commit, commit_ref)
                    .map_err(|error| DbError::Message(error.to_string()))?;
                Self::record_materialized_commit_with_device_operations_on(
                    conn,
                    commit,
                    commit_ref,
                    &device_operations,
                    &stream_activations,
                    MaterializedCommitRetention::Serial,
                    &ReclaimCommitActivation::serial(commit_ref.clone())
                        .map_err(store_reclaim_journal_error)?,
                )?;
            }
        }
        for drop in local_cleanup.drops {
            conn.execute(
                "INSERT INTO published_blob_drop_intents
                 (seq, namespace, blob_id, size, plaintext_hash, locator_hash, disposition)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(seq, namespace, blob_id, locator_hash) DO NOTHING",
                rusqlite::params![
                    Self::sequence_to_sqlite(
                        &match &commit_ref.coord {
                            StoreCommitCoord::MergeConcurrent { stream_id, .. } => {
                                stream_id.to_string()
                            }
                            StoreCommitCoord::Serial { .. } => SERIAL_STREAM_ID.to_string(),
                        },
                        commit_ref.coord.sequence(),
                    )?,
                    drop.namespace,
                    drop.id,
                    i64::try_from(drop.size).map_err(|_| DbError::Message(
                        "outbound local cleanup size exceeds SQLite integer".to_string()
                    ))?,
                    drop.plaintext_hash.to_string(),
                    drop.locator_hash.to_string(),
                    drop.disposition.as_db(),
                ],
            )
            .map_err(DbError::from)?;
        }
        conn.execute(
            "DELETE FROM store_write_packages WHERE write_id = ?1",
            [write_id.as_str()],
        )
        .map_err(DbError::from)?;
        conn.execute(
            "DELETE FROM store_write_blobs WHERE write_id = ?1",
            [write_id.as_str()],
        )
        .map_err(DbError::from)?;
        conn.execute(
            "DELETE FROM store_write_blob_leases WHERE write_id = ?1",
            [write_id.as_str()],
        )
        .map_err(DbError::from)?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn prepared_audience_objects(
        &self,
        write_id: &WriteId,
    ) -> Result<PreparedAudienceObjects, DbError> {
        let write_id = write_id.clone();
        let loaded = self
            .call(move |conn| load_prepared_audience_objects_on(conn, &write_id))
            .await?;

        let mut verified_blobs = Vec::with_capacity(loaded.blobs.len());
        for prepared in loaded.blobs {
            if let Some(spool_path) = prepared.spool_path() {
                crate::local_blob::verify_exact_file(prepared.blob().object(), spool_path)
                    .await
                    .map_err(|error| DbError::Message(format!("prepared blob spool: {error}")))?;
            }
            verified_blobs.push(prepared);
        }
        Ok(PreparedAudienceObjects {
            packages: loaded.packages,
            blobs: verified_blobs,
        })
    }

    pub(crate) async fn prepared_remote_objects(
        &self,
        write_id: &WriteId,
    ) -> Result<Vec<PreparedRemoteObject>, DbError> {
        let write_id = write_id.clone();
        self.call(move |conn| {
            let raw_prepared: String = conn
                .query_row(
                    "SELECT prepared FROM store_writes WHERE write_id = ?1",
                    [write_id.as_str()],
                    |row| row.get(0),
                )
                .map_err(DbError::from)?;
            let prepared: PreparedStoreWriteState = serde_json::from_str(&raw_prepared)
                .map_err(|error| DbError::Message(format!("prepared remote graph: {error}")))?;
            let commit = match &prepared {
                PreparedStoreWriteState::MergeConcurrent { .. }
                | PreparedStoreWriteState::MergeAbandonment { .. } => {
                    parse_prepared_merge_candidate_on(conn, &prepared)?
                        .ok_or_else(|| {
                            DbError::Message("prepared Merge candidate graph is absent".to_string())
                        })?
                        .commit
                }
                PreparedStoreWriteState::Serial { .. } => {
                    parse_prepared_serial_candidate(&raw_prepared)?
                        .ok_or_else(|| {
                            DbError::Message(
                                "prepared Serial candidate graph is absent".to_string(),
                            )
                        })?
                        .commit
                }
                PreparedStoreWriteState::SerialPreparing => {
                    return Err(DbError::Message(
                        "Serial write has no prepared candidate graph".to_string(),
                    ));
                }
            };
            let mut ids = candidate_graph_exact_objects(&commit)?
                .iter()
                .map(|object| (remote_object_id(object).to_string(), None))
                .collect::<Vec<_>>();
            let mut statement = conn
                .prepare(
                    "SELECT remote_object_id, spool_path
                     FROM store_write_blobs WHERE write_id = ?1
                     ORDER BY remote_object_id",
                )
                .map_err(DbError::from)?;
            let blobs = statement
                .query_map([write_id.as_str()], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
                })
                .map_err(DbError::from)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(DbError::from)?;
            ids.extend(blobs);
            ids.sort_by(|left, right| left.0.cmp(&right.0));
            ids.into_iter()
                .map(|(encoded, spool_path)| {
                    let id = encoded.parse().map_err(|error| {
                        DbError::Message(format!("prepared remote object id: {error}"))
                    })?;
                    Ok(PreparedRemoteObject {
                        record: load_remote_object_on(conn, id)?,
                        spool_path: spool_path.map(PathBuf::from),
                    })
                })
                .collect()
        })
        .await
    }

    pub(crate) async fn mark_remote_object_uploaded(
        &self,
        expected: RemoteObjectRecord,
    ) -> Result<RemoteObjectRecord, DbError> {
        self.call(move |conn| mark_remote_object_uploaded_on(conn, expected))
            .await
    }

    pub(crate) async fn mark_reusable_retained_authority_uploaded(
        &self,
        expected: RemoteObjectRecord,
    ) -> Result<RemoteObjectRecord, DbError> {
        self.call(move |conn| mark_reusable_retained_authority_uploaded_on(conn, expected))
            .await
    }

    pub(crate) async fn mark_store_device_exclusion_authority_uploaded(
        &self,
        operation: DurableStoreDeviceExclusionOperation,
    ) -> Result<(), DbError> {
        let expected = operation
            .authority_remote_object()
            .map_err(store_device_exclusion_journal_error)?;
        let candidate = operation
            .candidate()
            .ok_or_else(|| {
                DbError::Message(
                    "Store-device exclusion authority has no current candidate".to_string(),
                )
            })?
            .reference
            .clone();
        self.call(move |conn| {
            let object_id = expected.object_id();
            let current = load_remote_object_on(conn, object_id)?;
            let (
                RemoteObjectRecord::RetainedAuthority(expected_record),
                RemoteObjectRecord::RetainedAuthority(current_record),
            ) = (&expected, &current)
            else {
                return Err(DbError::Message(
                    "Store-device exclusion authority is not retained authority".to_string(),
                ));
            };
            if expected_record.identity != current_record.identity
                || expected_record.bytes != current_record.bytes
            {
                return Err(DbError::Message(
                    "Store-device exclusion authority changed before upload completion".to_string(),
                ));
            }
            match &current_record.state {
                crate::sync::remote_object::RetainedAuthorityObjectState::Prepared {
                    ownership,
                } if ownership.pending.contains(&candidate) => {
                    mark_remote_object_uploaded_on(conn, current)?;
                }
                crate::sync::remote_object::RetainedAuthorityObjectState::UploadedVerified {
                    ownership,
                } if ownership.pending.contains(&candidate) => {}
                _ => {
                    return Err(DbError::Message(
                        "Store-device exclusion authority does not belong to its current candidate"
                            .to_string(),
                    ));
                }
            }
            Ok(())
        })
        .await
    }

    #[cfg(test)]
    pub(crate) async fn protocol_inert_object(
        &self,
        object: ExactObjectRef,
    ) -> Result<Option<crate::sync::remote_object::ProtocolInertObject>, DbError> {
        self.call(move |conn| {
            let object_id = remote_object_id(&object);
            let exists: bool = conn
                .query_row(
                    "SELECT EXISTS(
                         SELECT 1 FROM protocol_inert_objects WHERE object_id = ?1
                     )",
                    [object_id.to_string()],
                    |row| row.get(0),
                )
                .map_err(DbError::from)?;
            exists
                .then(|| load_protocol_inert_object_on(conn, object_id))
                .transpose()
        })
        .await
    }

    pub(crate) async fn mark_candidate_commit_uploaded(
        &self,
        commit: StoreBatchCommitRef,
    ) -> Result<(), DbError> {
        self.call(move |conn| {
            let object_id = remote_object_id(&commit.object);
            let current = load_remote_object_on(conn, object_id)?;
            if matches!(
                &current,
                RemoteObjectRecord::RetainedAuthority(record)
                    if matches!(
                        &record.identity.domain,
                        crate::sync::remote_object::RetainedAuthorityObjectDomain::Commit {
                            reference
                        } if reference == &commit
                    ) && matches!(
                        &record.state,
                        crate::sync::remote_object::RetainedAuthorityObjectState::UploadedVerified {
                            ownership
                        } if ownership.activated.contains(&commit)
                    )
            ) {
                return Ok(());
            }
            if !matches!(&current, RemoteObjectRecord::CandidateCommit(record) if record.identity == commit)
            {
                return Err(DbError::Message(format!(
                    "remote object {object_id} is not the exact candidate commit"
                )));
            }
            mark_remote_object_uploaded_on(conn, current)?;
            Ok(())
        })
        .await
    }

    pub(crate) async fn mark_store_head_uploaded(
        &self,
        head: crate::sync::store_commit::StoreDeviceHeadRef,
    ) -> Result<(), DbError> {
        self.call(move |conn| {
            let object_id = remote_object_id(&head.object);
            let current = load_remote_object_on(conn, object_id)?;
            if !matches!(
                &current,
                RemoteObjectRecord::RetainedAuthority(record)
                    if matches!(
                        &record.identity.domain,
                        crate::sync::remote_object::RetainedAuthorityObjectDomain::DeviceHead {
                            reference
                        } if reference == &head
                    )
            ) {
                return Err(DbError::Message(format!(
                    "remote object {object_id} is not the exact prepared Store head"
                )));
            }
            mark_remote_object_uploaded_on(conn, current)?;
            Ok(())
        })
        .await
    }
}

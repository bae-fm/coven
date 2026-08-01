use crate::database::*;
use crate::protocol::audience_package::AudiencePackage;
use crate::protocol::circle::Audience;
use crate::protocol::remote_object::{remote_object_id, RemoteObjectRecord};
use crate::protocol::store_commit::{ObjectHash, StoreBatchCommit, StoreBatchCommitRef};
#[cfg(test)]
use crate::storage::ExactObjectRef;
use crate::sync::gate::Gates;
use crate::sync::session::SyncedTable;
use crate::write::WriteId;
use rusqlite::{Connection, OptionalExtension};
use std::path::PathBuf;

use super::candidate_records::parse_prepared_merge_candidate_on;
use super::publication_state::PreparedStoreWriteState;
use super::*;

struct UploadedBlobSpool {
    write_id: WriteId,
    remote_object_id: ObjectHash,
    path: PathBuf,
}

impl UploadedBlobSpool {
    async fn retire(&self) -> Result<(), String> {
        match tokio::fs::remove_file(&self.path).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(format!(
                    "remove uploaded prepared blob spool {}: {error}",
                    self.path.display()
                ))
            }
        }
        let parent = self
            .path
            .parent()
            .ok_or_else(|| format!("blob spool has no parent: {}", self.path.display()))?;
        tokio::fs::File::open(parent)
            .await
            .map_err(|error| format!("open blob spool parent {}: {error}", parent.display()))?
            .sync_all()
            .await
            .map_err(|error| format!("sync blob spool parent {}: {error}", parent.display()))
    }
}

impl StoreDatabase {
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

    pub(crate) fn persist_closed_write_objects_on(
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
            persist_prepared_remote_object_on(
                conn,
                remote,
                commit_ref,
                "candidate audience object",
            )?;
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
                crate::protocol::audience_package::PackageAudience::Store => {
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
                crate::protocol::audience_package::PackageAudience::Circle {
                    circle_id, ..
                } => {
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

    pub(crate) fn activate_prepared_write_on(
        conn: &rusqlite::Transaction<'_>,
        gates: &Gates,
        synced_tables: &[SyncedTable],
        write_id: &WriteId,
        verified_commit: &crate::protocol::store_commit::VerifiedStoreBatchCommit,
        local_cleanup: StoreBatchLocalCleanup,
        additional_object_ids: &[ObjectHash],
    ) -> Result<Vec<AudiencePackage>, DbError> {
        let commit = verified_commit.value();
        let commit_ref = verified_commit.reference();
        let remaining_spools: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM store_write_blobs
                 WHERE write_id = ?1 AND spool_path IS NOT NULL",
                [write_id.as_str()],
                |row| row.get(0),
            )
            .map_err(DbError::from)?;
        if remaining_spools != 0 {
            return Err(DbError::Message(format!(
                "prepared write {write_id} retains {remaining_spools} uploaded blob spool(s)"
            )));
        }
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
        let apply_schema = crate::database::TableSchema::for_apply(conn, synced_tables, gates)?;
        let store_transaction = MergeMaterializationTransaction::new(conn);
        let cloud_outbox = CloudOutboxRecords::new(conn);
        let mut consumed_uploads = 0;
        for package in &audiences.packages {
            let winning_rows = store_transaction
                .current_winning_rows(&apply_schema, package.package().changeset())?;
            Database::install_winning_blob_bindings_on(
                conn,
                gates,
                synced_tables,
                package.package(),
                &activation,
                &winning_rows,
            )?;
            for binding in package.package().blob_bindings() {
                if cloud_outbox.consume_created_upload_handoff(package.package(), binding)? {
                    consumed_uploads += 1;
                }
            }
        }
        match Database::make_remote_publication_root_on(conn, write_id)? {
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
                Database::complete_make_remote_publication_on(conn, write_id)?;
            }
            None if consumed_uploads != 0 => {
                return Err(DbError::Message(format!(
                    "Store write {write_id} consumed Created upload handoffs without a make_remote publication intent"
                )));
            }
            None => {}
        }
        for drop in local_cleanup.drops {
            conn.execute(
                "INSERT INTO published_blob_drop_intents
                 (seq, namespace, blob_id, size, plaintext_hash, locator_hash, disposition)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(seq, namespace, blob_id, locator_hash) DO NOTHING",
                rusqlite::params![
                    Database::sequence_to_sqlite(
                        &commit_ref.coord.stream_id.to_string(),
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
        Ok(retained_packages)
    }

    #[cfg(test)]
    pub(crate) async fn prepared_audience_objects(
        &self,
        write_id: &WriteId,
    ) -> Result<PreparedAudienceObjects, DbError> {
        let write_id = write_id.clone();
        let loaded = self
            .connection
            .call(move |conn| load_prepared_audience_objects_on(conn, &write_id))
            .await?;

        let mut verified_blobs = Vec::with_capacity(loaded.blobs.len());
        for prepared in loaded.blobs {
            if let Some(spool_path) = prepared.spool_path() {
                prepared
                    .blob()
                    .object()
                    .verify_file(spool_path)
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
        self.connection
            .call(move |conn| {
                let raw_prepared: String = conn
                    .query_row(
                        "SELECT prepared FROM store_writes WHERE write_id = ?1",
                        [write_id.as_str()],
                        |row| row.get(0),
                    )
                    .map_err(DbError::from)?;
                let prepared: PreparedStoreWriteState = serde_json::from_str(&raw_prepared)
                    .map_err(|error| DbError::Message(format!("prepared remote graph: {error}")))?;
                let commit = parse_prepared_merge_candidate_on(conn, &prepared)?.commit;
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
        self.connection
            .call(move |conn| mark_remote_object_uploaded_on(conn, expected))
            .await
    }

    pub(crate) async fn retire_uploaded_blob_spools(&self) -> Result<(), DbError> {
        let spools = self
            .connection
            .call(|conn| {
                let mut statement = conn
                    .prepare(
                        "SELECT write_id, remote_object_id, spool_path
                         FROM store_write_blobs
                         WHERE spool_path IS NOT NULL
                         ORDER BY write_id, audience, remote_object_id",
                    )
                    .map_err(DbError::from)?;
                let rows = statement
                    .query_map([], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                        ))
                    })
                    .map_err(DbError::from)?;
                let rows = rows.collect::<Result<Vec<_>, _>>().map_err(DbError::from)?;
                drop(statement);
                let mut spools = Vec::new();
                for (write_id, remote_object_id, path) in rows {
                    let remote_object_id = remote_object_id.parse().map_err(|error| {
                        DbError::Message(format!("prepared blob remote object id: {error}"))
                    })?;
                    let remote = load_remote_object_on(conn, remote_object_id)?;
                    if remote_object_is_uploaded(&remote) {
                        spools.push(UploadedBlobSpool {
                            write_id: WriteId::from_generated(write_id),
                            remote_object_id,
                            path: PathBuf::from(path),
                        });
                    }
                }
                Ok(spools)
            })
            .await?;

        for spool in spools {
            spool.retire().await.map_err(DbError::Message)?;
            self.clear_uploaded_blob_spool(spool).await?;
        }
        Ok(())
    }

    async fn clear_uploaded_blob_spool(&self, spool: UploadedBlobSpool) -> Result<(), DbError> {
        self.connection
            .call(move |conn| {
                let remote = load_remote_object_on(conn, spool.remote_object_id)?;
                if !remote_object_is_uploaded(&remote) {
                    return Err(DbError::Message(format!(
                        "prepared blob {} lost uploaded state before spool retirement",
                        spool.remote_object_id
                    )));
                }
                let path = spool.path.to_str().ok_or_else(|| {
                    DbError::Message("prepared blob spool path is not UTF-8".to_string())
                })?;
                let cleared = conn
                    .execute(
                        "UPDATE store_write_blobs SET spool_path = NULL
                         WHERE write_id = ?1 AND remote_object_id = ?2 AND spool_path = ?3",
                        rusqlite::params![
                            spool.write_id.as_str(),
                            spool.remote_object_id.to_string(),
                            path,
                        ],
                    )
                    .map_err(DbError::from)?;
                if cleared != 1 {
                    let current = conn
                        .query_row(
                            "SELECT spool_path FROM store_write_blobs
                             WHERE write_id = ?1 AND remote_object_id = ?2",
                            rusqlite::params![
                                spool.write_id.as_str(),
                                spool.remote_object_id.to_string(),
                            ],
                            |row| row.get::<_, Option<String>>(0),
                        )
                        .optional()
                        .map_err(DbError::from)?;
                    if current.flatten().is_some() {
                        return Err(DbError::Message(format!(
                            "prepared blob {} spool changed during retirement",
                            spool.remote_object_id
                        )));
                    }
                }
                Ok(())
            })
            .await
    }

    pub(crate) async fn mark_reusable_retained_authority_uploaded(
        &self,
        expected: RemoteObjectRecord,
    ) -> Result<RemoteObjectRecord, DbError> {
        self.connection
            .call(move |conn| mark_reusable_retained_authority_uploaded_on(conn, expected))
            .await
    }

    #[cfg(test)]
    pub(crate) async fn protocol_inert_object(
        &self,
        object: ExactObjectRef,
    ) -> Result<Option<crate::protocol::remote_object::ProtocolInertObject>, DbError> {
        self.connection
            .call(move |conn| {
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
        self.connection.call(move |conn| {
            let object_id = remote_object_id(&commit.object);
            let current = load_remote_object_on(conn, object_id)?;
            if matches!(
                &current,
                RemoteObjectRecord::RetainedAuthority(record)
                    if matches!(
                        &record.identity.domain,
                        crate::protocol::remote_object::RetainedAuthorityObjectDomain::Commit {
                            reference
                        } if reference == &commit
                    ) && matches!(
                        &record.state,
                        crate::protocol::remote_object::RetainedAuthorityObjectState::UploadedVerified {
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
        head: crate::protocol::store_commit::StoreDeviceHeadRef,
    ) -> Result<(), DbError> {
        self.connection
            .call(move |conn| {
                let object_id = remote_object_id(&head.object);
                let current = load_remote_object_on(conn, object_id)?;
                if !matches!(
                    &current,
                    RemoteObjectRecord::RetainedAuthority(record)
                        if matches!(
                            &record.identity.domain,
                            crate::protocol::remote_object::RetainedAuthorityObjectDomain::DeviceHead {
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

fn remote_object_is_uploaded(remote: &RemoteObjectRecord) -> bool {
    match remote {
        RemoteObjectRecord::CandidateCommit(record) => matches!(
            &record.state,
            crate::protocol::remote_object::CandidateCommitState::UploadedVerified
        ),
        RemoteObjectRecord::CandidateExclusive(record) => matches!(
            &record.state,
            crate::protocol::remote_object::CandidateObjectState::UploadedVerified { .. }
        ),
        RemoteObjectRecord::RetainedAuthority(record) => matches!(
            &record.state,
            crate::protocol::remote_object::RetainedAuthorityObjectState::UploadedVerified { .. }
        ),
        RemoteObjectRecord::SharedLiveSet(record) => matches!(
            &record.state,
            crate::protocol::remote_object::OwnedObjectState::UploadedVerified { .. }
        ),
    }
}

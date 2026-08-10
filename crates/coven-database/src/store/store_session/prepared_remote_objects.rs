use crate::*;
#[cfg(any(test, feature = "test-utils"))]
use coven_protocol::objects::ExactObjectRef;
use coven_protocol::remote_object::{remote_object_id, RemoteObjectRecord};
use coven_protocol::store_commit::{ObjectHash, StoreBatchCommitRef};
use coven_protocol::write::WriteId;
use rusqlite::OptionalExtension;
use std::path::PathBuf;

use super::publication_state::PreparedStoreWriteState;
use super::*;

struct UploadedBlobSpool {
    write_id: WriteId,
    remote_object_id: ObjectHash,
    path: PathBuf,
}

impl UploadedBlobSpool {
    async fn retire(&self, database: &StoreDatabase) -> Result<(), String> {
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
        database.sync_store_parent_dir(&self.path).await
    }
}

impl StoreSession<'_> {
    fn prepared_remote_objects(
        &mut self,
        write_id: &WriteId,
    ) -> Result<Vec<PreparedRemoteObject>, DbError> {
        let records = crate::store::store_session::StoreRecords::new(self.conn, self.store_dir);
        let raw_prepared: String = self
            .conn
            .query_row(
                "SELECT prepared FROM store_writes WHERE write_id = ?1",
                [write_id.as_str()],
                |row| row.get(0),
            )
            .map_err(DbError::from)?;
        let prepared: PreparedStoreWriteState = serde_json::from_str(&raw_prepared)
            .map_err(|error| DbError::context("prepared remote graph", error))?;
        let commit = self
            .verified_store_authority
            .prepared_merge_candidate_on(records, &prepared)?
            .commit;
        let mut ids = candidate_graph_exact_objects(&commit)?
            .iter()
            .map(|object| (remote_object_id(object).to_string(), None))
            .collect::<Vec<_>>();
        let mut statement = self
            .conn
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
                let id = encoded
                    .parse()
                    .map_err(|error| DbError::context("prepared remote object id", error))?;
                Ok(PreparedRemoteObject {
                    closed: crate::reopen_remote_object_on(self.conn, self.store_dir, id)?,
                    spool_path: spool_path.map(PathBuf::from),
                })
            })
            .collect()
    }

    fn mark_remote_object_uploaded(
        &self,
        expected: RemoteObjectRecord,
    ) -> Result<RemoteObjectRecord, DbError> {
        mark_remote_object_uploaded_on(self.conn, expected)
    }

    fn uploaded_blob_spools(&self) -> Result<Vec<UploadedBlobSpool>, DbError> {
        let conn = self.conn;
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
            let remote_object_id = remote_object_id
                .parse()
                .map_err(|error| DbError::context("prepared blob remote object id", error))?;
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
    }

    fn clear_uploaded_blob_spool(&self, spool: UploadedBlobSpool) -> Result<(), DbError> {
        let conn = self.conn;
        let remote = load_remote_object_on(conn, spool.remote_object_id)?;
        if !remote_object_is_uploaded(&remote) {
            return Err(DbError::Message(format!(
                "prepared blob {} lost uploaded state before spool retirement",
                spool.remote_object_id
            )));
        }
        let path = spool
            .path
            .to_str()
            .ok_or_else(|| DbError::Message("prepared blob spool path is not UTF-8".to_string()))?;
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
                    rusqlite::params![spool.write_id.as_str(), spool.remote_object_id.to_string(),],
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
    }

    fn mark_reusable_retained_authority_uploaded(
        &self,
        expected: RemoteObjectRecord,
    ) -> Result<RemoteObjectRecord, DbError> {
        mark_reusable_retained_authority_uploaded_on(self.conn, expected)
    }

    fn mark_candidate_commit_uploaded(&self, commit: StoreBatchCommitRef) -> Result<(), DbError> {
        let conn = self.conn;
        let object_id = remote_object_id(&commit.object);
        let current = load_remote_object_on(conn, object_id)?;
        if matches!(
            &current,
            RemoteObjectRecord::RetainedAuthority(record)
                if matches!(
                    &record.identity.domain,
                    coven_protocol::remote_object::RetainedAuthorityObjectDomain::Commit {
                        reference
                    } if reference == &commit
                ) && matches!(
                    &record.state,
                    coven_protocol::remote_object::RetainedAuthorityObjectState::UploadedVerified {
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
    }

    fn mark_store_head_uploaded(
        &self,
        head: coven_protocol::store_commit::StoreDeviceHeadRef,
    ) -> Result<(), DbError> {
        let conn = self.conn;
        let object_id = remote_object_id(&head.object);
        let current = load_remote_object_on(conn, object_id)?;
        if !matches!(
            &current,
            RemoteObjectRecord::RetainedAuthority(record)
                if matches!(
                    &record.identity.domain,
                    coven_protocol::remote_object::RetainedAuthorityObjectDomain::DeviceHead {
                        reference,
                        ..
                    } if reference == &head
                )
        ) {
            return Err(DbError::Message(format!(
                "remote object {object_id} is not the exact prepared Store head"
            )));
        }
        mark_remote_object_uploaded_on(conn, current)?;
        Ok(())
    }

    #[cfg(any(test, feature = "test-utils"))]
    fn prepared_audience_objects(
        &self,
        write_id: &WriteId,
    ) -> Result<PreparedAudienceObjects, DbError> {
        load_prepared_audience_objects_on(self.conn, self.store_dir, write_id)
    }

    #[cfg(any(test, feature = "test-utils"))]
    fn protocol_inert_object(
        &self,
        object: ExactObjectRef,
    ) -> Result<Option<coven_protocol::remote_object::ProtocolInertObject>, DbError> {
        let conn = self.conn;
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
    }
}

impl StoreDatabase {
    pub async fn prepared_remote_objects(
        &self,
        write_id: &WriteId,
    ) -> Result<Vec<PreparedRemoteObject>, DbError> {
        let write_id = write_id.clone();
        self.call_store(move |session| session.prepared_remote_objects(&write_id))
            .await
    }

    pub async fn mark_remote_object_uploaded(
        &self,
        expected: RemoteObjectRecord,
    ) -> Result<RemoteObjectRecord, DbError> {
        self.call_store(move |session| session.mark_remote_object_uploaded(expected))
            .await
    }

    pub async fn retire_uploaded_blob_spools(&self) -> Result<(), DbError> {
        let spools = self
            .call_store(|session| session.uploaded_blob_spools())
            .await?;

        for spool in spools {
            spool.retire(self).await.map_err(DbError::Message)?;
            self.clear_uploaded_blob_spool(spool).await?;
        }
        Ok(())
    }

    async fn clear_uploaded_blob_spool(&self, spool: UploadedBlobSpool) -> Result<(), DbError> {
        self.call_store(move |session| session.clear_uploaded_blob_spool(spool))
            .await
    }

    pub async fn mark_reusable_retained_authority_uploaded(
        &self,
        expected: RemoteObjectRecord,
    ) -> Result<RemoteObjectRecord, DbError> {
        self.call_store(move |session| session.mark_reusable_retained_authority_uploaded(expected))
            .await
    }

    pub async fn mark_candidate_commit_uploaded(
        &self,
        commit: StoreBatchCommitRef,
    ) -> Result<(), DbError> {
        self.call_store(move |session| session.mark_candidate_commit_uploaded(commit))
            .await
    }

    pub async fn mark_store_head_uploaded(
        &self,
        head: coven_protocol::store_commit::StoreDeviceHeadRef,
    ) -> Result<(), DbError> {
        self.call_store(move |session| session.mark_store_head_uploaded(head))
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn prepared_audience_objects(
        &self,
        write_id: &WriteId,
    ) -> Result<PreparedAudienceObjects, DbError> {
        let write_id = write_id.clone();
        let loaded = self
            .call_store(move |session| session.prepared_audience_objects(&write_id))
            .await?;

        let mut verified_blobs = Vec::with_capacity(loaded.blobs.len());
        for prepared in loaded.blobs {
            if let Some(spool_path) = prepared.spool_path() {
                {
                    let (size, digest) = coven_foundation::local_file::file_facts(spool_path)
                        .await
                        .map_err(|error| {
                            DbError::Message(format!("prepared blob spool: {error}"))
                        })?;
                    prepared
                        .blob()
                        .object()
                        .verify_stored_facts(
                            spool_path,
                            size,
                            coven_protocol::store_commit::ObjectHash::from_digest(digest),
                        )
                        .map_err(|error| DbError::context("prepared blob spool", error))?;
                }
            }
            verified_blobs.push(prepared);
        }
        Ok(PreparedAudienceObjects {
            packages: loaded.packages,
            blobs: verified_blobs,
        })
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn protocol_inert_object(
        &self,
        object: ExactObjectRef,
    ) -> Result<Option<coven_protocol::remote_object::ProtocolInertObject>, DbError> {
        self.call_store(move |session| session.protocol_inert_object(object))
            .await
    }
}

pub(crate) fn persist_prepared_audience_objects_on(
    conn: &rusqlite::Transaction<'_>,
    store_dir: &coven_foundation::store_dir::StoreDir,
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
        validate_prepared_package_on(conn, store_dir, write_id, prepared)?;
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

fn remote_object_is_uploaded(remote: &RemoteObjectRecord) -> bool {
    match remote {
        RemoteObjectRecord::CandidateCommit(record) => matches!(
            &record.state,
            coven_protocol::remote_object::CandidateCommitState::UploadedVerified
        ),
        RemoteObjectRecord::CandidateExclusive(record) => matches!(
            &record.state,
            coven_protocol::remote_object::CandidateObjectState::UploadedVerified { .. }
        ),
        RemoteObjectRecord::RetainedAuthority(record) => matches!(
            &record.state,
            coven_protocol::remote_object::RetainedAuthorityObjectState::UploadedVerified { .. }
        ),
        RemoteObjectRecord::SharedLiveSet(record) => matches!(
            &record.state,
            coven_protocol::remote_object::OwnedObjectState::UploadedVerified { .. }
        ),
    }
}

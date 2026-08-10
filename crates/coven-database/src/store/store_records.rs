use coven_foundation::store_dir::StoreDir;
use coven_protocol::remote_object::{remote_object_id, RemoteObjectRecord};
use coven_protocol::store_commit::ObjectHash;
use coven_protocol::write::{WriteId, WriteResolution, WriteStatus};
use rusqlite::Connection;

use super::candidate_records::{
    load_merge_candidate_head_cleanup_on, parse_prepared_merge_candidate_on,
    MergeCandidateHeadCleanup,
};
use super::payload_spool::{
    read_payload_blocking, read_verified_payload_blocking, write_payload_blocking,
    PayloadSpoolError,
};
use super::publication_state::PreparedStoreWriteState;
use crate::{candidate_graph_exact_objects, load_remote_object_on, Database, DbError};

/// One Store's row connection and matching payload directory.
///
/// A record whose bytes live in the spool is half a row and half a file, so
/// record operations carry both halves as one scoped value. Operations that
/// touch rows alone continue to take the connection in their private SQL leaf.
#[derive(Clone, Copy)]
pub(crate) struct StoreRecords<'store> {
    pub(crate) conn: &'store Connection,
    pub(crate) store_dir: &'store StoreDir,
}

impl<'store> StoreRecords<'store> {
    pub(crate) fn new(conn: &'store Connection, store_dir: &'store StoreDir) -> Self {
        Self { conn, store_dir }
    }

    pub(crate) fn payload(&self, hash: ObjectHash) -> Result<Vec<u8>, PayloadSpoolError> {
        read_payload_blocking(self.store_dir, hash)
    }

    pub(crate) fn verified_payload(&self, hash: ObjectHash) -> Result<Vec<u8>, PayloadSpoolError> {
        read_verified_payload_blocking(self.store_dir, hash)
    }

    pub(crate) fn install_payload(&self, bytes: &[u8]) -> Result<ObjectHash, PayloadSpoolError> {
        write_payload_blocking(self.store_dir, bytes)
    }
}

/// One Store transaction and its matching payload directory.
///
/// Payloads land before the row naming them commits, while ownership claims
/// land in this transaction. Keeping both borrows together prevents a record
/// mutation from using another Store's payload directory.
#[derive(Clone, Copy)]
pub(crate) struct StoreRecordTransaction<'store, 'connection> {
    pub(crate) transaction: &'store rusqlite::Transaction<'connection>,
    pub(crate) store_dir: &'store StoreDir,
}

struct UnpublishedWriteCleanup {
    removable: Vec<ObjectHash>,
    candidate: Option<coven_protocol::store_commit::StoreBatchCommitRef>,
}

impl<'store, 'connection> StoreRecordTransaction<'store, 'connection> {
    pub(crate) fn new(
        transaction: &'store rusqlite::Transaction<'connection>,
        store_dir: &'store StoreDir,
    ) -> Self {
        Self {
            transaction,
            store_dir,
        }
    }

    pub(crate) fn install_payload(&self, bytes: &[u8]) -> Result<ObjectHash, PayloadSpoolError> {
        write_payload_blocking(self.store_dir, bytes)
    }

    fn unpublished_write_cleanup(
        self,
        authority: &mut super::VerifiedStoreAuthority,
        write_id: &WriteId,
    ) -> Result<UnpublishedWriteCleanup, DbError> {
        let tx = self.transaction;
        let raw_prepared: Option<String> = tx
            .query_row(
                "SELECT prepared FROM store_writes WHERE write_id = ?1",
                [write_id.as_str()],
                |row| row.get(0),
            )
            .map_err(DbError::from)?;
        let mut removable = Vec::new();
        let mut candidate = None;
        if let Some(raw_prepared) = raw_prepared.as_deref() {
            let prepared: PreparedStoreWriteState = serde_json::from_str(raw_prepared)
                .map_err(|error| DbError::context("resolved prepared write", error))?;
            let merge = parse_prepared_merge_candidate_on(
                StoreRecords::new(tx, self.store_dir),
                authority,
                &prepared,
            )?;
            removable.push(remote_object_id(&merge.reference.object));
            match load_merge_candidate_head_cleanup_on(tx, &merge.head_object, &merge.reference)? {
                MergeCandidateHeadCleanup::Remote { .. } => {
                    removable.push(remote_object_id(&merge.head_object))
                }
                MergeCandidateHeadCleanup::ProtocolInert => {}
            }
            removable.extend(
                candidate_graph_exact_objects(&merge.commit)?
                    .iter()
                    .map(remote_object_id),
            );
            candidate = Some(merge.reference);
        }
        let mut statement = tx
            .prepare("SELECT remote_object_id FROM store_write_blobs WHERE write_id = ?1")
            .map_err(DbError::from)?;
        let indexed = statement
            .query_map([write_id.as_str()], |row| row.get::<_, String>(0))
            .map_err(DbError::from)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(DbError::from)?;
        drop(statement);
        for encoded in indexed {
            removable.push(
                encoded
                    .parse()
                    .map_err(|error| DbError::context("resolved remote object id", error))?,
            );
        }
        Ok(UnpublishedWriteCleanup {
            removable,
            candidate,
        })
    }

    fn unpublished_write_cleanup_complete(
        self,
        cleanup: &UnpublishedWriteCleanup,
    ) -> Result<bool, DbError> {
        let Some(candidate) = &cleanup.candidate else {
            return Ok(true);
        };
        for object_id in &cleanup.removable {
            let remote = load_remote_object_on(self.transaction, *object_id)?;
            if !remote
                .candidate_cleanup_complete(candidate)
                .map_err(|error| {
                    DbError::context(format!("validate candidate cleanup for {object_id}"), error)
                })?
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(super) fn unpublished_write_cleanup_is_complete(
        self,
        authority: &mut super::VerifiedStoreAuthority,
        write_id: &WriteId,
    ) -> Result<bool, DbError> {
        let cleanup = self.unpublished_write_cleanup(authority, write_id)?;
        self.unpublished_write_cleanup_complete(&cleanup)
    }

    pub(super) fn resolve_unpublished_writes(
        self,
        authority: &mut super::VerifiedStoreAuthority,
        write_ids: &[WriteId],
        resolution: &WriteResolution,
    ) -> Result<(), DbError> {
        let tx = self.transaction;
        let status = WriteStatus::Resolved(resolution.clone());
        for write_id in write_ids {
            let cleanup = self.unpublished_write_cleanup(authority, write_id)?;
            if !self.unpublished_write_cleanup_complete(&cleanup)? {
                return Err(DbError::Message(format!(
                    "candidate cleanup for write {write_id} is incomplete"
                )));
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
            for object_id in cleanup.removable {
                let remote = load_remote_object_on(tx, object_id)?;
                let absent = matches!(
                    remote,
                    RemoteObjectRecord::CandidateCommit(
                        coven_protocol::remote_object::CandidateCommitRecord {
                            state:
                                coven_protocol::remote_object::CandidateCommitState::AbsentVerified { .. },
                            ..
                        }
                    ) | RemoteObjectRecord::CandidateExclusive(
                        coven_protocol::remote_object::CandidateObjectRecord {
                            state:
                                coven_protocol::remote_object::CandidateObjectState::AbsentVerified { .. },
                            ..
                        }
                    ) | RemoteObjectRecord::RetainedAuthority(
                        coven_protocol::remote_object::RetainedAuthorityRecord {
                            state:
                                coven_protocol::remote_object::RetainedAuthorityObjectState::UncreatedVerified { .. },
                            ..
                        }
                    )
                );
                if absent {
                    crate::remote_object_records::delete_remote_object_on(tx, object_id)?;
                }
            }
            tx.execute(
                "UPDATE store_writes SET prepared = NULL WHERE write_id = ?1",
                [write_id.as_str()],
            )
            .map_err(DbError::from)?;
            Database::set_write_status_on(tx, write_id, &status)?;
        }
        Ok(())
    }
}

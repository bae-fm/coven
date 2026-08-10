use super::{
    candidate_records::parse_prepared_merge_candidate_parts_on,
    publication_state::PreparedStoreWriteState, StoreDatabase, StoreSession,
};
use crate::{
    load_prepared_audience_objects_on, DbError, ExactProtocolObject, PreparedStoreWriteCommit,
    StoreWriteBase,
};
use coven_protocol::membership::AuthorStreamId;
use coven_protocol::store_commit::{
    CommitFrontier, StoreBatchCommit, StoreBatchCommitRef, StoreCommitCoord, StoreDeviceHead,
    StoreDeviceRegistrationRef, VerifiedStoreBatchCommit,
};
use coven_protocol::write::WriteId;
use rusqlite::OptionalExtension;
use std::collections::BTreeMap;

/// One reading of the local ledger: the author's own latest position, the
/// materialized frontier it belongs to, and this device's turn to author the
/// commit that extends it. See [`StoreDatabase::local_commit_base`].
///
/// The turn is part of the reading rather than something a caller remembers to
/// take: the position is only true for as long as no other local writer can
/// take it. Hold this value until the commit composed from it has published its
/// head, or until the candidate is durably persisted for a later publisher to
/// activate.
pub struct LocalCommitBase {
    pub authorship: super::OwnStreamAuthorship,
    pub predecessor: Option<StoreBatchCommitRef>,
    pub frontier: BTreeMap<String, StoreBatchCommitRef>,
}

impl StoreSession<'_> {
    fn local_commit_ledger_base(
        &self,
        stream_id: &AuthorStreamId,
    ) -> Result<
        (
            Option<StoreBatchCommitRef>,
            BTreeMap<String, StoreBatchCommitRef>,
        ),
        DbError,
    > {
        let stream_id = stream_id.to_string();
        Ok((
            StoreDatabase::latest_position_for_device_on(self.conn, &stream_id)?,
            StoreDatabase::materialized_frontier_on(self.conn, None)?,
        ))
    }

    fn latest_local_store_position(
        &self,
        stream_id: &str,
    ) -> Result<Option<StoreBatchCommitRef>, DbError> {
        StoreDatabase::latest_position_for_device_on(self.conn, stream_id)
    }

    fn oldest_prepared_store_write(&mut self) -> Result<Option<PreparedStoreWriteCommit>, DbError> {
        let records = crate::store::StoreRecords::new(self.conn, self.store_dir);
        let row = records
            .conn
            .query_row(
                "SELECT write_id, base, prepared FROM store_writes
                 WHERE prepared IS NOT NULL
                   AND status = '\"publishing\"'
                 ORDER BY ordinal LIMIT 1",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(DbError::from)?;
        row.map(|(write_id, base, prepared)| {
            let prepared: PreparedStoreWriteState = serde_json::from_str(&prepared)
                .map_err(|error| DbError::context("prepared Store write", error))?;
            let (commit, head, graph_commit) = match &prepared {
                PreparedStoreWriteState::Publication { commit, head, .. } => (commit, head, None),
                PreparedStoreWriteState::MergeAbandonment {
                    candidate_commit,
                    authority_commit,
                    authority_head,
                    ..
                } => (authority_commit, authority_head, Some(candidate_commit)),
            };
            let write_id = WriteId::from_generated(write_id);
            let unverified_commit: StoreBatchCommit =
                serde_json::from_slice(commit.semantic_bytes())
                    .map_err(|error| DbError::context("prepared Store commit", error))?;
            if unverified_commit.write_id != write_id {
                return Err(DbError::Message(
                    "prepared write id differs from signed commit".to_string(),
                ));
            }
            let registration_ref = &unverified_commit.author_registration;
            let stored_registration_ref: String = records
                .conn
                .query_row(
                    "SELECT registration_object \
                         FROM store_device_registration_activations \
                         WHERE device_id = ?1 AND registration_hash = ?2",
                    (
                        registration_ref.device_id.to_string(),
                        registration_ref.registration_hash.to_string(),
                    ),
                    |row| row.get(0),
                )
                .map_err(DbError::from)?;
            let stored_registration_ref: StoreDeviceRegistrationRef =
                serde_json::from_str(&stored_registration_ref)
                    .map_err(|error| DbError::context("prepared write registration ref", error))?;
            if stored_registration_ref != *registration_ref {
                return Err(DbError::Message(
                    "prepared commit registration differs from its activation".to_string(),
                ));
            }
            let authority = self.activated_registration(registration_ref)?;
            let registration = authority.value();
            let root = &registration.store_root;
            let stream_id =
                coven_protocol::store_commit::StreamActivation::device_authorized_stream_id(
                    root.store_root_hash,
                    registration_ref,
                    coven_protocol::store_commit::StreamAnchorDomain::StoreAnnouncements,
                );
            let coord = StoreCommitCoord {
                stream_id,
                sequence: unverified_commit.seq(),
            };
            let commit_value = VerifiedStoreBatchCommit::parse_prepared(
                commit.semantic_bytes(),
                root.store_root_hash,
                coord,
                commit.prepared().reference().clone(),
                registration,
            )
            .map_err(|error| DbError::context("verify prepared Store commit", error))?;
            let commit_ref = commit_value.reference().clone();
            let head_value = StoreDeviceHead::parse_at(
                head.semantic_bytes(),
                root.store_root_hash,
                registration,
                &commit_ref,
            )
            .map_err(|error| DbError::context("verify prepared Store head", error))?;
            let base: StoreWriteBase = serde_json::from_str(&base)
                .map_err(|error| DbError::context("prepared write base", error))?;
            let dependencies = CommitFrontier::from_refs(base.dependencies)
                .map_err(|error| DbError::context("prepared dependency frontier", error))?;
            if dependencies.commits() != commit_value.merge_dependencies() {
                return Err(DbError::Message(
                    "prepared commit differs from its write dependency frontier".to_string(),
                ));
            }
            let partitions = StoreDatabase::store_write_partitions_on(records, write_id.as_str())?;
            let audiences =
                load_prepared_audience_objects_on(records.conn, records.store_dir, &write_id)?;
            let graph_commit = match graph_commit {
                Some(graph_commit) => {
                    let candidate_head = match &prepared {
                        PreparedStoreWriteState::MergeAbandonment { candidate_head, .. } => {
                            candidate_head
                        }
                        _ => unreachable!("matched Merge abandonment"),
                    };
                    let candidate = parse_prepared_merge_candidate_parts_on(
                        records,
                        self.verified_store_authority,
                        graph_commit.semantic_bytes(),
                        graph_commit.prepared().reference(),
                        candidate_head.semantic_bytes(),
                        candidate_head.prepared().reference(),
                    )?;
                    candidate.commit
                }
                None => commit_value.clone(),
            };
            let expected_package_count = usize::from(graph_commit.store_package().is_some())
                .checked_add(graph_commit.circle_packages().len())
                .ok_or_else(|| DbError::Message("package count overflow".to_string()))?;
            if audiences.packages.len() != expected_package_count
                || audiences.packages.len()
                    != usize::from(partitions.store.is_some()) + partitions.circles.len()
            {
                return Err(DbError::Message(
                    "prepared package indexes do not exactly cover commit audiences".to_string(),
                ));
            }
            for package in &audiences.packages {
                let value = package.package();
                if value.write_id() != &write_id
                    || value.commit_coord() != &commit_ref.coord
                    || value.candidate_family() != commit_value.candidate_family()
                {
                    return Err(DbError::Message(
                        "indexed audience package differs from its exact commit".to_string(),
                    ));
                }
                let expected_object = match value.audience() {
                    coven_protocol::audience_package::PackageAudience::Store => {
                        graph_commit
                            .verify_store_package(package.semantic_bytes())
                            .map_err(|error| DbError::Message(error.to_string()))?;
                        &graph_commit
                            .store_package()
                            .as_ref()
                            .expect("verified present")
                            .object
                    }
                    coven_protocol::audience_package::PackageAudience::Circle {
                        circle_id, ..
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
                        "indexed audience package exact object differs from its commit".to_string(),
                    ));
                }
            }
            for package in &audiences.packages {
                let audience = package.package().audience().remote_audience();
                for binding in package.package().blob_bindings() {
                    if !audiences
                        .blobs
                        .iter()
                        .any(|blob| blob.audience() == &audience && blob.blob() == binding.blob())
                    {
                        return Err(DbError::Message(
                            "prepared package blob binding has no exact blob index".to_string(),
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
                    bytes: commit.semantic_bytes().to_vec(),
                    prepared: commit.prepared().clone(),
                },
                head: ExactProtocolObject {
                    value: head_value,
                    bytes: head.semantic_bytes().to_vec(),
                    prepared: head.prepared().clone(),
                },
            })
        })
        .transpose()
    }
}

impl StoreDatabase {
    pub async fn oldest_prepared_store_write(
        &self,
    ) -> Result<Option<PreparedStoreWriteCommit>, DbError> {
        let loaded = self
            .connection
            .call_store(move |session| session.oldest_prepared_store_write())
            .await?;
        if let Some(batch) = &loaded {
            for blob in &batch.audiences.blobs {
                if let Some(spool_path) = blob.spool_path() {
                    {
                        let (size, digest) = coven_foundation::local_file::file_facts(spool_path)
                            .await
                            .map_err(|error| {
                                DbError::Message(format!("prepared blob spool: {error}"))
                            })?;
                        blob.blob()
                            .object()
                            .verify_stored_facts(
                                spool_path,
                                size,
                                coven_protocol::store_commit::ObjectHash::from_digest(digest),
                            )
                            .map_err(|error| DbError::context("prepared blob spool", error))?;
                    }
                }
            }
        }
        Ok(loaded)
    }

    /// The local device's own latest position and the materialized frontier
    /// that position belongs to, read as one state of the ledger.
    ///
    /// A commit order names one history, and both halves of it come from the
    /// same table. Reading them separately lets one of this device's own
    /// activations land in between, which leaves its own stream in the frontier
    /// one commit ahead of the position it extends. Such an order has no
    /// predecessor cut at all — the cut is the frontier with the predecessor
    /// inserted, and those two then contradict each other on the author's own
    /// stream — so every operation composed from it is refused. The device
    /// driving an operation also runs its sync loop, so that is the ordinary
    /// case rather than a hostile one.
    ///
    /// Taking this device's turn to author its own stream is part of the read:
    /// the position returned stays this device's next position for as long as
    /// the returned `LocalCommitBase` is held.
    pub async fn local_commit_base(
        &self,
        stream_id: AuthorStreamId,
    ) -> Result<LocalCommitBase, DbError> {
        let authorship = self.author_own_stream().await;
        let (predecessor, frontier) = self
            .connection
            .call_store(move |session| session.local_commit_ledger_base(&stream_id))
            .await?;
        Ok(LocalCommitBase {
            authorship,
            predecessor,
            frontier,
        })
    }

    pub async fn latest_local_store_position(
        &self,
        stream_id: AuthorStreamId,
    ) -> Result<Option<StoreBatchCommitRef>, DbError> {
        self.connection
            .call_store(move |session| session.latest_local_store_position(&stream_id.to_string()))
            .await
    }
}

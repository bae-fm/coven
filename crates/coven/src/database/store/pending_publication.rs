use super::{
    candidate_records::parse_prepared_merge_candidate_parts_on,
    publication_state::PreparedStoreWriteState, StoreDatabase,
};
use crate::database::{
    load_prepared_audience_objects_on, required_store_root_authority_on, DbError,
    ExactProtocolObject, PreparedStoreWriteCommit, StoreWriteBase,
};
use crate::protocol::membership::AuthorStreamId;
use crate::protocol::store_commit::{
    CommitFrontier, StoreBatchCommit, StoreBatchCommitRef, StoreCommitCoord, StoreDeviceHead,
    StoreDeviceRegistration, StoreDeviceRegistrationRef, VerifiedStoreBatchCommit,
};
use crate::write::WriteId;
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
pub(crate) struct LocalCommitBase {
    pub(crate) authorship: super::OwnStreamAuthorship,
    pub(crate) predecessor: Option<StoreBatchCommitRef>,
    pub(crate) frontier: BTreeMap<String, StoreBatchCommitRef>,
}

impl StoreDatabase {
    pub(crate) async fn oldest_prepared_store_write(
        &self,
    ) -> Result<Option<PreparedStoreWriteCommit>, DbError> {
        let loaded = self
            .connection
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
                        PreparedStoreWriteState::Publication { commit, head, .. } => {
                            (commit, head, None)
                        }
                        PreparedStoreWriteState::MergeAbandonment {
                            candidate_commit,
                            authority_commit,
                            authority_head,
                            ..
                        } => (authority_commit, authority_head, Some(candidate_commit)),
                    };
                    let write_id = WriteId::from_generated(write_id);
                    let unverified_commit: StoreBatchCommit =
                        serde_json::from_slice(commit.semantic_bytes()).map_err(|error| {
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
                        crate::protocol::store_commit::StreamActivation::device_authorized_stream_id(
                            root.store_root_hash,
                            registration_ref,
                            crate::protocol::store_commit::StreamAnchorDomain::StoreAnnouncements,
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
                        &registration,
                    )
                    .map_err(|error| {
                        DbError::Message(format!("verify prepared Store commit: {error}"))
                    })?;
                    let commit_ref = commit_value.reference().clone();
                    let head_value = StoreDeviceHead::parse_at(
                        head.semantic_bytes(),
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
                    let dependencies =
                        CommitFrontier::from_refs(base.dependencies).map_err(|error| {
                            DbError::Message(format!("prepared dependency frontier: {error}"))
                        })?;
                    if dependencies.commits() != commit_value.merge_dependencies() {
                        return Err(DbError::Message(
                            "prepared commit differs from its write dependency frontier"
                                .to_string(),
                        ));
                    }
                    let partitions = StoreDatabase::store_write_partitions_on(
                        conn,
                        write_id.as_str(),
                        &stored_changeset,
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
                            crate::protocol::audience_package::PackageAudience::Store => {
                                graph_commit
                                    .verify_store_package(package.semantic_bytes())
                                    .map_err(|error| DbError::Message(error.to_string()))?;
                                &graph_commit
                                    .store_package()
                                    .as_ref()
                                    .expect("verified present")
                                    .object
                            }
                            crate::protocol::audience_package::PackageAudience::Circle {
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
                            bytes: commit.semantic_bytes().to_vec(),
                            object: commit.prepared().reference().clone(),
                            prepared: commit.prepared().clone(),
                        },
                        head: ExactProtocolObject {
                            value: head_value,
                            bytes: head.semantic_bytes().to_vec(),
                            object: head.prepared().reference().clone(),
                            prepared: head.prepared().clone(),
                        },
                    })
                })
                .transpose()
            })
            .await?;
        if let Some(batch) = &loaded {
            for blob in &batch.audiences.blobs {
                if let Some(spool_path) = blob.spool_path() {
                    blob.blob()
                        .object()
                        .verify_file(spool_path)
                        .await
                        .map_err(|error| {
                            DbError::Message(format!("prepared blob spool: {error}"))
                        })?;
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
    /// the returned [`LocalCommitBase`] is held.
    pub(crate) async fn local_commit_base(
        &self,
        stream_id: AuthorStreamId,
    ) -> Result<LocalCommitBase, DbError> {
        let authorship = self.author_own_stream().await;
        let (predecessor, frontier) = self
            .connection
            .call(move |conn| {
                let stream_id = stream_id.to_string();
                Ok((
                    StoreDatabase::latest_position_for_device_on(conn, &stream_id)?,
                    StoreDatabase::materialized_frontier_on(conn, None)?,
                ))
            })
            .await?;
        Ok(LocalCommitBase {
            authorship,
            predecessor,
            frontier,
        })
    }

    pub(crate) async fn latest_local_store_position(
        &self,
        stream_id: AuthorStreamId,
    ) -> Result<Option<StoreBatchCommitRef>, DbError> {
        self.connection
            .call(move |conn| {
                let stream_id = stream_id.to_string();
                crate::database::StoreDatabase::latest_position_for_device_on(conn, &stream_id)
            })
            .await
    }
}

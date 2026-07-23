use crate::database::*;
use crate::sync::remote_object::{remote_object_id, RemoteObjectRecord};
use crate::sync::storage::ExactObjectRef;
use crate::sync::store_commit::{
    ObjectHash, StoreBatchCommit, StoreBatchCommitRef, StoreCommitCoord, StoreDeviceHead,
    StoreDeviceRegistrationRef, StoreHistoryCut,
};
use crate::write::WriteId;
use rusqlite::{Connection, OptionalExtension};
use std::collections::{BTreeMap, BTreeSet};

use super::publication_state::PreparedStoreWriteState;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PreparedMergeCandidate {
    pub(crate) commit: StoreBatchCommit,
    pub(crate) reference: StoreBatchCommitRef,
    pub(crate) canonical_signed_bytes: Vec<u8>,
    pub(crate) commit_prepared: crate::sync::storage::PreparedExactObject,
    pub(crate) head: StoreDeviceHead,
    pub(crate) head_prepared: crate::sync::storage::PreparedExactObject,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CandidateCleanupObject {
    pub(crate) object: ExactObjectRef,
}

pub(crate) fn parse_prepared_merge_candidate_on(
    conn: &Connection,
    prepared: &PreparedStoreWriteState,
) -> Result<PreparedMergeCandidate, DbError> {
    let (commit, head) = match prepared {
        PreparedStoreWriteState::Publication { commit, head, .. } => (commit, head),
        PreparedStoreWriteState::MergeAbandonment {
            candidate_commit,
            candidate_head,
            ..
        } => (candidate_commit, candidate_head),
    };
    parse_prepared_merge_candidate_parts_on(conn, commit, head)
}

pub(crate) fn parse_prepared_merge_candidate_parts_on(
    conn: &Connection,
    commit: &DurablePreparedProtocolObject,
    head: &DurablePreparedProtocolObject,
) -> Result<PreparedMergeCandidate, DbError> {
    let root = required_store_root_authority_on(conn)?;
    let unverified: StoreBatchCommit = serde_json::from_slice(commit.semantic_bytes())
        .map_err(|error| DbError::Message(format!("signed Merge candidate: {error}")))?;
    let registration =
        load_activated_registration_on(conn, &root, &unverified.author_registration)?;
    let coord = StoreCommitCoord {
        stream_id: crate::sync::store_commit::StreamActivation::device_authorized_stream_id(
            root.store_root_hash,
            &unverified.author_registration,
            crate::sync::store_commit::StreamAnchorDomain::StoreAnnouncements,
        ),
        sequence: unverified.seq(),
    };
    let value = StoreBatchCommit::parse_at(
        commit.semantic_bytes(),
        root.store_root_hash,
        &coord,
        &registration,
    )
    .map_err(|error| DbError::Message(format!("verify Merge candidate: {error}")))?;
    let reference =
        StoreBatchCommitRef::from_commit(&value, coord, commit.prepared().reference().clone())
            .map_err(|error| DbError::Message(error.to_string()))?;
    let head_value = StoreDeviceHead::parse_at(
        head.semantic_bytes(),
        root.store_root_hash,
        &registration,
        &reference,
    )
    .map_err(|error| DbError::Message(format!("verify Merge candidate head: {error}")))?;
    Ok(PreparedMergeCandidate {
        commit: value,
        reference,
        canonical_signed_bytes: commit.semantic_bytes().to_vec(),
        commit_prepared: commit.prepared().clone(),
        head: head_value,
        head_prepared: head.prepared().clone(),
    })
}

pub(super) fn blocked_merge_candidate_from_prepared(
    candidate: PreparedMergeCandidate,
) -> BlockedMergeCandidate {
    BlockedMergeCandidate {
        commit: ExactProtocolObject {
            value: candidate.commit,
            bytes: candidate.canonical_signed_bytes,
            object: candidate.reference.object,
            prepared: candidate.commit_prepared,
        },
        head: ExactProtocolObject {
            bytes: candidate.head.to_bytes(),
            object: candidate.head_prepared.reference().clone(),
            value: candidate.head,
            prepared: candidate.head_prepared,
        },
    }
}

pub(crate) fn parse_prepared_merge_publication_on(
    conn: &Connection,
    prepared: &PreparedStoreWriteState,
) -> Result<PreparedMergeCandidate, DbError> {
    match prepared {
        PreparedStoreWriteState::Publication { commit, head, .. } => {
            parse_prepared_merge_candidate_parts_on(conn, commit, head)
        }
        PreparedStoreWriteState::MergeAbandonment {
            authority_commit,
            authority_head,
            ..
        } => parse_prepared_merge_candidate_parts_on(conn, authority_commit, authority_head),
    }
}

pub(super) enum MergeCandidateHeadEvidence<'a> {
    OccupiedByProof,
    Verified(&'a crate::sync::remote_object::VerifiedCandidateHeadNonactivation),
}

pub(crate) fn author_exclusion_activation_for_candidate_on(
    conn: &Connection,
    candidate: &StoreBatchCommitRef,
    author: &StoreDeviceRegistrationRef,
) -> Result<Option<AuthorExclusionActivationLocator>, DbError> {
    let expected_stream = crate::sync::store_commit::StreamActivation::device_authorized_stream_id(
        required_store_root_authority_on(conn)?.store_root_hash,
        author,
        crate::sync::store_commit::StreamAnchorDomain::StoreAnnouncements,
    );
    let StoreCommitCoord {
        stream_id,
        sequence,
    } = &candidate.coord;
    if *stream_id != expected_stream {
        return Err(DbError::Message(
            "candidate stream differs from its exact author registration".to_string(),
        ));
    }
    let frontier = Database::materialized_frontier_on(conn, None)?
        .into_values()
        .map(|reference| (reference.coord.stream_id, reference))
        .collect::<BTreeMap<_, _>>();
    let (_, state) =
        crate::database::store_device_state_for_history_cut_on(conn, &StoreHistoryCut(frontier))?;
    let Some(record) = state.devices.get(&author.device_id) else {
        return Err(DbError::Message(
            "candidate author is absent from the current device state".to_string(),
        ));
    };
    if record.registration != *author {
        return Err(DbError::Message(
            "candidate author differs from the current device registration".to_string(),
        ));
    }
    let crate::sync::store_commit::StoreDeviceStatus::Inactive {
        terminals,
        accepted_cut: _,
    } = &record.status
    else {
        return Ok(None);
    };
    select_author_exclusion_activation_locator(
        terminals.as_slice(),
        &expected_stream,
        *sequence,
        |exclusion| load_author_exclusion_activation_locator_on(conn, exclusion),
    )
}

pub(crate) fn select_author_exclusion_activation_locator(
    terminals: &[crate::sync::store_commit::StoreDeviceExclusionRef],
    expected_stream: &crate::sync::causal_grants::AuthorStreamId,
    sequence: u64,
    mut load: impl FnMut(
        &crate::sync::store_commit::StoreDeviceExclusionRef,
    ) -> Result<AuthorExclusionActivationLocator, DbError>,
) -> Result<Option<AuthorExclusionActivationLocator>, DbError> {
    for exclusion in terminals {
        let locator = load(exclusion)?;
        let excluded_by_this_terminal = match locator.accepted_cut().get(expected_stream) {
            Some(reference) => sequence > reference.coord.sequence(),
            None => true,
        };
        if excluded_by_this_terminal {
            return Ok(Some(locator));
        }
    }
    Ok(None)
}

pub(crate) fn load_author_exclusion_activation_locator_on(
    conn: &Connection,
    exclusion: &crate::sync::store_commit::StoreDeviceExclusionRef,
) -> Result<AuthorExclusionActivationLocator, DbError> {
    let exclusion_json = serde_json::to_string(exclusion).map_err(|error| {
        DbError::Message(format!("serialize author exclusion reference: {error}"))
    })?;
    let stored: Option<(String, String, String)> = conn
        .query_row(
            "SELECT accepted_cut, activation_commit, activation_head
             FROM store_author_exclusion_activations
             WHERE exclusion_ref = ?1",
            [&exclusion_json],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(DbError::from)?;
    let Some((accepted_cut, activation_commit, activation_head)) = stored else {
        return Err(DbError::Message(
            "applied author exclusion has no exact activation locator".to_string(),
        ));
    };
    let accepted_cut = serde_json::from_str(&accepted_cut).map_err(|error| {
        DbError::Message(format!("parse author exclusion accepted cut: {error}"))
    })?;
    let activation_commit = serde_json::from_str(&activation_commit).map_err(|error| {
        DbError::Message(format!("parse author exclusion activation commit: {error}"))
    })?;
    let activation_head = serde_json::from_str(&activation_head).map_err(|error| {
        DbError::Message(format!("parse author exclusion activation head: {error}"))
    })?;
    let locator = AuthorExclusionActivationLocator::verified(
        exclusion.clone(),
        accepted_cut,
        activation_commit,
        activation_head,
    );
    let retained =
        Database::load_retained_merge_materialization_by_ref_on(conn, locator.activation_commit())?;
    let accepted_cut = StoreHistoryCut(locator.accepted_cut().clone());
    if retained.activation_head_object() != &locator.activation_head().object
        || retained.activation_head().head_hash() != locator.activation_head().head_hash
        || !retained
            .device_operations()
            .exclusions()
            .any(|(candidate, cut)| candidate == exclusion && cut == &accepted_cut)
    {
        return Err(DbError::Message(
            "author exclusion locator differs from its exact retained activation".to_string(),
        ));
    }
    Ok(locator)
}

pub(super) enum BlockedMergeCandidateNonactivation {
    Merge(crate::sync::remote_object::CandidateNonactivation),
    Terminal {
        durable: crate::sync::remote_object::CandidateNonactivation,
        head_nonactivation: crate::sync::remote_object::VerifiedCandidateHeadNonactivation,
    },
}

pub(super) fn blocked_merge_candidate_nonactivation(
    verified: crate::sync::remote_object::VerifiedCandidateNonactivation,
) -> Result<BlockedMergeCandidateNonactivation, DbError> {
    if matches!(
        verified.proof(),
        crate::sync::remote_object::CandidateNonactivationProof::AuthorExclusion { .. }
            | crate::sync::remote_object::CandidateNonactivationProof::MergeMembershipGrantRevocation { .. }
            | crate::sync::remote_object::CandidateNonactivationProof::MergeDependencyRetraction { .. }
    ) {
        let (durable, head_nonactivation) = verified
            .into_terminal_head_nonactivation()
            .map_err(|error| DbError::Message(error.to_string()))?;
        return Ok(BlockedMergeCandidateNonactivation::Terminal {
            durable,
            head_nonactivation,
        });
    }
    verified
        .merge_winner_commit()
        .map_err(|error| DbError::Message(error.to_string()))?;
    Ok(BlockedMergeCandidateNonactivation::Merge(
        verified.into_durable(),
    ))
}

pub(super) fn validate_terminal_candidate_authority_on(
    conn: &Connection,
    candidate: &PreparedMergeCandidate,
    durable: &crate::sync::remote_object::CandidateNonactivation,
) -> Result<(), DbError> {
    if durable
        .reference()
        .map_err(|error| DbError::Message(error.to_string()))?
        != candidate.reference
    {
        return Err(DbError::Message(
            "terminal candidate authority names another candidate".to_string(),
        ));
    }
    validate_terminal_nonactivation_authority_on(conn, durable)
}

pub(crate) fn validate_terminal_nonactivation_authority_on(
    conn: &Connection,
    durable: &crate::sync::remote_object::CandidateNonactivation,
) -> Result<(), DbError> {
    match durable.proof() {
        crate::sync::remote_object::CandidateNonactivationProof::AuthorExclusion {
            exclusion,
            accepted_cut,
            activation_head,
        } => {
            let commit: StoreBatchCommit = serde_json::from_slice(
                &durable.candidate().canonical_signed_bytes,
            )
            .map_err(|error| DbError::Message(format!("terminal candidate commit: {error}")))?;
            let reference = durable
                .reference()
                .map_err(|error| DbError::Message(error.to_string()))?;
            let current = author_exclusion_activation_for_candidate_on(
                conn,
                &reference,
                &commit.author_registration,
            )?
            .ok_or_else(|| {
                DbError::Message(
                    "candidate is no longer excluded by the selected terminal cutoff".to_string(),
                )
            })?;
            if current.exclusion() != exclusion
                || current.accepted_cut() != accepted_cut
                || current.activation_head() != activation_head
            {
                return Err(DbError::Message(
                    "author-exclusion activation changed after remote verification".to_string(),
                ));
            }
        }
        crate::sync::remote_object::CandidateNonactivationProof::MergeMembershipGrantRevocation {
            activation_commit,
            ..
        } => {
            let StoreCommitCoord {
                stream_id,
                sequence,
            } = &activation_commit.coord;
            if Database::materialized_commit_ref_on(
                conn,
                &stream_id.to_string(),
                *sequence,
            )?
            .as_ref()
                != Some(activation_commit)
            {
                return Err(DbError::Message(
                    "membership-grant revocation activation is no longer current accepted history"
                        .to_string(),
                ));
            }
        }
        crate::sync::remote_object::CandidateNonactivationProof::MergeDependencyRetraction {
            dependency_nonactivation,
            ..
        } => {
            validate_terminal_nonactivation_authority_on(conn, dependency_nonactivation)?;
        }
        crate::sync::remote_object::CandidateNonactivationProof::MergeWinner { .. } => {
            return Err(DbError::Message(
                "terminal candidate authority received another proof family".to_string(),
            ));
        }
    }
    Ok(())
}

pub(super) fn begin_blocked_merge_candidate_nonactivation_on(
    tx: &rusqlite::Transaction<'_>,
    write_id: &WriteId,
    candidate: &PreparedMergeCandidate,
    nonactivation: &BlockedMergeCandidateNonactivation,
    include_indexed_blobs: bool,
) -> Result<(), DbError> {
    if let BlockedMergeCandidateNonactivation::Terminal { durable, .. } = nonactivation {
        validate_terminal_candidate_authority_on(tx, candidate, durable)?;
    }
    match nonactivation {
        BlockedMergeCandidateNonactivation::Merge(durable) => {
            begin_merge_candidate_nonactivation_on(
                tx,
                write_id,
                candidate,
                durable,
                include_indexed_blobs,
            )
        }
        BlockedMergeCandidateNonactivation::Terminal {
            durable,
            head_nonactivation,
        } => begin_merge_candidate_nonactivation_with_verified_head_on(
            tx,
            write_id,
            candidate,
            durable,
            include_indexed_blobs,
            head_nonactivation,
        ),
    }
}

pub(crate) fn begin_merge_candidate_nonactivation_on(
    conn: &rusqlite::Transaction<'_>,
    write_id: &WriteId,
    candidate: &PreparedMergeCandidate,
    nonactivation: &crate::sync::remote_object::CandidateNonactivation,
    include_indexed_blobs: bool,
) -> Result<(), DbError> {
    begin_merge_candidate_nonactivation_with_head_evidence_on(
        conn,
        write_id,
        candidate,
        nonactivation,
        include_indexed_blobs,
        MergeCandidateHeadEvidence::OccupiedByProof,
    )
}

pub(super) fn begin_merge_candidate_nonactivation_with_verified_head_on(
    conn: &rusqlite::Transaction<'_>,
    write_id: &WriteId,
    candidate: &PreparedMergeCandidate,
    nonactivation: &crate::sync::remote_object::CandidateNonactivation,
    include_indexed_blobs: bool,
    head_nonactivation: &crate::sync::remote_object::VerifiedCandidateHeadNonactivation,
) -> Result<(), DbError> {
    begin_merge_candidate_nonactivation_with_head_evidence_on(
        conn,
        write_id,
        candidate,
        nonactivation,
        include_indexed_blobs,
        MergeCandidateHeadEvidence::Verified(head_nonactivation),
    )
}

pub(super) fn begin_merge_candidate_nonactivation_with_head_evidence_on(
    conn: &rusqlite::Transaction<'_>,
    write_id: &WriteId,
    candidate: &PreparedMergeCandidate,
    nonactivation: &crate::sync::remote_object::CandidateNonactivation,
    include_indexed_blobs: bool,
    head_evidence: MergeCandidateHeadEvidence<'_>,
) -> Result<(), DbError> {
    if nonactivation
        .reference()
        .map_err(|error| DbError::Message(error.to_string()))?
        != candidate.reference
        || nonactivation.candidate().canonical_signed_bytes != candidate.canonical_signed_bytes
    {
        return Err(DbError::Message(
            "verified Merge nonactivation names another prepared candidate".to_string(),
        ));
    }
    let mut object_ids = candidate_graph_exact_objects(&candidate.commit)?
        .iter()
        .map(|object| remote_object_id(object).to_string())
        .collect::<Vec<_>>();
    if include_indexed_blobs {
        let mut statement = conn
            .prepare(
                "SELECT remote_object_id FROM store_write_blobs WHERE write_id = ?1
                 ORDER BY remote_object_id",
            )
            .map_err(DbError::from)?;
        let indexed = statement
            .query_map([write_id.as_str()], |row| row.get::<_, String>(0))
            .map_err(DbError::from)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(DbError::from)?;
        drop(statement);
        object_ids.extend(indexed);
    }
    for encoded in object_ids {
        let object_id: ObjectHash = encoded.parse().map_err(|error| {
            DbError::Message(format!("Merge conflict remote object id: {error}"))
        })?;
        let _cleanup_target =
            begin_remote_candidate_nonactivation_on(conn, object_id, nonactivation.clone())?;
    }
    let head_object_id = remote_object_id(candidate.head_prepared.reference());
    let _head_cleanup_target = match head_evidence {
        MergeCandidateHeadEvidence::OccupiedByProof => {
            begin_remote_candidate_nonactivation_on(conn, head_object_id, nonactivation.clone())?
        }
        MergeCandidateHeadEvidence::Verified(head_nonactivation) => {
            begin_remote_candidate_nonactivation_with_verified_head_on(
                conn,
                head_object_id,
                nonactivation.clone(),
                head_nonactivation,
            )?
        }
    };
    let commit_object_id = remote_object_id(&candidate.reference.object);
    let _cleanup_target =
        begin_remote_candidate_nonactivation_on(conn, commit_object_id, nonactivation.clone())?;
    Ok(())
}

pub(super) fn merge_candidate_cleanup_targets_on(
    conn: &Connection,
    write_id: &WriteId,
    candidate: &PreparedMergeCandidate,
    include_indexed_blobs: bool,
) -> Result<Vec<CandidateCleanupObject>, DbError> {
    let commit_remote = load_remote_object_on(conn, remote_object_id(&candidate.reference.object))?;
    if !matches!(
        &commit_remote,
        RemoteObjectRecord::CandidateCommit(record)
            if matches!(
                &record.state,
                crate::sync::remote_object::CandidateCommitState::CleanupPending {
                    proof: crate::sync::remote_object::CandidateNonactivationProof::MergeWinner { .. }
                        | crate::sync::remote_object::CandidateNonactivationProof::AuthorExclusion { .. }
                        | crate::sync::remote_object::CandidateNonactivationProof::MergeMembershipGrantRevocation { .. }
                } | crate::sync::remote_object::CandidateCommitState::AbsentVerified {
                    proof: crate::sync::remote_object::CandidateNonactivationProof::MergeWinner { .. }
                        | crate::sync::remote_object::CandidateNonactivationProof::AuthorExclusion { .. }
                        | crate::sync::remote_object::CandidateNonactivationProof::MergeMembershipGrantRevocation { .. }
                }
            )
    ) {
        return Err(DbError::Message(
            "Merge candidate has no durable nonactivation proof".to_string(),
        ));
    }
    let mut cleanup = BTreeMap::new();
    {
        let mut encoded = candidate_graph_exact_objects(&candidate.commit)?
            .iter()
            .map(|object| remote_object_id(object).to_string())
            .collect::<Vec<_>>();
        if include_indexed_blobs {
            let mut statement = conn
                .prepare("SELECT remote_object_id FROM store_write_blobs WHERE write_id = ?1")
                .map_err(DbError::from)?;
            let indexed = statement
                .query_map([write_id.as_str()], |row| row.get::<_, String>(0))
                .map_err(DbError::from)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(DbError::from)?;
            drop(statement);
            encoded.extend(indexed);
        }
        for encoded in encoded {
            let object_id: ObjectHash = encoded.parse().map_err(|error| {
                DbError::Message(format!("Merge cleanup remote object id: {error}"))
            })?;
            let remote = load_remote_object_on(conn, object_id)?;
            if let Some(object) = remote.cleanup_target() {
                cleanup.insert(
                    object.clone(),
                    CandidateCleanupObject {
                        object: object.clone(),
                    },
                );
            } else if !remote
                .candidate_cleanup_complete(&candidate.reference)
                .map_err(|error| DbError::Message(format!("Merge cleanup {object_id}: {error}")))?
            {
                return Err(DbError::Message(format!(
                    "Merge candidate object {object_id} has no cleanup transition"
                )));
            }
        }
    }
    let head_cleanup = load_merge_candidate_head_cleanup_on(
        conn,
        candidate.head_prepared.reference(),
        &candidate.reference,
    )?;
    if matches!(
        head_cleanup,
        MergeCandidateHeadCleanup::Remote { complete: false }
    ) {
        return Err(DbError::Message(
            "Merge candidate head absence is not verified".to_string(),
        ));
    }
    let mut targets = Vec::new();
    for object in candidate_graph_exact_objects(&candidate.commit)? {
        if let Some(target) = cleanup.remove(&object) {
            targets.push(target);
        }
    }
    if !cleanup.is_empty() {
        return Err(DbError::Message(
            "Merge cleanup contains an object outside the signed candidate manifest".to_string(),
        ));
    }
    if let Some(object) = commit_remote.cleanup_target() {
        targets.push(CandidateCleanupObject {
            object: object.clone(),
        });
    } else if !commit_remote
        .candidate_cleanup_complete(&candidate.reference)
        .map_err(|error| DbError::Message(format!("Merge cleanup commit: {error}")))?
    {
        return Err(DbError::Message(
            "Merge candidate commit cleanup is incomplete".to_string(),
        ));
    }
    Ok(targets)
}

pub(super) fn finish_merge_retraction_cleanup_on(
    tx: &rusqlite::Transaction<'_>,
    candidate: &PreparedMergeCandidate,
) -> Result<(), DbError> {
    if !merge_candidate_cleanup_targets_on(tx, &candidate.commit.write_id, candidate, false)?
        .is_empty()
    {
        return Err(DbError::Message(
            "Merge retraction cleanup still has remote targets".to_string(),
        ));
    }
    let mut object_ids = candidate_graph_exact_objects(&candidate.commit)?
        .iter()
        .map(remote_object_id)
        .collect::<BTreeSet<_>>();
    object_ids.insert(remote_object_id(&candidate.reference.object));
    for object_id in object_ids {
        let remote = load_remote_object_on(tx, object_id)?;
        if !remote
            .candidate_cleanup_complete(&candidate.reference)
            .map_err(|error| {
                DbError::Message(format!(
                    "finish Merge retraction cleanup for {object_id}: {error}"
                ))
            })?
        {
            return Err(DbError::Message(format!(
                "Merge retraction object {object_id} is not terminal"
            )));
        }
        if matches!(
            remote,
            RemoteObjectRecord::CandidateCommit(
                crate::sync::remote_object::CandidateCommitRecord {
                    state: crate::sync::remote_object::CandidateCommitState::AbsentVerified { .. },
                    ..
                }
            ) | RemoteObjectRecord::CandidateExclusive(
                crate::sync::remote_object::CandidateObjectRecord {
                    state: crate::sync::remote_object::CandidateObjectState::AbsentVerified { .. },
                    ..
                }
            )
        ) {
            let removed = tx
                .execute(
                    "DELETE FROM remote_objects WHERE object_id = ?1",
                    [object_id.to_string()],
                )
                .map_err(DbError::from)?;
            if removed != 1 {
                return Err(DbError::Message(format!(
                    "Merge retraction object {object_id} disappeared during finalization"
                )));
            }
        }
    }
    let StoreCommitCoord {
        stream_id,
        sequence,
    } = &candidate.reference.coord;
    let deleted = tx
        .execute(
            "DELETE FROM merge_retraction_cleanups
             WHERE device_id = ?1 AND seq = ?2 AND commit_ref = ?3",
            rusqlite::params![
                stream_id.to_string(),
                Database::sequence_to_sqlite(&stream_id.to_string(), *sequence)?,
                serde_json::to_string(&candidate.reference).map_err(|error| {
                    DbError::Message(format!(
                        "serialize completed Merge retraction cleanup ref: {error}"
                    ))
                })?,
            ],
        )
        .map_err(DbError::from)?;
    if deleted != 1 {
        return Err(DbError::Message(
            "Merge retraction cleanup disappeared during finalization".to_string(),
        ));
    }
    Ok(())
}

pub(crate) enum MergeCandidateHeadCleanup {
    Remote { complete: bool },
    ProtocolInert,
}

pub(crate) fn load_merge_candidate_head_cleanup_on(
    conn: &Connection,
    head: &ExactObjectRef,
    candidate: &StoreBatchCommitRef,
) -> Result<MergeCandidateHeadCleanup, DbError> {
    let object_id = remote_object_id(head);
    let (remote_exists, inert_exists): (bool, bool) = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM remote_objects WHERE object_id = ?1),
                    EXISTS(SELECT 1 FROM protocol_inert_objects WHERE object_id = ?1)",
            [object_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(DbError::from)?;
    match (remote_exists, inert_exists) {
        (true, false) => load_remote_object_on(conn, object_id)?
            .candidate_cleanup_complete(candidate)
            .map(|complete| MergeCandidateHeadCleanup::Remote { complete })
            .map_err(|error| DbError::Message(format!("Merge cleanup head: {error}"))),
        (false, true) => {
            let inert = load_protocol_inert_object_on(conn, object_id)?;
            if !inert
                .is_terminal_head_for(candidate, head)
                .map_err(|error| DbError::Message(format!("Merge cleanup inert head: {error}")))?
            {
                return Err(DbError::Message(format!(
                    "protocol-inert Merge head {object_id} does not prove this excluded candidate"
                )));
            }
            Ok(MergeCandidateHeadCleanup::ProtocolInert)
        }
        (false, false) => Err(DbError::Message(format!(
            "Merge candidate head {object_id} is absent from durable remote state"
        ))),
        (true, true) => Err(DbError::Message(format!(
            "Merge candidate head {object_id} is both active and protocol-inert"
        ))),
    }
}

pub(super) fn remove_cleaned_merge_authority_on(
    tx: &rusqlite::Transaction<'_>,
    authority: &PreparedMergeCandidate,
) -> Result<(), DbError> {
    for object in [
        authority.reference.object.clone(),
        authority.head_prepared.reference().clone(),
    ] {
        let object_id = remote_object_id(&object);
        let remote = load_remote_object_on(tx, object_id)?;
        if !remote
            .candidate_cleanup_complete(&authority.reference)
            .map_err(|error| {
                DbError::Message(format!(
                    "validate abandoned authority cleanup for {object_id}: {error}"
                ))
            })?
        {
            return Err(DbError::Message(
                "losing Merge abandonment cleanup is incomplete".to_string(),
            ));
        }
        let removed = tx
            .execute(
                "DELETE FROM remote_objects WHERE object_id = ?1",
                [object_id.to_string()],
            )
            .map_err(DbError::from)?;
        if removed != 1 {
            return Err(DbError::Message(format!(
                "abandoned authority object {object_id} disappeared during removal"
            )));
        }
    }
    Ok(())
}

pub(super) fn remove_cleaned_author_excluded_merge_authority_on(
    tx: &rusqlite::Transaction<'_>,
    authority: &PreparedMergeCandidate,
) -> Result<(), DbError> {
    let commit_object_id = remote_object_id(&authority.reference.object);
    let commit = load_remote_object_on(tx, commit_object_id)?;
    if !commit
        .candidate_cleanup_complete(&authority.reference)
        .map_err(|error| {
            DbError::Message(format!(
                "validate excluded abandonment commit cleanup for {commit_object_id}: {error}"
            ))
        })?
    {
        return Err(DbError::Message(
            "excluded Merge abandonment commit cleanup is incomplete".to_string(),
        ));
    }
    let removed = tx
        .execute(
            "DELETE FROM remote_objects WHERE object_id = ?1",
            [commit_object_id.to_string()],
        )
        .map_err(DbError::from)?;
    if removed != 1 {
        return Err(DbError::Message(
            "excluded Merge abandonment commit disappeared during removal".to_string(),
        ));
    }

    let head = authority.head_prepared.reference();
    if let MergeCandidateHeadCleanup::Remote { complete } =
        load_merge_candidate_head_cleanup_on(tx, head, &authority.reference)?
    {
        if !complete {
            return Err(DbError::Message(
                "excluded Merge abandonment head cleanup is incomplete".to_string(),
            ));
        }
        let head_object_id = remote_object_id(head);
        let removed = tx
            .execute(
                "DELETE FROM remote_objects WHERE object_id = ?1",
                [head_object_id.to_string()],
            )
            .map_err(DbError::from)?;
        if removed != 1 {
            return Err(DbError::Message(
                "excluded Merge abandonment head disappeared during removal".to_string(),
            ));
        }
    }
    Ok(())
}

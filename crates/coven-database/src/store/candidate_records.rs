use crate::*;
use coven_protocol::objects::ExactObjectRef;
use coven_protocol::remote_object::{remote_object_id, RemoteObjectRecord};
use coven_protocol::store_commit::{
    ObjectHash, StoreBatchCommit, StoreBatchCommitRef, StoreCommitCoord, StoreDeviceHead,
    StoreDeviceRegistrationRef, StoreHistoryCut, VerifiedStoreBatchCommit,
};
use coven_protocol::write::WriteId;
use rusqlite::{Connection, OptionalExtension};
use std::collections::{BTreeMap, BTreeSet};

use super::publication_state::PreparedStoreWriteState;
use super::store_device_state::store_device_state_for_history_cut_on;
#[derive(Debug, Clone)]
pub struct PreparedMergeCandidate {
    pub commit: VerifiedStoreBatchCommit,
    pub reference: StoreBatchCommitRef,
    pub canonical_signed_bytes: Vec<u8>,
    pub head: StoreDeviceHead,
    pub head_object: ExactObjectRef,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CandidateCleanupObject {
    pub object: ExactObjectRef,
}

/// Record `nonactivation` against every object a losing candidate published, and
/// return the ones whose durable state now names them for deletion. An object
/// several candidates own stays until the last of them is nonactivated, so a
/// caller can never delete an object another live candidate still needs. The
/// candidate's own commit must reach a cleanup target: it is uploaded before the
/// head that decides the position, so a candidate that lost that position always
/// leaves it behind.
pub fn begin_candidate_nonactivation_targets_on(
    tx: &rusqlite::Transaction<'_>,
    candidate: &StoreBatchCommitRef,
    objects: &[ExactObjectRef],
    nonactivation: &coven_protocol::remote_object::CandidateNonactivation,
) -> Result<Vec<CandidateCleanupObject>, DbError> {
    let mut unique = BTreeSet::new();
    let mut cleanup = Vec::new();
    for object in objects {
        let object_id = remote_object_id(object);
        if !unique.insert(object_id) {
            return Err(DbError::Message(
                "losing candidate repeats an exact owned object".to_string(),
            ));
        }
        if let Some(target) =
            begin_remote_candidate_nonactivation_on(tx, object_id, nonactivation.clone())?
        {
            cleanup.push(CandidateCleanupObject { object: target });
        }
    }
    if !objects.contains(&candidate.object)
        || !cleanup
            .iter()
            .any(|target| target.object == candidate.object)
    {
        return Err(DbError::Message(
            "losing candidate has no exact commit cleanup target".to_string(),
        ));
    }
    cleanup.sort_by(|left, right| left.object.cmp(&right.object));
    Ok(cleanup)
}

/// The objects of an already-nonactivated candidate still awaiting deletion.
/// Reading it again after each delete is what makes an interrupted cleanup
/// resumable: every object has either a pending target or a completed cleanup,
/// and anything else is a state this candidate never reached.
pub fn candidate_cleanup_targets_on(
    conn: &Connection,
    candidate: &StoreBatchCommitRef,
    objects: &[ExactObjectRef],
) -> Result<Vec<CandidateCleanupObject>, DbError> {
    let mut unique = BTreeSet::new();
    let mut cleanup = Vec::new();
    for object in objects {
        let object_id = remote_object_id(object);
        if !unique.insert(object_id) {
            return Err(DbError::Message(
                "candidate cleanup repeats an exact object".to_string(),
            ));
        }
        let remote = load_remote_object_on(conn, object_id)?;
        if let Some(target) = remote.cleanup_target() {
            cleanup.push(CandidateCleanupObject {
                object: target.clone(),
            });
        } else if !remote
            .candidate_cleanup_complete(candidate)
            .map_err(|error| DbError::Message(error.to_string()))?
        {
            return Err(DbError::Message(format!(
                "candidate object {object_id} has no cleanup decision"
            )));
        }
    }
    cleanup.sort_by(|left, right| left.object.cmp(&right.object));
    Ok(cleanup)
}

pub fn require_candidate_cleanup_complete_on(
    conn: &Connection,
    candidate: &StoreBatchCommitRef,
    objects: &[ExactObjectRef],
    context: &str,
) -> Result<(), DbError> {
    if candidate_cleanup_targets_on(conn, candidate, objects)?.is_empty() {
        Ok(())
    } else {
        Err(DbError::Message(context.to_string()))
    }
}

pub fn delete_remote_objects_on(
    tx: &rusqlite::Transaction<'_>,
    object_ids: impl IntoIterator<Item = ObjectHash>,
    context: &str,
) -> Result<(), DbError> {
    let mut unique = BTreeSet::new();
    for object_id in object_ids {
        if !unique.insert(object_id) {
            return Err(DbError::Message(format!(
                "{context} repeats remote object {object_id}"
            )));
        }
        if !crate::remote_object_records::delete_remote_object_on(tx, object_id)? {
            return Err(DbError::Message(format!(
                "{context} object {object_id} disappeared during cleanup"
            )));
        }
    }
    Ok(())
}

pub fn parse_prepared_merge_candidate_on(
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
    parse_prepared_merge_candidate_parts_on(
        conn,
        commit.semantic_bytes(),
        commit.prepared().reference(),
        head.semantic_bytes(),
        head.prepared().reference(),
    )
}

/// Verify one candidate from the two objects that identify it.
///
/// Both objects arrive as their signed bytes plus the reference they are stored
/// under; the upload representation is not needed to verify a candidate, only to
/// create one, so it is not asked for.
pub fn parse_prepared_merge_candidate_parts_on(
    conn: &Connection,
    commit_bytes: &[u8],
    commit_object: &ExactObjectRef,
    head_bytes: &[u8],
    head_object: &ExactObjectRef,
) -> Result<PreparedMergeCandidate, DbError> {
    let root = required_store_root_authority_on(conn)?;
    let unverified: StoreBatchCommit = serde_json::from_slice(commit_bytes)
        .map_err(|error| DbError::context("signed Merge candidate", error))?;
    let registration =
        load_activated_registration_on(conn, &root, &unverified.author_registration)?;
    let coord = StoreCommitCoord {
        stream_id: coven_protocol::store_commit::StreamActivation::device_authorized_stream_id(
            root.store_root_hash,
            &unverified.author_registration,
            coven_protocol::store_commit::StreamAnchorDomain::StoreAnnouncements,
        ),
        sequence: unverified.seq(),
    };
    let value = VerifiedStoreBatchCommit::parse_prepared(
        commit_bytes,
        root.store_root_hash,
        coord,
        commit_object.clone(),
        &registration,
    )
    .map_err(|error| DbError::context("verify Merge candidate", error))?;
    let reference = value.reference().clone();
    let head_value =
        StoreDeviceHead::parse_at(head_bytes, root.store_root_hash, &registration, &reference)
            .map_err(|error| DbError::context("verify Merge candidate head", error))?;
    Ok(PreparedMergeCandidate {
        commit: value,
        reference,
        canonical_signed_bytes: commit_bytes.to_vec(),
        head: head_value,
        head_object: head_object.clone(),
    })
}

pub fn blocked_merge_candidate_from_prepared(
    candidate: PreparedMergeCandidate,
) -> BlockedMergeCandidate {
    BlockedMergeCandidate {
        commit: candidate.commit,
        commit_bytes: candidate.canonical_signed_bytes,
        commit_object: candidate.reference.object,
        head: candidate.head,
        head_object: candidate.head_object,
    }
}

pub fn parse_prepared_merge_publication_on(
    conn: &Connection,
    prepared: &PreparedStoreWriteState,
) -> Result<PreparedMergeCandidate, DbError> {
    match prepared {
        PreparedStoreWriteState::Publication { commit, head, .. } => {
            parse_prepared_merge_candidate_parts_on(
                conn,
                commit.semantic_bytes(),
                commit.prepared().reference(),
                head.semantic_bytes(),
                head.prepared().reference(),
            )
        }
        PreparedStoreWriteState::MergeAbandonment {
            authority_commit,
            authority_head,
            ..
        } => parse_prepared_merge_candidate_parts_on(
            conn,
            authority_commit.semantic_bytes(),
            authority_commit.prepared().reference(),
            authority_head.semantic_bytes(),
            authority_head.prepared().reference(),
        ),
    }
}

pub enum MergeCandidateHeadEvidence<'a> {
    OccupiedByProof,
    Verified(&'a coven_protocol::remote_object::VerifiedCandidateHeadNonactivation),
}

pub fn author_exclusion_activation_for_candidate_on(
    records: crate::payload_spool::StoreRecords<'_>,
    root: &coven_protocol::store_commit::StoreRootRef,
    candidate: &StoreBatchCommitRef,
    author: &StoreDeviceRegistrationRef,
) -> Result<Option<AuthorExclusionActivationLocator>, DbError> {
    let conn = records.conn();
    let expected_stream =
        coven_protocol::store_commit::StreamActivation::device_authorized_stream_id(
            root.store_root_hash,
            author,
            coven_protocol::store_commit::StreamAnchorDomain::StoreAnnouncements,
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
    let frontier = crate::StoreDatabase::materialized_frontier_on(conn, None)?
        .into_values()
        .map(|reference| (reference.coord.stream_id, reference))
        .collect::<BTreeMap<_, _>>();
    let (_, state) = store_device_state_for_history_cut_on(conn, &StoreHistoryCut(frontier))?;
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
    let coven_protocol::store_commit::StoreDeviceStatus::Inactive {
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
        |exclusion| load_author_exclusion_activation_locator_on(records, root, exclusion),
    )
}

pub fn select_author_exclusion_activation_locator(
    terminals: &[coven_protocol::store_commit::StoreDeviceExclusionRef],
    expected_stream: &coven_protocol::causal_grants::AuthorStreamId,
    sequence: u64,
    mut load: impl FnMut(
        &coven_protocol::store_commit::StoreDeviceExclusionRef,
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

pub fn load_author_exclusion_activation_locator_on(
    records: crate::payload_spool::StoreRecords<'_>,
    root: &coven_protocol::store_commit::StoreRootRef,
    exclusion: &coven_protocol::store_commit::StoreDeviceExclusionRef,
) -> Result<AuthorExclusionActivationLocator, DbError> {
    let conn = records.conn();
    let exclusion_json = serde_json::to_string(exclusion)
        .map_err(|error| DbError::context("serialize author exclusion reference", error))?;
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
    let accepted_cut = serde_json::from_str(&accepted_cut)
        .map_err(|error| DbError::context("parse author exclusion accepted cut", error))?;
    let activation_commit = serde_json::from_str(&activation_commit)
        .map_err(|error| DbError::context("parse author exclusion activation commit", error))?;
    let activation_head = serde_json::from_str(&activation_head)
        .map_err(|error| DbError::context("parse author exclusion activation head", error))?;
    let locator = AuthorExclusionActivationLocator::verified(
        exclusion.clone(),
        accepted_cut,
        activation_commit,
        activation_head,
    );
    let retained = crate::StoreDatabase::load_retained_merge_materialization_by_ref_on(
        records,
        root,
        locator.activation_commit(),
    )?;
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

pub enum BlockedMergeCandidateNonactivation {
    Merge(coven_protocol::remote_object::CandidateNonactivation),
    Terminal {
        durable: coven_protocol::remote_object::CandidateNonactivation,
        head_nonactivation: coven_protocol::remote_object::VerifiedCandidateHeadNonactivation,
    },
}

pub fn blocked_merge_candidate_nonactivation(
    verified: coven_protocol::remote_object::VerifiedCandidateNonactivation,
) -> Result<BlockedMergeCandidateNonactivation, DbError> {
    if matches!(
        verified.proof(),
        coven_protocol::remote_object::CandidateNonactivationProof::AuthorExclusion { .. }
            | coven_protocol::remote_object::CandidateNonactivationProof::MergeMembershipGrantRevocation { .. }
            | coven_protocol::remote_object::CandidateNonactivationProof::MergeDependencyRetraction { .. }
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

pub fn validate_terminal_candidate_authority_on(
    records: crate::payload_spool::StoreRecords<'_>,
    root: &coven_protocol::store_commit::StoreRootRef,
    candidate: &PreparedMergeCandidate,
    durable: &coven_protocol::remote_object::CandidateNonactivation,
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
    validate_terminal_nonactivation_authority_on(records, root, durable)
}

pub fn validate_terminal_nonactivation_authority_on(
    records: crate::payload_spool::StoreRecords<'_>,
    root: &coven_protocol::store_commit::StoreRootRef,
    durable: &coven_protocol::remote_object::CandidateNonactivation,
) -> Result<(), DbError> {
    let conn = records.conn();
    match durable.proof() {
        coven_protocol::remote_object::CandidateNonactivationProof::AuthorExclusion {
            exclusion,
            accepted_cut,
            activation_head,
        } => {
            let commit: StoreBatchCommit = serde_json::from_slice(
                &durable.candidate().canonical_signed_bytes,
            )
            .map_err(|error| DbError::context("terminal candidate commit", error))?;
            let reference = durable
                .reference()
                .map_err(|error| DbError::Message(error.to_string()))?;
            let current = author_exclusion_activation_for_candidate_on(
                records,
                root,
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
        coven_protocol::remote_object::CandidateNonactivationProof::MergeMembershipGrantRevocation {
            activation_commit,
            ..
        } => {
            let StoreCommitCoord {
                stream_id,
                sequence,
            } = &activation_commit.coord;
            if crate::StoreDatabase::materialized_commit_ref_on(
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
        coven_protocol::remote_object::CandidateNonactivationProof::MergeDependencyRetraction {
            dependency_nonactivation,
            ..
        } => {
            validate_terminal_nonactivation_authority_on(records, root, dependency_nonactivation)?;
        }
        coven_protocol::remote_object::CandidateNonactivationProof::MergeWinner { .. } => {
            return Err(DbError::Message(
                "terminal candidate authority received another proof family".to_string(),
            ));
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn begin_blocked_merge_candidate_nonactivation_on(
    records: crate::payload_spool::StoreRecordTransaction<'_, '_>,
    root: &coven_protocol::store_commit::StoreRootRef,
    write_id: &WriteId,
    candidate: &PreparedMergeCandidate,
    nonactivation: &BlockedMergeCandidateNonactivation,
    include_indexed_blobs: bool,
    extra_objects: &[ExactObjectRef],
) -> Result<(), DbError> {
    let tx = records.transaction();
    if let BlockedMergeCandidateNonactivation::Terminal { durable, .. } = nonactivation {
        validate_terminal_candidate_authority_on(*records, root, candidate, durable)?;
    }
    match nonactivation {
        BlockedMergeCandidateNonactivation::Merge(durable) => {
            begin_merge_candidate_nonactivation_on(
                tx,
                write_id,
                candidate,
                durable,
                include_indexed_blobs,
                extra_objects,
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
            extra_objects,
            head_nonactivation,
        ),
    }
}

pub fn begin_merge_candidate_nonactivation_on(
    conn: &rusqlite::Transaction<'_>,
    write_id: &WriteId,
    candidate: &PreparedMergeCandidate,
    nonactivation: &coven_protocol::remote_object::CandidateNonactivation,
    include_indexed_blobs: bool,
    extra_objects: &[ExactObjectRef],
) -> Result<(), DbError> {
    begin_merge_candidate_nonactivation_with_head_evidence_on(
        conn,
        write_id,
        candidate,
        nonactivation,
        include_indexed_blobs,
        extra_objects,
        MergeCandidateHeadEvidence::OccupiedByProof,
    )
}

pub fn begin_merge_candidate_nonactivation_with_verified_head_on(
    conn: &rusqlite::Transaction<'_>,
    write_id: &WriteId,
    candidate: &PreparedMergeCandidate,
    nonactivation: &coven_protocol::remote_object::CandidateNonactivation,
    include_indexed_blobs: bool,
    extra_objects: &[ExactObjectRef],
    head_nonactivation: &coven_protocol::remote_object::VerifiedCandidateHeadNonactivation,
) -> Result<(), DbError> {
    begin_merge_candidate_nonactivation_with_head_evidence_on(
        conn,
        write_id,
        candidate,
        nonactivation,
        include_indexed_blobs,
        extra_objects,
        MergeCandidateHeadEvidence::Verified(head_nonactivation),
    )
}

pub fn begin_merge_candidate_nonactivation_with_head_evidence_on(
    conn: &rusqlite::Transaction<'_>,
    write_id: &WriteId,
    candidate: &PreparedMergeCandidate,
    nonactivation: &coven_protocol::remote_object::CandidateNonactivation,
    include_indexed_blobs: bool,
    extra_objects: &[ExactObjectRef],
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
    object_ids.extend(
        extra_objects
            .iter()
            .map(|object| remote_object_id(object).to_string()),
    );
    for encoded in object_ids {
        let object_id: ObjectHash = encoded
            .parse()
            .map_err(|error| DbError::context("Merge conflict remote object id", error))?;
        let _cleanup_target =
            begin_remote_candidate_nonactivation_on(conn, object_id, nonactivation.clone())?;
    }
    let head_object_id = remote_object_id(&candidate.head_object);
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

/// Read a prepared candidate's durable nonactivation proof and lift it into the
/// terminal cleanup authority it names. Returns `None` when the candidate has no
/// proof yet or its proof is a non-terminal Merge-winner (whose head is cleaned
/// by occupation, not terminal reconciliation). Shared by Merge cleanup and
/// Circle-operation discard so both derive the authority identically.
pub fn terminal_candidate_verification_on(
    records: crate::payload_spool::StoreRecords<'_>,
    root: &coven_protocol::store_commit::StoreRootRef,
    candidate: PreparedMergeCandidate,
) -> Result<Option<TerminalCandidateCleanupVerification>, DbError> {
    let conn = records.conn();
    let remote = load_remote_object_on(conn, remote_object_id(&candidate.reference.object))?;
    let Some(proof) = remote
        .candidate_nonactivation_proof(&candidate.reference)
        .map_err(|error| DbError::Message(error.to_string()))?
    else {
        return Ok(None);
    };
    let authority = match proof {
        coven_protocol::remote_object::CandidateNonactivationProof::AuthorExclusion {
            exclusion,
            ..
        } => TerminalCandidateAuthority::AuthorExclusion(
            load_author_exclusion_activation_locator_on(records, root, exclusion)?,
        ),
        coven_protocol::remote_object::CandidateNonactivationProof::MergeMembershipGrantRevocation {
            grant_id,
            membership,
            activation_commit,
            activation_head,
        } => TerminalCandidateAuthority::MembershipGrantRevocation {
            grant_id: grant_id.clone(),
            membership: membership.clone(),
            activation_commit: activation_commit.clone(),
            activation_head: activation_head.clone(),
        },
        coven_protocol::remote_object::CandidateNonactivationProof::MergeDependencyRetraction { .. } => {
            let durable = coven_protocol::remote_object::CandidateNonactivation::from_durable_parts(
                &candidate.reference,
                &candidate.commit,
                proof.clone(),
            )
            .map_err(|error| DbError::Message(error.to_string()))?;
            validate_terminal_nonactivation_authority_on(records, root, &durable)?;
            TerminalCandidateAuthority::DependencyRetraction(
                coven_protocol::remote_object::VerifiedDependencyRetractionAuthority::after_live_authority_check(durable)
                    .map_err(|error| DbError::Message(error.to_string()))?,
            )
        }
        coven_protocol::remote_object::CandidateNonactivationProof::MergeWinner { .. } => {
            return Ok(None)
        }
    };
    Ok(Some(TerminalCandidateCleanupVerification {
        authority,
        candidate: blocked_merge_candidate_from_prepared(candidate),
    }))
}

pub fn merge_candidate_cleanup_targets_on(
    conn: &Connection,
    write_id: &WriteId,
    candidate: &PreparedMergeCandidate,
    include_indexed_blobs: bool,
    extra_objects: &[ExactObjectRef],
) -> Result<Vec<CandidateCleanupObject>, DbError> {
    let commit_remote = load_remote_object_on(conn, remote_object_id(&candidate.reference.object))?;
    if !matches!(
        &commit_remote,
        RemoteObjectRecord::CandidateCommit(record)
            if matches!(
                &record.state,
                coven_protocol::remote_object::CandidateCommitState::CleanupPending {
                    proof: coven_protocol::remote_object::CandidateNonactivationProof::MergeWinner { .. }
                        | coven_protocol::remote_object::CandidateNonactivationProof::AuthorExclusion { .. }
                        | coven_protocol::remote_object::CandidateNonactivationProof::MergeMembershipGrantRevocation { .. }
                } | coven_protocol::remote_object::CandidateCommitState::AbsentVerified {
                    proof: coven_protocol::remote_object::CandidateNonactivationProof::MergeWinner { .. }
                        | coven_protocol::remote_object::CandidateNonactivationProof::AuthorExclusion { .. }
                        | coven_protocol::remote_object::CandidateNonactivationProof::MergeMembershipGrantRevocation { .. }
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
        encoded.extend(
            extra_objects
                .iter()
                .map(|object| remote_object_id(object).to_string()),
        );
        for encoded in encoded {
            let object_id: ObjectHash = encoded
                .parse()
                .map_err(|error| DbError::context("Merge cleanup remote object id", error))?;
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
                .map_err(|error| DbError::context(format!("Merge cleanup {object_id}"), error))?
            {
                return Err(DbError::Message(format!(
                    "Merge candidate object {object_id} has no cleanup transition"
                )));
            }
        }
    }
    let head_cleanup =
        load_merge_candidate_head_cleanup_on(conn, &candidate.head_object, &candidate.reference)?;
    if matches!(
        head_cleanup,
        MergeCandidateHeadCleanup::Remote { complete: false }
    ) {
        return Err(DbError::Message(
            "Merge candidate head absence is not verified".to_string(),
        ));
    }
    let mut targets = Vec::new();
    for object in candidate_graph_exact_objects(&candidate.commit)?
        .into_iter()
        .chain(extra_objects.iter().cloned())
    {
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
        .map_err(|error| DbError::context("Merge cleanup commit", error))?
    {
        return Err(DbError::Message(
            "Merge candidate commit cleanup is incomplete".to_string(),
        ));
    }
    Ok(targets)
}

pub fn finish_merge_retraction_cleanup_on(
    tx: &rusqlite::Transaction<'_>,
    candidate: &PreparedMergeCandidate,
) -> Result<(), DbError> {
    if !merge_candidate_cleanup_targets_on(tx, &candidate.commit.write_id, candidate, false, &[])?
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
                DbError::context(
                    format!("finish Merge retraction cleanup for {object_id}"),
                    error,
                )
            })?
        {
            return Err(DbError::Message(format!(
                "Merge retraction object {object_id} is not terminal"
            )));
        }
        if matches!(
            remote,
            RemoteObjectRecord::CandidateCommit(
                coven_protocol::remote_object::CandidateCommitRecord {
                    state: coven_protocol::remote_object::CandidateCommitState::AbsentVerified { .. },
                    ..
                }
            ) | RemoteObjectRecord::CandidateExclusive(
                coven_protocol::remote_object::CandidateObjectRecord {
                    state: coven_protocol::remote_object::CandidateObjectState::AbsentVerified { .. },
                    ..
                }
            )
        ) && !crate::remote_object_records::delete_remote_object_on(tx, object_id)?
        {
            return Err(DbError::Message(format!(
                "Merge retraction object {object_id} disappeared during finalization"
            )));
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
                    DbError::context("serialize completed Merge retraction cleanup ref", error)
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

pub enum MergeCandidateHeadCleanup {
    Remote { complete: bool },
    ProtocolInert,
}

pub fn load_merge_candidate_head_cleanup_on(
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
            .map_err(|error| DbError::context("Merge cleanup head", error)),
        (false, true) => {
            let inert = load_protocol_inert_object_on(conn, object_id)?;
            if !inert
                .is_terminal_head_for(candidate, head)
                .map_err(|error| DbError::context("Merge cleanup inert head", error))?
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

pub fn remove_cleaned_merge_authority_on(
    tx: &rusqlite::Transaction<'_>,
    authority: &PreparedMergeCandidate,
) -> Result<(), DbError> {
    for object in [
        authority.reference.object.clone(),
        authority.head_object.clone(),
    ] {
        let object_id = remote_object_id(&object);
        let remote = load_remote_object_on(tx, object_id)?;
        if !remote
            .candidate_cleanup_complete(&authority.reference)
            .map_err(|error| {
                DbError::context(
                    format!("validate abandoned authority cleanup for {object_id}"),
                    error,
                )
            })?
        {
            return Err(DbError::Message(
                "losing Merge abandonment cleanup is incomplete".to_string(),
            ));
        }
        if !crate::remote_object_records::delete_remote_object_on(tx, object_id)? {
            return Err(DbError::Message(format!(
                "abandoned authority object {object_id} disappeared during removal"
            )));
        }
    }
    Ok(())
}

pub fn remove_cleaned_author_excluded_merge_authority_on(
    tx: &rusqlite::Transaction<'_>,
    authority: &PreparedMergeCandidate,
) -> Result<(), DbError> {
    let commit_object_id = remote_object_id(&authority.reference.object);
    let commit = load_remote_object_on(tx, commit_object_id)?;
    if !commit
        .candidate_cleanup_complete(&authority.reference)
        .map_err(|error| {
            DbError::context(
                format!("validate excluded abandonment commit cleanup for {commit_object_id}"),
                error,
            )
        })?
    {
        return Err(DbError::Message(
            "excluded Merge abandonment commit cleanup is incomplete".to_string(),
        ));
    }
    if !crate::remote_object_records::delete_remote_object_on(tx, commit_object_id)? {
        return Err(DbError::Message(
            "excluded Merge abandonment commit disappeared during removal".to_string(),
        ));
    }

    let head = &authority.head_object;
    if let MergeCandidateHeadCleanup::Remote { complete } =
        load_merge_candidate_head_cleanup_on(tx, head, &authority.reference)?
    {
        if !complete {
            return Err(DbError::Message(
                "excluded Merge abandonment head cleanup is incomplete".to_string(),
            ));
        }
        let head_object_id = remote_object_id(head);
        if !crate::remote_object_records::delete_remote_object_on(tx, head_object_id)? {
            return Err(DbError::Message(
                "excluded Merge abandonment head disappeared during removal".to_string(),
            ));
        }
    }
    Ok(())
}

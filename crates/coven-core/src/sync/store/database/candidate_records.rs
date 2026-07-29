use crate::database::*;
use crate::sync::remote_object::{remote_object_id, RemoteObjectRecord};
use crate::sync::storage::ExactObjectRef;
use crate::sync::store_commit::{
    ObjectHash, StoreBatchCommit, StoreBatchCommitRef, StoreCommitCoord, StoreDeviceHead,
    StoreDeviceRegistrationRef, StoreHistoryCut, VerifiedStoreBatchCommit,
};
use crate::write::WriteId;
use rusqlite::{Connection, OptionalExtension};
use std::collections::{BTreeMap, BTreeSet};

use super::publication_state::PreparedStoreWriteState;
use super::store_device_state::store_device_state_for_history_cut_on;
#[derive(Debug, Clone)]
pub(crate) struct PreparedMergeCandidate {
    pub(crate) commit: VerifiedStoreBatchCommit,
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

/// Record `nonactivation` against every object a losing candidate published, and
/// return the ones whose durable state now names them for deletion. An object
/// several candidates own stays until the last of them is nonactivated, so a
/// caller can never delete an object another live candidate still needs. The
/// candidate's own commit must reach a cleanup target: it is uploaded before the
/// head that decides the position, so a candidate that lost that position always
/// leaves it behind.
pub(super) fn begin_candidate_nonactivation_targets_on(
    tx: &rusqlite::Transaction<'_>,
    candidate: &StoreBatchCommitRef,
    objects: &[ExactObjectRef],
    nonactivation: &crate::sync::remote_object::CandidateNonactivation,
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
pub(super) fn candidate_cleanup_targets_on(
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

pub(super) fn require_candidate_cleanup_complete_on(
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

pub(super) fn delete_remote_objects_on(
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
        if tx
            .execute(
                "DELETE FROM remote_objects WHERE object_id = ?1",
                [object_id.to_string()],
            )
            .map_err(DbError::from)?
            != 1
        {
            return Err(DbError::Message(format!(
                "{context} object {object_id} disappeared during cleanup"
            )));
        }
    }
    Ok(())
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
    let value = VerifiedStoreBatchCommit::parse_prepared(
        commit.semantic_bytes(),
        root.store_root_hash,
        coord,
        commit.prepared().reference().clone(),
        &registration,
    )
    .map_err(|error| DbError::Message(format!("verify Merge candidate: {error}")))?;
    let reference = value.reference().clone();
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
    root: &crate::sync::store_commit::StoreRootRef,
    candidate: &StoreBatchCommitRef,
    author: &StoreDeviceRegistrationRef,
) -> Result<Option<AuthorExclusionActivationLocator>, DbError> {
    let expected_stream = crate::sync::store_commit::StreamActivation::device_authorized_stream_id(
        root.store_root_hash,
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
    let frontier =
        crate::sync::store::database::StoreDatabase::materialized_frontier_on(conn, None)?
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
        |exclusion| load_author_exclusion_activation_locator_on(conn, root, exclusion),
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
    root: &crate::sync::store_commit::StoreRootRef,
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
        crate::sync::store::database::StoreDatabase::load_retained_merge_materialization_by_ref_on(
            conn,
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
    root: &crate::sync::store_commit::StoreRootRef,
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
    validate_terminal_nonactivation_authority_on(conn, root, durable)
}

pub(crate) fn validate_terminal_nonactivation_authority_on(
    conn: &Connection,
    root: &crate::sync::store_commit::StoreRootRef,
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
        crate::sync::remote_object::CandidateNonactivationProof::MergeMembershipGrantRevocation {
            activation_commit,
            ..
        } => {
            let StoreCommitCoord {
                stream_id,
                sequence,
            } = &activation_commit.coord;
            if crate::sync::store::database::StoreDatabase::materialized_commit_ref_on(
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
            validate_terminal_nonactivation_authority_on(conn, root, dependency_nonactivation)?;
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
    root: &crate::sync::store_commit::StoreRootRef,
    write_id: &WriteId,
    candidate: &PreparedMergeCandidate,
    nonactivation: &BlockedMergeCandidateNonactivation,
    include_indexed_blobs: bool,
    extra_objects: &[ExactObjectRef],
) -> Result<(), DbError> {
    if let BlockedMergeCandidateNonactivation::Terminal { durable, .. } = nonactivation {
        validate_terminal_candidate_authority_on(tx, root, candidate, durable)?;
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

pub(crate) fn begin_merge_candidate_nonactivation_on(
    conn: &rusqlite::Transaction<'_>,
    write_id: &WriteId,
    candidate: &PreparedMergeCandidate,
    nonactivation: &crate::sync::remote_object::CandidateNonactivation,
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

pub(super) fn begin_merge_candidate_nonactivation_with_verified_head_on(
    conn: &rusqlite::Transaction<'_>,
    write_id: &WriteId,
    candidate: &PreparedMergeCandidate,
    nonactivation: &crate::sync::remote_object::CandidateNonactivation,
    include_indexed_blobs: bool,
    extra_objects: &[ExactObjectRef],
    head_nonactivation: &crate::sync::remote_object::VerifiedCandidateHeadNonactivation,
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

pub(super) fn begin_merge_candidate_nonactivation_with_head_evidence_on(
    conn: &rusqlite::Transaction<'_>,
    write_id: &WriteId,
    candidate: &PreparedMergeCandidate,
    nonactivation: &crate::sync::remote_object::CandidateNonactivation,
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

/// Read a prepared candidate's durable nonactivation proof and lift it into the
/// terminal cleanup authority it names. Returns `None` when the candidate has no
/// proof yet or its proof is a non-terminal Merge-winner (whose head is cleaned
/// by occupation, not terminal reconciliation). Shared by Merge cleanup and
/// Circle-operation discard so both derive the authority identically.
pub(super) fn terminal_candidate_verification_on(
    conn: &Connection,
    root: &crate::sync::store_commit::StoreRootRef,
    candidate: PreparedMergeCandidate,
) -> Result<Option<TerminalCandidateCleanupVerification>, DbError> {
    let remote = load_remote_object_on(conn, remote_object_id(&candidate.reference.object))?;
    let Some(proof) = remote
        .candidate_nonactivation_proof(&candidate.reference)
        .map_err(|error| DbError::Message(error.to_string()))?
    else {
        return Ok(None);
    };
    let authority = match proof {
        crate::sync::remote_object::CandidateNonactivationProof::AuthorExclusion {
            exclusion,
            ..
        } => TerminalCandidateAuthority::AuthorExclusion(
            load_author_exclusion_activation_locator_on(conn, root, exclusion)?,
        ),
        crate::sync::remote_object::CandidateNonactivationProof::MergeMembershipGrantRevocation {
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
        crate::sync::remote_object::CandidateNonactivationProof::MergeDependencyRetraction { .. } => {
            let durable = crate::sync::remote_object::CandidateNonactivation::from_durable_parts(
                &candidate.reference,
                &candidate.commit,
                proof.clone(),
            )
            .map_err(|error| DbError::Message(error.to_string()))?;
            validate_terminal_nonactivation_authority_on(conn, root, &durable)?;
            TerminalCandidateAuthority::DependencyRetraction(
                crate::sync::remote_object::VerifiedDependencyRetractionAuthority::after_live_authority_check(durable)
                    .map_err(|error| DbError::Message(error.to_string()))?,
            )
        }
        crate::sync::remote_object::CandidateNonactivationProof::MergeWinner { .. } => {
            return Ok(None)
        }
    };
    Ok(Some(TerminalCandidateCleanupVerification {
        authority,
        candidate: blocked_merge_candidate_from_prepared(candidate),
    }))
}

pub(super) fn merge_candidate_cleanup_targets_on(
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
        encoded.extend(
            extra_objects
                .iter()
                .map(|object| remote_object_id(object).to_string()),
        );
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

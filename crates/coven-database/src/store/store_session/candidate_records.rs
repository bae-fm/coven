use crate::*;
use coven_protocol::objects::ExactObjectRef;
use coven_protocol::remote_object::remote_object_id;
use coven_protocol::store_commit::{ObjectHash, StoreBatchCommitRef};
use rusqlite::Connection;
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CandidateCleanupObject {
    pub object: ExactObjectRef,
}

/// Record `nonactivation` against every object a losing candidate published, and
/// return the ones whose durable state now names them for deletion. An object
/// several candidates own stays until the last of them is nonactivated, so a
/// caller can never delete an object another live candidate still needs. The
/// candidate's own commit must reach a cleanup target: its upload may have
/// completed before publication failed, even when the caller received no
/// successful upload response.
pub(crate) fn begin_candidate_nonactivation_targets_on(
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
    let commit_complete = load_remote_object_on(tx, remote_object_id(&candidate.object))?
        .candidate_cleanup_complete(candidate)?;
    if !objects.contains(&candidate.object)
        || !(commit_complete
            || cleanup
                .iter()
                .any(|target| target.object == candidate.object))
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
pub(crate) fn candidate_cleanup_targets_on(
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
            .map_err(DbError::from)?
        {
            return Err(DbError::Message(format!(
                "candidate object {object_id} has no cleanup decision"
            )));
        }
    }
    cleanup.sort_by(|left, right| left.object.cmp(&right.object));
    Ok(cleanup)
}

pub(crate) fn require_candidate_cleanup_complete_on(
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

pub(crate) fn delete_remote_objects_on(
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

pub(super) fn load_device_exclusion_activation_on(
    records: crate::store::store_session::StoreRecords<'_>,
    retained: &mut dyn super::verified_store_authority::VerifiedStoreLookup,
    root: &coven_protocol::store_commit::StoreRootRef,
    exclusion: &coven_protocol::store_commit::StoreDeviceExclusionRef,
) -> Result<StoreBatchCommitRef, DbError> {
    let exclusion_json = serde_json::to_string(exclusion)
        .map_err(|error| DbError::context("serialize device exclusion reference", error))?;
    let activation_commit = records
        .author_exclusion_activation_row(&exclusion_json)?
        .ok_or_else(|| {
            DbError::Message("applied device exclusion has no exact activation".into())
        })?;
    let activation_commit: StoreBatchCommitRef = serde_json::from_str(&activation_commit)
        .map_err(|error| DbError::context("parse device exclusion activation commit", error))?;
    let materialization =
        retained.retained_materialization_by_ref_on(records, &activation_commit)?;
    if materialization.root() != root
        || !materialization
            .device_operations()
            .exclusions()
            .any(|candidate| candidate == exclusion)
    {
        return Err(DbError::Message(
            "device exclusion differs from its exact retained activation".into(),
        ));
    }
    Ok(activation_commit)
}

impl super::StoreTransaction<'_, '_> {
    pub(super) fn candidate_grant_nonactivation(
        self,
        authority: &mut super::verified_store_authority::VerifiedStoreAuthority,
        membership: &coven_protocol::membership::MembershipChain,
        candidate: &StoreBatchCommitRef,
        commit: &coven_protocol::store_commit::StoreBatchCommit,
        publication: &coven_protocol::store_commit::StorePublicationRef,
    ) -> Result<coven_protocol::remote_object::CandidateNonactivation, DbError> {
        let coverage = self.require_accepted_membership(authority, membership, publication)?;
        let records = self.records();
        let root = authority.required_root_authority_on(records)?;
        let registration =
            authority.activated_registration_on(records, &root, &commit.author_registration)?;
        coven_protocol::store_commit::VerifiedStoreBatchCommit::parse(
            &commit.to_bytes(),
            root.store_root_hash,
            candidate,
            &registration,
        )?;
        let creation = commit
            .membership_authority
            .as_ref()
            .ok_or_else(|| DbError::Message("candidate has no membership authority".into()))?;
        let retirement = membership
            .write_authority_retirement(creation, &registration.author_pubkey)
            .ok_or_else(|| DbError::Message("candidate has no accepted grant retirement".into()))?;
        coven_protocol::remote_object::CandidateNonactivation::from_durable_parts(
            candidate,
            commit,
            coven_protocol::remote_object::CandidateNonactivationProof::AuthorityRetirement {
                publication: publication.clone(),
                coverage,
                creation: creation.clone(),
                retirement: retirement.clone(),
            },
        )
        .map_err(DbError::from)
    }
}

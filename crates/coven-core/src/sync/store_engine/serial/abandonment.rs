use super::publication::{observe_serial_head, SerialHeadObservation};
use super::*;
use crate::database::{PreparedProtocolObject, SerialCandidateAbandonmentPreparation};
use crate::sync::storage::{
    ProtocolObjectContext, ProtocolObjectDomain, ReplaceHeadError, VersionedObject,
};
use crate::sync::store_commit::{
    commit_semantic_prefix, serial_head_key, CandidateCleanupManifest, ObjectHash,
    StoreBatchCommit, StoreBatchCommitDeletionTarget, StoreBatchCommitRef, StoreCommitCoord,
    StoreSerialHead, StoreSerialHeadState, StoreSerialPredecessor, SERIAL_STREAM_ID,
};
use crate::sync::store_objects::StoreObjectError;
use crate::sync::store_outbound::{
    load_local_store_authority, required_store_root, StoreOutboundError, SERIAL_COORDINATION_HEAD,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SerialCandidateAbandonmentWinner {
    Authority { accepted: VersionedObject },
    OriginalBranch { accepted: VersionedObject },
    Other { current: StoreSerialPredecessor },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SerialBranchAbandonment {
    Discarded,
    OriginalBranchActivated,
}

pub(crate) async fn prepare_serial_candidate_abandonment(
    db: &Database,
    storage: &dyn SyncStorage,
    device_id: &str,
    identity_signer: &UserKeypair,
    branch_id: crate::PendingBranchId,
) -> Result<bool, StoreOutboundError> {
    if let Some(prepared) = db.prepared_serial_candidate_abandonment().await? {
        if prepared.branch_id != branch_id {
            return Err(StoreOutboundError::InvalidOutbound(
                "another Serial branch already owns candidate abandonment".to_string(),
            ));
        }
        return Ok(false);
    }
    let branch = db.prepared_serial_store_branch().await?.ok_or_else(|| {
        StoreOutboundError::InvalidOutbound(
            "Serial candidate abandonment requires an exact prepared branch".to_string(),
        )
    })?;
    if branch.branch_id != branch_id {
        return Err(StoreOutboundError::InvalidOutbound(
            "Serial candidate abandonment names another prepared branch".to_string(),
        ));
    }
    let candidate = branch.writes.first().ok_or_else(|| {
        StoreOutboundError::InvalidOutbound("prepared Serial branch has no candidates".to_string())
    })?;
    let (root, registration_ref, registration, device_signer) =
        load_local_store_authority(db, device_id, identity_signer).await?;
    if candidate.commit.value.author_registration != registration_ref {
        return Err(StoreOutboundError::InvalidOutbound(
            "Serial candidate belongs to another local registration".to_string(),
        ));
    }
    let coord = StoreCommitCoord::Serial {
        sequence: candidate.commit.value.seq(),
    };
    let candidate_ref = StoreBatchCommitRef::from_commit(
        &candidate.commit.value,
        coord.clone(),
        candidate.commit.object.clone(),
    )
    .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    let commit = StoreBatchCommit::signed_with_candidate_abandonment(
        root.store_root_hash,
        candidate.commit.value.write_id.clone(),
        coord.clone(),
        registration_ref.clone(),
        &registration,
        candidate.commit.value.order.clone(),
        candidate.commit.value.membership_state.clone(),
        candidate.commit.value.device_state.clone(),
        vec![CandidateCleanupManifest {
            candidate: StoreBatchCommitDeletionTarget {
                coord: coord.clone(),
                object: candidate.commit.object.clone(),
                canonical_signed_bytes: candidate.commit.bytes.clone(),
            },
        }],
        &device_signer,
    )
    .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    let context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreCommit,
    );
    let prefix = commit_semantic_prefix(
        commit.candidate_family(),
        SERIAL_STREAM_ID,
        commit.seq(),
        commit.commit_hash(),
    );
    let slot = storage
        .allocate_protocol_slot(&context, &prefix, ".json")
        .await
        .map_err(StoreObjectError::from)?;
    let commit_prepared = storage
        .prepare_protocol_object(&context, slot, &prefix, commit.to_bytes())
        .map_err(StoreObjectError::from)?;
    let authority_ref =
        StoreBatchCommitRef::from_commit(&commit, coord, commit_prepared.reference().clone())
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    let head = StoreSerialHead::signed(
        root.store_root_hash,
        StoreSerialHeadState::Commit {
            author_registration: registration_ref,
            commit: authority_ref,
        },
        &device_signer,
    )
    .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    db.prepare_serial_candidate_abandonment(SerialCandidateAbandonmentPreparation {
        branch_id,
        candidate: candidate_ref,
        commit: PreparedProtocolObject {
            value: commit,
            prepared: commit_prepared,
        },
        head,
        original_head_bytes: branch.head.bytes,
    })
    .await?;
    Ok(true)
}

pub(crate) async fn publish_serial_candidate_abandonment(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: &dyn CoordinationStorage,
    branch_id: crate::PendingBranchId,
) -> Result<SerialCandidateAbandonmentWinner, StoreOutboundError> {
    let prepared = db
        .prepared_serial_candidate_abandonment()
        .await?
        .ok_or_else(|| {
            StoreOutboundError::InvalidOutbound(
                "Serial candidate abandonment is not prepared".to_string(),
            )
        })?;
    if prepared.branch_id != branch_id {
        return Err(StoreOutboundError::InvalidOutbound(
            "Serial abandonment belongs to another branch".to_string(),
        ));
    }
    let classify = |observed: SerialHeadObservation| {
        let accepted = observed.versioned().ok_or_else(|| {
            StoreOutboundError::InvalidOutbound(
                "Serial abandonment observed no versioned coordination head".to_string(),
            )
        })?;
        if observed.bytes() == Some(prepared.head.bytes.as_slice()) {
            return Ok(SerialCandidateAbandonmentWinner::Authority { accepted });
        }
        if observed.bytes() == Some(prepared.original_head_bytes.as_slice()) {
            return Ok(SerialCandidateAbandonmentWinner::OriginalBranch { accepted });
        }
        Ok(SerialCandidateAbandonmentWinner::Other {
            current: observed.predecessor()?,
        })
    };
    let observed = observe_serial_head(db, coordination).await?;
    if observed.bytes() == Some(prepared.head.bytes.as_slice())
        || observed.bytes() == Some(prepared.original_head_bytes.as_slice())
        || observed.predecessor()? != db.exact_serial_predecessor(prepared.base.clone()).await?
    {
        return classify(observed);
    }
    if observed.bytes() != Some(prepared.base_head.bytes.as_slice())
        || observed.version() != Some(&prepared.base_head.version)
    {
        return Err(StoreOutboundError::InvalidState {
            key: SERIAL_COORDINATION_HEAD,
            reason: "bytes or provider version changed at the abandonment base".to_string(),
        });
    }
    storage
        .create_protocol_object(&prepared.authority.prepared)
        .await
        .map_err(StoreObjectError::from)?;
    let context = ProtocolObjectContext::signed_plaintext(
        prepared.authority.value.store_root_hash,
        ProtocolObjectDomain::StoreCommit,
    );
    let prefix = commit_semantic_prefix(
        prepared.authority.value.candidate_family(),
        SERIAL_STREAM_ID,
        prepared.authority.value.seq(),
        prepared.authority.value.commit_hash(),
    );
    let opened = storage
        .read_protocol_object(&context, &prepared.authority.object, &prefix)
        .await
        .map_err(StoreObjectError::from)?;
    if opened != prepared.authority.bytes {
        return Err(StoreOutboundError::InvalidOutbound(
            "Serial abandonment commit exact readback differs from its signed bytes".to_string(),
        ));
    }
    let authority_ref = StoreBatchCommitRef::from_commit(
        &prepared.authority.value,
        StoreCommitCoord::Serial {
            sequence: prepared.authority.value.seq(),
        },
        prepared.authority.object.clone(),
    )
    .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    db.mark_candidate_commit_uploaded(authority_ref).await?;
    let activation = coordination
        .replace_head(
            serial_head_key(),
            &prepared.base_head.version,
            &prepared.head.bytes,
        )
        .await;
    match activation {
        Ok(accepted) if accepted.bytes == prepared.head.bytes => {
            Ok(SerialCandidateAbandonmentWinner::Authority { accepted })
        }
        Ok(_) => Err(StoreOutboundError::InvalidOutbound(
            "Serial abandonment head readback differs from its signed bytes".to_string(),
        )),
        Err(ReplaceHeadError::Coordination(error)) => {
            let after = observe_serial_head(db, coordination).await?;
            if after.bytes() != Some(prepared.base_head.bytes.as_slice())
                || after.version() != Some(&prepared.base_head.version)
            {
                classify(after)
            } else {
                Err(StoreOutboundError::Coordination(error))
            }
        }
        Err(ReplaceHeadError::VersionMismatch) => {
            classify(observe_serial_head(db, coordination).await?)
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn abandon_serial_branch(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: &dyn CoordinationStorage,
    device_id: &str,
    identity_signer: &UserKeypair,
    store_dir: &StoreDir,
    branch_id: crate::PendingBranchId,
) -> Result<SerialBranchAbandonment, StoreOutboundError> {
    prepare_serial_candidate_abandonment(
        db,
        storage,
        device_id,
        identity_signer,
        branch_id.clone(),
    )
    .await?;
    let prepared = db
        .prepared_serial_candidate_abandonment()
        .await?
        .ok_or_else(|| {
            StoreOutboundError::InvalidOutbound(
                "Serial candidate abandonment disappeared after preparation".to_string(),
            )
        })?;
    let winner =
        publish_serial_candidate_abandonment(db, storage, coordination, branch_id.clone()).await?;
    let root = required_store_root(db).await?;
    match winner {
        SerialCandidateAbandonmentWinner::OriginalBranch { accepted } => {
            let plan = super::pull::prepare_serial_resolution(
                db,
                storage,
                coordination,
                root.store_root_hash,
                store_dir,
                prepared.base,
                identity_signer,
            )
            .await?;
            super::pull::cleanup_serial_abandonment_authority(db, storage, &plan).await?;
            SerialDatabase::new(db)
                .remove_losing_abandonment_authority()
                .await?;
            db.complete_prepared_serial_branch(accepted).await?;
            Ok(SerialBranchAbandonment::OriginalBranchActivated)
        }
        SerialCandidateAbandonmentWinner::Authority { .. } => {
            let authority_ref = StoreBatchCommitRef::from_commit(
                &prepared.authority.value,
                StoreCommitCoord::Serial {
                    sequence: prepared.authority.value.seq(),
                },
                prepared.authority.object,
            )
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
            db.mark_serial_branch_conflict(
                branch_id.clone(),
                prepared.base.clone(),
                StoreSerialPredecessor::Commit(authority_ref),
            )
            .await?;
            finish_serial_branch_abandonment(
                db,
                storage,
                coordination,
                identity_signer,
                store_dir,
                root.store_root_hash,
                branch_id,
                prepared.base,
            )
            .await
        }
        SerialCandidateAbandonmentWinner::Other { current } => {
            db.mark_serial_branch_conflict(branch_id.clone(), prepared.base.clone(), current)
                .await?;
            finish_serial_branch_abandonment(
                db,
                storage,
                coordination,
                identity_signer,
                store_dir,
                root.store_root_hash,
                branch_id,
                prepared.base,
            )
            .await
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn finish_serial_branch_abandonment(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: &dyn CoordinationStorage,
    identity_signer: &UserKeypair,
    store_dir: &StoreDir,
    store_root_hash: ObjectHash,
    branch_id: crate::PendingBranchId,
    branch_base: Option<StoreBatchCommitRef>,
) -> Result<SerialBranchAbandonment, StoreOutboundError> {
    let plan = super::pull::prepare_serial_resolution(
        db,
        storage,
        coordination,
        store_root_hash,
        store_dir,
        branch_base,
        identity_signer,
    )
    .await?;
    super::pull::cleanup_serial_candidates(db, storage, branch_id.clone(), &plan).await?;
    super::pull::cleanup_serial_abandonment_authority(db, storage, &plan).await?;
    SerialDatabase::new(db)
        .discard_branch_after_abandonment(branch_id, plan)
        .await?;
    Ok(SerialBranchAbandonment::Discarded)
}

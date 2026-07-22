use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use rusqlite::session::{ConflictAction, ConflictType};

use super::*;
use crate::blob::decl::BlobDecls;
use crate::blob::local_cleanup::{self};
use crate::changeset::RowChange;
use crate::database::{BlobActivation, Database, DbError};
use crate::store_dir::StoreDir;
use crate::sync::apply::{apply_changeset_strict_on, ValidatedChangeset};
use crate::sync::audience_package::{AudiencePackage, PackageAudience};
use crate::sync::circle_activation::{CircleMembershipAuthority, VerifiedStreamActivationPrefix};
use crate::sync::conflict::TableSchema;
use crate::sync::membership::SerialAuthorizationState;
use crate::sync::pull::{
    advance_max_updated_at, cache_eager_blobs, local_blob_cleanup_intents, verify_package_blobs,
};
use crate::sync::session::SyncedTable;
use crate::sync::storage::{
    BlobSpoolProtection, CoordinationError, CoordinationStorage, SyncStorage,
};
use crate::sync::store_commit::{
    serial_head_key, ObjectHash, ResolvedStoreDeviceState, StoreBatchCommitRef, StoreCommitCoord,
    StoreDeviceRegistration, StoreDeviceRegistrationRef, StoreDeviceStateRef, StoreRootRef,
    StoreSerialHead, StoreSerialHeadState, StoreSerialPredecessor, VerifiedStoreDeviceOperations,
    SERIAL_STREAM_ID,
};
use crate::sync::store_objects::{
    load_founder_registration, load_founder_registration_with_root, load_registration_ref,
    load_store_protocol_root,
};
use crate::sync::store_pull::*;
use crate::sync::{
    circle, circle_ops, gate, provider, remote_object, storage, store_commit, store_objects,
    wrapped_store_key,
};

mod application;
mod history;
mod resolution;
mod snapshot_authority;

pub(crate) use application::*;
pub(crate) use history::*;
pub(crate) use resolution::cleanup_serial_abandonment_authority;
pub use resolution::SerialResolutionPlan;
pub(crate) use resolution::{
    cleanup_serial_candidates, prepare_serial_resolution, SerialResolutionCommit,
};
pub(in crate::sync::store_engine) use snapshot_authority::{
    verify_snapshot_for_acknowledgement, verify_snapshot_stability,
};

struct SerialApplicationCandidate {
    candidate: Candidate,
    device_operations: VerifiedStoreDeviceOperations,
    membership_authority: SerialAuthorizationState,
    authorization_after: SerialAuthorizationState,
}

impl AuthorizedSerialStoreEngine<'_> {
    pub(in crate::sync::store_engine) async fn pull(
        &self,
        store_dir: &StoreDir,
        identity: &UserKeypair,
    ) -> Result<StorePullResult, SyncCycleFailure> {
        pull_store_commits(
            self.db(),
            self.db().synced_tables(),
            self.storage(),
            self.coordination(),
            self.store_root().store_root_hash,
            store_dir,
            Some(identity),
        )
        .await
        .map_err(|error| SyncCycleFailure::operation("pull Store commits", error))
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn pull_store_commits<'a>(
    db: &'a Database,
    tables: &'a [crate::sync::session::SyncedTable],
    storage: &'a dyn SyncStorage,
    coordination: &'a dyn CoordinationStorage,
    store_root_hash: crate::sync::store_commit::ObjectHash,
    store_dir: &'a StoreDir,
    identity: Option<&'a UserKeypair>,
) -> Pin<Box<dyn Future<Output = Result<StorePullResult, StorePullError>> + Send + 'a>> {
    Box::pin(async move {
        let root = required_pull_root(db, store_root_hash).await?;
        let verified_root = load_store_protocol_root(storage, &root).await?.value;
        if verified_root.descriptor.write_policy != crate::WritePolicy::Serial {
            return Err(StorePullError::Database(
                "durable write policy differs from the signed Store root".to_string(),
            ));
        }
        pull_verified_store_commits(
            db,
            tables,
            storage,
            coordination,
            &root,
            verified_root,
            store_dir,
            identity,
        )
        .await
    })
}

#[allow(clippy::too_many_arguments)]
async fn pull_verified_store_commits(
    db: &Database,
    tables: &[crate::sync::session::SyncedTable],
    storage: &dyn SyncStorage,
    coordination: &dyn CoordinationStorage,
    root: &StoreRootRef,
    root_value: crate::sync::store_commit::StoreProtocolRoot,
    store_dir: &StoreDir,
    identity: Option<&UserKeypair>,
) -> Result<StorePullResult, StorePullError> {
    if root_value.descriptor.write_policy != crate::WritePolicy::Serial {
        return Err(StorePullError::Serial(
            "signed Store root is not Serial".to_string(),
        ));
    }
    let local = db.materialized_frontier().await?.remove(SERIAL_STREAM_ID);
    let head = read_serial_head(storage, coordination, root).await?.head;
    let authorized_chain = Box::pin(load_authorized_serial_chain(storage, root, &head)).await?;
    let tip = match &head.state {
        crate::sync::store_commit::StoreSerialHeadState::Genesis { .. } => None,
        crate::sync::store_commit::StoreSerialHeadState::Commit { commit, .. } => {
            Some(commit.clone())
        }
    };
    let Some(tip) = tip else {
        if local.is_some() {
            return Err(StorePullError::Serial(format!(
                "global head is genesis but the durable Serial frontier is {local:?}"
            )));
        }
        return empty_pull_result(db, store_dir, Some(head)).await;
    };
    if local
        .as_ref()
        .is_some_and(|local| local.coord.sequence() > tip.coord.sequence())
    {
        return Err(StorePullError::Serial(format!(
            "local Serial reference is ahead of the signed head: local={local:?}, head={tip:?}"
        )));
    }

    let first_unmaterialized = match local.as_ref() {
        None => 0,
        Some(local) => authorized_chain
            .iter()
            .position(|authorized| &authorized.commit_ref == local)
            .map(|index| index + 1)
            .ok_or_else(|| {
                StorePullError::Serial(format!(
                    "exact Serial predecessor chain does not reach local reference {local:?}"
                ))
            })?,
    };
    if let Some(local) = local.as_ref() {
        let authorization = authorized_chain
            .get(first_unmaterialized - 1)
            .expect("materialized Serial reference was found in the authorized chain")
            .authorization_after
            .clone();
        db.install_serial_authorization_at_position(local.clone(), authorization)
            .await?;
    }

    let mut candidates = Vec::with_capacity(authorized_chain.len() - first_unmaterialized);
    for authorized in authorized_chain.into_iter().skip(first_unmaterialized) {
        let package =
            load_serial_store_package(db, storage, &authorized.commit_ref, &authorized.commit)
                .await?;
        candidates.push(SerialApplicationCandidate {
            candidate: Candidate {
                commit_ref: authorized.commit_ref,
                commit: authorized.commit,
                author: authorized.author,
                package,
                registrations: authorized.registrations,
            },
            device_operations: authorized.device_operations,
            membership_authority: authorized.authorization_before,
            authorization_after: authorized.authorization_after,
        });
    }

    let schema: Arc<TableSchema> = {
        let tables = tables.to_vec();
        Arc::new(
            db.call(move |conn| TableSchema::from_db(conn, &tables))
                .await?,
        )
    };
    let mut row_changes = Vec::new();
    let mut authors = BTreeSet::new();
    let mut applied_candidates = 0_u64;
    for candidate in &candidates {
        let changes = match Box::pin(apply_serial_candidate(
            db,
            storage,
            store_dir,
            schema.clone(),
            candidate,
            root,
            identity,
        ))
        .await
        {
            Ok(changes) => changes,
            Err(StorePullError::BlobDownloads(failures)) if !failures.has_transport_failure() => {
                tracing::warn!(
                    stream_id = %commit_stream_id(&candidate.candidate.commit_ref.coord),
                    seq = candidate.candidate.commit_ref.coord.sequence(),
                    %failures,
                    "holding Serial commit on blob download failure"
                );
                let frontier = db.materialized_frontier().await?;
                let local_blob_cleanup_pending = local_cleanup::drain(db, store_dir).await?;
                return Ok(StorePullResult {
                    changesets_applied: applied_candidates,
                    devices_pulled: u64::try_from(authors.len()).map_err(|_| {
                        StorePullError::Serial("author count exceeds u64".to_string())
                    })?,
                    held_positions: vec![held_commit(
                        &candidate.candidate.commit_ref,
                        HeldStorePositionReason::BlobDownloadFailed,
                    )],
                    visible_heads: Vec::new(),
                    serial_head: Some(head),
                    row_changes,
                    asset_downloads_failed: true,
                    local_blob_cleanup_pending,
                    frontier,
                });
            }
            Err(error) => return Err(error),
        };
        authors.insert(candidate.candidate.author.device_id);
        row_changes.extend(changes);
        applied_candidates = applied_candidates
            .checked_add(1)
            .ok_or_else(|| StorePullError::Serial("apply count exceeds u64".to_string()))?;
    }
    let frontier = db.materialized_frontier().await?;
    let local_blob_cleanup_pending = local_cleanup::drain(db, store_dir).await?;
    Ok(StorePullResult {
        changesets_applied: applied_candidates,
        devices_pulled: u64::try_from(authors.len())
            .map_err(|_| StorePullError::Serial("author count exceeds u64".to_string()))?,
        held_positions: Vec::new(),
        visible_heads: Vec::new(),
        serial_head: Some(head),
        row_changes,
        asset_downloads_failed: false,
        local_blob_cleanup_pending,
        frontier,
    })
}

async fn empty_pull_result(
    db: &Database,
    store_dir: &StoreDir,
    serial_head: Option<StoreSerialHead>,
) -> Result<StorePullResult, StorePullError> {
    let frontier = db.materialized_frontier().await?;
    let local_blob_cleanup_pending = local_cleanup::drain(db, store_dir).await?;
    Ok(StorePullResult {
        changesets_applied: 0,
        devices_pulled: 0,
        held_positions: Vec::new(),
        visible_heads: Vec::new(),
        serial_head,
        row_changes: Vec::new(),
        asset_downloads_failed: false,
        local_blob_cleanup_pending,
        frontier,
    })
}

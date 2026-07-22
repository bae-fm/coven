use super::*;
use crate::sync::circle_ops::VerifiedCircleActivations;
use crate::sync::store_commit::{
    StoreBatchCommit, StoreBatchCommitRef, StoreDeviceRegistration, VerifiedStoreDeviceOperations,
};

#[doc(hidden)]
pub(crate) struct SerialResolutionCommit {
    pub(crate) commit: StoreBatchCommit,
    pub(crate) commit_ref: StoreBatchCommitRef,
    pub(crate) packages: Vec<AudiencePackage>,
    pub(crate) changesets: super::gate::SerialInboundChangesets,
    pub(crate) registrations: Vec<(
        StoreDeviceRegistration,
        super::store_commit::StoreDeviceRegistrationActivation,
    )>,
    pub(crate) verified_circle_activations: VerifiedCircleActivations,
    pub(crate) device_operations: VerifiedStoreDeviceOperations,
    pub(crate) authorization_after: SerialAuthorizationState,
}

#[doc(hidden)]
pub struct SerialResolutionPlan {
    pub(crate) head: StoreSerialHead,
    pub(crate) head_object: super::storage::VersionedObject,
    pub(crate) commits: Vec<SerialResolutionCommit>,
    pub(crate) verified_suffix: Option<VerifiedSerialAcceptedSuffix>,
}

impl SerialResolutionPlan {
    pub(crate) fn head(&self) -> &StoreSerialHead {
        &self.head
    }

    pub(crate) fn head_object(&self) -> &super::storage::VersionedObject {
        &self.head_object
    }

    pub(crate) fn commits(&self) -> &[SerialResolutionCommit] {
        &self.commits
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        StoreSerialHead,
        super::storage::VersionedObject,
        Vec<SerialResolutionCommit>,
    ) {
        (self.head, self.head_object, self.commits)
    }

    pub(crate) fn verified_suffix(&self) -> Result<VerifiedSerialAcceptedSuffix, StorePullError> {
        self.verified_suffix.clone().ok_or_else(|| {
            StorePullError::Serial("Serial resolution has no accepted successor suffix".to_string())
        })
    }
}

pub(crate) async fn prepare_serial_resolution(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: &dyn CoordinationStorage,
    store_root_hash: ObjectHash,
    store_dir: &StoreDir,
    branch_base: Option<StoreBatchCommitRef>,
    identity: &UserKeypair,
) -> Result<SerialResolutionPlan, StorePullError> {
    let root = db.local_store_root_ref().await?.ok_or_else(|| {
        StorePullError::Serial("Store root exact reference is absent".to_string())
    })?;
    if root.store_root_hash != store_root_hash {
        return Err(StorePullError::Serial(
            "Serial resolution root differs from durable exact root".to_string(),
        ));
    }
    let verified_head = read_serial_head(storage, coordination, &root).await?;
    let head = verified_head.head;
    let authorized_chain = load_authorized_serial_chain(storage, &root, &head).await?;
    let first = match branch_base.as_ref() {
        None => 0,
        Some(base) => authorized_chain
            .iter()
            .position(|authorized| &authorized.commit_ref == base)
            .map(|index| index + 1)
            .ok_or_else(|| {
                StorePullError::Serial(
                    "global chain does not descend from the exact conflicting branch base"
                        .to_string(),
                )
            })?,
    };
    let schema: Arc<TableSchema> = {
        let tables = db.synced_tables().to_vec();
        Arc::new(
            db.call(move |conn| TableSchema::from_db(conn, &tables))
                .await?,
        )
    };
    let mut commits = Vec::with_capacity(authorized_chain.len() - first);
    let mut prior_circle_accesses = CirclePackageAccesses::new();
    let mut verified_prefix = VerifiedStreamActivationPrefix::empty();
    for authorized in authorized_chain.into_iter().skip(first) {
        let device_operations = authorized.device_operations;
        let package =
            load_serial_store_package(db, storage, &authorized.commit_ref, &authorized.commit)
                .await?;
        let verified_circle_activations = match load_pull_circle_activations(
            db,
            storage,
            &root,
            &authorized.commit_ref,
            &authorized.commit,
            &authorized.author,
            Some(identity),
            &CircleMembershipAuthority::Serial(authorized.authorization_before.clone()),
            &verified_prefix,
        )
        .await
        {
            Ok(activations) => activations,
            Err(PullCircleActivationError::Database(error)) => return Err(error.into()),
            Err(PullCircleActivationError::Invalid(error)) => {
                return Err(StorePullError::Serial(error));
            }
        };
        let candidate = Candidate {
            commit_ref: authorized.commit_ref.clone(),
            commit: authorized.commit,
            author: authorized.author,
            package,
            registrations: authorized.registrations,
        };
        let prepared = prepare_serial_candidate(
            db,
            storage,
            store_dir,
            schema.clone(),
            &candidate,
            verified_circle_activations.circles(),
            &prior_circle_accesses,
        )
        .await?;
        for (key, access) in circle_package_accesses(verified_circle_activations.circles())
            .map_err(StorePullError::Serial)?
        {
            if prior_circle_accesses.insert(key, access).is_some() {
                return Err(StorePullError::Serial(
                    "Serial resolution repeats one exact Circle control".to_string(),
                ));
            }
        }
        verified_prefix
            .extend(verified_circle_activations.stream_activations())
            .map_err(|error| StorePullError::Serial(error.to_string()))?;
        commits.push(SerialResolutionCommit {
            commit: candidate.commit,
            commit_ref: candidate.commit_ref,
            packages: prepared.packages,
            changesets: prepared.changesets,
            registrations: candidate.registrations,
            verified_circle_activations,
            device_operations,
            authorization_after: authorized.authorization_after,
        });
    }
    let accepted_refs = commits
        .iter()
        .map(|commit| commit.commit_ref.clone())
        .collect::<Vec<_>>();
    let verified_suffix = (!accepted_refs.is_empty()).then(|| {
        VerifiedSerialAcceptedSuffix::new(
            root.store_root_hash,
            super::remote_object::SerialAcceptedSuffix {
                predecessor: branch_base,
                commits: accepted_refs,
                canonical_signed_head_bytes: verified_head.object.bytes.clone(),
                observed_version_hash: ObjectHash::digest(
                    verified_head
                        .object
                        .version
                        .cloud()
                        .as_provider()
                        .as_bytes(),
                ),
            },
        )
    });
    Ok(SerialResolutionPlan {
        head,
        head_object: verified_head.object,
        commits,
        verified_suffix,
    })
}

pub(crate) async fn cleanup_serial_candidates(
    db: &Database,
    storage: &dyn SyncStorage,
    branch_id: crate::PendingBranchId,
    plan: &SerialResolutionPlan,
) -> Result<(), StorePullError> {
    let targets = db.prepare_serial_candidate_cleanup(branch_id, plan).await?;
    for target in targets {
        super::store_objects::delete_exact_object(storage, &target.object).await?;
        db.mark_candidate_cleanup_absent(target.object).await?;
    }
    Ok(())
}

pub(crate) async fn cleanup_serial_abandonment_authority(
    db: &Database,
    storage: &dyn SyncStorage,
    plan: &SerialResolutionPlan,
) -> Result<(), StorePullError> {
    let target = db
        .prepare_serial_abandonment_authority_cleanup(plan)
        .await?;
    if let Some(target) = target {
        super::store_objects::delete_exact_object(storage, &target.object).await?;
        db.mark_candidate_cleanup_absent(target.object).await?;
    }
    Ok(())
}

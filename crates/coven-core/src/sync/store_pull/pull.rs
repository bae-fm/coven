use super::*;

#[doc(hidden)]
pub struct SerialResolutionCommit {
    pub(crate) commit: StoreBatchCommit,
    pub(crate) commit_ref: super::store_commit::StoreBatchCommitRef,
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
    pub(super) head: StoreSerialHead,
    pub(super) head_object: super::storage::VersionedObject,
    pub(super) commits: Vec<SerialResolutionCommit>,
    pub(super) verified_suffix: Option<VerifiedSerialAcceptedSuffix>,
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

pub(crate) enum ApplyOutcome {
    Applied(Vec<RowChange>),
    Held(HeldStorePositionReason),
}

pub(crate) async fn required_pull_root(
    db: &Database,
    requested_hash: ObjectHash,
) -> Result<StoreRootRef, StorePullError> {
    let root = db
        .local_store_root_ref()
        .await
        .map_err(|error| StorePullError::Database(format!("load exact Store root: {error}")))?
        .ok_or_else(|| {
            StorePullError::Database("Store root exact reference is absent".to_string())
        })?;
    if root.store_root_hash != requested_hash {
        return Err(StorePullError::Database(
            "requested Store root differs from the durable exact root reference".to_string(),
        ));
    }
    Ok(root)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn pull_serial_store_commits_with_identity<'a>(
    db: &'a Database,
    tables: &'a [SyncedTable],
    storage: &'a dyn SyncStorage,
    coordination: &'a dyn CoordinationStorage,
    store_root_hash: ObjectHash,
    store_dir: &'a StoreDir,
    identity: Option<&'a crate::keys::UserKeypair>,
) -> Pin<Box<dyn Future<Output = Result<StorePullResult, StorePullError>> + Send + 'a>> {
    Box::pin(async move {
        let root = required_pull_root(db, store_root_hash).await?;
        let verified_root = load_store_protocol_root(storage, &root).await?.value;
        if verified_root.descriptor.write_policy != crate::WritePolicy::Serial {
            return Err(StorePullError::Database(
                "durable write policy differs from the signed Store root".to_string(),
            ));
        }
        pull_serial_store_commits(
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

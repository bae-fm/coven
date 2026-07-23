use super::*;

pub(crate) enum PullCircleActivationError {
    Database(DbError),
    Invalid(String),
}

pub(crate) async fn load_circle_payload_activations(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit_ref: &StoreBatchCommitRef,
    commit: &StoreBatchCommit,
    author: &StoreDeviceRegistration,
    identity: Option<&crate::keys::UserKeypair>,
    verified_prefix: &VerifiedStreamActivationPrefix,
) -> Result<VerifiedCircleActivations, PullCircleActivationError> {
    if commit.circle_controls().is_empty() && commit.stream_activations().is_empty() {
        return VerifiedCircleActivations::none(commit, commit_ref)
            .map_err(|error| PullCircleActivationError::Invalid(error.to_string()));
    }
    let founder = database
        .sqlite()
        .get_protocol_state(super::membership_ops::OWNER_PUBKEY_STATE_KEY)
        .await
        .map_err(PullCircleActivationError::Database)?
        .ok_or_else(|| {
            PullCircleActivationError::Invalid(
                "Store founder is absent while loading circle controls".to_string(),
            )
        })?;
    Box::pin(
        crate::sync::store::circle_controls::activation::load_circle_activations_with_prefix(
            database,
            storage,
            root,
            commit_ref,
            commit,
            author,
            identity,
            &founder,
            verified_prefix,
        ),
    )
    .await
    .map_err(|error| PullCircleActivationError::Invalid(error.to_string()))
}

pub(crate) async fn load_applicable_circle_packages(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    commit_ref: &StoreBatchCommitRef,
    commit: &StoreBatchCommit,
    activations: &[crate::sync::store::circle_controls::VerifiedCircleReference],
    author: &StoreDeviceRegistration,
    local_store_membership: LocalStoreMembership,
) -> Result<Vec<LoadedCirclePackage>, PullCircleActivationError> {
    let db = database.sqlite();
    if commit.circle_packages().is_empty() {
        return Ok(Vec::new());
    }
    let mut replay_epochs = database
        .circle_replay_epoch_index()
        .await
        .map_err(PullCircleActivationError::Database)?;
    replay_epochs
        .include_verified_activations(activations)
        .map_err(PullCircleActivationError::Database)?;
    let mut loaded = Vec::new();
    for reference in commit.circle_packages() {
        let same_commit = activations.iter().find(|activation| {
            activation.circle_id == reference.circle_id
                && activation.control.coord == reference.control
        });
        if !replay_epochs
            .permits(commit_ref, reference.circle_id, &reference.control)
            .map_err(PullCircleActivationError::Database)?
        {
            debug!(
                circle_id = %reference.circle_id,
                control = ?reference.control,
                "skipping Circle package beyond its accepted epoch cutoff"
            );
            continue;
        }
        if matches!(
            local_store_membership,
            LocalStoreMembership::IdentityNotSupplied
        ) {
            return Err(PullCircleActivationError::Invalid(format!(
                "commit {} carries Circle packages but no verified local Store membership was supplied",
                commit.seq()
            )));
        }
        if matches!(local_store_membership, LocalStoreMembership::Removed) {
            debug!(
                circle_id = %reference.circle_id,
                control = ?reference.control,
                "skipping Circle package for an identity removed from Store membership"
            );
            continue;
        }
        if reference.package.schema_version > db.schema_version() {
            return Err(PullCircleActivationError::Invalid(format!(
                "Circle package for {} requires schema {}, local schema is {}",
                reference.circle_id,
                reference.package.schema_version,
                db.schema_version()
            )));
        }
        let context = if let Some(activation) = same_commit {
            let Some(access) = activation
                .package_access()
                .map_err(|error| PullCircleActivationError::Invalid(error.to_string()))?
            else {
                debug!(
                    circle_id = %reference.circle_id,
                    control = ?reference.control,
                    "skipping Circle package without active local access"
                );
                continue;
            };
            if !access.writers.contains(&author.author_pubkey) {
                return Err(PullCircleActivationError::Invalid(format!(
                    "Circle package author is not a member of {} at its exact control",
                    reference.circle_id
                )));
            }
            if access.key_fingerprint != reference.key_fingerprint {
                return Err(PullCircleActivationError::Invalid(format!(
                    "Circle package key for {} differs from its activated control",
                    reference.circle_id
                )));
            }
            access.encryption
        } else {
            let Some(access) = database
                .circle_package_access(reference.circle_id, reference.control.clone())
                .await
                .map_err(PullCircleActivationError::Database)?
            else {
                debug!(
                    circle_id = %reference.circle_id,
                    control = ?reference.control,
                    "skipping Circle package without durable local access"
                );
                continue;
            };
            if !access.writers.contains(&author.author_pubkey) {
                return Err(PullCircleActivationError::Invalid(format!(
                    "Circle package author is not a member of {} at its exact control",
                    reference.circle_id
                )));
            }
            if access.key_fingerprint != reference.key_fingerprint {
                return Err(PullCircleActivationError::Invalid(format!(
                    "Circle package key for {} differs from durable access",
                    reference.circle_id
                )));
            }
            access.encryption
        };
        let blob_protection = BlobSpoolProtection::Opaque(context.clone());
        let package = load_circle_package(storage, commit_ref, commit, reference, context)
            .await
            .map_err(|error| PullCircleActivationError::Invalid(error.to_string()))?;
        loaded.push(LoadedCirclePackage {
            reference: reference.clone(),
            bytes: package.value,
            blob_protection,
        });
    }
    Ok(loaded)
}

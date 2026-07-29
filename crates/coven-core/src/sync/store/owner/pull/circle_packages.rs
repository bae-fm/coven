use super::*;

pub(crate) enum PullCircleActivationError {
    Database(DbError),
    Invalid(String),
}

pub(crate) async fn load_circle_payload_activations(
    database: &StoreDatabase,
    commit_verifier: &mut StoreCommitVerifier<'_>,
    verified: &VerifiedStoreBatchCommit,
    identity: Option<&crate::keys::UserKeypair>,
    routing_key: Option<&crate::sync::circle::RowRoutingKey>,
    verified_prefix: &VerifiedStreamActivationPrefix,
    verified_membership_prefix: &VerifiedMergeMembershipPrefix,
) -> Result<VerifiedCircleActivations, PullCircleActivationError> {
    let commit = verified.value();
    let commit_ref = verified.reference();
    if commit.circle_controls().is_empty() && commit.stream_activations().is_empty() {
        return VerifiedCircleActivations::none(commit, commit_ref)
            .map_err(|error| PullCircleActivationError::Invalid(error.to_string()));
    }
    Box::pin(
        super::super::circles::activation::load_circle_activations_with_prefix(
            database,
            commit_verifier,
            verified,
            identity,
            routing_key,
            verified_prefix,
            verified_membership_prefix,
        ),
    )
    .await
    .map_err(|error| PullCircleActivationError::Invalid(error.to_string()))
}

pub(crate) async fn load_applicable_circle_packages(
    database: &StoreDatabase,
    commit_verifier: &mut StoreCommitVerifier<'_>,
    verified: &VerifiedStoreBatchCommit,
    activations: &[crate::sync::store::circle_controls::VerifiedCircleReference],
    author: &StoreDeviceRegistration,
    local_store_membership: LocalStoreMembership,
) -> Result<Vec<LoadedCirclePackage>, PullCircleActivationError> {
    let root = commit_verifier.root().clone();
    let db = database.sqlite();
    let commit_ref = verified.reference();
    let commit = verified.value();
    if commit.circle_packages().is_empty() {
        return Ok(Vec::new());
    }
    let mut replay_epochs = database
        .circle_replay_epoch_index(commit_verifier.root().clone())
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
        if database
            .circle_is_deleted(reference.circle_id)
            .await
            .map_err(PullCircleActivationError::Database)?
        {
            debug!(
                circle_id = %reference.circle_id,
                control = ?reference.control,
                "skipping Circle package for a deleted Circle"
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
        let exact_access = if let Some(activation) = same_commit {
            activation
                .package_access()
                .map_err(|error| PullCircleActivationError::Invalid(error.to_string()))?
        } else {
            database
                .circle_package_access(root.clone(), reference.circle_id, reference.control.clone())
                .await
                .map_err(PullCircleActivationError::Database)?
        };
        let access = if let Some(access) = exact_access {
            access
        } else {
            let Some(keyring) = database
                .circle_historical_package_keyring(
                    root.clone(),
                    reference.circle_id,
                    reference.control.clone(),
                    reference.key_fingerprint,
                )
                .await
                .map_err(PullCircleActivationError::Database)?
            else {
                debug!(
                    circle_id = %reference.circle_id,
                    control = ?reference.control,
                    "skipping Circle package without active local or successor access"
                );
                continue;
            };
            let Some((historical, historical_commit_ref)) = database
                .verified_circle_activation_context(
                    root.clone(),
                    reference.circle_id,
                    reference.control.clone(),
                )
                .await
                .map_err(PullCircleActivationError::Database)?
            else {
                return Err(PullCircleActivationError::Invalid(format!(
                    "Circle {} historical package control is not retained",
                    reference.circle_id
                )));
            };
            let historical_commit = commit_verifier
                .load_ref(&historical_commit_ref)
                .await
                .map_err(|error| {
                    PullCircleActivationError::Invalid(format!(
                        "load Circle {} historical package control: {error}",
                        reference.circle_id
                    ))
                })?;
            let roster_chain = super::super::circles::activation::load_circle_control_roster_chain(
                database,
                commit_verifier,
                &historical_commit,
                &historical.reference,
                &historical.control,
                &keyring,
            )
            .await
            .map_err(|error| PullCircleActivationError::Invalid(error.to_string()))?;
            let roster = roster_chain.try_resolved().map_err(|error| {
                PullCircleActivationError::Invalid(format!(
                    "resolve Circle {} historical package roster: {error}",
                    reference.circle_id
                ))
            })?;
            crate::sync::store::circle_controls::CirclePackageAccess::from_historical(
                reference.circle_id,
                reference.key_fingerprint,
                &keyring,
                &roster,
            )
            .map_err(|error| PullCircleActivationError::Invalid(error.to_string()))?
        };
        let package = access
            .open_package(commit_verifier.storage(), verified, reference, author)
            .await
            .map_err(|error| PullCircleActivationError::Invalid(error.to_string()))?;
        loaded.push(LoadedCirclePackage {
            reference: reference.clone(),
            bytes: package.object.value,
            blob_protection: package.blob_protection,
        });
    }
    Ok(loaded)
}

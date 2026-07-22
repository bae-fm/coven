use super::*;

pub(super) enum PullCircleActivationError {
    Database(DbError),
    Invalid(String),
}

pub(super) async fn load_pull_circle_activations(
    db: &Database,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit_ref: &StoreBatchCommitRef,
    commit: &StoreBatchCommit,
    author: &StoreDeviceRegistration,
    identity: Option<&crate::keys::UserKeypair>,
    membership_authority: &CircleMembershipAuthority,
    verified_prefix: &VerifiedStreamActivationPrefix,
) -> Result<VerifiedCircleActivations, PullCircleActivationError> {
    if matches!(
        commit.control(),
        Some(super::store_commit::StoreControl::MergeMembership { .. })
    ) {
        return verify_merge_membership_control(storage, root, commit_ref, commit)
            .await
            .map_err(PullCircleActivationError::Invalid);
    }
    if !carries_circle_payload(commit) {
        return VerifiedCircleActivations::none(commit, commit_ref)
            .map_err(|error| PullCircleActivationError::Invalid(error.to_string()));
    }
    let identity = identity.ok_or_else(|| {
        PullCircleActivationError::Invalid(format!(
            "commit {} carries circle controls but no device identity was supplied",
            commit.seq()
        ))
    })?;
    let founder = db
        .get_protocol_state(super::membership_ops::OWNER_PUBKEY_STATE_KEY)
        .await
        .map_err(PullCircleActivationError::Database)?
        .ok_or_else(|| {
            PullCircleActivationError::Invalid(
                "Store founder is absent while loading circle controls".to_string(),
            )
        })?;
    Box::pin(
        super::circle_activation::load_circle_activations_with_prefix(
            db,
            storage,
            root,
            commit_ref,
            commit,
            author,
            identity,
            &founder,
            membership_authority,
            verified_prefix,
        ),
    )
    .await
    .map_err(|error| PullCircleActivationError::Invalid(error.to_string()))
}

pub(super) async fn load_applicable_circle_packages(
    db: &Database,
    storage: &dyn SyncStorage,
    commit_ref: &StoreBatchCommitRef,
    commit: &StoreBatchCommit,
    activations: &[super::circle_ops::VerifiedCircleReference],
    author: &StoreDeviceRegistration,
) -> Result<Vec<LoadedCirclePackage>, PullCircleActivationError> {
    load_applicable_circle_packages_with_prior_accesses(
        db,
        storage,
        commit_ref,
        commit,
        activations,
        author,
        &CirclePackageAccesses::new(),
    )
    .await
}

fn circle_package_access(
    activation: &super::circle_ops::VerifiedCircleReference,
) -> Result<Option<CirclePackageAccess>, String> {
    let Some(access) = activation.local_access.as_ref() else {
        return Ok(None);
    };
    let Some(active) = access.active.as_ref() else {
        return Ok(None);
    };
    if !active.roster.verify() {
        return Err(format!(
            "Circle {} package roster is invalid",
            activation.circle_id
        ));
    }
    let super::circle::CircleAccessDisposition::Active {
        keyring,
        key_fingerprint,
        ..
    } = &access.leaf.value.disposition
    else {
        return Err(format!(
            "active Circle access for {} has an inactive leaf",
            activation.circle_id
        ));
    };
    if *key_fingerprint != activation.control.value.key_fingerprint() {
        return Err(format!(
            "Circle package key for {} differs from its activated control",
            activation.circle_id
        ));
    }
    let keyring = MasterKeyring::from_serialized(keyring).map_err(|error| {
        format!(
            "parse Circle package keyring for {}: {error}",
            activation.circle_id
        )
    })?;
    let encryption = EncryptionService::from(keyring)
        .service_for_fingerprint(key_fingerprint.as_bytes())
        .map_err(|error| {
            format!(
                "select Circle package key for {}: {error}",
                activation.circle_id
            )
        })?;
    Ok(Some(CirclePackageAccess {
        encryption,
        key_fingerprint: *key_fingerprint,
        writers: active.roster.members().keys().cloned().collect(),
    }))
}

pub(super) fn circle_package_accesses(
    activations: &[super::circle_ops::VerifiedCircleReference],
) -> Result<CirclePackageAccesses, String> {
    let mut accesses = CirclePackageAccesses::new();
    for activation in activations {
        let Some(access) = circle_package_access(activation)? else {
            continue;
        };
        let key = (activation.circle_id, activation.control.coord.clone());
        if accesses.insert(key, access).is_some() {
            return Err(format!(
                "Circle {} has duplicate package access at one control",
                activation.circle_id
            ));
        }
    }
    Ok(accesses)
}

pub(super) async fn load_applicable_circle_packages_with_prior_accesses(
    db: &Database,
    storage: &dyn SyncStorage,
    commit_ref: &StoreBatchCommitRef,
    commit: &StoreBatchCommit,
    activations: &[super::circle_ops::VerifiedCircleReference],
    author: &StoreDeviceRegistration,
    prior_accesses: &CirclePackageAccesses,
) -> Result<Vec<LoadedCirclePackage>, PullCircleActivationError> {
    let mut loaded = Vec::new();
    for reference in commit.circle_packages() {
        if reference.package.schema_version > db.schema_version() {
            return Err(PullCircleActivationError::Invalid(format!(
                "Circle package for {} requires schema {}, local schema is {}",
                reference.circle_id,
                reference.package.schema_version,
                db.schema_version()
            )));
        }
        let same_commit = activations.iter().find(|activation| {
            activation.circle_id == reference.circle_id
                && activation.control.coord == reference.control
        });
        let context = if let Some(activation) = same_commit {
            let Some(access) =
                circle_package_access(activation).map_err(PullCircleActivationError::Invalid)?
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
        } else if let Some(access) =
            prior_accesses.get(&(reference.circle_id, reference.control.clone()))
        {
            if !access.writers.contains(&author.author_pubkey) {
                return Err(PullCircleActivationError::Invalid(format!(
                    "Circle package author is not a member of {} at its exact control",
                    reference.circle_id
                )));
            }
            if access.key_fingerprint != reference.key_fingerprint {
                return Err(PullCircleActivationError::Invalid(format!(
                    "Circle package key for {} differs from prepared access",
                    reference.circle_id
                )));
            }
            access.encryption.clone()
        } else {
            let Some((encryption, key_fingerprint)) = db
                .circle_access_context(reference.circle_id, reference.control.clone())
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
            if !db
                .circle_authorizes_writer(
                    reference.circle_id,
                    reference.control.clone(),
                    author.author_pubkey.clone(),
                )
                .await
                .map_err(PullCircleActivationError::Database)?
            {
                return Err(PullCircleActivationError::Invalid(format!(
                    "Circle package author is not a member of {} at its exact control",
                    reference.circle_id
                )));
            }
            if key_fingerprint != reference.key_fingerprint {
                return Err(PullCircleActivationError::Invalid(format!(
                    "Circle package key for {} differs from durable access",
                    reference.circle_id
                )));
            }
            encryption
                .service_for_fingerprint(reference.key_fingerprint.as_bytes())
                .map_err(|error| {
                    PullCircleActivationError::Invalid(format!(
                        "select durable Circle package key for {}: {error}",
                        reference.circle_id
                    ))
                })?
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

pub(crate) async fn load_serial_store_package(
    db: &Database,
    storage: &dyn SyncStorage,
    commit_ref: &StoreBatchCommitRef,
    commit: &StoreBatchCommit,
) -> Result<Option<Vec<u8>>, StorePullError> {
    if let Some(package) = commit.store_package() {
        if package.schema_version > db.schema_version() {
            return Err(StorePullError::Serial(format!(
                "commit {} requires schema {}, local schema is {}",
                commit.seq(),
                package.schema_version,
                db.schema_version()
            )));
        }
    }
    match load_store_package(storage, commit_ref, commit).await? {
        Some(package) => Ok(Some(package.value)),
        None if commit.store_package().is_none() => Ok(None),
        None => Err(StorePullError::Serial(format!(
            "commit {} Store package is absent",
            commit.seq()
        ))),
    }
}

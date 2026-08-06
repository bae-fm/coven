use tracing::debug;

use crate::storage::run_blocking_object_verification;
use crate::sync::store::owner::pull::{LoadedCirclePackage, LocalStoreMembership};
use crate::sync::store::owner::verified_history::MergeHistoryVerifier;
use coven_database::{DbError, StoreDatabase};
use coven_protocol::objects::VerifiedObject;
use coven_protocol::store_commit::{
    CirclePackageRef, StoreDeviceRegistration, StoreProtocolError, VerifiedStoreBatchCommit,
};

pub(crate) enum CirclePackageReadError {
    Database(DbError),
    Invalid(String),
}

pub(crate) struct OpenedCirclePackage {
    pub(crate) object: VerifiedObject<Vec<u8>>,
    pub(crate) blob_protection: coven_protocol::objects::BlobSpoolProtection,
}

pub(crate) struct CirclePackageReader<'operation, 'storage> {
    database: &'operation StoreDatabase,
    storage: &'storage dyn crate::storage::SyncStorage,
    history: &'operation mut MergeHistoryVerifier<'storage>,
}

impl<'operation, 'storage> CirclePackageReader<'operation, 'storage> {
    pub(crate) fn new(
        database: &'operation StoreDatabase,
        storage: &'storage dyn crate::storage::SyncStorage,
        history: &'operation mut MergeHistoryVerifier<'storage>,
    ) -> Self {
        Self {
            database,
            storage,
            history,
        }
    }

    fn root(&self) -> &coven_protocol::store_commit::StoreRootRef {
        self.history.verified_root().reference()
    }

    pub(crate) async fn open_package(
        &self,
        access: &crate::sync::store::circle_controls::CircleEpochAccess,
        verified: &VerifiedStoreBatchCommit,
        reference: &CirclePackageRef,
        author: &StoreDeviceRegistration,
    ) -> Result<OpenedCirclePackage, CirclePackageReadError> {
        access
            .authorize_package(reference, author)
            .map_err(|error| CirclePackageReadError::Invalid(error.to_string()))?;
        let commit = verified.value();
        if !commit
            .circle_packages()
            .iter()
            .any(|committed| committed == reference)
        {
            return Err(CirclePackageReadError::Invalid(
                StoreProtocolError::MissingCirclePackage(reference.circle_id).to_string(),
            ));
        }
        let semantic_prefix = coven_protocol::store_commit::circle_package_semantic_prefix(
            reference.circle_id,
            commit.candidate_family(),
            &verified.reference().coord.stream_id.to_string(),
            commit.seq(),
            reference.package.content_hash,
        );
        let context = access.protocol_context(
            commit.store_root_hash,
            coven_protocol::objects::ProtocolObjectDomain::CirclePackage,
        );
        let bytes = self
            .storage
            .read_protocol_object(&context, &reference.package.object, &semantic_prefix)
            .await
            .map_err(|error| CirclePackageReadError::Invalid(error.to_string()))?;
        let verify_bytes = bytes.clone();
        let expected_commit = commit.clone();
        let expected_circle_id = reference.circle_id;
        let value = run_blocking_object_verification(
            &semantic_prefix,
            &reference.package.object,
            Box::new(move || {
                expected_commit.verify_circle_package(expected_circle_id, &verify_bytes)?;
                Ok(verify_bytes)
            }),
        )
        .await
        .map_err(|error| CirclePackageReadError::Invalid(error.to_string()))?;
        Ok(OpenedCirclePackage {
            object: VerifiedObject {
                value,
                bytes,
                semantic_hash: reference.package.content_hash,
                object: reference.package.object.clone(),
            },
            blob_protection: access.blob_protection(),
        })
    }

    pub(crate) async fn load_applicable(
        &mut self,
        verified: &VerifiedStoreBatchCommit,
        activations: &[crate::sync::store::circle_controls::VerifiedCircleReference],
        author: &StoreDeviceRegistration,
        local_store_membership: LocalStoreMembership,
    ) -> Result<Vec<LoadedCirclePackage>, CirclePackageReadError> {
        let root = self.root().clone();
        let commit_ref = verified.reference();
        let commit = verified.value();
        if commit.circle_packages().is_empty() {
            return Ok(Vec::new());
        }
        let mut replay_epochs = self
            .database
            .circle_replay_epoch_index(root.clone())
            .await
            .map_err(CirclePackageReadError::Database)?;
        replay_epochs
            .include_verified_activations(activations)
            .map_err(CirclePackageReadError::Database)?;
        let mut loaded = Vec::new();
        for reference in commit.circle_packages() {
            let same_commit = activations.iter().find(|activation| {
                activation.circle_id == reference.circle_id
                    && activation.control.coord == reference.control
            });
            if !replay_epochs
                .permits(commit_ref, reference.circle_id, &reference.control)
                .map_err(CirclePackageReadError::Database)?
            {
                debug!(
                    circle_id = %reference.circle_id,
                    control = ?reference.control,
                    "skipping Circle package beyond its accepted epoch cutoff"
                );
                continue;
            }
            if self
                .database
                .circle_is_deleted(reference.circle_id)
                .await
                .map_err(CirclePackageReadError::Database)?
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
                return Err(CirclePackageReadError::Invalid(format!(
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
            if reference.package.schema_version > self.database.schema_version() {
                return Err(CirclePackageReadError::Invalid(format!(
                    "Circle package for {} requires schema {}, local schema is {}",
                    reference.circle_id,
                    reference.package.schema_version,
                    self.database.schema_version()
                )));
            }
            let exact_access = if let Some(activation) = same_commit {
                activation
                    .epoch_access()
                    .map_err(|error| CirclePackageReadError::Invalid(error.to_string()))?
            } else {
                self.database
                    .circle_epoch_access(
                        root.clone(),
                        reference.circle_id,
                        reference.control.clone(),
                    )
                    .await
                    .map_err(CirclePackageReadError::Database)?
            };
            let access = if let Some(access) = exact_access {
                access
            } else {
                let Some(keyring) = self
                    .database
                    .circle_historical_package_keyring(
                        root.clone(),
                        reference.circle_id,
                        reference.control.clone(),
                        reference.key_fingerprint,
                    )
                    .await
                    .map_err(CirclePackageReadError::Database)?
                else {
                    debug!(
                        circle_id = %reference.circle_id,
                        control = ?reference.control,
                        "skipping Circle package without active local or successor access"
                    );
                    continue;
                };
                let Some((historical, historical_commit_ref)) = self
                    .database
                    .verified_circle_activation_context(
                        root.clone(),
                        reference.circle_id,
                        reference.control.clone(),
                    )
                    .await
                    .map_err(CirclePackageReadError::Database)?
                else {
                    return Err(CirclePackageReadError::Invalid(format!(
                        "Circle {} historical package control is not retained",
                        reference.circle_id
                    )));
                };
                let historical_commit = self
                    .history
                    .load_ref(&historical_commit_ref)
                    .await
                    .map_err(|error| {
                        CirclePackageReadError::Invalid(format!(
                            "load Circle {} historical package control: {error}",
                            reference.circle_id
                        ))
                    })?;
                let roster_chain = super::activation::CircleActivationVerifier::new(
                    self.database,
                    self.storage,
                    self.history,
                )
                .load_control_roster_chain(
                    &historical_commit,
                    &historical.reference,
                    &historical.control,
                    &keyring,
                )
                .await
                .map_err(|error| CirclePackageReadError::Invalid(error.to_string()))?;
                let roster = roster_chain.try_resolved().map_err(|error| {
                    CirclePackageReadError::Invalid(format!(
                        "resolve Circle {} historical package roster: {error}",
                        reference.circle_id
                    ))
                })?;
                crate::sync::store::circle_controls::CircleEpochAccess::from_historical(
                    reference.circle_id,
                    reference.key_fingerprint,
                    &keyring,
                    &roster,
                )
                .map_err(|error| CirclePackageReadError::Invalid(error.to_string()))?
            };
            let package = self
                .open_package(&access, verified, reference, author)
                .await?;
            loaded.push(LoadedCirclePackage {
                reference: reference.clone(),
                bytes: package.object.value,
                blob_protection: package.blob_protection,
            });
        }
        Ok(loaded)
    }
}

use tracing::debug;

use crate::sync::store::commit_verification::merge_history::MergeHistoryVerifier;
use crate::sync::store::pull::{LoadedCirclePackage, LocalStoreMembership};
use coven_database::store::CirclePackageAccess;
use coven_database::{DbError, StoreDatabase};
use coven_protocol::circle_activation::{
    VerifiedCircleActivations, VerifiedStreamActivationPrefix,
};
use coven_protocol::objects::VerifiedObject;
use coven_protocol::store_commit::{
    CirclePackageRef, StoreDeviceRegistration, StoreProtocolError, VerifiedStoreBatchCommit,
};
use coven_storage::run_blocking_object_verification;

#[derive(Debug, thiserror::Error)]
pub enum CirclePackageReadError {
    #[error("Circle package database: {0}")]
    Database(#[from] DbError),
    #[error("Circle package is invalid: {0}")]
    Invalid(String),
    #[error("Circle package state: {0}")]
    CircleState(#[from] coven_protocol::circle_activation::CircleStateError),
    #[error("Circle package storage: {0}")]
    Storage(#[from] coven_protocol::objects::StorageError),
    #[error("Circle package object: {0}")]
    StoreObject(#[from] coven_protocol::objects::StoreObjectError),
    #[error("Circle package sync cycle: {0}")]
    SyncCycle(#[source] Box<crate::sync::cycle::SyncCycleFailure>),
    #[error("Circle package operation: {0}")]
    CircleOperation(#[source] Box<crate::sync::store::circles::CircleOperationError>),
    #[error("Circle package pull: {0}")]
    Pull(#[source] Box<crate::sync::store::StorePullError>),
    #[error("Circle package roster: {0}")]
    Roster(#[from] coven_protocol::circle_roster::CircleRosterError),
}

impl From<crate::sync::cycle::SyncCycleFailure> for CirclePackageReadError {
    fn from(error: crate::sync::cycle::SyncCycleFailure) -> Self {
        Self::SyncCycle(Box::new(error))
    }
}

impl From<crate::sync::store::circles::CircleOperationError> for CirclePackageReadError {
    fn from(error: crate::sync::store::circles::CircleOperationError) -> Self {
        Self::CircleOperation(Box::new(error))
    }
}

impl From<crate::sync::store::StorePullError> for CirclePackageReadError {
    fn from(error: crate::sync::store::StorePullError) -> Self {
        Self::Pull(Box::new(error))
    }
}

pub(crate) struct OpenedCirclePackage {
    pub(crate) object: VerifiedObject<Vec<u8>>,
}

pub(crate) struct CirclePackageReader<'operation, 'storage> {
    database: &'operation StoreDatabase,
    storage: &'storage dyn coven_storage::CloudSyncObjectStorage,
    history: &'operation mut MergeHistoryVerifier<'storage>,
}

impl<'operation, 'storage> CirclePackageReader<'operation, 'storage> {
    pub(crate) fn new(
        database: &'operation StoreDatabase,
        storage: &'storage dyn coven_storage::CloudSyncObjectStorage,
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
        access: &coven_protocol::circle_activation::CircleEpochAccess,
        verified: &VerifiedStoreBatchCommit,
        reference: &CirclePackageRef,
        author: &StoreDeviceRegistration,
    ) -> Result<OpenedCirclePackage, CirclePackageReadError> {
        access
            .authorize_package(reference, author)
            .map_err(CirclePackageReadError::from)?;
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
            .map_err(CirclePackageReadError::from)?;
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
        .map_err(CirclePackageReadError::from)?;
        Ok(OpenedCirclePackage {
            object: VerifiedObject {
                value,
                bytes,
                semantic_hash: reference.package.content_hash,
                object: reference.package.object.clone(),
            },
        })
    }

    pub(crate) async fn load_applicable(
        &mut self,
        verified: &VerifiedStoreBatchCommit,
        prepared: &[&VerifiedCircleActivations],
        snapshot_access: &[coven_database::StagedCircleAccess],
        author: &StoreDeviceRegistration,
        local_store_membership: LocalStoreMembership,
    ) -> Result<Vec<LoadedCirclePackage>, CirclePackageReadError> {
        self.load_selected(
            verified,
            verified.value().circle_packages(),
            prepared,
            snapshot_access,
            author,
            local_store_membership,
        )
        .await
    }

    pub(crate) async fn load_selected(
        &mut self,
        verified: &VerifiedStoreBatchCommit,
        references: &[CirclePackageRef],
        prepared: &[&VerifiedCircleActivations],
        snapshot_access: &[coven_database::StagedCircleAccess],
        author: &StoreDeviceRegistration,
        local_store_membership: LocalStoreMembership,
    ) -> Result<Vec<LoadedCirclePackage>, CirclePackageReadError> {
        let root = self.root().clone();
        let commit_ref = verified.reference();
        let commit = verified.value();
        if references.is_empty() {
            return Ok(Vec::new());
        }
        let activations = prepared
            .iter()
            .flat_map(|group| group.circles())
            .chain(snapshot_access.iter().map(|access| &access.activation))
            .cloned()
            .collect::<Vec<_>>();
        let mut verified_prefix = VerifiedStreamActivationPrefix::empty();
        for group in prepared {
            verified_prefix
                .include(group.stream_activations())
                .map_err(|error| CirclePackageReadError::Invalid(error.to_string()))?;
        }
        let mut replay_epochs = self
            .database
            .circle_replay_epoch_index(root.clone())
            .await
            .map_err(CirclePackageReadError::Database)?;
        replay_epochs
            .include_verified_activations(&activations)
            .map_err(CirclePackageReadError::Database)?;
        let mut loaded = Vec::new();
        for reference in references {
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
            let Some(package_access) = self
                .database
                .circle_package_access(
                    root.clone(),
                    reference.circle_id,
                    reference.control.clone(),
                    reference.key_fingerprint,
                    activations.clone(),
                )
                .await
                .map_err(CirclePackageReadError::Database)?
            else {
                debug!(circle_id = %reference.circle_id, control = ?reference.control,
                    "skipping Circle package without applicable local access");
                continue;
            };
            let access = match package_access {
                CirclePackageAccess::Exact(access) => access,
                CirclePackageAccess::Historical(keyring) => {
                    let prepared_historical = prepared
                        .iter()
                        .find_map(|group| {
                            group
                                .circles()
                                .iter()
                                .find(|activation| {
                                    activation.circle_id == reference.circle_id
                                        && activation.control.coord == reference.control
                                })
                                .map(|activation| {
                                    (
                                        activation.clone(),
                                        group.stream_activations().activating_commit().clone(),
                                    )
                                })
                        })
                        .or_else(|| {
                            snapshot_access
                                .iter()
                                .find(|access| {
                                    access.activation.circle_id == reference.circle_id
                                        && access.activation.control.coord == reference.control
                                })
                                .map(|access| {
                                    (access.activation.clone(), access.activating_commit.clone())
                                })
                        });
                    let historical_context = match prepared_historical {
                        Some(context) => Some(context),
                        None => self
                            .database
                            .verified_circle_activation_context(
                                root.clone(),
                                reference.circle_id,
                                reference.control.clone(),
                            )
                            .await
                            .map_err(CirclePackageReadError::Database)?,
                    };
                    let Some((historical, historical_commit_ref)) = historical_context else {
                        return Err(CirclePackageReadError::Invalid(format!(
                            "Circle {} historical package control is not retained",
                            reference.circle_id
                        )));
                    };
                    let historical_commit = self.history.load_ref(&historical_commit_ref).await?;
                    let roster_chain = super::activation::CircleActivationVerifier::new(
                        self.database,
                        self.storage,
                        self.history,
                    )
                    .load_control_roster_chain_with_prefix(
                        &historical_commit,
                        &historical.reference,
                        &historical.control,
                        &keyring,
                        &verified_prefix,
                    )
                    .await
                    .map_err(CirclePackageReadError::from)?;
                    let roster = roster_chain.try_resolved()?;
                    coven_protocol::circle_activation::CircleEpochAccess::from_historical(
                        reference.circle_id,
                        reference.key_fingerprint,
                        &keyring,
                        &roster,
                    )
                    .map_err(CirclePackageReadError::from)?
                }
            };
            let package = self
                .open_package(&access, verified, reference, author)
                .await?;
            loaded.push(LoadedCirclePackage {
                reference: reference.clone(),
                bytes: package.object.value,
            });
        }
        Ok(loaded)
    }
}

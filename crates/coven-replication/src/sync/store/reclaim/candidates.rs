use super::*;

/// The package in one commit that could have published a blob of this audience.
/// The blob's own locator names its audience, and a commit carries at most one
/// package per audience, so the audience selects the package outright.
pub(super) fn audience_blob_binding_package(
    commit: &coven_protocol::store_commit::StoreBatchCommit,
    audience: coven_protocol::blob::locator::RemoteAudience,
) -> Option<AudienceBlobBindingPackage> {
    match audience {
        coven_protocol::blob::locator::RemoteAudience::Store => commit
            .store_package()
            .cloned()
            .map(AudienceBlobBindingPackage::Store),
        coven_protocol::blob::locator::RemoteAudience::Circle(circle_id) => commit
            .circle_packages()
            .iter()
            .find(|package| package.circle_id == circle_id)
            .cloned()
            .map(AudienceBlobBindingPackage::Circle),
    }
}

/// The maximal stable Circle snapshot: the one whose cut no other stable
/// snapshot strictly dominates.
pub(super) fn maximal_stable_circle_snapshot(
    stable: &[SelectedCircleSnapshot],
) -> Option<&SelectedCircleSnapshot> {
    let coverages: Vec<&CommitFrontier> = stable
        .iter()
        .map(|candidate| &candidate.meta.bootstrap.coverage)
        .collect();
    stable
        .iter()
        .filter(|candidate| {
            !coverages.iter().any(|other| {
                crate::sync::store::snapshots::coverage_dominates(
                    other,
                    &candidate.meta.bootstrap.coverage,
                )
            })
        })
        .max_by_key(|candidate| candidate.reference.snapshot_hash)
}

/// A stable Circle snapshot strictly supersedes a bootstrap seed when its cut
/// covers the seed and is not equal to it — the recipient has moved to a later
/// sufficient snapshot, not merely re-published coverage at the seed's own cut.
/// The strict inequality is load-bearing: a snapshot whose cut equals the seed
/// leaves the recipient exactly at its bootstrap and must not reclaim it.
pub(super) fn snapshot_supersedes_seed(cut: &CommitFrontier, seed: &CommitFrontier) -> bool {
    cut.covers(seed) && cut != seed
}

pub(super) type VerifiedReclaimSnapshot =
    crate::sync::store::commit_verification::merge_history::AcceptedStoreSnapshot;

impl<'operation, 'storage> AuthorizedReclaim<'operation, 'storage> {
    pub(super) async fn drive_candidate(
        &mut self,
        operation: DurableStoreReclaimOperation,
    ) -> Result<(), StoreReclaimError> {
        let database = self.database.clone();
        let candidate = match &operation {
            DurableStoreReclaimOperation::AuthorizationCandidate { object, candidate } => {
                let object = object.clone();
                let candidate = candidate.clone();
                Box::pin(create_reclaim_exact_objects(
                    object.as_ref(),
                    self.storage.as_ref(),
                ))
                .await
                .map_err(StoreReclaimError::from)?;
                for remote in object
                    .remote_objects(&candidate)
                    .map_err(StoreReclaimError::from)?
                {
                    if matches!(
                        remote.record(),
                        coven_protocol::remote_object::RemoteObjectRecord::RetainedAuthority(record)
                            if matches!(
                                record.identity.domain,
                                coven_protocol::remote_object::RetainedAuthorityObjectDomain::ReclaimEvidence { .. }
                                    | coven_protocol::remote_object::RetainedAuthorityObjectDomain::ReclaimAuthorization { .. }
                            )
                    ) {
                        database
                            .mark_reusable_retained_authority_uploaded(remote.into_record())
                            .await?;
                    }
                }
                candidate
            }
            DurableStoreReclaimOperation::CompletionCandidate { candidate, .. } => {
                candidate.clone()
            }
            _ => {
                return Err(StoreReclaimError::Authorization(
                    "Store reclaim journal has no publication candidate".to_string(),
                ));
            }
        };
        let _authorship = database.author_own_stream().await;
        Box::pin(self.writer.publish_prepared(candidate, None, None)).await?;
        Ok(())
    }

    pub(super) async fn prepare_completion(
        &mut self,
        operation: DurableStoreReclaimOperation,
    ) -> Result<(), StoreReclaimError> {
        let database = self.database.clone();
        let membership = self.membership.clone();
        let DurableStoreReclaimOperation::AbsentVerified {
            authorization,
            target,
            ..
        } = &operation
        else {
            return Err(StoreReclaimError::Authorization(
                "only an authorized reclaim can be executed".to_string(),
            ));
        };
        let opened = self
            .history()
            .load_reclaim_authorization(authorization)
            .await?;
        if &opened.authorization.value.target != target {
            return Err(StoreReclaimError::Authorization(
                "durable absent target differs from its signed authorization".to_string(),
            ));
        }

        let plan = self.writer.prepare_plan().await?;
        let resolved = membership.resolved();
        let provider_admin = resolved.provider_admin.combined_state().clone();
        let provider_admin_grant = plan
            .effective_provider_admin_grant(&provider_admin)
            .ok_or_else(|| {
                StoreReclaimError::Authorization(
                    "local Store device is not an effective provider administrator".to_string(),
                )
            })?;
        let candidate = self
            .writer
            .prepare_candidate(
                &plan,
                crate::sync::store::commit_publication::operation::commit_plan::StoreOperationBatch::ReclaimCompletion(
                    ReclaimCompletion {
                        authorization: authorization.clone(),
                        provider_admin_grant,
                    },
                ),
            )
            .await?;
        Box::pin(database.begin_store_reclaim_completion(operation, candidate)).await?;
        Ok(())
    }

    pub(super) async fn verify_target_absent(
        &self,
        target: &ReclaimTarget,
    ) -> Result<(), StoreReclaimError> {
        let database = &self.database;
        let storage = self.storage.clone();
        let root = &self.root;
        // A row blob is addressed by its locator, not by a protocol domain prefix, so
        // its absence is confirmed through the same exact-object verification the blob
        // primitives use.
        if let ReclaimTarget::AudienceBlob(blob) = target {
            return match storage.verify_blob_object(blob.blob()).await {
                Err(StorageError::NotFound(_)) => Ok(()),
                Ok(()) => Err(StoreReclaimError::Authorization(
                    "reclaim target remains readable after exact deletion".to_string(),
                )),
                Err(error) => Err(StoreReclaimError::Storage(error)),
            };
        }
        let (context, prefix) = match target {
            ReclaimTarget::StorePackage(target) => (
                ProtocolObjectContext::store_encrypted(
                    root.store_root_hash,
                    ProtocolObjectDomain::StorePackage,
                ),
                coven_protocol::store_commit::package_semantic_prefix(
                    target.package.candidate_family,
                    &target.activation.coord.stream_id.to_string(),
                    target.activation.coord.sequence(),
                    target.package.content_hash,
                ),
            ),
            ReclaimTarget::CirclePackage(target) => {
                // A Circle package is sealed under the Circle epoch key; resolving its
                // access only builds the read context — a deleted object reads back
                // absent before any decryption.
                let access = database
                    .circle_epoch_access(
                        root.clone(),
                        target.package.circle_id,
                        target.package.control.clone(),
                    )
                    .await?
                    .ok_or_else(|| {
                        StoreReclaimError::Authorization(
                            "reclaim target Circle package key is not resolvable".to_string(),
                        )
                    })?;
                (
                    access.protocol_context(
                        root.store_root_hash,
                        ProtocolObjectDomain::CirclePackage,
                    ),
                    coven_protocol::store_commit::circle_package_semantic_prefix(
                        target.package.circle_id,
                        target.package.package.candidate_family,
                        &target.activation.coord.stream_id.to_string(),
                        target.activation.coord.sequence(),
                        target.package.package.content_hash,
                    ),
                )
            }
            ReclaimTarget::CircleBootstrapImage(target) => {
                // A Circle bootstrap image is sealed under the Circle epoch key of the
                // control it activated under; resolving that access only builds the read
                // context — a deleted object reads back absent before any decryption. Its
                // readback prefix is the image object's own logical key without the domain
                // extension, so no recipient-sealed leaf field is needed to confirm absence.
                let access = database
                    .circle_epoch_access(
                        root.clone(),
                        target.coverage.circle_id,
                        target.coverage.control.clone(),
                    )
                    .await?
                    .ok_or_else(|| {
                        StoreReclaimError::Authorization(
                            "reclaim target Circle bootstrap image key is not resolvable"
                                .to_string(),
                        )
                    })?;
                (
                    access.protocol_context(
                        root.store_root_hash,
                        ProtocolObjectDomain::CircleBootstrapImage,
                    ),
                    coven_protocol::store_commit::semantic_prefix_from_exact_object(
                        &target.coverage.bootstrap.image.object,
                        coven_protocol::objects::ProtectedObjectDomain::CircleBootstrapImage
                            .extension(),
                    )
                    .map_err(StoreReclaimError::from)?,
                )
            }
            ReclaimTarget::CircleSnapshotImage(target) => {
                // A standalone Circle snapshot image is sealed under the epoch key of the
                // control its generation was authored under; resolving that access only
                // builds the read context — a deleted object reads back absent before any
                // decryption.
                let access = database
                    .circle_epoch_access(root.clone(), target.circle_id, target.control.clone())
                    .await?
                    .ok_or_else(|| {
                        StoreReclaimError::Authorization(
                            "reclaim target Circle snapshot image key is not resolvable"
                                .to_string(),
                        )
                    })?;
                (
                    access.protocol_context(
                        root.store_root_hash,
                        ProtocolObjectDomain::CircleSnapshotImage,
                    ),
                    coven_protocol::store_commit::semantic_prefix_from_exact_object(
                        &target.image.object,
                        coven_protocol::objects::ProtectedObjectDomain::CircleSnapshotImage
                            .extension(),
                    )
                    .map_err(StoreReclaimError::from)?,
                )
            }
            ReclaimTarget::AudienceBlob(_) => {
                return Err(StoreReclaimError::Authorization(
                    "audience blob reclaim target has no protocol object prefix".to_string(),
                ));
            }
        };
        match storage
            .read_protocol_object(&context, target.object(), &prefix)
            .await
        {
            Err(StorageError::NotFound(_)) => Ok(()),
            Ok(_) => Err(StoreReclaimError::Authorization(
                "reclaim target remains readable after exact deletion".to_string(),
            )),
            Err(error) => Err(StoreReclaimError::Storage(error)),
        }
    }

    pub(super) async fn choose_snapshot(
        &mut self,
    ) -> Result<VerifiedReclaimSnapshot, StoreReclaimError> {
        let storage = self.storage.clone();
        let root = self.root.clone();
        let selected = self
            .history()
            .current_accepted_snapshot()
            .await?
            .ok_or(StoreReclaimError::NoSnapshot)?;
        let snapshot = selected.snapshot();
        let image = storage
            .read_protocol_object(
                &ProtocolObjectContext::store_encrypted(
                    root.store_root_hash,
                    ProtocolObjectDomain::StoreSnapshotImage,
                ),
                &snapshot.meta.image.object,
                &snapshot_image_semantic_prefix(
                    snapshot.reference.object.slot(),
                    snapshot.meta.image.image_hash,
                ),
            )
            .await
            .map_err(StoreObjectError::from)?;
        if ObjectHash::digest(&image) != snapshot.meta.image.image_hash {
            return Err(StoreReclaimError::Authorization(
                "snapshot image differs from its signed exact reference".to_string(),
            ));
        }
        Ok(selected)
    }
}

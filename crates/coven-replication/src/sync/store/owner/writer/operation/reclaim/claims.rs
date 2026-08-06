use super::*;

impl<'operation, 'storage> AuthorizedReclaim<'operation, 'storage> {
    pub(super) async fn target_is_retained(
        &self,
        target: &ReclaimTarget,
    ) -> Result<bool, StoreReclaimError> {
        let database = &self.database;
        let root = &self.root;
        match target {
            ReclaimTarget::StorePackage(target) => Ok(database
                .store_package_is_retained_for_replay(
                    root.clone(),
                    target.package.clone(),
                    target.activation.clone(),
                )
                .await?),
            ReclaimTarget::CirclePackage(target) => Ok(database
                .circle_package_is_retained_for_replay(
                    root.clone(),
                    target.package.clone(),
                    target.activation.clone(),
                )
                .await?),
            ReclaimTarget::CircleBootstrapImage(target) => Ok(database
                .circle_bootstrap_image_is_retained_for_replay(target.coverage.clone())
                .await?),
            ReclaimTarget::CircleSnapshotImage(target) => Ok(database
                .circle_image_is_retained_for_replay(target.circle_id, target.image.clone())
                .await?),
            ReclaimTarget::AudienceBlob(target) => Ok(database
                .audience_blob_is_retained_for_replay(target.blob.clone())
                .await?),
        }
    }

    pub(super) async fn verify_authorized(
        &mut self,
        authorization_ref: &ReclaimAuthorizationRef,
        activation: &ReclaimCommitActivation,
    ) -> Result<ReclaimTarget, StoreReclaimError> {
        let opened = self
            .history()
            .load_reclaim_authorization(authorization_ref)
            .await?;
        self.verify_authorization_activation(authorization_ref, activation)
            .await?;
        self.verify_evidence(&opened.evidence.value).await
    }

    pub(super) async fn verify_authorization_activation(
        &mut self,
        authorization: &ReclaimAuthorizationRef,
        activation: &ReclaimCommitActivation,
    ) -> Result<(), StoreReclaimError> {
        activation
            .validate()
            .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?;
        let commit_ref = activation.commit();
        let verified_commit = self
            .history()
            .load_ref(commit_ref)
            .await
            .map_err(StoreReclaimError::from)?;
        let commit_value = verified_commit.value();
        let author = verified_commit.author();
        if commit_value.reclaim_authorization() != Some(authorization) {
            return Err(StoreReclaimError::Authorization(
                "reclaim activation commit names another authorization".to_string(),
            ));
        }
        let commit = &activation.commit;
        let head = &activation.head;
        let opened = self.history().load_head(head, author, commit).await?;
        if opened.value.commit != *commit {
            return Err(StoreReclaimError::Authorization(
                "reclaim head activates another commit".to_string(),
            ));
        }
        let (_, accepted_head) = self
            .history()
            .exact_next_announcement_slot(
                &commit_value.author_registration,
                author,
                Some(&verified_commit),
            )
            .await?;
        if accepted_head.as_ref() != Some(head) {
            return Err(StoreReclaimError::Authorization(
                "reclaim activation head is not the exact accepted stream position".to_string(),
            ));
        }
        self.history()
            .verify_currently_materialized(commit)
            .await
            .map_err(StoreReclaimError::from)
    }

    pub(super) async fn verify_evidence(
        &mut self,
        evidence: &ReclaimEvidence,
    ) -> Result<ReclaimTarget, StoreReclaimError> {
        let root = self.root.clone();
        evidence
            .verify()
            .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?;
        if evidence.store_root_hash != root.store_root_hash {
            return Err(StoreReclaimError::Authorization(
                "reclaim evidence belongs to another Store root".to_string(),
            ));
        }
        match &evidence.claim {
            ReclaimClaim::StorePackage(claim) => {
                let activation = self
                    .history()
                    .load_ref(&claim.target.activation)
                    .await
                    .map_err(StoreReclaimError::from)?;
                Ok(ReclaimTarget::StorePackage(
                    self.verify_store_package_reclaim_claim(&activation, claim)
                        .await?,
                ))
            }
            ReclaimClaim::CirclePackage(claim) => {
                let activation = self
                    .history()
                    .load_ref(&claim.target().activation)
                    .await
                    .map_err(StoreReclaimError::from)?;
                Ok(ReclaimTarget::CirclePackage(
                    self.verify_circle_package_reclaim_claim(&activation, claim)
                        .await?,
                ))
            }
            ReclaimClaim::CircleBootstrapImage(claim) => Ok(ReclaimTarget::CircleBootstrapImage(
                self.verify_circle_bootstrap_image_reclaim_claim(claim)
                    .await?,
            )),
            ReclaimClaim::CircleSnapshotImage(claim) => Ok(ReclaimTarget::CircleSnapshotImage(
                self.verify_circle_snapshot_image_reclaim_claim(claim)
                    .await?,
            )),
            ReclaimClaim::AudienceBlob(claim) => {
                let activation = self
                    .history()
                    .load_ref(&claim.target.activation)
                    .await
                    .map_err(StoreReclaimError::from)?;
                Ok(ReclaimTarget::AudienceBlob(
                    self.verify_audience_blob_reclaim_claim(&activation, claim)
                        .await?,
                ))
            }
        }
    }

    /// Re-verify that a row blob is free. The package the claim names is re-read
    /// from storage and must itself bind this exact blob — the signed statement
    /// that published it — and the orphan test is re-run against this device's
    /// own materialized rows rather than taken from the claim.
    pub(super) async fn verify_audience_blob_reclaim_claim(
        &self,
        activation: &VerifiedStoreBatchCommit,
        claim: &AudienceBlobReclaimClaim,
    ) -> Result<AudienceBlobReclaimTarget, StoreReclaimError> {
        if audience_blob_binding_package(activation.value(), claim.target.blob.locator().audience())
            .as_ref()
            != Some(&claim.target.package)
        {
            return Err(StoreReclaimError::Authorization(
                "audience blob reclaim activation names another package".to_string(),
            ));
        }
        let package = self
            .read_audience_blob_binding_package(&claim.target.package, &claim.target.activation)
            .await?;
        if !package
            .blob_bindings()
            .iter()
            .any(|binding| binding.blob() == &claim.target.blob)
        {
            return Err(StoreReclaimError::Authorization(
                "audience blob reclaim package does not bind the target blob".to_string(),
            ));
        }
        if !self
            .database
            .stored_blob_is_row_orphaned(claim.target.blob.clone())
            .await?
        {
            return Err(StoreReclaimError::Authorization(
                "a live row still binds the audience blob as a remote reference".to_string(),
            ));
        }
        Ok(claim.target.clone())
    }

    /// Read back the exact package body that published a blob. A Store package
    /// is sealed to the Store and a Circle package to its epoch, so the audience
    /// selects both the read context and the semantic prefix.
    pub(super) async fn read_audience_blob_binding_package(
        &self,
        package: &AudienceBlobBindingPackage,
        activation: &StoreBatchCommitRef,
    ) -> Result<coven_protocol::audience_package::AudiencePackage, StoreReclaimError> {
        let (context, prefix, object) = match package {
            AudienceBlobBindingPackage::Store(package) => (
                ProtocolObjectContext::store_encrypted(
                    self.root.store_root_hash,
                    ProtocolObjectDomain::StorePackage,
                ),
                coven_protocol::store_commit::package_semantic_prefix(
                    package.candidate_family,
                    &activation.coord.stream_id.to_string(),
                    activation.coord.sequence(),
                    package.content_hash,
                ),
                &package.object,
            ),
            AudienceBlobBindingPackage::Circle(package) => {
                let access = self
                    .database
                    .circle_epoch_access(
                        self.root.clone(),
                        package.circle_id,
                        package.control.clone(),
                    )
                    .await?
                    .ok_or_else(|| {
                        StoreReclaimError::Authorization(
                            "audience blob reclaim package key is not resolvable".to_string(),
                        )
                    })?;
                (
                    access.protocol_context(
                        self.root.store_root_hash,
                        ProtocolObjectDomain::CirclePackage,
                    ),
                    coven_protocol::store_commit::circle_package_semantic_prefix(
                        package.circle_id,
                        package.package.candidate_family,
                        &activation.coord.stream_id.to_string(),
                        activation.coord.sequence(),
                        package.package.content_hash,
                    ),
                    &package.package.object,
                )
            }
        };
        let bytes = self
            .storage
            .read_protocol_object(&context, object, &prefix)
            .await?;
        coven_protocol::audience_package::AudiencePackage::parse(&bytes)
            .map_err(|error| StoreReclaimError::Authorization(error.to_string()))
    }

    pub(super) async fn verify_circle_package_reclaim_claim(
        &mut self,
        activation: &VerifiedStoreBatchCommit,
        claim: &CirclePackageReclaimClaim,
    ) -> Result<CirclePackageReclaimTarget, StoreReclaimError> {
        match claim {
            CirclePackageReclaimClaim::SnapshotCovered(claim) => {
                // Re-verify a Circle package reclaim: a stable Circle snapshot on the
                // same Circle covers the package's activating commit, and every device
                // holding active Circle access has acknowledged coverage dominating the
                // snapshot cut. Each acknowledgement reference names the exact control
                // that resolves the epoch key it was sealed under.
                let database = self.database.clone();
                let root = self.root.clone();
                let mut history = self.history();
                let circle_id = claim.target.package.circle_id;
                let snapshot_control = &claim.covering_snapshot.control;
                // Read snapshot metadata under the current control's retained keyring so a
                // snapshot sealed before a rotation still resolves its epoch key.
                let current_control = database
                    .current_circle_control(circle_id)
                    .await?
                    .ok_or_else(|| {
                        StoreReclaimError::Authorization(format!(
                            "Circle {circle_id} has no active control for reclaim stability"
                        ))
                    })?;
                let access = database
                    .circle_epoch_access(root, circle_id, current_control)
                    .await?
                    .ok_or_else(|| {
                        StoreReclaimError::Authorization(format!(
                            "Circle {circle_id} snapshot key is not resolvable from retained controls"
                        ))
                    })?;
                let author = history
                    .load_registration(&claim.covering_snapshot.author_registration)
                    .await?;
                let stream = history
                    .load_circle_snapshot_stream_refs(
                        circle_id,
                        &access,
                        &claim.covering_snapshot.author_registration,
                        &author.value,
                    )
                    .await
                    .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?;
                let (_, snapshot) = stream
                    .into_iter()
                    .find(|(reference, _)| *reference == claim.covering_snapshot.snapshot)
                    .ok_or(StoreReclaimError::NoSnapshot)?;
                if snapshot.circle_id != circle_id
                    || snapshot.control != *snapshot_control
                    || snapshot.author_registration != claim.covering_snapshot.author_registration
                {
                    return Err(StoreReclaimError::Authorization(
                        "Circle reclaim snapshot differs from its exact locator".to_string(),
                    ));
                }
                let cut = &snapshot.bootstrap.coverage;
                let expected = history
                    .stable_circle_acknowledgements(circle_id, cut)
                    .await
                    .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?
                    .ok_or_else(|| {
                        StoreReclaimError::Authorization(
                            "Circle snapshot is not acknowledgement-stable across every active-access device"
                                .to_string(),
                        )
                    })?;
                if claim.acknowledgements != expected {
                    return Err(StoreReclaimError::Authorization(
                        "Circle reclaim acknowledgements differ from the active-access stability proof"
                            .to_string(),
                    ));
                }
                if !activation
                    .value()
                    .circle_packages()
                    .contains(&claim.target.package)
                    || !history
                        .snapshot_covers_target(cut, &claim.target.activation)
                        .await?
                {
                    return Err(StoreReclaimError::Authorization(
                        "Circle reclaim target is not the exact package covered by its snapshot"
                            .to_string(),
                    ));
                }
                Ok(claim.target.clone())
            }
            CirclePackageReclaimClaim::BeyondEpochCutoff(claim) => {
                self.verify_circle_package_beyond_cutoff_claim(activation, claim)
                    .await
            }
        }
    }

    /// Re-verify that a Circle package lies beyond its epoch's accepted close
    /// cutoff. The named successor control must be a retained activation whose
    /// closed-epoch origin names the epoch the package's own control belongs to,
    /// and the same replay-epoch predicate the pull path applies must refuse the
    /// package. A package the cutoff accepts, or one whose control the cutoff
    /// conflicts with, is not eligible under this arm and fails loud rather than
    /// falling back to coverage.
    pub(super) async fn verify_circle_package_beyond_cutoff_claim(
        &self,
        activation: &VerifiedStoreBatchCommit,
        claim: &CirclePackageBeyondCutoffClaim,
    ) -> Result<CirclePackageReclaimTarget, StoreReclaimError> {
        if !activation
            .value()
            .circle_packages()
            .contains(&claim.target.package)
        {
            return Err(StoreReclaimError::Authorization(
                "Circle package reclaim activation names another package".to_string(),
            ));
        }
        let circle_id = claim.target.package.circle_id;
        let successor = self
            .database
            .verified_circle_activation(
                self.root.clone(),
                circle_id,
                claim.successor_control.clone(),
            )
            .await?
            .ok_or_else(|| {
                StoreReclaimError::Authorization(format!(
                    "Circle {circle_id} beyond-cutoff successor control is not a retained activation"
                ))
            })?;
        let CircleControlState::ActiveEpoch(active) = successor.control.value.state() else {
            return Err(StoreReclaimError::Authorization(
                "Circle beyond-cutoff successor control is not an activated epoch".to_string(),
            ));
        };
        let CircleEpochOrigin::Closed {
            closed_epoch_id, ..
        } = &active.common.origin
        else {
            return Err(StoreReclaimError::Authorization(
                "Circle beyond-cutoff successor epoch did not close a predecessor".to_string(),
            ));
        };
        // The package must be addressed to the epoch that close cut off, not to
        // another epoch that merely happens to precede the successor.
        let package_control = self
            .database
            .verified_circle_activation(
                self.root.clone(),
                circle_id,
                claim.target.package.control.clone(),
            )
            .await?
            .ok_or_else(|| {
                StoreReclaimError::Authorization(format!(
                    "Circle {circle_id} package control is not a retained activation"
                ))
            })?;
        if package_control.control.value.epoch_id() != *closed_epoch_id {
            return Err(StoreReclaimError::Authorization(
                "Circle beyond-cutoff package belongs to another epoch than the one closed"
                    .to_string(),
            ));
        }
        // Apply the exact predicate pull uses to skip a package beyond its
        // accepted cutoff. A package it permits remains live history.
        if self
            .database
            .circle_replay_epoch_index(self.root.clone())
            .await?
            .permits(
                &claim.target.activation,
                circle_id,
                &claim.target.package.control,
            )
            .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?
        {
            return Err(StoreReclaimError::Authorization(
                "Circle package lies within its accepted epoch cutoff and is not reclaimable as beyond-cutoff"
                    .to_string(),
            ));
        }
        Ok(claim.target.clone())
    }

    /// Re-verify that a later generation of the reclaimed image's own stream
    /// supersedes it. The stream is re-walked from generation zero, so both the
    /// reclaimed generation and the named superseding one are re-read from their own
    /// signed metadata; the superseding generation must carry a cut that strictly
    /// dominates the reclaimed one, and every device holding active Circle access must
    /// have acknowledged that cut. Nothing the claim asserts about coverage,
    /// stability, or the image itself is taken on trust.
    pub(super) async fn verify_circle_snapshot_image_reclaim_claim(
        &mut self,
        claim: &CircleSnapshotImageReclaimClaim,
    ) -> Result<CircleSnapshotImageReclaimTarget, StoreReclaimError> {
        let database = self.database.clone();
        let mut history = self.history();
        let circle_id = claim.target.circle_id;
        let current_control = database
            .current_circle_control(circle_id)
            .await?
            .ok_or_else(|| {
                StoreReclaimError::Authorization(format!(
                    "Circle {circle_id} has no active control for snapshot image reclaim"
                ))
            })?;
        let author = database
            .activated_store_device_registration(claim.target.snapshot_author.clone())
            .await?;
        let author_stream = [author];
        let streams = history
            .load_circle_snapshot_streams(circle_id, &current_control, &author_stream)
            .await?;
        let [stream] = streams.as_slice() else {
            return Err(StoreReclaimError::Authorization(
                "Circle snapshot reclaim author's stream is not readable".to_string(),
            ));
        };
        let generation = stream
            .generations
            .iter()
            .find(|(reference, _)| *reference == claim.target.snapshot)
            .ok_or_else(|| {
                StoreReclaimError::Authorization(
                    "Circle snapshot reclaim target is absent from its author's stream".to_string(),
                )
            })?;
        if generation.1.circle_id != circle_id
            || generation.1.control != claim.target.control
            || generation.1.bootstrap.image != claim.target.image
        {
            return Err(StoreReclaimError::Authorization(
                "Circle snapshot reclaim target differs from its own signed generation".to_string(),
            ));
        }
        let superseding = stream
            .generations
            .iter()
            .find(|(reference, _)| *reference == claim.superseding)
            .ok_or_else(|| {
                StoreReclaimError::Authorization(
                    "Circle snapshot reclaim superseding generation is absent from the same stream"
                        .to_string(),
                )
            })?;
        if !snapshot_supersedes_seed(
            &superseding.1.bootstrap.coverage,
            &generation.1.bootstrap.coverage,
        ) {
            return Err(StoreReclaimError::Authorization(
            "Circle snapshot reclaim superseding generation does not strictly dominate the reclaimed cut"
                .to_string(),
        ));
        }
        if history
            .stable_circle_acknowledgements(circle_id, &superseding.1.bootstrap.coverage)
            .await
            .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?
            .is_none()
        {
            return Err(StoreReclaimError::Authorization(
                "Circle snapshot reclaim superseding generation is not acknowledgement-stable"
                    .to_string(),
            ));
        }
        Ok(claim.target.clone())
    }

    pub(super) async fn verify_store_package_reclaim_claim(
        &mut self,
        activation: &VerifiedStoreBatchCommit,
        claim: &StorePackageReclaimClaim,
    ) -> Result<StorePackageReclaimTarget, StoreReclaimError> {
        let mut history = self.history();
        let author = history
            .load_registration(&claim.covering_snapshot.author_registration)
            .await?;
        let (reference, metadata) = history
            .load_store_snapshot(
                &claim.covering_snapshot.author_registration,
                &author.value,
                &claim.covering_snapshot.snapshot,
            )
            .await
            .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?;
        let snapshot = coven_database::PublishedStoreSnapshot {
            reference,
            successor_slot: metadata.successor.next_slot.clone(),
            meta: metadata,
        };
        let authority = match history.verify_snapshot_stability(&snapshot).await {
            Ok(stability) => stability.into_authority(),
            Err(crate::sync::store::owner::writer::operation::pull::StorePullError::SnapshotNotStable { member, device_id }) => {
                return Err(StoreReclaimError::MissingAcknowledgement { member, device_id });
            }
            Err(
                crate::sync::store::owner::writer::operation::pull::StorePullError::SnapshotAuthorInactive
                | crate::sync::store::owner::writer::operation::pull::StorePullError::SnapshotAuthorNotOwner,
            ) => return Err(StoreReclaimError::NoSnapshot),
            Err(error) => return Err(StoreReclaimError::Authorization(error.to_string())),
        };
        let mut expected_acknowledgements = authority
            .acknowledgements
            .values()
            .map(|acknowledgement| {
                acknowledgement
                    .latest()
                    .map(|(reference, _)| reference.clone())
                    .ok_or_else(|| {
                        StoreReclaimError::Authorization(
                            "snapshot stability acknowledgement proof chain is empty".to_string(),
                        )
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        expected_acknowledgements.sort();
        if claim.acknowledgements != expected_acknowledgements {
            return Err(StoreReclaimError::Authorization(
            "reclaim evidence acknowledgements differ from the activated snapshot stability proof"
                .to_string(),
        ));
        }
        if activation.value().store_package() != Some(&claim.target.package)
            || !history
                .snapshot_covers_target(&snapshot.meta.coverage, &claim.target.activation)
                .await?
        {
            return Err(StoreReclaimError::Authorization(
                "reclaim target is not the exact Store package covered by its snapshot".to_string(),
            ));
        }
        Ok(claim.target.clone())
    }

    /// Re-verify a Circle bootstrap image reclaim: the recipient's own activated
    /// acknowledgement names the exact coverage being deleted (`seeded_from`), and
    /// either the recipient advanced strictly past that seed while still holding
    /// active access, or it lost authority under an activated successor control. The
    /// acknowledgement reference names the exact control that resolves the epoch key
    /// it was sealed under.
    pub(super) async fn verify_circle_bootstrap_image_reclaim_claim(
        &mut self,
        claim: &CircleBootstrapImageReclaimClaim,
    ) -> Result<CircleBootstrapImageReclaimTarget, StoreReclaimError> {
        let database = self.database.clone();
        let root = self.root.clone();
        let mut history = self.history();
        let activation = history
            .load_ref(&claim.target.coverage.activation_commit)
            .await
            .map_err(StoreReclaimError::from)?;
        if !activation
            .value()
            .circle_controls()
            .iter()
            .flat_map(|control| control.objects.access.iter())
            .any(|access| access.bootstrap.as_ref() == Some(&claim.target.coverage.bootstrap.image))
        {
            return Err(StoreReclaimError::Authorization(
                "Circle bootstrap reclaim activation names another image".to_string(),
            ));
        }
        let circle_id = claim.target.coverage.circle_id;
        let current_control = database
            .current_circle_control(circle_id)
            .await?
            .ok_or_else(|| {
                StoreReclaimError::Authorization(format!(
                    "Circle {circle_id} has no active control for bootstrap reclaim"
                ))
            })?;
        let acknowledgement_ref = claim.proof.acknowledgement();
        let acknowledgement = history
            .load_circle_acknowledgement(acknowledgement_ref)
            .await
            .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?;
        // The recipient's signed acknowledgement is the sole authority for the coverage
        // the Owner deletes: the target must be exactly what the recipient said it was
        // seeded from, so the Owner never fabricates the image, cut, or activation.
        if acknowledgement.seeded_from.as_ref() != Some(&claim.target.coverage) {
            return Err(StoreReclaimError::Authorization(
                "Circle bootstrap reclaim target differs from the recipient's signed seed coverage"
                    .to_string(),
            ));
        }
        let recipient = database
            .activated_store_device_registration(acknowledgement_ref.registration.clone())
            .await?;
        let roster = database.circle_current_roster_members(circle_id).await?;
        match &claim.proof {
            CircleBootstrapReclaimProof::RecipientCoverage { .. } => {
                if !roster.contains(&recipient.value().author_pubkey) {
                    return Err(StoreReclaimError::Authorization(
                    "Circle bootstrap recipient-coverage proof names a device outside the current roster"
                        .to_string(),
                ));
                }
                // Re-derive the maximal acknowledgement-stable Circle snapshot and require
                // its cut to strictly dominate the seed: the later sufficient snapshot the
                // recipient (with every active device) acknowledged past its bootstrap.
                let registrations = database
                    .activated_store_device_registration_records()
                    .await
                    .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?;
                let seed = &claim.target.coverage.bootstrap.coverage;
                let streams = history
                    .load_circle_snapshot_streams(circle_id, &current_control, &registrations)
                    .await?;
                let stable = history.stable_circle_snapshots(circle_id, &streams).await?;
                let superseded = maximal_stable_circle_snapshot(&stable).is_some_and(|selected| {
                    snapshot_supersedes_seed(&selected.meta.bootstrap.coverage, seed)
                });
                if !superseded {
                    return Err(StoreReclaimError::Authorization(
                    "no acknowledgement-stable Circle snapshot strictly dominates the recipient's seed coverage"
                        .to_string(),
                ));
                }
            }
            CircleBootstrapReclaimProof::LostAuthority {
                successor_control, ..
            } => {
                if roster.contains(&recipient.value().author_pubkey) {
                    return Err(StoreReclaimError::Authorization(
                    "Circle bootstrap lost-authority proof names a device still in the current roster"
                        .to_string(),
                ));
                }
                if *successor_control != current_control {
                    return Err(StoreReclaimError::Authorization(
                    "Circle bootstrap lost-authority successor is not the current activated control"
                        .to_string(),
                ));
                }
                if !database
                    .circle_control_covers_strictly(
                        root.clone(),
                        circle_id,
                        successor_control,
                        &claim.target.coverage.control,
                    )
                    .await?
                {
                    return Err(StoreReclaimError::Authorization(
                    "Circle bootstrap lost-authority successor does not strictly cover the seed control"
                        .to_string(),
                ));
                }
            }
        }
        Ok(claim.target.clone())
    }
}

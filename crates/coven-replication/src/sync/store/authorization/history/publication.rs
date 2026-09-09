use super::*;
use crate::sync::store::StoreError;
use coven_protocol::objects::{ProtocolObjectContext, ProtocolObjectDomain, StoreObjectError};

pub(crate) enum StoreCommitPublicationAttemptOutcome {
    Published(coven_database::StoreCommitPublicationOutcome),
    SnapshotCovered(coven_database::store::CoveredStoreWrite),
    AwaitingPreparation(coven_protocol::write::WriteId),
    SnapshotRetired(coven_protocol::store_commit::AcceptedStoreSnapshotRef),
}

pub(crate) enum StoreSnapshotPublicationAttemptOutcome {
    Accepted(coven_database::AcceptedStorePublicationInterval),
    Competing(coven_database::AcceptedStorePublicationInterval),
    Superseded {
        snapshot: coven_database::PublishedStoreSnapshot,
        accepted: coven_database::AcceptedStorePublicationInterval,
    },
}

impl StoreCommitPublicationAttemptOutcome {
    pub(crate) fn require_published(
        self,
    ) -> Result<coven_database::StoreCommitPublicationOutcome, StoreError> {
        match self {
            Self::Published(outcome) => Ok(outcome),
            Self::SnapshotCovered(_) => Err(StoreError::InvalidOutbound(
                "control publication cannot use a covered row-write receipt".into(),
            )),
            Self::AwaitingPreparation(write_id) => Err(StoreError::InvalidOutbound(format!(
                "control publication reached row-write reservation {write_id} awaiting preparation",
            ))),
            Self::SnapshotRetired(_) => Err(StoreError::InvalidOutbound(
                "Store operation must replace its candidate after snapshot retirement".into(),
            )),
        }
    }
}

impl AuthorizedStoreHistory<'_> {
    pub(crate) async fn publish_store_commit(
        &mut self,
        membership: &mut MembershipChain,
        identity: &UserKeypair,
        signer: &UserKeypair,
        commit: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
    ) -> Result<StoreCommitPublicationAttemptOutcome, StoreError> {
        loop {
            let active = self.database.active_store_publication().await?;
            if let Some(active) = &active {
                if active.commit_reservation()
                    != Some((
                        &commit.write_id,
                        &commit.author_registration,
                        &commit.reference().coord,
                    ))
                {
                    return Err(StoreError::InvalidOutbound(
                        "Store commit differs from its publication reservation".into(),
                    ));
                }
                if active.is_awaiting_preparation() {
                    return Ok(StoreCommitPublicationAttemptOutcome::AwaitingPreparation(
                        commit.write_id.clone(),
                    ));
                }
                active.attempt()?.verify_commit(commit)?;
                if active.superseded_entry().is_some() {
                    retire_superseded_publication_entry(
                        &self.database,
                        self.storage.as_ref(),
                        active.clone(),
                    )
                    .await?;
                    continue;
                }
            }
            if let Some(covered) = self.database.covered_store_write(commit.clone()).await? {
                return Ok(StoreCommitPublicationAttemptOutcome::SnapshotCovered(
                    covered,
                ));
            }
            if let Some(installed) = self
                .database
                .installed_store_commit_evidence(commit.clone())
                .await?
            {
                return Ok(StoreCommitPublicationAttemptOutcome::Published(
                    coven_database::StoreCommitPublicationOutcome::Installed(installed),
                ));
            }
            let active = active.ok_or_else(|| {
                StoreError::InvalidOutbound("Store commit has no active publication attempt".into())
            })?;
            let publication = active.attempt()?;
            let (current, version) = self
                .history_verifier
                .read_current_store_publication()
                .await?;
            if current == publication.replacement {
                // A provider may accept the exact CAS and lose its response.
                // Return its observed acceptance before materializing the
                // transition: authority results are finalized by that owner.
                let (interval, accepted_predecessor) = self
                    .verify_prepared_publication(publication, commit)
                    .await?;
                return Ok(StoreCommitPublicationAttemptOutcome::Published(
                    coven_database::StoreCommitPublicationOutcome::Accepted {
                        interval: coven_database::AcceptedStorePublicationInterval::from_verified(
                            interval,
                            Some(version),
                        ),
                        accepted_predecessor,
                    },
                ));
            }
            if current != publication.previous {
                let pulled = self
                    .install_current_publication(membership, identity)
                    .await?;
                if let Some(covered) = self.database.covered_store_write(commit.clone()).await? {
                    return Ok(StoreCommitPublicationAttemptOutcome::SnapshotCovered(
                        covered,
                    ));
                }
                if let Some(installed) = self
                    .database
                    .installed_store_commit_evidence(commit.clone())
                    .await?
                {
                    return Ok(StoreCommitPublicationAttemptOutcome::Published(
                        coven_database::StoreCommitPublicationOutcome::Installed(installed),
                    ));
                }
                if !pulled.held_positions.is_empty() {
                    return Err(StoreError::PublicationHeld(pulled.held_positions));
                }
                let current_active = self.database.active_store_publication().await?;
                if current_active.as_ref().is_some_and(|current| {
                    current.is_awaiting_preparation()
                        && current.commit_reservation() == active.commit_reservation()
                }) {
                    return Ok(StoreCommitPublicationAttemptOutcome::AwaitingPreparation(
                        commit.write_id.clone(),
                    ));
                }
                let installed = self.database.store_current_publication().await?;
                if installed.record().publication_base() != commit.publication_base {
                    let coven_protocol::store_commit::StorePublicationBase::Snapshot(snapshot) =
                        installed.record().publication_base()
                    else {
                        return Err(StoreError::InvalidOutbound(
                            "Store publication lost its accepted snapshot boundary".into(),
                        ));
                    };
                    return Ok(StoreCommitPublicationAttemptOutcome::SnapshotRetired(
                        snapshot,
                    ));
                }
                let replacement = self
                    .prepare_store_commit_publication(installed.require_observed()?, commit, signer)
                    .await?;
                self.verify_prepared_publication(&replacement, commit)
                    .await?;
                let replacement = active.replace_attempt(replacement)?;
                self.database
                    .replace_active_store_commit_publication(commit.clone(), active, replacement)
                    .await?;
                continue;
            }
            if version != publication.previous_version {
                return Err(StoreError::InvalidOutbound(
                    "Store publication revision changed without an accepted transition".into(),
                ));
            }
            let (interval, accepted_predecessor) = self
                .verify_prepared_publication(publication, commit)
                .await?;
            upload_store_publication_entry(
                self.storage.as_ref(),
                self.root().store_root_hash,
                publication,
            )
            .await?;
            let context = ProtocolObjectContext::signed_plaintext(
                self.root().store_root_hash,
                ProtocolObjectDomain::StoreCurrentPublication,
            );
            let result = self
                .storage
                .replace_protocol_record_if_version(
                    &context,
                    &self
                        .history_verifier
                        .verified_root()
                        .protocol()
                        .descriptor
                        .current_publication_slot,
                    coven_protocol::store_commit::store_current_publication_semantic_prefix(),
                    &publication.previous_version,
                    publication.replacement.to_bytes(),
                )
                .await;
            match result {
                Ok(coven_storage::cloud::ConditionalWriteOutcome::Replaced(version)) => {
                    return Ok(StoreCommitPublicationAttemptOutcome::Published(
                        coven_database::StoreCommitPublicationOutcome::Accepted {
                            interval:
                                coven_database::AcceptedStorePublicationInterval::from_verified(
                                    interval,
                                    Some(version),
                                ),
                            accepted_predecessor,
                        },
                    ));
                }
                Ok(coven_storage::cloud::ConditionalWriteOutcome::VersionChanged) => {}
                Err(source) => {
                    let observed = self.history_verifier.read_current_store_publication().await;
                    match observed {
                        Ok((current, version)) if current == publication.replacement => {
                            return Ok(StoreCommitPublicationAttemptOutcome::Published(
                                coven_database::StoreCommitPublicationOutcome::Accepted {
                            interval: coven_database::AcceptedStorePublicationInterval::from_verified(interval, Some(version)),
                            accepted_predecessor,
                        },
                            ));
                        }
                        Ok(_) => {}
                        Err(verification) => {
                            return Err(StoreError::PublicationSettlement {
                                publication: source,
                                verification: Box::new(verification.into()),
                            });
                        }
                    }
                    let pulled = match self.install_current_publication(membership, identity).await
                    {
                        Ok(pulled) => pulled,
                        Err(verification) => {
                            return Err(StoreError::PublicationSettlement {
                                publication: source,
                                verification: Box::new(verification),
                            });
                        }
                    };
                    if let Some(covered) = self.database.covered_store_write(commit.clone()).await?
                    {
                        return Ok(StoreCommitPublicationAttemptOutcome::SnapshotCovered(
                            covered,
                        ));
                    }
                    if let Some(installed) = self
                        .database
                        .installed_store_commit_evidence(commit.clone())
                        .await?
                    {
                        return Ok(StoreCommitPublicationAttemptOutcome::Published(
                            coven_database::StoreCommitPublicationOutcome::Installed(installed),
                        ));
                    }
                    if !pulled.held_positions.is_empty() {
                        return Err(StoreError::PublicationSettlement {
                            publication: source,
                            verification: Box::new(StoreError::PublicationHeld(
                                pulled.held_positions,
                            )),
                        });
                    }
                    let installed = self.database.store_current_publication().await?;
                    if installed.record() == &publication.previous {
                        return Err(StoreObjectError::from(source).into());
                    }
                    tracing::debug!(error = %source, "settled uncertain Store publication against an accepted competing transition");
                }
            }
        }
    }

    pub(crate) async fn publish_store_snapshot(
        &mut self,
        membership: &mut MembershipChain,
        identity: &UserKeypair,
        pending: &coven_database::DurableSnapshotPublication,
        objects: &crate::sync::store::snapshots::AuthorizedSnapshotPublication<'_>,
    ) -> Result<StoreSnapshotPublicationAttemptOutcome, crate::sync::store::snapshots::SnapshotError>
    {
        let publication = &pending.publication;
        let active = self
            .database
            .active_store_publication()
            .await?
            .ok_or_else(|| {
                StoreError::InvalidOutbound(
                    "Store snapshot has no active publication attempt".into(),
                )
            })?;
        if active.owner() != &coven_database::ActiveStorePublicationOwner::Snapshot
            || active.attempt()? != publication
        {
            return Err(StoreError::InvalidOutbound(
                "Store snapshot differs from its active publication attempt".into(),
            )
            .into());
        }
        publication
            .validate_snapshot_shape(&pending.meta.value, &pending.reference)
            .map_err(StoreError::from)?;
        let (current, version) = self
            .history_verifier
            .read_current_store_publication()
            .await?;
        if current != publication.previous {
            return self
                .settle_store_snapshot_publication(membership, identity, pending)
                .await
                .map_err(Into::into);
        }
        if version != publication.previous_version {
            return Err(StoreError::InvalidOutbound(
                "Store publication revision changed without an accepted transition".into(),
            )
            .into());
        }
        retire_superseded_publication_entry(&self.database, self.storage.as_ref(), active).await?;
        // Resolve an earlier attempt before uploading. Exact immutable inputs
        // are required by the receiver's verification before conditional acceptance.
        objects.upload_store(pending).await?;
        self.history_verifier
            .load_store_publications_for_replay(&self.database)
            .await?;
        let author = self
            .database
            .activated_store_device_registration(pending.meta.value.author_registration.clone())
            .await?;
        let interval = coven_protocol::store_commit::VerifiedStorePublicationInterval::verified(
            publication.previous.clone(),
            publication.replacement.clone(),
            vec![
                coven_protocol::store_commit::StorePublicationIntervalEntry::new(
                    publication.entry.clone(),
                    publication.reference().map_err(StoreError::from)?,
                    author,
                ),
            ],
        )?;
        self.history_verifier
            .verify_proposed_publication(interval.clone())
            .await?;
        upload_store_publication_entry(
            self.storage.as_ref(),
            self.root().store_root_hash,
            publication,
        )
        .await?;
        let context = ProtocolObjectContext::signed_plaintext(
            self.root().store_root_hash,
            ProtocolObjectDomain::StoreCurrentPublication,
        );
        match self
            .storage
            .replace_protocol_record_if_version(
                &context,
                &self
                    .history_verifier
                    .verified_root()
                    .protocol()
                    .descriptor
                    .current_publication_slot,
                coven_protocol::store_commit::store_current_publication_semantic_prefix(),
                &publication.previous_version,
                publication.replacement.to_bytes(),
            )
            .await
        {
            Ok(coven_storage::cloud::ConditionalWriteOutcome::Replaced(version)) => {
                Ok(StoreSnapshotPublicationAttemptOutcome::Accepted(
                    coven_database::AcceptedStorePublicationInterval::from_verified(
                        interval,
                        Some(version),
                    ),
                ))
            }
            Ok(coven_storage::cloud::ConditionalWriteOutcome::VersionChanged) => self
                .settle_store_snapshot_publication(membership, identity, pending)
                .await
                .map_err(Into::into),
            Err(source) => self
                .settle_store_snapshot_publication(membership, identity, pending)
                .await
                .map_err(|verification| {
                    StoreError::PublicationSettlement {
                        publication: source,
                        verification: Box::new(verification),
                    }
                    .into()
                }),
        }
    }

    async fn settle_store_snapshot_publication(
        &mut self,
        membership: &mut MembershipChain,
        identity: &UserKeypair,
        pending: &coven_database::DurableSnapshotPublication,
    ) -> Result<StoreSnapshotPublicationAttemptOutcome, StoreError> {
        let pulled = self
            .install_current_publication(membership, identity)
            .await?;
        let position = pending.publication.reference()?.position;
        let baseline = self.database.installed_replay_baseline().await?;
        let superseding = baseline
            .snapshot()
            .filter(|snapshot| {
                snapshot
                    .meta
                    .publication_predecessor
                    .accepted()
                    .is_some_and(|previous| previous.position >= position)
            })
            .cloned();
        let previous = match &superseding {
            Some(snapshot) => snapshot.meta.publication_predecessor.clone(),
            None => pending.publication.previous.clone(),
        };
        let accepted = self
            .history_verifier
            .retained_store_publication_interval(&self.database, previous)
            .await?;
        if accepted.interval().entries().iter().any(|entry| {
            entry.entry() == &pending.publication.entry
                && entry.reference().object == pending.publication.entry_object
        }) {
            return Ok(StoreSnapshotPublicationAttemptOutcome::Accepted(accepted));
        }
        if !pulled.held_positions.is_empty() {
            return Err(StoreError::PublicationHeld(pulled.held_positions));
        }
        if let Some(snapshot) = superseding {
            if !snapshot.meta.coverage.covers(&pending.meta.value.coverage) {
                return Err(StoreError::InvalidOutbound(
                    "newer snapshot does not cover the pending checkpoint request".into(),
                ));
            }
            return Ok(StoreSnapshotPublicationAttemptOutcome::Superseded { snapshot, accepted });
        }
        if accepted
            .interval()
            .entries()
            .iter()
            .any(|entry| entry.reference().position == position)
        {
            return Ok(StoreSnapshotPublicationAttemptOutcome::Competing(accepted));
        }
        Err(StoreError::InvalidOutbound(
            "snapshot settlement lacks its exact accepted position".into(),
        ))
    }

    pub(crate) async fn install_current_publication(
        &mut self,
        membership: &mut MembershipChain,
        identity: &UserKeypair,
    ) -> Result<pull::StorePullResult, StoreError> {
        let routing_encryption = self.routing_encryption.clone();
        let execution = self
            .pull(membership, Some(identity), routing_encryption.as_ref())
            .await?;
        *membership = execution.membership;
        Ok(execution.result)
    }

    pub(crate) async fn capture_current_store_snapshot_cut(
        &self,
    ) -> Result<
        (
            coven_database::CreatedSnapshot,
            coven_protocol::store_commit::CommitFrontier,
        ),
        coven_database::DbError,
    > {
        self.database
            .capture_store_snapshot_cut(
                self.root().clone(),
                self.store_dir.as_ref().to_path_buf(),
                self.routing_encryption.clone(),
            )
            .await
    }

    async fn verify_prepared_publication(
        &mut self,
        publication: &coven_protocol::prepared_commit::PreparedStorePublication,
        commit: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
    ) -> Result<
        (
            coven_protocol::store_commit::VerifiedStorePublicationInterval,
            coven_protocol::membership::MembershipFloor,
        ),
        StoreError,
    > {
        publication.verify_commit(commit)?;
        self.history_verifier
            .load_store_publications_for_replay(&self.database)
            .await?;
        let author = coven_protocol::store_commit::ReferencedStoreDeviceRegistration::verified(
            commit.author_registration.clone(),
            commit.author().clone(),
        )?;
        let interval = coven_protocol::store_commit::VerifiedStorePublicationInterval::verified(
            publication.previous.clone(),
            publication.replacement.clone(),
            vec![
                coven_protocol::store_commit::StorePublicationIntervalEntry::new(
                    publication.entry.clone(),
                    publication.reference()?,
                    author,
                ),
            ],
        )?;
        let mut verified = self
            .history_verifier
            .verify_proposed_publication(interval)
            .await?;
        let predecessor = verified
            .commits
            .remove(commit.reference())
            .ok_or_else(|| {
                StoreError::InvalidOutbound(
                    "verified publication omits its candidate authority".into(),
                )
            })?
            .accepted_predecessor;
        Ok((verified.interval, predecessor))
    }

    pub(crate) async fn accepted_publication_membership_predecessor(
        &mut self,
        accepted: &coven_database::AcceptedStoreCommitPublication,
        commit: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
    ) -> Result<coven_protocol::membership::MembershipFloor, StoreError> {
        let (mut verified, _) = self
            .history_verifier
            .load_store_publications_for_replay(&self.database)
            .await?;
        if verified.interval.accepted_commit(commit)?.reference() != accepted.reference() {
            return Err(StoreError::InvalidOutbound(
                "accepted membership predecessor belongs to another publication".into(),
            ));
        }
        Ok(verified
            .commits
            .remove(commit.reference())
            .ok_or_else(|| {
                StoreError::InvalidOutbound(
                    "accepted publication omits its verified predecessor authority".into(),
                )
            })?
            .accepted_predecessor)
    }

    async fn prepare_store_commit_publication(
        &self,
        previous: &coven_database::ObservedStorePublication,
        commit: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
        signer: &UserKeypair,
    ) -> Result<coven_protocol::prepared_commit::PreparedStorePublication, StoreError> {
        let entry = coven_protocol::store_commit::StorePublicationEntry::signed_commit(
            previous.record(),
            commit,
            signer,
        )?;
        let context = ProtocolObjectContext::signed_plaintext(
            self.root().store_root_hash,
            ProtocolObjectDomain::StorePublicationEntry,
        );
        let prefix = coven_protocol::store_commit::store_publication_entry_semantic_prefix(&entry);
        let slot = self
            .storage
            .allocate_protocol_slot(&context, &prefix, ".json")
            .await
            .map_err(StoreObjectError::from)?;
        let prepared = self
            .storage
            .prepare_protocol_object(&context, slot, &prefix, entry.to_bytes())
            .map_err(StoreObjectError::from)?;
        let reference = coven_protocol::store_commit::StorePublicationRef::from_entry(
            &entry,
            prepared.reference().clone(),
        )?;
        let replacement =
            coven_protocol::store_commit::StoreCurrentPublicationRecord::advance_commit(
                previous.record(),
                &entry,
                reference,
                commit,
                signer,
            )?;
        Ok(coven_protocol::prepared_commit::PreparedStorePublication {
            previous: previous.record().clone(),
            previous_version: previous.version().clone(),
            entry,
            entry_object: prepared.reference().clone(),
            replacement,
        })
    }
}

async fn retire_superseded_publication_entry(
    database: &coven_database::StoreDatabase,
    storage: &dyn coven_storage::CloudSyncObjectStorage,
    active: coven_database::ActiveStorePublication,
) -> Result<(), StoreError> {
    if let Some(superseded) = active.superseded_entry() {
        storage
            .delete_protocol_object(&superseded.object)
            .await
            .map_err(StoreObjectError::from)?;
        database
            .complete_superseded_publication_cleanup(active)
            .await?;
    }
    Ok(())
}

async fn upload_store_publication_entry(
    storage: &dyn coven_storage::CloudSyncObjectStorage,
    store_root_hash: coven_protocol::store_commit::ObjectHash,
    publication: &coven_protocol::prepared_commit::PreparedStorePublication,
) -> Result<coven_protocol::store_commit::StorePublicationRef, StoreError> {
    let entry = &publication.entry;
    let entry_prefix = coven_protocol::store_commit::store_publication_entry_semantic_prefix(entry);
    let entry_context = ProtocolObjectContext::signed_plaintext(
        store_root_hash,
        ProtocolObjectDomain::StorePublicationEntry,
    );
    let prepared_entry = publication.prepared_entry().map_err(StoreError::from)?;
    let reference = coven_protocol::store_commit::StorePublicationRef::from_entry(
        entry,
        publication.entry_object.clone(),
    )?;
    storage
        .create_verified_protocol_object(
            &entry_context,
            &prepared_entry,
            &entry_prefix,
            &entry.to_bytes(),
        )
        .await
        .map_err(StoreError::prepared_object)?;
    Ok(reference)
}

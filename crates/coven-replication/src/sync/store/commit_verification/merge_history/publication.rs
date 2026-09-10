use super::*;

mod authority;

pub(crate) struct VerifiedStorePublication {
    pub(crate) interval: store_commit::VerifiedStorePublicationInterval,
    pub(crate) version: Option<coven_protocol::objects::ExactObjectVersion>,
    pub(crate) commits: BTreeMap<StoreBatchCommitRef, VerifiedStorePublicationCommit>,
    pub(crate) accepted_snapshots: Vec<SelectedStoreSnapshot>,
}

pub(crate) enum StorePublicationReplayInstallation {
    Retained(coven_database::AcceptedStorePublicationInterval),
    Checkpoint {
        expected: coven_database::StorePublicationBoundary,
        accepted: coven_database::AcceptedStorePublicationInterval,
    },
}

#[derive(Clone)]
pub(crate) struct VerifiedStorePublicationCommit {
    pub(crate) commit: VerifiedStoreBatchCommit,
    pub(crate) accepted_predecessor: protocol_membership::MembershipFloor,
}

/// A snapshot whose exact publication belongs to the verified accepted interval.
#[derive(Clone)]
pub(crate) struct AcceptedStoreSnapshot {
    snapshot: coven_database::PublishedStoreSnapshot,
    publication: store_commit::StorePublicationRef,
}

impl AcceptedStoreSnapshot {
    pub(crate) fn snapshot(&self) -> &coven_database::PublishedStoreSnapshot {
        &self.snapshot
    }

    pub(crate) fn reference(&self) -> store_commit::AcceptedStoreSnapshotRef {
        store_commit::AcceptedStoreSnapshotRef {
            snapshot: self.snapshot.reference.clone(),
            publication: self.publication.clone(),
        }
    }
}

/// Semantic authority is published only after the whole interval verifies.
/// Dropping an unfinished verification also rolls back an interrupted future.
struct PublicationVerification<'verification, 'storage> {
    verifier: &'verification mut MergeHistoryVerifier<'storage>,
    previous: Option<PublicationVerificationState>,
}

struct PublicationVerificationState {
    history: VerifiedMergeHistory,
    accepted: BTreeMap<StoreBatchCommitRef, AcceptedStoreCommitEvidence>,
    memberships: Vec<VerifiedMembershipChain>,
}

impl<'verification, 'storage> PublicationVerification<'verification, 'storage> {
    fn new(verifier: &'verification mut MergeHistoryVerifier<'storage>) -> Self {
        Self {
            previous: Some(PublicationVerificationState {
                history: verifier.history.clone(),
                accepted: verifier.accepted_publications.clone(),
                memberships: verifier.verified_memberships.clone(),
            }),
            verifier,
        }
    }

    fn commit(mut self) {
        self.previous = None;
    }
}

impl Drop for PublicationVerification<'_, '_> {
    fn drop(&mut self) {
        if let Some(previous) = self.previous.take() {
            self.verifier.history = previous.history;
            self.verifier.accepted_publications = previous.accepted;
            self.verifier.verified_memberships = previous.memberships;
        }
    }
}

impl MergeHistoryVerifier<'_> {
    pub(crate) fn admit_published_commit(
        &mut self,
        publication: coven_database::AcceptedStoreCommitPublication,
        commit: VerifiedStoreBatchCommit,
    ) -> Result<(), StorePullError> {
        let reference = commit.reference().clone();
        if publication.entry().payload
            != store_commit::StorePublicationPayload::Commit(reference.clone())
        {
            return Err(StorePullError::InvalidState(
                "accepted publication names another Store commit".to_string(),
            ));
        }
        if let Some((accepted, evidence)) = self
            .accepted_publications
            .iter()
            .find(|(accepted, _)| accepted.coord == reference.coord)
        {
            if accepted == &reference
                && matches!(evidence, AcceptedStoreCommitEvidence::Exact(existing) if existing == &publication)
            {
                return Ok(());
            }
            return Err(StorePullError::InvalidState(
                "Store publication reuses an already accepted author sequence".to_string(),
            ));
        }
        if self
            .history
            .baseline
            .coverage()
            .commits()
            .get(&reference.coord.stream_id)
            .is_some_and(|tip| reference.coord.sequence() <= tip.coord.sequence())
        {
            return Err(StorePullError::InvalidState(
                "Store publication reuses a snapshot-covered author sequence".to_string(),
            ));
        }
        self.commit_verifier
            .remember(commit)
            .map_err(StorePullError::Protocol)?;
        self.accepted_publications
            .insert(reference, AcceptedStoreCommitEvidence::Exact(publication));
        Ok(())
    }

    fn accepted_snapshot_value(
        accepted: &store_commit::AcceptedStoreSnapshotRef,
        meta: store_commit::SnapshotMeta,
    ) -> coven_database::PublishedStoreSnapshot {
        coven_database::PublishedStoreSnapshot {
            reference: accepted.snapshot.clone(),
            meta,
        }
    }

    pub(crate) async fn load_installed_current_accepted_snapshot(
        &mut self,
        database: &coven_database::StoreDatabase,
    ) -> Result<Option<coven_database::PublishedStoreSnapshot>, StorePullError> {
        let (current, exact_entries) = database.retained_store_publication().await?;
        let Some(accepted) = current.record().latest_snapshot().cloned() else {
            return Ok(None);
        };
        let exact = exact_entries
            .into_iter()
            .find(|entry| {
                entry.prepared.reference() == &accepted.publication.object
                    && entry.value.position == accepted.publication.position
                    && entry.value.entry_hash() == accepted.publication.entry_hash
            })
            .ok_or_else(|| {
                StorePullError::InvalidState(
                    "current accepted Store snapshot is absent from the installed publication interval"
                        .to_string(),
                )
            })?;
        let author = self
            .load_registration(&exact.value.author_registration)
            .await?;
        let entry = store_commit::StorePublicationEntry::parse_at(
            &exact.bytes,
            self.root.reference().store_root_hash,
            &accepted.publication,
            &author.value.device_signing_pubkey,
        )
        .map_err(StorePullError::Protocol)?;
        if entry != exact.value
            || !matches!(
                &entry.payload,
                store_commit::StorePublicationPayload::Snapshot(reference)
                    if reference == &accepted.snapshot
            )
        {
            return Err(StorePullError::InvalidState(
                "current accepted Store snapshot differs from its installed publication entry"
                    .to_string(),
            ));
        }
        let (_, meta) = self
            .load_store_snapshot(
                &entry.author_registration,
                &author.value,
                &accepted.snapshot,
            )
            .await?;
        Ok(Some(Self::accepted_snapshot_value(&accepted, meta)))
    }

    pub(crate) async fn verify_retained_join_snapshot(
        &mut self,
        accepted: &store_commit::AcceptedStoreSnapshotRef,
    ) -> Result<SelectedStoreSnapshot, StorePullError> {
        let metadata = self.load_snapshot_metadata(&accepted.snapshot).await?;
        let snapshot = Self::accepted_snapshot_value(accepted, metadata);
        // This historical handoff must not replace the active verifier's floor.
        let verification = PublicationVerification::new(self);
        let authority = verification
            .verifier
            .admit_accepted_snapshot_verification_baseline(snapshot.clone())
            .await?;
        let verified = VerifiedStoreSnapshotAuthority::from_authority(authority)?;
        Ok(SelectedStoreSnapshot { snapshot, verified })
    }

    pub(crate) async fn retained_store_publication_interval(
        &mut self,
        database: &coven_database::StoreDatabase,
        previous: store_commit::StoreCurrentPublicationRecord,
    ) -> Result<coven_database::AcceptedStorePublicationInterval, StorePullError> {
        let (current, exact_entries) = database.retained_store_publication().await?;
        let entries = self
            .open_retained_publication_entries(exact_entries, previous.accepted())
            .await?;
        let interval = store_commit::VerifiedStorePublicationInterval::verified(
            previous,
            current.record().clone(),
            entries,
        )
        .map_err(StorePullError::Protocol)?;
        Ok(
            coven_database::AcceptedStorePublicationInterval::from_verified(
                interval,
                current.observed_version().cloned(),
            ),
        )
    }

    pub(crate) async fn retained_store_publication_prefix(
        &mut self,
        database: &coven_database::StoreDatabase,
        previous: store_commit::StoreCurrentPublicationRecord,
        accepted: store_commit::StoreCurrentPublicationRecord,
    ) -> Result<coven_database::AcceptedStorePublicationInterval, StorePullError> {
        let (_, mut exact_entries) = database.retained_store_publication().await?;
        let terminal = accepted.accepted().ok_or_else(|| {
            StorePullError::InvalidState("retained accepted prefix has no terminal entry".into())
        })?;
        exact_entries.retain(|entry| entry.value.position <= terminal.position);
        let entries = self
            .open_retained_publication_entries(exact_entries, previous.accepted())
            .await?;
        let interval =
            store_commit::VerifiedStorePublicationInterval::verified(previous, accepted, entries)
                .map_err(StorePullError::Protocol)?;
        Ok(coven_database::AcceptedStorePublicationInterval::from_verified(interval, None))
    }

    async fn open_retained_publication_entries(
        &mut self,
        exact_entries: Vec<
            coven_protocol::objects::ExactProtocolObject<store_commit::StorePublicationEntry>,
        >,
        after: Option<&store_commit::StorePublicationRef>,
    ) -> Result<Vec<store_commit::StorePublicationIntervalEntry>, StorePullError> {
        let mut entries = Vec::with_capacity(exact_entries.len());
        for exact in exact_entries {
            if after.is_some_and(|boundary| exact.value.position <= boundary.position) {
                continue;
            }
            let author = self
                .load_registration(&exact.value.author_registration)
                .await?;
            let author = store_commit::ReferencedStoreDeviceRegistration::verified(
                exact.value.author_registration.clone(),
                author.value,
            )
            .map_err(StorePullError::Protocol)?;
            let reference = store_commit::StorePublicationRef::from_entry(
                &exact.value,
                exact.prepared.reference().clone(),
            )
            .map_err(StorePullError::Protocol)?;
            entries.push(store_commit::StorePublicationIntervalEntry::new(
                exact.value,
                reference,
                author,
            ));
        }
        Ok(entries)
    }

    /// Accepted entries remain replay inputs until their rows are installed.
    /// Verify that retained prefix before a new extension, so a held control
    /// still constrains the authority of later publications after reopening.
    pub(crate) async fn load_store_publications_for_replay(
        &mut self,
        database: &coven_database::StoreDatabase,
    ) -> Result<(VerifiedStorePublication, StorePublicationReplayInstallation), StorePullError>
    {
        let (baseline, observed, exact_entries, retained, materialized) = database
            .retained_store_replay(self.root.reference().clone())
            .await?;
        let (current, version) = self.read_current_store_publication().await?;
        if let (Some(observed), Some(checkpoint)) = (&observed, current.latest_snapshot()) {
            let newer_checkpoint = observed
                .record()
                .accepted()
                .is_none_or(|local| checkpoint.publication.position > local.position);
            let requires_image = if newer_checkpoint {
                let metadata = self.load_snapshot_metadata(&checkpoint.snapshot).await?;
                // An observed prefix can still contain held rows. Reconstruct
                // only when both its exact boundary and installed rows match
                // the snapshot; the retained installer verifies and folds them.
                observed.record() != &metadata.publication_predecessor
                    || materialized != metadata.coverage
            } else {
                false
            };
            if requires_image {
                let verification = PublicationVerification::new(self);
                let publication = Box::pin(
                    verification
                        .verifier
                        .load_current_accepted_publication_at(current, version),
                )
                .await?;
                let accepted = coven_database::AcceptedStorePublicationInterval::from_verified(
                    publication.interval.clone(),
                    publication.version.clone(),
                );
                verification.commit();
                return Ok((
                    publication,
                    StorePublicationReplayInstallation::Checkpoint {
                        expected: observed.clone(),
                        accepted,
                    },
                ));
            }
        }
        let snapshot = baseline.snapshot().cloned();
        self.admit_installed_baseline(baseline)?;
        self.admit_retained_history(&retained)?;
        self.verify_refs(retained.iter().map(|input| input.commit_ref().clone()))
            .await?;
        let Some(observed) = observed else {
            let publication = self.load_publication_from_genesis(current, version).await?;
            let accepted = coven_database::AcceptedStorePublicationInterval::from_verified(
                publication.interval.clone(),
                publication.version.clone(),
            );
            return Ok((
                publication,
                StorePublicationReplayInstallation::Retained(accepted),
            ));
        };
        // An installed snapshot can have an unretired local publication prefix.
        // Its signed predecessor is the replay anchor; earlier entries have
        // already been folded and must never become replay candidates again.
        let after = snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.meta.publication_predecessor.accepted());
        let mut entries = self
            .open_retained_publication_entries(exact_entries, after)
            .await?;
        let retained_interval = match &snapshot {
            Some(snapshot) => store_commit::VerifiedStorePublicationInterval::verified(
                snapshot.meta.publication_predecessor.clone(),
                observed.record().clone(),
                entries.clone(),
            ),
            None => store_commit::VerifiedStorePublicationInterval::from_genesis(
                self.root.reference().store_root_hash,
                &self.root.protocol().descriptor.founder_pubkey,
                observed.record().clone(),
                entries.clone(),
            ),
        }
        .map_err(StorePullError::Protocol)?;
        let mut retained_publication = self
            .verify_remote_publication(retained_interval, observed.observed_version().cloned())
            .await?;
        let extension = self
            .load_store_publication_interval_between(observed.record().clone(), current, version)
            .await?;
        let accepted = coven_database::AcceptedStorePublicationInterval::from_verified(
            extension.interval.clone(),
            extension.version.clone(),
        );
        entries.extend(extension.interval.entries().iter().map(|entry| {
            store_commit::StorePublicationIntervalEntry::new(
                entry.entry().clone(),
                entry.reference().clone(),
                entry.author().clone(),
            )
        }));
        let interval = match snapshot {
            Some(snapshot) => store_commit::VerifiedStorePublicationInterval::verified(
                snapshot.meta.publication_predecessor.clone(),
                extension.interval.current().clone(),
                entries,
            ),
            None => store_commit::VerifiedStorePublicationInterval::from_genesis(
                self.root.reference().store_root_hash,
                &self.root.protocol().descriptor.founder_pubkey,
                extension.interval.current().clone(),
                entries,
            ),
        }
        .map_err(StorePullError::Protocol)?;
        retained_publication.interval = interval;
        retained_publication.version = extension.version;
        retained_publication.commits.extend(extension.commits);
        retained_publication
            .accepted_snapshots
            .extend(extension.accepted_snapshots);
        Ok((
            retained_publication,
            StorePublicationReplayInstallation::Retained(accepted),
        ))
    }

    pub(crate) async fn load_store_publication_interval(
        &mut self,
        previous: &coven_database::StorePublicationBoundary,
    ) -> Result<VerifiedStorePublication, StorePullError> {
        let (current, version) = self.read_current_store_publication().await?;
        self.load_store_publication_interval_between(previous.record().clone(), current, version)
            .await
    }

    pub(crate) async fn load_current_store_publication_interval(
        &mut self,
        previous: store_commit::StoreCurrentPublicationRecord,
    ) -> Result<VerifiedStorePublication, StorePullError> {
        let (current, version) = self.read_current_store_publication().await?;
        self.load_store_publication_interval_between(previous, current, version)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn load_initial_store_publication_interval(
        &mut self,
    ) -> Result<VerifiedStorePublication, StorePullError> {
        let (current, version) = self.read_current_store_publication().await?;
        self.load_publication_from_genesis(current, version).await
    }

    async fn load_publication_from_genesis(
        &mut self,
        current: store_commit::StoreCurrentPublicationRecord,
        version: coven_protocol::objects::ExactObjectVersion,
    ) -> Result<VerifiedStorePublication, StorePullError> {
        let root_hash = self.root.reference().store_root_hash;
        let previous = store_commit::StoreCurrentPublicationRecordBody::genesis(root_hash);
        let entries = self.load_publication_entries(&previous, &current).await?;
        let interval = store_commit::VerifiedStorePublicationInterval::from_genesis(
            root_hash,
            &self.root.protocol().descriptor.founder_pubkey,
            current,
            entries,
        )
        .map_err(StorePullError::Protocol)?;
        self.verify_remote_publication(interval, Some(version))
            .await
    }

    pub(crate) async fn read_current_store_publication(
        &self,
    ) -> Result<
        (
            store_commit::StoreCurrentPublicationRecord,
            coven_protocol::objects::ExactObjectVersion,
        ),
        StorePullError,
    > {
        let context = ProtocolObjectContext::signed_plaintext(
            self.root.reference().store_root_hash,
            ProtocolObjectDomain::StoreCurrentPublication,
        );
        let prefix = store_commit::store_current_publication_semantic_prefix();
        let slot = &self.root.protocol().descriptor.current_publication_slot;
        let (current_bytes, version) = self
            .commit_verifier
            .read_versioned_protocol_record(&context, slot, prefix)
            .await?;
        let current: store_commit::StoreCurrentPublicationRecord =
            coven_protocol::objects::decode_protocol_object(&current_bytes)
                .map_err(StorePullError::Protocol)?;
        if current.to_bytes() != current_bytes {
            return Err(StorePullError::InvalidState(
                "Store current publication record is not canonical".to_string(),
            ));
        }
        Ok((current, version))
    }

    async fn load_store_publication_interval_between(
        &mut self,
        previous: store_commit::StoreCurrentPublicationRecord,
        current: store_commit::StoreCurrentPublicationRecord,
        version: coven_protocol::objects::ExactObjectVersion,
    ) -> Result<VerifiedStorePublication, StorePullError> {
        let interval = self
            .load_accepted_publication_interval(previous, current)
            .await?;
        self.verify_remote_publication(interval, Some(version))
            .await
    }

    pub(super) async fn load_accepted_publication_interval(
        &self,
        previous: store_commit::StoreCurrentPublicationRecord,
        current: store_commit::StoreCurrentPublicationRecord,
    ) -> Result<store_commit::VerifiedStorePublicationInterval, StorePullError> {
        let entries = self.load_publication_entries(&previous, &current).await?;
        store_commit::VerifiedStorePublicationInterval::verified(previous, current, entries)
            .map_err(StorePullError::Protocol)
    }

    async fn load_publication_entries(
        &self,
        previous: &store_commit::StoreCurrentPublicationRecordBody,
        current: &store_commit::StoreCurrentPublicationRecord,
    ) -> Result<Vec<store_commit::StorePublicationIntervalEntry>, StorePullError> {
        if &**current == previous {
            return Ok(Vec::new());
        }

        let mut cursor = current.accepted().cloned().ok_or_else(|| {
            StorePullError::InvalidState(
                "Store publication current moved behind its installed boundary".to_string(),
            )
        })?;
        let stop = previous.accepted();
        let entry_context = ProtocolObjectContext::signed_plaintext(
            self.root.reference().store_root_hash,
            ProtocolObjectDomain::StorePublicationEntry,
        );
        let mut reversed = Vec::new();
        loop {
            if stop == Some(&cursor) {
                break;
            }
            let semantic_prefix =
                store_commit::semantic_prefix_from_exact_object(&cursor.object, ".json")
                    .map_err(StorePullError::Protocol)?;
            let bytes = self
                .commit_verifier
                .read_protocol_object(&entry_context, &cursor.object, &semantic_prefix)
                .await?;
            let entry: store_commit::StorePublicationEntry =
                coven_protocol::objects::decode_protocol_object(&bytes)
                    .map_err(StorePullError::Protocol)?;
            if entry.to_bytes() != bytes {
                return Err(StorePullError::InvalidState(
                    "Store publication entry is not canonical".to_string(),
                ));
            }
            let author = self
                .commit_verifier
                .load_registration(&entry.author_registration)
                .await?;
            let author = store_commit::ReferencedStoreDeviceRegistration::verified(
                entry.author_registration.clone(),
                author.value,
            )
            .map_err(StorePullError::Protocol)?;
            let predecessor = entry.predecessor.clone();
            reversed.push(store_commit::StorePublicationIntervalEntry::new(
                entry, cursor, author,
            ));
            match predecessor {
                Some(predecessor) => cursor = predecessor,
                None if stop.is_none() => break,
                None => {
                    return Err(StorePullError::InvalidState(
                        "Store publication interval does not extend the installed boundary"
                            .to_string(),
                    ));
                }
            }
        }
        reversed.reverse();
        Ok(reversed)
    }

    pub(super) fn accepted_frontier_before(
        &self,
        position: store_commit::StorePublicationPosition,
    ) -> Result<BTreeMap<protocol_membership::AuthorStreamId, StoreBatchCommitRef>, StorePullError>
    {
        if self.history.baseline.snapshot().is_some_and(|snapshot| {
            snapshot
                .meta
                .publication_predecessor
                .accepted()
                .is_some_and(|predecessor| position <= predecessor.position)
        }) {
            return Err(StorePullError::SnapshotBehindReplayBaseline);
        }
        let mut frontier = self.history.baseline.coverage().commits().clone();
        for (accepted, evidence) in &self.accepted_publications {
            let precedes = match evidence {
                AcceptedStoreCommitEvidence::Exact(publication) => {
                    publication.reference().position < position
                }
                AcceptedStoreCommitEvidence::SnapshotCovered => true,
            };
            if precedes {
                let tip = frontier
                    .entry(accepted.coord.stream_id)
                    .or_insert_with(|| accepted.clone());
                if accepted.coord.sequence() > tip.coord.sequence() {
                    *tip = accepted.clone();
                }
            }
        }
        Ok(frontier)
    }

    /// Exercise the receiver's checks without granting a prepared candidate authority.
    pub(crate) async fn verify_proposed_publication(
        &mut self,
        interval: store_commit::VerifiedStorePublicationInterval,
    ) -> Result<VerifiedStorePublication, StorePullError> {
        let verification = PublicationVerification::new(self);
        let publication = Box::pin(
            verification
                .verifier
                .verify_remote_publication_inner(interval, None),
        )
        .await?;
        Ok(publication)
    }

    async fn verify_remote_publication(
        &mut self,
        interval: store_commit::VerifiedStorePublicationInterval,
        version: Option<coven_protocol::objects::ExactObjectVersion>,
    ) -> Result<VerifiedStorePublication, StorePullError> {
        let verification = PublicationVerification::new(self);
        let publication = Box::pin(
            verification
                .verifier
                .verify_remote_publication_inner(interval, version),
        )
        .await?;
        verification.commit();
        Ok(publication)
    }

    async fn verify_remote_publication_inner(
        &mut self,
        interval: store_commit::VerifiedStorePublicationInterval,
        version: Option<coven_protocol::objects::ExactObjectVersion>,
    ) -> Result<VerifiedStorePublication, StorePullError> {
        let mut commits = BTreeMap::new();
        let mut accepted_snapshots = Vec::new();
        // A snapshot establishes the inventory used by subsequent reclaim
        // commits. Verify both payload kinds in their accepted order.
        for entry in interval.entries() {
            match &entry.entry().payload {
                store_commit::StorePublicationPayload::Commit(reference) => {
                    let commit = self.commit_verifier.load_ref(reference).await?;
                    let publication = interval
                        .accepted_commit(&commit)
                        .map_err(StorePullError::Protocol)?;
                    self.admit_published_commit(
                        coven_database::AcceptedStoreCommitPublication::from_verified(publication),
                        commit.clone(),
                    )?;
                    self.verify_refs([reference.clone()]).await?;
                    let accepted_predecessor = Box::pin(self.verify_commit_publication_authority(
                        reference,
                        entry.reference().position,
                    ))
                    .await?;
                    commits.insert(
                        reference.clone(),
                        VerifiedStorePublicationCommit {
                            commit,
                            accepted_predecessor,
                        },
                    );
                }
                store_commit::StorePublicationPayload::Snapshot(reference) => {
                    let (_, meta) = self
                        .load_store_snapshot(
                            &entry.entry().author_registration,
                            entry.author().value(),
                            reference,
                        )
                        .await?;
                    if meta.author_registration != entry.entry().author_registration
                        || meta.publication_predecessor.state_hash()
                            != entry.entry().previous_state_hash
                    {
                        return Err(StorePullError::InvalidState(
                            "accepted Store snapshot names another publication predecessor"
                                .to_string(),
                        ));
                    }
                    if meta.coverage.commits()
                        != &self.accepted_frontier_before(entry.reference().position)?
                    {
                        return Err(StorePullError::InvalidState(
                            "Store snapshot coverage differs from its accepted publication predecessor"
                                .into(),
                        ));
                    }
                    let published = Self::accepted_snapshot_value(
                        &store_commit::AcceptedStoreSnapshotRef {
                            publication: entry.reference().clone(),
                            snapshot: reference.clone(),
                        },
                        meta,
                    );
                    let verified = self.verify_installable_snapshot(&published).await?;
                    accepted_snapshots.push(SelectedStoreSnapshot {
                        snapshot: published,
                        verified,
                    });
                }
            }
        }
        Ok(VerifiedStorePublication {
            interval,
            version,
            commits,
            accepted_snapshots,
        })
    }

    pub(crate) fn snapshot_authenticates_reclaim_authorization(
        &self,
        authorization: &coven_protocol::reclaim::ReclaimAuthorizationRef,
        activation: &StoreBatchCommitRef,
    ) -> bool {
        self.history.baseline.snapshot().is_some_and(|baseline| {
            baseline
                .meta
                .history_summary
                .reclaim
                .authorizations
                .get(&authorization.authorization_hash)
                .is_some_and(|retained| {
                    &retained.authorization == authorization && &retained.activation == activation
                })
        })
    }

    pub(crate) async fn load_snapshot_metadata(
        &self,
        reference: &store_commit::StoreSnapshotRef,
    ) -> Result<store_commit::SnapshotMeta, StorePullError> {
        if let Some(installed) = self.history.baseline.snapshot() {
            if &installed.reference == reference {
                return Ok(installed.meta.clone());
            }
        }
        self.commit_verifier
            .load_snapshot_metadata(reference)
            .await
            .map_err(StorePullError::from)
    }

    pub(crate) async fn current_accepted_snapshot(
        &mut self,
    ) -> Result<Option<AcceptedStoreSnapshot>, StorePullError> {
        let publication = self.load_current_accepted_publication().await?;
        let Some(selected) = publication.accepted_snapshots.last() else {
            return Ok(None);
        };
        let accepted = publication
            .interval
            .current()
            .latest_snapshot()
            .ok_or_else(|| {
                StorePullError::InvalidState(
                    "verified snapshot has no accepted publication".to_string(),
                )
            })?;
        if accepted.snapshot != selected.snapshot.reference {
            return Err(StorePullError::InvalidState(
                "verified snapshot differs from the accepted current snapshot".to_string(),
            ));
        }
        Ok(Some(AcceptedStoreSnapshot {
            snapshot: selected.snapshot.clone(),
            publication: accepted.publication.clone(),
        }))
    }

    pub(crate) async fn load_current_accepted_snapshot(
        &mut self,
    ) -> Result<SelectedStoreSnapshot, StorePullError> {
        let verification = PublicationVerification::new(self);
        let mut publication = Box::pin(
            verification
                .verifier
                .load_current_accepted_publication_inner(),
        )
        .await?;
        let snapshot = publication.accepted_snapshots.pop().ok_or_else(|| {
            StorePullError::InvalidState(
                "Store current publication record has no accepted snapshot".to_string(),
            )
        })?;
        verification.commit();
        Ok(snapshot)
    }

    pub(super) async fn load_current_accepted_publication(
        &mut self,
    ) -> Result<VerifiedStorePublication, StorePullError> {
        let verification = PublicationVerification::new(self);
        let publication = Box::pin(
            verification
                .verifier
                .load_current_accepted_publication_inner(),
        )
        .await?;
        verification.commit();
        Ok(publication)
    }

    async fn load_current_accepted_publication_inner(
        &mut self,
    ) -> Result<VerifiedStorePublication, StorePullError> {
        let (current, version) = self.read_current_store_publication().await?;
        self.load_current_accepted_publication_at(current, version)
            .await
    }

    async fn load_current_accepted_publication_at(
        &mut self,
        current: store_commit::StoreCurrentPublicationRecord,
        version: coven_protocol::objects::ExactObjectVersion,
    ) -> Result<VerifiedStorePublication, StorePullError> {
        let Some(accepted) = current.latest_snapshot().cloned() else {
            return self.load_publication_from_genesis(current, version).await;
        };
        let meta = self.load_snapshot_metadata(&accepted.snapshot).await?;
        let publication = self
            .load_accepted_publication_interval(meta.publication_predecessor.clone(), current)
            .await?;
        let included = publication.entries().iter().any(|entry| {
            entry.reference() == &accepted.publication
                && entry.author().reference() == &meta.author_registration
                && matches!(
                    &entry.entry().payload,
                    store_commit::StorePublicationPayload::Snapshot(reference)
                        if reference == &accepted.snapshot
                )
        });
        if !included {
            return Err(StorePullError::InvalidState(
                "Store current publication snapshot is absent from its verified interval"
                    .to_string(),
            ));
        }
        if !self.history.baseline.stands_on(&accepted.snapshot) {
            self.admit_accepted_snapshot_verification_baseline(Self::accepted_snapshot_value(
                &accepted,
                meta.clone(),
            ))
            .await?;
        }
        for reference in meta.history_summary.causal_cut.values() {
            self.accepted_publications
                .entry(reference.clone())
                .or_insert(AcceptedStoreCommitEvidence::SnapshotCovered);
        }
        self.verify_remote_publication(publication, Some(version))
            .await
    }
}

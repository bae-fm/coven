use super::*;

impl<'a> StoreCommitVerifier<'a> {
    /// Read the Store packages `commits` name, all at once, and hold the bytes.
    ///
    /// A commit's package is named by that commit and nothing else, so the reads
    /// have no order between them — while applying the commits very much does.
    /// Catching up ten commits used to cost ten round trips one after another,
    /// each one the latency of a phone's link, because the read sat inside the
    /// ordered pass. It happens here instead, and the ordered pass finds the
    /// bytes already in hand.
    ///
    /// Holding bytes is not believing them: [`load_exact_object`] re-runs its
    /// verification on a hit, so a package still has to answer to the commit
    /// that names it. A read that fails is simply not held — the ordered pass
    /// reads that one itself and reports the failure at its own position, which
    /// is the only place a held position means anything.
    ///
    /// [`load_exact_object`]: Self::load_exact_object
    pub(crate) async fn prefetch_store_packages<'commits>(
        &self,
        commits: impl IntoIterator<Item = (&'commits StoreBatchCommitRef, &'commits StoreBatchCommit)>,
    ) {
        use futures_util::StreamExt;

        let reads = commits
            .into_iter()
            .filter_map(|(reference, commit)| {
                let package = commit.store_package()?;
                let semantic_prefix = package_semantic_prefix(
                    commit.candidate_family(),
                    &reference.coord.stream_id.to_string(),
                    commit.seq(),
                    package.content_hash,
                );
                let context = ProtocolObjectContext::store_encrypted(
                    commit.store_root_hash,
                    ProtocolObjectDomain::StorePackage,
                );
                Some((context, package.object.clone(), semantic_prefix))
            })
            .collect::<Vec<_>>();
        futures_util::stream::iter(reads)
            .map(|(context, object, semantic_prefix)| async move {
                if let Ok(bytes) = self
                    .storage
                    .read_protocol_object(&context, &object, &semantic_prefix)
                    .await
                {
                    self.remember_exact_object(&object, &bytes);
                }
            })
            .buffer_unordered(PROTOCOL_SLOT_READ_WIDTH)
            .collect::<()>()
            .await;
    }

    pub(crate) async fn load_store_package(
        &mut self,
        reference: &StoreBatchCommitRef,
    ) -> Result<Option<VerifiedObject<Vec<u8>>>, StoreObjectError> {
        let verified = self.load_ref(reference).await?;
        let commit = verified.value();
        let stream_id = verified.reference().coord.stream_id.to_string();
        let Some(package) = commit.store_package() else {
            return Ok(None);
        };
        let semantic_prefix = package_semantic_prefix(
            commit.candidate_family(),
            &stream_id,
            commit.seq(),
            package.content_hash,
        );
        let context = ProtocolObjectContext::store_encrypted(
            commit.store_root_hash,
            ProtocolObjectDomain::StorePackage,
        );
        let expected_commit = commit.clone();
        self.load_exact_object(
            &context,
            &package.object,
            &semantic_prefix,
            package.content_hash,
            move |bytes| {
                expected_commit.verify_store_package(bytes)?;
                Ok(bytes.to_vec())
            },
        )
        .await
        .map(Some)
    }

    pub(crate) async fn load_ref(
        &mut self,
        reference: &StoreBatchCommitRef,
    ) -> Result<VerifiedStoreBatchCommit, StoreObjectError> {
        if let Some(commit) = self.commits.get(reference) {
            return Ok(commit.clone());
        }
        let verified = self.load_verified_commit(reference).await?;
        self.commits.insert(reference.clone(), verified.clone());
        Ok(verified)
    }

    pub(crate) async fn authenticate_bytes(
        &mut self,
        reference: &StoreBatchCommitRef,
        bytes: &[u8],
    ) -> Result<VerifiedStoreBatchCommit, StoreObjectError> {
        if let Some(commit) = self.commits.get(reference) {
            if commit.value().to_bytes() == bytes {
                return Ok(commit.clone());
            }
            return Err(StoreObjectError::InvalidObject {
                semantic_prefix: "Store candidate commit".to_string(),
                key: reference.object.slot().logical_key().to_string(),
                source: Box::new(StoreProtocolError::Malformed(
                    "supplied Store commit bytes differ from the authenticated operation input"
                        .to_string(),
                )),
            });
        }
        let verified = self.verify_commit_bytes(reference, bytes.to_vec()).await?;
        self.commits.insert(reference.clone(), verified.clone());
        Ok(verified)
    }

    pub(crate) async fn load_verified_commit(
        &self,
        reference: &StoreBatchCommitRef,
    ) -> Result<VerifiedStoreBatchCommit, StoreObjectError> {
        let semantic_prefix = semantic_prefix_from_exact_object(&reference.object, ".json")
            .map_err(|source| StoreObjectError::InvalidObject {
                semantic_prefix: "Store candidate commit".to_string(),
                key: reference.object.slot().logical_key().to_string(),
                source: Box::new(source),
            })?;
        let context = ProtocolObjectContext::signed_plaintext(
            self.root.reference().store_root_hash,
            ProtocolObjectDomain::StoreCommit,
        );
        let bytes = self
            .storage
            .read_protocol_object(&context, &reference.object, &semantic_prefix)
            .await
            .map_err(StoreObjectError::Storage)?;
        self.verify_commit_bytes(reference, bytes).await
    }

    pub(crate) async fn verify_commit_bytes(
        &self,
        reference: &StoreBatchCommitRef,
        bytes: Vec<u8>,
    ) -> Result<VerifiedStoreBatchCommit, StoreObjectError> {
        let semantic_prefix = semantic_prefix_from_exact_object(&reference.object, ".json")
            .map_err(|source| StoreObjectError::InvalidObject {
                semantic_prefix: "Store candidate commit".to_string(),
                key: reference.object.slot().logical_key().to_string(),
                source: Box::new(source),
            })?;
        /// Who signed the commit, read out of the envelope before the whole
        /// commit is parsed: the author's registration is what the full parse
        /// then verifies the signature against, so it has to be known first.
        #[derive(serde::Deserialize)]
        struct StoreCommitAuthorProjection {
            body: StoreCommitAuthorBody,
        }

        #[derive(serde::Deserialize)]
        struct StoreCommitAuthorBody {
            author_registration: StoreDeviceRegistrationRef,
        }

        let parse_bytes = bytes.clone();
        let author_reference = run_blocking_object_verification(
            &semantic_prefix,
            &reference.object,
            Box::new(move || {
                serde_json::from_slice::<StoreCommitAuthorProjection>(&parse_bytes)
                    .map(|projection| projection.body.author_registration)
                    .map_err(StoreProtocolError::from)
            }),
        )
        .await?;
        let author = self.load_registration(&author_reference).await?.value;
        let expected_reference = reference.clone();
        let expected_author = author.clone();
        let store_root_hash = self.root.reference().store_root_hash;
        let verify_bytes = bytes;
        run_blocking_object_verification(
            &semantic_prefix,
            &reference.object,
            Box::new(move || {
                VerifiedStoreBatchCommit::parse(
                    &verify_bytes,
                    store_root_hash,
                    &expected_reference,
                    &expected_author,
                )
            }),
        )
        .await
    }

    pub(crate) async fn load_commit_device_operations(
        &mut self,
        resolver: Option<&DeviceStateResolver<'_>>,
        commit: &StoreBatchCommit,
        predecessor_state: &ResolvedStoreDeviceState,
        predecessor_membership: Option<&MembershipChain>,
    ) -> Result<VerifiedStoreDeviceOperations, RegistrationLoadError> {
        if commit.device_exclusion_proposals().is_empty()
            && commit.device_exclusion_outcomes().is_empty()
        {
            return VerifiedStoreDeviceOperations::without_exclusions(commit)
                .map_err(RegistrationLoadError::from);
        }
        let predecessor = predecessor_membership.ok_or_else(|| {
            RegistrationLoadError::Invalid(
                "device exclusion activation has no exact predecessor membership authority"
                    .to_string(),
            )
        })?;
        let mut proposals = Vec::with_capacity(commit.device_exclusion_proposals().len());
        for reference in commit.device_exclusion_proposals() {
            let opened = self
                .load_device_exclusion_proposal(reference)
                .await
                .map_err(RegistrationLoadError::Object)?;
            let proposal = &opened.object.value;
            if proposal.frozen_device_state != commit.device_state
                || !device_state_has_active_registration(predecessor_state, &proposal.target)
                || !device_state_has_active_registration(
                    predecessor_state,
                    &proposal.owner_registration,
                )
                || !predecessor_verifies_owner(
                    predecessor,
                    &commit.membership_state,
                    &opened.owner.author_pubkey,
                    &proposal.owner_grant,
                )
            {
                return Err(RegistrationLoadError::Invalid(
                    "device exclusion proposal differs from its active predecessor authority"
                        .to_string(),
                ));
            }
            proposals.push(RetainedStoreDeviceExclusionProposal::from_verified(&opened));
        }
        let mut outcomes = Vec::with_capacity(commit.device_exclusion_outcomes().len());
        for reference in commit.device_exclusion_outcomes() {
            if !device_state_has_pending_proposal(predecessor_state, reference.proposal()) {
                return Err(RegistrationLoadError::Invalid(
                    "device exclusion outcome does not resolve an exact pending proposal"
                        .to_string(),
                ));
            }
            let proposal = self
                .load_device_exclusion_proposal(reference.proposal())
                .await
                .map_err(RegistrationLoadError::Object)?;
            let outcome = self
                .load_device_exclusion_outcome(reference, &proposal)
                .await
                .map_err(RegistrationLoadError::Object)?;
            let (owner_registration, owner_grant) = match &outcome.object.value {
                StoreDeviceExclusionOutcome::Excluded(exclusion) => {
                    (&exclusion.owner_registration, &exclusion.owner_grant)
                }
                StoreDeviceExclusionOutcome::Cancelled(cancellation) => {
                    (&cancellation.owner_registration, &cancellation.owner_grant)
                }
            };
            if !device_state_has_active_registration(predecessor_state, owner_registration)
                || !predecessor_verifies_owner(
                    predecessor,
                    &commit.membership_state,
                    &outcome.owner.author_pubkey,
                    owner_grant,
                )
            {
                return Err(RegistrationLoadError::Invalid(
                    "device exclusion outcome signer is not an active Owner at its predecessor"
                        .to_string(),
                ));
            }
            match (&outcome.object.value, reference) {
                (
                    StoreDeviceExclusionOutcome::Cancelled(_),
                    StoreDeviceExclusionOutcomeRef::Cancelled(_),
                ) => {}
                (
                    StoreDeviceExclusionOutcome::Excluded(exclusion),
                    StoreDeviceExclusionOutcomeRef::Excluded(_),
                ) => {
                    let StoreDeviceExclusionProof {
                        frozen_device_state,
                        remaining_device_acks,
                        cutoff,
                    } = &exclusion.proof;
                    if frozen_device_state != &proposal.object.value.frozen_device_state {
                        return Err(RegistrationLoadError::Invalid(
                            "device exclusion proof names another frozen device state".to_string(),
                        ));
                    }
                    let resolver = resolver.ok_or_else(|| {
                        RegistrationLoadError::Invalid(
                            "Merge device exclusion proof has no materialized state resolver"
                                .to_string(),
                        )
                    })?;
                    let frozen = resolver
                        .resolve(&proposal.object.value.frozen_device_state)
                        .await?;
                    if !device_state_has_active_registration(&frozen, &proposal.object.value.target)
                    {
                        return Err(RegistrationLoadError::Invalid(
                            "device exclusion proposal frozen state does not contain its active target"
                                .to_string(),
                        ));
                    }
                    let required = frozen
                        .devices
                        .values()
                        .filter(|record| {
                            record.registration != proposal.object.value.target
                                && matches!(record.status, StoreDeviceStatus::Active)
                        })
                        .map(|record| (record.registration.clone(), record))
                        .collect::<BTreeMap<_, _>>();
                    let target_stream = StreamActivation::device_authorized_stream_id(
                        self.root.reference().store_root_hash,
                        &proposal.object.value.target,
                        StreamAnchorDomain::StoreAnnouncements,
                    );
                    let mut certified = BTreeSet::new();
                    let mut joined = BTreeMap::new();
                    for reference in remaining_device_acks {
                        let required_record = required.get(&reference.registration).ok_or_else(|| {
                            RegistrationLoadError::Invalid(
                                "device exclusion proof contains an acknowledgement from an ineligible registration"
                                    .to_string(),
                            )
                        })?;
                        if !certified.insert(reference.registration.clone()) {
                            return Err(RegistrationLoadError::Invalid(
                                "device exclusion proof repeats a remaining registration"
                                    .to_string(),
                            ));
                        }
                        let registration = self
                            .load_registration(&required_record.registration)
                            .await
                            .map_err(RegistrationLoadError::Object)?
                            .value;
                        let ack = self
                            .load_store_ack(reference, &registration)
                            .await
                            .map_err(RegistrationLoadError::Object)?;
                        if !self
                            .predecessor_activates_acknowledgement(&commit.order, reference, &ack)
                            .await
                            .map_err(registration_attempt_error)?
                        {
                            return Err(RegistrationLoadError::Invalid(
                                "device exclusion proof acknowledgement is not activated in the outcome predecessor"
                                    .to_string(),
                            ));
                        }
                        let ack_state = resolver.resolve(&ack.device_state).await?;
                        if !device_state_has_pending_proposal(&ack_state, &proposal.reference) {
                            return Err(RegistrationLoadError::Invalid(
                                "device exclusion proof acknowledgement does not observe the pending proposal"
                                    .to_string(),
                            ));
                        }
                        let freeze = ack
                            .exclusions
                            .proposal_freezes
                            .iter()
                            .find(|freeze| freeze.proposal == proposal.reference)
                            .ok_or_else(|| {
                                RegistrationLoadError::Invalid(
                                    "device exclusion proof acknowledgement omits the exact proposal freeze"
                                        .to_string(),
                                )
                            })?;
                        let target_cut = &freeze.target_cut.0;
                        if target_cut.len() > 1
                            || target_cut.keys().any(|stream| stream != &target_stream)
                        {
                            return Err(RegistrationLoadError::Invalid(
                                "device exclusion proof acknowledgement includes a non-target stream"
                                    .to_string(),
                            ));
                        }
                        if !ack
                            .store_cut
                            .frontier()
                            .covers(&freeze.target_cut.frontier())
                        {
                            return Err(RegistrationLoadError::Invalid(
                                "device exclusion proof acknowledgement target cut exceeds its Store cut"
                                    .to_string(),
                            ));
                        }
                        if let Some(reference) = target_cut.get(&target_stream) {
                            match joined.entry(target_stream) {
                                std::collections::btree_map::Entry::Vacant(entry) => {
                                    entry.insert(reference.clone());
                                }
                                std::collections::btree_map::Entry::Occupied(mut entry) => {
                                    let current = entry.get();
                                    if reference.coord.sequence() > current.coord.sequence() {
                                        entry.insert(reference.clone());
                                    } else if reference.coord.sequence() == current.coord.sequence()
                                        && reference != current
                                    {
                                        return Err(RegistrationLoadError::Invalid(
                                            "device exclusion proof target cuts fork at one sequence"
                                                .to_string(),
                                        ));
                                    }
                                }
                            }
                        }
                    }
                    if certified != required.into_keys().collect()
                        || cutoff != &StoreHistoryCut(joined)
                    {
                        return Err(RegistrationLoadError::Invalid(
                            "device exclusion proof does not certify every remaining registration and exact cutoff"
                                .to_string(),
                        ));
                    }
                    let predecessor_cut = commit
                        .order
                        .predecessor_cut()
                        .map_err(RegistrationLoadError::from)?;
                    let predecessor_target = predecessor_cut
                        .0
                        .get(&target_stream)
                        .map(|reference| BTreeMap::from([(target_stream, reference.clone())]));
                    let target_predecessor_cut =
                        StoreHistoryCut(predecessor_target.unwrap_or_default());
                    if !cutoff.frontier().covers(&target_predecessor_cut.frontier()) {
                        return Err(RegistrationLoadError::Invalid(
                            "device exclusion outcome predecessor advances the target beyond its certified cutoff"
                                .to_string(),
                        ));
                    }
                }
                _ => {
                    return Err(RegistrationLoadError::Invalid(
                        "device exclusion outcome variant differs from its exact reference"
                            .to_string(),
                    ))
                }
            }
            outcomes.push(
                RetainedStoreDeviceExclusionOutcome::from_verified(
                    reference,
                    RetainedStoreDeviceExclusionProposal::from_verified(&proposal),
                    &outcome,
                )
                .map_err(RegistrationLoadError::from)?,
            );
        }
        RetainedStoreDeviceOperations::from_sources(proposals, outcomes)
            .verify_for(self.root.reference(), commit)
            .map_err(RegistrationLoadError::from)
    }

    pub(crate) async fn read_protocol_object(
        &self,
        context: &ProtocolObjectContext,
        object: &ExactObjectRef,
        semantic_prefix: &str,
    ) -> Result<Vec<u8>, StoreObjectError> {
        self.storage
            .read_protocol_object(context, object, semantic_prefix)
            .await
            .map_err(StoreObjectError::from)
    }

    pub(crate) async fn read_protocol_slot(
        &self,
        context: &ProtocolObjectContext,
        slot: &coven_protocol::objects::ObjectSlot,
        semantic_prefix: &str,
    ) -> Result<(Vec<u8>, ExactObjectRef), StorageError> {
        self.storage
            .read_protocol_slot(context, slot, semantic_prefix)
            .await
    }

    /// Every slot the walk of `listing_prefix` might reach, fetched together.
    ///
    /// What this changes for a caller is how many round trips it makes, never
    /// what it ends up believing: which slots exist is decided by an unsigned
    /// listing that gets no trust, and the bytes are handed back unverified for
    /// the caller to check exactly as it would have checked its own reads.
    /// Anything the caller reaches and does not find here it still reads for
    /// itself.
    ///
    /// A read that fails is a different matter from a slot that is absent, and
    /// is reported rather than dropped. Absence is ordinary — it is how a chain
    /// walk learns it has reached the end — but a slot under this prefix that
    /// cannot be read is an object this Store wrote and can no longer open, and
    /// swallowing that would leave a caller retrying it silently and, if the
    /// second read happened to succeed, believing a chain whose first read had
    /// failed.
    ///
    /// The result is remembered under the prefix, because one anchored-chain
    /// load walks the same stream several times and each walk would otherwise
    /// list and fetch it again.
    /// Hold bytes for a slot stream this verifier did not read off the
    /// provider, so a walk of `listing_prefix` finds them where its own
    /// prefetch would have put them.
    ///
    /// Same standing as a prefetch: the walk still runs every check on every
    /// slot it reaches, and a slot it reaches that is not here is read the way
    /// it always was. The caller is responsible for having established that
    /// these bytes belong at these slots before handing them over — this only
    /// decides which round trips are skipped.
    pub(crate) fn remember_slot_stream(&self, listing_prefix: &str, reads: StreamSlotReads) {
        self.prefetched_slot_streams
            .lock()
            .expect("prefetched slot stream cache poisoned")
            .entry(listing_prefix.to_string())
            .or_insert(reads);
    }

    pub(crate) async fn prefetch_slot_stream(
        &self,
        context: &ProtocolObjectContext,
        listing_prefix: &str,
        anchor_slots: Vec<coven_protocol::objects::ObjectSlot>,
    ) -> Result<PrefetchedSlotStream, StorageError> {
        if let Some(prefetched) = self
            .prefetched_slot_streams
            .lock()
            .expect("prefetched slot stream cache poisoned")
            .get(listing_prefix)
        {
            return Ok(PrefetchedSlotStream::Remembered(prefetched.clone()));
        }
        let mut slots = anchor_slots;
        slots.extend(
            self.storage
                .list_protocol_slots(context, listing_prefix)
                .await?,
        );
        let prefetched: StreamSlotReads =
            std::sync::Arc::new(self.read_protocol_slots(context, slots).await?);
        self.prefetched_slot_streams
            .lock()
            .expect("prefetched slot stream cache poisoned")
            .insert(listing_prefix.to_string(), prefetched.clone());
        Ok(PrefetchedSlotStream::Fetched(prefetched))
    }

    /// Read every slot in `slots`, `PROTOCOL_SLOT_READ_WIDTH` at a time.
    ///
    /// Slots whose names a reader already knows have no ordering between them,
    /// so reading them one after another spends a provider round trip per slot
    /// for nothing.
    ///
    /// A slot that is absent, or whose key is not one this domain writes, is
    /// left out — the caller decides what a missing slot means, and for a chain
    /// walk it means the end. Every other failure is returned, because a reader
    /// that cannot tell "not there" from "could not look" would read a provider
    /// fault as the end of history.
    ///
    /// Nothing here is verified beyond opening the stored bytes; the caller
    /// runs its own checks on whatever it takes.
    async fn read_protocol_slots(
        &self,
        context: &ProtocolObjectContext,
        slots: Vec<coven_protocol::objects::ObjectSlot>,
    ) -> Result<BTreeMap<coven_protocol::objects::ObjectSlot, ReadProtocolSlot>, StorageError> {
        use futures_util::StreamExt;

        let mut slots = slots;
        slots.sort();
        slots.dedup();
        futures_util::stream::iter(slots)
            .map(|slot| async move {
                let Some(semantic_prefix) = context.semantic_prefix_of(&slot) else {
                    return Ok(None);
                };
                match self
                    .storage
                    .read_protocol_slot(context, &slot, semantic_prefix)
                    .await
                {
                    Ok((bytes, object)) => Ok(Some((slot, ReadProtocolSlot { bytes, object }))),
                    Err(StorageError::NotFound(_)) => Ok(None),
                    Err(error) => Err(error),
                }
            })
            .buffer_unordered(PROTOCOL_SLOT_READ_WIDTH)
            .filter_map(|read| std::future::ready(read.transpose()))
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect()
    }

    pub(crate) fn remember(
        &mut self,
        commit: VerifiedStoreBatchCommit,
    ) -> Result<(), StoreProtocolError> {
        if commit.store_root_hash() != self.root.reference().store_root_hash {
            return Err(StoreProtocolError::StoreRootMismatch {
                expected: self.root.reference().store_root_hash,
                actual: commit.store_root_hash(),
            });
        }
        self.commits.insert(commit.reference().clone(), commit);
        Ok(())
    }
}

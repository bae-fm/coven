use super::*;

impl<'a> StoreCommitVerifier<'a> {
    /// An acknowledgement this verifier has authenticated, by reference.
    ///
    /// A cache hit still checks the whole reference, not just the object it is
    /// keyed by: the reference names the registration, sequence, and semantic
    /// hash, so an entry that matches it is the same bytes verified under the
    /// same author.
    pub(crate) fn remembered_acknowledgement(&self, reference: &StoreAckRef) -> Option<StoreAck> {
        self.acknowledgements
            .lock()
            .expect("authenticated acknowledgement cache poisoned")
            .get(&reference.object)
            .filter(|(cached, _)| cached == reference)
            .map(|(_, value)| value.clone())
    }

    /// The newest snapshot `registration` has published an acknowledgement of.
    ///
    /// A device's own acknowledgement is what licenses it to stand on a
    /// snapshot, and the statement keeps standing after the acknowledgement
    /// that carried it stops being the latest one. Its later acknowledgements
    /// name no snapshot at all once the store's device state moves past what
    /// any published snapshot describes — a device registered, excluded or
    /// recovered, and nothing the owner has published still describes the store
    /// — so reading the licence off the newest acknowledgement, or off what the
    /// device could acknowledge next, finds nothing exactly when the device has
    /// most history to retire.
    ///
    /// Answered from the acknowledgements this verifier holds, which the pull
    /// seeds from this device's own retained rows: no read, and no fact that
    /// was not already authenticated.
    pub(crate) fn newest_acknowledged_snapshot(
        &self,
        registration: &StoreDeviceRegistrationRef,
    ) -> Option<StoreSnapshotLocator> {
        self.acknowledgements
            .lock()
            .expect("authenticated acknowledgement cache poisoned")
            .values()
            .filter(|(reference, value)| {
                &reference.registration == registration && value.snapshot.is_some()
            })
            .max_by_key(|(reference, _)| reference.sequence)
            .and_then(|(_, value)| value.snapshot.clone())
    }

    /// Admit an acknowledgement this verifier did not read itself, from a source
    /// that authenticated it under the same root — a retained materialization
    /// row's activated-ack evidence. Rejects a value that disagrees with its
    /// reference or with an entry already admitted, so nothing enters the cache
    /// that reading the object would have refused.
    pub(crate) fn remember_acknowledgement(
        &self,
        reference: &StoreAckRef,
        value: &StoreAck,
    ) -> Result<(), StoreProtocolError> {
        if value.registration != reference.registration
            || value.sequence != reference.sequence
            || value.ack_hash() != reference.ack_hash
        {
            return Err(StoreProtocolError::Malformed(
                "Store acknowledgement differs from its exact reference".to_string(),
            ));
        }
        let mut acknowledgements = self
            .acknowledgements
            .lock()
            .expect("authenticated acknowledgement cache poisoned");
        match acknowledgements.get(&reference.object) {
            Some((cached, cached_value)) if cached == reference && cached_value == value => {}
            Some(_) => {
                return Err(StoreProtocolError::Malformed(
                    "one exact Store acknowledgement object produced different values".to_string(),
                ))
            }
            None => {
                acknowledgements
                    .insert(reference.object.clone(), (reference.clone(), value.clone()));
            }
        }
        Ok(())
    }

    pub(crate) async fn load_store_ack(
        &self,
        reference: &StoreAckRef,
        registration: &StoreDeviceRegistration,
    ) -> Result<StoreAck, StoreObjectError> {
        let registration_matches = reference.registration.device_id == registration.device_id
            && reference.registration.registration_hash == registration.registration_hash();
        if registration_matches {
            if let Some(acknowledgement) = self.remembered_acknowledgement(reference) {
                return Ok(acknowledgement);
            }
        }
        let context = ProtocolObjectContext::signed_plaintext(
            self.root.reference().store_root_hash,
            ProtocolObjectDomain::StoreAck,
        );
        let semantic_prefix =
            ack_slot_prefix(&registration.device_id.to_string(), reference.sequence);
        let expected_root = self.root.reference().clone();
        let expected = reference.clone();
        let expected_registration = registration.clone();
        let acknowledgement = self
            .load_exact_object(
                &context,
                &reference.object,
                &semantic_prefix,
                reference.ack_hash,
                move |bytes| {
                    StoreAck::parse_at(bytes, &expected_root, &expected, &expected_registration)
                },
            )
            .await?;
        self.acknowledgements
            .lock()
            .expect("authenticated acknowledgement cache poisoned")
            .insert(
                reference.object.clone(),
                (reference.clone(), acknowledgement.value.clone()),
            );
        Ok(acknowledgement.value)
    }

    pub(crate) async fn predecessor_activates_acknowledgement(
        &mut self,
        order: &StoreCommitOrder,
        expected: &StoreAckRef,
        ack: &StoreAck,
    ) -> Result<bool, StorePullError> {
        let mut pending = order
            .predecessor
            .iter()
            .chain(order.dependencies.values())
            .cloned()
            .collect::<Vec<_>>();
        let mut visited = BTreeSet::new();
        while let Some(reference) = pending.pop() {
            if !visited.insert(reference.clone()) {
                continue;
            }
            let commit = self.load_ref(&reference).await?;
            if commit.value().acknowledgement() == Some(expected) {
                let predecessor_cut = commit
                    .value()
                    .order
                    .predecessor_cut()
                    .map_err(StorePullError::Protocol)?;
                return Ok(commit.value().author_registration == expected.registration
                    && ack.registration == expected.registration
                    && ack.store_cut == predecessor_cut
                    && ack.device_state == commit.value().device_state);
            }
            pending.extend(commit.value().order.predecessor.iter().cloned());
            pending.extend(commit.value().order.dependencies.values().cloned());
        }
        Ok(false)
    }

    pub(crate) async fn load_store_snapshot(
        &self,
        registration_ref: &StoreDeviceRegistrationRef,
        registration: &StoreDeviceRegistration,
        reference: &StoreSnapshotRef,
    ) -> Result<(StoreSnapshotRef, SnapshotMeta), StoreObjectError> {
        let prefix =
            snapshot_slot_prefix(&registration.device_id.to_string(), reference.generation);
        if registration_ref.device_id != registration.device_id {
            return Err(StoreObjectError::InvalidObject {
                semantic_prefix: prefix,
                key: reference.object.slot().logical_key().to_string(),
                source: Box::new(StoreProtocolError::Malformed(
                    "Store snapshot registration reference names another device".to_string(),
                )),
            });
        }
        let cached = self
            .snapshots
            .lock()
            .expect("authenticated snapshot cache poisoned")
            .get(reference)
            .cloned();
        if let Some(metadata) = cached {
            return Ok((reference.clone(), metadata));
        }
        let context = ProtocolObjectContext::signed_plaintext(
            self.root.reference().store_root_hash,
            ProtocolObjectDomain::StoreSnapshotMeta,
        );
        let expected_root = self.root.reference().clone();
        let expected_registration_ref = registration_ref.clone();
        let expected_registration = registration.clone();
        let expected_reference = reference.clone();
        let opened = self
            .load_exact_object(
                &context,
                &reference.object,
                &prefix,
                reference.snapshot_hash,
                move |bytes| {
                    SnapshotMeta::parse_stream_entry_at(
                        bytes,
                        &expected_root,
                        &expected_registration_ref,
                        &expected_registration,
                        &expected_reference,
                    )
                },
            )
            .await?;
        self.snapshots
            .lock()
            .expect("authenticated snapshot cache poisoned")
            .insert(reference.clone(), opened.value.clone());
        Ok((reference.clone(), opened.value))
    }

    /// Every snapshot slot the provider holds, by author device and generation.
    ///
    /// A snapshot stream is generation-linked, so the only way to enumerate one
    /// by following it is a read per generation the store has ever published —
    /// inside a device join, that is a walk of history to answer a question
    /// about its newest point. The slots name their own coordinates
    /// (`store-v1/snapshots/{device}/{generation}.json`), so one listing names
    /// every candidate there is.
    ///
    /// The listing decides what is worth reading and nothing else. Each slot it
    /// yields is authenticated against this Store's root, its author's
    /// registration, and the generation its own key claims, exactly as a stream
    /// walk authenticates it; a key this domain does not write is dropped here
    /// rather than read.
    pub(crate) async fn listed_store_snapshot_slots(
        &self,
    ) -> Result<
        BTreeMap<String, BTreeMap<u64, coven_protocol::objects::ObjectSlot>>,
        StoreObjectError,
    > {
        let context = ProtocolObjectContext::signed_plaintext(
            self.root.reference().store_root_hash,
            ProtocolObjectDomain::StoreSnapshotMeta,
        );
        let mut listed: BTreeMap<String, BTreeMap<u64, coven_protocol::objects::ObjectSlot>> =
            BTreeMap::new();
        for slot in self
            .storage
            .list_protocol_slots(&context, STORE_SNAPSHOT_LISTING_PREFIX)
            .await?
        {
            let Some((device_id, generation)) = listed_snapshot_coordinate(&slot) else {
                continue;
            };
            listed
                .entry(device_id)
                .or_default()
                .insert(generation, slot);
        }
        Ok(listed)
    }

    /// Authenticate one listed slot as `generation` of `registration`'s stream.
    ///
    /// `None` when the slot is not there: a listing is a picture of a moment,
    /// and a reader that finds nothing at a key it named has learned only that
    /// the picture was stale.
    ///
    /// This runs the identical checks the stream walk runs on the same bytes —
    /// `parse_stream_entry_at` binds the metadata to this Store's root, to its
    /// author's registration and signature, to the generation the key claims,
    /// and to the successor slot its own stream reserves. What it does not
    /// check is the walk's one further claim, that the object this metadata
    /// names as its predecessor is the one standing at the generation below:
    /// that is a statement about the *enumeration*, not about this snapshot,
    /// and nothing an installing device concludes rests on it.
    pub(crate) async fn load_listed_store_snapshot(
        &self,
        registration_ref: &StoreDeviceRegistrationRef,
        registration: &StoreDeviceRegistration,
        generation: u64,
        slot: &coven_protocol::objects::ObjectSlot,
    ) -> Result<Option<coven_database::PublishedStoreSnapshot>, StoreObjectError> {
        let context = ProtocolObjectContext::signed_plaintext(
            self.root.reference().store_root_hash,
            ProtocolObjectDomain::StoreSnapshotMeta,
        );
        let Some(semantic_prefix) = context.semantic_prefix_of(slot) else {
            return Ok(None);
        };
        let (bytes, object) = match self
            .storage
            .read_protocol_slot(&context, slot, semantic_prefix)
            .await
        {
            Ok(value) => value,
            Err(StorageError::NotFound(_)) => return Ok(None),
            Err(error) => return Err(StoreObjectError::from(error)),
        };
        self.verify_listed_store_snapshot(
            registration_ref,
            registration,
            generation,
            semantic_prefix,
            bytes,
            object,
        )
        .await
        .map(Some)
    }

    /// The checks [`load_listed_store_snapshot`](Self::load_listed_store_snapshot)
    /// runs, over bytes a caller already read.
    pub(crate) async fn verify_listed_store_snapshot(
        &self,
        registration_ref: &StoreDeviceRegistrationRef,
        registration: &StoreDeviceRegistration,
        generation: u64,
        semantic_prefix: &str,
        bytes: Vec<u8>,
        object: ExactObjectRef,
    ) -> Result<coven_database::PublishedStoreSnapshot, StoreObjectError> {
        let expected_root = self.root.reference().clone();
        let expected_registration_ref = registration_ref.clone();
        let expected_registration = registration.clone();
        let expected_object = object.clone();
        let (reference, meta) = run_blocking_object_verification(
            semantic_prefix,
            &object,
            Box::new(move || {
                let reference = StoreSnapshotRef {
                    generation,
                    snapshot_hash: SnapshotMeta::semantic_hash_from_bytes(&bytes)?,
                    object: expected_object,
                };
                let meta = SnapshotMeta::parse_stream_entry_at(
                    &bytes,
                    &expected_root,
                    &expected_registration_ref,
                    &expected_registration,
                    &reference,
                )?;
                Ok((reference, meta))
            }),
        )
        .await?;
        let successor_slot = meta.successor.next_slot.clone();
        Ok(coven_database::PublishedStoreSnapshot {
            reference,
            successor_slot,
            meta,
        })
    }

    /// The newest snapshot any device has published, found by listing the
    /// snapshot prefix instead of walking a device's snapshot stream.
    ///
    /// This is a hint and is treated as one. A snapshot stream is generation-
    /// linked, so a reader that has to *settle* which snapshot to install walks
    /// it — `load_store_snapshot_stream` does, and a bootstrap picking the
    /// image it will run on still does. But the membership rollup a snapshot
    /// names is content-addressed and re-verified object by object by the walk
    /// that consumes it, so nothing rests on picking the right snapshot here:
    /// the worst a wrong or stale pick can do is leave the anchored walk with
    /// fewer objects in hand and more to read.
    ///
    /// The listing is unsigned and gets no trust. It chooses one candidate; the
    /// candidate's own bytes are then authenticated against this Store's root
    /// and its author's registration exactly as a stream walk authenticates
    /// them, and a candidate that fails is simply not used.
    ///
    /// So the worst anyone who can write to the provider — a removed member
    /// whose credentials the owner has not revoked there yet — can do by
    /// planting a higher generation is cost a reader its rollup and send it
    /// back to walking the chain. That is the pace this had before the rollup
    /// existed, not a wrong answer, and it is the same reader who would
    /// otherwise be reading objects that writer could equally have deleted.
    pub(crate) async fn newest_listed_store_snapshot(
        &self,
    ) -> Result<Option<SnapshotMeta>, StoreObjectError> {
        let Some((device_id, generation, slot)) = self
            .listed_store_snapshot_slots()
            .await?
            .into_iter()
            .filter_map(|(device_id, generations)| {
                generations
                    .into_iter()
                    .next_back()
                    .map(|(generation, slot)| (device_id, generation, slot))
            })
            .max_by_key(|(_, generation, _)| *generation)
        else {
            return Ok(None);
        };
        let _ = device_id;
        let context = ProtocolObjectContext::signed_plaintext(
            self.root.reference().store_root_hash,
            ProtocolObjectDomain::StoreSnapshotMeta,
        );
        let Some(semantic_prefix) = context.semantic_prefix_of(&slot) else {
            return Ok(None);
        };
        let (bytes, object) = self
            .storage
            .read_protocol_slot(&context, &slot, semantic_prefix)
            .await?;
        let unverified: SnapshotMeta = coven_protocol::objects::decode_protocol_object(&bytes)
            .map_err(|source| StoreObjectError::InvalidObject {
                semantic_prefix: semantic_prefix.to_string(),
                key: slot.logical_key().to_string(),
                source: Box::new(source),
            })?;
        let registration = self
            .load_registration(&unverified.author_registration)
            .await?;
        let semantic_prefix = semantic_prefix.to_string();
        self.verify_listed_store_snapshot(
            &unverified.author_registration,
            &registration.value,
            generation,
            &semantic_prefix,
            bytes,
            object,
        )
        .await
        .map(|published| Some(published.meta))
    }

    pub(crate) async fn load_membership_rollup(
        &self,
        meta: &SnapshotMeta,
    ) -> Result<coven_protocol::store_commit::MembershipRollup, StoreObjectError> {
        let context = ProtocolObjectContext::signed_plaintext(
            self.root.reference().store_root_hash,
            ProtocolObjectDomain::StoreMembershipRollup,
        );
        let registration = self.load_registration(&meta.author_registration).await?;
        let prefix = coven_protocol::store_commit::membership_rollup_semantic_prefix(
            &registration.value.device_id.to_string(),
            meta.membership_rollup.rollup_hash,
        );
        let expected = meta.membership_rollup.clone();
        let object = expected.object.clone();
        let store_root_hash = self.root.reference().store_root_hash;
        let author = registration.value.clone();
        self.load_exact_object(
            &context,
            &object,
            &prefix,
            expected.rollup_hash,
            move |bytes| {
                coven_protocol::store_commit::MembershipRollup::parse_at(
                    bytes,
                    store_root_hash,
                    &expected,
                    &author,
                )
            },
        )
        .await
        .map(|opened| opened.value)
    }

    /// Adopt a device's own published snapshots as the walked prefix of its
    /// stream.
    ///
    /// The rows this device wrote when it published them, re-parsed and
    /// signature-checked against its registration on the way out of the
    /// database, so nothing enters here that reading the objects back would
    /// have refused. A row is written by the transaction that *completes* a
    /// publication — the one that retires the outbound claim — so it names a
    /// generation the provider accepted, not one this device meant to write.
    /// Without this, choosing which snapshot to acknowledge reads every
    /// generation the store has ever published, every time it is asked — a
    /// walk from generation zero over a stream this device is the author of.
    ///
    /// Refuses anything but a dense prefix from generation zero: the walk
    /// resumes at the length of what it holds, so a gap would make it re-read
    /// one generation as another.
    pub(crate) fn remember_published_snapshot_stream(
        &self,
        registration: &StoreDeviceRegistrationRef,
        snapshots: Vec<coven_database::PublishedStoreSnapshot>,
    ) -> Result<(), StoreProtocolError> {
        if snapshots
            .iter()
            .enumerate()
            .any(|(index, snapshot)| snapshot.reference.generation != index as u64)
        {
            return Err(StoreProtocolError::Malformed(
                "published Store snapshot stream is not dense from generation zero".to_string(),
            ));
        }
        let mut streams = self
            .snapshot_streams
            .lock()
            .expect("verified snapshot stream cache poisoned");
        let held = streams.entry(registration.clone()).or_default();
        // Whatever this verifier already walked wins: it read those objects.
        if held.len() < snapshots.len() {
            *held = snapshots;
        }
        Ok(())
    }

    pub(crate) async fn load_store_snapshot_stream(
        &self,
        registration_ref: &StoreDeviceRegistrationRef,
        registration: &StoreDeviceRegistration,
    ) -> Result<
        Vec<coven_database::PublishedStoreSnapshot>,
        crate::sync::store::snapshots::SnapshotError,
    > {
        let DeviceStreamAnchor::StoreSnapshots { first_slot } = &registration.snapshots else {
            return Err(
                crate::sync::store::snapshots::SnapshotError::PublicationState(
                    "local Store registration has no snapshot stream anchor".to_string(),
                ),
            );
        };
        let context = ProtocolObjectContext::signed_plaintext(
            self.root.reference().store_root_hash,
            ProtocolObjectDomain::StoreSnapshotMeta,
        );
        // Resume from what this verifier has already walked of this stream. The
        // walk below still probes on from the end, so a generation published
        // since the last walk is found; only the ones already read are skipped.
        let mut snapshots = self
            .snapshot_streams
            .lock()
            .expect("verified snapshot stream cache poisoned")
            .get(registration_ref)
            .cloned()
            .unwrap_or_default();
        let mut generation = snapshots.len() as u64;
        let mut predecessor = snapshots.last().map(|entry| entry.reference.clone());
        let mut slot = snapshots
            .last()
            .map_or_else(|| first_slot.clone(), |entry| entry.successor_slot.clone());
        loop {
            let prefix = snapshot_slot_prefix(&registration.device_id.to_string(), generation);
            let (bytes, object) = match self
                .storage
                .read_protocol_slot(&context, &slot, &prefix)
                .await
            {
                Ok(value) => value,
                Err(StorageError::NotFound(_)) => break,
                Err(error) => {
                    return Err(crate::sync::store::snapshots::SnapshotError::Bucket(error));
                }
            };
            let expected_root = self.root.reference().clone();
            let expected_registration_ref = registration_ref.clone();
            let expected_registration = registration.clone();
            let expected_object = object.clone();
            let (reference, meta) = run_blocking_object_verification(
                &prefix,
                &object,
                Box::new(move || {
                    let semantic_hash = SnapshotMeta::semantic_hash_from_bytes(&bytes)?;
                    let reference = StoreSnapshotRef {
                        generation,
                        snapshot_hash: semantic_hash,
                        object: expected_object,
                    };
                    let meta = SnapshotMeta::parse_stream_entry_at(
                        &bytes,
                        &expected_root,
                        &expected_registration_ref,
                        &expected_registration,
                        &reference,
                    )?;
                    Ok((reference, meta))
                }),
            )
            .await
            .map_err(crate::sync::store::snapshots::SnapshotError::StoreObject)?;
            if meta.predecessor != predecessor {
                return Err(crate::sync::store::snapshots::SnapshotError::Parse(
                    "Store snapshot stream has an invalid exact predecessor".to_string(),
                ));
            }
            let successor_slot = meta.successor.next_slot.clone();
            slot = successor_slot.clone();
            predecessor = Some(reference.clone());
            snapshots.push(coven_database::PublishedStoreSnapshot {
                reference,
                successor_slot,
                meta,
            });
            generation = generation.checked_add(1).ok_or_else(|| {
                crate::sync::store::snapshots::SnapshotError::Parse(
                    "Store snapshot generation overflow".to_string(),
                )
            })?;
        }
        self.snapshot_streams
            .lock()
            .expect("verified snapshot stream cache poisoned")
            .insert(registration_ref.clone(), snapshots.clone());
        Ok(snapshots)
    }

    pub(crate) async fn load_reclaim_authorization(
        &self,
        reference: &ReclaimAuthorizationRef,
    ) -> Result<VerifiedReclaimAuthorization, StoreObjectError> {
        let evidence_context = ProtocolObjectContext::store_encrypted(
            self.root.reference().store_root_hash,
            ProtocolObjectDomain::StoreReclaimEvidence,
        );
        let evidence_prefix = reclaim_evidence_semantic_prefix(reference.evidence.evidence_hash);
        let expected_evidence = reference.evidence.clone();
        let expected_store_root_hash = self.root.reference().store_root_hash;
        let evidence = self
            .load_exact_object(
                &evidence_context,
                &reference.evidence.object,
                &evidence_prefix,
                reference.evidence.evidence_hash,
                move |bytes| {
                    let evidence: ReclaimEvidence = decode_protocol_object(bytes)?;
                    expected_evidence.verify(&evidence)?;
                    verify_store_root(expected_store_root_hash, evidence.store_root_hash)?;
                    Ok(evidence)
                },
            )
            .await?;
        let authorization_context = ProtocolObjectContext::signed_plaintext(
            self.root.reference().store_root_hash,
            ProtocolObjectDomain::StoreReclaimAuthorization,
        );
        let authorization_prefix =
            reclaim_authorization_semantic_prefix(reference.authorization_hash);
        let owner_pubkey = evidence.value.author_pubkey.clone();
        let expected_authorization = reference.clone();
        let expected_store_root_hash = self.root.reference().store_root_hash;
        let authorization = self
            .load_exact_object(
                &authorization_context,
                &reference.object,
                &authorization_prefix,
                reference.authorization_hash,
                move |bytes| {
                    let authorization: ReclaimAuthorization = decode_protocol_object(bytes)?;
                    expected_authorization.verify(&authorization, &owner_pubkey)?;
                    verify_store_root(expected_store_root_hash, authorization.store_root_hash)?;
                    Ok(authorization)
                },
            )
            .await?;
        if authorization.value.target != evidence.value.claim.target() {
            return Err(StoreObjectError::InvalidObject {
                semantic_prefix: authorization_prefix,
                key: reference.object.slot().logical_key().to_string(),
                source: Box::new(StoreProtocolError::Malformed(
                    "reclaim authorization target differs from its exact evidence".to_string(),
                )),
            });
        }
        Ok(VerifiedReclaimAuthorization {
            authorization,
            evidence,
        })
    }

    pub(crate) async fn load_reclaim_receipt(
        &self,
        reference: &ReclaimReceiptRef,
    ) -> Result<VerifiedReclaimReceipt, StoreObjectError> {
        self.load_reclaim_authorization(&reference.authorization)
            .await?;
        let context = ProtocolObjectContext::signed_plaintext(
            self.root.reference().store_root_hash,
            ProtocolObjectDomain::StoreReclaimReceipt,
        );
        let prefix = reclaim_receipt_semantic_prefix(reference.receipt_hash);
        let bytes = self
            .storage
            .read_protocol_object(&context, &reference.object, &prefix)
            .await?;
        let unverified: ReclaimReceipt =
            serde_json::from_slice(&bytes).map_err(|error| StoreObjectError::InvalidObject {
                semantic_prefix: prefix.clone(),
                key: reference.object.slot().logical_key().to_string(),
                source: Box::new(StoreProtocolError::from(error)),
            })?;
        let executor = self.load_registration(&unverified.executor).await?.value;
        let receipt = reference
            .verify(&unverified, &executor)
            .and_then(|()| {
                verify_store_root(
                    self.root.reference().store_root_hash,
                    unverified.store_root_hash,
                )?;
                Ok(unverified)
            })
            .map_err(|source| StoreObjectError::InvalidObject {
                semantic_prefix: prefix,
                key: reference.object.slot().logical_key().to_string(),
                source: Box::new(source),
            })?;
        Ok(VerifiedReclaimReceipt {
            receipt: VerifiedObject {
                value: receipt,
                bytes,
                semantic_hash: reference.receipt_hash,
                object: reference.object.clone(),
            },
            executor,
        })
    }

    /// The acknowledgement one sequence below `successor`, from the cache when
    /// this verifier already holds it and from the provider otherwise.
    ///
    /// Consulting the cache here is what keeps a chain walk from re-reading the
    /// whole history: every ack a walk passes through is remembered, so a later
    /// walk over an overlapping prefix stops at the first entry it already has.
    pub(crate) async fn load_store_ack_predecessor(
        &self,
        successor_ref: &StoreAckRef,
        successor: &StoreAck,
        registration: &StoreDeviceRegistration,
    ) -> Result<Option<(StoreAckRef, StoreAck)>, StoreObjectError> {
        if successor.registration != successor_ref.registration
            || successor.sequence != successor_ref.sequence
        {
            return Err(StoreObjectError::InvalidObject {
                semantic_prefix: ack_slot_prefix(
                    &registration.device_id.to_string(),
                    successor_ref.sequence,
                ),
                key: successor_ref.object.slot().logical_key().to_string(),
                source: Box::new(StoreProtocolError::Malformed(
                    "Store acknowledgement differs from its exact reference".to_string(),
                )),
            });
        }
        let Some(object) = successor.successor.predecessor.as_ref() else {
            return Ok(None);
        };
        let sequence =
            successor
                .sequence
                .checked_sub(1)
                .ok_or_else(|| StoreObjectError::InvalidObject {
                    semantic_prefix: ack_slot_prefix(&registration.device_id.to_string(), 0),
                    key: object.slot().logical_key().to_string(),
                    source: Box::new(StoreProtocolError::InvalidAckSequence(0)),
                })?;
        let cached = self
            .acknowledgements
            .lock()
            .expect("authenticated acknowledgement cache poisoned")
            .get(object)
            .cloned();
        if let Some((reference, value)) = cached {
            if reference.registration != successor_ref.registration
                || reference.sequence != sequence
            {
                return Err(StoreObjectError::InvalidObject {
                    semantic_prefix: ack_slot_prefix(&registration.device_id.to_string(), sequence),
                    key: object.slot().logical_key().to_string(),
                    source: Box::new(StoreProtocolError::Malformed(
                        "remembered Store acknowledgement differs from its successor".to_string(),
                    )),
                });
            }
            return Ok(Some((reference, value)));
        }
        let context = ProtocolObjectContext::signed_plaintext(
            self.root.reference().store_root_hash,
            ProtocolObjectDomain::StoreAck,
        );
        let semantic_prefix = ack_slot_prefix(&registration.device_id.to_string(), sequence);
        let bytes = self
            .storage
            .read_protocol_object(&context, object, &semantic_prefix)
            .await?;
        let ack_hash = StoreAck::semantic_hash_from_bytes(&bytes).map_err(|source| {
            StoreObjectError::InvalidObject {
                semantic_prefix: semantic_prefix.clone(),
                key: object.slot().logical_key().to_string(),
                source: Box::new(source),
            }
        })?;
        let reference = StoreAckRef {
            registration: successor_ref.registration.clone(),
            sequence,
            ack_hash,
            object: object.clone(),
        };
        let value = StoreAck::parse_at(&bytes, self.root.reference(), &reference, registration)
            .map_err(|source| StoreObjectError::InvalidObject {
                semantic_prefix,
                key: object.slot().logical_key().to_string(),
                source: Box::new(source),
            })?;
        self.acknowledgements
            .lock()
            .expect("authenticated acknowledgement cache poisoned")
            .insert(reference.object.clone(), (reference.clone(), value.clone()));
        Ok(Some((reference, value)))
    }

    pub(crate) async fn load_owner_recovery_node(
        &self,
        reference: &OwnerRecoveryNodeRef,
    ) -> Result<VerifiedObject<OwnerRecoveryNode>, StoreObjectError> {
        let semantic_prefix = owner_recovery_semantic_prefix(
            &reference.owner_pubkey,
            reference.owner_grant.clone(),
            reference.sequence,
        );
        let context = ProtocolObjectContext::signed_plaintext(
            self.root.reference().store_root_hash,
            ProtocolObjectDomain::OwnerRecoveryNode,
        );
        let expected_root = self.root.reference().clone();
        let expected = reference.clone();
        self.load_exact_object(
            &context,
            &reference.object,
            &semantic_prefix,
            reference.node_hash,
            move |bytes| OwnerRecoveryNode::parse_at(bytes, &expected_root, &expected),
        )
        .await
    }

    pub(crate) async fn load_head(
        &self,
        reference: &StoreDeviceHeadRef,
        registration: &StoreDeviceRegistration,
        commit: &StoreBatchCommitRef,
    ) -> Result<VerifiedObject<StoreDeviceHead>, StoreObjectError> {
        let semantic_prefix =
            head_slot_prefix(&registration.device_id.to_string(), commit.coord.sequence());
        if let Some(head) = self
            .verified_heads
            .lock()
            .expect("verified Store device head cache mutex is not poisoned")
            .get(reference)
            .cloned()
        {
            head.value
                .author_registration
                .verify_registration(registration)
                .and_then(|()| {
                    if &head.value.commit == commit {
                        Ok(())
                    } else {
                        Err(StoreProtocolError::Malformed(
                            "Store head activates a different exact commit".to_string(),
                        ))
                    }
                })
                .map_err(|source| StoreObjectError::InvalidObject {
                    semantic_prefix,
                    key: reference.object.slot().logical_key().to_string(),
                    source: Box::new(source),
                })?;
            return Ok(head);
        }
        let context = ProtocolObjectContext::signed_plaintext(
            self.root.reference().store_root_hash,
            ProtocolObjectDomain::StoreHead,
        );
        let expected = reference.clone();
        let expected_registration = registration.clone();
        let expected_commit = commit.clone();
        let store_root_hash = self.root.reference().store_root_hash;
        let verified = self
            .load_exact_object(
                &context,
                &reference.object,
                &semantic_prefix,
                reference.head_hash,
                move |bytes| {
                    let head = StoreDeviceHead::parse_at(
                        bytes,
                        store_root_hash,
                        &expected_registration,
                        &expected_commit,
                    )?;
                    let actual = head.head_hash();
                    if actual != expected.head_hash {
                        return Err(StoreProtocolError::ObjectHashMismatch {
                            expected: expected.head_hash,
                            actual,
                        });
                    }
                    Ok(head)
                },
            )
            .await?;
        self.remember_verified_head(reference, verified)
            .map_err(|source| StoreObjectError::InvalidObject {
                semantic_prefix,
                key: reference.object.slot().logical_key().to_string(),
                source: Box::new(source),
            })
    }
}

/// The author device and generation a listed snapshot slot names, or `None`
/// for a key this domain does not write.
fn listed_snapshot_coordinate(slot: &coven_protocol::objects::ObjectSlot) -> Option<(String, u64)> {
    let (device_id, generation) = slot
        .logical_key()
        .strip_prefix(STORE_SNAPSHOT_LISTING_PREFIX)?
        .strip_suffix(".json")?
        .split_once('/')?;
    Some((device_id.to_string(), generation.parse().ok()?))
}

const STORE_SNAPSHOT_LISTING_PREFIX: &str = "store-v1/snapshots/";

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
                ));
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

    pub(crate) async fn load_snapshot_metadata(
        &self,
        reference: &StoreSnapshotRef,
    ) -> Result<SnapshotMeta, StoreObjectError> {
        if let Some(metadata) = self
            .snapshots
            .lock()
            .expect("authenticated snapshot cache poisoned")
            .get(reference)
            .cloned()
        {
            return Ok(metadata);
        }
        let prefix =
            semantic_prefix_from_exact_object(&reference.object, ".json").map_err(|source| {
                StoreObjectError::InvalidObject {
                    semantic_prefix: reference.object.slot().logical_key().to_string(),
                    key: reference.object.slot().logical_key().to_string(),
                    source: Box::new(source),
                }
            })?;
        let context = ProtocolObjectContext::signed_plaintext(
            self.root.reference().store_root_hash,
            ProtocolObjectDomain::StoreSnapshotMeta,
        );
        let bytes = self
            .storage
            .read_protocol_object(&context, &reference.object, &prefix)
            .await?;
        let unverified: SnapshotMeta =
            decode_protocol_object(&bytes).map_err(|source| StoreObjectError::InvalidObject {
                semantic_prefix: prefix,
                key: reference.object.slot().logical_key().to_string(),
                source: Box::new(source),
            })?;
        let author = self
            .load_registration(&unverified.author_registration)
            .await?;
        self.remember_exact_object(&reference.object, &bytes);
        self.load_store_snapshot(&unverified.author_registration, &author.value, reference)
            .await
            .map(|(_, metadata)| metadata)
    }

    pub(crate) async fn load_store_snapshot(
        &self,
        registration_ref: &StoreDeviceRegistrationRef,
        registration: &StoreDeviceRegistration,
        reference: &StoreSnapshotRef,
    ) -> Result<(StoreSnapshotRef, SnapshotMeta), StoreObjectError> {
        let prefix =
            semantic_prefix_from_exact_object(&reference.object, ".json").map_err(|source| {
                StoreObjectError::InvalidObject {
                    semantic_prefix: reference.object.slot().logical_key().to_string(),
                    key: reference.object.slot().logical_key().to_string(),
                    source: Box::new(source),
                }
            })?;
        if registration_ref.device_id != registration.device_id
            || registration_ref.registration_hash != registration.registration_hash()
        {
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
            if &metadata.author_registration != registration_ref {
                return Err(StoreObjectError::InvalidObject {
                    semantic_prefix: prefix,
                    key: reference.object.slot().logical_key().to_string(),
                    source: Box::new(StoreProtocolError::Malformed(
                        "Store snapshot names another exact author registration".to_string(),
                    )),
                });
            }
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
                    let metadata = SnapshotMeta::parse_at(
                        bytes,
                        expected_root.store_root_hash,
                        &expected_reference,
                        &expected_registration,
                    )?;
                    if metadata.author_registration != expected_registration_ref {
                        return Err(StoreProtocolError::Malformed(
                            "Store snapshot names another exact author registration".to_string(),
                        ));
                    }
                    Ok(metadata)
                },
            )
            .await?;
        self.snapshots
            .lock()
            .expect("authenticated snapshot cache poisoned")
            .insert(reference.clone(), opened.value.clone());
        Ok((reference.clone(), opened.value))
    }

    pub(crate) async fn load_store_snapshot_image(
        &self,
        reference: &StoreSnapshotRef,
        metadata: &SnapshotMeta,
    ) -> Result<Vec<u8>, StoreObjectError> {
        let prefix = coven_protocol::store_commit::snapshot_image_semantic_prefix(
            reference.object.slot(),
            metadata.image.image_hash,
        );
        reference
            .validate_artifact_slots(&metadata.image, &metadata.membership_rollup)
            .map_err(|source| StoreObjectError::InvalidObject {
                semantic_prefix: prefix.clone(),
                key: metadata.image.object.slot().logical_key().to_string(),
                source: Box::new(source),
            })?;
        let context = ProtocolObjectContext::store_encrypted(
            self.root.reference().store_root_hash,
            ProtocolObjectDomain::StoreSnapshotImage,
        );
        let bytes = self
            .storage
            .read_protocol_object(&context, &metadata.image.object, &prefix)
            .await?;
        let expected = metadata.image.image_hash;
        run_blocking_object_verification(
            &prefix,
            &metadata.image.object,
            Box::new(move || {
                let actual = ObjectHash::digest(&bytes);
                if actual != expected {
                    return Err(StoreProtocolError::ObjectHashMismatch { expected, actual });
                }
                Ok(bytes)
            }),
        )
        .await
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
        let prefix = coven_protocol::store_commit::semantic_prefix_from_exact_object(
            &meta.membership_rollup.object,
            ".json",
        )
        .map_err(|source| StoreObjectError::InvalidObject {
            semantic_prefix: "Store snapshot membership rollup".into(),
            key: meta.membership_rollup.object.slot().logical_key().into(),
            source: Box::new(source),
        })?;
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
        let authorization = self
            .load_reclaim_authorization_record(reference, &evidence.value.author_pubkey)
            .await?;
        if authorization.value.target != evidence.value.claim.target() {
            return Err(StoreObjectError::InvalidObject {
                semantic_prefix: reclaim_authorization_semantic_prefix(
                    reference.authorization_hash,
                ),
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

    /// Authenticate the public grant without opening encrypted reclaim evidence.
    /// Membership loading needs this before it can obtain the current Store key;
    /// the reclaim executor verifies the evidence before authorizing deletion.
    pub(crate) async fn load_reclaim_authorization_record(
        &self,
        reference: &ReclaimAuthorizationRef,
        owner_pubkey: &str,
    ) -> Result<VerifiedObject<ReclaimAuthorization>, StoreObjectError> {
        let authorization_context = ProtocolObjectContext::signed_plaintext(
            self.root.reference().store_root_hash,
            ProtocolObjectDomain::StoreReclaimAuthorization,
        );
        let authorization_prefix =
            reclaim_authorization_semantic_prefix(reference.authorization_hash);
        let owner_pubkey = owner_pubkey.to_string();
        let expected_authorization = reference.clone();
        let expected_store_root_hash = self.root.reference().store_root_hash;
        self.load_exact_object(
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
        .await
    }

    pub(crate) async fn load_reclaim_receipt(
        &self,
        reference: &ReclaimReceiptRef,
    ) -> Result<VerifiedReclaimReceipt, StoreObjectError> {
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
}

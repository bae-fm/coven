use super::*;

impl<'a> StoreCommitVerifier<'a> {
    pub(crate) async fn load_store_ack(
        &self,
        reference: &StoreAckRef,
        registration: &StoreDeviceRegistration,
    ) -> Result<VerifiedObject<StoreAck>, StoreObjectError> {
        let context = ProtocolObjectContext::signed_plaintext(
            self.root.reference().store_root_hash,
            ProtocolObjectDomain::StoreAck,
        );
        let semantic_prefix =
            ack_slot_prefix(&registration.device_id.to_string(), reference.sequence);
        let expected_root = self.root.reference().clone();
        let expected = reference.clone();
        let expected_registration = registration.clone();
        self.load_exact_object(
            &context,
            &reference.object,
            &semantic_prefix,
            reference.ack_hash,
            move |bytes| {
                StoreAck::parse_at(bytes, &expected_root, &expected, &expected_registration)
            },
        )
        .await
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
                    .map_err(|error| StoreProtocolError::Malformed(error.to_string()))
                },
            )
            .await?;
        Ok((reference.clone(), opened.value))
    }

    pub(crate) async fn load_store_snapshot_stream(
        &self,
        registration_ref: &StoreDeviceRegistrationRef,
        registration: &StoreDeviceRegistration,
    ) -> Result<
        Vec<crate::database::PublishedStoreSnapshot>,
        crate::sync::store::owner::writer::snapshot::SnapshotError,
    > {
        let mut slot = match &registration.snapshots {
            DeviceStreamAnchor::StoreSnapshots { first_slot } => first_slot.clone(),
            _ => {
                return Err(
                    crate::sync::store::owner::writer::snapshot::SnapshotError::PublicationState(
                        "local Store registration has no snapshot stream anchor".to_string(),
                    ),
                );
            }
        };
        let context = ProtocolObjectContext::signed_plaintext(
            self.root.reference().store_root_hash,
            ProtocolObjectDomain::StoreSnapshotMeta,
        );
        let mut generation = 0_u64;
        let mut predecessor = None;
        let mut snapshots = Vec::new();
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
                    return Err(
                        crate::sync::store::owner::writer::snapshot::SnapshotError::Bucket(error),
                    );
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
                    let semantic_hash = SnapshotMeta::semantic_hash_from_bytes(&bytes)
                        .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
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
                    )
                    .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
                    Ok((reference, meta))
                }),
            )
            .await
            .map_err(crate::sync::store::owner::writer::snapshot::SnapshotError::StoreObject)?;
            if meta.predecessor != predecessor {
                return Err(
                    crate::sync::store::owner::writer::snapshot::SnapshotError::Parse(
                        "Store snapshot stream has an invalid exact predecessor".to_string(),
                    ),
                );
            }
            let successor_slot = meta.successor.next_slot.clone();
            slot = successor_slot.clone();
            predecessor = Some(reference.clone());
            snapshots.push(crate::database::PublishedStoreSnapshot {
                reference,
                successor_slot,
                meta,
            });
            generation = generation.checked_add(1).ok_or_else(|| {
                crate::sync::store::owner::writer::snapshot::SnapshotError::Parse(
                    "Store snapshot generation overflow".to_string(),
                )
            })?;
        }
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
                source: Box::new(StoreProtocolError::Malformed(error.to_string())),
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

    pub(crate) async fn load_store_ack_predecessor(
        &self,
        successor_ref: &StoreAckRef,
        successor: &StoreAck,
        registration: &StoreDeviceRegistration,
    ) -> Result<Option<(StoreAckRef, VerifiedObject<StoreAck>)>, StoreObjectError> {
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
        Ok(Some((
            reference.clone(),
            VerifiedObject {
                value,
                bytes,
                semantic_hash: reference.ack_hash,
                object: reference.object.clone(),
            },
        )))
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
        let context = ProtocolObjectContext::signed_plaintext(
            self.root.reference().store_root_hash,
            ProtocolObjectDomain::StoreHead,
        );
        let expected = reference.clone();
        let expected_registration = registration.clone();
        let expected_commit = commit.clone();
        let store_root_hash = self.root.reference().store_root_hash;
        self.load_exact_object(
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
        .await
    }
}

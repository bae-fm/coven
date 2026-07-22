use super::*;

pub(crate) struct MergeStreamDiscovery {
    pub(crate) latest_head: Option<StoreDeviceHead>,
    pub(crate) commits: Vec<(
        super::store_commit::StoreDeviceHeadRef,
        StoreDeviceHead,
        StoreBatchCommitRef,
        StoreBatchCommit,
    )>,
    pub(crate) block: Option<MergeStreamBlock>,
}

pub(crate) enum MergeStreamBlock {
    Unauthenticated(HeldStorePosition),
    Authenticated(HeldStorePosition),
}

impl MergeStreamBlock {
    pub(crate) fn into_position(self) -> HeldStorePosition {
        match self {
            Self::Unauthenticated(position) | Self::Authenticated(position) => position,
        }
    }
}

pub(crate) async fn load_active_merge_registrations(
    db: &Database,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
) -> Result<Vec<(StoreDeviceRegistrationRef, StoreDeviceRegistration)>, StorePullError> {
    let durable = db.activated_store_device_registration_records().await?;
    let mut verified = Vec::with_capacity(durable.len());
    for (reference, expected) in durable {
        let opened = load_registration_ref(storage, root, &reference).await?;
        if opened.value != expected {
            return Err(StorePullError::Database(format!(
                "activated Store registration {} differs from its exact remote bytes",
                reference.device_id
            )));
        }
        if !matches!(
            opened.value.store_commits,
            StoreCommitAnchor::MergeConcurrent {
                announcements: DeviceStreamAnchor::StoreAnnouncements { .. }
            }
        ) {
            return Err(StorePullError::Database(format!(
                "activated Store registration {} has no Merge announcement anchor",
                reference.device_id
            )));
        }
        verified.push((reference, opened.value));
    }
    Ok(verified)
}

pub(crate) async fn discover_merge_owner_recoveries(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    protocol: &super::store_commit::StoreProtocolRoot,
    membership: &MembershipChain,
) -> Result<Vec<(StoreDeviceRegistrationRef, StoreDeviceRegistration)>, StorePullError> {
    if membership
        .active_owner_grant(&protocol.descriptor.founder_pubkey)
        .as_ref()
        != Some(&protocol.descriptor.founder_grant)
    {
        return Ok(Vec::new());
    }
    let super::store_commit::GrantStreamAnchor::OwnerRecovery { first_slot } =
        &protocol.descriptor.founder_recovery
    else {
        return Err(StorePullError::Database(
            "Store founder recovery authority has no recovery stream".into(),
        ));
    };
    let context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::OwnerRecoveryNode,
    );
    let authority = RegistrationPredecessorAuthority::MergeConcurrent(membership);
    let mut slot = first_slot.clone();
    let mut predecessor: Option<OwnerRecoveryNodeRef> = None;
    let mut sequence = 1_u64;
    let mut recovered = Vec::new();
    loop {
        let prefix = super::store_commit::owner_recovery_semantic_prefix(
            &protocol.descriptor.founder_pubkey,
            protocol.descriptor.founder_grant.clone(),
            sequence,
        );
        let (bytes, object) = match storage.read_protocol_slot(&context, &slot, &prefix).await {
            Ok(opened) => opened,
            Err(StorageError::NotFound(_)) => break,
            Err(error) => return Err(StoreObjectError::Storage(error).into()),
        };
        let unverified: OwnerRecoveryNode = serde_json::from_slice(&bytes)
            .map_err(|error| StorePullError::Database(format!("Owner recovery node: {error}")))?;
        let reference = OwnerRecoveryNodeRef {
            owner_pubkey: unverified.owner_pubkey.clone(),
            owner_grant: unverified.owner_grant.clone(),
            sequence: unverified.sequence,
            node_hash: unverified.node_hash(),
            object,
        };
        let node = OwnerRecoveryNode::parse_at(&bytes, root, &reference)
            .map_err(|error| StorePullError::Database(error.to_string()))?;
        if reference.owner_pubkey != protocol.descriptor.founder_pubkey
            || reference.owner_grant != protocol.descriptor.founder_grant
            || reference.sequence != sequence
            || node.predecessor != predecessor
            || !authority.verifies_owner(&node.membership, &node.owner_pubkey, &node.owner_grant)
        {
            return Err(StorePullError::Database(
                "Owner recovery stream differs from its root-anchored authority".into(),
            ));
        }
        let registration = load_registration_ref(storage, root, &node.readiness.registration)
            .await?
            .value;
        let initial_ack =
            load_store_ack_ref(storage, root, &node.readiness.initial_ack, &registration)
                .await?
                .value;
        let origin_matches = matches!(
            &registration.origin,
            StoreDeviceRegistrationOrigin::Recovery {
                recovery_id,
                recovery_slot,
                owner_grant,
            } if *recovery_id == node.recovery_id
                && recovery_slot == reference.slot()
                && owner_grant == &node.owner_grant
        );
        if !origin_matches
            || registration.author_pubkey != node.owner_pubkey
            || initial_ack.sequence != 1
            || initial_ack.successor.predecessor.is_some()
            || initial_ack.store_cut != node.readiness.bootstrap_cut
            || initial_ack.registration != node.readiness.registration
        {
            return Err(StorePullError::Database(
                "Owner recovery readiness differs from its registration graph".into(),
            ));
        }
        recovered.push((node.readiness.registration.clone(), registration));
        slot = node.next_slot.clone();
        predecessor = Some(reference);
        sequence = sequence
            .checked_add(1)
            .ok_or_else(|| StorePullError::Database("Owner recovery sequence overflow".into()))?;
    }
    Ok(recovered)
}

pub(crate) async fn discover_merge_stream(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    registration_ref: &StoreDeviceRegistrationRef,
    registration: &StoreDeviceRegistration,
    inactive_accepted_cut: Option<&StoreHistoryCut>,
) -> Result<MergeStreamDiscovery, StorePullError> {
    let StoreCommitAnchor::MergeConcurrent {
        announcements: DeviceStreamAnchor::StoreAnnouncements { first_slot },
    } = &registration.store_commits
    else {
        return Err(StorePullError::Database(format!(
            "Store registration {} has no Merge announcement anchor",
            registration.device_id
        )));
    };
    let stream_id = super::store_commit::StreamActivation::device_authorized_stream_id(
        root.store_root_hash,
        registration_ref,
        super::store_commit::StreamAnchorDomain::StoreAnnouncements,
    );
    let maximum_sequence = match inactive_accepted_cut {
        None => None,
        Some(StoreHistoryCut::MergeConcurrent(cut)) => Some(
            cut.get(&stream_id)
                .map_or(0, |reference| reference.coord.sequence()),
        ),
        Some(StoreHistoryCut::Serial(_)) => {
            return Err(StorePullError::Database(
                "Merge device state carries a Serial inactive cutoff".to_string(),
            ));
        }
    };
    let activation = registration
        .store_announcement_activation(registration_ref)
        .map_err(|error| StorePullError::Database(error.to_string()))?
        .activation_id();
    let context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreHead,
    );
    let mut slot = first_slot.clone();
    let mut predecessor = None;
    let mut sequence = 1_u64;
    let mut latest_head = None;
    let mut commits = Vec::new();
    let mut block = None;
    let mut visited = BTreeSet::new();

    loop {
        if maximum_sequence.is_some_and(|maximum| sequence > maximum) {
            break;
        }
        if !visited.insert(slot.clone()) {
            return Err(StorePullError::Database(format!(
                "Store announcement stream {stream_id} repeats a reserved slot"
            )));
        }
        let semantic_prefix = head_slot_prefix(&registration.device_id.to_string(), sequence);
        let (bytes, object) = match storage
            .read_protocol_slot(&context, &slot, &semantic_prefix)
            .await
        {
            Ok(opened) => opened,
            Err(StorageError::NotFound(_)) => break,
            Err(error) => return Err(StoreObjectError::Storage(error).into()),
        };
        let unverified: StoreDeviceHead = match serde_json::from_slice(&bytes) {
            Ok(head) => head,
            Err(error) => {
                block = Some(MergeStreamBlock::Unauthenticated(HeldStorePosition {
                    coordinate: HeldStoreCoordinate::Head {
                        device_id: stream_id.to_string(),
                        seq: sequence,
                        head_hash: ObjectHash::digest(&bytes),
                    },
                    reason: HeldStorePositionReason::InvalidObject(error.to_string()),
                }));
                break;
            }
        };
        let authenticated = unverified.signature_is_valid_for(registration);
        let coord_matches = matches!(
            unverified.commit.coord,
            StoreCommitCoord::MergeConcurrent {
                stream_id: declared,
                sequence: declared_sequence,
            } if declared == stream_id && declared_sequence == sequence
        );
        if !coord_matches
            || unverified.author_registration != *registration_ref
            || unverified.successor.activation != activation
            || unverified.successor.predecessor != predecessor
        {
            let position = HeldStorePosition {
                coordinate: HeldStoreCoordinate::Head {
                    device_id: stream_id.to_string(),
                    seq: sequence,
                    head_hash: unverified.head_hash(),
                },
                reason: HeldStorePositionReason::WrongSlot(
                    "Store head differs from its activated successor chain".to_string(),
                ),
            };
            block = Some(if authenticated {
                MergeStreamBlock::Authenticated(position)
            } else {
                MergeStreamBlock::Unauthenticated(position)
            });
            break;
        }
        let head = match StoreDeviceHead::parse_at(
            &bytes,
            root.store_root_hash,
            registration,
            &unverified.commit,
        ) {
            Ok(head) => head,
            Err(error) => {
                let position = HeldStorePosition {
                    coordinate: HeldStoreCoordinate::Head {
                        device_id: stream_id.to_string(),
                        seq: sequence,
                        head_hash: unverified.head_hash(),
                    },
                    reason: held_protocol_error(error),
                };
                block = Some(if authenticated {
                    MergeStreamBlock::Authenticated(position)
                } else {
                    MergeStreamBlock::Unauthenticated(position)
                });
                break;
            }
        };
        let commit = match load_commit_ref(
            storage,
            root.store_root_hash,
            &unverified.commit,
            registration,
        )
        .await
        {
            Ok(commit) => commit,
            Err(error) => {
                block = Some(MergeStreamBlock::Authenticated(held_commit(
                    &unverified.commit,
                    held_object_error(error),
                )));
                break;
            }
        };
        let next_slot = head.successor.next_slot.clone();
        let head_ref = super::store_commit::StoreDeviceHeadRef {
            head_hash: head.head_hash(),
            object: object.clone(),
        };
        predecessor = Some(object);
        sequence = sequence.checked_add(1).ok_or_else(|| {
            StorePullError::Database(format!(
                "Store announcement stream {stream_id} sequence overflow"
            ))
        })?;
        commits.push((head_ref, head.clone(), head.commit.clone(), commit.value));
        latest_head = Some(head);
        slot = next_slot;
    }

    Ok(MergeStreamDiscovery {
        latest_head,
        commits,
        block,
    })
}

fn held_protocol_error(error: StoreProtocolError) -> HeldStorePositionReason {
    match error {
        StoreProtocolError::InvalidSignature => HeldStorePositionReason::InvalidSignature,
        StoreProtocolError::RelocatedSlot { .. }
        | StoreProtocolError::RelocatedPackage { .. }
        | StoreProtocolError::StoreRootMismatch { .. }
        | StoreProtocolError::StoreMismatch { .. }
        | StoreProtocolError::FounderMismatch { .. } => {
            HeldStorePositionReason::WrongSlot(error.to_string())
        }
        error => HeldStorePositionReason::InvalidObject(error.to_string()),
    }
}

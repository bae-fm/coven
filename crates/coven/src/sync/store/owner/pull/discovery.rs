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

pub(crate) async fn discover_merge_stream(
    history_verifier: &mut MergeHistoryVerifier<'_>,
    registration_ref: &StoreDeviceRegistrationRef,
    registration: &StoreDeviceRegistration,
    inactive_accepted_cut: Option<&StoreHistoryCut>,
) -> Result<MergeStreamDiscovery, StorePullError> {
    let storage = history_verifier.storage();
    let root = history_verifier.root().clone();
    let DeviceStreamAnchor::StoreAnnouncements { first_slot } = &registration.store_commits else {
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
    let maximum_sequence = inactive_accepted_cut.map(|cut| {
        cut.0
            .get(&stream_id)
            .map_or(0, |reference| reference.coord.sequence())
    });
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
        let coord_matches = unverified.commit.coord.stream_id == stream_id
            && unverified.commit.coord.sequence == sequence;
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
        let commit = match history_verifier.load_ref(&unverified.commit).await {
            Ok(verified)
                if verified.value().author_registration == *registration_ref
                    && verified.author() == registration =>
            {
                verified.value().clone()
            }
            Ok(_) => {
                block = Some(MergeStreamBlock::Authenticated(HeldStorePosition::commit(
                    &unverified.commit,
                    HeldStorePositionReason::Unauthorized,
                )));
                break;
            }
            Err(error) => {
                let reason = match error {
                    StorePullError::Object(error) => held_object_error(error),
                    error => HeldStorePositionReason::InvalidObject(error.to_string()),
                };
                block = Some(MergeStreamBlock::Authenticated(HeldStorePosition::commit(
                    &unverified.commit,
                    reason,
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
        commits.push((head_ref, head.clone(), head.commit.clone(), commit));
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

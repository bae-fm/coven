use super::*;

pub(crate) async fn exact_next_announcement_slot(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    registration_ref: &StoreDeviceRegistrationRef,
    registration: &StoreDeviceRegistration,
    previous: Option<&StoreBatchCommitRef>,
) -> Result<
    (
        crate::storage::cloud::ObjectSlot,
        Option<StoreDeviceHeadRef>,
    ),
    StoreOutboundError,
> {
    exact_next_announcement_slot_impl(
        storage,
        root,
        registration_ref,
        registration,
        previous,
        false,
    )
    .await
}

pub(crate) async fn exact_next_announcement_slot_for_verified_commit(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    registration_ref: &StoreDeviceRegistrationRef,
    registration: &StoreDeviceRegistration,
    reference: &StoreBatchCommitRef,
    commit: &StoreBatchCommit,
) -> Result<
    (
        crate::storage::cloud::ObjectSlot,
        Option<StoreDeviceHeadRef>,
    ),
    StoreOutboundError,
> {
    reference
        .verify_commit(commit)
        .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    if commit.author_registration != *registration_ref {
        return Err(StoreOutboundError::InvalidOutbound(
            "verified Store commit author differs from its announcement registration".to_string(),
        ));
    }
    exact_next_announcement_slot_impl(
        storage,
        root,
        registration_ref,
        registration,
        Some(reference),
        true,
    )
    .await
}

async fn exact_next_announcement_slot_impl(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    registration_ref: &StoreDeviceRegistrationRef,
    registration: &StoreDeviceRegistration,
    previous: Option<&StoreBatchCommitRef>,
    target_is_verified: bool,
) -> Result<
    (
        crate::storage::cloud::ObjectSlot,
        Option<StoreDeviceHeadRef>,
    ),
    StoreOutboundError,
> {
    let super::store_commit::StoreCommitAnchor::MergeConcurrent {
        announcements: super::store_commit::DeviceStreamAnchor::StoreAnnouncements { first_slot },
    } = &registration.store_commits
    else {
        return Err(StoreOutboundError::InvalidOutbound(
            "Merge registration has no Store announcement anchor".to_string(),
        ));
    };
    let Some(target) = previous else {
        return Ok((first_slot.clone(), None));
    };
    let expected_stream = super::store_commit::StreamActivation::device_authorized_stream_id(
        root.store_root_hash,
        registration_ref,
        super::store_commit::StreamAnchorDomain::StoreAnnouncements,
    );
    if !matches!(
        target.coord,
        StoreCommitCoord::MergeConcurrent { stream_id, .. } if stream_id == expected_stream
    ) {
        return Err(StoreOutboundError::InvalidOutbound(
            "local predecessor belongs to another Store announcement stream".to_string(),
        ));
    }
    let activation = registration
        .store_announcement_activation(registration_ref)
        .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?
        .activation_id();
    let context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreHead,
    );
    let mut slot = first_slot.clone();
    let mut predecessor: Option<StoreDeviceHeadRef> = None;
    for sequence in 1..=target.coord.sequence() {
        let prefix = head_slot_prefix(&registration.device_id.to_string(), sequence);
        let (bytes, object) = storage
            .read_protocol_slot(&context, &slot, &prefix)
            .await
            .map_err(StoreObjectError::from)?;
        let verify_bytes = bytes.clone();
        let expected_registration = registration_ref.clone();
        let expected_registration_value = registration.clone();
        let store_root_hash = root.store_root_hash;
        let expected_predecessor = predecessor
            .as_ref()
            .map(|reference| reference.object.clone());
        let head = run_blocking_object_verification(
            &prefix,
            &object,
            Box::new(move || {
                let unverified: StoreDeviceHead = serde_json::from_slice(&verify_bytes)
                    .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
                if unverified.author_registration != expected_registration
                    || unverified.successor.activation != activation
                    || unverified.successor.predecessor != expected_predecessor
                {
                    return Err(StoreProtocolError::Malformed(format!(
                        "local Store head {sequence} does not extend its exact activated predecessor"
                    )));
                }
                StoreDeviceHead::parse_at(
                    &verify_bytes,
                    store_root_hash,
                    &expected_registration_value,
                    &unverified.commit,
                )
            }),
        )
        .await?;
        let is_target = sequence == target.coord.sequence();
        if is_target && head.commit != *target {
            return Err(StoreOutboundError::MergeAnnouncementOccupied {
                expected: Box::new(target.clone()),
                actual: Box::new(head.commit),
            });
        }
        if !is_target || !target_is_verified {
            super::store_objects::load_commit_ref(
                storage,
                root.store_root_hash,
                &head.commit,
                registration,
            )
            .await?;
        }
        let reference = StoreDeviceHeadRef {
            head_hash: head.head_hash(),
            object,
        };
        if is_target {
            return Ok((head.successor.next_slot, Some(reference)));
        }
        slot = head.successor.next_slot;
        predecessor = Some(reference);
    }
    Err(StoreOutboundError::InvalidOutbound(
        "local Store predecessor traversal ended early".to_string(),
    ))
}

pub(crate) async fn reject_excluded_merge_candidate(
    db: &Database,
    candidate: &StoreBatchCommitRef,
    author: &StoreDeviceRegistrationRef,
) -> Result<(), StoreOutboundError> {
    if db
        .author_exclusion_activation_for_candidate(candidate.clone(), author.clone())
        .await?
        .is_some()
    {
        return Err(StoreOutboundError::AuthorExcluded {
            device_id: author.device_id,
        });
    }
    Ok(())
}

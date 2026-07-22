use super::*;

/// Prepare the oldest pending write as exact signed bytes. A blocked or already
/// prepared oldest write holds later writes behind it.
#[allow(clippy::too_many_arguments)]
#[cfg(any(test, feature = "test-utils"))]
pub async fn prepare_pending_store_write(
    db: &Database,
    storage: &dyn SyncStorage,
    device_id: &str,
    timestamp: &str,
    keypair: &UserKeypair,
    store_dir: &StoreDir,
    membership: Option<&MembershipChain>,
) -> Result<bool, StoreOutboundError> {
    let membership = membership.ok_or_else(|| {
        StoreOutboundError::InvalidOutbound(
            "Merge Store write has no exact membership state".to_string(),
        )
    })?;
    prepare_pending_merge_store_write(
        db, storage, device_id, timestamp, keypair, store_dir, membership,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
#[cfg(test)]
pub(crate) async fn prepare_pending_store_write_with_coordination(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn CoordinationStorage>,
    device_id: &str,
    _timestamp: &str,
    keypair: &UserKeypair,
    store_dir: &StoreDir,
    membership: Option<&MembershipChain>,
) -> Result<bool, StoreOutboundError> {
    if db.write_policy() == crate::WritePolicy::Serial {
        return prepare_pending_serial_store_write(
            db,
            storage,
            coordination.ok_or(StoreOutboundError::MissingSerialCoordination)?,
            device_id,
            keypair,
            store_dir,
        )
        .await;
    }
    let membership = membership.ok_or_else(|| {
        StoreOutboundError::InvalidOutbound(
            "Merge Store write has no exact membership state".to_string(),
        )
    })?;
    prepare_pending_merge_store_write(
        db, storage, device_id, _timestamp, keypair, store_dir, membership,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn prepare_pending_serial_store_write(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: &dyn CoordinationStorage,
    device_id: &str,
    keypair: &UserKeypair,
    store_dir: &StoreDir,
) -> Result<bool, StoreOutboundError> {
    crate::sync::store_engine::serial::publication::prepare_serial_store_branch(
        db,
        storage,
        coordination,
        device_id,
        keypair,
        store_dir,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn prepare_pending_merge_store_write(
    db: &Database,
    storage: &dyn SyncStorage,
    device_id: &str,
    _timestamp: &str,
    keypair: &UserKeypair,
    store_dir: &StoreDir,
    membership: &MembershipChain,
) -> Result<bool, StoreOutboundError> {
    let Some(PreparedStoreWrite {
        write_id,
        changeset,
        inverse_changeset,
        base,
        blob_facts,
        partitions,
    }) = db.prepare_store_write().await?
    else {
        return Ok(false);
    };
    if !changeset.is_empty() && inverse_changeset.is_empty() {
        return Err(StoreOutboundError::InvalidOutbound(
            "shared Store write has no inverse changeset".to_string(),
        ));
    }
    let dependencies = match base {
        StoreWriteBase::MergeConcurrent { dependencies } => {
            super::store_commit::CommitFrontier::from_refs(
                crate::WritePolicy::MergeConcurrent,
                dependencies,
            )
            .and_then(|frontier| frontier.merge_commits().cloned())
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?
        }
        StoreWriteBase::Serial { .. } => {
            return Err(StoreOutboundError::InvalidOutbound(
                "serial Store write reached MergeConcurrent preparation".to_string(),
            ));
        }
    };
    let preparation = async {
        let (root, registration_ref, registration, device_signer) =
            load_local_store_authority(db, device_id, keypair).await?;
        let blob_write_authority = BlobWriteAuthority::new(&registration_ref, &registration)
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
        let store_root_hash = root.store_root_hash;
        let stream_id = super::store_commit::StreamActivation::device_authorized_stream_id(
            root.store_root_hash,
            &registration_ref,
            super::store_commit::StreamAnchorDomain::StoreAnnouncements,
        );
        let previous = db.latest_local_store_position().await?;
        let seq = next_store_sequence(previous.as_ref())?;
        let coord = StoreCommitCoord::MergeConcurrent {
            stream_id,
            sequence: seq,
        };
        let order = StoreCommitOrder::MergeConcurrent {
            seq,
            predecessor: previous.clone(),
            dependencies,
        };
        let candidate_membership = membership;
        let authorization = super::store_pull::load_retained_merge_outbound_authorization(
            db,
            storage,
            &root,
            &order,
            candidate_membership.head_refs(),
            &registration_ref,
        )
        .await
        .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
        let payload = super::service::prepare_store_payload(
            &blob_facts,
            keypair,
            store_dir,
            super::service::StorePayloadMembership::MergeConcurrent(&authorization.membership),
        )
        .await
        .map_err(StoreOutboundError::Preparation)?;
        let membership_state = authorization.membership_state;
        let device_state = authorization.device_state_ref;
        let candidate_family =
            CandidateFamilyId::derive(store_root_hash, &registration_ref, &write_id, &order);
        let mut prepared_packages = Vec::new();
        if let Some(partition) = partitions.store {
            prepared_packages.push(
                prepare_partition_package(
                    db,
                    storage,
                    store_root_hash,
                    candidate_family,
                    &write_id,
                    &coord,
                    db.schema_version(),
                    stream_id.to_string(),
                    seq,
                    partition,
                    &blob_facts,
                    &blob_write_authority,
                    store_dir,
                )
                .await?,
            );
        }
        for partition in partitions.circles {
            prepared_packages.push(
                prepare_partition_package(
                    db,
                    storage,
                    store_root_hash,
                    candidate_family,
                    &write_id,
                    &coord,
                    db.schema_version(),
                    stream_id.to_string(),
                    seq,
                    partition,
                    &blob_facts,
                    &blob_write_authority,
                    store_dir,
                )
                .await?,
            );
        }
        let commit_context = ProtocolObjectContext::signed_plaintext(
            store_root_hash,
            ProtocolObjectDomain::StoreCommit,
        );
        let head_context = ProtocolObjectContext::signed_plaintext(
            store_root_hash,
            ProtocolObjectDomain::StoreHead,
        );
        let device_id = registration_ref.device_id.to_string();
        let head_prefix = head_slot_prefix(&device_id, seq);
        let next_head_prefix = head_slot_prefix(&device_id, successor_store_sequence(seq)?);
        let next_head_slot = storage
            .allocate_protocol_slot(&head_context, &next_head_prefix, ".json")
            .await
            .map_err(StoreObjectError::from)?;

        let store_package = prepared_packages
            .iter()
            .find(|package| package.audience == super::circle::Audience::Store)
            .map(|package| StorePackageInput {
                candidate_family,
                schema_version: db.schema_version(),
                bytes: package.semantic_bytes.as_slice(),
                object: package.prepared.reference().clone(),
            });
        let circle_packages = prepared_packages
            .iter()
            .filter_map(|package| {
                let super::circle::Audience::Circle(circle_id) = package.audience else {
                    return None;
                };
                let control = package
                    .control
                    .as_ref()
                    .expect("Circle partition carries exact control");
                Some(CirclePackageInput {
                    circle_id,
                    control: control.coordinate().clone(),
                    key_fingerprint: package
                        .key_fingerprint
                        .expect("Circle partition carries exact key fingerprint"),
                    package: StorePackageInput {
                        candidate_family,
                        schema_version: db.schema_version(),
                        bytes: package.semantic_bytes.as_slice(),
                        object: package.prepared.reference().clone(),
                    },
                })
            })
            .collect::<Vec<_>>();
        let commit = StoreBatchCommit::signed_operations(
            store_root_hash,
            write_id.clone(),
            coord.clone(),
            registration_ref.clone(),
            &registration,
            order,
            membership_state,
            device_state,
            payload.membership_authority,
            StoreCommitOperationsInput {
                acknowledgement: None,
                control: None,
                device_join_attempt_decisions: Vec::new(),
                device_join_outcomes: Vec::new(),
                device_join_cleanup_receipts: Vec::new(),
                provider_access_grants: Vec::new(),
                provider_access_withdrawals: Vec::new(),
                device_registrations: Vec::new(),
                device_exclusion_proposals: Vec::new(),
                device_exclusion_outcomes: Vec::new(),
                stream_activations: Vec::new(),
                circle_controls: Vec::new(),
                store_package,
                circle_packages: &circle_packages,
            },
            &device_signer,
        )
        .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
        let commit_prefix = commit_semantic_prefix(
            commit.candidate_family(),
            &stream_id.to_string(),
            seq,
            commit.commit_hash(),
        );
        let commit_slot = storage
            .allocate_protocol_slot(&commit_context, &commit_prefix, ".json")
            .await
            .map_err(StoreObjectError::from)?;
        let commit_prepared = storage
            .prepare_protocol_object(
                &commit_context,
                commit_slot,
                &commit_prefix,
                commit.to_bytes(),
            )
            .map_err(StoreObjectError::from)?;
        let commit_ref =
            StoreBatchCommitRef::from_commit(&commit, coord, commit_prepared.reference().clone())
                .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
        let successor = super::store_pull::prepare_merge_history_successor(
            db,
            &root,
            &commit,
            &commit_ref,
            &authorization.membership,
            &registration,
            None,
            authorization.device_state.clone(),
            super::store_pull::MergeHistorySuccessorEvidence::none(),
        )
        .await
        .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
        let activation = registration
            .store_announcement_activation(&registration_ref)
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?
            .activation_id();
        let head = StoreDeviceHead::signed(
            store_root_hash,
            registration_ref,
            commit_ref.clone(),
            successor.summary.digest(),
            SuccessorLink {
                activation,
                predecessor: successor.predecessor_head.map(|reference| reference.object),
                next_slot: next_head_slot,
            },
            &device_signer,
        )
        .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
        let head_prepared = storage
            .prepare_protocol_object(
                &head_context,
                successor.head_slot,
                &head_prefix,
                head.to_bytes(),
            )
            .map_err(StoreObjectError::from)?;
        let (remote_objects, audience_objects) =
            close_prepared_packages(prepared_packages, &commit, &commit_ref)?;
        let local_cleanup =
            super::service::bind_local_cleanup(payload.local_cleanup, &audience_objects.blobs)
                .map_err(StoreOutboundError::Preparation)?;
        Ok::<_, StoreOutboundError>(StoreWritePreparation {
            write_id: write_id.clone(),
            remote_objects,
            audiences: audience_objects,
            commit: PreparedProtocolObject {
                value: commit,
                prepared: commit_prepared,
            },
            head: PreparedProtocolObject {
                value: head,
                prepared: head_prepared,
            },
            history_summary: successor.summary,
            local_cleanup,
            completion: payload.completion,
        })
    }
    .await;
    let preparation = match preparation {
        Ok(preparation) => preparation,
        Err(error) => {
            record_preparation_failure(db, &write_id, &error).await?;
            return Err(error);
        }
    };
    db.prepare_store_write_commit(preparation).await?;
    Ok(true)
}

pub(crate) async fn load_local_store_authority(
    db: &Database,
    expected_device_id: &str,
    identity_signer: &UserKeypair,
) -> Result<
    (
        super::store_commit::StoreRootRef,
        StoreDeviceRegistrationRef,
        StoreDeviceRegistration,
        UserKeypair,
    ),
    StoreOutboundError,
> {
    let root = db
        .local_store_root_ref()
        .await?
        .ok_or(StoreOutboundError::MissingState {
            key: STORE_ROOT_AUTHORITY,
        })?;
    let durable = db.latest_local_store_device_registration().await?.ok_or(
        StoreOutboundError::MissingState {
            key: crate::database::LOCAL_DEVICE_ID_STATE_KEY,
        },
    )?;
    if !durable.is_activated() || durable.device_id.to_string() != expected_device_id {
        return Err(StoreOutboundError::InvalidState {
            key: crate::database::LOCAL_DEVICE_ID_STATE_KEY,
            reason: "local Store device registration is not the activated writer".to_string(),
        });
    }
    let registration =
        StoreDeviceRegistration::parse_at(&durable.registration_bytes, &root, durable.device_id)
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    if registration.registration_hash() != durable.registration_hash {
        return Err(StoreOutboundError::InvalidOutbound(
            "local Store device registration differs from its durable hash".to_string(),
        ));
    }
    let reference = StoreDeviceRegistrationRef::from_registration(
        &registration,
        durable.prepared.reference().clone(),
    );
    let activated = db
        .activated_store_device_registration(reference.clone())
        .await?;
    if activated != registration {
        return Err(StoreOutboundError::InvalidOutbound(
            "local Store writer differs from its activated exact registration".to_string(),
        ));
    }
    let device_signer = registration
        .device_signer(identity_signer)
        .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    Ok((root, reference, registration, device_signer))
}

//! Exact-reference access to Store protocol objects.

use super::membership::{
    AuthorHead, MembershipEntry, MembershipEntryRef, MembershipHeadRef,
    StoreMembershipConflictResolution, StoreMembershipConflictResolutionRef,
};
use super::storage::{
    ExactObjectRef, PreparedExactObject, ProtocolObjectContext, ProtocolObjectDomain, StorageError,
    SyncStorage,
};
use super::store_commit::{
    ack_slot_prefix, commit_semantic_prefix, device_join_attempt_semantic_prefix,
    device_join_outcome_semantic_prefix, founder_registration_semantic_prefix, head_slot_prefix,
    membership_entry_semantic_prefix, membership_resolution_semantic_prefix,
    package_semantic_prefix, provider_access_grant_semantic_prefix,
    provider_access_withdrawal_semantic_prefix, registration_semantic_prefix,
    store_protocol_root_logical_key, DeviceJoinAttempt, DeviceJoinAttemptRef, DeviceJoinOutcome,
    DeviceJoinOutcomeRef, ObjectHash, OwnerRecoveryNode, OwnerRecoveryNodeRef, StoreAck,
    StoreAckRef, StoreBatchCommit, StoreBatchCommitRef, StoreCommitCoord, StoreDeviceHead,
    StoreDeviceHeadRef, StoreDeviceRegistration, StoreDeviceRegistrationRef, StoreProtocolError,
    StoreProtocolRoot, StoreRootRef, SERIAL_STREAM_ID,
};

#[derive(Debug)]
pub struct VerifiedObject<T> {
    pub value: T,
    pub bytes: Vec<u8>,
    pub semantic_hash: ObjectHash,
    pub object: ExactObjectRef,
}

pub async fn load_provider_access_grant_ref(
    storage: &dyn SyncStorage,
    store_root: &StoreRootRef,
    reference: &super::provider::StoreMemberProviderAccessGrantRef,
    administrator: &StoreDeviceRegistration,
) -> Result<VerifiedObject<super::provider::StoreMemberProviderAccessGrant>, StoreObjectError> {
    let store = load_store_protocol_root(storage, store_root)
        .await?
        .value
        .descriptor
        .provider;
    let context = ProtocolObjectContext::signed_plaintext(
        store_root.store_root_hash,
        ProtocolObjectDomain::ProviderAccessGrant,
    );
    let semantic_prefix = provider_access_grant_semantic_prefix(&reference.grant_id);
    load_exact_object(
        storage,
        &context,
        &reference.object,
        &semantic_prefix,
        reference.grant_hash,
        |bytes| {
            let grant: super::provider::StoreMemberProviderAccessGrant =
                serde_json::from_slice(bytes)
                    .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
            reference
                .verify(&grant)
                .and_then(|()| grant.verify(&store, administrator))
                .map_err(|_| StoreProtocolError::ProviderAccessMismatch)?;
            Ok(grant)
        },
    )
    .await
}

pub async fn load_provider_access_withdrawal_ref(
    storage: &dyn SyncStorage,
    store_root: &StoreRootRef,
    reference: &super::provider::StoreMemberProviderAccessWithdrawalReceiptRef,
    administrator: &StoreDeviceRegistration,
) -> Result<
    VerifiedObject<super::provider::StoreMemberProviderAccessWithdrawalReceipt>,
    StoreObjectError,
> {
    let context = ProtocolObjectContext::signed_plaintext(
        store_root.store_root_hash,
        ProtocolObjectDomain::ProviderAccessWithdrawal,
    );
    let semantic_prefix = provider_access_withdrawal_semantic_prefix(&reference.grant_id);
    load_exact_object(
        storage,
        &context,
        &reference.object,
        &semantic_prefix,
        reference.receipt_hash,
        |bytes| {
            let receipt: super::provider::StoreMemberProviderAccessWithdrawalReceipt =
                serde_json::from_slice(bytes)
                    .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
            reference
                .verify(&receipt)
                .and_then(|()| receipt.verify(administrator))
                .map_err(|_| StoreProtocolError::ProviderAccessMismatch)?;
            Ok(receipt)
        },
    )
    .await
}

#[derive(Debug, thiserror::Error)]
pub enum StoreObjectError {
    #[error("{0}")]
    Storage(
        #[from]
        #[source]
        StorageError,
    ),
    #[error("Store object {key:?} is invalid for semantic object {semantic_prefix:?}: {source}")]
    InvalidObject {
        semantic_prefix: String,
        key: String,
        #[source]
        source: Box<StoreProtocolError>,
    },
}

/// Create bytes already sealed for one reserved exact object.
pub async fn create_exact_object(
    storage: &dyn SyncStorage,
    prepared: &PreparedExactObject,
) -> Result<ExactObjectRef, StoreObjectError> {
    storage.create_protocol_object(prepared).await?;
    Ok(prepared.reference().clone())
}

/// Open one supplied exact reference and verify its typed semantic contents.
pub async fn load_exact_object<T>(
    storage: &dyn SyncStorage,
    context: &ProtocolObjectContext,
    object: &ExactObjectRef,
    semantic_prefix: &str,
    semantic_hash: ObjectHash,
    verify: impl FnOnce(&[u8]) -> Result<T, StoreProtocolError>,
) -> Result<VerifiedObject<T>, StoreObjectError> {
    let bytes = storage
        .read_protocol_object(context, object, semantic_prefix)
        .await?;
    let value = verify(&bytes).map_err(|source| StoreObjectError::InvalidObject {
        semantic_prefix: semantic_prefix.to_string(),
        key: object.slot().logical_key().to_string(),
        source: Box::new(source),
    })?;
    Ok(VerifiedObject {
        value,
        bytes,
        semantic_hash,
        object: object.clone(),
    })
}

/// Delete one supplied exact reference and verify that exact object is absent.
pub async fn delete_exact_object(
    storage: &dyn SyncStorage,
    object: &ExactObjectRef,
) -> Result<(), StoreObjectError> {
    storage.delete_protocol_object(object).await?;
    Ok(())
}

pub async fn load_store_protocol_root(
    storage: &dyn SyncStorage,
    reference: &StoreRootRef,
) -> Result<VerifiedObject<StoreProtocolRoot>, StoreObjectError> {
    let context = ProtocolObjectContext::signed_plaintext(
        reference.store_root_hash,
        ProtocolObjectDomain::StoreProtocolRoot,
    );
    let semantic_prefix = store_protocol_root_logical_key();
    load_exact_object(
        storage,
        &context,
        &reference.object,
        semantic_prefix,
        reference.store_root_hash,
        |bytes| StoreProtocolRoot::parse_pinned(bytes, reference),
    )
    .await
}

pub async fn load_registration_ref(
    storage: &dyn SyncStorage,
    store_root: &StoreRootRef,
    reference: &StoreDeviceRegistrationRef,
) -> Result<VerifiedObject<StoreDeviceRegistration>, StoreObjectError> {
    let pinned_root = load_store_protocol_root(storage, store_root).await?.value;
    let context = ProtocolObjectContext::signed_plaintext(
        store_root.store_root_hash,
        ProtocolObjectDomain::StoreDeviceRegistration,
    );
    let semantic_prefix = registration_slot_semantic_prefix(&reference.object)?;
    load_exact_object(
        storage,
        &context,
        &reference.object,
        &semantic_prefix,
        reference.registration_hash,
        |bytes| verify_opened_registration(bytes, store_root, reference, &pinned_root),
    )
    .await
}

pub async fn load_founder_registration(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
) -> Result<VerifiedObject<StoreDeviceRegistration>, StoreObjectError> {
    let root_value = load_store_protocol_root(storage, root).await?.value;
    let context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreDeviceRegistration,
    );
    let semantic_prefix = founder_registration_semantic_prefix(root_value.descriptor.creation_id);
    let (bytes, object) = storage
        .read_protocol_slot(
            &context,
            &root_value.descriptor.founder_registration,
            &semantic_prefix,
        )
        .await?;
    let unverified: StoreDeviceRegistration =
        serde_json::from_slice(&bytes).map_err(|error| StoreObjectError::InvalidObject {
            semantic_prefix: semantic_prefix.clone(),
            key: object.slot().logical_key().to_string(),
            source: Box::new(StoreProtocolError::Malformed(error.to_string())),
        })?;
    let reference = StoreDeviceRegistrationRef::from_registration(&unverified, object.clone());
    let value =
        verify_opened_registration(&bytes, root, &reference, &root_value).map_err(|source| {
            StoreObjectError::InvalidObject {
                semantic_prefix: semantic_prefix.clone(),
                key: object.slot().logical_key().to_string(),
                source: Box::new(source),
            }
        })?;
    Ok(VerifiedObject {
        value,
        bytes,
        semantic_hash: reference.registration_hash,
        object,
    })
}

fn registration_slot_semantic_prefix(object: &ExactObjectRef) -> Result<String, StoreObjectError> {
    object
        .slot()
        .logical_key()
        .strip_suffix(".json")
        .map(str::to_string)
        .ok_or_else(|| StoreObjectError::InvalidObject {
            semantic_prefix: object.slot().logical_key().to_string(),
            key: object.slot().logical_key().to_string(),
            source: Box::new(StoreProtocolError::Malformed(
                "device registration slot has no .json suffix".to_string(),
            )),
        })
}

fn verify_opened_registration(
    bytes: &[u8],
    root: &StoreRootRef,
    reference: &StoreDeviceRegistrationRef,
    pinned_root: &StoreProtocolRoot,
) -> Result<StoreDeviceRegistration, StoreProtocolError> {
    let registration = StoreDeviceRegistration::parse_at(bytes, root, reference.device_id)?;
    reference.verify_registration(&registration)?;
    let expected_prefix = match &registration.origin {
        super::store_commit::StoreDeviceRegistrationOrigin::Founder { creation_id }
            if *creation_id == pinned_root.descriptor.creation_id
                && registration.provider
                    == pinned_root.descriptor.founder_provider_admin.provider
                && reference.object.slot() == &pinned_root.descriptor.founder_registration =>
        {
            founder_registration_semantic_prefix(*creation_id)
        }
        super::store_commit::StoreDeviceRegistrationOrigin::Founder { .. } => {
            return Err(StoreProtocolError::InvalidFounder);
        }
        _ if reference.object.slot() != &pinned_root.descriptor.founder_registration => {
            registration_semantic_prefix(&reference.device_id.to_string())
        }
        _ => return Err(StoreProtocolError::InvalidFounder),
    };
    let actual_prefix = reference
        .object
        .slot()
        .logical_key()
        .strip_suffix(".json")
        .ok_or_else(|| {
            StoreProtocolError::Malformed(
                "device registration slot has no .json suffix".to_string(),
            )
        })?;
    if actual_prefix != expected_prefix {
        return Err(StoreProtocolError::Malformed(
            "device registration exact slot does not match its signed origin".to_string(),
        ));
    }
    Ok(registration)
}

pub async fn load_device_join_attempt_ref(
    storage: &dyn SyncStorage,
    store_root: &StoreRootRef,
    reference: &DeviceJoinAttemptRef,
    owner: &StoreDeviceRegistration,
) -> Result<VerifiedObject<DeviceJoinAttempt>, StoreObjectError> {
    let context = ProtocolObjectContext::signed_plaintext(
        store_root.store_root_hash,
        ProtocolObjectDomain::DeviceJoinAttempt,
    );
    let semantic_prefix = device_join_attempt_semantic_prefix(reference.attempt_id);
    let bytes = storage
        .read_protocol_object(&context, &reference.object, &semantic_prefix)
        .await?;
    let parse_bytes = bytes.clone();
    let expected = reference.clone();
    let owner = owner.clone();
    let value = tokio::task::spawn_blocking(move || {
        DeviceJoinAttempt::parse_at(&parse_bytes, &expected, &owner)
    })
    .await
    .map_err(|error| {
        StoreObjectError::Storage(StorageError::Storage(format!(
            "device join attempt verification task failed: {error}"
        )))
    })?
    .map_err(|source| StoreObjectError::InvalidObject {
        semantic_prefix: semantic_prefix.clone(),
        key: reference.object.slot().logical_key().to_string(),
        source: Box::new(source),
    })?;
    Ok(VerifiedObject {
        value,
        bytes,
        semantic_hash: reference.attempt_hash,
        object: reference.object.clone(),
    })
}

pub async fn load_device_join_outcome_ref(
    storage: &dyn SyncStorage,
    store_root: &StoreRootRef,
    reference: &DeviceJoinOutcomeRef,
    owner: &StoreDeviceRegistration,
) -> Result<VerifiedObject<DeviceJoinOutcome>, StoreObjectError> {
    let context = ProtocolObjectContext::signed_plaintext(
        store_root.store_root_hash,
        ProtocolObjectDomain::DeviceJoinOutcome,
    );
    let semantic_prefix = device_join_outcome_semantic_prefix(reference.attempt().attempt_id);
    let expected_hash = match reference {
        DeviceJoinOutcomeRef::Activated { outcome_hash, .. }
        | DeviceJoinOutcomeRef::Cancelled { outcome_hash, .. } => *outcome_hash,
    };
    load_exact_object(
        storage,
        &context,
        reference.object(),
        &semantic_prefix,
        expected_hash,
        |bytes| {
            let outcome: DeviceJoinOutcome = serde_json::from_slice(bytes)
                .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
            if outcome.store_root_hash != store_root.store_root_hash {
                return Err(StoreProtocolError::StoreRootMismatch {
                    expected: store_root.store_root_hash,
                    actual: outcome.store_root_hash,
                });
            }
            outcome.owner_registration.verify_registration(owner)?;
            if !crate::keys::verify_signature_hex(
                &owner.device_signing_pubkey,
                &outcome.signature,
                &outcome.canonical_signed_bytes(),
            ) {
                return Err(StoreProtocolError::InvalidSignature);
            }
            reference.verify_outcome(&outcome)?;
            Ok(outcome)
        },
    )
    .await
}

pub async fn load_store_ack_ref(
    storage: &dyn SyncStorage,
    store_root: &StoreRootRef,
    reference: &StoreAckRef,
    registration: &StoreDeviceRegistration,
) -> Result<VerifiedObject<StoreAck>, StoreObjectError> {
    let context = ProtocolObjectContext::signed_plaintext(
        store_root.store_root_hash,
        ProtocolObjectDomain::StoreAck,
    );
    let semantic_prefix = ack_slot_prefix(&registration.device_id.to_string(), reference.sequence);
    load_exact_object(
        storage,
        &context,
        &reference.object,
        &semantic_prefix,
        reference.ack_hash,
        |bytes| StoreAck::parse_at(bytes, store_root, reference, registration),
    )
    .await
}

pub async fn load_store_ack_predecessor(
    storage: &dyn SyncStorage,
    store_root: &StoreRootRef,
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
        store_root.store_root_hash,
        ProtocolObjectDomain::StoreAck,
    );
    let semantic_prefix = ack_slot_prefix(&registration.device_id.to_string(), sequence);
    let bytes = storage
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
    let value =
        StoreAck::parse_at(&bytes, store_root, &reference, registration).map_err(|source| {
            StoreObjectError::InvalidObject {
                semantic_prefix,
                key: object.slot().logical_key().to_string(),
                source: Box::new(source),
            }
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

pub async fn load_owner_recovery_node_ref(
    storage: &dyn SyncStorage,
    store_root: &StoreRootRef,
    reference: &OwnerRecoveryNodeRef,
) -> Result<VerifiedObject<OwnerRecoveryNode>, StoreObjectError> {
    let semantic_prefix = crate::sync::store_commit::owner_recovery_semantic_prefix(
        &reference.owner_pubkey,
        reference.owner_grant.clone(),
        reference.sequence,
    );
    let context = ProtocolObjectContext::signed_plaintext(
        store_root.store_root_hash,
        ProtocolObjectDomain::OwnerRecoveryNode,
    );
    load_exact_object(
        storage,
        &context,
        &reference.object,
        &semantic_prefix,
        reference.node_hash,
        |bytes| OwnerRecoveryNode::parse_at(bytes, store_root, reference),
    )
    .await
}

pub async fn load_commit_ref(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    reference: &StoreBatchCommitRef,
    author: &StoreDeviceRegistration,
) -> Result<VerifiedObject<StoreBatchCommit>, StoreObjectError> {
    let semantic_prefix =
        super::store_commit::semantic_prefix_from_exact_object(&reference.object, ".json")
            .map_err(|source| StoreObjectError::InvalidObject {
                semantic_prefix: "Store candidate commit".to_string(),
                key: reference.object.slot().logical_key().to_string(),
                source: Box::new(source),
            })?;
    let context =
        ProtocolObjectContext::signed_plaintext(store_root_hash, ProtocolObjectDomain::StoreCommit);
    load_exact_object(
        storage,
        &context,
        &reference.object,
        &semantic_prefix,
        reference.commit_hash,
        |bytes| {
            let commit =
                StoreBatchCommit::parse_at(bytes, store_root_hash, &reference.coord, author)?;
            reference.verify_commit(&commit)?;
            Ok(commit)
        },
    )
    .await
}

pub async fn load_head_ref(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    reference: &StoreDeviceHeadRef,
    registration: &StoreDeviceRegistration,
    commit: &StoreBatchCommitRef,
) -> Result<VerifiedObject<StoreDeviceHead>, StoreObjectError> {
    let semantic_prefix =
        head_slot_prefix(&registration.device_id.to_string(), commit.coord.sequence());
    let context =
        ProtocolObjectContext::signed_plaintext(store_root_hash, ProtocolObjectDomain::StoreHead);
    load_exact_object(
        storage,
        &context,
        &reference.object,
        &semantic_prefix,
        reference.head_hash,
        |bytes| {
            let head = StoreDeviceHead::parse_at(bytes, store_root_hash, registration, commit)?;
            let actual = head.head_hash();
            if actual != reference.head_hash {
                return Err(StoreProtocolError::ObjectHashMismatch {
                    expected: reference.head_hash,
                    actual,
                });
            }
            Ok(head)
        },
    )
    .await
}

pub async fn load_store_package(
    storage: &dyn SyncStorage,
    reference: &StoreBatchCommitRef,
    commit: &StoreBatchCommit,
) -> Result<Option<VerifiedObject<Vec<u8>>>, StoreObjectError> {
    let stream_id = commit_stream_id(&reference.coord);
    reference
        .verify_commit(commit)
        .map_err(|source| StoreObjectError::InvalidObject {
            semantic_prefix: commit_semantic_prefix(
                commit.candidate_family(),
                &stream_id,
                reference.coord.sequence(),
                reference.commit_hash,
            ),
            key: reference.object.slot().logical_key().to_string(),
            source: Box::new(source),
        })?;
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
    load_exact_object(
        storage,
        &context,
        &package.object,
        &semantic_prefix,
        package.content_hash,
        |bytes| {
            commit.verify_store_package(bytes)?;
            Ok(bytes.to_vec())
        },
    )
    .await
    .map(Some)
}

pub async fn prepare_membership_entry(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    entry: &MembershipEntry,
) -> Result<(PreparedExactObject, MembershipEntryRef), StoreObjectError> {
    let coord = entry.coord();
    let semantic_prefix = membership_entry_semantic_prefix(
        &coord.author_pubkey,
        &coord.author_owner_grant,
        coord.stream_id,
        coord.seq,
        coord.entry_hash,
    );
    let context = ProtocolObjectContext::signed_plaintext(
        store_root_hash,
        ProtocolObjectDomain::StoreMembershipEntry,
    );
    let slot = storage
        .allocate_protocol_slot(&context, &semantic_prefix, ".json")
        .await?;
    let prepared = storage.prepare_protocol_object(
        &context,
        slot,
        &semantic_prefix,
        serde_json::to_vec(entry).expect("membership entry serialization cannot fail"),
    )?;
    let reference = MembershipEntryRef {
        coord,
        object: prepared.reference().clone(),
    };
    Ok((prepared, reference))
}

pub async fn load_membership_entry_ref(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    reference: &MembershipEntryRef,
) -> Result<VerifiedObject<MembershipEntry>, StoreObjectError> {
    let coord = &reference.coord;
    let semantic_prefix = membership_entry_semantic_prefix(
        &coord.author_pubkey,
        &coord.author_owner_grant,
        coord.stream_id,
        coord.seq,
        coord.entry_hash,
    );
    let context = ProtocolObjectContext::signed_plaintext(
        store_root_hash,
        ProtocolObjectDomain::StoreMembershipEntry,
    );
    load_exact_object(
        storage,
        &context,
        &reference.object,
        &semantic_prefix,
        coord.entry_hash,
        |bytes| {
            let entry: MembershipEntry = serde_json::from_slice(bytes)
                .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
            if entry.coord() != *coord || !super::membership::verify_membership_entry(&entry) {
                return Err(StoreProtocolError::Malformed(
                    "exact membership entry differs from its reference".to_string(),
                ));
            }
            Ok(entry)
        },
    )
    .await
}

pub async fn load_membership_head_ref(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    reference: &MembershipHeadRef,
    registration: &StoreDeviceRegistration,
) -> Result<VerifiedObject<AuthorHead>, StoreObjectError> {
    let coord = &reference.coord;
    let semantic_prefix = reference
        .object
        .slot()
        .logical_key()
        .strip_suffix(".json")
        .ok_or_else(|| StoreObjectError::InvalidObject {
            semantic_prefix: reference.object.slot().logical_key().to_string(),
            key: reference.object.slot().logical_key().to_string(),
            source: Box::new(StoreProtocolError::Malformed(
                "membership head slot has no .json suffix".to_string(),
            )),
        })?;
    let context = ProtocolObjectContext::signed_plaintext(
        store_root_hash,
        ProtocolObjectDomain::StoreMembershipHead,
    );
    load_exact_object(
        storage,
        &context,
        &reference.object,
        semantic_prefix,
        reference.head_hash,
        |bytes| {
            let head: AuthorHead = serde_json::from_slice(bytes)
                .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
            if head.entry_coord() != *coord
                || head.head_hash() != reference.head_hash
                || !head.verify(registration)
            {
                return Err(StoreProtocolError::Malformed(
                    "exact membership head differs from its reference".to_string(),
                ));
            }
            Ok(head)
        },
    )
    .await
}

pub async fn load_membership_resolution_ref(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    reference: &StoreMembershipConflictResolutionRef,
) -> Result<VerifiedObject<StoreMembershipConflictResolution>, StoreObjectError> {
    let semantic_prefix = membership_resolution_semantic_prefix(
        reference.conflict_hash,
        &reference.resolver_pubkey,
        reference.resolution_hash,
    );
    let context = ProtocolObjectContext::signed_plaintext(
        store_root_hash,
        ProtocolObjectDomain::StoreMembershipResolution,
    );
    load_exact_object(
        storage,
        &context,
        &reference.object,
        &semantic_prefix,
        reference.resolution_hash,
        |bytes| {
            let resolution: StoreMembershipConflictResolution = serde_json::from_slice(bytes)
                .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
            if resolution.store_root_hash != store_root_hash
                || resolution.conflict_hash != reference.conflict_hash
                || resolution.resolver_pubkey != reference.resolver_pubkey
                || resolution.resolution_hash() != reference.resolution_hash
                || !resolution.verify_signature()
            {
                return Err(StoreProtocolError::Malformed(
                    "exact membership resolution differs from its reference".to_string(),
                ));
            }
            Ok(resolution)
        },
    )
    .await
}

fn commit_stream_id(coord: &StoreCommitCoord) -> String {
    match coord {
        StoreCommitCoord::MergeConcurrent { stream_id, .. } => stream_id.to_string(),
        StoreCommitCoord::Serial { .. } => SERIAL_STREAM_ID.to_string(),
    }
}

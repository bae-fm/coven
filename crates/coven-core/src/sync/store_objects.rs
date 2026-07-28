//! Exact-reference access to Store protocol objects.

use std::future::Future;
use std::pin::Pin;

use super::membership::{
    AuthorHead, MembershipEntry, MembershipEntryRef, MembershipHeadRef,
    StoreMembershipConflictResolution, StoreMembershipConflictResolutionRef,
};
use super::storage::{
    ExactObjectRef, PreparedExactObject, ProtocolObjectContext, ProtocolObjectDomain, StorageError,
    SyncStorage,
};
use super::store_commit::{
    ack_slot_prefix, device_exclusion_outcome_semantic_prefix,
    device_exclusion_proposal_semantic_prefix, device_join_attempt_semantic_prefix,
    device_join_outcome_semantic_prefix, founder_registration_semantic_prefix, head_slot_prefix,
    membership_entry_semantic_prefix, membership_resolution_semantic_prefix,
    package_semantic_prefix, provider_access_grant_semantic_prefix, registration_semantic_prefix,
    store_protocol_root_logical_key, CirclePackageRef, DeviceJoinAttempt, DeviceJoinAttemptRef,
    DeviceJoinOutcome, DeviceJoinOutcomeRef, ObjectHash, OwnerRecoveryNode, OwnerRecoveryNodeRef,
    StoreAck, StoreAckRef, StoreBatchCommitRef, StoreCommitCoord, StoreDeviceExclusionOutcome,
    StoreDeviceExclusionOutcomeRef, StoreDeviceExclusionProposal, StoreDeviceExclusionProposalRef,
    StoreDeviceHead, StoreDeviceHeadRef, StoreDeviceRegistration, StoreDeviceRegistrationRef,
    StoreProtocolError, StoreProtocolRoot, StoreRootRef, VerifiedStoreBatchCommit,
};

#[derive(Clone, Debug)]
pub struct VerifiedObject<T> {
    pub value: T,
    pub bytes: Vec<u8>,
    pub semantic_hash: ObjectHash,
    pub object: ExactObjectRef,
}

#[derive(Debug)]
pub struct VerifiedDeviceExclusionProposal {
    pub reference: StoreDeviceExclusionProposalRef,
    pub object: VerifiedObject<StoreDeviceExclusionProposal>,
    pub target: StoreDeviceRegistration,
    pub owner: StoreDeviceRegistration,
}

#[derive(Debug)]
pub struct VerifiedDeviceExclusionOutcome {
    pub object: VerifiedObject<StoreDeviceExclusionOutcome>,
    pub owner: StoreDeviceRegistration,
}

#[derive(Debug)]
pub struct VerifiedReclaimAuthorization {
    pub authorization: VerifiedObject<super::store::ReclaimAuthorization>,
    pub evidence: VerifiedObject<super::store::ReclaimEvidence>,
}

#[derive(Debug)]
pub struct VerifiedReclaimReceipt {
    pub receipt: VerifiedObject<super::store::ReclaimReceipt>,
    pub authorization: VerifiedReclaimAuthorization,
    pub executor: StoreDeviceRegistration,
}

pub(crate) fn load_provider_access_grant_ref_with_root<'a>(
    storage: &'a dyn SyncStorage,
    store_root: &'a StoreRootRef,
    verified_root: &'a StoreProtocolRoot,
    reference: &'a super::provider::StoreMemberProviderAccessGrantRef,
    administrator: &'a StoreDeviceRegistration,
) -> StoreObjectFuture<'a, VerifiedObject<super::provider::StoreMemberProviderAccessGrant>> {
    let store = verified_root.descriptor.provider.clone();
    Box::pin(async move {
        let context = ProtocolObjectContext::signed_plaintext(
            store_root.store_root_hash,
            ProtocolObjectDomain::ProviderAccessGrant,
        );
        let semantic_prefix = provider_access_grant_semantic_prefix(&reference.grant_id);
        let expected = reference.clone();
        let administrator = administrator.clone();
        load_exact_object(
            storage,
            &context,
            &reference.object,
            &semantic_prefix,
            reference.grant_hash,
            move |bytes| {
                let grant: super::provider::StoreMemberProviderAccessGrant =
                    decode_protocol_object(bytes)?;
                expected
                    .verify(&grant)
                    .and_then(|()| grant.verify(&store, &administrator))
                    .map_err(|_| StoreProtocolError::ProviderAccessMismatch)?;
                Ok(grant)
            },
        )
        .await
    })
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

pub(crate) type StoreObjectFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, StoreObjectError>> + Send + 'a>>;

/// Create bytes already sealed for one reserved exact object.
pub async fn create_exact_object(
    storage: &dyn SyncStorage,
    prepared: &PreparedExactObject,
) -> Result<ExactObjectRef, StoreObjectError> {
    storage.create_protocol_object(prepared).await?;
    Ok(prepared.reference().clone())
}

/// Decode the JSON body of one protocol object. Bytes that do not parse as `T`
/// are malformed for the slot they were read from.
pub(crate) fn decode_protocol_object<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
) -> Result<T, StoreProtocolError> {
    serde_json::from_slice(bytes).map_err(|error| StoreProtocolError::Malformed(error.to_string()))
}

/// Reject an object that names a different Store root than the one it was read
/// under.
pub(crate) fn verify_store_root(
    expected: ObjectHash,
    actual: ObjectHash,
) -> Result<(), StoreProtocolError> {
    if actual != expected {
        return Err(StoreProtocolError::StoreRootMismatch { expected, actual });
    }
    Ok(())
}

/// Open one supplied exact reference and verify its typed semantic contents.
pub async fn load_exact_object<T>(
    storage: &dyn SyncStorage,
    context: &ProtocolObjectContext,
    object: &ExactObjectRef,
    semantic_prefix: &str,
    semantic_hash: ObjectHash,
    verify: impl FnOnce(&[u8]) -> Result<T, StoreProtocolError> + Send + 'static,
) -> Result<VerifiedObject<T>, StoreObjectError>
where
    T: Send + 'static,
{
    let bytes = storage
        .read_protocol_object(context, object, semantic_prefix)
        .await?;
    let verify_bytes = bytes.clone();
    let value = Box::pin(run_blocking_object_verification(
        semantic_prefix,
        object,
        Box::new(move || verify(&verify_bytes)),
    ))
    .await?;
    Ok(VerifiedObject {
        value,
        bytes,
        semantic_hash,
        object: object.clone(),
    })
}

pub(crate) async fn run_blocking_object_verification<T>(
    semantic_prefix: &str,
    object: &ExactObjectRef,
    verify: Box<dyn FnOnce() -> Result<T, StoreProtocolError> + Send>,
) -> Result<T, StoreObjectError>
where
    T: Send + 'static,
{
    super::blocking::run(verify)
        .await
        .map_err(|error| {
            StoreObjectError::Storage(StorageError::Storage(format!(
                "Store object verification task failed: {error}"
            )))
        })?
        .map_err(|source| StoreObjectError::InvalidObject {
            semantic_prefix: semantic_prefix.to_string(),
            key: object.slot().logical_key().to_string(),
            source: Box::new(source),
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

pub fn load_store_protocol_root<'a>(
    storage: &'a dyn SyncStorage,
    reference: &'a StoreRootRef,
) -> StoreObjectFuture<'a, VerifiedObject<StoreProtocolRoot>> {
    let context = ProtocolObjectContext::signed_plaintext(
        reference.store_root_hash,
        ProtocolObjectDomain::StoreProtocolRoot,
    );
    let semantic_prefix = store_protocol_root_logical_key();
    let expected = reference.clone();
    Box::pin(async move {
        load_exact_object(
            storage,
            &context,
            &reference.object,
            semantic_prefix,
            reference.store_root_hash,
            move |bytes| StoreProtocolRoot::parse_pinned(bytes, &expected),
        )
        .await
    })
}

pub fn load_registration_ref<'a>(
    storage: &'a dyn SyncStorage,
    store_root: &'a StoreRootRef,
    reference: &'a StoreDeviceRegistrationRef,
) -> StoreObjectFuture<'a, VerifiedObject<StoreDeviceRegistration>> {
    Box::pin(async move {
        let pinned_root = load_store_protocol_root(storage, store_root).await?.value;
        load_registration_ref_with_root(storage, store_root, &pinned_root, reference).await
    })
}

pub(crate) async fn load_registration_ref_with_root(
    storage: &dyn SyncStorage,
    store_root: &StoreRootRef,
    pinned_root: &StoreProtocolRoot,
    reference: &StoreDeviceRegistrationRef,
) -> Result<VerifiedObject<StoreDeviceRegistration>, StoreObjectError> {
    let context = ProtocolObjectContext::signed_plaintext(
        store_root.store_root_hash,
        ProtocolObjectDomain::StoreDeviceRegistration,
    );
    let semantic_prefix = registration_slot_semantic_prefix(&reference.object)?;
    let expected_root = store_root.clone();
    let expected_reference = reference.clone();
    let pinned_root = pinned_root.clone();
    load_exact_object(
        storage,
        &context,
        &reference.object,
        &semantic_prefix,
        reference.registration_hash,
        move |bytes| {
            verify_opened_registration(bytes, &expected_root, &expected_reference, &pinned_root)
        },
    )
    .await
}

pub async fn load_founder_registration(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
) -> Result<VerifiedObject<StoreDeviceRegistration>, StoreObjectError> {
    let root_value = load_store_protocol_root(storage, root).await?.value;
    load_founder_registration_with_root(storage, root, &root_value).await
}

pub(crate) async fn load_founder_registration_with_root(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    root_value: &StoreProtocolRoot,
) -> Result<VerifiedObject<StoreDeviceRegistration>, StoreObjectError> {
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
    let verify_bytes = bytes.clone();
    let verify_object = object.clone();
    let verify_root = root.clone();
    let verify_root_value = root_value.clone();
    let (value, reference) = run_blocking_object_verification(
        &semantic_prefix,
        &object,
        Box::new(move || {
            let unverified: StoreDeviceRegistration = serde_json::from_slice(&verify_bytes)
                .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
            let reference =
                StoreDeviceRegistrationRef::from_registration(&unverified, verify_object);
            let value = verify_opened_registration(
                &verify_bytes,
                &verify_root,
                &reference,
                &verify_root_value,
            )?;
            Ok((value, reference))
        }),
    )
    .await?;
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

pub(crate) async fn load_owner_signed_device_join_attempt_ref(
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
    let expected = reference.clone();
    let owner = owner.clone();
    load_exact_object(
        storage,
        &context,
        &reference.object,
        &semantic_prefix,
        reference.attempt_hash,
        move |bytes| DeviceJoinAttempt::parse_at(bytes, &expected, &owner),
    )
    .await
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
    let expected = reference.clone();
    let expected_owner = owner.clone();
    let expected_store_root_hash = store_root.store_root_hash;
    load_exact_object(
        storage,
        &context,
        reference.object(),
        &semantic_prefix,
        expected_hash,
        move |bytes| {
            let outcome: DeviceJoinOutcome = decode_protocol_object(bytes)?;
            verify_store_root(expected_store_root_hash, outcome.store_root_hash)?;
            outcome
                .owner_registration
                .verify_registration(&expected_owner)?;
            if !crate::keys::verify_signature_hex(
                &expected_owner.device_signing_pubkey,
                &outcome.signature,
                &outcome.canonical_signed_bytes(),
            ) {
                return Err(StoreProtocolError::InvalidSignature);
            }
            expected.verify_outcome(&outcome)?;
            Ok(outcome)
        },
    )
    .await
}

pub async fn load_device_exclusion_proposal_ref(
    storage: &dyn SyncStorage,
    store_root: &StoreRootRef,
    reference: &StoreDeviceExclusionProposalRef,
) -> Result<VerifiedDeviceExclusionProposal, StoreObjectError> {
    let context = ProtocolObjectContext::signed_plaintext(
        store_root.store_root_hash,
        ProtocolObjectDomain::StoreDeviceExclusionProposal,
    );
    let semantic_prefix = device_exclusion_proposal_semantic_prefix(
        reference.target.device_id,
        reference.proposal_id,
        reference.proposal_hash,
    );
    let expected = reference.clone();
    let expected_store_root_hash = store_root.store_root_hash;
    let opened = load_exact_object(
        storage,
        &context,
        &reference.object,
        &semantic_prefix,
        reference.proposal_hash,
        move |bytes| {
            let proposal: StoreDeviceExclusionProposal = decode_protocol_object(bytes)?;
            expected.verify_proposal(&proposal)?;
            verify_store_root(expected_store_root_hash, proposal.store_root_hash)?;
            Ok(proposal)
        },
    )
    .await?;
    let target = load_registration_ref(storage, store_root, &opened.value.target)
        .await?
        .value;
    let owner = load_registration_ref(storage, store_root, &opened.value.owner_registration)
        .await?
        .value;
    let verified =
        StoreDeviceExclusionProposal::parse_at(&opened.bytes, reference, &target, &owner).map_err(
            |source| StoreObjectError::InvalidObject {
                semantic_prefix,
                key: reference.object.slot().logical_key().to_string(),
                source: Box::new(source),
            },
        )?;
    Ok(VerifiedDeviceExclusionProposal {
        reference: reference.clone(),
        object: VerifiedObject {
            value: verified,
            ..opened
        },
        target,
        owner,
    })
}

pub async fn load_device_exclusion_outcome_ref(
    storage: &dyn SyncStorage,
    store_root: &StoreRootRef,
    reference: &StoreDeviceExclusionOutcomeRef,
    proposal: &VerifiedDeviceExclusionProposal,
) -> Result<VerifiedDeviceExclusionOutcome, StoreObjectError> {
    let context = ProtocolObjectContext::signed_plaintext(
        store_root.store_root_hash,
        ProtocolObjectDomain::StoreDeviceExclusionOutcome,
    );
    let semantic_prefix = device_exclusion_outcome_semantic_prefix(
        proposal.object.value.target.device_id,
        proposal.object.value.proposal_id,
    );
    let expected = reference.clone();
    let opened = load_exact_object(
        storage,
        &context,
        reference.object(),
        &semantic_prefix,
        reference.outcome_hash(),
        move |bytes| {
            let outcome: StoreDeviceExclusionOutcome = decode_protocol_object(bytes)?;
            if outcome.outcome_hash() != expected.outcome_hash()
                || outcome.proposal() != expected.proposal()
            {
                return Err(StoreProtocolError::DeviceStateMismatch);
            }
            Ok(outcome)
        },
    )
    .await?;
    let owner_ref = match &opened.value {
        StoreDeviceExclusionOutcome::Excluded(exclusion) => &exclusion.owner_registration,
        StoreDeviceExclusionOutcome::Cancelled(cancellation) => &cancellation.owner_registration,
    };
    let owner = load_registration_ref(storage, store_root, owner_ref)
        .await?
        .value;
    let verified = StoreDeviceExclusionOutcome::parse_at(
        &opened.bytes,
        reference,
        &proposal.object.value,
        &proposal.target,
        &owner,
    )
    .map_err(|source| StoreObjectError::InvalidObject {
        semantic_prefix,
        key: reference.object().slot().logical_key().to_string(),
        source: Box::new(source),
    })?;
    Ok(VerifiedDeviceExclusionOutcome {
        object: VerifiedObject {
            value: verified,
            ..opened
        },
        owner,
    })
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
    let expected_root = store_root.clone();
    let expected = reference.clone();
    let expected_registration = registration.clone();
    load_exact_object(
        storage,
        &context,
        &reference.object,
        &semantic_prefix,
        reference.ack_hash,
        move |bytes| StoreAck::parse_at(bytes, &expected_root, &expected, &expected_registration),
    )
    .await
}

pub async fn load_reclaim_authorization_ref(
    storage: &dyn SyncStorage,
    store_root: &StoreRootRef,
    reference: &super::store::ReclaimAuthorizationRef,
) -> Result<VerifiedReclaimAuthorization, StoreObjectError> {
    let evidence_context = ProtocolObjectContext::store_encrypted(
        store_root.store_root_hash,
        ProtocolObjectDomain::StoreReclaimEvidence,
    );
    let evidence_prefix =
        super::store::reclaim_evidence_semantic_prefix(reference.evidence.evidence_hash);
    let expected_evidence = reference.evidence.clone();
    let expected_store_root_hash = store_root.store_root_hash;
    let evidence = load_exact_object(
        storage,
        &evidence_context,
        &reference.evidence.object,
        &evidence_prefix,
        reference.evidence.evidence_hash,
        move |bytes| {
            let evidence: super::store::ReclaimEvidence = decode_protocol_object(bytes)?;
            expected_evidence.verify(&evidence)?;
            verify_store_root(expected_store_root_hash, evidence.store_root_hash)?;
            Ok(evidence)
        },
    )
    .await?;
    let authorization_context = ProtocolObjectContext::signed_plaintext(
        store_root.store_root_hash,
        ProtocolObjectDomain::StoreReclaimAuthorization,
    );
    let authorization_prefix =
        super::store::reclaim_authorization_semantic_prefix(reference.authorization_hash);
    let owner_pubkey = evidence.value.author_pubkey.clone();
    let expected_authorization = reference.clone();
    let expected_store_root_hash = store_root.store_root_hash;
    let authorization = load_exact_object(
        storage,
        &authorization_context,
        &reference.object,
        &authorization_prefix,
        reference.authorization_hash,
        move |bytes| {
            let authorization: super::store::ReclaimAuthorization = decode_protocol_object(bytes)?;
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

pub async fn load_reclaim_receipt_ref(
    storage: &dyn SyncStorage,
    store_root: &StoreRootRef,
    reference: &super::store::ReclaimReceiptRef,
) -> Result<VerifiedReclaimReceipt, StoreObjectError> {
    let authorization = Box::pin(load_reclaim_authorization_ref(
        storage,
        store_root,
        &reference.authorization,
    ))
    .await?;
    let context = ProtocolObjectContext::signed_plaintext(
        store_root.store_root_hash,
        ProtocolObjectDomain::StoreReclaimReceipt,
    );
    let prefix = super::store::reclaim_receipt_semantic_prefix(reference.receipt_hash);
    let bytes = storage
        .read_protocol_object(&context, &reference.object, &prefix)
        .await?;
    let unverified: super::store::ReclaimReceipt =
        serde_json::from_slice(&bytes).map_err(|error| StoreObjectError::InvalidObject {
            semantic_prefix: prefix.clone(),
            key: reference.object.slot().logical_key().to_string(),
            source: Box::new(StoreProtocolError::Malformed(error.to_string())),
        })?;
    let executor = load_registration_ref(storage, store_root, &unverified.executor)
        .await?
        .value;
    let receipt = reference
        .verify(&unverified, &executor)
        .and_then(|()| {
            verify_store_root(store_root.store_root_hash, unverified.store_root_hash)?;
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
        authorization,
        executor,
    })
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
    let expected_root = store_root.clone();
    let expected = reference.clone();
    load_exact_object(
        storage,
        &context,
        &reference.object,
        &semantic_prefix,
        reference.node_hash,
        move |bytes| OwnerRecoveryNode::parse_at(bytes, &expected_root, &expected),
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
    let expected = reference.clone();
    let expected_registration = registration.clone();
    let expected_commit = commit.clone();
    load_exact_object(
        storage,
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

pub(crate) async fn load_store_package(
    storage: &dyn SyncStorage,
    verified: &VerifiedStoreBatchCommit,
) -> Result<Option<VerifiedObject<Vec<u8>>>, StoreObjectError> {
    let reference = verified.reference();
    let commit = verified.value();
    let stream_id = commit_stream_id(&reference.coord);
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
    load_exact_object(
        storage,
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

pub(crate) async fn load_circle_package(
    storage: &dyn SyncStorage,
    verified: &VerifiedStoreBatchCommit,
    package: &CirclePackageRef,
    encryption: crate::encryption::EncryptionService,
) -> Result<VerifiedObject<Vec<u8>>, StoreObjectError> {
    let reference = verified.reference();
    let commit = verified.value();
    let stream_id = commit_stream_id(&reference.coord);
    if !commit
        .circle_packages()
        .iter()
        .any(|committed| committed == package)
    {
        return Err(StoreObjectError::InvalidObject {
            semantic_prefix: package.package.object.slot().logical_key().to_string(),
            key: package.package.object.slot().logical_key().to_string(),
            source: Box::new(StoreProtocolError::MissingCirclePackage(package.circle_id)),
        });
    }
    let semantic_prefix = super::store_commit::circle_package_semantic_prefix(
        package.circle_id,
        commit.candidate_family(),
        &stream_id,
        commit.seq(),
        package.package.content_hash,
    );
    let context = ProtocolObjectContext::circle(
        commit.store_root_hash,
        ProtocolObjectDomain::CirclePackage,
        encryption,
    );
    let expected_commit = commit.clone();
    let expected_circle_id = package.circle_id;
    load_exact_object(
        storage,
        &context,
        &package.package.object,
        &semantic_prefix,
        package.package.content_hash,
        move |bytes| {
            expected_commit.verify_circle_package(expected_circle_id, bytes)?;
            Ok(bytes.to_vec())
        },
    )
    .await
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
    let expected_coord = coord.clone();
    load_exact_object(
        storage,
        &context,
        &reference.object,
        &semantic_prefix,
        coord.entry_hash,
        move |bytes| {
            let entry: MembershipEntry = decode_protocol_object(bytes)?;
            if entry.coord() != expected_coord
                || !super::membership::verify_membership_entry(&entry)
            {
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
    let expected_coord = coord.clone();
    let expected_head_hash = reference.head_hash;
    let expected_registration = registration.clone();
    load_exact_object(
        storage,
        &context,
        &reference.object,
        semantic_prefix,
        reference.head_hash,
        move |bytes| {
            let head: AuthorHead = decode_protocol_object(bytes)?;
            if head.entry_coord() != expected_coord
                || head.head_hash() != expected_head_hash
                || !head.verify(&expected_registration)
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
    let expected = reference.clone();
    load_exact_object(
        storage,
        &context,
        &reference.object,
        &semantic_prefix,
        reference.resolution_hash,
        move |bytes| {
            let resolution: StoreMembershipConflictResolution = decode_protocol_object(bytes)?;
            if resolution.store_root_hash != store_root_hash
                || resolution.conflict_hash != expected.conflict_hash
                || resolution.resolver_pubkey != expected.resolver_pubkey
                || resolution.resolution_hash() != expected.resolution_hash
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
    coord.stream_id.to_string()
}

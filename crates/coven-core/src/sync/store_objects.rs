//! Verification for append-only physical copies of Store protocol objects.

use std::collections::{BTreeMap, BTreeSet};

use crate::storage::cloud::ListingCoverage;

use super::membership::{
    entry_hash, verify_membership_entry, AuthorHead, MembershipCoord, MembershipEntry, OwnerGrantId,
};
use super::storage::{
    ProtocolObjectContext, ProtocolObjectDomain, ProtocolObjectLocator, StorageError, SyncStorage,
};
use super::store_commit::{
    ack_slot_prefix, commit_semantic_prefix, commit_slot_prefix, head_slot_prefix,
    package_semantic_prefix, parse_ack_copy_key, parse_commit_copy_key, parse_head_copy_key,
    parse_membership_entry_copy_key, parse_membership_head_copy_key, parse_package_copy_key,
    parse_registration_copy_key, parse_snapshot_meta_copy_key, parse_store_protocol_root_copy_key,
    registration_slot_prefix, snapshot_image_semantic_prefix, CommitFrontier, CommitPosition,
    SnapshotMeta, StoreAck, StoreBatchCommit, StoreDeviceHead, StoreDeviceRegistration,
    StoreDeviceRegistrationRef, StoreDeviceRegistrationState, StoreProtocolRoot,
};
use super::store_commit::{ObjectHash, StoreProtocolError};

#[derive(Debug)]
pub struct VerifiedCopies<T> {
    pub value: T,
    pub bytes: Vec<u8>,
    pub semantic_hash: ObjectHash,
    pub copies: Vec<ProtocolObjectLocator>,
    pub coverage: ListingCoverage,
}

#[derive(Debug)]
pub struct VerifiedAckChains {
    pub latest_by_device: BTreeMap<String, VerifiedCopies<StoreAck>>,
    pub coverage: ListingCoverage,
}

#[derive(Debug)]
pub struct VerifiedRegistrationChains {
    pub latest_by_device: BTreeMap<String, VerifiedCopies<StoreDeviceRegistration>>,
    pub coverage: ListingCoverage,
}

#[derive(Debug)]
pub struct VerifiedStorePackage {
    pub device_id: String,
    pub seq: u64,
    pub commit_position: super::store_commit::CommitPosition,
    pub package: VerifiedCopies<Vec<u8>>,
    pub commit_coverage: ListingCoverage,
}

#[derive(Debug)]
pub struct VerifiedPackageListing {
    pub packages: Vec<VerifiedStorePackage>,
    pub coverage: ListingCoverage,
}

#[derive(Debug)]
pub struct VerifiedSnapshotListing {
    pub metas: Vec<VerifiedCopies<SnapshotMeta>>,
    pub coverage: ListingCoverage,
}

#[derive(Debug)]
pub struct VerifiedHeadListing {
    pub heads: Vec<VerifiedCopies<StoreDeviceHead>>,
    pub failures: Vec<StoreHeadFailure>,
    pub coverage: ListingCoverage,
}

#[derive(Debug)]
pub struct StoreHeadFailure {
    pub device_id: String,
    pub seq: u64,
    pub semantic_hashes: Vec<ObjectHash>,
    pub error: StoreObjectError,
}

#[derive(Debug)]
pub struct VerifiedMembershipEntryListing {
    pub entries: Vec<(MembershipCoord, VerifiedCopies<MembershipEntry>)>,
    pub coverage: ListingCoverage,
}

#[derive(Debug)]
pub struct VerifiedMembershipHeadListing {
    pub heads: Vec<VerifiedCopies<AuthorHead>>,
    pub coverage: ListingCoverage,
}

#[derive(Debug, thiserror::Error)]
pub enum StoreObjectError {
    #[error("{0}")]
    Storage(#[from] StorageError),
    #[error("Store candidate {key:?} could not be opened: {source}")]
    CandidateUnreadable { key: String, source: StorageError },
    #[error("Store candidate {key:?} collides with semantic object {semantic_prefix:?}: {reason}")]
    Collision {
        semantic_prefix: String,
        key: String,
        reason: String,
    },
    #[error(
        "Store candidate {key:?} is invalid for semantic object {semantic_prefix:?}: {source}"
    )]
    InvalidCandidate {
        semantic_prefix: String,
        key: String,
        source: Box<StoreProtocolError>,
    },
    #[error("Store semantic slot {slot:?} contains valid forks {hashes:?}")]
    SemanticFork {
        slot: String,
        hashes: Vec<ObjectHash>,
    },
    #[error("Store append readback differs at {key:?}")]
    AppendReadbackMismatch { key: String },
    #[error("Store acknowledgement chain for {device_id:?} is missing revision {revision}")]
    MissingAckRevision { device_id: String, revision: u64 },
    #[error("Store acknowledgement chain for {device_id:?} revision {revision} names previous hash {actual:?}, expected {expected:?}")]
    BrokenAckChain {
        device_id: String,
        revision: u64,
        expected: Option<ObjectHash>,
        actual: Option<ObjectHash>,
    },
    #[error("Store device registration chain for {device_id:?} is missing revision {revision}")]
    MissingRegistrationRevision { device_id: String, revision: u64 },
    #[error("Store device registration chain for {device_id:?} revision {revision} names previous hash {actual:?}, expected {expected:?}")]
    BrokenRegistrationChain {
        device_id: String,
        revision: u64,
        expected: Option<ObjectHash>,
        actual: Option<ObjectHash>,
    },
    #[error("Store device registration chain for {device_id:?} changes author from {expected:?} to {actual:?} at revision {revision}")]
    RegistrationAuthorChanged {
        device_id: String,
        revision: u64,
        expected: String,
        actual: String,
    },
    #[error("Store device registration chain for {device_id:?} has invalid {previous:?} -> {current:?} state transition at revision {revision}")]
    InvalidRegistrationTransition {
        device_id: String,
        revision: u64,
        previous: Option<StoreDeviceRegistrationState>,
        current: StoreDeviceRegistrationState,
    },
}

/// Append exact semantic bytes and verify the returned physical locator before
/// allowing durable protocol state to advance.
pub async fn append_and_verify(
    storage: &dyn SyncStorage,
    context: &ProtocolObjectContext,
    semantic_prefix: &str,
    extension: &str,
    bytes: &[u8],
) -> Result<ProtocolObjectLocator, StoreObjectError> {
    let object = storage
        .append_protocol_object(context, semantic_prefix, extension, bytes.to_vec())
        .await?;
    let opened = storage
        .read_protocol_object(context, &object, semantic_prefix)
        .await
        .map_err(|source| StoreObjectError::CandidateUnreadable {
            key: object.logical_key().to_string(),
            source,
        })?;
    if opened != bytes {
        return Err(StoreObjectError::AppendReadbackMismatch {
            key: object.logical_key().to_string(),
        });
    }
    Ok(object)
}

pub async fn load_expected_store_protocol_root(
    storage: &dyn SyncStorage,
    expected_hash: ObjectHash,
    expected_store_id: &str,
    expected_founder: &str,
    expected_policy: crate::WritePolicy,
    expected_sync_routing_hash: ObjectHash,
) -> Result<Option<VerifiedCopies<StoreProtocolRoot>>, StoreObjectError> {
    let semantic_prefix = super::store_commit::store_protocol_root_semantic_prefix(expected_hash);
    load_semantic_copies(
        storage,
        &ProtocolObjectContext::store(expected_hash, ProtocolObjectDomain::StoreProtocolRoot),
        &semantic_prefix,
        ".json",
        expected_hash,
        |bytes| {
            StoreProtocolRoot::parse_expected(
                bytes,
                expected_hash,
                expected_store_id,
                expected_founder,
                expected_policy,
                expected_sync_routing_hash,
            )
        },
    )
    .await
}

pub async fn load_pinned_store_protocol_root(
    storage: &dyn SyncStorage,
    expected_hash: ObjectHash,
    expected_store_id: &str,
    expected_founder: &str,
) -> Result<Option<VerifiedCopies<StoreProtocolRoot>>, StoreObjectError> {
    let semantic_prefix = super::store_commit::store_protocol_root_semantic_prefix(expected_hash);
    load_semantic_copies(
        storage,
        &ProtocolObjectContext::store(expected_hash, ProtocolObjectDomain::StoreProtocolRoot),
        &semantic_prefix,
        ".json",
        expected_hash,
        |bytes| {
            StoreProtocolRoot::parse_pinned(
                bytes,
                expected_hash,
                expected_store_id,
                expected_founder,
            )
        },
    )
    .await
}

pub async fn load_store_protocol_root_at_hash(
    storage: &dyn SyncStorage,
    expected_hash: ObjectHash,
) -> Result<Option<VerifiedCopies<StoreProtocolRoot>>, StoreObjectError> {
    let semantic_prefix = super::store_commit::store_protocol_root_semantic_prefix(expected_hash);
    load_semantic_copies(
        storage,
        &ProtocolObjectContext::store(expected_hash, ProtocolObjectDomain::StoreProtocolRoot),
        &semantic_prefix,
        ".json",
        expected_hash,
        |bytes| {
            let root = StoreProtocolRoot::parse(bytes)?;
            let actual = root.object_hash();
            if actual != expected_hash {
                return Err(StoreProtocolError::ObjectHashMismatch {
                    expected: expected_hash,
                    actual,
                });
            }
            Ok(root)
        },
    )
    .await
}

pub async fn discover_store_protocol_root(
    storage: &dyn SyncStorage,
    expected_store_id: &str,
    expected_founder: Option<&str>,
) -> Result<VerifiedCopies<StoreProtocolRoot>, StoreObjectError> {
    let listing = storage
        .list_protocol_objects("store-v1/store-protocol-root/")
        .await?;
    let mut groups: BTreeMap<ObjectHash, Vec<ProtocolObjectLocator>> = BTreeMap::new();
    for object in listing.objects {
        let (hash, _) =
            parse_store_protocol_root_copy_key(object.logical_key()).map_err(|error| {
                StoreObjectError::Collision {
                    semantic_prefix: "store-v1/store-protocol-root".to_string(),
                    key: object.logical_key().to_string(),
                    reason: error.to_string(),
                }
            })?;
        groups.entry(hash).or_default().push(object);
    }
    let mut valid = Vec::new();
    for (hash, objects) in groups {
        let semantic_prefix = super::store_commit::store_protocol_root_semantic_prefix(hash);
        if let Some(store_protocol_root) = load_semantic_candidates(
            storage,
            &ProtocolObjectContext::store(hash, ProtocolObjectDomain::StoreProtocolRoot),
            &semantic_prefix,
            ".json",
            hash,
            objects,
            listing.coverage,
            |bytes| {
                let store_protocol_root = StoreProtocolRoot::parse(bytes)?;
                if store_protocol_root.object_hash() != hash {
                    return Err(StoreProtocolError::ObjectHashMismatch {
                        expected: hash,
                        actual: store_protocol_root.object_hash(),
                    });
                }
                if store_protocol_root.store_id != expected_store_id {
                    return Err(StoreProtocolError::StoreMismatch {
                        expected: expected_store_id.to_string(),
                        actual: store_protocol_root.store_id,
                    });
                }
                if let Some(founder) = expected_founder {
                    if store_protocol_root.author_pubkey != founder {
                        return Err(StoreProtocolError::FounderMismatch {
                            expected: founder.to_string(),
                            actual: store_protocol_root.author_pubkey,
                        });
                    }
                }
                Ok(store_protocol_root)
            },
        )
        .await?
        {
            valid.push(store_protocol_root);
        }
    }
    match valid.len() {
        1 => Ok(valid.pop().expect("one Store protocol root exists")),
        0 => Err(StoreObjectError::Storage(StorageError::NotFound(
            "store-v1/store-protocol-root".to_string(),
        ))),
        _ => Err(StoreObjectError::SemanticFork {
            slot: "store-v1/store-protocol-root".to_string(),
            hashes: valid
                .iter()
                .map(|store_protocol_root| store_protocol_root.semantic_hash)
                .collect(),
        }),
    }
}

pub async fn load_commit_slot(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    device_id: &str,
    seq: u64,
) -> Result<Option<VerifiedCopies<StoreBatchCommit>>, StoreObjectError> {
    load_commit_slot_for_policy(
        storage,
        store_root_hash,
        crate::WritePolicy::MergeConcurrent,
        device_id,
        seq,
    )
    .await
}

pub async fn load_serial_commit_at_position(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    position: &CommitPosition,
) -> Result<Option<VerifiedCopies<StoreBatchCommit>>, StoreObjectError> {
    let semantic_prefix = commit_semantic_prefix(
        super::store_commit::SERIAL_STREAM_ID,
        position.seq,
        position.commit_hash,
    );
    load_semantic_copies(
        storage,
        &ProtocolObjectContext::store(store_root_hash, ProtocolObjectDomain::StoreCommit),
        &semantic_prefix,
        ".json",
        position.commit_hash,
        |bytes| {
            let commit = StoreBatchCommit::parse_at(
                bytes,
                store_root_hash,
                crate::WritePolicy::Serial,
                super::store_commit::SERIAL_STREAM_ID,
                position.seq,
            )?;
            if commit.commit_hash() != position.commit_hash {
                return Err(StoreProtocolError::ObjectHashMismatch {
                    expected: position.commit_hash,
                    actual: commit.commit_hash(),
                });
            }
            Ok(commit)
        },
    )
    .await
}

async fn load_commit_slot_for_policy(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    write_policy: crate::WritePolicy,
    device_id: &str,
    seq: u64,
) -> Result<Option<VerifiedCopies<StoreBatchCommit>>, StoreObjectError> {
    let slot = commit_slot_prefix(device_id, seq);
    load_singleton_slot(
        storage,
        &ProtocolObjectContext::store(store_root_hash, ProtocolObjectDomain::StoreCommit),
        &slot,
        ".json",
        |key| Ok(parse_commit_copy_key(key)?.semantic_hash),
        |semantic_hash, bytes| {
            let commit =
                StoreBatchCommit::parse_at(bytes, store_root_hash, write_policy, device_id, seq)?;
            if commit.commit_hash() != semantic_hash {
                return Err(StoreProtocolError::ObjectHashMismatch {
                    expected: semantic_hash,
                    actual: commit.commit_hash(),
                });
            }
            Ok(commit)
        },
    )
    .await
}

pub async fn load_head_slot(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    device_id: &str,
    seq: u64,
) -> Result<Option<VerifiedCopies<StoreDeviceHead>>, StoreObjectError> {
    let slot = head_slot_prefix(device_id, seq);
    load_singleton_slot(
        storage,
        &ProtocolObjectContext::store(store_root_hash, ProtocolObjectDomain::StoreHead),
        &slot,
        ".json",
        |key| Ok(parse_head_copy_key(key)?.semantic_hash),
        |semantic_hash, bytes| {
            let head = StoreDeviceHead::parse_at(bytes, store_root_hash, device_id, seq)?;
            if head.head_hash() != semantic_hash {
                return Err(StoreProtocolError::ObjectHashMismatch {
                    expected: semantic_hash,
                    actual: head.head_hash(),
                });
            }
            Ok(head)
        },
    )
    .await
}

pub async fn load_ack_slot(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    device_id: &str,
    revision: u64,
) -> Result<Option<VerifiedCopies<StoreAck>>, StoreObjectError> {
    let slot = ack_slot_prefix(device_id, revision);
    load_singleton_slot(
        storage,
        &ProtocolObjectContext::store(store_root_hash, ProtocolObjectDomain::StoreAck),
        &slot,
        ".json",
        |key| Ok(parse_ack_copy_key(key)?.semantic_hash),
        |semantic_hash, bytes| {
            let ack = StoreAck::parse_at(bytes, store_root_hash, device_id, revision)?;
            if ack.ack_hash() != semantic_hash {
                return Err(StoreProtocolError::ObjectHashMismatch {
                    expected: semantic_hash,
                    actual: ack.ack_hash(),
                });
            }
            Ok(ack)
        },
    )
    .await
}

pub async fn load_package(
    storage: &dyn SyncStorage,
    commit: &StoreBatchCommit,
) -> Result<Option<VerifiedCopies<Vec<u8>>>, StoreObjectError> {
    let Some(package) = commit.store_package.as_ref() else {
        return Ok(None);
    };
    let semantic_prefix = package_semantic_prefix(
        commit.order.stream_id(&commit.device_id),
        commit.seq(),
        package.content_hash,
    );
    load_semantic_copies(
        storage,
        &ProtocolObjectContext::store(commit.store_root_hash, ProtocolObjectDomain::StorePackage),
        &semantic_prefix,
        ".pkg",
        package.content_hash,
        |bytes| {
            commit.verify_store_package(bytes)?;
            Ok(bytes.to_vec())
        },
    )
    .await
}

fn membership_entry_slot_prefix(author: &str, grant: &OwnerGrantId, seq: u64) -> String {
    format!("store-v1/membership/entries/{author}/{grant}/{seq}")
}

fn membership_head_slot_prefix(author: &str, grant: &OwnerGrantId, seq: u64) -> String {
    format!("store-v1/membership/heads/{author}/{grant}/{seq}")
}

fn parse_membership_entry_at(
    semantic_hash: ObjectHash,
    author: &str,
    grant: &OwnerGrantId,
    seq: u64,
    bytes: &[u8],
) -> Result<MembershipEntry, StoreProtocolError> {
    if ObjectHash::digest(bytes) != semantic_hash {
        return Err(StoreProtocolError::ObjectHashMismatch {
            expected: semantic_hash,
            actual: ObjectHash::digest(bytes),
        });
    }
    let entry: MembershipEntry = serde_json::from_slice(bytes)
        .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
    if entry.author_pubkey != author || entry.author_owner_grant != *grant || entry.seq != seq {
        return Err(StoreProtocolError::MembershipCoordinateMismatch {
            expected_author: author.to_string(),
            expected_grant: grant.to_string(),
            expected_seq: seq,
            declared_author: entry.author_pubkey,
            declared_grant: entry.author_owner_grant.to_string(),
            declared_seq: entry.seq,
        });
    }
    if seq == 0 {
        return Err(StoreProtocolError::InvalidMembershipCoordinate {
            author: author.to_string(),
            grant: grant.to_string(),
            seq,
            entry_hash: semantic_hash.to_string(),
        });
    }
    if !verify_membership_entry(&entry) {
        return Err(StoreProtocolError::InvalidSignature);
    }
    let canonical_hash = entry_hash(&entry);
    if canonical_hash != semantic_hash {
        return Err(StoreProtocolError::ObjectHashMismatch {
            expected: semantic_hash,
            actual: canonical_hash,
        });
    }
    Ok(entry)
}

fn parse_membership_head_at(
    semantic_hash: ObjectHash,
    author: &str,
    grant: &OwnerGrantId,
    seq: u64,
    bytes: &[u8],
) -> Result<AuthorHead, StoreProtocolError> {
    if ObjectHash::digest(bytes) != semantic_hash {
        return Err(StoreProtocolError::ObjectHashMismatch {
            expected: semantic_hash,
            actual: ObjectHash::digest(bytes),
        });
    }
    let head: AuthorHead = serde_json::from_slice(bytes)
        .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
    if head.author_pubkey != author || head.author_owner_grant != *grant || head.seq != seq {
        return Err(StoreProtocolError::RelocatedSlot {
            expected: membership_head_slot_prefix(author, grant, seq),
            actual: membership_head_slot_prefix(
                &head.author_pubkey,
                &head.author_owner_grant,
                head.seq,
            ),
        });
    }
    if !head.verify() {
        return Err(StoreProtocolError::InvalidSignature);
    }
    Ok(head)
}

pub async fn append_membership_entry_object(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    coord: &MembershipCoord,
    entry: &MembershipEntry,
) -> Result<VerifiedCopies<MembershipEntry>, StoreObjectError> {
    let bytes = serde_json::to_vec(entry)
        .map_err(|error| StoreProtocolError::Malformed(error.to_string()))
        .map_err(|error| StoreObjectError::Collision {
            semantic_prefix: membership_entry_slot_prefix(
                &coord.author_pubkey,
                &coord.author_owner_grant,
                coord.seq,
            ),
            key: membership_entry_slot_prefix(
                &coord.author_pubkey,
                &coord.author_owner_grant,
                coord.seq,
            ),
            reason: error.to_string(),
        })?;
    let semantic_hash = coord.entry_hash;
    parse_membership_entry_at(
        semantic_hash,
        &coord.author_pubkey,
        &coord.author_owner_grant,
        coord.seq,
        &bytes,
    )
    .map_err(|error| StoreObjectError::Collision {
        semantic_prefix: membership_entry_slot_prefix(
            &coord.author_pubkey,
            &coord.author_owner_grant,
            coord.seq,
        ),
        key: membership_entry_slot_prefix(
            &coord.author_pubkey,
            &coord.author_owner_grant,
            coord.seq,
        ),
        reason: error.to_string(),
    })?;
    let semantic_prefix = super::store_commit::membership_entry_semantic_prefix(
        &coord.author_pubkey,
        &coord.author_owner_grant,
        coord.seq,
        semantic_hash,
    );
    append_and_verify(
        storage,
        &ProtocolObjectContext::store(store_root_hash, ProtocolObjectDomain::StoreMembershipEntry),
        &semantic_prefix,
        ".json",
        &bytes,
    )
    .await?;
    load_membership_entry_slot(
        storage,
        store_root_hash,
        &coord.author_pubkey,
        &coord.author_owner_grant,
        coord.seq,
    )
    .await?
    .ok_or_else(|| StorageError::NotFound(semantic_prefix).into())
}

pub async fn load_membership_entry_slot(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    author: &str,
    grant: &OwnerGrantId,
    seq: u64,
) -> Result<Option<VerifiedCopies<MembershipEntry>>, StoreObjectError> {
    let slot = membership_entry_slot_prefix(author, grant, seq);
    load_singleton_slot(
        storage,
        &ProtocolObjectContext::store(store_root_hash, ProtocolObjectDomain::StoreMembershipEntry),
        &slot,
        ".json",
        |key| Ok(parse_membership_entry_copy_key(key)?.semantic_hash),
        |hash, bytes| parse_membership_entry_at(hash, author, grant, seq, bytes),
    )
    .await
}

pub async fn list_membership_entry_objects(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
) -> Result<VerifiedMembershipEntryListing, StoreObjectError> {
    let listing = storage
        .list_protocol_objects("store-v1/membership/entries/")
        .await?;
    let mut slots: BTreeMap<(String, OwnerGrantId, u64), Vec<ProtocolObjectLocator>> =
        BTreeMap::new();
    for object in listing.objects {
        let parsed = parse_membership_entry_copy_key(object.logical_key()).map_err(|error| {
            StoreObjectError::Collision {
                semantic_prefix: "store-v1/membership/entries".to_string(),
                key: object.logical_key().to_string(),
                reason: error.to_string(),
            }
        })?;
        slots
            .entry((parsed.author, parsed.author_owner_grant, parsed.sequence))
            .or_default()
            .push(object);
    }
    let mut entries = Vec::with_capacity(slots.len());
    for ((author, grant, seq), objects) in slots {
        let slot = membership_entry_slot_prefix(&author, &grant, seq);
        if let Some(entry) = load_singleton_candidates(
            storage,
            &ProtocolObjectContext::store(
                store_root_hash,
                ProtocolObjectDomain::StoreMembershipEntry,
            ),
            &slot,
            ".json",
            objects,
            listing.coverage,
            |key| Ok(parse_membership_entry_copy_key(key)?.semantic_hash),
            |hash, bytes| parse_membership_entry_at(hash, &author, &grant, seq, bytes),
        )
        .await?
        {
            entries.push((
                MembershipCoord {
                    author_pubkey: author,
                    author_owner_grant: grant,
                    seq,
                    entry_hash: entry.semantic_hash,
                },
                entry,
            ));
        }
    }
    Ok(VerifiedMembershipEntryListing {
        entries,
        coverage: listing.coverage,
    })
}

pub async fn append_membership_head_object(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    head: &AuthorHead,
) -> Result<VerifiedCopies<AuthorHead>, StoreObjectError> {
    let bytes = serde_json::to_vec(head)
        .map_err(|error| StoreProtocolError::Malformed(error.to_string()))
        .map_err(|error| StoreObjectError::Collision {
            semantic_prefix: membership_head_slot_prefix(
                &head.author_pubkey,
                &head.author_owner_grant,
                head.seq,
            ),
            key: membership_head_slot_prefix(
                &head.author_pubkey,
                &head.author_owner_grant,
                head.seq,
            ),
            reason: error.to_string(),
        })?;
    let semantic_hash = ObjectHash::digest(&bytes);
    parse_membership_head_at(
        semantic_hash,
        &head.author_pubkey,
        &head.author_owner_grant,
        head.seq,
        &bytes,
    )
    .map_err(|error| StoreObjectError::Collision {
        semantic_prefix: membership_head_slot_prefix(
            &head.author_pubkey,
            &head.author_owner_grant,
            head.seq,
        ),
        key: membership_head_slot_prefix(&head.author_pubkey, &head.author_owner_grant, head.seq),
        reason: error.to_string(),
    })?;
    let semantic_prefix = super::store_commit::membership_head_semantic_prefix(
        &head.author_pubkey,
        &head.author_owner_grant,
        head.seq,
        semantic_hash,
    );
    append_and_verify(
        storage,
        &ProtocolObjectContext::store(store_root_hash, ProtocolObjectDomain::StoreMembershipHead),
        &semantic_prefix,
        ".json",
        &bytes,
    )
    .await?;
    load_membership_head_slot(
        storage,
        store_root_hash,
        &head.author_pubkey,
        &head.author_owner_grant,
        head.seq,
    )
    .await?
    .ok_or_else(|| StorageError::NotFound(semantic_prefix).into())
}

pub async fn load_membership_head_slot(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    author: &str,
    grant: &OwnerGrantId,
    seq: u64,
) -> Result<Option<VerifiedCopies<AuthorHead>>, StoreObjectError> {
    let slot = membership_head_slot_prefix(author, grant, seq);
    load_singleton_slot(
        storage,
        &ProtocolObjectContext::store(store_root_hash, ProtocolObjectDomain::StoreMembershipHead),
        &slot,
        ".json",
        |key| Ok(parse_membership_head_copy_key(key)?.semantic_hash),
        |hash, bytes| parse_membership_head_at(hash, author, grant, seq, bytes),
    )
    .await
}

pub async fn list_membership_head_objects(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
) -> Result<VerifiedMembershipHeadListing, StoreObjectError> {
    let listing = storage
        .list_protocol_objects("store-v1/membership/heads/")
        .await?;
    let mut slots: BTreeMap<(String, OwnerGrantId, u64), Vec<ProtocolObjectLocator>> =
        BTreeMap::new();
    for object in listing.objects {
        let parsed = parse_membership_head_copy_key(object.logical_key()).map_err(|error| {
            StoreObjectError::Collision {
                semantic_prefix: "store-v1/membership/heads".to_string(),
                key: object.logical_key().to_string(),
                reason: error.to_string(),
            }
        })?;
        slots
            .entry((parsed.author, parsed.author_owner_grant, parsed.sequence))
            .or_default()
            .push(object);
    }
    let mut heads = Vec::with_capacity(slots.len());
    for ((author, grant, seq), objects) in slots {
        let slot = membership_head_slot_prefix(&author, &grant, seq);
        if let Some(head) = load_singleton_candidates(
            storage,
            &ProtocolObjectContext::store(
                store_root_hash,
                ProtocolObjectDomain::StoreMembershipHead,
            ),
            &slot,
            ".json",
            objects,
            listing.coverage,
            |key| Ok(parse_membership_head_copy_key(key)?.semantic_hash),
            |hash, bytes| parse_membership_head_at(hash, &author, &grant, seq, bytes),
        )
        .await?
        {
            heads.push(head);
        }
    }
    Ok(VerifiedMembershipHeadListing {
        heads,
        coverage: listing.coverage,
    })
}

pub async fn list_snapshot_metas(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
) -> Result<VerifiedSnapshotListing, StoreObjectError> {
    let listing = storage.list_protocol_objects("store-v1/snapshots/").await?;
    let mut groups: BTreeMap<(String, ObjectHash), Vec<ProtocolObjectLocator>> = BTreeMap::new();
    for object in listing.objects {
        let parsed = parse_snapshot_meta_copy_key(object.logical_key()).map_err(|error| {
            StoreObjectError::Collision {
                semantic_prefix: "store-v1/snapshots".to_string(),
                key: object.logical_key().to_string(),
                reason: error.to_string(),
            }
        })?;
        groups
            .entry((parsed.author, parsed.semantic_hash))
            .or_default()
            .push(object);
    }
    let mut metas = Vec::with_capacity(groups.len());
    for ((author, snapshot_hash), objects) in groups {
        let semantic_prefix = super::store_commit::snapshot_semantic_prefix(&author, snapshot_hash);
        let loaded = load_semantic_candidates(
            storage,
            &ProtocolObjectContext::store(store_root_hash, ProtocolObjectDomain::StoreSnapshotMeta),
            &semantic_prefix,
            ".json",
            snapshot_hash,
            objects,
            listing.coverage,
            |bytes| SnapshotMeta::parse_at(bytes, store_root_hash, &author, snapshot_hash),
        )
        .await?;
        if let Some(loaded) = loaded {
            metas.push(loaded);
        }
    }
    Ok(VerifiedSnapshotListing {
        metas,
        coverage: listing.coverage,
    })
}

pub async fn load_snapshot_image(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    author: &str,
    image_hash: ObjectHash,
) -> Result<Option<VerifiedCopies<Vec<u8>>>, StoreObjectError> {
    let semantic_prefix = snapshot_image_semantic_prefix(author, image_hash);
    load_semantic_copies(
        storage,
        &ProtocolObjectContext::store(store_root_hash, ProtocolObjectDomain::StoreSnapshotImage),
        &semantic_prefix,
        ".db",
        image_hash,
        |bytes| {
            if ObjectHash::digest(bytes) != image_hash {
                return Err(StoreProtocolError::ObjectHashMismatch {
                    expected: image_hash,
                    actual: ObjectHash::digest(bytes),
                });
            }
            Ok(bytes.to_vec())
        },
    )
    .await
}

pub async fn list_visible_heads(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
) -> Result<VerifiedHeadListing, StoreObjectError> {
    let listing = storage.list_protocol_objects("store-v1/heads/").await?;
    let mut slots: BTreeMap<(String, u64), Vec<ProtocolObjectLocator>> = BTreeMap::new();
    for object in listing.objects {
        let parsed = parse_head_copy_key(object.logical_key()).map_err(|error| {
            StoreObjectError::Collision {
                semantic_prefix: "store-v1/heads".to_string(),
                key: object.logical_key().to_string(),
                reason: error.to_string(),
            }
        })?;
        slots
            .entry((parsed.owner, parsed.sequence))
            .or_default()
            .push(object);
    }
    let mut heads = Vec::with_capacity(slots.len());
    let mut failures = Vec::new();
    for ((device_id, seq), objects) in slots {
        let slot = head_slot_prefix(&device_id, seq);
        let semantic_hashes = objects
            .iter()
            .map(|object| {
                parse_head_copy_key(object.logical_key())
                    .expect("head group was built from parsed head candidates")
                    .semantic_hash
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        match load_singleton_candidates(
            storage,
            &ProtocolObjectContext::store(store_root_hash, ProtocolObjectDomain::StoreHead),
            &slot,
            ".json",
            objects,
            listing.coverage,
            |key| Ok(parse_head_copy_key(key)?.semantic_hash),
            |semantic_hash, bytes| {
                let head = StoreDeviceHead::parse_at(bytes, store_root_hash, &device_id, seq)?;
                if head.head_hash() != semantic_hash {
                    return Err(StoreProtocolError::ObjectHashMismatch {
                        expected: semantic_hash,
                        actual: head.head_hash(),
                    });
                }
                Ok(head)
            },
        )
        .await
        {
            Ok(Some(head)) => heads.push(head),
            Ok(None) => {}
            Err(error) => failures.push(StoreHeadFailure {
                device_id,
                seq,
                semantic_hashes,
                error,
            }),
        }
    }
    Ok(VerifiedHeadListing {
        heads,
        failures,
        coverage: listing.coverage,
    })
}

pub async fn list_latest_ack_chains(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
) -> Result<VerifiedAckChains, StoreObjectError> {
    let listing = storage.list_protocol_objects("store-v1/acks/").await?;
    let mut slots: BTreeMap<(String, u64), Vec<ProtocolObjectLocator>> = BTreeMap::new();
    for object in listing.objects {
        let parsed = parse_ack_copy_key(object.logical_key()).map_err(|error| {
            StoreObjectError::Collision {
                semantic_prefix: "store-v1/acks".to_string(),
                key: object.logical_key().to_string(),
                reason: error.to_string(),
            }
        })?;
        slots
            .entry((parsed.owner, parsed.sequence))
            .or_default()
            .push(object);
    }
    let mut chains: BTreeMap<String, BTreeMap<u64, VerifiedCopies<StoreAck>>> = BTreeMap::new();
    for ((device_id, revision), objects) in slots {
        let slot = ack_slot_prefix(&device_id, revision);
        if let Some(ack) = load_singleton_candidates(
            storage,
            &ProtocolObjectContext::store(store_root_hash, ProtocolObjectDomain::StoreAck),
            &slot,
            ".json",
            objects,
            listing.coverage,
            |key| Ok(parse_ack_copy_key(key)?.semantic_hash),
            |semantic_hash, bytes| {
                let ack = StoreAck::parse_at(bytes, store_root_hash, &device_id, revision)?;
                if ack.ack_hash() != semantic_hash {
                    return Err(StoreProtocolError::ObjectHashMismatch {
                        expected: semantic_hash,
                        actual: ack.ack_hash(),
                    });
                }
                Ok(ack)
            },
        )
        .await?
        {
            chains.entry(device_id).or_default().insert(revision, ack);
        }
    }
    let mut latest_by_device = BTreeMap::new();
    for (device_id, mut revisions) in chains {
        let max_revision = *revisions
            .keys()
            .next_back()
            .expect("ack chain group is non-empty");
        let mut previous_hash = None;
        for revision in 1..=max_revision {
            let ack =
                revisions
                    .get(&revision)
                    .ok_or_else(|| StoreObjectError::MissingAckRevision {
                        device_id: device_id.clone(),
                        revision,
                    })?;
            if ack.value.previous_ack_hash != previous_hash {
                return Err(StoreObjectError::BrokenAckChain {
                    device_id: device_id.clone(),
                    revision,
                    expected: previous_hash,
                    actual: ack.value.previous_ack_hash,
                });
            }
            previous_hash = Some(ack.semantic_hash);
        }
        latest_by_device.insert(
            device_id,
            revisions
                .remove(&max_revision)
                .expect("latest acknowledgement remains in chain"),
        );
    }
    Ok(VerifiedAckChains {
        latest_by_device,
        coverage: listing.coverage,
    })
}

pub async fn list_latest_registration_chains(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
) -> Result<VerifiedRegistrationChains, StoreObjectError> {
    let listing = storage.list_protocol_objects("store-v1/devices/").await?;
    let mut slots: BTreeMap<(String, u64), Vec<ProtocolObjectLocator>> = BTreeMap::new();
    for object in listing.objects {
        let parsed = parse_registration_copy_key(object.logical_key()).map_err(|error| {
            StoreObjectError::Collision {
                semantic_prefix: "store-v1/devices".to_string(),
                key: object.logical_key().to_string(),
                reason: error.to_string(),
            }
        })?;
        slots
            .entry((parsed.owner, parsed.sequence))
            .or_default()
            .push(object);
    }
    let mut chains: BTreeMap<String, BTreeMap<u64, VerifiedCopies<StoreDeviceRegistration>>> =
        BTreeMap::new();
    for ((device_id, revision), objects) in slots {
        let slot = registration_slot_prefix(&device_id, revision);
        if let Some(registration) = load_singleton_candidates(
            storage,
            &ProtocolObjectContext::store(
                store_root_hash,
                ProtocolObjectDomain::StoreDeviceRegistration,
            ),
            &slot,
            ".json",
            objects,
            listing.coverage,
            |key| Ok(parse_registration_copy_key(key)?.semantic_hash),
            |semantic_hash, bytes| {
                let registration = StoreDeviceRegistration::parse_at(
                    bytes,
                    store_root_hash,
                    &device_id,
                    revision,
                )?;
                if registration.registration_hash() != semantic_hash {
                    return Err(StoreProtocolError::ObjectHashMismatch {
                        expected: semantic_hash,
                        actual: registration.registration_hash(),
                    });
                }
                Ok(registration)
            },
        )
        .await?
        {
            chains
                .entry(device_id)
                .or_default()
                .insert(revision, registration);
        }
    }
    let mut latest_by_device = BTreeMap::new();
    for (device_id, mut revisions) in chains {
        let max_revision = *revisions
            .keys()
            .next_back()
            .expect("registration chain group is non-empty");
        let mut previous_hash = None;
        let mut author: Option<String> = None;
        let mut previous_state = None;
        for revision in 1..=max_revision {
            let registration = revisions.get(&revision).ok_or_else(|| {
                StoreObjectError::MissingRegistrationRevision {
                    device_id: device_id.clone(),
                    revision,
                }
            })?;
            if registration.value.previous_registration_hash != previous_hash {
                return Err(StoreObjectError::BrokenRegistrationChain {
                    device_id: device_id.clone(),
                    revision,
                    expected: previous_hash,
                    actual: registration.value.previous_registration_hash,
                });
            }
            if let Some(expected) = author.as_ref() {
                if registration.value.author_pubkey != *expected {
                    return Err(StoreObjectError::RegistrationAuthorChanged {
                        device_id: device_id.clone(),
                        revision,
                        expected: expected.clone(),
                        actual: registration.value.author_pubkey.clone(),
                    });
                }
            } else {
                author = Some(registration.value.author_pubkey.clone());
            }
            let valid_transition = matches!(
                (previous_state, registration.value.state),
                (None, StoreDeviceRegistrationState::Active)
                    | (
                        Some(StoreDeviceRegistrationState::Active),
                        StoreDeviceRegistrationState::Retired
                    )
            );
            if !valid_transition {
                return Err(StoreObjectError::InvalidRegistrationTransition {
                    device_id: device_id.clone(),
                    revision,
                    previous: previous_state,
                    current: registration.value.state,
                });
            }
            previous_hash = Some(registration.semantic_hash);
            previous_state = Some(registration.value.state);
        }
        latest_by_device.insert(
            device_id,
            revisions
                .remove(&max_revision)
                .expect("latest registration remains in chain"),
        );
    }
    Ok(VerifiedRegistrationChains {
        latest_by_device,
        coverage: listing.coverage,
    })
}

pub async fn load_registration_ref(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    reference: &StoreDeviceRegistrationRef,
) -> Result<Option<VerifiedCopies<StoreDeviceRegistration>>, StoreObjectError> {
    let slot = registration_slot_prefix(&reference.device_id, reference.revision);
    let listing = storage.list_protocol_objects(&format!("{slot}/")).await?;
    let registration = load_singleton_candidates(
        storage,
        &ProtocolObjectContext::store(
            store_root_hash,
            ProtocolObjectDomain::StoreDeviceRegistration,
        ),
        &slot,
        ".json",
        listing.objects,
        listing.coverage,
        |key| Ok(parse_registration_copy_key(key)?.semantic_hash),
        |semantic_hash, bytes| {
            let registration = StoreDeviceRegistration::parse_at(
                bytes,
                store_root_hash,
                &reference.device_id,
                reference.revision,
            )?;
            if registration.registration_hash() != semantic_hash {
                return Err(StoreProtocolError::ObjectHashMismatch {
                    expected: semantic_hash,
                    actual: registration.registration_hash(),
                });
            }
            Ok(registration)
        },
    )
    .await?;
    match registration {
        Some(registration) if registration.semantic_hash == reference.registration_hash => {
            Ok(Some(registration))
        }
        Some(registration) => Err(StoreObjectError::InvalidCandidate {
            semantic_prefix: slot,
            key: registration.copies.first().map_or_else(
                || "registration copy".to_string(),
                |copy| copy.logical_key().to_string(),
            ),
            source: Box::new(StoreProtocolError::DeviceRegistrationRefMismatch {
                device_id: reference.device_id.clone(),
                revision: reference.revision,
                expected: reference.registration_hash,
                actual: registration.semantic_hash,
            }),
        }),
        None => Ok(None),
    }
}

pub async fn list_reclaimable_store_packages(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    snapshot_coverage: &CommitFrontier,
) -> Result<VerifiedPackageListing, StoreObjectError> {
    let mut authoritative_serial_commits = BTreeMap::new();
    if let CommitFrontier::Serial(mut position) = snapshot_coverage.clone() {
        while let Some(expected) = position {
            let commit = load_serial_commit_at_position(storage, store_root_hash, &expected)
                .await?
                .ok_or_else(|| {
                    StoreObjectError::Storage(StorageError::NotFound(commit_semantic_prefix(
                        super::store_commit::SERIAL_STREAM_ID,
                        expected.seq,
                        expected.commit_hash,
                    )))
                })?;
            position = commit
                .value
                .previous_commit_hash()
                .map(|commit_hash| CommitPosition {
                    seq: commit.value.seq() - 1,
                    commit_hash,
                });
            authoritative_serial_commits.insert(expected.seq, commit);
        }
    }
    let listing = storage.list_protocol_objects("store-v1/packages/").await?;
    let mut groups: BTreeMap<(String, u64, ObjectHash), Vec<ProtocolObjectLocator>> =
        BTreeMap::new();
    for object in listing.objects {
        let parsed = parse_package_copy_key(object.logical_key()).map_err(|error| {
            StoreObjectError::Collision {
                semantic_prefix: "store-v1/packages".to_string(),
                key: object.logical_key().to_string(),
                reason: error.to_string(),
            }
        })?;
        groups
            .entry((parsed.owner, parsed.sequence, parsed.semantic_hash))
            .or_default()
            .push(object);
    }
    let mut packages = Vec::with_capacity(groups.len());
    for ((device_id, seq, package_hash), objects) in groups {
        let merge_commit = match snapshot_coverage {
            CommitFrontier::MergeConcurrent(_) => {
                load_commit_slot(storage, store_root_hash, &device_id, seq).await?
            }
            CommitFrontier::Serial(_) => None,
        };
        let commit = match snapshot_coverage {
            CommitFrontier::MergeConcurrent(_) => merge_commit.as_ref().ok_or_else(|| {
                StoreObjectError::Storage(StorageError::NotFound(commit_slot_prefix(
                    &device_id, seq,
                )))
            })?,
            CommitFrontier::Serial(_) if device_id == super::store_commit::SERIAL_STREAM_ID => {
                let Some(commit) = authoritative_serial_commits.get(&seq) else {
                    tracing::debug!(
                        device_id,
                        seq,
                        package_hash = %package_hash,
                        "ignoring Serial package outside snapshot ancestry"
                    );
                    continue;
                };
                let Some(authoritative_package) = commit.value.store_package.as_ref() else {
                    tracing::debug!(
                        device_id,
                        seq,
                        package_hash = %package_hash,
                        "ignoring Serial Store package not named by its authoritative commit"
                    );
                    continue;
                };
                if authoritative_package.content_hash != package_hash {
                    tracing::debug!(
                        device_id,
                        seq,
                        package_hash = %package_hash,
                        authoritative_package_hash = %authoritative_package.content_hash,
                        "ignoring Serial package outside the authoritative commit ancestry"
                    );
                    continue;
                }
                commit
            }
            CommitFrontier::Serial(_) => {
                return Err(StoreObjectError::Collision {
                    semantic_prefix: package_semantic_prefix(&device_id, seq, package_hash),
                    key: package_semantic_prefix(&device_id, seq, package_hash),
                    reason: format!("Serial Store package is in non-serial stream {device_id:?}"),
                });
            }
        };
        let Some(authoritative_package) = commit.value.store_package.as_ref() else {
            return Err(StoreObjectError::Collision {
                semantic_prefix: package_semantic_prefix(&device_id, seq, package_hash),
                key: package_semantic_prefix(&device_id, seq, package_hash),
                reason: "commit names no Store package".to_string(),
            });
        };
        if authoritative_package.content_hash != package_hash {
            return Err(StoreObjectError::Collision {
                semantic_prefix: package_semantic_prefix(&device_id, seq, package_hash),
                key: package_semantic_prefix(&device_id, seq, package_hash),
                reason: format!(
                    "commit names package hash {}, path names {package_hash}",
                    authoritative_package.content_hash
                ),
            });
        }
        let semantic_prefix = package_semantic_prefix(&device_id, seq, package_hash);
        let package = load_semantic_candidates(
            storage,
            &ProtocolObjectContext::store(store_root_hash, ProtocolObjectDomain::StorePackage),
            &semantic_prefix,
            ".pkg",
            package_hash,
            objects,
            listing.coverage,
            |bytes| {
                commit.value.verify_store_package(bytes)?;
                Ok(bytes.to_vec())
            },
        )
        .await?
        .expect("listed package group has candidates");
        packages.push(VerifiedStorePackage {
            device_id,
            seq,
            commit_position: commit.value.position(),
            package,
            commit_coverage: commit.coverage,
        });
    }
    Ok(VerifiedPackageListing {
        packages,
        coverage: listing.coverage,
    })
}

/// Open every physical candidate in one semantic-hash directory. Different
/// ciphertext is expected; every opened plaintext must be the exact same bytes.
pub async fn load_semantic_copies<T>(
    storage: &dyn SyncStorage,
    context: &ProtocolObjectContext,
    semantic_prefix: &str,
    extension: &str,
    semantic_hash: ObjectHash,
    validate: impl Fn(&[u8]) -> Result<T, StoreProtocolError>,
) -> Result<Option<VerifiedCopies<T>>, StoreObjectError> {
    let expected_copy_prefix = format!("{semantic_prefix}/copies/");
    let listing = storage.list_protocol_objects(&expected_copy_prefix).await?;
    load_semantic_candidates(
        storage,
        context,
        semantic_prefix,
        extension,
        semantic_hash,
        listing.objects,
        listing.coverage,
        validate,
    )
    .await
}

async fn load_semantic_candidates<T>(
    storage: &dyn SyncStorage,
    context: &ProtocolObjectContext,
    semantic_prefix: &str,
    extension: &str,
    semantic_hash: ObjectHash,
    objects: Vec<ProtocolObjectLocator>,
    coverage: ListingCoverage,
    validate: impl Fn(&[u8]) -> Result<T, StoreProtocolError>,
) -> Result<Option<VerifiedCopies<T>>, StoreObjectError> {
    let expected_copy_prefix = format!("{semantic_prefix}/copies/");
    let mut canonical: Option<(T, Vec<u8>)> = None;
    let mut copies = Vec::new();
    for object in objects {
        validate_copy_key(&object, &expected_copy_prefix, extension, semantic_prefix)?;
        let bytes = storage
            .read_protocol_object(context, &object, semantic_prefix)
            .await
            .map_err(|source| StoreObjectError::CandidateUnreadable {
                key: object.logical_key().to_string(),
                source,
            })?;
        if let Some((_, canonical_bytes)) = canonical.as_ref() {
            if canonical_bytes != &bytes {
                return Err(StoreObjectError::Collision {
                    semantic_prefix: semantic_prefix.to_string(),
                    key: object.logical_key().to_string(),
                    reason:
                        "opened plaintext differs from another copy under the same semantic hash"
                            .to_string(),
                });
            }
        } else {
            let value = validate(&bytes).map_err(|source| StoreObjectError::InvalidCandidate {
                semantic_prefix: semantic_prefix.to_string(),
                key: object.logical_key().to_string(),
                source: Box::new(source),
            })?;
            canonical = Some((value, bytes));
        }
        copies.push(object);
    }
    Ok(canonical.map(|(value, bytes)| VerifiedCopies {
        value,
        bytes,
        semantic_hash,
        copies,
        coverage,
    }))
}

/// Open every candidate in a singleton semantic slot. Candidates below one
/// hash coalesce only when their plaintext bytes match; two valid hashes are a
/// fail-stop fork.
pub async fn load_singleton_slot<T>(
    storage: &dyn SyncStorage,
    context: &ProtocolObjectContext,
    slot_prefix: &str,
    extension: &str,
    parse_hash: impl Fn(&str) -> Result<ObjectHash, StoreProtocolError>,
    validate: impl Fn(ObjectHash, &[u8]) -> Result<T, StoreProtocolError>,
) -> Result<Option<VerifiedCopies<T>>, StoreObjectError> {
    let listing = storage
        .list_protocol_objects(&format!("{slot_prefix}/"))
        .await?;
    load_singleton_candidates(
        storage,
        context,
        slot_prefix,
        extension,
        listing.objects,
        listing.coverage,
        parse_hash,
        validate,
    )
    .await
}

async fn load_singleton_candidates<T>(
    storage: &dyn SyncStorage,
    context: &ProtocolObjectContext,
    slot_prefix: &str,
    extension: &str,
    objects: Vec<ProtocolObjectLocator>,
    coverage: ListingCoverage,
    parse_hash: impl Fn(&str) -> Result<ObjectHash, StoreProtocolError>,
    validate: impl Fn(ObjectHash, &[u8]) -> Result<T, StoreProtocolError>,
) -> Result<Option<VerifiedCopies<T>>, StoreObjectError> {
    let mut groups: BTreeMap<ObjectHash, (T, Vec<u8>, Vec<ProtocolObjectLocator>)> =
        BTreeMap::new();
    for object in objects {
        let semantic_hash =
            parse_hash(object.logical_key()).map_err(|error| StoreObjectError::Collision {
                semantic_prefix: slot_prefix.to_string(),
                key: object.logical_key().to_string(),
                reason: error.to_string(),
            })?;
        let semantic_prefix = format!("{slot_prefix}/{semantic_hash}");
        validate_copy_key(
            &object,
            &format!("{semantic_prefix}/copies/"),
            extension,
            &semantic_prefix,
        )?;
        let bytes = storage
            .read_protocol_object(context, &object, &semantic_prefix)
            .await
            .map_err(|source| StoreObjectError::CandidateUnreadable {
                key: object.logical_key().to_string(),
                source,
            })?;
        match groups.get_mut(&semantic_hash) {
            Some((_, canonical_bytes, copies)) => {
                if canonical_bytes != &bytes {
                    return Err(StoreObjectError::Collision {
                        semantic_prefix,
                        key: object.logical_key().to_string(),
                        reason: "opened plaintext differs from another copy under the same semantic hash"
                            .to_string(),
                    });
                }
                copies.push(object);
            }
            None => {
                let value = validate(semantic_hash, &bytes).map_err(|source| {
                    StoreObjectError::InvalidCandidate {
                        semantic_prefix: semantic_prefix.clone(),
                        key: object.logical_key().to_string(),
                        source: Box::new(source),
                    }
                })?;
                groups.insert(semantic_hash, (value, bytes, vec![object]));
            }
        }
    }
    if groups.len() > 1 {
        return Err(StoreObjectError::SemanticFork {
            slot: slot_prefix.to_string(),
            hashes: groups.keys().copied().collect(),
        });
    }
    Ok(groups
        .into_iter()
        .next()
        .map(|(semantic_hash, (value, bytes, copies))| VerifiedCopies {
            value,
            bytes,
            semantic_hash,
            copies,
            coverage,
        }))
}

fn validate_copy_key(
    object: &ProtocolObjectLocator,
    expected_prefix: &str,
    extension: &str,
    semantic_prefix: &str,
) -> Result<(), StoreObjectError> {
    let key = object.logical_key();
    let Some(copy) = key
        .strip_prefix(expected_prefix)
        .and_then(|filename| filename.strip_suffix(extension))
    else {
        return Err(StoreObjectError::Collision {
            semantic_prefix: semantic_prefix.to_string(),
            key: key.to_string(),
            reason: "candidate is not in the canonical copies path".to_string(),
        });
    };
    if copy.contains('/') || copy.parse::<crate::storage::cloud::CopyId>().is_err() {
        return Err(StoreObjectError::Collision {
            semantic_prefix: semantic_prefix.to_string(),
            key: key.to_string(),
            reason: "candidate has a non-canonical copy id".to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::keys::UserKeypair;
    use crate::storage::cloud::test_utils::InMemoryCloudHome;
    use crate::storage::cloud::SequentialCopyIdGenerator;
    use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
    use crate::sync::store_commit::{
        commit_slot_prefix, parse_commit_copy_key, ObjectHash, StoreProtocolError,
    };

    fn storage(home: &InMemoryCloudHome) -> CloudSyncStorage {
        CloudSyncStorage::new(
            Arc::new(home.clone()),
            CloudCipher::Plaintext,
            BlobPathScheme::Plain,
            "store-object-tests",
            UserKeypair::generate(),
        )
        .with_copy_ids(Arc::new(SequentialCopyIdGenerator::new("store-copy")))
    }

    fn validate_digest(expected: ObjectHash, bytes: &[u8]) -> Result<Vec<u8>, StoreProtocolError> {
        if ObjectHash::digest(bytes) != expected {
            return Err(StoreProtocolError::PackageHashMismatch {
                expected,
                actual: ObjectHash::digest(bytes),
            });
        }
        Ok(bytes.to_vec())
    }

    #[tokio::test]
    async fn identical_retry_copies_coalesce() {
        let home = InMemoryCloudHome::new();
        let storage = storage(&home);
        let bytes = b"semantic bytes";
        let hash = ObjectHash::digest(bytes);
        let prefix = format!("store-v1/store-protocol-root/{hash}");
        let context = ProtocolObjectContext::store(hash, ProtocolObjectDomain::StoreProtocolRoot);
        append_and_verify(&storage, &context, &prefix, ".json", bytes)
            .await
            .unwrap();
        append_and_verify(&storage, &context, &prefix, ".json", bytes)
            .await
            .unwrap();

        let loaded =
            load_semantic_copies(&storage, &context, &prefix, ".json", hash, |candidate| {
                validate_digest(hash, candidate)
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.bytes, bytes);
        assert_eq!(loaded.copies.len(), 2);
    }

    #[tokio::test]
    async fn late_bad_candidate_fails_the_whole_semantic_hash() {
        let home = InMemoryCloudHome::new();
        let storage = storage(&home);
        let bytes = b"semantic bytes";
        let hash = ObjectHash::digest(bytes);
        let prefix = format!("store-v1/store-protocol-root/{hash}");
        let context = ProtocolObjectContext::store(hash, ProtocolObjectDomain::StoreProtocolRoot);
        append_and_verify(&storage, &context, &prefix, ".json", bytes)
            .await
            .unwrap();
        let bad_copy = crate::storage::cloud::CopyId::random();
        home.insert_appended_candidate(
            &format!("{prefix}/copies/{bad_copy}.json"),
            b"different bytes".to_vec(),
        );

        let error = load_semantic_copies(&storage, &context, &prefix, ".json", hash, |candidate| {
            validate_digest(hash, candidate)
        })
        .await
        .expect_err("one late bad candidate must fail-stop");
        assert!(matches!(error, StoreObjectError::Collision { .. }));
    }

    #[tokio::test]
    async fn two_valid_hashes_in_one_commit_slot_are_a_semantic_fork() {
        let home = InMemoryCloudHome::new();
        let storage = storage(&home);
        let slot = commit_slot_prefix("device-a", 1);
        let context = ProtocolObjectContext::store(
            ObjectHash::digest(b"store root"),
            ProtocolObjectDomain::StoreCommit,
        );
        for bytes in [b"first".as_slice(), b"second".as_slice()] {
            let hash = ObjectHash::digest(bytes);
            append_and_verify(
                &storage,
                &context,
                &format!("{slot}/{hash}"),
                ".json",
                bytes,
            )
            .await
            .unwrap();
        }

        let error = load_singleton_slot(
            &storage,
            &context,
            &slot,
            ".json",
            |key| Ok(parse_commit_copy_key(key)?.semantic_hash),
            validate_digest,
        )
        .await
        .expect_err("two valid semantic hashes must fail-stop");
        assert!(matches!(error, StoreObjectError::SemanticFork { .. }));
    }
}

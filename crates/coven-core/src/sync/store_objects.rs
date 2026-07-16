//! Verification for append-only physical copies of Store protocol objects.

use std::collections::{BTreeMap, BTreeSet};

use crate::storage::cloud::ListingCoverage;

use super::circle::{circle_semantic_prefix, CircleId, CircleSemanticSlot};
use super::circle_roster::{CircleRosterConflictResolution, CircleRosterConflictResolutionRef};

use super::membership::{
    entry_hash, verify_membership_entry, AuthorHead, AuthorStreamId, MembershipCoord,
    MembershipEntry, MembershipGrantId, StoreMembershipConflictResolution,
    StoreMembershipConflictResolutionRef,
};
use super::storage::{
    ImmutableObjectLocator, ProtocolObjectContext, ProtocolObjectDomain, StorageError, SyncStorage,
};
use super::store_commit::{
    ack_slot_prefix, commit_semantic_prefix, commit_slot_prefix, head_slot_prefix,
    package_semantic_prefix, parse_ack_copy_key, parse_commit_copy_key, parse_head_copy_key,
    parse_membership_entry_copy_key, parse_membership_head_copy_key, parse_package_copy_key,
    parse_registration_copy_key, parse_snapshot_meta_copy_key, parse_store_protocol_root_copy_key,
    registration_slot_prefix, snapshot_image_semantic_prefix, CommitFrontier, CommitPosition,
    MembershipCopySlot, SnapshotMeta, StoreAck, StoreBatchCommit, StoreDeviceHead,
    StoreDeviceRegistration, StoreDeviceRegistrationRef, StoreDeviceRegistrationState,
    StoreProtocolRoot, STORE_ACK_PREFIX, STORE_DEVICE_REGISTRATION_PREFIX, STORE_HEAD_PREFIX,
    STORE_MEMBERSHIP_ENTRY_PREFIX, STORE_MEMBERSHIP_HEAD_PREFIX, STORE_PACKAGE_PREFIX,
    STORE_PROTOCOL_ROOT_PREFIX, STORE_SNAPSHOT_META_PREFIX,
};
use super::store_commit::{ObjectHash, StoreProtocolError};

#[derive(Debug)]
pub struct VerifiedCopies<T> {
    pub value: T,
    pub bytes: Vec<u8>,
    pub semantic_hash: ObjectHash,
    pub copies: Vec<ImmutableObjectLocator>,
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

#[derive(Debug)]
pub struct VerifiedMembershipResolutionListing {
    pub resolutions: Vec<VerifiedCopies<StoreMembershipConflictResolution>>,
    pub coverage: ListingCoverage,
}

#[derive(Debug)]
pub struct VerifiedCircleRosterResolutionListing {
    pub resolutions: Vec<VerifiedCopies<CircleRosterConflictResolution>>,
    pub coverage: ListingCoverage,
}

const STORE_MEMBERSHIP_RESOLUTION_PREFIX: &str = "store-v1/membership/resolutions/";

fn membership_resolution_semantic_prefix(
    reference: &StoreMembershipConflictResolutionRef,
) -> String {
    format!(
        "{STORE_MEMBERSHIP_RESOLUTION_PREFIX}{}/{}/{}",
        reference.conflict_hash, reference.resolver_pubkey, reference.resolution_hash
    )
}

fn parse_resolution_copy_key_parts(
    key: &str,
    prefix: &str,
) -> Result<(ObjectHash, String, ObjectHash), StoreProtocolError> {
    let relative = key
        .strip_prefix(prefix)
        .ok_or_else(|| StoreProtocolError::MalformedPath(key.to_string()))?;
    let segments = relative.split('/').collect::<Vec<_>>();
    if segments.len() != 5
        || segments[1].is_empty()
        || segments[3] != "copies"
        || !segments[4].ends_with(".json")
    {
        return Err(StoreProtocolError::MalformedPath(key.to_string()));
    }
    Ok((
        segments[0]
            .parse()
            .map_err(|_| StoreProtocolError::MalformedPath(key.to_string()))?,
        segments[1].to_string(),
        segments[2]
            .parse()
            .map_err(|_| StoreProtocolError::MalformedPath(key.to_string()))?,
    ))
}

fn parse_membership_resolution_copy_key(
    key: &str,
) -> Result<StoreMembershipConflictResolutionRef, StoreProtocolError> {
    let (conflict_hash, resolver_pubkey, resolution_hash) =
        parse_resolution_copy_key_parts(key, STORE_MEMBERSHIP_RESOLUTION_PREFIX)?;
    Ok(StoreMembershipConflictResolutionRef {
        conflict_hash,
        resolver_pubkey,
        resolution_hash,
    })
}

type ResolutionCopyObjectGroups<R> = BTreeMap<R, Vec<ImmutableObjectLocator>>;

fn group_resolution_copy_objects<R: Ord>(
    objects: Vec<ImmutableObjectLocator>,
    prefix: &str,
    parse: impl Fn(&str) -> Result<R, StoreProtocolError>,
) -> Result<ResolutionCopyObjectGroups<R>, StoreObjectError> {
    let mut groups = BTreeMap::new();
    for object in objects {
        let reference =
            parse(object.logical_key()).map_err(|error| StoreObjectError::Collision {
                semantic_prefix: prefix.trim_end_matches('/').to_string(),
                key: object.logical_key().to_string(),
                reason: error.to_string(),
            })?;
        groups
            .entry(reference)
            .or_insert_with(Vec::new)
            .push(object);
    }
    Ok(groups)
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
) -> Result<ImmutableObjectLocator, StoreObjectError> {
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
        .list_protocol_objects(STORE_PROTOCOL_ROOT_PREFIX)
        .await?;
    let mut groups: BTreeMap<ObjectHash, Vec<ImmutableObjectLocator>> = BTreeMap::new();
    for object in listing.objects {
        let (hash, _) =
            parse_store_protocol_root_copy_key(object.logical_key()).map_err(|error| {
                StoreObjectError::Collision {
                    semantic_prefix: STORE_PROTOCOL_ROOT_PREFIX.trim_end_matches('/').to_string(),
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
            STORE_PROTOCOL_ROOT_PREFIX.trim_end_matches('/').to_string(),
        ))),
        _ => Err(StoreObjectError::SemanticFork {
            slot: STORE_PROTOCOL_ROOT_PREFIX.trim_end_matches('/').to_string(),
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
        package.content_hash,
        |bytes| {
            commit.verify_store_package(bytes)?;
            Ok(bytes.to_vec())
        },
    )
    .await
}

fn membership_entry_slot_prefix(
    author: &str,
    grant: &MembershipGrantId,
    stream_id: AuthorStreamId,
    seq: u64,
) -> String {
    format!("{STORE_MEMBERSHIP_ENTRY_PREFIX}{author}/{grant}/{stream_id}/{seq}")
}

fn membership_head_slot_prefix(
    author: &str,
    grant: &MembershipGrantId,
    stream_id: AuthorStreamId,
    seq: u64,
) -> String {
    format!("{STORE_MEMBERSHIP_HEAD_PREFIX}{author}/{grant}/{stream_id}/{seq}")
}

type MembershipCopyGroups =
    BTreeMap<(String, MembershipGrantId, AuthorStreamId, u64), Vec<ImmutableObjectLocator>>;

fn group_membership_copy_slots(
    objects: Vec<ImmutableObjectLocator>,
    prefix: &str,
    parser: fn(&str) -> Result<MembershipCopySlot, StoreProtocolError>,
) -> Result<MembershipCopyGroups, StoreObjectError> {
    let mut slots: MembershipCopyGroups = BTreeMap::new();
    for object in objects {
        let parsed = parser(object.logical_key()).map_err(|error| StoreObjectError::Collision {
            semantic_prefix: prefix.trim_end_matches('/').to_string(),
            key: object.logical_key().to_string(),
            reason: error.to_string(),
        })?;
        slots
            .entry((
                parsed.author,
                parsed.author_owner_grant,
                parsed.stream_id,
                parsed.sequence,
            ))
            .or_default()
            .push(object);
    }
    Ok(slots)
}

fn parse_membership_entry_at(
    semantic_hash: ObjectHash,
    author: &str,
    grant: &MembershipGrantId,
    stream_id: AuthorStreamId,
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
    if entry.author_pubkey != author
        || entry.author_owner_grant != *grant
        || entry.stream_id != stream_id
        || entry.seq != seq
    {
        return Err(StoreProtocolError::MembershipCoordinateMismatch {
            expected: Box::new(MembershipCoord {
                author_pubkey: author.to_string(),
                author_owner_grant: grant.clone(),
                stream_id,
                seq,
                entry_hash: semantic_hash,
            }),
            declared: Box::new(entry.coord()),
        });
    }
    if seq == 0 {
        return Err(StoreProtocolError::InvalidMembershipCoordinate {
            author: author.to_string(),
            grant: grant.to_string(),
            stream_id: stream_id.to_string(),
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
    grant: &MembershipGrantId,
    stream_id: AuthorStreamId,
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
    if head.author_pubkey != author
        || head.author_owner_grant != *grant
        || head.stream_id != stream_id
        || head.seq != seq
    {
        return Err(StoreProtocolError::RelocatedSlot {
            expected: membership_head_slot_prefix(author, grant, stream_id, seq),
            actual: membership_head_slot_prefix(
                &head.author_pubkey,
                &head.author_owner_grant,
                head.stream_id,
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
                coord.stream_id,
                coord.seq,
            ),
            key: membership_entry_slot_prefix(
                &coord.author_pubkey,
                &coord.author_owner_grant,
                coord.stream_id,
                coord.seq,
            ),
            reason: error.to_string(),
        })?;
    let semantic_hash = coord.entry_hash;
    parse_membership_entry_at(
        semantic_hash,
        &coord.author_pubkey,
        &coord.author_owner_grant,
        coord.stream_id,
        coord.seq,
        &bytes,
    )
    .map_err(|error| StoreObjectError::Collision {
        semantic_prefix: membership_entry_slot_prefix(
            &coord.author_pubkey,
            &coord.author_owner_grant,
            coord.stream_id,
            coord.seq,
        ),
        key: membership_entry_slot_prefix(
            &coord.author_pubkey,
            &coord.author_owner_grant,
            coord.stream_id,
            coord.seq,
        ),
        reason: error.to_string(),
    })?;
    let semantic_prefix = super::store_commit::membership_entry_semantic_prefix(
        &coord.author_pubkey,
        &coord.author_owner_grant,
        coord.stream_id,
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
        coord.stream_id,
        coord.seq,
    )
    .await?
    .ok_or_else(|| StorageError::NotFound(semantic_prefix).into())
}

pub async fn load_membership_entry_slot(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    author: &str,
    grant: &MembershipGrantId,
    stream_id: AuthorStreamId,
    seq: u64,
) -> Result<Option<VerifiedCopies<MembershipEntry>>, StoreObjectError> {
    let slot = membership_entry_slot_prefix(author, grant, stream_id, seq);
    load_singleton_slot(
        storage,
        &ProtocolObjectContext::store(store_root_hash, ProtocolObjectDomain::StoreMembershipEntry),
        &slot,
        |key| Ok(parse_membership_entry_copy_key(key)?.semantic_hash),
        |hash, bytes| parse_membership_entry_at(hash, author, grant, stream_id, seq, bytes),
    )
    .await
}

pub async fn list_membership_entry_objects(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
) -> Result<VerifiedMembershipEntryListing, StoreObjectError> {
    let listing = storage
        .list_protocol_objects(STORE_MEMBERSHIP_ENTRY_PREFIX)
        .await?;
    let slots = group_membership_copy_slots(
        listing.objects,
        STORE_MEMBERSHIP_ENTRY_PREFIX,
        parse_membership_entry_copy_key,
    )?;
    let mut entries = Vec::with_capacity(slots.len());
    for ((author, grant, stream_id, seq), objects) in slots {
        let slot = membership_entry_slot_prefix(&author, &grant, stream_id, seq);
        if let Some(entry) = load_singleton_candidates(
            storage,
            &ProtocolObjectContext::store(
                store_root_hash,
                ProtocolObjectDomain::StoreMembershipEntry,
            ),
            &slot,
            objects,
            listing.coverage,
            |key| Ok(parse_membership_entry_copy_key(key)?.semantic_hash),
            |hash, bytes| parse_membership_entry_at(hash, &author, &grant, stream_id, seq, bytes),
        )
        .await?
        {
            entries.push((
                MembershipCoord {
                    author_pubkey: author,
                    author_owner_grant: grant,
                    stream_id,
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
                head.stream_id,
                head.seq,
            ),
            key: membership_head_slot_prefix(
                &head.author_pubkey,
                &head.author_owner_grant,
                head.stream_id,
                head.seq,
            ),
            reason: error.to_string(),
        })?;
    let semantic_hash = ObjectHash::digest(&bytes);
    parse_membership_head_at(
        semantic_hash,
        &head.author_pubkey,
        &head.author_owner_grant,
        head.stream_id,
        head.seq,
        &bytes,
    )
    .map_err(|error| StoreObjectError::Collision {
        semantic_prefix: membership_head_slot_prefix(
            &head.author_pubkey,
            &head.author_owner_grant,
            head.stream_id,
            head.seq,
        ),
        key: membership_head_slot_prefix(
            &head.author_pubkey,
            &head.author_owner_grant,
            head.stream_id,
            head.seq,
        ),
        reason: error.to_string(),
    })?;
    let semantic_prefix = super::store_commit::membership_head_semantic_prefix(
        &head.author_pubkey,
        &head.author_owner_grant,
        head.stream_id,
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
        head.stream_id,
        head.seq,
    )
    .await?
    .ok_or_else(|| StorageError::NotFound(semantic_prefix).into())
}

pub async fn load_membership_head_slot(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    author: &str,
    grant: &MembershipGrantId,
    stream_id: AuthorStreamId,
    seq: u64,
) -> Result<Option<VerifiedCopies<AuthorHead>>, StoreObjectError> {
    let slot = membership_head_slot_prefix(author, grant, stream_id, seq);
    load_singleton_slot(
        storage,
        &ProtocolObjectContext::store(store_root_hash, ProtocolObjectDomain::StoreMembershipHead),
        &slot,
        |key| Ok(parse_membership_head_copy_key(key)?.semantic_hash),
        |hash, bytes| parse_membership_head_at(hash, author, grant, stream_id, seq, bytes),
    )
    .await
}

pub async fn list_membership_head_objects(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
) -> Result<VerifiedMembershipHeadListing, StoreObjectError> {
    let listing = storage
        .list_protocol_objects(STORE_MEMBERSHIP_HEAD_PREFIX)
        .await?;
    let slots = group_membership_copy_slots(
        listing.objects,
        STORE_MEMBERSHIP_HEAD_PREFIX,
        parse_membership_head_copy_key,
    )?;
    let mut heads = Vec::with_capacity(slots.len());
    for ((author, grant, stream_id, seq), objects) in slots {
        let slot = membership_head_slot_prefix(&author, &grant, stream_id, seq);
        if let Some(head) = load_singleton_candidates(
            storage,
            &ProtocolObjectContext::store(
                store_root_hash,
                ProtocolObjectDomain::StoreMembershipHead,
            ),
            &slot,
            objects,
            listing.coverage,
            |key| Ok(parse_membership_head_copy_key(key)?.semantic_hash),
            |hash, bytes| parse_membership_head_at(hash, &author, &grant, stream_id, seq, bytes),
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

fn parse_membership_resolution_at(
    store_root_hash: ObjectHash,
    reference: &StoreMembershipConflictResolutionRef,
    bytes: &[u8],
) -> Result<StoreMembershipConflictResolution, StoreProtocolError> {
    let resolution: StoreMembershipConflictResolution = serde_json::from_slice(bytes)
        .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
    if resolution.store_root_hash != store_root_hash
        || resolution.resolution_ref() != *reference
        || resolution.resolution_hash() != reference.resolution_hash
        || !resolution.verify_signature()
    {
        return Err(StoreProtocolError::InvalidSignature);
    }
    Ok(resolution)
}

pub async fn append_membership_resolution_object(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    resolution: &StoreMembershipConflictResolution,
) -> Result<VerifiedCopies<StoreMembershipConflictResolution>, StoreObjectError> {
    let reference = resolution.resolution_ref();
    let semantic_prefix = membership_resolution_semantic_prefix(&reference);
    let bytes = serde_json::to_vec(resolution)
        .map_err(|error| StoreProtocolError::Malformed(error.to_string()))
        .map_err(|error| StoreObjectError::Collision {
            semantic_prefix: semantic_prefix.clone(),
            key: semantic_prefix.clone(),
            reason: error.to_string(),
        })?;
    parse_membership_resolution_at(store_root_hash, &reference, &bytes).map_err(|error| {
        StoreObjectError::Collision {
            semantic_prefix: semantic_prefix.clone(),
            key: semantic_prefix.clone(),
            reason: error.to_string(),
        }
    })?;
    append_and_verify(
        storage,
        &ProtocolObjectContext::store(
            store_root_hash,
            ProtocolObjectDomain::StoreMembershipResolution,
        ),
        &semantic_prefix,
        ".json",
        &bytes,
    )
    .await?;
    load_membership_resolution_object(storage, store_root_hash, &reference)
        .await?
        .ok_or_else(|| StorageError::NotFound(semantic_prefix).into())
}

pub async fn load_membership_resolution_object(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    reference: &StoreMembershipConflictResolutionRef,
) -> Result<Option<VerifiedCopies<StoreMembershipConflictResolution>>, StoreObjectError> {
    let semantic_prefix = membership_resolution_semantic_prefix(reference);
    load_semantic_copies(
        storage,
        &ProtocolObjectContext::store(
            store_root_hash,
            ProtocolObjectDomain::StoreMembershipResolution,
        ),
        &semantic_prefix,
        reference.resolution_hash,
        |bytes| parse_membership_resolution_at(store_root_hash, reference, bytes),
    )
    .await
}

pub async fn list_membership_resolution_objects(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
) -> Result<VerifiedMembershipResolutionListing, StoreObjectError> {
    let listing = storage
        .list_protocol_objects(STORE_MEMBERSHIP_RESOLUTION_PREFIX)
        .await?;
    let groups = group_resolution_copy_objects(
        listing.objects,
        STORE_MEMBERSHIP_RESOLUTION_PREFIX,
        parse_membership_resolution_copy_key,
    )?;
    let mut resolutions = Vec::with_capacity(groups.len());
    for (reference, objects) in groups {
        let semantic_prefix = membership_resolution_semantic_prefix(&reference);
        if let Some(resolution) = load_semantic_candidates(
            storage,
            &ProtocolObjectContext::store(
                store_root_hash,
                ProtocolObjectDomain::StoreMembershipResolution,
            ),
            &semantic_prefix,
            reference.resolution_hash,
            objects,
            listing.coverage,
            |bytes| parse_membership_resolution_at(store_root_hash, &reference, bytes),
        )
        .await?
        {
            resolutions.push(resolution);
        }
    }
    Ok(VerifiedMembershipResolutionListing {
        resolutions,
        coverage: listing.coverage,
    })
}

fn parse_circle_roster_resolution_copy_key(
    key: &str,
    circle_id: CircleId,
) -> Result<CircleRosterConflictResolutionRef, StoreProtocolError> {
    let prefix = format!("circles/{circle_id}/roster/resolutions/");
    let (conflict_hash, resolver_pubkey, resolution_hash) =
        parse_resolution_copy_key_parts(key, &prefix)?;
    Ok(CircleRosterConflictResolutionRef {
        conflict_hash,
        resolver_pubkey,
        resolution_hash,
    })
}

fn parse_circle_roster_resolution_at(
    store_root_hash: ObjectHash,
    circle_id: CircleId,
    reference: &CircleRosterConflictResolutionRef,
    bytes: &[u8],
) -> Result<CircleRosterConflictResolution, StoreProtocolError> {
    let resolution: CircleRosterConflictResolution = serde_json::from_slice(bytes)
        .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
    if resolution.store_root_hash != store_root_hash
        || resolution.circle_id != circle_id
        || resolution.resolution_ref() != *reference
        || resolution.resolution_hash() != reference.resolution_hash
        || !resolution.verify_signature()
    {
        return Err(StoreProtocolError::InvalidSignature);
    }
    Ok(resolution)
}

pub async fn append_circle_roster_resolution_object(
    storage: &dyn SyncStorage,
    encryption: crate::encryption::EncryptionService,
    resolution: &CircleRosterConflictResolution,
) -> Result<VerifiedCopies<CircleRosterConflictResolution>, StoreObjectError> {
    let reference = resolution.resolution_ref();
    let semantic_prefix = circle_semantic_prefix(CircleSemanticSlot::RosterResolution {
        circle_id: resolution.circle_id,
        resolution: &reference,
    });
    let bytes = serde_json::to_vec(resolution)
        .map_err(|error| StoreProtocolError::Malformed(error.to_string()))
        .map_err(|error| StoreObjectError::Collision {
            semantic_prefix: semantic_prefix.clone(),
            key: semantic_prefix.clone(),
            reason: error.to_string(),
        })?;
    parse_circle_roster_resolution_at(
        resolution.store_root_hash,
        resolution.circle_id,
        &reference,
        &bytes,
    )
    .map_err(|error| StoreObjectError::Collision {
        semantic_prefix: semantic_prefix.clone(),
        key: semantic_prefix.clone(),
        reason: error.to_string(),
    })?;
    append_and_verify(
        storage,
        &ProtocolObjectContext::circle(
            resolution.store_root_hash,
            ProtocolObjectDomain::CircleRosterResolution,
            encryption.clone(),
        ),
        &semantic_prefix,
        ".json",
        &bytes,
    )
    .await?;
    load_circle_roster_resolution_object(
        storage,
        resolution.store_root_hash,
        resolution.circle_id,
        encryption,
        &reference,
    )
    .await?
    .ok_or_else(|| StorageError::NotFound(semantic_prefix).into())
}

pub async fn load_circle_roster_resolution_object(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    circle_id: CircleId,
    encryption: crate::encryption::EncryptionService,
    reference: &CircleRosterConflictResolutionRef,
) -> Result<Option<VerifiedCopies<CircleRosterConflictResolution>>, StoreObjectError> {
    let semantic_prefix = circle_semantic_prefix(CircleSemanticSlot::RosterResolution {
        circle_id,
        resolution: reference,
    });
    load_semantic_copies(
        storage,
        &ProtocolObjectContext::circle(
            store_root_hash,
            ProtocolObjectDomain::CircleRosterResolution,
            encryption,
        ),
        &semantic_prefix,
        reference.resolution_hash,
        |bytes| parse_circle_roster_resolution_at(store_root_hash, circle_id, reference, bytes),
    )
    .await
}

pub async fn list_circle_roster_resolution_objects(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    circle_id: CircleId,
    encryption: crate::encryption::EncryptionService,
) -> Result<VerifiedCircleRosterResolutionListing, StoreObjectError> {
    let listing_prefix = format!("circles/{circle_id}/roster/resolutions/");
    let listing = storage.list_protocol_objects(&listing_prefix).await?;
    let groups = group_resolution_copy_objects(listing.objects, &listing_prefix, |key| {
        parse_circle_roster_resolution_copy_key(key, circle_id)
    })?;
    let context = ProtocolObjectContext::circle(
        store_root_hash,
        ProtocolObjectDomain::CircleRosterResolution,
        encryption,
    );
    let mut resolutions = Vec::with_capacity(groups.len());
    for (reference, objects) in groups {
        let semantic_prefix = circle_semantic_prefix(CircleSemanticSlot::RosterResolution {
            circle_id,
            resolution: &reference,
        });
        if let Some(resolution) = load_semantic_candidates(
            storage,
            &context,
            &semantic_prefix,
            reference.resolution_hash,
            objects,
            listing.coverage,
            |bytes| {
                parse_circle_roster_resolution_at(store_root_hash, circle_id, &reference, bytes)
            },
        )
        .await?
        {
            resolutions.push(resolution);
        }
    }
    Ok(VerifiedCircleRosterResolutionListing {
        resolutions,
        coverage: listing.coverage,
    })
}

pub async fn list_snapshot_metas(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
) -> Result<VerifiedSnapshotListing, StoreObjectError> {
    let listing = storage
        .list_protocol_objects(STORE_SNAPSHOT_META_PREFIX)
        .await?;
    let mut groups: BTreeMap<(String, ObjectHash), Vec<ImmutableObjectLocator>> = BTreeMap::new();
    for object in listing.objects {
        let parsed = parse_snapshot_meta_copy_key(object.logical_key()).map_err(|error| {
            StoreObjectError::Collision {
                semantic_prefix: STORE_SNAPSHOT_META_PREFIX.trim_end_matches('/').to_string(),
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
    let listing = storage.list_protocol_objects(STORE_HEAD_PREFIX).await?;
    let mut slots: BTreeMap<(String, u64), Vec<ImmutableObjectLocator>> = BTreeMap::new();
    for object in listing.objects {
        let parsed = parse_head_copy_key(object.logical_key()).map_err(|error| {
            StoreObjectError::Collision {
                semantic_prefix: STORE_HEAD_PREFIX.trim_end_matches('/').to_string(),
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
    let listing = storage.list_protocol_objects(STORE_ACK_PREFIX).await?;
    let mut slots: BTreeMap<(String, u64), Vec<ImmutableObjectLocator>> = BTreeMap::new();
    for object in listing.objects {
        let parsed = parse_ack_copy_key(object.logical_key()).map_err(|error| {
            StoreObjectError::Collision {
                semantic_prefix: STORE_ACK_PREFIX.trim_end_matches('/').to_string(),
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
    let listing = storage
        .list_protocol_objects(STORE_DEVICE_REGISTRATION_PREFIX)
        .await?;
    let mut slots: BTreeMap<(String, u64), Vec<ImmutableObjectLocator>> = BTreeMap::new();
    for object in listing.objects {
        let parsed = parse_registration_copy_key(object.logical_key()).map_err(|error| {
            StoreObjectError::Collision {
                semantic_prefix: STORE_DEVICE_REGISTRATION_PREFIX
                    .trim_end_matches('/')
                    .to_string(),
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
    let listing = storage.list_protocol_objects(STORE_PACKAGE_PREFIX).await?;
    let mut groups: BTreeMap<(String, u64, ObjectHash), Vec<ImmutableObjectLocator>> =
        BTreeMap::new();
    for object in listing.objects {
        let parsed = parse_package_copy_key(object.logical_key()).map_err(|error| {
            StoreObjectError::Collision {
                semantic_prefix: STORE_PACKAGE_PREFIX.trim_end_matches('/').to_string(),
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
    semantic_hash: ObjectHash,
    validate: impl Fn(&[u8]) -> Result<T, StoreProtocolError>,
) -> Result<Option<VerifiedCopies<T>>, StoreObjectError> {
    let expected_copy_prefix = format!("{semantic_prefix}/copies/");
    let listing = storage.list_protocol_objects(&expected_copy_prefix).await?;
    load_semantic_candidates(
        storage,
        context,
        semantic_prefix,
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
    semantic_hash: ObjectHash,
    objects: Vec<ImmutableObjectLocator>,
    coverage: ListingCoverage,
    validate: impl Fn(&[u8]) -> Result<T, StoreProtocolError>,
) -> Result<Option<VerifiedCopies<T>>, StoreObjectError> {
    let mut canonical: Option<(T, Vec<u8>)> = None;
    let mut copies = Vec::new();
    for object in objects {
        context
            .validate_locator(&object, semantic_prefix)
            .map_err(|error| StoreObjectError::Collision {
                semantic_prefix: semantic_prefix.to_string(),
                key: object.logical_key().to_string(),
                reason: error.to_string(),
            })?;
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
    objects: Vec<ImmutableObjectLocator>,
    coverage: ListingCoverage,
    parse_hash: impl Fn(&str) -> Result<ObjectHash, StoreProtocolError>,
    validate: impl Fn(ObjectHash, &[u8]) -> Result<T, StoreProtocolError>,
) -> Result<Option<VerifiedCopies<T>>, StoreObjectError> {
    let mut groups: BTreeMap<ObjectHash, (T, Vec<u8>, Vec<ImmutableObjectLocator>)> =
        BTreeMap::new();
    for object in objects {
        let semantic_hash =
            parse_hash(object.logical_key()).map_err(|error| StoreObjectError::Collision {
                semantic_prefix: slot_prefix.to_string(),
                key: object.logical_key().to_string(),
                reason: error.to_string(),
            })?;
        let semantic_prefix = format!("{slot_prefix}/{semantic_hash}");
        context
            .validate_locator(&object, &semantic_prefix)
            .map_err(|error| StoreObjectError::Collision {
                semantic_prefix: semantic_prefix.clone(),
                key: object.logical_key().to_string(),
                reason: error.to_string(),
            })?;
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::keys::UserKeypair;
    use crate::storage::cloud::test_utils::InMemoryCloudHome;
    use crate::storage::cloud::{AppendedObject, SequentialCopyIdGenerator};
    use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
    use crate::sync::store_commit::{
        commit_slot_prefix, parse_commit_copy_key, ObjectHash, StoreProtocolError,
    };

    fn store_resolution(store_root_hash: ObjectHash) -> StoreMembershipConflictResolution {
        use crate::sync::membership::{
            founder_entry, AuthorHead, AuthorStreamId, MemberRole, MembershipChain,
            MembershipConflict,
        };

        let first_owner = UserKeypair::generate();
        let second_owner = UserKeypair::generate();
        let first_pubkey = crate::keys::public_key_hex(&first_owner);
        let second_pubkey = crate::keys::public_key_hex(&second_owner);
        let mut base = MembershipChain::from_entries(vec![founder_entry(
            "resolution-object-store",
            &first_owner,
            "founder",
        )])
        .unwrap();
        let add_second = base
            .signed_set_member(
                &first_owner,
                second_pubkey.clone(),
                None,
                MemberRole::Owner,
                "add second".to_string(),
            )
            .unwrap();
        base.add_entry(add_second).unwrap();
        let remove_second = base
            .signed_remove_member(&first_owner, second_pubkey, "remove second".to_string())
            .unwrap();
        let remove_first = base
            .signed_remove_member_in_stream(
                &second_owner,
                AuthorStreamId::from_bytes([61; 16]),
                first_pubkey.clone(),
                "remove first".to_string(),
            )
            .unwrap();
        let mut entries = base.entries().to_vec();
        entries.extend([remove_second.clone(), remove_first.clone()]);
        let conflicted = MembershipChain::from_entries_with_coords_and_heads(
            entries
                .into_iter()
                .map(|entry| (entry.coord(), entry))
                .collect(),
            vec![
                AuthorHead::signed(
                    remove_second.store_id.clone(),
                    remove_second.author_owner_grant.clone(),
                    remove_second.stream_id,
                    remove_second.seq,
                    crate::sync::membership::entry_hash(&remove_second),
                    &first_owner,
                ),
                AuthorHead::signed(
                    remove_first.store_id.clone(),
                    remove_first.author_owner_grant.clone(),
                    remove_first.stream_id,
                    remove_first.seq,
                    crate::sync::membership::entry_hash(&remove_first),
                    &second_owner,
                ),
            ],
        )
        .unwrap();
        let MembershipConflict::RevocationCycle {
            maximal_valid_branches,
            ..
        } = conflicted.conflict().unwrap()
        else {
            panic!("expected revocation cycle");
        };
        let branch = maximal_valid_branches
            .iter()
            .find(|branch| {
                branch.active_grants.values().any(|record| {
                    record.member_pubkey == first_pubkey && record.role == MemberRole::Owner
                })
            })
            .unwrap()
            .heads
            .clone();
        conflicted
            .signed_cycle_resolution(store_root_hash, branch, &first_owner)
            .unwrap()
    }

    fn circle_resolution(store_root_hash: ObjectHash) -> CircleRosterConflictResolution {
        use crate::sync::circle::{
            CircleId, CircleRole, CircleRosterChain, CircleRosterEntry, CircleRosterHead,
            CircleRosterStatus,
        };
        use crate::sync::membership::{AuthorStreamId, MembershipGrantId};

        let first_owner = UserKeypair::generate();
        let second_owner = UserKeypair::generate();
        let first_pubkey = crate::keys::public_key_hex(&first_owner);
        let second_pubkey = crate::keys::public_key_hex(&second_owner);
        let first_grant = MembershipGrantId(ObjectHash::digest(b"Circle object founder grant"));
        let circle_id = CircleId::founder(store_root_hash, &first_pubkey, &first_grant);
        let first_stream = AuthorStreamId::from_bytes([62; 16]);
        let second_stream = AuthorStreamId::from_bytes([63; 16]);
        let founder = CircleRosterEntry::founder(
            store_root_hash,
            circle_id,
            "first-device",
            first_stream,
            first_grant,
            &first_owner,
        );
        let mut base = vec![founder];
        let add_second = CircleRosterChain::from_entries(base.clone())
            .unwrap()
            .signed_set_member(
                "first-device",
                first_stream,
                second_pubkey.clone(),
                CircleRole::Owner,
                &first_owner,
            )
            .unwrap();
        base.push(add_second);
        let remove_second = CircleRosterChain::from_entries(base.clone())
            .unwrap()
            .signed_remove_member("first-device", first_stream, second_pubkey, &first_owner)
            .unwrap();
        let remove_first = CircleRosterChain::from_entries(base.clone())
            .unwrap()
            .signed_remove_member(
                "second-device",
                second_stream,
                first_pubkey.clone(),
                &second_owner,
            )
            .unwrap();
        base.extend([remove_second.clone(), remove_first.clone()]);
        let conflicted = CircleRosterChain::from_entries_with_heads(
            base,
            vec![
                CircleRosterHead::signed(&remove_second, &first_owner),
                CircleRosterHead::signed(&remove_first, &second_owner),
            ],
        )
        .unwrap();
        let CircleRosterStatus::Conflict(
            crate::sync::circle::CircleRosterConflict::RevocationCycle {
                maximal_valid_branches,
                ..
            },
        ) = conflicted.status()
        else {
            panic!("expected revocation cycle");
        };
        let branch = maximal_valid_branches
            .iter()
            .find(|branch| {
                branch.active_grants.values().any(|record| {
                    record.member_pubkey == first_pubkey && record.role == CircleRole::Owner
                })
            })
            .unwrap()
            .heads
            .clone();
        conflicted
            .signed_cycle_resolution(branch, &first_owner)
            .unwrap()
    }

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

    #[test]
    fn shared_resolution_copy_parser_accepts_only_its_physical_path_shape() {
        let prefix = "resolution-parser/";
        let conflict_hash = ObjectHash::digest(b"parser conflict");
        let resolution_hash = ObjectHash::digest(b"parser resolution");
        let key =
            format!("{prefix}{conflict_hash}/resolver/{resolution_hash}/copies/physical-copy.json");
        assert_eq!(
            parse_resolution_copy_key_parts(&key, prefix).unwrap(),
            (conflict_hash, "resolver".to_string(), resolution_hash)
        );
        assert!(matches!(
            parse_resolution_copy_key_parts(&key, "wrong-prefix/"),
            Err(StoreProtocolError::MalformedPath(path)) if path == key
        ));

        let malformed =
            format!("{prefix}{conflict_hash}/resolver/{resolution_hash}/objects/copy.json");
        assert!(matches!(
            parse_resolution_copy_key_parts(&malformed, prefix),
            Err(StoreProtocolError::MalformedPath(path)) if path == malformed
        ));
    }

    #[test]
    fn shared_resolution_copy_grouping_coalesces_references_and_rejects_malformed_keys() {
        let conflict_hash = ObjectHash::digest(b"grouping conflict");
        let resolution_hash = ObjectHash::digest(b"grouping resolution");
        let reference = StoreMembershipConflictResolutionRef {
            conflict_hash,
            resolver_pubkey: "resolver".to_string(),
            resolution_hash,
        };
        let prefix = STORE_MEMBERSHIP_RESOLUTION_PREFIX;
        let semantic_prefix = membership_resolution_semantic_prefix(&reference);
        let locator = |key: String, provider: &str| {
            ImmutableObjectLocator::new(
                key.clone(),
                AppendedObject::from_provider(key, provider.to_string()),
            )
        };
        let first = locator(
            format!("{semantic_prefix}/copies/first.json"),
            "first-provider-copy",
        );
        let second = locator(
            format!("{semantic_prefix}/copies/second.json"),
            "second-provider-copy",
        );
        let groups = group_resolution_copy_objects(
            vec![first, second],
            prefix,
            parse_membership_resolution_copy_key,
        )
        .unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[&reference].len(), 2);

        let malformed_key = format!("{prefix}malformed");
        let error = group_resolution_copy_objects(
            vec![locator(malformed_key.clone(), "malformed-provider-copy")],
            prefix,
            parse_membership_resolution_copy_key,
        )
        .expect_err("a malformed resolution copy key fails grouping");
        assert!(matches!(
            error,
            StoreObjectError::Collision {
                semantic_prefix,
                key,
                ..
            } if semantic_prefix == prefix.trim_end_matches('/') && key == malformed_key
        ));
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

        let loaded = load_semantic_copies(&storage, &context, &prefix, hash, |candidate| {
            validate_digest(hash, candidate)
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(loaded.bytes, bytes);
        assert_eq!(loaded.copies.len(), 2);
    }

    #[tokio::test]
    async fn membership_entry_listing_groups_copies_and_rejects_a_malformed_key() {
        let home = InMemoryCloudHome::new();
        let storage = storage(&home);
        let owner = UserKeypair::generate();
        let entry =
            crate::sync::membership::founder_entry("membership-entry-listing", &owner, "founder");
        let coord = entry.coord();
        let store_root_hash = ObjectHash::digest(b"membership entry listing Store root");
        append_membership_entry_object(&storage, store_root_hash, &coord, &entry)
            .await
            .unwrap();
        append_membership_entry_object(&storage, store_root_hash, &coord, &entry)
            .await
            .unwrap();

        let listed = list_membership_entry_objects(&storage, store_root_hash)
            .await
            .unwrap();
        assert_eq!(listed.entries.len(), 1);
        assert_eq!(listed.entries[0].0, coord);
        assert_eq!(listed.entries[0].1.copies.len(), 2);

        let malformed = format!("{STORE_MEMBERSHIP_ENTRY_PREFIX}malformed");
        home.insert_appended_candidate(&malformed, b"malformed membership entry".to_vec());
        let error = list_membership_entry_objects(&storage, store_root_hash)
            .await
            .expect_err("a malformed membership entry copy key fails the listing");
        assert!(matches!(
            error,
            StoreObjectError::Collision {
                semantic_prefix,
                key,
                ..
            } if semantic_prefix == STORE_MEMBERSHIP_ENTRY_PREFIX.trim_end_matches('/')
                && key == malformed
        ));
    }

    #[tokio::test]
    async fn membership_head_listing_groups_copies_and_rejects_a_malformed_key() {
        let home = InMemoryCloudHome::new();
        let storage = storage(&home);
        let owner = UserKeypair::generate();
        let entry =
            crate::sync::membership::founder_entry("membership-head-listing", &owner, "founder");
        let head = AuthorHead::signed(
            entry.store_id.clone(),
            entry.author_owner_grant.clone(),
            entry.stream_id,
            entry.seq,
            entry_hash(&entry),
            &owner,
        );
        let store_root_hash = ObjectHash::digest(b"membership head listing Store root");
        append_membership_head_object(&storage, store_root_hash, &head)
            .await
            .unwrap();
        append_membership_head_object(&storage, store_root_hash, &head)
            .await
            .unwrap();

        let listed = list_membership_head_objects(&storage, store_root_hash)
            .await
            .unwrap();
        assert_eq!(listed.heads.len(), 1);
        assert_eq!(listed.heads[0].value, head);
        assert_eq!(listed.heads[0].copies.len(), 2);

        let malformed = format!("{STORE_MEMBERSHIP_HEAD_PREFIX}malformed");
        home.insert_appended_candidate(&malformed, b"malformed membership head".to_vec());
        let error = list_membership_head_objects(&storage, store_root_hash)
            .await
            .expect_err("a malformed membership head copy key fails the listing");
        assert!(matches!(
            error,
            StoreObjectError::Collision {
                semantic_prefix,
                key,
                ..
            } if semantic_prefix == STORE_MEMBERSHIP_HEAD_PREFIX.trim_end_matches('/')
                && key == malformed
        ));
    }

    #[tokio::test]
    async fn membership_resolution_append_load_and_list_use_the_exact_reference_path() {
        let home = InMemoryCloudHome::new();
        let storage = storage(&home);
        let store_root_hash = ObjectHash::digest(b"membership resolution Store root");
        let resolution = store_resolution(store_root_hash);
        let reference = resolution.resolution_ref();

        let appended = append_membership_resolution_object(&storage, store_root_hash, &resolution)
            .await
            .unwrap();
        assert_eq!(appended.value, resolution);
        assert!(appended.copies.iter().all(|copy| {
            copy.logical_key()
                .starts_with(&membership_resolution_semantic_prefix(&reference))
        }));
        assert_eq!(
            load_membership_resolution_object(&storage, store_root_hash, &reference)
                .await
                .unwrap()
                .unwrap()
                .value,
            resolution
        );
        let listed = list_membership_resolution_objects(&storage, store_root_hash)
            .await
            .unwrap();
        assert_eq!(listed.resolutions.len(), 1);
        assert_eq!(listed.resolutions[0].value, resolution);
    }

    #[tokio::test]
    async fn circle_resolution_append_load_and_list_use_the_exact_reference_path() {
        let home = InMemoryCloudHome::new();
        let storage = storage(&home);
        let store_root_hash = ObjectHash::digest(b"Circle resolution Store root");
        let resolution = circle_resolution(store_root_hash);
        let reference = resolution.resolution_ref();
        let encryption = crate::encryption::EncryptionService::from_key([17; 32]);

        let appended =
            append_circle_roster_resolution_object(&storage, encryption.clone(), &resolution)
                .await
                .unwrap();
        assert_eq!(appended.value, resolution);
        let prefix = circle_semantic_prefix(CircleSemanticSlot::RosterResolution {
            circle_id: resolution.circle_id,
            resolution: &reference,
        });
        assert!(appended
            .copies
            .iter()
            .all(|copy| copy.logical_key().starts_with(&prefix)));
        assert_eq!(
            load_circle_roster_resolution_object(
                &storage,
                store_root_hash,
                resolution.circle_id,
                encryption.clone(),
                &reference,
            )
            .await
            .unwrap()
            .unwrap()
            .value,
            resolution
        );
        let listed = list_circle_roster_resolution_objects(
            &storage,
            store_root_hash,
            resolution.circle_id,
            encryption,
        )
        .await
        .unwrap();
        assert_eq!(listed.resolutions.len(), 1);
        assert_eq!(listed.resolutions[0].value, resolution);
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

        let error = load_semantic_copies(&storage, &context, &prefix, hash, |candidate| {
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
            |key| Ok(parse_commit_copy_key(key)?.semantic_hash),
            validate_digest,
        )
        .await
        .expect_err("two valid semantic hashes must fail-stop");
        assert!(matches!(error, StoreObjectError::SemanticFork { .. }));
    }
}

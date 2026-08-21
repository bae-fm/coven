use super::merge_history::predecessor_verifies_owner;
use super::merge_history::registration::{
    device_state_has_active_registration, device_state_has_pending_proposal,
    registration_attempt_error, RegistrationLoadError,
};
use crate::sync::store::pull::*;
use crate::sync::store::StoreError;
use coven_database::{activated_merge_membership_remote_objects, MembershipAuthorityBytes};
use coven_protocol::membership::{MembershipChain, MembershipChange, MembershipHeadRef};
use coven_protocol::objects::{
    decode_protocol_object, verify_store_root, StoreObjectError, VerifiedObject,
};
use coven_protocol::objects::{
    ExactObjectRef, ProtocolObjectContext, ProtocolObjectDomain, StorageError,
};
use coven_protocol::reclaim::{
    reclaim_authorization_semantic_prefix, reclaim_evidence_semantic_prefix,
    reclaim_receipt_semantic_prefix, ReclaimAuthorization, ReclaimAuthorizationRef,
    ReclaimEvidence, ReclaimReceipt, ReclaimReceiptRef,
};
use coven_protocol::remote_object;
use coven_protocol::store_commit::*;
use coven_protocol::store_commit::{
    ack_slot_prefix, device_exclusion_outcome_semantic_prefix,
    device_exclusion_proposal_semantic_prefix, founder_registration_semantic_prefix,
    package_semantic_prefix, provider_access_grant_semantic_prefix, registration_semantic_prefix,
    snapshot_slot_prefix, SnapshotMeta, StoreAck, StoreAckRef, StoreDeviceExclusionOutcomeRef,
    StoreDeviceExclusionProposal, StoreDeviceExclusionProposalRef, StoreDeviceHeadRef,
    StoreSnapshotRef,
};
use coven_storage::run_blocking_object_verification;
use coven_storage::CloudSyncObjectStorage;
use std::collections::{BTreeMap, BTreeSet};

mod membership;

mod acknowledgements_snapshots;
mod announcements;
mod commits;
mod device_lifecycle;
mod registrations;
pub(crate) use membership::StoreMembershipObjectVerifier;

pub(crate) enum DeviceStateResolver<'a> {
    Database(&'a coven_database::StoreDatabase),
    Loaded {
        genesis: &'a ResolvedStoreDeviceState,
        states: &'a BTreeMap<StoreBatchCommitRef, ResolvedStoreDeviceState>,
    },
}

impl DeviceStateResolver<'_> {
    async fn resolve(
        &self,
        reference: &StoreDeviceStateRef,
    ) -> Result<ResolvedStoreDeviceState, RegistrationLoadError> {
        let state = match self {
            DeviceStateResolver::Database(database) => {
                return database
                    .resolved_store_device_state(reference)
                    .await
                    .map_err(RegistrationLoadError::from);
            }
            DeviceStateResolver::Loaded { genesis, states } => {
                let frontier = &reference.frontier().0;
                if frontier.is_empty() {
                    (*genesis).clone()
                } else {
                    ResolvedStoreDeviceState::merge(
                        frontier
                            .values()
                            .map(|commit| {
                                states.get(commit).cloned().ok_or_else(|| {
                                    RegistrationLoadError::Invalid(
                                        "device state references an unloaded predecessor snapshot"
                                            .to_string(),
                                    )
                                })
                            })
                            .collect::<Result<Vec<_>, _>>()?,
                    )
                    .map_err(RegistrationLoadError::from)?
                }
            }
        };
        if state.state_hash != reference.state_hash() || state.recovery != reference.recovery() {
            return Err(RegistrationLoadError::Invalid(
                "device state differs from its exact predecessor snapshots".to_string(),
            ));
        }
        Ok(state)
    }
}

/// How many protocol slots a reader fetches at once.
///
/// Protocol objects are small signed documents, so the limit that matters is
/// how many requests a provider will take at once, not bandwidth. One value,
/// so it is a constant rather than a setting; blob transfer limits are a
/// separate policy because blobs are large enough for their width to be a
/// bandwidth decision.
pub(crate) const PROTOCOL_SLOT_READ_WIDTH: usize = 16;

/// Bytes read out of one protocol slot, with the exact object the stored bytes
/// identify. What [`StoreCommitVerifier::read_protocol_slot`] returns, named so
/// a batch of them can be handed around.
pub(crate) struct ReadProtocolSlot {
    pub(crate) bytes: Vec<u8>,
    pub(crate) object: ExactObjectRef,
}

/// What one prefix's worth of speculative slot reads found, shared by every
/// reader that goes on to walk that prefix.
pub(crate) type StreamSlotReads =
    std::sync::Arc<BTreeMap<coven_protocol::objects::ObjectSlot, ReadProtocolSlot>>;

/// A prefetched slot stream, and whether this caller is the one that fetched
/// it. Callers that go on to prefetch what the fetched bytes *name* need to
/// know, so the second walk of a stream does not repeat that follow-on work.
pub(crate) enum PrefetchedSlotStream {
    Fetched(StreamSlotReads),
    Remembered(StreamSlotReads),
}

impl PrefetchedSlotStream {
    pub(crate) fn reads(&self) -> &StreamSlotReads {
        match self {
            Self::Fetched(reads) | Self::Remembered(reads) => reads,
        }
    }

    pub(crate) fn freshly_fetched(&self) -> Option<&StreamSlotReads> {
        match self {
            Self::Fetched(reads) => Some(reads),
            Self::Remembered(_) => None,
        }
    }
}

pub(crate) struct StoreCommitVerifier<'a> {
    storage: &'a dyn CloudSyncObjectStorage,
    root: crate::sync::store::protocol_root::VerifiedStoreRoot,
    commits: BTreeMap<StoreBatchCommitRef, VerifiedStoreBatchCommit>,
    registrations: std::sync::Mutex<
        BTreeMap<StoreDeviceRegistrationRef, VerifiedObject<StoreDeviceRegistration>>,
    >,
    /// Which registration is this Store's founder.
    ///
    /// The founder is reached by slot from the root descriptor, so it cannot be
    /// asked for by reference until it has been read once; this remembers the
    /// reference that read produced. The registration itself lives in
    /// `registrations` with every other one — this names the entry rather than
    /// holding a second copy of it.
    founder_registration: std::sync::OnceLock<StoreDeviceRegistrationRef>,
    verified_heads: std::sync::Mutex<BTreeMap<StoreDeviceHeadRef, VerifiedObject<StoreDeviceHead>>>,
    /// Store acknowledgements authenticated under this verifier's root, keyed by
    /// the exact object that carries them so both ways of asking reach one
    /// entry: by reference, which is how a commit names the ack it activates,
    /// and by the predecessor object a chain walk follows. The reference is kept
    /// beside the value because a lookup by object still has to confirm it is
    /// the ack that was asked for.
    acknowledgements: std::sync::Mutex<BTreeMap<ExactObjectRef, (StoreAckRef, StoreAck)>>,
    /// Snapshot metadata authenticated under this verifier's root. An
    /// acknowledgement may name the snapshot it covers, and every commit that
    /// activates one re-checks that coverage — over a handful of snapshots that
    /// the whole history keeps naming, so without this the same few objects were
    /// read once per acknowledging commit.
    snapshots: std::sync::Mutex<BTreeMap<StoreSnapshotRef, SnapshotMeta>>,
    /// Each device's snapshot stream as far as this verifier has walked it.
    ///
    /// A stream is read by walking one slot per generation until a slot is
    /// absent, so re-walking costs a read per generation every time — and a
    /// cycle walks each stream several times, from publication, from history
    /// loading, and from reclaim. The walk resumes from the prefix here and
    /// probes on from its end, so generations already read are not read again
    /// and a generation published since is still found. Same shape as the
    /// accepted announcement path, for the same reason.
    /// Bytes of every content-addressed protocol object this verifier has read.
    ///
    /// `load_exact_object` is the one place a verified object is fetched by
    /// reference — membership heads and entries, acknowledgements, snapshots,
    /// commits, registrations, packages all come through it — and it had no
    /// reuse of any kind, so a cycle paid a provider round trip for every ask.
    /// A settled two-device cycle makes about fifty of these, twenty-five of
    /// them membership, which is the six to sixteen seconds a quiet field cycle
    /// still spent after the retained-history work.
    ///
    /// This holds bytes, not verdicts: a hit still runs the caller's
    /// verification, and the reference carries the semantic hash that
    /// verification checks, so an answer from here is the answer a read would
    /// have produced. Objects named this way are immutable, so a verifier's
    /// lifetime is a safe one to hold them for.
    exact_objects: std::sync::Mutex<BTreeMap<ExactObjectRef, Vec<u8>>>,
    /// Each author membership stream's head slots, as far as this verifier has
    /// fetched them, keyed by the provider prefix the stream is listed under.
    ///
    /// One anchored-chain load traverses the same stream several times — the
    /// founder's seven times over a small fixture — because discovery, layering
    /// and activation each walk it for their own reasons. The walks are by
    /// slot, and a slot read is the one read that cannot go through
    /// `exact_objects`, since a walker does not know a head's reference until
    /// it has read the head. This holds what a stream's listing found so the
    /// second walk and the seventh cost nothing.
    ///
    /// A stream that has grown since is not a problem: what is missing here is
    /// read from the provider, so this decides round trips and never contents.
    prefetched_slot_streams: std::sync::Mutex<BTreeMap<String, StreamSlotReads>>,
    snapshot_streams: std::sync::Mutex<
        BTreeMap<StoreDeviceRegistrationRef, Vec<coven_database::PublishedStoreSnapshot>>,
    >,
    accepted_announcements:
        BTreeMap<StoreDeviceRegistrationRef, Vec<VerifiedAcceptedStoreAnnouncement>>,
    /// Where each author's announcement chain has been restated by the Store
    /// snapshot this device stands on, so a walk resumes there instead of at
    /// the anchor slot.
    ///
    /// The chain is a slot-linked list: sequence one names the slot of two, and
    /// so on, so a walker cannot skip into the middle of it — it either holds a
    /// position already or reads every head from the anchor. A device whose
    /// replay baseline advanced holds no row under the snapshot's cut, and
    /// without a resume point the only place left to start is the anchor: the
    /// whole chain re-read on every pull, forever, growing with the store's
    /// history. The snapshot's history summary carries the accepted
    /// announcement at each covered tip, signed by the owner alongside the
    /// state it restates, and that is the resume point.
    covered_announcements: BTreeMap<StoreDeviceRegistrationRef, CoveredStoreAnnouncement>,
    /// Announcement heads found at positions the installed snapshot covers.
    ///
    /// The accepted path holds only what stands above the coverage, so a query
    /// about an older position — a join activation, an exclusion-history walk —
    /// has to read the chain from the anchor to reach it. It is the same walk
    /// every time it is asked, so one per verifier is enough; without this the
    /// per-commit questions those walks ask turn one chain read into one per
    /// commit.
    covered_walk: BTreeMap<(StoreDeviceRegistrationRef, u64), VerifiedAcceptedStoreAnnouncement>,
}

pub(crate) struct VerifiedMergeMembershipClosure {
    objects: coven_database::VerifiedMergeMembershipObjects,
    remote_objects: Vec<remote_object::ClosedRemoteObject>,
    pub(crate) proof: RetainedMergeMembershipProof,
}

impl VerifiedMergeMembershipClosure {
    pub(crate) fn objects(&self) -> &coven_database::VerifiedMergeMembershipObjects {
        &self.objects
    }

    pub(crate) fn into_remote_objects(self) -> Vec<remote_object::ClosedRemoteObject> {
        self.remote_objects
    }
}

#[derive(Clone, PartialEq, Eq)]
struct VerifiedAcceptedStoreAnnouncement {
    commit: StoreBatchCommitRef,
    head: StoreDeviceHeadRef,
    next_slot: coven_protocol::objects::ObjectSlot,
}

/// One author's announcement position as of the installed snapshot: the
/// accepted head at the covered tip, and the slot its successor occupies.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct CoveredStoreAnnouncement {
    pub(crate) sequence: u64,
    pub(crate) commit: StoreBatchCommitRef,
    pub(crate) head: StoreDeviceHeadRef,
    pub(crate) next_slot: coven_protocol::objects::ObjectSlot,
}

pub(crate) struct VerifiedAcceptedStoreAnnouncementPrefix {
    pub(crate) commits: Vec<(
        StoreDeviceHeadRef,
        StoreDeviceHead,
        StoreBatchCommitRef,
        StoreBatchCommit,
    )>,
    pub(crate) next_slot: coven_protocol::objects::ObjectSlot,
    pub(crate) predecessor: Option<ExactObjectRef>,
    pub(crate) next_sequence: u64,
}

#[derive(Debug)]
pub(crate) struct VerifiedReclaimAuthorization {
    pub(crate) authorization: VerifiedObject<ReclaimAuthorization>,
    pub(crate) evidence: VerifiedObject<ReclaimEvidence>,
}

#[derive(Debug)]
pub(crate) struct VerifiedReclaimReceipt {
    pub(crate) receipt: VerifiedObject<ReclaimReceipt>,
    pub(crate) executor: StoreDeviceRegistration,
}

impl<'a> StoreCommitVerifier<'a> {
    pub(super) fn store_root_hash(&self) -> ObjectHash {
        self.root.reference().store_root_hash
    }

    pub(crate) fn membership_objects(&self) -> StoreMembershipObjectVerifier<'_, 'a> {
        StoreMembershipObjectVerifier::new(self)
    }

    pub(crate) async fn verified_merge_membership_objects(
        &self,
        commit_ref: &StoreBatchCommitRef,
        commit: &StoreBatchCommit,
    ) -> Result<Option<VerifiedMergeMembershipClosure>, StorePullError> {
        let Some(StoreControl { transition }) = commit.control() else {
            return Ok(None);
        };
        let entry = self
            .membership_objects()
            .load_entry(&transition.body.entry)
            .await
            .map_err(StorePullError::Object)?;
        let coord = &transition.body.entry.coord;
        let loaded_head = self
            .membership_objects()
            .load_head_at_slot(
                &transition.head_slot,
                &coord.author_pubkey,
                &coord.author_owner_grant,
                coord.stream_id,
                coord.seq,
            )
            .await
            .map_err(StorePullError::Object)?;
        let head_bytes = loaded_head.bytes;
        let head_object = loaded_head.object;
        let head = loaded_head.value;
        let head_ref = MembershipHeadRef {
            coord: head.entry_coord(),
            head_hash: head.head_hash(),
            object: head_object,
        };
        let objects = coven_database::VerifiedMergeMembershipObjects::verify(
            commit,
            commit_ref,
            &entry.value,
            &head,
            head_ref.clone(),
        )
        .map_err(StorePullError::Database)?;
        let family = commit.candidate_family();
        let resolution = match &entry.value.change {
            MembershipChange::ResolutionActivation { resolution } => Some(resolution.clone()),
            _ => None,
        };
        let resolution_loaded = if let Some(resolution) = &resolution {
            let loaded = self
                .membership_objects()
                .load_resolution(resolution)
                .await
                .map_err(StorePullError::Object)?;
            Some((loaded.bytes, loaded.value))
        } else {
            None
        };
        let remote_objects = activated_merge_membership_remote_objects(
            family,
            &objects,
            MembershipAuthorityBytes::new(entry.bytes.clone(), entry.bytes),
            MembershipAuthorityBytes::new(head_bytes.clone(), head_bytes),
            resolution_loaded
                .as_ref()
                .map(|(bytes, _)| MembershipAuthorityBytes::new(bytes.clone(), bytes.clone())),
            commit_ref,
        )
        .map_err(StorePullError::RemoteObject)?;
        let resolution_value = resolution_loaded.map(|(_, value)| value);
        let proof = RetainedMergeMembershipProof {
            commit: commit_ref.clone(),
            commit_value: commit.clone(),
            announcement: None,
            entry: transition.body.entry.clone(),
            entry_value: entry.value,
            head: head_ref,
            head_value: head,
            resolution,
            resolution_value,
        };
        Ok(Some(VerifiedMergeMembershipClosure {
            objects,
            remote_objects,
            proof,
        }))
    }

    pub(crate) fn from_verified_root(
        _authority: crate::sync::store::authorization::HistoryConstructionAuthority,
        storage: &'a dyn CloudSyncObjectStorage,
        root: crate::sync::store::protocol_root::VerifiedStoreRoot,
    ) -> Self {
        Self {
            storage,
            root,
            commits: BTreeMap::new(),
            registrations: std::sync::Mutex::new(BTreeMap::new()),
            founder_registration: std::sync::OnceLock::new(),
            verified_heads: std::sync::Mutex::new(BTreeMap::new()),
            acknowledgements: std::sync::Mutex::new(BTreeMap::new()),
            snapshots: std::sync::Mutex::new(BTreeMap::new()),
            exact_objects: std::sync::Mutex::new(BTreeMap::new()),
            prefetched_slot_streams: std::sync::Mutex::new(BTreeMap::new()),
            snapshot_streams: std::sync::Mutex::new(BTreeMap::new()),
            accepted_announcements: BTreeMap::new(),
            covered_announcements: BTreeMap::new(),
            covered_walk: BTreeMap::new(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CommitCoverageError {
    #[error(transparent)]
    Object(#[from] StoreObjectError),
    #[error("exact Store ancestry is missing commit {commit_hash}")]
    MissingAncestry { commit_hash: ObjectHash },
}

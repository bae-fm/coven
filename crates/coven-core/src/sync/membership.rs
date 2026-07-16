//! Store-bound causal membership protocol.
//!
//! Every causal author stream is identified by its author, the Owner grant that
//! authorizes it, and an independently generated stream id. Entries carry the
//! complete observed stream frontier; authorization is derived from that causal
//! past, never from `created_at`.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::causal_grants::{
    self, CausalAssignment, CausalChange, CausalCoordinate, CausalEntry, CausalGrantError,
    OwnerGrantBarrier,
};
pub use super::causal_grants::{AuthorStreamId, MembershipGrantId};
use super::store_commit::{
    ObjectHash, StoreBatchCommit, StoreControl, StoreDeviceRegistration, STORE_PROTOCOL_VERSION,
};
use crate::keys::{self, UserKeypair};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum MemberRole {
    Owner,
    Member,
    Follower,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SerialMember {
    pub role: MemberRole,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_account_email: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SerialMembershipState {
    store_root_hash: ObjectHash,
    members: BTreeMap<String, SerialMember>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SerialAuthorizationState {
    pub membership: SerialMembershipState,
    pub key_generation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum SerialMembershipChange {
    SetMember {
        user_pubkey: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        provider_account_email: Option<String>,
        role: MemberRole,
    },
    RemoveMember {
        user_pubkey: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SerialMembershipEntry {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub previous_state_hash: ObjectHash,
    pub author_pubkey: String,
    pub created_at: String,
    pub change: SerialMembershipChange,
    pub signature: String,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SerialMembershipError {
    #[error("Serial membership founder does not match the Store protocol root founder")]
    InvalidFounder,
    #[error("Serial membership entry has unsupported version {0}")]
    UnsupportedVersion(u32),
    #[error("Serial membership entry belongs to root {actual}, expected {expected}")]
    StoreRootMismatch {
        expected: ObjectHash,
        actual: ObjectHash,
    },
    #[error("Serial membership entry has an invalid signature")]
    InvalidSignature,
    #[error("Serial membership entry names state {actual}, expected {expected}")]
    StaleState {
        expected: ObjectHash,
        actual: ObjectHash,
    },
    #[error("Serial membership author {0} is not a current Owner")]
    AuthorIsNotOwner(String),
    #[error("Serial membership member {0} is absent")]
    NotAMember(String),
    #[error("Serial membership removal would leave no Owner")]
    LastOwner,
    #[error("Serial commit carries a causal membership grant")]
    CausalGrant,
    #[error("Serial commit author {0} is not a current writer")]
    AuthorIsNotWriter(String),
    #[error("Serial key rotation is not paired with a membership removal")]
    RotationWithoutRemoval,
    #[error("Serial key rotation names generation {actual}, expected {expected}")]
    KeyGeneration { expected: u64, actual: u64 },
}

impl SerialAuthorizationState {
    pub fn from_founder(
        store_root_hash: ObjectHash,
        founder: &MembershipEntry,
    ) -> Result<Self, SerialMembershipError> {
        Ok(Self {
            membership: SerialMembershipState::from_founder(store_root_hash, founder)?,
            key_generation: crate::encryption::INITIAL_KEY_GENERATION,
        })
    }

    pub fn authorize_and_apply(
        &self,
        commit: &StoreBatchCommit,
    ) -> Result<Self, SerialMembershipError> {
        self.authorize_and_apply_with_registrations(commit, &[])
    }

    pub(crate) fn authorize_and_apply_with_registrations(
        &self,
        commit: &StoreBatchCommit,
        registrations: &[StoreDeviceRegistration],
    ) -> Result<Self, SerialMembershipError> {
        if commit.membership_grant.is_some() {
            return Err(SerialMembershipError::CausalGrant);
        }
        let authorized = self.membership.can_write(&commit.author_pubkey)
            || self.membership.contains(&commit.author_pubkey)
                && is_exact_self_registration(commit, registrations);
        if !authorized {
            return Err(SerialMembershipError::AuthorIsNotWriter(
                commit.author_pubkey.clone(),
            ));
        }
        let Some(control) = commit.control.as_ref() else {
            return Ok(self.clone());
        };
        let membership = self.membership.apply(control.serial_membership_entry())?;
        let key_generation = match control {
            StoreControl::SerialMembership { .. } => self.key_generation,
            StoreControl::SerialMembershipAndKeyRotation { entry, generation } => {
                if !entry.change.is_removal() {
                    return Err(SerialMembershipError::RotationWithoutRemoval);
                }
                let expected = self.key_generation.checked_add(1).ok_or(
                    SerialMembershipError::KeyGeneration {
                        expected: self.key_generation,
                        actual: *generation,
                    },
                )?;
                if *generation != expected {
                    return Err(SerialMembershipError::KeyGeneration {
                        expected,
                        actual: *generation,
                    });
                }
                *generation
            }
        };
        Ok(Self {
            membership,
            key_generation,
        })
    }
}

pub(crate) fn is_exact_self_registration(
    commit: &StoreBatchCommit,
    registrations: &[StoreDeviceRegistration],
) -> bool {
    let [reference] = commit.device_registrations.as_slice() else {
        return false;
    };
    let [registration] = registrations else {
        return false;
    };
    commit.control.is_none()
        && commit.store_package.is_none()
        && commit.circle_controls.is_empty()
        && commit.circle_packages.is_empty()
        && registration.store_root_hash == commit.store_root_hash
        && registration.device_id == commit.device_id
        && registration.author_pubkey == commit.author_pubkey
        && reference.device_id == registration.device_id
        && reference.revision == registration.revision
        && reference.registration_hash == registration.registration_hash()
}

impl SerialMembershipState {
    pub fn from_founder(
        store_root_hash: ObjectHash,
        founder: &MembershipEntry,
    ) -> Result<Self, SerialMembershipError> {
        let MembershipChange::Founder {
            owner_pubkey,
            owner_grant_id,
        } = &founder.change
        else {
            return Err(SerialMembershipError::InvalidFounder);
        };
        if founder.author_pubkey != *owner_pubkey
            || founder.author_owner_grant != *owner_grant_id
            || founder.stream_id != derive_founder_stream_id(&founder.store_id, owner_pubkey)
            || founder.seq != 1
            || founder.previous_hash.is_some()
            || !founder.dependencies.is_empty()
            || !verify_membership_entry(founder)
        {
            return Err(SerialMembershipError::InvalidFounder);
        }
        Ok(Self {
            store_root_hash,
            members: BTreeMap::from([(
                owner_pubkey.clone(),
                SerialMember {
                    role: MemberRole::Owner,
                    provider_account_email: None,
                },
            )]),
        })
    }

    pub fn state_hash(&self) -> ObjectHash {
        #[derive(Serialize)]
        struct StateFields<'a> {
            domain: &'static str,
            store_root_hash: ObjectHash,
            members: &'a BTreeMap<String, SerialMember>,
        }
        ObjectHash::digest(
            &serde_json::to_vec(&StateFields {
                domain: "coven.serial-membership-state.v1",
                store_root_hash: self.store_root_hash,
                members: &self.members,
            })
            .expect("Serial membership state serialization cannot fail"),
        )
    }

    pub fn store_root_hash(&self) -> ObjectHash {
        self.store_root_hash
    }

    pub fn current_members(&self) -> Vec<(String, MemberRole)> {
        self.members
            .iter()
            .map(|(pubkey, member)| (pubkey.clone(), member.role.clone()))
            .collect()
    }

    pub fn current_member_provider_email(&self, pubkey: &str) -> Option<&str> {
        self.members
            .get(pubkey)
            .and_then(|member| member.provider_account_email.as_deref())
    }

    pub fn can_write(&self, pubkey: &str) -> bool {
        self.members
            .get(pubkey)
            .is_some_and(|member| member.role.can_write())
    }

    fn contains(&self, pubkey: &str) -> bool {
        self.members.contains_key(pubkey)
    }

    pub fn is_owner(&self, pubkey: &str) -> bool {
        self.members
            .get(pubkey)
            .is_some_and(|member| member.role == MemberRole::Owner)
    }

    pub fn signed_set_member(
        &self,
        signer: &UserKeypair,
        user_pubkey: String,
        provider_account_email: Option<String>,
        role: MemberRole,
        created_at: String,
    ) -> Result<SerialMembershipEntry, SerialMembershipError> {
        self.signed_change(
            signer,
            SerialMembershipChange::SetMember {
                user_pubkey,
                provider_account_email,
                role,
            },
            created_at,
        )
    }

    pub fn signed_remove_member(
        &self,
        signer: &UserKeypair,
        user_pubkey: String,
        created_at: String,
    ) -> Result<SerialMembershipEntry, SerialMembershipError> {
        if !self.members.contains_key(&user_pubkey) {
            return Err(SerialMembershipError::NotAMember(user_pubkey));
        }
        self.signed_change(
            signer,
            SerialMembershipChange::RemoveMember { user_pubkey },
            created_at,
        )
    }

    fn signed_change(
        &self,
        signer: &UserKeypair,
        change: SerialMembershipChange,
        created_at: String,
    ) -> Result<SerialMembershipEntry, SerialMembershipError> {
        let author_pubkey = keys::public_key_hex(signer);
        if !self.is_owner(&author_pubkey) {
            return Err(SerialMembershipError::AuthorIsNotOwner(author_pubkey));
        }
        let mut entry = SerialMembershipEntry {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash: self.store_root_hash,
            previous_state_hash: self.state_hash(),
            author_pubkey,
            created_at,
            change,
            signature: String::new(),
        };
        let (_, signature) = keys::sign_hex(signer, &entry.canonical_bytes());
        entry.signature = signature;
        Ok(entry)
    }

    pub fn apply(&self, entry: &SerialMembershipEntry) -> Result<Self, SerialMembershipError> {
        if entry.version != STORE_PROTOCOL_VERSION {
            return Err(SerialMembershipError::UnsupportedVersion(entry.version));
        }
        if entry.store_root_hash != self.store_root_hash {
            return Err(SerialMembershipError::StoreRootMismatch {
                expected: self.store_root_hash,
                actual: entry.store_root_hash,
            });
        }
        if !entry.verify() {
            return Err(SerialMembershipError::InvalidSignature);
        }
        let expected = self.state_hash();
        if entry.previous_state_hash != expected {
            return Err(SerialMembershipError::StaleState {
                expected,
                actual: entry.previous_state_hash,
            });
        }
        if !self.is_owner(&entry.author_pubkey) {
            return Err(SerialMembershipError::AuthorIsNotOwner(
                entry.author_pubkey.clone(),
            ));
        }
        let mut next = self.clone();
        match &entry.change {
            SerialMembershipChange::SetMember {
                user_pubkey,
                provider_account_email,
                role,
            } => {
                next.members.insert(
                    user_pubkey.clone(),
                    SerialMember {
                        role: role.clone(),
                        provider_account_email: provider_account_email.clone(),
                    },
                );
            }
            SerialMembershipChange::RemoveMember { user_pubkey } => {
                let removed = next
                    .members
                    .remove(user_pubkey)
                    .ok_or_else(|| SerialMembershipError::NotAMember(user_pubkey.clone()))?;
                if removed.role == MemberRole::Owner
                    && !next
                        .members
                        .values()
                        .any(|member| member.role == MemberRole::Owner)
                {
                    return Err(SerialMembershipError::LastOwner);
                }
            }
        }
        Ok(next)
    }
}

impl SerialMembershipChange {
    pub fn user_pubkey(&self) -> &str {
        match self {
            Self::SetMember { user_pubkey, .. } | Self::RemoveMember { user_pubkey } => user_pubkey,
        }
    }

    pub fn is_removal(&self) -> bool {
        matches!(self, Self::RemoveMember { .. })
    }
}

impl SerialMembershipEntry {
    fn canonical_bytes(&self) -> Vec<u8> {
        #[derive(Serialize)]
        struct Signed<'a> {
            domain: &'static str,
            version: u32,
            store_root_hash: ObjectHash,
            previous_state_hash: ObjectHash,
            author_pubkey: &'a str,
            created_at: &'a str,
            change: &'a SerialMembershipChange,
        }
        serde_json::to_vec(&Signed {
            domain: "coven.serial-membership-entry.v1",
            version: self.version,
            store_root_hash: self.store_root_hash,
            previous_state_hash: self.previous_state_hash,
            author_pubkey: &self.author_pubkey,
            created_at: &self.created_at,
            change: &self.change,
        })
        .expect("Serial membership entry serialization cannot fail")
    }

    pub fn verify(&self) -> bool {
        keys::verify_signature_hex(
            &self.author_pubkey,
            &self.signature,
            &self.canonical_bytes(),
        )
    }
}

impl MemberRole {
    pub fn can_write(&self) -> bool {
        matches!(self, Self::Owner | Self::Member)
    }
}

#[derive(Debug, Clone)]
pub struct MemberInfo {
    pub pubkey: String,
    pub role: MemberRole,
    pub is_self: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub enum MembershipChange {
    Founder {
        owner_pubkey: String,
        owner_grant_id: MembershipGrantId,
    },
    SetMember {
        user_pubkey: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        provider_account_email: Option<String>,
        role: MemberRole,
        grant_id: MembershipGrantId,
        replaces: BTreeSet<MembershipGrantId>,
        owner_barriers: BTreeMap<MembershipGrantId, OwnerStreamBarrier>,
    },
    RemoveMember {
        user_pubkey: String,
        removes: BTreeSet<MembershipGrantId>,
        owner_barriers: BTreeMap<MembershipGrantId, OwnerStreamBarrier>,
    },
}

impl MembershipChange {
    pub fn user_pubkey(&self) -> &str {
        match self {
            Self::Founder { owner_pubkey, .. } => owner_pubkey,
            Self::SetMember { user_pubkey, .. } | Self::RemoveMember { user_pubkey, .. } => {
                user_pubkey
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MembershipCoord {
    pub author_pubkey: String,
    pub author_owner_grant: MembershipGrantId,
    pub stream_id: AuthorStreamId,
    pub seq: u64,
    pub entry_hash: ObjectHash,
}

impl MembershipCoord {
    pub(crate) fn stream_key(&self) -> MembershipStreamKey {
        MembershipStreamKey {
            author_pubkey: self.author_pubkey.clone(),
            author_owner_grant: self.author_owner_grant.clone(),
            stream_id: self.stream_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct MembershipStreamKey {
    pub(crate) author_pubkey: String,
    pub(crate) author_owner_grant: MembershipGrantId,
    pub(crate) stream_id: AuthorStreamId,
}

impl CausalCoordinate for MembershipCoord {
    type StreamKey = MembershipStreamKey;

    fn stream_key(&self) -> Self::StreamKey {
        MembershipCoord::stream_key(self)
    }

    fn author_pubkey(&self) -> &str {
        &self.author_pubkey
    }

    fn author_owner_grant(&self) -> &MembershipGrantId {
        &self.author_owner_grant
    }

    fn seq(&self) -> u64 {
        self.seq
    }

    fn entry_hash(&self) -> ObjectHash {
        self.entry_hash
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StoreAssignment {
    role: MemberRole,
    provider_account_email: Option<String>,
}

impl CausalAssignment for StoreAssignment {
    fn is_owner(&self) -> bool {
        self.role == MemberRole::Owner
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct OwnerStreamBarrier {
    pub observed_streams: Vec<MembershipCoord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MembershipEntry {
    pub version: u32,
    pub store_id: String,
    pub author_pubkey: String,
    pub author_owner_grant: MembershipGrantId,
    pub stream_id: AuthorStreamId,
    pub seq: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_hash: Option<ObjectHash>,
    pub dependencies: Vec<MembershipCoord>,
    pub created_at: String,
    pub change: MembershipChange,
    pub signature: String,
}

impl MembershipEntry {
    pub fn coord(&self) -> MembershipCoord {
        MembershipCoord {
            author_pubkey: self.author_pubkey.clone(),
            author_owner_grant: self.author_owner_grant.clone(),
            stream_id: self.stream_id,
            seq: self.seq,
            entry_hash: entry_hash(self),
        }
    }

    pub fn provider_account_email(&self) -> Option<&str> {
        match &self.change {
            MembershipChange::SetMember {
                provider_account_email,
                ..
            } => provider_account_email.as_deref(),
            MembershipChange::Founder { .. } | MembershipChange::RemoveMember { .. } => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorHead {
    pub version: u32,
    pub store_id: String,
    pub author_pubkey: String,
    pub author_owner_grant: MembershipGrantId,
    pub stream_id: AuthorStreamId,
    pub seq: u64,
    pub tip_hash: ObjectHash,
    pub signature: String,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum MembershipError {
    #[error("membership chain is empty")]
    EmptyChain,
    #[error("membership entry {0} has unsupported version")]
    UnsupportedVersion(usize),
    #[error("membership entry {index} belongs to store {actual:?}, expected {expected:?}")]
    StoreMismatch {
        index: usize,
        expected: String,
        actual: String,
    },
    #[error("membership entry {0} has an invalid signature")]
    InvalidSignature(usize),
    #[error("membership entry {index} is in coordinate {actual:?}, expected {expected:?}")]
    CoordinateMismatch {
        index: usize,
        expected: Box<MembershipCoord>,
        actual: Box<MembershipCoord>,
    },
    #[error("membership stream {author}/{grant} is missing sequence {seq}")]
    MissingSequence {
        author: String,
        grant: MembershipGrantId,
        seq: u64,
    },
    #[error("membership stream {author}/{grant} has conflicting entries at sequence {seq}")]
    ConflictingSequence {
        author: String,
        grant: MembershipGrantId,
        seq: u64,
    },
    #[error("membership entry {index} has predecessor {actual:?}, expected {expected:?}")]
    BrokenStreamLink {
        index: usize,
        expected: Option<ObjectHash>,
        actual: Option<ObjectHash>,
    },
    #[error("membership entry {index} does not carry its complete own-stream dependency")]
    MissingOwnDependency { index: usize },
    #[error("membership entry {index} depends on missing coordinate {dependency:?}")]
    MissingDependency {
        index: usize,
        dependency: MembershipCoord,
    },
    #[error(
        "membership entry {index} dependency frontier is not strictly ordered by author stream"
    )]
    NonCanonicalDependencyFrontier { index: usize },
    #[error("membership dependency graph contains a cycle")]
    DependencyCycle,
    #[error("membership founder entry is invalid")]
    InvalidFounder,
    #[error("membership entry {index} author is not active under Owner grant {grant}")]
    AuthorGrantInactive {
        index: usize,
        grant: MembershipGrantId,
    },
    #[error("membership entry {index} creates an already-defined grant {grant}")]
    DuplicateGrant {
        index: usize,
        grant: MembershipGrantId,
    },
    #[error("membership entry {index} replaces or removes grant {grant} owned by another member")]
    GrantOwnerMismatch {
        index: usize,
        grant: MembershipGrantId,
    },
    #[error("membership entry {index} does not name the exact active grants for member {pubkey}")]
    GrantSetMismatch { index: usize, pubkey: String },
    #[error("membership entry {index} removes no exact grants")]
    EmptyRemoval { index: usize },
    #[error("membership entry {index} removes Owner grant {grant} without its exact observed-through coordinate")]
    MissingOwnerRevocationBarrier {
        index: usize,
        grant: MembershipGrantId,
    },
    #[error(
        "membership entry {index} carries an invalid revocation barrier for Owner grant {grant}"
    )]
    InvalidOwnerRevocationBarrier {
        index: usize,
        grant: MembershipGrantId,
    },
    #[error("concurrent Owner revocations leave no active Owner")]
    ConcurrentOwnerRevocationConflict,
    #[error("member {pubkey} has concurrent active grants {grants:?}")]
    ConcurrentMemberGrantConflict {
        pubkey: String,
        grants: Vec<MembershipGrantId>,
    },
    #[error("signer {0} has no active Owner grant")]
    SignerIsNotOwner(String),
    #[error("member {0} has no active grants")]
    NotAMember(String),
    #[error("membership author stream contains a pruned suffix and cannot be extended")]
    PrunedAuthorStream,
}

#[derive(Debug, Clone)]
struct GrantRecord {
    pubkey: String,
    role: MemberRole,
    provider_account_email: Option<String>,
    created_at: MembershipCoord,
}

#[derive(Debug, Clone, Default)]
struct CausalState {
    grants: BTreeMap<MembershipGrantId, GrantRecord>,
    removed: BTreeSet<MembershipGrantId>,
}

#[derive(Debug, Clone, Default)]
pub struct MembershipChain {
    entries: Vec<MembershipEntry>,
    coords: Vec<MembershipCoord>,
    state: CausalState,
    included: BTreeSet<MembershipCoord>,
}

impl MembershipChain {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_entries(entries: Vec<MembershipEntry>) -> Result<Self, MembershipError> {
        Self::from_entries_with_coords(
            entries
                .into_iter()
                .map(|entry| (entry.coord(), entry))
                .collect(),
        )
    }

    pub fn from_entries_with_coords(
        entries: Vec<(MembershipCoord, MembershipEntry)>,
    ) -> Result<Self, MembershipError> {
        if entries.is_empty() {
            return Err(MembershipError::EmptyChain);
        }
        let (coords, entries): (Vec<_>, Vec<_>) = entries.into_iter().unzip();
        let mut chain = Self {
            entries,
            coords,
            state: CausalState::default(),
            included: BTreeSet::new(),
        };
        chain.rebuild()?;
        Ok(chain)
    }

    pub fn entries(&self) -> &[MembershipEntry] {
        &self.entries
    }

    pub fn entries_with_coords(
        &self,
    ) -> impl Iterator<Item = (&MembershipCoord, &MembershipEntry)> {
        self.coords.iter().zip(self.entries.iter())
    }

    pub fn store_id(&self) -> Option<&str> {
        self.entries.first().map(|entry| entry.store_id.as_str())
    }

    pub fn founder_coord(&self) -> Option<&MembershipCoord> {
        self.entries_with_coords().find_map(|(coord, entry)| {
            matches!(entry.change, MembershipChange::Founder { .. }).then_some(coord)
        })
    }

    pub fn founder_pubkey(&self) -> Option<&str> {
        self.entries.iter().find_map(|entry| match &entry.change {
            MembershipChange::Founder { owner_pubkey, .. } => Some(owner_pubkey.as_str()),
            MembershipChange::SetMember { .. } | MembershipChange::RemoveMember { .. } => None,
        })
    }

    pub fn is_founded_by(&self, owner_pubkey: &str) -> bool {
        self.founder_pubkey() == Some(owner_pubkey)
    }

    pub fn validate(&self) -> Result<(), MembershipError> {
        let mut rebuilt = self.clone();
        rebuilt.rebuild()
    }

    pub fn add_entry(&mut self, entry: MembershipEntry) -> Result<(), MembershipError> {
        self.add_entry_at(entry.coord(), entry)
    }

    pub fn add_entry_at(
        &mut self,
        coord: MembershipCoord,
        entry: MembershipEntry,
    ) -> Result<(), MembershipError> {
        self.entries.push(entry);
        self.coords.push(coord);
        if let Err(error) = self.rebuild() {
            self.entries.pop();
            self.coords.pop();
            self.rebuild().expect("previous membership chain validated");
            return Err(error);
        }
        Ok(())
    }

    pub fn can_write_now(&self, pubkey: &str) -> bool {
        self.active_grants_for(pubkey)
            .iter()
            .any(|(_, record)| record.role.can_write())
    }

    pub(crate) fn contains_member_now(&self, pubkey: &str) -> bool {
        !self.active_grants_for(pubkey).is_empty()
    }

    pub fn is_owner_now(&self, pubkey: &str) -> bool {
        self.active_grants_for(pubkey)
            .iter()
            .any(|(_, record)| record.role == MemberRole::Owner)
    }

    pub fn authorizes_write_at(&self, coord: &MembershipCoord, pubkey: &str) -> bool {
        self.active_grants_for(pubkey)
            .iter()
            .any(|(_, record)| record.role.can_write() && record.created_at == *coord)
    }

    pub fn contains_coord(&self, expected: &MembershipCoord) -> bool {
        self.coords.iter().any(|coord| coord == expected)
    }

    pub fn current_members(&self) -> Vec<(String, MemberRole)> {
        let mut members = BTreeMap::new();
        for (grant, record) in &self.state.grants {
            if !self.state.removed.contains(grant) {
                members.insert(record.pubkey.clone(), record.role.clone());
            }
        }
        members.into_iter().collect()
    }

    pub fn current_member_provider_email(&self, pubkey: &str) -> Option<&str> {
        self.active_grants_for(pubkey)
            .into_iter()
            .next()
            .and_then(|(_, record)| record.provider_account_email.as_deref())
    }

    pub fn write_grant_coord(&self, pubkey: &str) -> Option<MembershipCoord> {
        self.active_grants_for(pubkey)
            .into_iter()
            .find(|(_, record)| record.role.can_write())
            .map(|(_, record)| record.created_at.clone())
    }

    pub fn active_grant_ids(&self, pubkey: &str) -> BTreeSet<MembershipGrantId> {
        self.active_grants_for(pubkey)
            .into_iter()
            .map(|(grant, _)| grant.clone())
            .collect()
    }

    pub fn active_owner_grant(&self, pubkey: &str) -> Option<MembershipGrantId> {
        self.active_grants_for(pubkey)
            .into_iter()
            .find(|(_, record)| record.role == MemberRole::Owner)
            .map(|(grant, _)| grant.clone())
    }

    pub(crate) fn reusable_author_streams(
        &self,
        author_pubkey: &str,
        grant: &MembershipGrantId,
    ) -> BTreeSet<AuthorStreamId> {
        self.effective_frontier()
            .into_iter()
            .filter(|coord| {
                coord.author_pubkey == author_pubkey
                    && coord.author_owner_grant == *grant
                    && self.raw_stream_tip(author_pubkey, grant, coord.stream_id)
                        == Some(coord.clone())
            })
            .map(|coord| coord.stream_id)
            .collect()
    }

    pub(crate) fn preferred_author_stream(
        &self,
        author_pubkey: &str,
        grant: &MembershipGrantId,
    ) -> Option<AuthorStreamId> {
        self.reusable_author_streams(author_pubkey, grant)
            .into_iter()
            .next_back()
    }

    /// Raw signed coverage: the greatest loaded coordinate in every stream,
    /// including suffixes removed by causal pruning.
    pub fn author_heads(&self) -> Vec<MembershipCoord> {
        self.frontier_from_coords(self.coords.iter())
    }

    /// Effective authoring frontier after causal pruning.
    pub fn effective_frontier(&self) -> Vec<MembershipCoord> {
        self.frontier_from_coords(
            self.coords
                .iter()
                .filter(|coord| self.included.contains(*coord)),
        )
    }

    fn frontier_from_coords<'a>(
        &self,
        coords: impl Iterator<Item = &'a MembershipCoord>,
    ) -> Vec<MembershipCoord> {
        let mut heads = BTreeMap::<MembershipStreamKey, MembershipCoord>::new();
        for coord in coords {
            heads
                .entry(coord.stream_key())
                .and_modify(|current| {
                    if coord.seq > current.seq {
                        *current = coord.clone();
                    }
                })
                .or_insert_with(|| coord.clone());
        }
        heads.into_values().collect()
    }

    pub fn stream_tip(
        &self,
        author_pubkey: &str,
        grant: &MembershipGrantId,
        stream_id: AuthorStreamId,
    ) -> Option<MembershipCoord> {
        self.effective_frontier().into_iter().find(|coord| {
            coord.author_pubkey == author_pubkey
                && coord.author_owner_grant == *grant
                && coord.stream_id == stream_id
        })
    }

    pub fn raw_stream_tip(
        &self,
        author_pubkey: &str,
        grant: &MembershipGrantId,
        stream_id: AuthorStreamId,
    ) -> Option<MembershipCoord> {
        self.coords
            .iter()
            .filter(|coord| {
                coord.author_pubkey == author_pubkey
                    && coord.author_owner_grant == *grant
                    && coord.stream_id == stream_id
            })
            .max_by_key(|coord| coord.seq)
            .cloned()
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn signed_head(&self, signer: &UserKeypair) -> Option<AuthorHead> {
        let author = keys::public_key_hex(signer);
        let grant = self.active_owner_grant(&author)?;
        let tip = self
            .effective_frontier()
            .into_iter()
            .filter(|coord| coord.author_pubkey == author && coord.author_owner_grant == grant)
            .max_by_key(|coord| coord.stream_id)?;
        Some(AuthorHead::signed(
            self.store_id()?.to_string(),
            grant,
            tip.stream_id,
            tip.seq,
            tip.entry_hash,
            signer,
        ))
    }

    pub fn signed_head_for_stream(
        &self,
        signer: &UserKeypair,
        stream_id: AuthorStreamId,
    ) -> Option<AuthorHead> {
        let author = keys::public_key_hex(signer);
        let grant = self.active_owner_grant(&author)?;
        let tip = self.stream_tip(&author, &grant, stream_id)?;
        Some(AuthorHead::signed(
            self.store_id()?.to_string(),
            grant,
            stream_id,
            tip.seq,
            tip.entry_hash,
            signer,
        ))
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn signed_set_member(
        &self,
        signer: &UserKeypair,
        user_pubkey: String,
        provider_account_email: Option<String>,
        role: MemberRole,
        created_at: String,
    ) -> Result<MembershipEntry, MembershipError> {
        let author = keys::public_key_hex(signer);
        let grant = self
            .active_owner_grant(&author)
            .ok_or_else(|| MembershipError::SignerIsNotOwner(author.clone()))?;
        let stream_id = self
            .preferred_author_stream(&author, &grant)
            .unwrap_or_else(|| {
                AuthorStreamId::from_digest(ObjectHash::digest(
                    format!("coven.test-membership-author-stream.v1\0{author}\0{grant}").as_bytes(),
                ))
            });
        self.signed_set_member_in_stream(
            signer,
            stream_id,
            user_pubkey,
            provider_account_email,
            role,
            created_at,
        )
    }

    pub fn signed_set_member_in_stream(
        &self,
        signer: &UserKeypair,
        stream_id: AuthorStreamId,
        user_pubkey: String,
        provider_account_email: Option<String>,
        role: MemberRole,
        created_at: String,
    ) -> Result<MembershipEntry, MembershipError> {
        let author = keys::public_key_hex(signer);
        let author_grant = self
            .active_owner_grant(&author)
            .ok_or_else(|| MembershipError::SignerIsNotOwner(author.clone()))?;
        let (seq, previous_hash) = self.next_stream_position(&author, &author_grant, stream_id)?;
        let grant_id = derive_grant_id(
            self.store_id().expect("validated chain has a store id"),
            &author,
            &author_grant,
            stream_id,
            seq,
            &user_pubkey,
        );
        let replaces = self.active_grant_ids(&user_pubkey);
        let owner_barriers = self.owner_barriers(&replaces);
        let mut entry = MembershipEntry {
            version: STORE_PROTOCOL_VERSION,
            store_id: self
                .store_id()
                .expect("validated chain has a store id")
                .to_string(),
            author_pubkey: author,
            author_owner_grant: author_grant,
            stream_id,
            seq,
            previous_hash,
            dependencies: self.frontier(),
            created_at,
            change: MembershipChange::SetMember {
                user_pubkey: user_pubkey.clone(),
                provider_account_email,
                role,
                grant_id,
                replaces,
                owner_barriers,
            },
            signature: String::new(),
        };
        sign_membership_entry(&mut entry, signer);
        let mut candidate = self.clone();
        candidate.add_entry(entry.clone())?;
        Ok(entry)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn signed_remove_member(
        &self,
        signer: &UserKeypair,
        user_pubkey: String,
        created_at: String,
    ) -> Result<MembershipEntry, MembershipError> {
        let author = keys::public_key_hex(signer);
        let grant = self
            .active_owner_grant(&author)
            .ok_or_else(|| MembershipError::SignerIsNotOwner(author.clone()))?;
        let stream_id = self
            .preferred_author_stream(&author, &grant)
            .unwrap_or_else(|| {
                AuthorStreamId::from_digest(ObjectHash::digest(
                    format!("coven.test-membership-author-stream.v1\0{author}\0{grant}").as_bytes(),
                ))
            });
        self.signed_remove_member_in_stream(signer, stream_id, user_pubkey, created_at)
    }

    pub fn signed_remove_member_in_stream(
        &self,
        signer: &UserKeypair,
        stream_id: AuthorStreamId,
        user_pubkey: String,
        created_at: String,
    ) -> Result<MembershipEntry, MembershipError> {
        let removes = self.active_grant_ids(&user_pubkey);
        if removes.is_empty() {
            return Err(MembershipError::NotAMember(user_pubkey));
        }
        let author = keys::public_key_hex(signer);
        let author_grant = self
            .active_owner_grant(&author)
            .ok_or_else(|| MembershipError::SignerIsNotOwner(author.clone()))?;
        let (seq, previous_hash) = self.next_stream_position(&author, &author_grant, stream_id)?;
        let owner_barriers = self.owner_barriers(&removes);
        let mut entry = MembershipEntry {
            version: STORE_PROTOCOL_VERSION,
            store_id: self
                .store_id()
                .expect("validated chain has a store id")
                .to_string(),
            author_pubkey: author,
            author_owner_grant: author_grant,
            stream_id,
            seq,
            previous_hash,
            dependencies: self.frontier(),
            created_at,
            change: MembershipChange::RemoveMember {
                user_pubkey,
                removes,
                owner_barriers,
            },
            signature: String::new(),
        };
        sign_membership_entry(&mut entry, signer);
        let mut candidate = self.clone();
        candidate.add_entry(entry.clone())?;
        Ok(entry)
    }

    fn next_stream_position(
        &self,
        author: &str,
        grant: &MembershipGrantId,
        stream_id: AuthorStreamId,
    ) -> Result<(u64, Option<ObjectHash>), MembershipError> {
        let raw_tip = self.raw_stream_tip(author, grant, stream_id);
        let effective_tip = self.stream_tip(author, grant, stream_id);
        if raw_tip != effective_tip {
            return Err(MembershipError::PrunedAuthorStream);
        }
        Ok(effective_tip.map_or((1, None), |tip| (tip.seq + 1, Some(tip.entry_hash))))
    }

    fn frontier(&self) -> Vec<MembershipCoord> {
        self.effective_frontier()
    }

    fn owner_barriers(
        &self,
        grants: &BTreeSet<MembershipGrantId>,
    ) -> BTreeMap<MembershipGrantId, OwnerStreamBarrier> {
        grants
            .iter()
            .filter_map(|grant| {
                let record = self.state.grants.get(grant)?;
                (record.role == MemberRole::Owner).then(|| {
                    let observed_streams = self
                        .effective_frontier()
                        .into_iter()
                        .filter(|coord| coord.author_owner_grant == *grant)
                        .collect();
                    (grant.clone(), OwnerStreamBarrier { observed_streams })
                })
            })
            .collect()
    }

    fn active_grants_for(&self, pubkey: &str) -> Vec<(&MembershipGrantId, &GrantRecord)> {
        self.state
            .grants
            .iter()
            .filter(|(grant, record)| {
                record.pubkey == pubkey && !self.state.removed.contains(*grant)
            })
            .collect()
    }

    fn rebuild(&mut self) -> Result<(), MembershipError> {
        let expected_store = self
            .entries
            .first()
            .ok_or(MembershipError::EmptyChain)?
            .store_id
            .clone();
        if expected_store.is_empty() {
            return Err(MembershipError::InvalidFounder);
        }

        for (index, (coord, entry)) in self.entries_with_coords().enumerate() {
            if entry.version != STORE_PROTOCOL_VERSION {
                return Err(MembershipError::UnsupportedVersion(index));
            }
            if entry.store_id != expected_store {
                return Err(MembershipError::StoreMismatch {
                    index,
                    expected: expected_store.clone(),
                    actual: entry.store_id.clone(),
                });
            }
            if !verify_membership_entry(entry) {
                return Err(MembershipError::InvalidSignature(index));
            }
            let actual = entry.coord();
            if *coord != actual {
                return Err(MembershipError::CoordinateMismatch {
                    index,
                    expected: Box::new(coord.clone()),
                    actual: Box::new(actual),
                });
            }
            if !entry
                .dependencies
                .windows(2)
                .all(|pair| pair[0].stream_key() < pair[1].stream_key())
            {
                return Err(MembershipError::NonCanonicalDependencyFrontier { index });
            }
            let barriers = match &entry.change {
                MembershipChange::SetMember { owner_barriers, .. }
                | MembershipChange::RemoveMember { owner_barriers, .. } => owner_barriers,
                MembershipChange::Founder { .. } => continue,
            };
            if let Some((grant, _)) = barriers.iter().find(|(_, barrier)| {
                !barrier
                    .observed_streams
                    .windows(2)
                    .all(|pair| pair[0].stream_key() < pair[1].stream_key())
            }) {
                return Err(MembershipError::InvalidOwnerRevocationBarrier {
                    index,
                    grant: grant.clone(),
                });
            }
        }

        let founders = self
            .entries
            .iter()
            .filter_map(|entry| {
                let MembershipChange::Founder {
                    owner_pubkey,
                    owner_grant_id,
                } = &entry.change
                else {
                    return None;
                };
                Some((entry, owner_pubkey, owner_grant_id))
            })
            .collect::<Vec<_>>();
        let [(founder, owner_pubkey, owner_grant_id)] = founders.as_slice() else {
            return Err(MembershipError::InvalidFounder);
        };
        if founder.author_pubkey != **owner_pubkey
            || founder.author_owner_grant != **owner_grant_id
            || owner_grant_id != &&derive_founder_grant_id(&founder.store_id, owner_pubkey)
            || founder.stream_id != derive_founder_stream_id(&founder.store_id, owner_pubkey)
        {
            return Err(MembershipError::InvalidFounder);
        }

        let reduced = reduce_store_membership(&self.entries)?;
        self.state = CausalState {
            grants: reduced
                .grants
                .into_iter()
                .map(|(grant, record)| {
                    (
                        grant,
                        GrantRecord {
                            pubkey: record.member_pubkey,
                            role: record.assignment.role,
                            provider_account_email: record.assignment.provider_account_email,
                            created_at: record.created_at,
                        },
                    )
                })
                .collect(),
            removed: reduced.removed,
        };
        self.included = reduced.included;
        Ok(())
    }
}

fn reduce_store_membership(
    entries: &[MembershipEntry],
) -> Result<causal_grants::ReducedGrants<MembershipCoord, StoreAssignment>, MembershipError> {
    let normalized = entries
        .iter()
        .map(|entry| {
            let dependencies = entry
                .dependencies
                .iter()
                .cloned()
                .map(|coord| (coord.stream_key(), coord))
                .collect();
            let change = match &entry.change {
                MembershipChange::Founder {
                    owner_pubkey,
                    owner_grant_id,
                } => CausalChange::Founder {
                    member_pubkey: owner_pubkey.clone(),
                    grant_id: owner_grant_id.clone(),
                    assignment: StoreAssignment {
                        role: MemberRole::Owner,
                        provider_account_email: None,
                    },
                },
                MembershipChange::SetMember {
                    user_pubkey,
                    provider_account_email,
                    role,
                    grant_id,
                    replaces,
                    owner_barriers,
                } => CausalChange::SetMember {
                    member_pubkey: user_pubkey.clone(),
                    assignment: StoreAssignment {
                        role: role.clone(),
                        provider_account_email: provider_account_email.clone(),
                    },
                    grant_id: grant_id.clone(),
                    replaces: replaces.clone(),
                    owner_barriers: owner_barriers
                        .iter()
                        .map(|(grant, barrier)| (grant.clone(), shared_store_barrier(barrier)))
                        .collect(),
                },
                MembershipChange::RemoveMember {
                    user_pubkey,
                    removes,
                    owner_barriers,
                } => CausalChange::RemoveMember {
                    member_pubkey: user_pubkey.clone(),
                    removes: removes.clone(),
                    owner_barriers: owner_barriers
                        .iter()
                        .map(|(grant, barrier)| (grant.clone(), shared_store_barrier(barrier)))
                        .collect(),
                },
            };
            CausalEntry {
                coord: entry.coord(),
                previous_hash: entry.previous_hash,
                dependencies,
                change,
            }
        })
        .collect::<Vec<_>>();
    causal_grants::reduce(&normalized).map_err(map_store_causal_error)
}

fn shared_store_barrier(barrier: &OwnerStreamBarrier) -> OwnerGrantBarrier<MembershipCoord> {
    let observed_streams = barrier
        .observed_streams
        .iter()
        .cloned()
        .map(|coord| (coord.stream_key(), coord))
        .collect();
    OwnerGrantBarrier { observed_streams }
}

fn map_store_causal_error(error: CausalGrantError<MembershipCoord>) -> MembershipError {
    match error {
        CausalGrantError::Empty => MembershipError::EmptyChain,
        CausalGrantError::ConflictingSequence { stream, seq } => {
            MembershipError::ConflictingSequence {
                author: stream.author_pubkey,
                grant: stream.author_owner_grant,
                seq,
            }
        }
        CausalGrantError::MissingSequence { stream, seq } => MembershipError::MissingSequence {
            author: stream.author_pubkey,
            grant: stream.author_owner_grant,
            seq,
        },
        CausalGrantError::BrokenStreamLink {
            index,
            expected,
            actual,
        } => MembershipError::BrokenStreamLink {
            index,
            expected,
            actual,
        },
        CausalGrantError::MissingOwnDependency { index } => {
            MembershipError::MissingOwnDependency { index }
        }
        CausalGrantError::DependencyStreamMismatch { .. } => {
            unreachable!("Store dependencies are normalized from their signed coordinates")
        }
        CausalGrantError::MissingDependency { index, dependency } => {
            MembershipError::MissingDependency { index, dependency }
        }
        CausalGrantError::DependencyCycle => MembershipError::DependencyCycle,
        CausalGrantError::InvalidFounder => MembershipError::InvalidFounder,
        CausalGrantError::AuthorGrantInactive { index, grant } => {
            MembershipError::AuthorGrantInactive { index, grant }
        }
        CausalGrantError::DuplicateGrant { index, grant } => {
            MembershipError::DuplicateGrant { index, grant }
        }
        CausalGrantError::GrantOwnerMismatch { index, grant } => {
            MembershipError::GrantOwnerMismatch { index, grant }
        }
        CausalGrantError::GrantSetMismatch {
            index,
            member_pubkey,
        } => MembershipError::GrantSetMismatch {
            index,
            pubkey: member_pubkey,
        },
        CausalGrantError::EmptyRemoval { index } => MembershipError::EmptyRemoval { index },
        CausalGrantError::MissingOwnerRevocationBarrier { index, grant } => {
            MembershipError::MissingOwnerRevocationBarrier { index, grant }
        }
        CausalGrantError::InvalidOwnerRevocationBarrier { index, grant } => {
            MembershipError::InvalidOwnerRevocationBarrier { index, grant }
        }
        CausalGrantError::ConcurrentOwnerRevocationConflict => {
            MembershipError::ConcurrentOwnerRevocationConflict
        }
        CausalGrantError::ConcurrentMemberGrantConflict {
            member_pubkey,
            grants,
        } => MembershipError::ConcurrentMemberGrantConflict {
            pubkey: member_pubkey,
            grants,
        },
    }
}

pub fn derive_founder_grant_id(store_id: &str, owner_pubkey: &str) -> MembershipGrantId {
    MembershipGrantId(ObjectHash::digest(
        format!("coven.membership-founder-grant.v1\0{store_id}\0{owner_pubkey}").as_bytes(),
    ))
}

fn derive_founder_stream_id(store_id: &str, owner_pubkey: &str) -> AuthorStreamId {
    AuthorStreamId::from_digest(ObjectHash::digest(
        format!("coven.membership-founder-stream.v1\0{store_id}\0{owner_pubkey}").as_bytes(),
    ))
}

pub fn derive_grant_id(
    store_id: &str,
    author_pubkey: &str,
    author_grant: &MembershipGrantId,
    stream_id: AuthorStreamId,
    seq: u64,
    user_pubkey: &str,
) -> MembershipGrantId {
    MembershipGrantId(ObjectHash::digest(
        format!(
            "coven.membership-grant.v1\0{store_id}\0{author_pubkey}\0{author_grant}\0{stream_id}\0{seq}\0{user_pubkey}"
        )
        .as_bytes(),
    ))
}

pub fn founder_entry(store_id: &str, owner: &UserKeypair, created_at: &str) -> MembershipEntry {
    let owner_pubkey = keys::public_key_hex(owner);
    let owner_grant_id = derive_founder_grant_id(store_id, &owner_pubkey);
    let stream_id = derive_founder_stream_id(store_id, &owner_pubkey);
    let mut entry = MembershipEntry {
        version: STORE_PROTOCOL_VERSION,
        store_id: store_id.to_string(),
        author_pubkey: owner_pubkey.clone(),
        author_owner_grant: owner_grant_id.clone(),
        stream_id,
        seq: 1,
        previous_hash: None,
        dependencies: Vec::new(),
        created_at: created_at.to_string(),
        change: MembershipChange::Founder {
            owner_pubkey,
            owner_grant_id,
        },
        signature: String::new(),
    };
    sign_membership_entry(&mut entry, owner);
    entry
}

pub fn canonical_bytes(entry: &MembershipEntry) -> Vec<u8> {
    #[derive(Serialize)]
    struct Signed<'a> {
        version: u32,
        store_id: &'a str,
        author_pubkey: &'a str,
        author_owner_grant: &'a MembershipGrantId,
        stream_id: AuthorStreamId,
        seq: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        previous_hash: Option<ObjectHash>,
        dependencies: &'a [MembershipCoord],
        created_at: &'a str,
        change: &'a MembershipChange,
    }
    serde_json::to_vec(&Signed {
        version: entry.version,
        store_id: &entry.store_id,
        author_pubkey: &entry.author_pubkey,
        author_owner_grant: &entry.author_owner_grant,
        stream_id: entry.stream_id,
        seq: entry.seq,
        previous_hash: entry.previous_hash,
        dependencies: &entry.dependencies,
        created_at: &entry.created_at,
        change: &entry.change,
    })
    .expect("membership signed fields serialize")
}

pub fn entry_hash(entry: &MembershipEntry) -> ObjectHash {
    ObjectHash::digest(
        &serde_json::to_vec(entry).expect("membership entry serialization cannot fail"),
    )
}

pub fn sign_membership_entry(entry: &mut MembershipEntry, keypair: &UserKeypair) {
    entry.author_pubkey = keys::public_key_hex(keypair);
    let (_, signature) = keys::sign_hex(keypair, &canonical_bytes(entry));
    entry.signature = signature;
}

pub fn verify_membership_entry(entry: &MembershipEntry) -> bool {
    keys::verify_signature_hex(
        &entry.author_pubkey,
        &entry.signature,
        &canonical_bytes(entry),
    )
}

impl AuthorHead {
    pub fn signed(
        store_id: String,
        author_owner_grant: MembershipGrantId,
        stream_id: AuthorStreamId,
        seq: u64,
        tip_hash: ObjectHash,
        signer: &UserKeypair,
    ) -> Self {
        let author_pubkey = keys::public_key_hex(signer);
        let mut head = Self {
            version: STORE_PROTOCOL_VERSION,
            store_id,
            author_pubkey,
            author_owner_grant,
            stream_id,
            seq,
            tip_hash,
            signature: String::new(),
        };
        let (_, signature) = keys::sign_hex(signer, &head.canonical_bytes());
        head.signature = signature;
        head
    }

    pub fn verify(&self) -> bool {
        self.version == STORE_PROTOCOL_VERSION
            && keys::verify_signature_hex(
                &self.author_pubkey,
                &self.signature,
                &self.canonical_bytes(),
            )
    }

    fn canonical_bytes(&self) -> Vec<u8> {
        #[derive(Serialize)]
        struct Signed<'a> {
            version: u32,
            store_id: &'a str,
            author_pubkey: &'a str,
            author_owner_grant: &'a MembershipGrantId,
            stream_id: AuthorStreamId,
            seq: u64,
            tip_hash: ObjectHash,
        }
        serde_json::to_vec(&Signed {
            version: self.version,
            store_id: &self.store_id,
            author_pubkey: &self.author_pubkey,
            author_owner_grant: &self.author_owner_grant,
            stream_id: self.stream_id,
            seq: self.seq,
            tip_hash: self.tip_hash,
        })
        .expect("membership head signed fields serialize")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::store_commit::{
        StoreCommitOrder, StoreDeviceRegistrationRef, StoreDeviceRegistrationState,
    };

    fn key() -> UserKeypair {
        UserKeypair::generate()
    }

    fn founded(store_id: &str, owner: &UserKeypair) -> MembershipChain {
        MembershipChain::from_entries(vec![founder_entry(store_id, owner, "founder")]).unwrap()
    }

    fn serial_authorization_with_follower(
        root: ObjectHash,
        owner: &UserKeypair,
        follower: &UserKeypair,
    ) -> SerialAuthorizationState {
        let founder = founder_entry("serial-registration", owner, "founder");
        let authorization = SerialAuthorizationState::from_founder(root, &founder).unwrap();
        let add = authorization
            .membership
            .signed_set_member(
                owner,
                keys::public_key_hex(follower),
                None,
                MemberRole::Follower,
                "add follower".to_string(),
            )
            .unwrap();
        SerialAuthorizationState {
            membership: authorization.membership.apply(&add).unwrap(),
            key_generation: authorization.key_generation,
        }
    }

    fn registration_commit(
        root: ObjectHash,
        seq: u64,
        previous_commit_hash: Option<ObjectHash>,
        registration: &StoreDeviceRegistration,
        signer: &UserKeypair,
    ) -> StoreBatchCommit {
        StoreBatchCommit::signed_with_registrations(
            root,
            crate::WriteId::from_generated(format!("registration-{seq}")),
            registration.device_id.clone(),
            StoreCommitOrder::Serial {
                seq,
                previous_commit_hash,
            },
            None,
            vec![StoreDeviceRegistrationRef::from_registration(registration)],
            signer,
        )
        .unwrap()
    }

    #[test]
    fn serial_follower_can_activate_and_retire_its_exact_registration() {
        let root = ObjectHash::digest(b"follower registration root");
        let owner = key();
        let follower = key();
        let authorization = serial_authorization_with_follower(root, &owner, &follower);
        let active = StoreDeviceRegistration::signed(
            root,
            "follower-device".to_string(),
            1,
            None,
            StoreDeviceRegistrationState::Active,
            &follower,
        )
        .unwrap();
        let active_commit = registration_commit(root, 1, None, &active, &follower);
        authorization
            .authorize_and_apply_with_registrations(&active_commit, std::slice::from_ref(&active))
            .expect("active self-registration");

        let retired = StoreDeviceRegistration::signed(
            root,
            "follower-device".to_string(),
            2,
            Some(active.registration_hash()),
            StoreDeviceRegistrationState::Retired,
            &follower,
        )
        .unwrap();
        let retirement_commit = registration_commit(
            root,
            2,
            Some(active_commit.commit_hash()),
            &retired,
            &follower,
        );
        authorization
            .authorize_and_apply_with_registrations(
                &retirement_commit,
                std::slice::from_ref(&retired),
            )
            .expect("self-signed retirement");
    }

    #[test]
    fn serial_follower_cannot_activate_another_identity_or_mixed_payload() {
        let root = ObjectHash::digest(b"follower registration negatives");
        let owner = key();
        let follower = key();
        let outsider = key();
        let authorization = serial_authorization_with_follower(root, &owner, &follower);
        let another_identity = StoreDeviceRegistration::signed(
            root,
            "follower-device".to_string(),
            1,
            None,
            StoreDeviceRegistrationState::Active,
            &outsider,
        )
        .unwrap();
        let another_identity_commit =
            registration_commit(root, 1, None, &another_identity, &follower);
        assert!(matches!(
            authorization.authorize_and_apply_with_registrations(
                &another_identity_commit,
                std::slice::from_ref(&another_identity),
            ),
            Err(SerialMembershipError::AuthorIsNotWriter(_))
        ));

        let own = StoreDeviceRegistration::signed(
            root,
            "follower-device".to_string(),
            1,
            None,
            StoreDeviceRegistrationState::Active,
            &follower,
        )
        .unwrap();
        let mixed = StoreBatchCommit::signed_batch(
            root,
            crate::WriteId::from_generated("mixed-registration".to_string()),
            own.device_id.clone(),
            StoreCommitOrder::Serial {
                seq: 1,
                previous_commit_hash: None,
            },
            None,
            None,
            vec![StoreDeviceRegistrationRef::from_registration(&own)],
            Vec::new(),
            Some(crate::sync::store_commit::StorePackageInput {
                schema_version: 1,
                bytes: b"row payload",
            }),
            &[],
            &follower,
        )
        .unwrap();
        assert!(matches!(
            authorization
                .authorize_and_apply_with_registrations(&mixed, std::slice::from_ref(&own)),
            Err(SerialMembershipError::AuthorIsNotWriter(_))
        ));
    }

    #[test]
    fn timestamp_does_not_change_causal_authorization() {
        let owner = key();
        let member = key();
        let mut chain = founded("store", &owner);
        let add = chain
            .signed_set_member(
                &owner,
                keys::public_key_hex(&member),
                None,
                MemberRole::Member,
                "9999".to_string(),
            )
            .unwrap();
        chain.add_entry(add).unwrap();
        let remove = chain
            .signed_remove_member(&owner, keys::public_key_hex(&member), "0000".to_string())
            .unwrap();
        chain.add_entry(remove).unwrap();
        assert!(!chain.can_write_now(&keys::public_key_hex(&member)));
    }

    #[test]
    fn signed_candidate_is_validated_before_it_is_returned() {
        let owner = key();
        let chain = founded("store", &owner);

        assert!(matches!(
            chain.signed_remove_member(
                &owner,
                keys::public_key_hex(&owner),
                "remove last owner".to_string(),
            ),
            Err(MembershipError::ConcurrentOwnerRevocationConflict)
        ));
    }

    #[test]
    fn dependency_frontier_must_be_strictly_ordered_by_author_stream() {
        let founder = key();
        let second_owner = key();
        let mut chain = founded("store", &founder);
        let add_owner = chain
            .signed_set_member(
                &founder,
                keys::public_key_hex(&second_owner),
                None,
                MemberRole::Owner,
                "add owner".to_string(),
            )
            .unwrap();
        chain.add_entry(add_owner).unwrap();
        let second_stream = chain
            .signed_set_member(
                &second_owner,
                keys::public_key_hex(&key()),
                None,
                MemberRole::Member,
                "second stream".to_string(),
            )
            .unwrap();
        chain.add_entry(second_stream).unwrap();
        let mut unsorted = chain
            .signed_set_member(
                &founder,
                keys::public_key_hex(&key()),
                None,
                MemberRole::Member,
                "candidate".to_string(),
            )
            .unwrap();
        assert!(unsorted.dependencies.len() > 1);
        unsorted.dependencies.reverse();
        sign_membership_entry(&mut unsorted, &founder);

        assert!(matches!(
            chain.add_entry(unsorted),
            Err(MembershipError::NonCanonicalDependencyFrontier { .. })
        ));
    }

    #[test]
    fn owner_barrier_must_be_strictly_ordered_by_author_stream() {
        let founder = key();
        let second_owner = key();
        let second_owner_pubkey = keys::public_key_hex(&second_owner);
        let mut chain = founded("store", &founder);
        let add_owner = chain
            .signed_set_member(
                &founder,
                second_owner_pubkey.clone(),
                None,
                MemberRole::Owner,
                "add owner".to_string(),
            )
            .unwrap();
        chain.add_entry(add_owner).unwrap();
        for (stream_id, timestamp) in [
            (AuthorStreamId::from_bytes([1; 16]), "first stream"),
            (AuthorStreamId::from_bytes([2; 16]), "second stream"),
        ] {
            let authored = chain
                .signed_set_member_in_stream(
                    &second_owner,
                    stream_id,
                    keys::public_key_hex(&key()),
                    None,
                    MemberRole::Member,
                    timestamp.to_string(),
                )
                .unwrap();
            chain.add_entry(authored).unwrap();
        }
        let mut removal = chain
            .signed_remove_member(&founder, second_owner_pubkey, "remove owner".to_string())
            .unwrap();
        let MembershipChange::RemoveMember { owner_barriers, .. } = &mut removal.change else {
            unreachable!();
        };
        let observed = &mut owner_barriers
            .values_mut()
            .next()
            .expect("owner removal barrier")
            .observed_streams;
        assert!(observed.len() > 1);
        observed.reverse();
        sign_membership_entry(&mut removal, &founder);

        assert!(matches!(
            chain.add_entry(removal),
            Err(MembershipError::InvalidOwnerRevocationBarrier { .. })
        ));
    }

    #[test]
    fn owner_readd_uses_a_new_sequence_one_stream() {
        let owner = key();
        let second = key();
        let mut chain = founded("store", &owner);
        let first = chain
            .signed_set_member(
                &owner,
                keys::public_key_hex(&second),
                None,
                MemberRole::Owner,
                "add".to_string(),
            )
            .unwrap();
        chain.add_entry(first).unwrap();
        let old_grant = chain
            .active_owner_grant(&keys::public_key_hex(&second))
            .unwrap();
        let remove = chain
            .signed_remove_member(&owner, keys::public_key_hex(&second), "remove".to_string())
            .unwrap();
        chain.add_entry(remove).unwrap();
        let readd = chain
            .signed_set_member(
                &owner,
                keys::public_key_hex(&second),
                None,
                MemberRole::Owner,
                "readd".to_string(),
            )
            .unwrap();
        chain.add_entry(readd).unwrap();
        let new_grant = chain
            .active_owner_grant(&keys::public_key_hex(&second))
            .unwrap();
        assert_ne!(old_grant, new_grant);
        let authored = chain
            .signed_set_member(
                &second,
                keys::public_key_hex(&key()),
                None,
                MemberRole::Member,
                "authored".to_string(),
            )
            .unwrap();
        assert_eq!(authored.seq, 1);
        assert_eq!(authored.author_owner_grant, new_grant);
    }

    #[test]
    fn owner_self_removal_remains_effective_when_its_grant_is_capped_before_first() {
        let founder = key();
        let departing_owner = key();
        let departing_pubkey = keys::public_key_hex(&departing_owner);
        let mut chain = founded("store", &founder);
        let add_owner = chain
            .signed_set_member(
                &founder,
                departing_pubkey.clone(),
                None,
                MemberRole::Owner,
                "add owner".to_string(),
            )
            .unwrap();
        chain.add_entry(add_owner).unwrap();

        let self_removal = chain
            .signed_remove_member(
                &departing_owner,
                departing_pubkey.clone(),
                "self removal".to_string(),
            )
            .unwrap();
        assert!(matches!(
            &self_removal.change,
            MembershipChange::RemoveMember { owner_barriers, .. }
                if owner_barriers.values().all(|barrier| barrier.observed_streams.is_empty())
        ));
        chain.add_entry(self_removal).unwrap();

        assert!(!chain.is_owner_now(&departing_pubkey));
    }

    #[test]
    fn before_first_barrier_excludes_every_entry_from_the_revoked_owner_stream() {
        let founder = key();
        let second_owner = key();
        let target = key();
        let mut observed = founded("store", &founder);
        let add_owner = observed
            .signed_set_member(
                &founder,
                keys::public_key_hex(&second_owner),
                None,
                MemberRole::Owner,
                "add owner".to_string(),
            )
            .unwrap();
        observed.add_entry(add_owner).unwrap();

        let stale_entry = observed
            .signed_set_member(
                &second_owner,
                keys::public_key_hex(&target),
                None,
                MemberRole::Member,
                "stale entry".to_string(),
            )
            .unwrap();
        let removal = observed
            .signed_remove_member(
                &founder,
                keys::public_key_hex(&second_owner),
                "remove owner".to_string(),
            )
            .unwrap();
        assert!(matches!(
            &removal.change,
            MembershipChange::RemoveMember { owner_barriers, .. }
                if owner_barriers.values().all(|barrier| barrier.observed_streams.is_empty())
        ));

        let mut entries = observed.entries().to_vec();
        entries.extend([removal, stale_entry]);
        let chain = MembershipChain::from_entries(entries).unwrap();
        assert!(!chain.can_write_now(&keys::public_key_hex(&target)));
        assert!(chain
            .author_heads()
            .iter()
            .any(|coord| coord.author_pubkey == keys::public_key_hex(&second_owner)));
        assert!(chain
            .effective_frontier()
            .iter()
            .all(|coord| coord.author_pubkey != keys::public_key_hex(&second_owner)));
    }

    #[test]
    fn through_barrier_keeps_its_exact_prefix_and_prunes_the_stale_suffix() {
        let founder = key();
        let second_owner = key();
        let first_target = key();
        let second_target = key();
        let third_target = key();
        let mut observed = founded("store", &founder);
        let add_owner = observed
            .signed_set_member(
                &founder,
                keys::public_key_hex(&second_owner),
                None,
                MemberRole::Owner,
                "add owner".to_string(),
            )
            .unwrap();
        observed.add_entry(add_owner).unwrap();
        let first = observed
            .signed_set_member(
                &second_owner,
                keys::public_key_hex(&first_target),
                None,
                MemberRole::Member,
                "first".to_string(),
            )
            .unwrap();
        observed.add_entry(first.clone()).unwrap();

        let removal = observed
            .signed_remove_member(
                &founder,
                keys::public_key_hex(&second_owner),
                "remove owner".to_string(),
            )
            .unwrap();
        assert!(matches!(
            &removal.change,
            MembershipChange::RemoveMember { owner_barriers, .. }
                if owner_barriers.values().any(|barrier| barrier.observed_streams == vec![first.coord()])
        ));

        let second = observed
            .signed_set_member(
                &second_owner,
                keys::public_key_hex(&second_target),
                None,
                MemberRole::Member,
                "second".to_string(),
            )
            .unwrap();
        let mut exact_entries = observed.entries().to_vec();
        exact_entries.extend([removal.clone(), second.clone()]);
        let exact = MembershipChain::from_entries(exact_entries).unwrap();
        assert!(exact.can_write_now(&keys::public_key_hex(&first_target)));
        assert!(!exact.can_write_now(&keys::public_key_hex(&second_target)));

        let mut stale = observed;
        stale.add_entry(second).unwrap();
        let third = stale
            .signed_set_member(
                &second_owner,
                keys::public_key_hex(&third_target),
                None,
                MemberRole::Member,
                "third".to_string(),
            )
            .unwrap();
        stale.add_entry(third.clone()).unwrap();
        let mut beyond_entries = stale.entries().to_vec();
        beyond_entries.push(removal);
        let pruned = MembershipChain::from_entries(beyond_entries).unwrap();
        assert!(pruned.can_write_now(&keys::public_key_hex(&first_target)));
        assert!(!pruned.can_write_now(&keys::public_key_hex(&second_target)));
        assert!(!pruned.can_write_now(&keys::public_key_hex(&third_target)));
    }

    #[test]
    fn through_barrier_rejects_a_coordinate_hash_that_is_not_its_dependency() {
        let founder = key();
        let second_owner = key();
        let mut chain = founded("store", &founder);
        let add_owner = chain
            .signed_set_member(
                &founder,
                keys::public_key_hex(&second_owner),
                None,
                MemberRole::Owner,
                "add owner".to_string(),
            )
            .unwrap();
        chain.add_entry(add_owner).unwrap();
        let authored = chain
            .signed_set_member(
                &second_owner,
                keys::public_key_hex(&key()),
                None,
                MemberRole::Member,
                "authored".to_string(),
            )
            .unwrap();
        chain.add_entry(authored).unwrap();
        let mut removal = chain
            .signed_remove_member(
                &founder,
                keys::public_key_hex(&second_owner),
                "remove owner".to_string(),
            )
            .unwrap();
        let MembershipChange::RemoveMember { owner_barriers, .. } = &mut removal.change else {
            unreachable!();
        };
        let barrier = owner_barriers
            .values_mut()
            .next()
            .expect("owner removal barrier")
            .observed_streams
            .first_mut()
            .expect("observed owner stream");
        barrier.entry_hash = ObjectHash::digest(b"wrong barrier hash");
        sign_membership_entry(&mut removal, &founder);
        assert!(matches!(
            chain.add_entry(removal),
            Err(MembershipError::InvalidOwnerRevocationBarrier { .. })
        ));
    }

    #[test]
    fn cross_store_replay_fails_even_with_the_same_founder_key() {
        let owner = key();
        let from_a = founder_entry("store-a", &owner, "founder");
        let mut replayed = from_a.clone();
        replayed.store_id = "store-b".to_string();
        assert!(!verify_membership_entry(&replayed));
        assert!(MembershipChain::from_entries(vec![from_a])
            .unwrap()
            .is_founded_by(&keys::public_key_hex(&owner)));
    }

    #[test]
    fn created_at_is_signed_but_never_orders_entries() {
        let owner = key();
        let entry = founder_entry("store", &owner, "display-time");
        let mut tampered = entry.clone();
        tampered.created_at = "other".to_string();
        assert!(!verify_membership_entry(&tampered));
    }

    #[test]
    fn serial_membership_applies_only_against_its_exact_previous_state() {
        let owner = key();
        let first_member = key();
        let second_member = key();
        let root = ObjectHash::digest(b"Serial membership root");
        let state = SerialMembershipState::from_founder(
            root,
            &founder_entry("serial-store", &owner, "founder"),
        )
        .unwrap();
        let first = state
            .signed_set_member(
                &owner,
                keys::public_key_hex(&first_member),
                None,
                MemberRole::Member,
                "first".to_string(),
            )
            .unwrap();
        let stale = state
            .signed_set_member(
                &owner,
                keys::public_key_hex(&second_member),
                None,
                MemberRole::Member,
                "stale".to_string(),
            )
            .unwrap();
        let after_first = state.apply(&first).unwrap();
        assert!(matches!(
            after_first.apply(&stale),
            Err(SerialMembershipError::StaleState { .. })
        ));

        let removal = after_first
            .signed_remove_member(
                &owner,
                keys::public_key_hex(&first_member),
                "remove".to_string(),
            )
            .unwrap();
        let after_removal = after_first.apply(&removal).unwrap();
        assert!(!after_removal.can_write(&keys::public_key_hex(&first_member)));
        assert_eq!(
            removal.previous_state_hash,
            after_first.state_hash(),
            "removal names the exact globally preceding membership state"
        );
    }
}

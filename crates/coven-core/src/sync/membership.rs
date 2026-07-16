//! Store-bound causal membership protocol.
//!
//! Every causal author stream is identified by its author, the Owner grant that
//! authorizes it, and an independently generated stream id. Entries carry the
//! complete observed stream frontier; authorization is derived from that causal
//! past, never from `created_at`.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::causal_grants::{
    self, CausalAssignment, CausalChange, CausalCoordinate, CausalEntry, CausalGrantConflict,
    CausalGrantError, CausalGrantStatus, OwnerGrantBarrier,
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
    pub member_pubkey: String,
    pub role: MemberRole,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_account_email: Option<String>,
    pub created_at_generation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SerialMembershipState {
    store_root_hash: ObjectHash,
    active_grants: BTreeMap<MembershipGrantId, SerialMember>,
    current_generation: u64,
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
        grant_id: MembershipGrantId,
        replaces: BTreeSet<MembershipGrantId>,
    },
    RemoveMember {
        user_pubkey: String,
        removes: BTreeSet<MembershipGrantId>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SerialMembershipEntry {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub previous_state_hash: ObjectHash,
    pub created_at_generation: u64,
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
    #[error("Serial membership generation is {actual}, expected {expected}")]
    MembershipGeneration { expected: u64, actual: u64 },
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
        if commit.membership_authority.is_some() {
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
        let membership = match commit.control.as_ref() {
            Some(control) => self
                .membership
                .apply_at(control.serial_membership_entry(), commit.seq())?,
            None => self.membership.advance_to(commit.seq())?,
        };
        let Some(control) = commit.control.as_ref() else {
            return Ok(Self {
                membership,
                key_generation: self.key_generation,
            });
        };
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
            active_grants: BTreeMap::from([(
                owner_grant_id.clone(),
                SerialMember {
                    member_pubkey: owner_pubkey.clone(),
                    role: MemberRole::Owner,
                    provider_account_email: None,
                    created_at_generation: 0,
                },
            )]),
            current_generation: 0,
        })
    }

    pub fn state_hash(&self) -> ObjectHash {
        #[derive(Serialize)]
        struct StateFields<'a> {
            domain: &'static str,
            store_root_hash: ObjectHash,
            active_grants: &'a BTreeMap<MembershipGrantId, SerialMember>,
        }
        ObjectHash::digest(
            &serde_json::to_vec(&StateFields {
                domain: "coven.serial-membership-state.v1",
                store_root_hash: self.store_root_hash,
                active_grants: &self.active_grants,
            })
            .expect("Serial membership state serialization cannot fail"),
        )
    }

    pub fn store_root_hash(&self) -> ObjectHash {
        self.store_root_hash
    }

    pub fn current_members(&self) -> Vec<(String, MemberRole)> {
        self.active_grants
            .values()
            .map(|member| (member.member_pubkey.clone(), member.role.clone()))
            .collect::<BTreeMap<_, _>>()
            .into_iter()
            .collect()
    }

    pub fn current_member_provider_email(&self, pubkey: &str) -> Option<&str> {
        self.active_grants
            .values()
            .find(|member| member.member_pubkey == pubkey)
            .and_then(|member| member.provider_account_email.as_deref())
    }

    pub fn can_write(&self, pubkey: &str) -> bool {
        self.active_grants
            .values()
            .any(|member| member.member_pubkey == pubkey && member.role.can_write())
    }

    fn contains(&self, pubkey: &str) -> bool {
        self.active_grants
            .values()
            .any(|member| member.member_pubkey == pubkey)
    }

    pub fn is_owner(&self, pubkey: &str) -> bool {
        self.active_grants
            .values()
            .any(|member| member.member_pubkey == pubkey && member.role == MemberRole::Owner)
    }

    pub fn signed_set_member(
        &self,
        signer: &UserKeypair,
        user_pubkey: String,
        provider_account_email: Option<String>,
        role: MemberRole,
        created_at: String,
    ) -> Result<SerialMembershipEntry, SerialMembershipError> {
        let created_at_generation = self.next_generation()?;
        let grant_id =
            serial_membership_grant_id(self.store_root_hash, created_at_generation, &user_pubkey);
        let replaces = self.active_grants_for(&user_pubkey);
        self.signed_change(
            signer,
            SerialMembershipChange::SetMember {
                user_pubkey,
                provider_account_email,
                role,
                grant_id,
                replaces,
            },
            created_at_generation,
            created_at,
        )
    }

    pub fn signed_remove_member(
        &self,
        signer: &UserKeypair,
        user_pubkey: String,
        created_at: String,
    ) -> Result<SerialMembershipEntry, SerialMembershipError> {
        let removes = self.active_grants_for(&user_pubkey);
        if removes.is_empty() {
            return Err(SerialMembershipError::NotAMember(user_pubkey));
        }
        let created_at_generation = self.next_generation()?;
        self.signed_change(
            signer,
            SerialMembershipChange::RemoveMember {
                user_pubkey,
                removes,
            },
            created_at_generation,
            created_at,
        )
    }

    fn signed_change(
        &self,
        signer: &UserKeypair,
        change: SerialMembershipChange,
        created_at_generation: u64,
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
            created_at_generation,
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
        self.apply_at(entry, entry.created_at_generation)
    }

    fn apply_at(
        &self,
        entry: &SerialMembershipEntry,
        generation: u64,
    ) -> Result<Self, SerialMembershipError> {
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
        let expected_generation = self.next_generation()?;
        if entry.created_at_generation != generation || generation != expected_generation {
            return Err(SerialMembershipError::MembershipGeneration {
                expected: expected_generation,
                actual: entry.created_at_generation,
            });
        }
        let mut next = self.clone();
        match &entry.change {
            SerialMembershipChange::SetMember {
                user_pubkey,
                provider_account_email,
                role,
                grant_id,
                replaces,
            } => {
                if *replaces != self.active_grants_for(user_pubkey)
                    || next.active_grants.contains_key(grant_id)
                {
                    return Err(SerialMembershipError::StaleState {
                        expected,
                        actual: entry.previous_state_hash,
                    });
                }
                for replaced in replaces {
                    next.active_grants.remove(replaced);
                }
                next.active_grants.insert(
                    grant_id.clone(),
                    SerialMember {
                        member_pubkey: user_pubkey.clone(),
                        role: role.clone(),
                        provider_account_email: provider_account_email.clone(),
                        created_at_generation: generation,
                    },
                );
            }
            SerialMembershipChange::RemoveMember {
                user_pubkey,
                removes,
            } => {
                if *removes != self.active_grants_for(user_pubkey) {
                    return Err(SerialMembershipError::NotAMember(user_pubkey.clone()));
                }
                for removed in removes {
                    next.active_grants.remove(removed);
                }
                if !next
                    .active_grants
                    .values()
                    .any(|member| member.role == MemberRole::Owner)
                {
                    return Err(SerialMembershipError::LastOwner);
                }
            }
        }
        next.current_generation = generation;
        Ok(next)
    }

    fn active_grants_for(&self, pubkey: &str) -> BTreeSet<MembershipGrantId> {
        self.active_grants
            .iter()
            .filter_map(|(grant, member)| (member.member_pubkey == pubkey).then_some(grant.clone()))
            .collect()
    }

    fn next_generation(&self) -> Result<u64, SerialMembershipError> {
        self.current_generation
            .checked_add(1)
            .ok_or(SerialMembershipError::MembershipGeneration {
                expected: self.current_generation,
                actual: self.current_generation,
            })
    }

    fn advance_to(&self, generation: u64) -> Result<Self, SerialMembershipError> {
        let expected = self.next_generation()?;
        if generation != expected {
            return Err(SerialMembershipError::MembershipGeneration {
                expected,
                actual: generation,
            });
        }
        let mut next = self.clone();
        next.current_generation = generation;
        Ok(next)
    }
}

fn serial_membership_grant_id(
    store_root_hash: ObjectHash,
    created_at_generation: u64,
    member_pubkey: &str,
) -> MembershipGrantId {
    MembershipGrantId(ObjectHash::digest(
        format!(
            "coven.serial-membership-grant.v1\0{store_root_hash}\0{created_at_generation}\0{member_pubkey}"
        )
        .as_bytes(),
    ))
}

impl SerialMembershipChange {
    pub fn user_pubkey(&self) -> &str {
        match self {
            Self::SetMember { user_pubkey, .. } | Self::RemoveMember { user_pubkey, .. } => {
                user_pubkey
            }
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
            created_at_generation: u64,
            author_pubkey: &'a str,
            created_at: &'a str,
            change: &'a SerialMembershipChange,
        }
        serde_json::to_vec(&Signed {
            domain: "coven.serial-membership-entry.v1",
            version: self.version,
            store_root_hash: self.store_root_hash,
            previous_state_hash: self.previous_state_hash,
            created_at_generation: self.created_at_generation,
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
    ResolutionActivation {
        resolution: StoreMembershipConflictResolutionRef,
    },
}

impl MembershipChange {
    pub fn user_pubkey(&self) -> &str {
        match self {
            Self::Founder { owner_pubkey, .. } => owner_pubkey,
            Self::SetMember { user_pubkey, .. } | Self::RemoveMember { user_pubkey, .. } => {
                user_pubkey
            }
            Self::ResolutionActivation { resolution } => &resolution.resolver_pubkey,
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
    pub previous_hash: Option<ObjectHash>,
    pub dependencies: Vec<MembershipCoord>,
    pub resolution_dependencies: Vec<StoreMembershipConflictResolutionRef>,
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
            MembershipChange::Founder { .. }
            | MembershipChange::RemoveMember { .. }
            | MembershipChange::ResolutionActivation { .. } => None,
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
    pub resolutions: Vec<StoreMembershipConflictResolutionRef>,
    pub signature: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MembershipHeadRef {
    pub coord: MembershipCoord,
    pub head_hash: ObjectHash,
}

impl MembershipHeadRef {
    pub fn from_head(head: &AuthorHead) -> Self {
        Self {
            coord: head.entry_coord(),
            head_hash: head.head_hash(),
        }
    }
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
    #[error("membership history leaves no active Owner")]
    NoActiveOwner,
    #[error(
        "membership revocation cycle has {sources} sources, exceeding the protocol limit of {maximum}"
    )]
    RevocationCycleTooWide { sources: usize, maximum: usize },
    #[error("signer {0} has no active Owner grant")]
    SignerIsNotOwner(String),
    #[error("member {0} has no active grants")]
    NotAMember(String),
    #[error("membership author stream contains a pruned suffix and cannot be extended")]
    PrunedAuthorStream,
    #[error("membership author has no reusable stream; a fresh persisted stream is required")]
    MissingAuthorStream,
    #[error("membership resolution activation entry {0} is invalid")]
    InvalidResolutionActivation(usize),
    #[error("membership resolution activation requires a fresh persisted author stream")]
    ResolutionActivationRequiresFreshStream,
    #[error("membership has an unresolved semantic conflict")]
    Conflict,
    #[error("membership conflict is missing its exact signed raw heads")]
    MissingConflictHeads,
    #[error("membership conflict resolution does not name exact validated conflict evidence")]
    InvalidConflictResolution,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MembershipGrantRecord {
    pub member_pubkey: String,
    pub role: MemberRole,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_account_email: Option<String>,
    pub creation_authority: MembershipGrantCreationAuthority,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum MembershipGrantCreationAuthority {
    Entry(MembershipCoord),
    ConflictResolution(StoreMembershipConflictResolutionRef),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedStoreMembership {
    pub active_grants: BTreeMap<MembershipGrantId, MembershipGrantRecord>,
    pub state_hash: ObjectHash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreMembershipBranch {
    pub heads: Vec<MembershipHeadRef>,
    pub effective_frontier: Vec<MembershipCoord>,
    pub active_grants: BTreeMap<MembershipGrantId, MembershipGrantRecord>,
    pub state_hash: ObjectHash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum MembershipConflict {
    ConcurrentMemberAssignments {
        conflict_hash: ObjectHash,
        heads: Vec<MembershipHeadRef>,
        effective_frontier: Vec<MembershipCoord>,
        member_pubkey: String,
        conflicting_grants: BTreeMap<MembershipGrantId, MembershipGrantRecord>,
        uncontested_grants: BTreeMap<MembershipGrantId, MembershipGrantRecord>,
    },
    RevocationCycle {
        conflict_hash: ObjectHash,
        heads: Vec<MembershipHeadRef>,
        cyclic_sources: Vec<MembershipCoord>,
        involved_owner_grants: BTreeSet<MembershipGrantId>,
        maximal_valid_branches: Vec<StoreMembershipBranch>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum MembershipStatus {
    Resolved(ResolvedStoreMembership),
    Conflict(MembershipConflict),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreMembershipConflictResolutionRef {
    pub conflict_hash: ObjectHash,
    pub resolver_pubkey: String,
    pub resolution_hash: ObjectHash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreMembershipConflictResolution {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub conflict_hash: ObjectHash,
    pub conflicting_heads: Vec<MembershipHeadRef>,
    pub retired_owner_grants: BTreeSet<MembershipGrantId>,
    pub resolver_pubkey: String,
    pub resolver_branch_heads: Vec<MembershipHeadRef>,
    pub replacement_grant: MembershipGrantId,
    pub signature: String,
}

impl StoreMembershipConflictResolution {
    fn canonical_bytes(&self) -> Vec<u8> {
        #[derive(Serialize)]
        struct Signed<'a> {
            domain: &'static str,
            version: u32,
            store_root_hash: ObjectHash,
            conflict_hash: ObjectHash,
            conflicting_heads: &'a [MembershipHeadRef],
            retired_owner_grants: &'a BTreeSet<MembershipGrantId>,
            resolver_pubkey: &'a str,
            resolver_branch_heads: &'a [MembershipHeadRef],
            replacement_grant: &'a MembershipGrantId,
        }
        serde_json::to_vec(&Signed {
            domain: "coven.store-membership-conflict-resolution.v1",
            version: self.version,
            store_root_hash: self.store_root_hash,
            conflict_hash: self.conflict_hash,
            conflicting_heads: &self.conflicting_heads,
            retired_owner_grants: &self.retired_owner_grants,
            resolver_pubkey: &self.resolver_pubkey,
            resolver_branch_heads: &self.resolver_branch_heads,
            replacement_grant: &self.replacement_grant,
        })
        .expect("Store membership resolution serialization cannot fail")
    }

    pub fn resolution_hash(&self) -> ObjectHash {
        ObjectHash::digest(
            &serde_json::to_vec(self)
                .expect("Store membership resolution serialization cannot fail"),
        )
    }

    pub fn resolution_ref(&self) -> StoreMembershipConflictResolutionRef {
        StoreMembershipConflictResolutionRef {
            conflict_hash: self.conflict_hash,
            resolver_pubkey: self.resolver_pubkey.clone(),
            resolution_hash: self.resolution_hash(),
        }
    }

    pub fn verify_signature(&self) -> bool {
        self.version == STORE_PROTOCOL_VERSION
            && self.replacement_grant
                == derive_store_resolution_grant(&self.conflict_hash, &self.resolver_pubkey)
            && keys::verify_signature_hex(
                &self.resolver_pubkey,
                &self.signature,
                &self.canonical_bytes(),
            )
    }

    pub fn verify_against(
        &self,
        store_root_hash: ObjectHash,
        conflict: &MembershipConflict,
    ) -> bool {
        let MembershipConflict::RevocationCycle {
            conflict_hash,
            heads,
            involved_owner_grants,
            maximal_valid_branches,
            ..
        } = conflict
        else {
            return false;
        };
        let Some(branch) = maximal_valid_branches
            .iter()
            .find(|branch| branch.heads == self.resolver_branch_heads)
        else {
            return false;
        };
        let mut expected_retired = involved_owner_grants.clone();
        expected_retired.extend(branch.active_grants.iter().filter_map(|(grant, record)| {
            (record.member_pubkey == self.resolver_pubkey && record.role == MemberRole::Owner)
                .then_some(grant.clone())
        }));
        self.version == STORE_PROTOCOL_VERSION
            && self.store_root_hash == store_root_hash
            && self.conflict_hash == *conflict_hash
            && self.conflicting_heads == *heads
            && self.retired_owner_grants == expected_retired
            && self.replacement_grant
                == derive_store_resolution_grant(conflict_hash, &self.resolver_pubkey)
            && branch.active_grants.values().any(|record| {
                record.member_pubkey == self.resolver_pubkey && record.role == MemberRole::Owner
            })
            && self.verify_signature()
    }
}

pub fn derive_store_resolution_grant(
    conflict_hash: &ObjectHash,
    resolver_pubkey: &str,
) -> MembershipGrantId {
    MembershipGrantId(ObjectHash::digest(
        format!("coven.store-membership-resolution-grant.v1\0{conflict_hash}\0{resolver_pubkey}")
            .as_bytes(),
    ))
}

pub fn resolve_store_membership_conflict(
    store_root_hash: ObjectHash,
    conflict: &MembershipConflict,
    resolutions: &[StoreMembershipConflictResolution],
) -> Result<ResolvedStoreMembership, MembershipError> {
    let MembershipConflict::RevocationCycle {
        maximal_valid_branches,
        ..
    } = conflict
    else {
        return Err(MembershipError::InvalidConflictResolution);
    };
    if resolutions.is_empty() {
        return Err(MembershipError::InvalidConflictResolution);
    }
    let mut by_resolver = BTreeMap::new();
    let mut selected_branches = Vec::new();
    let mut retired_owner_grants = BTreeSet::new();
    for resolution in resolutions {
        if !resolution.verify_against(store_root_hash, conflict) {
            return Err(MembershipError::InvalidConflictResolution);
        }
        if let Some(existing) = by_resolver.insert(
            resolution.resolver_pubkey.clone(),
            resolution.resolution_hash(),
        ) {
            if existing != resolution.resolution_hash() {
                return Err(MembershipError::InvalidConflictResolution);
            }
            continue;
        }
        let branch = maximal_valid_branches
            .iter()
            .find(|branch| branch.heads == resolution.resolver_branch_heads)
            .ok_or(MembershipError::InvalidConflictResolution)?;
        if !selected_branches
            .iter()
            .any(|selected: &&StoreMembershipBranch| selected.heads == branch.heads)
        {
            selected_branches.push(branch);
        }
        retired_owner_grants.extend(resolution.retired_owner_grants.iter().cloned());
    }
    let (first_branch, other_branches) = selected_branches
        .split_first()
        .ok_or(MembershipError::InvalidConflictResolution)?;
    let mut active_grants = first_branch
        .active_grants
        .iter()
        .filter(|(grant, _)| !retired_owner_grants.contains(*grant))
        .map(|(grant, record)| (grant.clone(), record.clone()))
        .collect::<BTreeMap<_, _>>();
    active_grants.retain(|grant, record| {
        other_branches
            .iter()
            .all(|branch| branch.active_grants.get(grant) == Some(record))
    });
    for resolution in resolutions {
        let record = MembershipGrantRecord {
            member_pubkey: resolution.resolver_pubkey.clone(),
            role: MemberRole::Owner,
            provider_account_email: None,
            creation_authority: MembershipGrantCreationAuthority::ConflictResolution(
                resolution.resolution_ref(),
            ),
        };
        if active_grants
            .insert(resolution.replacement_grant.clone(), record.clone())
            .is_some_and(|current| current != record)
        {
            return Err(MembershipError::InvalidConflictResolution);
        }
    }
    let mut members = BTreeSet::new();
    if !active_grants
        .values()
        .any(|record| record.role == MemberRole::Owner)
        || active_grants
            .values()
            .any(|record| !members.insert(record.member_pubkey.clone()))
    {
        return Err(MembershipError::InvalidConflictResolution);
    }
    Ok(ResolvedStoreMembership {
        state_hash: store_membership_state_hash(&active_grants),
        active_grants,
    })
}

#[derive(Debug, Clone)]
struct GrantRecord {
    pubkey: String,
    role: MemberRole,
    provider_account_email: Option<String>,
    creation_authority: MembershipGrantCreationAuthority,
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
    status: Option<MembershipStatus>,
    head_refs: Vec<MembershipHeadRef>,
    resolution_checkpoint: Option<MembershipResolutionCheckpoint>,
}

#[derive(Debug, Clone)]
struct MembershipResolutionCheckpoint {
    raw_heads: Vec<MembershipCoord>,
    effective_frontier: Vec<MembershipCoord>,
    grants: BTreeMap<MembershipGrantId, MembershipGrantRecord>,
    removed: BTreeSet<MembershipGrantId>,
    included: BTreeSet<MembershipCoord>,
    resolutions: Vec<StoreMembershipConflictResolutionRef>,
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
        Self::from_entries_with_coords_and_head_refs(entries, Vec::new())
    }

    pub fn from_entries_with_coords_and_heads(
        entries: Vec<(MembershipCoord, MembershipEntry)>,
        heads: Vec<AuthorHead>,
    ) -> Result<Self, MembershipError> {
        let expected_store = entries
            .first()
            .map(|(_, entry)| entry.store_id.as_str())
            .ok_or(MembershipError::EmptyChain)?;
        if heads.iter().any(|head| {
            !head.verify()
                || head.store_id != expected_store
                || entries
                    .iter()
                    .find(|(coord, _)| *coord == head.entry_coord())
                    .is_none_or(|(_, entry)| head.resolutions != entry.resolution_dependencies)
        }) {
            return Err(MembershipError::MissingConflictHeads);
        }
        Self::from_entries_with_coords_and_head_refs(
            entries,
            heads.iter().map(MembershipHeadRef::from_head).collect(),
        )
    }

    fn from_entries_with_coords_and_head_refs(
        entries: Vec<(MembershipCoord, MembershipEntry)>,
        head_refs: Vec<MembershipHeadRef>,
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
            status: None,
            head_refs,
            resolution_checkpoint: None,
        };
        chain.rebuild()?;
        Ok(chain)
    }

    pub fn entries(&self) -> &[MembershipEntry] {
        &self.entries
    }

    pub fn status(&self) -> &MembershipStatus {
        self.status
            .as_ref()
            .expect("a loaded membership chain always has status")
    }

    pub fn head_refs(&self) -> &[MembershipHeadRef] {
        &self.head_refs
    }

    pub fn resolution_refs(&self) -> &[StoreMembershipConflictResolutionRef] {
        self.resolution_checkpoint
            .as_ref()
            .map_or(&[], |checkpoint| checkpoint.resolutions.as_slice())
    }

    pub(crate) fn resolution_checkpoint_covers(&self, coord: &MembershipCoord) -> bool {
        self.resolution_checkpoint
            .as_ref()
            .is_some_and(|checkpoint| {
                checkpoint.included.contains(coord) || checkpoint.raw_heads.contains(coord)
            })
    }

    pub(crate) fn replay_resolved_history_to_heads(
        &self,
        entries: Vec<(MembershipCoord, MembershipEntry)>,
        heads: Vec<AuthorHead>,
    ) -> Result<Self, MembershipError> {
        let resolution_checkpoint = self
            .resolution_checkpoint
            .clone()
            .ok_or(MembershipError::InvalidConflictResolution)?;
        let expected_store = entries
            .first()
            .map(|(_, entry)| entry.store_id.as_str())
            .ok_or(MembershipError::EmptyChain)?;
        if heads.iter().any(|head| {
            !head.verify()
                || head.store_id != expected_store
                || entries
                    .iter()
                    .find(|(coord, _)| *coord == head.entry_coord())
                    .is_none_or(|(_, entry)| head.resolutions != entry.resolution_dependencies)
        }) {
            return Err(MembershipError::MissingConflictHeads);
        }
        let (coords, entries): (Vec<_>, Vec<_>) = entries.into_iter().unzip();
        let mut chain = Self {
            entries,
            coords,
            state: CausalState::default(),
            included: BTreeSet::new(),
            status: None,
            head_refs: heads.iter().map(MembershipHeadRef::from_head).collect(),
            resolution_checkpoint: Some(resolution_checkpoint),
        };
        chain.rebuild()?;
        Ok(chain)
    }

    pub(crate) fn replay_merged_resolved_histories_to_heads(
        chains: &[&MembershipChain],
        entries: Vec<(MembershipCoord, MembershipEntry)>,
        heads: Vec<AuthorHead>,
    ) -> Result<Self, MembershipError> {
        let mut raw_by_stream = BTreeMap::new();
        let mut effective_by_stream = BTreeMap::new();
        let mut grants = BTreeMap::new();
        let mut removed = BTreeSet::new();
        let mut included = BTreeSet::new();
        let mut resolutions = BTreeSet::new();
        for chain in chains {
            let checkpoint = chain
                .resolution_checkpoint
                .as_ref()
                .ok_or(MembershipError::InvalidConflictResolution)?;
            if !causal_grants::merge_checkpoint_frontier(&mut raw_by_stream, &checkpoint.raw_heads)
                || !causal_grants::merge_checkpoint_frontier(
                    &mut effective_by_stream,
                    &checkpoint.effective_frontier,
                )
                || !causal_grants::merge_checkpoint_evidence(
                    &mut grants,
                    &mut removed,
                    &mut included,
                    &checkpoint.grants,
                    &checkpoint.removed,
                    &checkpoint.included,
                )
            {
                return Err(MembershipError::InvalidConflictResolution);
            }
            resolutions.extend(checkpoint.resolutions.iter().cloned());
        }
        let checkpoint = MembershipResolutionCheckpoint {
            raw_heads: raw_by_stream.into_values().collect(),
            effective_frontier: effective_by_stream.into_values().collect(),
            grants,
            removed,
            included,
            resolutions: resolutions.into_iter().collect(),
        };
        let base = chains
            .first()
            .ok_or(MembershipError::InvalidConflictResolution)?;
        let mut merged = (*base).clone();
        merged.resolution_checkpoint = Some(checkpoint);
        merged.replay_resolved_history_to_heads(entries, heads)
    }

    pub(crate) fn checkpoint_current_resolved_state(&mut self) -> Result<(), MembershipError> {
        self.ensure_resolved()?;
        let resolutions = self
            .resolution_checkpoint
            .as_ref()
            .map_or_else(Vec::new, |checkpoint| checkpoint.resolutions.clone());
        let grants = self
            .state
            .grants
            .iter()
            .map(|(grant, record)| {
                (
                    grant.clone(),
                    MembershipGrantRecord {
                        member_pubkey: record.pubkey.clone(),
                        role: record.role.clone(),
                        provider_account_email: record.provider_account_email.clone(),
                        creation_authority: record.creation_authority.clone(),
                    },
                )
            })
            .collect();
        self.resolution_checkpoint = Some(MembershipResolutionCheckpoint {
            raw_heads: self.author_heads(),
            effective_frontier: self.effective_frontier(),
            grants,
            removed: self.state.removed.clone(),
            included: self.included.clone(),
            resolutions,
        });
        Ok(())
    }

    pub fn conflict(&self) -> Option<&MembershipConflict> {
        match self.status() {
            MembershipStatus::Resolved(_) => None,
            MembershipStatus::Conflict(conflict) => Some(conflict),
        }
    }

    pub fn ensure_resolved(&self) -> Result<(), MembershipError> {
        match self.status() {
            MembershipStatus::Resolved(_) => Ok(()),
            MembershipStatus::Conflict(_) => Err(MembershipError::Conflict),
        }
    }

    pub fn resolved_with(
        &self,
        store_root_hash: ObjectHash,
        resolutions: &[StoreMembershipConflictResolution],
    ) -> Result<ResolvedStoreMembership, MembershipError> {
        match self.status() {
            MembershipStatus::Resolved(resolved) if resolutions.is_empty() => Ok(resolved.clone()),
            MembershipStatus::Conflict(conflict) => {
                resolve_store_membership_conflict(store_root_hash, conflict, resolutions)
            }
            MembershipStatus::Resolved(_) => Err(MembershipError::InvalidConflictResolution),
        }
    }

    pub fn signed_cycle_resolution(
        &self,
        store_root_hash: ObjectHash,
        resolver_branch_heads: Vec<MembershipHeadRef>,
        signer: &UserKeypair,
    ) -> Result<StoreMembershipConflictResolution, MembershipError> {
        let MembershipStatus::Conflict(MembershipConflict::RevocationCycle {
            conflict_hash,
            heads,
            involved_owner_grants,
            maximal_valid_branches,
            ..
        }) = self.status()
        else {
            return Err(MembershipError::Conflict);
        };
        let resolver_pubkey = keys::public_key_hex(signer);
        let branch = maximal_valid_branches
            .iter()
            .find(|branch| branch.heads == resolver_branch_heads)
            .ok_or(MembershipError::InvalidConflictResolution)?;
        if !branch.active_grants.values().any(|record| {
            record.member_pubkey == resolver_pubkey && record.role == MemberRole::Owner
        }) {
            return Err(MembershipError::SignerIsNotOwner(resolver_pubkey));
        }
        let replacement_grant = derive_store_resolution_grant(conflict_hash, &resolver_pubkey);
        let mut retired_owner_grants = involved_owner_grants.clone();
        retired_owner_grants.extend(branch.active_grants.iter().filter_map(|(grant, record)| {
            (record.member_pubkey == resolver_pubkey && record.role == MemberRole::Owner)
                .then_some(grant.clone())
        }));
        let mut resolution = StoreMembershipConflictResolution {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash,
            conflict_hash: *conflict_hash,
            conflicting_heads: heads.clone(),
            retired_owner_grants,
            resolver_pubkey,
            resolver_branch_heads,
            replacement_grant,
            signature: String::new(),
        };
        resolution.signature = keys::sign_hex(signer, &resolution.canonical_bytes()).1;
        Ok(resolution)
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
            MembershipChange::SetMember { .. }
            | MembershipChange::RemoveMember { .. }
            | MembershipChange::ResolutionActivation { .. } => None,
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
        if self.conflict().is_some() {
            return false;
        }
        self.active_grants_for(pubkey)
            .iter()
            .any(|(_, record)| record.role.can_write())
    }

    pub(crate) fn contains_member_now(&self, pubkey: &str) -> bool {
        if self.conflict().is_some() {
            return false;
        }
        !self.active_grants_for(pubkey).is_empty()
    }

    pub fn is_owner_now(&self, pubkey: &str) -> bool {
        if self.conflict().is_some() {
            return false;
        }
        self.active_grants_for(pubkey)
            .iter()
            .any(|(_, record)| record.role == MemberRole::Owner)
    }

    pub fn authorizes_write_at(&self, coord: &MembershipCoord, pubkey: &str) -> bool {
        self.active_grants_for(pubkey).iter().any(|(_, record)| {
            record.role.can_write()
                && record.creation_authority
                    == MembershipGrantCreationAuthority::Entry(coord.clone())
        })
    }

    pub fn authorizes_write_authority(
        &self,
        authority: &MembershipGrantCreationAuthority,
        pubkey: &str,
    ) -> bool {
        self.active_grants_for(pubkey)
            .iter()
            .any(|(_, record)| record.role.can_write() && &record.creation_authority == authority)
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
            .and_then(|(_, record)| match &record.creation_authority {
                MembershipGrantCreationAuthority::Entry(coord) => Some(coord.clone()),
                MembershipGrantCreationAuthority::ConflictResolution(_) => None,
            })
    }

    pub fn write_grant_authority(&self, pubkey: &str) -> Option<MembershipGrantCreationAuthority> {
        self.active_grants_for(pubkey)
            .into_iter()
            .find(|(_, record)| record.role.can_write())
            .map(|(_, record)| record.creation_authority.clone())
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
        Some(AuthorHead::signed_with_resolutions(
            self.store_id()?.to_string(),
            grant,
            tip.stream_id,
            tip.seq,
            tip.entry_hash,
            self.entries
                .iter()
                .find(|entry| entry.coord() == tip)
                .expect("effective membership tip has an entry")
                .resolution_dependencies
                .clone(),
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
        Some(AuthorHead::signed_with_resolutions(
            self.store_id()?.to_string(),
            grant,
            stream_id,
            tip.seq,
            tip.entry_hash,
            self.entries
                .iter()
                .find(|entry| entry.coord() == tip)
                .expect("effective membership tip has an entry")
                .resolution_dependencies
                .clone(),
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
            .ok_or(MembershipError::MissingAuthorStream)?;
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
            resolution_dependencies: self.resolution_refs().to_vec(),
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
            .ok_or(MembershipError::MissingAuthorStream)?;
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
            resolution_dependencies: self.resolution_refs().to_vec(),
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

    pub fn signed_resolution_activation_in_stream(
        &self,
        store_root_hash: ObjectHash,
        signer: &UserKeypair,
        stream_id: AuthorStreamId,
        resolution: &StoreMembershipConflictResolution,
        created_at: String,
    ) -> Result<MembershipEntry, MembershipError> {
        self.ensure_resolved()?;
        let MembershipStatus::Resolved(resolved_before) = self.status() else {
            unreachable!("ensure_resolved accepted a conflict")
        };
        let reference = resolution.resolution_ref();
        let author = keys::public_key_hex(signer);
        if !resolution.verify_signature()
            || resolution.store_root_hash != store_root_hash
            || reference.resolver_pubkey != author
            || !self.resolution_refs().contains(&reference)
            || self.active_owner_grant(&author) != Some(resolution.replacement_grant.clone())
        {
            return Err(MembershipError::InvalidConflictResolution);
        }
        let author_grant = resolution.replacement_grant.clone();
        if self
            .raw_stream_tip(&author, &author_grant, stream_id)
            .is_some()
        {
            return Err(MembershipError::ResolutionActivationRequiresFreshStream);
        }
        let mut entry = MembershipEntry {
            version: STORE_PROTOCOL_VERSION,
            store_id: self
                .store_id()
                .expect("validated chain has a store id")
                .to_string(),
            author_pubkey: author,
            author_owner_grant: author_grant,
            stream_id,
            seq: 1,
            previous_hash: None,
            dependencies: self.effective_frontier(),
            resolution_dependencies: self.resolution_refs().to_vec(),
            created_at,
            change: MembershipChange::ResolutionActivation {
                resolution: reference,
            },
            signature: String::new(),
        };
        sign_membership_entry(&mut entry, signer);
        let mut candidate = self.clone();
        candidate.add_entry(entry.clone())?;
        let MembershipStatus::Resolved(resolved_after) = candidate.status() else {
            return Err(MembershipError::InvalidConflictResolution);
        };
        if resolved_after.state_hash != resolved_before.state_hash {
            return Err(MembershipError::InvalidConflictResolution);
        }
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
                MembershipChange::ResolutionActivation { resolution } => {
                    if resolution.resolver_pubkey != entry.author_pubkey
                        || entry.seq != 1
                        || entry.previous_hash.is_some()
                        || entry
                            .dependencies
                            .iter()
                            .any(|dependency| dependency.stream_key() == entry.coord().stream_key())
                        || entry.author_owner_grant
                            != derive_store_resolution_grant(
                                &resolution.conflict_hash,
                                &resolution.resolver_pubkey,
                            )
                        || entry
                            .resolution_dependencies
                            .binary_search(resolution)
                            .is_err()
                        || self
                            .resolution_checkpoint
                            .as_ref()
                            .is_none_or(|checkpoint| {
                                let already_checkpointed =
                                    checkpoint.included.contains(&entry.coord())
                                        || checkpoint.raw_heads.contains(&entry.coord());
                                !already_checkpointed
                                    && (entry.dependencies != checkpoint.effective_frontier
                                        || entry.resolution_dependencies != checkpoint.resolutions)
                            })
                    {
                        return Err(MembershipError::InvalidResolutionActivation(index));
                    }
                    continue;
                }
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

        let reduced = match &self.resolution_checkpoint {
            Some(checkpoint) => reduce_store_membership_from_checkpoint(&self.entries, checkpoint)?,
            None => reduce_store_membership(&self.entries)?,
        };
        let checkpoint_grants = self
            .resolution_checkpoint
            .as_ref()
            .map(|checkpoint| &checkpoint.grants);
        let (state_source, status) = match reduced {
            CausalGrantStatus::Resolved(reduced) => {
                let resolved = resolved_store_membership(&reduced, checkpoint_grants);
                (Some(reduced), MembershipStatus::Resolved(resolved))
            }
            CausalGrantStatus::Conflict(CausalGrantConflict::ConcurrentMemberAssignments {
                raw_heads,
                effective_frontier,
                member_pubkey,
                conflicting_grants,
                uncontested_grants,
                reduced,
            }) => {
                let heads = self.exact_head_refs(&raw_heads)?;
                let conflict = MembershipConflict::ConcurrentMemberAssignments {
                    conflict_hash: membership_assignment_conflict_hash(
                        &heads,
                        &member_pubkey,
                        &conflicting_grants,
                    ),
                    heads,
                    effective_frontier,
                    member_pubkey,
                    conflicting_grants: map_store_grants(conflicting_grants, checkpoint_grants),
                    uncontested_grants: map_store_grants(uncontested_grants, checkpoint_grants),
                };
                (Some(reduced), MembershipStatus::Conflict(conflict))
            }
            CausalGrantStatus::Conflict(CausalGrantConflict::RevocationCycle {
                raw_heads,
                cyclic_sources,
                involved_owner_grants,
                maximal_valid_branches,
            }) => {
                let heads = self.exact_head_refs(&raw_heads)?;
                let branches = maximal_valid_branches
                    .into_iter()
                    .map(|branch| -> Result<StoreMembershipBranch, MembershipError> {
                        let resolved =
                            resolved_store_membership(&branch.reduced, checkpoint_grants);
                        Ok(StoreMembershipBranch {
                            heads: self.branch_head_refs(&branch.raw_heads)?,
                            effective_frontier: branch.effective_frontier,
                            active_grants: resolved.active_grants,
                            state_hash: resolved.state_hash,
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let conflict_hash = membership_revocation_conflict_hash(
                    &heads,
                    &cyclic_sources,
                    &involved_owner_grants,
                );
                (
                    None,
                    MembershipStatus::Conflict(MembershipConflict::RevocationCycle {
                        conflict_hash,
                        heads,
                        cyclic_sources,
                        involved_owner_grants,
                        maximal_valid_branches: branches,
                    }),
                )
            }
        };
        if let Some(reduced) = state_source {
            self.state = CausalState {
                grants: reduced
                    .grants
                    .into_iter()
                    .map(|(grant, record)| {
                        let creation_authority = membership_creation_authority(
                            &grant,
                            record.creation,
                            checkpoint_grants,
                        );
                        (
                            grant,
                            GrantRecord {
                                pubkey: record.member_pubkey,
                                role: record.assignment.role,
                                provider_account_email: record.assignment.provider_account_email,
                                creation_authority,
                            },
                        )
                    })
                    .collect(),
                removed: reduced.removed,
            };
            self.included = reduced.included;
        } else {
            self.state = CausalState::default();
            self.included.clear();
        }
        self.status = Some(status);
        Ok(())
    }

    pub fn apply_resolutions(
        &mut self,
        store_root_hash: ObjectHash,
        resolutions: &[StoreMembershipConflictResolution],
    ) -> Result<(), MembershipError> {
        let (raw_heads, effective_frontier) = match self.conflict() {
            Some(MembershipConflict::RevocationCycle {
                heads,
                maximal_valid_branches,
                ..
            }) => {
                let selected = resolutions
                    .iter()
                    .map(|resolution| {
                        maximal_valid_branches
                            .iter()
                            .find(|branch| branch.heads == resolution.resolver_branch_heads)
                            .map(|branch| branch.effective_frontier.as_slice())
                            .ok_or(MembershipError::InvalidConflictResolution)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                (
                    heads
                        .iter()
                        .map(|reference| reference.coord.clone())
                        .collect(),
                    causal_grants::common_frontier(&selected),
                )
            }
            _ => return Err(MembershipError::InvalidConflictResolution),
        };
        let resolved = self.resolved_with(store_root_hash, resolutions)?;
        let mut grants = self
            .resolution_checkpoint
            .as_ref()
            .map_or_else(BTreeMap::new, |checkpoint| checkpoint.grants.clone());
        for entry in &self.entries {
            let (grant, record) = match &entry.change {
                MembershipChange::Founder {
                    owner_pubkey,
                    owner_grant_id,
                } => (
                    owner_grant_id.clone(),
                    MembershipGrantRecord {
                        member_pubkey: owner_pubkey.clone(),
                        role: MemberRole::Owner,
                        provider_account_email: None,
                        creation_authority: MembershipGrantCreationAuthority::Entry(entry.coord()),
                    },
                ),
                MembershipChange::SetMember {
                    user_pubkey,
                    provider_account_email,
                    role,
                    grant_id,
                    ..
                } => (
                    grant_id.clone(),
                    MembershipGrantRecord {
                        member_pubkey: user_pubkey.clone(),
                        role: role.clone(),
                        provider_account_email: provider_account_email.clone(),
                        creation_authority: MembershipGrantCreationAuthority::Entry(entry.coord()),
                    },
                ),
                MembershipChange::RemoveMember { .. }
                | MembershipChange::ResolutionActivation { .. } => continue,
            };
            grants.insert(grant, record);
        }
        grants.extend(resolved.active_grants.clone());
        let removed: BTreeSet<_> = grants
            .keys()
            .filter(|grant| !resolved.active_grants.contains_key(*grant))
            .cloned()
            .collect();
        let included = membership_history_closure(&self.entries, &effective_frontier);
        let mut resolution_refs = self
            .resolution_checkpoint
            .as_ref()
            .map_or_else(Vec::new, |checkpoint| checkpoint.resolutions.clone());
        resolution_refs.extend(
            resolutions
                .iter()
                .map(StoreMembershipConflictResolution::resolution_ref),
        );
        resolution_refs.sort();
        resolution_refs.dedup();
        self.resolution_checkpoint = Some(MembershipResolutionCheckpoint {
            raw_heads,
            effective_frontier: effective_frontier.clone(),
            grants: grants.clone(),
            removed: removed.clone(),
            included: included.clone(),
            resolutions: resolution_refs,
        });
        self.state = CausalState {
            grants: grants
                .iter()
                .map(|(grant, record)| {
                    (
                        grant.clone(),
                        GrantRecord {
                            pubkey: record.member_pubkey.clone(),
                            role: record.role.clone(),
                            provider_account_email: record.provider_account_email.clone(),
                            creation_authority: record.creation_authority.clone(),
                        },
                    )
                })
                .collect(),
            removed,
        };
        self.included = included;
        self.status = Some(MembershipStatus::Resolved(resolved));
        Ok(())
    }

    fn exact_head_refs(
        &self,
        raw_heads: &[MembershipCoord],
    ) -> Result<Vec<MembershipHeadRef>, MembershipError> {
        let expected = raw_heads.iter().cloned().collect::<BTreeSet<_>>();
        let mut references = self
            .head_refs
            .iter()
            .filter(|reference| expected.contains(&reference.coord))
            .cloned()
            .collect::<Vec<_>>();
        let actual = references
            .iter()
            .map(|reference| reference.coord.clone())
            .collect::<BTreeSet<_>>();
        if expected != actual || references.len() != expected.len() {
            return Err(MembershipError::MissingConflictHeads);
        }
        references.sort();
        Ok(references)
    }

    fn branch_head_refs(
        &self,
        branch_heads: &[MembershipCoord],
    ) -> Result<Vec<MembershipHeadRef>, MembershipError> {
        let by_coord = self
            .head_refs
            .iter()
            .map(|reference| (reference.coord.clone(), reference.clone()))
            .collect::<BTreeMap<_, _>>();
        let mut references = branch_heads
            .iter()
            .map(|coord| {
                by_coord
                    .get(coord)
                    .cloned()
                    .ok_or(MembershipError::MissingConflictHeads)
            })
            .collect::<Result<Vec<_>, _>>()?;
        references.sort();
        Ok(references)
    }
}

fn reduce_store_membership(
    entries: &[MembershipEntry],
) -> Result<CausalGrantStatus<MembershipCoord, StoreAssignment>, MembershipError> {
    let normalized = normalize_store_membership(entries);
    causal_grants::reduce(&normalized).map_err(map_store_causal_error)
}

fn reduce_store_membership_from_checkpoint(
    entries: &[MembershipEntry],
    checkpoint: &MembershipResolutionCheckpoint,
) -> Result<CausalGrantStatus<MembershipCoord, StoreAssignment>, MembershipError> {
    let checkpoint_by_stream = checkpoint
        .raw_heads
        .iter()
        .map(|coord| (coord.stream_key(), coord))
        .collect::<BTreeMap<_, _>>();
    let suffix = entries
        .iter()
        .filter(|entry| {
            checkpoint_by_stream
                .get(&entry.coord().stream_key())
                .is_none_or(|head| entry.seq > head.seq)
        })
        .cloned()
        .collect::<Vec<_>>();
    let normalized = normalize_store_membership(&suffix);
    let seeds = checkpoint
        .grants
        .iter()
        .map(|(grant, record)| {
            (
                grant.clone(),
                causal_grants::CausalSeedGrant {
                    member_pubkey: record.member_pubkey.clone(),
                    assignment: StoreAssignment {
                        role: record.role.clone(),
                        provider_account_email: record.provider_account_email.clone(),
                    },
                },
            )
        })
        .collect();
    causal_grants::reduce_from_checkpoint(
        &normalized,
        &checkpoint.raw_heads,
        &checkpoint.effective_frontier,
        &seeds,
        &checkpoint.removed,
        &checkpoint.included,
    )
    .map_err(map_store_causal_error)
}

fn membership_history_closure(
    entries: &[MembershipEntry],
    frontier: &[MembershipCoord],
) -> BTreeSet<MembershipCoord> {
    let by_coord = entries
        .iter()
        .map(|entry| (entry.coord(), entry))
        .collect::<BTreeMap<_, _>>();
    let mut pending = frontier.iter().cloned().collect::<BTreeSet<_>>();
    let mut included = BTreeSet::new();
    while let Some(coord) = pending.pop_first() {
        if !included.insert(coord.clone()) {
            continue;
        }
        if let Some(entry) = by_coord.get(&coord) {
            pending.extend(entry.dependencies.iter().cloned());
        }
    }
    included
}

fn normalize_store_membership(
    entries: &[MembershipEntry],
) -> Vec<CausalEntry<MembershipCoord, StoreAssignment>> {
    entries
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
                MembershipChange::ResolutionActivation { .. } => CausalChange::ResolutionActivation,
            };
            CausalEntry {
                coord: entry.coord(),
                previous_hash: entry.previous_hash,
                dependencies,
                change,
            }
        })
        .collect()
}

fn map_store_grants(
    grants: BTreeMap<
        MembershipGrantId,
        causal_grants::GrantRecord<MembershipCoord, StoreAssignment>,
    >,
    checkpoint: Option<&BTreeMap<MembershipGrantId, MembershipGrantRecord>>,
) -> BTreeMap<MembershipGrantId, MembershipGrantRecord> {
    grants
        .into_iter()
        .map(|(grant, record)| {
            let creation_authority =
                membership_creation_authority(&grant, record.creation, checkpoint);
            (
                grant,
                MembershipGrantRecord {
                    member_pubkey: record.member_pubkey,
                    role: record.assignment.role,
                    provider_account_email: record.assignment.provider_account_email,
                    creation_authority,
                },
            )
        })
        .collect()
}

fn resolved_store_membership(
    reduced: &causal_grants::ReducedGrants<MembershipCoord, StoreAssignment>,
    checkpoint: Option<&BTreeMap<MembershipGrantId, MembershipGrantRecord>>,
) -> ResolvedStoreMembership {
    let active_grants = reduced
        .grants
        .iter()
        .filter(|(grant, _)| !reduced.removed.contains(*grant))
        .map(|(grant, record)| {
            (
                grant.clone(),
                MembershipGrantRecord {
                    member_pubkey: record.member_pubkey.clone(),
                    role: record.assignment.role.clone(),
                    provider_account_email: record.assignment.provider_account_email.clone(),
                    creation_authority: membership_creation_authority(
                        grant,
                        record.creation.clone(),
                        checkpoint,
                    ),
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    let state_hash = store_membership_state_hash(&active_grants);
    ResolvedStoreMembership {
        active_grants,
        state_hash,
    }
}

fn membership_creation_authority(
    grant: &MembershipGrantId,
    creation: causal_grants::CausalGrantCreation<MembershipCoord>,
    checkpoint: Option<&BTreeMap<MembershipGrantId, MembershipGrantRecord>>,
) -> MembershipGrantCreationAuthority {
    match creation {
        causal_grants::CausalGrantCreation::Entry(coord) => {
            MembershipGrantCreationAuthority::Entry(coord)
        }
        causal_grants::CausalGrantCreation::Checkpoint => checkpoint
            .and_then(|grants| grants.get(grant))
            .expect("checkpoint reducer seed has exact domain grant record")
            .creation_authority
            .clone(),
    }
}

fn store_membership_state_hash(
    active_grants: &BTreeMap<MembershipGrantId, MembershipGrantRecord>,
) -> ObjectHash {
    #[derive(Serialize)]
    struct State<'a> {
        domain: &'static str,
        active_grants: &'a BTreeMap<MembershipGrantId, MembershipGrantRecord>,
    }
    ObjectHash::digest(
        &serde_json::to_vec(&State {
            domain: "coven.store-membership-state.v1",
            active_grants,
        })
        .expect("Store membership state serialization cannot fail"),
    )
}

fn membership_assignment_conflict_hash(
    heads: &[MembershipHeadRef],
    member_pubkey: &str,
    conflicting_grants: &BTreeMap<
        MembershipGrantId,
        causal_grants::GrantRecord<MembershipCoord, StoreAssignment>,
    >,
) -> ObjectHash {
    #[derive(Serialize)]
    struct Conflict<'a> {
        domain: &'static str,
        heads: &'a [MembershipHeadRef],
        member_pubkey: &'a str,
        conflicting_grant_ids: Vec<&'a MembershipGrantId>,
    }
    ObjectHash::digest(
        &serde_json::to_vec(&Conflict {
            domain: "coven.store-membership-assignment-conflict.v1",
            heads,
            member_pubkey,
            conflicting_grant_ids: conflicting_grants.keys().collect(),
        })
        .expect("Store membership conflict serialization cannot fail"),
    )
}

fn membership_revocation_conflict_hash(
    heads: &[MembershipHeadRef],
    cyclic_sources: &[MembershipCoord],
    involved_owner_grants: &BTreeSet<MembershipGrantId>,
) -> ObjectHash {
    #[derive(Serialize)]
    struct Conflict<'a> {
        domain: &'static str,
        heads: &'a [MembershipHeadRef],
        cyclic_sources: &'a [MembershipCoord],
        involved_owner_grants: &'a BTreeSet<MembershipGrantId>,
    }
    ObjectHash::digest(
        &serde_json::to_vec(&Conflict {
            domain: "coven.store-membership-revocation-conflict.v1",
            heads,
            cyclic_sources,
            involved_owner_grants,
        })
        .expect("Store membership revocation conflict serialization cannot fail"),
    )
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
        CausalGrantError::NoActiveOwner => MembershipError::NoActiveOwner,
        CausalGrantError::RevocationCycleTooWide { sources, maximum } => {
            MembershipError::RevocationCycleTooWide { sources, maximum }
        }
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
        resolution_dependencies: Vec::new(),
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
        resolution_dependencies: &'a [StoreMembershipConflictResolutionRef],
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
        resolution_dependencies: &entry.resolution_dependencies,
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
    let activation_position_is_valid = match &entry.change {
        MembershipChange::ResolutionActivation { .. } => {
            entry.seq == 1
                && entry.previous_hash.is_none()
                && entry
                    .dependencies
                    .iter()
                    .all(|dependency| dependency.stream_key() != entry.coord().stream_key())
        }
        _ => true,
    };
    activation_position_is_valid
        && entry
            .resolution_dependencies
            .windows(2)
            .all(|pair| pair[0] < pair[1])
        && keys::verify_signature_hex(
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
        Self::signed_with_resolutions(
            store_id,
            author_owner_grant,
            stream_id,
            seq,
            tip_hash,
            Vec::new(),
            signer,
        )
    }

    pub fn signed_with_resolutions(
        store_id: String,
        author_owner_grant: MembershipGrantId,
        stream_id: AuthorStreamId,
        seq: u64,
        tip_hash: ObjectHash,
        mut resolutions: Vec<StoreMembershipConflictResolutionRef>,
        signer: &UserKeypair,
    ) -> Self {
        resolutions.sort();
        resolutions.dedup();
        let author_pubkey = keys::public_key_hex(signer);
        let mut head = Self {
            version: STORE_PROTOCOL_VERSION,
            store_id,
            author_pubkey,
            author_owner_grant,
            stream_id,
            seq,
            tip_hash,
            resolutions,
            signature: String::new(),
        };
        let (_, signature) = keys::sign_hex(signer, &head.canonical_bytes());
        head.signature = signature;
        head
    }

    pub fn verify(&self) -> bool {
        self.version == STORE_PROTOCOL_VERSION
            && self.resolutions.windows(2).all(|pair| pair[0] < pair[1])
            && keys::verify_signature_hex(
                &self.author_pubkey,
                &self.signature,
                &self.canonical_bytes(),
            )
    }

    pub fn entry_coord(&self) -> MembershipCoord {
        MembershipCoord {
            author_pubkey: self.author_pubkey.clone(),
            author_owner_grant: self.author_owner_grant.clone(),
            stream_id: self.stream_id,
            seq: self.seq,
            entry_hash: self.tip_hash,
        }
    }

    pub fn head_hash(&self) -> ObjectHash {
        ObjectHash::digest(
            &serde_json::to_vec(self).expect("membership head serialization cannot fail"),
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
            resolutions: &'a [StoreMembershipConflictResolutionRef],
        }
        serde_json::to_vec(&Signed {
            version: self.version,
            store_id: &self.store_id,
            author_pubkey: &self.author_pubkey,
            author_owner_grant: &self.author_owner_grant,
            stream_id: self.stream_id,
            seq: self.seq,
            tip_hash: self.tip_hash,
            resolutions: &self.resolutions,
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

    fn three_owner_store_cycle() -> (UserKeypair, UserKeypair, UserKeypair, MembershipChain) {
        let first = key();
        let second = key();
        let third = key();
        let first_pubkey = keys::public_key_hex(&first);
        let second_pubkey = keys::public_key_hex(&second);
        let third_pubkey = keys::public_key_hex(&third);
        let mut base = founded("three-owner-store", &first);
        let add_second = base
            .signed_set_member(
                &first,
                second_pubkey.clone(),
                None,
                MemberRole::Owner,
                "add second Owner".to_string(),
            )
            .expect("add second Owner");
        base.add_entry(add_second).expect("apply second Owner");
        let add_third = base
            .signed_set_member(
                &first,
                third_pubkey,
                None,
                MemberRole::Owner,
                "add third Owner".to_string(),
            )
            .expect("add third Owner");
        base.add_entry(add_third).expect("apply third Owner");
        let remove_second = base
            .signed_remove_member(&first, second_pubkey, "first branch".to_string())
            .expect("first branch");
        let remove_first = base
            .signed_remove_member_in_stream(
                &second,
                AuthorStreamId::from_bytes([92; 16]),
                first_pubkey,
                "second branch".to_string(),
            )
            .expect("second branch");
        let mut entries = base.entries().to_vec();
        entries.extend([remove_second.clone(), remove_first.clone()]);
        let heads = vec![
            AuthorHead::signed(
                remove_second.store_id.clone(),
                remove_second.author_owner_grant.clone(),
                remove_second.stream_id,
                remove_second.seq,
                entry_hash(&remove_second),
                &first,
            ),
            AuthorHead::signed(
                remove_first.store_id.clone(),
                remove_first.author_owner_grant.clone(),
                remove_first.stream_id,
                remove_first.seq,
                entry_hash(&remove_first),
                &second,
            ),
        ];
        let conflict = MembershipChain::from_entries_with_coords_and_heads(
            entries
                .into_iter()
                .map(|entry| (entry.coord(), entry))
                .collect(),
            heads,
        )
        .expect("three-Owner Store conflict");
        (first, second, third, conflict)
    }

    #[test]
    fn unaffected_store_owner_resolution_retires_its_selected_branch_grant() {
        let (_first, _second, third, conflicted) = three_owner_store_cycle();
        let third_pubkey = keys::public_key_hex(&third);
        let (branch, old_grant) = match conflicted.conflict().expect("conflict") {
            MembershipConflict::RevocationCycle {
                maximal_valid_branches,
                ..
            } => {
                let branch = maximal_valid_branches
                    .iter()
                    .find(|branch| {
                        branch.active_grants.values().any(|record| {
                            record.member_pubkey == third_pubkey && record.role == MemberRole::Owner
                        })
                    })
                    .expect("unaffected Owner branch");
                let old_grant = branch
                    .active_grants
                    .iter()
                    .find_map(|(grant, record)| {
                        (record.member_pubkey == third_pubkey).then_some(grant.clone())
                    })
                    .expect("unaffected Owner grant");
                (branch.heads.clone(), old_grant)
            }
            _ => panic!("expected revocation conflict"),
        };
        let store_root_hash = ObjectHash::digest(b"unaffected Store resolver root");
        let resolution = conflicted
            .signed_cycle_resolution(store_root_hash, branch, &third)
            .expect("unaffected Owner resolution");
        let resolved = conflicted
            .resolved_with(store_root_hash, std::slice::from_ref(&resolution))
            .expect("unaffected Owner resolution is valid");

        assert!(resolution.retired_owner_grants.contains(&old_grant));
        assert!(!resolved.active_grants.contains_key(&old_grant));
        assert!(resolved
            .active_grants
            .contains_key(&resolution.replacement_grant));
    }

    #[test]
    fn store_revocation_cycle_over_protocol_bound_is_typed() {
        let owners = (0..13).map(|_| key()).collect::<Vec<_>>();
        let pubkeys = owners.iter().map(keys::public_key_hex).collect::<Vec<_>>();
        let mut base = founded("bounded-store-cycle", &owners[0]);
        for pubkey in pubkeys.iter().skip(1) {
            let add = base
                .signed_set_member(
                    &owners[0],
                    pubkey.clone(),
                    None,
                    MemberRole::Owner,
                    format!("add {pubkey}"),
                )
                .expect("add ring Owner");
            base.add_entry(add).expect("apply ring Owner");
        }
        let removals = owners
            .iter()
            .enumerate()
            .map(|(index, owner)| {
                base.signed_remove_member_in_stream(
                    owner,
                    AuthorStreamId::from_bytes([index as u8 + 101; 16]),
                    pubkeys[(index + 1) % pubkeys.len()].clone(),
                    format!("remove ring successor {index}"),
                )
                .expect("sign ring removal")
            })
            .collect::<Vec<_>>();
        let mut entries = base.entries().to_vec();
        entries.extend(removals.iter().cloned());
        let heads = removals
            .iter()
            .zip(&owners)
            .map(|(entry, owner)| {
                AuthorHead::signed(
                    entry.store_id.clone(),
                    entry.author_owner_grant.clone(),
                    entry.stream_id,
                    entry.seq,
                    entry_hash(entry),
                    owner,
                )
            })
            .collect();

        assert!(matches!(
            MembershipChain::from_entries_with_coords_and_heads(
                entries
                    .into_iter()
                    .map(|entry| (entry.coord(), entry))
                    .collect(),
                heads,
            ),
            Err(MembershipError::RevocationCycleTooWide {
                sources: 13,
                maximum: 12,
            })
        ));
    }

    fn serial_authorization_with_follower(
        root: ObjectHash,
        owner: &UserKeypair,
        follower: &UserKeypair,
    ) -> (SerialAuthorizationState, StoreBatchCommit) {
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
        let add_commit = StoreBatchCommit::signed_with_control(
            root,
            crate::WriteId::from_generated("add-serial-follower".to_string()),
            "owner-device".to_string(),
            StoreCommitOrder::Serial {
                seq: 1,
                previous_commit_hash: None,
            },
            None,
            Some(StoreControl::SerialMembership { entry: add }),
            1,
            &[],
            owner,
        )
        .unwrap();
        let after_add = authorization.authorize_and_apply(&add_commit).unwrap();
        (after_add, add_commit)
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
        let (authorization, add_commit) =
            serial_authorization_with_follower(root, &owner, &follower);
        let active = StoreDeviceRegistration::signed(
            root,
            "follower-device".to_string(),
            1,
            None,
            StoreDeviceRegistrationState::Active,
            &follower,
        )
        .unwrap();
        let active_commit =
            registration_commit(root, 2, Some(add_commit.commit_hash()), &active, &follower);
        let after_active = authorization
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
            3,
            Some(active_commit.commit_hash()),
            &retired,
            &follower,
        );
        after_active
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
        let (authorization, add_commit) =
            serial_authorization_with_follower(root, &owner, &follower);
        let another_identity = StoreDeviceRegistration::signed(
            root,
            "follower-device".to_string(),
            1,
            None,
            StoreDeviceRegistrationState::Active,
            &outsider,
        )
        .unwrap();
        let another_identity_commit = registration_commit(
            root,
            2,
            Some(add_commit.commit_hash()),
            &another_identity,
            &follower,
        );
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
                seq: 2,
                previous_commit_hash: Some(add_commit.commit_hash()),
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
            Err(MembershipError::NoActiveOwner)
        ));
    }

    #[test]
    fn concurrent_member_assignments_are_validated_conflict_state() {
        let owner = key();
        let target = key();
        let chain = founded("store", &owner);
        let first = chain
            .signed_set_member_in_stream(
                &owner,
                AuthorStreamId::from_bytes([21; 16]),
                keys::public_key_hex(&target),
                None,
                MemberRole::Member,
                "first".to_string(),
            )
            .unwrap();
        let second = chain
            .signed_set_member_in_stream(
                &owner,
                AuthorStreamId::from_bytes([22; 16]),
                keys::public_key_hex(&target),
                None,
                MemberRole::Owner,
                "second".to_string(),
            )
            .unwrap();
        let mut entries = chain.entries().to_vec();
        entries.extend([first.clone(), second.clone()]);
        let heads = entries
            .iter()
            .filter(|entry| {
                !entries.iter().any(|candidate| {
                    candidate
                        .dependencies
                        .iter()
                        .any(|dependency| dependency == &entry.coord())
                        && candidate.stream_id == entry.stream_id
                })
            })
            .map(|entry| {
                AuthorHead::signed(
                    entry.store_id.clone(),
                    entry.author_owner_grant.clone(),
                    entry.stream_id,
                    entry.seq,
                    entry_hash(entry),
                    &owner,
                )
            })
            .collect();

        let conflicted = MembershipChain::from_entries_with_coords_and_heads(
            entries
                .into_iter()
                .map(|entry| (entry.coord(), entry))
                .collect(),
            heads,
        )
        .expect("well-formed conflict");
        assert!(matches!(
            conflicted.status(),
            MembershipStatus::Conflict(MembershipConflict::ConcurrentMemberAssignments {
                member_pubkey,
                conflicting_grants,
                ..
            }) if member_pubkey == &keys::public_key_hex(&target)
                && conflicting_grants.len() == 2
        ));
    }

    #[test]
    fn concurrent_cross_revocation_is_a_validated_cycle_conflict() {
        let first_owner = key();
        let second_owner = key();
        let first_pubkey = keys::public_key_hex(&first_owner);
        let second_pubkey = keys::public_key_hex(&second_owner);
        let mut base = founded("store", &first_owner);
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
            .signed_remove_member(
                &first_owner,
                second_pubkey.clone(),
                "remove second".to_string(),
            )
            .unwrap();
        let remove_first = base
            .signed_remove_member_in_stream(
                &second_owner,
                AuthorStreamId::from_bytes([23; 16]),
                first_pubkey.clone(),
                "remove first".to_string(),
            )
            .unwrap();
        let mut entries = base.entries().to_vec();
        entries.extend([remove_second.clone(), remove_first.clone()]);
        let heads = vec![
            AuthorHead::signed(
                remove_second.store_id.clone(),
                remove_second.author_owner_grant.clone(),
                remove_second.stream_id,
                remove_second.seq,
                entry_hash(&remove_second),
                &first_owner,
            ),
            AuthorHead::signed(
                remove_first.store_id.clone(),
                remove_first.author_owner_grant.clone(),
                remove_first.stream_id,
                remove_first.seq,
                entry_hash(&remove_first),
                &second_owner,
            ),
        ];

        let conflicted = MembershipChain::from_entries_with_coords_and_heads(
            entries
                .into_iter()
                .map(|entry| (entry.coord(), entry))
                .collect(),
            heads,
        )
        .expect("well-formed conflict");
        assert!(matches!(
            conflicted.status(),
            MembershipStatus::Conflict(MembershipConflict::RevocationCycle {
                cyclic_sources,
                involved_owner_grants,
                maximal_valid_branches,
                ..
            }) if cyclic_sources.len() == 2
                && involved_owner_grants.len() == 2
                && maximal_valid_branches.len() == 2
        ));

        let MembershipConflict::RevocationCycle {
            maximal_valid_branches,
            ..
        } = conflicted.conflict().expect("cycle conflict")
        else {
            unreachable!();
        };
        let resolver_branch_state = maximal_valid_branches
            .iter()
            .find(|branch| {
                branch.active_grants.values().any(|record| {
                    record.member_pubkey == first_pubkey && record.role == MemberRole::Owner
                })
            })
            .expect("first Owner branch")
            .clone();
        let resolver_branch = resolver_branch_state.heads.clone();
        let second_resolver_branch = maximal_valid_branches
            .iter()
            .find(|branch| {
                branch.active_grants.values().any(|record| {
                    record.member_pubkey == second_pubkey && record.role == MemberRole::Owner
                })
            })
            .expect("second Owner branch")
            .heads
            .clone();
        let store_root_hash = ObjectHash::digest(b"resolution Store root");
        let resolution = conflicted
            .signed_cycle_resolution(store_root_hash, resolver_branch.clone(), &first_owner)
            .expect("branch Owner resolves the conflict");
        let second_resolution = conflicted
            .signed_cycle_resolution(store_root_hash, second_resolver_branch, &second_owner)
            .expect("other branch Owner resolves the conflict");
        let retried = conflicted
            .signed_cycle_resolution(store_root_hash, resolver_branch, &first_owner)
            .expect("same resolver retry");
        assert_eq!(resolution, retried);
        assert!(resolution.verify_against(
            store_root_hash,
            conflicted.conflict().expect("cycle conflict"),
        ));
        let resolved_once = conflicted
            .resolved_with(store_root_hash, std::slice::from_ref(&resolution))
            .expect("one resolution applies");
        let resolved_duplicate = conflicted
            .resolved_with(store_root_hash, &[resolution.clone(), resolution.clone()])
            .expect("an exact retry is idempotent");
        assert_eq!(resolved_once, resolved_duplicate);
        assert!(resolved_once
            .active_grants
            .contains_key(&resolution.replacement_grant));
        assert!(resolution
            .retired_owner_grants
            .iter()
            .all(|grant| !resolved_once.active_grants.contains_key(grant)));

        let resolved_union = conflicted
            .resolved_with(
                store_root_hash,
                &[resolution.clone(), second_resolution.clone()],
            )
            .expect("distinct resolvers are unioned");
        assert!(resolved_union
            .active_grants
            .contains_key(&resolution.replacement_grant));
        assert!(resolved_union
            .active_grants
            .contains_key(&second_resolution.replacement_grant));

        let mut branch_specific = conflicted.conflict().expect("cycle conflict").clone();
        let MembershipConflict::RevocationCycle {
            maximal_valid_branches,
            ..
        } = &mut branch_specific
        else {
            unreachable!()
        };
        let branch_only_grant = MembershipGrantId(ObjectHash::digest(b"branch-only grant"));
        let branch_only_creation = maximal_valid_branches[0].effective_frontier[0].clone();
        maximal_valid_branches[0].active_grants.insert(
            branch_only_grant.clone(),
            MembershipGrantRecord {
                member_pubkey: keys::public_key_hex(&key()),
                role: MemberRole::Member,
                provider_account_email: None,
                creation_authority: MembershipGrantCreationAuthority::Entry(branch_only_creation),
            },
        );
        let composed = resolve_store_membership_conflict(
            store_root_hash,
            &branch_specific,
            &[resolution.clone(), second_resolution.clone()],
        )
        .expect("compose only grants agreed by every valid branch");
        assert!(!composed.active_grants.contains_key(&branch_only_grant));

        let mut duplicate_member = branch_specific;
        let MembershipConflict::RevocationCycle {
            maximal_valid_branches,
            ..
        } = &mut duplicate_member
        else {
            unreachable!()
        };
        let duplicate_pubkey = keys::public_key_hex(&key());
        let duplicate_creation = resolution.conflicting_heads[0].coord.clone();
        for branch in maximal_valid_branches {
            for suffix in [b'a', b'b'] {
                branch.active_grants.insert(
                    MembershipGrantId(ObjectHash::digest(&[suffix])),
                    MembershipGrantRecord {
                        member_pubkey: duplicate_pubkey.clone(),
                        role: MemberRole::Member,
                        provider_account_email: None,
                        creation_authority: MembershipGrantCreationAuthority::Entry(
                            duplicate_creation.clone(),
                        ),
                    },
                );
            }
        }
        assert!(matches!(
            resolve_store_membership_conflict(
                store_root_hash,
                &duplicate_member,
                &[resolution.clone(), second_resolution.clone()],
            ),
            Err(MembershipError::InvalidConflictResolution)
        ));

        let mut resumed = conflicted.clone();
        let raw_heads = resumed.author_heads();
        resumed
            .apply_resolutions(store_root_hash, std::slice::from_ref(&resolution))
            .expect("resolution activates replacement Owner grant");
        assert_eq!(resumed.author_heads(), raw_heads);
        assert_eq!(
            resumed.effective_frontier(),
            resolver_branch_state.effective_frontier
        );
        assert_eq!(resumed.resolution_refs(), &[resolution.resolution_ref()]);
        let after_resolution = resumed
            .signed_set_member_in_stream(
                &first_owner,
                AuthorStreamId::from_bytes([37; 16]),
                keys::public_key_hex(&key()),
                None,
                MemberRole::Member,
                "write after resolution".to_string(),
            )
            .expect("replacement Owner can author from a fresh stream");
        assert_eq!(
            after_resolution.author_owner_grant,
            resolution.replacement_grant
        );
        resumed
            .add_entry(after_resolution)
            .expect("future authoring validates from the resolved checkpoint");
        let activated_head = resumed
            .signed_head_for_stream(&first_owner, AuthorStreamId::from_bytes([37; 16]))
            .expect("sign post-resolution head");
        assert_eq!(
            activated_head.resolutions,
            vec![resolution.resolution_ref()]
        );
        let authority =
            MembershipGrantCreationAuthority::ConflictResolution(resolution.resolution_ref());
        assert!(resumed.authorizes_write_authority(&authority, &first_pubkey));
        let commit = StoreBatchCommit::signed(
            store_root_hash,
            crate::WriteId::from_generated("resolution-authorized-write".to_string()),
            "first-device".to_string(),
            super::super::store_commit::StoreCommitOrder::MergeConcurrent {
                seq: 1,
                previous_commit_hash: None,
                dependencies: BTreeMap::new(),
            },
            Some(authority),
            1,
            b"resolution-authorized package",
            &first_owner,
        )
        .expect("resolution-created Owner signs a Store commit");
        assert_eq!(
            commit.membership_authority,
            Some(MembershipGrantCreationAuthority::ConflictResolution(
                resolution.resolution_ref(),
            ))
        );
        assert!(matches!(
            conflicted.signed_cycle_resolution(
                store_root_hash,
                resolution.resolver_branch_heads.clone(),
                &key(),
            ),
            Err(MembershipError::SignerIsNotOwner(_))
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
            .signed_set_member_in_stream(
                &second_owner,
                AuthorStreamId::from_bytes([31; 16]),
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
            .signed_set_member_in_stream(
                &second,
                AuthorStreamId::from_bytes([32; 16]),
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
            .signed_remove_member_in_stream(
                &departing_owner,
                AuthorStreamId::from_bytes([33; 16]),
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
            .signed_set_member_in_stream(
                &second_owner,
                AuthorStreamId::from_bytes([34; 16]),
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
            .signed_set_member_in_stream(
                &second_owner,
                AuthorStreamId::from_bytes([35; 16]),
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
            .signed_set_member_in_stream(
                &second_owner,
                AuthorStreamId::from_bytes([35; 16]),
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
            .signed_set_member_in_stream(
                &second_owner,
                AuthorStreamId::from_bytes([35; 16]),
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
            .signed_set_member_in_stream(
                &second_owner,
                AuthorStreamId::from_bytes([36; 16]),
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

    #[test]
    fn serial_membership_hash_changes_when_an_assignment_is_recreated() {
        let owner = key();
        let member = key();
        let root = ObjectHash::digest(b"Serial grant-bearing membership root");
        let state = SerialMembershipState::from_founder(
            root,
            &founder_entry("serial-grant-store", &owner, "founder"),
        )
        .unwrap();
        let first = state
            .signed_set_member(
                &owner,
                keys::public_key_hex(&member),
                None,
                MemberRole::Member,
                "first assignment".to_string(),
            )
            .unwrap();
        let first_state = state.apply(&first).unwrap();
        let replacement = first_state
            .signed_set_member(
                &owner,
                keys::public_key_hex(&member),
                None,
                MemberRole::Member,
                "replacement assignment".to_string(),
            )
            .unwrap();
        let replacement_state = first_state.apply(&replacement).unwrap();

        assert_ne!(first_state.state_hash(), replacement_state.state_hash());
    }

    #[test]
    fn membership_head_resolution_cut_must_equal_its_tip_entry_cut() {
        let owner = UserKeypair::generate();
        let entry = founder_entry("head-tip-resolution-cut", &owner, "founder");
        let fake = StoreMembershipConflictResolutionRef {
            conflict_hash: ObjectHash::digest(b"head-tip conflict"),
            resolver_pubkey: keys::public_key_hex(&owner),
            resolution_hash: ObjectHash::digest(b"head-tip resolution"),
        };
        let head = AuthorHead::signed_with_resolutions(
            entry.store_id.clone(),
            entry.author_owner_grant.clone(),
            entry.stream_id,
            entry.seq,
            entry_hash(&entry),
            vec![fake],
            &owner,
        );

        assert!(matches!(
            MembershipChain::from_entries_with_coords_and_heads(
                vec![(entry.coord(), entry)],
                vec![head],
            ),
            Err(MembershipError::MissingConflictHeads)
        ));
    }

    #[test]
    fn membership_entry_rejects_unsorted_or_duplicate_resolution_dependencies() {
        let owner = UserKeypair::generate();
        let founder = founder_entry("entry-resolution-cut", &owner, "founder");
        let chain = MembershipChain::from_entries(vec![founder]).unwrap();
        let entry = chain
            .signed_set_member(
                &owner,
                keys::public_key_hex(&UserKeypair::generate()),
                None,
                MemberRole::Member,
                "member".to_string(),
            )
            .unwrap();
        let mut refs = [b"first".as_slice(), b"second".as_slice()]
            .into_iter()
            .map(|label| StoreMembershipConflictResolutionRef {
                conflict_hash: ObjectHash::digest(label),
                resolver_pubkey: keys::public_key_hex(&owner),
                resolution_hash: ObjectHash::digest(&[label, b" resolution"].concat()),
            })
            .collect::<Vec<_>>();
        refs.sort();

        let mut unsorted = entry.clone();
        unsorted.resolution_dependencies = refs.iter().rev().cloned().collect();
        sign_membership_entry(&mut unsorted, &owner);
        assert!(!verify_membership_entry(&unsorted));

        let mut duplicate = entry;
        duplicate.resolution_dependencies = vec![refs[0].clone(), refs[0].clone()];
        sign_membership_entry(&mut duplicate, &owner);
        assert!(!verify_membership_entry(&duplicate));
    }
}

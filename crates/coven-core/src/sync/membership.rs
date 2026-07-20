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
use super::storage::ExactObjectRef;
use super::store_commit::{
    GrantStreamAnchor, ObjectHash, StoreBatchCommit, StoreControl, StoreDeviceRegistration,
    StoreDeviceRegistrationOrigin, StoreDeviceRegistrationRef, StoreProtocolRoot, StoreRootRef,
    SuccessorLink, STORE_PROTOCOL_VERSION,
};
use super::wrapped_store_key::WrappedStoreKeyRef;
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
    pub provider_admin: super::provider::ProviderAdminState,
    pub key_generation: u64,
    pub(crate) active_wrapped_keys: BTreeSet<WrappedStoreKeyRef>,
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
        wrapped_key: WrappedStoreKeyRef,
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
    #[error("Serial device lifecycle commit also carries a Store or Circle package")]
    LifecycleWithPackage,
    #[error("Serial key rotation is not paired with a membership removal")]
    RotationWithoutRemoval,
    #[error("Serial key rotation names generation {actual}, expected {expected}")]
    KeyGeneration { expected: u64, actual: u64 },
    #[error("Serial commit reference does not authenticate the accepted commit")]
    InvalidCommitRef,
    #[error("Serial membership change carries invalid wrapped Store-key authority")]
    InvalidWrappedKey,
    #[error("Serial provider administrator history is invalid: {0}")]
    ProviderAdmin(#[from] super::provider::ProviderAdminReducerError),
}

impl SerialAuthorizationState {
    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn from_test_membership(
        founder: &MembershipEntry,
        membership: SerialMembershipState,
    ) -> Result<Self, MembershipError> {
        Ok(Self {
            membership,
            provider_admin: test_provider_admin_genesis(std::slice::from_ref(founder))?,
            key_generation: crate::encryption::INITIAL_KEY_GENERATION,
            active_wrapped_keys: BTreeSet::new(),
        })
    }

    pub fn membership_state_hash(&self) -> ObjectHash {
        ObjectHash::digest(
            &serde_json::to_vec(&(
                "coven.serial-authorization-membership-state.v1",
                self.membership.state_hash(),
                self.provider_admin.state_hash(),
            ))
            .expect("Serial authorization membership state serialization cannot fail"),
        )
    }

    pub fn from_founder(
        root: &StoreRootRef,
        root_value: &StoreProtocolRoot,
        founder_ref: &StoreDeviceRegistrationRef,
        founder: &StoreDeviceRegistration,
    ) -> Result<Self, SerialMembershipError> {
        if root_value.descriptor.write_policy != crate::WritePolicy::Serial
            || root_value.descriptor.store_root_id() != root.store_root_id
            || root_value.object_hash() != root.store_root_hash
            || founder_ref.object.slot() != &root_value.descriptor.founder_registration
            || founder_ref.verify_registration(founder).is_err()
            || &founder.store_root != root
            || founder.author_pubkey != root_value.descriptor.founder_pubkey
            || founder.provider != root_value.descriptor.founder_provider_admin.provider
            || !matches!(
                founder.origin,
                StoreDeviceRegistrationOrigin::Founder { creation_id }
                    if creation_id == root_value.descriptor.creation_id
            )
        {
            return Err(SerialMembershipError::InvalidFounder);
        }
        Ok(Self {
            membership: SerialMembershipState::from_genesis(
                root.store_root_hash,
                root_value.descriptor.founder_pubkey.clone(),
                root_value.descriptor.founder_grant.clone(),
            ),
            provider_admin: super::provider::ProviderAdminState::founder_from_root(
                root.clone(),
                founder_ref.clone(),
                &root_value.descriptor.founder_provider_admin,
            ),
            key_generation: crate::encryption::INITIAL_KEY_GENERATION,
            active_wrapped_keys: BTreeSet::new(),
        })
    }

    pub fn authorize_and_apply(
        &self,
        commit_ref: &super::store_commit::StoreBatchCommitRef,
        commit: &StoreBatchCommit,
        author: &StoreDeviceRegistration,
    ) -> Result<Self, SerialMembershipError> {
        commit_ref
            .verify_commit(commit)
            .map_err(|_| SerialMembershipError::InvalidCommitRef)?;
        if commit.membership_authority.is_some() {
            return Err(SerialMembershipError::CausalGrant);
        }
        if commit
            .author_registration
            .verify_registration(author)
            .is_err()
        {
            return Err(SerialMembershipError::AuthorIsNotWriter(
                author.author_pubkey.clone(),
            ));
        }
        let carries_lifecycle =
            !commit.device_registrations().is_empty() || !commit.device_retirements().is_empty();
        let carries_package =
            commit.store_package().is_some() || !commit.circle_packages().is_empty();
        if carries_lifecycle && carries_package {
            return Err(SerialMembershipError::LifecycleWithPackage);
        }
        let authorized = self.membership.can_write(&author.author_pubkey)
            || (self.membership.contains(&author.author_pubkey)
                && (is_exact_self_retirement_only(commit)
                    || is_exact_acknowledgement_only(commit)));
        if !authorized {
            return Err(SerialMembershipError::AuthorIsNotWriter(
                author.author_pubkey.clone(),
            ));
        }
        let membership = match commit.control() {
            Some(control) => match control.serial_membership_entry() {
                Some(entry) => self.membership.apply_at(entry, commit.seq())?,
                None => self.membership.advance_to(commit.seq())?,
            },
            None => self.membership.advance_to(commit.seq())?,
        };
        let Some(control) = commit.control() else {
            return Ok(Self {
                membership,
                provider_admin: self.provider_admin.clone(),
                key_generation: self.key_generation,
                active_wrapped_keys: self.active_wrapped_keys.clone(),
            });
        };
        let key_generation = match control {
            StoreControl::SerialMembership { .. } => self.key_generation,
            StoreControl::SerialMembershipAndKeyRotation {
                entry, generation, ..
            } => {
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
            StoreControl::ProviderAdmin { .. } => self.key_generation,
        };
        let mut provider_admin = self.provider_admin.clone();
        if let StoreControl::ProviderAdmin { change } = control {
            provider_admin.apply_membership_change(
                super::provider::ProviderAdminMembershipChange::Serial {
                    change: change.clone(),
                },
                super::provider::ProviderAdminGrantOrigin::SerialCommit {
                    commit: commit_ref.clone(),
                },
            )?;
        }
        let active_wrapped_keys = match control {
            StoreControl::SerialMembership { entry } => {
                let SerialMembershipChange::SetMember {
                    wrapped_key,
                    user_pubkey,
                    ..
                } = &entry.change
                else {
                    return Err(SerialMembershipError::InvalidWrappedKey);
                };
                if wrapped_key.owner_pubkey != entry.author_pubkey
                    || wrapped_key.recipient_pubkey != *user_pubkey
                    || wrapped_key.generation != self.key_generation
                    || wrapped_key.validate_identity().is_err()
                {
                    return Err(SerialMembershipError::InvalidWrappedKey);
                }
                let mut keys = self.active_wrapped_keys.clone();
                keys.retain(|reference| reference.recipient_pubkey != *user_pubkey);
                keys.insert(wrapped_key.clone());
                keys
            }
            StoreControl::SerialMembershipAndKeyRotation {
                entry,
                generation,
                wrapped_keys,
            } => {
                let SerialMembershipChange::RemoveMember { user_pubkey, .. } = &entry.change else {
                    return Err(SerialMembershipError::RotationWithoutRemoval);
                };
                let expected_recipients = membership
                    .current_members()
                    .into_iter()
                    .map(|(pubkey, _)| pubkey)
                    .collect::<BTreeSet<_>>();
                let actual_recipients = wrapped_keys
                    .iter()
                    .map(|reference| reference.recipient_pubkey.clone())
                    .collect::<BTreeSet<_>>();
                if wrapped_keys.is_empty()
                    || !wrapped_keys.windows(2).all(|pair| pair[0] < pair[1])
                    || expected_recipients != actual_recipients
                    || actual_recipients.len() != wrapped_keys.len()
                    || wrapped_keys.iter().any(|reference| {
                        reference.owner_pubkey != entry.author_pubkey
                            || reference.recipient_pubkey == *user_pubkey
                            || reference.generation != *generation
                            || reference.validate_identity().is_err()
                    })
                {
                    return Err(SerialMembershipError::InvalidWrappedKey);
                }
                wrapped_keys.iter().cloned().collect()
            }
            StoreControl::ProviderAdmin { .. } => self.active_wrapped_keys.clone(),
        };
        Ok(Self {
            membership,
            provider_admin,
            key_generation,
            active_wrapped_keys,
        })
    }

    pub fn active_wrapped_keys_for(&self, recipient_pubkey: &str) -> Vec<WrappedStoreKeyRef> {
        self.active_wrapped_keys
            .iter()
            .filter(|reference| reference.recipient_pubkey == recipient_pubkey)
            .cloned()
            .collect()
    }
}

fn is_exact_self_retirement_only(commit: &StoreBatchCommit) -> bool {
    let [retirement] = commit.device_retirements() else {
        return false;
    };
    retirement.target == commit.author_registration
        && commit.control().is_none()
        && commit.device_registrations().is_empty()
        && commit.circle_controls().is_empty()
        && commit.store_package().is_none()
        && commit.circle_packages().is_empty()
}

fn is_exact_acknowledgement_only(commit: &StoreBatchCommit) -> bool {
    commit
        .operations()
        .is_some_and(|operations| operations.is_acknowledgement_only())
}

impl SerialMembershipState {
    fn from_genesis(
        store_root_hash: ObjectHash,
        founder_pubkey: String,
        founder_grant: MembershipGrantId,
    ) -> Self {
        Self {
            store_root_hash,
            active_grants: BTreeMap::from([(
                founder_grant,
                SerialMember {
                    member_pubkey: founder_pubkey,
                    role: MemberRole::Owner,
                    provider_account_email: None,
                    created_at_generation: 0,
                },
            )]),
            current_generation: 0,
        }
    }

    pub fn from_founder(
        store_root_hash: ObjectHash,
        founder: &MembershipEntry,
    ) -> Result<Self, SerialMembershipError> {
        let MembershipChange::Founder {
            owner_pubkey,
            owner_grant_id,
            ..
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

    pub fn active_owner_grant(&self, pubkey: &str) -> Option<MembershipGrantId> {
        self.active_grants.iter().find_map(|(grant_id, member)| {
            (member.member_pubkey == pubkey && member.role == MemberRole::Owner)
                .then(|| grant_id.clone())
        })
    }

    pub(crate) fn authorizes_owner_grant_id(
        &self,
        pubkey: &str,
        grant_id: &MembershipGrantId,
    ) -> bool {
        self.active_grants.get(grant_id).is_some_and(|member| {
            member.member_pubkey == pubkey && member.role == MemberRole::Owner
        })
    }

    pub fn signed_set_member_with_wrapped_key(
        &self,
        signer: &UserKeypair,
        user_pubkey: String,
        provider_account_email: Option<String>,
        role: MemberRole,
        wrapped_key: WrappedStoreKeyRef,
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
                wrapped_key,
            },
            created_at_generation,
            created_at,
        )
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn signed_set_member(
        &self,
        signer: &UserKeypair,
        user_pubkey: String,
        provider_account_email: Option<String>,
        role: MemberRole,
        created_at: String,
    ) -> Result<SerialMembershipEntry, SerialMembershipError> {
        let wrapped_key = test_wrapped_key_ref(
            &keys::public_key_hex(signer),
            &user_pubkey,
            crate::encryption::INITIAL_KEY_GENERATION,
            b"Serial membership test wrap",
        );
        self.signed_set_member_with_wrapped_key(
            signer,
            user_pubkey,
            provider_account_email,
            role,
            wrapped_key,
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
                wrapped_key,
            } => {
                if wrapped_key.owner_pubkey != entry.author_pubkey
                    || wrapped_key.recipient_pubkey != *user_pubkey
                    || wrapped_key.validate_identity().is_err()
                {
                    return Err(SerialMembershipError::InvalidWrappedKey);
                }
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

#[cfg(any(test, feature = "test-utils"))]
pub(crate) fn test_wrapped_key_ref(
    owner_pubkey: &str,
    recipient_pubkey: &str,
    generation: u64,
    label: &[u8],
) -> WrappedStoreKeyRef {
    let wrap_hash = ObjectHash::digest(
        &[
            label,
            owner_pubkey.as_bytes(),
            recipient_pubkey.as_bytes(),
            &generation.to_le_bytes(),
        ]
        .concat(),
    );
    let logical_key =
        format!("keys/{owner_pubkey}/{recipient_pubkey}/{generation}/{wrap_hash}.json");
    WrappedStoreKeyRef {
        owner_pubkey: owner_pubkey.to_string(),
        recipient_pubkey: recipient_pubkey.to_string(),
        generation,
        wrap_hash,
        object: ExactObjectRef::new(
            crate::storage::cloud::ObjectSlot::logical(logical_key)
                .expect("test wrapped-key slot is valid"),
            label.len() as u64,
            ObjectHash::digest(label),
        ),
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
        membership: GrantStreamAnchor,
        provider_admin: super::provider::FounderProviderAdminGrant,
    },
    SetMember {
        user_pubkey: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        provider_account_email: Option<String>,
        role: MemberRole,
        grant_id: MembershipGrantId,
        membership: Option<GrantStreamAnchor>,
        replaces: BTreeSet<MembershipGrantId>,
        owner_barriers: BTreeMap<MembershipGrantId, OwnerStreamBarrier>,
        wrapped_key: WrappedStoreKeyRef,
    },
    RemoveMember {
        user_pubkey: String,
        removes: BTreeSet<MembershipGrantId>,
        owner_barriers: BTreeMap<MembershipGrantId, OwnerStreamBarrier>,
        wrapped_keys: Vec<WrappedStoreKeyRef>,
    },
    ProviderAdmin,
    ResolutionActivation {
        resolution: StoreMembershipConflictResolutionRef,
    },
}

impl MembershipChange {
    pub(crate) fn membership_anchor(&self) -> Option<GrantStreamAnchor> {
        match self {
            Self::Founder { membership, .. } => Some(membership.clone()),
            Self::SetMember { membership, .. } => membership.clone(),
            Self::RemoveMember { .. } | Self::ProviderAdmin | Self::ResolutionActivation { .. } => {
                None
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
    pub previous_hash: Option<ObjectHash>,
    pub dependencies: Vec<MembershipCoord>,
    pub resolution_dependencies: Vec<StoreMembershipConflictResolutionRef>,
    pub created_at: String,
    pub change: MembershipChange,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_admin: Option<super::provider::ProviderAdminMembershipChange>,
    pub signature: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MembershipEntryRef {
    pub coord: MembershipCoord,
    pub object: ExactObjectRef,
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
            | MembershipChange::ProviderAdmin
            | MembershipChange::ResolutionActivation { .. } => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorHead {
    pub version: u32,
    pub store_id: String,
    pub author_registration: StoreDeviceRegistrationRef,
    pub entry: MembershipEntryRef,
    pub predecessor: Option<MembershipHeadRef>,
    pub resolutions: Vec<StoreMembershipConflictResolutionRef>,
    pub successor: SuccessorLink,
    pub signature: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MembershipHeadRef {
    pub coord: MembershipCoord,
    pub head_hash: ObjectHash,
    pub object: ExactObjectRef,
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
        dependency: Box<MembershipCoord>,
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
    #[error("membership entry {0} carries an invalid Owner membership stream anchor")]
    InvalidOwnerMembershipAnchor(usize),
    #[error("membership entry {0} carries invalid wrapped Store-key authority")]
    InvalidWrappedKeys(usize),
    #[error(
        "current member {recipient_pubkey} lacks wrapped Store-key coverage for rotation {rotation:?}"
    )]
    MissingWrappedKeyCoverage {
        recipient_pubkey: String,
        rotation: Box<MembershipCoord>,
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
    #[error("provider administrator control entry {0} is invalid")]
    InvalidProviderAdminChange(usize),
    #[error("membership has an unresolved semantic conflict")]
    Conflict,
    #[error("membership conflict is missing its exact signed raw heads")]
    MissingConflictHeads,
    #[error("membership conflict resolution does not name exact validated conflict evidence")]
    InvalidConflictResolution,
    #[error("provider administrator history is invalid: {0}")]
    ProviderAdmin(#[from] super::provider::ProviderAdminReducerError),
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
    pub provider_admin: super::provider::ProviderAdminResolution,
    pub state_hash: ObjectHash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreMembershipBranch {
    pub heads: Vec<MembershipHeadRef>,
    pub effective_frontier: Vec<MembershipCoord>,
    pub active_grants: BTreeMap<MembershipGrantId, MembershipGrantRecord>,
    pub provider_admin: super::provider::ProviderAdminResolution,
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
    pub object: ExactObjectRef,
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
    pub replacement_membership: GrantStreamAnchor,
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
            replacement_membership: &'a GrantStreamAnchor,
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
            replacement_membership: &self.replacement_membership,
        })
        .expect("Store membership resolution serialization cannot fail")
    }

    pub fn resolution_hash(&self) -> ObjectHash {
        ObjectHash::digest(
            &serde_json::to_vec(self)
                .expect("Store membership resolution serialization cannot fail"),
        )
    }

    pub fn resolution_ref(&self, object: ExactObjectRef) -> StoreMembershipConflictResolutionRef {
        StoreMembershipConflictResolutionRef {
            conflict_hash: self.conflict_hash,
            resolver_pubkey: self.resolver_pubkey.clone(),
            resolution_hash: self.resolution_hash(),
            object,
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
    resolutions: &[(
        StoreMembershipConflictResolutionRef,
        StoreMembershipConflictResolution,
    )],
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
    for (_, resolution) in resolutions {
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
    for (reference, resolution) in resolutions {
        let record = MembershipGrantRecord {
            member_pubkey: resolution.resolver_pubkey.clone(),
            role: MemberRole::Owner,
            provider_account_email: None,
            creation_authority: MembershipGrantCreationAuthority::ConflictResolution(
                reference.clone(),
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
    let provider_admin = super::provider::ProviderAdminResolution::Resolved(
        super::provider::ProviderAdminState::merge(
            selected_branches
                .iter()
                .map(|branch| branch.provider_admin.combined_state().clone()),
        )?,
    );
    Ok(ResolvedStoreMembership {
        state_hash: store_membership_state_hash(&active_grants, &provider_admin),
        active_grants,
        provider_admin,
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

#[derive(Debug, Clone)]
pub struct MembershipChain {
    entries: Vec<MembershipEntry>,
    coords: Vec<MembershipCoord>,
    state: CausalState,
    included: BTreeSet<MembershipCoord>,
    status: Option<MembershipStatus>,
    head_refs: Vec<MembershipHeadRef>,
    resolution_checkpoint: Option<MembershipResolutionCheckpoint>,
    provider_admin_genesis: super::provider::ProviderAdminState,
}

#[derive(Debug, Clone)]
struct MembershipResolutionCheckpoint {
    raw_heads: Vec<MembershipCoord>,
    effective_frontier: Vec<MembershipCoord>,
    grants: BTreeMap<MembershipGrantId, MembershipGrantRecord>,
    grant_anchors: BTreeMap<MembershipGrantId, GrantStreamAnchor>,
    removed: BTreeSet<MembershipGrantId>,
    included: BTreeSet<MembershipCoord>,
    resolutions: Vec<StoreMembershipConflictResolutionRef>,
    provider_admin: super::provider::ProviderAdminState,
}

#[cfg(any(test, feature = "test-utils"))]
fn test_provider_admin_genesis(
    entries: &[MembershipEntry],
) -> Result<super::provider::ProviderAdminState, MembershipError> {
    let founder = entries
        .iter()
        .find_map(|entry| match &entry.change {
            MembershipChange::Founder { provider_admin, .. } => Some((entry, provider_admin)),
            _ => None,
        })
        .ok_or(MembershipError::InvalidFounder)?;
    let root_bytes = founder.0.store_id.as_bytes();
    let root = StoreRootRef {
        store_root_id: ObjectHash::digest(
            format!("{} test root id", founder.0.store_id).as_bytes(),
        ),
        store_root_hash: ObjectHash::digest(root_bytes),
        object: ExactObjectRef::new(
            crate::storage::cloud::ObjectSlot::logical(format!(
                "store-v1/test/{}/root.json",
                founder.0.store_id
            ))
            .expect("valid test root slot"),
            root_bytes.len() as u64,
            ObjectHash::digest(root_bytes),
        ),
    };
    let registration: StoreDeviceRegistrationRef =
        serde_json::from_value(serde_json::json!({
            "device_id": ObjectHash::digest(format!("{} founder device", founder.0.store_id).as_bytes()),
            "registration_hash": ObjectHash::digest(format!("{} founder registration", founder.0.store_id).as_bytes()),
            "object": {
                "slot": {"logical_key": format!("store-v1/test/{}/registration.json", founder.0.store_id), "physical": {"kind": "logical_key"}},
                "stored_size": 1,
                "stored_hash": ObjectHash::digest(format!("{} founder registration object", founder.0.store_id).as_bytes()),
            }
        }))
        .expect("valid test founder registration reference");
    Ok(super::provider::ProviderAdminState::founder_from_root(
        root,
        registration,
        founder.1,
    ))
}

impl MembershipChain {
    #[cfg(any(test, feature = "test-utils"))]
    pub fn from_entries(entries: Vec<MembershipEntry>) -> Result<Self, MembershipError> {
        let provider_admin = test_provider_admin_genesis(&entries)?;
        Self::from_entries_with_coords_and_provider_admin(
            entries
                .into_iter()
                .map(|entry| (entry.coord(), entry))
                .collect(),
            provider_admin,
        )
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn from_entries_with_coords(
        entries: Vec<(MembershipCoord, MembershipEntry)>,
    ) -> Result<Self, MembershipError> {
        let values = entries
            .iter()
            .map(|(_, entry)| entry.clone())
            .collect::<Vec<_>>();
        let provider_admin = test_provider_admin_genesis(&values)?;
        Self::from_entries_with_coords_and_provider_admin(entries, provider_admin)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn from_entries_with_coords_and_heads(
        entries: Vec<(MembershipCoord, MembershipEntry)>,
        heads: Vec<(MembershipHeadRef, AuthorHead)>,
    ) -> Result<Self, MembershipError> {
        let values = entries
            .iter()
            .map(|(_, entry)| entry.clone())
            .collect::<Vec<_>>();
        let provider_admin = test_provider_admin_genesis(&values)?;
        Self::from_entries_with_coords_and_heads_and_provider_admin(entries, heads, provider_admin)
    }

    pub fn from_entries_with_coords_and_provider_admin(
        entries: Vec<(MembershipCoord, MembershipEntry)>,
        provider_admin: super::provider::ProviderAdminState,
    ) -> Result<Self, MembershipError> {
        Self::from_entries_with_coords_and_head_refs(entries, Vec::new(), provider_admin)
    }

    pub fn from_entries_with_coords_and_heads_and_provider_admin(
        entries: Vec<(MembershipCoord, MembershipEntry)>,
        heads: Vec<(MembershipHeadRef, AuthorHead)>,
        provider_admin: super::provider::ProviderAdminState,
    ) -> Result<Self, MembershipError> {
        let expected_store = entries
            .first()
            .map(|(_, entry)| entry.store_id.as_str())
            .ok_or(MembershipError::EmptyChain)?;
        if heads.iter().any(|(reference, head)| {
            reference.head_hash != head.head_hash()
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
            heads.into_iter().map(|(reference, _)| reference).collect(),
            provider_admin,
        )
    }

    fn from_entries_with_coords_and_head_refs(
        entries: Vec<(MembershipCoord, MembershipEntry)>,
        head_refs: Vec<MembershipHeadRef>,
        provider_admin_genesis: super::provider::ProviderAdminState,
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
            provider_admin_genesis,
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

    pub(crate) fn head_ref_for_stream(
        &self,
        author: &str,
        grant: &MembershipGrantId,
        stream_id: AuthorStreamId,
    ) -> Option<&MembershipHeadRef> {
        self.head_refs.iter().find(|reference| {
            reference.coord.author_pubkey == author
                && reference.coord.author_owner_grant == *grant
                && reference.coord.stream_id == stream_id
        })
    }

    pub(crate) fn membership_anchor(
        &self,
        grant: &MembershipGrantId,
    ) -> Option<&GrantStreamAnchor> {
        self.entries
            .iter()
            .find_map(|entry| match &entry.change {
                MembershipChange::Founder {
                    owner_grant_id,
                    membership,
                    ..
                } if owner_grant_id == grant => Some(membership),
                MembershipChange::SetMember {
                    grant_id,
                    membership: Some(membership),
                    ..
                } if grant_id == grant => Some(membership),
                _ => None,
            })
            .or_else(|| {
                self.resolution_checkpoint
                    .as_ref()?
                    .grant_anchors
                    .get(grant)
            })
    }

    pub(crate) fn membership_stream_id(&self, grant: &MembershipGrantId) -> Option<AuthorStreamId> {
        let record = self.state.grants.get(grant)?;
        store_membership_anchor_stream(&record.pubkey, grant, self.membership_anchor(grant)?)
    }

    pub(crate) fn activated_membership_streams(
        &self,
    ) -> Vec<(MembershipStreamKey, GrantStreamAnchor)> {
        let mut streams = self
            .state
            .grants
            .iter()
            .filter_map(|(grant, record)| {
                let anchor = self.membership_anchor(grant)?.clone();
                let stream_id = self.membership_stream_id(grant)?;
                Some((
                    MembershipStreamKey {
                        author_pubkey: record.pubkey.clone(),
                        author_owner_grant: grant.clone(),
                        stream_id,
                    },
                    anchor,
                ))
            })
            .collect::<BTreeMap<_, _>>();
        let mut included = self.included.clone();
        if let MembershipStatus::Conflict(MembershipConflict::RevocationCycle {
            maximal_valid_branches,
            ..
        }) = self.status()
        {
            for branch in maximal_valid_branches {
                included.extend(membership_history_closure(
                    &self.entries,
                    &branch.effective_frontier,
                ));
            }
        }
        for (coord, entry) in self.entries_with_coords() {
            if !included.contains(coord) {
                continue;
            }
            let (owner_pubkey, grant, anchor) = match &entry.change {
                MembershipChange::SetMember {
                    user_pubkey,
                    role: MemberRole::Owner,
                    grant_id,
                    membership: Some(membership),
                    ..
                } => (user_pubkey, grant_id, membership),
                _ => continue,
            };
            let stream_id = store_membership_anchor_stream(owner_pubkey, grant, anchor)
                .expect("validated Owner grant has a Store membership stream anchor");
            streams.insert(
                MembershipStreamKey {
                    author_pubkey: owner_pubkey.clone(),
                    author_owner_grant: grant.clone(),
                    stream_id,
                },
                anchor.clone(),
            );
        }
        streams.into_iter().collect()
    }

    pub(crate) fn activate_head_ref(
        &mut self,
        reference: MembershipHeadRef,
    ) -> Result<(), MembershipError> {
        if !self.coords.contains(&reference.coord) {
            return Err(MembershipError::MissingConflictHeads);
        }
        let stream = reference.coord.stream_key();
        self.head_refs
            .retain(|current| current.coord.stream_key() != stream);
        self.head_refs.push(reference);
        self.head_refs.sort();
        self.rebuild()
    }

    pub fn resolution_refs(&self) -> &[StoreMembershipConflictResolutionRef] {
        self.resolution_checkpoint
            .as_ref()
            .map_or(&[], |checkpoint| checkpoint.resolutions.as_slice())
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
        resolutions: &[(
            StoreMembershipConflictResolutionRef,
            StoreMembershipConflictResolution,
        )],
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
        replacement_membership: GrantStreamAnchor,
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
            replacement_membership,
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

    pub fn founder_entry(&self) -> Option<&MembershipEntry> {
        self.entries
            .iter()
            .find(|entry| matches!(entry.change, MembershipChange::Founder { .. }))
    }

    pub fn founder_pubkey(&self) -> Option<&str> {
        self.founder_entry().and_then(|entry| match &entry.change {
            MembershipChange::Founder { owner_pubkey, .. } => Some(owner_pubkey.as_str()),
            MembershipChange::SetMember { .. }
            | MembershipChange::RemoveMember { .. }
            | MembershipChange::ProviderAdmin
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

    pub fn active_wrapped_keys_for(&self, recipient_pubkey: &str) -> Vec<WrappedStoreKeyRef> {
        let active_grants = self.active_grant_ids(recipient_pubkey);
        self.entries_with_coords()
            .filter(|(coord, _)| self.included.contains(*coord))
            .flat_map(|(_, entry)| match &entry.change {
                MembershipChange::SetMember {
                    grant_id,
                    wrapped_key,
                    ..
                } if active_grants.contains(grant_id) => std::slice::from_ref(wrapped_key),
                MembershipChange::RemoveMember { wrapped_keys, .. } => wrapped_keys.as_slice(),
                MembershipChange::Founder { .. }
                | MembershipChange::SetMember { .. }
                | MembershipChange::ProviderAdmin
                | MembershipChange::ResolutionActivation { .. } => &[],
            })
            .filter(|reference| reference.recipient_pubkey == recipient_pubkey)
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    pub fn wrapped_key_authority_for(
        &self,
        recipient_pubkey: &str,
    ) -> Result<Vec<WrappedStoreKeyRef>, MembershipError> {
        let active_grants = self.active_grants_for(recipient_pubkey);
        for (index, (rotation_coord, entry)) in self
            .entries_with_coords()
            .enumerate()
            .filter(|(_, (coord, _))| self.included.contains(*coord))
        {
            let MembershipChange::RemoveMember { wrapped_keys, .. } = &entry.change else {
                continue;
            };
            if wrapped_keys
                .iter()
                .any(|reference| reference.recipient_pubkey == recipient_pubkey)
            {
                continue;
            }
            let rotation_generation = wrapped_keys
                .first()
                .ok_or(MembershipError::InvalidWrappedKeys(index))?
                .generation;
            let covered_by_later_grant = !active_grants.is_empty()
                && active_grants.iter().all(|(active_grant, _)| {
                    let Some((_, creation)) = self.entries_with_coords().find(|(_, entry)| {
                        matches!(
                            &entry.change,
                            MembershipChange::SetMember { grant_id, .. }
                                if grant_id == *active_grant
                        )
                    }) else {
                        return false;
                    };
                    let MembershipChange::SetMember { wrapped_key, .. } = &creation.change else {
                        return false;
                    };
                    wrapped_key.generation >= rotation_generation
                        && membership_history_closure(&self.entries, &creation.dependencies)
                            .contains(rotation_coord)
                });
            if !covered_by_later_grant {
                return Err(MembershipError::MissingWrappedKeyCoverage {
                    recipient_pubkey: recipient_pubkey.to_string(),
                    rotation: Box::new(rotation_coord.clone()),
                });
            }
        }
        Ok(self.active_wrapped_keys_for(recipient_pubkey))
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

    pub(crate) fn next_member_grant_id_in_stream(
        &self,
        signer: &UserKeypair,
        stream_id: AuthorStreamId,
        user_pubkey: &str,
    ) -> Result<MembershipGrantId, MembershipError> {
        let author = keys::public_key_hex(signer);
        let author_grant = self
            .active_owner_grant(&author)
            .ok_or_else(|| MembershipError::SignerIsNotOwner(author.clone()))?;
        let (seq, _) = self.next_stream_position(&author, &author_grant, stream_id)?;
        Ok(derive_grant_id(
            self.store_id().expect("validated chain has a store id"),
            &author,
            &author_grant,
            stream_id,
            seq,
            user_pubkey,
        ))
    }

    pub(crate) fn signed_set_member_with_anchor_and_wrapped_key_in_stream(
        &self,
        signer: &UserKeypair,
        stream_id: AuthorStreamId,
        user_pubkey: String,
        provider_account_email: Option<String>,
        role: MemberRole,
        membership: Option<GrantStreamAnchor>,
        wrapped_key: WrappedStoreKeyRef,
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
        if (role == MemberRole::Owner) != membership.is_some() {
            return Err(MembershipError::InvalidOwnerMembershipAnchor(
                self.entries.len(),
            ));
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
                membership,
                replaces,
                owner_barriers,
                wrapped_key,
            },
            provider_admin: None,
            signature: String::new(),
        };
        sign_membership_entry(&mut entry, signer);
        let mut candidate = self.clone();
        candidate.add_entry(entry.clone())?;
        Ok(entry)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn signed_set_member_in_stream(
        &self,
        signer: &UserKeypair,
        stream_id: AuthorStreamId,
        user_pubkey: String,
        provider_account_email: Option<String>,
        role: MemberRole,
        created_at: String,
    ) -> Result<MembershipEntry, MembershipError> {
        let grant_id = self.next_member_grant_id_in_stream(signer, stream_id, &user_pubkey)?;
        let membership = (role == MemberRole::Owner).then(|| GrantStreamAnchor::StoreMembership {
            first_slot: crate::storage::cloud::ObjectSlot::logical(format!(
                "{}.json",
                super::store_commit::membership_head_slot_prefix(
                    &user_pubkey,
                    &grant_id,
                    stream_id,
                    1,
                )
            ))
            .expect("test membership head slot is a valid logical key"),
        });
        let dependencies = self.frontier();
        let wrapped_key = test_wrapped_key_ref(
            &keys::public_key_hex(signer),
            &user_pubkey,
            membership_causal_generation(&self.entries, &dependencies),
            b"Merge membership test wrap",
        );
        self.signed_set_member_with_anchor_and_wrapped_key_in_stream(
            signer,
            stream_id,
            user_pubkey,
            provider_account_email,
            role,
            membership,
            wrapped_key,
            created_at,
        )
    }

    pub fn signed_remove_member_with_wrapped_keys_in_stream(
        &self,
        signer: &UserKeypair,
        stream_id: AuthorStreamId,
        user_pubkey: String,
        wrapped_keys: Vec<WrappedStoreKeyRef>,
        created_at: String,
    ) -> Result<MembershipEntry, MembershipError> {
        let removes = self.active_grant_ids(&user_pubkey);
        if removes.is_empty() {
            return Err(MembershipError::NotAMember(user_pubkey));
        }
        let retains_owner = self.state.grants.iter().any(|(grant, record)| {
            !self.state.removed.contains(grant)
                && !removes.contains(grant)
                && record.role == MemberRole::Owner
        });
        if !retains_owner {
            return Err(MembershipError::NoActiveOwner);
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
                wrapped_keys,
            },
            provider_admin: None,
            signature: String::new(),
        };
        sign_membership_entry(&mut entry, signer);
        let mut candidate = self.clone();
        candidate.add_entry(entry.clone())?;
        Ok(entry)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn signed_remove_member_in_stream(
        &self,
        signer: &UserKeypair,
        stream_id: AuthorStreamId,
        user_pubkey: String,
        created_at: String,
    ) -> Result<MembershipEntry, MembershipError> {
        let owner = keys::public_key_hex(signer);
        let dependencies = self.frontier();
        let generation = membership_causal_generation(&self.entries, &dependencies)
            .checked_add(1)
            .ok_or(MembershipError::InvalidWrappedKeys(self.entries.len()))?;
        let wrapped_keys = self
            .current_members()
            .into_iter()
            .filter(|(member, _)| member != &user_pubkey)
            .map(|(member, _)| {
                test_wrapped_key_ref(&owner, &member, generation, b"Merge removal test wrap")
            })
            .collect();
        self.signed_remove_member_with_wrapped_keys_in_stream(
            signer,
            stream_id,
            user_pubkey,
            wrapped_keys,
            created_at,
        )
    }

    pub fn signed_provider_admin_change_in_stream(
        &self,
        signer: &UserKeypair,
        stream_id: AuthorStreamId,
        change: super::provider::ProviderAdminChange,
        created_at: String,
    ) -> Result<MembershipEntry, MembershipError> {
        let author = keys::public_key_hex(signer);
        let author_grant = self
            .active_owner_grant(&author)
            .ok_or_else(|| MembershipError::SignerIsNotOwner(author.clone()))?;
        let (seq, previous_hash) = self.next_stream_position(&author, &author_grant, stream_id)?;
        let owner_grants = self
            .state
            .grants
            .iter()
            .filter(|(grant_id, record)| {
                record.role == MemberRole::Owner && !self.state.removed.contains(*grant_id)
            })
            .map(|(grant_id, _)| grant_id.clone())
            .collect();
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
            resolution_dependencies: Vec::new(),
            created_at,
            change: MembershipChange::ProviderAdmin,
            provider_admin: Some(
                super::provider::ProviderAdminMembershipChange::MergeConcurrent {
                    change,
                    owner_barriers: self.owner_barriers(&owner_grants),
                },
            ),
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
        reference: StoreMembershipConflictResolutionRef,
        resolution: &StoreMembershipConflictResolution,
        created_at: String,
    ) -> Result<MembershipEntry, MembershipError> {
        self.ensure_resolved()?;
        let MembershipStatus::Resolved(resolved_before) = self.status() else {
            unreachable!("ensure_resolved accepted a conflict")
        };
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
            provider_admin: None,
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
                MembershipChange::SetMember {
                    user_pubkey,
                    role,
                    grant_id,
                    membership,
                    owner_barriers,
                    ..
                } => {
                    if (role == &MemberRole::Owner)
                        != membership.as_ref().is_some_and(|anchor| {
                            store_membership_anchor_stream(user_pubkey, grant_id, anchor).is_some()
                        })
                    {
                        return Err(MembershipError::InvalidOwnerMembershipAnchor(index));
                    }
                    owner_barriers
                }
                MembershipChange::RemoveMember { owner_barriers, .. } => owner_barriers,
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
                MembershipChange::ProviderAdmin => {
                    let Some(super::provider::ProviderAdminMembershipChange::MergeConcurrent {
                        owner_barriers,
                        ..
                    }) = &entry.provider_admin
                    else {
                        return Err(MembershipError::InvalidProviderAdminChange(index));
                    };
                    if !entry.resolution_dependencies.is_empty()
                        || owner_barriers.values().any(|barrier| {
                            !barrier
                                .observed_streams
                                .windows(2)
                                .all(|pair| pair[0].stream_key() < pair[1].stream_key())
                        })
                    {
                        return Err(MembershipError::InvalidProviderAdminChange(index));
                    }
                    continue;
                }
                MembershipChange::Founder { .. } => continue,
            };
            if entry.provider_admin.is_some() {
                return Err(MembershipError::InvalidProviderAdminChange(index));
            }
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
                    ..
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
            || founder.stream_id != derive_founder_stream_id(&founder.store_id, owner_pubkey)
            || founder.provider_admin.is_some()
        {
            return Err(MembershipError::InvalidFounder);
        }

        validate_provider_admin_controls(&self.entries, self.resolution_checkpoint.as_ref())?;
        validate_membership_wrapped_keys(&self.entries, self.resolution_checkpoint.as_ref())?;

        let reduced = match &self.resolution_checkpoint {
            Some(checkpoint) => reduce_store_membership_from_checkpoint(&self.entries, checkpoint)?,
            None => reduce_store_membership(&self.entries)?,
        };
        let checkpoint_grants = self
            .resolution_checkpoint
            .as_ref()
            .map(|checkpoint| &checkpoint.grants);
        let provider_admin_seed = self
            .resolution_checkpoint
            .as_ref()
            .map_or(&self.provider_admin_genesis, |checkpoint| {
                &checkpoint.provider_admin
            });
        let (state_source, status) = match reduced {
            CausalGrantStatus::Resolved(reduced) => {
                let provider_admin = super::provider::ProviderAdminState::reduce_merge(
                    provider_admin_seed,
                    &self.entries,
                    &reduced.included,
                )?;
                let resolved =
                    resolved_store_membership(&reduced, checkpoint_grants, provider_admin);
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
                        let resolved = resolved_store_membership(
                            &branch.reduced,
                            checkpoint_grants,
                            super::provider::ProviderAdminState::reduce_merge(
                                provider_admin_seed,
                                &self.entries,
                                &branch.reduced.included,
                            )?,
                        );
                        Ok(StoreMembershipBranch {
                            heads: self.branch_head_refs(&branch.raw_heads)?,
                            effective_frontier: branch.effective_frontier,
                            active_grants: resolved.active_grants,
                            provider_admin: resolved.provider_admin,
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
        resolutions: &[(
            StoreMembershipConflictResolutionRef,
            StoreMembershipConflictResolution,
        )],
    ) -> Result<(), MembershipError> {
        let (raw_heads, effective_frontier) = match self.conflict() {
            Some(MembershipConflict::RevocationCycle {
                heads,
                maximal_valid_branches,
                ..
            }) => {
                let selected = resolutions
                    .iter()
                    .map(|(_, resolution)| {
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
        let mut grant_anchors = self
            .resolution_checkpoint
            .as_ref()
            .map_or_else(BTreeMap::new, |checkpoint| checkpoint.grant_anchors.clone());
        for entry in &self.entries {
            let (grant, record) = match &entry.change {
                MembershipChange::Founder {
                    owner_pubkey,
                    owner_grant_id,
                    ..
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
                | MembershipChange::ProviderAdmin
                | MembershipChange::ResolutionActivation { .. } => continue,
            };
            grants.insert(grant, record);
            match &entry.change {
                MembershipChange::Founder {
                    owner_grant_id,
                    membership,
                    ..
                } => {
                    grant_anchors.insert(owner_grant_id.clone(), membership.clone());
                }
                MembershipChange::SetMember {
                    grant_id,
                    membership: Some(membership),
                    ..
                } => {
                    grant_anchors.insert(grant_id.clone(), membership.clone());
                }
                _ => {}
            }
        }
        grants.extend(resolved.active_grants.clone());
        for (_, resolution) in resolutions {
            grant_anchors.insert(
                resolution.replacement_grant.clone(),
                resolution.replacement_membership.clone(),
            );
        }
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
        resolution_refs.extend(resolutions.iter().map(|(reference, _)| reference.clone()));
        resolution_refs.sort();
        resolution_refs.dedup();
        self.resolution_checkpoint = Some(MembershipResolutionCheckpoint {
            raw_heads,
            effective_frontier: effective_frontier.clone(),
            grants: grants.clone(),
            grant_anchors,
            removed: removed.clone(),
            included: included.clone(),
            resolutions: resolution_refs,
            provider_admin: resolved.provider_admin.combined_state().clone(),
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

fn validate_membership_wrapped_keys(
    entries: &[MembershipEntry],
    checkpoint: Option<&MembershipResolutionCheckpoint>,
) -> Result<(), MembershipError> {
    for (index, entry) in entries.iter().enumerate() {
        let included = membership_history_closure(entries, &entry.dependencies);
        let causal_generation = membership_causal_generation(entries, &entry.dependencies);
        let references = match &entry.change {
            MembershipChange::SetMember {
                user_pubkey,
                wrapped_key,
                ..
            } => {
                if wrapped_key.owner_pubkey != entry.author_pubkey
                    || wrapped_key.recipient_pubkey != *user_pubkey
                    || wrapped_key.generation != causal_generation
                    || wrapped_key.validate_identity().is_err()
                {
                    return Err(MembershipError::InvalidWrappedKeys(index));
                }
                continue;
            }
            MembershipChange::RemoveMember {
                user_pubkey,
                wrapped_keys,
                ..
            } => (user_pubkey, wrapped_keys),
            MembershipChange::Founder { .. }
            | MembershipChange::ProviderAdmin
            | MembershipChange::ResolutionActivation { .. } => continue,
        };
        let (removed_pubkey, wrapped_keys) = references;
        let rotation_generation = wrapped_keys.first().map(|reference| reference.generation);
        if causal_generation.checked_add(1) != rotation_generation
            || !wrapped_keys.windows(2).all(|pair| pair[0] < pair[1])
            || wrapped_keys.iter().any(|reference| {
                reference.owner_pubkey != entry.author_pubkey
                    || reference.recipient_pubkey == *removed_pubkey
                    || Some(reference.generation) != rotation_generation
                    || reference.validate_identity().is_err()
            })
        {
            return Err(MembershipError::InvalidWrappedKeys(index));
        }
        let causal_past = entries
            .iter()
            .filter(|candidate| included.contains(&candidate.coord()))
            .cloned()
            .collect::<Vec<_>>();
        let precedes_checkpoint = checkpoint.is_some_and(|checkpoint| {
            checkpoint.raw_heads.iter().any(|head| {
                head.stream_key() == entry.coord().stream_key() && entry.seq <= head.seq
            })
        });
        let reduced = match (checkpoint, precedes_checkpoint) {
            (Some(checkpoint), false) => {
                reduce_store_membership_from_checkpoint(&causal_past, checkpoint)?
            }
            (None, _) | (Some(_), true) => reduce_store_membership(&causal_past)?,
        };
        let CausalGrantStatus::Resolved(reduced) = reduced else {
            return Err(MembershipError::InvalidWrappedKeys(index));
        };
        let expected_recipients = reduced
            .grants
            .iter()
            .filter(|(grant, record)| {
                !reduced.removed.contains(*grant) && record.member_pubkey != *removed_pubkey
            })
            .map(|(_, record)| record.member_pubkey.clone())
            .collect::<BTreeSet<_>>();
        let actual_recipients = wrapped_keys
            .iter()
            .map(|reference| reference.recipient_pubkey.clone())
            .collect::<BTreeSet<_>>();
        if expected_recipients != actual_recipients || actual_recipients.len() != wrapped_keys.len()
        {
            return Err(MembershipError::InvalidWrappedKeys(index));
        }
    }
    Ok(())
}

fn membership_causal_generation(
    entries: &[MembershipEntry],
    dependencies: &[MembershipCoord],
) -> u64 {
    let included = membership_history_closure(entries, dependencies);
    entries
        .iter()
        .filter(|candidate| included.contains(&candidate.coord()))
        .flat_map(|candidate| match &candidate.change {
            MembershipChange::SetMember { wrapped_key, .. } => std::slice::from_ref(wrapped_key),
            MembershipChange::RemoveMember { wrapped_keys, .. } => wrapped_keys.as_slice(),
            MembershipChange::Founder { .. }
            | MembershipChange::ProviderAdmin
            | MembershipChange::ResolutionActivation { .. } => &[],
        })
        .map(|reference| reference.generation)
        .max()
        .unwrap_or(crate::encryption::INITIAL_KEY_GENERATION)
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

fn validate_provider_admin_controls(
    entries: &[MembershipEntry],
    checkpoint: Option<&MembershipResolutionCheckpoint>,
) -> Result<(), MembershipError> {
    for (index, entry) in entries.iter().enumerate() {
        let Some(super::provider::ProviderAdminMembershipChange::MergeConcurrent {
            owner_barriers,
            ..
        }) = &entry.provider_admin
        else {
            continue;
        };
        let included = membership_history_closure(entries, &entry.dependencies);
        let causal_past = entries
            .iter()
            .filter(|candidate| included.contains(&candidate.coord()))
            .cloned()
            .collect::<Vec<_>>();
        let reduced = match checkpoint {
            Some(checkpoint) => reduce_store_membership_from_checkpoint(&causal_past, checkpoint)?,
            None => reduce_store_membership(&causal_past)?,
        };
        let CausalGrantStatus::Resolved(reduced) = reduced else {
            return Err(MembershipError::InvalidProviderAdminChange(index));
        };
        let expected = reduced
            .grants
            .iter()
            .filter(|(grant_id, record)| {
                !reduced.removed.contains(*grant_id) && record.assignment.is_owner()
            })
            .map(|(grant_id, _)| {
                let observed_streams = entry
                    .dependencies
                    .iter()
                    .filter(|coord| coord.author_owner_grant == *grant_id)
                    .cloned()
                    .collect();
                (grant_id.clone(), OwnerStreamBarrier { observed_streams })
            })
            .collect::<BTreeMap<_, _>>();
        if *owner_barriers != expected {
            return Err(MembershipError::InvalidProviderAdminChange(index));
        }
    }
    Ok(())
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
                    ..
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
                    membership: _,
                    replaces,
                    owner_barriers,
                    ..
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
                    ..
                } => CausalChange::RemoveMember {
                    member_pubkey: user_pubkey.clone(),
                    removes: removes.clone(),
                    owner_barriers: owner_barriers
                        .iter()
                        .map(|(grant, barrier)| (grant.clone(), shared_store_barrier(barrier)))
                        .collect(),
                },
                MembershipChange::ProviderAdmin => CausalChange::Control,
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
    provider_admin: super::provider::ProviderAdminResolution,
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
    let state_hash = store_membership_state_hash(&active_grants, &provider_admin);
    ResolvedStoreMembership {
        active_grants,
        provider_admin,
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
    provider_admin: &super::provider::ProviderAdminResolution,
) -> ObjectHash {
    #[derive(Serialize)]
    struct State<'a> {
        domain: &'static str,
        active_grants: &'a BTreeMap<MembershipGrantId, MembershipGrantRecord>,
        provider_admin: &'a super::provider::ProviderAdminResolution,
    }
    ObjectHash::digest(
        &serde_json::to_vec(&State {
            domain: "coven.store-membership-state.v1",
            active_grants,
            provider_admin,
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
            MembershipError::MissingDependency {
                index,
                dependency: Box::new(dependency),
            }
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

pub(crate) fn derive_founder_stream_id(store_id: &str, owner_pubkey: &str) -> AuthorStreamId {
    AuthorStreamId::from_digest(ObjectHash::digest(
        format!("coven.membership-founder-stream.v1\0{store_id}\0{owner_pubkey}").as_bytes(),
    ))
}

fn store_membership_anchor_stream(
    owner_pubkey: &str,
    owner_grant: &MembershipGrantId,
    anchor: &GrantStreamAnchor,
) -> Option<AuthorStreamId> {
    let GrantStreamAnchor::StoreMembership { first_slot } = anchor else {
        return None;
    };
    let prefix = format!(
        "{}{owner_pubkey}/{owner_grant}/",
        super::store_commit::STORE_MEMBERSHIP_HEAD_PREFIX,
    );
    first_slot
        .logical_key()
        .strip_prefix(&prefix)?
        .strip_suffix("/1.json")?
        .parse()
        .ok()
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

pub fn founder_entry(
    store_id: &str,
    owner: &UserKeypair,
    owner_grant_id: MembershipGrantId,
    created_at: &str,
    membership: GrantStreamAnchor,
    provider_admin: super::provider::FounderProviderAdminGrant,
) -> MembershipEntry {
    let owner_pubkey = keys::public_key_hex(owner);
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
            membership,
            provider_admin,
        },
        provider_admin: None,
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
        #[serde(skip_serializing_if = "Option::is_none")]
        provider_admin: Option<&'a super::provider::ProviderAdminMembershipChange>,
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
        provider_admin: entry.provider_admin.as_ref(),
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
    #[allow(clippy::too_many_arguments)]
    pub fn signed(
        store_id: String,
        author_registration: StoreDeviceRegistrationRef,
        entry: MembershipEntryRef,
        predecessor: Option<MembershipHeadRef>,
        mut resolutions: Vec<StoreMembershipConflictResolutionRef>,
        successor: SuccessorLink,
        device_signer: &UserKeypair,
    ) -> Self {
        resolutions.sort();
        resolutions.dedup();
        let mut head = Self {
            version: STORE_PROTOCOL_VERSION,
            store_id,
            author_registration,
            entry,
            predecessor,
            resolutions,
            successor,
            signature: String::new(),
        };
        let (_, signature) = keys::sign_hex(device_signer, &head.canonical_bytes());
        head.signature = signature;
        head
    }

    pub fn verify(&self, registration: &StoreDeviceRegistration) -> bool {
        self.version == STORE_PROTOCOL_VERSION
            && self.resolutions.windows(2).all(|pair| pair[0] < pair[1])
            && self
                .author_registration
                .verify_registration(registration)
                .is_ok()
            && registration.author_pubkey == self.entry.coord.author_pubkey
            && self.successor.predecessor
                == self
                    .predecessor
                    .as_ref()
                    .map(|reference| reference.object.clone())
            && keys::verify_signature_hex(
                &registration.device_signing_pubkey,
                &self.signature,
                &self.canonical_bytes(),
            )
    }

    pub fn entry_coord(&self) -> MembershipCoord {
        self.entry.coord.clone()
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
            author_registration: &'a StoreDeviceRegistrationRef,
            entry: &'a MembershipEntryRef,
            #[serde(skip_serializing_if = "Option::is_none")]
            predecessor: Option<&'a MembershipHeadRef>,
            resolutions: &'a [StoreMembershipConflictResolutionRef],
            successor: &'a SuccessorLink,
        }
        serde_json::to_vec(&Signed {
            version: self.version,
            store_id: &self.store_id,
            author_registration: &self.author_registration,
            entry: &self.entry,
            predecessor: self.predecessor.as_ref(),
            resolutions: &self.resolutions,
            successor: &self.successor,
        })
        .expect("membership head signed fields serialize")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::cloud::ObjectSlot;
    use crate::sync::circle_control::StoreMembershipStateRef;
    use crate::sync::storage::{ProviderDeviceBinding, ProviderPrincipalId};
    use crate::sync::store_commit::{
        commit_semantic_prefix, device_self_retirement_semantic_prefix,
        membership_entry_semantic_prefix, membership_head_semantic_prefix,
        membership_resolution_semantic_prefix, registration_semantic_prefix, CandidateFamilyId,
        DeviceJoinAttemptId, DeviceStreamAnchor, GrantStreamAnchor, ResolvedStoreDeviceState,
        StoreBatchCommitRef, StoreCommitAnchor, StoreCommitCoord, StoreCommitOrder,
        StoreCreationId, StoreDeviceRegistrationOrigin, StoreDeviceRegistrationRef,
        StoreDeviceSelfRetirement, StoreDeviceSelfRetirementRef, StoreDeviceStateRef,
        StoreHistoryCut, StoreRootRef, StoreSerialPredecessor, StreamActivation, SERIAL_STREAM_ID,
    };

    fn key() -> UserKeypair {
        UserKeypair::generate()
    }

    fn stream(byte: u8) -> AuthorStreamId {
        AuthorStreamId::from_bytes([byte; 32])
    }

    fn slot(key: impl Into<String>) -> ObjectSlot {
        ObjectSlot::logical(key.into()).expect("valid test object slot")
    }

    fn exact(key: impl Into<String>, bytes: &[u8]) -> ExactObjectRef {
        ExactObjectRef::new(slot(key), bytes.len() as u64, ObjectHash::digest(bytes))
    }

    fn membership_anchor(store_id: &str) -> GrantStreamAnchor {
        GrantStreamAnchor::StoreMembership {
            first_slot: slot(format!("test/{store_id}/membership/1.json")),
        }
    }

    fn recovery_anchor(store_id: &str) -> GrantStreamAnchor {
        GrantStreamAnchor::OwnerRecovery {
            first_slot: slot(format!("test/{store_id}/recovery/1.json")),
        }
    }

    fn test_founder_entry(
        store_id: &str,
        owner: &UserKeypair,
        created_at: &str,
        membership: GrantStreamAnchor,
    ) -> MembershipEntry {
        founder_entry(
            store_id,
            owner,
            crate::sync::test_helpers::test_membership_grant_id(store_id),
            created_at,
            membership,
            crate::sync::test_helpers::test_founder_provider_admin(store_id),
        )
    }

    fn test_root(store_id: &str) -> StoreRootRef {
        let bytes = store_id.as_bytes();
        StoreRootRef {
            store_root_id: ObjectHash::digest(format!("{store_id} identity").as_bytes()),
            store_root_hash: ObjectHash::digest(bytes),
            object: exact(format!("test/{store_id}/root.json"), bytes),
        }
    }

    fn registration(
        root: &StoreRootRef,
        label: &str,
        signer: &UserKeypair,
    ) -> (StoreDeviceRegistration, StoreDeviceRegistrationRef) {
        let registration = StoreDeviceRegistration::signed(
            root.clone(),
            StoreDeviceRegistrationOrigin::Founder {
                creation_id: StoreCreationId::from_nonce(label),
            },
            ProviderDeviceBinding {
                principal: ProviderPrincipalId::CustomS3Credential {
                    access_key_id_hash: ObjectHash::digest(label.as_bytes()),
                },
            },
            StoreCommitAnchor::Serial,
            DeviceStreamAnchor::StoreAcknowledgements {
                first_slot: slot(format!("test/{label}/acks/1.json")),
            },
            DeviceStreamAnchor::StoreSnapshots {
                first_slot: slot(format!("test/{label}/snapshots/1.json")),
            },
            signer,
        )
        .expect("sign test registration");
        let bytes = registration.to_bytes();
        let reference = StoreDeviceRegistrationRef::from_registration(
            &registration,
            exact(
                format!(
                    "{}.json",
                    registration_semantic_prefix(&registration.device_id.to_string())
                ),
                &bytes,
            ),
        );
        (registration, reference)
    }

    fn exact_head(
        entry: &MembershipEntry,
        signer: &UserKeypair,
    ) -> (MembershipHeadRef, AuthorHead) {
        exact_head_with_resolutions(entry, signer, entry.resolution_dependencies.clone())
    }

    fn exact_head_with_resolutions(
        entry: &MembershipEntry,
        signer: &UserKeypair,
        resolutions: Vec<StoreMembershipConflictResolutionRef>,
    ) -> (MembershipHeadRef, AuthorHead) {
        let root = test_root(&entry.store_id);
        let (registration, registration_ref) = registration(
            &root,
            &format!("{}-{}", entry.store_id, entry.author_pubkey),
            signer,
        );
        let entry_bytes = serde_json::to_vec(entry).expect("serialize membership entry");
        let coord = entry.coord();
        let entry_ref = MembershipEntryRef {
            coord: coord.clone(),
            object: exact(
                format!(
                    "{}.json",
                    membership_entry_semantic_prefix(
                        &coord.author_pubkey,
                        &coord.author_owner_grant,
                        coord.stream_id,
                        coord.seq,
                        coord.entry_hash,
                    )
                ),
                &entry_bytes,
            ),
        };
        let anchor = membership_anchor(&entry.store_id);
        let successor = SuccessorLink {
            activation: StreamActivation::grant_authorized(
                root.store_root_hash,
                registration_ref.clone(),
                entry.author_owner_grant.clone(),
                anchor,
            )
            .activation_id(),
            predecessor: None,
            next_slot: slot(format!(
                "test/{}/membership-heads/{}/next.json",
                entry.store_id, coord.entry_hash
            )),
        };
        let device_signer = registration.device_signer(signer).unwrap();
        let head = AuthorHead::signed(
            entry.store_id.clone(),
            registration_ref,
            entry_ref,
            None,
            resolutions,
            successor,
            &device_signer,
        );
        let head_bytes = serde_json::to_vec(&head).expect("serialize membership head");
        let reference = MembershipHeadRef {
            coord: coord.clone(),
            head_hash: head.head_hash(),
            object: exact(
                format!(
                    "{}.json",
                    membership_head_semantic_prefix(
                        &coord.author_pubkey,
                        &coord.author_owner_grant,
                        coord.stream_id,
                        coord.seq,
                        head.head_hash(),
                    )
                ),
                &head_bytes,
            ),
        };
        (reference, head)
    }

    fn exact_resolution(
        resolution: StoreMembershipConflictResolution,
    ) -> (
        StoreMembershipConflictResolutionRef,
        StoreMembershipConflictResolution,
    ) {
        let bytes = serde_json::to_vec(&resolution).expect("serialize membership resolution");
        let reference = resolution.resolution_ref(exact(
            format!(
                "{}.json",
                membership_resolution_semantic_prefix(
                    resolution.conflict_hash,
                    &resolution.resolver_pubkey,
                    resolution.resolution_hash(),
                )
            ),
            &bytes,
        ));
        (reference, resolution)
    }

    fn join_registration(
        root: &StoreRootRef,
        label: &str,
        signer: &UserKeypair,
    ) -> (StoreDeviceRegistration, StoreDeviceRegistrationRef) {
        let attempt_id = DeviceJoinAttemptId::from_hash(ObjectHash::digest(label.as_bytes()));
        let registration = StoreDeviceRegistration::signed(
            root.clone(),
            StoreDeviceRegistrationOrigin::Join {
                attempt_id,
                attempt_slot: slot(format!("test/{label}/join/attempt.json")),
                outcome_slot: slot(format!("test/{label}/join/outcome.json")),
            },
            ProviderDeviceBinding {
                principal: ProviderPrincipalId::CustomS3Credential {
                    access_key_id_hash: ObjectHash::digest(
                        format!("{label} access key").as_bytes(),
                    ),
                },
            },
            StoreCommitAnchor::Serial,
            DeviceStreamAnchor::StoreAcknowledgements {
                first_slot: slot(format!("test/{label}/acks/1.json")),
            },
            DeviceStreamAnchor::StoreSnapshots {
                first_slot: slot(format!("test/{label}/snapshots/1.json")),
            },
            signer,
        )
        .expect("sign test join registration");
        let bytes = registration.to_bytes();
        let reference = StoreDeviceRegistrationRef::from_registration(
            &registration,
            exact(
                format!(
                    "{}.json",
                    registration_semantic_prefix(&registration.device_id.to_string())
                ),
                &bytes,
            ),
        );
        (registration, reference)
    }

    fn serial_retirement_commit(
        role: MemberRole,
    ) -> (
        SerialAuthorizationState,
        StoreBatchCommitRef,
        StoreBatchCommit,
        StoreDeviceRegistration,
    ) {
        let store_id = "serial-follower-retirement";
        let owner = key();
        let follower = key();
        let root = test_root(store_id);
        let founder = test_founder_entry(store_id, &owner, "founder", membership_anchor(store_id));
        let founder_recovery = recovery_anchor(store_id);
        let (owner_registration, owner_registration_ref) =
            registration(&root, "serial-retirement-owner", &owner);
        let founder_devices = ResolvedStoreDeviceState::founder(
            &root,
            owner_registration_ref.clone(),
            &founder.author_pubkey,
            founder.author_owner_grant.clone(),
            &founder_recovery,
        )
        .unwrap();
        let genesis = StoreSerialPredecessor::Genesis {
            root: root.clone(),
            founder_registration: owner_registration_ref.clone(),
        };
        let membership = SerialMembershipState::from_founder(root.store_root_hash, &founder)
            .expect("founder membership");
        let authorization = SerialAuthorizationState::from_test_membership(&founder, membership)
            .expect("founder authorization");
        let add_follower = authorization
            .membership
            .signed_set_member(
                &owner,
                keys::public_key_hex(&follower),
                None,
                role,
                "add retirement author".to_string(),
            )
            .unwrap();
        let add_order = StoreCommitOrder::Serial {
            seq: 1,
            predecessor: genesis.clone(),
        };
        let add_membership = StoreMembershipStateRef::serial(
            genesis.clone(),
            founder_devices.recovery.clone(),
            &authorization,
        )
        .unwrap();
        let add_devices = StoreDeviceStateRef::serial(genesis, &founder_devices).unwrap();
        let add_commit = StoreBatchCommit::signed_with_control(
            root.store_root_hash,
            crate::WriteId::from_generated("add-retirement-follower".to_string()),
            StoreCommitCoord::Serial { sequence: 1 },
            owner_registration_ref,
            &owner_registration,
            add_order,
            add_membership,
            add_devices,
            None,
            Some(StoreControl::SerialMembership {
                entry: add_follower,
            }),
            None,
            &owner_registration.device_signer(&owner).unwrap(),
        )
        .unwrap();
        let add_bytes = add_commit.to_bytes();
        let add_ref = StoreBatchCommitRef::from_commit(
            &add_commit,
            StoreCommitCoord::Serial { sequence: 1 },
            exact(
                format!(
                    "{}.json",
                    commit_semantic_prefix(
                        add_commit.candidate_family(),
                        SERIAL_STREAM_ID,
                        1,
                        add_commit.commit_hash(),
                    )
                ),
                &add_bytes,
            ),
        )
        .unwrap();
        let authorization = authorization
            .authorize_and_apply(&add_ref, &add_commit, &owner_registration)
            .unwrap();
        let (follower_registration, follower_ref) =
            join_registration(&root, "serial-retirement-follower", &follower);
        let active_devices = founder_devices
            .activate_registration(follower_ref.clone(), None)
            .unwrap();
        let predecessor = StoreSerialPredecessor::Commit(add_ref);
        let order = StoreCommitOrder::Serial {
            seq: 2,
            predecessor: predecessor.clone(),
        };
        let write_id = crate::WriteId::from_generated("retire-follower".to_string());
        let candidate_family =
            CandidateFamilyId::derive(root.store_root_hash, &follower_ref, &write_id, &order);
        let retirement = StoreDeviceSelfRetirement::signed(
            root.store_root_hash,
            candidate_family,
            follower_ref.clone(),
            StoreHistoryCut::Serial(predecessor.clone()),
            &follower_registration.device_signer(&follower).unwrap(),
        )
        .unwrap();
        let retirement_bytes = retirement.to_bytes();
        let retirement_ref = StoreDeviceSelfRetirementRef::from_retirement(
            &retirement,
            exact(
                format!(
                    "{}.json",
                    device_self_retirement_semantic_prefix(
                        candidate_family,
                        &follower_registration.device_id,
                        retirement.retirement_hash(),
                    )
                ),
                &retirement_bytes,
            ),
        );
        StoreDeviceSelfRetirement::parse_at(
            &retirement_bytes,
            &retirement_ref,
            &follower_registration,
        )
        .expect("verify exact retirement object");
        let membership_state = StoreMembershipStateRef::serial(
            predecessor.clone(),
            active_devices.recovery.clone(),
            &authorization,
        )
        .unwrap();
        let device_state = StoreDeviceStateRef::serial(predecessor, &active_devices).unwrap();
        let signer = follower_registration.device_signer(&follower).unwrap();
        let commit = StoreBatchCommit::signed_with_self_retirement(
            root.store_root_hash,
            write_id,
            StoreCommitCoord::Serial { sequence: 2 },
            follower_ref,
            &follower_registration,
            order,
            membership_state,
            device_state,
            None,
            retirement_ref,
            &signer,
        )
        .unwrap();
        let commit_bytes = commit.to_bytes();
        let commit_ref = StoreBatchCommitRef::from_commit(
            &commit,
            StoreCommitCoord::Serial { sequence: 2 },
            exact(
                format!(
                    "{}.json",
                    commit_semantic_prefix(
                        commit.candidate_family(),
                        SERIAL_STREAM_ID,
                        2,
                        commit.commit_hash(),
                    )
                ),
                &commit_bytes,
            ),
        )
        .unwrap();
        (authorization, commit_ref, commit, follower_registration)
    }

    fn founded(store_id: &str, owner: &UserKeypair) -> MembershipChain {
        MembershipChain::from_entries(vec![test_founder_entry(
            store_id,
            owner,
            "founder",
            membership_anchor(store_id),
        )])
        .unwrap()
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
            .signed_set_member_in_stream(
                &first,
                stream(1),
                second_pubkey.clone(),
                None,
                MemberRole::Owner,
                "add second Owner".to_string(),
            )
            .expect("add second Owner");
        base.add_entry(add_second).expect("apply second Owner");
        let add_third = base
            .signed_set_member_in_stream(
                &first,
                stream(1),
                third_pubkey,
                None,
                MemberRole::Owner,
                "add third Owner".to_string(),
            )
            .expect("add third Owner");
        base.add_entry(add_third).expect("apply third Owner");
        let remove_second = base
            .signed_remove_member_in_stream(
                &first,
                stream(1),
                second_pubkey,
                "first branch".to_string(),
            )
            .expect("first branch");
        let remove_first = base
            .signed_remove_member_in_stream(
                &second,
                stream(92),
                first_pubkey,
                "second branch".to_string(),
            )
            .expect("second branch");
        let mut entries = base.entries().to_vec();
        entries.extend([remove_second.clone(), remove_first.clone()]);
        let heads = vec![
            exact_head(
                base.entries().first().expect("founder membership entry"),
                &first,
            ),
            exact_head(&remove_second, &first),
            exact_head(&remove_first, &second),
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
            .signed_cycle_resolution(
                store_root_hash,
                branch,
                membership_anchor("unaffected-store-resolver"),
                &third,
            )
            .expect("unaffected Owner resolution");
        let resolution = exact_resolution(resolution);
        let resolved = conflicted
            .resolved_with(store_root_hash, std::slice::from_ref(&resolution))
            .expect("unaffected Owner resolution is valid");

        assert!(resolution.1.retired_owner_grants.contains(&old_grant));
        assert!(!resolved.active_grants.contains_key(&old_grant));
        assert!(resolved
            .active_grants
            .contains_key(&resolution.1.replacement_grant));
    }

    #[test]
    fn store_revocation_cycle_over_protocol_bound_is_typed() {
        let owners = (0..13).map(|_| key()).collect::<Vec<_>>();
        let pubkeys = owners.iter().map(keys::public_key_hex).collect::<Vec<_>>();
        let mut base = founded("bounded-store-cycle", &owners[0]);
        for pubkey in pubkeys.iter().skip(1) {
            let add = base
                .signed_set_member_in_stream(
                    &owners[0],
                    stream(1),
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
                    stream(index as u8 + 101),
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
            .map(|(entry, owner)| exact_head(entry, owner))
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

    #[test]
    fn serial_follower_can_author_its_exact_self_retirement() {
        let (authorization, commit_ref, commit, follower_registration) =
            serial_retirement_commit(MemberRole::Follower);

        authorization
            .authorize_and_apply(&commit_ref, &commit, &follower_registration)
            .expect("Follower exact self-retirement is authorized");
    }

    #[test]
    fn serial_device_state_activates_and_retires_an_exact_registration() {
        let store_id = "serial-registration";
        let owner = key();
        let follower = key();
        let root = test_root(store_id);
        let founder = test_founder_entry(store_id, &owner, "founder", membership_anchor(store_id));
        let founder_recovery = recovery_anchor(store_id);
        let (_owner_registration, owner_registration_ref) =
            registration(&root, "serial-registration-owner", &owner);
        let founder_state = ResolvedStoreDeviceState::founder(
            &root,
            owner_registration_ref.clone(),
            &founder.author_pubkey,
            founder.author_owner_grant.clone(),
            &founder_recovery,
        )
        .unwrap();
        let follower_registration = StoreDeviceRegistration::signed(
            root.clone(),
            StoreDeviceRegistrationOrigin::Join {
                attempt_id: DeviceJoinAttemptId::from_hash(ObjectHash::digest(
                    b"follower join attempt",
                )),
                attempt_slot: slot("test/serial-registration/join/attempt.json"),
                outcome_slot: slot("test/serial-registration/join/outcome.json"),
            },
            ProviderDeviceBinding {
                principal: ProviderPrincipalId::CustomS3Credential {
                    access_key_id_hash: ObjectHash::digest(b"follower access key"),
                },
            },
            StoreCommitAnchor::Serial,
            DeviceStreamAnchor::StoreAcknowledgements {
                first_slot: slot("test/serial-registration/follower/acks/1.json"),
            },
            DeviceStreamAnchor::StoreSnapshots {
                first_slot: slot("test/serial-registration/follower/snapshots/1.json"),
            },
            &follower,
        )
        .unwrap();
        let follower_bytes = follower_registration.to_bytes();
        let follower_ref = StoreDeviceRegistrationRef::from_registration(
            &follower_registration,
            exact(
                format!(
                    "{}.json",
                    registration_semantic_prefix(&follower_registration.device_id.to_string())
                ),
                &follower_bytes,
            ),
        );
        let active_state = founder_state
            .activate_registration(follower_ref.clone(), None)
            .expect("activate exact follower registration");
        let predecessor = StoreSerialPredecessor::Genesis {
            root: root.clone(),
            founder_registration: owner_registration_ref,
        };
        let order = StoreCommitOrder::Serial {
            seq: 1,
            predecessor: predecessor.clone(),
        };
        let write_id = crate::WriteId::from_generated("follower-retirement".to_string());
        let family =
            CandidateFamilyId::derive(root.store_root_hash, &follower_ref, &write_id, &order);
        let retirement = StoreDeviceSelfRetirement::signed(
            root.store_root_hash,
            family,
            follower_ref,
            StoreHistoryCut::Serial(predecessor),
            &follower_registration.device_signer(&follower).unwrap(),
        )
        .unwrap();
        let retirement_bytes = retirement.to_bytes();
        let retirement_ref = StoreDeviceSelfRetirementRef::from_retirement(
            &retirement,
            exact(
                format!(
                    "{}.json",
                    device_self_retirement_semantic_prefix(
                        family,
                        &follower_registration.device_id,
                        retirement.retirement_hash(),
                    )
                ),
                &retirement_bytes,
            ),
        );
        StoreDeviceSelfRetirement::parse_at(
            &retirement_bytes,
            &retirement_ref,
            &follower_registration,
        )
        .expect("verify exact self-retirement");
        let retired_state = active_state.self_retire(retirement_ref).unwrap();
        assert!(matches!(
            retired_state
                .devices
                .get(&follower_registration.device_id)
                .expect("follower device state")
                .status,
            crate::sync::store_commit::StoreDeviceStatus::Inactive { .. }
        ));
    }

    #[test]
    fn self_retirement_signature_cannot_retire_another_identity_registration() {
        let root = test_root("follower-registration-negatives");
        let follower = key();
        let outsider = key();
        let (follower_registration, follower_ref) =
            registration(&root, "negative-follower", &follower);
        let (outsider_registration, outsider_ref) =
            registration(&root, "negative-outsider", &outsider);
        let predecessor = StoreSerialPredecessor::Genesis {
            root: root.clone(),
            founder_registration: follower_ref,
        };
        let order = StoreCommitOrder::Serial {
            seq: 1,
            predecessor: predecessor.clone(),
        };
        let write_id = crate::WriteId::from_generated("foreign-retirement".to_string());
        let family =
            CandidateFamilyId::derive(root.store_root_hash, &outsider_ref, &write_id, &order);
        let retirement = StoreDeviceSelfRetirement::signed(
            root.store_root_hash,
            family,
            outsider_ref,
            StoreHistoryCut::Serial(predecessor),
            &follower_registration.device_signer(&follower).unwrap(),
        )
        .unwrap();
        let bytes = retirement.to_bytes();
        let reference = StoreDeviceSelfRetirementRef::from_retirement(
            &retirement,
            exact(
                format!(
                    "{}.json",
                    device_self_retirement_semantic_prefix(
                        family,
                        &outsider_registration.device_id,
                        retirement.retirement_hash(),
                    )
                ),
                &bytes,
            ),
        );
        assert!(matches!(
            StoreDeviceSelfRetirement::parse_at(&bytes, &reference, &outsider_registration),
            Err(crate::sync::store_commit::StoreProtocolError::InvalidSignature)
        ));
    }

    #[test]
    fn timestamp_does_not_change_causal_authorization() {
        let owner = key();
        let member = key();
        let mut chain = founded("store", &owner);
        let add = chain
            .signed_set_member_in_stream(
                &owner,
                stream(1),
                keys::public_key_hex(&member),
                None,
                MemberRole::Member,
                "9999".to_string(),
            )
            .unwrap();
        chain.add_entry(add).unwrap();
        let remove = chain
            .signed_remove_member_in_stream(
                &owner,
                stream(1),
                keys::public_key_hex(&member),
                "0000".to_string(),
            )
            .unwrap();
        chain.add_entry(remove).unwrap();
        assert!(!chain.can_write_now(&keys::public_key_hex(&member)));
    }

    #[test]
    fn signed_candidate_is_validated_before_it_is_returned() {
        let owner = key();
        let chain = founded("store", &owner);

        assert!(matches!(
            chain.signed_remove_member_in_stream(
                &owner,
                stream(1),
                keys::public_key_hex(&owner),
                "remove last owner".to_string(),
            ),
            Err(MembershipError::NoActiveOwner)
        ));
    }

    #[test]
    fn membership_candidates_require_exact_wrapped_key_recipient_coverage() {
        let owner = key();
        let member = key();
        let owner_pubkey = keys::public_key_hex(&owner);
        let member_pubkey = keys::public_key_hex(&member);
        let mut chain = founded("store", &owner);
        let wrong_recipient = test_wrapped_key_ref(
            &owner_pubkey,
            &owner_pubkey,
            crate::encryption::INITIAL_KEY_GENERATION,
            b"wrong invitation recipient",
        );
        assert!(matches!(
            chain.signed_set_member_with_anchor_and_wrapped_key_in_stream(
                &owner,
                stream(1),
                member_pubkey.clone(),
                None,
                MemberRole::Member,
                None,
                wrong_recipient,
                "invalid invitation".to_string(),
            ),
            Err(MembershipError::InvalidWrappedKeys(_))
        ));

        let add = chain
            .signed_set_member_in_stream(
                &owner,
                stream(1),
                member_pubkey.clone(),
                None,
                MemberRole::Member,
                "add member".to_string(),
            )
            .unwrap();
        chain.add_entry(add).unwrap();
        assert!(matches!(
            chain.signed_remove_member_with_wrapped_keys_in_stream(
                &owner,
                stream(1),
                member_pubkey,
                Vec::new(),
                "missing owner wrap".to_string(),
            ),
            Err(MembershipError::InvalidWrappedKeys(_))
        ));
    }

    #[test]
    fn wrapped_key_generations_follow_the_causal_membership_history() {
        let owner = key();
        let first_member = key();
        let second_member = key();
        let later_member = key();
        let owner_pubkey = keys::public_key_hex(&owner);
        let first_pubkey = keys::public_key_hex(&first_member);
        let second_pubkey = keys::public_key_hex(&second_member);
        let later_pubkey = keys::public_key_hex(&later_member);
        let mut chain = founded("wrapped-generation-history", &owner);
        for member in [&first_pubkey, &second_pubkey] {
            let add = chain
                .signed_set_member_in_stream(
                    &owner,
                    stream(1),
                    member.clone(),
                    None,
                    MemberRole::Member,
                    format!("add {member}"),
                )
                .unwrap();
            chain.add_entry(add).unwrap();
        }
        let mut first_rotation_wraps = vec![
            test_wrapped_key_ref(&owner_pubkey, &owner_pubkey, 2, b"first owner rotation"),
            test_wrapped_key_ref(&owner_pubkey, &second_pubkey, 2, b"first member rotation"),
        ];
        first_rotation_wraps.sort();
        let first_rotation = chain
            .signed_remove_member_with_wrapped_keys_in_stream(
                &owner,
                stream(1),
                first_pubkey,
                first_rotation_wraps,
                "first rotation".to_string(),
            )
            .unwrap();
        chain.add_entry(first_rotation).unwrap();

        assert!(matches!(
            chain.signed_set_member_with_anchor_and_wrapped_key_in_stream(
                &owner,
                stream(1),
                later_pubkey.clone(),
                None,
                MemberRole::Member,
                None,
                test_wrapped_key_ref(&owner_pubkey, &later_pubkey, 1, b"stale later invitation",),
                "stale later invitation".to_string(),
            ),
            Err(MembershipError::InvalidWrappedKeys(_))
        ));
        assert!(matches!(
            chain.signed_remove_member_with_wrapped_keys_in_stream(
                &owner,
                stream(1),
                second_pubkey,
                vec![test_wrapped_key_ref(
                    &owner_pubkey,
                    &owner_pubkey,
                    2,
                    b"reused rotation generation",
                )],
                "reused rotation generation".to_string(),
            ),
            Err(MembershipError::InvalidWrappedKeys(_))
        ));
    }

    #[test]
    fn concurrent_add_and_rotation_has_incomplete_wrapped_key_authority() {
        let owner = key();
        let removed = key();
        let concurrent_member = key();
        let owner_pubkey = keys::public_key_hex(&owner);
        let removed_pubkey = keys::public_key_hex(&removed);
        let concurrent_pubkey = keys::public_key_hex(&concurrent_member);
        let mut chain = founded("concurrent-add-rotation", &owner);
        let add_removed = chain
            .signed_set_member_in_stream(
                &owner,
                stream(1),
                removed_pubkey.clone(),
                None,
                MemberRole::Member,
                "add member that will be removed".to_string(),
            )
            .unwrap();
        chain.add_entry(add_removed).unwrap();

        let add_concurrent = chain
            .signed_set_member_in_stream(
                &owner,
                stream(2),
                concurrent_pubkey.clone(),
                None,
                MemberRole::Member,
                "concurrent add".to_string(),
            )
            .unwrap();
        let owner_rotation = test_wrapped_key_ref(
            &owner_pubkey,
            &owner_pubkey,
            2,
            b"rotation missing concurrent member",
        );
        let remove = chain
            .signed_remove_member_with_wrapped_keys_in_stream(
                &owner,
                stream(3),
                removed_pubkey,
                vec![owner_rotation],
                "concurrent removal".to_string(),
            )
            .unwrap();
        chain.add_entry(add_concurrent).unwrap();
        chain.add_entry(remove).unwrap();

        assert!(matches!(
            chain.wrapped_key_authority_for(&concurrent_pubkey),
            Err(MembershipError::MissingWrappedKeyCoverage { .. })
        ));

        let replacement_wrap = test_wrapped_key_ref(
            &owner_pubkey,
            &concurrent_pubkey,
            2,
            b"post-rotation replacement invitation",
        );
        let replacement = chain
            .signed_set_member_with_anchor_and_wrapped_key_in_stream(
                &owner,
                stream(4),
                concurrent_pubkey.clone(),
                None,
                MemberRole::Member,
                None,
                replacement_wrap.clone(),
                "replace concurrent invitation after rotation".to_string(),
            )
            .unwrap();
        chain.add_entry(replacement).unwrap();
        assert_eq!(
            chain.wrapped_key_authority_for(&concurrent_pubkey).unwrap(),
            vec![replacement_wrap],
        );
    }

    #[test]
    fn concurrent_member_assignments_are_validated_conflict_state() {
        let owner = key();
        let target = key();
        let chain = founded("store", &owner);
        let first = chain
            .signed_set_member_in_stream(
                &owner,
                stream(21),
                keys::public_key_hex(&target),
                None,
                MemberRole::Member,
                "first".to_string(),
            )
            .unwrap();
        let second = chain
            .signed_set_member_in_stream(
                &owner,
                stream(22),
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
            .map(|entry| exact_head(entry, &owner))
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
            .signed_set_member_in_stream(
                &first_owner,
                stream(1),
                second_pubkey.clone(),
                None,
                MemberRole::Owner,
                "add second".to_string(),
            )
            .unwrap();
        base.add_entry(add_second).unwrap();
        let remove_second = base
            .signed_remove_member_in_stream(
                &first_owner,
                stream(1),
                second_pubkey.clone(),
                "remove second".to_string(),
            )
            .unwrap();
        let remove_first = base
            .signed_remove_member_in_stream(
                &second_owner,
                stream(23),
                first_pubkey.clone(),
                "remove first".to_string(),
            )
            .unwrap();
        let mut entries = base.entries().to_vec();
        entries.extend([remove_second.clone(), remove_first.clone()]);
        let heads = vec![
            exact_head(
                base.entries().first().expect("founder membership entry"),
                &first_owner,
            ),
            exact_head(&remove_second, &first_owner),
            exact_head(&remove_first, &second_owner),
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
        let resolution_value = conflicted
            .signed_cycle_resolution(
                store_root_hash,
                resolver_branch.clone(),
                membership_anchor("first-cycle-resolution"),
                &first_owner,
            )
            .expect("branch Owner resolves the conflict");
        let second_resolution_value = conflicted
            .signed_cycle_resolution(
                store_root_hash,
                second_resolver_branch,
                membership_anchor("second-cycle-resolution"),
                &second_owner,
            )
            .expect("other branch Owner resolves the conflict");
        let retried = conflicted
            .signed_cycle_resolution(
                store_root_hash,
                resolver_branch,
                membership_anchor("first-cycle-resolution"),
                &first_owner,
            )
            .expect("same resolver retry");
        assert_eq!(resolution_value, retried);
        assert!(resolution_value.verify_against(
            store_root_hash,
            conflicted.conflict().expect("cycle conflict"),
        ));
        let resolution = exact_resolution(resolution_value);
        let second_resolution = exact_resolution(second_resolution_value);
        let resolved_once = conflicted
            .resolved_with(store_root_hash, std::slice::from_ref(&resolution))
            .expect("one resolution applies");
        let resolved_duplicate = conflicted
            .resolved_with(store_root_hash, &[resolution.clone(), resolution.clone()])
            .expect("an exact retry is idempotent");
        assert_eq!(resolved_once, resolved_duplicate);
        assert!(resolved_once
            .active_grants
            .contains_key(&resolution.1.replacement_grant));
        assert!(resolution
            .1
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
            .contains_key(&resolution.1.replacement_grant));
        assert!(resolved_union
            .active_grants
            .contains_key(&second_resolution.1.replacement_grant));

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
        let duplicate_creation = resolution.1.conflicting_heads[0].coord.clone();
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
        assert_eq!(
            resumed.resolution_refs(),
            std::slice::from_ref(&resolution.0)
        );
        let after_resolution = resumed
            .signed_set_member_in_stream(
                &first_owner,
                stream(37),
                keys::public_key_hex(&key()),
                None,
                MemberRole::Member,
                "write after resolution".to_string(),
            )
            .expect("replacement Owner can author from a fresh stream");
        assert_eq!(
            after_resolution.author_owner_grant,
            resolution.1.replacement_grant
        );
        let activated_head = exact_head(&after_resolution, &first_owner).1;
        resumed
            .add_entry(after_resolution)
            .expect("future authoring validates from the resolved checkpoint");
        assert_eq!(activated_head.resolutions, vec![resolution.0.clone()]);
        let authority = MembershipGrantCreationAuthority::ConflictResolution(resolution.0.clone());
        assert!(resumed.authorizes_write_authority(&authority, &first_pubkey));
        assert!(matches!(
            conflicted.signed_cycle_resolution(
                store_root_hash,
                resolution.1.resolver_branch_heads.clone(),
                membership_anchor("non-owner-cycle-resolution"),
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
            .signed_set_member_in_stream(
                &founder,
                stream(1),
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
                stream(31),
                keys::public_key_hex(&key()),
                None,
                MemberRole::Member,
                "second stream".to_string(),
            )
            .unwrap();
        chain.add_entry(second_stream).unwrap();
        let mut unsorted = chain
            .signed_set_member_in_stream(
                &founder,
                stream(1),
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
            .signed_set_member_in_stream(
                &founder,
                stream(1),
                second_owner_pubkey.clone(),
                None,
                MemberRole::Owner,
                "add owner".to_string(),
            )
            .unwrap();
        chain.add_entry(add_owner).unwrap();
        for (stream_id, timestamp) in [(stream(41), "first stream"), (stream(42), "second stream")]
        {
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
            .signed_remove_member_in_stream(
                &founder,
                stream(1),
                second_owner_pubkey,
                "remove owner".to_string(),
            )
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
            .signed_set_member_in_stream(
                &owner,
                stream(1),
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
            .signed_remove_member_in_stream(
                &owner,
                stream(1),
                keys::public_key_hex(&second),
                "remove".to_string(),
            )
            .unwrap();
        chain.add_entry(remove).unwrap();
        let readd = chain
            .signed_set_member_in_stream(
                &owner,
                stream(1),
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
                stream(32),
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
            .signed_set_member_in_stream(
                &founder,
                stream(1),
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
                stream(33),
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
            .signed_set_member_in_stream(
                &founder,
                stream(1),
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
                stream(34),
                keys::public_key_hex(&target),
                None,
                MemberRole::Member,
                "stale entry".to_string(),
            )
            .unwrap();
        let removal = observed
            .signed_remove_member_in_stream(
                &founder,
                stream(1),
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
            .signed_set_member_in_stream(
                &founder,
                stream(1),
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
                stream(35),
                keys::public_key_hex(&first_target),
                None,
                MemberRole::Member,
                "first".to_string(),
            )
            .unwrap();
        observed.add_entry(first.clone()).unwrap();

        let removal = observed
            .signed_remove_member_in_stream(
                &founder,
                stream(1),
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
                stream(35),
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
                stream(35),
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
            .signed_set_member_in_stream(
                &founder,
                stream(1),
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
                stream(36),
                keys::public_key_hex(&key()),
                None,
                MemberRole::Member,
                "authored".to_string(),
            )
            .unwrap();
        chain.add_entry(authored).unwrap();
        let mut removal = chain
            .signed_remove_member_in_stream(
                &founder,
                stream(1),
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
        let from_a = test_founder_entry("store-a", &owner, "founder", membership_anchor("store-a"));
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
        let entry = test_founder_entry("store", &owner, "display-time", membership_anchor("store"));
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
            &test_founder_entry(
                "serial-store",
                &owner,
                "founder",
                membership_anchor("serial-store"),
            ),
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
            &test_founder_entry(
                "serial-grant-store",
                &owner,
                "founder",
                membership_anchor("serial-grant-store"),
            ),
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
        let entry = test_founder_entry(
            "head-tip-resolution-cut",
            &owner,
            "founder",
            membership_anchor("head-tip-resolution-cut"),
        );
        let fake = StoreMembershipConflictResolutionRef {
            conflict_hash: ObjectHash::digest(b"head-tip conflict"),
            resolver_pubkey: keys::public_key_hex(&owner),
            resolution_hash: ObjectHash::digest(b"head-tip resolution"),
            object: exact(
                "test/head-tip-resolution-cut/resolution.json",
                b"head-tip resolution",
            ),
        };
        let head = exact_head_with_resolutions(&entry, &owner, vec![fake]);

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
        let founder = test_founder_entry(
            "entry-resolution-cut",
            &owner,
            "founder",
            membership_anchor("entry-resolution-cut"),
        );
        let chain = MembershipChain::from_entries(vec![founder]).unwrap();
        let entry = chain
            .signed_set_member_in_stream(
                &owner,
                stream(1),
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
                object: exact(
                    format!(
                        "test/entry-resolution-cut/{}.json",
                        String::from_utf8_lossy(label)
                    ),
                    label,
                ),
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

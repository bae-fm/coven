/// Membership chain: an append-only log of membership changes for shared libraries.
///
/// The chain is stored as encrypted files in sync storage and reconstructed
/// on each sync. It is not stored in the DB.
///
/// Layout in storage:
/// ```text
/// membership/{author_pubkey_hex}/{seq}.enc
/// ```
///
/// Each entry records an Add or Remove action, signed by a current owner.
/// The first entry must be a self-signed Add with role Owner.
use serde::{Deserialize, Serialize};

use crate::keys::{self, UserKeypair};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum MembershipAction {
    Add,
    Remove,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum MemberRole {
    Owner,
    Member,
    /// Read-only member: holds the library key and is registered in the chain,
    /// but may not author catalog changesets (rejected on pull) and is gated to
    /// reads at the proxy. Revocable like any member.
    Follower,
}

impl MemberRole {
    /// Stable lowercase wire string. Written into `auth/keys/{pubkey}` so the
    /// proxy can role-gate writes without decrypting the membership chain.
    pub fn as_str(&self) -> &'static str {
        match self {
            MemberRole::Owner => "owner",
            MemberRole::Member => "member",
            MemberRole::Follower => "follower",
        }
    }

    /// Whether this role may author catalog changesets (write). Followers can't.
    pub fn can_write(&self) -> bool {
        matches!(self, MemberRole::Owner | MemberRole::Member)
    }
}

/// A single membership entry in the chain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MembershipEntry {
    pub action: MembershipAction,
    pub user_pubkey: String,
    pub role: MemberRole,
    pub timestamp: String,
    pub author_pubkey: String,
    pub signature: String,
}

#[derive(Debug, thiserror::Error)]
pub enum MembershipError {
    #[error("first entry must be a self-signed owner Add")]
    InvalidFirstEntry,
    #[error("entry at index {0} has an invalid signature")]
    InvalidSignature(usize),
    #[error("entry at index {0}: author is not an owner at that point in the chain")]
    NotAnOwner(usize),
    #[error("chain is empty")]
    EmptyChain,
}

/// Deterministic serialization of the signed fields (everything except signature).
pub fn canonical_bytes(entry: &MembershipEntry) -> Vec<u8> {
    // Use serde_json::json! with explicit field ordering for determinism.
    // JSON object keys from json! macro are sorted alphabetically by serde_json.
    let canonical = serde_json::json!({
        "action": entry.action,
        "author_pubkey": entry.author_pubkey,
        "role": entry.role,
        "timestamp": entry.timestamp,
        "user_pubkey": entry.user_pubkey,
    });
    serde_json::to_vec(&canonical).expect("canonical serialization cannot fail")
}

/// Build the founder entry of a library's membership chain: a self-signed `Add`
/// of `owner` with the `Owner` role. Every library is founded by exactly one such
/// entry at creation, and the chain is later anchored to `owner`'s pubkey so no
/// one can wipe `membership/*` and refound themselves as Owner (issue #95).
pub fn founder_entry(owner: &UserKeypair, timestamp: &str) -> MembershipEntry {
    let pk_hex = hex::encode(owner.public_key);
    let mut entry = MembershipEntry {
        action: MembershipAction::Add,
        user_pubkey: pk_hex.clone(),
        role: MemberRole::Owner,
        timestamp: timestamp.to_string(),
        author_pubkey: pk_hex,
        signature: String::new(),
    };
    sign_membership_entry(&mut entry, owner);
    entry
}

/// Sign a membership entry with the given keypair.
///
/// Sets `author_pubkey` and `signature` on the entry.
pub fn sign_membership_entry(entry: &mut MembershipEntry, keypair: &UserKeypair) {
    entry.author_pubkey = hex::encode(keypair.public_key);
    let bytes = canonical_bytes(entry);
    let sig = keypair.sign(&bytes);
    entry.signature = hex::encode(sig);
}

/// Verify the signature on a membership entry.
pub fn verify_membership_entry(entry: &MembershipEntry) -> bool {
    let Ok(pk_bytes) = hex::decode(&entry.author_pubkey) else {
        return false;
    };
    let Ok(sig_bytes) = hex::decode(&entry.signature) else {
        return false;
    };

    let Ok(pk): Result<[u8; keys::SIGN_PUBLICKEYBYTES], _> = pk_bytes.try_into() else {
        return false;
    };
    let Ok(sig): Result<[u8; keys::SIGN_BYTES], _> = sig_bytes.try_into() else {
        return false;
    };

    let bytes = canonical_bytes(entry);
    keys::verify_signature(&sig, &bytes, &pk)
}

/// An append-only membership chain.
///
/// Entries are sorted by timestamp (HLC string comparison gives causal order).
#[derive(Debug, Clone, Default)]
pub struct MembershipChain {
    entries: Vec<MembershipEntry>,
}

impl MembershipChain {
    /// Create an empty chain.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a chain from existing entries (e.g., downloaded from storage).
    /// Entries are sorted by timestamp and validated on construction.
    pub fn from_entries(mut entries: Vec<MembershipEntry>) -> Result<Self, MembershipError> {
        entries.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
        let chain = Self { entries };
        chain.validate()?;
        Ok(chain)
    }

    /// Return the entries in the chain.
    pub fn entries(&self) -> &[MembershipEntry] {
        &self.entries
    }

    /// The pubkey of the founder (chain entry #1, the self-signed Owner Add), or
    /// `None` for an empty chain. The library is anchored to this pubkey: a chain
    /// whose founder is not the library's established owner is a takeover attempt
    /// (issue #95), so callers compare this against the pinned owner pubkey.
    pub fn founder_pubkey(&self) -> Option<&str> {
        self.entries.first().map(|e| e.user_pubkey.as_str())
    }

    /// Whether this chain is founded by `owner_pubkey` — its entry #1 is an Owner
    /// Add of exactly that pubkey. The anchoring check that prevents a wiped
    /// `membership/*` from being refounded under an attacker's key.
    pub fn is_founded_by(&self, owner_pubkey: &str) -> bool {
        self.founder_pubkey() == Some(owner_pubkey)
    }

    /// Validate the entire chain.
    ///
    /// Rules:
    /// 1. First entry must be Add with role Owner, self-signed.
    /// 2. Every entry must have a valid signature.
    /// 3. Every entry's author must be a current Owner at that point.
    pub fn validate(&self) -> Result<(), MembershipError> {
        if self.entries.is_empty() {
            return Err(MembershipError::EmptyChain);
        }

        let first = &self.entries[0];
        if first.action != MembershipAction::Add
            || first.role != MemberRole::Owner
            || first.author_pubkey != first.user_pubkey
        {
            return Err(MembershipError::InvalidFirstEntry);
        }

        if !verify_membership_entry(first) {
            return Err(MembershipError::InvalidSignature(0));
        }

        // Track active members as we walk the chain.
        let mut active: Vec<(String, MemberRole)> = vec![];
        active.push((first.user_pubkey.clone(), first.role.clone()));

        for (i, entry) in self.entries.iter().enumerate().skip(1) {
            if !verify_membership_entry(entry) {
                return Err(MembershipError::InvalidSignature(i));
            }

            // Author must be an active owner.
            let is_owner = active
                .iter()
                .any(|(pk, role)| pk == &entry.author_pubkey && *role == MemberRole::Owner);

            if !is_owner {
                return Err(MembershipError::NotAnOwner(i));
            }

            match entry.action {
                MembershipAction::Add => {
                    // Remove any existing entry for this pubkey (role change).
                    active.retain(|(pk, _)| pk != &entry.user_pubkey);
                    active.push((entry.user_pubkey.clone(), entry.role.clone()));
                }
                MembershipAction::Remove => {
                    active.retain(|(pk, _)| pk != &entry.user_pubkey);
                }
            }
        }

        Ok(())
    }

    /// Whether a pubkey is *currently* authorized to author catalog changesets
    /// — a current Owner or Member. Followers are read-only, so a changeset
    /// they authored is rejected on pull.
    ///
    /// Non-temporal by design: pull asks this of the latest chain it has, not
    /// of an author-supplied (spoofable) timestamp. Revocation is enforced by
    /// the key rotation `remove_member` performs, not by replaying the chain at
    /// a claimed instant.
    pub fn can_write_now(&self, pubkey: &str) -> bool {
        self.current_members()
            .into_iter()
            .any(|(pk, role)| pk == pubkey && role.can_write())
    }

    /// Return current active members with their roles.
    pub fn current_members(&self) -> Vec<(String, MemberRole)> {
        let mut active: Vec<(String, MemberRole)> = Vec::new();

        for entry in &self.entries {
            match entry.action {
                MembershipAction::Add => {
                    active.retain(|(pk, _)| pk != &entry.user_pubkey);
                    active.push((entry.user_pubkey.clone(), entry.role.clone()));
                }
                MembershipAction::Remove => {
                    active.retain(|(pk, _)| pk != &entry.user_pubkey);
                }
            }
        }

        active
    }

    /// Validate and append an entry to the chain.
    pub fn add_entry(&mut self, entry: MembershipEntry) -> Result<(), MembershipError> {
        if self.entries.is_empty() {
            // First entry: must be self-signed owner Add.
            if entry.action != MembershipAction::Add
                || entry.role != MemberRole::Owner
                || entry.author_pubkey != entry.user_pubkey
            {
                return Err(MembershipError::InvalidFirstEntry);
            }

            if !verify_membership_entry(&entry) {
                return Err(MembershipError::InvalidSignature(0));
            }

            self.entries.push(entry);
            return Ok(());
        }

        if !verify_membership_entry(&entry) {
            return Err(MembershipError::InvalidSignature(self.entries.len()));
        }

        // Author must be an active owner.
        let members = self.current_members();
        let is_owner = members
            .iter()
            .any(|(pk, role)| pk == &entry.author_pubkey && *role == MemberRole::Owner);

        if !is_owner {
            return Err(MembershipError::NotAnOwner(self.entries.len()));
        }

        self.entries.push(entry);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keys::UserKeypair;
    use crate::sync::test_helpers::{founder_entry, make_entry, pubkey_hex};

    fn gen_keypair() -> UserKeypair {
        UserKeypair::generate()
    }

    #[test]
    fn single_owner_chain() {
        let owner = gen_keypair();
        let entry = founder_entry(&owner, "0000000001000-0000-dev1");

        let mut chain = MembershipChain::new();
        chain.add_entry(entry).unwrap();
        chain.validate().unwrap();

        let members = chain.current_members();
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].0, pubkey_hex(&owner));
        assert_eq!(members[0].1, MemberRole::Owner);
    }

    #[test]
    fn founder_pubkey_identifies_the_anchor() {
        let owner = gen_keypair();
        let member = gen_keypair();
        let owner_pk = pubkey_hex(&owner);
        let chain = MembershipChain::from_entries(vec![
            founder_entry(&owner, "0000000001000-0000-dev1"),
            make_entry(
                &owner,
                MembershipAction::Add,
                &member,
                MemberRole::Member,
                "0000000002000-0000-dev1",
            ),
        ])
        .unwrap();

        // The founder is entry #1's pubkey regardless of later members.
        assert_eq!(chain.founder_pubkey(), Some(owner_pk.as_str()));
        assert!(chain.is_founded_by(&owner_pk));
        assert!(!chain.is_founded_by(&pubkey_hex(&member)));
        assert!(!chain.is_founded_by("deadbeef"));

        // An empty chain has no founder to anchor to.
        assert_eq!(MembershipChain::new().founder_pubkey(), None);
        assert!(!MembershipChain::new().is_founded_by(&owner_pk));
    }

    #[test]
    fn follower_role_is_read_only_and_revocable() {
        let owner = gen_keypair();
        let follower = gen_keypair();
        let member = gen_keypair();

        let mut chain = MembershipChain::new();
        chain
            .add_entry(founder_entry(&owner, "0000000001000-0000-dev1"))
            .unwrap();
        chain
            .add_entry(make_entry(
                &owner,
                MembershipAction::Add,
                &follower,
                MemberRole::Follower,
                "0000000002000-0000-dev1",
            ))
            .unwrap();
        chain
            .add_entry(make_entry(
                &owner,
                MembershipAction::Add,
                &member,
                MemberRole::Member,
                "0000000003000-0000-dev1",
            ))
            .unwrap();
        chain.validate().unwrap();

        // Owners and Members may author changesets; a Follower may not.
        assert!(chain.can_write_now(&pubkey_hex(&owner)));
        assert!(chain.can_write_now(&pubkey_hex(&member)));
        assert!(!chain.can_write_now(&pubkey_hex(&follower)));
        // ...but the Follower is still a registered member (can read).
        assert!(chain
            .current_members()
            .iter()
            .any(|(pk, _)| pk == &pubkey_hex(&follower)));

        // Revoking the follower drops them entirely: not a current member,
        // cannot write.
        chain
            .add_entry(make_entry(
                &owner,
                MembershipAction::Remove,
                &follower,
                MemberRole::Follower,
                "0000000004000-0000-dev1",
            ))
            .unwrap();
        assert!(!chain
            .current_members()
            .iter()
            .any(|(pk, _)| pk == &pubkey_hex(&follower)));
        assert!(!chain.can_write_now(&pubkey_hex(&follower)));
    }

    #[test]
    fn role_change_member_to_follower_revokes_write() {
        let owner = gen_keypair();
        let m = gen_keypair();

        let mut chain = MembershipChain::new();
        chain
            .add_entry(founder_entry(&owner, "0000000001000-0000-dev1"))
            .unwrap();
        chain
            .add_entry(make_entry(
                &owner,
                MembershipAction::Add,
                &m,
                MemberRole::Member,
                "0000000002000-0000-dev1",
            ))
            .unwrap();
        // While a Member, they may author changesets.
        assert!(chain.can_write_now(&pubkey_hex(&m)));

        // Re-add with a lower role downgrades them; write is revoked.
        chain
            .add_entry(make_entry(
                &owner,
                MembershipAction::Add,
                &m,
                MemberRole::Follower,
                "0000000003000-0000-dev1",
            ))
            .unwrap();
        assert!(!chain.can_write_now(&pubkey_hex(&m)));
    }

    #[test]
    fn add_member_signed_by_owner() {
        let owner = gen_keypair();
        let member = gen_keypair();

        let mut chain = MembershipChain::new();
        chain
            .add_entry(founder_entry(&owner, "0000000001000-0000-dev1"))
            .unwrap();
        chain
            .add_entry(make_entry(
                &owner,
                MembershipAction::Add,
                &member,
                MemberRole::Member,
                "0000000002000-0000-dev1",
            ))
            .unwrap();

        chain.validate().unwrap();

        let members = chain.current_members();
        assert_eq!(members.len(), 2);
        assert!(members
            .iter()
            .any(|(pk, r)| pk == &pubkey_hex(&owner) && *r == MemberRole::Owner));
        assert!(members
            .iter()
            .any(|(pk, r)| pk == &pubkey_hex(&member) && *r == MemberRole::Member));
    }

    #[test]
    fn remove_member_signed_by_owner() {
        let owner = gen_keypair();
        let member = gen_keypair();

        let mut chain = MembershipChain::new();
        chain
            .add_entry(founder_entry(&owner, "0000000001000-0000-dev1"))
            .unwrap();
        chain
            .add_entry(make_entry(
                &owner,
                MembershipAction::Add,
                &member,
                MemberRole::Member,
                "0000000002000-0000-dev1",
            ))
            .unwrap();
        chain
            .add_entry(make_entry(
                &owner,
                MembershipAction::Remove,
                &member,
                MemberRole::Member,
                "0000000003000-0000-dev1",
            ))
            .unwrap();

        chain.validate().unwrap();

        let members = chain.current_members();
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].0, pubkey_hex(&owner));
    }

    #[test]
    fn current_members_returns_active() {
        let owner = gen_keypair();
        let m1 = gen_keypair();
        let m2 = gen_keypair();

        let chain = MembershipChain::from_entries(vec![
            founder_entry(&owner, "0000000001000-0000-dev1"),
            make_entry(
                &owner,
                MembershipAction::Add,
                &m1,
                MemberRole::Member,
                "0000000002000-0000-dev1",
            ),
            make_entry(
                &owner,
                MembershipAction::Add,
                &m2,
                MemberRole::Member,
                "0000000003000-0000-dev1",
            ),
            make_entry(
                &owner,
                MembershipAction::Remove,
                &m1,
                MemberRole::Member,
                "0000000004000-0000-dev1",
            ),
        ])
        .unwrap();

        let members = chain.current_members();
        assert_eq!(members.len(), 2);
        assert!(members.iter().any(|(pk, _)| pk == &pubkey_hex(&owner)));
        assert!(members.iter().any(|(pk, _)| pk == &pubkey_hex(&m2)));
        assert!(!members.iter().any(|(pk, _)| pk == &pubkey_hex(&m1)));
    }

    #[test]
    fn add_signed_by_non_owner_fails() {
        let owner = gen_keypair();
        let member = gen_keypair();
        let outsider = gen_keypair();

        let mut chain = MembershipChain::new();
        chain
            .add_entry(founder_entry(&owner, "0000000001000-0000-dev1"))
            .unwrap();
        chain
            .add_entry(make_entry(
                &owner,
                MembershipAction::Add,
                &member,
                MemberRole::Member,
                "0000000002000-0000-dev1",
            ))
            .unwrap();

        // Member (not owner) tries to add someone.
        let result = chain.add_entry(make_entry(
            &member,
            MembershipAction::Add,
            &outsider,
            MemberRole::Member,
            "0000000003000-0000-dev1",
        ));

        assert!(matches!(result, Err(MembershipError::NotAnOwner(_))));
    }

    #[test]
    fn remove_signed_by_non_owner_fails() {
        let owner = gen_keypair();
        let m1 = gen_keypair();
        let m2 = gen_keypair();

        let mut chain = MembershipChain::new();
        chain
            .add_entry(founder_entry(&owner, "0000000001000-0000-dev1"))
            .unwrap();
        chain
            .add_entry(make_entry(
                &owner,
                MembershipAction::Add,
                &m1,
                MemberRole::Member,
                "0000000002000-0000-dev1",
            ))
            .unwrap();
        chain
            .add_entry(make_entry(
                &owner,
                MembershipAction::Add,
                &m2,
                MemberRole::Member,
                "0000000003000-0000-dev1",
            ))
            .unwrap();

        // m1 (Member, not Owner) tries to remove m2.
        let result = chain.add_entry(make_entry(
            &m1,
            MembershipAction::Remove,
            &m2,
            MemberRole::Member,
            "0000000004000-0000-dev1",
        ));

        assert!(matches!(result, Err(MembershipError::NotAnOwner(_))));
    }

    #[test]
    fn first_entry_not_self_signed_owner_add_fails() {
        let kp1 = gen_keypair();
        let kp2 = gen_keypair();

        // First entry signed by kp1 but adding kp2 as owner (not self-signed).
        let entry = make_entry(
            &kp1,
            MembershipAction::Add,
            &kp2,
            MemberRole::Owner,
            "0000000001000-0000-dev1",
        );

        let mut chain = MembershipChain::new();
        let result = chain.add_entry(entry);
        assert!(matches!(result, Err(MembershipError::InvalidFirstEntry)));
    }

    #[test]
    fn first_entry_as_member_fails() {
        let kp = gen_keypair();
        let pk_hex = pubkey_hex(&kp);

        // Self-signed but role is Member, not Owner.
        let mut entry = MembershipEntry {
            action: MembershipAction::Add,
            user_pubkey: pk_hex.clone(),
            role: MemberRole::Member,
            timestamp: "0000000001000-0000-dev1".to_string(),
            author_pubkey: pk_hex,
            signature: String::new(),
        };
        sign_membership_entry(&mut entry, &kp);

        let mut chain = MembershipChain::new();
        let result = chain.add_entry(entry);
        assert!(matches!(result, Err(MembershipError::InvalidFirstEntry)));
    }

    #[test]
    fn tampered_entry_fails_signature_verification() {
        let owner = gen_keypair();
        let member = gen_keypair();

        let mut entry = make_entry(
            &owner,
            MembershipAction::Add,
            &member,
            MemberRole::Member,
            "0000000002000-0000-dev1",
        );

        // Tamper with the role after signing.
        entry.role = MemberRole::Owner;

        assert!(!verify_membership_entry(&entry));

        // Also fails when added to a chain.
        let mut chain = MembershipChain::new();
        chain
            .add_entry(founder_entry(&owner, "0000000001000-0000-dev1"))
            .unwrap();

        let result = chain.add_entry(entry);
        assert!(matches!(result, Err(MembershipError::InvalidSignature(_))));
    }

    #[test]
    fn entry_ordering_by_timestamp() {
        let owner = gen_keypair();
        let m1 = gen_keypair();
        let m2 = gen_keypair();

        // Create entries out of order.
        let e3 = make_entry(
            &owner,
            MembershipAction::Add,
            &m2,
            MemberRole::Member,
            "0000000003000-0000-dev1",
        );
        let e1 = founder_entry(&owner, "0000000001000-0000-dev1");
        let e2 = make_entry(
            &owner,
            MembershipAction::Add,
            &m1,
            MemberRole::Member,
            "0000000002000-0000-dev1",
        );

        // from_entries should sort and validate them.
        let chain = MembershipChain::from_entries(vec![e3, e1, e2]).unwrap();

        // Verify they're sorted.
        let entries = chain.entries();
        assert!(entries[0].timestamp < entries[1].timestamp);
        assert!(entries[1].timestamp < entries[2].timestamp);
    }

    #[test]
    fn validate_empty_chain_fails() {
        let chain = MembershipChain::new();
        let result = chain.validate();
        assert!(matches!(result, Err(MembershipError::EmptyChain)));
    }

    #[test]
    fn validate_detects_invalid_signature_in_middle() {
        let owner = gen_keypair();
        let member = gen_keypair();

        let e1 = founder_entry(&owner, "0000000001000-0000-dev1");
        let mut e2 = make_entry(
            &owner,
            MembershipAction::Add,
            &member,
            MemberRole::Member,
            "0000000002000-0000-dev1",
        );

        // Tamper with e2's timestamp after signing.
        e2.timestamp = "0000000002500-0000-dev1".to_string();

        let result = MembershipChain::from_entries(vec![e1, e2]);
        assert!(matches!(result, Err(MembershipError::InvalidSignature(1))));
    }

    #[test]
    fn canonical_bytes_is_deterministic() {
        let kp = gen_keypair();
        let entry = MembershipEntry {
            action: MembershipAction::Add,
            user_pubkey: pubkey_hex(&kp),
            role: MemberRole::Owner,
            timestamp: "0000000001000-0000-dev1".to_string(),
            author_pubkey: pubkey_hex(&kp),
            signature: "does-not-matter".to_string(),
        };

        let b1 = canonical_bytes(&entry);
        let b2 = canonical_bytes(&entry);
        assert_eq!(b1, b2);

        // Signature is not included in canonical bytes.
        let mut entry2 = entry.clone();
        entry2.signature = "something-else".to_string();
        assert_eq!(canonical_bytes(&entry2), b1);
    }
}

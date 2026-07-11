/// Membership chain: an append-only log of membership changes for shared stores.
///
/// The chain is stored as encrypted files in sync storage and reconstructed
/// on each sync. It is not stored in the DB.
///
/// Layout in storage:
/// ```text
/// membership/{author_pubkey_hex}/{seq}.enc   -- one Add/Remove entry
/// membership/{author_pubkey_hex}/head.enc    -- that author's signed head
/// ```
///
/// Each entry records an Add or Remove action, signed by a current owner.
/// The first entry must be a self-signed Add with role Owner. Each owner also
/// publishes a [`AuthorHead`] certifying how far its own entry prefix is
/// committed; the committed membership set is the union of every current owner's
/// committed prefix, so there is no shared last-writer-wins head and concurrent
/// owners commit without racing each other.
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

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
    /// Read-only member: holds the store key and is registered in the chain,
    /// but may not author catalog changesets. The restriction is enforced
    /// acceptance-side: a puller re-derives each author's role from the chain and
    /// rejects a Follower's changesets — there is no proxy. Revocable like any
    /// member.
    Follower,
}

impl MemberRole {
    /// Whether this role may author catalog changesets (write). Followers can't.
    pub fn can_write(&self) -> bool {
        matches!(self, MemberRole::Owner | MemberRole::Member)
    }
}

/// A current member of the shared store, as returned by `get_members`: the
/// member's public key, their role, and whether it is the local user.
#[derive(Debug, Clone)]
pub struct MemberInfo {
    pub pubkey: String,
    pub role: MemberRole,
    pub is_self: bool,
}

/// A single membership entry in the chain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MembershipEntry {
    pub action: MembershipAction,
    pub user_pubkey: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_account_email: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prev_hash: Option<String>,
    pub role: MemberRole,
    pub timestamp: String,
    pub author_pubkey: String,
    pub signature: String,
}

/// Storage coordinate of a membership entry: the owner who signed it
/// (`author_pubkey`, an owner's pubkey) and that owner's per-author sequence
/// number (`seq`). Together they are the entry's object key
/// `membership/{author_pubkey}/{seq}`.
///
/// A changeset names the coordinate of the entry that grants its author write
/// access. A puller that does not yet see that entry — membership entries and
/// changesets are separate, unordered object streams — can then fetch exactly
/// that one object (a keyed GET is strongly consistent, unlike the LIST that
/// rebuilds the chain each cycle) and resolve the gap deterministically instead
/// of dropping the changeset as non-member.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct MembershipCoord {
    pub author_pubkey: String,
    pub seq: u64,
}

/// A signed statement by one author certifying how far its own membership prefix
/// is committed: entries `membership/{author}/1..=seq` exist and the entry at
/// `seq` hashes to `tip_hash`. Stored at `membership/{author}/head`.
///
/// Each owner publishes and signs its own head under its own prefix, so concurrent
/// owners commit independently and entries can only accumulate — there is no shared
/// object a later writer can roll back over a peer's. A reader admits an author's
/// prefix only up to that author's head `seq`, so an entry is uncommitted until its
/// own author's head covers it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorHead {
    pub author_pubkey: String,
    pub seq: u64,
    pub tip_hash: String,
    pub signature: String,
}

/// The coordinate of the entry that grants `pubkey` write access: the most
/// recent (by causal timestamp) write-capable `Add` of `pubkey` among `entries`,
/// each paired with the storage coordinate it was loaded from. Returns `None`
/// when no such entry exists. A device embeds this in changesets it authors so a
/// puller can name and fetch the exact entry that authorizes the write.
///
/// A later `Remove` or role downgrade is not consulted here — the puller merges
/// the named entry into its own full chain and re-judges, so a revocation it
/// already holds still wins. This only needs to name the granting entry.
pub fn write_grant_coord(
    entries: &[(MembershipCoord, MembershipEntry)],
    pubkey: &str,
) -> Option<MembershipCoord> {
    entries
        .iter()
        .filter(|(_, e)| {
            e.action == MembershipAction::Add && e.user_pubkey == pubkey && e.role.can_write()
        })
        .max_by(|(_, a), (_, b)| a.timestamp.cmp(&b.timestamp))
        .map(|(coord, _)| coord.clone())
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
    #[error("entry at index {index} has predecessor hash {actual:?}, expected {expected:?}")]
    BrokenAuthorLink {
        index: usize,
        expected: Option<String>,
        actual: Option<String>,
    },
}

/// Canonical bytes of the signed fields (everything except `signature`).
/// Field order is fixed here so the bytes are deterministic without depending
/// on any serde feature flag.
pub fn canonical_bytes(entry: &MembershipEntry) -> Vec<u8> {
    #[derive(Serialize)]
    struct Signed<'a> {
        action: &'a MembershipAction,
        author_pubkey: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        provider_account_email: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        prev_hash: Option<&'a str>,
        role: &'a MemberRole,
        timestamp: &'a str,
        user_pubkey: &'a str,
    }
    let signed = Signed {
        action: &entry.action,
        author_pubkey: &entry.author_pubkey,
        provider_account_email: entry.provider_account_email.as_deref(),
        prev_hash: entry.prev_hash.as_deref(),
        role: &entry.role,
        timestamp: &entry.timestamp,
        user_pubkey: &entry.user_pubkey,
    };
    serde_json::to_vec(&signed).expect("entry fields serialize")
}

pub fn entry_hash(entry: &MembershipEntry) -> String {
    hex::encode(Sha256::digest(
        serde_json::to_vec(entry).expect("membership entry serialization cannot fail"),
    ))
}

/// Build the founder entry of a store's membership chain: a self-signed `Add`
/// of `owner` with the `Owner` role. Every store is founded by exactly one such
/// entry at creation, and the chain is later anchored to `owner`'s pubkey so no
/// one can wipe `membership/*` and refound themselves as Owner (issue #95).
pub fn founder_entry(owner: &UserKeypair, timestamp: &str) -> MembershipEntry {
    let pk_hex = keys::public_key_hex(owner);
    let mut entry = MembershipEntry {
        action: MembershipAction::Add,
        user_pubkey: pk_hex.clone(),
        provider_account_email: None,
        prev_hash: None,
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
    entry.author_pubkey = keys::public_key_hex(keypair);
    let bytes = canonical_bytes(entry);
    let (_, signature) = keys::sign_hex(keypair, &bytes);
    entry.signature = signature;
}

/// Verify the signature on a membership entry.
pub fn verify_membership_entry(entry: &MembershipEntry) -> bool {
    keys::verify_signature_hex(
        &entry.author_pubkey,
        &entry.signature,
        &canonical_bytes(entry),
    )
}

fn apply_active_member_rule(active: &mut Vec<(String, MemberRole)>, entry: &MembershipEntry) {
    active.retain(|(pk, _)| pk != &entry.user_pubkey);
    if entry.action == MembershipAction::Add {
        active.push((entry.user_pubkey.clone(), entry.role.clone()));
    }
}

fn members_can_write(members: &[(String, MemberRole)], pubkey: &str) -> bool {
    members
        .iter()
        .any(|(pk, role)| pk == pubkey && role.can_write())
}

fn members_has_owner(members: &[(String, MemberRole)], pubkey: &str) -> bool {
    members
        .iter()
        .any(|(pk, role)| pk == pubkey && *role == MemberRole::Owner)
}

/// An append-only membership chain.
///
/// Entries are sorted by timestamp (HLC string comparison gives causal order).
#[derive(Debug, Clone, Default)]
pub struct MembershipChain {
    entries: Vec<MembershipEntry>,
    coords: Vec<Option<MembershipCoord>>,
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
        let coords = vec![None; entries.len()];
        let chain = Self { entries, coords };
        chain.validate()?;
        Ok(chain)
    }

    pub fn from_entries_with_coords(
        mut entries: Vec<(MembershipCoord, MembershipEntry)>,
    ) -> Result<Self, MembershipError> {
        entries.sort_by(|(_, a), (_, b)| a.timestamp.cmp(&b.timestamp));
        let (coords, entries): (Vec<_>, Vec<_>) = entries
            .into_iter()
            .map(|(coord, entry)| (Some(coord), entry))
            .unzip();
        let chain = Self { entries, coords };
        chain.validate()?;
        chain.validate_author_links()?;
        Ok(chain)
    }

    /// Return the entries in the chain.
    pub fn entries(&self) -> &[MembershipEntry] {
        &self.entries
    }

    /// The coordinate of the entry that grants `pubkey` write access, derived from
    /// this chain's own entries and their storage coordinates. The chain-scoped form
    /// of [`write_grant_coord`], used to bind an outgoing changeset to its
    /// authorizing entry without a second membership download.
    pub fn write_grant_coord(&self, pubkey: &str) -> Option<MembershipCoord> {
        let paired: Vec<(MembershipCoord, MembershipEntry)> = self
            .entries_with_effective_coords()
            .into_iter()
            .map(|(_, coord, entry)| (coord, entry.clone()))
            .collect();
        write_grant_coord(&paired, pubkey)
    }

    /// The pubkey of the founder (chain entry #1, the self-signed Owner Add), or
    /// `None` for an empty chain. The store is anchored to this pubkey: a chain
    /// whose founder is not the store's established owner is a takeover attempt
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

        let mut active: Vec<(String, MemberRole)> = Vec::new();
        apply_active_member_rule(&mut active, first);

        for (i, entry) in self.entries.iter().enumerate().skip(1) {
            if !verify_membership_entry(entry) {
                return Err(MembershipError::InvalidSignature(i));
            }

            if !members_has_owner(&active, &entry.author_pubkey) {
                return Err(MembershipError::NotAnOwner(i));
            }

            apply_active_member_rule(&mut active, entry);
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
        members_can_write(&self.current_members(), pubkey)
    }

    /// Whether `pubkey` is *currently* a store Owner. Owner-only writes
    /// (snapshots) authorize against this rather than `can_write_now`.
    pub fn is_owner_now(&self, pubkey: &str) -> bool {
        members_has_owner(&self.current_members(), pubkey)
    }

    /// Return current active members with their roles.
    pub fn current_members(&self) -> Vec<(String, MemberRole)> {
        let mut active: Vec<(String, MemberRole)> = Vec::new();

        for entry in &self.entries {
            apply_active_member_rule(&mut active, entry);
        }

        active
    }

    pub fn current_member_provider_email(&self, pubkey: &str) -> Option<&str> {
        let mut email = None;
        for entry in &self.entries {
            if entry.user_pubkey != pubkey {
                continue;
            }
            match entry.action {
                MembershipAction::Add => {
                    email = entry.provider_account_email.as_deref();
                }
                MembershipAction::Remove => {
                    email = None;
                }
            }
        }
        email
    }

    /// Validate and append an entry to the chain.
    pub fn add_entry(&mut self, entry: MembershipEntry) -> Result<(), MembershipError> {
        self.entries.push(entry);
        self.coords.push(None);
        match self.validate() {
            Ok(()) => Ok(()),
            Err(e) => {
                self.entries.pop();
                self.coords.pop();
                Err(e)
            }
        }
    }

    pub fn add_entry_at(
        &mut self,
        coord: MembershipCoord,
        entry: MembershipEntry,
    ) -> Result<(), MembershipError> {
        self.entries.push(entry);
        self.coords.push(Some(coord));
        match self.validate().and_then(|()| self.validate_author_links()) {
            Ok(()) => Ok(()),
            Err(e) => {
                self.entries.pop();
                self.coords.pop();
                Err(e)
            }
        }
    }

    /// The greatest committed seq for `author_pubkey` and the hash of that entry,
    /// or `None` if the author has no entries in this chain. This pair is exactly
    /// what the author's [`AuthorHead`] certifies.
    pub fn author_tip(&self, author_pubkey: &str) -> Option<(u64, String)> {
        let mut latest: Option<(u64, &MembershipEntry)> = None;
        for (_, coord, entry) in self.entries_with_effective_coords() {
            if coord.author_pubkey != author_pubkey {
                continue;
            };
            if latest
                .as_ref()
                .is_none_or(|(latest_seq, _)| coord.seq > *latest_seq)
            {
                latest = Some((coord.seq, entry));
            }
        }
        latest.map(|(seq, entry)| (seq, entry_hash(entry)))
    }

    /// The hash of `author_pubkey`'s latest entry — the `prev_hash` a fresh entry
    /// by that author links to. `None` when the author has no entries yet.
    pub fn latest_author_hash(&self, author_pubkey: &str) -> Option<String> {
        self.author_tip(author_pubkey).map(|(_, hash)| hash)
    }

    /// Build `signer`'s signed head from this chain's current tip for that author,
    /// or `None` if the author has authored no entries (nothing to certify).
    pub fn signed_head(&self, signer: &UserKeypair) -> Option<AuthorHead> {
        let author = keys::public_key_hex(signer);
        self.author_tip(&author)
            .map(|(seq, tip_hash)| AuthorHead::signed(seq, tip_hash, signer))
    }

    fn entries_with_effective_coords(&self) -> Vec<(usize, MembershipCoord, &MembershipEntry)> {
        let mut derived_seq = BTreeMap::<String, u64>::new();
        self.entries
            .iter()
            .enumerate()
            .map(|(index, entry)| {
                let coord = match self.coords[index].clone() {
                    Some(coord) => {
                        let seq = derived_seq.entry(coord.author_pubkey.clone()).or_insert(0);
                        *seq = (*seq).max(coord.seq);
                        coord
                    }
                    None => {
                        let seq = derived_seq.entry(entry.author_pubkey.clone()).or_insert(0);
                        *seq += 1;
                        MembershipCoord {
                            author_pubkey: entry.author_pubkey.clone(),
                            seq: *seq,
                        }
                    }
                };
                (index, coord, entry)
            })
            .collect()
    }

    fn validate_author_links(&self) -> Result<(), MembershipError> {
        let mut by_author: BTreeMap<String, Vec<(usize, u64)>> = BTreeMap::new();
        for (index, coord, _) in self.entries_with_effective_coords() {
            by_author
                .entry(coord.author_pubkey)
                .or_default()
                .push((index, coord.seq));
        }

        for entries in by_author.values_mut() {
            entries.sort_by_key(|(_, seq)| *seq);
            let mut expected = None;
            for (index, _) in entries {
                let entry = &self.entries[*index];
                if entry.prev_hash != expected {
                    return Err(MembershipError::BrokenAuthorLink {
                        index: *index,
                        expected,
                        actual: entry.prev_hash.clone(),
                    });
                }
                expected = Some(entry_hash(entry));
            }
        }
        Ok(())
    }
}

impl AuthorHead {
    /// Sign a head under `keypair`'s own pubkey, certifying its prefix through
    /// `seq` with tip hash `tip_hash`.
    pub fn signed(seq: u64, tip_hash: String, keypair: &UserKeypair) -> Self {
        let author_pubkey = keys::public_key_hex(keypair);
        let payload = author_head_payload(&author_pubkey, seq, &tip_hash);
        let (_, signature) = keys::sign_hex(keypair, &payload);
        AuthorHead {
            author_pubkey,
            seq,
            tip_hash,
            signature,
        }
    }

    pub fn verify(&self) -> bool {
        keys::verify_signature_hex(
            &self.author_pubkey,
            &self.signature,
            &author_head_payload(&self.author_pubkey, self.seq, &self.tip_hash),
        )
    }
}

fn author_head_payload(author_pubkey: &str, seq: u64, tip_hash: &str) -> Vec<u8> {
    #[derive(Serialize)]
    struct HeadFields<'a> {
        author_pubkey: &'a str,
        seq: u64,
        tip_hash: &'a str,
    }
    serde_json::to_vec(&HeadFields {
        author_pubkey,
        seq,
        tip_hash,
    })
    .expect("membership head fields serialization cannot fail")
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
    fn write_grant_coord_names_the_latest_write_granting_add() {
        let owner = gen_keypair();
        let member = gen_keypair();
        let follower = gen_keypair();

        // Pairs as a puller holds them: each entry with the storage coordinate it
        // was loaded from. Founder at (owner, 1); the owner then adds a member at
        // (owner, 2) and a follower at (owner, 3).
        let coord = |kp: &UserKeypair, seq| MembershipCoord {
            author_pubkey: pubkey_hex(kp),
            seq,
        };
        let entries = vec![
            (
                coord(&owner, 1),
                founder_entry(&owner, "0000000001000-0000-dev1"),
            ),
            (
                coord(&owner, 2),
                make_entry(
                    &owner,
                    MembershipAction::Add,
                    &member,
                    MemberRole::Member,
                    "0000000002000-0000-dev1",
                ),
            ),
            (
                coord(&owner, 3),
                make_entry(
                    &owner,
                    MembershipAction::Add,
                    &follower,
                    MemberRole::Follower,
                    "0000000003000-0000-dev1",
                ),
            ),
        ];

        // The owner's grant is the founder entry; the member's is its Add.
        assert_eq!(
            write_grant_coord(&entries, &pubkey_hex(&owner)),
            Some(coord(&owner, 1))
        );
        assert_eq!(
            write_grant_coord(&entries, &pubkey_hex(&member)),
            Some(coord(&owner, 2))
        );
        // A read-only follower has no write grant; nor does an outsider.
        assert_eq!(write_grant_coord(&entries, &pubkey_hex(&follower)), None);
        assert_eq!(write_grant_coord(&entries, "deadbeef"), None);
    }

    /// A re-add at a later coordinate (e.g. a role restore) wins over the earlier
    /// one: the grant names the entry with the greatest timestamp, so a puller
    /// fetches the still-current grant, not a stale one.
    #[test]
    fn write_grant_coord_prefers_the_latest_add() {
        let owner = gen_keypair();
        let member = gen_keypair();
        let coord = |kp: &UserKeypair, seq| MembershipCoord {
            author_pubkey: pubkey_hex(kp),
            seq,
        };
        let entries = vec![
            (
                coord(&owner, 1),
                founder_entry(&owner, "0000000001000-0000-dev1"),
            ),
            (
                coord(&owner, 2),
                make_entry(
                    &owner,
                    MembershipAction::Add,
                    &member,
                    MemberRole::Member,
                    "0000000002000-0000-dev1",
                ),
            ),
            // Same member re-added later (a fresh Add) at a higher coordinate.
            (
                coord(&owner, 5),
                make_entry(
                    &owner,
                    MembershipAction::Add,
                    &member,
                    MemberRole::Member,
                    "0000000005000-0000-dev1",
                ),
            ),
        ];
        assert_eq!(
            write_grant_coord(&entries, &pubkey_hex(&member)),
            Some(coord(&owner, 5))
        );
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
        let members = chain.current_members();
        assert!(members_can_write(&members, &pubkey_hex(&owner)));
        assert!(members_can_write(&members, &pubkey_hex(&member)));
        assert!(!members_can_write(&members, &pubkey_hex(&follower)));
        // ...but the Follower is still a registered member (can read).
        assert!(members.iter().any(|(pk, _)| pk == &pubkey_hex(&follower)));

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
        let members = chain.current_members();
        assert!(!members.iter().any(|(pk, _)| pk == &pubkey_hex(&follower)));
        assert!(!members_can_write(&members, &pubkey_hex(&follower)));
    }

    #[test]
    fn is_owner_now_is_true_only_for_current_owners() {
        let owner = gen_keypair();
        let member = gen_keypair();
        let follower = gen_keypair();
        let owner2 = gen_keypair();

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
                MembershipAction::Add,
                &follower,
                MemberRole::Follower,
                "0000000003000-0000-dev1",
            ))
            .unwrap();
        // The founder promotes a second Owner — already-supported multi-owner.
        chain
            .add_entry(make_entry(
                &owner,
                MembershipAction::Add,
                &owner2,
                MemberRole::Owner,
                "0000000004000-0000-dev1",
            ))
            .unwrap();
        chain.validate().unwrap();

        // Only Owners pass: the founder and the second Owner.
        let members = chain.current_members();
        assert!(members_has_owner(&members, &pubkey_hex(&owner)));
        assert!(members_has_owner(&members, &pubkey_hex(&owner2)));
        // A write-capable Member is NOT an Owner — the owner-only bar is stricter
        // than can_write_now.
        assert!(members_can_write(&members, &pubkey_hex(&member)));
        assert!(!members_has_owner(&members, &pubkey_hex(&member)));
        // A read-only Follower and an outsider are not Owners.
        assert!(!members_has_owner(&members, &pubkey_hex(&follower)));
        assert!(!members_has_owner(&members, "deadbeef"));
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
            provider_account_email: None,
            prev_hash: None,
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
        assert_eq!(chain.entries().len(), 1);
    }

    #[test]
    fn failed_append_reports_new_entry_index_and_rolls_back() {
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

        let result = chain.add_entry(make_entry(
            &member,
            MembershipAction::Add,
            &outsider,
            MemberRole::Member,
            "0000000003000-0000-dev1",
        ));

        assert!(matches!(result, Err(MembershipError::NotAnOwner(2))));
        assert_eq!(chain.entries().len(), 2);
        assert!(chain.validate().is_ok());
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
            provider_account_email: None,
            prev_hash: None,
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

    /// Pin the exact signed bytes: fields serialize in alphabetical order with
    /// no whitespace. A change here alters every membership signature, so it must
    /// be caught, not silently absorbed.
    #[test]
    fn canonical_bytes_pins_exact_json() {
        let entry = MembershipEntry {
            action: MembershipAction::Add,
            user_pubkey: "bbbb".to_string(),
            provider_account_email: None,
            prev_hash: None,
            role: MemberRole::Owner,
            timestamp: "0000000001000-0000-dev1".to_string(),
            author_pubkey: "aaaa".to_string(),
            signature: "does-not-matter".to_string(),
        };
        let expected = r#"{"action":"Add","author_pubkey":"aaaa","role":"Owner","timestamp":"0000000001000-0000-dev1","user_pubkey":"bbbb"}"#;
        assert_eq!(canonical_bytes(&entry), expected.as_bytes());
    }

    #[test]
    fn provider_email_is_signed_when_present() {
        let owner = gen_keypair();
        let member = gen_keypair();
        let mut entry = make_entry(
            &owner,
            MembershipAction::Add,
            &member,
            MemberRole::Member,
            "0000000002000-0000-dev1",
        );
        entry.provider_account_email = Some("member@example.com".to_string());
        sign_membership_entry(&mut entry, &owner);

        assert!(verify_membership_entry(&entry));
        let bytes = canonical_bytes(&entry);
        let text = std::str::from_utf8(&bytes).unwrap();
        assert!(text.contains(r#""provider_account_email":"member@example.com""#));

        entry.provider_account_email = Some("other@example.com".to_string());
        assert!(!verify_membership_entry(&entry));
    }

    #[test]
    fn current_member_provider_email_tracks_latest_active_add() {
        let owner = gen_keypair();
        let member = gen_keypair();
        let member_pk = pubkey_hex(&member);

        let mut chain = MembershipChain::new();
        chain
            .add_entry(founder_entry(&owner, "0000000001000-0000-dev1"))
            .unwrap();

        let mut first = make_entry(
            &owner,
            MembershipAction::Add,
            &member,
            MemberRole::Member,
            "0000000002000-0000-dev1",
        );
        first.provider_account_email = Some("first@example.com".to_string());
        sign_membership_entry(&mut first, &owner);
        chain.add_entry(first).unwrap();
        assert_eq!(
            chain.current_member_provider_email(&member_pk),
            Some("first@example.com")
        );

        let mut second = make_entry(
            &owner,
            MembershipAction::Add,
            &member,
            MemberRole::Member,
            "0000000003000-0000-dev1",
        );
        second.provider_account_email = Some("second@example.com".to_string());
        sign_membership_entry(&mut second, &owner);
        chain.add_entry(second).unwrap();
        assert_eq!(
            chain.current_member_provider_email(&member_pk),
            Some("second@example.com")
        );

        chain
            .add_entry(make_entry(
                &owner,
                MembershipAction::Remove,
                &member,
                MemberRole::Member,
                "0000000004000-0000-dev1",
            ))
            .unwrap();
        assert_eq!(chain.current_member_provider_email(&member_pk), None);
    }
}

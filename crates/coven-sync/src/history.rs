//! Authenticated administrative history, indexed by its counted slots.

use crate::{
    effects, snapshot_object::RoutedKey, CutOffRefusal, OperationRejection, Refusal, SyncError,
};
use coven_crypto::{MemberIdentity, SigningKey, StoreKeyring};
use coven_database::{AdminEffect, DeviceStop, StoreCircle, StoreDevice, StoreMember, StoreState};
use coven_format::{
    admin::StoreChange,
    sealed_key::{CopyBox, KeyCopyPrefix},
    sealed_slot::{BoxPlace, SlotObjectPrefix},
    sequence::{SlotFrame, SlotHeader, SnapshotAnnouncement},
    MemberAccess,
};
use coven_foundation::id_source::{CircleId, DeviceId, InviteId, KeyId, MemberId};
use coven_merge::{Audience, Slot};
use std::collections::{BTreeMap, BTreeSet};

/// One key box in a signed slot's clear prefix, with what its context binds:
/// the slot's path, format and sealing key, and the box's place in the slot
/// (D9, D11). A key-copy slot carries one; an admission carries its member's.
#[derive(Clone)]
pub(crate) struct KeyCopy {
    pub(crate) path: coven_format::path::ObjectPath,
    pub(crate) format: coven_format::FormatVersion,
    pub(crate) key: KeyId,
    pub(crate) place: BoxPlace,
    pub(crate) copy: CopyBox,
}

impl KeyCopy {
    /// Every box `prefix`, the clear prefix of the slot at `path`, carries.
    pub(crate) fn all<'a>(
        path: &'a coven_format::path::ObjectPath,
        prefix: &'a SlotObjectPrefix,
    ) -> impl Iterator<Item = Self> + 'a {
        prefix.kind.boxes().map(move |(place, copy)| Self {
            path: path.clone(),
            format: prefix.format,
            key: prefix.key,
            place,
            copy: copy.clone(),
        })
    }

    /// The copy's routing.
    pub(crate) fn route(&self) -> &KeyCopyPrefix {
        &self.copy.routing
    }
}

/// One accepted introduction's place in the sequence and key commitment.
#[derive(Clone)]
pub(crate) struct Introduction {
    pub(crate) slot: Slot,
    pub(crate) audience: Audience,
    pub(crate) key: KeyId,
    pub(crate) hash: [u8; 32],
}

/// A member id bound to a key pair by a counted entry, with its authoring
/// device: a joiner is admitted only by its approving device's binding (§13.2).
#[derive(Clone)]
pub(crate) struct Binding {
    pub(crate) member: MemberId,
    pub(crate) keys: coven_crypto::MemberPublicKeys,
    pub(crate) device: DeviceId,
}

/// A counted entry that cut a device or member off (§14.1): a removal, a
/// reset's registration in a device's place, or a member-key replacement,
/// with the device that wrote it.
#[derive(Clone)]
pub(crate) struct Cutoff {
    pub(crate) slot: Slot,
    pub(crate) author: DeviceId,
    pub(crate) change: StoreChange,
}

/// The share an invite grants, as the inviting device recorded it before
/// granting it (§13.2): the account stays shared while the share is open,
/// and the owner's devices take it back once it isn't.
#[derive(Clone)]
pub(crate) struct InviteShare {
    /// The device that made the invite, and alone ends its share.
    pub(crate) device: DeviceId,
    pub(crate) account: String,
    /// Its device recorded the share's end.
    pub(crate) ended: bool,
}

/// Changes to existing domain values, rather than a copy of the entire state
/// at every prefix. Writes resolve their original credentials by `read_through`.
#[derive(Clone, Default)]
pub(crate) struct History {
    pub(crate) state: StoreState,
    pub(crate) introductions: Vec<Introduction>,
    pub(crate) copies: Vec<KeyCopy>,
    pub(crate) announcements: BTreeMap<Slot, SnapshotAnnouncement>,
    pub(crate) boundaries: Vec<coven_database::BoundaryCoverage>,
    pub(crate) cutoffs: Vec<Cutoff>,
    pub(crate) bindings: Vec<Binding>,
    /// Every invite share a counted entry recorded, ended or not.
    pub(crate) invite_shares: BTreeMap<InviteId, InviteShare>,
    members: BTreeMap<MemberId, BTreeMap<u64, StoreMember>>,
    devices: BTreeMap<DeviceId, BTreeMap<u64, StoreDevice>>,
    circles: BTreeMap<CircleId, BTreeMap<u64, StoreCircle>>,
}

fn at<'a, K: Ord, V>(
    history: &'a BTreeMap<K, BTreeMap<u64, V>>,
    id: &K,
    through: u64,
) -> Option<&'a V> {
    history.get(id)?.range(..=through).next_back().map(|(_, value)| value)
}

fn changed<K: Ord + Clone, V: PartialEq + Clone>(
    before: &BTreeMap<K, V>,
    after: &BTreeMap<K, V>,
    slot: Slot,
    history: &mut BTreeMap<K, BTreeMap<u64, V>>,
) {
    for (id, value) in after {
        if before.get(id) != Some(value) {
            history.entry(id.clone()).or_default().insert(slot.get(), value.clone());
        }
    }
}

impl History {
    pub(crate) fn recorded_access(&self) -> impl Iterator<Item = (MemberId, u64, &MemberAccess)> {
        self.members.iter().flat_map(|(id, changes)| {
            changes.iter().map(move |(at, value)| (*id, *at, &value.access))
        })
    }

    pub(crate) fn signer(
        &self,
        header: &SlotHeader,
        frame: Option<&SlotFrame>,
    ) -> Result<SigningKey, Refusal> {
        if let Some(SlotFrame::Admin(StoreChange::CreateStore { admin, .. })) = frame {
            return Ok(admin.signing);
        }
        at(&self.members, &header.author, header.after)
            .map(|member| member.keys.signing)
            .ok_or(Refusal::Signature { cause: None })
    }

    /// Owner and active credential in the original signed prefix, retained after removal.
    pub(crate) fn owner(
        &self,
        device: DeviceId,
        through: u64,
    ) -> Result<(MemberId, SigningKey), Refusal> {
        // Only slot one's device may author offline originals before setup.
        let through = if through == 0
            && self.devices.get(&device).is_some_and(|changes| changes.contains_key(&1))
        {
            1
        } else {
            through
        };
        let device = at(&self.devices, &device, through).ok_or(Refusal::NotAuthorized)?;
        let member = at(&self.members, &device.member, through).ok_or(Refusal::NotAuthorized)?;
        if !device.active() || member.removed {
            return Err(Refusal::NotAuthorized);
        }
        Ok((device.member, member.keys.signing))
    }

    /// The slot whose entry added `device`. Each of its writes is checked
    /// against a state that has it (`owner`), so each read through that
    /// slot, except slot one's offline writes, which read nothing.
    pub(crate) fn added(&self, device: DeviceId) -> Option<u64> {
        self.devices.get(&device)?.keys().next().copied()
    }

    /// The other devices that can still write: every device besides `me`
    /// not gone for good, each with the slot that added it. Deleting stored
    /// files (§17.10) and forgetting kept records (§16.7) wait for each to
    /// write past what they free.
    pub(crate) fn writers(&self, me: DeviceId) -> coven_database::Writers {
        coven_database::Writers::new(
            self.state
                .devices
                .keys()
                .filter(|device| **device != me && self.gone_for_good(**device).is_none())
                .map(|device| (*device, self.added(*device)))
                .collect(),
        )
    }

    /// The first counted entry that stopped `device` for good, and how
    /// (§14.1): its removal or its member's, a reset registering another
    /// installation in its place, or its member's keys replaced or the
    /// member recovered, after it was added, by another of the member's
    /// devices or an admin's. The pair
    /// that replacement binds is one the device doesn't hold; the device
    /// that wrote it does. Another copy of an installation using its device
    /// id is never here: only that installation sees it (§19.3).
    pub(crate) fn gone_for_good(&self, device: DeviceId) -> Option<(Slot, DeviceStop)> {
        self.stops(device).next()
    }

    /// Every counted entry that stops `device` for good, oldest first: the
    /// first is where it went (`gone_for_good`), and each later one stops it
    /// again, as its member's removal after its own does.
    pub(crate) fn stops(
        &self,
        device: DeviceId,
    ) -> impl DoubleEndedIterator<Item = (Slot, DeviceStop)> + '_ {
        let registered =
            self.state.devices.get(&device).map(|value| value.member).zip(self.added(device));
        self.cutoffs.iter().filter_map(move |cutoff| {
            let (member, added) = registered?;
            let stop = match &cutoff.change {
                StoreChange::RemoveDevice { device: removed } if *removed == device => {
                    DeviceStop::Removed
                }
                StoreChange::RemoveMember { member: removed } if *removed == member => {
                    DeviceStop::Removed
                }
                StoreChange::AddDevice { replaces: Some(replaced), .. } if *replaced == device => {
                    DeviceStop::Replaced
                }
                StoreChange::ReplaceMemberKeys { member: replaced, .. }
                | StoreChange::RecoverMember { member: replaced, .. }
                    if *replaced == member
                        && cutoff.slot.get() > added
                        && cutoff.author != device =>
                {
                    DeviceStop::MemberKeysReplaced
                }
                _ => return None,
            };
            Some((cutoff.slot, stop))
        })
    }

    /// The invite shares that still hold their accounts: not ended, and
    /// made by a device not gone for good (§13.2, §14.2).
    pub(crate) fn open_invite_shares(&self) -> impl Iterator<Item = &InviteShare> {
        self.invite_shares
            .values()
            .filter(|share| !share.ended && self.gone_for_good(share.device).is_none())
    }

    /// The provider work that takes back what a lost owner device's shares
    /// may have granted after this device's own revocations: every account
    /// those shares named that nothing intends now (§20.9). Refused when
    /// `member`'s devices don't change sharing here (S3, or another account
    /// holds the store) or the device isn't gone for good, each as its own
    /// `CutOffRefusal`.
    pub(crate) fn cut_off_retakes(
        &self,
        member: MemberId,
        device: DeviceId,
    ) -> Result<Vec<coven_database::ProviderAccessChange>, SyncError> {
        let provider_account =
            matches!(self.store_access(), Some(MemberAccess::ProviderAccount(_)));
        if !provider_account {
            return Err(CutOffRefusal::NoAccountShared.into());
        }
        if !self.owns_storage(member) {
            return Err(CutOffRefusal::NotStoreOwner.into());
        }
        if self.gone_for_good(device).is_none() {
            return Err(CutOffRefusal::DeviceNotGone.into());
        }
        let accounts: BTreeSet<String> = self
            .invite_shares
            .values()
            .filter(|share| share.device == device)
            .map(|share| MemberAccess::normalized_account(&share.account))
            .filter(|account| !self.account_intended(account))
            .collect();
        accounts
            .iter()
            .map(|account| crate::sequence_sync::account_change(account, false, false))
            .collect()
    }

    /// Whether `account` is meant to be shared: an active member reaches
    /// storage through it, or an open invite share names it (§14.2).
    pub(crate) fn account_intended(&self, account: &str) -> bool {
        let access = MemberAccess::ProviderAccount(account.to_owned());
        self.state
            .members
            .values()
            .any(|member| !member.removed && member.access.same_credential(&access))
            || self
                .open_invite_shares()
                .any(|share| MemberAccess::same_account(&share.account, account))
    }

    pub(crate) fn belonged(&self, audience: &Audience, member: &MemberId, through: u64) -> bool {
        at(&self.members, member, through).is_some_and(|member| !member.removed)
            && match audience {
                Audience::Store => true,
                Audience::Circle(circle) => at(&self.circles, circle, through)
                    .is_some_and(|circle| circle.members.contains(member)),
            }
    }

    pub(crate) fn write_signer(
        &self,
        header: &coven_format::write_stream::WriteHeaderFrame,
    ) -> Result<SigningKey, Refusal> {
        let (member, signer) =
            self.owner(header.header.position.device, header.header.read_through)?;
        let through = header.header.read_through.max(1);
        if !header.parts.iter().all(|part| self.belonged(&part.audience, &member, through)) {
            return Err(Refusal::NotAuthorized);
        }
        Ok(signer)
    }

    pub(crate) fn validate(
        &self,
        header: &SlotHeader,
        slot: Slot,
        change: &StoreChange,
    ) -> Result<Option<AdminEffect>, OperationRejection> {
        if let StoreChange::RemoveMember { member } = change {
            if self.holds_store(*member) {
                return Err(OperationRejection::StoreOwner);
            }
        }
        let effect = effects::checked_change(
            &self.state,
            header,
            slot,
            change,
            &self.announcements,
            &self.boundaries,
        )?;
        match change {
            StoreChange::AdmitMember { member, access, .. }
            | StoreChange::SetAccess { member, access }
            | StoreChange::RecoverMember { member, access: Some(access), .. } => {
                self.access_allowed(*member, access)?
            }
            StoreChange::AddInviteShare { invite, account } => {
                if let Some(share) = self.invite_shares.get(invite) {
                    // Its device recording the same open share again is
                    // satisfied; any other use of the id is refused.
                    return if !share.ended
                        && share.device == header.device
                        && MemberAccess::same_account(&share.account, account)
                    {
                        Ok(None)
                    } else {
                        Err(OperationRejection::NotAllowed)
                    };
                }
                // Only the member whose account holds the store shares it
                // with accounts (§20.9).
                if !self.holds_store(header.author) {
                    return Err(OperationRejection::NotAllowed);
                }
            }
            StoreChange::EndInviteShare { invite } => match self.invite_shares.get(invite) {
                Some(share) if share.device == header.device => {
                    if share.ended {
                        return Ok(None);
                    }
                }
                _ => return Err(OperationRejection::NotAllowed),
            },
            _ => (),
        }
        Ok(effect)
    }

    /// Validate an admission: its entry, as `validate` does, and the copies
    /// its clear prefix carries. They all go to the entry's member and new
    /// sealing key, name each store key introduced before the slot, and
    /// otherwise only keys introduced for circles the member reads before it
    /// (§13.2, format.md D6). The format keeps each key to one copy.
    pub(crate) fn validate_admission(
        &self,
        header: &SlotHeader,
        slot: Slot,
        change: &StoreChange,
        copies: &[CopyBox],
    ) -> Result<Option<AdminEffect>, OperationRejection> {
        let effect = self.validate(header, slot, change)?;
        let (StoreChange::AdmitMember { member, keys, .. }
        | StoreChange::RecoverMember { member, keys, .. }) = change
        else {
            unreachable!("an admission's kind holds only an admit or recover change")
        };
        let introduced: BTreeSet<KeyId> = self
            .introductions
            .iter()
            .filter(|intro| intro.audience == Audience::Store)
            .map(|intro| intro.key)
            .collect();
        let copied: BTreeSet<KeyId> = copies
            .iter()
            .filter(|copy| copy.routing.audience == Audience::Store)
            .map(|copy| copy.routing.key)
            .collect();
        let routed = copies.iter().all(|copy| {
            let route = &copy.routing;
            route.member == *member
                && route.sealing == keys.sealing
                && match &route.audience {
                    Audience::Store => true,
                    audience => {
                        effects::belongs(&self.state, audience, member)
                            && self.commitment(audience, route.key).is_some()
                    }
                }
        });
        if !routed || copied != introduced {
            return Err(OperationRejection::NotAllowed);
        }
        Ok(effect)
    }

    /// Install only the effect validated against this preceding state. Invalid
    /// admin attempts still consume their slot and retain their signed original.
    pub(crate) fn accept_admin(&mut self, slot: Slot, header: &SlotHeader, effect: &AdminEffect) {
        let bound = match &effect.change {
            StoreChange::CreateStore { admin, .. } => Some((header.author, *admin)),
            StoreChange::AdmitMember { member, keys, .. }
            | StoreChange::RecoverMember { member, keys, .. }
            | StoreChange::ReplaceMemberKeys { member, keys, .. } => Some((*member, *keys)),
            _ => None,
        };
        if let Some((member, keys)) = bound {
            self.bindings.push(Binding { member, keys, device: header.device });
        }
        match &effect.change {
            StoreChange::AddInviteShare { invite, account } => {
                self.invite_shares.insert(
                    *invite,
                    InviteShare { device: header.device, account: account.clone(), ended: false },
                );
            }
            StoreChange::EndInviteShare { invite } => {
                if let Some(share) = self.invite_shares.get_mut(invite) {
                    share.ended = true;
                }
            }
            _ => (),
        }
        changed(&self.state.members, &effect.state.members, slot, &mut self.members);
        changed(&self.state.devices, &effect.state.devices, slot, &mut self.devices);
        changed(&self.state.circles, &effect.state.circles, slot, &mut self.circles);
        if let Some((audience, key, hash)) = introduction(&effect.change) {
            self.introductions.push(Introduction { slot, audience, key, hash });
        }
        if let Some(boundary) = &effect.boundary {
            let announcement = match &effect.change {
                StoreChange::RaiseSchema { announcement, .. }
                | StoreChange::Reset { announcement } => announcement,
                _ => unreachable!("an activation owns its boundary"),
            };
            self.boundaries.push(coven_database::BoundaryCoverage {
                boundary: boundary.clone(),
                included: self.announcements[announcement].covers,
            });
        }
        if matches!(
            effect.change,
            StoreChange::RemoveDevice { .. }
                | StoreChange::RemoveMember { .. }
                | StoreChange::ReplaceMemberKeys { .. }
                | StoreChange::RecoverMember { .. }
                | StoreChange::AddDevice { replaces: Some(_), .. }
        ) {
            self.cutoffs.push(Cutoff {
                slot,
                author: header.device,
                change: effect.change.clone(),
            });
        }
        self.state = effect.state.clone();
    }

    /// The store's audience and every audience a key introduction names:
    /// those whose history and snapshots the store keeps.
    pub(crate) fn introduced_audiences(&self) -> BTreeSet<Audience> {
        self.introduced_through(u64::MAX)
    }

    /// [`Self::introduced_audiences`] as the slots through number `through` left them.
    pub(crate) fn introduced_through(&self, through: u64) -> BTreeSet<Audience> {
        std::iter::once(Audience::Store)
            .chain(
                self.introductions
                    .iter()
                    .filter(|intro| intro.slot.get() <= through)
                    .map(|intro| intro.audience.clone()),
            )
            .collect()
    }

    pub(crate) fn commitment(&self, audience: &Audience, key: KeyId) -> Option<[u8; 32]> {
        let mut values = self
            .introductions
            .iter()
            .filter(|value| &value.audience == audience && value.key == key);
        let first = values.next()?.hash;
        values.all(|value| value.hash == first).then_some(first)
    }

    /// The circle an applied introduction names for `key`, with the key's
    /// commitment: none for a store key or a key with no single commitment.
    pub(crate) fn circle_commitment(&self, key: KeyId) -> Option<(CircleId, [u8; 32])> {
        let circle = self.introductions.iter().find_map(|value| match value.audience {
            Audience::Circle(circle) if value.key == key => Some(circle),
            _ => None,
        })?;
        Some((circle, self.commitment(&Audience::Circle(circle), key)?))
    }

    pub(crate) fn accepted(&self, ring: &StoreKeyring, audience: &Audience, key: KeyId) -> bool {
        self.commitment(audience, key).is_some_and(|hash| {
            crate::write_seal::held(ring, audience, key).map(|held| held.hash()) == Some(hash)
        })
    }

    pub(crate) fn exposed(&self, audience: &Audience, key: KeyId) -> bool {
        self.copies.iter().any(|copy| {
            let route = copy.route();
            &route.audience == audience
                && route.key == key
                && (!effects::belongs(&self.state, audience, &route.member)
                    || self
                        .state
                        .members
                        .get(&route.member)
                        .is_none_or(|member| member.keys.sealing != route.sealing))
        })
    }

    /// First attempts use the last introduced held key whose copies expose it
    /// to no excluded member or credential. A departed circle member can send
    /// its fixed old writes with the newest accepted key it still holds.
    pub(crate) fn sending_key(
        &self,
        ring: &StoreKeyring,
        member: MemberId,
        audience: &Audience,
    ) -> Result<KeyId, SyncError> {
        let belongs = effects::belongs(&self.state, audience, &member);
        if !belongs && *audience == Audience::Store {
            return Err(SyncError::Removed);
        }
        self.introductions
            .iter()
            .rev()
            .find(|value| {
                &value.audience == audience
                    && self.accepted(ring, audience, value.key)
                    && (!belongs || !self.exposed(audience, value.key))
            })
            .map(|value| value.key)
            .ok_or_else(|| {
                match self.introductions.iter().rev().find(|value| &value.audience == audience) {
                    Some(value) => {
                        SyncError::KeyUnavailable { audience: audience.clone(), key: value.key }
                    }
                    None => SyncError::Rejected(OperationRejection::NotAllowed),
                }
            })
    }

    /// Metadata must remain publishable while a disclosed key is replaced.
    pub(crate) fn preparation_current(&self, announcement: &SnapshotAnnouncement) -> bool {
        self.announcements
            .values()
            .filter(|prior| prior.snapshot.audience == announcement.snapshot.audience)
            .all(|prior| prior.covers <= announcement.covers)
            && self
                .boundaries
                .iter()
                .filter(|prior| {
                    prior.boundary.snapshot().audience == announcement.snapshot.audience
                })
                .all(|prior| prior.boundary.entry().get() <= announcement.covers)
    }

    pub(crate) fn metadata_key(&self, ring: &StoreKeyring) -> Result<KeyId, SyncError> {
        self.introductions
            .iter()
            .rev()
            .find(|value| {
                value.audience == Audience::Store && self.accepted(ring, &value.audience, value.key)
            })
            .map(|value| value.key)
            .ok_or_else(|| SyncError::Rejected(OperationRejection::NotAllowed))
    }

    pub(crate) fn routes(&self, audience: &Audience, key: KeyId) -> Vec<KeyCopyPrefix> {
        self.state
            .members
            .iter()
            .filter(|(member, _)| effects::belongs(&self.state, audience, member))
            .map(|(member, value)| KeyCopyPrefix {
                audience: audience.clone(),
                key,
                member: *member,
                sealing: value.keys.sealing,
            })
            .collect()
    }

    /// Whether a copy with `route`'s routing is in the sequence.
    pub(crate) fn has_copy(&self, route: &KeyCopyPrefix) -> bool {
        self.copies.iter().any(|copy| copy.route() == route)
    }

    /// Whether `author` should send the copy `route` describes: they and the
    /// recipient are still in its audience, the recipient's sealing key is
    /// still the one it names, and the sequence has no such copy (§12.1).
    pub(crate) fn copy_wanted(&self, route: &KeyCopyPrefix, author: MemberId) -> bool {
        effects::belongs(&self.state, &route.audience, &author)
            && effects::belongs(&self.state, &route.audience, &route.member)
            && self
                .state
                .members
                .get(&route.member)
                .is_some_and(|member| member.keys.sealing == route.sealing)
            && !self.has_copy(route)
    }

    /// The copies `me` owes (§12.1): for each audience `me` belongs to, each
    /// introduced key `ring` holds and accepts, to each current member whose
    /// current sealing key has no copy of it. Removed members get none.
    pub(crate) fn owed_copies(&self, ring: &StoreKeyring, me: MemberId) -> Vec<KeyCopyPrefix> {
        self.introductions
            .iter()
            .filter(|intro| {
                effects::belongs(&self.state, &intro.audience, &me)
                    && self.accepted(ring, &intro.audience, intro.key)
            })
            .flat_map(|intro| self.routes(&intro.audience, intro.key))
            .filter(|route| !self.has_copy(route))
            .collect()
    }

    /// Open addressed boxes tentatively; an introduction must supply a matching
    /// commitment before use. Other recipients' routing still records exposure.
    pub(crate) fn acquire(
        &self,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
    ) -> Result<bool, SyncError> {
        let mut changed = false;
        for saved in &self.copies {
            let route = saved.route();
            if !route.held_by(member) || self.accepted(ring, &route.audience, route.key) {
                continue;
            }
            let hash = self.commitment(&route.audience, route.key);
            if hash.is_none() && crate::write_seal::held(ring, &route.audience, route.key).is_some()
            {
                continue;
            }
            let key = RoutedKey::open(saved, member)?;
            if let Some(hash) = hash {
                key.check_hash(&hash).map_err(|error| {
                    crate::write_object::damaged(
                        &saved.path,
                        Refusal::WrongIdentity { cause: Some(std::sync::Arc::new(error)) },
                    )
                })?;
            }
            // The copy holds the key history committed to, so other bytes
            // held under its id conflict in this device's custody (E5).
            key.insert(ring)?;
            changed = true;
        }
        Ok(changed)
    }
}

pub(crate) fn introduction(change: &StoreChange) -> Option<(Audience, KeyId, [u8; 32])> {
    match change {
        StoreChange::CreateStore { key, key_hash, .. } => Some((Audience::Store, *key, *key_hash)),
        StoreChange::CreateCircle { circle, key, key_hash, .. } => {
            Some((Audience::Circle(*circle), *key, *key_hash))
        }
        StoreChange::RotateKey { audience, key, key_hash } => {
            Some((audience.clone(), *key, *key_hash))
        }
        _ => None,
    }
}

#[cfg(test)]
#[path = "history_tests.rs"]
pub(crate) mod tests;

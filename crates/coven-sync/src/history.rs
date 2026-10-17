//! Authenticated administrative history, indexed by its counted slots.

use crate::{effects, OperationRejection, Refusal, SyncError};
use coven_crypto::{MemberIdentity, SigningKey, StoreKeyring};
use coven_database::{AdminEffect, StoreCircle, StoreDevice, StoreMember, StoreState};
use coven_format::{
    admin::StoreChange,
    sealed_key::KeyCopyPrefix,
    sealed_slot::SlotObjectPrefix,
    sequence::{SlotFrame, SlotHeader, SnapshotAnnouncement},
};
use coven_foundation::id_source::{CircleId, DeviceId, KeyId, MemberId};
use coven_merge::{Audience, Slot};
use std::collections::BTreeMap;

/// A signed routing prefix and the slot path which authenticates its anonymous box.
#[derive(Clone)]
pub(crate) struct KeyCopy {
    pub(crate) path: coven_format::path::ObjectPath,
    pub(crate) prefix: SlotObjectPrefix,
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
/// device: a joiner is admitted only by its approving device's binding (§12.2).
#[derive(Clone)]
pub(crate) struct Binding {
    pub(crate) member: MemberId,
    pub(crate) keys: coven_crypto::MemberPublicKeys,
    pub(crate) device: DeviceId,
}

/// An applied entry recording a member's S3 key, with its author.
#[derive(Clone)]
pub(crate) struct KeyRecord {
    pub(crate) member: MemberId,
    pub(crate) access_key_id: String,
    pub(crate) author: MemberId,
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
    pub(crate) cutoffs: Vec<(Slot, StoreChange)>,
    pub(crate) bindings: Vec<Binding>,
    /// Who recorded each member's S3 key: a delivered key is installed only
    /// from the author of the applied entry naming it (§13).
    pub(crate) key_records: Vec<KeyRecord>,
    /// Every S3 key id an admin entry names, valid or refused: a key some
    /// entry recorded is never reclaimed as stray (§13).
    pub(crate) named_keys: std::collections::BTreeSet<String>,
    members: BTreeMap<MemberId, BTreeMap<u64, StoreMember>>,
    devices: BTreeMap<DeviceId, BTreeMap<u64, StoreDevice>>,
    circles: BTreeMap<CircleId, BTreeMap<u64, StoreCircle>>,
}

fn at<'a, K: Ord, V>(
    history: &'a BTreeMap<K, BTreeMap<u64, V>>,
    id: &K,
    through: u64,
) -> Option<&'a V> {
    history
        .get(id)?
        .range(..=through)
        .next_back()
        .map(|(_, value)| value)
}

fn changed<K: Ord + Clone, V: PartialEq + Clone>(
    before: &BTreeMap<K, V>,
    after: &BTreeMap<K, V>,
    slot: Slot,
    history: &mut BTreeMap<K, BTreeMap<u64, V>>,
) {
    for (id, value) in after {
        if before.get(id) != Some(value) {
            history
                .entry(id.clone())
                .or_default()
                .insert(slot.0, value.clone());
        }
    }
}

impl History {
    pub(crate) fn recorded_access(
        &self,
    ) -> impl Iterator<Item = (MemberId, u64, &coven_format::MemberAccess)> {
        self.members.iter().flat_map(|(id, changes)| {
            changes
                .iter()
                .map(move |(at, value)| (*id, *at, &value.access))
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
            && self
                .devices
                .get(&device)
                .is_some_and(|changes| changes.contains_key(&1))
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
        if !header
            .parts
            .iter()
            .all(|part| self.belonged(&part.audience, &member, through))
        {
            return Err(Refusal::NotAuthorized);
        }
        Ok(signer)
    }

    /// Note the S3 key an admin entry names, whether or not it applies.
    pub(crate) fn note_named_key(&mut self, change: &StoreChange) {
        let access = match change {
            StoreChange::CreateStore { access, .. }
            | StoreChange::AddMember { access, .. }
            | StoreChange::SetAccess { access, .. } => access,
            _ => return,
        };
        if let coven_format::MemberAccess::S3AccessKey { access_key_id, .. } = access {
            self.named_keys.insert(access_key_id.clone());
        }
    }

    pub(crate) fn validate(
        &self,
        header: &SlotHeader,
        slot: Slot,
        change: &StoreChange,
    ) -> Result<Option<AdminEffect>, OperationRejection> {
        effects::checked_change(
            &self.state,
            header,
            slot,
            change,
            &self.announcements,
            &self.boundaries,
        )
    }

    /// Install only the effect validated against this preceding state. Invalid
    /// admin attempts still consume their slot and retain their signed original.
    pub(crate) fn accept_admin(&mut self, slot: Slot, header: &SlotHeader, effect: &AdminEffect) {
        let bound = match &effect.change {
            StoreChange::CreateStore { admin, .. } => Some((header.author, *admin)),
            StoreChange::AddMember { member, keys, .. }
            | StoreChange::ReplaceMemberKeys { member, keys, .. } => Some((*member, *keys)),
            _ => None,
        };
        if let StoreChange::SetAccess {
            member,
            access: coven_format::MemberAccess::S3AccessKey { access_key_id, .. },
        } = &effect.change
        {
            self.key_records.push(KeyRecord {
                member: *member,
                access_key_id: access_key_id.clone(),
                author: header.author,
            });
        }
        if let Some((member, keys)) = bound {
            self.bindings.push(Binding {
                member,
                keys,
                device: header.device,
            });
        }
        changed(
            &self.state.members,
            &effect.state.members,
            slot,
            &mut self.members,
        );
        changed(
            &self.state.devices,
            &effect.state.devices,
            slot,
            &mut self.devices,
        );
        changed(
            &self.state.circles,
            &effect.state.circles,
            slot,
            &mut self.circles,
        );
        if let Some((audience, key, hash)) = introduction(&effect.change) {
            self.introductions.push(Introduction {
                slot,
                audience,
                key,
                hash,
            });
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
                | StoreChange::AddDevice {
                    replaces: Some(_),
                    ..
                }
        ) {
            self.cutoffs.push((slot, effect.change.clone()));
        }
        self.state = effect.state.clone();
    }

    pub(crate) fn commitment(&self, audience: &Audience, key: KeyId) -> Option<[u8; 32]> {
        let mut values = self
            .introductions
            .iter()
            .filter(|value| &value.audience == audience && value.key == key);
        let first = values.next()?.hash;
        values.all(|value| value.hash == first).then_some(first)
    }

    pub(crate) fn accepted(&self, ring: &StoreKeyring, audience: &Audience, key: KeyId) -> bool {
        self.commitment(audience, key)
            .is_some_and(|hash| held_hash(ring, audience, key) == Some(hash))
    }

    pub(crate) fn exposed(&self, audience: &Audience, key: KeyId) -> bool {
        self.copies.iter().any(|copy| {
            let coven_format::sealed_slot::SlotKind::KeyCopy(copy) = &copy.prefix.kind else {
                unreachable!("copy routing")
            };
            let route = &copy.routing;
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
                match self
                    .introductions
                    .iter()
                    .rev()
                    .find(|value| &value.audience == audience)
                {
                    Some(value) => SyncError::KeyUnavailable(value.key),
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
                .all(|prior| prior.boundary.entry().0 <= announcement.covers)
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

    /// Open a required signed box only after its introduction supplies a
    /// matching commitment. Boxes for other recipients still record exposure.
    pub(crate) fn acquire(
        &self,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
    ) -> Result<bool, SyncError> {
        let mut changed = false;
        for saved in &self.copies {
            let coven_format::sealed_slot::SlotKind::KeyCopy(copy) = &saved.prefix.kind else {
                unreachable!("copy routing")
            };
            let route = &copy.routing;
            if !route.held_by(member) || self.accepted(ring, &route.audience, route.key) {
                continue;
            }
            let hash = self.commitment(&route.audience, route.key);
            if hash.is_none() && held_hash(ring, &route.audience, route.key).is_some() {
                continue;
            }
            let context =
                SlotObjectPrefix::copy_context(saved.prefix.format, saved.prefix.key, route)?;
            let result = match route.audience {
                Audience::Store => copy
                    .open_store_key(member, saved.path.as_str(), &context)
                    .map(|key| {
                        if let Some(hash) = hash {
                            key.check_hash(&hash)?;
                        }
                        ring.insert_store_key(key)
                    }),
                Audience::Circle(_) => copy
                    .open_circle_key(member, saved.path.as_str(), &context)
                    .map(|key| {
                        if let Some(hash) = hash {
                            key.check_hash(&hash)?;
                        }
                        ring.insert_circle_key(key)
                    }),
            };
            result
                .map_err(|error| {
                    crate::write_object::damaged(
                        &saved.path,
                        Refusal::Decryption {
                            cause: Some(std::sync::Arc::new(error)),
                        },
                    )
                })?
                .map_err(|error| {
                    crate::write_object::damaged(
                        &saved.path,
                        Refusal::WrongIdentity {
                            cause: Some(std::sync::Arc::new(error)),
                        },
                    )
                })?;
            changed = true;
        }
        Ok(changed)
    }
}

pub(crate) fn introduction(change: &StoreChange) -> Option<(Audience, KeyId, [u8; 32])> {
    match change {
        StoreChange::CreateStore { key, key_hash, .. } => Some((Audience::Store, *key, *key_hash)),
        StoreChange::CreateCircle {
            circle,
            key,
            key_hash,
            ..
        } => Some((Audience::Circle(*circle), *key, *key_hash)),
        StoreChange::RotateKey {
            audience,
            key,
            key_hash,
        } => Some((audience.clone(), *key, *key_hash)),
        _ => None,
    }
}

fn held_hash(ring: &StoreKeyring, audience: &Audience, key: KeyId) -> Option<[u8; 32]> {
    match audience {
        Audience::Store => match ring.store_key(key) {
            Ok(held) => Some(held.hash()),
            Err(coven_crypto::MaterialError::UnknownStoreKey(_)) => None,
            Err(_) => unreachable!("store-key lookup only reports absence"),
        },
        Audience::Circle(circle) => match ring.circle_key(*circle, key) {
            Ok(held) => Some(held.hash()),
            Err(coven_crypto::MaterialError::UnknownCircleKey { .. }) => None,
            Err(_) => unreachable!("circle-key lookup only reports absence"),
        },
    }
}

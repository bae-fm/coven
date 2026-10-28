//! The first store keys of an installation whose custody holds none, and
//! the registration that follows its admission (§13).

use super::{SequenceSync, Session};
use crate::{floor_object, slot_object, snapshot_object::RoutedKey, SyncError};
use coven_crypto::{MemberIdentity, StoreKeyring};
use coven_format::{floor::OriginalSlot, sealed_slot::SlotKind};
use coven_foundation::id_source::{DeviceId, KeyId};
use coven_storage::{ObjectPath, StorageFailure};

/// What storage supplied to a member that holds no store key yet.
pub(crate) enum Bootstrap {
    /// Copies routed to this member opened these keys. They stay tentative
    /// until the history introducing them authenticates.
    Keys(StoreKeyring),
    /// The sequence exists, but no copy for this member was found; reading
    /// it needs this key.
    Missing(KeyId),
    /// Neither a floor nor a first slot exists.
    Empty,
}

/// How this member's binding stands in the applied history (§13).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MemberBinding {
    /// The member id is bound to this installation's pair, by `device`.
    Bound { device: DeviceId },
    /// The member id is absent from the applied history.
    Absent,
    /// The member id is bound to another pair.
    Replaced,
    /// The member was removed.
    Removed,
}

impl SequenceSync {
    /// Custody's keyring, or the store keys this member's own copies supply
    /// when custody holds none (§13.1). A new installation's keys enter
    /// custody with the slots that apply them. An installation that applied
    /// history before custody lost its keys reconstructs that history with
    /// the copies' keys; authenticating it admits them to custody (§12.1).
    pub(crate) async fn ring(
        &self,
        session: &mut Session,
        member: &MemberIdentity,
    ) -> Result<StoreKeyring, SyncError> {
        let mut ring = match self.keys.read()? {
            Some(ring) => ring,
            None => match self.bootstrap_keys(session, member).await? {
                Bootstrap::Keys(ring) => ring,
                Bootstrap::Missing(key) => return Err(SyncError::KeyUnavailable(key)),
                Bootstrap::Empty => {
                    return Err(StorageFailure::NotFound
                        .with_source("the store has neither a floor nor a first slot")
                        .into())
                }
            },
        };
        self.restore_history(session, &mut ring, member).await?;
        Ok(ring)
    }

    /// An installation that applied history before it held keys, such as a
    /// join stopped while loading, reconstructs that history once keys open
    /// it, so the bindings it applied count again (§12.1, §13.2).
    pub(crate) async fn restore_history(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
    ) -> Result<(), SyncError> {
        if session.locked {
            let originals = self.database.original_slots().await?;
            session.history = super::History::restore(&originals, self.store(), ring, member)?;
            self.keys.persist(ring)?;
            session.locked = false;
        }
        Ok(())
    }

    /// The store key that sealed this installation's newest applied original
    /// while custody holds no store key: syncing needs it unlocked first (§12.4).
    pub async fn missing_store_key(
        database: &coven_database::Database,
        keys: &coven_crypto::custody::KeySession<StoreKeyring>,
    ) -> Result<Option<KeyId>, SyncError> {
        if keys.read()?.is_some() {
            return Ok(None);
        }
        let Some(newest) = database
            .original_slots()
            .await?
            .into_iter()
            .max_by_key(|original| original.slot)
        else {
            return Ok(None);
        };
        let length = coven_format::sealed_slot::SlotObjectPrefix::length(&newest.object)?;
        let prefix = coven_format::sealed_slot::SlotObjectPrefix::decode(
            newest
                .object
                .get(..length)
                .ok_or(coven_format::Error::Truncated)?,
        )?;
        Ok(Some(prefix.key))
    }

    /// Open this member's store keys from their copies in storage, keep them
    /// in custody and read forward, without sending (§12.1, E5).
    pub async fn unlock(&self) -> Result<(), SyncError> {
        let mut session = self.session.lock().await;
        let member = self
            .member
            .read()?
            .ok_or(StorageFailure::MemberKeysMissing)?;
        let mut ring = self.ring(&mut session, &member).await?;
        self.read_forward(&mut session, &mut ring, &member).await?;
        self.keys.persist(&ring)?;
        Ok(())
    }

    /// Start the key scan after `slot`: a joiner's copies follow its invite.
    pub(crate) fn scan_after(&self, session: &mut Session, slot: u64) {
        session.scanned = session.scanned.max(slot.saturating_add(1));
    }

    /// The floor supplies copies and the first kept slot before keys are held.
    /// A miss rereads it, since retention may have passed the scan meanwhile.
    /// This applies nothing and advances no applied slot. A scan without keys
    /// keeps its position only on success; opened keys must remain discoverable
    /// until authenticated history retains them in custody.
    pub(crate) async fn bootstrap_keys(
        &self,
        session: &mut Session,
        member: &MemberIdentity,
    ) -> Result<Bootstrap, SyncError> {
        let started = self.clock.elapsed();
        let mut floors = 0;
        let mut scanned = 0;
        let mut bytes = 0;
        let mut keys = Vec::new();
        let mut needed = None;
        let mut next = session.scanned.max(1);
        let mut missed = false;
        loop {
            let first = match floor_object::routed_keys(
                &self.database,
                self.storage.as_ref(),
                &ObjectPath::floor(self.store()),
                member,
            )
            .await?
            {
                Some((key, first, routed)) => {
                    floors += 1;
                    needed = Some(key);
                    keys.extend(routed);
                    first.0
                }
                None => 1,
            };
            if keys.iter().any(|key| matches!(key, RoutedKey::Store(_)))
                || (missed && first <= next)
            {
                break;
            }
            next = next.max(first);
            let mut reads = super::SlotReads::new(self, next, u64::MAX);
            while let Some(input) = reads.next().await? {
                let path = self.path(next)?;
                scanned += 1;
                bytes += input.object().size;
                let prefix = slot_object::clear_prefix(input.local_bytes().await?, &path)?;
                needed.get_or_insert(prefix.key);
                if let SlotKind::KeyCopy(copy) = &prefix.kind {
                    if copy.routing.held_by(member) {
                        let object = slot_object::read_exact(
                            &mut input.local_bytes().await?,
                            (input.object().size)
                                .min(coven_format::sequence::MAX_BOUNDED_SLOT as u64)
                                as usize,
                            &path,
                        )?;
                        let original = OriginalSlot {
                            slot: coven_merge::Slot(next),
                            object,
                        };
                        keys.extend(crate::snapshot_object::routed_key(
                            &original,
                            self.store(),
                            member,
                        )?);
                    }
                }
                next = next.checked_add(1).ok_or(SyncError::SequenceExhausted)?;
            }
            if keys.iter().any(|key| matches!(key, RoutedKey::Store(_))) {
                break;
            }
            missed = true;
        }
        let mut ring: Option<StoreKeyring> = None;
        let mut circles = Vec::new();
        for key in keys {
            match (key, &mut ring) {
                (RoutedKey::Store(key), None) => ring = Some(StoreKeyring::new(key)),
                (key @ RoutedKey::Store(_), Some(ring)) => key.insert(ring)?,
                (key @ RoutedKey::Circle(_), _) => circles.push(key),
            }
        }
        let result = match (ring, needed) {
            (Some(mut ring), _) => {
                for key in circles {
                    key.insert(&mut ring)?;
                }
                Bootstrap::Keys(ring)
            }
            (None, Some(key)) => Bootstrap::Missing(key),
            (None, None) => Bootstrap::Empty,
        };
        if !matches!(result, Bootstrap::Keys(_)) {
            session.scanned = next;
        }
        tracing::info!(
            found = matches!(result, Bootstrap::Keys(_)),
            floors,
            scanned_slots = scanned,
            bytes,
            time = ?(self.clock.elapsed() - started),
            "read this member's store keys from their copies"
        );
        Ok(result)
    }

    /// This member's binding in the applied history.
    pub(crate) fn admission(&self, session: &Session, member: &MemberIdentity) -> MemberBinding {
        let keys = member.keys().public_keys();
        match session.history.state.members.get(&member.member()) {
            None => MemberBinding::Absent,
            Some(current) if current.removed => MemberBinding::Removed,
            Some(current) if current.keys != keys => MemberBinding::Replaced,
            Some(_) => match session
                .history
                .bindings
                .iter()
                .rev()
                .find(|binding| binding.member == member.member() && binding.keys == keys)
            {
                Some(binding) => MemberBinding::Bound {
                    device: binding.device,
                },
                None => MemberBinding::Replaced,
            },
        }
    }
}

/// Check a restored member's pair before it can register: an old restore
/// code cannot bind its keys back (§13.1).
pub(crate) fn restorable(
    admission: MemberBinding,
    member: &MemberIdentity,
) -> Result<(), SyncError> {
    match admission {
        MemberBinding::Bound { .. } => Ok(()),
        MemberBinding::Removed => Err(SyncError::Removed),
        MemberBinding::Replaced => Err(SyncError::MemberKeysReplaced {
            member: member.member(),
        }),
        MemberBinding::Absent => Err(SyncError::Rejected(crate::OperationRejection::TargetGone(
            crate::EntryTarget::Member(member.member()),
        ))),
    }
}

#[cfg(test)]
#[path = "sequence_bootstrap_tests.rs"]
mod tests;
